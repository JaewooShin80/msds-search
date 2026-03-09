import json
import os
import uuid
from pathlib import Path
from typing import Optional
from urllib.parse import quote

import httpx
from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, StreamingResponse

from db.database import get_connection
from services.analyzer import analyze

router = APIRouter()

UPLOAD_DIR = Path(__file__).parent.parent / os.getenv("UPLOAD_DIR", "./uploads/pdfs")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


# ---------- 헬퍼 ----------

def row_to_dict(row) -> dict:
    d = dict(row)
    d["keywords"] = json.loads(d.get("keywords") or "[]")
    return d


def _save_pdf(upload: UploadFile) -> str:
    """업로드 파일을 저장하고 파일명 반환"""
    if upload.content_type != "application/pdf":
        raise HTTPException(status_code=400, detail="PDF 파일만 업로드 가능합니다.")
    ext = Path(upload.filename).suffix or ".pdf"
    filename = f"{uuid.uuid4().hex}{ext}"
    (UPLOAD_DIR / filename).write_bytes(upload.file.read())
    return filename


# ---------- PDF 분석 (등록 전 미리보기) ----------

@router.post("/analyze")
async def analyze_pdf(pdf: UploadFile = File(...)):
    """
    PDF를 업로드하면 텍스트를 추출하고 AI(또는 수동 입력용 빈 폼)를 반환합니다.

    반환:
      mode        : "ai" | "manual"
      fields      : 폼 자동 채우기용 필드 (mode=manual 이면 빈 값)
      content_html: HTML 변환 내용
      extracted   : 원문 텍스트 (수동 입력 시 참조용)
    """
    if pdf.content_type != "application/pdf":
        raise HTTPException(status_code=400, detail="PDF 파일만 업로드 가능합니다.")
    pdf_bytes = await pdf.read()
    result = analyze(pdf_bytes)
    # extracted는 용량이 크므로 앞 3000자만 반환 (참조용)
    result["extracted_preview"] = result.pop("extracted", "")[:3000]
    return result


# ---------- 목록 조회 (검색 + 필터) ----------

@router.get("")
def get_all(
    q: Optional[str] = None,
    category: Optional[str] = None,
    hazard: Optional[str] = None,
    manufacturer: Optional[str] = None,
    conn=Depends(get_connection),
):
    sql = "SELECT * FROM msds WHERE 1=1"
    params: list = []

    if q:
        like = f"%{q}%"
        sql += """
            AND (product_name LIKE ? OR manufacturer LIKE ?
                 OR cas_number LIKE ? OR description LIKE ? OR keywords LIKE ?)
        """
        params.extend([like, like, like, like, like])

    if category:
        cats = [c.strip() for c in category.split(",") if c.strip()]
        sql += f" AND category IN ({','.join('?' * len(cats))})"
        params.extend(cats)

    if hazard:
        hazards = [h.strip() for h in hazard.split(",") if h.strip()]
        sql += f" AND hazard_level IN ({','.join('?' * len(hazards))})"
        params.extend(hazards)

    if manufacturer:
        mfrs = [m.strip() for m in manufacturer.split(",") if m.strip()]
        sql += f" AND manufacturer IN ({','.join('?' * len(mfrs))})"
        params.extend(mfrs)

    sql += " ORDER BY id ASC"

    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [row_to_dict(r) for r in rows]


# ---------- 단건 조회 ----------

@router.get("/{msds_id}")
def get_one(msds_id: int, conn=Depends(get_connection)):
    row = conn.execute("SELECT * FROM msds WHERE id = ?", (msds_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="MSDS를 찾을 수 없습니다.")
    return row_to_dict(row)


# ---------- 다운로드 ----------

@router.get("/{msds_id}/download")
async def download(msds_id: int, conn=Depends(get_connection)):
    """로컬 파일이 있으면 직접 전송, 외부 URL만 있으면 서버에서 프록시 다운로드"""
    row = conn.execute("SELECT * FROM msds WHERE id = ?", (msds_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="MSDS를 찾을 수 없습니다.")

    filename_header = f"attachment; filename*=UTF-8''{quote(row['product_name'])}.pdf"

    if row["pdf_path"]:
        path = UPLOAD_DIR / row["pdf_path"]
        if not path.exists():
            raise HTTPException(status_code=404, detail="파일이 존재하지 않습니다.")
        return FileResponse(
            path=str(path),
            media_type="application/pdf",
            headers={"Content-Disposition": filename_header},
        )

    if row["pdf_url"]:
        async with httpx.AsyncClient(timeout=30, verify=False) as client:
            r = await client.get(row["pdf_url"])
        if r.status_code != 200:
            raise HTTPException(status_code=502, detail="원본 PDF를 가져올 수 없습니다.")
        return StreamingResponse(
            iter([r.content]),
            media_type="application/pdf",
            headers={"Content-Disposition": filename_header},
        )

    raise HTTPException(status_code=404, detail="다운로드 가능한 파일이 없습니다.")


# ---------- 등록 ----------

@router.post("", status_code=201)
async def create(
    product_name:  str  = Form(...),
    manufacturer:  str  = Form(...),
    category:      str  = Form(...),
    hazard_level:  str  = Form(...),
    revision_date: str  = Form(...),
    cas_number:    str  = Form("-"),
    pdf_url:       Optional[str] = Form(None),
    description:   Optional[str] = Form(None),
    keywords:      Optional[str] = Form("[]"),
    content_html:  Optional[str] = Form(None),
    ai_analyzed:   int  = Form(0),
    pdf:           Optional[UploadFile] = File(None),
    conn=Depends(get_connection),
):
    pdf_path = None
    if pdf and pdf.filename:
        pdf_bytes = await pdf.read()
        # 분석 결과의 content_html이 없으면 여기서 추출
        if not content_html:
            result = analyze(pdf_bytes)
            content_html = result.get("content_html")
        ext = Path(pdf.filename).suffix or ".pdf"
        filename = f"{uuid.uuid4().hex}{ext}"
        (UPLOAD_DIR / filename).write_bytes(pdf_bytes)
        pdf_path = filename

    kw = json.dumps(json.loads(keywords) if keywords else [])

    cur = conn.execute(
        """
        INSERT INTO msds
            (product_name, manufacturer, category, hazard_level,
             cas_number, revision_date, pdf_path, pdf_url,
             description, keywords, content_html, ai_analyzed)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (product_name, manufacturer, category, hazard_level,
         cas_number, revision_date, pdf_path, pdf_url,
         description, kw, content_html, ai_analyzed),
    )
    conn.commit()
    row = conn.execute("SELECT * FROM msds WHERE id = ?", (cur.lastrowid,)).fetchone()
    conn.close()
    return row_to_dict(row)


# ---------- 수정 ----------

@router.put("/{msds_id}")
async def update(
    msds_id:       int,
    product_name:  Optional[str] = Form(None),
    manufacturer:  Optional[str] = Form(None),
    category:      Optional[str] = Form(None),
    hazard_level:  Optional[str] = Form(None),
    revision_date: Optional[str] = Form(None),
    cas_number:    Optional[str] = Form(None),
    pdf_url:       Optional[str] = Form(None),
    description:   Optional[str] = Form(None),
    keywords:      Optional[str] = Form(None),
    content_html:  Optional[str] = Form(None),
    pdf:           Optional[UploadFile] = File(None),
    conn=Depends(get_connection),
):
    existing = conn.execute("SELECT * FROM msds WHERE id = ?", (msds_id,)).fetchone()
    if not existing:
        conn.close()
        raise HTTPException(status_code=404, detail="MSDS를 찾을 수 없습니다.")

    e = dict(existing)
    pdf_path = e["pdf_path"]

    if pdf and pdf.filename:
        pdf_bytes = await pdf.read()
        if not content_html:
            result = analyze(pdf_bytes)
            content_html = result.get("content_html")
        ext = Path(pdf.filename).suffix or ".pdf"
        filename = f"{uuid.uuid4().hex}{ext}"
        (UPLOAD_DIR / filename).write_bytes(pdf_bytes)
        pdf_path = filename

    kw = json.dumps(json.loads(keywords)) if keywords else e["keywords"]

    conn.execute(
        """
        UPDATE msds SET
            product_name  = ?,
            manufacturer  = ?,
            category      = ?,
            hazard_level  = ?,
            cas_number    = ?,
            revision_date = ?,
            pdf_path      = ?,
            pdf_url       = ?,
            description   = ?,
            keywords      = ?,
            content_html  = ?,
            updated_at    = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (
            product_name  or e["product_name"],
            manufacturer  or e["manufacturer"],
            category      or e["category"],
            hazard_level  or e["hazard_level"],
            cas_number    if cas_number is not None else e["cas_number"],
            revision_date or e["revision_date"],
            pdf_path,
            pdf_url       if pdf_url is not None else e["pdf_url"],
            description   if description is not None else e["description"],
            kw,
            content_html  if content_html is not None else e["content_html"],
            msds_id,
        ),
    )
    conn.commit()
    row = conn.execute("SELECT * FROM msds WHERE id = ?", (msds_id,)).fetchone()
    conn.close()
    return row_to_dict(row)


# ---------- 삭제 ----------

@router.delete("/{msds_id}")
def delete(msds_id: int, conn=Depends(get_connection)):
    existing = conn.execute("SELECT * FROM msds WHERE id = ?", (msds_id,)).fetchone()
    if not existing:
        conn.close()
        raise HTTPException(status_code=404, detail="MSDS를 찾을 수 없습니다.")
    conn.execute("DELETE FROM msds WHERE id = ?", (msds_id,))
    conn.commit()
    conn.close()
    return {"message": "삭제되었습니다."}
