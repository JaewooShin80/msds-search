import json
import os
import re
import uuid
from pathlib import Path
from datetime import date
from typing import Optional
from urllib.parse import quote

import httpx
from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from typing import List
from fastapi.responses import FileResponse, StreamingResponse

from db.database import get_connection
from services.analyzer import analyze
from services.gcs import upload_pdf as gcs_upload_pdf, download_bytes as gcs_download, exists as gcs_exists
from services.gdrive import extract_folder_id, iter_folder_pdfs

router = APIRouter()

UPLOAD_DIR = Path(__file__).parent.parent / os.getenv("UPLOAD_DIR", "./uploads/pdfs")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


# ---------- 헬퍼 ----------

def row_to_dict(row) -> dict:
    d = dict(row)
    d["keywords"] = json.loads(d.get("keywords") or "[]")
    return d


def _save_pdf_to_gcs(pdf_bytes: bytes, original_filename: str) -> str:
    """PDF를 GCS에 업로드하고 GCS 경로 반환"""
    return gcs_upload_pdf(pdf_bytes, original_filename)


def _extract_gdrive_file_id(url: str) -> str | None:
    """Google Drive 공유 URL에서 파일 ID 추출"""
    patterns = [
        r'/file/d/([a-zA-Z0-9_-]+)',       # /file/d/{id}/view
        r'[?&]id=([a-zA-Z0-9_-]+)',         # ?id={id}
        r'/open\?id=([a-zA-Z0-9_-]+)',      # /open?id={id}
    ]
    for p in patterns:
        m = re.search(p, url)
        if m:
            return m.group(1)
    return None


async def _download_from_gdrive(url: str) -> bytes:
    """Google Drive URL에서 PDF 다운로드"""
    file_id = _extract_gdrive_file_id(url)
    if not file_id:
        raise HTTPException(status_code=400, detail="유효한 Google Drive URL이 아닙니다.")

    download_url = f"https://drive.google.com/uc?export=download&id={file_id}"

    async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
        r = await client.get(download_url)

        # 대용량 파일: 확인 페이지 우회
        if r.status_code == 200 and b"virus scan warning" in r.content.lower() or b"confirm=" in r.content:
            confirm_match = re.search(r'confirm=([0-9A-Za-z_-]+)', r.text)
            if confirm_match:
                confirmed_url = f"{download_url}&confirm={confirm_match.group(1)}"
                r = await client.get(confirmed_url)

        if r.status_code != 200:
            raise HTTPException(status_code=502, detail="Google Drive에서 파일을 다운로드할 수 없습니다.")

        content_type = r.headers.get("content-type", "")
        if "application/pdf" not in content_type and len(r.content) < 1000:
            raise HTTPException(status_code=400, detail="PDF 파일이 아니거나 접근 권한이 없습니다. 공유 설정을 확인하세요.")

    return r.content


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


@router.post("/analyze-gdrive")
async def analyze_gdrive(gdrive_url: str = Form(...)):
    """Google Drive URL에서 PDF를 다운로드하여 분석"""
    pdf_bytes = await _download_from_gdrive(gdrive_url)
    result = analyze(pdf_bytes)
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
    """GCS 파일 우선, 로컬 파일 차선, 외부 URL 최후"""
    row = conn.execute("SELECT * FROM msds WHERE id = ?", (msds_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="MSDS를 찾을 수 없습니다.")

    filename_header = f"attachment; filename*=UTF-8''{quote(row['product_name'])}.pdf"

    # 1) GCS 경로 (pdfs/ 로 시작하는 경로)
    if row["pdf_path"] and row["pdf_path"].startswith("pdfs/"):
        try:
            data = gcs_download(row["pdf_path"])
            return StreamingResponse(
                iter([data]),
                media_type="application/pdf",
                headers={"Content-Disposition": filename_header},
            )
        except Exception:
            pass  # GCS 실패 시 다음 방법 시도

    # 2) 로컬 파일 (기존 호환)
    if row["pdf_path"]:
        path = UPLOAD_DIR / row["pdf_path"]
        if path.exists():
            return FileResponse(
                path=str(path),
                media_type="application/pdf",
                headers={"Content-Disposition": filename_header},
            )

    # 3) 외부 URL
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
    gdrive_url:    Optional[str] = Form(None),
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
        if not content_html:
            result = analyze(pdf_bytes)
            content_html = result.get("content_html")
        pdf_path = _save_pdf_to_gcs(pdf_bytes, pdf.filename)
    elif gdrive_url:
        pdf_bytes = await _download_from_gdrive(gdrive_url)
        if not content_html:
            result = analyze(pdf_bytes)
            content_html = result.get("content_html")
        pdf_path = _save_pdf_to_gcs(pdf_bytes, "gdrive.pdf")

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
    gdrive_url:    Optional[str] = Form(None),
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
        pdf_path = _save_pdf_to_gcs(pdf_bytes, pdf.filename)
    elif gdrive_url:
        pdf_bytes = await _download_from_gdrive(gdrive_url)
        if not content_html:
            result = analyze(pdf_bytes)
            content_html = result.get("content_html")
        pdf_path = _save_pdf_to_gcs(pdf_bytes, "gdrive.pdf")

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


# ---------- 로컬 파일 다중 업로드 + AI 분석 + DB 등록 ----------

@router.post("/bulk-upload")
async def bulk_upload(
    pdfs: List[UploadFile] = File(...),
    conn=Depends(get_connection),
):
    """
    여러 PDF 파일을 한번에 업로드하여:
    1. GCS에 저장
    2. AI로 자동 분석
    3. DB에 MSDS 등록
    """
    uploaded = []
    errors = []

    for pdf in pdfs:
        filename = pdf.filename or "unknown.pdf"
        try:
            if pdf.content_type and pdf.content_type != "application/pdf":
                errors.append({"filename": filename, "error": "PDF 파일이 아닙니다."})
                continue

            pdf_bytes = await pdf.read()

            # 1) GCS 업로드
            gcs_path = gcs_upload_pdf(pdf_bytes, filename)

            # 2) AI 분석
            result = analyze(pdf_bytes)
            fields = result.get("fields", {})
            content_html = result.get("content_html", "")
            ai_analyzed = 1 if result.get("mode") == "ai" else 0

            # 3) DB 등록
            kw = json.dumps(fields.get("keywords", []))
            cur = conn.execute(
                """
                INSERT INTO msds
                    (product_name, manufacturer, category, hazard_level,
                     cas_number, revision_date, pdf_path, pdf_url,
                     description, keywords, content_html, ai_analyzed)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    fields.get("product_name") or filename.replace(".pdf", ""),
                    fields.get("manufacturer", "-"),
                    fields.get("category", "기타"),
                    fields.get("hazard_level", "경고"),
                    fields.get("cas_number", "-"),
                    fields.get("revision_date", str(date.today())),
                    gcs_path,
                    None,
                    fields.get("description", ""),
                    kw,
                    content_html,
                    ai_analyzed,
                ),
            )
            conn.commit()

            uploaded.append({
                "id": cur.lastrowid,
                "filename": filename,
                "product_name": fields.get("product_name") or filename,
                "category": fields.get("category", "기타"),
                "mode": result.get("mode", "manual"),
            })
        except Exception as e:
            errors.append({"filename": filename, "error": str(e)})

    conn.close()

    return {
        "message": f"{len(uploaded)}개 MSDS 등록 완료",
        "uploaded": uploaded,
        "errors": errors,
    }


# ---------- Google Drive 폴더 → GCS 업로드 + AI 분석 + DB 등록 ----------

@router.post("/import-gdrive-folder")
def import_gdrive_folder(
    gdrive_folder_url: str = Form(...),
    conn=Depends(get_connection),
):
    """
    Google Drive 공유 폴더의 PDF 파일들을:
    1. GCS에 업로드
    2. AI로 자동 분석 (제품명, 제조사, 카테고리 등)
    3. DB에 MSDS로 등록
    """
    folder_id = extract_folder_id(gdrive_folder_url)
    if not folder_id:
        raise HTTPException(status_code=400, detail="유효한 Google Drive 폴더 URL이 아닙니다.")

    uploaded = []
    errors = []

    for filename, pdf_bytes in iter_folder_pdfs(folder_id):
        try:
            # 1) GCS 업로드
            gcs_path = gcs_upload_pdf(pdf_bytes, filename)

            # 2) AI 분석
            result = analyze(pdf_bytes)
            fields = result.get("fields", {})
            content_html = result.get("content_html", "")
            ai_analyzed = 1 if result.get("mode") == "ai" else 0

            # 3) DB 등록
            kw = json.dumps(fields.get("keywords", []))
            cur = conn.execute(
                """
                INSERT INTO msds
                    (product_name, manufacturer, category, hazard_level,
                     cas_number, revision_date, pdf_path, pdf_url,
                     description, keywords, content_html, ai_analyzed)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    fields.get("product_name") or filename.replace(".pdf", ""),
                    fields.get("manufacturer", "-"),
                    fields.get("category", "기타"),
                    fields.get("hazard_level", "경고"),
                    fields.get("cas_number", "-"),
                    fields.get("revision_date", str(date.today())),
                    gcs_path,
                    None,
                    fields.get("description", ""),
                    kw,
                    content_html,
                    ai_analyzed,
                ),
            )
            conn.commit()

            uploaded.append({
                "id": cur.lastrowid,
                "filename": filename,
                "product_name": fields.get("product_name") or filename,
                "category": fields.get("category", "기타"),
                "mode": result.get("mode", "manual"),
            })
        except Exception as e:
            errors.append({"filename": filename, "error": str(e)})

    conn.close()

    return {
        "message": f"{len(uploaded)}개 MSDS 등록 완료",
        "uploaded": uploaded,
        "errors": errors,
    }
