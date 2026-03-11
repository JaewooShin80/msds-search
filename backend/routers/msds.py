import asyncio
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

from auth import require_admin
from db.database import get_connection
from services.analyzer import analyze
from services.gcs import upload_pdf as gcs_upload_pdf, download_bytes as gcs_download, exists as gcs_exists, iter_prefix_pdfs as gcs_iter_prefix_pdfs

router = APIRouter()

UPLOAD_DIR = Path(__file__).parent.parent / os.getenv("UPLOAD_DIR", "./uploads/pdfs")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

MAX_PDF_SIZE = 50 * 1024 * 1024  # 50MB


# ---------- 헬퍼 ----------

def row_to_dict(row) -> dict:
    d = dict(row)
    d["keywords"] = json.loads(d.get("keywords") or "[]")
    return d


def _validate_pdf(pdf_bytes: bytes) -> None:
    """H4: magic bytes 검증 + 크기 제한 (50MB)"""
    if len(pdf_bytes) > MAX_PDF_SIZE:
        raise HTTPException(status_code=413, detail="파일 크기가 50MB를 초과합니다.")
    if not pdf_bytes.startswith(b"%PDF"):
        raise HTTPException(status_code=400, detail="유효한 PDF 파일이 아닙니다.")


def _save_pdf_to_gcs(pdf_bytes: bytes, original_filename: str) -> str:
    """PDF를 GCS에 업로드하고 GCS 경로 반환"""
    return gcs_upload_pdf(pdf_bytes, original_filename)


def _extract_gdrive_file_id(url: str) -> Optional[str]:
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

        # H2: 연산자 우선순위 버그 수정 — 괄호 추가
        if r.status_code == 200 and (b"virus scan warning" in r.content.lower() or b"confirm=" in r.content):
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

@router.post("/analyze", dependencies=[Depends(require_admin)])
async def analyze_pdf(pdf: UploadFile = File(...)):
    pdf_bytes = await pdf.read()
    _validate_pdf(pdf_bytes)                              # H4 — magic bytes로 PDF 검증
    result = await asyncio.to_thread(analyze, pdf_bytes)  # H6
    result["extracted_preview"] = result.pop("extracted", "")[:3000]
    return result


@router.post("/analyze-gdrive", dependencies=[Depends(require_admin)])
async def analyze_gdrive(gdrive_url: str = Form(...)):
    pdf_bytes = await _download_from_gdrive(gdrive_url)
    _validate_pdf(pdf_bytes)                              # H4
    result = await asyncio.to_thread(analyze, pdf_bytes)  # H6
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
            AND (product_name LIKE %s OR manufacturer LIKE %s
                 OR cas_number LIKE %s OR description LIKE %s OR keywords LIKE %s)
        """
        params.extend([like, like, like, like, like])

    if category:
        cats = [c.strip() for c in category.split(",") if c.strip()]
        sql += f" AND category IN ({','.join(['%s'] * len(cats))})"
        params.extend(cats)

    if hazard:
        hazards = [h.strip() for h in hazard.split(",") if h.strip()]
        sql += f" AND hazard_level IN ({','.join(['%s'] * len(hazards))})"
        params.extend(hazards)

    if manufacturer:
        mfrs = [m.strip() for m in manufacturer.split(",") if m.strip()]
        sql += f" AND manufacturer IN ({','.join(['%s'] * len(mfrs))})"
        params.extend(mfrs)

    sql += " ORDER BY id ASC"

    cur = conn.cursor()
    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return [row_to_dict(r) for r in rows]


# ---------- 단건 조회 ----------

@router.get("/{msds_id}")
def get_one(msds_id: int, conn=Depends(get_connection)):
    cur = conn.cursor()
    cur.execute("SELECT * FROM msds WHERE id = %s", (msds_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="MSDS를 찾을 수 없습니다.")
    return row_to_dict(row)


# ---------- 다운로드 ----------

@router.get("/{msds_id}/download")
async def download(msds_id: int, conn=Depends(get_connection)):
    """GCS 파일 우선, 로컬 파일 차선, 외부 URL 최후"""
    cur = conn.cursor()
    cur.execute("SELECT * FROM msds WHERE id = %s", (msds_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="MSDS를 찾을 수 없습니다.")

    filename_header = f"attachment; filename*=UTF-8''{quote(row['product_name'])}.pdf"

    # 1) GCS 우선 시도
    if row["pdf_path"]:
        try:
            data = gcs_download(row["pdf_path"])
            return StreamingResponse(
                iter([data]),
                media_type="application/pdf",
                headers={"Content-Disposition": filename_header},
            )
        except Exception:
            pass

    # 2) 로컬 파일 (기존 호환)
    if row["pdf_path"]:
        path = UPLOAD_DIR / row["pdf_path"]
        if path.exists():
            return FileResponse(
                path=str(path),
                media_type="application/pdf",
                headers={"Content-Disposition": filename_header},
            )

    # 3) 외부 URL — H3: verify=False 제거
    if row["pdf_url"]:
        async with httpx.AsyncClient(timeout=30) as client:
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

@router.post("", status_code=201, dependencies=[Depends(require_admin)])
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
        _validate_pdf(pdf_bytes)                                      # H4
        if not content_html:
            result = await asyncio.to_thread(analyze, pdf_bytes)      # H6
            content_html = result.get("content_html")
        pdf_path = _save_pdf_to_gcs(pdf_bytes, pdf.filename)
    elif gdrive_url:
        pdf_bytes = await _download_from_gdrive(gdrive_url)
        _validate_pdf(pdf_bytes)                                      # H4
        if not content_html:
            result = await asyncio.to_thread(analyze, pdf_bytes)      # H6
            content_html = result.get("content_html")
        pdf_path = _save_pdf_to_gcs(pdf_bytes, "gdrive.pdf")

    try:
        kw = json.dumps(json.loads(keywords) if keywords else [])
    except json.JSONDecodeError:
        raise HTTPException(status_code=422, detail="keywords는 유효한 JSON 배열이어야 합니다.")

    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO msds
            (product_name, manufacturer, category, hazard_level,
             cas_number, revision_date, pdf_path, pdf_url,
             description, keywords, content_html, ai_analyzed)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (product_name, manufacturer, category, hazard_level,
         cas_number, revision_date, pdf_path, pdf_url,
         description, kw, content_html, ai_analyzed),
    )
    new_id = cur.fetchone()["id"]
    conn.commit()
    cur.execute("SELECT * FROM msds WHERE id = %s", (new_id,))
    row = cur.fetchone()
    conn.close()
    return row_to_dict(row)


# ---------- 수정 ----------

@router.put("/{msds_id}", dependencies=[Depends(require_admin)])
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
    cur = conn.cursor()
    cur.execute("SELECT * FROM msds WHERE id = %s", (msds_id,))
    existing = cur.fetchone()
    if not existing:
        conn.close()
        raise HTTPException(status_code=404, detail="MSDS를 찾을 수 없습니다.")

    e = dict(existing)
    pdf_path = e["pdf_path"]

    if pdf and pdf.filename:
        pdf_bytes = await pdf.read()
        _validate_pdf(pdf_bytes)                                      # H4
        if not content_html:
            result = await asyncio.to_thread(analyze, pdf_bytes)      # H6
            content_html = result.get("content_html")
        pdf_path = _save_pdf_to_gcs(pdf_bytes, pdf.filename)
    elif gdrive_url:
        pdf_bytes = await _download_from_gdrive(gdrive_url)
        _validate_pdf(pdf_bytes)                                      # H4
        if not content_html:
            result = await asyncio.to_thread(analyze, pdf_bytes)      # H6
            content_html = result.get("content_html")
        pdf_path = _save_pdf_to_gcs(pdf_bytes, "gdrive.pdf")

    try:
        kw = json.dumps(json.loads(keywords)) if keywords else e["keywords"]
    except json.JSONDecodeError:
        raise HTTPException(status_code=422, detail="keywords는 유효한 JSON 배열이어야 합니다.")

    cur.execute(
        """
        UPDATE msds SET
            product_name  = %s,
            manufacturer  = %s,
            category      = %s,
            hazard_level  = %s,
            cas_number    = %s,
            revision_date = %s,
            pdf_path      = %s,
            pdf_url       = %s,
            description   = %s,
            keywords      = %s,
            content_html  = %s,
            updated_at    = CURRENT_TIMESTAMP
        WHERE id = %s
        """,
        (
            product_name  if product_name  is not None else e["product_name"],
            manufacturer  if manufacturer  is not None else e["manufacturer"],
            category      if category      is not None else e["category"],
            hazard_level  if hazard_level  is not None else e["hazard_level"],
            cas_number    if cas_number    is not None else e["cas_number"],
            revision_date if revision_date is not None else e["revision_date"],
            pdf_path,
            pdf_url       if pdf_url       is not None else e["pdf_url"],
            description   if description   is not None else e["description"],
            kw,
            content_html  if content_html  is not None else e["content_html"],
            msds_id,
        ),
    )
    conn.commit()
    cur.execute("SELECT * FROM msds WHERE id = %s", (msds_id,))
    row = cur.fetchone()
    conn.close()
    return row_to_dict(row)


# ---------- 삭제 ----------

@router.delete("/{msds_id}", dependencies=[Depends(require_admin)])
def delete(msds_id: int, conn=Depends(get_connection)):
    cur = conn.cursor()
    cur.execute("SELECT * FROM msds WHERE id = %s", (msds_id,))
    existing = cur.fetchone()
    if not existing:
        conn.close()
        raise HTTPException(status_code=404, detail="MSDS를 찾을 수 없습니다.")
    cur.execute("DELETE FROM msds WHERE id = %s", (msds_id,))
    conn.commit()
    conn.close()
    return {"message": "삭제되었습니다."}


# ---------- 로컬 파일 다중 업로드 + AI 분석 + DB 등록 ----------

@router.post("/bulk-upload", dependencies=[Depends(require_admin)])
async def bulk_upload(
    pdfs: List[UploadFile] = File(...),
    conn=Depends(get_connection),
):
    uploaded = []
    errors = []
    cur = conn.cursor()

    for pdf in pdfs:
        filename = pdf.filename or "unknown.pdf"
        try:
            pdf_bytes = await pdf.read()
            _validate_pdf(pdf_bytes)                                  # H4 — magic bytes로 PDF 검증

            gcs_path = gcs_upload_pdf(pdf_bytes, filename)

            result = await asyncio.to_thread(analyze, pdf_bytes)      # H6
            fields = result.get("fields", {})
            content_html = result.get("content_html", "")
            ai_analyzed = 1 if result.get("mode") == "ai" else 0

            kw = json.dumps(fields.get("keywords", []))
            cur.execute(
                """
                INSERT INTO msds
                    (product_name, manufacturer, category, hazard_level,
                     cas_number, revision_date, pdf_path, pdf_url,
                     description, keywords, content_html, ai_analyzed)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
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
            new_id = cur.fetchone()["id"]
            conn.commit()

            uploaded.append({
                "id": new_id,
                "filename": filename,
                "product_name": fields.get("product_name") or filename,
                "category": fields.get("category", "기타"),
                "mode": result.get("mode", "manual"),
            })
        except HTTPException as e:
            errors.append({"filename": filename, "error": e.detail})
        except Exception as e:
            errors.append({"filename": filename, "error": str(e)})

    conn.close()
    return {
        "message": f"{len(uploaded)}개 MSDS 등록 완료",
        "uploaded": uploaded,
        "errors": errors,
    }


# ---------- GCS 폴더 → AI 분석 + DB 등록 ----------

@router.post("/import-gcs-folder", dependencies=[Depends(require_admin)])
async def import_gcs_folder(
    gcs_prefix: str = Form(...),
    conn=Depends(get_connection),
):
    CONCURRENCY = 8

    prefix = gcs_prefix.rstrip("/") + "/"

    cur = conn.cursor()
    cur.execute("SELECT pdf_path FROM msds WHERE pdf_path IS NOT NULL")
    existing_paths = set(r["pdf_path"] for r in cur.fetchall())

    blobs = await asyncio.to_thread(
        lambda: list(gcs_iter_prefix_pdfs(prefix))
    )

    pending = [(b, f, d) for b, f, d in blobs if b not in existing_paths]
    skipped = [{"filename": f} for b, f, d in blobs if b in existing_paths]

    uploaded = []
    errors = []
    sem = asyncio.Semaphore(CONCURRENCY)

    async def process_one(blob_name: str, filename: str, pdf_bytes: bytes):
        async with sem:
            try:
                result = await asyncio.to_thread(analyze, pdf_bytes)
                fields = result.get("fields", {})
                return {
                    "blob_name": blob_name,
                    "filename": filename,
                    "fields": fields,
                    "content_html": result.get("content_html", ""),
                    "ai_analyzed": 1 if result.get("mode") == "ai" else 0,
                    "mode": result.get("mode", "manual"),
                    "error": None,
                }
            except Exception as e:
                return {"blob_name": blob_name, "filename": filename, "error": str(e)}

    results = await asyncio.gather(*[process_one(b, f, d) for b, f, d in pending])

    for r in results:
        if r.get("error"):
            errors.append({"filename": r["filename"], "error": r["error"]})
            continue
        fields = r["fields"]
        kw = json.dumps(fields.get("keywords", []))
        try:
            cur.execute(
                """
                INSERT INTO msds
                    (product_name, manufacturer, category, hazard_level,
                     cas_number, revision_date, pdf_path, pdf_url,
                     description, keywords, content_html, ai_analyzed)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    fields.get("product_name") or r["filename"].replace(".pdf", ""),
                    fields.get("manufacturer", "-"),
                    fields.get("category", "기타"),
                    fields.get("hazard_level", "경고"),
                    fields.get("cas_number", "-"),
                    fields.get("revision_date", str(date.today())),
                    r["blob_name"],
                    None,
                    fields.get("description", ""),
                    kw,
                    r["content_html"],
                    r["ai_analyzed"],
                ),
            )
            new_id = cur.fetchone()["id"]
            conn.commit()
            uploaded.append({
                "id": new_id,
                "filename": r["filename"],
                "product_name": fields.get("product_name") or r["filename"],
                "category": fields.get("category", "기타"),
                "mode": r["mode"],
            })
        except Exception as e:
            errors.append({"filename": r["filename"], "error": str(e)})

    conn.close()
    return {
        "message": f"{len(uploaded)}개 MSDS 등록 완료 (건너뜀: {len(skipped)}개)",
        "uploaded": uploaded,
        "skipped": skipped,
        "errors": errors,
    }


# ---------- Google Drive 폴더 → GCS 업로드 + AI 분석 + DB 등록 ----------

@router.post("/import-gdrive-folder", dependencies=[Depends(require_admin)])
def import_gdrive_folder(
    gdrive_folder_url: str = Form(...),
    conn=Depends(get_connection),
):
    from services.gdrive import extract_folder_id, iter_folder_pdfs

    folder_id = extract_folder_id(gdrive_folder_url)
    if not folder_id:
        raise HTTPException(status_code=400, detail="유효한 Google Drive 폴더 URL이 아닙니다.")

    uploaded = []
    errors = []
    cur = conn.cursor()

    for filename, pdf_bytes in iter_folder_pdfs(folder_id):
        try:
            gcs_path = gcs_upload_pdf(pdf_bytes, filename)

            result = analyze(pdf_bytes)  # sync 함수이므로 to_thread 불필요
            fields = result.get("fields", {})
            content_html = result.get("content_html", "")
            ai_analyzed = 1 if result.get("mode") == "ai" else 0

            kw = json.dumps(fields.get("keywords", []))
            cur.execute(
                """
                INSERT INTO msds
                    (product_name, manufacturer, category, hazard_level,
                     cas_number, revision_date, pdf_path, pdf_url,
                     description, keywords, content_html, ai_analyzed)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
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
            new_id = cur.fetchone()["id"]
            conn.commit()

            uploaded.append({
                "id": new_id,
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


# ---------- 미분석 레코드 AI 재분석 ----------

@router.post("/reanalyze-pending", dependencies=[Depends(require_admin)])
async def reanalyze_pending(conn=Depends(get_connection)):
    CONCURRENCY = 8

    cur = conn.cursor()
    cur.execute(
        """SELECT id, product_name, pdf_path, pdf_url
           FROM msds
           WHERE ai_analyzed = 0
             AND (pdf_path IS NOT NULL OR pdf_url IS NOT NULL)"""
    )
    rows = cur.fetchall()

    if not rows:
        conn.close()
        return {"message": "재분석할 항목이 없습니다.", "updated": [], "errors": []}

    sem = asyncio.Semaphore(CONCURRENCY)

    async def fetch_and_analyze(row):
        async with sem:
            msds_id  = row["id"]
            pdf_path = row["pdf_path"]
            pdf_url  = row["pdf_url"]
            name     = row["product_name"]
            try:
                if pdf_path:
                    pdf_bytes = await asyncio.to_thread(gcs_download, pdf_path)
                else:
                    # H3: verify=False 제거
                    async with httpx.AsyncClient(timeout=60) as client:
                        r = await client.get(pdf_url)
                    if r.status_code != 200:
                        return {"id": msds_id, "name": name, "error": f"HTTP {r.status_code}"}
                    pdf_bytes = r.content

                result = await asyncio.to_thread(analyze, pdf_bytes)
                return {
                    "id": msds_id,
                    "name": name,
                    "fields": result.get("fields", {}),
                    "content_html": result.get("content_html", ""),
                    "ai_analyzed": 1 if result.get("mode") == "ai" else 0,
                    "mode": result.get("mode", "manual"),
                    "error": None,
                }
            except Exception as e:
                return {"id": msds_id, "name": name, "error": str(e)}

    results = await asyncio.gather(*[fetch_and_analyze(row) for row in rows])

    updated = []
    errors  = []

    for r in results:
        if r.get("error"):
            errors.append({"id": r["id"], "name": r["name"], "error": r["error"]})
            continue

        fields = r["fields"]
        kw = json.dumps(fields.get("keywords", []))

        if r["ai_analyzed"]:
            cur.execute(
                """UPDATE msds SET
                    product_name  = %s,
                    manufacturer  = %s,
                    category      = %s,
                    hazard_level  = %s,
                    cas_number    = %s,
                    revision_date = %s,
                    description   = %s,
                    keywords      = %s,
                    content_html  = %s,
                    ai_analyzed   = 1,
                    updated_at    = CURRENT_TIMESTAMP
                   WHERE id = %s""",
                (
                    fields.get("product_name") or r["name"],
                    fields.get("manufacturer", "-"),
                    fields.get("category", "기타"),
                    fields.get("hazard_level", "경고"),
                    fields.get("cas_number", "-"),
                    fields.get("revision_date", str(date.today())),
                    fields.get("description", ""),
                    kw,
                    r["content_html"],
                    r["id"],
                ),
            )
        else:
            cur.execute(
                "UPDATE msds SET content_html = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s",
                (r["content_html"], r["id"]),
            )

        conn.commit()
        updated.append({
            "id": r["id"],
            "product_name": fields.get("product_name") or r["name"],
            "mode": r["mode"],
        })

    conn.close()
    return {
        "message": f"{len(updated)}개 재분석 완료",
        "updated": updated,
        "errors": errors,
    }
