import asyncio
import ipaddress
import json
import logging
import os
import re
import uuid
from datetime import date
from pathlib import Path
from typing import Optional
from urllib.parse import quote, urlparse

import httpx
from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from typing import List
from fastapi.responses import FileResponse, StreamingResponse

from auth import require_admin
from db.database import get_connection
from services.analyzer import analyze
from services.gcs import (
    upload_pdf as gcs_upload_pdf,
    download_bytes as gcs_download,
    exists as gcs_exists,
    list_prefix_pdfs as gcs_list_prefix_pdfs,
)

router = APIRouter()
logger = logging.getLogger(__name__)

UPLOAD_DIR = Path(__file__).parent.parent / os.getenv("UPLOAD_DIR", "./uploads/pdfs")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

MAX_PDF_SIZE = 50 * 1024 * 1024  # 50MB


# ---------- 헬퍼 ----------

def row_to_dict(row) -> dict:
    d = dict(row)
    d["keywords"] = json.loads(d.get("keywords") or "[]")
    d.pop("search_vector", None)  # FTS 컬럼은 응답에서 제외
    return d


def _validate_pdf(pdf_bytes: bytes) -> None:
    """magic bytes 검증 + 크기 제한 (50MB)"""
    if len(pdf_bytes) > MAX_PDF_SIZE:
        raise HTTPException(status_code=413, detail="파일 크기가 50MB를 초과합니다.")
    if not pdf_bytes.startswith(b"%PDF"):
        raise HTTPException(status_code=400, detail="유효한 PDF 파일이 아닙니다.")


def _validate_url(url: str) -> None:
    """SSRF 방어: https만 허용, 사설IP/localhost/메타데이터 서버 차단"""
    parsed = urlparse(url)
    if parsed.scheme not in ("https",):
        raise HTTPException(status_code=400, detail="https URL만 허용됩니다.")

    hostname = parsed.hostname
    if not hostname:
        raise HTTPException(status_code=400, detail="유효하지 않은 URL입니다.")

    # Localhost 및 메타데이터 서버 차단
    blocked_hosts = {"localhost", "127.0.0.1", "::1", "169.254.169.254", "metadata.google.internal"}
    if hostname in blocked_hosts:
        raise HTTPException(status_code=400, detail="내부 주소로의 요청은 허용되지 않습니다.")

    # 사설 IP 주소 차단
    try:
        ip = ipaddress.ip_address(hostname)
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
            raise HTTPException(status_code=400, detail="사설 IP 주소로의 요청은 허용되지 않습니다.")
    except ValueError:
        pass  # 도메인 이름은 IP 파싱 불필요


def _insert_msds(cur, data: dict) -> int:
    """MSDS INSERT 공통 헬퍼 — 새 id 반환"""
    cur.execute(
        """
        INSERT INTO msds
            (product_name, manufacturer, category, hazard_level,
             revision_date, pdf_path, pdf_url,
             description, keywords, content_html, ai_analyzed)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            data["product_name"],
            data["manufacturer"],
            data["category"],
            data["hazard_level"],
            data["revision_date"],
            data.get("pdf_path"),
            data.get("pdf_url"),
            data.get("description", ""),
            data["keywords"],
            data.get("content_html"),
            data.get("ai_analyzed", 0),
        ),
    )
    return cur.fetchone()["id"]


async def _process_pdf(pdf_bytes: bytes, filename: str) -> tuple:
    """PDF 처리 파이프라인: validate → analyze → GCS upload
    반환: (gcs_path, fields, content_html, ai_analyzed)"""
    _validate_pdf(pdf_bytes)
    gcs_path = await asyncio.to_thread(gcs_upload_pdf, pdf_bytes, filename)
    result = await asyncio.to_thread(analyze, pdf_bytes)
    fields = result.get("fields", {})
    content_html = result.get("content_html", "")
    ai_analyzed = 1 if result.get("mode") == "ai" else 0
    return gcs_path, fields, content_html, ai_analyzed


def _extract_gdrive_file_id(url: str) -> Optional[str]:
    """Google Drive 공유 URL에서 파일 ID 추출"""
    patterns = [
        r'/file/d/([a-zA-Z0-9_-]+)',
        r'[?&]id=([a-zA-Z0-9_-]+)',
        r'/open\?id=([a-zA-Z0-9_-]+)',
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
    _validate_pdf(pdf_bytes)
    result = await asyncio.to_thread(analyze, pdf_bytes)
    result["extracted_preview"] = result.pop("extracted", "")[:3000]
    return result


@router.post("/analyze-gdrive", dependencies=[Depends(require_admin)])
async def analyze_gdrive(gdrive_url: str = Form(...)):
    pdf_bytes = await _download_from_gdrive(gdrive_url)
    _validate_pdf(pdf_bytes)
    result = await asyncio.to_thread(analyze, pdf_bytes)
    result["extracted_preview"] = result.pop("extracted", "")[:3000]
    return result


# ---------- 목록 조회 (검색 + 필터 + 페이지네이션) ----------

@router.get("")
def get_all(
    q: Optional[str] = None,
    category: Optional[str] = None,
    hazard: Optional[str] = None,
    manufacturer: Optional[str] = None,
    page: int = 1,
    page_size: int = 20,
    conn=Depends(get_connection),
):
    page_size = min(max(page_size, 1), 100)
    page = max(page, 1)
    offset = (page - 1) * page_size

    where = "WHERE 1=1"
    params: list = []

    if q:
        where += """
            AND (
                search_vector @@ plainto_tsquery('simple', %s)
                OR product_name ILIKE %s
                OR manufacturer ILIKE %s
                OR description ILIKE %s
                OR keywords ILIKE %s
            )
        """
        like = f"%{q}%"
        params.extend([q, like, like, like, like])

    if category:
        cats = [c.strip() for c in category.split(",") if c.strip()]
        where += f" AND category IN ({','.join(['%s'] * len(cats))})"
        params.extend(cats)

    if hazard:
        hazards = [h.strip() for h in hazard.split(",") if h.strip()]
        where += f" AND hazard_level IN ({','.join(['%s'] * len(hazards))})"
        params.extend(hazards)

    if manufacturer:
        mfrs = [m.strip() for m in manufacturer.split(",") if m.strip()]
        where += f" AND manufacturer IN ({','.join(['%s'] * len(mfrs))})"
        params.extend(mfrs)

    cur = conn.cursor()

    cur.execute(f"SELECT COUNT(*) FROM msds {where}", params)
    total = cur.fetchone()["count"]

    cur.execute(
        f"""
        SELECT id, product_name, manufacturer, category, hazard_level,
               revision_date, pdf_path, pdf_url, description, keywords,
               ai_analyzed, created_at, updated_at
        FROM msds {where}
        ORDER BY id ASC
        LIMIT %s OFFSET %s
        """,
        params + [page_size, offset],
    )
    rows = cur.fetchall()

    return {
        "items": [row_to_dict(r) for r in rows],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


# ---------- 단건 조회 ----------

@router.get("/{msds_id}")
def get_one(msds_id: int, conn=Depends(get_connection)):
    cur = conn.cursor()
    cur.execute("SELECT * FROM msds WHERE id = %s", (msds_id,))
    row = cur.fetchone()
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
    if not row:
        raise HTTPException(status_code=404, detail="MSDS를 찾을 수 없습니다.")

    filename_header = f"inline; filename*=UTF-8''{quote(row['product_name'])}.pdf"

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

    # 2) 로컬 파일 (기존 호환) — 경로 탐색 방어
    if row["pdf_path"]:
        path = UPLOAD_DIR / row["pdf_path"]
        try:
            path.resolve().relative_to(UPLOAD_DIR.resolve())
        except ValueError:
            raise HTTPException(status_code=400, detail="유효하지 않은 파일 경로입니다.")
        if path.exists():
            return FileResponse(
                path=str(path),
                media_type="application/pdf",
                headers={"Content-Disposition": filename_header},
            )

    # 3) 외부 URL — SSRF 방어
    if row["pdf_url"]:
        _validate_url(row["pdf_url"])
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
        _validate_pdf(pdf_bytes)
        if not content_html:
            result = await asyncio.to_thread(analyze, pdf_bytes)
            content_html = result.get("content_html")
        pdf_path = await asyncio.to_thread(gcs_upload_pdf, pdf_bytes, pdf.filename)
    elif gdrive_url:
        pdf_bytes = await _download_from_gdrive(gdrive_url)
        _validate_pdf(pdf_bytes)
        if not content_html:
            result = await asyncio.to_thread(analyze, pdf_bytes)
            content_html = result.get("content_html")
        pdf_path = await asyncio.to_thread(gcs_upload_pdf, pdf_bytes, "gdrive.pdf")

    if pdf_url:
        _validate_url(pdf_url)

    try:
        kw = json.dumps(json.loads(keywords) if keywords else [])
    except json.JSONDecodeError:
        raise HTTPException(status_code=422, detail="keywords는 유효한 JSON 배열이어야 합니다.")

    cur = conn.cursor()
    new_id = _insert_msds(cur, {
        "product_name": product_name,
        "manufacturer": manufacturer,
        "category": category,
        "hazard_level": hazard_level,
        "revision_date": revision_date,
        "pdf_path": pdf_path,
        "pdf_url": pdf_url,
        "description": description,
        "keywords": kw,
        "content_html": content_html,
        "ai_analyzed": ai_analyzed,
    })
    conn.commit()
    cur.execute("SELECT * FROM msds WHERE id = %s", (new_id,))
    row = cur.fetchone()
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
        raise HTTPException(status_code=404, detail="MSDS를 찾을 수 없습니다.")

    e = dict(existing)
    pdf_path = e["pdf_path"]

    if pdf and pdf.filename:
        pdf_bytes = await pdf.read()
        _validate_pdf(pdf_bytes)
        if not content_html:
            result = await asyncio.to_thread(analyze, pdf_bytes)
            content_html = result.get("content_html")
        pdf_path = await asyncio.to_thread(gcs_upload_pdf, pdf_bytes, pdf.filename)
    elif gdrive_url:
        pdf_bytes = await _download_from_gdrive(gdrive_url)
        _validate_pdf(pdf_bytes)
        if not content_html:
            result = await asyncio.to_thread(analyze, pdf_bytes)
            content_html = result.get("content_html")
        pdf_path = await asyncio.to_thread(gcs_upload_pdf, pdf_bytes, "gdrive.pdf")

    if pdf_url:
        _validate_url(pdf_url)

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
    return row_to_dict(row)


# ---------- 삭제 ----------

@router.delete("/{msds_id}", dependencies=[Depends(require_admin)])
def delete(msds_id: int, conn=Depends(get_connection)):
    cur = conn.cursor()
    cur.execute("SELECT id FROM msds WHERE id = %s", (msds_id,))
    if not cur.fetchone():
        raise HTTPException(status_code=404, detail="MSDS를 찾을 수 없습니다.")
    cur.execute("DELETE FROM msds WHERE id = %s", (msds_id,))
    conn.commit()
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
            _validate_pdf(pdf_bytes)

            gcs_path = await asyncio.to_thread(gcs_upload_pdf, pdf_bytes, filename)
            result = await asyncio.to_thread(analyze, pdf_bytes)
            fields = result.get("fields", {})
            content_html = result.get("content_html", "")
            ai_analyzed = 1 if result.get("mode") == "ai" else 0

            new_id = _insert_msds(cur, {
                "product_name": fields.get("product_name") or filename.replace(".pdf", ""),
                "manufacturer": fields.get("manufacturer", "-"),
                "category": fields.get("category", "기타"),
                "hazard_level": fields.get("hazard_level", "경고"),
                "revision_date": fields.get("revision_date", str(date.today())),
                "pdf_path": gcs_path,
                "pdf_url": None,
                "description": fields.get("description", ""),
                "keywords": json.dumps(fields.get("keywords", [])),
                "content_html": content_html,
                "ai_analyzed": ai_analyzed,
            })
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
            logger.error("bulk_upload 오류: %s", str(e))
            errors.append({"filename": filename, "error": str(e)})

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

    # 메타데이터만 먼저 수집 (바이트 미로드)
    all_meta = await asyncio.to_thread(
        lambda: list(gcs_list_prefix_pdfs(prefix))
    )

    pending = [(b, f) for b, f in all_meta if b not in existing_paths]
    skipped = [{"filename": f} for b, f in all_meta if b in existing_paths]

    uploaded = []
    errors = []
    sem = asyncio.Semaphore(CONCURRENCY)

    async def process_one(blob_name: str, filename: str):
        async with sem:
            try:
                pdf_bytes = await asyncio.to_thread(gcs_download, blob_name)
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

    results = await asyncio.gather(*[process_one(b, f) for b, f in pending])

    for r in results:
        if r.get("error"):
            errors.append({"filename": r["filename"], "error": r["error"]})
            continue
        fields = r["fields"]
        try:
            new_id = _insert_msds(cur, {
                "product_name": fields.get("product_name") or r["filename"].replace(".pdf", ""),
                "manufacturer": fields.get("manufacturer", "-"),
                "category": fields.get("category", "기타"),
                "hazard_level": fields.get("hazard_level", "경고"),
                "revision_date": fields.get("revision_date", str(date.today())),
                "pdf_path": r["blob_name"],
                "pdf_url": None,
                "description": fields.get("description", ""),
                "keywords": json.dumps(fields.get("keywords", [])),
                "content_html": r["content_html"],
                "ai_analyzed": r["ai_analyzed"],
            })
            conn.commit()
            uploaded.append({
                "id": new_id,
                "filename": r["filename"],
                "product_name": fields.get("product_name") or r["filename"],
                "category": fields.get("category", "기타"),
                "mode": r["mode"],
            })
        except Exception as e:
            logger.error("import_gcs_folder INSERT 오류: %s", str(e))
            errors.append({"filename": r["filename"], "error": str(e)})

    return {
        "message": f"{len(uploaded)}개 MSDS 등록 완료 (건너뜀: {len(skipped)}개)",
        "uploaded": uploaded,
        "skipped": skipped,
        "errors": errors,
    }


# ---------- Google Drive 폴더 → GCS 업로드 + AI 분석 + DB 등록 ----------

@router.post("/import-gdrive-folder", dependencies=[Depends(require_admin)])
async def import_gdrive_folder(
    gdrive_folder_url: str = Form(...),
    conn=Depends(get_connection),
):
    from services.gdrive import extract_folder_id, iter_folder_pdfs

    folder_id = extract_folder_id(gdrive_folder_url)
    if not folder_id:
        raise HTTPException(status_code=400, detail="유효한 Google Drive 폴더 URL이 아닙니다.")

    # 동기 generator를 스레드풀에서 실행
    blobs = await asyncio.to_thread(lambda: list(iter_folder_pdfs(folder_id)))

    uploaded = []
    errors = []
    cur = conn.cursor()

    for filename, pdf_bytes in blobs:
        try:
            gcs_path = await asyncio.to_thread(gcs_upload_pdf, pdf_bytes, filename)
            result = await asyncio.to_thread(analyze, pdf_bytes)
            fields = result.get("fields", {})
            content_html = result.get("content_html", "")
            ai_analyzed = 1 if result.get("mode") == "ai" else 0

            new_id = _insert_msds(cur, {
                "product_name": fields.get("product_name") or filename.replace(".pdf", ""),
                "manufacturer": fields.get("manufacturer", "-"),
                "category": fields.get("category", "기타"),
                "hazard_level": fields.get("hazard_level", "경고"),
                "revision_date": fields.get("revision_date", str(date.today())),
                "pdf_path": gcs_path,
                "pdf_url": None,
                "description": fields.get("description", ""),
                "keywords": json.dumps(fields.get("keywords", [])),
                "content_html": content_html,
                "ai_analyzed": ai_analyzed,
            })
            conn.commit()

            uploaded.append({
                "id": new_id,
                "filename": filename,
                "product_name": fields.get("product_name") or filename,
                "category": fields.get("category", "기타"),
                "mode": result.get("mode", "manual"),
            })
        except Exception as e:
            logger.error("import_gdrive_folder 오류: %s", str(e))
            errors.append({"filename": filename, "error": str(e)})

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
                    _validate_url(pdf_url)
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

    return {
        "message": f"{len(updated)}개 재분석 완료",
        "updated": updated,
        "errors": errors,
    }
