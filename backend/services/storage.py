"""
Supabase Storage 유틸리티 (구 GCS 대체)
- PDF 업로드 / 다운로드 / 존재 여부 확인 / 폴더 순회
"""
import os
import uuid
from pathlib import Path
from typing import Optional

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")
STORAGE_BUCKET = os.getenv("STORAGE_BUCKET", "pdfs")

_client = None


def _get_client():
    global _client
    if _client is None:
        from supabase import create_client
        _client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    return _client


def upload_pdf(pdf_bytes: bytes, original_filename: str = "") -> str:
    """PDF 바이트를 고유 이름으로 Supabase Storage에 업로드, 경로 반환"""
    ext = Path(original_filename).suffix if original_filename else ".pdf"
    path = f"pdfs/{uuid.uuid4().hex}{ext}"
    _get_client().storage.from_(STORAGE_BUCKET).upload(
        path,
        pdf_bytes,
        {"content-type": "application/pdf", "upsert": "false"},
    )
    return path


def download_bytes(path: str) -> bytes:
    """Supabase Storage에서 파일을 다운로드하여 바이트로 반환"""
    return _get_client().storage.from_(STORAGE_BUCKET).download(path)


def get_public_url(path: str) -> str:
    """Supabase Storage 공개 URL 반환 (버킷이 public인 경우)"""
    return _get_client().storage.from_(STORAGE_BUCKET).get_public_url(path)


def exists(path: str) -> bool:
    """Supabase Storage에 파일이 존재하는지 확인"""
    try:
        download_bytes(path)
        return True
    except Exception:
        return False


def list_prefix_pdfs(prefix: str):
    """
    Supabase Storage prefix 내 PDF 파일 목록 반환 — (storage_path, 파일명) 튜플 리스트.
    다운로드 없이 메타데이터만 수집 (import-storage-folder 의 두 단계 처리용).
    """
    prefix = prefix.rstrip("/")
    folder = prefix.split("/", 1)[-1] if "/" in prefix else prefix

    try:
        files = _get_client().storage.from_(STORAGE_BUCKET).list(folder)
    except Exception:
        return []

    result = []
    for file_obj in files:
        name = file_obj.get("name", "")
        if name.lower().endswith(".pdf"):
            result.append((f"{folder}/{name}", name))
    return result


def iter_prefix_pdfs(prefix: str):
    """
    Supabase Storage prefix 내 PDF 파일을 순회하며
    (storage_path, 파일명, 바이트) 반환
    """
    prefix = prefix.rstrip("/")
    folder = prefix.split("/", 1)[-1] if "/" in prefix else prefix

    try:
        files = _get_client().storage.from_(STORAGE_BUCKET).list(folder)
    except Exception:
        return

    for file_obj in files:
        name = file_obj.get("name", "")
        if name.lower().endswith(".pdf"):
            storage_path = f"{folder}/{name}"
            try:
                pdf_bytes = download_bytes(storage_path)
                yield storage_path, name, pdf_bytes
            except Exception:
                continue
