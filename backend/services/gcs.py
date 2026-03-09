"""
Google Cloud Storage 유틸리티
- PDF 업로드 / 다운로드 / 공개 URL 생성
"""
import os
import uuid
from pathlib import Path

from google.cloud import storage

BUCKET_NAME = os.getenv("GCS_BUCKET", "msdsdata")

_client: storage.Client | None = None


def _get_client() -> storage.Client:
    global _client
    if _client is None:
        _client = storage.Client()
    return _client


def _get_bucket() -> storage.Bucket:
    return _get_client().bucket(BUCKET_NAME)


def upload_bytes(data: bytes, destination_path: str, content_type: str = "application/pdf") -> str:
    """바이트 데이터를 GCS에 업로드하고 GCS 경로를 반환"""
    blob = _get_bucket().blob(destination_path)
    blob.upload_from_string(data, content_type=content_type)
    return destination_path


def upload_pdf(pdf_bytes: bytes, original_filename: str = "") -> str:
    """PDF 바이트를 고유 이름으로 GCS에 업로드, GCS 경로 반환"""
    ext = Path(original_filename).suffix if original_filename else ".pdf"
    gcs_path = f"pdfs/{uuid.uuid4().hex}{ext}"
    return upload_bytes(pdf_bytes, gcs_path)


def download_bytes(gcs_path: str) -> bytes:
    """GCS에서 파일을 다운로드하여 바이트로 반환"""
    blob = _get_bucket().blob(gcs_path)
    return blob.download_as_bytes()


def get_public_url(gcs_path: str) -> str:
    """GCS 객체의 공개 URL 반환 (버킷이 공개 설정된 경우)"""
    return f"https://storage.googleapis.com/{BUCKET_NAME}/{gcs_path}"


def get_signed_url(gcs_path: str, expiration_minutes: int = 60) -> str:
    """서명된 임시 URL 생성"""
    from datetime import timedelta
    blob = _get_bucket().blob(gcs_path)
    return blob.generate_signed_url(expiration=timedelta(minutes=expiration_minutes))


def exists(gcs_path: str) -> bool:
    """GCS에 파일이 존재하는지 확인"""
    blob = _get_bucket().blob(gcs_path)
    return blob.exists()
