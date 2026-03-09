"""
Google Drive 유틸리티
- 공유 폴더의 파일 목록 조회 및 다운로드
- Cloud Run 서비스 계정 인증 사용
"""
import io
import re
from typing import Generator

import google.auth
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

_service = None


def _get_service():
    global _service
    if _service is None:
        creds, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/drive.readonly"]
        )
        _service = build("drive", "v3", credentials=creds)
    return _service


def extract_folder_id(url: str) -> str | None:
    """Google Drive 폴더 URL에서 폴더 ID 추출"""
    patterns = [
        r'/folders/([a-zA-Z0-9_-]+)',
        r'[?&]id=([a-zA-Z0-9_-]+)',
    ]
    for p in patterns:
        m = re.search(p, url)
        if m:
            return m.group(1)
    return None


def list_files(folder_id: str) -> list[dict]:
    """폴더 내 파일 목록 조회 (PDF만 필터)"""
    service = _get_service()
    results = []
    page_token = None

    while True:
        resp = service.files().list(
            q=f"'{folder_id}' in parents and trashed = false",
            fields="nextPageToken, files(id, name, mimeType, size)",
            pageSize=100,
            pageToken=page_token,
        ).execute()

        for f in resp.get("files", []):
            results.append({
                "id": f["id"],
                "name": f["name"],
                "mimeType": f.get("mimeType", ""),
                "size": int(f.get("size", 0)),
            })

        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    return results


def download_file(file_id: str) -> bytes:
    """Google Drive 파일을 바이트로 다운로드"""
    service = _get_service()
    request = service.files().get_media(fileId=file_id)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)

    done = False
    while not done:
        _, done = downloader.next_chunk()

    return buffer.getvalue()


def iter_folder_pdfs(folder_id: str) -> Generator[tuple[str, bytes], None, None]:
    """폴더 내 PDF 파일을 순회하며 (파일명, 바이트) 반환"""
    files = list_files(folder_id)
    for f in files:
        if f["mimeType"] == "application/pdf" or f["name"].lower().endswith(".pdf"):
            data = download_file(f["id"])
            yield f["name"], data
