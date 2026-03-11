import os
import hmac
import hashlib
import secrets
from fastapi import Header, HTTPException


def _expected_token() -> str:
    """ADMIN_ID + ADMIN_PW 기반 stateless 토큰 생성 (Cloud Run 다중 인스턴스 대응)"""
    admin_id = os.getenv("ADMIN_ID", "")
    admin_pw = os.getenv("ADMIN_PW", "")
    return hmac.new(admin_pw.encode(), admin_id.encode(), hashlib.sha256).hexdigest()


def verify_login(admin_id: str, admin_pw: str) -> str:
    """ID/PW 검증 후 토큰 반환. 실패 시 HTTPException 발생."""
    expected_id = os.getenv("ADMIN_ID", "")
    expected_pw = os.getenv("ADMIN_PW", "")

    if not expected_id or not expected_pw:
        raise HTTPException(status_code=500, detail="서버에 관리자 계정이 설정되지 않았습니다.")

    id_ok = secrets.compare_digest(admin_id, expected_id)
    pw_ok = secrets.compare_digest(admin_pw, expected_pw)

    if not (id_ok and pw_ok):
        raise HTTPException(status_code=401, detail="아이디 또는 비밀번호가 올바르지 않습니다.")

    return _expected_token()


def require_admin(x_admin_token: str = Header(default=None)):
    """POST/PUT/DELETE 엔드포인트에 적용하는 관리자 인증 dependency"""
    if not x_admin_token:
        raise HTTPException(status_code=401, detail="관리자 인증이 필요합니다.")
    if not secrets.compare_digest(x_admin_token, _expected_token()):
        raise HTTPException(status_code=401, detail="유효하지 않은 인증 토큰입니다.")
