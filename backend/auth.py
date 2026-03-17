"""관리자 인증 — PyJWT 기반 (8시간 만료)"""
import hashlib
import os
import secrets
from datetime import datetime, timezone, timedelta

import jwt
from fastapi import Header, HTTPException

_JWT_SALT = "msds-jwt-signing-key-v1"


def _secret_key() -> str:
    # 1) 전용 JWT 서명 키 우선
    jwt_secret = os.getenv("JWT_SECRET", "")
    if jwt_secret:
        return jwt_secret
    # 2) 미설정 시 ADMIN_PW에서 파생 (하위 호환)
    admin_pw = os.getenv("ADMIN_PW", "")
    if not admin_pw:
        raise HTTPException(status_code=500, detail="서버에 관리자 계정이 설정되지 않았습니다.")
    return hashlib.sha256(f"{admin_pw}{_JWT_SALT}".encode()).hexdigest()


def create_token() -> str:
    """JWT 토큰 생성 (만료: 8시간)"""
    now = datetime.now(timezone.utc)
    payload = {"sub": "admin", "iat": now, "exp": now + timedelta(hours=8)}
    return jwt.encode(payload, _secret_key(), algorithm="HS256")


def verify_token(token: str) -> bool:
    """JWT 토큰 검증 — 만료·무효 시 False"""
    try:
        jwt.decode(token, _secret_key(), algorithms=["HS256"])
        return True
    except jwt.ExpiredSignatureError:
        return False
    except jwt.InvalidTokenError:
        return False


def verify_login(admin_id: str, admin_pw: str) -> str:
    """ID/PW 검증 후 JWT 토큰 반환. 실패 시 HTTPException 발생."""
    expected_id = os.getenv("ADMIN_ID", "")
    expected_pw = os.getenv("ADMIN_PW", "")

    if not expected_id or not expected_pw:
        raise HTTPException(status_code=500, detail="서버에 관리자 계정이 설정되지 않았습니다.")

    id_ok = secrets.compare_digest(admin_id, expected_id)
    pw_ok = secrets.compare_digest(admin_pw, expected_pw)

    if not (id_ok and pw_ok):
        raise HTTPException(status_code=401, detail="아이디 또는 비밀번호가 올바르지 않습니다.")

    return create_token()


def require_admin(x_admin_token: str = Header(default=None)):
    """POST/PUT/DELETE 엔드포인트에 적용하는 관리자 인증 dependency"""
    if not x_admin_token:
        raise HTTPException(status_code=401, detail="관리자 인증이 필요합니다.")
    if not verify_token(x_admin_token):
        raise HTTPException(status_code=401, detail="유효하지 않거나 만료된 인증 토큰입니다.")
