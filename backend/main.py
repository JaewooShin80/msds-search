import logging
import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pythonjsonlogger import jsonlogger
from pydantic import BaseModel
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from auth import verify_login, require_admin
from db.database import init_db
from db.seed import run as seed_db
from routers import meta, msds
from utils import configure_encoding

configure_encoding()
load_dotenv()

# ========== 구조화 로깅 설정 ==========
_handler = logging.StreamHandler()
_handler.setFormatter(jsonlogger.JsonFormatter("%(asctime)s %(name)s %(levelname)s %(message)s"))
logging.basicConfig(level=logging.INFO, handlers=[_handler])
logger = logging.getLogger(__name__)

# ========== Rate Limiter ==========
limiter = Limiter(key_func=get_remote_address)


# ========== DB 초기화 및 Seed ==========
@asynccontextmanager
async def lifespan(app: FastAPI):
    # SKIP_DB_INIT=1 이면 init/seed 건너뜀 (Vercel cold start 지연 방지)
    if os.getenv("SKIP_DB_INIT", "0") != "1":
        try:
            init_db()
            seed_db()
            logger.info("DB 초기화 완료")
        except Exception as e:
            logger.error("DB 초기화 실패 — 앱은 시작하지만 DB 기능은 동작하지 않을 수 있습니다", extra={"error": str(e)})
    yield


# ========== 앱 초기화 ==========
app = FastAPI(title="MSDS 검색 시스템", version="1.0.0", lifespan=lifespan)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# CORS — 환경변수로 허용 오리진 제한 (미설정 시 cross-origin 차단)
_raw_origins = os.getenv("ALLOWED_ORIGINS", "")
if _raw_origins == "*":
    _origins = ["*"]
elif _raw_origins:
    _origins = [o.strip() for o in _raw_origins.split(",") if o.strip()]
else:
    _origins = []  # same-origin only (no cross-origin allowed)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ========== 보안 헤더 ==========
@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "SAMEORIGIN"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline' cdn.jsdelivr.net; "
        "style-src 'self' 'unsafe-inline' cdn.jsdelivr.net fonts.googleapis.com; "
        "font-src cdn.jsdelivr.net fonts.gstatic.com; "
        "img-src 'self' data: blob:; "
        "connect-src 'self'; "
        "frame-src 'self' blob:;"
    )
    return response


# ========== 헬스체크 ==========
@app.get("/api/health")
def health_check():
    return {"status": "ok"}


@app.get("/api/health/ready")
def readiness_check():
    from db.database import get_db_connection
    try:
        conn = get_db_connection()
        conn.cursor().execute("SELECT 1")
        conn.close()
        return {"status": "ok"}
    except Exception as e:
        logger.error("헬스체크 DB 연결 실패", extra={"error": str(e)})
        return JSONResponse(status_code=503, content={"status": "error", "detail": str(e)})


# ========== 관리자 인증 ==========
class LoginRequest(BaseModel):
    admin_id: str
    admin_pw: str


@app.post("/api/admin/login")
@limiter.limit("5/minute")
def admin_login(request: Request, body: LoginRequest):
    token = verify_login(body.admin_id, body.admin_pw)
    return {"token": token}


@app.get("/api/admin/verify", dependencies=[Depends(require_admin)])
def admin_verify():
    return {"ok": True}


# ========== API 라우터 ==========
app.include_router(msds.router, prefix="/api/msds", tags=["MSDS"])
app.include_router(meta.router, prefix="/api",      tags=["Meta"])


# ========== 로컬 직접 실행 ==========
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", 8000)),
        reload=os.getenv("DEV_RELOAD", "false").lower() == "true",
    )
