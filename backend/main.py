import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path

# Windows CP949 인코딩 충돌 방지 — stdout/stderr를 UTF-8로 강제 지정
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

from dotenv import load_dotenv
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from auth import verify_login, require_admin
from db.database import init_db
from db.seed import run as seed_db
from routers import meta, msds

load_dotenv()


# ========== DB 초기화 및 Seed ==========
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    seed_db()
    print("DB 초기화 완료")
    yield


# ========== 앱 초기화 ==========
app = FastAPI(title="MSDS 검색 시스템", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ========== 관리자 인증 ==========
class LoginRequest(BaseModel):
    admin_id: str
    admin_pw: str


@app.post("/api/admin/login")
def admin_login(body: LoginRequest):
    token = verify_login(body.admin_id, body.admin_pw)
    return {"token": token}


@app.get("/api/admin/verify", dependencies=[Depends(require_admin)])
def admin_verify():
    return {"ok": True}


# ========== API 라우터 ==========
app.include_router(msds.router, prefix="/api/msds", tags=["MSDS"])
app.include_router(meta.router, prefix="/api",      tags=["Meta"])

# ========== 정적 파일 서빙 ==========
UPLOAD_DIR   = Path(__file__).parent / os.getenv("UPLOAD_DIR", "./uploads/pdfs")
FRONTEND_DIR = Path(__file__).parent / os.getenv("FRONTEND_DIR", "../frontend")

UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

app.mount("/uploads", StaticFiles(directory=str(UPLOAD_DIR.parent)), name="uploads")
app.mount("/",        StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")


# ========== 서버 직접 실행 ==========
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", 8000)),
        reload=True,
    )
