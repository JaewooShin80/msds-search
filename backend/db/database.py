import sqlite3
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

DB_PATH = Path(__file__).parent.parent / os.getenv("DB_PATH", "./db/msds.db")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

SCHEMA_PATH = Path(__file__).parent / "schema.sql"


def get_connection() -> sqlite3.Connection:
    """요청마다 새 연결 반환 (FastAPI Depends 용)"""
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row   # dict-like 접근
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    """앱 시작 시 스키마 초기화"""
    conn = get_connection()
    schema = SCHEMA_PATH.read_text(encoding="utf-8")
    conn.executescript(schema)
    conn.commit()
    conn.close()
