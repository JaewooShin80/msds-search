import os
import psycopg2
import psycopg2.extras
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
SCHEMA_PATH = Path(__file__).parent / "schema.sql"


def get_db_connection() -> psycopg2.extensions.connection:
    """직접 연결 생성 (스크립트/init_db 용)"""
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)


def get_connection():
    """FastAPI Depends 용 generator"""
    conn = get_db_connection()
    try:
        yield conn
    finally:
        if not conn.closed:
            conn.close()


def init_db():
    """앱 시작 시 스키마 초기화"""
    conn = get_db_connection()
    cur = conn.cursor()
    schema = SCHEMA_PATH.read_text(encoding="utf-8")
    for stmt in schema.split(";"):
        stmt = stmt.strip()
        if stmt:
            cur.execute(stmt)
    conn.commit()
    cur.close()
    conn.close()
