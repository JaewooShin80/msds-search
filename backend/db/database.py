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


def _migrate(cur):
    """idempotent 마이그레이션: 주의→해당없음, cas_number 컬럼 드롭"""
    # 1) 데이터 먼저 변환 — constraint 교체 전에 실행해야 ADD CONSTRAINT 성공
    cur.execute("UPDATE msds SET hazard_level='해당없음' WHERE hazard_level='주의'")
    # 2) CHECK constraint 교체 (이미 변경돼 있으면 스킵)
    cur.execute("""
        SELECT constraint_name FROM information_schema.table_constraints
        WHERE table_name='msds' AND constraint_type='CHECK'
          AND constraint_name='msds_hazard_level_check'
    """)
    if cur.fetchone():
        cur.execute("ALTER TABLE msds DROP CONSTRAINT msds_hazard_level_check")
        cur.execute("""ALTER TABLE msds
            ADD CONSTRAINT msds_hazard_level_check
            CHECK (hazard_level IN ('위험', '경고', '해당없음'))""")
    # 3) cas_number 컬럼 드롭
    cur.execute("ALTER TABLE msds DROP COLUMN IF EXISTS cas_number")


def init_db():
    """앱 시작 시 스키마 초기화"""
    conn = get_db_connection()
    cur = conn.cursor()
    schema = SCHEMA_PATH.read_text(encoding="utf-8")
    for stmt in schema.split(";"):
        stmt = stmt.strip()
        if stmt:
            cur.execute(stmt)
    _migrate(cur)
    conn.commit()
    cur.close()
    conn.close()
