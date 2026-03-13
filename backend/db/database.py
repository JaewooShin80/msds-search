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
    # 1) 데이터 먼저 변환 (idempotent)
    cur.execute("UPDATE msds SET hazard_level='해당없음' WHERE hazard_level='주의'")

    # 2) 구 CHECK constraint 제거 (이미 없으면 스킵)
    cur.execute("""
        SELECT 1 FROM information_schema.table_constraints
        WHERE table_name='msds' AND constraint_type='CHECK'
          AND constraint_name='msds_hazard_level_check'
    """)
    if cur.fetchone():
        cur.execute("ALTER TABLE msds DROP CONSTRAINT msds_hazard_level_check")

    # 3) 새 CHECK constraint 추가 — 다른 이름으로 구분해 중복 추가 방지
    cur.execute("""
        SELECT 1 FROM information_schema.table_constraints
        WHERE table_name='msds' AND constraint_type='CHECK'
          AND constraint_name='msds_hazard_level_check2'
    """)
    if not cur.fetchone():
        cur.execute("""
            ALTER TABLE msds ADD CONSTRAINT msds_hazard_level_check2
            CHECK (hazard_level IN ('위험', '경고', '해당없음'))
        """)

    # 4) cas_number 컬럼 드롭
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
