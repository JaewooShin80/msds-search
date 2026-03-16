import logging
import os

import psycopg2
import psycopg2.extras
import psycopg2.pool
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")
SCHEMA_PATH = Path(__file__).parent / "schema.sql"

_pool: psycopg2.pool.ThreadedConnectionPool | None = None


def _get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    global _pool
    if _pool is None:
        _pool = psycopg2.pool.ThreadedConnectionPool(
            2, 10,
            DATABASE_URL,
            cursor_factory=psycopg2.extras.RealDictCursor,
        )
    return _pool


def get_db_connection() -> psycopg2.extensions.connection:
    """직접 연결 생성 (init_db 전용)"""
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)


def get_connection():
    """FastAPI Depends 용 generator (커넥션 풀 기반)"""
    pool = _get_pool()
    conn = pool.getconn()
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


def _migrate(cur):
    """idempotent 마이그레이션"""
    # 1) 구 CHECK constraint 제거
    cur.execute("""
        SELECT 1 FROM information_schema.table_constraints
        WHERE table_name='msds' AND constraint_type='CHECK'
          AND constraint_name='msds_hazard_level_check'
    """)
    if cur.fetchone():
        cur.execute("ALTER TABLE msds DROP CONSTRAINT msds_hazard_level_check")

    # 2) 데이터 변환 (idempotent)
    cur.execute("UPDATE msds SET hazard_level='해당없음' WHERE hazard_level='주의'")

    # 3) 새 CHECK constraint 추가
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

    # 5) 전문 검색 컬럼 + GIN 인덱스 추가 (search_vector)
    cur.execute("""
        SELECT 1 FROM information_schema.columns
        WHERE table_name='msds' AND column_name='search_vector'
    """)
    if not cur.fetchone():
        cur.execute("""
            ALTER TABLE msds ADD COLUMN search_vector tsvector
                GENERATED ALWAYS AS (
                    to_tsvector('simple',
                        coalesce(product_name, '') || ' ' ||
                        coalesce(manufacturer, '') || ' ' ||
                        coalesce(description, '') || ' ' ||
                        coalesce(keywords, '')
                    )
                ) STORED
        """)
        cur.execute(
            "CREATE INDEX idx_msds_search_vector ON msds USING GIN(search_vector)"
        )
        logger.info("search_vector 컬럼 및 GIN 인덱스 추가 완료")


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
