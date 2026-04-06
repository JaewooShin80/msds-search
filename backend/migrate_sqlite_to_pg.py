"""
SQLite → PostgreSQL 마이그레이션 스크립트

사전 준비:
  1. Cloud SQL Auth Proxy를 로컬에서 실행
     ./cloud-sql-proxy.exe msds-service:asia-northeast3:msds-db --port 5432
  2. .env 또는 환경변수에 PG_URL 설정
     PG_URL=postgresql://msds_user:PASSWORD@localhost:5432/msds

실행:
  venv/Scripts/python.exe migrate_sqlite_to_pg.py
"""

import json
import os
import sqlite3
import sys

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

SQLITE_PATH = os.getenv("DB_PATH", "./db/msds.db")
PG_URL = os.getenv("PG_URL") or os.getenv("DATABASE_URL")

if not PG_URL or "localhost" not in PG_URL and "127.0.0.1" not in PG_URL:
    print("ERROR: PG_URL 환경변수를 localhost로 설정하세요.")
    print("  예) PG_URL=postgresql://msds_user:PASSWORD@localhost:5432/msds")
    print("  (Cloud SQL Auth Proxy가 실행 중이어야 합니다)")
    sys.exit(1)


def migrate():
    # SQLite 연결
    sqlite_conn = sqlite3.connect(SQLITE_PATH)
    sqlite_conn.row_factory = sqlite3.Row
    sqlite_cur = sqlite_conn.cursor()

    # PostgreSQL 연결
    pg_conn = psycopg2.connect(PG_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    pg_cur = pg_conn.cursor()

    # 기존 레코드 수 확인
    sqlite_cur.execute("SELECT COUNT(*) as cnt FROM msds")
    total = sqlite_cur.fetchone()["cnt"]
    print(f"SQLite 레코드 수: {total}건")

    pg_cur.execute("SELECT COUNT(*) as cnt FROM msds")
    pg_existing = pg_cur.fetchone()["cnt"]
    print(f"PostgreSQL 기존 레코드 수: {pg_existing}건")

    # 전체 데이터 읽기
    sqlite_cur.execute("SELECT * FROM msds ORDER BY id")
    rows = sqlite_cur.fetchall()

    inserted = 0
    skipped = 0

    for row in rows:
        r = dict(row)

        # keywords: 문자열이면 그대로, 없으면 []
        keywords = r.get("keywords") or "[]"
        try:
            json.loads(keywords)
        except (json.JSONDecodeError, TypeError):
            keywords = "[]"

        try:
            pg_cur.execute(
                """
                INSERT INTO msds
                    (product_name, manufacturer, category, hazard_level,
                     revision_date, pdf_path, pdf_url,
                     description, keywords, content_html, ai_analyzed,
                     created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        COALESCE(%s::timestamptz, CURRENT_TIMESTAMP),
                        COALESCE(%s::timestamptz, CURRENT_TIMESTAMP))
                ON CONFLICT DO NOTHING
                """,
                (
                    r.get("product_name"),
                    r.get("manufacturer"),
                    r.get("category"),
                    r.get("hazard_level"),
                    r.get("revision_date"),
                    r.get("pdf_path"),
                    r.get("pdf_url"),
                    r.get("description"),
                    keywords,
                    r.get("content_html"),
                    r.get("ai_analyzed", 0),
                    r.get("created_at"),
                    r.get("updated_at"),
                ),
            )
            if pg_cur.rowcount > 0:
                inserted += 1
            else:
                skipped += 1
        except Exception as e:
            print(f"  [ERROR] id={r.get('id')} {r.get('product_name')}: {e}")
            pg_conn.rollback()
            continue

    pg_conn.commit()

    # 결과 확인
    pg_cur.execute("SELECT COUNT(*) as cnt FROM msds")
    pg_final = pg_cur.fetchone()["cnt"]

    print(f"\n마이그레이션 완료!")
    print(f"  삽입: {inserted}건")
    print(f"  스킵(중복): {skipped}건")
    print(f"  PostgreSQL 최종 레코드 수: {pg_final}건")

    sqlite_conn.close()
    pg_conn.close()


if __name__ == "__main__":
    migrate()
