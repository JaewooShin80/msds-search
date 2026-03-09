"""
3단계: analysis_results/ 의 JSON을 읽어 DB content_html 업데이트

사용법:
    python apply_analysis.py            # 실제 적용
    python apply_analysis.py --dry-run  # 미리보기 (DB 변경 없음)
    python apply_analysis.py --id 15    # 특정 ID만 적용
"""
import os
import sys
import json
import sqlite3
import argparse
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

BASE_DIR   = Path(__file__).parent
BACKEND    = BASE_DIR / "backend"
DB_PATH    = BACKEND / "db" / "msds.db"
RESULT_DIR = BASE_DIR / "analysis_results"


def get_connection():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="DB 변경 없이 미리보기")
    parser.add_argument("--id", type=int, default=0, help="특정 MSDS ID만 처리")
    args = parser.parse_args()

    result_files = sorted(RESULT_DIR.glob("*.json"), key=lambda f: int(f.stem))

    if args.id:
        result_files = [f for f in result_files if int(f.stem) == args.id]

    if not result_files:
        print("처리할 결과 파일이 없습니다. (analysis_results/ 확인)")
        return

    conn = get_connection()
    ok = fail = 0

    for result_file in result_files:
        msds_id = int(result_file.stem)
        try:
            data = json.loads(result_file.read_text(encoding="utf-8"))
            content_html = data.get("content_html", "")

            if not content_html:
                print(f"[{msds_id:3d}] SKIP content_html 없음")
                continue

            # HTML 길이와 표 태그 포함 여부 확인
            table_count = content_html.count("<table")
            html_len = len(content_html)

            if args.dry_run:
                print(f"[{msds_id:3d}] DRY  {data.get('product_name','?')}  "
                      f"HTML {html_len:,}자 / 표 {table_count}개")
                ok += 1
                continue

            conn.execute(
                """
                UPDATE msds
                SET content_html = ?,
                    ai_analyzed  = 1,
                    updated_at   = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (content_html, msds_id),
            )
            conn.commit()
            print(f"[{msds_id:3d}] OK   {data.get('product_name','?')}  "
                  f"HTML {html_len:,}자 / 표 {table_count}개")
            ok += 1

        except Exception as e:
            print(f"[{msds_id:3d}] FAIL → {e}")
            fail += 1

    conn.close()

    mode = "(DRY-RUN)" if args.dry_run else ""
    print(f"\n완료 {mode}: 성공 {ok}개 / 실패 {fail}개")


if __name__ == "__main__":
    main()
