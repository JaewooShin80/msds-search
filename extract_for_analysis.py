"""
1단계: PDF에서 텍스트 + 표 데이터를 추출하여 analysis_queue/ 에 JSON으로 저장

사용법:
    python extract_for_analysis.py           # 전체 처리
    python extract_for_analysis.py --limit 5 # 테스트용 5개만
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

BASE_DIR    = Path(__file__).parent
BACKEND     = BASE_DIR / "backend"
DB_PATH     = BACKEND / "db" / "msds.db"
UPLOAD_DIR  = BACKEND / "uploads" / "pdfs"
QUEUE_DIR   = BASE_DIR / "analysis_queue"
RESULT_DIR  = BASE_DIR / "analysis_results"

QUEUE_DIR.mkdir(exist_ok=True)
RESULT_DIR.mkdir(exist_ok=True)

sys.path.insert(0, str(BACKEND))


def get_connection():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def extract_page_data(page) -> dict:
    """페이지에서 텍스트(표 영역 제외)와 표 데이터를 추출"""
    tables = []
    table_bboxes = []

    try:
        detected = page.find_tables()
        for t in detected:
            rows = t.extract()
            clean_rows = [
                [str(cell).strip() if cell is not None else "" for cell in row]
                for row in rows
            ]
            tables.append({
                "bbox": list(t.bbox),
                "rows": clean_rows,
            })
            table_bboxes.append(t.bbox)
    except Exception:
        pass  # find_tables 미지원 버전 or 오류 시 무시

    if table_bboxes:
        # 표 영역 바깥의 텍스트 블록만 수집 (이중 출력 방지)
        outside_lines = []
        for block in sorted(page.get_text("blocks"), key=lambda b: b[1]):
            bx0, by0, bx1, by1, btext, *_ = block
            in_table = any(
                bx0 >= tx0 - 5 and by0 >= ty0 - 5 and bx1 <= tx1 + 5 and by1 <= ty1 + 5
                for tx0, ty0, tx1, ty1 in table_bboxes
            )
            if not in_table and btext.strip():
                outside_lines.append(btext)
        text = "\n".join(outside_lines)
    else:
        text = page.get_text()

    return {
        "text": text,
        "tables": tables,
        "has_tables": len(tables) > 0,
    }


def already_queued(msds_id: int) -> bool:
    return (QUEUE_DIR / f"{msds_id}.json").exists()


def already_done(msds_id: int) -> bool:
    return (RESULT_DIR / f"{msds_id}.json").exists()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0, help="처리 개수 제한 (0=전체)")
    parser.add_argument("--force", action="store_true", help="이미 처리된 것도 재처리")
    args = parser.parse_args()

    import fitz

    conn = get_connection()
    rows = conn.execute(
        "SELECT id, product_name, manufacturer, category, pdf_path FROM msds WHERE pdf_path IS NOT NULL ORDER BY id"
    ).fetchall()
    conn.close()

    total = len(rows)
    if args.limit:
        rows = rows[:args.limit]

    print(f"DB 레코드: {total}개 / 처리 대상: {len(rows)}개\n")

    ok = skip = fail = 0

    for rec in rows:
        msds_id      = rec["id"]
        product_name = rec["product_name"]
        manufacturer = rec["manufacturer"]
        category     = rec["category"]
        pdf_path     = rec["pdf_path"]

        prefix = f"[{msds_id:3d}]"

        if not args.force and (already_queued(msds_id) or already_done(msds_id)):
            print(f"{prefix} SKIP (기존 파일 존재) {product_name}")
            skip += 1
            continue

        pdf_file = UPLOAD_DIR / pdf_path
        if not pdf_file.exists():
            print(f"{prefix} FAIL (PDF 없음) {pdf_file.name}")
            fail += 1
            continue

        try:
            doc = fitz.open(str(pdf_file))
            pages_data = [extract_page_data(page) for page in doc]
            doc.close()

            table_count = sum(len(p["tables"]) for p in pages_data)

            payload = {
                "id":           msds_id,
                "product_name": product_name,
                "manufacturer": manufacturer,
                "category":     category,
                "pages":        pages_data,
            }

            out_path = QUEUE_DIR / f"{msds_id}.json"
            out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

            print(f"{prefix} OK  {product_name}  [표 {table_count}개 감지]")
            ok += 1

        except Exception as e:
            print(f"{prefix} FAIL {product_name} → {e}")
            fail += 1

    print(f"\n완료: 성공 {ok}개 / 스킵 {skip}개 / 실패 {fail}개")
    print(f"→ analysis_queue/ 에 {ok}개 JSON 저장됨")


if __name__ == "__main__":
    main()
