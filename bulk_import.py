"""
DATA 폴더의 PDF를 일괄 등록하는 스크립트
사용법: python bulk_import.py [--ai]
  --ai  : ANTHROPIC_API_KEY가 있을 때 Claude AI 분석 사용 (느림)
"""
import os
import sys
import json
import uuid
import sqlite3
import argparse
from pathlib import Path
from datetime import date

# Windows 인코딩 설정
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

# ── 경로 설정 ─────────────────────────────────────────────
BASE_DIR   = Path(__file__).parent
DATA_DIR   = BASE_DIR / "DATA"
BACKEND    = BASE_DIR / "backend"
DB_PATH    = BACKEND / "db" / "msds.db"
UPLOAD_DIR = BACKEND / "uploads" / "pdfs"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

# analyzer 모듈 경로 추가
sys.path.insert(0, str(BACKEND))

# ── 폴더명 → 카테고리 매핑 ───────────────────────────────
FOLDER_TO_CATEGORY = {
    "01_그리스-윤활유":           "윤활유/그리스",
    "02_시멘트-몰탈-혼화제":      "시멘트류",
    "03_접착제-실란트":           "몰탈/접착제",
    "04_가스(산소-질소-아르곤-LPG)": "가스류",
    "05_스프레류(락카 등)":        "스프레이류",
    "06_도료-페인트":             "기타",
    "07_절단-연마":               "절단/연마",
    "08_연료(휘발유,경유)":        "연료(유류)",
    "09_발파-폭약":               "발파/폭약류",
    "10_부동액":                  "부동액",
    "11_요소수":                  "요소수",
    "12_박리제":                  "박리제",
    "13_용접 재료":               "용접재료",
    "14_품질시험":                "품질시험",
    "15_기타":                    "기타",
}


def parse_filename(stem: str):
    """
    파일명 줄기(stem)에서 제품명과 제조사 추출.
    형식: "제품명_제조사명"  (마지막 _ 기준 분리)
    """
    if "_" in stem:
        idx = stem.rfind("_")
        return stem[:idx].strip(), stem[idx+1:].strip()
    return stem.strip(), "미상"


def get_connection():
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def already_exists(conn, product_name: str, manufacturer: str) -> bool:
    row = conn.execute(
        "SELECT id FROM msds WHERE product_name=? AND manufacturer=?",
        (product_name, manufacturer),
    ).fetchone()
    return row is not None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ai", action="store_true", help="Claude AI 분석 사용")
    args = parser.parse_args()

    use_ai = args.ai and bool(os.environ.get("ANTHROPIC_API_KEY", "").strip())
    if args.ai and not use_ai:
        print("⚠️  ANTHROPIC_API_KEY 없음 → 수동 모드로 진행")

    from dotenv import load_dotenv
    load_dotenv(BACKEND / ".env")

    from services.analyzer import analyze, extract_text, text_to_html
    from services.gcs import upload_pdf as gcs_upload_pdf

    conn = get_connection()

    pdf_files = list(DATA_DIR.rglob("*.pdf"))
    total = len(pdf_files)
    print(f"총 {total}개 PDF 발견\n")

    ok = skip = fail = 0

    for i, pdf_path in enumerate(sorted(pdf_files), 1):
        folder = pdf_path.parent.name
        category = FOLDER_TO_CATEGORY.get(folder, "기타")
        product_name, manufacturer = parse_filename(pdf_path.stem)

        prefix = f"[{i:3d}/{total}]"

        # 중복 체크
        if already_exists(conn, product_name, manufacturer):
            print(f"{prefix} SKIP (중복) {product_name} / {manufacturer}")
            skip += 1
            continue

        try:
            pdf_bytes = pdf_path.read_bytes()

            # PDF를 GCS에 업로드
            new_filename = gcs_upload_pdf(pdf_bytes, pdf_path.name)

            # 분석
            if use_ai:
                result = analyze(pdf_bytes)
                fields = result.get("fields", {})
                content_html = result.get("content_html", "")
                ai_analyzed = 1 if result.get("mode") == "ai" else 0
                # AI가 추출한 값 우선, 없으면 파일명 기반
                product_name  = fields.get("product_name")  or product_name
                manufacturer  = fields.get("manufacturer")  or manufacturer
                category      = fields.get("category")      or category
                hazard_level  = fields.get("hazard_level")  or "경고"
                cas_number    = fields.get("cas_number")    or "-"
                revision_date = fields.get("revision_date") or str(date.today())
                description   = fields.get("description")  or ""
                keywords      = json.dumps(fields.get("keywords") or [], ensure_ascii=False)
            else:
                text = extract_text(pdf_bytes)
                content_html = text_to_html(text)
                ai_analyzed  = 0
                hazard_level = "경고"
                cas_number   = "-"
                revision_date = str(date.today())
                description  = ""
                keywords     = "[]"

            conn.execute(
                """
                INSERT INTO msds
                    (product_name, manufacturer, category, hazard_level,
                     cas_number, revision_date, pdf_path, pdf_url,
                     description, keywords, content_html, ai_analyzed)
                VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?)
                """,
                (product_name, manufacturer, category, hazard_level,
                 cas_number, revision_date, new_filename,
                 description, keywords, content_html, ai_analyzed),
            )
            conn.commit()
            print(f"{prefix} OK  {product_name} / {manufacturer}  [{category}]")
            ok += 1

        except Exception as e:
            print(f"{prefix} FAIL {pdf_path.name} → {e}")
            fail += 1

    conn.close()
    print(f"\n완료: 성공 {ok}개 / 스킵 {skip}개 / 실패 {fail}개")


if __name__ == "__main__":
    main()
