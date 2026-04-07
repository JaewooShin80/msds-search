"""
DATA/ 폴더 PDF를 Supabase Storage에 업로드하고 DB에 등록
- analysis_results/ 의 content_html 활용 (이미 HTML 변환 완료)
- analysis_queue/ 에서 product_name, manufacturer, category 매칭

사용법: python upload_to_supabase.py
"""
import sys
import os
import json
from pathlib import Path
from datetime import date

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

BASE_DIR    = Path(__file__).parent
BACKEND     = BASE_DIR / "backend"
DATA_DIR    = BASE_DIR / "DATA"
QUEUE_DIR   = BASE_DIR / "analysis_queue"
RESULT_DIR  = BASE_DIR / "analysis_results"

sys.path.insert(0, str(BACKEND))

from dotenv import load_dotenv
load_dotenv(BACKEND / ".env")

from db.database import get_db_connection
from services.storage import upload_pdf

FOLDER_TO_CATEGORY = {
    "01_그리스-윤활유":                "윤활유/그리스",
    "02_시멘트-몰탈-혼화제":           "시멘트류",
    "03_접착제-실란트":                "몰탈/접착제",
    "04_가스(산소-질소-아르곤-LPG)":   "가스류",
    "05_스프레류(락카 등)":             "스프레이류",
    "06_도료-페인트":                  "기타",
    "07_절단-연마":                    "절단/연마",
    "08_연료(휘발유,경유)":             "연료(유류)",
    "09_발파-폭약":                    "발파/폭약류",
    "10_부동액":                       "부동액",
    "11_요소수":                       "요소수",
    "12_박리제":                       "박리제",
    "13_용접 재료":                    "용접재료",
    "14_품질시험":                     "품질시험",
    "15_기타":                         "기타",
}


def parse_filename(stem: str):
    """'제품명_제조사명' → (product_name, manufacturer)"""
    if "_" in stem:
        idx = stem.rfind("_")
        return stem[:idx].strip(), stem[idx + 1:].strip()
    return stem.strip(), "미상"


def build_html_lookup() -> dict:
    """analysis_queue + analysis_results 매칭 → {product_name: content_html}"""
    # queue: id → product_name
    id_to_name = {}
    for qf in QUEUE_DIR.glob("*.json"):
        try:
            d = json.loads(qf.read_text(encoding="utf-8"))
            id_to_name[int(qf.stem)] = d.get("product_name", "")
        except Exception:
            pass

    # results: id → content_html
    lookup = {}
    for rf in RESULT_DIR.glob("*.json"):
        try:
            d = json.loads(rf.read_text(encoding="utf-8"))
            pname = d.get("product_name") or id_to_name.get(int(rf.stem), "")
            html  = d.get("content_html", "")
            if pname and html:
                lookup[pname.strip()] = html
        except Exception:
            pass

    return lookup


def already_exists(cur, product_name: str, manufacturer: str) -> bool:
    cur.execute(
        "SELECT id FROM msds WHERE product_name=%s AND manufacturer=%s",
        (product_name, manufacturer),
    )
    return cur.fetchone() is not None


def main():
    html_lookup = build_html_lookup()
    print(f"HTML 캐시: {len(html_lookup)}개 로드\n")

    conn = get_db_connection()
    cur  = conn.cursor()

    pdf_files = sorted(DATA_DIR.rglob("*.pdf"))
    total = len(pdf_files)
    print(f"총 {total}개 PDF 처리 시작\n")

    ok = skip = fail = 0

    for i, pdf_path in enumerate(pdf_files, 1):
        folder       = pdf_path.parent.name
        category     = FOLDER_TO_CATEGORY.get(folder, "기타")
        product_name, manufacturer = parse_filename(pdf_path.stem)
        prefix = f"[{i:3d}/{total}]"

        if already_exists(cur, product_name, manufacturer):
            print(f"{prefix} SKIP (중복) {product_name}")
            skip += 1
            continue

        try:
            pdf_bytes    = pdf_path.read_bytes()
            storage_path = upload_pdf(pdf_bytes, pdf_path.name)
            content_html = html_lookup.get(product_name, "")
            ai_analyzed  = 1 if content_html else 0

            cur.execute(
                """
                INSERT INTO msds
                    (product_name, manufacturer, category, hazard_level,
                     revision_date, pdf_path, pdf_url,
                     description, keywords, content_html, ai_analyzed)
                VALUES (%s, %s, %s, %s, %s, %s, NULL, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    product_name, manufacturer, category, "경고",
                    str(date.today()), storage_path,
                    "", "[]", content_html, ai_analyzed,
                ),
            )
            new_id = cur.fetchone()["id"]
            conn.commit()
            html_mark = f"HTML {len(content_html):,}자" if content_html else "HTML 없음"
            print(f"{prefix} OK  [{new_id}] {product_name} / {manufacturer}  ({html_mark})")
            ok += 1

        except Exception as e:
            conn.rollback()
            print(f"{prefix} FAIL {pdf_path.name} → {e}")
            fail += 1

    cur.close()
    conn.close()
    print(f"\n완료: 성공 {ok}개 / 스킵 {skip}개 / 실패 {fail}개")


if __name__ == "__main__":
    main()
