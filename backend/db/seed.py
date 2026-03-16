"""
기존 msds-data.js의 10개 데이터를 DB에 마이그레이션
실행: python db/seed.py
"""
import json
import logging
import sys
from pathlib import Path

# 프로젝트 루트를 경로에 추가
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils import configure_encoding

configure_encoding()

from constants import CATEGORIES
from db.database import get_db_connection

logger = logging.getLogger(__name__)

MSDS_DATA = [
    {
        "product_name": "CSF-71T 용접재료 와이어",
        "manufacturer": "조선선재",
        "category": "용접재료",
        "hazard_level": "위험",
        "revision_date": "2023-01-15",
        "pdf_url": "https://www.genspark.ai/api/files/s/y5boIoQM",
        "description": "플럭스 코어드 와이어",
        "keywords": json.dumps(["용접", "와이어", "플럭스"]),
    },
    {
        "product_name": "CR-13 용접봉",
        "manufacturer": "조선선재",
        "category": "용접재료",
        "hazard_level": "위험",
        "revision_date": "2023-02-10",
        "pdf_url": "https://www.genspark.ai/api/files/s/WzO9F6O3",
        "description": "피복아크 용접봉",
        "keywords": json.dumps(["용접", "용접봉", "아크"]),
    },
    {
        "product_name": "CAT HYDO Advanced 10",
        "manufacturer": "모빌코리아",
        "category": "윤활유/그리스",
        "hazard_level": "경고",
        "revision_date": "2023-03-20",
        "pdf_url": "https://www.genspark.ai/api/files/s/ifxjUq4k",
        "description": "유압유",
        "keywords": json.dumps(["유압유", "윤활유", "CAT"]),
    },
    {
        "product_name": "KOMATSU Lithium EP Grease G2-LI",
        "manufacturer": "Komatsu Ltd.",
        "category": "윤활유/그리스",
        "hazard_level": "경고",
        "revision_date": "2023-04-15",
        "pdf_url": "https://www.genspark.ai/api/files/s/lHCX7xje",
        "description": "리튬 EP 그리스",
        "keywords": json.dumps(["그리스", "리튬", "EP", "KOMATSU"]),
    },
    {
        "product_name": "GHP EP 2",
        "manufacturer": "한일루켐(주)",
        "category": "윤활유/그리스",
        "hazard_level": "경고",
        "revision_date": "2022-09-05",
        "pdf_url": "https://www.genspark.ai/api/files/s/m7Y5YfJ0",
        "description": "윤활 그리스",
        "keywords": json.dumps(["그리스", "윤활", "EP"]),
    },
    {
        "product_name": "XTeer Grease 2",
        "manufacturer": "현대오일뱅크",
        "category": "윤활유/그리스",
        "hazard_level": "경고",
        "revision_date": "2023-05-10",
        "pdf_url": "https://www.genspark.ai/api/files/s/gkTX3cA3",
        "description": "윤활 그리스",
        "keywords": json.dumps(["그리스", "윤활", "XTeer", "현대"]),
    },
    {
        "product_name": "폴리카르복실산계 고성능AE감수제",
        "manufacturer": "영남씨앤씨",
        "category": "콘크리트혼화제",
        "hazard_level": "경고",
        "revision_date": "2023-06-01",
        "pdf_url": "https://www.genspark.ai/api/files/s/XMMFZdZ0",
        "description": "폴리카르복실산계 고성능AE감수제 (표준형, 지연형)",
        "keywords": json.dumps(["감수제", "콘크리트", "혼화제", "AE"]),
    },
    {
        "product_name": "고로슬래그시멘트 2종",
        "manufacturer": "대한시멘트",
        "category": "시멘트류",
        "hazard_level": "위험",
        "revision_date": "2023-07-12",
        "pdf_url": "https://www.genspark.ai/api/files/s/W3WDCC5U",
        "description": "고로슬래그시멘트 2종",
        "keywords": json.dumps(["시멘트", "고로슬래그", "대한시멘트"]),
    },
    {
        "product_name": "플라이애시 시멘트",
        "manufacturer": "대한시멘트",
        "category": "시멘트류",
        "hazard_level": "위험",
        "revision_date": "2023-08-20",
        "pdf_url": "https://www.genspark.ai/api/files/s/9Wx1Rlch",
        "description": "플라이애시 시멘트",
        "keywords": json.dumps(["시멘트", "플라이애시", "대한시멘트"]),
    },
    {
        "product_name": "레디믹스트 콘크리트",
        "manufacturer": "고성레미콘(주) 동해지점",
        "category": "시멘트류",
        "hazard_level": "위험",
        "revision_date": "2023-09-15",
        "pdf_url": "https://www.genspark.ai/api/files/s/BZCnFqO5",
        "description": "레디믹스트 콘크리트",
        "keywords": json.dumps(["콘크리트", "레미콘", "레디믹스"]),
    },
]


def run():
    conn = get_db_connection()
    cur = conn.cursor()

    # 카테고리 삽입
    for name in CATEGORIES:
        cur.execute(
            "INSERT INTO categories (name) VALUES (%s) ON CONFLICT DO NOTHING",
            (name,),
        )

    # MSDS 중복 방지
    cur.execute("SELECT COUNT(*) FROM msds")
    count = cur.fetchone()["count"]
    if count > 0:
        logger.info("이미 %d개의 데이터가 존재합니다. seed를 건너뜁니다.", count)
    else:
        for item in MSDS_DATA:
            cur.execute(
                """
                INSERT INTO msds
                    (product_name, manufacturer, category, hazard_level,
                     revision_date, pdf_url, description, keywords)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    item["product_name"], item["manufacturer"], item["category"],
                    item["hazard_level"], item["revision_date"],
                    item["pdf_url"], item["description"], item["keywords"],
                ),
            )
        logger.info("%d개의 MSDS 데이터 삽입 완료", len(MSDS_DATA))

    conn.commit()
    cur.close()
    conn.close()
    logger.info("%d개의 카테고리 삽입 완료", len(CATEGORIES))
    logger.info("Seed 완료")


if __name__ == "__main__":
    run()
