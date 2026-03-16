import logging

from fastapi import APIRouter, Depends
from db.database import get_connection
from constants import HAZARD_LEVELS

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/stats")
def get_stats(conn=Depends(get_connection)):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM msds")
    total = cur.fetchone()["count"]
    cur.execute("SELECT COUNT(*) FROM categories")
    cat_count = cur.fetchone()["count"]
    return {"total": total, "categoryCount": cat_count}


@router.get("/categories")
def get_categories(conn=Depends(get_connection)):
    cur = conn.cursor()
    cur.execute("SELECT category AS name, COUNT(*) AS count FROM msds GROUP BY category")
    count_map = {r["name"]: r["count"] for r in cur.fetchall()}

    cur.execute("SELECT name FROM categories ORDER BY id")
    all_cats = cur.fetchall()

    return [{"name": c["name"], "count": count_map.get(c["name"], 0)} for c in all_cats]


@router.get("/hazard-levels")
def get_hazard_levels(conn=Depends(get_connection)):
    cur = conn.cursor()
    cur.execute(
        "SELECT hazard_level AS name, COUNT(*) AS count FROM msds GROUP BY hazard_level"
    )
    count_map = {r["name"]: r["count"] for r in cur.fetchall()}
    return [{"name": h, "count": count_map.get(h, 0)} for h in HAZARD_LEVELS]


@router.get("/manufacturers")
def get_manufacturers(conn=Depends(get_connection)):
    cur = conn.cursor()
    cur.execute(
        "SELECT manufacturer AS name, COUNT(*) AS count FROM msds GROUP BY manufacturer ORDER BY manufacturer"
    )
    rows = cur.fetchall()
    return [dict(r) for r in rows]


@router.get("/dashboard")
def get_dashboard(conn=Depends(get_connection)):
    cur = conn.cursor()

    # 쿼리 1: 전체 수 + 위험등급별 통계 (1 round-trip으로 통합)
    cur.execute("""
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE hazard_level = '위험') AS danger_count,
            COUNT(*) FILTER (WHERE hazard_level = '경고') AS warning_count,
            COUNT(*) FILTER (WHERE hazard_level = '해당없음') AS caution_count
        FROM msds
    """)
    s = cur.fetchone()
    total = s["total"]
    by_hazard = {
        "위험": s["danger_count"],
        "경고": s["warning_count"],
        "해당없음": s["caution_count"],
    }

    # 쿼리 2: 카테고리별 통계 + 제조사별 통계 (UNION ALL로 통합)
    cur.execute("""
        (
            SELECT 'cat' AS qtype, c.name AS name, COALESCE(m.count, 0) AS count
            FROM categories c
            LEFT JOIN (
                SELECT category, COUNT(*) AS count FROM msds GROUP BY category
            ) m ON c.name = m.category
        )
        UNION ALL
        (
            SELECT 'mfr', manufacturer, COUNT(*)
            FROM msds GROUP BY manufacturer ORDER BY count DESC LIMIT 10
        )
    """)
    union_rows = cur.fetchall()
    by_category = [{"name": r["name"], "count": r["count"]} for r in union_rows if r["qtype"] == "cat"]
    by_manufacturer = [{"name": r["name"], "count": r["count"]} for r in union_rows if r["qtype"] == "mfr"]

    # 쿼리 3: 최근 등록
    cur.execute(
        "SELECT id, product_name, category, hazard_level, created_at FROM msds ORDER BY created_at DESC LIMIT 5"
    )
    recent = cur.fetchall()

    # 쿼리 4: 월별 추이
    cur.execute(
        """
        SELECT TO_CHAR(created_at, 'YYYY-MM') AS month, COUNT(*) AS count
        FROM msds
        GROUP BY month
        ORDER BY month DESC
        LIMIT 12
        """
    )
    monthly = cur.fetchall()

    return {
        "total": total,
        "by_hazard": by_hazard,
        "by_category": by_category,
        "by_manufacturer": by_manufacturer,
        "recent": [dict(r) for r in recent],
        "monthly_trend": [dict(r) for r in reversed(monthly)],
    }


@router.get("/ai-status")
def get_ai_status(conn=Depends(get_connection)):
    """AI 분석 가능 여부 + 미분석 건수 반환"""
    import os
    has_key = bool(os.environ.get("ANTHROPIC_API_KEY", "").strip())
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM msds WHERE ai_analyzed = 0 AND (pdf_path IS NOT NULL OR pdf_url IS NOT NULL)"
    )
    pending = cur.fetchone()["count"]
    return {"ai_available": has_key, "pending_count": pending}
