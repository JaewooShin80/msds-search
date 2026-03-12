from fastapi import APIRouter, Depends
from db.database import get_connection

router = APIRouter()

HAZARD_LEVELS = ["위험", "경고", "해당없음"]


@router.get("/stats")
def get_stats(conn=Depends(get_connection)):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM msds")
    total = cur.fetchone()["count"]
    cur.execute("SELECT COUNT(*) FROM categories")
    cat_count = cur.fetchone()["count"]
    conn.close()
    return {"total": total, "categoryCount": cat_count}


@router.get("/categories")
def get_categories(conn=Depends(get_connection)):
    cur = conn.cursor()
    cur.execute("SELECT category AS name, COUNT(*) AS count FROM msds GROUP BY category")
    count_map = {r["name"]: r["count"] for r in cur.fetchall()}

    cur.execute("SELECT name FROM categories ORDER BY id")
    all_cats = cur.fetchall()
    conn.close()

    return [{"name": c["name"], "count": count_map.get(c["name"], 0)} for c in all_cats]


@router.get("/hazard-levels")
def get_hazard_levels(conn=Depends(get_connection)):
    cur = conn.cursor()
    cur.execute(
        "SELECT hazard_level AS name, COUNT(*) AS count FROM msds GROUP BY hazard_level"
    )
    count_map = {r["name"]: r["count"] for r in cur.fetchall()}
    conn.close()
    return [{"name": h, "count": count_map.get(h, 0)} for h in HAZARD_LEVELS]


@router.get("/manufacturers")
def get_manufacturers(conn=Depends(get_connection)):
    cur = conn.cursor()
    cur.execute(
        "SELECT manufacturer AS name, COUNT(*) AS count FROM msds GROUP BY manufacturer ORDER BY manufacturer"
    )
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


@router.get("/dashboard")
def get_dashboard(conn=Depends(get_connection)):
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM msds")
    total = cur.fetchone()["count"]

    cur.execute(
        "SELECT hazard_level AS name, COUNT(*) AS count FROM msds GROUP BY hazard_level"
    )
    hazard_map = {r["name"]: r["count"] for r in cur.fetchall()}

    cur.execute(
        """
        SELECT c.name, COALESCE(m.count, 0) AS count
        FROM categories c
        LEFT JOIN (
            SELECT category, COUNT(*) AS count FROM msds GROUP BY category
        ) m ON c.name = m.category
        ORDER BY count DESC
        """
    )
    by_category = cur.fetchall()

    cur.execute(
        "SELECT manufacturer AS name, COUNT(*) AS count FROM msds GROUP BY manufacturer ORDER BY count DESC LIMIT 10"
    )
    by_manufacturer = cur.fetchall()

    cur.execute(
        "SELECT id, product_name, category, hazard_level, created_at FROM msds ORDER BY created_at DESC LIMIT 5"
    )
    recent = cur.fetchall()

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

    conn.close()
    return {
        "total": total,
        "by_hazard": {
            "위험": hazard_map.get("위험", 0),
            "경고": hazard_map.get("경고", 0),
            "해당없음": hazard_map.get("해당없음", 0),
        },
        "by_category": [dict(r) for r in by_category],
        "by_manufacturer": [dict(r) for r in by_manufacturer],
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
    conn.close()
    return {"ai_available": has_key, "pending_count": pending}
