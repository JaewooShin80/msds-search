from fastapi import APIRouter, Depends
from db.database import get_connection

router = APIRouter()

HAZARD_LEVELS = ["위험", "경고", "주의"]


@router.get("/stats")
def get_stats(conn=Depends(get_connection)):
    total    = conn.execute("SELECT COUNT(*) FROM msds").fetchone()[0]
    cat_count = conn.execute("SELECT COUNT(*) FROM categories").fetchone()[0]
    conn.close()
    return {"total": total, "categoryCount": cat_count}


@router.get("/categories")
def get_categories(conn=Depends(get_connection)):
    # DB에 있는 건수 맵
    rows = conn.execute(
        "SELECT category AS name, COUNT(*) AS count FROM msds GROUP BY category"
    ).fetchall()
    count_map = {r["name"]: r["count"] for r in rows}

    # 전체 카테고리 순서 유지
    all_cats = conn.execute(
        "SELECT name FROM categories ORDER BY id"
    ).fetchall()
    conn.close()

    return [{"name": c["name"], "count": count_map.get(c["name"], 0)} for c in all_cats]


@router.get("/hazard-levels")
def get_hazard_levels(conn=Depends(get_connection)):
    rows = conn.execute(
        "SELECT hazard_level AS name, COUNT(*) AS count FROM msds GROUP BY hazard_level"
    ).fetchall()
    count_map = {r["name"]: r["count"] for r in rows}
    conn.close()
    return [{"name": h, "count": count_map.get(h, 0)} for h in HAZARD_LEVELS]


@router.get("/manufacturers")
def get_manufacturers(conn=Depends(get_connection)):
    rows = conn.execute(
        "SELECT manufacturer AS name, COUNT(*) AS count FROM msds GROUP BY manufacturer ORDER BY manufacturer"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@router.get("/dashboard")
def get_dashboard(conn=Depends(get_connection)):
    total = conn.execute("SELECT COUNT(*) FROM msds").fetchone()[0]

    by_hazard = conn.execute(
        "SELECT hazard_level AS name, COUNT(*) AS count FROM msds GROUP BY hazard_level"
    ).fetchall()
    hazard_map = {r["name"]: r["count"] for r in by_hazard}

    by_category = conn.execute(
        """
        SELECT c.name, COALESCE(m.count, 0) AS count
        FROM categories c
        LEFT JOIN (
            SELECT category, COUNT(*) AS count FROM msds GROUP BY category
        ) m ON c.name = m.category
        ORDER BY count DESC
        """
    ).fetchall()

    by_manufacturer = conn.execute(
        "SELECT manufacturer AS name, COUNT(*) AS count FROM msds GROUP BY manufacturer ORDER BY count DESC LIMIT 10"
    ).fetchall()

    recent = conn.execute(
        "SELECT id, product_name, category, hazard_level, created_at FROM msds ORDER BY created_at DESC LIMIT 5"
    ).fetchall()

    monthly = conn.execute(
        """
        SELECT strftime('%Y-%m', created_at) AS month, COUNT(*) AS count
        FROM msds
        GROUP BY month
        ORDER BY month DESC
        LIMIT 12
        """
    ).fetchall()

    conn.close()
    return {
        "total": total,
        "by_hazard": {
            "위험": hazard_map.get("위험", 0),
            "경고": hazard_map.get("경고", 0),
            "주의": hazard_map.get("주의", 0),
        },
        "by_category": [dict(r) for r in by_category],
        "by_manufacturer": [dict(r) for r in by_manufacturer],
        "recent": [dict(r) for r in recent],
        "monthly_trend": [dict(r) for r in reversed(monthly)],
    }


@router.get("/ai-status")
def get_ai_status():
    """프론트엔드가 AI 분석 가능 여부를 확인하는 엔드포인트"""
    import os
    has_key = bool(os.environ.get("ANTHROPIC_API_KEY", "").strip())
    return {"ai_available": has_key}
