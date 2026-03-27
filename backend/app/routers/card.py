"""Card Router — API endpoints cho card & dashboard view statistics."""

from fastapi import APIRouter, Query, HTTPException
from app.config import get_settings
from app.core.api import DomoAPI
from app.core.db import DomoDatabase
from app.services.card import CardService

router = APIRouter(prefix="/api/cards", tags=["cards"])


def _get_db() -> DomoDatabase:
    settings = get_settings()
    return DomoDatabase(
        host=settings.db_host, port=settings.db_port,
        dbname=settings.db_name, user=settings.db_user,
        password=settings.db_password,
    )


@router.get("/list")
def list_cards(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    sort_by: str = Query(default="view_count", description="Sort field"),
    sort_order: str = Query(default="DESC", description="ASC or DESC"),
    search: str = Query(default="", description="Search by title"),
    card_type: str = Query(default="", description="Filter by card type"),
    page_title: str = Query(default="", description="Filter by dashboard name"),
    owner: str = Query(default="", description="Filter by owner"),
):
    """Lấy danh sách cards từ DB với phân trang, filter, sort."""
    db = _get_db()
    try:
        # Build WHERE clause
        conditions = []
        params = []

        if search.strip():
            conditions.append("LOWER(title) LIKE %s")
            params.append(f"%{search.strip().lower()}%")

        if card_type.strip():
            conditions.append("LOWER(card_type) = %s")
            params.append(card_type.strip().lower())

        if page_title.strip():
            conditions.append("LOWER(page_title) LIKE %s")
            params.append(f"%{page_title.strip().lower()}%")

        if owner.strip():
            conditions.append("LOWER(owner_name) LIKE %s")
            params.append(f"%{owner.strip().lower()}%")

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        # Validate sort
        allowed_sorts = {"view_count", "title", "card_type", "page_title", "owner_name", "last_viewed_at"}
        if sort_by not in allowed_sorts:
            sort_by = "view_count"
        if sort_order.upper() not in ("ASC", "DESC"):
            sort_order = "DESC"

        # Count total
        count_sql = f"SELECT COUNT(*) as cnt FROM cards WHERE {where_clause}"
        count_result = db.query(count_sql, tuple(params))
        total = count_result[0]["cnt"] if count_result else 0

        # Fetch page
        offset = (page - 1) * page_size
        data_sql = f"""
            SELECT id, title, card_type, view_count, owner_name,
                   page_id, page_title, last_viewed_at
            FROM cards
            WHERE {where_clause}
            ORDER BY {sort_by} {sort_order} NULLS LAST
            LIMIT %s OFFSET %s
        """
        rows = db.query(data_sql, tuple(params) + (page_size, offset))

        return {
            "data": [dict(r) for r in (rows or [])],
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size if total > 0 else 0,
        }
    finally:
        db.close()


@router.get("/types")
def get_card_types():
    """Lấy danh sách card types cho filter dropdown."""
    db = _get_db()
    try:
        rows = db.query(
            "SELECT DISTINCT card_type FROM cards WHERE card_type IS NOT NULL AND card_type != '' ORDER BY card_type"
        )
        return [r["card_type"] for r in (rows or [])]
    finally:
        db.close()


@router.get("/dashboards")
def get_dashboards(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    sort_by: str = Query(default="total_views", description="Sort: total_views, card_count, page_title"),
    sort_order: str = Query(default="DESC"),
    search: str = Query(default=""),
):
    """Lấy tất cả dashboards với phân trang và sort."""
    db = _get_db()
    try:
        where = "page_id IS NOT NULL AND page_title IS NOT NULL AND page_title != ''"
        params: list = []
        if search.strip():
            where += " AND LOWER(page_title) LIKE %s"
            params.append(f"%{search.strip().lower()}%")

        allowed = {"total_views", "card_count", "page_title"}
        if sort_by not in allowed:
            sort_by = "total_views"
        if sort_order.upper() not in ("ASC", "DESC"):
            sort_order = "DESC"

        count_sql = f"""
            SELECT COUNT(DISTINCT page_id) as cnt FROM cards WHERE {where}
        """
        count_result = db.query(count_sql, tuple(params))
        total = count_result[0]["cnt"] if count_result else 0

        offset = (page - 1) * page_size
        data_sql = f"""
            SELECT page_id, page_title, COUNT(*) as card_count,
                   COALESCE(SUM(view_count), 0) as total_views
            FROM cards
            WHERE {where}
            GROUP BY page_id, page_title
            ORDER BY {sort_by} {sort_order} NULLS LAST
            LIMIT %s OFFSET %s
        """
        rows = db.query(data_sql, tuple(params) + (page_size, offset))

        return {
            "data": [dict(r) for r in (rows or [])],
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size if total > 0 else 0,
        }
    finally:
        db.close()


@router.get("/stats")
def get_card_stats():
    """Thống kê tổng quan."""
    db = _get_db()
    try:
        stats = db.query("""
            SELECT
                COUNT(*) as total_cards,
                COUNT(DISTINCT page_id) as total_dashboards,
                COALESCE(SUM(view_count), 0) as total_views,
                COUNT(DISTINCT card_type) as total_types,
                COUNT(CASE WHEN view_count = 0 OR view_count IS NULL THEN 1 END) as zero_view_cards
            FROM cards
        """)
        row = dict(stats[0]) if stats else {}

        # Top card types
        type_dist = db.query("""
            SELECT card_type, COUNT(*) as count, COALESCE(SUM(view_count), 0) as views
            FROM cards
            WHERE card_type IS NOT NULL AND card_type != ''
            GROUP BY card_type
            ORDER BY count DESC
            LIMIT 10
        """)

        # Top dashboards
        top_dashboards = db.query("""
            SELECT page_title, COUNT(*) as card_count, COALESCE(SUM(view_count), 0) as total_views
            FROM cards
            WHERE page_title IS NOT NULL AND page_title != ''
            GROUP BY page_title
            ORDER BY total_views DESC
            LIMIT 10
        """)

        return {
            **row,
            "type_distribution": [dict(r) for r in (type_dist or [])],
            "top_dashboards": [dict(r) for r in (top_dashboards or [])],
        }
    finally:
        db.close()


@router.get("/low-usage")
def get_low_usage_cards(
    max_views: int = Query(default=10, description="Ngưỡng view tối đa để coi là ít dùng"),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0),
    card_type: str = Query(default=""),
    owner: str = Query(default=""),
):
    """Phân tích cards ít sử dụng (view_count <= max_views).

    Trả về:
    - Danh sách cards ít dùng
    - Phân tích theo owner
    - Phân tích theo dashboard
    - Thống kê tổng
    """
    db = _get_db()
    try:
        conditions = ["(view_count <= %s OR view_count IS NULL)"]
        params: list = [max_views]

        if card_type.strip():
            conditions.append("LOWER(card_type) = %s")
            params.append(card_type.strip().lower())

        if owner.strip():
            conditions.append("LOWER(owner_name) LIKE %s")
            params.append(f"%{owner.strip().lower()}%")

        where = " AND ".join(conditions)

        # Total count
        total_row = db.query_one(f"SELECT COUNT(*) as cnt FROM cards WHERE {where}", params)
        total = total_row["cnt"] if total_row else 0

        # Cards list
        cards = db.query(
            f"""SELECT id, title, card_type, view_count, owner_name, page_id, page_title, last_viewed_at
                FROM cards WHERE {where}
                ORDER BY view_count ASC NULLS FIRST, title ASC
                LIMIT %s OFFSET %s""",
            params + [limit, offset],
        )

        # By owner — ai có nhiều card ít dùng nhất
        by_owner = db.query(
            f"""SELECT owner_name, COUNT(*) as card_count,
                       COALESCE(SUM(view_count), 0) as total_views,
                       COUNT(CASE WHEN view_count = 0 OR view_count IS NULL THEN 1 END) as zero_view_count
                FROM cards WHERE {where}
                GROUP BY owner_name
                ORDER BY card_count DESC
                LIMIT 20""",
            params,
        )

        # By dashboard — dashboard nào chứa nhiều card ít dùng nhất
        by_dashboard = db.query(
            f"""SELECT page_title, page_id, COUNT(*) as card_count,
                       COALESCE(SUM(view_count), 0) as total_views,
                       COUNT(CASE WHEN view_count = 0 OR view_count IS NULL THEN 1 END) as zero_view_count
                FROM cards WHERE {where} AND page_title IS NOT NULL
                GROUP BY page_title, page_id
                ORDER BY card_count DESC
                LIMIT 20""",
            params,
        )

        # By card type
        by_type = db.query(
            f"""SELECT card_type, COUNT(*) as card_count,
                       COALESCE(SUM(view_count), 0) as total_views
                FROM cards WHERE {where} AND card_type IS NOT NULL
                GROUP BY card_type
                ORDER BY card_count DESC""",
            params,
        )

        return {
            "total": total,
            "max_views_threshold": max_views,
            "cards": [dict(r) for r in (cards or [])],
            "by_owner": [dict(r) for r in (by_owner or [])],
            "by_dashboard": [dict(r) for r in (by_dashboard or [])],
            "by_type": [dict(r) for r in (by_type or [])],
        }
    finally:
        db.close()

