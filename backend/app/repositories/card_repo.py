"""CardRepository — Toàn bộ business logic truy vấn Card từ DB."""

from sqlalchemy.orm import Session
from sqlalchemy import func, nullslast

from app.models.card import Card
from app.schemas.card import (
    CardFilterParams, DashboardFilterParams, LowUsageFilterParams,
    CardResponse, DashboardResponse, CardStatsResponse,
    CardTypeDistribution, TopDashboard, OwnerStats, LowUsageResponse,
)
from app.schemas.common import PaginatedResponse


class CardRepository:
    """Repository chuyên xử lý query Card. Router không cần biết SQL."""

    def __init__(self, db: Session):
        self.db = db

    # ─── Helpers ──────────────────────────────────────────────────

    @staticmethod
    def _paginate(total: int, page: int, page_size: int) -> int:
        return (total + page_size - 1) // page_size if total > 0 else 0

    @staticmethod
    def _resolve_sort(model, sort_by: str, allowed: set[str], default: str = "view_count"):
        if sort_by not in allowed:
            sort_by = default
        return getattr(model, sort_by), sort_by

    @staticmethod
    def _apply_sort(query, sort_col, sort_order: str):
        if sort_order.upper() == "ASC":
            return query.order_by(nullslast(sort_col.asc()))
        return query.order_by(nullslast(sort_col.desc()))

    # ─── Card List ────────────────────────────────────────────────

    def get_paginated_cards(self, params: CardFilterParams) -> PaginatedResponse[CardResponse]:
        query = self.db.query(Card)

        if params.search.strip():
            query = query.filter(Card.title.ilike(f"%{params.search.strip()}%"))
        if params.card_type.strip():
            query = query.filter(func.lower(Card.card_type) == params.card_type.strip().lower())
        if params.page_title.strip():
            query = query.filter(Card.page_title.ilike(f"%{params.page_title.strip()}%"))
        if params.owner.strip():
            query = query.filter(Card.owner_name.ilike(f"%{params.owner.strip()}%"))

        total = query.count()

        sort_col, _ = self._resolve_sort(
            Card, params.sort_by,
            {"view_count", "title", "card_type", "page_title", "owner_name", "last_viewed_at"}
        )
        query = self._apply_sort(query, sort_col, params.sort_order)

        offset = (params.page - 1) * params.page_size
        rows = query.offset(offset).limit(params.page_size).all()

        return PaginatedResponse[CardResponse](
            data=[CardResponse.model_validate(r) for r in rows],
            total=total,
            page=params.page,
            page_size=params.page_size,
            total_pages=self._paginate(total, params.page, params.page_size),
        )

    # ─── Card Types ───────────────────────────────────────────────

    def get_card_types(self) -> list[str]:
        rows = self.db.query(Card.card_type).filter(
            Card.card_type.isnot(None), Card.card_type != ""
        ).distinct().order_by(Card.card_type).all()
        return [r[0] for r in rows]

    # ─── Dashboards ──────────────────────────────────────────────

    def get_paginated_dashboards(self, params: DashboardFilterParams) -> PaginatedResponse[DashboardResponse]:
        base_filter = [
            Card.page_id.isnot(None),
            Card.page_title.isnot(None),
            Card.page_title != "",
        ]
        if params.search.strip():
            base_filter.append(Card.page_title.ilike(f"%{params.search.strip()}%"))

        # Count distinct pages
        total = self.db.query(func.count(Card.page_id.distinct())).filter(*base_filter).scalar() or 0

        # Build grouped query
        query = self.db.query(
            Card.page_id,
            Card.page_title,
            func.count(Card.id).label("card_count"),
            func.coalesce(func.sum(Card.view_count), 0).label("total_views"),
        ).filter(*base_filter).group_by(Card.page_id, Card.page_title)

        # Sort
        allowed = {"total_views", "card_count", "page_title"}
        sort_by = params.sort_by if params.sort_by in allowed else "total_views"
        sort_col = (
            func.count(Card.id) if sort_by == "card_count"
            else Card.page_title if sort_by == "page_title"
            else func.coalesce(func.sum(Card.view_count), 0)
        )
        query = self._apply_sort(query, sort_col, params.sort_order)

        offset = (params.page - 1) * params.page_size
        rows = query.offset(offset).limit(params.page_size).all()

        return PaginatedResponse[DashboardResponse](
            data=[DashboardResponse(
                page_id=r.page_id, page_title=r.page_title,
                card_count=r.card_count, total_views=int(r.total_views)
            ) for r in rows],
            total=total,
            page=params.page,
            page_size=params.page_size,
            total_pages=self._paginate(total, params.page, params.page_size),
        )

    # ─── Stats ────────────────────────────────────────────────────

    def get_stats(self) -> CardStatsResponse:
        stats = self.db.query(
            func.count(Card.id).label("total_cards"),
            func.count(Card.page_id.distinct()).label("total_dashboards"),
            func.coalesce(func.sum(Card.view_count), 0).label("total_views"),
            func.count(Card.card_type.distinct()).label("total_types"),
        ).first()

        zero_view_cards = self.db.query(func.count(Card.id)).filter(
            (Card.view_count == 0) | (Card.view_count.is_(None))
        ).scalar() or 0

        type_dist = self.db.query(
            Card.card_type,
            func.count(Card.id).label("count"),
            func.coalesce(func.sum(Card.view_count), 0).label("views"),
        ).filter(
            Card.card_type.isnot(None), Card.card_type != ""
        ).group_by(Card.card_type).order_by(func.count(Card.id).desc()).limit(10).all()

        top_dashboards = self.db.query(
            Card.page_title,
            func.count(Card.id).label("card_count"),
            func.coalesce(func.sum(Card.view_count), 0).label("total_views"),
        ).filter(
            Card.page_title.isnot(None), Card.page_title != ""
        ).group_by(Card.page_title).order_by(
            func.coalesce(func.sum(Card.view_count), 0).desc()
        ).limit(10).all()

        return CardStatsResponse(
            total_cards=stats.total_cards if stats else 0,
            total_dashboards=stats.total_dashboards if stats else 0,
            total_views=int(stats.total_views) if stats else 0,
            total_types=stats.total_types if stats else 0,
            zero_view_cards=zero_view_cards,
            type_distribution=[
                CardTypeDistribution(card_type=r.card_type, count=r.count, views=int(r.views))
                for r in type_dist
            ],
            top_dashboards=[
                TopDashboard(page_title=r.page_title, card_count=r.card_count, total_views=int(r.total_views))
                for r in top_dashboards
            ],
        )

    # ─── Low Usage ────────────────────────────────────────────────

    def get_low_usage(self, params: LowUsageFilterParams) -> LowUsageResponse:
        base_filter = [(Card.view_count <= params.max_views) | (Card.view_count.is_(None))]

        if params.card_type.strip():
            base_filter.append(func.lower(Card.card_type) == params.card_type.strip().lower())
        if params.owner.strip():
            base_filter.append(Card.owner_name.ilike(f"%{params.owner.strip()}%"))

        total = self.db.query(func.count(Card.id)).filter(*base_filter).scalar() or 0

        cards = self.db.query(Card).filter(*base_filter).order_by(
            Card.view_count.asc().nullsfirst(), Card.title.asc()
        ).offset(params.offset).limit(params.limit).all()

        by_owner = self.db.query(
            Card.owner_name,
            func.count(Card.id).label("card_count"),
            func.coalesce(func.sum(Card.view_count), 0).label("total_views"),
        ).filter(*base_filter).group_by(Card.owner_name).order_by(
            func.count(Card.id).desc()
        ).limit(20).all()

        return LowUsageResponse(
            total=total,
            max_views_threshold=params.max_views,
            cards=[CardResponse.model_validate(r) for r in cards],
            by_owner=[
                OwnerStats(owner_name=r.owner_name, card_count=r.card_count, total_views=int(r.total_views))
                for r in by_owner
            ],
        )
