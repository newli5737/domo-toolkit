"""Card Router — Thin controller, chỉ nhận params và gọi Repository."""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.repositories.card_repo import CardRepository
from app.schemas.common import PaginatedResponse
from app.schemas.card import (
    CardFilterParams, DashboardFilterParams, LowUsageFilterParams,
    CardResponse, DashboardResponse, CardStatsResponse, LowUsageResponse,
)

router = APIRouter(prefix="/api/cards", tags=["cards"])


@router.get("/list", response_model=PaginatedResponse[CardResponse])
def list_cards(
    params: CardFilterParams = Depends(),
    db: Session = Depends(get_db),
):
    """Lấy danh sách cards với phân trang, filter, sort."""
    return CardRepository(db).get_paginated_cards(params)


@router.get("/types", response_model=list[str])
def get_card_types(db: Session = Depends(get_db)):
    """Lấy danh sách card types cho filter dropdown."""
    return CardRepository(db).get_card_types()


@router.get("/dashboards", response_model=PaginatedResponse[DashboardResponse])
def get_dashboards(
    params: DashboardFilterParams = Depends(),
    db: Session = Depends(get_db),
):
    """Lấy tất cả dashboards với phân trang và sort."""
    return CardRepository(db).get_paginated_dashboards(params)


@router.get("/stats", response_model=CardStatsResponse)
def get_card_stats(db: Session = Depends(get_db)):
    """Thống kê tổng quan."""
    return CardRepository(db).get_stats()


@router.get("/low-usage", response_model=LowUsageResponse)
def get_low_usage_cards(
    params: LowUsageFilterParams = Depends(),
    db: Session = Depends(get_db),
):
    """Phân tích cards ít sử dụng."""
    return CardRepository(db).get_low_usage(params)
