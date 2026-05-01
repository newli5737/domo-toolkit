

from datetime import datetime
from pydantic import BaseModel, Field




class CardFilterParams(BaseModel):
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=50, ge=1, le=200)
    sort_by: str = Field(default="view_count")
    sort_order: str = Field(default="DESC")
    search: str = ""
    card_type: str = ""
    page_title: str = ""
    owner: str = ""


class DashboardFilterParams(BaseModel):
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=50, ge=1, le=200)
    sort_by: str = Field(default="total_views")
    sort_order: str = Field(default="DESC")
    search: str = ""


class LowUsageFilterParams(BaseModel):
    max_views: int = Field(default=10, description="Ngưỡng view tối đa")
    limit: int = Field(default=100, ge=1, le=500)
    offset: int = Field(default=0, ge=0)
    card_type: str = ""
    owner: str = ""


class CardResponse(BaseModel):
    id: str
    title: str | None = None
    card_type: str | None = None
    view_count: int | None = 0
    owner_name: str | None = None
    page_id: int | None = None
    page_title: str | None = None
    last_viewed_at: datetime | None = None

    model_config = {"from_attributes": True}


class DashboardResponse(BaseModel):
    page_id: int
    page_title: str
    card_count: int
    total_views: int

    model_config = {"from_attributes": True}


class CardTypeDistribution(BaseModel):
    card_type: str
    count: int
    views: int


class TopDashboard(BaseModel):
    page_title: str
    card_count: int
    total_views: int


class CardStatsResponse(BaseModel):
    total_cards: int = 0
    total_dashboards: int = 0
    total_views: int = 0
    total_types: int = 0
    zero_view_cards: int = 0
    type_distribution: list[CardTypeDistribution] = []
    top_dashboards: list[TopDashboard] = []


class OwnerStats(BaseModel):
    owner_name: str | None = None
    card_count: int = 0
    total_views: int = 0


class LowUsageResponse(BaseModel):
    total: int = 0
    max_views_threshold: int = 10
    cards: list[CardResponse] = []
    by_owner: list[OwnerStats] = []
    by_dashboard: list = []
    by_type: list = []
