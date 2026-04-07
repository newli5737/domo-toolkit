"""BeastMode schemas — Request & Response models cho BeastMode endpoints."""

from pydantic import BaseModel, Field
from typing import Any


# ─── Request ──────────────────────────────────────────────

class ReanalyzeRequest(BaseModel):
    low_view_threshold: int = Field(default=10, ge=1, le=10000)


# ─── Response ─────────────────────────────────────────────

class CrawlStartResponse(BaseModel):
    job_id: int
    message: str


class CrawlStatusResponse(BaseModel):
    job_id: int = 0
    status: str = "none"
    total: int = 0
    processed: int = 0
    found: int = 0
    errors: int = 0
    message: str = ""
    started_at: str | None = None
    finished_at: str | None = None
    current_step: int = 0
    total_steps: int = 5
    step_name: str = ""
    step_processed: int = 0
    step_total: int = 0
    step_percent: int = 0
    overall_percent: int = 0
    elapsed_seconds: int = 0


class GroupDataResponse(BaseModel):
    group: int
    total: int = 0
    data: list[dict] = []


class SearchResponse(BaseModel):
    data: list[dict] = []
    total: int = 0


class DeleteResponse(BaseModel):
    success: bool
    error: str = ""
    bm_id: int = 0
    affected_cards: int = 0


class CancelResponse(BaseModel):
    message: str
