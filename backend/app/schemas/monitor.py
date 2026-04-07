"""Monitor schemas — Request & Response models cho Monitor endpoints."""

from datetime import datetime
from pydantic import BaseModel, Field
from typing import Any


# ─── Request Params ───────────────────────────────────────

class HealthCheckParams(BaseModel):
    stale_hours: int = Field(default=24)
    min_card_count: int = Field(default=0)
    provider_type: str = ""
    min_dataflow_count: int = Field(default=0)
    max_workers: int = Field(default=10)


class CrawlParams(BaseModel):
    max_workers: int = Field(default=10)


class AutoCheckRequest(BaseModel):
    min_card_count: int = 40
    provider_type: str = "mysql-ssh"
    comment_ok: str = ""
    alert_email: str = ""
    schedule_enabled: bool = False
    schedule_hour: int = 8
    schedule_minute: int = 0
    schedule_days: str = "mon,tue,wed,thu,fri"


class DatasetFilterParams(BaseModel):
    provider_type: str = ""
    min_card_count: int = Field(default=0, ge=0)
    limit: int = Field(default=5000, ge=1)
    offset: int = Field(default=0, ge=0)


class DataflowFilterParams(BaseModel):
    status_filter: str = ""
    limit: int = Field(default=5000, ge=1)
    offset: int = Field(default=0, ge=0)


class DatasetCsvParams(BaseModel):
    provider_type: str = ""
    min_card_count: int = Field(default=0)
    search: str = ""


class DataflowCsvParams(BaseModel):
    status_filter: str = ""
    search: str = ""


# ─── Response Models ──────────────────────────────────────

class JobStatusResponse(BaseModel):
    status: str
    message: str = ""
    started_at: str | None = None
    progress: dict | None = None


class DatasetResponse(BaseModel):
    id: str
    name: str | None = None
    row_count: int | None = None
    column_count: int | None = 0
    card_count: int | None = 0
    data_flow_count: int | None = 0
    provider_type: str | None = None
    stream_id: str | None = None
    schedule_state: str | None = None
    dataset_status: str | None = None
    last_execution_state: str | None = None
    last_updated: datetime | None = None
    updated_at: datetime | None = None

    model_config = {"from_attributes": True}


class DataflowResponse(BaseModel):
    id: str
    name: str | None = None
    status: str | None = None
    paused: bool | None = False
    database_type: str | None = None
    last_execution_time: datetime | None = None
    last_execution_state: str | None = None
    execution_count: int | None = 0
    owner: str | None = None
    output_dataset_count: int | None = 0
    updated_at: datetime | None = None

    model_config = {"from_attributes": True}


class DatasetListResponse(BaseModel):
    total: int = 0
    datasets: list[dict] = []


class DataflowListResponse(BaseModel):
    total: int = 0
    dataflows: list[dict] = []


class ProviderTypesResponse(BaseModel):
    provider_types: list[str] = []


class AutoCheckResult(BaseModel):
    checked_at: str | None = None
    all_ok: bool = True
    failed_dataset_count: int = 0
    failed_dataflow_count: int = 0
    backlog_posted: bool = False
    email_sent: bool = False


class AlertDataResponse(BaseModel):
    checked_at: str | None = None
    all_ok: bool = True
    failed_datasets: list[dict] = []
    failed_dataflows: list[dict] = []


class AutoCheckConfigResponse(BaseModel):
    backlog_base_url: str = ""
    backlog_issue_id: str = ""
    has_backlog_cookie: bool = False
    alert_email_to: str = ""
    min_card_count: int = 40
    provider_type: str = ""
    has_gmail: bool = False
    schedule_enabled: bool = False
    schedule_hour: int = 8
    schedule_minute: int = 0
    schedule_days: str = "mon,tue,wed,thu,fri"


class SaveConfigResponse(BaseModel):
    saved: bool = True
    config: dict = {}
