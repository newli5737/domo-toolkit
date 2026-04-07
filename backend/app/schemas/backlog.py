"""Backlog schemas — Request & Response models cho Backlog endpoints."""

from pydantic import BaseModel
from typing import Any


class BacklogPostRequest(BaseModel):
    comment: str = ""


class BacklogPostResponse(BaseModel):
    success: bool
    errors: list[str] = []
    comment: Any = None


class BacklogStatusResponse(BaseModel):
    connected: bool
    user: str = ""
    backlog_base_url: str = ""
    error: str = ""


class BacklogConfigResponse(BaseModel):
    backlog_base_url: str = ""
    issue_id: str = ""
    has_api_key: bool = False
