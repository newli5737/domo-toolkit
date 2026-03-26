"""Backlog Router — API endpoints cho tích hợp Backlog."""

import json
import glob
import os
import requests
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional

from app.config import get_settings

router = APIRouter(prefix="/api/backlog", tags=["backlog"])

# Cookie file directory (same location as the backend)
COOKIE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _load_backlog_cookies() -> tuple[str, str]:
    """Load cookies from exported browser JSON file.
    Returns (cookie_header_string, csrf_token).
    """
    # Find the latest backlog cookie JSON file
    pattern = os.path.join(COOKIE_DIR, "mothers-sp.backlog.jp*.json")
    files = glob.glob(pattern)
    if not files:
        raise HTTPException(status_code=400, detail=f"Không tìm thấy file cookie Backlog tại {COOKIE_DIR}")

    # Use the latest file
    latest_file = max(files, key=os.path.getmtime)
    print(f"[BACKLOG] Loading cookies from: {latest_file}")

    with open(latest_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    cookies = data.get("cookies", [])
    if not cookies:
        raise HTTPException(status_code=400, detail="File cookie rỗng")

    # Build cookie header string
    cookie_parts = []
    csrf_token = ""
    for c in cookies:
        name = c.get("name", "")
        value = c.get("value", "")
        if name and value:
            cookie_parts.append(f"{name}={value}")

    cookie_str = "; ".join(cookie_parts)
    return cookie_str, csrf_token


class BacklogPostRequest(BaseModel):
    """Request body — chỉ cần comment."""
    comment: str = ""


@router.post("/post-status")
def post_backlog_status(req: BacklogPostRequest):
    """Post status update lên Backlog issue."""
    settings = get_settings()
    cookie_str, _ = _load_backlog_cookies()

    if not settings.backlog_issue_id:
        raise HTTPException(status_code=400, detail="Chưa cấu hình BACKLOG_ISSUE_ID trong .env")

    url = f"{settings.backlog_base_url}/SwitchStatusAjax.action"
    import datetime
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ".0"

    headers = {
        "accept": "*/*",
        "content-type": "application/x-www-form-urlencoded",
        "cache-control": "no-cache",
        "pragma": "no-cache",
        "x-csrf-token": settings.backlog_csrf_token,
        "x-requested-with": "XMLHttpRequest",
        "cookie": cookie_str,
    }

    form_data = {
        "switchStatusIssue.id": settings.backlog_issue_id,
        "switchStatusIssue.updated": now,
        "switchStatusIssue.statusId": "2",
        "switchStatusIssue.startDate": "",
        "switchStatusIssue.limitDate": "",
        "switchStatusIssue.actualHours": "0.0",
        "switchStatusIssue.assignerId": "",
        "oldSwitchStatusIssue.statusId": "2",
        "oldSwitchStatusIssue.startDate": "",
        "oldSwitchStatusIssue.limitDate": "",
        "oldSwitchStatusIssue.actualHours": "0.0",
        "oldSwitchStatusIssue.assignerId": "",
        "comment.issueId": settings.backlog_issue_id,
        "comment.content": req.comment,
    }

    try:
        resp = requests.post(url, headers=headers, data=form_data, timeout=30)
        print(f"[BACKLOG] POST {url} -> {resp.status_code}")
        return {
            "success": resp.status_code < 400,
            "status_code": resp.status_code,
            "response_text": resp.text[:500],
        }
    except Exception as e:
        print(f"[BACKLOG] Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/config")
def get_backlog_config():
    """Lấy cấu hình Backlog hiện tại."""
    settings = get_settings()
    try:
        cookie_str, _ = _load_backlog_cookies()
        has_cookie = bool(cookie_str)
    except Exception:
        has_cookie = False

    return {
        "backlog_base_url": settings.backlog_base_url,
        "issue_id": settings.backlog_issue_id,
        "has_cookie": has_cookie,
    }
