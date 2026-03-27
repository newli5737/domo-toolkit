"""Backlog Router — Dùng Backlog REST API v2 với API key.

Không cần login, không cần cookie, không bao giờ hết hạn.
Ref: https://developer.nulab.com/docs/backlog/
"""

import logging
import requests
import datetime
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.config import get_settings

router = APIRouter(prefix="/api/backlog", tags=["backlog"])
log = logging.getLogger(__name__)


def _api_url(settings, path: str) -> str:
    """Tạo URL REST API kèm apiKey."""
    return f"{settings.backlog_base_url}/api/v2{path}?apiKey={settings.backlog_api_key}"


def _check_api_key(settings) -> None:
    if not settings.backlog_api_key:
        raise HTTPException(
            status_code=503,
            detail="Chưa cấu hình BACKLOG_API_KEY trong .env",
        )


# ── Models ─────────────────────────────────────────────────────

class BacklogPostRequest(BaseModel):
    """Request body — comment kèm theo khi đổi status."""
    comment: str = ""


# ── Endpoints ──────────────────────────────────────────────────

@router.post("/post-status")
def post_backlog_status(req: BacklogPostRequest):
    """Đổi status Backlog issue lên In Progress (statusId=2) + thêm comment."""
    settings = get_settings()
    _check_api_key(settings)

    if not settings.backlog_issue_id:
        raise HTTPException(status_code=400, detail="Chưa cấu hình BACKLOG_ISSUE_ID trong .env")

    issue_id = settings.backlog_issue_id
    errors = []

    # ── Bước 1: Update status issue → In Progress (statusId=2) ──
    patch_url = _api_url(settings, f"/issues/{issue_id}")
    try:
        resp = requests.patch(
            patch_url,
            json={"statusId": 2},
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        print(f"[BACKLOG] PATCH /issues/{issue_id} → {resp.status_code}")
        if resp.status_code >= 400:
            errors.append(f"Update status thất bại: HTTP {resp.status_code} — {resp.text[:200]}")
    except Exception as e:
        errors.append(f"Update status error: {str(e)}")

    # ── Bước 2: Thêm comment nếu có ──
    comment_result = None
    if req.comment:
        comment_url = _api_url(settings, f"/issues/{issue_id}/comments")
        try:
            resp2 = requests.post(
                comment_url,
                json={"content": req.comment},
                headers={"Content-Type": "application/json"},
                timeout=30,
            )
            print(f"[BACKLOG] POST /issues/{issue_id}/comments → {resp2.status_code}")
            if resp2.status_code < 400:
                comment_result = resp2.json()
            else:
                errors.append(f"Thêm comment thất bại: HTTP {resp2.status_code} — {resp2.text[:200]}")
        except Exception as e:
            errors.append(f"Comment error: {str(e)}")

    return {
        "success": len(errors) == 0,
        "errors": errors,
        "comment": comment_result,
    }


@router.get("/status")
def backlog_api_status():
    """Kiểm tra kết nối Backlog API."""
    settings = get_settings()
    _check_api_key(settings)

    try:
        # Thử lấy thông tin user hiện tại để xác nhận API key hợp lệ
        resp = requests.get(
            _api_url(settings, "/users/myself"),
            timeout=15,
        )
        if resp.status_code == 200:
            user = resp.json()
            return {
                "connected": True,
                "user": user.get("name", ""),
                "backlog_base_url": settings.backlog_base_url,
            }
        else:
            return {
                "connected": False,
                "error": f"HTTP {resp.status_code}: {resp.text[:200]}",
            }
    except Exception as e:
        return {"connected": False, "error": str(e)}


@router.get("/config")
def get_backlog_config():
    """Lấy cấu hình Backlog hiện tại."""
    settings = get_settings()
    return {
        "backlog_base_url": settings.backlog_base_url,
        "issue_id": settings.backlog_issue_id,
        "has_api_key": bool(settings.backlog_api_key),
    }
