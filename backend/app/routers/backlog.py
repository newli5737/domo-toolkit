"""Backlog Router — API endpoints cho tích hợp Backlog.

Session được quản lý bởi BacklogAuth (HTTP login simulation).
Không còn đọc cookie từ file JSON.
"""

import json
import logging
import requests
from datetime import datetime
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.config import get_settings
from app.core.backlog_auth import BacklogAuth
from app.core.db import DomoDatabase

router = APIRouter(prefix="/api/backlog", tags=["backlog"])
log = logging.getLogger(__name__)

# ── Global BacklogAuth instance ────────────────────────────────

_backlog_auth: BacklogAuth | None = None


def get_backlog_auth() -> BacklogAuth:
    global _backlog_auth
    if _backlog_auth is None:
        settings = get_settings()
        _backlog_auth = BacklogAuth(
            backlog_base_url=settings.backlog_base_url,
            device_key=settings.backlog_device_key,
        )
    return _backlog_auth


def get_db() -> DomoDatabase:
    settings = get_settings()
    return DomoDatabase(
        host=settings.db_host, port=settings.db_port,
        dbname=settings.db_name, user=settings.db_user,
        password=settings.db_password,
    )


def _save_backlog_session(bauth: BacklogAuth):
    """Lưu Backlog session vào DB."""
    try:
        db = get_db()
        db.upsert("backlog_sessions", {
            "id": 1,
            "cookies_json": json.dumps(bauth.to_dict().get("cookies", {})),
            "csrf_token": bauth.csrf_token,
            "logged_in_at": datetime.now().isoformat(),
            "is_active": True,
        }, "id")
        db.close()
    except Exception as e:
        log.warning(f"[BACKLOG] Không lưu được session vào DB: {e}")


def _load_backlog_session() -> bool:
    """Tải Backlog session từ DB vào instance."""
    try:
        db = get_db()
        rows = db.query("SELECT * FROM backlog_sessions WHERE is_active = TRUE LIMIT 1")
        db.close()
        if not rows:
            return False
        row = rows[0]
        bauth = get_backlog_auth()
        logged_at = row.get("logged_in_at")
        bauth.load_from_dict({
            "cookies": json.loads(row.get("cookies_json") or "{}"),
            "csrf_token": row.get("csrf_token", ""),
            "logged_in_at": logged_at.isoformat() if hasattr(logged_at, "isoformat") else logged_at,
        })
        return bauth.is_valid
    except Exception as e:
        log.warning(f"[BACKLOG] Không load được session từ DB: {e}")
        return False


def _ensure_backlog_session() -> BacklogAuth:
    """Đảm bảo có session hợp lệ. Tự động login nếu cần."""
    bauth = get_backlog_auth()

    if bauth.is_valid:
        return bauth

    # Thử load từ DB
    if _load_backlog_session():
        return bauth

    # Login mới
    settings = get_settings()
    email = settings.backlog_email
    password = settings.backlog_password

    if not email or not password:
        raise HTTPException(
            status_code=503,
            detail="Chưa cấu hình BACKLOG_EMAIL / BACKLOG_PASSWORD trong .env. "
                   "Hoặc chưa đăng nhập qua /api/backlog/login.",
        )

    result = bauth.login(email, password)
    if not result["success"]:
        raise HTTPException(status_code=503, detail=f"Backlog login thất bại: {result['message']}")

    _save_backlog_session(bauth)
    return bauth


# ── Models ─────────────────────────────────────────────────────

class BacklogLoginRequest(BaseModel):
    email: str = ""
    password: str = ""


class BacklogPostRequest(BaseModel):
    """Request body — chỉ cần comment."""
    comment: str = ""


# ── Endpoints ──────────────────────────────────────────────────

@router.post("/login")
def backlog_login(req: BacklogLoginRequest):
    """Đăng nhập Backlog thủ công (dùng credentials từ request hoặc .env)."""
    settings = get_settings()
    email = req.email or settings.backlog_email
    password = req.password or settings.backlog_password

    if not email or not password:
        raise HTTPException(status_code=400, detail="Thiếu email hoặc password")

    bauth = get_backlog_auth()
    result = bauth.login(email, password)

    if result["success"]:
        _save_backlog_session(bauth)

    return {
        "success": result["success"],
        "message": result["message"],
        "csrf_token": bauth.csrf_token if result["success"] else "",
    }


@router.post("/post-status")
def post_backlog_status(req: BacklogPostRequest):
    """Post status update lên Backlog issue."""
    settings = get_settings()

    if not settings.backlog_issue_id:
        raise HTTPException(status_code=400, detail="Chưa cấu hình BACKLOG_ISSUE_ID trong .env")

    bauth = _ensure_backlog_session()

    url = f"{settings.backlog_base_url}/SwitchStatusAjax.action"
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ".0"

    headers = {
        "accept": "*/*",
        "content-type": "application/x-www-form-urlencoded",
        "cache-control": "no-cache",
        "pragma": "no-cache",
        "x-csrf-token": bauth.csrf_token,
        "x-requested-with": "XMLHttpRequest",
        "cookie": bauth.cookie_header,
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


@router.get("/status")
def backlog_auth_status():
    """Kiểm tra trạng thái session Backlog hiện tại."""
    bauth = get_backlog_auth()
    return {
        "logged_in": bauth.is_valid,
        "backlog_base_url": get_settings().backlog_base_url,
        "has_csrf": bool(bauth.csrf_token),
    }


@router.get("/config")
def get_backlog_config():
    """Lấy cấu hình Backlog hiện tại."""
    settings = get_settings()
    bauth = get_backlog_auth()
    return {
        "backlog_base_url": settings.backlog_base_url,
        "issue_id": settings.backlog_issue_id,
        "logged_in": bauth.is_valid,
        "has_credentials": bool(settings.backlog_email and settings.backlog_password),
    }
