"""Auth Router — Login Domo và quản lý session."""

import json
from datetime import datetime
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.config import get_settings
from app.core.auth import DomoAuth
from app.core.db import DomoDatabase

router = APIRouter(prefix="/api/auth", tags=["auth"])

# Global auth instance
_auth: DomoAuth | None = None


def get_auth() -> DomoAuth:
    global _auth
    if _auth is None:
        settings = get_settings()
        _auth = DomoAuth(settings.domo_instance)
    return _auth


def get_db() -> DomoDatabase:
    settings = get_settings()
    return DomoDatabase(
        host=settings.db_host, port=settings.db_port,
        dbname=settings.db_name, user=settings.db_user,
        password=settings.db_password,
    )


class LoginResponse(BaseModel):
    success: bool
    message: str
    username: str = ""

import sys
import asyncio

@router.post("/login", response_model=LoginResponse)
def login():
    """Mở browser Playwright cho user tự đăng nhập.
    Chạy trong 1 Thread Pool thay vì event loop gốc của Uvicorn để tránh lỗi NotImplementedError.
    """
    auth = get_auth()
    
    # Force khởi tạo tường minh Proactor Event Loop trên Windows
    if sys.platform == "win32":
        loop = asyncio.ProactorEventLoop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(auth.interactive_login())
        finally:
            loop.close()
    else:
        result = asyncio.run(auth.interactive_login())

    if result["success"]:
        # Lưu session vào DB
        try:
            db = get_db()
            db.execute(
                "UPDATE domo_sessions SET is_active = FALSE WHERE is_active = TRUE"
            )
            db.upsert("domo_sessions", {
                "id": 1,
                "username": auth.username,
                "cookies_json": json.dumps(auth.cookies),
                "csrf_token": auth.csrf_token,
                "logged_in_at": datetime.now().isoformat(),
                "is_active": True,
            }, "id")
            db.close()
        except Exception as e:
            # DB lỗi nhưng login vẫn OK
            pass

    return LoginResponse(
        success=result["success"],
        message=result["message"],
        username=auth.username if result["success"] else "",
    )


@router.post("/upload-cookies", response_model=LoginResponse)
async def upload_cookies(payload: dict):
    """Import cookies từ J2 Team Cookie extension export JSON.
    Format: {"url": "...", "cookies": [{"name": "...", "value": "...", ...}]}
    """
    auth = get_auth()
    result = auth.load_from_j2_cookies(payload)

    if result["success"]:
        try:
            db = get_db()
            db.execute(
                "UPDATE domo_sessions SET is_active = FALSE WHERE is_active = TRUE"
            )
            db.upsert("domo_sessions", {
                "id": 1,
                "username": auth.username,
                "cookies_json": json.dumps(auth.cookies),
                "csrf_token": auth.csrf_token,
                "logged_in_at": datetime.now().isoformat(),
                "is_active": True,
            }, "id")
            db.close()
        except Exception:
            pass

    return LoginResponse(
        success=result["success"],
        message=result["message"],
        username=auth.username if result["success"] else "",
    )


@router.get("/status")
async def auth_status():
    """Kiểm tra session hiện tại."""
    auth = get_auth()
    return {
        "logged_in": auth.is_valid,
        "username": auth.username if auth.is_valid else "",
        "domo_url": f"https://{get_settings().domo_instance}",
    }
