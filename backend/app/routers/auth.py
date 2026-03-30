"""Auth Router — Login Domo bằng username/password hoặc J2 cookie upload."""

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


def _save_session(auth: DomoAuth):
    """Lưu session vào DB sau khi login thành công."""
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


# ─── Models ───────────────────────────────────────────────

class LoginRequest(BaseModel):
    username: str = ""
    password: str = ""

class LoginResponse(BaseModel):
    success: bool
    message: str
    username: str = ""


# ─── Endpoints ────────────────────────────────────────────

@router.post("/login", response_model=LoginResponse)
def login(req: LoginRequest):
    """Login Domo bằng username/password.
    Nếu không truyền credentials, dùng giá trị từ .env.
    """
    auth = get_auth()
    settings = get_settings()

    username = req.username or settings.domo_username
    password = req.password or settings.domo_password

    if not username or not password:
        return LoginResponse(
            success=False,
            message="Thiếu username/password. Truyền trong request hoặc cấu hình trong .env",
        )

    result = auth.login(username, password)

    if result["success"]:
        _save_session(auth)

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
        _save_session(auth)

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


@router.post("/logout")
async def logout():
    """Đăng xuất — xóa session cookie."""
    global _auth
    auth = get_auth()

    # Clear auth object
    auth._cookies = {}
    auth._headers = {}
    auth._csrf_token = ""
    auth._logged_in_at = None
    auth._username = ""

    # Deactivate DB sessions
    try:
        db = get_db()
        db.execute("UPDATE domo_sessions SET is_active = FALSE WHERE is_active = TRUE")
        db.close()
    except Exception:
        pass

    return {"success": True, "message": "Đã đăng xuất thành công"}
