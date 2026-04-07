"""AuthRepository — Business logic cho Authentication."""

import json
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.core.auth import DomoAuth
from app.config import get_settings
from app.schemas.auth import LoginResponse, AuthStatusResponse, LogoutResponse
from app.models.auth import DomoSession

# Singleton auth instance
_auth: DomoAuth | None = None

def get_auth(db: Session = None) -> DomoAuth:
    global _auth
    if _auth is None:
        settings = get_settings()
        _auth = DomoAuth(settings.domo_instance)
        # Khôi phục session từ DB nếu có
        if db:
            session_data = db.query(DomoSession).filter(DomoSession.is_active == True).first()
            if session_data and session_data.cookies_json:
                try:
                    _auth._cookies = json.loads(session_data.cookies_json)
                    _auth._csrf_token = session_data.csrf_token
                    _auth._username = session_data.username
                    _auth._logged_in_at = session_data.logged_in_at
                except Exception:
                    pass
    return _auth


class AuthRepository:
    """Xử lý logic đăng nhập, session, và trạng thái auth."""

    def __init__(self, db: Session):
        self.db = db
        self.auth = get_auth(db)
        self.settings = get_settings()

    def _save_session(self):
        """Lưu session vào DB sau khi login thành công."""
        try:
            self.db.execute(text("UPDATE domo_sessions SET is_active = FALSE WHERE is_active = TRUE"))
            
            session_record = self.db.query(DomoSession).filter(DomoSession.id == 1).first()
            if not session_record:
                session_record = DomoSession(id=1)
                self.db.add(session_record)
                
            session_record.username = self.auth.username
            session_record.cookies_json = json.dumps(self.auth.cookies)
            session_record.csrf_token = self.auth.csrf_token
            session_record.logged_in_at = datetime.now()
            session_record.is_active = True
            
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            print(f"Lỗi lưu session: {e}")

    def login(self, username: str, password: str) -> LoginResponse:
        username = username or self.settings.domo_username
        password = password or self.settings.domo_password

        if not username or not password:
            return LoginResponse(
                success=False,
                message="Thiếu username/password. Truyền trong request hoặc cấu hình trong .env",
            )

        result = self.auth.login(username, password)
        if result["success"]:
            self._save_session()

        return LoginResponse(
            success=result["success"],
            message=result["message"],
            username=self.auth.username if result["success"] else "",
        )

    def upload_cookies(self, payload: dict) -> LoginResponse:
        result = self.auth.load_from_j2_cookies(payload)
        if result["success"]:
            self._save_session()

        return LoginResponse(
            success=result["success"],
            message=result["message"],
            username=self.auth.username if result["success"] else "",
        )

    def get_status(self) -> AuthStatusResponse:
        return AuthStatusResponse(
            logged_in=self.auth.is_valid,
            username=self.auth.username if self.auth.is_valid else "",
            domo_url=f"https://{self.settings.domo_instance}",
        )

    def logout(self) -> LogoutResponse:
        self.auth._cookies = {}
        self.auth._headers = {}
        self.auth._csrf_token = ""
        self.auth._logged_in_at = None
        self.auth._username = ""

        try:
            self.db.execute(text("UPDATE domo_sessions SET is_active = FALSE WHERE is_active = TRUE"))
            self.db.commit()
        except Exception:
            self.db.rollback()

        return LogoutResponse(success=True, message="Đã đăng xuất thành công")
