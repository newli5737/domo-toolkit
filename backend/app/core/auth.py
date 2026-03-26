"""DomoAuth — Xác thực Domo bằng programmatic login hoặc J2 cookie import."""

import base64
import requests
from datetime import datetime, timedelta


class DomoAuth:
    """Quản lý xác thực Domo — hỗ trợ login API và J2 cookie import."""

    LOGIN_PATH = "/api/domoweb/auth/login"

    def __init__(self, instance: str):
        self.instance = instance
        self._base_url = f"https://{instance}"
        self._cookies: dict = {}
        self._headers: dict = {}
        self._csrf_token: str = ""
        self._logged_in_at: datetime | None = None
        self._username: str = ""

    @property
    def cookies(self) -> dict:
        return self._cookies

    @property
    def headers(self) -> dict:
        return self._headers

    @property
    def csrf_token(self) -> str:
        return self._csrf_token

    @property
    def is_valid(self) -> bool:
        if not self._cookies or not self._logged_in_at:
            return False
        # Session hết hạn sau 8 giờ
        return datetime.now() - self._logged_in_at < timedelta(hours=8)

    @property
    def username(self) -> str:
        return self._username

    # ─── Programmatic Login ───────────────────────────────────

    def login(self, username: str, password: str) -> dict:
        """
        Login Domo qua API, giả lập request như trình duyệt.
        POST /api/domoweb/auth/login
        Body: {"username": "...", "password": "<base64>", "nocookie": true, "base64": true}

        Response trả về user info + Set-Cookie headers chứa:
        - csrf-token
        - DA-SID-* (session ID)
        - PLAY_SESSION
        - _dsidv1
        """
        url = f"{self._base_url}{self.LOGIN_PATH}"

        # Encode password thành base64
        password_b64 = base64.b64encode(password.encode("utf-8")).decode("utf-8")

        payload = {
            "username": username,
            "password": password_b64,
            "nocookie": True,
            "base64": True,
        }

        req_headers = {
            "content-type": "application/json;charset=UTF-8",
            "accept": "application/json, text/plain, */*",
            "user-agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/146.0.0.0 Safari/537.36"
            ),
            "origin": self._base_url,
            "sec-fetch-site": "same-origin",
            "sec-fetch-mode": "cors",
            "sec-fetch-dest": "empty",
        }

        try:
            resp = requests.post(
                url, json=payload, headers=req_headers, timeout=30
            )

            if resp.status_code != 200:
                return {
                    "success": False,
                    "message": f"Login thất bại: HTTP {resp.status_code} — {resp.text[:300]}",
                }

            # Lấy cookies từ Set-Cookie headers
            self._cookies = dict(resp.cookies)

            # Tìm csrf-token
            self._csrf_token = self._cookies.get("csrf-token", "")

            if not self._csrf_token:
                return {
                    "success": False,
                    "message": "Login thành công nhưng không tìm thấy csrf-token trong cookies.",
                }

            # Thiết lập headers cho API calls
            self._headers = {
                "x-csrf-token": self._csrf_token,
                "x-xsrf-token": self._csrf_token,
                "x-requested-with": "XMLHttpRequest",
                "content-type": "application/json",
                "accept": "application/json",
            }

            # Parse user info từ response body
            try:
                data = resp.json()
                user = data.get("user", {})
                self._username = user.get("USER_NAME", username)
            except Exception:
                self._username = username

            self._logged_in_at = datetime.now()

            return {
                "success": True,
                "message": f"Login thành công! ({len(self._cookies)} cookies)",
            }

        except requests.exceptions.ConnectionError:
            return {"success": False, "message": f"Không thể kết nối tới {self._base_url}"}
        except requests.exceptions.Timeout:
            return {"success": False, "message": "Request timeout sau 30 giây"}
        except Exception as e:
            return {"success": False, "message": f"Lỗi: {str(e)}"}

    # ─── J2 Team Cookie Import ────────────────────────────────

    def load_from_j2_cookies(self, data: dict) -> dict:
        """Load session từ J2 Team cookie export JSON.
        Format: {"url": "...", "cookies": [{"name": "...", "value": "...", ...}]}
        """
        cookie_list = data.get("cookies", [])
        if not cookie_list:
            return {"success": False, "message": "Không tìm thấy cookies trong file JSON."}

        # Build cookie dict, filter None name
        self._cookies = {
            c["name"]: c["value"]
            for c in cookie_list
            if c.get("name") and c.get("value") is not None
        }

        if not self._cookies:
            return {"success": False, "message": "Không có cookie hợp lệ nào."}

        # Tìm csrf-token từ cookie
        csrf = self._cookies.get("csrf-token", "")
        self._csrf_token = csrf

        self._headers = {
            "x-csrf-token": self._csrf_token,
            "x-xsrf-token": self._csrf_token,
            "x-requested-with": "XMLHttpRequest",
            "content-type": "application/json",
            "accept": "application/json",
        }

        self._logged_in_at = datetime.now()
        self._username = "Cookie Upload"

        return {
            "success": True,
            "message": f"Đã import {len(self._cookies)} cookies thành công!",
        }

    # ─── Load / Export session ────────────────────────────────

    def load_from_dict(self, data: dict):
        """Load session từ dict (đã lưu trong DB)."""
        raw_cookies = data.get("cookies", {})
        self._cookies = {k: v for k, v in raw_cookies.items() if k is not None}
        self._csrf_token = data.get("csrf_token", "")
        self._username = data.get("username", "")
        self._logged_in_at = (
            datetime.fromisoformat(data["logged_in_at"])
            if data.get("logged_in_at")
            else None
        )
        self._headers = {
            "x-csrf-token": self._csrf_token,
            "x-xsrf-token": self._csrf_token,
            "x-requested-with": "XMLHttpRequest",
            "content-type": "application/json",
            "accept": "application/json",
        }

    def to_dict(self) -> dict:
        """Export session để lưu DB."""
        return {
            "cookies": self._cookies,
            "csrf_token": self._csrf_token,
            "username": self._username,
            "logged_in_at": self._logged_in_at.isoformat() if self._logged_in_at else None,
        }
