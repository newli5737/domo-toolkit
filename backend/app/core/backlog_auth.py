"""BacklogAuth — Đăng nhập Backlog bằng HTTP requests thuần (không Playwright).

Flow:
  1. GET https://apps.nulab.com/signin  → lấy JSESSIONID + device_key cookies
  2. Parse _csrf token từ HTML
  3. POST /signin/account/auth-type     → kiểm tra loại tài khoản
  4. POST /signin                       → đăng nhập với email + password

Session cookie cuối cùng được lưu để gọi Backlog API.
"""

import re
import logging
import requests
from datetime import datetime, timedelta

log = logging.getLogger(__name__)

NULAB_BASE = "https://apps.nulab.com"


class BacklogAuth:
    """Quản lý session đăng nhập Backlog qua Nulab SSO."""

    def __init__(self, backlog_base_url: str = "https://mothers-sp.backlog.jp"):
        self.backlog_base_url = backlog_base_url
        self._session = requests.Session()
        self._session.headers.update({
            "user-agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/146.0.0.0 Safari/537.36"
            ),
            "accept-language": "vi-VN,vi;q=0.9,en-US;q=0.6,en;q=0.5",
        })
        self._csrf_token: str = ""
        self._logged_in_at: datetime | None = None
        self._cookies: dict = {}

    # ── Public properties ──────────────────────────────────────

    @property
    def is_valid(self) -> bool:
        if not self._cookies or self._logged_in_at is None:
            return False
        # Session thường có giá trị hơn 1 ngày, re-login mỗi 20 tiếng cho an toàn
        return datetime.now() - self._logged_in_at < timedelta(hours=20)

    @property
    def cookie_header(self) -> str:
        """Cookie string để đặt vào header 'Cookie'."""
        return "; ".join(f"{k}={v}" for k, v in self._cookies.items())

    @property
    def csrf_token(self) -> str:
        return self._csrf_token

    # ── Login flow ────────────────────────────────────────────

    def login(self, email: str, password: str) -> dict:
        """
        Đăng nhập Backlog qua Nulab SSO.
        Trả về {"success": bool, "message": str}
        """
        try:
            # ── Bước 1: GET trang signin → nhận cookies + CSRF ──
            csrf = self._get_csrf_and_cookies()
            if not csrf:
                return {"success": False, "message": "Không lấy được _csrf token từ trang signin"}

            # ── Bước 2: Kiểm tra loại tài khoản ──
            auth_type_ok = self._check_auth_type(email, csrf)
            if not auth_type_ok:
                log.warning("[BACKLOG] auth-type check không thành công, tiếp tục thử login...")

            # ── Bước 3: POST signin ──
            result = self._post_signin(email, password, csrf)
            return result

        except requests.exceptions.ConnectionError:
            return {"success": False, "message": f"Không kết nối được tới {NULAB_BASE}"}
        except requests.exceptions.Timeout:
            return {"success": False, "message": "Request timeout"}
        except Exception as e:
            log.exception("[BACKLOG] Login error")
            return {"success": False, "message": f"Lỗi: {str(e)}"}

    def _get_csrf_and_cookies(self) -> str:
        """GET trang signin, lấy JSESSIONID và _csrf từ HTML."""
        resp = self._session.get(
            f"{NULAB_BASE}/signin",
            headers={
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "sec-fetch-site": "none",
                "sec-fetch-mode": "navigate",
                "sec-fetch-dest": "document",
            },
            timeout=30,
            allow_redirects=True,
        )
        print(f"[BACKLOG DEBUG] GET /signin → status={resp.status_code}, url={resp.url}")
        print(f"[BACKLOG DEBUG] Cookies nhận được: {list(resp.cookies.keys())}")
        log.info(f"[BACKLOG] GET /signin → {resp.status_code}")

        # Parse _csrf từ HTML (hidden input hoặc meta)
        csrf = self._extract_csrf(resp.text)
        if not csrf:
            # Thử từ cookies (một số version đặt vào cookie)
            csrf = resp.cookies.get("CSRF-TOKEN", "") or resp.cookies.get("_csrf", "")

        # Lấy x-csrf-token từ response header nếu có
        if not csrf:
            csrf = resp.headers.get("x-csrf-token", "")

        print(f"[BACKLOG DEBUG] _csrf tìm được: '{csrf[:40] if csrf else '(trống)'}'")
        return csrf

    def _extract_csrf(self, html: str) -> str:
        """Parse _csrf từ HTML — thử nhiều pattern."""
        patterns = [
            r'<input[^>]+name=["\']_csrf["\'][^>]+value=["\']([^"\']+)["\']',
            r'<input[^>]+value=["\']([^"\']+)["\'][^>]+name=["\']_csrf["\']',
            r'"csrfToken"\s*:\s*"([^"]+)"',
            r'csrfToken\s*=\s*["\']([^"\']+)["\']',
        ]
        for pat in patterns:
            m = re.search(pat, html, re.IGNORECASE)
            if m:
                return m.group(1)
        return ""

    def _check_auth_type(self, email: str, csrf: str) -> bool:
        """POST /signin/account/auth-type — xác định loại tài khoản."""
        try:
            resp = self._session.post(
                f"{NULAB_BASE}/signin/account/auth-type",
                json={"email": email},
                headers={
                    "accept": "application/json, text/javascript, */*; q=0.01",
                    "content-type": "application/json; charset=UTF-8",
                    "x-csrf-token": csrf,
                    "x-requested-with": "XMLHttpRequest",
                    "origin": NULAB_BASE,
                    "referer": f"{NULAB_BASE}/signin",
                    "sec-fetch-site": "same-origin",
                    "sec-fetch-mode": "cors",
                    "sec-fetch-dest": "empty",
                },
                timeout=20,
            )
            log.info(f"[BACKLOG] auth-type → {resp.status_code}: {resp.text[:100]}")
            return resp.status_code < 400
        except Exception as e:
            log.warning(f"[BACKLOG] auth-type error: {e}")
            return False

    def _post_signin(self, email: str, password: str, csrf: str) -> dict:
        """POST /signin với form data."""
        form = {
            "_csrf": csrf,
            "contact_me": "",
            "email": email,
            "autoSigninWebAuthn": "on",
            "password": password,
            "autoSignin": "on",
            "mixpanelDistinctId": "",
            "mixpanelDeviceId": "",
        }
        print(f"[BACKLOG DEBUG] POST /signin với email={email}, _csrf='{csrf[:20]}...'")

        resp = self._session.post(
            f"{NULAB_BASE}/signin",
            data=form,
            headers={
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "content-type": "application/x-www-form-urlencoded",
                "origin": NULAB_BASE,
                "referer": f"{NULAB_BASE}/signin",
                "sec-fetch-site": "same-origin",
                "sec-fetch-mode": "navigate",
                "sec-fetch-user": "?1",
                "sec-fetch-dest": "document",
                "cache-control": "max-age=0",
                "upgrade-insecure-requests": "1",
            },
            timeout=30,
            allow_redirects=True,
        )

        print(f"[BACKLOG DEBUG] POST /signin → status={resp.status_code}, final_url={resp.url}")
        print(f"[BACKLOG DEBUG] Session cookies sau login: {list(self._session.cookies.keys())}")
        log.info(f"[BACKLOG] POST /signin → {resp.status_code}, final_url={resp.url}")

        # Kiểm tra kết quả
        final_url = str(resp.url)
        if "signin" in final_url and resp.status_code < 400:
            # Vẫn ở trang signin → login thất bại (sai pass hoặc bị captcha)
            error_msg = self._extract_error(resp.text)
            print(f"[BACKLOG DEBUG] ❌ Vẫn ở trang signin → THẤT BẠI")
            print(f"[BACKLOG DEBUG] Error từ HTML: {error_msg or '(không tìm thấy)'} ")
            print(f"[BACKLOG DEBUG] HTML excerpt (2000 ký tự đầu):\n{resp.text[:2000]}")
            return {
                "success": False,
                "message": f"Login thất bại: {error_msg or 'Sai email/password hoặc bị captcha'}",
            }

        if resp.status_code >= 400:
            print(f"[BACKLOG DEBUG] ❌ HTTP error {resp.status_code}")
            print(f"[BACKLOG DEBUG] Response: {resp.text[:500]}")
            return {
                "success": False,
                "message": f"HTTP {resp.status_code}: {resp.text[:200]}",
            }

        # Cập nhật cookies từ session
        self._cookies = dict(self._session.cookies)
        self._csrf_token = self._cookies.get("CSRF-TOKEN", "") or self._cookies.get("csrf-token", "")
        self._logged_in_at = datetime.now()

        # Thử lấy CSRF từ Backlog trực tiếp nếu chưa có
        if not self._csrf_token:
            self._csrf_token = self._fetch_backlog_csrf()

        print(f"[BACKLOG DEBUG] ✅ Login thành công! Cookies: {list(self._cookies.keys())}")
        log.info(f"[BACKLOG] ✅ Login thành công! Cookies: {list(self._cookies.keys())}")
        return {
            "success": True,
            "message": f"Đăng nhập Backlog thành công! ({len(self._cookies)} cookies)",
        }

    def _extract_error(self, html: str) -> str:
        """Trích lỗi từ HTML trang signin."""
        m = re.search(r'<[^>]+class=["\'][^"\']*error[^"\']*["\'][^>]*>([^<]{5,})<', html, re.IGNORECASE)
        if m:
            return m.group(1).strip()
        return ""

    def _fetch_backlog_csrf(self) -> str:
        """Lấy CSRF token từ trang Backlog sau khi đã login."""
        try:
            resp = self._session.get(
                self.backlog_base_url,
                headers={"accept": "text/html,*/*"},
                timeout=15,
                allow_redirects=True,
            )
            # Tìm meta csrf hoặc cookie
            csrf = resp.cookies.get("CSRF-TOKEN", "")
            if not csrf:
                m = re.search(r'csrfToken["\s:=]+["\']([^"\']{10,})["\']', resp.text)
                if m:
                    csrf = m.group(1)
            # Cập nhật cookies từ Backlog
            self._cookies.update(dict(resp.cookies))
            return csrf
        except Exception as e:
            log.warning(f"[BACKLOG] Không lấy được CSRF từ Backlog: {e}")
            return ""

    # ── Session persistence ───────────────────────────────────

    def load_from_dict(self, data: dict):
        """Khôi phục session từ dict đã lưu."""
        self._cookies = data.get("cookies", {})
        self._csrf_token = data.get("csrf_token", "")
        logged_at = data.get("logged_in_at")
        self._logged_in_at = datetime.fromisoformat(logged_at) if logged_at else None
        # Đồng bộ vào requests.Session
        for k, v in self._cookies.items():
            self._session.cookies.set(k, v)

    def to_dict(self) -> dict:
        """Export session để lưu DB."""
        logged_at = self._logged_in_at.isoformat() if self._logged_in_at is not None else None
        return {
            "cookies": self._cookies,
            "csrf_token": self._csrf_token,
            "logged_in_at": logged_at,
        }
