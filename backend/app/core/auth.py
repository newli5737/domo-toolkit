"""DomoAuth — Tự động login Domo bằng Playwright, lấy cookie."""

import asyncio
from datetime import datetime, timedelta
from playwright.async_api import async_playwright

class DomoAuth:
    """Quản lý xác thực Domo qua Playwright headless login."""

    def __init__(self, instance: str):
        self.instance = instance
        self.login_url = f"https://{instance}/auth/index"
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
        # Cookie hết hạn sau 4 giờ
        return datetime.now() - self._logged_in_at < timedelta(hours=4)

    @property
    def username(self) -> str:
        return self._username

    async def interactive_login(self, timeout_ms: int = 300000) -> dict:
        """
        Mở Playwright với headless=False để user tự đăng nhập (chọn SSO, nhập pass, pass MFA).
        Chờ đến khi cookie `domoAuth` xuất hiện, báo hiệu login thành công.
        """
        try:
            print(f"Mở browser để login vào {self.instance}...")
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=False)
                context = await browser.new_context()
                page = await context.new_page()

                await page.goto(self.login_url)

                success = False
                wait_time = 0
                while wait_time < timeout_ms / 1000:
                    try:
                        if not browser.is_connected():
                            return {"success": False, "message": "Đã đóng cửa sổ trình duyệt trước khi login thành công."}

                        current_url = page.url
                        print(f"[{wait_time}s] Đang chờ login... (URL: {current_url})")
                        
                        cookies = await context.cookies()
                        domo_auth = next((c for c in cookies if c["name"] == "domoAuth"), None)
                        
                        # Điều kiện thành công: 
                        # 1. Có cookie domoAuth
                        # 2. Hoặc url đã vào dashboard (ko chứa /auth, /login) sau ít nhất 5s (tránh đứt gãy lúc redirect ban đầu)
                        is_inside_app = "/auth/" not in current_url and "/login" not in current_url and wait_time > 5
                        
                        token = None
                        try:
                            # Cố tìm token trong object JS
                            token = await page.evaluate("window.DomoCSRF || window.DomoContext?.developerToken")
                        except:
                            pass
                            
                        if domo_auth or is_inside_app or token:
                            print(f"🚀 Phát hiện login thành công! (URL: {current_url}, Token có: {bool(token)}, Cookie có: {bool(domo_auth)})")
                            await asyncio.sleep(2) # Chờ 2s cho trang load nốt session JS
                            
                            cookies = await context.cookies() # Lấy lại vòng cuối
                            self._cookies = {c["name"]: c["value"] for c in cookies}
                            
                            if token:
                                self._csrf_token = token
                            else:
                                try:
                                    self._csrf_token = await page.evaluate("window.DomoCSRF || window.DomoContext?.developerToken")
                                except:
                                    pass
                                
                            try:
                                user_info = await page.evaluate("window.DomoContext?.user || window.DomoContext?.currentUser || {}")
                                self._username = user_info.get("email") or user_info.get("displayName") or "Domo User"
                            except:
                                self._username = "Domo User"
                                
                            success = True
                            break
                    except Exception:
                        pass
                        
                    await asyncio.sleep(2)
                    wait_time += 2

                await browser.close()

                if success:
                    self._logged_in_at = datetime.now()
                    self._headers = {
                        "x-csrf-token": self._csrf_token,
                        "x-requested-with": "XMLHttpRequest",
                        "content-type": "application/json",
                        "accept": "application/json",
                    }
                    return {"success": True, "message": "Login thành công!"}
                else:
                    return {"success": False, "message": "Quá thời gian (5 phút) không thấy session hợp lệ."}

        except Exception as e:
            return {"success": False, "message": f"Lỗi: {str(e)}"}

    def load_from_dict(self, data: dict):
        """Load session từ dict (đã lưu trong DB)."""
        self._cookies = data.get("cookies", {})
        self._csrf_token = data.get("csrf_token", "")
        self._username = data.get("username", "")
        self._logged_in_at = datetime.fromisoformat(data["logged_in_at"]) if data.get("logged_in_at") else None
        self._headers = {
            "x-csrf-token": self._csrf_token,
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
