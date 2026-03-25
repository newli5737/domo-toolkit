"""DomoAPI — HTTP client sync/async với retry và rate limit."""

import time
import asyncio
import requests
import aiohttp
from aiohttp import ClientSession, TCPConnector


class DomoAPI:
    """HTTP client cho Domo API — hỗ trợ sync, async, retry, pagination."""

    def __init__(self, auth, max_retries: int = 10, retry_delay: float = 5.0):
        self.auth = auth
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._base_url = f"https://{auth.instance}"

    @property
    def base_url(self) -> str:
        return self._base_url

    # ─── Sync requests ───────────────────────────────────────

    def _request(self, method: str, url: str, json=None, params=None, timeout=60, extra_headers=None) -> requests.Response | None:
        """Request sync với auto-retry khi bị 429."""
        safe_cookies = {
            str(k): str(v) for k, v in self.auth.cookies.items()
            if k is not None and v is not None
        } if self.auth.cookies else {}
        safe_headers = {
            str(k): str(v) for k, v in self.auth.headers.items()
            if k is not None and v is not None
        } if self.auth.headers else {}
        if extra_headers:
            safe_headers.update(extra_headers)

        for attempt in range(self.max_retries):
            try:
                resp = requests.request(
                    method, url,
                    headers=safe_headers,
                    cookies=safe_cookies,
                    json=json,
                    params=params,
                    timeout=timeout,
                )
                if resp.status_code == 429:
                    wait = self.retry_delay * (attempt + 1)
                    time.sleep(wait)
                    continue
                if resp.status_code >= 400:
                    print(f"❌ [{method}] {resp.status_code} - {url}")
                    print(f"   Response: {resp.text[:500]}")
                return resp
            except Exception as e:
                print(f"❌ [{method}] Exception: {e} - {url}")
                wait = 3 * (attempt + 1)
                time.sleep(wait)
        return None

    def get(self, path: str, params=None, timeout=60) -> requests.Response | None:
        url = f"{self._base_url}{path}" if path.startswith("/") else path
        return self._request("GET", url, params=params, timeout=timeout)

    def post(self, path: str, json=None, params=None, timeout=60) -> requests.Response | None:
        url = f"{self._base_url}{path}" if path.startswith("/") else path
        return self._request("POST", url, json=json, params=params, timeout=timeout)

    def put(self, path: str, json=None, timeout=60, extra_headers=None) -> requests.Response | None:
        url = f"{self._base_url}{path}" if path.startswith("/") else path
        return self._request("PUT", url, json=json, timeout=timeout, extra_headers=extra_headers)

    # ─── Pagination helper ────────────────────────────────────

    def paginate(self, path: str, payload: dict, results_key: str,
                 batch_size: int = 100, offset_key: str = "offset",
                 count_key: str = "count") -> list:
        """
        Tự động phân trang cho Domo search APIs.
        Trả về list tất cả results gộp lại.
        """
        all_results = []
        offset = 0

        while True:
            page_payload = {**payload, offset_key: offset, count_key: batch_size}
            resp = self.post(path, json=page_payload)

            if not resp or resp.status_code != 200:
                break

            data = resp.json()
            results = data.get(results_key, [])

            if not results:
                break

            all_results.extend(results)

            if len(results) < batch_size:
                break

            offset += batch_size

        return all_results

    # ─── Async requests ───────────────────────────────────────

    _debug_printed = False

    async def async_get(self, session: ClientSession, url: str, timeout: int = 20) -> dict | None:
        """GET async với retry."""
        import json as builtin_json
        # Force tất cả key/value thành string, loại bỏ None
        safe_cookies = {
            str(k): str(v) for k, v in self.auth.cookies.items()
            if k is not None and v is not None
        } if self.auth.cookies else {}
        safe_headers = {
            str(k): str(v) for k, v in self.auth.headers.items()
            if k is not None and v is not None
        } if self.auth.headers else {}

        # Debug: in 1 lần duy nhất
        if not DomoAPI._debug_printed:
            DomoAPI._debug_printed = True
            print(f"🔍 [DEBUG] cookies keys: {list(self.auth.cookies.keys())}")
            print(f"🔍 [DEBUG] headers keys: {list(self.auth.headers.keys())}")
            none_cookies = [k for k in self.auth.cookies.keys() if k is None]
            none_headers = [k for k in self.auth.headers.keys() if k is None]
            print(f"🔍 [DEBUG] None cookie keys: {none_cookies}")
            print(f"🔍 [DEBUG] None header keys: {none_headers}")
            print(f"🔍 [DEBUG] safe_cookies keys: {list(safe_cookies.keys())}")
            print(f"🔍 [DEBUG] safe_headers keys: {list(safe_headers.keys())}")

        for attempt in range(3):
            try:
                async with session.get(
                    url,
                    headers=safe_headers,
                    cookies=safe_cookies,
                    timeout=timeout,
                ) as resp:
                    if resp.status == 429:
                        print(f"⚠️ [async_get] 429 Rate Limit - {url}")
                        await asyncio.sleep(3 * (attempt + 1))
                        continue
                    if resp.status != 200:
                        print(f"❌ [async_get] Lỗi {resp.status} - {url}")
                        return None
                    
                    text_data = await resp.text()
                    try:
                        return builtin_json.loads(text_data)
                    except Exception as parse_e:
                        print(f"❌ [async_get] Parse JSON lỗi: {parse_e} - {url}")
                        return None
            except Exception as e:
                print(f"❌ [async_get] Exception: {e} - {url}")
                await asyncio.sleep(2)
        return None

    async def async_post(self, session: ClientSession, url: str, json=None, timeout: int = 20) -> dict | None:
        """POST async với retry."""
        for attempt in range(3):
            try:
                async with session.post(
                    url,
                    headers=self.auth.headers,
                    cookies=self.auth.cookies,
                    json=json,
                    timeout=timeout,
                ) as resp:
                    if resp.status == 429:
                        await asyncio.sleep(3 * (attempt + 1))
                        continue
                    if resp.status != 200:
                        return None
                    return await resp.json()
            except Exception:
                await asyncio.sleep(2)
        return None

    def create_async_session(self, limit: int = 50) -> ClientSession:
        """Tạo aiohttp session với connection pool."""
        connector = TCPConnector(limit=limit, limit_per_host=30)
        return ClientSession(connector=connector)
