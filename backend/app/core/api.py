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

    def _request(self, method: str, url: str, json=None, params=None, timeout=60) -> requests.Response | None:
        """Request sync với auto-retry khi bị 429."""
        for attempt in range(self.max_retries):
            try:
                resp = requests.request(
                    method, url,
                    headers=self.auth.headers,
                    cookies=self.auth.cookies,
                    json=json,
                    params=params,
                    timeout=timeout,
                )
                if resp.status_code == 429:
                    wait = self.retry_delay * (attempt + 1)
                    time.sleep(wait)
                    continue
                return resp
            except Exception:
                wait = 3 * (attempt + 1)
                time.sleep(wait)
        return None

    def get(self, path: str, params=None, timeout=60) -> requests.Response | None:
        url = f"{self._base_url}{path}" if path.startswith("/") else path
        return self._request("GET", url, params=params, timeout=timeout)

    def post(self, path: str, json=None, params=None, timeout=60) -> requests.Response | None:
        url = f"{self._base_url}{path}" if path.startswith("/") else path
        return self._request("POST", url, json=json, params=params, timeout=timeout)

    def put(self, path: str, json=None, timeout=60) -> requests.Response | None:
        url = f"{self._base_url}{path}" if path.startswith("/") else path
        return self._request("PUT", url, json=json, timeout=timeout)

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

    async def async_get(self, session: ClientSession, url: str, timeout: int = 20) -> dict | None:
        """GET async với retry."""
        for attempt in range(3):
            try:
                async with session.get(
                    url,
                    headers=self.auth.headers,
                    cookies=self.auth.cookies,
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
