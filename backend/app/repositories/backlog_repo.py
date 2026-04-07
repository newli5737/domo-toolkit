"""BacklogRepository — Business logic cho Backlog API integration."""

import logging
import requests
from fastapi import HTTPException

from app.config import get_settings
from app.schemas.backlog import BacklogPostResponse, BacklogStatusResponse, BacklogConfigResponse


log = logging.getLogger(__name__)


from sqlalchemy.orm import Session

class BacklogRepository:
    """Xử lý giao tiếp với Backlog REST API."""

    def __init__(self, db: Session = None):
        self.db = db
        self.settings = get_settings()

    def _api_url(self, path: str) -> str:
        return f"{self.settings.backlog_base_url}/api/v2{path}?apiKey={self.settings.backlog_api_key}"

    def _check_api_key(self):
        if not self.settings.backlog_api_key:
            raise HTTPException(status_code=503, detail="Chưa cấu hình BACKLOG_API_KEY trong .env")

    def post_status(self, comment: str) -> BacklogPostResponse:
        self._check_api_key()

        if not self.settings.backlog_issue_id:
            raise HTTPException(status_code=400, detail="Chưa cấu hình BACKLOG_ISSUE_ID trong .env")

        issue_id = self.settings.backlog_issue_id
        errors = []

        # Bước 1: Update status → In Progress
        try:
            resp = requests.patch(
                self._api_url(f"/issues/{issue_id}"),
                json={"statusId": 2},
                headers={"Content-Type": "application/json"},
                timeout=30,
            )
            log.info(f"PATCH /issues/{issue_id} → {resp.status_code}")
            if resp.status_code >= 400:
                errors.append(f"Update status thất bại: HTTP {resp.status_code} — {resp.text[:200]}")
        except Exception as e:
            errors.append(f"Update status error: {str(e)}")

        # Bước 2: Thêm comment
        comment_result = None
        if comment:
            try:
                resp2 = requests.post(
                    self._api_url(f"/issues/{issue_id}/comments"),
                    json={"content": comment},
                    headers={"Content-Type": "application/json"},
                    timeout=30,
                )
                log.info(f"POST /issues/{issue_id}/comments → {resp2.status_code}")
                if resp2.status_code < 400:
                    comment_result = resp2.json()
                else:
                    errors.append(f"Thêm comment thất bại: HTTP {resp2.status_code} — {resp2.text[:200]}")
            except Exception as e:
                errors.append(f"Comment error: {str(e)}")

        return BacklogPostResponse(success=len(errors) == 0, errors=errors, comment=comment_result)

    def get_status(self) -> BacklogStatusResponse:
        self._check_api_key()
        try:
            resp = requests.get(self._api_url("/users/myself"), timeout=15)
            if resp.status_code == 200:
                user = resp.json()
                return BacklogStatusResponse(
                    connected=True,
                    user=user.get("name", ""),
                    backlog_base_url=self.settings.backlog_base_url,
                )
            return BacklogStatusResponse(connected=False, error=f"HTTP {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            return BacklogStatusResponse(connected=False, error=str(e))

    def get_config(self) -> BacklogConfigResponse:
        return BacklogConfigResponse(
            backlog_base_url=self.settings.backlog_base_url,
            issue_id=self.settings.backlog_issue_id,
            has_api_key=bool(self.settings.backlog_api_key),
        )
