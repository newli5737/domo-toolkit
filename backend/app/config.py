"""Cấu hình ứng dụng — đọc từ .env."""

from pydantic_settings import BaseSettings
from functools import lru_cache
import os


class Settings(BaseSettings):
    # PostgreSQL
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "DOMO"
    db_user: str = "postgres"
    db_password: str = "test1234"

    # Domo
    domo_instance: str = "astecpaints-co-jp.domo.com"
    domo_username: str = ""
    domo_password: str = ""

    # Monitor
    monitor_stale_hours: int = 24
    monitor_min_card_count: int = 0

    # Google API (optional)
    google_api_key: str = ""

    # Backlog
    backlog_base_url: str = "https://mothers-sp.backlog.jp"
    backlog_cookie: str = ""
    backlog_issue_id: str = ""
    backlog_csrf_token: str = ""

    # Email Alert (Gmail SMTP)
    gmail_email: str = ""
    gmail_app_password: str = ""
    alert_email_to: str = ""

    # CORS
    cors_origins: str = ""

    @property
    def db_url(self) -> str:
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"

    @property
    def domo_base_url(self) -> str:
        return f"https://{self.domo_instance}"

    model_config = {
        "env_file": os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"),
        "env_file_encoding": "utf-8",
        "extra": "ignore",
    }


@lru_cache()
def get_settings() -> Settings:
    return Settings()
