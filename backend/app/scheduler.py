"""Scheduler — APScheduler cron jobs for auto-check datasets & dataflows,
DOMO midnight re-login, và Backlog midnight re-login.
"""

import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

log = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None

# Day name mapping
DAY_MAP = {
    "mon": "mon", "tue": "tue", "wed": "wed", "thu": "thu",
    "fri": "fri", "sat": "sat", "sun": "sun",
}


# ─── Job functions ────────────────────────────────────────────


def _run_auto_check():
    """Execute the auto-check logic (crawl + check + alert)."""
    log.info("⏰ Auto-check triggered by cron")
    try:
        from app.routers.monitor import trigger_auto_check, AutoCheckRequest, _load_alert_config
        config = _load_alert_config()
        req = AutoCheckRequest(
            alert_email=config.get("alert_email", ""),
            min_card_count=config.get("min_card_count", 40),
            provider_type=config.get("provider_type", "mysql-ssh"),
        )
        result = trigger_auto_check(req)
        log.info(f"✅ Auto-check done: {result}")
    except Exception as e:
        log.error(f"❌ Auto-check error: {e}", exc_info=True)


def _run_domo_relogin():
    """Re-login DOMO lúc 0h00 mỗi ngày — session chỉ có hiệu lực 1 ngày."""
    log.info("⏰ DOMO midnight re-login triggered")
    try:
        from app.config import get_settings
        from app.routers.auth import get_auth, _save_session

        settings = get_settings()
        if not settings.domo_username or not settings.domo_password:
            log.warning("⚠️ DOMO credentials chưa cấu hình trong .env, bỏ qua.")
            return

        auth = get_auth()
        result = auth.login(settings.domo_username, settings.domo_password)
        if result["success"]:
            _save_session(auth)
            log.info(f"✅ DOMO re-login thành công: {auth.username}")
        else:
            log.error(f"❌ DOMO re-login thất bại: {result['message']}")
    except Exception as e:
        log.error(f"❌ DOMO re-login error: {e}", exc_info=True)



def init_scheduler(config: dict | None = None):
    """Initialize the scheduler on app startup."""
    global _scheduler
    if _scheduler is not None:
        return

    _scheduler = BackgroundScheduler(timezone="Asia/Tokyo")
    _scheduler.start()
    log.info("🚀 Scheduler started (Asia/Tokyo)")

    # ── Midnight re-login jobs (0h00 JST mỗi ngày) ──
    _scheduler.add_job(
        _run_domo_relogin,
        trigger=CronTrigger(hour=0, minute=0, timezone="Asia/Tokyo"),
        id="domo_midnight_relogin",
        name="DOMO Midnight Re-Login",
        replace_existing=True,
    )
    log.info("✅ DOMO midnight re-login job đã đăng ký (00:00 JST)")

    if config is None:
        try:
            from app.routers.monitor import _load_alert_config
            config = _load_alert_config()
        except Exception:
            config = {}

    update_schedule(config)


def update_schedule(config: dict):
    """Update or create the auto-check cron job based on config."""
    global _scheduler
    if _scheduler is None:
        return

    job_id = "auto_check_cron"

    # Remove existing job if any
    try:
        _scheduler.remove_job(job_id)
        log.info(f"Removed old job '{job_id}'")
    except Exception:
        pass

    enabled = config.get("schedule_enabled", False)
    if not enabled:
        log.info("Schedule disabled, no job created")
        return

    hour = config.get("schedule_hour", 8)
    minute = config.get("schedule_minute", 0)
    days_str = config.get("schedule_days", "mon,tue,wed,thu,fri")
    days = ",".join(d.strip().lower()[:3] for d in days_str.split(",") if d.strip())

    trigger = CronTrigger(
        day_of_week=days,
        hour=hour,
        minute=minute,
        timezone="Asia/Tokyo",
    )

    _scheduler.add_job(
        _run_auto_check,
        trigger=trigger,
        id=job_id,
        name="Domo Auto-Check (Dataset + Dataflow)",
        replace_existing=True,
    )
    log.info(f"✅ Job scheduled: {days} at {hour:02d}:{minute:02d} JST")


def shutdown_scheduler():
    """Shutdown the scheduler."""
    global _scheduler
    if _scheduler:
        _scheduler.shutdown(wait=False)
        _scheduler = None
        log.info("Scheduler shut down")
