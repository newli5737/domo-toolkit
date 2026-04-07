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


def _run_auto_check(manual_req=None, auth_override=None):
    """Execute the auto-check logic: crawl dataflows → crawl datasets → check + alert.

    Thứ tự bắt buộc:
      1. Crawl dataflows (sync, chờ xong)
      2. Crawl datasets  (sync, chờ xong)
      3. Auto-check DB → post Backlog nếu BOTH conditions met
    """
    import concurrent.futures
    from datetime import datetime, timezone

    log.info("=" * 60)
    log.info("⏰ [AUTO-CHECK] Bắt đầu — triggered by cron scheduler")
    log.info("=" * 60)

    try:
        from app.config import get_settings
        from app.core.auth import DomoAuth
        from app.core.api import DomoAPI
        from app.core.database import SessionLocal
        from sqlalchemy import text
        from app.services.monitor import MonitorService
        from app.repositories.monitor_repo import MonitorRepository
        from app.schemas.monitor import AutoCheckRequest
        from app.repositories.auth_repo import get_auth

        # ── Load config & in ra để debug ────────────────────────────
        db = SessionLocal()
        config = MonitorRepository(db).load_alert_config()
        settings = get_settings()
        auth = auth_override if auth_override else get_auth()

        log.info("[AUTO-CHECK] ⚙️  Cấu hình hiện tại:")
        log.info(f"  schedule_enabled : {config.get('schedule_enabled')}")
        log.info(f"  schedule_days    : {config.get('schedule_days')}")
        log.info(f"  schedule_hour    : {config.get('schedule_hour', 8):02d}:{config.get('schedule_minute', 0):02d} JST")
        log.info(f"  provider_type    : {manual_req.provider_type if manual_req else config.get('provider_type')}  ← Import Type để filter dataset")
        log.info(f"  min_card_count   : {manual_req.min_card_count if manual_req else config.get('min_card_count')}  ← card > N mới check")
        log.info(f"  alert_email      : {manual_req.alert_email if manual_req else config.get('alert_email')} (chưa cấu hình nếu trống)")
        log.info(f"  backlog_issue_id : {settings.backlog_issue_id or '(chưa cấu hình)'}")
        log.info(f"  has_gmail        : {bool(settings.gmail_email and settings.gmail_app_password)}")
        if not auth.is_valid:
            log.error("❌ [AUTO-CHECK] Bị bỏ qua: chưa login DOMO. Hãy login trước.")
            db.close()
            return

        api = DomoAPI(auth)
        service = MonitorService(api, db)
        max_workers = 10

        # ════════════════════════════════════════════════════════════
        # STEP 1: Crawl toàn bộ Dataflows (PHẢI CHẠY TRƯỚC dataset)
        # Lý do: propagate_dataflow_status_to_datasets() sẽ ghi
        #        last_execution_state vào output dataset của dataflow.
        # ════════════════════════════════════════════════════════════
        log.info("-" * 50)
        log.info("[STEP 1/3] 🔄 Crawl Dataflows bắt đầu...")
        log.info("-" * 50)

        raw_dataflows = service.crawl_all_dataflows()
        log.info(f"[STEP 1] Search xong: {len(raw_dataflows)} dataflows thô")

        dataflow_details = []
        df_done = [0]
        df_total = len(raw_dataflows)

        def _process_df_with_log(df_stub):
            result = service.process_dataflow(df_stub)
            df_done[0] += 1
            if df_done[0] % 20 == 0 or df_done[0] == df_total:
                log.info(f"  [STEP 1] Executions fetched: {df_done[0]}/{df_total}")
            return result

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_process_df_with_log, df): df for df in raw_dataflows}
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        dataflow_details.append(result)
                except Exception as e:
                    log.error(f"  [STEP 1] Process dataflow lỗi: {e}")

        # Thống kê trạng thái dataflow
        from collections import Counter
        df_state_dist = Counter(df.get("last_execution_state", "") or "UNKNOWN" for df in dataflow_details)
        df_failed_list = [df for df in dataflow_details if "FAILED" in (df.get("last_execution_state") or "").upper()
                          or "FAILED" in (df.get("status") or "").upper()]

        log.info(f"[STEP 1] Execution state distribution: {dict(df_state_dist)}")
        log.info(f"[STEP 1] Dataflows FAILED: {len(df_failed_list)} / {len(dataflow_details)}")
        for f in df_failed_list[:10]:  # in tối đa 10 cái
            log.info(f"  ⚠️  FAILED Dataflow: [{f['id']}] {f.get('name','')[:60]}")

        service.save_dataflows(dataflow_details)
        service.propagate_dataflow_status_to_datasets(dataflow_details)
        log.info(f"[STEP 1] ✅ Xong — {len(dataflow_details)} dataflows lưu DB, propagated sang output datasets")

        # ════════════════════════════════════════════════════════════
        # STEP 2: Crawl toàn bộ Datasets (SAU khi propagate xong)
        # ════════════════════════════════════════════════════════════
        log.info("-" * 50)
        log.info("[STEP 2/3] 🔄 Crawl Datasets bắt đầu...")
        log.info("-" * 50)

        dataset_details = service.crawl_all_datasets()
        total_ds = len(dataset_details)
        log.info(f"[STEP 2] Search xong: {total_ds} datasets thô")
        done_count = [0]

        def fetch_execution_state(ds):
            ds_id = ds["id"]
            search_state = ds.get("state", "") or ds.get("status", "")
            ds["dataset_status"] = "DISABLED" if search_state.upper() == "INACTIVE" else search_state
            try:
                detail = service.fetch_dataset_detail(ds_id)
                if detail:
                    if detail.get("card_count") is not None:
                        ds["card_count"] = detail["card_count"]
                    if detail.get("row_count") is not None:
                        ds["row_count"] = detail["row_count"]
                    detail_exec_status = detail.get("status", "")
                    stream_id = detail.get("stream_id", "")
                    if stream_id:
                        ds["stream_id"] = str(stream_id)
                        schedule = service.fetch_dataset_schedule(stream_id)
                        if schedule:
                            last_exec = schedule.get("last_execution")
                            if last_exec:
                                ds["last_execution_state"] = last_exec.get("state", "")
                            ds["schedule_state"] = schedule.get("schedule_state", ds.get("schedule_state", ""))
                    if not ds.get("last_execution_state") and detail_exec_status:
                        ds["last_execution_state"] = detail_exec_status
            except Exception as e:
                log.error(f"  [STEP 2] Dataset {ds_id} fetch error: {e}")
            finally:
                done_count[0] += 1
                if done_count[0] % 50 == 0 or done_count[0] == total_ds:
                    log.info(f"  [STEP 2] Detail fetched: {done_count[0]}/{total_ds}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            executor.map(fetch_execution_state, dataset_details)

        # Thống kê dataset
        ds_exec_dist = Counter(ds.get("last_execution_state", "") or "UNKNOWN" for ds in dataset_details)
        ds_status_dist = Counter(ds.get("dataset_status", "") or "UNKNOWN" for ds in dataset_details)
        log.info(f"[STEP 2] last_execution_state distribution: {dict(ds_exec_dist)}")
        log.info(f"[STEP 2] dataset_status distribution     : {dict(ds_status_dist)}")

        service.save_datasets(dataset_details)
        log.info(f"[STEP 2] ✅ Xong — {len(dataset_details)} datasets lưu DB")

        # ════════════════════════════════════════════════════════════
        # STEP 3: Kiểm tra điều kiện → Post Backlog / Gửi email
        # Điều kiện post Backlog:
        #   - provider_type = <Import Type cấu hình>
        #   - card_count >= <min_card_count>
        #   - KHÔNG có dataset nào FAILED thỏa 2 điều kiện trên
        # ════════════════════════════════════════════════════════════
        log.info("-" * 50)
        log.info("[STEP 3/3] 🔍 Kiểm tra điều kiện post Backlog...")
        log.info("-" * 50)
        if manual_req:
            req = manual_req
        else:
            req = AutoCheckRequest(
                alert_email=config.get("alert_email", ""),
                min_card_count=config.get("min_card_count", 40),
                provider_type=config.get("provider_type", "mysql-ssh"),
            )

        log.info(f"[STEP 3] Điều kiện filter: provider_type='{req.provider_type}' AND card_count >= {req.min_card_count}")

        # Query thủ công để log trước khi gọi trigger_auto_check
        try:
            pt = req.provider_type.strip()
            mc = req.min_card_count
            if pt:
                candidates = db.execute(text(
                    "SELECT id, name, provider_type, card_count, last_execution_state "
                    "FROM datasets WHERE LOWER(provider_type) = LOWER(:pt) AND card_count >= :mc "
                    "ORDER BY card_count DESC LIMIT 20"
                ), {"pt": pt, "mc": mc}).mappings().all()
                failed_cands = [r for r in (candidates or []) if "FAILED" in (r.get("last_execution_state") or "").upper()]
                log.info(f"[STEP 3] Datasets khớp điều kiện (type='{pt}', card>={mc}): {len(candidates or [])} cái")
                for r in (candidates or [])[:10]:
                    state = r.get("last_execution_state") or "-"
                    log.info(f"  {'❌' if 'FAILED' in state.upper() else '✅'} [{r['id']}] {r.get('name','')[:50]:50s} | cards={r.get('card_count',0):4d} | exec={state}")
                log.info(f"[STEP 3] → Trong đó FAILED: {len(failed_cands)} cái")
                if failed_cands:
                    log.info(f"[STEP 3] ⚠️  Có FAILED → KHÔNG post Backlog")
                else:
                    log.info(f"[STEP 3] ✅ Không có FAILED → SẼ post Backlog (nếu cấu hình Backlog OK)")
            else:
                log.info(f"[STEP 3] provider_type trống → kiểm tra TẤT CẢ datasets")
        except Exception as qe:
            log.error(f"[STEP 3] Debug query lỗi: {qe}")

        repo = MonitorRepository(db)
        check_result = repo.run_auto_check(req.provider_type, req.min_card_count, req.alert_email)
        log.info(f"[STEP 3] Kết quả auto-check: {check_result}")
        log.info("=" * 60)
        log.info(f"✅ [AUTO-CHECK] HOÀN THÀNH")
        log.info(f"   backlog_posted        : {check_result.backlog_posted}")
        log.info(f"   email_sent            : {check_result.email_sent}")
        log.info(f"   failed_dataset_count  : {check_result.failed_dataset_count}")
        log.info(f"   failed_dataflow_count : {check_result.failed_dataflow_count}")
        db.close()
    except Exception as e:
        log.error(f"❌ [AUTO-CHECK] Error: {e}", exc_info=True)
        try:
            db.close()
        except Exception:
            pass


def _run_domo_relogin():
    """Re-login DOMO lúc 0h00 mỗi ngày — session chỉ có hiệu lực 1 ngày."""
    log.info("⏰ DOMO midnight re-login triggered")
    try:
        from app.config import get_settings
        from app.repositories.auth_repo import AuthRepository
        from app.core.database import SessionLocal

        settings = get_settings()
        if not settings.domo_username or not settings.domo_password:
            log.warning("⚠️ DOMO credentials chưa cấu hình trong .env, bỏ qua.")
            return

        with SessionLocal() as db:
            repo = AuthRepository(db)
            result = repo.login(settings.domo_username, settings.domo_password)
            if result.success:
                log.info(f"✅ DOMO re-login thành công: {result.username}")
            else:
                log.error(f"❌ DOMO re-login thất bại: {result.message}")
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
            from app.core.database import SessionLocal
            from app.repositories.monitor_repo import MonitorRepository
            with SessionLocal() as db:
                config = MonitorRepository(db).load_alert_config()
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
