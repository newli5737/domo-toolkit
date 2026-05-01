

import io
import threading
from datetime import datetime
from fastapi import APIRouter, Query, HTTPException, Depends
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.config import get_settings
from app.core.auth import DomoAuth
from app.core.api import DomoAPI
from app.core.database import SessionLocal, get_db
from app.services.monitor import MonitorService
from app.core.logger import DomoLogger
from app.repositories.monitor_repo import MonitorRepository
from app.schemas.monitor import (
    AutoCheckRequest, AutoCheckResult, AutoCheckConfigResponse,
    DatasetListResponse, DataflowListResponse, ProviderTypesResponse,
    AlertDataResponse, SaveConfigResponse, JobStatusResponse,
)

from app.services.monitor_tasks import (
    monitor_job, alert_data,
    run_health_check_task, run_crawl_datasets_task, run_crawl_dataflows_task
)

_log = DomoLogger("monitor")
router = APIRouter(prefix="/api/monitor", tags=["monitor"])

_alert_config: dict | None = None

def _get_alert_config() -> dict:
    global _alert_config
    if _alert_config is None:
        try:
            db = SessionLocal()
            _alert_config = MonitorRepository(db).load_alert_config()
            db.close()
        except Exception:
            _alert_config = {}
    return _alert_config


from app.dependencies import require_auth



@router.post("/check", response_model=JobStatusResponse)
def trigger_health_check(
    stale_hours: int = Query(default=24),
    min_card_count: int = Query(default=0),
    provider_type: str = Query(default=""),
    min_dataflow_count: int = Query(default=0),
    max_workers: int = Query(default=10),
    db: Session = Depends(get_db),
    auth=Depends(require_auth)
):

    if monitor_job["running"]:
        return JobStatusResponse(status="already_running", message="Health check đang chạy...", started_at=monitor_job["started_at"])

    monitor_job.update(running=True, started_at=datetime.now().isoformat(), result=None, progress=None)

    threading.Thread(
        target=run_health_check_task, 
        args=(stale_hours, min_card_count, provider_type, min_dataflow_count, max_workers, auth), 
        daemon=True
    ).start()
    return JobStatusResponse(status="started", message="Health check đã bắt đầu.")


@router.post("/crawl/datasets", response_model=JobStatusResponse)
def crawl_datasets_only(max_workers: int = Query(default=10), db: Session = Depends(get_db), auth=Depends(require_auth)):
    import concurrent.futures
    if monitor_job["running"]:
        return JobStatusResponse(status="already_running", message="Đang chạy crawl...")

    monitor_job.update(running=True, started_at=datetime.now().isoformat(), result=None, progress=None)

    threading.Thread(
        target=run_crawl_datasets_task, 
        args=(max_workers, auth), 
        daemon=True
    ).start()
    return JobStatusResponse(status="started", message="Đang cào datasets...")


@router.post("/crawl/dataflows", response_model=JobStatusResponse)
def crawl_dataflows_only(max_workers: int = Query(default=10), db: Session = Depends(get_db), auth=Depends(require_auth)):
    import concurrent.futures
    if monitor_job["running"]:
        return JobStatusResponse(status="already_running", message="Đang chạy crawl...")

    monitor_job.update(running=True, started_at=datetime.now().isoformat(), result=None, progress=None)

    threading.Thread(
        target=run_crawl_dataflows_task, 
        args=(max_workers, auth), 
        daemon=True
    ).start()
    return JobStatusResponse(status="started", message="Đang cào dataflows...")




@router.get("/status")
def get_check_status():
    if monitor_job["running"]:
        return {"status": "running", "started_at": monitor_job["started_at"], "progress": monitor_job.get("progress")}
    if monitor_job["result"]:
        return {"status": "completed", "result": monitor_job["result"]}
    return {"status": "idle", "message": "Chưa có health check nào chạy."}


@router.get("/datasets", response_model=DatasetListResponse)
def list_datasets(
    provider_type: str = Query(default=""),
    min_card_count: int = Query(default=0),
    limit: int = Query(default=5000),
    offset: int = Query(default=0),
    db: Session = Depends(get_db)
):
    return MonitorRepository(db).list_datasets(provider_type, min_card_count, limit, offset)


@router.get("/dataflows", response_model=DataflowListResponse)
def list_dataflows(
    status_filter: str = Query(default=""),
    limit: int = Query(default=5000),
    offset: int = Query(default=0),
    db: Session = Depends(get_db)
):
    return MonitorRepository(db).list_dataflows(status_filter, limit, offset)


@router.get("/provider-types", response_model=ProviderTypesResponse)
def get_provider_types(db: Session = Depends(get_db)):
    return MonitorRepository(db).get_provider_types()


@router.get("/datasets/{dataset_id}/schedule")
def get_dataset_schedule(dataset_id: str, db: Session = Depends(get_db), auth=Depends(require_auth)):

    row = db.execute(text("SELECT stream_id FROM datasets WHERE id = :id"), {"id": dataset_id}).mappings().first()
    stream_id = row.get("stream_id") if row else None

    api = DomoAPI(auth)
    service = MonitorService(api, db)

    if not stream_id:
        detail = service.fetch_dataset_detail(dataset_id)
        stream_id = detail.get("stream_id") if detail else None

    if not stream_id:
        raise HTTPException(status_code=404, detail=f"Dataset {dataset_id} không có stream_id.")

    schedule = service.fetch_dataset_schedule(stream_id)
    if not schedule:
        raise HTTPException(status_code=404, detail="Không tìm thấy schedule info.")
    return schedule


@router.get("/dataflows/{dataflow_id}/executions")
def get_dataflow_executions(dataflow_id: str, limit: int = Query(default=100), offset: int = Query(default=0), db: Session = Depends(get_db), auth=Depends(require_auth)):

    api = DomoAPI(auth)
    service = MonitorService(api, db)
    executions = service.fetch_dataflow_execution_history(dataflow_id, limit=limit, offset=offset)
    return {"dataflow_id": dataflow_id, "total": len(executions), "executions": executions}




@router.post("/auto-check", response_model=JobStatusResponse)
def trigger_auto_check(req: AutoCheckRequest, db: Session = Depends(get_db), auth=Depends(require_auth)):
    cfg = _get_alert_config()
    if req.alert_email:
        cfg["alert_email"] = req.alert_email
    cfg["min_card_count"] = req.min_card_count
    cfg["provider_type"] = req.provider_type

    if monitor_job.get("running"):
        return JobStatusResponse(status="already_running", message="Auto-check đang chạy ngầm...", started_at=monitor_job.get("started_at"))

    from app.scheduler import _run_auto_check
    monitor_job.update(running=True, started_at=datetime.now().isoformat(), result=None, progress=None)

    threading.Thread(target=_run_auto_check, args=(req, auth), daemon=True).start()
    return JobStatusResponse(status="started", message="Đã bắt đầu Crawl và Auto-Check (chạy ngầm)...")


@router.get("/alerts", response_model=AlertDataResponse)
def get_alerts(db: Session = Depends(get_db)):
    if not alert_data.get("checked_at"):
        try:
            repo = MonitorRepository(db)
            alert = repo.get_alerts_from_db()
            alert_data.update(
                checked_at=alert.checked_at, all_ok=alert.all_ok,
                failed_datasets=alert.failed_datasets, failed_dataflows=alert.failed_dataflows,
            )
        except Exception as e:
            print(f"[ALERTS] DB error: {e}")
    return alert_data


@router.post("/save-alert-config", response_model=SaveConfigResponse)
def save_alert_config_endpoint(req: AutoCheckRequest, db: Session = Depends(get_db)):
    cfg = _get_alert_config()
    cfg.update(
        alert_email=req.alert_email, min_card_count=req.min_card_count,
        provider_type=req.provider_type, schedule_enabled=req.schedule_enabled,
        schedule_hour=req.schedule_hour, schedule_minute=req.schedule_minute,
        schedule_days=req.schedule_days,
    )
    MonitorRepository(db).save_alert_config(cfg)

    try:
        from app.scheduler import update_schedule
        update_schedule(cfg)
    except Exception as e:
        print(f"[SAVE-CONFIG] Scheduler update error: {e}")

    return SaveConfigResponse(saved=True, config=cfg)


@router.get("/auto-check-config", response_model=AutoCheckConfigResponse)
def get_auto_check_config():
    settings = get_settings()
    cfg = _get_alert_config()
    return AutoCheckConfigResponse(
        backlog_base_url=settings.backlog_base_url,
        backlog_issue_id=settings.backlog_issue_id,
        has_backlog_cookie=False,
        alert_email_to=cfg.get("alert_email", ""),
        min_card_count=cfg.get("min_card_count", 40),
        provider_type=cfg.get("provider_type", "mysql-ssh"),
        has_gmail=bool(settings.gmail_email and settings.gmail_app_password),
        schedule_enabled=cfg.get("schedule_enabled", False),
        schedule_hour=cfg.get("schedule_hour", 8),
        schedule_minute=cfg.get("schedule_minute", 0),
        schedule_days=cfg.get("schedule_days", "mon,tue,wed,thu,fri"),
    )




@router.get("/export/datasets/csv")
def export_datasets_csv(
    provider_type: str = Query(default=""),
    min_card_count: int = Query(default=0),
    search: str = Query(default=""),
    db: Session = Depends(get_db)
):
    csv_bytes = MonitorRepository(db).export_datasets_csv(provider_type, min_card_count, search)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return StreamingResponse(
        io.BytesIO(csv_bytes), media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=datasets_{timestamp}.csv"},
    )


@router.get("/export/dataflows/csv")
def export_dataflows_csv(
    status_filter: str = Query(default=""),
    search: str = Query(default=""),
    db: Session = Depends(get_db)
):
    csv_bytes = MonitorRepository(db).export_dataflows_csv(status_filter, search)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return StreamingResponse(
        io.BytesIO(csv_bytes), media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=dataflows_{timestamp}.csv"},
    )
