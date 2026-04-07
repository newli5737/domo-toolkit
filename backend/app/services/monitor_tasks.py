import threading
import concurrent.futures
from datetime import datetime
from sqlalchemy.orm import Session

from app.core.api import DomoAPI
from app.core.database import SessionLocal
from app.services.monitor import MonitorService
from app.repositories.monitor_repo import MonitorRepository
from app.repositories.auth_repo import get_auth
from app.core.logger import DomoLogger

_log = DomoLogger("monitor-tasks")

# ─── Global state ─────────────────────────────────────────

monitor_job = {
    "running": False, "started_at": None, "result": None, "progress": None,
}

alert_data = {
    "checked_at": None, "all_ok": True, "failed_datasets": [], "failed_dataflows": [],
}

def set_progress(step: str, processed: int, total: int):
    pct = round(processed / total * 100, 1) if total > 0 else 0
    monitor_job["progress"] = {"step": step, "processed": processed, "total": total, "percent": pct}


def post_crawl_alert(db: Session):
    """Sau mỗi lần crawl, kiểm tra FAILED."""
    repo = MonitorRepository(db)
    alert = repo.get_alerts_from_db()
    alert_data["checked_at"] = alert.checked_at
    alert_data["all_ok"] = alert.all_ok
    alert_data["failed_datasets"] = alert.failed_datasets
    alert_data["failed_dataflows"] = alert.failed_dataflows


def run_health_check_task(stale_hours: int, min_card_count: int, provider_type: str, min_dataflow_count: int, max_workers: int, auth):
    try:
        db_bg = SessionLocal()
        api = DomoAPI(auth)
        service = MonitorService(api, db_bg)
        monitor_job["result"] = service.check_health(
            stale_hours=stale_hours, min_card_count=min_card_count,
            provider_type=provider_type, min_dataflow_count=min_dataflow_count,
            max_workers=max_workers,
        )
        db_bg.close()
    except Exception as e:
        monitor_job["result"] = {"error": str(e)}
    finally:
        monitor_job["running"] = False


def run_crawl_datasets_task(max_workers: int, auth):
    try:
        db_bg = SessionLocal()
        api = DomoAPI(auth)
        service = MonitorService(api, db_bg)

        dataset_details = service.crawl_all_datasets(
            progress_callback=lambda done, total: set_progress("Đang cào datasets...", done, total)
        )

        total_ds = len(dataset_details)
        set_progress("Đang lấy trạng thái thực thi...", 0, total_ds)
        done_count = [0]

        def fetch_execution_state(ds):
            ds_id = ds["id"]
            search_state = ds.get("state", "") or ds.get("status", "")
            ds["dataset_status"] = "DISABLED" if search_state.upper() == "INACTIVE" else search_state

            try:
                detail = service.fetch_dataset_detail(ds_id)
                if detail:
                    if detail.get("card_count") is not None: ds["card_count"] = detail["card_count"]
                    if detail.get("row_count") is not None: ds["row_count"] = detail["row_count"]
                    detail_exec = detail.get("status", "")
                    stream_id = detail.get("stream_id", "")
                    if stream_id:
                        ds["stream_id"] = str(stream_id)
                        schedule = service.fetch_dataset_schedule(stream_id)
                        if schedule:
                            last_exec = schedule.get("last_execution")
                            if last_exec: ds["last_execution_state"] = last_exec.get("state", "")
                            ds["schedule_state"] = schedule.get("schedule_state", ds.get("schedule_state", ""))
                    if not ds.get("last_execution_state") and detail_exec:
                        ds["last_execution_state"] = detail_exec
            except Exception as e:
                print(f"[CRAWL] Dataset {ds_id} error: {e}")
            finally:
                done_count[0] += 1
                if done_count[0] % 20 == 0 or done_count[0] == total_ds:
                    set_progress("Đang lấy trạng thái...", done_count[0], total_ds)

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            executor.map(fetch_execution_state, dataset_details)

        set_progress("Đang lưu vào database...", 0, 1)
        service.save_datasets(dataset_details)
        post_crawl_alert(db_bg)

        monitor_job["result"] = {
            "type": "crawl_datasets",
            "summary": {"datasets": {"total_crawled": len(dataset_details)}},
            "checked_at": datetime.now().isoformat(),
        }
        db_bg.close()
    except Exception as e:
        import traceback; traceback.print_exc()
        monitor_job["result"] = {"error": str(e)}
    finally:
        monitor_job["running"] = False


def run_crawl_dataflows_task(max_workers: int, auth):
    try:
        db_bg = SessionLocal()
        api = DomoAPI(auth)
        service = MonitorService(api, db_bg)

        set_progress("Đang cào danh sách dataflows...", 0, 1)
        raw_dataflows = service.crawl_all_dataflows()

        total = len(raw_dataflows)
        set_progress("Đang lấy trạng thái thực thi...", 0, total)
        dataflow_details = []
        done = 0

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(service.process_dataflow, df): df for df in raw_dataflows}
            for future in concurrent.futures.as_completed(futures):
                done += 1
                if done % 10 == 0 or done == total:
                    set_progress("Đang lấy trạng thái...", done, total)
                try:
                    r = future.result()
                    if r: dataflow_details.append(r)
                except Exception as e:
                    print(f"[CRAWL] Dataflow error: {e}")

        set_progress("Đang lưu vào database...", 0, 1)
        service.save_dataflows(dataflow_details)
        service.propagate_dataflow_status_to_datasets(dataflow_details)
        post_crawl_alert(db_bg)

        monitor_job["result"] = {
            "type": "crawl_dataflows",
            "summary": {"dataflows": {"total_crawled": len(dataflow_details)}},
            "checked_at": datetime.now().isoformat(),
        }
        db_bg.close()
    except Exception as e:
        import traceback; traceback.print_exc()
        monitor_job["result"] = {"error": str(e)}
    finally:
        monitor_job["running"] = False
