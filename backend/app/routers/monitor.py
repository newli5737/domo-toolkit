"""Monitor Router — API endpoints cho giám sát datasets & dataflows."""

import json
import os
import threading
import requests as http_requests
from datetime import datetime
from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel
from typing import Optional

from app.config import get_settings
from app.core.auth import DomoAuth
from app.core.api import DomoAPI
from app.core.db import DomoDatabase
from app.services.monitor import MonitorService
from app.services.email_service import send_alert_email

router = APIRouter(prefix="/api/monitor", tags=["monitor"])

# Track running job
_monitor_job = {
    "running": False,
    "started_at": None,
    "result": None,
    "progress": None,  # {"step": "...", "processed": N, "total": M, "percent": P}
}

# Alert data
_alert_data = {
    "checked_at": None,
    "all_ok": True,
    "failed_datasets": [],   # [{id, name, provider_type, last_execution_state, card_count}]
    "failed_dataflows": [],  # [{id, name, last_execution_state}]
}

# Alert config — stored in DB app_settings table

def _load_alert_config() -> dict:
    """Load alert config from DB app_settings table."""
    try:
        db = _get_db()
        rows = db.query("SELECT key, value FROM app_settings WHERE key LIKE 'alert_%' OR key LIKE 'schedule_%'")
        db.close()
        config = {"alert_email": "", "min_card_count": 40, "provider_type": "mysql-ssh",
                  "schedule_enabled": False, "schedule_hour": 8, "schedule_minute": 0,
                  "schedule_days": "mon,tue,wed,thu,fri"}
        for row in (rows or []):
            k, v = row["key"], row["value"]
            if k == "alert_email":
                config["alert_email"] = v
            elif k == "alert_min_card_count":
                config["min_card_count"] = int(v)
            elif k == "alert_provider_type":
                config["provider_type"] = v
            elif k == "schedule_enabled":
                config["schedule_enabled"] = v.lower() == "true"
            elif k == "schedule_hour":
                config["schedule_hour"] = int(v)
            elif k == "schedule_minute":
                config["schedule_minute"] = int(v)
            elif k == "schedule_days":
                config["schedule_days"] = v
        return config
    except Exception as e:
        print(f"[ALERT-CONFIG] Load from DB error: {e}")
        return {"alert_email": "", "min_card_count": 40, "provider_type": "mysql-ssh",
                "schedule_enabled": False, "schedule_hour": 8, "schedule_minute": 0,
                "schedule_days": "mon,tue,wed,thu,fri"}

def _save_alert_config(config: dict):
    """Save alert config to DB app_settings table."""
    try:
        db = _get_db()
        mappings = {
            "alert_email": str(config.get("alert_email", "")),
            "alert_min_card_count": str(config.get("min_card_count", 40)),
            "alert_provider_type": str(config.get("provider_type", "mysql-ssh")),
            "schedule_enabled": str(config.get("schedule_enabled", False)).lower(),
            "schedule_hour": str(config.get("schedule_hour", 8)),
            "schedule_minute": str(config.get("schedule_minute", 0)),
            "schedule_days": str(config.get("schedule_days", "mon,tue,wed,thu,fri")),
        }
        for k, v in mappings.items():
            db.execute(
                """INSERT INTO app_settings (key, value, updated_at) VALUES (%s, %s, NOW())
                   ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()""",
                (k, v)
            )
        db.close()
    except Exception as e:
        print(f"[ALERT-CONFIG] Save to DB error: {e}")


def _get_db() -> DomoDatabase:
    settings = get_settings()
    return DomoDatabase(
        host=settings.db_host, port=settings.db_port,
        dbname=settings.db_name, user=settings.db_user,
        password=settings.db_password,
    )

_alert_config = _load_alert_config()


def _get_auth() -> DomoAuth:
    from app.routers.auth import get_auth
    return get_auth()


def _set_progress(step: str, processed: int, total: int):
    pct = round(processed / total * 100, 1) if total > 0 else 0
    _monitor_job["progress"] = {
        "step": step, "processed": processed, "total": total, "percent": pct,
    }


def _post_crawl_alert(db: DomoDatabase):
    """Sau mỗi lần crawl, kiểm tra FAILED và gửi email alert tự động."""
    settings = get_settings()

    # Query failed datasets
    failed_ds = db.query(
        "SELECT id, name, provider_type, last_execution_state, card_count "
        "FROM datasets WHERE UPPER(COALESCE(last_execution_state, '')) LIKE 'FAILED%'"
    )
    # Query failed dataflows
    failed_df = db.query(
        "SELECT id, name, last_execution_state "
        "FROM dataflows WHERE UPPER(COALESCE(last_execution_state, '')) LIKE 'FAILED%'"
    )

    all_failed_ds = [dict(r) for r in (failed_ds or [])]
    all_failed_df = [dict(r) for r in (failed_df or [])]

    # Update alert state
    _alert_data["checked_at"] = datetime.now().isoformat()
    _alert_data["all_ok"] = len(all_failed_ds) == 0 and len(all_failed_df) == 0
    _alert_data["failed_datasets"] = all_failed_ds
    _alert_data["failed_dataflows"] = all_failed_df

    if not _alert_data["all_ok"]:
        # Gửi email alert — lấy email từ FE config, không phải .env
        to_email = _alert_config.get("alert_email", "")
        if settings.gmail_email and settings.gmail_app_password and to_email:
            subject = "【Domo監視】データエラー検出"
            body_lines = ["Domoデータ監視でエラーが検出されました。\n"]

            if all_failed_ds:
                body_lines.append(f"■ エラーDataSet ({len(all_failed_ds)}件):")
                for ds in all_failed_ds:
                    body_lines.append(
                        f"  - {ds.get('name', '?')} (ID: {ds.get('id', '?')}, "
                        f"Type: {ds.get('provider_type', '?')}, Cards: {ds.get('card_count', 0)})"
                    )

            if all_failed_df:
                body_lines.append(f"\n■ エラーDataFlow ({len(all_failed_df)}件):")
                for df in all_failed_df:
                    body_lines.append(f"  - {df.get('name', '?')} (ID: {df.get('id', '?')})")

            body_lines.append(f"\n確認時刻: {_alert_data['checked_at']}")
            body = "\n".join(body_lines)

            sent = send_alert_email(
                subject=subject,
                body=body,
                to_email=to_email,
                from_email=settings.gmail_email,
                app_password=settings.gmail_app_password,
            )
            print(f"[AUTO-ALERT] Email sent={sent} to={to_email}, "
                  f"ds_fail={len(all_failed_ds)}, df_fail={len(all_failed_df)}")
        else:
            print(f"[AUTO-ALERT] Has failures but email not configured "
                  f"(gmail={bool(settings.gmail_email)}, to={bool(to_email)})")
    else:
        print("[AUTO-ALERT] All OK — no alert needed")


# ─── Endpoints ────────────────────────────────────────────

@router.post("/check")
def trigger_health_check(
    stale_hours: int = Query(default=24, description="Coi là stale nếu không update quá N giờ"),
    min_card_count: int = Query(default=0, description="Chỉ check dataset dùng bởi >= N cards"),
    provider_type: str = Query(default="", description="Chỉ check dataset import type (mysql, redshift...)"),
    min_dataflow_count: int = Query(default=0, description="Chỉ check dataset liên kết >= N dataflows"),
    max_workers: int = Query(default=10, description="Số luồng crawl song song"),
):
    """Trigger kiểm tra health check ngay lập tức.
    Chạy async trong background thread.
    """
    global _monitor_job

    auth = _get_auth()
    if not auth.is_valid:
        raise HTTPException(status_code=401, detail="Chưa login. Hãy login trước.")

    if _monitor_job["running"]:
        return {
            "status": "already_running",
            "message": "Health check đang chạy, vui lòng đợi...",
            "started_at": _monitor_job["started_at"],
        }

    _monitor_job["running"] = True
    _monitor_job["started_at"] = datetime.now().isoformat()
    _monitor_job["result"] = None
    _monitor_job["progress"] = None

    def run_check():
        try:
            db = _get_db()
            api = DomoAPI(auth)
            service = MonitorService(api, db)
            result = service.check_health(
                stale_hours=stale_hours,
                min_card_count=min_card_count,
                provider_type=provider_type,
                min_dataflow_count=min_dataflow_count,
                max_workers=max_workers,
            )
            _monitor_job["result"] = result
            db.close()
        except Exception as e:
            _monitor_job["result"] = {"error": str(e)}
        finally:
            _monitor_job["running"] = False
            _monitor_job["progress"] = None

    thread = threading.Thread(target=run_check, daemon=True)
    thread.start()

    return {
        "status": "started",
        "message": "Health check đã bắt đầu chạy.",
        "filters": {
            "stale_hours": stale_hours,
            "min_card_count": min_card_count,
            "provider_type": provider_type,
            "min_dataflow_count": min_dataflow_count,
        },
    }


@router.post("/crawl/datasets")
def crawl_datasets_only(
    max_workers: int = Query(default=10, description="Số luồng crawl song song"),
):
    """Cào toàn bộ datasets + fetch stream detail cho execution state + lưu DB."""
    global _monitor_job

    auth = _get_auth()
    if not auth.is_valid:
        raise HTTPException(status_code=401, detail="Chưa login. Hãy login trước.")

    if _monitor_job["running"]:
        return {"status": "already_running", "message": "Đang chạy crawl, vui lòng đợi..."}

    _monitor_job["running"] = True
    _monitor_job["started_at"] = datetime.now().isoformat()
    _monitor_job["result"] = None
    _monitor_job["progress"] = None

    def run_crawl():
        import concurrent.futures
        try:
            db = _get_db()
            api = DomoAPI(auth)
            service = MonitorService(api, db)

            # Phase 1: Search all datasets (with progress)
            def on_phase1_progress(done, total):
                _set_progress("Đang cào danh sách datasets...", done, total)

            dataset_details = service.crawl_all_datasets(progress_callback=on_phase1_progress)
            print(f"[CRAWL] Phase 1 xong: {len(dataset_details)} datasets")

            # Phase 2: Fetch detail + stream schedule for execution state (parallel)
            total_ds = len(dataset_details)
            print(f"[CRAWL] Phase 2: Fetching detail cho {total_ds} datasets...")
            _set_progress("Đang lấy trạng thái thực thi...", 0, total_ds)

            done_count = [0]  # use list for mutable in closure

            def fetch_execution_state(ds):
                ds_id = ds["id"]
                try:
                    # Get detail to retrieve streamId + accurate provider_type
                    detail = service.fetch_dataset_detail(ds_id)
                    if detail:
                        # Update provider_type from detail API (search API chỉ trả "STANDARD")
                        if detail.get("provider_type"):
                            ds["provider_type"] = detail["provider_type"]
                        # Update card_count, row_count from detail (chính xác hơn search)
                        if detail.get("card_count") is not None:
                            ds["card_count"] = detail["card_count"]
                        if detail.get("row_count") is not None:
                            ds["row_count"] = detail["row_count"]

                        stream_id = detail.get("stream_id", "")
                        if stream_id:
                            ds["stream_id"] = str(stream_id)
                            # Fetch schedule via stream
                            schedule = service.fetch_dataset_schedule(stream_id)
                            if schedule:
                                last_exec = schedule.get("last_execution")
                                if last_exec:
                                    ds["last_execution_state"] = last_exec.get("state", "")
                                ds["schedule_state"] = schedule.get("schedule_state", ds.get("schedule_state", ""))
                except Exception as e:
                    if done_count[0] < 3:
                        print(f"[CRAWL] Dataset {ds_id} error: {e}")
                finally:
                    done_count[0] += 1
                    if done_count[0] % 20 == 0 or done_count[0] == total_ds:
                        _set_progress("Đang lấy trạng thái thực thi...", done_count[0], total_ds)

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                executor.map(fetch_execution_state, dataset_details)

            print(f"[CRAWL] Phase 2 xong")

            # Phase 3: Save to DB  
            _set_progress("Đang lưu vào database...", 0, 1)
            service.save_datasets(dataset_details)
            print(f"[CRAWL] Lưu xong {len(dataset_details)} datasets vào DB")

            # Auto-alert nếu có FAILED
            _post_crawl_alert(db)

            _monitor_job["result"] = {
                "type": "crawl_datasets",
                "summary": {"datasets": {"total_crawled": len(dataset_details)}},
                "checked_at": datetime.now().isoformat(),
            }
            db.close()
        except Exception as e:
            import traceback
            traceback.print_exc()
            _monitor_job["result"] = {"error": str(e)}
        finally:
            _monitor_job["running"] = False
            _monitor_job["progress"] = None

    thread = threading.Thread(target=run_crawl, daemon=True)
    thread.start()
    return {"status": "started", "message": "Đang cào datasets..."}


@router.post("/crawl/dataflows")
def crawl_dataflows_only(
    max_workers: int = Query(default=10, description="Số luồng crawl song song"),
):
    """Cào toàn bộ dataflows + fetch executions + lưu DB."""
    global _monitor_job

    auth = _get_auth()
    if not auth.is_valid:
        raise HTTPException(status_code=401, detail="Chưa login. Hãy login trước.")

    if _monitor_job["running"]:
        return {"status": "already_running", "message": "Đang chạy crawl, vui lòng đợi..."}

    _monitor_job["running"] = True
    _monitor_job["started_at"] = datetime.now().isoformat()
    _monitor_job["result"] = None
    _monitor_job["progress"] = None

    def run_crawl():
        import concurrent.futures
        try:
            db = _get_db()
            api = DomoAPI(auth)
            service = MonitorService(api, db)

            # Step 1: Search all dataflows
            _set_progress("Đang cào danh sách dataflows...", 0, 1)
            raw_dataflows = service.crawl_all_dataflows()
            print(f"[CRAWL] Search xong: {len(raw_dataflows)} dataflows")

            # Step 2: Fetch executions in parallel
            total = len(raw_dataflows)
            _set_progress("Đang lấy trạng thái thực thi...", 0, total)
            dataflow_details = []
            done = 0

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(service.process_dataflow, df): df
                    for df in raw_dataflows
                }
                for future in concurrent.futures.as_completed(futures):
                    done += 1
                    if done % 10 == 0 or done == total:
                        _set_progress("Đang lấy trạng thái thực thi...", done, total)
                    try:
                        result = future.result()
                        if result:
                            dataflow_details.append(result)
                    except Exception as e:
                        print(f"[CRAWL] Process dataflow lỗi: {e}")

            # Step 3: Save to DB
            _set_progress("Đang lưu vào database...", 0, 1)
            service.save_dataflows(dataflow_details)
            print(f"[CRAWL] Lưu xong {len(dataflow_details)} dataflows vào DB")

            # Step 4: Propagate execution state to output datasets
            _set_progress("Đang cập nhật trạng thái output datasets...", 0, 1)
            service.propagate_dataflow_status_to_datasets(dataflow_details)
            print(f"[CRAWL] Propagated dataflow status to output datasets")

            # Auto-alert nếu có FAILED
            _post_crawl_alert(db)

            _monitor_job["result"] = {
                "type": "crawl_dataflows",
                "summary": {"dataflows": {"total_crawled": len(dataflow_details)}},
                "checked_at": datetime.now().isoformat(),
            }
            db.close()
        except Exception as e:
            import traceback
            traceback.print_exc()
            _monitor_job["result"] = {"error": str(e)}
        finally:
            _monitor_job["running"] = False
            _monitor_job["progress"] = None

    thread = threading.Thread(target=run_crawl, daemon=True)
    thread.start()
    return {"status": "started", "message": "Đang cào dataflows..."}



@router.get("/status")
def get_check_status():
    """Xem trạng thái health check hiện tại / gần nhất."""
    if _monitor_job["running"]:
        return {
            "status": "running",
            "started_at": _monitor_job["started_at"],
            "progress": _monitor_job.get("progress"),
        }

    if _monitor_job["result"]:
        return {
            "status": "completed",
            "result": _monitor_job["result"],
        }

    return {"status": "idle", "message": "Chưa có health check nào chạy."}


@router.get("/datasets")
def list_datasets(
    provider_type: str = Query(default="", description="Filter theo provider type"),
    min_card_count: int = Query(default=0, description="Filter dataset >= N cards"),
    limit: int = Query(default=100, description="Số lượng tối đa"),
    offset: int = Query(default=0, description="Offset phân trang"),
):
    """Danh sách datasets trong DB + trạng thái cập nhật."""
    db = _get_db()

    where_clauses = ["1=1"]
    params = []

    if provider_type:
        where_clauses.append("LOWER(provider_type) = LOWER(%s)")
        params.append(provider_type)

    if min_card_count > 0:
        where_clauses.append("card_count >= %s")
        params.append(min_card_count)

    where_str = " AND ".join(where_clauses)

    # Count total
    total = db.query_one(
        f"SELECT COUNT(*) as cnt FROM datasets WHERE {where_str}", params
    )
    total_count = total["cnt"] if total else 0

    # Fetch rows
    params.extend([limit, offset])
    rows = db.query(
        f"""SELECT id, name, row_count, column_count, card_count, data_flow_count,
                   provider_type, stream_id, schedule_state, last_execution_state,
                   last_updated, updated_at
            FROM datasets
            WHERE {where_str}
            ORDER BY last_updated DESC NULLS LAST
            LIMIT %s OFFSET %s""",
        params,
    )

    db.close()

    return {
        "total": total_count,
        "datasets": rows,
    }


@router.get("/dataflows")
def list_dataflows(
    status_filter: str = Query(default="", description="Filter theo status (FAILED, SUCCESS...)"),
    limit: int = Query(default=100, description="Số lượng tối đa"),
    offset: int = Query(default=0, description="Offset phân trang"),
):
    """Danh sách dataflows trong DB + execution gần nhất."""
    db = _get_db()

    where_clauses = ["1=1"]
    params = []

    if status_filter:
        where_clauses.append("UPPER(last_execution_state) = UPPER(%s)")
        params.append(status_filter)

    where_str = " AND ".join(where_clauses)

    total = db.query_one(
        f"SELECT COUNT(*) as cnt FROM dataflows WHERE {where_str}", params
    )
    total_count = total["cnt"] if total else 0

    params.extend([limit, offset])
    rows = db.query(
        f"""SELECT id, name, status, paused, database_type,
                   last_execution_time, last_execution_state,
                   execution_count, owner, output_dataset_count, updated_at
            FROM dataflows
            WHERE {where_str}
            ORDER BY last_execution_time DESC NULLS LAST
            LIMIT %s OFFSET %s""",
        params,
    )

    db.close()

    return {
        "total": total_count,
        "dataflows": rows,
    }


@router.get("/provider-types")
def get_provider_types():
    """Lấy danh sách provider types duy nhất từ datasets đã cào."""
    db = _get_db()
    rows = db.query(
        """SELECT DISTINCT provider_type
           FROM datasets
           WHERE provider_type IS NOT NULL AND provider_type != ''
           ORDER BY provider_type""",
    )
    db.close()
    return {"provider_types": [r["provider_type"] for r in rows]}


@router.get("/datasets/{dataset_id}/schedule")
def get_dataset_schedule(dataset_id: str):
    """Lấy thông tin schedule của dataset qua Stream API.

    Cần stream_id — tra trong DB hoặc truyền qua query param.
    """
    auth = _get_auth()
    if not auth.is_valid:
        raise HTTPException(status_code=401, detail="Chưa login.")

    # Tìm stream_id từ DB
    db = _get_db()
    row = db.query_one(
        "SELECT stream_id FROM datasets WHERE id = %s", (dataset_id,)
    )

    stream_id = None
    if row and row.get("stream_id"):
        stream_id = row["stream_id"]

    if not stream_id:
        # Thử fetch detail để lấy stream_id
        api = DomoAPI(auth)
        service = MonitorService(api, db)
        detail = service.fetch_dataset_detail(dataset_id)
        if detail and detail.get("stream_id"):
            stream_id = detail["stream_id"]
        else:
            db.close()
            raise HTTPException(
                status_code=404,
                detail=f"Dataset {dataset_id} không có stream_id (có thể là DataFlow output)."
            )

    api = DomoAPI(auth)
    service = MonitorService(api, db)
    schedule = service.fetch_dataset_schedule(stream_id)
    db.close()

    if not schedule:
        raise HTTPException(status_code=404, detail="Không tìm thấy schedule info.")

    return schedule


@router.get("/dataflows/{dataflow_id}/executions")
def get_dataflow_executions(
    dataflow_id: str,
    limit: int = Query(default=100, description="Số lượng executions tối đa"),
    offset: int = Query(default=0, description="Offset phân trang"),
):
    """Lấy execution history của dataflow."""
    auth = _get_auth()
    if not auth.is_valid:
        raise HTTPException(status_code=401, detail="Chưa login.")

    db = _get_db()
    api = DomoAPI(auth)
    service = MonitorService(api, db)
    executions = service.fetch_dataflow_execution_history(dataflow_id, limit=limit, offset=offset)
    db.close()

    return {
        "dataflow_id": dataflow_id,
        "total": len(executions),
        "executions": executions,
    }


# ─── Auto-Check + Alerts ─────────────────────────────────

class AutoCheckRequest(BaseModel):
    """Cấu hình auto-check từ FE."""
    min_card_count: int = 40
    provider_type: str = "mysql-ssh"
    comment_ok: str = "【1次データ取得エラー確認結果】\nエラーがなかった旨\n\n【メインDataSetエラー確認結果】\nエラーがなかった旨"
    alert_email: str = ""
    schedule_enabled: bool = False
    schedule_hour: int = 8
    schedule_minute: int = 0
    schedule_days: str = "mon,tue,wed,thu,fri"


@router.post("/auto-check")
def trigger_auto_check(req: AutoCheckRequest):
    """Kiểm tra datasets trong DB → post Backlog nếu OK, gửi email nếu có lỗi.
    
    Dataset check: provider_type AND card_count >= N
    Dataflow check: tất cả dataflows
    """
    # Lưu config từ FE vào module-level để _post_crawl_alert dùng
    if req.alert_email:
        _alert_config["alert_email"] = req.alert_email
    _alert_config["min_card_count"] = req.min_card_count
    _alert_config["provider_type"] = req.provider_type

    settings = get_settings()
    db = _get_db()

    try:
        # ── 1. Query datasets đã crawl ──
        # Datasets: provider_type AND card_count >= N bị FAILED
        all_failed_ds = []
        if req.provider_type.strip():
            failed_ds = db.query(
                "SELECT id, name, provider_type, last_execution_state, card_count "
                "FROM datasets WHERE LOWER(provider_type) = LOWER(%s) "
                "AND card_count >= %s "
                "AND UPPER(COALESCE(last_execution_state, '')) LIKE 'FAILED%%'",
                (req.provider_type.strip(), req.min_card_count)
            )
            all_failed_ds = [dict(r) for r in (failed_ds or [])]

        # Dataflows bị FAILED
        failed_df = db.query(
            "SELECT id, name, last_execution_state "
            "FROM dataflows WHERE UPPER(COALESCE(last_execution_state, '')) LIKE 'FAILED%'"
        )
        all_failed_df = [dict(r) for r in (failed_df or [])]

        has_issues = len(all_failed_ds) > 0 or len(all_failed_df) > 0
        datasets_ok = len(all_failed_ds) == 0
        dataflows_failed = len(all_failed_df) > 0

        # ── 2. ALWAYS update alert state (for /alert page) ──
        _alert_data["checked_at"] = datetime.now().isoformat()
        _alert_data["all_ok"] = not has_issues
        _alert_data["failed_datasets"] = [dict(r) for r in all_failed_ds]
        _alert_data["failed_dataflows"] = all_failed_df
        print(f"[AUTO-CHECK] _alert_data updated: checked_at={_alert_data['checked_at']}, "
              f"all_ok={_alert_data['all_ok']}, "
              f"ds_fail={len(_alert_data['failed_datasets'])}, "
              f"df_fail={len(_alert_data['failed_dataflows'])}")

        result = {
            "checked_at": _alert_data["checked_at"],
            "all_ok": not has_issues,
            "failed_dataset_count": len(all_failed_ds),
            "failed_dataflow_count": len(all_failed_df),
            "backlog_posted": False,
            "email_sent": False,
        }

        # ── 3a. Datasets OK → Post comment to Backlog via REST API ──
        if datasets_ok and settings.backlog_issue_id and settings.backlog_api_key:
            try:
                api_key = settings.backlog_api_key
                issue_id = settings.backlog_issue_id
                base_url = settings.backlog_base_url

                # Update status → In Progress (statusId=2)
                patch_resp = http_requests.patch(
                    f"{base_url}/api/v2/issues/{issue_id}?apiKey={api_key}",
                    json={"statusId": 2},
                    headers={"Content-Type": "application/json"},
                    timeout=30,
                )
                print(f"[AUTO-CHECK] Backlog PATCH status → {patch_resp.status_code}")

                # Add comment
                if req.comment_ok:
                    comment_resp = http_requests.post(
                        f"{base_url}/api/v2/issues/{issue_id}/comments?apiKey={api_key}",
                        json={"content": req.comment_ok},
                        headers={"Content-Type": "application/json"},
                        timeout=30,
                    )
                    print(f"[AUTO-CHECK] Backlog POST comment → {comment_resp.status_code}")
                    result["backlog_posted"] = comment_resp.status_code < 400
                else:
                    result["backlog_posted"] = patch_resp.status_code < 400

            except Exception as e:
                print(f"[AUTO-CHECK] Backlog error: {e}")

        # ── 3b. Dataflows FAILED → Send email alert (independent of dataset status) ──
        if dataflows_failed and settings.gmail_email and settings.gmail_app_password and req.alert_email:
            subject = "【Domo監視】DataFlowエラー検出"
            body_lines = ["DomoデータフローでFAILEDが検出されました。\n"]

            body_lines.append(f"■ エラーDataFlow ({len(all_failed_df)}件):")
            for df in all_failed_df:
                body_lines.append(f"  - {df.get('name', '?')} (ID: {df.get('id', '?')}, Status: {df.get('last_execution_state', '?')})")

            if all_failed_ds:
                body_lines.append(f"\n■ エラーDataSet ({len(all_failed_ds)}件):")
                for ds in all_failed_ds:
                    body_lines.append(f"  - {ds.get('name', '?')} (ID: {ds.get('id', '?')}, Cards: {ds.get('card_count', 0)})")

            body_lines.append(f"\n確認時刻: {_alert_data['checked_at']}")
            body = "\n".join(body_lines)

            result["email_sent"] = send_alert_email(
                subject=subject,
                body=body,
                to_email=req.alert_email,
                from_email=settings.gmail_email,
                app_password=settings.gmail_app_password,
            )
            print(f"[AUTO-CHECK] Email sent={result['email_sent']} to={req.alert_email}")

        # ── 3c. Datasets FAILED → Also send email ──
        if len(all_failed_ds) > 0 and settings.gmail_email and settings.gmail_app_password and req.alert_email:
            subject = "【Domo監視】DataSetエラー検出"
            body_lines = ["DomoデータセットでFAILEDが検出されました。\n"]

            body_lines.append(f"■ エラーDataSet ({len(all_failed_ds)}件):")
            for ds in all_failed_ds:
                body_lines.append(
                    f"  - {ds.get('name', '?')} (ID: {ds.get('id', '?')}, "
                    f"Type: {ds.get('provider_type', '?')}, Cards: {ds.get('card_count', 0)})"
                )

            body_lines.append(f"\n確認時刻: {_alert_data['checked_at']}")
            body = "\n".join(body_lines)

            send_alert_email(
                subject=subject,
                body=body,
                to_email=req.alert_email,
                from_email=settings.gmail_email,
                app_password=settings.gmail_app_password,
            )

        db.close()
        return result

    except Exception as e:
        db.close()
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/alerts")
def get_alerts():
    """Trả về danh sách datasets/dataflows có vấn đề."""
    print(f"[GET /alerts] checked_at={_alert_data.get('checked_at')}, "
          f"ds={len(_alert_data.get('failed_datasets', []))}, "
          f"df={len(_alert_data.get('failed_dataflows', []))}")
    # Nếu in-memory trống, query DB trực tiếp
    if not _alert_data.get("checked_at"):
        try:
            db = _get_db()
            failed_ds = db.query(
                "SELECT id, name, provider_type, last_execution_state, card_count "
                "FROM datasets WHERE UPPER(COALESCE(last_execution_state, '')) LIKE 'FAILED%'"
            )
            failed_df = db.query(
                "SELECT id, name, last_execution_state "
                "FROM dataflows WHERE UPPER(COALESCE(last_execution_state, '')) LIKE 'FAILED%'"
            )
            all_failed_ds = [dict(r) for r in (failed_ds or [])]
            all_failed_df = [dict(r) for r in (failed_df or [])]
            _alert_data["checked_at"] = datetime.now().isoformat()
            _alert_data["all_ok"] = len(all_failed_ds) == 0 and len(all_failed_df) == 0
            _alert_data["failed_datasets"] = all_failed_ds
            _alert_data["failed_dataflows"] = all_failed_df
            db.close()
            print(f"[GET /alerts] DB fallback: ds={len(all_failed_ds)}, df={len(all_failed_df)}")
        except Exception as e:
            print(f"[ALERTS] DB query error: {e}")
            import traceback
            traceback.print_exc()
    return _alert_data


@router.post("/save-alert-config")
def save_alert_config_endpoint(req: AutoCheckRequest):
    """Lưu cấu hình alert email + schedule vào DB."""
    _alert_config["alert_email"] = req.alert_email
    _alert_config["min_card_count"] = req.min_card_count
    _alert_config["provider_type"] = req.provider_type
    _alert_config["schedule_enabled"] = req.schedule_enabled
    _alert_config["schedule_hour"] = req.schedule_hour
    _alert_config["schedule_minute"] = req.schedule_minute
    _alert_config["schedule_days"] = req.schedule_days
    _save_alert_config(_alert_config)

    # Update scheduler
    try:
        from app.scheduler import update_schedule
        update_schedule(_alert_config)
    except Exception as e:
        print(f"[SAVE-CONFIG] Scheduler update error: {e}")

    return {"saved": True, "config": _alert_config}


@router.get("/auto-check-config")
def get_auto_check_config():
    """Trả về config cho FE Settings."""
    settings = get_settings()
    # Detect cookie from JSON file
    try:
        from app.routers.backlog import _load_backlog_cookies
        cookie_str, _ = _load_backlog_cookies()
        has_cookie = bool(cookie_str)
    except Exception:
        has_cookie = False
    return {
        "backlog_base_url": settings.backlog_base_url,
        "backlog_issue_id": settings.backlog_issue_id,
        "has_backlog_cookie": has_cookie,
        "alert_email_to": _alert_config.get("alert_email", ""),
        "min_card_count": _alert_config.get("min_card_count", 40),
        "provider_type": _alert_config.get("provider_type", "mysql-ssh"),
        "has_gmail": bool(settings.gmail_email and settings.gmail_app_password),
        "schedule_enabled": _alert_config.get("schedule_enabled", False),
        "schedule_hour": _alert_config.get("schedule_hour", 8),
        "schedule_minute": _alert_config.get("schedule_minute", 0),
        "schedule_days": _alert_config.get("schedule_days", "mon,tue,wed,thu,fri"),
    }
