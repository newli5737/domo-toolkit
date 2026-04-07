import json
import concurrent.futures
from datetime import datetime, timezone, timedelta
from app.core.api import DomoAPI
from sqlalchemy.orm import Session
from app.core.logger import DomoLogger

from app.services.dataset_service import DatasetCrawlService
from app.services.dataflow_service import DataflowCrawlService

log = DomoLogger("monitor-health")
JST = timezone(timedelta(hours=9))

class HealthCheckService:
    def __init__(self, api: DomoAPI, db: Session, dataset_service: DatasetCrawlService, dataflow_service: DataflowCrawlService):
        self.api = api
        self.db = db
        self.dataset_service = dataset_service
        self.dataflow_service = dataflow_service

    def check_health(
        self,
        stale_hours: int = 24,
        min_card_count: int = 0,
        provider_type: str = "",
        min_dataflow_count: int = 0,
        max_workers: int = 10,
    ) -> dict:
        now = datetime.now(tz=timezone.utc)
        stale_threshold = now - timedelta(hours=stale_hours)
        filters = {
            "stale_hours": stale_hours,
            "min_card_count": min_card_count,
            "provider_type": provider_type,
            "min_dataflow_count": min_dataflow_count,
        }

        # ─── Phase 1: Crawl & save datasets ──────────────────
        log.info("Phase 1: Crawl datasets...")
        raw_datasets = self.dataset_service.crawl_all_datasets()

        dataset_details_map = {}
        ds_ids = [str(ds.get("dataSourceId", ds.get("id", ""))) for ds in raw_datasets]
        log.info(f"  Fetching detail cho {len(ds_ids)} datasets ({max_workers} workers)...")

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self.dataset_service.fetch_dataset_detail, ds_id): ds_id
                for ds_id in ds_ids
            }
            done = 0
            for future in concurrent.futures.as_completed(futures):
                done += 1
                if done % 50 == 0:
                    log.info(f"  Dataset detail: {done}/{len(ds_ids)}")
                try:
                    result = future.result()
                    if result:
                        dataset_details_map[result["id"]] = result
                except Exception as e:
                    log.error(f"  Fetch dataset detail lỗi: {e}")

        final_datasets = []
        exec_states = {}
        for search_ds in raw_datasets:
            ds_id = search_ds["id"]
            dd = dataset_details_map.get(ds_id)
            
            if dd:
                for k, v in dd.items():
                    if v is not None and v != "":
                        search_ds[k] = v

            search_pt = search_ds.get("provider_type", "")
            if search_pt and search_pt not in ("STANDARD", "STANDARD_OAUTH"):
                pass
            else:
                search_ds["provider_type"] = dd.get("provider_type", "") if dd else ""
            
            if not search_ds.get("schedule_state") and dd:
                sa = dd.get("schedule_active")
                if sa is True:
                    search_ds["schedule_state"] = "ACTIVE"
                elif sa is False:
                    search_ds["schedule_state"] = "INACTIVE"

            search_state = search_ds.get("state", "") or search_ds.get("status", "")
            if search_state.upper() == "INACTIVE":
                search_ds["dataset_status"] = "DISABLED"
            else:
                search_ds["dataset_status"] = search_state

            if not search_ds.get("last_execution_state") and dd:
                detail_exec = dd.get("status", "")
                if detail_exec:
                    search_ds["last_execution_state"] = detail_exec

            final_datasets.append(search_ds)
            st = search_ds.get("last_execution_state", "")
            exec_states[st] = exec_states.get(st, 0) + 1

        log.info(f"  [DEBUG] last_execution_state distribution: {exec_states}")

        self.dataset_service.save_datasets(final_datasets)
        dataset_details = final_datasets

        # ─── Phase 2: Crawl & save dataflows ──────────────────
        log.info("Phase 2: Crawl dataflows...")
        raw_dataflows = self.dataflow_service.crawl_all_dataflows()

        dataflow_details = []
        log.info(f"  Fetching executions cho {len(raw_dataflows)} dataflows ({max_workers} workers)...")

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self.dataflow_service.process_dataflow, df): df
                for df in raw_dataflows
            }
            done = 0
            for future in concurrent.futures.as_completed(futures):
                done += 1
                if done % 50 == 0:
                    log.info(f"  Dataflow detail: {done}/{len(raw_dataflows)}")
                try:
                    result = future.result()
                    if result:
                        dataflow_details.append(result)
                except Exception as e:
                    log.error(f"  Process dataflow lỗi: {e}")

        self.dataflow_service.save_dataflows(dataflow_details)
        self.dataflow_service.propagate_dataflow_status_to_datasets(dataflow_details)

        # ─── Phase 3: Phân tích kết quả ──────────────────────
        log.info("Phase 3: Phân tích...")

        ds_alerts = []
        ds_ok = 0
        ds_stale = 0
        ds_checked = 0

        for ds in dataset_details:
            if min_card_count > 0 and (ds.get("card_count") or 0) < min_card_count:
                continue
            if provider_type and ds.get("provider_type", "").lower() != provider_type.lower():
                continue
            if min_dataflow_count > 0 and (ds.get("data_flow_count") or 0) < min_dataflow_count:
                continue

            ds_checked += 1
            last_updated = ds.get("last_updated")

            if last_updated:
                try:
                    ts = last_updated
                    if isinstance(ts, (int, float)):
                        ts_sec = ts / 1000.0 if ts > 10_000_000_000 else ts
                        last_dt = datetime.fromtimestamp(ts_sec, tz=timezone.utc)
                    else:
                        last_dt = None

                    if last_dt and last_dt < stale_threshold:
                        ds_stale += 1
                        hours_ago = int((now - last_dt).total_seconds() / 3600)
                        ds_alerts.append({
                            "type": "dataset",
                            "status": "stale",
                            "id": ds["id"],
                            "name": ds.get("name", ""),
                            "provider_type": ds.get("provider_type", ""),
                            "card_count": ds.get("card_count", 0),
                            "last_updated": last_dt.astimezone(JST).strftime("%Y-%m-%d %H:%M JST"),
                            "hours_ago": hours_ago,
                        })
                    else:
                        ds_ok += 1
                except Exception:
                    ds_ok += 1
            else:
                ds_stale += 1
                ds_alerts.append({
                    "type": "dataset",
                    "status": "no_update",
                    "id": ds["id"],
                    "name": ds.get("name", ""),
                    "provider_type": ds.get("provider_type", ""),
                    "card_count": ds.get("card_count", 0),
                    "last_updated": None,
                    "hours_ago": None,
                })

        df_alerts = []
        df_ok = 0
        df_failed = 0
        df_stale = 0
        df_checked = len(dataflow_details)

        for df in dataflow_details:
            last_state = df.get("last_execution_state", "")
            df_status = df.get("status", "")
            last_time = df.get("last_execution_time")

            is_disabled = df_status.upper() == "DISABLED" if df_status else False
            is_failed = not is_disabled and (
                (last_state and "FAILED" in last_state.upper()) or
                (df_status and "FAILED" in df_status.upper())
            )

            if is_failed:
                df_failed += 1
                df_alerts.append({
                    "type": "dataflow",
                    "status": "failed",
                    "id": df["id"],
                    "name": df.get("name", ""),
                    "database_type": df.get("database_type", ""),
                    "last_execution_state": last_state or df_status,
                    "last_execution_time": (
                        last_time.astimezone(JST).strftime("%Y-%m-%d %H:%M JST")
                        if last_time else None
                    ),
                })
            elif last_time and last_time < stale_threshold:
                df_stale += 1
                hours_ago = int((now - last_time).total_seconds() / 3600)
                df_alerts.append({
                    "type": "dataflow",
                    "status": "stale",
                    "id": df["id"],
                    "name": df.get("name", ""),
                    "database_type": df.get("database_type", ""),
                    "last_execution_state": last_state,
                    "last_execution_time": (
                        last_time.astimezone(JST).strftime("%Y-%m-%d %H:%M JST")
                        if last_time else None
                    ),
                    "hours_ago": hours_ago,
                })
            else:
                df_ok += 1

        all_alerts = ds_alerts + df_alerts
        summary = {
            "datasets": {
                "total_crawled": len(dataset_details),
                "checked": ds_checked,
                "ok": ds_ok,
                "stale": ds_stale,
            },
            "dataflows": {
                "total_crawled": len(dataflow_details),
                "checked": df_checked,
                "ok": df_ok,
                "failed": df_failed,
                "stale": df_stale,
            },
            "total_alerts": len(all_alerts),
        }

        self.db.execute(
            """INSERT INTO monitor_checks
               (check_type, total_checked, failed_count, stale_count, ok_count, filters_json, details_json)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            ("dataset", ds_checked, 0, ds_stale, ds_ok,
             json.dumps(filters, ensure_ascii=False),
             json.dumps(ds_alerts, ensure_ascii=False, default=str)),
        )

        self.db.execute(
            """INSERT INTO monitor_checks
               (check_type, total_checked, failed_count, stale_count, ok_count, filters_json, details_json)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            ("dataflow", df_checked, df_failed, df_stale, df_ok,
             json.dumps(filters, ensure_ascii=False),
             json.dumps(df_alerts, ensure_ascii=False, default=str)),
        )

        log.info(f"Health check hoàn thành: {len(all_alerts)} alerts")
        log.info(f"  Datasets: {ds_checked} checked, {ds_ok} OK, {ds_stale} stale")
        log.info(f"  Dataflows: {df_checked} checked, {df_ok} OK, {df_failed} failed, {df_stale} stale")

        return {
            "summary": summary,
            "alerts": all_alerts,
            "filters": filters,
            "checked_at": now.astimezone(JST).strftime("%Y-%m-%d %H:%M JST"),
        }
