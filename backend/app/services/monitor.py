"""MonitorService — Crawl datasets & dataflows, kiểm tra lần cập nhật gần nhất."""

import json
import time
import concurrent.futures
from datetime import datetime, timezone, timedelta
from app.core.api import DomoAPI
from app.core.db import DomoDatabase
from app.core.logger import DomoLogger

log = DomoLogger("monitor")

JST = timezone(timedelta(hours=9))


class MonitorService:
    """Crawl và giám sát trạng thái cập nhật datasets & dataflows."""

    SEARCH_URL = "/api/search/v1/query"
    DATASOURCE_DETAIL_URL = "/api/data/v3/datasources/{}?includeAllDetails=true&includePrivate=true"
    DATAFLOW_DETAIL_URL = "/api/dataprocessing/v2/dataflows/{}?hydrationState=VISUALIZATION&validationType=SAVE"
    EXECUTIONS_URL = "/api/dataprocessing/v1/dataflows/{}/executions?limit=10&offset=0"
    EXECUTIONS_HISTORY_URL = "/api/dataprocessing/v1/dataflows/{}/executions?limit={}&offset={}"
    STREAM_DETAIL_URL = "/api/data/v1/streams/{}?fields=all"

    PAGE_SIZE = 30

    def __init__(self, api: DomoAPI, db: DomoDatabase):
        self.api = api
        self.db = db

    # ─── Crawl Datasets ───────────────────────────────────────

    DATASOURCES_SEARCH_URL = "/api/data/ui/v3/datasources/search"

    def crawl_all_datasets(self, progress_callback=None) -> list[dict]:
        """Search toàn bộ datasets qua /api/data/ui/v3/datasources/search."""
        log.info("Bắt đầu crawl datasets...")

        all_datasets = []
        offset = 0
        total_expected = None

        while True:
            payload = {
                "entities": ["DATASET"],
                "filters": [],
                "combineResults": True,
                "query": "*",
                "count": self.PAGE_SIZE,
                "offset": offset,
                "sort": {
                    "isRelevance": False,
                    "fieldSorts": [{"field": "card_count", "sortOrder": "DESC"}]
                }
            }

            resp = self.api.post(self.DATASOURCES_SEARCH_URL, json=payload)

            if not resp or resp.status_code != 200:
                log.error(f"Search datasets thất bại tại offset={offset}")
                break

            data = resp.json()
            datasets = data.get("dataSources", [])

            if total_expected is None:
                meta = data.get("_metaData", {})
                total_expected = meta.get("totalCount", 0)
                log.info(f"  Tổng datasets: {total_expected}")

            if not datasets:
                break

            # Parse fields từ response
            for i, ds in enumerate(datasets):
                card_info = ds.get("cardInfo", {})

                # Determine schedule state
                schedule_active = ds.get("scheduleActive")
                if schedule_active is True:
                    sched_state = "ACTIVE"
                elif schedule_active is False:
                    sched_state = "INACTIVE"
                else:
                    sched_state = ""

                all_datasets.append({
                    "id": str(ds.get("id", "")),
                    "name": ds.get("name", ""),
                    "row_count": ds.get("rowCount", 0),
                    "column_count": ds.get("columnCount", 0),
                    "card_count": card_info.get("cardCount", 0) if isinstance(card_info, dict) else 0,
                    "data_flow_count": ds.get("dataFlowCount", 0),
                    "provider_type": ds.get("dataProviderType", ds.get("type", "")),
                    "stream_id": str(ds.get("streamId", "")) if ds.get("streamId") else "",
                    "schedule_state": sched_state,
                    "last_updated": ds.get("lastUpdated"),
                })

            if progress_callback:
                progress_callback(len(all_datasets), total_expected)

            log.progress(len(all_datasets), total_expected, "Crawl Datasets")

            if len(datasets) < self.PAGE_SIZE:
                break

            offset += self.PAGE_SIZE
            time.sleep(0.3)

        log.info(f"Crawl datasets xong: {len(all_datasets)}")
        return all_datasets

    def fetch_dataset_detail(self, dataset_id: str) -> dict | None:
        """Lấy chi tiết 1 dataset qua /api/data/v3/datasources/{id}.
        Trả về đầy đủ fields cho cả health check lẫn schedule viewer.
        """
        url = self.DATASOURCE_DETAIL_URL.format(dataset_id)
        resp = self.api.get(url)
        if not resp or resp.status_code != 200:
            return None

        data = resp.json()
        return {
            "id": str(data.get("id", dataset_id)),
            "name": data.get("name", ""),
            "row_count": data.get("rowCount", 0),
            "column_count": data.get("columnCount", 0),
            "card_count": data.get("cardCount", 0),
            "data_flow_count": data.get("dataFlowCount", 0),
            "provider_type": (
                data.get("dataProvider", {}).get("type", "")
                if isinstance(data.get("dataProvider"), dict) else ""
            ),
            "stream_id": data.get("streamId", ""),
            "schedule_active": data.get("scheduleActive"),
            "status": data.get("status"),
            "last_updated": data.get("lastUpdated"),
        }

    def save_datasets(self, datasets: list[dict]):
        """Lưu datasets vào DB."""
        rows = []
        for ds in datasets:
            last_updated = ds.get("last_updated")
            last_updated_ts = None
            if last_updated:
                try:
                    if isinstance(last_updated, (int, float)):
                        ts = last_updated / 1000.0 if last_updated > 10_000_000_000 else last_updated
                        last_updated_ts = datetime.fromtimestamp(ts, tz=timezone.utc)
                except Exception:
                    pass

            rows.append({
                "id": str(ds["id"]),
                "name": ds.get("name", ""),
                "row_count": ds.get("row_count", 0),
                "column_count": ds.get("column_count", 0),
                "card_count": ds.get("card_count", 0),
                "data_flow_count": ds.get("data_flow_count", 0),
                "provider_type": ds.get("provider_type", ""),
                "stream_id": ds.get("stream_id", ""),
                "schedule_state": ds.get("schedule_state", ""),
                "last_execution_state": ds.get("last_execution_state", ""),
                "last_updated": last_updated_ts,
                "updated_at": datetime.now(),
            })

        if rows:
            self.db.bulk_upsert("datasets", rows, "id")
            log.info(f"Lưu {len(rows)} datasets vào DB")

    # ─── Schedule / Execution History ─────────────────────────

    def fetch_dataset_schedule(self, stream_id: int | str) -> dict | None:
        """Lấy thông tin schedule của dataset qua Stream API.
        GET /api/data/v1/streams/{streamId}?fields=all
        """
        url = self.STREAM_DETAIL_URL.format(stream_id)
        resp = self.api.get(url)
        if not resp or resp.status_code != 200:
            log.error(f"Fetch stream {stream_id} thất bại: {resp.status_code if resp else 'None'}")
            return None

        data = resp.json()
        ds = data.get("dataSource", {})
        last_exec = data.get("lastExecution", {})
        last_success = data.get("lastSuccessfulExecution", {})
        current_exec = data.get("currentExecution")

        # Parse schedule
        result = {
            "stream_id": data.get("id"),
            "dataset_id": ds.get("id"),
            "dataset_name": ds.get("name"),
            "dataset_type": ds.get("dataProviderType", ds.get("type", "")),
            "dataset_status": ds.get("status"),
            "row_count": ds.get("rowCount", 0),
            "column_count": ds.get("columnCount", 0),
            "schedule_expression": data.get("scheduleExpression"),
            "schedule_state": data.get("scheduleState"),
            "update_method": data.get("updateMethod"),
            "owner": ds.get("owner", {}).get("name") if isinstance(ds.get("owner"), dict) else None,
            "last_execution": {
                "execution_id": last_exec.get("executionId"),
                "state": last_exec.get("currentState"),
                "started_at": last_exec.get("startedAt"),
                "ended_at": last_exec.get("endedAt"),
                "rows_inserted": last_exec.get("rowsInserted"),
                "bytes_inserted": last_exec.get("bytesInserted"),
                "errors": last_exec.get("errors", []),
            } if last_exec else None,
            "last_successful_execution": {
                "execution_id": last_success.get("executionId"),
                "state": last_success.get("currentState"),
                "started_at": last_success.get("startedAt"),
                "ended_at": last_success.get("endedAt"),
                "rows_inserted": last_success.get("rowsInserted"),
            } if last_success else None,
            "current_execution_state": data.get("currentExecutionState"),
            "is_running": current_exec is not None,
            "tags": ds.get("tags"),
            "schedule_active": ds.get("scheduleActive"),
        }
        return result

    def fetch_dataflow_execution_history(
        self, df_id: int | str, limit: int = 100, offset: int = 0
    ) -> list[dict]:
        """Lấy execution history của dataflow.
        GET /api/dataprocessing/v1/dataflows/{id}/executions?limit=N&offset=M
        """
        url = self.EXECUTIONS_HISTORY_URL.format(df_id, limit, offset)
        resp = self.api.get(url)
        if not resp or resp.status_code != 200:
            log.error(f"Fetch executions cho dataflow {df_id} thất bại")
            return []

        try:
            raw = resp.json()
        except Exception:
            return []

        if not isinstance(raw, list):
            return []

        executions = []
        for ex in raw:
            errors = ex.get("errors", [])
            error_messages = []
            for err in errors:
                msg = err.get("localizedMessage") or err.get("message") or ""
                if msg:
                    error_messages.append(msg)

            begin_time = ex.get("beginTime")
            end_time = ex.get("endTime")

            executions.append({
                "id": ex.get("id"),
                "state": ex.get("state"),
                "failed": ex.get("failed", False),
                "activation_type": ex.get("activationType"),
                "begin_time": begin_time,
                "end_time": end_time,
                "duration_seconds": (
                    (end_time - begin_time) / 1000.0
                    if begin_time and end_time else None
                ),
                "total_rows_read": ex.get("totalRowsRead", 0),
                "total_bytes_read": ex.get("totalBytesRead", 0),
                "total_rows_written": ex.get("totalRowsWritten", 0),
                "total_bytes_written": ex.get("totalBytesWritten", 0),
                "input_count": len(ex.get("inputDataSources", [])),
                "output_count": len(ex.get("outputDataSources", [])),
                "error_count": len(errors),
                "error_messages": error_messages[:5],  # Limit to 5 error messages
                "dataflow_version": ex.get("dataFlowVersion"),
            })

        return executions

    # ─── Crawl Dataflows ──────────────────────────────────────

    def crawl_all_dataflows(self, progress_callback=None) -> list[dict]:
        """Search toàn bộ dataflows."""
        log.info("Bắt đầu crawl dataflows...")

        all_dataflows = []
        offset = 0
        total_expected = None

        while True:
            payload = {
                "entities": ["DATAFLOW"],
                "filters": [],
                "combineResults": True,
                "query": "*",
                "count": self.PAGE_SIZE,
                "offset": offset,
                "sort": {
                    "isRelevance": False,
                    "fieldSorts": [{"field": "create_date", "sortOrder": "DESC"}]
                }
            }

            resp = self.api.post(self.SEARCH_URL, json=payload)
            if not resp or resp.status_code != 200:
                log.error(f"Search dataflows thất bại tại offset={offset}")
                break

            data = resp.json()
            dataflows = data.get("searchObjects", [])

            if total_expected is None:
                total_expected = data.get("totalResultCount", 0)
                log.info(f"  Tổng dataflows: {total_expected}")

            if not dataflows:
                break

            all_dataflows.extend(dataflows)

            if progress_callback:
                progress_callback(len(all_dataflows), total_expected)

            log.progress(len(all_dataflows), total_expected, "Crawl Dataflows")

            if len(dataflows) < self.PAGE_SIZE:
                break

            offset += self.PAGE_SIZE
            time.sleep(0.5)

        log.info(f"Crawl dataflows xong: {len(all_dataflows)}")
        return all_dataflows

    def fetch_dataflow_executions(self, df_id: str) -> list[dict]:
        """Lấy execution history gần nhất của 1 dataflow."""
        url = self.EXECUTIONS_URL.format(df_id)
        resp = self.api.get(url)
        if not resp or resp.status_code != 200:
            return []
        try:
            return resp.json()
        except Exception:
            return []

    def process_dataflow(self, df_stub: dict) -> dict | None:
        """Xử lý 1 dataflow từ search result: lấy execution gần nhất."""
        df_id = str(df_stub.get("databaseId", ""))
        df_name = df_stub.get("name", "")
        owner = df_stub.get("ownedByName", "")
        status = df_stub.get("status", "")
        paused = df_stub.get("paused", False)
        output_ds = df_stub.get("outputDatasets", [])

        # Lấy execution history
        executions = self.fetch_dataflow_executions(df_id)

        last_exec_time = None
        last_exec_state = None
        if executions:
            latest = executions[0]
            begin_time = latest.get("beginTime")
            if begin_time:
                try:
                    last_exec_time = datetime.fromtimestamp(
                        begin_time / 1000.0, tz=timezone.utc
                    )
                except Exception:
                    pass
            last_exec_state = latest.get("state", "UNKNOWN")

        return {
            "id": df_id,
            "name": df_name,
            "status": status,
            "paused": paused,
            "database_type": df_stub.get("dataFlowType", ""),
            "last_execution_time": last_exec_time,
            "last_execution_state": last_exec_state,
            "execution_count": df_stub.get("executionCount", 0),
            "owner": owner,
            "output_dataset_count": len(output_ds),
            "output_dataset_ids": [str(d.get("dataSourceId", d.get("id", ""))) for d in output_ds] if output_ds else [],
            "updated_at": datetime.now(),
        }

    def save_dataflows(self, dataflows: list[dict]):
        """Lưu dataflows vào DB (bỏ output_dataset_ids trước khi insert)."""
        if dataflows:
            rows = []
            for df in dataflows:
                row = {k: v for k, v in df.items() if k != "output_dataset_ids"}
                rows.append(row)
            self.db.bulk_upsert("dataflows", rows, "id")
            log.info(f"Lưu {len(rows)} dataflows vào DB")

    def propagate_dataflow_status_to_datasets(self, dataflows: list[dict]):
        """Cập nhật last_execution_state cho các output datasets dựa trên trạng thái dataflow."""
        updated = 0
        for df in dataflows:
            state = df.get("last_execution_state", "")
            output_ids = df.get("output_dataset_ids", [])
            if not state or not output_ids:
                continue
            for ds_id in output_ids:
                if ds_id:
                    try:
                        self.db.execute(
                            "UPDATE datasets SET last_execution_state = %s WHERE id = %s",
                            (state, str(ds_id)),
                        )
                        updated += 1
                    except Exception:
                        pass
        log.info(f"Propagated execution state to {updated} output datasets")

    # ─── Health Check ─────────────────────────────────────────

    def check_health(
        self,
        stale_hours: int = 24,
        min_card_count: int = 0,
        provider_type: str = "",
        min_dataflow_count: int = 0,
        max_workers: int = 10,
    ) -> dict:
        """
        Entry point: crawl datasets + dataflows → phân tích → lưu kết quả.

        Filters:
        - stale_hours: dataset/dataflow coi là stale nếu không update quá N giờ
        - min_card_count: chỉ check dataset được dùng bởi >= N cards
        - provider_type: chỉ check dataset có provider type chỉ định (mysql, etc)
        - min_dataflow_count: chỉ check dataset liên kết >= N dataflows
        """
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
        raw_datasets = self.crawl_all_datasets()

        # Fetch detail in parallel
        dataset_details = []
        ds_ids = [str(ds.get("dataSourceId", ds.get("id", ""))) for ds in raw_datasets]
        log.info(f"  Fetching detail cho {len(ds_ids)} datasets ({max_workers} workers)...")

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self.fetch_dataset_detail, ds_id): ds_id
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
                        dataset_details.append(result)
                except Exception as e:
                    log.error(f"  Fetch dataset detail lỗi: {e}")

        self.save_datasets(dataset_details)

        # ─── Phase 2: Crawl & save dataflows ──────────────────
        log.info("Phase 2: Crawl dataflows...")
        raw_dataflows = self.crawl_all_dataflows()

        dataflow_details = []
        log.info(f"  Fetching executions cho {len(raw_dataflows)} dataflows ({max_workers} workers)...")

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self.process_dataflow, df): df
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

        self.save_dataflows(dataflow_details)

        # ─── Phase 3: Phân tích kết quả ──────────────────────
        log.info("Phase 3: Phân tích...")

        # Dataset analysis
        ds_alerts = []
        ds_ok = 0
        ds_stale = 0
        ds_checked = 0

        for ds in dataset_details:
            # Apply filters
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

        # Dataflow analysis
        df_alerts = []
        df_ok = 0
        df_failed = 0
        df_stale = 0
        df_checked = len(dataflow_details)

        for df in dataflow_details:
            last_state = df.get("last_execution_state", "")
            last_time = df.get("last_execution_time")

            if last_state == "FAILED":
                df_failed += 1
                df_alerts.append({
                    "type": "dataflow",
                    "status": "failed",
                    "id": df["id"],
                    "name": df.get("name", ""),
                    "database_type": df.get("database_type", ""),
                    "last_execution_state": last_state,
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

        # ─── Lưu kết quả check vào DB ────────────────────────
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

        # Lưu dataset check
        self.db.execute(
            """INSERT INTO monitor_checks
               (check_type, total_checked, failed_count, stale_count, ok_count, filters_json, details_json)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            ("dataset", ds_checked, 0, ds_stale, ds_ok,
             json.dumps(filters, ensure_ascii=False),
             json.dumps(ds_alerts, ensure_ascii=False, default=str)),
        )

        # Lưu dataflow check
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
