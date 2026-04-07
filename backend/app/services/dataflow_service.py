import time
from datetime import datetime, timezone
from app.core.api import DomoAPI
from sqlalchemy.orm import Session
from app.core.logger import DomoLogger

log = DomoLogger("monitor-dataflow")

class DataflowCrawlService:
    SEARCH_URL = "/api/search/v1/query"
    DATAFLOW_DETAIL_URL = "/api/dataprocessing/v2/dataflows/{}?hydrationState=VISUALIZATION&validationType=SAVE"
    EXECUTIONS_URL = "/api/dataprocessing/v1/dataflows/{}/executions?limit=10&offset=0"
    EXECUTIONS_HISTORY_URL = "/api/dataprocessing/v1/dataflows/{}/executions?limit={}&offset={}"
    PAGE_SIZE = 30

    def __init__(self, api: DomoAPI, db: Session):
        self.api = api
        self.db = db

    def fetch_dataflow_execution_history(self, df_id: int | str, limit: int = 100, offset: int = 0) -> list[dict]:
        """Lấy execution history của dataflow."""
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
                "error_messages": error_messages[:5],
                "dataflow_version": ex.get("dataFlowVersion"),
            })

        return executions

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

            if len(dataflows) < self.PAGE_SIZE and (
                total_expected is None or len(all_dataflows) >= total_expected
            ):
                break

            if total_expected and len(all_dataflows) >= total_expected:
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

        if status or last_exec_state:
            has_failed = ("FAILED" in (status or "").upper()) or ("FAILED" in (last_exec_state or "").upper())
            if has_failed:
                log.info(f"  [DEBUG-DF] id={df_id}, name={df_name[:40]}, "
                         f"search_status='{status}', exec_state='{last_exec_state}', "
                         f"exec_count={len(executions)}")

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
            from app.models.dataset import Dataflow
            for df in dataflows:
                row = {k: v for k, v in df.items() if k != "output_dataset_ids"}
                self.db.merge(Dataflow(**row))
            self.db.commit()
            log.info(f"Lưu {len(dataflows)} dataflows vào DB")

    def propagate_dataflow_status_to_datasets(self, dataflows: list[dict]):
        """Cập nhật last_execution_state cho các output datasets."""
        from sqlalchemy import update
        from app.models.dataset import Dataset
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
                            update(Dataset)
                            .where(Dataset.id == str(ds_id))
                            .values(last_execution_state=state)
                        )
                        updated += 1
                    except Exception:
                        pass
        self.db.commit()
        log.info(f"Propagated execution state to {updated} output datasets")
