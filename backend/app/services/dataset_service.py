import time
from datetime import datetime, timezone
from app.core.api import DomoAPI
from sqlalchemy.orm import Session
from app.core.logger import DomoLogger

log = DomoLogger("monitor-dataset")

class DatasetCrawlService:
    DATASOURCES_SEARCH_URL = "/api/data/ui/v3/datasources/search"
    DATASOURCE_DETAIL_URL = "/api/data/v3/datasources/{}?includeAllDetails=true&includePrivate=true"
    STREAM_DETAIL_URL = "/api/data/v1/streams/{}?fields=all"
    PAGE_SIZE = 30
    _detail_debug_count = 0

    def __init__(self, api: DomoAPI, db: Session):
        self.api = api
        self.db = db

    def crawl_all_datasets(self, progress_callback=None) -> list[dict]:
        """Search toàn bộ datasets qua /api/data/ui/v3/datasources/search."""
        log.info("Bắt đầu crawl datasets...")
        DatasetCrawlService._detail_debug_count = 0  # Reset debug counter

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
                    "fieldSorts": [
                        {
                            "field": "card_count",
                            "sortOrder": "DESC"
                        }
                    ]
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

            for i, ds in enumerate(datasets):
                card_info = ds.get("cardInfo", {})

                if offset == 0 and i < 3:
                    type_keys = {k: v for k, v in ds.items()
                                 if 'type' in k.lower() or 'provider' in k.lower() or 'transport' in k.lower()}
                    log.info(f"  [DEBUG-SEARCH] Dataset #{i}: name={ds.get('name','')[:40]}, type_fields={type_keys}")

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
                    "status": ds.get("status", ""),
                    "state": ds.get("state", ""),
                    "last_updated": ds.get("lastUpdated"),
                })

            if progress_callback:
                progress_callback(len(all_datasets), total_expected)

            log.progress(len(all_datasets), total_expected, "Crawl Datasets")

            if not datasets:
                break

            if total_expected and len(all_datasets) >= total_expected:
                break

            if len(datasets) < self.PAGE_SIZE and (
                total_expected is None or len(all_datasets) >= total_expected
            ):
                break

            offset += self.PAGE_SIZE
            time.sleep(0.3)

        log.info(f"Crawl datasets xong: {len(all_datasets)}")
        return all_datasets

    def fetch_dataset_detail(self, dataset_id: str) -> dict | None:
        """Lấy chi tiết 1 dataset."""
        url = self.DATASOURCE_DETAIL_URL.format(dataset_id)
        resp = self.api.get(url)
        if not resp or resp.status_code != 200:
            return None

        data = resp.json()
        dp = data.get("dataProvider")
        dp_type = dp.get("type", "") if isinstance(dp, dict) else ""
        dp_name = dp.get("name", "") if isinstance(dp, dict) else ""
        display_type = data.get("displayType", "")
        root_type = data.get("type", "")
        data_provider_type = data.get("dataProviderType", "")
        
        provider_type = ""
        for candidate in [display_type, data_provider_type, dp_name, root_type, dp_type]:
            if candidate and candidate not in ("STANDARD", "STANDARD_OAUTH"):
                provider_type = candidate
                break
        if not provider_type:
            provider_type = display_type or data_provider_type or dp_type or root_type or ""

        DatasetCrawlService._detail_debug_count += 1
        if DatasetCrawlService._detail_debug_count <= 5:
            log.info(f"  [DEBUG-DETAIL] id={dataset_id}, name={data.get('name','')[:40]}, "
                     f"FINAL='{provider_type}', displayType='{display_type}', "
                     f"dataProviderType='{data_provider_type}', dp.name='{dp_name}', "
                     f"dp.type='{dp_type}', root.type='{root_type}', "
                     f"STATE='{data.get('state','')}', STATUS='{data.get('status','')}'")

        return {
            "id": str(data.get("id", dataset_id)),
            "name": data.get("name", ""),
            "row_count": data.get("rowCount", 0),
            "column_count": data.get("columnCount", 0),
            "card_count": data.get("cardCount", 0),
            "data_flow_count": data.get("dataFlowCount", 0),
            "provider_type": provider_type,
            "stream_id": data.get("streamId", ""),
            "schedule_active": data.get("scheduleActive"),
            "status": data.get("status", ""),
            "state": data.get("state", ""),
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
                "dataset_status": ds.get("dataset_status", ""),
                "last_execution_state": ds.get("last_execution_state", ""),
                "last_updated": last_updated_ts,
                "updated_at": datetime.now(),
            })

        if rows:
            from app.models.dataset import Dataset
            for r in rows:
                self.db.merge(Dataset(**r))
            self.db.commit()
            from collections import Counter
            pt_counts = Counter(r.get("provider_type", "") for r in rows)
            log.info(f"Lưu {len(rows)} datasets vào DB")
            log.info(f"  [DEBUG] provider_type distribution: {dict(pt_counts)}")

    def fetch_dataset_schedule(self, stream_id: int | str) -> dict | None:
        """Lấy thông tin schedule của dataset qua Stream API."""
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

        return {
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
