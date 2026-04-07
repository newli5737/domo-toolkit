from app.core.api import DomoAPI
from sqlalchemy.orm import Session
from app.services.dataset_service import DatasetCrawlService
from app.services.dataflow_service import DataflowCrawlService
from app.services.health_check_service import HealthCheckService

class MonitorService:
    """Facade for monitor services."""
    def __init__(self, api: DomoAPI, db: Session):
        self.api = api
        self.db = db
        self._dataset_service = DatasetCrawlService(api, db)
        self._dataflow_service = DataflowCrawlService(api, db)
        self._health_check_service = HealthCheckService(
            api, db, self._dataset_service, self._dataflow_service
        )

    # ─── Dataset Crawl Service ──────────────────────────────
    def crawl_all_datasets(self, progress_callback=None):
        return self._dataset_service.crawl_all_datasets(progress_callback)

    def fetch_dataset_detail(self, dataset_id: str):
        return self._dataset_service.fetch_dataset_detail(dataset_id)

    def save_datasets(self, datasets: list[dict]):
        return self._dataset_service.save_datasets(datasets)

    def fetch_dataset_schedule(self, stream_id):
        return self._dataset_service.fetch_dataset_schedule(stream_id)

    # ─── Dataflow Crawl Service ─────────────────────────────
    def fetch_dataflow_execution_history(self, df_id, limit=100, offset=0):
        return self._dataflow_service.fetch_dataflow_execution_history(df_id, limit, offset)

    def crawl_all_dataflows(self, progress_callback=None):
        return self._dataflow_service.crawl_all_dataflows(progress_callback)

    def fetch_dataflow_executions(self, df_id: str):
        return self._dataflow_service.fetch_dataflow_executions(df_id)

    def process_dataflow(self, df_stub: dict):
        return self._dataflow_service.process_dataflow(df_stub)

    def save_dataflows(self, dataflows: list[dict]):
        return self._dataflow_service.save_dataflows(dataflows)

    def propagate_dataflow_status_to_datasets(self, dataflows: list[dict]):
        return self._dataflow_service.propagate_dataflow_status_to_datasets(dataflows)

    # ─── Health Check Service ───────────────────────────────
    def check_health(self, stale_hours=24, min_card_count=0, provider_type="", min_dataflow_count=0, max_workers=10):
        return self._health_check_service.check_health(
            stale_hours, min_card_count, provider_type, min_dataflow_count, max_workers
        )
