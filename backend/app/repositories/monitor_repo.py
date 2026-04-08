"""MonitorRepository — Business logic cho Monitor data queries & alert config."""

import csv
import io
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text, func, or_

from app.config import get_settings
from app.core.logger import DomoLogger
from app.models.dataset import Dataset, Dataflow
from app.schemas.monitor import (
    DatasetListResponse, DataflowListResponse, ProviderTypesResponse,
    AlertDataResponse, AutoCheckConfigResponse, AutoCheckResult, SaveConfigResponse,
)

_log = DomoLogger("monitor-repo")


class MonitorRepository:
    """Repository cho phần đọc dữ liệu Monitor từ DB."""

    def __init__(self, db: Session):
        self.db = db
        self.settings = get_settings()

    # ─── Alert Config (app_settings table) ─────────────────

    def load_alert_config(self) -> dict:
        try:
            from app.models.monitor import AppSetting
            rows = self.db.query(AppSetting).filter(
                (AppSetting.key.like('alert_%')) | (AppSetting.key.like('schedule_%'))
            ).all()
            config = {
                "alert_email": "", "min_card_count": 40, "provider_type": "mysql-ssh",
                "schedule_enabled": False, "schedule_hour": 8, "schedule_minute": 0,
                "schedule_days": "mon,tue,wed,thu,fri"
            }
            for row in rows:
                k, v = row.key, row.value
                if k == "alert_email": config["alert_email"] = v
                elif k == "alert_min_card_count": config["min_card_count"] = int(v)
                elif k == "alert_provider_type": config["provider_type"] = v
                elif k == "schedule_enabled": config["schedule_enabled"] = v.lower() == "true"
                elif k == "schedule_hour": config["schedule_hour"] = int(v)
                elif k == "schedule_minute": config["schedule_minute"] = int(v)
                elif k == "schedule_days": config["schedule_days"] = v
            return config
        except Exception as e:
            print(f"[ALERT-CONFIG] Load error: {e}")
            return {
                "alert_email": "", "min_card_count": 40, "provider_type": "mysql-ssh",
                "schedule_enabled": False, "schedule_hour": 8, "schedule_minute": 0,
                "schedule_days": "mon,tue,wed,thu,fri"
            }

    def save_alert_config(self, config: dict):
        mappings = {
            "alert_email": str(config.get("alert_email", "")),
            "alert_min_card_count": str(config.get("min_card_count", 40)),
            "alert_provider_type": str(config.get("provider_type", "mysql-ssh")),
            "schedule_enabled": str(config.get("schedule_enabled", False)).lower(),
            "schedule_hour": str(config.get("schedule_hour", 8)),
            "schedule_minute": str(config.get("schedule_minute", 0)),
            "schedule_days": str(config.get("schedule_days", "mon,tue,wed,thu,fri")),
        }
        try:
            from app.models.monitor import AppSetting
            for k, v in mappings.items():
                self.db.merge(AppSetting(key=k, value=v))
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            _log.error(f"Save alert config error: {e}")

    # ─── Dataset queries ───────────────────────────────────

    def list_datasets(self, provider_type: str = "", min_card_count: int = 0,
                      limit: int = 5000, offset: int = 0) -> DatasetListResponse:
        query = self.db.query(Dataset)
        if provider_type:
            query = query.filter(func.lower(Dataset.provider_type) == provider_type.lower())
        if min_card_count > 0:
            query = query.filter(Dataset.card_count >= min_card_count)

        total = query.count()
        datasets = query.order_by(Dataset.last_updated.desc().nullslast()).offset(offset).limit(limit).all()
        # Convert models to dicts to match Pydantic schema and remove _sa_instance_state
        rows = [{k: v for k, v in d.__dict__.items() if not k.startswith('_')} for d in datasets]
        return DatasetListResponse(total=total, datasets=rows)

    def list_dataflows(self, status_filter: str = "", limit: int = 5000, offset: int = 0) -> DataflowListResponse:
        query = self.db.query(Dataflow)
        if status_filter:
            query = query.filter(func.upper(Dataflow.last_execution_state) == status_filter.upper())

        total = query.count()
        dataflows = query.order_by(Dataflow.last_execution_time.desc().nullslast()).offset(offset).limit(limit).all()
        rows = [{k: v for k, v in d.__dict__.items() if not k.startswith('_')} for d in dataflows]
        return DataflowListResponse(total=total, dataflows=rows)

    def get_provider_types(self) -> ProviderTypesResponse:
        rows = self.db.query(Dataset.provider_type).filter(
            Dataset.provider_type != None,
            Dataset.provider_type != ''
        ).distinct().order_by(Dataset.provider_type).all()
        return ProviderTypesResponse(provider_types=[r[0] for r in rows if r[0]])

    # ─── Alerts ────────────────────────────────────────────

    def get_alerts_from_db(self) -> AlertDataResponse:
        # Exclude DISABLED datasets
        failed_ds = self.db.query(Dataset).filter(
            func.upper(func.coalesce(Dataset.dataset_status, '')).not_like('DISABLED%'),
            or_(
                func.upper(func.coalesce(Dataset.last_execution_state, '')).like('FAILED%'),
                func.upper(func.coalesce(Dataset.last_execution_state, '')).like('ERROR%'),
                func.upper(func.coalesce(Dataset.dataset_status, '')).like('ERROR%')
            )
        ).all()

        # Exclude DISABLED dataflows
        failed_df = self.db.query(Dataflow).filter(
            func.upper(func.coalesce(Dataflow.status, '')).not_like('DISABLED%'),
            or_(
                func.upper(func.coalesce(Dataflow.last_execution_state, '')).like('FAILED%'),
                func.upper(func.coalesce(Dataflow.status, '')).like('FAILED%'),
                func.upper(func.coalesce(Dataflow.last_execution_state, '')).like('ERROR%'),
                func.upper(func.coalesce(Dataflow.status, '')).like('ERROR%')
            )
        ).all()
        
        all_ds = [{"id": ds.id, "name": ds.name, "provider_type": ds.provider_type, "last_execution_state": ds.last_execution_state, "card_count": ds.card_count} for ds in failed_ds]
        all_df = [{"id": df.id, "name": df.name, "last_execution_state": df.last_execution_state, "status": df.status} for df in failed_df]

        return AlertDataResponse(
            checked_at=datetime.now().isoformat(),
            all_ok=len(all_ds) == 0 and len(all_df) == 0,
            failed_datasets=all_ds,
            failed_dataflows=all_df,
        )

    # ─── Auto Check Logic ─────────────────────────────────

    def run_auto_check(self, provider_type: str, min_card_count: int, alert_email: str) -> AutoCheckResult:
        """Kiểm tra datasets/dataflows trong DB → post Backlog nếu OK."""
        import requests as http_requests

        _log.info("=" * 60)
        _log.info("[AUTO-CHECK] ▶ Bắt đầu kiểm tra điều kiện")
        _log.info(f"  provider_type  : '{provider_type}'")
        _log.info(f"  min_card_count : {min_card_count}")

        # Cond 1: provider_type FAILED
        failed_by_type = []
        if provider_type.strip():
            rows = self.db.query(Dataset).filter(
                func.lower(Dataset.provider_type) == func.lower(provider_type.strip()),
                func.upper(func.coalesce(Dataset.dataset_status, '')).not_like('DISABLED%'),
                or_(
                    func.upper(func.coalesce(Dataset.last_execution_state, '')).like('FAILED%'),
                    func.upper(func.coalesce(Dataset.last_execution_state, '')).like('ERROR%'),
                    func.upper(func.coalesce(Dataset.dataset_status, '')).like('ERROR%')
                )
            ).all()
            failed_by_type = [{"id": r.id, "name": r.name} for r in rows]

        # Cond 2: card >= N FAILED
        rows_card = self.db.query(Dataset).filter(
            Dataset.card_count >= min_card_count,
            func.upper(func.coalesce(Dataset.dataset_status, '')).not_like('DISABLED%'),
            or_(
                func.upper(func.coalesce(Dataset.last_execution_state, '')).like('FAILED%'),
                func.upper(func.coalesce(Dataset.last_execution_state, '')).like('ERROR%'),
                func.upper(func.coalesce(Dataset.dataset_status, '')).like('ERROR%')
            )
        ).all()
        failed_by_card = [{"id": r.id, "name": r.name} for r in rows_card]

        # Merge dedup
        seen_ids = set()
        all_failed_ds = []
        for ds in failed_by_type + failed_by_card:
            if ds["id"] not in seen_ids:
                seen_ids.add(ds["id"])
                all_failed_ds.append(ds)

        # Dataflows FAILED (exclude DISABLED)
        rows_df = self.db.query(Dataflow).filter(
            func.upper(func.coalesce(Dataflow.status, '')).not_like('DISABLED%'),
            or_(
                func.upper(func.coalesce(Dataflow.last_execution_state, '')).like('FAILED%'),
                func.upper(func.coalesce(Dataflow.status, '')).like('FAILED%'),
                func.upper(func.coalesce(Dataflow.last_execution_state, '')).like('ERROR%'),
                func.upper(func.coalesce(Dataflow.status, '')).like('ERROR%')
            )
        ).all()
        all_failed_df = [{"id": r.id, "name": r.name} for r in rows_df]

        has_issues = len(all_failed_ds) > 0 or len(all_failed_df) > 0

        result = AutoCheckResult(
            checked_at=datetime.now().isoformat(),
            all_ok=not has_issues,
            failed_dataset_count=len(all_failed_ds),
            failed_dataflow_count=len(all_failed_df),
        )

        # Post Backlog
        if self.settings.backlog_issue_id and self.settings.backlog_api_key:
            try:
                api_key = self.settings.backlog_api_key
                issue_id = self.settings.backlog_issue_id
                base_url = self.settings.backlog_base_url
                domo_base = f"https://{self.settings.domo_instance}"

                http_requests.patch(
                    f"{base_url}/api/v2/issues/{issue_id}?apiKey={api_key}",
                    json={"statusId": 2},
                    headers={"Content-Type": "application/json"},
                    timeout=30,
                )

                def fmt_ds(ds_list):
                    if not ds_list:
                        return "エラーがなかった\n"
                    return "".join(f"{ds.get('name','')}\nURL: {domo_base}/datasources/{ds.get('id','')}/details/overview\n" for ds in ds_list)

                def fmt_df(df_list):
                    if not df_list:
                        return "エラーがなかった\n"
                    return "エラー有\n" + "".join(f"{df.get('name','')}\nURL: {domo_base}/datacenter/dataflows/{df.get('id','')}/details\n" for df in df_list)

                comment = (
                    f"【1次データ取得エラー確認結果】\n{fmt_ds(failed_by_type)}\n"
                    f"【メインDataSetエラー確認結果】\n{fmt_ds(failed_by_card)}\n"
                    f"【DataFlow】\n{fmt_df(all_failed_df)}"
                ).strip()

                resp = http_requests.post(
                    f"{base_url}/api/v2/issues/{issue_id}/comments?apiKey={api_key}",
                    json={"content": comment},
                    headers={"Content-Type": "application/json"},
                    timeout=30,
                )
                result.backlog_posted = resp.status_code < 400
            except Exception as e:
                _log.error(f"Backlog error: {e}")

        _log.info(f"[AUTO-CHECK] Done → backlog={result.backlog_posted}")
        _log.info("=" * 60)
        return result

    # ─── CSV Export ────────────────────────────────────────

    def export_datasets_csv(self, provider_type: str = "", min_card_count: int = 0, search: str = "") -> bytes:
        query = self.db.query(Dataset)
        if provider_type:
            query = query.filter(func.lower(Dataset.provider_type) == func.lower(provider_type))
        if min_card_count > 0:
            query = query.filter(Dataset.card_count >= min_card_count)
        if search.strip():
            query = query.filter(func.lower(Dataset.name).like(f"%{search.strip().lower()}%"))

        datasets = query.order_by(Dataset.card_count.desc().nullslast()).all()
        rows = [d.__dict__ for d in datasets]

        headers = ["ID", "Name", "Row Count", "Column Count", "Card Count", "Dataflow Count",
                    "Provider Type", "Stream ID", "Schedule State", "Dataset Status",
                    "Last Execution State", "Last Updated", "Crawled At"]
        keys = ["id", "name", "row_count", "column_count", "card_count", "data_flow_count",
                "provider_type", "stream_id", "schedule_state", "dataset_status",
                "last_execution_state", "last_updated", "updated_at"]

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(headers)
        for row in rows:
            writer.writerow([row.get(k, "") for k in keys])

        output.seek(0)
        return output.getvalue().encode("utf-8-sig")

    def export_dataflows_csv(self, status_filter: str = "", search: str = "") -> bytes:
        query = self.db.query(Dataflow)
        if status_filter:
            query = query.filter(func.upper(Dataflow.last_execution_state) == status_filter.upper())
        if search.strip():
            query = query.filter(func.lower(Dataflow.name).like(f"%{search.strip().lower()}%"))

        dataflows = query.order_by(Dataflow.last_execution_time.desc().nullslast()).all()
        rows = [d.__dict__ for d in dataflows]

        headers = ["ID", "Name", "Status", "Paused", "Database Type",
                    "Last Execution Time", "Last Execution State",
                    "Execution Count", "Owner", "Output Dataset Count", "Crawled At"]
        keys = ["id", "name", "status", "paused", "database_type",
                "last_execution_time", "last_execution_state",
                "execution_count", "owner", "output_dataset_count", "updated_at"]

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(headers)
        for row in rows:
            writer.writerow([row.get(k, "") for k in keys])

        output.seek(0)
        return output.getvalue().encode("utf-8-sig")
