"""BeastModeRepository — Business logic cho BeastMode read-only queries."""

import csv
import io
from datetime import datetime

from sqlalchemy.orm import Session
from sqlalchemy import text
from app.core.api import DomoAPI
from app.core.auth import DomoAuth
from app.config import get_settings
from app.models.monitor import CrawlJob
from app.models.beastmode import BMAnalysis
from app.services.beastmode import BeastModeService
from app.schemas.beastmode import (
    CrawlStatusResponse, GroupDataResponse, SearchResponse, DeleteResponse,
)


def _get_bm_service(db: Session, auth=None) -> BeastModeService:
    """Helper: tạo BeastModeService. auth=None → dùng dummy (read-only)."""
    if auth is None:
        dummy = DomoAuth(get_settings().domo_instance)
        api = DomoAPI(dummy)
    else:
        api = DomoAPI(auth)
    return BeastModeService(api, db)


class BeastModeRepository:
    """Repository cho BeastMode queries (read-only endpoints)."""

    def __init__(self, db: Session, auth=None):
        self.db = db
        self.service = _get_bm_service(db, auth)

    def get_crawl_status(self) -> CrawlStatusResponse:
        job = self.db.query(CrawlJob).filter(CrawlJob.job_type == 'beastmode_full').order_by(CrawlJob.id.desc()).first()
        if not job:
            return CrawlStatusResponse(status="none", message="Chưa có crawl nào")

        step_total = job.step_total or 0
        step_processed = job.step_processed or 0
        step_percent = round((step_processed / step_total) * 100) if step_total > 0 else 0

        current_step = job.current_step or 0
        total_steps = job.total_steps or 5
        if total_steps > 0 and current_step > 0:
            step_weight = 100 / total_steps
            overall_percent = round((current_step - 1) * step_weight + (step_percent / 100) * step_weight)
        else:
            overall_percent = 0

        started_at = job.started_at
        elapsed = 0
        if started_at and job.status == "running":
            elapsed = (datetime.now() - started_at).total_seconds()

        return CrawlStatusResponse(
            job_id=job.id,
            status=job.status,
            total=job.total or 0,
            processed=job.processed or 0,
            found=job.found or 0,
            errors=job.errors or 0,
            message=job.message or "",
            started_at=job.started_at.isoformat() if job.started_at else None,
            finished_at=job.finished_at.isoformat() if job.finished_at else None,
            current_step=current_step,
            total_steps=total_steps,
            step_name=job.step_name or "",
            step_processed=step_processed,
            step_total=step_total,
            step_percent=step_percent,
            overall_percent=min(overall_percent, 100),
            elapsed_seconds=round(elapsed),
        )

    def get_summary(self) -> dict:
        return self.service.get_summary()

    def get_group_data(self, group_number: int, limit: int, offset: int) -> GroupDataResponse:
        data = self.service.get_group_data(group_number, limit, offset)
        total = self.db.query(BMAnalysis).filter(BMAnalysis.group_number == group_number).count()
        return GroupDataResponse(group=group_number, total=total, data=data)

    def search(self, query: str, limit: int) -> SearchResponse:
        data = self.service.search_bm(query, limit)
        return SearchResponse(data=data, total=len(data))

    def delete_bm(self, bm_id: int) -> DeleteResponse:
        result = self.service.delete_bm(bm_id)
        if not result["success"]:
            return DeleteResponse(success=False, error=result.get("error", "Xóa thất bại"), bm_id=bm_id)
        return DeleteResponse(success=True, bm_id=bm_id, affected_cards=result.get("affected_cards", 0))

    def export_csv(self, group: int, lang: str) -> bytes:
        rows = self.service.export_csv(group_number=group, lang=lang)
        if not rows:
            return b""

        header_map_ja = {
            "bm_id": "BM ID", "bm_name": "BM名", "legacy_id": "Legacy ID",
            "group_number": "グループ番号", "group_label": "グループラベル",
            "owner_name": "オーナー", "active_cards_count": "使用カード数",
            "total_views": "合計閲覧数", "referenced_by_count": "参照数",
            "dataset_names": "データセット名", "card_ids": "リンクカード",
            "complexity_score": "複雑度スコア", "duplicate_hash": "重複ハッシュ",
            "normalized_hash": "正規化ハッシュ", "structure_hash": "構造ハッシュ", "url": "URL",
        }
        header_map_vi = {
            "bm_id": "BM ID", "bm_name": "Tên Beast Mode", "legacy_id": "Legacy ID",
            "group_number": "Nhóm số", "group_label": "Nhãn nhóm",
            "owner_name": "Người tạo", "active_cards_count": "Số Card đang dùng",
            "total_views": "Tổng lượt xem", "referenced_by_count": "Số BM tham chiếu",
            "dataset_names": "Tên Dataset", "card_ids": "Link tới Card",
            "complexity_score": "Độ phức tạp", "duplicate_hash": "Mã Hash Trùng Lặp",
            "normalized_hash": "Mã Hash Chuẩn Hóa", "structure_hash": "Mã Hash Cấu Trúc", "url": "Đường dẫn",
        }

        original_keys = list(rows[0].keys())
        current_map = header_map_ja if lang == "ja" else header_map_vi
        mapped_keys = [current_map.get(k, k) for k in original_keys]
        mapped_rows = [{current_map.get(k, k): v for k, v in row.items()} for row in rows]

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=mapped_keys)
        writer.writeheader()
        writer.writerows(mapped_rows)

        output.seek(0)
        return output.getvalue().encode("utf-8-sig")

    def create_crawl_job(self, job_type: str, message: str, total_steps: int) -> int:
        """Tạo crawl job record, trả về job_id."""
        job = CrawlJob(
            job_type='beastmode_full',
            status='pending',
            started_at=datetime.now(),
            message=message,
            total_steps=total_steps,
            current_step=0,
            step_name='Khởi tạo'
        )
        self.db.add(job)
        self.db.commit()
        return job.id

    def cancel_stale_jobs(self):
        """Hủy các job bị kẹt do restart server."""
        try:
            stale_jobs = self.db.query(CrawlJob).filter(CrawlJob.status.in_(['running', 'pending'])).all()
            for job in stale_jobs:
                job.status = 'cancelled'
                job.message = 'Server restarted'
                job.finished_at = datetime.now()
            self.db.commit()
        except Exception:
            self.db.rollback()

    def update_job_status(self, job_id: int, status: str, message: str = None, 
                          started_at: datetime = None, finished_at: datetime = None,
                          total_steps: int = None, current_step: int = None, found: int = None):
        """Cập nhật trạng thái job."""
        job = self.db.query(CrawlJob).filter(CrawlJob.id == job_id).first()
        if not job:
            return
        
        job.status = status
        if message is not None: job.message = message
        if started_at is not None: job.started_at = started_at
        if finished_at is not None: job.finished_at = finished_at
        if total_steps is not None: job.total_steps = total_steps
        if current_step is not None: job.current_step = current_step
        if found is not None: job.found = found
        
        self.db.commit()

    def truncate_tables(self, tables: list[str]):
        """Xóa trắng dữ liệu các bảng trước khi crawl mới."""
        for tbl in tables:
            self.db.execute(text(f"DELETE FROM {tbl}"))
        self.db.commit()

    def get_missing_expression_bm_ids(self) -> list[int]:
        """Lấy danh sách ID các BM bị thiếu expression cấu trúc."""
        rows = self.db.execute(text("SELECT id FROM beastmodes WHERE expression IS NULL OR expression = ''")).mappings().all()
        return [int(r["id"]) for r in rows]
