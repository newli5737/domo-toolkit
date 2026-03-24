"""Beast Mode Router — Crawl, phân tích, và export Beast Mode."""

import csv
import io
import asyncio
from datetime import datetime
from fastapi import APIRouter, HTTPException, BackgroundTasks
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.config import get_settings
from app.core.api import DomoAPI
from app.core.db import DomoDatabase
from app.core.logger import DomoLogger
from app.routers.auth import get_auth
from app.services.beastmode import BeastModeService
from app.services.card import CardService

router = APIRouter(prefix="/api/beastmode", tags=["beastmode"])
log = DomoLogger("bm-router")


def get_db() -> DomoDatabase:
    settings = get_settings()
    db = DomoDatabase(
        host=settings.db_host, port=settings.db_port,
        dbname=settings.db_name, user=settings.db_user,
        password=settings.db_password,
    )
    return db


def require_auth():
    auth = get_auth()
    if not auth.is_valid:
        raise HTTPException(status_code=401, detail="Chưa login Domo. Vui lòng login trước.")
    return auth


# ─── Background crawl task ────────────────────────────────

def _run_full_crawl(job_id: int):
    """Background task: crawl BM + cards + view counts + analyze."""
    auth = require_auth()
    db = get_db()
    api = DomoAPI(auth)
    bm_service = BeastModeService(api, db)
    card_service = CardService(api, db)

    try:
        # Cập nhật job status
        db.execute(
            "UPDATE crawl_jobs SET status = 'running', started_at = %s WHERE id = %s",
            (datetime.now(), job_id)
        )

        # 1. Crawl tất cả BM
        db.execute(
            "UPDATE crawl_jobs SET message = 'Đang crawl Beast Mode...' WHERE id = %s",
            (job_id,)
        )
        all_bms = bm_service.crawl_all(job_id=job_id)
        bm_ids = [bm["id"] for bm in all_bms]

        # 2. Fetch BM details (async)
        db.execute(
            "UPDATE crawl_jobs SET message = 'Đang lấy chi tiết BM...' WHERE id = %s",
            (job_id,)
        )

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(bm_service.fetch_details_batch(bm_ids, job_id=job_id))
        finally:
            loop.close()

        # 3. Crawl cards
        db.execute(
            "UPDATE crawl_jobs SET message = 'Đang crawl Cards...' WHERE id = %s",
            (job_id,)
        )
        all_cards = card_service.crawl_all_cards()
        card_service.save_cards_from_summary(all_cards)

        # 4. Fetch view counts
        db.execute(
            "UPDATE crawl_jobs SET message = 'Đang lấy view counts...' WHERE id = %s",
            (job_id,)
        )
        urns = card_service.get_all_urns()
        card_service.fetch_view_counts(urns)

        # 5. Phân tích
        db.execute(
            "UPDATE crawl_jobs SET message = 'Đang phân tích...' WHERE id = %s",
            (job_id,)
        )
        summary = bm_service.analyze()

        # Done
        db.execute(
            """UPDATE crawl_jobs SET status = 'done', finished_at = %s,
               message = %s, found = %s WHERE id = %s""",
            (datetime.now(), f"Hoàn tất: {summary['total']} BM", summary["total"], job_id)
        )

    except Exception as e:
        log.error(f"Crawl lỗi: {e}")
        db.execute(
            "UPDATE crawl_jobs SET status = 'error', message = %s, finished_at = %s WHERE id = %s",
            (str(e)[:500], datetime.now(), job_id)
        )
    finally:
        db.close()


# ─── Endpoints ────────────────────────────────────────────

@router.post("/crawl")
async def start_crawl(background_tasks: BackgroundTasks):
    """Bắt đầu crawl toàn bộ BM + cards (chạy background)."""
    require_auth()
    db = get_db()

    # Tạo job record
    db.execute(
        """INSERT INTO crawl_jobs (job_type, status, started_at, message)
           VALUES ('beastmode_full', 'pending', %s, 'Đang khởi tạo...')""",
        (datetime.now(),)
    )
    job = db.query_one(
        "SELECT id FROM crawl_jobs WHERE job_type = 'beastmode_full' ORDER BY id DESC LIMIT 1"
    )
    job_id = job["id"]
    db.close()

    background_tasks.add_task(_run_full_crawl, job_id)

    return {"job_id": job_id, "message": "Crawl đã bắt đầu"}


@router.get("/status")
async def crawl_status():
    """Lấy trạng thái crawl hiện tại."""
    db = get_db()
    job = db.query_one(
        "SELECT * FROM crawl_jobs WHERE job_type = 'beastmode_full' ORDER BY id DESC LIMIT 1"
    )
    db.close()

    if not job:
        return {"status": "none", "message": "Chưa có crawl nào"}

    return {
        "job_id": job["id"],
        "status": job["status"],
        "total": job["total"],
        "processed": job["processed"],
        "found": job["found"],
        "errors": job["errors"],
        "message": job["message"],
        "started_at": job["started_at"].isoformat() if job["started_at"] else None,
        "finished_at": job["finished_at"].isoformat() if job["finished_at"] else None,
    }


@router.get("/summary")
async def get_summary():
    """Lấy tổng hợp kết quả phân tích."""
    require_auth()
    db = get_db()
    api = DomoAPI(get_auth())
    bm_service = BeastModeService(api, db)
    result = bm_service.get_summary()
    db.close()
    return result


@router.get("/group/{group_number}")
async def get_group(group_number: int, limit: int = 100, offset: int = 0):
    """Lấy danh sách BM theo nhóm."""
    if group_number not in (1, 2, 3, 4):
        raise HTTPException(status_code=400, detail="group_number phải từ 1-4")

    require_auth()
    db = get_db()
    api = DomoAPI(get_auth())
    bm_service = BeastModeService(api, db)
    data = bm_service.get_group_data(group_number, limit, offset)
    total = db.count("bm_analysis", "group_number = %s", (group_number,))
    db.close()

    return {"group": group_number, "total": total, "data": data}


@router.get("/export/csv")
async def export_csv():
    """Tải file CSV kết quả phân tích."""
    require_auth()
    db = get_db()
    api = DomoAPI(get_auth())
    bm_service = BeastModeService(api, db)
    rows = bm_service.export_csv()
    db.close()

    if not rows:
        raise HTTPException(status_code=404, detail="Chưa có dữ liệu phân tích")

    # Tạo CSV in-memory
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)

    output.seek(0)
    csv_bytes = output.getvalue().encode("utf-8-sig")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"beastmode_cleanup_{timestamp}.csv"

    return StreamingResponse(
        io.BytesIO(csv_bytes),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )
