

import io
import json
import time
import asyncio
import threading
from datetime import datetime
from fastapi import APIRouter, HTTPException, BackgroundTasks, WebSocket, WebSocketDisconnect, Depends
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.config import get_settings
from app.core.api import DomoAPI
from app.core.database import SessionLocal, get_db
from app.core.logger import DomoLogger
from app.repositories.auth_repo import get_auth
from app.repositories.beastmode_repo import BeastModeRepository
from app.services.beastmode import BeastModeService
from app.services.card import CardService
from app.schemas.beastmode import (
    ReanalyzeRequest, CrawlStartResponse, CrawlStatusResponse,
    GroupDataResponse, SearchResponse, CancelResponse,
)

router = APIRouter(prefix="/api/beastmode", tags=["beastmode"])
log = DomoLogger("bm-router")

from app.services.bm_crawler import (
    run_full_crawl, run_view_and_analyze, run_retry_details, run_bm_only_crawl,
    crawl_cancel, register_ws_client, unregister_ws_client, TOTAL_STEPS
)

from app.dependencies import require_auth


@router.post("/crawl", response_model=CrawlStartResponse)
async def start_crawl(background_tasks: BackgroundTasks, db: Session = Depends(get_db), auth: Session = Depends(require_auth)):
    job_id = BeastModeRepository(db).create_crawl_job("beastmode_full", "Đang khởi tạo...", TOTAL_STEPS)
    background_tasks.add_task(run_full_crawl, job_id)
    return CrawlStartResponse(job_id=job_id, message="Crawl đã bắt đầu")


@router.post("/crawl/reanalyze", response_model=CrawlStartResponse)
async def start_reanalyze(body: ReanalyzeRequest = ReanalyzeRequest(), background_tasks: BackgroundTasks = None, db: Session = Depends(get_db)):
    threshold = max(1, min(body.low_view_threshold, 10000))
    job_id = BeastModeRepository(db).create_crawl_job("beastmode_full", "Đang khởi tạo (reanalyze)...", 2)
    background_tasks.add_task(run_view_and_analyze, job_id, threshold)
    return CrawlStartResponse(job_id=job_id, message=f"Reanalyze đã bắt đầu (threshold={threshold})")


@router.post("/crawl/retry-details", response_model=CrawlStartResponse)
async def start_retry_details(background_tasks: BackgroundTasks, db: Session = Depends(get_db), auth: Session = Depends(require_auth)):
    job_id = BeastModeRepository(db).create_crawl_job("beastmode_full", "Retry BM Details...", 2)
    background_tasks.add_task(run_retry_details, job_id)
    return CrawlStartResponse(job_id=job_id, message="Retry BM Details đã bắt đầu")


@router.post("/crawl/bm-only", response_model=CrawlStartResponse)
async def start_bm_only_crawl(background_tasks: BackgroundTasks, db: Session = Depends(get_db), auth: Session = Depends(require_auth)):
    job_id = BeastModeRepository(db).create_crawl_job("beastmode_full", "BM-Only Crawl...", 2)
    background_tasks.add_task(run_bm_only_crawl, job_id)
    return CrawlStartResponse(job_id=job_id, message="BM-Only Crawl đã bắt đầu")


@router.post("/crawl/cancel", response_model=CancelResponse)
async def cancel_crawl(db: Session = Depends(get_db), auth: Session = Depends(require_auth)):
    crawl_cancel.set()
    BeastModeRepository(db).cancel_stale_jobs()
    return CancelResponse(message="Đã gửi yêu cầu hủy crawl")


@router.get("/status", response_model=CrawlStatusResponse)
async def crawl_status(db: Session = Depends(get_db)):
    return BeastModeRepository(db).get_crawl_status()


@router.get("/summary")
async def get_summary(db: Session = Depends(get_db)):
    return BeastModeRepository(db).get_summary()


@router.get("/group/{group_number}", response_model=GroupDataResponse)
async def get_group(group_number: int, limit: int = 100, offset: int = 0, db: Session = Depends(get_db)):
    if group_number not in (1, 2, 3, 4):
        raise HTTPException(status_code=400, detail="group_number phải từ 1-4")
    return BeastModeRepository(db).get_group_data(group_number, limit, offset)


@router.get("/search", response_model=SearchResponse)
async def search_beastmode(q: str = "", limit: int = 50, db: Session = Depends(get_db)):
    if not q.strip():
        return SearchResponse()
    return BeastModeRepository(db).search(q, limit)


@router.get("/export/csv")
async def export_csv(lang: str = "vi", group: int = 0, db: Session = Depends(get_db)):
    csv_bytes = BeastModeRepository(db).export_csv(group, lang)

    if not csv_bytes:
        raise HTTPException(status_code=404, detail="Chưa có dữ liệu phân tích")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return StreamingResponse(
        io.BytesIO(csv_bytes), media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=beastmode_cleanup_{timestamp}.csv"},
    )


@router.delete("/{bm_id}")
async def delete_beastmode(bm_id: int, db: Session = Depends(get_db), auth: Session = Depends(require_auth)):
    result = BeastModeRepository(db, auth).delete_bm(bm_id)
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
    return result


@router.websocket("/ws/progress")
async def ws_progress(websocket: WebSocket):
    await websocket.accept()
    register_ws_client(websocket, asyncio.get_event_loop())

    try:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        pass
    finally:
        unregister_ws_client(websocket)
