"""Beast Mode Router — Crawl, phân tích, và export Beast Mode."""

import csv
import io
import time
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

TOTAL_STEPS = 5
STEP_NAMES = {
    1: "Crawl Beast Mode",
    2: "Fetch BM Details",
    3: "Crawl Cards",
    4: "Fetch View Counts",
    5: "Phân tích & phân loại",
}


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


def _update_step(db: DomoDatabase, job_id: int, step: int, step_name: str,
                 message: str, step_processed: int = 0, step_total: int = 0):
    """Cập nhật step progress vào DB."""
    db.execute(
        """UPDATE crawl_jobs
           SET current_step = %s, step_name = %s, message = %s,
               step_processed = %s, step_total = %s
           WHERE id = %s""",
        (step, step_name, message, step_processed, step_total, job_id)
    )


def _update_step_progress(db: DomoDatabase, job_id: int, step_processed: int, step_total: int):
    """Cập nhật progress trong step hiện tại."""
    db.execute(
        """UPDATE crawl_jobs
           SET step_processed = %s, step_total = %s
           WHERE id = %s""",
        (step_processed, step_total, job_id)
    )


# ─── Background crawl task ────────────────────────────────

def _run_full_crawl(job_id: int):
    """Background task: crawl BM + cards + view counts + analyze."""
    auth = require_auth()
    db = get_db()
    api = DomoAPI(auth)
    bm_service = BeastModeService(api, db)
    card_service = CardService(api, db)

    step_times = {}
    crawl_start = time.time()

    log.info("=" * 60)
    log.info(f"🚀 BẮT ĐẦU CRAWL JOB #{job_id}")
    log.info("=" * 60)

    try:
        # Cập nhật job status
        db.execute(
            """UPDATE crawl_jobs SET status = 'running', started_at = %s,
               total_steps = %s WHERE id = %s""",
            (datetime.now(), TOTAL_STEPS, job_id)
        )

        # ─── Step 1: Crawl tất cả BM ───────────────────────
        step = 1
        step_start = time.time()
        log.step(step, TOTAL_STEPS, f"🔍 {STEP_NAMES[step]}")
        _update_step(db, job_id, step, STEP_NAMES[step],
                     f"[{step}/{TOTAL_STEPS}] Đang crawl Beast Mode...")

        all_bms = bm_service.crawl_all(
            job_id=job_id,
            progress_callback=lambda p, t: _update_step_progress(db, job_id, p, t)
        )
        bm_ids = [bm["id"] for bm in all_bms]

        step_times[step] = time.time() - step_start
        log.success(f"  ✅ Step {step} hoàn tất: {len(all_bms)} BM ({step_times[step]:.1f}s)")

        # ─── Step 2: Fetch BM details ──────────────────────
        step = 2
        step_start = time.time()
        log.step(step, TOTAL_STEPS, f"📋 {STEP_NAMES[step]} ({len(bm_ids)} BMs)")
        _update_step(db, job_id, step, STEP_NAMES[step],
                     f"[{step}/{TOTAL_STEPS}] Đang lấy chi tiết BM...",
                     0, len(bm_ids))

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(bm_service.fetch_details_batch(
                bm_ids,
                job_id=job_id,
                progress_callback=lambda p, t: _update_step_progress(db, job_id, p, t)
            ))
        finally:
            loop.close()

        step_times[step] = time.time() - step_start
        log.success(f"  ✅ Step {step} hoàn tất: {len(bm_ids)} details ({step_times[step]:.1f}s)")

        # ─── Step 3: Crawl Cards ───────────────────────────
        step = 3
        step_start = time.time()
        log.step(step, TOTAL_STEPS, f"🃏 {STEP_NAMES[step]}")
        _update_step(db, job_id, step, STEP_NAMES[step],
                     f"[{step}/{TOTAL_STEPS}] Đang crawl Cards...")

        all_cards = card_service.crawl_all_cards(
            job_id=job_id,
            progress_callback=lambda p, t: _update_step_progress(db, job_id, p, t)
        )
        card_service.save_cards_from_summary(all_cards)

        step_times[step] = time.time() - step_start
        log.success(f"  ✅ Step {step} hoàn tất: {len(all_cards)} cards ({step_times[step]:.1f}s)")

        # ─── Step 4: Fetch View Counts ─────────────────────
        step = 4
        step_start = time.time()
        urns = card_service.get_all_urns()
        log.step(step, TOTAL_STEPS, f"👁️ {STEP_NAMES[step]} ({len(urns)} cards)")
        _update_step(db, job_id, step, STEP_NAMES[step],
                     f"[{step}/{TOTAL_STEPS}] Đang lấy view counts...",
                     0, len(urns))

        card_service.fetch_view_counts(
            urns,
            job_id=job_id,
            progress_callback=lambda p, t: _update_step_progress(db, job_id, p, t)
        )

        step_times[step] = time.time() - step_start
        log.success(f"  ✅ Step {step} hoàn tất: {len(urns)} views ({step_times[step]:.1f}s)")

        # ─── Step 5: Phân tích ─────────────────────────────
        step = 5
        step_start = time.time()
        log.step(step, TOTAL_STEPS, f"📊 {STEP_NAMES[step]}")
        _update_step(db, job_id, step, STEP_NAMES[step],
                     f"[{step}/{TOTAL_STEPS}] Đang phân tích...", 0, 1)

        summary = bm_service.analyze()
        _update_step_progress(db, job_id, 1, 1)

        step_times[step] = time.time() - step_start
        log.success(f"  ✅ Step {step} hoàn tất ({step_times[step]:.1f}s)")

        # ─── Done ─────────────────────────────────────────
        total_time = time.time() - crawl_start
        log.info("=" * 60)
        log.success(f"🎉 CRAWL HOÀN TẤT! Tổng: {summary['total']} BM")
        log.info(f"⏱️  Tổng thời gian: {total_time:.1f}s ({total_time/60:.1f} phút)")
        for s, t in step_times.items():
            log.info(f"   Step {s} ({STEP_NAMES[s]}): {t:.1f}s")
        log.info("=" * 60)

        db.execute(
            """UPDATE crawl_jobs SET status = 'done', finished_at = %s,
               message = %s, found = %s, current_step = %s,
               step_name = 'Hoàn tất', step_processed = 1, step_total = 1
               WHERE id = %s""",
            (datetime.now(), f"Hoàn tất: {summary['total']} BM",
             summary["total"], TOTAL_STEPS, job_id)
        )

    except Exception as e:
        total_time = time.time() - crawl_start
        log.error("=" * 60)
        log.exception(f"❌ CRAWL LỖI sau {total_time:.1f}s", e)
        log.error("=" * 60)
        db.execute(
            "UPDATE crawl_jobs SET status = 'error', message = %s, finished_at = %s WHERE id = %s",
            (str(e)[:500], datetime.now(), job_id)
        )
    finally:
        db.close()


def _run_view_and_analyze(job_id: int):
    """Background task: chỉ chạy step 4 (view counts) + step 5 (analyze)."""
    auth = require_auth()
    db = get_db()
    api = DomoAPI(auth)
    bm_service = BeastModeService(api, db)
    card_service = CardService(api, db)

    partial_steps = 2  # chỉ 2 step
    step_times = {}
    crawl_start = time.time()

    log.info("=" * 60)
    log.info(f"🔄 CHẠY LẠI VIEW COUNTS + PHÂN TÍCH (Job #{job_id})")
    log.info("=" * 60)

    try:
        db.execute(
            """UPDATE crawl_jobs SET status = 'running', started_at = %s,
               total_steps = %s WHERE id = %s""",
            (datetime.now(), partial_steps, job_id)
        )

        # ─── Step 1/2: Fetch View Counts ─────────────────────
        step = 1
        step_start = time.time()
        urns = card_service.get_all_urns()
        log.step(step, partial_steps, f"👁️ Fetch View Counts ({len(urns)} cards)")
        _update_step(db, job_id, step, "Fetch View Counts",
                     f"[{step}/{partial_steps}] Đang lấy view counts...",
                     0, len(urns))

        card_service.fetch_view_counts(
            urns,
            job_id=job_id,
            progress_callback=lambda p, t: _update_step_progress(db, job_id, p, t)
        )

        step_times[step] = time.time() - step_start
        log.success(f"  ✅ View counts hoàn tất: {len(urns)} cards ({step_times[step]:.1f}s)")

        # ─── Step 2/2: Phân tích ─────────────────────────────
        step = 2
        step_start = time.time()
        log.step(step, partial_steps, "📊 Phân tích & phân loại")
        _update_step(db, job_id, step, "Phân tích & phân loại",
                     f"[{step}/{partial_steps}] Đang phân tích...", 0, 1)

        summary = bm_service.analyze()
        _update_step_progress(db, job_id, 1, 1)

        step_times[step] = time.time() - step_start
        log.success(f"  ✅ Phân tích hoàn tất ({step_times[step]:.1f}s)")

        # ─── Done ─────────────────────────────────────────
        total_time = time.time() - crawl_start
        log.info("=" * 60)
        log.success(f"🎉 HOÀN TẤT! Tổng: {summary['total']} BM")
        log.info(f"⏱️  Tổng thời gian: {total_time:.1f}s")
        for s, t in step_times.items():
            log.info(f"   Step {s}: {t:.1f}s")
        log.info("=" * 60)

        db.execute(
            """UPDATE crawl_jobs SET status = 'done', finished_at = %s,
               message = %s, found = %s, current_step = %s,
               step_name = 'Hoàn tất', step_processed = 1, step_total = 1
               WHERE id = %s""",
            (datetime.now(), f"Hoàn tất: {summary['total']} BM",
             summary["total"], partial_steps, job_id)
        )

    except Exception as e:
        total_time = time.time() - crawl_start
        log.exception(f"❌ LỖI sau {total_time:.1f}s", e)
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
        """INSERT INTO crawl_jobs (job_type, status, started_at, message, total_steps, current_step, step_name)
           VALUES ('beastmode_full', 'pending', %s, 'Đang khởi tạo...', %s, 0, 'Khởi tạo')""",
        (datetime.now(), TOTAL_STEPS)
    )
    job = db.query_one(
        "SELECT id FROM crawl_jobs WHERE job_type = 'beastmode_full' ORDER BY id DESC LIMIT 1"
    )
    job_id = job["id"]
    db.close()

    log.info(f"📝 Tạo crawl job #{job_id}")
    background_tasks.add_task(_run_full_crawl, job_id)

    return {"job_id": job_id, "message": "Crawl đã bắt đầu"}


@router.post("/crawl/reanalyze")
async def start_reanalyze(background_tasks: BackgroundTasks):
    """Chỉ chạy lại View Counts + Phân tích (skip crawl BM/Cards)."""
    require_auth()
    db = get_db()

    db.execute(
        """INSERT INTO crawl_jobs (job_type, status, started_at, message, total_steps, current_step, step_name)
           VALUES ('beastmode_full', 'pending', %s, 'Đang khởi tạo (reanalyze)...', 2, 0, 'Khởi tạo')""",
        (datetime.now(),)
    )
    job = db.query_one(
        "SELECT id FROM crawl_jobs WHERE job_type = 'beastmode_full' ORDER BY id DESC LIMIT 1"
    )
    job_id = job["id"]
    db.close()

    log.info(f"📝 Tạo reanalyze job #{job_id}")
    background_tasks.add_task(_run_view_and_analyze, job_id)

    return {"job_id": job_id, "message": "Reanalyze đã bắt đầu (View Counts + Phân tích)"}


@router.get("/status")
async def crawl_status():
    """Lấy trạng thái crawl hiện tại (kèm step progress)."""
    db = get_db()
    job = db.query_one(
        "SELECT * FROM crawl_jobs WHERE job_type = 'beastmode_full' ORDER BY id DESC LIMIT 1"
    )
    db.close()

    if not job:
        return {"status": "none", "message": "Chưa có crawl nào"}

    # Tính step percent
    step_total = job.get("step_total") or 0
    step_processed = job.get("step_processed") or 0
    step_percent = round((step_processed / step_total) * 100) if step_total > 0 else 0

    # Tính overall percent (theo step)
    current_step = job.get("current_step") or 0
    total_steps = job.get("total_steps") or TOTAL_STEPS
    if total_steps > 0 and current_step > 0:
        step_weight = 100 / total_steps
        overall_percent = round((current_step - 1) * step_weight + (step_percent / 100) * step_weight)
    else:
        overall_percent = 0

    # Tính elapsed time
    started_at = job.get("started_at")
    elapsed_seconds = 0
    if started_at and job["status"] == "running":
        elapsed_seconds = (datetime.now() - started_at).total_seconds()

    return {
        "job_id": job["id"],
        "status": job["status"],
        "total": job.get("total") or 0,
        "processed": job.get("processed") or 0,
        "found": job.get("found") or 0,
        "errors": job.get("errors") or 0,
        "message": job.get("message") or "",
        "started_at": job["started_at"].isoformat() if job.get("started_at") else None,
        "finished_at": job["finished_at"].isoformat() if job.get("finished_at") else None,
        # Step progress
        "current_step": current_step,
        "total_steps": total_steps,
        "step_name": job.get("step_name") or "",
        "step_processed": step_processed,
        "step_total": step_total,
        "step_percent": step_percent,
        "overall_percent": min(overall_percent, 100),
        "elapsed_seconds": round(elapsed_seconds),
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
