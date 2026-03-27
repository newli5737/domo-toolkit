"""Beast Mode Router — Crawl, phân tích, và export Beast Mode."""

import csv
import io
import json
import time
import asyncio
import threading
from datetime import datetime
from fastapi import APIRouter, HTTPException, BackgroundTasks, WebSocket, WebSocketDisconnect
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

# ─── Global state ─────────────────────────────────────────
crawl_cancel = threading.Event()
ws_clients: list[WebSocket] = []
_ws_loop: asyncio.AbstractEventLoop | None = None

# In-memory per-step progress (thay thế DB polling)
crawl_progress: dict = {
    "status": "idle",       # idle | running | done | error | cancelled
    "job_id": None,
    "started_at": None,
    "elapsed": 0,
    "message": "",
    "steps": {}             # { 1: {name, status, processed, total, percent}, ... }
}
_progress_lock = threading.Lock()

TOTAL_STEPS = 5
STEP_NAMES = {
    1: "Crawl Beast Mode",
    2: "Fetch BM Details",
    3: "Crawl Cards",
    4: "Fetch View Counts",
    5: "Phân tích & phân loại",
}


def _init_progress(job_id: int):
    """Reset progress khi bắt đầu crawl mới."""
    with _progress_lock:
        crawl_progress["status"] = "running"
        crawl_progress["job_id"] = job_id
        crawl_progress["started_at"] = time.time()
        crawl_progress["elapsed"] = 0
        crawl_progress["message"] = "Đang khởi tạo..."
        crawl_progress["steps"] = {
            i: {"name": STEP_NAMES[i], "status": "pending", "processed": 0, "total": 0, "percent": 0}
            for i in range(1, TOTAL_STEPS + 1)
        }


def _update_progress(step: int, processed: int = 0, total: int = 0,
                      status: str = "running", message: str = ""):
    """Cập nhật progress cho 1 step và broadcast qua WebSocket."""
    with _progress_lock:
        s = crawl_progress["steps"].get(step, {})
        s["status"] = status
        s["processed"] = processed
        s["total"] = total
        s["percent"] = round((processed / total) * 100) if total > 0 else 0
        crawl_progress["steps"][step] = s
        if message:
            crawl_progress["message"] = message
        if crawl_progress["started_at"]:
            crawl_progress["elapsed"] = round(time.time() - crawl_progress["started_at"])

    _broadcast_progress()


def _finish_progress(status: str = "done", message: str = ""):
    """Đánh dấu crawl hoàn tất."""
    with _progress_lock:
        crawl_progress["status"] = status
        crawl_progress["message"] = message
        if crawl_progress["started_at"]:
            crawl_progress["elapsed"] = round(time.time() - crawl_progress["started_at"])
    _broadcast_progress()


def _broadcast_progress():
    """Gửi toàn bộ crawl_progress tới tất cả WebSocket clients."""
    if not ws_clients or not _ws_loop:
        return
    with _progress_lock:
        data = json.dumps(crawl_progress, default=str)
    disconnected = []
    for ws in ws_clients:
        try:
            asyncio.run_coroutine_threadsafe(ws.send_text(data), _ws_loop)
        except Exception:
            disconnected.append(ws)
    for ws in disconnected:
        if ws in ws_clients:
            ws_clients.remove(ws)


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



def _check_cancelled():
    """Raise nếu crawl bị hủy."""
    if crawl_cancel.is_set():
        raise Exception("Crawl đã bị hủy bởi người dùng")


def cleanup_stale_jobs():
    """Gọi khi startup: đánh dấu các job 'running' cũ thành 'cancelled'."""
    try:
        db = get_db()
        db.execute(
            """UPDATE crawl_jobs SET status = 'cancelled', message = 'Server restarted',
               finished_at = %s WHERE status IN ('running', 'pending')""",
            (datetime.now(),)
        )
        count = db.query_one("SELECT COUNT(*) as c FROM crawl_jobs WHERE status = 'cancelled' AND message = 'Server restarted'")
        if count and count["c"] > 0:
            log.info(f"🧹 Đã hủy {count['c']} job cũ đang chạy")
        db.close()
    except Exception as e:
        log.warn(f"Cleanup stale jobs lỗi: {e}")

# ─── Background crawl task (PIPELINE: 5 steps song song) ──

def _run_full_crawl(job_id: int):
    """Background task: 5 steps chạy pipeline song song.
    - Thread A: Step 1 (Crawl BM) → đẩy BM IDs vào queue → Step 2 (BM Details) nhận ngay
    - Thread B: Step 3 (Crawl Cards) → Step 4 (View Counts)
    - Main: chờ tất cả → Step 5 (Analyze)
    """
    import queue as queue_mod

    auth = require_auth()
    db = get_db()
    api = DomoAPI(auth)
    bm_service = BeastModeService(api, db)

    step_times = {}
    crawl_start = time.time()
    errors_list = []
    crawl_cancel.clear()
    _init_progress(job_id)

    log.info("=" * 60)
    log.info(f"🚀 BẮT ĐẦU CRAWL JOB #{job_id} (PIPELINE MODE)")
    log.info("=" * 60)

    try:
        db.execute(
            """UPDATE crawl_jobs SET status = 'running', started_at = %s,
               total_steps = %s WHERE id = %s""",
            (datetime.now(), TOTAL_STEPS, job_id)
        )

        # Xóa dữ liệu cũ trước khi crawl mới
        log.info("🧹 Xóa dữ liệu crawl cũ...")
        for tbl in ("bm_analysis", "bm_card_map", "beastmodes", "cards"):
            db.execute(f"DELETE FROM {tbl}")
        log.info("  ✅ Đã xóa xong dữ liệu cũ")

        # Queue để Step 1 đẩy BM IDs → Step 2 nhận ngay
        bm_queue = queue_mod.Queue()
        step1_done = threading.Event()

        # ─── Thread A: Step 1 (Crawl BM) + pipe → Step 2 (BM Details) ───

        def run_step1_and_step2():
            step1_start = time.time()
            try:
                db_a = get_db()
                api_a = DomoAPI(auth)
                bm_a = BeastModeService(api_a, db_a)

                # Step 2 consumer thread
                step2_processed = [0]
                step2_total = [0]

                def step2_consumer():
                    db_s2 = get_db()
                    api_s2 = DomoAPI(auth)
                    bm_s2 = BeastModeService(api_s2, db_s2)
                    s2_start = time.time()
                    log.step(2, TOTAL_STEPS, f"📋 {STEP_NAMES[2]} (chờ BM IDs...)")

                    while not crawl_cancel.is_set():
                        try:
                            batch_ids = bm_queue.get(timeout=2)
                        except queue_mod.Empty:
                            if step1_done.is_set() and bm_queue.empty():
                                break
                            continue

                        if batch_ids is None:
                            break

                        step2_total[0] += len(batch_ids)
                        log.info(f"  📋 Step 2: nhận {len(batch_ids)} BM IDs (tổng: {step2_total[0]})")

                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        try:
                            loop.run_until_complete(bm_s2.fetch_details_batch(batch_ids))
                        finally:
                            loop.close()

                        step2_processed[0] += len(batch_ids)
                        _update_progress(2, step2_processed[0], step2_total[0])

                    db_s2.close()
                    step_times[2] = time.time() - s2_start
                    _update_progress(2, step2_processed[0], step2_total[0], status="done")
                    log.success(f"  ✅ Step 2 hoàn tất: {step2_processed[0]} BMs ({step_times[2]:.1f}s)")

                t_s2 = threading.Thread(target=step2_consumer, name="step2-consumer", daemon=True)
                t_s2.start()

                # Step 1: Crawl BM
                log.step(1, TOTAL_STEPS, f"🔍 {STEP_NAMES[1]}")
                _update_progress(1, 0, 0, message="Đang crawl Beast Mode...")

                all_bms = bm_a.crawl_all(
                    job_id=job_id,
                    progress_callback=lambda p, t: _update_progress(1, p, t),
                    on_batch_callback=lambda ids: bm_queue.put(ids),
                )

                step_times[1] = time.time() - step1_start
                _update_progress(1, len(all_bms), len(all_bms), status="done")
                log.success(f"  ✅ Step 1 hoàn tất: {len(all_bms)} BM ({step_times[1]:.1f}s)")

                step1_done.set()
                bm_queue.put(None)  # sentinel
                t_s2.join()
                db_a.close()

            except Exception as e:
                step1_done.set()
                errors_list.append(("Step 1+2", e))
                log.exception("❌ Step 1+2 lỗi", e)

        # ─── Thread B: Step 3 (Crawl Cards) → Step 4 (View Counts) ───

        def run_step34():
            step34_start = time.time()
            try:
                db_b = get_db()
                api_b = DomoAPI(auth)
                card_b = CardService(api_b, db_b)

                log.step(3, TOTAL_STEPS, f"🃏 {STEP_NAMES[3]}")
                _update_progress(3, 0, 0, message="Đang crawl cards...")
                all_cards = card_b.crawl_all_cards(
                    progress_callback=lambda p, t: _update_progress(3, p, t),
                )
                card_b.save_cards_from_summary(all_cards)
                step_times[3] = time.time() - step34_start
                _update_progress(3, len(all_cards), len(all_cards), status="done")
                log.success(f"  ✅ Step 3 hoàn tất: {len(all_cards)} cards ({step_times[3]:.1f}s)")

                step4_start = time.time()
                urns = card_b.get_all_urns()
                log.step(4, TOTAL_STEPS, f"👁️ {STEP_NAMES[4]} ({len(urns)} cards)")
                _update_progress(4, 0, len(urns))
                card_b.fetch_view_counts(
                    urns,
                    progress_callback=lambda p, t: _update_progress(4, p, t),
                )
                step_times[4] = time.time() - step4_start
                _update_progress(4, len(urns), len(urns), status="done")
                log.success(f"  ✅ Step 4 hoàn tất ({step_times[4]:.1f}s)")

                db_b.close()
            except Exception as e:
                errors_list.append(("Step 3+4", e))
                log.exception("❌ Step 3+4 lỗi", e)

        # ─── Khởi chạy pipeline ────────────────────────────

        log.info("🔀 PIPELINE: Step 1→2 (BM Crawl→Details) ‖ Step 3→4 (Cards→Views)")

        t_a = threading.Thread(target=run_step1_and_step2, name="pipeline-bm", daemon=True)
        t_b = threading.Thread(target=run_step34, name="pipeline-cards", daemon=True)
        t_a.start()
        t_b.start()
        t_a.join()
        t_b.join()

        if errors_list:
            err_msgs = "; ".join(f"{name}: {e}" for name, e in errors_list)
            raise Exception(err_msgs)

        # ─── Step 5: Phân tích ─────────────────────────────
        step = 5
        step_start = time.time()
        log.step(step, TOTAL_STEPS, f"📊 {STEP_NAMES[step]}")
        _update_progress(5, 0, 1, message="Đang phân tích...")

        summary = bm_service.analyze()
        _update_progress(5, 1, 1, status="done")

        step_times[step] = time.time() - step_start
        log.success(f"  ✅ Step {step} hoàn tất ({step_times[step]:.1f}s)")

        # ─── Done ─────────────────────────────────────────
        total_time = time.time() - crawl_start
        log.info("=" * 60)
        log.success(f"🎉 CRAWL HOÀN TẤT! Tổng: {summary['total']} BM")
        log.info(f"⏱️  Tổng thời gian: {total_time:.1f}s ({total_time/60:.1f} phút)")
        for s, t in step_times.items():
            log.info(f"   Step {s} ({STEP_NAMES.get(s, '')}): {t:.1f}s")
        log.info("=" * 60)

        db.execute(
            """UPDATE crawl_jobs SET status = 'done', finished_at = %s,
               message = %s, found = %s, current_step = %s
               WHERE id = %s""",
            (datetime.now(), f"Hoàn tất: {summary['total']} BM",
             summary["total"], TOTAL_STEPS, job_id)
        )
        _finish_progress("done", f"Hoàn tất: {summary['total']} BM")

    except Exception as e:
        total_time = time.time() - crawl_start
        log.error("=" * 60)
        log.exception(f"❌ CRAWL LỖI sau {total_time:.1f}s", e)
        log.error("=" * 60)
        db.execute(
            "UPDATE crawl_jobs SET status = 'error', message = %s, finished_at = %s WHERE id = %s",
            (str(e)[:500], datetime.now(), job_id)
        )
        _finish_progress("error", str(e)[:500])
    finally:
        db.close()


def _run_view_and_analyze(job_id: int, low_view_threshold: int = 10):
    """Background task: chỉ chạy phân tích lại từ DB sẵn có (KHÔNG crawl API)."""
    db = get_db()
    # Dùng dummy auth vì chỉ cần DB, không gọi API
    from app.core.auth import DomoAuth
    dummy_auth = DomoAuth("astecpaints-co-jp.domo.com")
    api = DomoAPI(dummy_auth)
    bm_service = BeastModeService(api, db)

    crawl_start = time.time()
    _init_progress(job_id)

    log.info("=" * 60)
    log.info(f"🔄 PHÂN TÍCH LẠI TỪ DB (Job #{job_id}, low_view_threshold={low_view_threshold})")
    log.info("=" * 60)

    try:
        db.execute(
            """UPDATE crawl_jobs SET status = 'running', started_at = %s,
               total_steps = 1 WHERE id = %s""",
            (datetime.now(), job_id)
        )

        _update_progress(5, 0, 1, message=f"Đang phân tích (threshold={low_view_threshold})...")

        summary = bm_service.analyze(low_view_threshold=low_view_threshold)
        _update_progress(5, 1, 1, status="done")

        total_time = time.time() - crawl_start
        log.info("=" * 60)
        log.success(f"🎉 HOÀN TẤT! Tổng: {summary['total']} BM ({total_time:.1f}s)")
        log.info("=" * 60)

        db.execute(
            """UPDATE crawl_jobs SET status = 'done', finished_at = %s,
               message = %s, found = %s, current_step = 1,
               step_name = 'Hoàn tất', step_processed = 1, step_total = 1
               WHERE id = %s""",
            (datetime.now(), f"Hoàn tất: {summary['total']} BM",
             summary["total"], job_id)
        )
        _finish_progress("done", f"Hoàn tất: {summary['total']} BM")

    except Exception as e:
        total_time = time.time() - crawl_start
        log.exception(f"❌ LỖI sau {total_time:.1f}s", e)
        db.execute(
            "UPDATE crawl_jobs SET status = 'error', message = %s, finished_at = %s WHERE id = %s",
            (str(e)[:500], datetime.now(), job_id)
        )
        _finish_progress("error", str(e)[:500])
    finally:
        db.close()


def _run_retry_details(job_id: int):
    """Background task: chỉ retry fetch BM details cho các BM thiếu expression, rồi phân tích lại."""
    auth = require_auth()
    db = get_db()
    api = DomoAPI(auth)
    bm_service = BeastModeService(api, db)

    step_times = {}
    crawl_start = time.time()
    crawl_cancel.clear()

    # Tìm các BM chưa có detail
    missing = db.query(
        "SELECT id FROM beastmodes WHERE expression IS NULL OR expression = ''"
    )
    missing_ids = [int(r["id"]) for r in missing]

    log.info("=" * 60)
    log.info(f"🔄 RETRY BM DETAILS (Job #{job_id}): {len(missing_ids)} BM thiếu detail")
    log.info("=" * 60)

    if not missing_ids:
        log.info("✅ Tất cả BM đã có detail, chỉ chạy phân tích lại")

    # Init progress: chỉ hiển thị Step 2 + Step 5
    _init_progress(job_id)
    # Đánh dấu step 1, 3, 4 đã xong (vì không cần chạy lại)
    for s in [1, 3, 4]:
        with _progress_lock:
            crawl_progress["steps"][s]["status"] = "done"
            crawl_progress["steps"][s]["percent"] = 100

    try:
        db.execute(
            """UPDATE crawl_jobs SET status = 'running', started_at = %s,
               total_steps = 2 WHERE id = %s""",
            (datetime.now(), job_id)
        )

        # ─── Step 2: Fetch BM Details ─────────────────────────
        if missing_ids:
            step_start = time.time()
            log.step(1, 2, f"📋 Retry BM Details ({len(missing_ids)} BMs)")
            _update_progress(2, 0, len(missing_ids), message=f"Retry {len(missing_ids)} BM details...")

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(
                    bm_service.fetch_details_batch(
                        missing_ids,
                        concurrency=50,
                        progress_callback=lambda p, t: _update_progress(2, p, t),
                    )
                )
            finally:
                loop.close()

            step_times[2] = time.time() - step_start
            _update_progress(2, len(missing_ids), len(missing_ids), status="done")
            log.success(f"  ✅ Retry details hoàn tất ({step_times[2]:.1f}s)")
        else:
            _update_progress(2, 0, 0, status="done")

        # ─── Step 5: Phân tích ─────────────────────────────
        step_start = time.time()
        log.step(2, 2, "📊 Phân tích & phân loại")
        _update_progress(5, 0, 1, message="Đang phân tích...")

        summary = bm_service.analyze()
        _update_progress(5, 1, 1, status="done")

        step_times[5] = time.time() - step_start
        log.success(f"  ✅ Phân tích hoàn tất ({step_times[5]:.1f}s)")

        # ─── Done ─────────────────────────────────────────
        total_time = time.time() - crawl_start
        log.info("=" * 60)
        log.success(f"🎉 RETRY HOÀN TẤT! Tổng: {summary['total']} BM")
        log.info(f"⏱️  Tổng thời gian: {total_time:.1f}s")
        log.info("=" * 60)

        db.execute(
            """UPDATE crawl_jobs SET status = 'done', finished_at = %s,
               message = %s, found = %s WHERE id = %s""",
            (datetime.now(), f"Retry hoàn tất: {summary['total']} BM",
             summary["total"], job_id)
        )
        _finish_progress("done", f"Retry hoàn tất: {summary['total']} BM")

    except Exception as e:
        log.exception(f"❌ RETRY LỖI", e)
        db.execute(
            "UPDATE crawl_jobs SET status = 'error', message = %s, finished_at = %s WHERE id = %s",
            (str(e)[:500], datetime.now(), job_id)
        )
        _finish_progress("error", str(e)[:500])
    finally:
        db.close()

@router.post("/crawl")
async def start_crawl(background_tasks: BackgroundTasks):
    """Bắt đầu crawl toàn bộ BM + cards (chạy background). CẦN LOGIN."""
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


class ReanalyzeRequest(BaseModel):
    low_view_threshold: int = 10


@router.post("/crawl/reanalyze")
async def start_reanalyze(body: ReanalyzeRequest = ReanalyzeRequest(), background_tasks: BackgroundTasks = None):
    """Chỉ chạy phân tích lại từ DB sẵn có (KHÔNG crawl API). KHÔNG CẦN LOGIN."""
    threshold = max(1, min(body.low_view_threshold, 10000))  # clamp 1–10000
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

    log.info(f"📝 Tạo reanalyze job #{job_id} (threshold={threshold})")
    background_tasks.add_task(_run_view_and_analyze, job_id, threshold)

    return {"job_id": job_id, "message": f"Reanalyze đã bắt đầu (threshold={threshold})"}


@router.post("/crawl/retry-details")
async def start_retry_details(background_tasks: BackgroundTasks):
    """Chỉ retry fetch BM details cho các BM thiếu expression + phân tích lại. CẦN LOGIN."""
    require_auth()
    db = get_db()

    db.execute(
        """INSERT INTO crawl_jobs (job_type, status, started_at, message, total_steps, current_step, step_name)
           VALUES ('beastmode_full', 'pending', %s, 'Retry BM Details...', 2, 0, 'Retry Details')""",
        (datetime.now(),)
    )
    job = db.query_one(
        "SELECT id FROM crawl_jobs WHERE job_type = 'beastmode_full' ORDER BY id DESC LIMIT 1"
    )
    job_id = job["id"]
    db.close()

    log.info(f"📝 Tạo retry-details job #{job_id}")
    background_tasks.add_task(_run_retry_details, job_id)

    return {"job_id": job_id, "message": "Retry BM Details đã bắt đầu"}


# ─── Read-only endpoints (KHÔNG CẦN LOGIN) ───────────────

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
    """Lấy tổng hợp kết quả phân tích. KHÔNG CẦN LOGIN."""
    db = get_db()
    try:
        # Dùng dummy auth — chỉ cần DB, không gọi API
        from app.core.auth import DomoAuth
        dummy_auth = DomoAuth("astecpaints-co-jp.domo.com")
        api = DomoAPI(dummy_auth)
        bm_service = BeastModeService(api, db)
        result = bm_service.get_summary()
        return result
    finally:
        db.close()


@router.get("/group/{group_number}")
async def get_group(group_number: int, limit: int = 100, offset: int = 0):
    """Lấy danh sách BM theo nhóm. KHÔNG CẦN LOGIN."""
    if group_number not in (1, 2, 3, 4):
        raise HTTPException(status_code=400, detail="group_number phải từ 1-4")

    db = get_db()
    try:
        from app.core.auth import DomoAuth
        dummy_auth = DomoAuth("astecpaints-co-jp.domo.com")
        api = DomoAPI(dummy_auth)
        bm_service = BeastModeService(api, db)
        data = bm_service.get_group_data(group_number, limit, offset)
        total = db.count("bm_analysis", "group_number = %s", (group_number,))
        return {"group": group_number, "total": total, "data": data}
    finally:
        db.close()


@router.get("/export/csv")
async def export_csv(lang: str = "vi", group: int = 0):
    """Tải file CSV kết quả phân tích. group=0 lấy tất cả. KHÔNG CẦN LOGIN."""
    db = get_db()
    try:
        from app.core.auth import DomoAuth
        dummy_auth = DomoAuth("astecpaints-co-jp.domo.com")
        api = DomoAPI(dummy_auth)
        bm_service = BeastModeService(api, db)
        rows = bm_service.export_csv(group_number=group, lang=lang)

        if not rows:
            raise HTTPException(status_code=404, detail="Chưa có dữ liệu phân tích")

        # Header mapping for Japanese
        header_map_ja = {
            "bm_id": "BM ID",
            "bm_name": "BM名",
            "legacy_id": "Legacy ID",
            "group_number": "グループ番号",
            "group_label": "グループラベル",
            "active_cards_count": "使用カード数",
            "total_views": "合計閲覧数",
            "referenced_by_count": "参照数",
            "dataset_names": "データセット名",
            "complexity_score": "複雑度スコア",
            "duplicate_hash": "重複ハッシュ",
            "normalized_hash": "正規化ハッシュ",
            "structure_hash": "構造ハッシュ",
            "url": "URL",
        }

        original_keys = list(rows[0].keys())

        if lang == "ja":
            mapped_keys = [header_map_ja.get(k, k) for k in original_keys]
            mapped_rows = [
                {header_map_ja.get(k, k): v for k, v in row.items()}
                for row in rows
            ]
        else:
            mapped_keys = original_keys
            mapped_rows = rows

        # Tạo CSV in-memory
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=mapped_keys)
        writer.writeheader()
        writer.writerows(mapped_rows)

        output.seek(0)
        csv_bytes = output.getvalue().encode("utf-8-sig")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"beastmode_cleanup_{timestamp}.csv"

        return StreamingResponse(
            io.BytesIO(csv_bytes),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )
    finally:
        db.close()


# ─── Search endpoint (KHÔNG CẦN LOGIN) ───────────────────

@router.get("/search")
async def search_beastmode(q: str = "", limit: int = 50):
    """Tìm BM theo tên hoặc raw ID. KHÔNG CẦN LOGIN."""
    if not q.strip():
        return {"data": [], "total": 0}

    db = get_db()
    try:
        from app.core.auth import DomoAuth
        dummy_auth = DomoAuth("astecpaints-co-jp.domo.com")
        api = DomoAPI(dummy_auth)
        bm_service = BeastModeService(api, db)
        data = bm_service.search_bm(q, limit)
        return {"data": data, "total": len(data)}
    finally:
        db.close()


# ─── Delete endpoint (CẦN LOGIN) ─────────────────────────

@router.delete("/{bm_id}")
async def delete_beastmode(bm_id: int):
    """Xóa BM bằng cách gỡ khỏi tất cả cards liên kết. CẦN LOGIN."""
    auth = require_auth()
    db = get_db()
    api = DomoAPI(auth)
    bm_service = BeastModeService(api, db)

    try:
        result = bm_service.delete_bm(bm_id)
        if not result["success"]:
            raise HTTPException(status_code=400, detail=result.get("error", "Xóa thất bại"))
        return result
    finally:
        db.close()


# ─── Cancel endpoint (CẦN LOGIN) ─────────────────────────

@router.post("/crawl/cancel")
async def cancel_crawl():
    """Hủy crawl đang chạy. CẦN LOGIN."""
    require_auth()
    crawl_cancel.set()

    db = get_db()
    try:
        db.execute(
            """UPDATE crawl_jobs SET status = 'cancelled', message = 'Đã hủy bởi người dùng',
               finished_at = %s WHERE status = 'running'""",
            (datetime.now(),)
        )
    finally:
        db.close()

    log.info("🛑 Crawl đã bị hủy bởi người dùng")
    _finish_progress("cancelled", "Crawl đã bị hủy")
    return {"message": "Đã gửi yêu cầu hủy crawl"}


# ─── WebSocket endpoint (REAL-TIME PROGRESS) ─────────────

@router.websocket("/ws/progress")
async def ws_progress(websocket: WebSocket):
    """WebSocket endpoint: nhận crawl progress real-time."""
    global _ws_loop
    await websocket.accept()
    _ws_loop = asyncio.get_event_loop()
    ws_clients.append(websocket)
    log.info(f"🔌 WebSocket connected (total: {len(ws_clients)})")

    try:
        while True:
            # Giữ connection sống, nhận ping/pong từ client
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        pass
    finally:
        if websocket in ws_clients:
            ws_clients.remove(websocket)
        log.info(f"🔌 WebSocket disconnected (total: {len(ws_clients)})")
