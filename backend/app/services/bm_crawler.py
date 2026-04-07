import time
import json
import asyncio
import threading
from datetime import datetime

from app.core.api import DomoAPI
from app.core.database import SessionLocal
from app.core.logger import DomoLogger
from app.repositories.auth_repo import get_auth
from app.repositories.beastmode_repo import BeastModeRepository
from app.services.beastmode import BeastModeService
from app.services.card import CardService

log = DomoLogger("bm-crawler")

def cleanup_stale_jobs():
    try:
        db = SessionLocal()
        BeastModeRepository(db).cancel_stale_jobs()
        db.close()
    except Exception as e:
        log.warn(f"Cleanup stale jobs lỗi: {e}")

# ─── Global State & Synchronization ─────────────────────────────────
crawl_cancel = threading.Event()
ws_clients = []
_ws_loop: asyncio.AbstractEventLoop | None = None

crawl_progress: dict = {
    "status": "idle", "job_id": None, "started_at": None,
    "elapsed": 0, "message": "", "steps": {}
}
_progress_lock = threading.Lock()

TOTAL_STEPS = 5
STEP_NAMES = {
    1: "Crawl Beast Mode", 2: "Fetch BM Details",
    3: "Crawl Cards", 4: "Fetch View Counts", 5: "Phân tích & phân loại",
}

def register_ws_client(ws, loop):
    global _ws_loop
    _ws_loop = loop
    ws_clients.append(ws)

def unregister_ws_client(ws):
    if ws in ws_clients:
        ws_clients.remove(ws)

def _broadcast_progress():
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

def _init_progress(job_id: int):
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
    with _progress_lock:
        crawl_progress["status"] = status
        crawl_progress["message"] = message
        if crawl_progress["started_at"]:
            crawl_progress["elapsed"] = round(time.time() - crawl_progress["started_at"])
    _broadcast_progress()


# ─── Background Tasks ──────────────────────────────────────────────

def run_full_crawl(job_id: int):
    """Background: 5 steps pipeline song song."""
    import queue as queue_mod

    db = SessionLocal()
    auth = get_auth(db)
    api = DomoAPI(auth)
    bm_service = BeastModeService(api, db)

    step_times = {}
    crawl_start = time.time()
    errors_list = []
    crawl_cancel.clear()
    _init_progress(job_id)

    log.info("=" * 60)
    log.info(f"🚀 BẮT ĐẦU CRAWL JOB #{job_id} (PIPELINE MODE)")

    try:
        repo = BeastModeRepository(db, auth)
        repo.update_job_status(job_id, status='running', started_at=datetime.now(), total_steps=TOTAL_STEPS)
        repo.truncate_tables(["bm_analysis", "bm_card_map", "beastmodes", "cards"])

        bm_queue = queue_mod.Queue()
        step1_done = threading.Event()

        def run_step1_and_step2():
            s1_start = time.time()
            try:
                db_a = SessionLocal()
                api_a = DomoAPI(auth)
                bm_a = BeastModeService(api_a, db_a)

                step2_processed = [0]
                step2_total = [0]

                def step2_consumer():
                    db_s2 = SessionLocal()
                    api_s2 = DomoAPI(auth)
                    bm_s2 = BeastModeService(api_s2, db_s2)
                    s2_start = time.time()

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

                t_s2 = threading.Thread(target=step2_consumer, daemon=True)
                t_s2.start()

                _update_progress(1, 0, 0, message="Đang crawl Beast Mode...")
                all_bms = bm_a.crawl_all(
                    job_id=job_id,
                    progress_callback=lambda p, t: _update_progress(1, p, t),
                    on_batch_callback=lambda ids: bm_queue.put(ids),
                )
                step_times[1] = time.time() - s1_start
                _update_progress(1, len(all_bms), len(all_bms), status="done")

                step1_done.set()
                bm_queue.put(None)
                t_s2.join()
                db_a.close()
            except Exception as e:
                step1_done.set()
                errors_list.append(("Step 1+2", e))

        def run_step34():
            try:
                db_b = SessionLocal()
                api_b = DomoAPI(auth)
                card_b = CardService(api_b, db_b)

                _update_progress(3, 0, 0, message="Đang crawl cards...")
                all_cards = card_b.crawl_all_cards(
                    progress_callback=lambda p, t: _update_progress(3, p, t),
                )
                card_b.save_cards_from_summary(all_cards)
                _update_progress(3, len(all_cards), len(all_cards), status="done")

                urns = card_b.get_all_urns()
                _update_progress(4, 0, len(urns))
                card_b.fetch_view_counts(
                    urns, progress_callback=lambda p, t: _update_progress(4, p, t),
                )
                _update_progress(4, len(urns), len(urns), status="done")
                db_b.close()
            except Exception as e:
                errors_list.append(("Step 3+4", e))

        t_a = threading.Thread(target=run_step1_and_step2, daemon=True)
        t_b = threading.Thread(target=run_step34, daemon=True)
        t_a.start()
        t_b.start()
        t_a.join()
        t_b.join()

        if errors_list:
            raise Exception("; ".join(f"{n}: {e}" for n, e in errors_list))

        # Step 5: Analyze
        _update_progress(5, 0, 1, message="Đang phân tích...")
        summary = bm_service.analyze()
        _update_progress(5, 1, 1, status="done")

        total_time = time.time() - crawl_start
        repo.update_job_status(
            job_id, status='done', finished_at=datetime.now(),
            message=f"Hoàn tất: {summary['total']} BM", found=summary["total"], current_step=TOTAL_STEPS
        )
        _finish_progress("done", f"Hoàn tất: {summary['total']} BM")
        log.success(f"🎉 CRAWL HOÀN TẤT! {summary['total']} BM ({total_time:.1f}s)")

    except Exception as e:
        log.exception("❌ CRAWL LỖI", e)
        BeastModeRepository(db).update_job_status(
            job_id, status='error', message=str(e)[:500], finished_at=datetime.now()
        )
        _finish_progress("error", str(e)[:500])
    finally:
        db.close()


def run_view_and_analyze(job_id: int, low_view_threshold: int = 10):
    """Background: phân tích lại từ DB (không crawl API)."""
    db = SessionLocal()
    auth = get_auth(db)
    api = DomoAPI(auth)
    repo = BeastModeRepository(db, auth)
    bm_service = BeastModeService(api, db)
    _init_progress(job_id)

    try:
        repo.update_job_status(job_id, status='running', started_at=datetime.now(), total_steps=1)
        _update_progress(5, 0, 1, message=f"Đang phân tích (threshold={low_view_threshold})...")
        summary = bm_service.analyze(low_view_threshold=low_view_threshold)
        _update_progress(5, 1, 1, status="done")

        repo.update_job_status(
            job_id, status='done', finished_at=datetime.now(),
            message=f"Hoàn tất: {summary['total']} BM", found=summary["total"], current_step=1
        )
        _finish_progress("done", f"Hoàn tất: {summary['total']} BM")
    except Exception as e:
        repo.update_job_status(
            job_id, status='error', message=str(e)[:500], finished_at=datetime.now()
        )
        _finish_progress("error", str(e)[:500])
    finally:
        db.close()


def run_retry_details(job_id: int):
    """Background: retry fetch BM details thiếu expression."""
    db = SessionLocal()
    auth = get_auth(db)
    api = DomoAPI(auth)
    repo = BeastModeRepository(db, auth)
    bm_service = BeastModeService(api, db)
    crawl_cancel.clear()

    missing_ids = repo.get_missing_expression_bm_ids()

    _init_progress(job_id)
    for s in [1, 3, 4]:
        with _progress_lock:
            crawl_progress["steps"][s]["status"] = "done"
            crawl_progress["steps"][s]["percent"] = 100

    try:
        repo.update_job_status(job_id, status='running', started_at=datetime.now(), total_steps=2)

        if missing_ids:
            _update_progress(2, 0, len(missing_ids), message=f"Retry {len(missing_ids)} BM details...")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(bm_service.fetch_details_batch(
                    missing_ids, concurrency=50,
                    progress_callback=lambda p, t: _update_progress(2, p, t),
                ))
            finally:
                loop.close()
            _update_progress(2, len(missing_ids), len(missing_ids), status="done")
        else:
            _update_progress(2, 0, 0, status="done")

        _update_progress(5, 0, 1, message="Đang phân tích...")
        summary = bm_service.analyze()
        _update_progress(5, 1, 1, status="done")

        repo.update_job_status(
            job_id, status='done', finished_at=datetime.now(),
            message=f"Retry hoàn tất: {summary['total']} BM", found=summary["total"]
        )
        _finish_progress("done", f"Retry hoàn tất: {summary['total']} BM")
    except Exception as e:
        repo.update_job_status(
            job_id, status='error', message=str(e)[:500], finished_at=datetime.now()
        )
        _finish_progress("error", str(e)[:500])
    finally:
        db.close()


def run_bm_only_crawl(job_id: int):
    """Background: chỉ Step 1+2 (BM crawl + details), không crawl cards."""
    import queue as queue_mod

    db = SessionLocal()
    auth = get_auth(db)
    api = DomoAPI(auth)
    repo = BeastModeRepository(db, auth)
    bm_service = BeastModeService(api, db)
    step_times = {}
    crawl_start = time.time()
    crawl_cancel.clear()
    _init_progress(job_id)

    with _progress_lock:
        crawl_progress["steps"] = {
            1: {"name": STEP_NAMES[1], "status": "pending", "processed": 0, "total": 0, "percent": 0},
            2: {"name": STEP_NAMES[2], "status": "pending", "processed": 0, "total": 0, "percent": 0},
        }

    try:
        repo.update_job_status(job_id, status='running', started_at=datetime.now(), total_steps=2)
        repo.truncate_tables(["bm_analysis", "bm_card_map", "beastmodes"])

        bm_queue = queue_mod.Queue()
        step1_done = threading.Event()
        step2_processed = [0]
        step2_total = [0]

        def step2_consumer():
            db_s2 = SessionLocal()
            api_s2 = DomoAPI(auth)
            bm_s2 = BeastModeService(api_s2, db_s2)
            while not crawl_cancel.is_set():
                try:
                    batch_ids = bm_queue.get(timeout=2)
                except Exception:
                    if step1_done.is_set() and bm_queue.empty():
                        break
                    continue
                if batch_ids is None:
                    break
                step2_total[0] += len(batch_ids)
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(bm_s2.fetch_details_batch(batch_ids))
                finally:
                    loop.close()
                step2_processed[0] += len(batch_ids)
                _update_progress(2, step2_processed[0], step2_total[0])
            db_s2.close()
            _update_progress(2, step2_processed[0], step2_total[0], status="done")

        t_s2 = threading.Thread(target=step2_consumer, daemon=True)
        t_s2.start()

        _update_progress(1, 0, 0, message="Đang crawl Beast Mode...")
        all_bms = bm_service.crawl_all(
            job_id=job_id,
            progress_callback=lambda p, t: _update_progress(1, p, t),
            on_batch_callback=lambda ids: bm_queue.put(ids),
        )
        _update_progress(1, len(all_bms), len(all_bms), status="done")

        step1_done.set()
        bm_queue.put(None)
        t_s2.join()

        total_time = time.time() - crawl_start
        repo.update_job_status(
            job_id, status='done', finished_at=datetime.now(),
            message=f"BM-only hoàn tất: {len(all_bms)} BM", found=len(all_bms), current_step=2
        )
        _finish_progress("done", f"BM-only hoàn tất: {len(all_bms)} BM")
    except Exception as e:
        repo.update_job_status(
            job_id, status='error', message=str(e)[:500], finished_at=datetime.now()
        )
        _finish_progress("error", str(e)[:500])
    finally:
        db.close()
