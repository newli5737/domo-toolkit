import sys
import asyncio

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings
from app.core.db import DomoDatabase
from app.routers import auth, beastmode, monitor, backlog

app = FastAPI(
    title="DOMO Toolkit",
    description="Quản lý và phân tích Beast Mode, Card, DataFlow trong Domo",
    version="0.2.0",
)

# CORS cho frontend dev server
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://localhost:5174",
        "http://localhost:5175",
        "http://localhost:3000"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(auth.router)
app.include_router(beastmode.router)
app.include_router(monitor.router)
app.include_router(backlog.router)


@app.on_event("startup")
def startup():
    """Tạo bảng DB khi khởi động + auto-login nếu có cấu hình."""
    settings = get_settings()
    db = DomoDatabase(
        host=settings.db_host, port=settings.db_port,
        dbname=settings.db_name, user=settings.db_user,
        password=settings.db_password,
    )
    db.init_schema()
    db.close()
    print(f"✅ DB schema initialized ({settings.db_name})")

    # Hủy các crawl job cũ đang bị treo
    from app.routers.beastmode import cleanup_stale_jobs
    cleanup_stale_jobs()

    # Auto-login nếu có credentials trong .env
    if settings.domo_username and settings.domo_password:
        from app.routers.auth import get_auth, _save_session
        auth_inst = get_auth()
        result = auth_inst.login(settings.domo_username, settings.domo_password)
        if result["success"]:
            _save_session(auth_inst)
            print(f"✅ Auto-login thành công: {auth_inst.username}")
        else:
            print(f"⚠️ Auto-login thất bại: {result['message']}")


@app.get("/api/health")
def health():
    return {"status": "ok"}


def start():
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)


if __name__ == "__main__":
    start()
