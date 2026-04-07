import sys
import asyncio

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings
from app.core.database import engine, Base
import app.models  # Load all models for metadata

from app.routers import auth, beastmode, monitor, backlog, card

app = FastAPI(
    title="DOMO Toolkit",
    description="Quản lý và phân tích Beast Mode, Card, DataFlow trong Domo",
    version="0.2.0",
)

# CORS cho frontend (đọc từ .env hoặc mặc định localhost)
settings = get_settings()
cors_origins = [
    o.strip() for o in settings.cors_origins.split(",") if o.strip()
] if settings.cors_origins else [
    "http://localhost:5173",
    "http://localhost:5174",
    "http://localhost:5175",
    "http://localhost:3000",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(auth.router)
app.include_router(beastmode.router)
app.include_router(monitor.router)
app.include_router(backlog.router)
app.include_router(card.router)


@app.on_event("startup")
def startup():
    """Tạo bảng DB khi khởi động + auto-login nếu có cấu hình."""
    settings = get_settings()
    
    Base.metadata.create_all(bind=engine)
    print(f"✅ DB schema initialized ({settings.db_name}) with SQLAlchemy")

    from app.services.bm_crawler import cleanup_stale_jobs
    cleanup_stale_jobs()

    from app.scheduler import init_scheduler
    init_scheduler()

    if settings.domo_username and settings.domo_password:
        from app.repositories.auth_repo import AuthRepository
        from app.core.database import SessionLocal
        with SessionLocal() as db:
            repo = AuthRepository(db)
            result = repo.login(settings.domo_username, settings.domo_password)
            if result.success:
                print(f"✅ Auto-login DOMO thành công: {result.username}")
            else:
                print(f"⚠️ Auto-login DOMO thất bại: {result.message}")


@app.on_event("shutdown")
def shutdown():
    from app.scheduler import shutdown_scheduler
    shutdown_scheduler()
    print("🛑 Đóng Scheduler thành công.")

@app.get("/api/health")
def health():
    return {"status": "ok"}


def start():
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)


if __name__ == "__main__":
    start()
