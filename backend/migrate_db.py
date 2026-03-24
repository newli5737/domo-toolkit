import asyncio
import os
import sys

# Thêm đường dẫn backend vào sys.path để có cấu trúc module
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.core.db import DomoDatabase
from app.config import get_settings

def run_migration():
    settings = get_settings()
    db = DomoDatabase(
        host=settings.db_host,
        port=settings.db_port,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password
    )
    
    print("Đang migrate database...")
    try:
        db.execute("ALTER TABLE bm_card_map ALTER COLUMN card_id TYPE TEXT;")
        db.execute("ALTER TABLE cards ALTER COLUMN id TYPE TEXT;")
        print("Migration OK!")
    except Exception as e:
        print(f"Lỗi migration (có thể table chưa có hoặc đã là TEXT): {e}")

if __name__ == "__main__":
    run_migration()
