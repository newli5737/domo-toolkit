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
    migrations = [
        ("ALTER TABLE bm_card_map ALTER COLUMN card_id TYPE TEXT", "bm_card_map.card_id → TEXT"),
        ("ALTER TABLE cards ALTER COLUMN id TYPE TEXT", "cards.id → TEXT"),
        ("""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                               WHERE table_name='bm_analysis' AND column_name='legacy_id') THEN
                    ALTER TABLE bm_analysis ADD COLUMN legacy_id TEXT;
                END IF;
            END $$;
        """, "bm_analysis + legacy_id"),
    ]

    for sql, label in migrations:
        try:
            db.execute(sql)
            print(f"  ✅ {label}")
        except Exception as e:
            print(f"  ⚠️ {label}: {e}")

    db.close()
    print("Migration hoàn tất!")

if __name__ == "__main__":
    run_migration()

