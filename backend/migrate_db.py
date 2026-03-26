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
        ("ALTER TABLE bm_card_map ALTER COLUMN card_id TYPE TEXT", "bm_card_map.card_id -> TEXT"),
        ("ALTER TABLE cards ALTER COLUMN id TYPE TEXT", "cards.id -> TEXT"),
        ("""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                               WHERE table_name='bm_analysis' AND column_name='legacy_id') THEN
                    ALTER TABLE bm_analysis ADD COLUMN legacy_id TEXT;
                END IF;
            END $$;
        """, "bm_analysis + legacy_id"),
        ("""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                               WHERE table_name='bm_analysis' AND column_name='normalized_hash') THEN
                    ALTER TABLE bm_analysis ADD COLUMN normalized_hash VARCHAR(12);
                END IF;
            END $$;
        """, "bm_analysis + normalized_hash"),
        ("""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                               WHERE table_name='bm_analysis' AND column_name='structure_hash') THEN
                    ALTER TABLE bm_analysis ADD COLUMN structure_hash VARCHAR(12);
                END IF;
            END $$;
        """, "bm_analysis + structure_hash"),
    ]

    # Datasets table — new columns for monitor
    dataset_new_cols = [
        ("column_count", "INTEGER DEFAULT 0"),
        ("data_flow_count", "INTEGER DEFAULT 0"),
        ("provider_type", "TEXT"),
        ("stream_id", "TEXT"),
        ("schedule_state", "TEXT"),
        ("updated_at", "TIMESTAMP DEFAULT NOW()"),
    ]
    for col_name, col_type in dataset_new_cols:
        migrations.append((f"""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                               WHERE table_name='datasets' AND column_name='{col_name}') THEN
                    ALTER TABLE datasets ADD COLUMN {col_name} {col_type};
                END IF;
            END $$;
        """, f"datasets + {col_name}"))

    # Dataflows table
    migrations.append(("""
        CREATE TABLE IF NOT EXISTS dataflows (
            id TEXT PRIMARY KEY,
            name TEXT,
            status TEXT,
            paused BOOLEAN DEFAULT FALSE,
            database_type TEXT,
            last_execution_time TIMESTAMP,
            last_execution_state TEXT,
            execution_count INTEGER DEFAULT 0,
            owner TEXT,
            output_dataset_count INTEGER DEFAULT 0,
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """, "CREATE dataflows"))

    # Monitor checks table
    migrations.append(("""
        CREATE TABLE IF NOT EXISTS monitor_checks (
            id SERIAL PRIMARY KEY,
            check_type TEXT NOT NULL,
            total_checked INTEGER DEFAULT 0,
            failed_count INTEGER DEFAULT 0,
            stale_count INTEGER DEFAULT 0,
            ok_count INTEGER DEFAULT 0,
            filters_json TEXT,
            details_json TEXT,
            checked_at TIMESTAMP DEFAULT NOW()
        )
    """, "CREATE monitor_checks"))

    # App settings table — key-value store for persistent config
    migrations.append(("""
        CREATE TABLE IF NOT EXISTS app_settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """, "CREATE app_settings"))

    for sql, label in migrations:
        try:
            db.execute(sql)
            print(f"  [OK] {label}")
        except Exception as e:
            print(f"  [WARN] {label}: {e}")

    db.close()
    print("Migration done!")

if __name__ == "__main__":
    run_migration()

