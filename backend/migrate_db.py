"""migrate_db.py — Quản lý migration cho PostgreSQL schema.

Chạy trực tiếp:
    cd backend
    python migrate_db.py

Hoặc import trong code:
    from migrate_db import run_migrations
"""

import os
import sys
import psycopg2


# ─── Đọc config từ .env ──────────────────────────────────

def load_env():
    env_file = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_file):
        with open(env_file, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip())

load_env()

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "dbname":   os.getenv("DB_NAME", "DOMO"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "test1234"),
}


# ─── Helper: SQL thêm column nếu chưa tồn tại ───────────

def _add_col(table: str, column: str, col_type: str) -> str:
    return f"""DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='{table}' AND column_name='{column}'
    ) THEN
        ALTER TABLE {table} ADD COLUMN {column} {col_type};
    END IF;
END $$;"""


# ─── Danh sách migrations ────────────────────────────────
# Mỗi entry: (id, description, list[sql])
# id phải duy nhất, KHÔNG được đổi sau khi deploy

MIGRATIONS = [
    # ── v1: cards thêm datasource ──
    (
        "v1_cards_datasource",
        "Thêm datasource_id, datasource_name vào cards",
        [
            """ALTER TABLE cards
                ADD COLUMN IF NOT EXISTS datasource_id   VARCHAR(100),
                ADD COLUMN IF NOT EXISTS datasource_name TEXT;""",
        ],
    ),

    # ── v2: index cards.datasource_id ──
    (
        "v2_cards_datasource_index",
        "Index cho cards.datasource_id",
        [
            """CREATE INDEX IF NOT EXISTS idx_cards_datasource_id
                ON cards(datasource_id);""",
        ],
    ),

    # ── v3: crawl_jobs thêm step tracking ──
    (
        "v3_crawl_jobs_step_columns",
        "Thêm step tracking columns vào crawl_jobs",
        [
            _add_col("crawl_jobs", "current_step",   "INTEGER DEFAULT 0"),
            _add_col("crawl_jobs", "total_steps",    "INTEGER DEFAULT 5"),
            _add_col("crawl_jobs", "step_name",      "TEXT DEFAULT ''"),
            _add_col("crawl_jobs", "step_processed", "INTEGER DEFAULT 0"),
            _add_col("crawl_jobs", "step_total",     "INTEGER DEFAULT 0"),
        ],
    ),

    # ── v4: datasets thêm columns mới ──
    (
        "v4_datasets_new_columns",
        "Thêm column_count, provider_type, stream_id, ... vào datasets",
        [
            _add_col("datasets", "column_count",         "INTEGER DEFAULT 0"),
            _add_col("datasets", "data_flow_count",      "INTEGER DEFAULT 0"),
            _add_col("datasets", "provider_type",        "TEXT"),
            _add_col("datasets", "stream_id",            "TEXT"),
            _add_col("datasets", "schedule_state",       "TEXT"),
            _add_col("datasets", "last_execution_state", "TEXT"),
            _add_col("datasets", "updated_at",           "TIMESTAMP DEFAULT NOW()"),
        ],
    ),

    # ── v5: bm_analysis thêm owner/cards/hash ──
    (
        "v5_bm_analysis_new_columns",
        "Thêm owner_name, card_ids, normalized_hash, structure_hash vào bm_analysis",
        [
            _add_col("bm_analysis", "owner_name",      "TEXT"),
            _add_col("bm_analysis", "card_ids",        "TEXT"),
            _add_col("bm_analysis", "normalized_hash", "TEXT"),
            _add_col("bm_analysis", "structure_hash",  "TEXT"),
        ],
    ),
]


# ─── Runner ───────────────────────────────────────────────

def run_migrations(conn=None):
    """Chạy tất cả migrations chưa được apply.

    Args:
        conn: psycopg2 connection (optional). Nếu None sẽ tự tạo.
    """
    own_conn = False
    if conn is None:
        print(f"🔌 Connecting to {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']} ...")
        conn = psycopg2.connect(**DB_CONFIG)
        own_conn = True

    conn.autocommit = True
    cur = conn.cursor()

    # Tạo bảng tracking nếu chưa có
    cur.execute("""
        CREATE TABLE IF NOT EXISTS migration_log (
            id SERIAL PRIMARY KEY,
            name VARCHAR(200) UNIQUE NOT NULL,
            description TEXT,
            applied_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    # Thêm column description nếu bảng cũ chưa có
    cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                           WHERE table_name='migration_log' AND column_name='description') THEN
                ALTER TABLE migration_log ADD COLUMN description TEXT;
            END IF;
        END $$;
    """)

    # Lấy danh sách đã apply
    cur.execute("SELECT name FROM migration_log")
    applied = {row[0] for row in cur.fetchall()}

    new_count = 0
    for mid, desc, sql_list in MIGRATIONS:
        if mid in applied:
            print(f"  [SKIP] {mid}")
            continue

        try:
            print(f"  [RUN]  {mid}: {desc}")
            for sql in sql_list:
                cur.execute(sql)
            cur.execute(
                "INSERT INTO migration_log(name, description) VALUES(%s, %s)",
                (mid, desc),
            )
            print(f"  [OK]   {mid}")
            new_count += 1
        except Exception as e:
            print(f"  [ERR]  {mid}: {e}", file=sys.stderr)

    cur.close()
    if own_conn:
        conn.close()

    if new_count == 0:
        print("✅ Tất cả migrations đã được apply")
    else:
        print(f"🎉 Đã apply {new_count} migration(s)")


# ─── CLI ──────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 50)
    print("🔧 DOMO Toolkit — Database Migration")
    print("=" * 50)
    run_migrations()
    print("=" * 50)
    print("✅ Done!")
