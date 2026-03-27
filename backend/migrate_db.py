"""migrate_db.py — Chay migration de cap nhat DB schema.

Usage:
    cd backend
    python migrate_db.py

Se tu dong doc ket noi tu .env hoac config mac dinh.
"""

import os
import sys
import psycopg2

# Doc config tu .env neu co
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

MIGRATIONS = [
    # v1: them datasource_id, datasource_name vao bang cards
    (
        "v1_cards_datasource",
        """
        ALTER TABLE cards
            ADD COLUMN IF NOT EXISTS datasource_id   VARCHAR(100),
            ADD COLUMN IF NOT EXISTS datasource_name TEXT;
        """,
    ),
    # v2: index cho datasource_id de query nhanh hon
    (
        "v2_cards_datasource_index",
        """
        CREATE INDEX IF NOT EXISTS idx_cards_datasource_id
            ON cards(datasource_id);
        """,
    ),
]


def run_migrations():
    print(f"Connecting to {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']} ...")
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()

    # Tao bang migration_log neu chua co
    cur.execute("""
        CREATE TABLE IF NOT EXISTS migration_log (
            id SERIAL PRIMARY KEY,
            name VARCHAR(200) UNIQUE NOT NULL,
            applied_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    for name, sql in MIGRATIONS:
        # Kiem tra da chay chua
        cur.execute("SELECT 1 FROM migration_log WHERE name = %s", (name,))
        if cur.fetchone():
            print(f"  [SKIP] {name} (already applied)")
            continue

        try:
            print(f"  [RUN]  {name} ...")
            cur.execute(sql)
            cur.execute("INSERT INTO migration_log(name) VALUES(%s)", (name,))
            print(f"  [OK]   {name}")
        except Exception as e:
            print(f"  [ERR]  {name}: {e}", file=sys.stderr)

    cur.close()
    conn.close()
    print("Done.")


if __name__ == "__main__":
    run_migrations()
