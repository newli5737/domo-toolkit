"""DomoDatabase — PostgreSQL wrapper dùng psycopg2."""

import json
import psycopg2
import psycopg2.extras
from contextlib import contextmanager


class DomoDatabase:
    """PostgreSQL wrapper — upsert, bulk ops, và schema management."""

    def __init__(self, host: str, port: int, dbname: str, user: str, password: str):
        self._conn_params = {
            "host": host,
            "port": port,
            "dbname": dbname,
            "user": user,
            "password": password,
        }
        self._conn = None

    def connect(self):
        """Mở connection."""
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(**self._conn_params)
            self._conn.autocommit = False
        return self._conn

    def close(self):
        if self._conn and not self._conn.closed:
            self._conn.close()

    @contextmanager
    def cursor(self):
        """Context manager cho cursor."""
        conn = self.connect()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()

    def execute(self, sql: str, params=None):
        """Chạy SQL statement."""
        with self.cursor() as cur:
            cur.execute(sql, params)

    def query(self, sql: str, params=None) -> list[dict]:
        """Chạy SELECT, trả về list of dict."""
        with self.cursor() as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]

    def query_one(self, sql: str, params=None) -> dict | None:
        """Chạy SELECT, trả về 1 row hoặc None."""
        with self.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return dict(row) if row else None

    def count(self, table: str, where: str = None, params=None) -> int:
        """Đếm rows."""
        sql = f"SELECT COUNT(*) as cnt FROM {table}"
        if where:
            sql += f" WHERE {where}"
        result = self.query_one(sql, params)
        return result["cnt"] if result else 0

    def ensure_table(self, name: str, schema: str):
        """Tạo bảng nếu chưa tồn tại."""
        self.execute(f"CREATE TABLE IF NOT EXISTS {name} ({schema})")

    def upsert(self, table: str, data: dict, pk: str):
        """INSERT ... ON CONFLICT (pk) DO UPDATE."""
        columns = list(data.keys())
        values = list(data.values())
        placeholders = ", ".join(["%s"] * len(columns))
        cols_str = ", ".join(columns)
        update_cols = [c for c in columns if c != pk]
        update_str = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

        sql = f"""
            INSERT INTO {table} ({cols_str})
            VALUES ({placeholders})
            ON CONFLICT ({pk}) DO UPDATE SET {update_str}
        """
        with self.cursor() as cur:
            cur.execute(sql, values)

    def bulk_upsert(self, table: str, rows: list[dict], pk: str):
        """Batch upsert nhiều rows."""
        if not rows:
            return

        columns = list(rows[0].keys())
        cols_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        update_cols = [c for c in columns if c != pk]
        update_str = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

        sql = f"""
            INSERT INTO {table} ({cols_str})
            VALUES ({placeholders})
            ON CONFLICT ({pk}) DO UPDATE SET {update_str}
        """

        with self.cursor() as cur:
            values_list = [tuple(row.get(c) for c in columns) for row in rows]
            psycopg2.extras.execute_batch(cur, sql, values_list, page_size=500)

    def truncate(self, table: str):
        """Xoá toàn bộ data trong bảng."""
        self.execute(f"TRUNCATE TABLE {table}")

    def init_schema(self):
        """Tạo toàn bộ bảng cần thiết."""

        # Session Domo
        self.ensure_table("domo_sessions", """
            id SERIAL PRIMARY KEY,
            username TEXT NOT NULL,
            cookies_json TEXT,
            csrf_token TEXT,
            logged_in_at TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE
        """)

        # Beast Modes
        self.ensure_table("beastmodes", """
            id BIGINT PRIMARY KEY,
            name TEXT,
            owner_id BIGINT,
            legacy_id TEXT,
            expression TEXT,
            column_positions TEXT,
            datasources TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        """)

        # Beast Mode ↔ Card mapping
        self.ensure_table("bm_card_map", """
            bm_id BIGINT NOT NULL,
            card_id TEXT NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            PRIMARY KEY (bm_id, card_id)
        """)

        # Beast Mode ↔ Beast Mode dependency
        self.ensure_table("bm_dependency_map", """
            bm_id BIGINT NOT NULL,
            depends_on_bm_id BIGINT NOT NULL,
            PRIMARY KEY (bm_id, depends_on_bm_id)
        """)

        # Cards
        self.ensure_table("cards", """
            id TEXT PRIMARY KEY,
            title TEXT,
            card_type TEXT,
            view_count INTEGER DEFAULT 0,
            last_viewed_at TIMESTAMP,
            owner_name TEXT,
            page_id BIGINT,
            page_title TEXT
        """)

        # Datasets
        self.ensure_table("datasets", """
            id TEXT PRIMARY KEY,
            name TEXT,
            row_count BIGINT,
            card_count INTEGER,
            last_updated TIMESTAMP
        """)

        # Kết quả phân tích BM
        self.ensure_table("bm_analysis", """
            bm_id BIGINT PRIMARY KEY,
            group_number INTEGER,
            group_label TEXT,
            active_cards_count INTEGER DEFAULT 0,
            total_views INTEGER DEFAULT 0,
            referenced_by_count INTEGER DEFAULT 0,
            dataset_names TEXT,
            naming_flag TEXT,
            complexity_score INTEGER DEFAULT 0,
            duplicate_hash TEXT,
            url TEXT
        """)

        # Crawl jobs tracking
        self.ensure_table("crawl_jobs", """
            id SERIAL PRIMARY KEY,
            job_type TEXT,
            status TEXT DEFAULT 'pending',
            total INTEGER DEFAULT 0,
            processed INTEGER DEFAULT 0,
            found INTEGER DEFAULT 0,
            errors INTEGER DEFAULT 0,
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            message TEXT,
            current_step INTEGER DEFAULT 0,
            total_steps INTEGER DEFAULT 5,
            step_name TEXT DEFAULT '',
            step_processed INTEGER DEFAULT 0,
            step_total INTEGER DEFAULT 0
        """)

        # Migrate: add step columns if missing
        try:
            self.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                                   WHERE table_name='crawl_jobs' AND column_name='current_step') THEN
                        ALTER TABLE crawl_jobs ADD COLUMN current_step INTEGER DEFAULT 0;
                        ALTER TABLE crawl_jobs ADD COLUMN total_steps INTEGER DEFAULT 5;
                        ALTER TABLE crawl_jobs ADD COLUMN step_name TEXT DEFAULT '';
                        ALTER TABLE crawl_jobs ADD COLUMN step_processed INTEGER DEFAULT 0;
                        ALTER TABLE crawl_jobs ADD COLUMN step_total INTEGER DEFAULT 0;
                    END IF;
                END $$;
            """)
        except Exception:
            pass
