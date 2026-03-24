"""BeastModeService — Crawl, fetch chi tiết, và phân tích Beast Mode."""

import re
import json
import hashlib
import asyncio
from datetime import datetime
from aiohttp import ClientSession

from app.core.api import DomoAPI
from app.core.db import DomoDatabase
from app.core.logger import DomoLogger

log = DomoLogger("beastmode")


class BeastModeService:
    """Xử lý toàn bộ logic liên quan đến Beast Mode."""

    SEARCH_URL = "/api/query/v1/functions/search"
    DETAIL_URL = "/api/query/v1/functions/template"

    # Patterns tên có vấn đề
    BAD_NAME_PATTERNS = ["test", "tmp", "copy", "backup", "old", "sample", "dummy", "コピー", "テスト"]

    def __init__(self, api: DomoAPI, db: DomoDatabase):
        self.api = api
        self.db = db

    # ─── Crawl tất cả BM ─────────────────────────────────────

    def crawl_all(self, job_id: int = None) -> list[dict]:
        """Crawl toàn bộ Beast Mode qua search API."""
        all_bms = []
        offset = 0
        batch_size = 1000

        while True:
            payload = {
                "name": "",
                "filters": [{"field": "notvariable"}],
                "sort": {"field": "name", "ascending": True},
                "limit": batch_size,
                "offset": offset,
            }

            resp = self.api.post(self.SEARCH_URL, json=payload)
            if not resp or resp.status_code != 200:
                log.error(f"Search thất bại tại offset {offset}")
                break

            data = resp.json()
            results = data.get("results", [])
            total_hits = data.get("totalHits", 0)

            if not results:
                break

            # Parse và lưu DB
            rows_bm = []
            rows_card_map = []
            for bm in results:
                bm_id = bm.get("id")
                cards = []
                datasources = []
                for link in bm.get("links", []):
                    resource = link.get("resource", {})
                    if resource.get("type") == "CARD" and link.get("active"):
                        cards.append(resource.get("id"))
                        rows_card_map.append({
                            "bm_id": bm_id,
                            "card_id": resource.get("id"),
                            "is_active": True,
                        })
                    elif resource.get("type") == "DATA_SOURCE":
                        datasources.append(resource.get("id"))

                rows_bm.append({
                    "id": bm_id,
                    "name": bm.get("name"),
                    "owner_id": bm.get("owner"),
                    "datasources": json.dumps(datasources),
                })

            if rows_bm:
                self.db.bulk_upsert("beastmodes", rows_bm, "id")
            if rows_card_map:
                # Xoá mapping cũ rồi insert lại
                for row in rows_card_map:
                    self.db.upsert("bm_card_map", row, "bm_id, card_id")

            all_bms.extend(results)

            # Cập nhật progress
            if job_id:
                self.db.execute(
                    "UPDATE crawl_jobs SET processed = %s, total = %s WHERE id = %s",
                    (len(all_bms), total_hits, job_id)
                )

            log.info(f"Crawl BM: {len(all_bms)}/{total_hits}")

            if len(results) < batch_size:
                break
            offset += batch_size

        return all_bms

    # ─── Fetch chi tiết (async) ───────────────────────────────

    async def fetch_details_batch(self, bm_ids: list[int], job_id: int = None, concurrency: int = 50):
        """Fetch chi tiết từng BM bằng async."""
        sem = asyncio.Semaphore(concurrency)
        total = len(bm_ids)
        processed = 0
        dep_rows = []
        update_rows = []

        async with self.api.create_async_session() as session:

            async def fetch_one(bm_id: int):
                nonlocal processed
                async with sem:
                    url = f"{self.api.base_url}{self.DETAIL_URL}/{bm_id}?hidden=true"
                    detail = await self.api.async_get(session, url)

                    if detail:
                        expression = detail.get("expression", "")
                        legacy_id = detail.get("legacyId", "")
                        col_positions = json.dumps(detail.get("columnPositions", []))

                        update_rows.append({
                            "id": bm_id,
                            "expression": expression,
                            "legacy_id": legacy_id,
                            "column_positions": col_positions,
                        })

                        # Parse BM dependencies
                        deps = re.findall(r'DOMO_BEAST_MODE\((\d+)\)', expression)
                        for dep_id in deps:
                            dep_rows.append({
                                "bm_id": bm_id,
                                "depends_on_bm_id": int(dep_id),
                            })

                    processed += 1

            # Chạy theo batch
            batch_size = 200
            for i in range(0, total, batch_size):
                batch = bm_ids[i:i + batch_size]
                tasks = [fetch_one(bm_id) for bm_id in batch]
                await asyncio.gather(*tasks, return_exceptions=True)

                # Lưu DB theo batch
                if update_rows:
                    for row in update_rows:
                        self.db.execute(
                            """UPDATE beastmodes
                               SET expression = %s, legacy_id = %s, column_positions = %s
                               WHERE id = %s""",
                            (row["expression"], row["legacy_id"], row["column_positions"], row["id"])
                        )
                    update_rows.clear()

                if dep_rows:
                    for row in dep_rows:
                        self.db.execute(
                            """INSERT INTO bm_dependency_map (bm_id, depends_on_bm_id)
                               VALUES (%s, %s) ON CONFLICT DO NOTHING""",
                            (row["bm_id"], row["depends_on_bm_id"])
                        )
                    dep_rows.clear()

                if job_id:
                    self.db.execute(
                        "UPDATE crawl_jobs SET processed = %s WHERE id = %s",
                        (processed, job_id)
                    )

                log.info(f"Details: {processed}/{total}")

    # ─── Phân tích 4 nhóm ────────────────────────────────────

    def analyze(self, low_view_threshold: int = 10) -> dict:
        """Phân loại tất cả BM thành 4 nhóm."""

        # Lấy tất cả BM
        all_bms = self.db.query("SELECT id, name, expression, datasources, legacy_id FROM beastmodes")

        # Map: bm_id → list card_ids (active)
        card_maps = self.db.query("SELECT bm_id, card_id FROM bm_card_map WHERE is_active = TRUE")
        bm_cards = {}
        for row in card_maps:
            bm_cards.setdefault(row["bm_id"], []).append(row["card_id"])

        # Map: bm_id → list of BM IDs tham chiếu tới nó
        dep_maps = self.db.query("SELECT bm_id, depends_on_bm_id FROM bm_dependency_map")
        referenced_by = {}
        for row in dep_maps:
            referenced_by.setdefault(row["depends_on_bm_id"], []).append(row["bm_id"])

        # Map: card_id → view_count
        cards = self.db.query("SELECT id, view_count FROM cards")
        card_views = {row["id"]: row["view_count"] or 0 for row in cards}

        # Phân loại
        results = []
        group_counts = {1: 0, 2: 0, 3: 0, 4: 0}

        for bm in all_bms:
            bm_id = bm["id"]
            active_cards = bm_cards.get(bm_id, [])
            refs = referenced_by.get(bm_id, [])
            total_views = sum(card_views.get(c, 0) for c in active_cards)

            has_cards = len(active_cards) > 0
            has_refs = len(refs) > 0

            if not has_cards and not has_refs and total_views == 0:
                group = 1
                label = "Không sử dụng"
            elif not has_cards and not has_refs and total_views > 0:
                group = 2
                label = "Từng được dùng"
            elif has_cards and total_views <= low_view_threshold:
                group = 3
                label = "Card ít xem"
            else:
                group = 4
                label = "Đang hoạt động"

            group_counts[group] += 1

            # Naming flag
            naming_flag = self._check_naming(bm["name"] or "")

            # Complexity
            complexity = self._calc_complexity(bm.get("expression") or "")

            # Duplicate hash
            expr = (bm.get("expression") or "").strip()
            dup_hash = hashlib.md5(expr.encode()).hexdigest()[:12] if expr else ""

            # Dataset names
            try:
                ds_ids = json.loads(bm.get("datasources") or "[]")
            except:
                ds_ids = []

            result_row = {
                "bm_id": bm_id,
                "group_number": group,
                "group_label": label,
                "active_cards_count": len(active_cards),
                "total_views": total_views,
                "referenced_by_count": len(refs),
                "dataset_names": ", ".join(str(d) for d in ds_ids[:5]),
                "naming_flag": naming_flag,
                "complexity_score": complexity,
                "duplicate_hash": dup_hash,
                "url": f"https://{self.api.auth.instance}/datacenter/beastmode?id={bm_id}",
            }
            results.append(result_row)

        # Lưu DB
        if results:
            self.db.execute("TRUNCATE TABLE bm_analysis")
            self.db.bulk_upsert("bm_analysis", results, "bm_id")

        # Thống kê duplicate
        duplicates = self._find_duplicates()

        # Thống kê per dataset
        dataset_stats = self._dataset_stats()

        return {
            "total": len(all_bms),
            "groups": group_counts,
            "duplicates_count": len(duplicates),
            "naming_issues_count": sum(1 for r in results if r["naming_flag"]),
            "dataset_stats": dataset_stats[:10],
        }

    def get_group_data(self, group_number: int, limit: int = 100, offset: int = 0) -> list[dict]:
        """Lấy danh sách BM theo nhóm."""
        return self.db.query(
            """SELECT a.*, b.name as bm_name
               FROM bm_analysis a
               JOIN beastmodes b ON a.bm_id = b.id
               WHERE a.group_number = %s
               ORDER BY a.total_views DESC
               LIMIT %s OFFSET %s""",
            (group_number, limit, offset)
        )

    def get_summary(self) -> dict:
        """Lấy tổng hợp kết quả phân tích."""
        groups = self.db.query(
            """SELECT group_number, group_label, COUNT(*) as count
               FROM bm_analysis GROUP BY group_number, group_label ORDER BY group_number"""
        )
        total = self.db.count("bm_analysis")

        duplicates = self._find_duplicates()
        naming_issues = self.db.count("bm_analysis", "naming_flag IS NOT NULL AND naming_flag != ''")
        dataset_stats = self._dataset_stats()

        return {
            "total": total,
            "groups": [dict(g) for g in groups],
            "duplicates": duplicates[:20],
            "naming_issues_count": naming_issues,
            "top_dirty_datasets": dataset_stats[:10],
        }

    # ─── Helpers ──────────────────────────────────────────────

    def _check_naming(self, name: str) -> str:
        name_lower = name.lower()
        for pattern in self.BAD_NAME_PATTERNS:
            if pattern in name_lower:
                return pattern
        if len(name) <= 3:
            return "tên quá ngắn"
        return ""

    def _calc_complexity(self, expression: str) -> int:
        if not expression:
            return 0
        score = 0
        score += min(len(expression) // 100, 5)  # Độ dài
        score += len(re.findall(r'DOMO_BEAST_MODE\(\d+\)', expression))  # Nested BM
        score += len(re.findall(r'CASE\s+WHEN', expression, re.IGNORECASE))  # CASE WHEN
        return score

    def _find_duplicates(self) -> list[dict]:
        """Tìm các nhóm BM có expression giống nhau."""
        return self.db.query(
            """SELECT duplicate_hash, COUNT(*) as cnt,
                      ARRAY_AGG(bm_id) as bm_ids
               FROM bm_analysis
               WHERE duplicate_hash != '' AND duplicate_hash IS NOT NULL
               GROUP BY duplicate_hash
               HAVING COUNT(*) > 1
               ORDER BY cnt DESC
               LIMIT 50"""
        )

    def _dataset_stats(self) -> list[dict]:
        """Thống kê BM rác theo dataset."""
        return self.db.query(
            """SELECT dataset_names, COUNT(*) as total,
                      SUM(CASE WHEN group_number = 1 THEN 1 ELSE 0 END) as unused,
                      SUM(CASE WHEN group_number IN (1,2,3) THEN 1 ELSE 0 END) as cleanup_candidates
               FROM bm_analysis
               WHERE dataset_names != ''
               GROUP BY dataset_names
               ORDER BY unused DESC
               LIMIT 20"""
        )

    def export_csv(self) -> list[dict]:
        """Lấy toàn bộ data cho CSV export."""
        return self.db.query(
            """SELECT a.bm_id, b.name as bm_name, b.legacy_id,
                      a.group_number, a.group_label,
                      a.active_cards_count, a.total_views,
                      a.referenced_by_count, a.dataset_names,
                      a.naming_flag, a.complexity_score,
                      a.duplicate_hash, a.url
               FROM bm_analysis a
               JOIN beastmodes b ON a.bm_id = b.id
               ORDER BY a.group_number, a.total_views DESC"""
        )
