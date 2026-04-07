"""BeastModeService — Crawl, fetch chi tiết, và phân tích Beast Mode."""

import re
import json
import hashlib
import asyncio
import time
from datetime import datetime
from aiohttp import ClientSession

from app.core.api import DomoAPI
from sqlalchemy.orm import Session
from app.core.logger import DomoLogger

log = DomoLogger("beastmode")


class BeastModeService:
    """Xử lý toàn bộ logic liên quan đến Beast Mode."""

    SEARCH_URL = "/api/query/v1/functions/search"
    DETAIL_URL = "/api/query/v1/functions/template"
    USERS_URL = "/api/content/v3/users"



    def __init__(self, api: DomoAPI, db: Session):
        self.api = api
        self.db = db

    # ─── Crawl tất cả BM ─────────────────────────────────────

    def crawl_all(self, job_id: int = None, progress_callback=None,
                   on_batch_callback=None) -> list[dict]:
        """Crawl toàn bộ Beast Mode qua search API.
        on_batch_callback(bm_ids): gọi sau mỗi batch để pipeline xử lý ngay.
        """
        all_bms = []
        offset = 0
        batch_size = 1000

        log.info(f"Bắt đầu crawl BM (batch_size={batch_size})")

        while True:
            payload = {
                "name": "",
                "filters": [{"field": "notvariable"}],
                "sort": {"field": "name", "ascending": True},
                "limit": batch_size,
                "offset": offset,
            }

            batch_start = time.time()
            resp = self.api.post(self.SEARCH_URL, json=payload)
            api_time = time.time() - batch_start

            if not resp or resp.status_code != 200:
                status_code = resp.status_code if resp else "None"
                log.error(f"Search thất bại tại offset {offset} (status={status_code}, took {api_time:.1f}s)")
                break

            data = resp.json()
            results = data.get("results", [])
            total_hits = data.get("totalHits", 0)

            if not results:
                log.debug(f"Không còn results tại offset {offset}")
                break

            # Parse và lưu DB
            rows_bm = []
            rows_card_map = []
            batch_bm_ids = []
            for bm in results:
                bm_id = bm.get("id")
                batch_bm_ids.append(bm_id)
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

            db_start = time.time()
            if rows_bm or rows_card_map:
                from app.models.beastmode import BeastMode, BMCardMap
                for r in rows_bm:
                    self.db.merge(BeastMode(**r))
                for r in rows_card_map:
                    self.db.merge(BMCardMap(**r))
                self.db.commit()
            db_time = time.time() - db_start

            all_bms.extend(results)

            # Pipeline callback: gửi batch BM IDs cho Step 2 xử lý ngay
            if on_batch_callback and batch_bm_ids:
                on_batch_callback(batch_bm_ids)

            # Cập nhật progress
            if job_id:
                from app.models.monitor import CrawlJob
                from sqlalchemy import update
                self.db.execute(update(CrawlJob).where(CrawlJob.id == job_id).values(processed=len(all_bms), total=total_hits))
                self.db.commit()
            if progress_callback:
                progress_callback(len(all_bms), total_hits)

            log.progress(len(all_bms), total_hits, f"Crawl BM")
            log.debug(f"  API: {api_time:.1f}s | DB: {db_time:.1f}s | batch: {len(results)} rows | card_maps: {len(rows_card_map)}")

            if len(results) < batch_size:
                break
            offset += batch_size

        log.info(f"Crawl BM xong: tổng {len(all_bms)} BM")
        return all_bms

    # ─── Fetch chi tiết (async) ───────────────────────────────

    async def fetch_details_batch(self, bm_ids: list[int], job_id: int = None,
                                   concurrency: int = 50, progress_callback=None):
        """Fetch chi tiết từng BM bằng async. Retry failed IDs."""
        total = len(bm_ids)
        all_processed = 0
        dep_rows = []
        update_rows = []

        log.info(f"Bắt đầu fetch details cho {total} BM (concurrency={concurrency})")

        async with self.api.create_async_session() as session:

            async def run_fetch(ids_to_fetch: list[int], sem_limit: int):
                nonlocal all_processed
                sem = asyncio.Semaphore(sem_limit)
                batch_failed = []
                batch_errors = 0

                async def fetch_one(bm_id: int):
                    nonlocal all_processed, batch_errors
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

                            deps = re.findall(r'DOMO_BEAST_MODE\((\d+)\)', expression)
                            for dep_id in deps:
                                dep_rows.append({
                                    "bm_id": bm_id,
                                    "depends_on_bm_id": int(dep_id),
                                })
                        else:
                            batch_failed.append(bm_id)
                            batch_errors += 1

                        all_processed += 1

                batch_size = 200
                for i in range(0, len(ids_to_fetch), batch_size):
                    batch = ids_to_fetch[i:i + batch_size]
                    batch_start = time.time()

                    tasks = [fetch_one(bm_id) for bm_id in batch]
                    await asyncio.gather(*tasks, return_exceptions=True)

                    api_time = time.time() - batch_start

                    db_start = time.time()
                    if update_rows or dep_rows:
                        from sqlalchemy import update
                        from app.models.beastmode import BeastMode, BMDependencyMap
                        for r in update_rows:
                            self.db.execute(update(BeastMode).where(BeastMode.id == r["id"]).values(expression=r["expression"], legacy_id=r["legacy_id"], column_positions=r["column_positions"]))
                        update_rows.clear()
                        for r in dep_rows:
                            self.db.merge(BMDependencyMap(**r))
                        dep_rows.clear()
                        self.db.commit()
                    db_time = time.time() - db_start

                    if job_id:
                        from app.models.monitor import CrawlJob
                        from sqlalchemy import update
                        self.db.execute(update(CrawlJob).where(CrawlJob.id == job_id).values(processed=all_processed))
                        self.db.commit()
                    if progress_callback:
                        progress_callback(all_processed, total)

                    log.progress(all_processed, total, "BM Details")
                    log.debug(f"  API: {api_time:.1f}s | DB: {db_time:.1f}s | errors: {batch_errors}")

                return batch_failed

            # Lần đầu
            failed_ids = await run_fetch(bm_ids, concurrency)

            # Retry failed IDs (tối đa 3 lần, giảm concurrency)
            for retry_round in range(1, 4):
                if not failed_ids:
                    break
                retry_concurrency = max(5, concurrency // (retry_round * 2))
                log.warn(f"🔄 Retry lần {retry_round}: {len(failed_ids)} BM thất bại (concurrency={retry_concurrency})")
                await asyncio.sleep(3 * retry_round)
                all_processed -= len(failed_ids)  # Reset count cho retry
                failed_ids = await run_fetch(failed_ids, retry_concurrency)

            if failed_ids:
                log.error(f"❌ {len(failed_ids)} BM vẫn thất bại sau retry: {failed_ids[:20]}...")

        log.info(f"Fetch details xong: {all_processed}/{total} (failed: {len(failed_ids)})")

    # ─── Fetch User Names ─────────────────────────────────────

    def fetch_user_names(self, user_ids: list[int]) -> dict:
        """Batch-fetch user display names từ Domo API.
        GET /api/content/v3/users?id=123&id=456
        Returns: {user_id: display_name, ...}
        """
        result = {}
        if not user_ids:
            return result

        # Skip nếu không có auth (ví dụ reanalyze dùng dummy_auth)
        if not self.api.auth.is_valid:
            log.info("  ⚠️ Bỏ qua fetch_user_names: không có auth session")
            return result

        # Batch requests (50 IDs per batch)
        batch_size = 50
        for i in range(0, len(user_ids), batch_size):
            batch = user_ids[i:i + batch_size]
            params = "&".join(f"id={uid}" for uid in batch)
            url = f"{self.USERS_URL}?{params}"
            try:
                resp = self.api.get(url)
                if resp and resp.status_code == 200:
                    users = resp.json()
                    if isinstance(users, list):
                        for u in users:
                            uid = u.get("id")
                            name = u.get("displayName", "")
                            if uid:
                                result[uid] = name
                    log.info(f"  Users batch {i//batch_size + 1}: resolved {len(users)} names")
                else:
                    log.error(f"  Users batch lỗi: status={resp.status_code if resp else 'None'}")
            except Exception as e:
                log.error(f"  Users batch lỗi: {e}")
            time.sleep(0.2)

        return result

    # ─── Phân tích 4 nhóm ────────────────────────────────────

    def analyze(self, low_view_threshold: int = 10) -> dict:
        """Phân loại tất cả BM thành 4 nhóm."""
        log.info("Bắt đầu phân tích...")

        # Lấy tất cả BM
        from app.models.beastmode import BeastMode, BMCardMap, BMDependencyMap, BMAnalysis, BMDeleteLog
        from app.models.card import Card
        bms = self.db.query(BeastMode.id, BeastMode.name, BeastMode.expression, BeastMode.datasources, BeastMode.legacy_id, BeastMode.owner_id).all()
        all_bms = [{"id": r[0], "name": r[1], "expression": r[2], "datasources": r[3], "legacy_id": r[4], "owner_id": r[5]} for r in bms]
        log.info(f"  Tổng BM trong DB: {len(all_bms)}")

        # Map: bm_id → list card_ids (active)
        card_maps_res = self.db.query(BMCardMap.bm_id, BMCardMap.card_id).filter(BMCardMap.is_active == True).all()
        card_maps = [{"bm_id": r[0], "card_id": r[1]} for r in card_maps_res]
        bm_cards = {}
        for row in card_maps:
            bm_cards.setdefault(row["bm_id"], []).append(row["card_id"])
        log.info(f"  Card mappings: {len(card_maps)} rows, {len(bm_cards)} unique BMs")

        # Map: bm_id → list of BM IDs tham chiếu tới nó
        dep_maps_res = self.db.query(BMDependencyMap.bm_id, BMDependencyMap.depends_on_bm_id).all()
        dep_maps = [{"bm_id": r[0], "depends_on_bm_id": r[1]} for r in dep_maps_res]
        referenced_by = {}
        for row in dep_maps:
            referenced_by.setdefault(row["depends_on_bm_id"], []).append(row["bm_id"])
        log.info(f"  Dependencies: {len(dep_maps)} rows, {len(referenced_by)} referenced BMs")

        # Map: card_id → view_count
        cards_res = self.db.query(Card.id, Card.view_count).all()
        cards = [{"id": r[0], "view_count": r[1]} for r in cards_res]
        card_views = {row["id"]: row["view_count"] or 0 for row in cards}
        log.info(f"  Cards with views: {len(cards)}")

        # Resolve owner IDs → display names
        owner_ids = set()
        for bm in all_bms:
            oid = bm.get("owner_id")
            if oid:
                owner_ids.add(int(oid))
        owner_map = self.fetch_user_names(list(owner_ids)) if owner_ids else {}
        log.info(f"  Resolved {len(owner_map)} owner names out of {len(owner_ids)} unique IDs")

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

            # Complexity
            complexity = self._calc_complexity(bm.get("expression") or "")

            # 3-layer duplicate hash
            expr = (bm.get("expression") or "").strip()
            exact_hash = hashlib.md5(expr.encode()).hexdigest()[:12] if expr else ""
            normalized_hash = hashlib.md5(self._normalize_expr(expr).encode()).hexdigest()[:12] if expr else ""
            structure_hash = hashlib.md5(self._structure_expr(expr).encode()).hexdigest()[:12] if expr else ""

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
                "naming_flag": "",
                "complexity_score": complexity,
                "duplicate_hash": exact_hash,
                "normalized_hash": normalized_hash,
                "structure_hash": structure_hash,
                "url": f"https://{self.api.auth.instance}/datacenter/beastmode?id={bm_id}",
                "legacy_id": bm.get("legacy_id") or "",
                "owner_name": owner_map.get(bm.get("owner_id"), ""),
                "card_ids": "\n".join((str(c).split(":")[1] if str(c).startswith("dr:") else str(c)) for c in active_cards),
            }
            results.append(result_row)

        # Lưu DB
        if results:
            from sqlalchemy import delete
            self.db.execute(delete(BMAnalysis))
            for r in results:
                self.db.add(BMAnalysis(**r))
            self.db.commit()

        # Thống kê duplicate (3 tầng)
        dup_exact = self._find_duplicates("duplicate_hash")
        dup_normalized = self._find_duplicates("normalized_hash")
        dup_structure = self._find_duplicates("structure_hash")
        dup_names = self._find_name_duplicates()

        # Log kết quả
        log.info("=" * 40)
        log.success(f"📊 KẾT QUẢ PHÂN TÍCH:")
        log.info(f"  Tổng BM: {len(all_bms)}")
        for g, cnt in group_counts.items():
            pct = (cnt / len(all_bms) * 100) if all_bms else 0
            log.info(f"  Nhóm {g}: {cnt} ({pct:.1f}%)")
        log.info(f"  Duplicates (exact): {len(dup_exact)} groups")
        log.info(f"  Duplicates (normalized): {len(dup_normalized)} groups")
        log.info(f"  Duplicates (structure): {len(dup_structure)} groups")
        log.info(f"  Duplicate names: {len(dup_names)} groups")
        log.info("=" * 40)

        return {
            "total": len(all_bms),
            "groups": group_counts,
            "duplicates_exact": len(dup_exact),
            "duplicates_normalized": len(dup_normalized),
            "duplicates_structure": len(dup_structure),
            "duplicates_names": len(dup_names),
        }

    def get_group_data(self, group_number: int, limit: int = 100, offset: int = 0) -> list[dict]:
        """Lấy danh sách BM theo nhóm."""
        from sqlalchemy import select
        from app.models.beastmode import BMAnalysis, BeastMode
        stmt = (
            select(BMAnalysis, BeastMode.name.label("bm_name"))
            .join(BeastMode, BMAnalysis.bm_id == BeastMode.id)
            .where(BMAnalysis.group_number == group_number)
            .order_by(BMAnalysis.total_views.desc())
            .limit(limit)
            .offset(offset)
        )
        res = self.db.execute(stmt).all()
        out = []
        for row in res:
            bm = {col.name: getattr(row.BMAnalysis, col.name) for col in row.BMAnalysis.__table__.columns}
            bm["bm_name"] = row.bm_name
            out.append(bm)
        return out

    def search_bm(self, query: str, limit: int = 50) -> list[dict]:
        """Tìm BM theo tên (ILIKE) hoặc raw ID."""
        query = query.strip()
        if not query:
            return []

        from sqlalchemy import select
        from app.models.beastmode import BMAnalysis, BeastMode
        
        # Nếu query là số → tìm theo ID
        if query.isdigit():
            stmt = (
                select(BMAnalysis, BeastMode.name.label("bm_name"))
                .join(BeastMode, BMAnalysis.bm_id == BeastMode.id)
                .where(BMAnalysis.bm_id == int(query))
                .limit(limit)
            )
        else:
            # Tìm theo tên (ILIKE)
            stmt = (
                select(BMAnalysis, BeastMode.name.label("bm_name"))
                .join(BeastMode, BMAnalysis.bm_id == BeastMode.id)
                .where(BeastMode.name.ilike(f"%{query}%"))
                .order_by(BMAnalysis.total_views.desc())
                .limit(limit)
            )
            
        res = self.db.execute(stmt).all()
        out = []
        for row in res:
            bm = {col.name: getattr(row.BMAnalysis, col.name) for col in row.BMAnalysis.__table__.columns}
            bm["bm_name"] = row.bm_name
            out.append(bm)
        return out

    def get_summary(self) -> dict:
        """Lấy tổng hợp kết quả phân tích."""
        from app.models.beastmode import BMAnalysis
        from sqlalchemy import select, func
        stmt = (
            select(BMAnalysis.group_number, BMAnalysis.group_label, func.count().label("count"))
            .group_by(BMAnalysis.group_number, BMAnalysis.group_label)
            .order_by(BMAnalysis.group_number)
        )
        groups_res = self.db.execute(stmt).all()
        groups = [{"group_number": g[0], "group_label": g[1], "count": g[2]} for g in groups_res]
        
        total = self.db.query(BMAnalysis).count()

        dup_exact = self._find_duplicates("duplicate_hash")
        dup_normalized = self._find_duplicates("normalized_hash")
        dup_structure = self._find_duplicates("structure_hash")
        dup_names = self._find_name_duplicates()
        dataset_stats = self._dataset_stats()

        return {
            "total": total,
            "groups": [dict(g) for g in groups],
            "duplicates_exact": dup_exact[:20],
            "duplicates_normalized": dup_normalized[:20],
            "duplicates_structure": dup_structure[:20],
            "duplicates_names": dup_names[:20],
            "top_dirty_datasets": dataset_stats[:10],
        }

    # ─── Xóa BM qua Card ──────────────────────────────────────

    def get_bm_detail(self, bm_id: int) -> dict | None:
        """Lấy chi tiết BM template, gồm links (cards) và dependencies."""
        url = f"{self.DETAIL_URL}/{bm_id}?hidden=true"
        resp = self.api.get(url)
        if not resp or resp.status_code != 200:
            log.error(f"Không thể lấy BM detail #{bm_id}")
            return None
        return resp.json()

    def _parse_card_ids_from_links(self, links: list) -> list[str]:
        """Trích card IDs từ links, format dr:CARD_ID:SUB_ID → lấy CARD_ID."""
        card_ids = []
        for link in links:
            resource = link.get("resource", {})
            if resource.get("type") == "CARD":
                raw_id = resource.get("id", "")
                if raw_id.startswith("dr:"):
                    parts = raw_id.split(":")
                    if len(parts) >= 2:
                        card_ids.append(parts[1])
                else:
                    card_ids.append(raw_id)
        return card_ids

    def get_card_definition(self, card_id: str) -> dict | None:
        """Lấy card KPI definition qua PUT."""
        url = "/api/content/v3/cards/kpi/definition"
        payload = {
            "dynamicText": True,
            "variables": True,
            "urn": int(card_id),
        }
        resp = self.api.put(url, json=payload)
        if not resp or resp.status_code != 200:
            log.error(f"Không thể lấy card definition #{card_id}")
            return None
        return resp.json()

    def remove_bm_from_card(self, card_id: str, bm_id: int, bm_name: str, bm_legacy_id: str) -> dict:
        """
        Gỡ BM ra khỏi card bằng cách sửa card definition và PUT lại.
        - Lưu state cũ (card definition) vào DB trước khi xóa
        - Lọc BM ra khỏi formulas (match bằng id == legacy_id)
        - Chỉ tính thành công khi PUT trả về 200
        """
        log.info(f"Đang gỡ BM (legacy={bm_legacy_id}) khỏi card #{card_id}")

        card_def = self.get_card_definition(card_id)
        if not card_def:
            return {"success": False, "error": f"Không lấy được card definition #{card_id}"}

        definition = card_def.get("definition", card_def)

        # ─── Lưu state cũ vào DB ─────────────────────────────
        card_def_json = json.dumps(card_def, ensure_ascii=False)
        from app.models.beastmode import BMDeleteLog
        new_log = BMDeleteLog(
            bm_id=bm_id, bm_name=bm_name, bm_legacy_id=bm_legacy_id,
            card_id=card_id, card_definition_json=card_def_json, status='pending'
        )
        self.db.add(new_log)
        self.db.commit()
        self.db.refresh(new_log)
        log_id = new_log.id
        log.info(f"  💾 Đã lưu card definition cũ (log #{log_id})")

        # ─── Debug: dump card_def ra file ────────────────────
        try:
            with open("debug_card_def.json", "w", encoding="utf-8") as f:
                json.dump(card_def, f, indent=2, ensure_ascii=False)
            log.info(f"  📝 Đã lưu debug_card_def.json")
        except Exception as e:
            log.error(f"  ❌ Không thể lưu debug_card_def.json: {e}")

        # ─── Lấy dataSourceId ─────────────────────────────────
        # Thử lấy từ nhiều nguồn trong card definition response
        data_source_id = None

        # 1) Từ subscriptions (list format - card info API)
        subscriptions = card_def.get("subscriptions", [])
        if isinstance(subscriptions, list):
            for sub in subscriptions:
                if isinstance(sub, dict) and sub.get("dataSourceId"):
                    data_source_id = sub["dataSourceId"]
                    break
        elif isinstance(subscriptions, dict):
            # subscriptions là dict {name: {...}} trong definition
            for sub_name, sub_data in subscriptions.items():
                if isinstance(sub_data, dict):
                    ds_id = sub_data.get("dataSourceId")
                    if ds_id:
                        data_source_id = ds_id
                        break

        # 2) Từ datasources array (card info API format)
        if not data_source_id:
            datasources = card_def.get("datasources", [])
            if isinstance(datasources, list):
                for ds in datasources:
                    if isinstance(ds, dict) and ds.get("dataSourceId"):
                        data_source_id = ds["dataSourceId"]
                        break

        # 3) Từ definition.conditionalFormats
        if not data_source_id:
            cond_formats = definition.get("conditionalFormats", [])
            for cf in cond_formats:
                cond = cf.get("condition", {})
                if cond.get("dataSourceId"):
                    data_source_id = cond["dataSourceId"]
                    break

        # 4) Fallback: gọi card info API (tham khảo 1901/get_card_info.py)
        if not data_source_id:
            log.info(f"  🔍 dataSourceId chưa tìm thấy, thử card info API...")
            card_info_resp = self.api.get(
                f"/api/content/v1/cards?urns={card_id}&parts=datasources&includeFiltered=true"
            )
            if card_info_resp and card_info_resp.status_code == 200:
                try:
                    card_info_data = card_info_resp.json()
                    if isinstance(card_info_data, list) and len(card_info_data) > 0:
                        card_info = card_info_data[0]
                        # Lấy từ subscriptions
                        for sub in card_info.get("subscriptions", []):
                            if isinstance(sub, dict) and sub.get("dataSourceId"):
                                data_source_id = sub["dataSourceId"]
                                log.info(f"  ✅ Tìm thấy dataSourceId từ card info subscriptions")
                                break
                        # Lấy từ datasources
                        if not data_source_id:
                            for ds in card_info.get("datasources", []):
                                if isinstance(ds, dict) and ds.get("dataSourceId"):
                                    data_source_id = ds["dataSourceId"]
                                    log.info(f"  ✅ Tìm thấy dataSourceId từ card info datasources")
                                    break
                except Exception as e:
                    log.error(f"  ❌ Parse card info lỗi: {e}")

        if not data_source_id:
            if log_id:
                from sqlalchemy import update
                self.db.execute(update(BMDeleteLog).where(BMDeleteLog.id == log_id).values(status='failed', error_message="Không tìm thấy dataSourceId"))
                self.db.commit()
            return {"success": False, "error": f"Không tìm thấy dataSourceId cho card #{card_id}"}

        log.info(f"  📦 dataSourceId={data_source_id}")

        # ─── Lọc BM ra khỏi formulas ─────────────────────────
        formulas = definition.get("formulas", [])
        original_count = len(formulas)
        filtered_formulas = [f for f in formulas if f.get("id") != bm_legacy_id]
        removed_count = original_count - len(filtered_formulas)

        if removed_count == 0:
            if log_id:
                from sqlalchemy import update
                self.db.execute(update(BMDeleteLog).where(BMDeleteLog.id == log_id).values(status='skipped', error_message=f"BM không có trong formulas card #{card_id}"))
                self.db.commit()
            return {"success": False, "error": f"Không tìm thấy BM (legacy={bm_legacy_id}) trong formulas card #{card_id}"}

        log.info(f"  Đã lọc {removed_count} formula, còn {len(filtered_formulas)}/{original_count}")

        # ─── Lọc BM ra khỏi subscription ────────────────────
        # BM bị xóa khỏi formulas nhưng nếu vẫn còn trong subscription.columns/orderBy/groupBy
        # → Domo sẽ 500 vì cố query formula không tồn tại
        subscriptions = definition.get("subscriptions", {})
        cleaned_subs = {}
        for sub_name, sub_data in subscriptions.items():
            if not isinstance(sub_data, dict):
                cleaned_subs[sub_name] = sub_data
                continue
            cleaned_sub = dict(sub_data)
            # Lọc columns
            if "columns" in cleaned_sub:
                original_cols = len(cleaned_sub["columns"])
                cleaned_sub["columns"] = [
                    c for c in cleaned_sub["columns"]
                    if c.get("formulaId") != bm_legacy_id
                ]
                removed_cols = original_cols - len(cleaned_sub["columns"])
                if removed_cols > 0:
                    log.info(f"  🧹 Đã xóa {removed_cols} column ref trong subscription '{sub_name}'")
            # Lọc orderBy
            if "orderBy" in cleaned_sub:
                cleaned_sub["orderBy"] = [
                    o for o in cleaned_sub["orderBy"]
                    if o.get("formulaId") != bm_legacy_id
                ]
            # Lọc groupBy
            if "groupBy" in cleaned_sub:
                cleaned_sub["groupBy"] = [
                    g for g in cleaned_sub["groupBy"]
                    if g.get("formulaId") != bm_legacy_id
                ]
            cleaned_subs[sub_name] = cleaned_sub

        # ─── PUT lại card ─────────────────────────────────────
        # Payload gồm: urn + dataProvider + formulas + subscription
        # subscription phải là object trực tiếp (e.g. {"name":"main","columns":[...]})
        # KHÔNG phải {"main": {...}}
        main_sub = cleaned_subs.get("main", {})
        if not main_sub and cleaned_subs:
            # Lấy subscription đầu tiên nếu không có "main"
            main_sub = next(iter(cleaned_subs.values()), {})
        payload = {
            "urn": int(card_id),
            "dataProvider": {"dataSourceId": data_source_id},
            "formulas": filtered_formulas,
            "subscription": main_sub,
        }

        url = "/api/content/v3/cards/kpi/table/query"

        # ─── Headers bắt buộc cho PUT card query ──────────────
        session_toe = self.api.auth.cookies.get("SESSION_TOE", "")
        extra_headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "ja",
            "cache-control": "no-cache",
            "content-type": "application/json;charset=UTF-8",
            "pragma": "no-cache",
            "origin": f"https://{self.api.auth.instance}",
        }
        if session_toe:
            import json as _json
            import random, string
            suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))
            extra_headers["x-domo-requestcontext"] = _json.dumps({"clientToe": f"{session_toe}-{suffix}"})

        log.info(f"  🔍 [PUT] card_id={card_id}, dataSourceId={data_source_id}")
        log.info(f"  🔍 [PUT] formulas: {original_count} → {len(filtered_formulas)}")
        log.info(f"  🔍 [PUT] payload keys: {list(payload.keys())}")
        log.info(f"  🔍 [PUT] SESSION_TOE={'✅' if session_toe else '❌ MISSING'}")

        # ─── Debug: dump JSON ra file ──────────────────────────
        try:
            with open("debug_card_def.json", "w", encoding="utf-8") as f:
                json.dump(card_def, f, indent=2, ensure_ascii=False)
            with open("debug_put_payload.json", "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, ensure_ascii=False)
            log.info(f"  📝 Đã lưu debug_card_def.json + debug_put_payload.json")
        except Exception as e:
            log.error(f"  ❌ Không thể lưu debug JSON: {e}")

        resp = self.api.put(url, json=payload, extra_headers=extra_headers)

        if not resp or resp.status_code != 200:
            status = resp.status_code if resp else "None"
            body = resp.text[:500] if resp else ""
            error_msg = f"PUT thất bại (status={status}): {body}"
            log.error(f"  ❌ {error_msg}")
            if log_id:
                from sqlalchemy import update
                self.db.execute(update(BMDeleteLog).where(BMDeleteLog.id == log_id).values(status='failed', error_message=error_msg[:500]))
                self.db.commit()
            return {"success": False, "error": error_msg}

        # ─── Thành công ───────────────────────────────────────
        if log_id:
            from sqlalchemy import update
            self.db.execute(update(BMDeleteLog).where(BMDeleteLog.id == log_id).values(status='success'))
            self.db.commit()
        log.success(f"  ✅ Đã gỡ BM khỏi card #{card_id}")
        return {"success": True, "removed": removed_count, "card_id": card_id}

    def delete_bm(self, bm_id: int) -> dict:
        """Xóa BM trực tiếp bằng DELETE request như test_delete.py"""
        log.info(f"=== Bắt đầu xóa trực tiếp BM #{bm_id} ===")
        
        # Get BM Detail first to get the name for logging
        detail = self.get_bm_detail(bm_id)
        bm_name = detail.get("name", f"BM #{bm_id}") if detail else f"BM #{bm_id}"
        legacy_id = detail.get("legacyId", "") if detail else ""

        url = f"{self.DETAIL_URL}/{bm_id}"
        
        csrf_token = self.api.auth.cookies.get("csrf-token", "")
        extra_headers = {
            "x-csrf-token": csrf_token,
            "x-requested-with": "XMLHttpRequest",
            "content-type": "application/json",
            "accept": "application/json, text/plain, */*"
        }
        
        resp = self.api.delete(url, extra_headers=extra_headers)
        
        if resp and resp.status_code in (200, 204):
            # Xóa khỏi DB cục bộ để sync
            try:
                from sqlalchemy import delete
                from app.models.beastmode import BeastMode, BMAnalysis, BMCardMap
                self.db.execute(delete(BeastMode).where(BeastMode.id == bm_id))
                self.db.execute(delete(BMAnalysis).where(BMAnalysis.bm_id == bm_id))
                self.db.execute(delete(BMCardMap).where(BMCardMap.bm_id == bm_id))
                self.db.commit()
            except Exception as e:
                log.warn(f"Lỗi xóa DB local: {e}")
            log.success(f"=== Đã xóa thành công BM #{bm_id} ===")
            return {"success": True, "bm_id": bm_id, "bm_name": bm_name, "legacy_id": legacy_id, "message": "Xóa thành công", "success_count": 1, "details":[]}
        else:
            status = resp.status_code if resp else "None"
            body = resp.text[:500] if resp else ""
            error_msg = f"DELETE thất bại (status={status}): {body}"
            log.error(f"  ❌ {error_msg}")
            return {"success": False, "bm_id": bm_id, "bm_name": bm_name, "error": error_msg}

    # ─── Helpers ──────────────────────────────────────────────

    def _normalize_expr(self, expr: str) -> str:
        """Tầng 2: Normalize expression — lowercase, collapse whitespace, remove comments, strip backticks."""
        if not expr:
            return ""
        s = expr
        # Remove block comments /* ... */
        s = re.sub(r'/\*.*?\*/', '', s, flags=re.DOTALL)
        # Remove line comments -- ...
        s = re.sub(r'--[^\n]*', '', s)
        # Lowercase
        s = s.lower()
        # Strip backticks around column names
        s = s.replace('`', '')
        # Collapse whitespace
        s = re.sub(r'\s+', ' ', s).strip()
        return s

    def _structure_expr(self, expr: str) -> str:
        """Tầng 3: Chỉ giữ cấu trúc logic — thay column names, strings, numbers bằng placeholder."""
        if not expr:
            return ""
        s = self._normalize_expr(expr)
        # Replace DOMO_BEAST_MODE(id) → BM_REF
        s = re.sub(r'domo_beast_mode\(\d+\)', 'BM_REF', s)
        # Replace string literals 'anything' → STR
        s = re.sub(r"'[^']*'", 'STR', s)
        # Replace column names (backticks already stripped, so match word-like identifiers
        # between known SQL tokens). Replace quoted identifiers "name" → COL
        s = re.sub(r'"[^"]*"', 'COL', s)
        # Replace numbers (integers and decimals)
        s = re.sub(r'\b\d+(\.\d+)?\b', 'NUM', s)
        # Collapse whitespace again
        s = re.sub(r'\s+', ' ', s).strip()
        return s

    def _calc_complexity(self, expression: str) -> int:
        if not expression:
            return 0
        score = 0
        score += min(len(expression) // 100, 5)  # Độ dài
        score += len(re.findall(r'DOMO_BEAST_MODE\(\d+\)', expression))  # Nested BM
        score += len(re.findall(r'CASE\s+WHEN', expression, re.IGNORECASE))  # CASE WHEN
        return score

    def _find_duplicates(self, hash_column: str = "duplicate_hash") -> list[dict]:
        """Tìm các nhóm BM có expression giống nhau theo hash column."""
        from sqlalchemy import select, func
        from app.models.beastmode import BMAnalysis
        hash_col_attr = getattr(BMAnalysis, hash_column)
        
        stmt = (
            select(
                hash_col_attr.label("dup_hash"),
                func.count().label("cnt"),
                func.array_agg(BMAnalysis.bm_id).label("bm_ids")
            )
            .where(hash_col_attr != '', hash_col_attr.isnot(None))
            .group_by(hash_col_attr)
            .having(func.count() > 1)
            .order_by(func.count().desc())
            .limit(100)
        )
        try:
            res = self.db.execute(stmt).all()
            return [{"dup_hash": r[0], "cnt": r[1], "bm_ids": r[2]} for r in res]
        except Exception:
            # Fallback cho database không hỗ trợ array_agg (như SQLite lúc test)
            from sqlalchemy import text
            fallback_stmt = text(f"SELECT {hash_column} as dup_hash, COUNT(*) as cnt, GROUP_CONCAT(bm_id) as bm_ids FROM bm_analysis WHERE {hash_column} != '' AND {hash_column} IS NOT NULL GROUP BY {hash_column} HAVING COUNT(*) > 1 ORDER BY cnt DESC LIMIT 100")
            res = self.db.execute(fallback_stmt).mappings().all()
            return [{"dup_hash": r["dup_hash"], "cnt": r["cnt"], "bm_ids": [int(x) for x in str(r["bm_ids"]).split(',')] if r["bm_ids"] else []} for r in res]

    def _find_name_duplicates(self) -> list[dict]:
        """Tìm các BM có cùng tên."""
        from sqlalchemy import select, func
        from app.models.beastmode import BMAnalysis, BeastMode
        stmt = (
            select(
                BeastMode.name,
                func.count().label("cnt"),
                func.array_agg(BMAnalysis.bm_id).label("bm_ids")
            )
            .select_from(BMAnalysis)
            .join(BeastMode, BMAnalysis.bm_id == BeastMode.id)
            .where(BeastMode.name.isnot(None), BeastMode.name != '')
            .group_by(BeastMode.name)
            .having(func.count() > 1)
            .order_by(func.count().desc())
            .limit(100)
        )
        try:
            res = self.db.execute(stmt).all()
            return [{"name": r[0], "cnt": r[1], "bm_ids": r[2]} for r in res]
        except Exception:
            from sqlalchemy import text
            fallback_stmt = text("SELECT b.name, COUNT(*) as cnt, GROUP_CONCAT(a.bm_id) as bm_ids FROM bm_analysis a JOIN beastmodes b ON a.bm_id = b.id WHERE b.name IS NOT NULL AND b.name != '' GROUP BY b.name HAVING COUNT(*) > 1 ORDER BY cnt DESC LIMIT 100")
            res = self.db.execute(fallback_stmt).mappings().all()
            return [{"name": r["name"], "cnt": r["cnt"], "bm_ids": [int(x) for x in str(r["bm_ids"]).split(',')] if r["bm_ids"] else []} for r in res]

    def _dataset_stats(self) -> list[dict]:
        """Thống kê BM rác theo dataset."""
        from sqlalchemy import select, func, case
        from app.models.beastmode import BMAnalysis
        instance = self.api.auth.instance if self.api.auth else ""
        
        stmt = (
            select(
                BMAnalysis.dataset_names,
                func.count().label("total"),
                func.sum(case((BMAnalysis.group_number == 1, 1), else_=0)).label("unused"),
                func.sum(case((BMAnalysis.group_number.in_([1, 2, 3]), 1), else_=0)).label("cleanup_candidates")
            )
            .where(BMAnalysis.dataset_names != '')
            .group_by(BMAnalysis.dataset_names)
            .order_by(func.sum(case((BMAnalysis.group_number == 1, 1), else_=0)).desc())
            .limit(20)
        )
        raw = self.db.execute(stmt).all()
        results = []
        for row in raw:
            ds_id = (row[0] or "").strip().split(",")[0].strip()
            total = int(row[1] or 0)
            unused = int(row[2] or 0)
            candidates = int(row[3] or 0)
            results.append({
                "dataset_id": ds_id,
                "url": f"https://{instance}/datasources/{ds_id}" if ds_id else "",
                "total": total,
                "unused": unused,
                "cleanup_candidates": candidates,
            })
        return results

    GROUP_LABELS_VI = {1: 'Không sử dụng', 2: 'Từng được dùng', 3: 'Card ít xem', 4: 'Đang hoạt động'}
    GROUP_LABELS_JA = {1: '未使用', 2: '過去使用', 3: '低閲覧', 4: '稼働中'}

    def export_csv(self, group_number: int = 0, lang: str = 'vi') -> list[dict]:
        """Lấy data cho CSV export. group_number=0 → lấy tất cả."""
        from sqlalchemy import select
        from app.models.beastmode import BMAnalysis, BeastMode
        
        stmt = (
            select(
                BMAnalysis.bm_id, BeastMode.name.label("bm_name"), BeastMode.legacy_id,
                BMAnalysis.group_number, BMAnalysis.group_label,
                BMAnalysis.owner_name,
                BMAnalysis.active_cards_count, BMAnalysis.total_views,
                BMAnalysis.referenced_by_count, BMAnalysis.dataset_names,
                BMAnalysis.card_ids,
                BMAnalysis.complexity_score,
                BMAnalysis.duplicate_hash, BMAnalysis.normalized_hash, BMAnalysis.structure_hash,
                BMAnalysis.url
            )
            .select_from(BMAnalysis)
            .join(BeastMode, BMAnalysis.bm_id == BeastMode.id)
        )
        if group_number > 0:
            stmt = stmt.where(BMAnalysis.group_number == group_number)
            
        stmt = stmt.order_by(BMAnalysis.group_number, BMAnalysis.total_views.desc())
        
        res = self.db.execute(stmt).all()
        rows = [dict(r._mapping) for r in res]
        # Translate group_label theo lang
        label_map = self.GROUP_LABELS_JA if lang == 'ja' else self.GROUP_LABELS_VI
        instance = self.api.auth.instance if self.api.auth else ""
        for row in rows:
            gnum = row.get('group_number') or 0
            if gnum in label_map:
                row['group_label'] = label_map[gnum]
            # Format card_ids: each card on its own line, with link
            cids = row.get('card_ids') or ''
            if cids and instance:
                lines = []
                for cid in cids.split('\n'):
                    cid = cid.strip()
                    if cid:
                        lines.append(f"https://{instance}/page/kpis/details/{cid}")
                row['card_ids'] = '\n'.join(lines)
        return rows

