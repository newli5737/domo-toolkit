"""CardService — Crawl danh sách card + lấy view count."""

import json
import time
from app.core.api import DomoAPI
from sqlalchemy.orm import Session
from app.core.logger import DomoLogger

log = DomoLogger("card")


class CardService:
    """Xử lý logic liên quan đến Card trong Domo."""

    ADMIN_SUMMARY_URL = "/api/content/v2/cards/adminsummary"
    CARD_INFO_URL = "/api/content/v1/cards"

    def __init__(self, api: DomoAPI, db: Session):
        self.api = api
        self.db = db

    def crawl_all_cards(self, job_id: int = None, progress_callback=None) -> list[dict]:
        """Crawl tất cả card qua adminsummary API."""
        all_cards = []
        skip = 0
        batch_size = 100
        total_expected = None

        log.info(f"Bắt đầu crawl cards (batch_size={batch_size})")

        while True:
            url = f"{self.ADMIN_SUMMARY_URL}?limit={batch_size}&skip={skip}"
            payload = {"ascending": True, "orderBy": "cardTitle"}

            batch_start = time.time()
            resp = self.api.post(url, json=payload)
            api_time = time.time() - batch_start

            if not resp or resp.status_code != 200:
                status_code = resp.status_code if resp else "None"
                log.error(f"Card crawl thất bại tại skip={skip} (status={status_code}, took {api_time:.1f}s)")
                break

            data = resp.json()
            cards = data.get("cardAdminSummaries", [])

            if total_expected is None:
                total_expected = data.get("totalCardCount", 0)
                log.info(f"  Tổng cards dự kiến: {total_expected}")

            if not cards:
                log.debug(f"Không còn cards tại skip={skip}")
                break

            all_cards.extend(cards)

            if job_id:
                from sqlalchemy import update
                from app.models.monitor import CrawlJob
                self.db.execute(
                    update(CrawlJob).where(CrawlJob.id == job_id).values(processed=len(all_cards), total=total_expected)
                )
                self.db.commit()
            if progress_callback:
                progress_callback(len(all_cards), total_expected or 0)

            log.progress(len(all_cards), total_expected or 0, "Crawl Cards")
            log.debug(f"  API: {api_time:.1f}s | batch: {len(cards)} cards")

            if len(cards) < batch_size:
                break
            skip += batch_size

        log.info(f"Crawl cards xong: {len(all_cards)} cards")
        return all_cards

    def fetch_view_counts(self, card_urns: list[str], batch_size: int = 50,
                          job_id: int = None, progress_callback=None):
        """Lấy view count + datasource info cho list card URNs, lưu vào DB."""
        from sqlalchemy import update
        from app.models.card import Card
        
        total = len(card_urns)
        processed = 0
        updated = 0
        errors = 0

        log.info(f"Bắt đầu fetch view counts + datasources cho {total} cards (batch_size={batch_size})")

        for i in range(0, total, batch_size):
            batch = card_urns[i:i + batch_size]
            params = (
                [("parts", "viewInfo"), ("parts", "datasources")]
                + [("urns", u) for u in batch]
            )

            url = f"{self.CARD_INFO_URL}"
            batch_start = time.time()
            resp = self.api.get(url, params=params)
            api_time = time.time() - batch_start

            if not resp or resp.status_code != 200:
                processed += len(batch)
                errors += len(batch)
                log.warn(f"  View count batch thất bại ({len(batch)} cards, took {api_time:.1f}s)")
                if progress_callback:
                    progress_callback(processed, total)
                continue

            try:
                cards_data = resp.json()
                if isinstance(cards_data, list):
                    params_list = []
                    for card in cards_data:
                        card_id = card.get("id")
                        view_info = card.get("viewInfo", {})
                        datasources = card.get("datasources", [])
                        ds = datasources[0] if datasources else {}

                        if card_id:
                            params_list.append((
                                view_info.get("totalViewCount", 0),
                                view_info.get("lastViewedDate", 0),
                                view_info.get("lastViewedDate", 0),
                                ds.get("dataSourceId"),
                                ds.get("dataSourceName"),
                                str(card_id),
                            ))

                    if params_list:
                        from datetime import datetime
                        for p in params_list:
                            view_count, lv1, lv2, ds_id, ds_name, card_id_str = p
                            ts = datetime.fromtimestamp(lv1 / 1000.0) if lv1 > 0 else None
                            
                            self.db.execute(
                                update(Card)
                                .where(Card.id == card_id_str)
                                .values(
                                    view_count=view_count,
                                    last_viewed_at=ts,
                                    datasource_id=ds_id,
                                    datasource_name=ds_name
                                )
                            )
                        self.db.commit()
                        updated += len(params_list)

            except Exception as e:
                log.error(f"Parse viewInfo/datasources lỗi: {e}")
                errors += 1

            processed += len(batch)
            if job_id:
                from sqlalchemy import update
                from app.models.monitor import CrawlJob
                self.db.execute(
                    update(CrawlJob).where(CrawlJob.id == job_id).values(processed=processed)
                )
                self.db.commit()
            if progress_callback:
                progress_callback(processed, total)

            log.progress(processed, total, "View Counts + Datasources")
            log.debug(f"  API: {api_time:.1f}s | updated: {updated} | errors: {errors}")

        log.info(f"Fetch view counts xong: {updated} updated, {errors} errors")

    def save_cards_from_summary(self, cards: list[dict]):
        """Lưu card data từ adminsummary vào DB cards."""
        rows = []
        for card in cards:
            pages = card.get("pageHierarchy", [])
            page = pages[0] if pages else {}

            owners = card.get("owners", [])
            owner_name = owners[0].get("displayName", "") if owners else ""

            rows.append({
                "id": card.get("id"),
                "title": card.get("title", ""),
                "card_type": card.get("type", ""),
                "view_count": 0,
                "owner_name": owner_name,
                "page_id": page.get("pageId"),
                "page_title": page.get("title", ""),
            })

        if rows:
            from app.models.card import Card
            from app.core.database import bulk_upsert
            db_start = time.time()
            bulk_upsert(self.db, Card, rows, ["id"])
            self.db.commit()
            db_time = time.time() - db_start
            log.info(f"Lưu {len(rows)} cards vào DB ({db_time:.1f}s)")

    def get_all_urns(self) -> list[str]:
        """Lấy tất cả card URNs từ DB."""
        from app.models.card import Card
        result = self.db.query(Card.id).all()
        urns = [str(r[0]) for r in result]
        log.info(f"Tổng card URNs: {len(urns)}")
        return urns
