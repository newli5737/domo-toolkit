"""CardService — Crawl danh sách card + lấy view count."""

import json
from app.core.api import DomoAPI
from app.core.db import DomoDatabase
from app.core.logger import DomoLogger

log = DomoLogger("card")


class CardService:
    """Xử lý logic liên quan đến Card trong Domo."""

    ADMIN_SUMMARY_URL = "/api/content/v2/cards/adminsummary"
    CARD_INFO_URL = "/api/content/v1/cards"

    def __init__(self, api: DomoAPI, db: DomoDatabase):
        self.api = api
        self.db = db

    def crawl_all_cards(self, job_id: int = None) -> list[dict]:
        """Crawl tất cả card qua adminsummary API."""
        all_cards = []
        skip = 0
        batch_size = 100
        total_expected = None

        while True:
            url = f"{self.ADMIN_SUMMARY_URL}?limit={batch_size}&skip={skip}"
            payload = {"ascending": True, "orderBy": "cardTitle"}

            resp = self.api.post(url, json=payload)
            if not resp or resp.status_code != 200:
                log.error(f"Card crawl thất bại tại skip={skip}")
                break

            data = resp.json()
            cards = data.get("cardAdminSummaries", [])

            if total_expected is None:
                total_expected = data.get("totalCardCount", 0)

            if not cards:
                break

            all_cards.extend(cards)

            if job_id:
                self.db.execute(
                    "UPDATE crawl_jobs SET processed = %s, total = %s WHERE id = %s",
                    (len(all_cards), total_expected, job_id)
                )

            log.info(f"Cards: {len(all_cards)}/{total_expected}")

            if len(cards) < batch_size:
                break
            skip += batch_size

        return all_cards

    def fetch_view_counts(self, card_urns: list[str], batch_size: int = 50, job_id: int = None):
        """Lấy view count cho list card URNs, lưu vào DB."""
        total = len(card_urns)
        processed = 0

        for i in range(0, total, batch_size):
            batch = card_urns[i:i + batch_size]
            params = [("parts", "viewInfo")] + [("urns", u) for u in batch]

            url = f"{self.CARD_INFO_URL}"
            resp = self.api.get(url, params=params)

            if not resp or resp.status_code != 200:
                processed += len(batch)
                continue

            try:
                cards_data = resp.json()
                if isinstance(cards_data, list):
                    for card in cards_data:
                        card_id = card.get("id")
                        view_info = card.get("viewInfo", {})
                        if card_id:
                            self.db.execute(
                                """UPDATE cards SET view_count = %s, last_viewed_at = 
                                   CASE WHEN %s > 0 THEN to_timestamp(%s / 1000.0) ELSE NULL END
                                   WHERE id = %s""",
                                (
                                    view_info.get("totalViewCount", 0),
                                    view_info.get("lastViewedDate", 0),
                                    view_info.get("lastViewedDate", 0),
                                    card_id,
                                )
                            )
            except Exception as e:
                log.error(f"Parse viewInfo lỗi: {e}")

            processed += len(batch)
            if job_id:
                self.db.execute(
                    "UPDATE crawl_jobs SET processed = %s WHERE id = %s",
                    (processed, job_id)
                )

            log.info(f"ViewInfo: {processed}/{total}")

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
            self.db.bulk_upsert("cards", rows, "id")
            log.info(f"Lưu {len(rows)} cards vào DB")

    def get_all_urns(self) -> list[str]:
        """Lấy tất cả card URNs từ DB."""
        result = self.db.query("SELECT id FROM cards")
        return [str(r["id"]) for r in result]
