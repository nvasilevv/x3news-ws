from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from x3news_feed import Attachment, NewsItem
from x3news_store import EventStore


def make_item(extri_id: str, company: str) -> NewsItem:
    return NewsItem(
        extri_id=extri_id,
        company=company,
        headline="Headline",
        published_at="2026-03-12T15:58:00+02:00",
        raw_time_text="15:58",
        detail_url=f"https://www.x3news.com/?page=ShowNews&ExtriID={extri_id}&output=ajax&language=en",
        row_summary="Headline",
        detail_summary=f"Summary {extri_id}",
        body=f"Body {extri_id}",
        attachments=[Attachment(name="doc", url=f"https://files/{extri_id}.pdf")],
    )


class EventStoreTests(unittest.TestCase):
    def test_recent_and_since_queries(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = EventStore(str(Path(tmpdir) / "x3news.db"))
            store.upsert_item(make_item("1", "One AD"), "2026-03-12T14:00:00Z")
            store.upsert_item(make_item("2", "Two AD"), "2026-03-12T14:05:00Z")
            store.upsert_item(make_item("3", "Three AD"), "2026-03-12T14:10:00Z")

            recent = store.get_recent_items(2)
            self.assertEqual([item["extri_id"] for item in recent], ["3", "2"])

            since = store.get_items_after_extri_id("1", 10)
            self.assertEqual([item["extri_id"] for item in since], ["2", "3"])

    def test_missing_anchor_returns_empty_since_result(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = EventStore(str(Path(tmpdir) / "x3news.db"))
            store.upsert_item(make_item("1", "One AD"), "2026-03-12T14:00:00Z")
            self.assertEqual(store.get_items_after_extri_id("999", 10), [])


if __name__ == "__main__":
    unittest.main()
