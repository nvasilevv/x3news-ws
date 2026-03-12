from __future__ import annotations

import unittest
from datetime import datetime
from pathlib import Path

from x3news_feed import LOCAL_TZ, parse_detail_html, parse_listing_html, parse_published_at


FIXTURE_DIR = Path(__file__).resolve().parents[1]


class X3NewsFeedTests(unittest.TestCase):
    def test_parse_listing_fixture(self) -> None:
        html = (FIXTURE_DIR / "debug_first.html").read_text(encoding="utf-8")

        items = parse_listing_html(
            html,
            language="en",
            now=datetime(2026, 3, 5, 15, 0, tzinfo=LOCAL_TZ),
            limit=3,
        )

        self.assertEqual(len(items), 3)
        self.assertEqual(items[0].extri_id, "196508")
        self.assertEqual(items[0].company, "Sopharma AD")
        self.assertEqual(
            items[0].published_at,
            "2026-03-05T14:34:00+02:00",
        )
        self.assertEqual(
            items[0].detail_url,
            "https://www.x3news.com/?page=ShowNews&ExtriID=196508&output=ajax&language=en",
        )

    def test_parse_published_at_uses_today_for_time_only_rows(self) -> None:
        parsed = parse_published_at("15:58", now=datetime(2026, 3, 12, 16, 10, tzinfo=LOCAL_TZ))
        self.assertEqual(parsed, "2026-03-12T15:58:00+02:00")

    def test_parse_published_at_handles_full_dates(self) -> None:
        parsed = parse_published_at("11-03-2026 16:36")
        self.assertEqual(parsed, "2026-03-11T16:36:00+02:00")

    def test_parse_detail_html_extracts_summary_and_attachments(self) -> None:
        html = """
        <p>Webinar on: TBS Group - Financial Overview: FY 2025 and Outlook 2026-2030</p>
        <dl>
            <dt>Attachments:</dt>
            <dd><a href="./show/download.php?id=679650">Announcement</a></dd>
            <dd><a href="./show/download.php?id=679651">Presentation</a></dd>
        </dl>
        """

        detail = parse_detail_html(html)

        self.assertEqual(
            detail.detail_summary,
            "Webinar on: TBS Group - Financial Overview: FY 2025 and Outlook 2026-2030",
        )
        self.assertEqual(len(detail.attachments), 2)
        self.assertEqual(detail.attachments[0].name, "Announcement")
        self.assertEqual(
            detail.attachments[0].url,
            "https://www.x3news.com/show/download.php?id=679650",
        )


if __name__ == "__main__":
    unittest.main()
