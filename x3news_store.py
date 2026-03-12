from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from x3news_feed import Attachment, NewsItem


class EventStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        path = Path(db_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS news_events (
                    extri_id TEXT PRIMARY KEY,
                    company TEXT NOT NULL,
                    headline TEXT NOT NULL,
                    published_at TEXT NOT NULL,
                    raw_time_text TEXT NOT NULL,
                    detail_url TEXT NOT NULL,
                    row_summary TEXT NOT NULL,
                    detail_summary TEXT NOT NULL,
                    body TEXT NOT NULL,
                    attachments_json TEXT NOT NULL,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_news_events_first_seen_at
                ON news_events(first_seen_at DESC)
                """
            )

    def upsert_item(self, item: NewsItem, seen_at: str) -> None:
        attachments_json = json.dumps(
            [attachment.to_dict() for attachment in item.attachments or []],
            ensure_ascii=False,
        )
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO news_events (
                    extri_id,
                    company,
                    headline,
                    published_at,
                    raw_time_text,
                    detail_url,
                    row_summary,
                    detail_summary,
                    body,
                    attachments_json,
                    first_seen_at,
                    last_seen_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(extri_id) DO UPDATE SET
                    company = excluded.company,
                    headline = excluded.headline,
                    published_at = excluded.published_at,
                    raw_time_text = excluded.raw_time_text,
                    detail_url = excluded.detail_url,
                    row_summary = excluded.row_summary,
                    detail_summary = excluded.detail_summary,
                    body = excluded.body,
                    attachments_json = excluded.attachments_json,
                    last_seen_at = excluded.last_seen_at
                """,
                (
                    item.extri_id,
                    item.company,
                    item.headline,
                    item.published_at,
                    item.raw_time_text,
                    item.detail_url,
                    item.row_summary,
                    item.detail_summary,
                    item.body,
                    attachments_json,
                    seen_at,
                    seen_at,
                ),
            )

    def get_recent_items(self, limit: int) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM news_events
                ORDER BY first_seen_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def get_items_after_extri_id(self, extri_id: str, limit: int) -> list[dict[str, Any]]:
        with self._connect() as connection:
            anchor = connection.execute(
                "SELECT first_seen_at FROM news_events WHERE extri_id = ?",
                (extri_id,),
            ).fetchone()
            if anchor is None:
                return []

            rows = connection.execute(
                """
                SELECT *
                FROM news_events
                WHERE first_seen_at > ?
                ORDER BY first_seen_at ASC
                LIMIT ?
                """,
                (anchor["first_seen_at"], limit),
            ).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def count(self) -> int:
        with self._connect() as connection:
            row = connection.execute("SELECT COUNT(*) AS count FROM news_events").fetchone()
        return int(row["count"])

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
        attachments_payload = json.loads(row["attachments_json"])
        attachments = [
            Attachment(name=str(item.get("name", "")), url=str(item.get("url", ""))).to_dict()
            for item in attachments_payload
        ]
        return {
            "extri_id": row["extri_id"],
            "company": row["company"],
            "headline": row["headline"],
            "published_at": row["published_at"],
            "raw_time_text": row["raw_time_text"],
            "detail_url": row["detail_url"],
            "row_summary": row["row_summary"],
            "detail_summary": row["detail_summary"],
            "body": row["body"],
            "attachments": attachments,
            "first_seen_at": row["first_seen_at"],
            "last_seen_at": row["last_seen_at"],
        }
