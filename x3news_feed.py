from __future__ import annotations

import re
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import requests
import urllib3
from bs4 import BeautifulSoup


BASE_URL = "https://www.x3news.com/"
DEFAULT_LANGUAGE = "en"
DEFAULT_TIMEOUT = 20
LOCAL_TZ = ZoneInfo("Europe/Sofia")
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
)
EXTRI_ID_RE = re.compile(r"showNews\('\d+',\s*(\d+)\)", re.IGNORECASE)
ATTACHMENT_RE = re.compile(r"\.(pdf|docx?|xlsx?|xls|zip|xml|xhtml)$", re.IGNORECASE)
TODAY_TIME_RE = re.compile(r"^\d{1,2}:\d{2}$")
FULL_TIMESTAMP_RE = re.compile(r"^\d{2}-\d{2}-\d{4}\s+\d{1,2}:\d{2}$")
WHITESPACE_RE = re.compile(r"\s+")


def normalize_whitespace(value: str) -> str:
    return WHITESPACE_RE.sub(" ", value or "").strip()


def parse_published_at(raw_value: str, now: datetime | None = None) -> str:
    now = now or datetime.now(LOCAL_TZ)
    value = normalize_whitespace(raw_value)

    if TODAY_TIME_RE.fullmatch(value):
        parsed = datetime.strptime(value, "%H:%M").replace(
            year=now.year,
            month=now.month,
            day=now.day,
            tzinfo=LOCAL_TZ,
        )
        return parsed.isoformat()

    if FULL_TIMESTAMP_RE.fullmatch(value):
        parsed = datetime.strptime(value, "%d-%m-%Y %H:%M").replace(tzinfo=LOCAL_TZ)
        return parsed.isoformat()

    return value


def is_attachment_href(href: str) -> bool:
    href_lower = href.lower()
    return "download.php?id=" in href_lower or "downloadextri" in href_lower or bool(ATTACHMENT_RE.search(href_lower))


@dataclass(slots=True)
class Attachment:
    name: str
    url: str

    def to_dict(self) -> dict[str, str]:
        return {"name": self.name, "url": self.url}


@dataclass(slots=True)
class NewsItem:
    extri_id: str
    company: str
    headline: str
    published_at: str
    raw_time_text: str
    detail_url: str
    row_summary: str
    detail_summary: str = ""
    body: str = ""
    attachments: list[Attachment] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "extri_id": self.extri_id,
            "company": self.company,
            "headline": self.headline,
            "published_at": self.published_at,
            "raw_time_text": self.raw_time_text,
            "detail_url": self.detail_url,
            "row_summary": self.row_summary,
            "detail_summary": self.detail_summary,
            "body": self.body,
            "attachments": [attachment.to_dict() for attachment in self.attachments or []],
        }


@dataclass(slots=True)
class NewsDetail:
    detail_summary: str
    body: str
    attachments: list[Attachment]


class X3NewsClient:
    def __init__(
        self,
        *,
        language: str = DEFAULT_LANGUAGE,
        timeout: int = DEFAULT_TIMEOUT,
        verify: bool = True,
        session: requests.Session | None = None,
    ) -> None:
        self.language = language
        self.timeout = timeout
        self.verify = verify
        self.session = session or requests.Session()
        self.session.headers.setdefault("User-Agent", USER_AGENT)
        if not verify:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def fetch_listing(self, *, limit: int | None = None, now: datetime | None = None) -> list[NewsItem]:
        response = self.session.get(
            BASE_URL,
            params={"page": "news", "language": self.language},
            timeout=self.timeout,
            verify=self.verify,
        )
        response.raise_for_status()
        return parse_listing_html(response.text, language=self.language, now=now, limit=limit)

    def fetch_detail(self, extri_id: str) -> NewsDetail:
        response = self.session.get(
            BASE_URL,
            params={
                "page": "ShowNews",
                "ExtriID": extri_id,
                "output": "ajax",
                "language": self.language,
            },
            timeout=self.timeout,
            verify=self.verify,
        )
        response.raise_for_status()
        return parse_detail_html(response.text)


def parse_listing_html(
    html: str,
    *,
    language: str = DEFAULT_LANGUAGE,
    now: datetime | None = None,
    limit: int | None = None,
) -> list[NewsItem]:
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("div.news-row")
    items: list[NewsItem] = []

    for row in rows:
        trigger = row.find("a", href=EXTRI_ID_RE)
        company_node = row.find("b")
        headline_node = row.select_one(".newsHeaderLink")
        timestamp_node = row.find("span")

        if not trigger or not company_node or not headline_node or not timestamp_node:
            continue

        match = EXTRI_ID_RE.search(trigger["href"])
        if not match:
            continue

        extri_id = match.group(1)
        company = normalize_whitespace(company_node.get_text(" ", strip=True))
        headline = normalize_whitespace(headline_node.get_text(" ", strip=True))
        raw_time_text = normalize_whitespace(timestamp_node.get_text(" ", strip=True))
        detail_url = urljoin(
            BASE_URL,
            f"?page=ShowNews&ExtriID={extri_id}&output=ajax&language={language}",
        )

        items.append(
            NewsItem(
                extri_id=extri_id,
                company=company,
                headline=headline,
                published_at=parse_published_at(raw_time_text, now=now),
                raw_time_text=raw_time_text,
                detail_url=detail_url,
                row_summary=headline,
            )
        )

        if limit is not None and len(items) >= limit:
            break

    return items


def parse_detail_html(html: str) -> NewsDetail:
    soup = BeautifulSoup(html, "html.parser")

    paragraphs: list[str] = []
    for paragraph in soup.find_all("p"):
        text = normalize_whitespace(paragraph.get_text(" ", strip=True))
        if text:
            paragraphs.append(text)

    detail_summary = paragraphs[0] if paragraphs else ""
    body = "\n\n".join(paragraphs)

    attachments_by_url: OrderedDict[str, Attachment] = OrderedDict()
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"].strip()
        if not is_attachment_href(href):
            continue
        absolute_url = urljoin(BASE_URL, href)
        name = normalize_whitespace(anchor.get_text(" ", strip=True)) or absolute_url.rsplit("/", 1)[-1]
        attachments_by_url.setdefault(absolute_url, Attachment(name=name, url=absolute_url))

    return NewsDetail(
        detail_summary=detail_summary,
        body=body,
        attachments=list(attachments_by_url.values()),
    )


def merge_item_with_detail(item: NewsItem, detail: NewsDetail) -> NewsItem:
    return NewsItem(
        extri_id=item.extri_id,
        company=item.company,
        headline=item.headline,
        published_at=item.published_at,
        raw_time_text=item.raw_time_text,
        detail_url=item.detail_url,
        row_summary=item.row_summary,
        detail_summary=detail.detail_summary,
        body=detail.body,
        attachments=detail.attachments,
    )
