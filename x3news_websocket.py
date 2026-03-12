from __future__ import annotations

import argparse
import asyncio
import base64
import json
import logging
import os
from collections import OrderedDict
from datetime import datetime
from typing import Any
from urllib.parse import parse_qs, urlparse

from websockets.exceptions import ConnectionClosed
from websockets.legacy.server import WebSocketServerProtocol, serve

from x3news_feed import NewsDetail, NewsItem, X3NewsClient, merge_item_with_detail
from x3news_store import EventStore


LOGGER = logging.getLogger("x3news.websocket")
_RESERVED_LOG_ATTRS = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__.keys())


def env_str(name: str, default: str) -> str:
    return os.getenv(name, default)


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value else default


def env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    return float(value) if value else default


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def env_json(name: str, default: dict[str, str] | None = None) -> dict[str, str]:
    value = os.getenv(name)
    if not value:
        return default or {}
    parsed = json.loads(value)
    if not isinstance(parsed, dict):
        raise ValueError(f"{name} must be a JSON object mapping client ids to tokens")
    return {str(key): str(token) for key, token in parsed.items()}


def env_client_tokens(default: dict[str, str] | None = None) -> dict[str, str]:
    encoded = os.getenv("X3NEWS_CLIENT_TOKENS_B64")
    if encoded:
        decoded = base64.b64decode(encoded).decode("utf-8")
        parsed = json.loads(decoded)
        if not isinstance(parsed, dict):
            raise ValueError("X3NEWS_CLIENT_TOKENS_B64 must decode to a JSON object")
        return {str(key): str(token) for key, token in parsed.items()}
    return env_json("X3NEWS_CLIENT_TOKENS_JSON", default)


def utc_now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat(timespec="milliseconds") + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key in _RESERVED_LOG_ATTRS or key in payload:
                continue
            payload[key] = value
        return json.dumps(payload, ensure_ascii=False, default=str)


def configure_logging(level_name: str, log_format: str) -> None:
    root = logging.getLogger()
    root.handlers.clear()
    handler = logging.StreamHandler()
    if log_format == "json":
        handler.setFormatter(JsonLogFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    root.addHandler(handler)
    root.setLevel(getattr(logging, level_name))


def log_event(level: int, event: str, **fields: Any) -> None:
    LOGGER.log(level, event, extra={"event": event, **fields})


class WebSocketAuthenticator:
    def __init__(self, client_tokens: dict[str, str]) -> None:
        self.client_tokens = client_tokens
        self.token_to_client_id = {token: client_id for client_id, token in client_tokens.items()}

    @property
    def enabled(self) -> bool:
        return bool(self.token_to_client_id)

    def authenticate(
        self,
        path: str,
        request_headers: Any,
    ) -> tuple[str | None, str | None]:
        if not self.enabled:
            return "anonymous", None

        parsed = urlparse(path)
        token = parse_qs(parsed.query).get("token", [None])[0]

        auth_header = None
        if request_headers is not None:
            auth_header = request_headers.get("Authorization")
        if not token and auth_header and auth_header.startswith("Bearer "):
            token = auth_header[7:].strip()

        if not token:
            return None, "Missing token"

        client_id = self.token_to_client_id.get(token)
        if client_id is None:
            return None, "Invalid token"

        return client_id, None


class ReplayRequest:
    def __init__(self, mode: str, limit: int, since_extri_id: str | None = None) -> None:
        self.mode = mode
        self.limit = limit
        self.since_extri_id = since_extri_id


def parse_replay_request(path: str, replay_max_items: int) -> ReplayRequest | None:
    parsed = urlparse(path)
    query = parse_qs(parsed.query)

    since_extri_id = query.get("since_extri_id", [None])[0]
    replay_raw = query.get("replay", [None])[0]
    replay_limit = None
    if replay_raw:
        try:
            replay_limit = max(1, min(int(replay_raw), replay_max_items))
        except ValueError:
            replay_limit = replay_max_items

    if since_extri_id:
        limit = replay_limit or replay_max_items
        return ReplayRequest(mode="since_extri_id", limit=limit, since_extri_id=since_extri_id)

    if replay_limit is not None:
        return ReplayRequest(mode="recent", limit=replay_limit)

    return None


class X3NewsWebSocketService:
    def __init__(
        self,
        *,
        client: X3NewsClient,
        poll_interval: float,
        snapshot_size: int,
        listing_size: int,
        max_seen_ids: int = 5000,
        replay_max_items: int = 100,
        authenticator: WebSocketAuthenticator | None = None,
        store: EventStore | None = None,
    ) -> None:
        self.client = client
        self.poll_interval = poll_interval
        self.snapshot_size = snapshot_size
        self.listing_size = max(snapshot_size, listing_size)
        self.max_seen_ids = max_seen_ids
        self.replay_max_items = replay_max_items
        self.authenticator = authenticator or WebSocketAuthenticator({})
        self.store = store or EventStore("./data/x3news.db")
        self.clients: set[WebSocketServerProtocol] = set()
        self.client_metadata: dict[WebSocketServerProtocol, dict[str, Any]] = {}
        self._seen_ids: OrderedDict[str, None] = OrderedDict()
        self._detail_cache: OrderedDict[str, NewsDetail] = OrderedDict()
        self._snapshot: list[dict[str, Any]] = []
        self._initialized = False

    async def bootstrap(self) -> None:
        await self.poll_once()

    async def poll_forever(self) -> None:
        while True:
            try:
                await self.poll_once()
            except Exception:
                LOGGER.exception("Polling cycle failed", extra={"event": "poll_failed"})
            await asyncio.sleep(self.poll_interval)

    async def poll_once(self) -> None:
        items = await asyncio.to_thread(self.client.fetch_listing, limit=self.listing_size)
        if not items:
            log_event(logging.WARNING, "listing_empty")
            return

        enriched_for_snapshot = await self._enrich_many(items[: self.snapshot_size])
        self._snapshot = [item.to_dict() for item in enriched_for_snapshot]

        if not self._initialized:
            enriched_for_bootstrap = await self._enrich_many(items)
            seen_at = utc_now_iso()
            for item in enriched_for_bootstrap:
                self.store.upsert_item(item, seen_at)
                self._remember_seen(item.extri_id)
            self._initialized = True
            log_event(
                logging.INFO,
                "bootstrap_completed",
                listing_items=len(items),
                snapshot_size=len(self._snapshot),
                stored_events=self.store.count(),
            )
            return

        unseen_items = [item for item in items if item.extri_id not in self._seen_ids]
        if not unseen_items:
            return

        for item in reversed(unseen_items):
            enriched = await self._enrich_one(item)
            self.store.upsert_item(enriched, utc_now_iso())
            payload = {
                "type": "news",
                "generated_at": utc_now_iso(),
                "item": enriched.to_dict(),
            }
            await self._broadcast(payload)
            self._remember_seen(item.extri_id)
            log_event(
                logging.INFO,
                "news_broadcast",
                extri_id=item.extri_id,
                company=item.company,
                connected_clients=len(self.clients),
            )

    async def handler(self, websocket: WebSocketServerProtocol, path: str) -> None:
        request_path = urlparse(path).path
        replay_request = parse_replay_request(path, self.replay_max_items)
        client_id, auth_error = self.authenticator.authenticate(
            path,
            getattr(websocket, "request_headers", None),
        )
        remote_ip, remote_port = self._remote_details(websocket.remote_address)

        if auth_error is not None:
            log_event(
                logging.WARNING,
                "client_auth_rejected",
                reason=auth_error,
                remote_ip=remote_ip,
                remote_port=remote_port,
                path=request_path,
            )
            await websocket.close(code=4401, reason="Unauthorized")
            return

        if request_path not in {"/", "/ws"}:
            await websocket.close(code=1008, reason="Use /ws")
            return

        self.clients.add(websocket)
        metadata = {
            "client_id": client_id,
            "remote_ip": remote_ip,
            "remote_port": remote_port,
            "path": request_path,
        }
        self.client_metadata[websocket] = metadata
        log_event(
            logging.INFO,
            "client_connected",
            client_id=client_id,
            remote_ip=remote_ip,
            remote_port=remote_port,
            path=request_path,
            connected_clients=len(self.clients),
        )
        try:
            if replay_request is not None:
                replay_items = self._load_replay_items(replay_request)
                await websocket.send(
                    json.dumps(
                        {
                            "type": "replay",
                            "generated_at": utc_now_iso(),
                            "mode": replay_request.mode,
                            "requested_limit": replay_request.limit,
                            "since_extri_id": replay_request.since_extri_id,
                            "items": replay_items,
                        },
                        ensure_ascii=False,
                    )
                )
                log_event(
                    logging.INFO,
                    "replay_sent",
                    client_id=client_id,
                    remote_ip=remote_ip,
                    remote_port=remote_port,
                    replay_mode=replay_request.mode,
                    replay_items=len(replay_items),
                    since_extri_id=replay_request.since_extri_id,
                )
            else:
                await websocket.send(
                    json.dumps(
                        {
                            "type": "snapshot",
                            "generated_at": utc_now_iso(),
                            "items": self._snapshot,
                        },
                        ensure_ascii=False,
                    )
                )
            async for _message in websocket:
                # The server is push-only. Incoming messages are ignored.
                continue
        except ConnectionClosed:
            pass
        finally:
            self.clients.discard(websocket)
            metadata = self.client_metadata.pop(websocket, metadata)
            log_event(
                logging.INFO,
                "client_disconnected",
                client_id=metadata.get("client_id"),
                remote_ip=metadata.get("remote_ip"),
                remote_port=metadata.get("remote_port"),
                connected_clients=len(self.clients),
            )

    async def _broadcast(self, payload: dict[str, Any]) -> None:
        if not self.clients:
            return

        message = json.dumps(payload, ensure_ascii=False)
        stale_clients: list[WebSocketServerProtocol] = []
        for client in list(self.clients):
            try:
                await client.send(message)
            except ConnectionClosed:
                stale_clients.append(client)

        for client in stale_clients:
            self.clients.discard(client)
            metadata = self.client_metadata.pop(client, {})
            log_event(
                logging.INFO,
                "client_pruned",
                client_id=metadata.get("client_id"),
                remote_ip=metadata.get("remote_ip"),
                remote_port=metadata.get("remote_port"),
                connected_clients=len(self.clients),
            )

    async def _enrich_many(self, items: list[NewsItem]) -> list[NewsItem]:
        enriched: list[NewsItem] = []
        for item in items:
            enriched.append(await self._enrich_one(item))
        return enriched

    async def _enrich_one(self, item: NewsItem) -> NewsItem:
        detail = self._detail_cache.get(item.extri_id)
        if detail is None:
            detail = await asyncio.to_thread(self.client.fetch_detail, item.extri_id)
            self._detail_cache[item.extri_id] = detail
            while len(self._detail_cache) > self.max_seen_ids:
                self._detail_cache.popitem(last=False)
        return merge_item_with_detail(item, detail)

    def _remember_seen(self, extri_id: str) -> None:
        self._seen_ids[extri_id] = None
        self._seen_ids.move_to_end(extri_id)
        while len(self._seen_ids) > self.max_seen_ids:
            self._seen_ids.popitem(last=False)

    def _load_replay_items(self, replay_request: ReplayRequest) -> list[dict[str, Any]]:
        if replay_request.mode == "since_extri_id" and replay_request.since_extri_id:
            return self.store.get_items_after_extri_id(replay_request.since_extri_id, replay_request.limit)

        replay_items = self.store.get_recent_items(replay_request.limit)
        replay_items.reverse()
        return replay_items

    @staticmethod
    def _remote_details(remote_address: Any) -> tuple[str | None, int | None]:
        if isinstance(remote_address, tuple) and len(remote_address) >= 2:
            return str(remote_address[0]), int(remote_address[1])
        return None, None


async def run_server(args: argparse.Namespace) -> None:
    client = X3NewsClient(language=args.language, timeout=args.timeout, verify=not args.insecure)
    authenticator = WebSocketAuthenticator(args.client_tokens_json)
    store = EventStore(args.db_path)
    service = X3NewsWebSocketService(
        client=client,
        poll_interval=args.poll_interval,
        snapshot_size=args.snapshot_size,
        listing_size=args.listing_size,
        max_seen_ids=args.max_seen_ids,
        replay_max_items=args.replay_max_items,
        authenticator=authenticator,
        store=store,
    )

    try:
        await service.bootstrap()
    except Exception:
        LOGGER.exception("Initial bootstrap failed; starting with an empty snapshot", extra={"event": "bootstrap_failed"})

    async with serve(
        service.handler,
        args.host,
        args.port,
        ping_interval=args.ping_interval,
        ping_timeout=args.ping_timeout,
    ):
        log_event(
            logging.INFO,
            "server_started",
            host=args.host,
            port=args.port,
            language=args.language,
            poll_interval=args.poll_interval,
            snapshot_size=args.snapshot_size,
            listing_size=args.listing_size,
            auth_enabled=authenticator.enabled,
            replay_max_items=args.replay_max_items,
            db_path=args.db_path,
        )
        await service.poll_forever()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Stream X3News updates over WebSocket")
    parser.add_argument("--host", default=env_str("X3NEWS_HOST", "0.0.0.0"), help="Bind host")
    parser.add_argument("--port", default=env_int("X3NEWS_PORT", 8765), type=int, help="Bind port")
    parser.add_argument(
        "--language",
        default=env_str("X3NEWS_LANGUAGE", "en"),
        choices=("en", "bg"),
        help="X3News language",
    )
    parser.add_argument(
        "--poll-interval",
        default=env_float("X3NEWS_POLL_INTERVAL", 30.0),
        type=float,
        help="Seconds between listing polls",
    )
    parser.add_argument(
        "--timeout",
        default=env_int("X3NEWS_TIMEOUT", 20),
        type=int,
        help="HTTP timeout in seconds",
    )
    parser.add_argument(
        "--insecure",
        action="store_true",
        default=env_bool("X3NEWS_INSECURE", False),
        help="Skip TLS certificate verification for x3news.com",
    )
    parser.add_argument(
        "--snapshot-size",
        default=env_int("X3NEWS_SNAPSHOT_SIZE", 5),
        type=int,
        help="Items sent immediately on connect",
    )
    parser.add_argument(
        "--listing-size",
        default=env_int("X3NEWS_LISTING_SIZE", 15),
        type=int,
        help="Rows fetched from the first listing page",
    )
    parser.add_argument(
        "--max-seen-ids",
        default=env_int("X3NEWS_MAX_SEEN_IDS", 5000),
        type=int,
        help="In-memory deduplication size",
    )
    parser.add_argument(
        "--replay-max-items",
        default=env_int("X3NEWS_REPLAY_MAX_ITEMS", 100),
        type=int,
        help="Maximum replay items served to one client",
    )
    parser.add_argument(
        "--db-path",
        default=env_str("X3NEWS_DB_PATH", "./data/x3news.db"),
        help="SQLite database path for durable replay storage",
    )
    parser.add_argument(
        "--ping-interval",
        default=env_float("X3NEWS_PING_INTERVAL", 20.0),
        type=float,
        help="WebSocket ping interval",
    )
    parser.add_argument(
        "--ping-timeout",
        default=env_float("X3NEWS_PING_TIMEOUT", 20.0),
        type=float,
        help="WebSocket ping timeout",
    )
    parser.add_argument(
        "--log-level",
        default=env_str("X3NEWS_LOG_LEVEL", "INFO"),
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="Logging verbosity",
    )
    parser.add_argument(
        "--log-format",
        default=env_str("X3NEWS_LOG_FORMAT", "json"),
        choices=("json", "text"),
        help="Logging format",
    )
    parser.add_argument(
        "--client-tokens-json",
        default=env_client_tokens({}),
        type=json.loads,
        help="JSON object mapping client ids to websocket auth tokens",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    configure_logging(args.log_level, args.log_format)
    asyncio.run(run_server(args))


if __name__ == "__main__":
    main()
