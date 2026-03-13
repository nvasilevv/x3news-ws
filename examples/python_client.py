from __future__ import annotations

import argparse
import asyncio
import json
import ssl

from websockets import connect


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Consume the X3News websocket feed")
    parser.add_argument("--url", required=True, help="WebSocket URL, for example wss://x3news.nikivs.com/ws")
    parser.add_argument("--token", required=True, help="Client token")
    parser.add_argument("--replay", type=int, default=None, help="Replay the most recent N stored events on connect")
    parser.add_argument("--since-extri-id", default=None, help="Replay events seen after this ExtriID")
    parser.add_argument("--insecure", action="store_true", help="Skip TLS certificate verification")
    return parser


def build_ws_url(base_url: str, token: str, replay: int | None, since_extri_id: str | None) -> str:
    query = [f"token={token}"]
    if replay is not None:
        query.append(f"replay={replay}")
    if since_extri_id:
        query.append(f"since_extri_id={since_extri_id}")
    separator = "&" if "?" in base_url else "?"
    return f"{base_url}{separator}{'&'.join(query)}"


async def run_client(url: str, insecure: bool) -> None:
    ssl_context = None
    if url.startswith("wss://") and insecure:
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

    async with connect(url, ssl=ssl_context) as websocket:
        print(f"connected {url}")
        async for message in websocket:
            payload = json.loads(message)
            print(json.dumps(payload, ensure_ascii=False, indent=2))


def main() -> None:
    args = build_parser().parse_args()
    url = build_ws_url(args.url, args.token, args.replay, args.since_extri_id)
    asyncio.run(run_client(url, args.insecure))


if __name__ == "__main__":
    main()
