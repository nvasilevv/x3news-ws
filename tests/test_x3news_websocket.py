from __future__ import annotations

import unittest

from x3news_websocket import WebSocketAuthenticator, parse_replay_request


class Headers(dict):
    def get(self, key, default=None):  # type: ignore[override]
        return super().get(key, default)


class WebSocketAuthenticatorTests(unittest.TestCase):
    def test_authenticates_query_token(self) -> None:
        authenticator = WebSocketAuthenticator({"dashboard": "secret-1"})
        client_id, error = authenticator.authenticate("/ws?token=secret-1", Headers())
        self.assertEqual(client_id, "dashboard")
        self.assertIsNone(error)

    def test_authenticates_bearer_token(self) -> None:
        authenticator = WebSocketAuthenticator({"dashboard": "secret-1"})
        client_id, error = authenticator.authenticate(
            "/ws",
            Headers({"Authorization": "Bearer secret-1"}),
        )
        self.assertEqual(client_id, "dashboard")
        self.assertIsNone(error)

    def test_rejects_missing_token_when_auth_enabled(self) -> None:
        authenticator = WebSocketAuthenticator({"dashboard": "secret-1"})
        client_id, error = authenticator.authenticate("/ws", Headers())
        self.assertIsNone(client_id)
        self.assertEqual(error, "Missing token")

    def test_allows_anonymous_when_auth_disabled(self) -> None:
        authenticator = WebSocketAuthenticator({})
        client_id, error = authenticator.authenticate("/ws", Headers())
        self.assertEqual(client_id, "anonymous")
        self.assertIsNone(error)

    def test_parse_recent_replay_request(self) -> None:
        replay_request = parse_replay_request("/ws?replay=25", replay_max_items=100)
        self.assertIsNotNone(replay_request)
        self.assertEqual(replay_request.mode, "recent")
        self.assertEqual(replay_request.limit, 25)

    def test_parse_since_replay_request_caps_limit(self) -> None:
        replay_request = parse_replay_request("/ws?since_extri_id=123&replay=500", replay_max_items=100)
        self.assertIsNotNone(replay_request)
        self.assertEqual(replay_request.mode, "since_extri_id")
        self.assertEqual(replay_request.since_extri_id, "123")
        self.assertEqual(replay_request.limit, 100)

    def test_parse_invalid_replay_request_uses_cap(self) -> None:
        replay_request = parse_replay_request("/ws?replay=abc", replay_max_items=100)
        self.assertIsNotNone(replay_request)
        self.assertEqual(replay_request.limit, 100)


if __name__ == "__main__":
    unittest.main()
