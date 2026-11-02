"""The Redis client must be built with socket-level timeouts so
a hung/blackholed Redis fails fast into the existing graceful-degradation `except`
paths instead of blocking the event-loop thread forever.

Pins: socket_connect_timeout/socket_timeout/health_check_interval are passed to
`Redis.from_url`; retry_on_timeout is NOT set (routing
Lua is non-idempotent). Also pins the `mget` failure log message (copy-paste fix).
"""

import redis as redis_lib

from hs.infra.redis import Cache

_EXPECTED = {"socket_connect_timeout": 2, "socket_timeout": 5, "health_check_interval": 30}


def test_single_node_client_built_with_timeouts(monkeypatch):
    captured = {}

    def fake_from_url(url, **kwargs):
        captured["url"] = url
        captured["kwargs"] = kwargs
        return object()

    monkeypatch.setattr(redis_lib.Redis, "from_url", staticmethod(fake_from_url))
    c = Cache()
    _ = c.client
    for k, v in _EXPECTED.items():
        assert captured["kwargs"].get(k) == v, f"{k} not passed to Redis.from_url"
    assert captured["kwargs"].get("decode_responses") is True
    assert "retry_on_timeout" not in captured["kwargs"], "retry_on_timeout must NOT be set (non-idempotent Lua)"
