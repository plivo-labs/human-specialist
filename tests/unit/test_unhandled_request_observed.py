"""Every response the catch-all handler builds must count once in both request metrics and carry the
request id. That handler runs in Starlette's outermost ServerErrorMiddleware, past
add_response_time_metric and add_request_id.
"""

import pytest
import redis
from fastapi.testclient import TestClient
from prometheus_client import REGISTRY
from starlette.exceptions import HTTPException
from starlette.requests import ClientDisconnect

from hs.app import REQUEST_ID_HEADER, app

_PATH = "/__test_ser6518_probe"


def _sample(name, labels):
    for metric in REGISTRY.collect():
        for s in metric.samples:
            if s.name == name and s.labels == labels:
                return s.value
    return 0.0


def _counts(status):
    code = str(status)
    hist = _sample(
        "hs_http_request_response_seconds_count", {"endpoint": _PATH, "method": "GET", "response_code": code}
    )
    outcome = _sample("hs_request_outcome_total", {"route_family": _PATH, "method": "GET", "status": code})
    return hist, outcome


@pytest.fixture()
def probe():
    fault = {}

    @app.get(_PATH)
    async def _probe():
        if "exc" in fault:
            raise fault["exc"]
        return {"ok": True}

    try:
        yield fault
    finally:
        app.router.routes = [r for r in app.router.routes if getattr(r, "path", None) != _PATH]


def _body_read_disconnect() -> HTTPException:
    """FastAPI's body read wraps a disconnect in HTTPException(400)."""
    wrapped = HTTPException(400, "There was an error parsing the body")
    wrapped.__cause__ = ClientDisconnect()
    return wrapped


@pytest.mark.parametrize(
    "exc, status",
    [
        (RuntimeError("injected"), 500),
        (redis.ConnectionError("redis at 10.0.0.9 refused"), 503),
        (ClientDisconnect(), 499),
        (_body_read_disconnect(), 499),
        (None, 200),
    ],
    ids=["uncaught-500", "redis-down-503", "disconnect-499", "wrapped-disconnect-499", "success-200"],
)
def test_every_response_is_observed_exactly_once(probe, exc, status):
    fault = probe
    if exc is not None:
        fault["exc"] = exc
    before = _counts(status)

    resp = TestClient(app, raise_server_exceptions=False).get(_PATH, headers={REQUEST_ID_HEADER: "ser6518-req"})

    assert resp.status_code == status
    assert resp.headers[REQUEST_ID_HEADER] == "ser6518-req"
    assert _counts(status) == (before[0] + 1, before[1] + 1)
    if status >= 500:
        assert resp.json()["error"]["code"] == {500: "internal_error", 503: "dependency_unavailable"}[status]
        assert "10.0.0.9" not in resp.text
