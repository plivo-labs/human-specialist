"""Request-id correlation middleware (hs/app.py::add_request_id).

Covers:
  - no inbound X-Request-Id -> a generated uuid4-hex header comes back on the response.
  - inbound X-Request-Id -> passed through unchanged (not regenerated).
  - the request_id is visible inside a handler via the context_request_id contextvar
    (i.e. the middleware runs OUTSIDE/EARLIER than request handling).

Uses the shared offline `client` fixture (tests/conftest.py) against a dependency-free
route (/status) for the header-only assertions, and a throwaway route registered/removed
around the contextvar-visibility test so as not to leak state into the shared `app` used
by the rest of the suite.
"""

import uuid

from fastapi.testclient import TestClient

from hs.app import REQUEST_ID_HEADER, app
from hs.infra.context import context_request_id


def test_generates_request_id_header_when_absent(client):
    resp = client.get("/status")
    assert resp.status_code == 200
    assert REQUEST_ID_HEADER in resp.headers
    generated = resp.headers[REQUEST_ID_HEADER]
    # uuid4().hex shape: 32 lowercase hex chars, no dashes.
    assert len(generated) == 32
    uuid.UUID(generated)  # parses fine as a uuid4 despite the hex (no-dash) rendering


def test_passes_through_inbound_request_id(client):
    inbound = "caller-supplied-request-id-123"
    resp = client.get("/status", headers={REQUEST_ID_HEADER: inbound})
    assert resp.status_code == 200
    assert resp.headers[REQUEST_ID_HEADER] == inbound


def test_each_request_gets_a_distinct_generated_id(client):
    first = client.get("/status").headers[REQUEST_ID_HEADER]
    second = client.get("/status").headers[REQUEST_ID_HEADER]
    assert first != second


def test_request_id_visible_in_context_inside_handler():
    """The contextvar the middleware populates must be readable by a handler running
    "inside" it — proving add_request_id wraps EARLIER than route dispatch."""
    seen = {}

    @app.get("/__test_request_id_probe")
    async def _probe():
        seen["request_id"] = context_request_id.get()
        return {"ok": True}

    try:
        with TestClient(app) as c:
            inbound = "probe-request-id-abc"
            resp = c.get("/__test_request_id_probe", headers={REQUEST_ID_HEADER: inbound})
            assert resp.status_code == 200
            assert resp.headers[REQUEST_ID_HEADER] == inbound
            assert seen["request_id"] == inbound
    finally:
        app.router.routes = [r for r in app.router.routes if getattr(r, "path", None) != "/__test_request_id_probe"]
