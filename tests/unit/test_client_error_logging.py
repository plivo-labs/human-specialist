"""A 4xx reply is the caller's error, not the service's: it logs no ERROR line (the reply body carries the detail)."""

import hs.errors as errors


def test_an_unknown_route_logs_no_error(client, monkeypatch):
    seen = []
    monkeypatch.setattr(errors.log, "error", lambda *a, **k: seen.append((a, k)))
    r = client.get("/no/such/route")
    assert (r.status_code, r.json()["error"]["code"], seen) == (404, "not_found", [])
