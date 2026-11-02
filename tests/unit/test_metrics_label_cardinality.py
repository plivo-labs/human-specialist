"""Prometheus label cardinality must stay bounded for unmatched (404) requests.

Labelling with the RAW url path would mint a new series per scanner/typo URL for the life of the
process. Matched routes keep their route-template labels.
"""

import tests.conftest  # noqa: F401


def _capture(monkeypatch):
    import hs.app as app_mod

    seen = {"observe": [], "outcome": []}
    monkeypatch.setattr(
        app_mod.http_request_response_metric, "observe", lambda labels, value: seen["observe"].append(labels)
    )
    monkeypatch.setattr(
        app_mod, "record_request_outcome", lambda path, method, status: seen["outcome"].append((path, method, status))
    )
    return seen


def test_unmatched_404s_collapse_to_one_label(client, monkeypatch):
    seen = _capture(monkeypatch)
    for p in ("/wp-admin/setup.php", "/.env", "/totally/made/up/scanner/path-123abc"):
        client.get(p)
    endpoints = {labels["endpoint"] for labels in seen["observe"]}
    assert endpoints == {"<unmatched>"}  # one series, not one per URL
    assert {o[0] for o in seen["outcome"]} == {"<unmatched>"}


def test_matched_static_route_label_unchanged(client, monkeypatch):
    seen = _capture(monkeypatch)
    client.get("/status")
    assert seen["observe"][-1]["endpoint"] == "/status"
    assert seen["outcome"][-1][0] == "/status"


def test_matched_param_route_label_is_templated_not_raw(client, monkeypatch):
    seen = _capture(monkeypatch)
    # Auth-rejected (401) but route-MATCHED: the label must be the templated path, and the
    # 401 must not fall into the unmatched bucket.
    client.get("/api/v1/queues/00000000-0000-4000-a000-000000000001/stats")
    labels = seen["observe"][-1]
    assert labels["endpoint"] != "<unmatched>"
    assert ":queue_id" in labels["endpoint"]


def test_outcome_label_is_the_route_template_for_uncollapsible_ids(client, monkeypatch):
    seen = _capture(monkeypatch)
    for n in range(3):
        client.get(f"/api/v1/queues/00000000-0000-4000-a000-00000000000{n}/stats")
    assert len(seen["outcome"]) == 3
    assert {o[0] for o in seen["outcome"]} == {"/api/v1/queues/{queue_id}/stats"}
