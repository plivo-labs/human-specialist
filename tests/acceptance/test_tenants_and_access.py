"""Tenants, credentials, scopes, isolation, and the conventions every public route shares."""

import contextlib
import json
import uuid

from tests.acceptance.conftest import (
    answer_params,
    connect,
    data_frames,
    disposition,
    error,
    ok,
    plivo_post,
    read_stream,
    refused,
    skill,
    sse_frames,
    status_changes,
    team,
)
from tests.acceptance.test_views import grouped
from tests.harness import World

T = "/api/v1/tenants"


def answer_status(world, tenant, token: str) -> int:
    return plivo_post(world, tenant.id, "answer", answer_params(tenant.number()), token).status_code


def test_registration_returns_app_id_and_a_key_with_every_scope(world, tenant):
    body = ok(world.operator.get(f"{T}/{tenant.id}"))
    assert body["plivo_app_id"] and tenant.key.startswith("hs_tk_")
    for path in ("/settings", "/conversations", "/floor", "/events", "/recordings"):
        ok(tenant.api.get("/api/v1" + path))
    assert tenant.agent().login(available=False).agent_token
    assert tenant.auth_token not in world.operator.get(f"{T}/{tenant.id}").text
    assert tenant.auth_token not in world.operator.get(T, params={"limit": 200}).text


def test_registration_creates_a_plivo_application_with_tenant_urls(world, tenant):
    apps = [r for r in world.plivo.requests_to("/Application/") if r.method == "POST" and tenant.auth_id in r.path]
    assert any(
        a.params.get("answer_url") == f"{world.base_url}/plivo/{tenant.id}/answer"
        and a.params.get("hangup_url") == f"{world.base_url}/plivo/{tenant.id}/hangup"
        for a in apps
    )


def test_registering_a_registered_subaccount_is_409_tenant_exists(world, tenant):
    body = {"name": "Again", "plivo_auth_id": tenant.auth_id, "plivo_auth_token": tenant.auth_token}
    err = error(world.operator.post(T, json=body), 409, "tenant_exists")
    assert err["details"]["tenant_id"] == tenant.id


def test_registration_is_refused_when_plivo_rejects_the_credentials(world):
    body = {"name": "Ghost", "plivo_auth_id": "SA" + uuid.uuid4().hex[:18].upper(), "plivo_auth_token": "nope"}
    refused(world.operator.post(T, json=body))
    assert "Ghost" not in [t["name"] for t in ok(world.operator.get(T, params={"limit": 200}))["data"]]


def test_patch_renames_and_reapplies_the_app_urls_at_plivo(world, tenant):
    app_id = ok(world.operator.get(f"{T}/{tenant.id}"))["plivo_app_id"]

    def updates():
        return [r for r in world.plivo.requests_to(f"/Application/{app_id}") if r.method == "POST"]

    before = len(updates())
    ok(world.operator.patch(f"{T}/{tenant.id}", json={"name": "Renamed"}))
    assert ok(world.operator.get(f"{T}/{tenant.id}"))["name"] == "Renamed"
    assert len(updates()) == before + 1
    assert updates()[-1].params["answer_url"] == f"{world.base_url}/plivo/{tenant.id}/answer"


def test_patch_replaces_the_plivo_token_only_when_plivo_accepts_it(world, tenant):
    refused(world.operator.patch(f"{T}/{tenant.id}", json={"plivo_auth_token": "wrong-token"}))
    assert answer_status(world, tenant, tenant.auth_token) != 401
    rotated = world.plivo.rotate_token(tenant.auth_id)
    ok(world.operator.patch(f"{T}/{tenant.id}", json={"plivo_auth_token": rotated}))
    assert answer_status(world, tenant, rotated) != 401
    assert answer_status(world, tenant, tenant.auth_token) == 401


def test_delete_revokes_keys_sessions_streams_and_plivo_resources(world, tenant):
    agent = tenant.agent().login()
    endpoint_id = ok(tenant.api.get(f"/api/v1/agents/{agent.id}"))["endpoint_id"]
    app_id = ok(world.operator.get(f"{T}/{tenant.id}"))["plivo_app_id"]
    stream = tenant.stream(token=agent.stream_token)
    ok(world.operator.delete(f"{T}/{tenant.id}"))
    error(tenant.api.get("/api/v1/settings"), 401)
    error(agent.api().get("/api/v1/me"), 401)
    stream.wait_closed()
    made = {("Endpoint", endpoint_id), ("Application", app_id)}
    world.wait(lambda: not made & world.plivo.resources.keys(), what="the tenant's endpoint and application deleted")


def test_a_change_on_a_replica_whose_cache_bump_fails_reaches_the_first_replica_within_the_job_lag():
    with contextlib.closing(World()) as world:
        tenant = world.new_tenant()
        world.redis("ACL", "SETUSER", "reader", "on", "nopass", "~*", "&*", "+@all", "-@write")
        replica = world.replica(redis_user="reader")

        def ring_timeout() -> int:
            return ok(tenant.api.get("/api/v1/settings"))["ring_timeout_sec"]

        def hangup() -> int:
            return plivo_post(world, tenant.id, "hangup", answer_params("+14155550100"), tenant.auth_token).status_code

        assert ring_timeout() == 30 and hangup() == 200
        ok(replica.client(tenant.key).patch("/api/v1/settings", json={"ring_timeout_sec": 9}))
        world.wait(lambda: ring_timeout() == 9, timeout=30, what="the first reads the change within the job lag")
        ok(replica.operator.delete(f"{T}/{tenant.id}"))
        world.wait(lambda: hangup() == 401, timeout=30, what="the first refuses the deleted tenant's callback")


def test_operator_mints_lists_and_revokes_scoped_keys(world, tenant):
    minted = ok(world.operator.post(f"{T}/{tenant.id}/api-keys", json={"scopes": ["reports"]}))
    listing = world.operator.get(f"{T}/{tenant.id}/api-keys").text
    assert minted["id"] in listing and minted["api_key"] not in listing and tenant.key not in listing
    reports_only = world.client(minted["api_key"])
    ok(reports_only.get("/api/v1/floor"))
    error(reports_only.get("/api/v1/agents"), 403)
    ok(world.operator.delete(f"{T}/{tenant.id}/api-keys/{minted['id']}"))
    error(reports_only.get("/api/v1/floor"), 401)


def test_operator_and_tenant_keys_do_not_cross(world, tenant):
    error(tenant.api.get(T), (401, 403))
    error(tenant.api.post(f"{T}/{tenant.id}/api-keys", json={"scopes": ["admin"]}), (401, 403))
    error(world.operator.get("/api/v1/agents"), (401, 403))
    error(world.operator.post("/api/v1/calls", json={}), (401, 403))


def test_agent_tokens_cannot_use_key_only_routes(tenant):
    agent = tenant.agent().login(available=False)
    cases = [("POST", "/api/v1/queues"), ("GET", "/api/v1/settings"), ("POST", f"/api/v1/agents/{agent.id}/sessions")]
    desk = agent.api()
    bad = [
        (m, p, r.status_code)
        for m, p in cases
        if (r := desk.request(m, p, json={} if m != "GET" else None)).status_code not in (401, 403)
    ]
    assert not bad


def test_the_stream_token_works_only_on_the_stream_and_only_it_rides_in_the_query(world, tenant):
    agent = tenant.agent().login(available=False)
    error(world.client(agent.stream_token).get("/api/v1/me"), (401, 403))
    error(world.client(agent.stream_token).get("/api/v1/events"), (401, 403))
    for token in (agent.agent_token, tenant.key):
        error(world.anon().get("/api/v1/events/stream", params={"access_token": token}), (401, 403))
    error(world.anon().get("/api/v1/me", params={"access_token": agent.stream_token}), (401, 403))


def test_the_stream_token_opens_the_stream_from_the_query(world, tenant):
    agent = tenant.agent().login(available=False)
    [event] = status_changes(tenant, agent, "Away")
    lines = read_stream(world.anon(), data_frames(1), params={"access_token": agent.stream_token, "after": 0})
    assert json.loads(next(f for f in sse_frames(lines) if "data" in f)["data"])["id"] == event["id"]


def test_another_tenants_number_cannot_be_taken(world, tenant):
    number, _ = tenant.inbound_line(tenant.agent())
    refused(world.new_tenant("Other").api.put(f"/api/v1/numbers/{number}", json={"open_action": {"type": "hangup"}}))
    ok(tenant.api.get(f"/api/v1/numbers/{number}"))


def test_another_tenants_conversation_and_call_ids_are_404(world, tenant):
    live = connect(tenant)
    other = world.new_tenant("Other")
    attempts = [
        ("GET", f"/conversations/{live.id}", None),
        ("GET", f"/conversations/{live.id}/timeline", None),
        ("POST", f"/conversations/{live.id}/notes", {"text": "x"}),
        ("POST", f"/calls/{live.id}/hold", {"on": True}),
        ("POST", f"/calls/{live.id}/recording", {"action": "pause"}),
        ("POST", f"/calls/{live.id}/participants", {"target": {"type": "number", "number": "+14155550199"}}),
        ("POST", f"/calls/{live.id}/monitor", {"mode": "listen", "supervisor_id": other.agent(role="supervisor").id}),
        ("POST", f"/calls/{live.id}/hangup", None),
    ]
    bad = [
        (m, p, r.status_code)
        for m, p, b in attempts
        if (r := other.api.request(m, "/api/v1" + p, json=b)).status_code != 404
    ]
    assert not bad and live.caller.state == "in_progress"


def test_foreign_ids_in_bodies_are_422(world, tenant):
    other = world.new_tenant("Other")
    foreign_skill = [{"skill_id": skill(other), "operator": ">=", "level": 1}]
    attempts = [
        ("POST", "/api/v1/queues", {"name": "Q", "agents": [other.agent().id]}),
        ("POST", "/api/v1/queues", {"name": "Q", "teams": [team(other)]}),
        ("POST", "/api/v1/queues", {"name": "Q", "skills": foreign_skill}),
        ("POST", "/api/v1/agents", {"name": "A", "role": "agent", "teams": [team(other)]}),
        (
            "PUT",
            f"/api/v1/numbers/{tenant.number()}",
            {"open_action": {"type": "queue", "queue_id": other.queue(name="X")["id"]}},
        ),
    ]
    bad = [
        (m, p, b, r.status_code) for m, p, b in attempts if (r := tenant.api.request(m, p, json=b)).status_code != 422
    ]
    assert not bad


def test_another_tenants_ids_in_a_search_or_export_body_are_422(world, tenant):
    other = world.new_tenant("Other")
    theirs = {"queue_id": other.queue(name="X")["id"], "team_id": team(other), "agent_id": other.agent().id}
    theirs["disposition_id"] = disposition(other)["id"]
    for path in ("search", "export", "export/content"):
        for field, value in theirs.items():
            for body in ({field: value}, grouped({"field": field, "op": "is", "value": [value]})):
                error(tenant.api.post(f"/api/v1/conversations/{path}", json=body), 422, "foreign_id")
        assert tenant.api.post(f"/api/v1/conversations/{path}", json={"team_id": team(tenant)}).status_code == 200


def test_errors_share_one_envelope(tenant):
    error(tenant.api.get(f"/api/v1/queues/{uuid.uuid4()}"), 404)
    err = error(tenant.api.post("/api/v1/queues", json={"name": 5, "agents_at_once": "many"}), 422)
    assert err["details"]["fields"]
    error(tenant.api.post("/api/v1/queues", content=b"{not json", headers={"Content-Type": "application/json"}), 400)


def test_lists_page_with_limit_and_cursor(tenant):
    for n in range(3):
        team(tenant, f"T{n}")
    first = ok(tenant.api.get("/api/v1/teams", params={"limit": 2}))
    assert len(first["data"]) == 2 and first["next_cursor"]
    rest = ok(tenant.api.get("/api/v1/teams", params={"limit": 2, "cursor": first["next_cursor"]}))
    assert len(rest["data"]) == 1 and not rest.get("next_cursor")
    assert {t["name"] for t in first["data"] + rest["data"]} == {"T0", "T1", "T2"}
    error(tenant.api.get("/api/v1/teams", params={"limit": 201}), 422)


def test_an_idempotent_replay_returns_the_stored_response_and_a_new_body_is_409(tenant):
    key = {"Idempotency-Key": str(uuid.uuid4())}
    first = tenant.api.post("/api/v1/teams", json={"name": "Once"}, headers=key)
    again = tenant.api.post("/api/v1/teams", json={"name": "Once"}, headers=key)
    assert (again.status_code, again.json()) == (first.status_code, first.json())
    error(tenant.api.post("/api/v1/teams", json={"name": "Twice"}, headers=key), 409, "idempotency_conflict")
    assert [t["name"] for t in ok(tenant.api.get("/api/v1/teams"))["data"]] == ["Once"]


def test_idempotency_keys_are_scoped_per_principal(tenant):
    key = {"Idempotency-Key": str(uuid.uuid4())}
    ok(tenant.api.post("/api/v1/teams", json={"name": "Mine"}, headers=key))
    ok(tenant.key_with("admin").post("/api/v1/teams", json={"name": "Yours"}, headers=key))
    assert {t["name"] for t in ok(tenant.api.get("/api/v1/teams"))["data"]} == {"Mine", "Yours"}


def test_replaying_a_secret_route_is_409_without_the_secret(world, tenant):
    routes = [
        (tenant.api, f"/api/v1/agents/{tenant.agent().id}/sessions", None),
        (tenant.api, f"/api/v1/agents/{tenant.agent().id}/sip-credentials", None),
        (world.operator, f"{T}/{tenant.id}/api-keys", {"scopes": ["admin"]}),
        (tenant.api, "/api/v1/webhooks", {"url": world.webhook_sink().url}),
    ]
    for client, path, body in routes:
        key = {"Idempotency-Key": str(uuid.uuid4())}
        first = ok(client.post(path, json=body, headers=key))
        again = client.post(path, json=body, headers=key)
        error(again, 409, "idempotency_replay_secret")
        assert not [
            k for k in ("agent_token", "password", "api_key", "secret") if k in first and str(first[k]) in again.text
        ]


def test_replaying_a_tenant_registration_is_409_without_the_key(world):
    sub = world.plivo.subaccount("Replay")
    body = {"name": "Replay", "plivo_auth_id": sub.auth_id, "plivo_auth_token": sub.auth_token}
    key = {"Idempotency-Key": str(uuid.uuid4())}
    first = ok(world.operator.post(T, json=body, headers=key))
    again = world.operator.post(T, json=body, headers=key)
    error(again, 409, "idempotency_replay_secret")
    assert first["api_key"] not in again.text


def test_cors_allows_configured_origins_with_the_contract_headers(world, tenant):
    asked = "authorization, content-type, idempotency-key, last-event-id"
    pre = world.anon().options(
        "/api/v1/agents",
        headers={
            "Origin": world.cors_origin,
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": asked,
        },
    )
    assert pre.headers["access-control-allow-origin"] == world.cors_origin
    assert all(h in pre.headers["access-control-allow-headers"].lower() for h in asked.split(", "))
    assert pre.headers.get("access-control-allow-credentials") != "true"
    exposed = tenant.api.get("/api/v1/settings", headers={"Origin": world.cors_origin}).headers[
        "access-control-expose-headers"
    ]
    assert "retry-after" in exposed.lower() and "x-request-id" in exposed.lower()


def test_cors_never_allows_other_origins_or_plivo_routes(world, tenant):
    evil = world.anon().options(
        "/api/v1/agents", headers={"Origin": "https://evil.example", "Access-Control-Request-Method": "GET"}
    )
    assert evil.headers.get("access-control-allow-origin") not in ("https://evil.example", "*")
    plivo = world.anon().options(
        f"/plivo/{tenant.id}/answer", headers={"Origin": world.cors_origin, "Access-Control-Request-Method": "POST"}
    )
    assert "access-control-allow-origin" not in plivo.headers


def test_only_the_public_surface_is_mounted(world):
    assert world.anon().get("/metrics").status_code == 404
    status = ok(world.anon().get("/status"))
    assert status and all(v is True for v in status.values())
    paths = ok(world.anon().get("/openapi.json"))["paths"]
    assert "/api/v1/calls" in paths and not [p for p in paths if p.startswith("/v1/")]


def test_registration_relays_why_plivo_refused_the_application(world):
    sub = world.plivo.subaccount("Bad")
    world.plivo.reject_next("/Application/", 400, account=sub.auth_id)
    body = {"name": "Bad", "plivo_auth_id": sub.auth_id, "plivo_auth_token": sub.auth_token}
    err = error(world.operator.post(T, json=body), 422, "plivo_refused")
    assert err["details"] == {"status": 400, "errors": {"error": "rejected by the test"}}
    assert sub.auth_token not in str(err)
