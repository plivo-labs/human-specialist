"""Agents and their softphone endpoints, sessions and tokens, presence, teams, skills, roles and statuses."""

import time
from datetime import datetime

from tests.acceptance.conftest import (
    connect,
    error,
    hs_header,
    ok,
    registered_number,
    set_status,
    settings,
    skill,
    status_changes,
    team,
)

A = "/api/v1/agents"


def status_of(tenant, agent) -> str:
    return ok(tenant.api.get(f"{A}/{agent.id}/status"))["status"]["id"]


def session(tenant, agent) -> dict:
    return ok(tenant.api.post(f"{A}/{agent.id}/sessions"))


def test_creating_an_agent_creates_its_endpoint_and_starts_offline(world, tenant):
    agent = tenant.agent("Ann")
    assert ok(tenant.api.get(f"{A}/{agent.id}"))["endpoint_id"] and agent.endpoint_username
    assert [r for r in world.plivo.requests_to("/Endpoint/") if r.method == "POST" and tenant.auth_id in r.path]
    assert status_of(tenant, agent) == tenant.status_id("Offline")


def test_an_agent_named_like_a_person_gets_its_endpoint(tenant):
    agent = ok(tenant.api.post(A, json={"name": "Agent One", "role": "agent", "external_id": "u-1"}))
    assert agent["name"] == "Agent One" and agent["endpoint_id"]


def test_external_id_is_unique_per_tenant_and_filters_the_list(world, tenant):
    first = ok(tenant.api.post(A, json={"name": "A", "role": "agent", "external_id": "u-1"}))
    err = error(tenant.api.post(A, json={"name": "B", "role": "agent", "external_id": "u-1"}), 409, "agent_exists")
    assert err["details"]["id"] == first["id"]
    tenant.agent("Other")
    assert [a["id"] for a in ok(tenant.api.get(A, params={"external_id": "u-1"}))["data"]] == [first["id"]]
    ok(world.new_tenant("Other").api.post(A, json={"name": "C", "role": "agent", "external_id": "u-1"}))


def test_a_session_returns_agent_stream_and_plivo_tokens(tenant):
    agent = tenant.agent()
    s = session(tenant, agent)
    assert s["agent_token"] and s["stream_token"] and s["agent_token"] != s["stream_token"]
    assert s["plivo"]["endpoint_username"] == agent.endpoint_username and s["plivo"]["access_token"]
    expires = datetime.fromisoformat(s["expires_at"]).timestamp()
    assert 0 < expires - time.time() <= 3600 + 60
    assert expires >= datetime.fromisoformat(s["plivo"]["expires_at"]).timestamp()


def test_the_plivo_token_is_minted_by_plivo_for_the_endpoint_app_and_session(world, tenant):
    agent = tenant.agent()
    s = session(tenant, agent)
    claims = world.plivo.tokens[s["plivo"]["access_token"]]
    app = ok(world.operator.get(f"/api/v1/tenants/{tenant.id}"))["plivo_app_id"]
    assert (claims["iss"], claims["sub"], claims["app"]) == (tenant.auth_id, agent.endpoint_username, app)
    assert claims["per"]["voice"] == {"incoming_allow": True, "outgoing_allow": False}
    assert claims["nbf"] <= time.time() < claims["exp"] == datetime.fromisoformat(s["plivo"]["expires_at"]).timestamp()
    assert claims["exp"] <= datetime.fromisoformat(s["expires_at"]).timestamp()
    settings(tenant, agent_direct_dial=True)
    later = world.plivo.tokens[session(tenant, agent)["plivo"]["access_token"]]
    assert later["per"]["voice"]["outgoing_allow"] is True


def test_the_second_heartbeat_makes_the_agent_available(tenant):
    agent = tenant.agent().login(available=False)
    agent.heartbeat()
    assert status_of(tenant, agent) == tenant.status_id("Offline")
    agent.heartbeat()
    tenant.world.wait(lambda: status_of(tenant, agent) == tenant.status_id("Available"), what="available")
    changed = tenant.stream(after=0).wait_for("agent.status_changed", agent_id=agent.id, reason="heartbeat")
    assert changed["data"]["to_status"]["name"] == "Available"


def test_sip_credentials_rotate_the_password_at_plivo(world, tenant):
    agent = tenant.agent()
    endpoint_id = ok(tenant.api.get(f"{A}/{agent.id}"))["endpoint_id"]
    first = ok(tenant.api.post(f"{A}/{agent.id}/sip-credentials"))
    second = ok(tenant.api.post(f"{A}/{agent.id}/sip-credentials"))
    assert first["username"] == agent.endpoint_username and first["password"] != second["password"]
    sent = [r.params.get("password") for r in world.plivo.requests_to(f"/Endpoint/{endpoint_id}") if r.method == "POST"]
    assert sent[-2:] == [first["password"], second["password"]]
    assert first["password"] not in tenant.api.get(f"{A}/{agent.id}").text


def test_a_sip_device_agent_set_available_by_api_takes_calls(tenant):
    agent = tenant.agent()
    ok(tenant.api.post(f"{A}/{agent.id}/sip-credentials"))
    ok(set_status(tenant, agent, "Available"))
    number, _ = tenant.inbound_line(agent)
    tenant.world.plivo.call(number)
    assert hs_header(agent.phone.wait_ring(), "HsKind") == "offer"


def test_a_browser_agent_rings_at_its_sdk_login_and_a_sip_device_at_its_endpoint(tenant):
    browser, desk = tenant.agent("Browser"), tenant.agent("Desk phone")
    browser.phone.unregister()
    browser.login()
    ok(set_status(tenant, desk, "Available"))
    desk.phone.on_ring("ignore")
    number, _ = tenant.inbound_line(browser, desk, strategy="Simultaneous")
    stream = tenant.stream(after=0)
    tenant.world.plivo.call(number)
    assert browser.phone.wait_ring().to == f"sip:{browser.endpoint_username}_{tenant.auth_id}@phone.plivo.com"
    assert desk.phone.wait_ring().to == f"sip:{desk.endpoint_username}@phone.plivo.com"
    stream.wait_for("offer.accepted", agent_id=browser.id)


def test_deleting_an_agent_removes_its_endpoint_but_not_while_on_a_call(world, tenant):
    live = connect(tenant)
    error(tenant.api.delete(f"{A}/{live.agent.id}"), 409)
    assert live.caller.state == "in_progress"
    idle = tenant.agent("Idle")
    endpoint_id = ok(tenant.api.get(f"{A}/{idle.id}"))["endpoint_id"]
    ok(tenant.api.delete(f"{A}/{idle.id}"))
    assert any(r.method == "DELETE" and f"/Endpoint/{endpoint_id}" in r.path for r in world.plivo.requests)
    error(tenant.api.get(f"{A}/{idle.id}"), 404)


def test_revoking_sessions_or_changing_the_role_kills_tokens_and_streams(tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(token=agent.stream_token)
    ok(tenant.api.delete(f"{A}/{agent.id}/sessions"))
    stream.wait_closed()
    error(agent.api().get("/api/v1/me"), 401)
    error(tenant.world.anon().get("/api/v1/events/stream", params={"access_token": agent.stream_token}), 401)
    agent.login(available=False)
    ok(tenant.api.patch(f"{A}/{agent.id}", json={"role": "supervisor"}))
    error(agent.api().get("/api/v1/me"), 401)


def test_a_reminted_session_keeps_the_call(tenant):
    live = connect(tenant)
    fresh = session(tenant, live.agent)
    assert live.caller.state == "in_progress" and live.leg.state == "in_progress"
    ok(tenant.world.client(fresh["agent_token"]).post(f"/api/v1/calls/{live.id}/hold", json={"on": True}))
    live.stream.wait_for("call.held", conversation_id=live.id)


def test_teams_and_skill_levels_are_set_on_the_agent(tenant):
    sales, support, spanish = team(tenant, "Sales"), team(tenant, "Support"), skill(tenant, "Spanish")
    agent = tenant.agent(teams=[sales])
    assert ok(tenant.api.get(f"{A}/{agent.id}"))["teams"] == [sales]
    ok(tenant.api.patch(f"{A}/{agent.id}", json={"teams": [support], "skills": [{"skill_id": spanish, "level": 7}]}))
    got = ok(tenant.api.get(f"{A}/{agent.id}"))
    assert got["teams"] == [support] and [(k["skill_id"], k["level"]) for k in got["skills"]] == [(spanish, 7)]
    ok(tenant.api.delete(f"/api/v1/teams/{sales}"))
    assert [t["id"] for t in ok(tenant.api.get("/api/v1/teams"))["data"]] == [support]


def test_agents_read_shared_lists_but_cannot_write_them(tenant):
    desk = tenant.agent().login(available=False).api()
    families = ("teams", "skills", "agent-statuses", "dispositions")
    assert [f for f in families if desk.get(f"/api/v1/{f}").status_code != 200] == []
    assert [f for f in families if desk.post(f"/api/v1/{f}", json={"name": "Mine"}).status_code not in (401, 403)] == []


def test_registration_seeds_the_roles_and_statuses(tenant):
    assert {r["name"] for r in ok(tenant.api.get("/api/v1/roles"))["data"]} == {"admin", "supervisor", "agent"}
    rows = {s["name"]: s for s in ok(tenant.api.get("/api/v1/agent-statuses", params={"limit": 200}))["data"]}
    assert {"Available", "Idle", "Busy", "After Contact Work", "Away", "Offline", "Break"} <= rows.keys()
    assert rows["Available"]["availability"] is True and rows["Break"]["availability"] is False


def test_status_changes_by_admin_and_by_the_agent_are_announced(tenant):
    lunch = ok(tenant.api.post("/api/v1/agent-statuses", json={"name": "Lunch", "availability": False}))
    agent = tenant.agent()
    [by_admin] = status_changes(tenant, agent, "Lunch")
    to = by_admin["data"]["to_status"]
    assert (by_admin["data"]["reason"], to["id"], to["name"]) == ("admin", lunch["id"], "Lunch")
    assert by_admin["data"]["from_status"]["name"] == "Offline" and status_of(tenant, agent) == lunch["id"]
    agent.login()
    stream = tenant.stream(after=0)
    agent.set_status("Away")
    assert (
        stream.wait_for("agent.status_changed", agent_id=agent.id, reason="manual")["data"]["to_status"]["name"]
        == "Away"
    )


def test_me_returns_profile_role_status_and_desk_settings(tenant):
    agent = tenant.agent(role="agent").login()
    me = ok(agent.api().get("/api/v1/me"))
    assert (me["id"], me["role"], me["endpoint_username"]) == (agent.id, "agent", agent.endpoint_username)
    assert me["status"]["name"] == "Available"
    assert me["settings"]["agent_direct_dial"] is False and me["settings"]["wrap_up"]["mode"] in ("manual", "auto")


def test_caller_id_must_be_a_registered_number(tenant):
    agent = tenant.agent()
    error(tenant.api.patch(f"{A}/{agent.id}", json={"caller_id": "+14155550123"}), 422, "from_not_allowed")
    number = registered_number(tenant)
    ok(tenant.api.patch(f"{A}/{agent.id}", json={"caller_id": number}))
    assert ok(tenant.api.get(f"{A}/{agent.id}"))["caller_id"] == number
