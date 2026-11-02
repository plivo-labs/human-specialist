"""Click-to-call: one Make Call path to the agent, the lead added at answer; dialing is a server right."""

import uuid

from tests.acceptance.conftest import (
    connect,
    dial,
    error,
    fence,
    hs_header,
    in_order,
    lead,
    ok,
    registered_number,
    set_status,
    settings,
    types,
)

C = "/api/v1/calls"


def body(tenant, agent, to: str, **extra) -> dict:
    return {"agent_id": agent.id, "to": to, "from": registered_number(tenant), **extra}


def test_click_to_call_rings_the_agent_then_the_lead(world, tenant):
    number, phone = lead(world)
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    r = tenant.api.post(C, json=body(tenant, agent, number))
    assert r.status_code == 201 and r.json()["status"] == "dialing_agent"
    cid = r.json()["conversation_id"]
    leg = agent.phone.wait_ring()
    assert (hs_header(leg, "HsKind"), hs_header(leg, "HsConversation")) == ("outbound", cid)
    customer = phone.wait_ring()
    stream.wait_for("call.answered", conversation_id=cid)
    assert customer.mpc == leg.mpc and customer.to == number
    mine = [e for e in stream.events if e.get("conversation_id") == cid]
    assert in_order(types(mine), "conversation.started", "call.started", "call.answered")


def test_the_lead_hanging_up_ends_the_call_and_starts_wrap_up(world, tenant):
    number, phone = lead(world)
    stream = tenant.stream(after=0)
    agent, cid = dial(tenant, to=number)
    stream.wait_for("call.answered", conversation_id=cid)
    phone.legs[0].hangup()
    ended = stream.wait_for("call.ended", conversation_id=cid)
    assert ended["data"]["ended_by"] == "customer" and ended["direction"] == "outbound"
    assert stream.wait_for("conversation.wrap_up_started", conversation_id=cid)["agent_id"] == agent.id


def test_a_bad_from_or_to_is_422(world, tenant):
    agent = tenant.agent().login()
    r = tenant.api.post(C, json={"agent_id": agent.id, "to": lead(world)[0], "from": "+14155550999"})
    error(r, 422, "from_not_allowed")
    error(tenant.api.post(C, json=body(tenant, agent, "12345")), 422, "invalid_number")
    assert agent.phone.legs == []


def test_the_default_from_is_the_agents_caller_id(world, tenant):
    caller_id = registered_number(tenant)
    agent = tenant.agent(caller_id=caller_id).login()
    number, phone = lead(world)
    assert tenant.api.post(C, json={"agent_id": agent.id, "to": number}).status_code == 201
    assert phone.wait_ring().from_ == caller_id


def test_an_offline_or_busy_agent_is_409(world, tenant):
    error(tenant.api.post(C, json=body(tenant, tenant.agent(), lead(world)[0])), 409, "agent_unavailable")
    live = connect(tenant)
    error(tenant.api.post(C, json=body(tenant, live.agent, lead(world)[0])), 409, "agent_on_call")


def test_an_agent_with_no_registered_device_is_409_agent_not_registered(world, tenant):
    agent = tenant.agent()
    ok(set_status(tenant, agent, "Available"))
    agent.phone.unregister()
    error(tenant.api.post(C, json=body(tenant, agent, lead(world)[0])), 409, "agent_not_registered")


def test_an_agent_token_cannot_dial_while_direct_dial_is_off(world, tenant):
    agent = tenant.agent().login()
    error(agent.api().post(C, json=body(tenant, agent, lead(world)[0])), 403, "dial_requires_server")
    assert agent.phone.legs == []


def test_with_direct_dial_an_agent_token_dials_only_for_itself(world, tenant):
    settings(tenant, agent_direct_dial=True)
    me, other = tenant.agent("Me").login(), tenant.agent("Other").login()
    assert me.api().post(C, json=body(tenant, me, lead(world)[0])).status_code == 201
    error(me.api().post(C, json=body(tenant, other, lead(world)[0])), 403)
    assert other.phone.legs == []


def test_sdk_and_sip_device_dial_outs_are_refused_while_direct_dial_is_off(world, tenant):
    browser, desk_phone = tenant.agent("Browser").login(), tenant.agent("Desk phone")
    ok(tenant.api.post(f"/api/v1/agents/{desk_phone.id}/sip-credentials"))
    ok(set_status(tenant, desk_phone, "Available"))
    number, phone = lead(world)
    for agent in (browser, desk_phone):
        agent.phone.dial(number).wait_state("completed")
    fence(tenant)
    assert phone.legs == [] and not tenant.events(type="conversation.started")


def test_a_token_minted_while_direct_dial_was_on_cannot_dial_once_it_is_off(world, tenant):
    settings(tenant, agent_direct_dial=True)
    agent = tenant.agent().login()
    settings(tenant, agent_direct_dial=False)
    number, phone = lead(world)
    error(agent.api().post(C, json=body(tenant, agent, number)), 403, "dial_requires_server")
    agent.phone.dial(number).wait_state("completed")
    assert phone.legs == []


def test_with_direct_dial_an_sdk_dial_reaches_the_lead_from_the_caller_id(world, tenant):
    settings(tenant, agent_direct_dial=True)
    caller_id = registered_number(tenant)
    agent = tenant.agent(caller_id=caller_id).login()
    number, phone = lead(world)
    stream = tenant.stream(after=0)
    agent.phone.dial(number)
    assert phone.wait_ring().from_ == caller_id
    started = stream.wait_for("conversation.started")
    assert started["direction"] == "outbound"
    stream.wait_for("call.answered", conversation_id=started["conversation_id"])


def test_the_ring_timeout_is_set_per_dial(world, tenant):
    number, phone = lead(world)
    dial(tenant, to=number, ring_timeout_sec=7)
    phone.wait_ring()
    adds = [r for r in world.plivo.requests_to("/MultiPartyCall/") if r.params.get("to") == number]
    assert adds and str(adds[-1].params["ring_timeout"]) == "7"


def test_hanging_up_while_the_agent_rings_cancels_the_dial(world, tenant):
    agent = tenant.agent().login()
    agent.phone.on_ring("ignore")
    stream = tenant.stream(after=0)
    _, cid = dial(tenant, agent)
    leg = agent.phone.wait_ring()
    ok(tenant.api.post(f"{C}/{cid}/hangup"))
    leg.wait_state("completed")
    assert stream.wait_for("call.failed", conversation_id=cid)["data"]["result"] == "cancelled"


def test_a_plivo_outage_on_cancelling_a_ringing_dial_is_a_503_and_a_retry_cancels(world, tenant):
    agent = tenant.agent().login()
    agent.phone.on_ring("ignore")
    stream = tenant.stream(after=0)
    _, cid = dial(tenant, agent)
    leg = agent.phone.wait_ring()
    world.plivo.reject_next("/Request/", 503)
    err = error(tenant.api.post(f"{C}/{cid}/hangup"), 503, "dependency_unavailable")
    assert "503" not in err["message"] and leg.state == "ringing"
    ok(tenant.api.post(f"{C}/{cid}/hangup"))
    leg.wait_state("completed")
    assert stream.wait_for("call.failed", conversation_id=cid)["data"]["result"] == "cancelled"


def test_a_carrier_rate_limit_is_429_with_retry_after(world, tenant):
    agent = tenant.agent().login()
    world.plivo.reject_next("/Call/", 429)
    r = tenant.api.post(C, json=body(tenant, agent, lead(world)[0]))
    error(r, 429, "carrier_rate_limited")
    assert int(r.headers["Retry-After"]) >= 0


def test_post_calls_with_an_idempotency_key_dials_once(world, tenant):
    agent = tenant.agent().login()
    agent.phone.on_ring("ignore")
    payload, key = body(tenant, agent, lead(world)[0]), {"Idempotency-Key": str(uuid.uuid4())}
    first = tenant.api.post(C, json=payload, headers=key)
    again = tenant.api.post(C, json=payload, headers=key)
    assert first.status_code == again.status_code == 201 and first.json() == again.json()
    agent.phone.wait_ring()
    fence(tenant)
    assert len(agent.phone.legs) == 1
