"""In-call controls in both directions: hold, transfers, conference, recording, hangup."""

import logging
import xml.etree.ElementTree as ET

import pytest

from tests.acceptance.conftest import (
    MUSIC,
    activity,
    connect,
    dial,
    disposition,
    error,
    fence,
    fresh_second,
    hang_up_with_the_room_alive,
    hs_header,
    lead,
    ok,
    refused,
    seen,
    settings,
    team,
    voicemail_action,
    wrap_up,
)


def act(client, cid: str, action: str, body=None):
    return client.post(f"/api/v1/calls/{cid}/{action}", json=body)


def to_agent(agent) -> dict:
    return {"type": "agent", "agent_id": agent.id}


def answered_outbound(tenant):
    """An outbound call the lead answered. Returns (agent, conversation_id, stream, lead leg)."""
    number, phone = lead(tenant.world)
    stream = tenant.stream(after=0)
    agent, cid = dial(tenant, to=number)
    stream.wait_for("call.answered", conversation_id=cid)
    return agent, cid, stream, phone.legs[0]


def test_hold_and_resume_emit_events(tenant):
    live = connect(tenant)
    ok(act(live.agent.api(), live.id, "hold", {"on": True}))
    assert live.stream.wait_for("call.held", conversation_id=live.id)["data"]["by_agent_id"] == live.agent.id
    ok(act(live.agent.api(), live.id, "hold", {"on": False}))
    assert live.stream.wait_for("call.resumed", conversation_id=live.id)["data"]["by_agent_id"] == live.agent.id
    assert live.caller.state == "in_progress"


def test_a_hold_on_a_held_call_emits_no_second_event(world, tenant):
    live = connect(tenant)
    for client, on in ((live.agent.api(), True), (live.agent.api(), True), (tenant.api, True), (tenant.api, False)):
        ok(act(client, live.id, "hold", {"on": on}))
        world.wait(world.plivo.idle, what="the hold to reach the room")
    fence(tenant)
    holds = [e["type"] for e in tenant.events() if e.get("conversation_id") == live.id]
    assert [t for t in holds if t in ("call.held", "call.resumed")] == ["call.held", "call.resumed"]


def test_a_control_sent_after_the_hangup_is_refused(world, tenant):
    settings(tenant, recording={"inbound": False, "allow_pause": True})
    live = connect(tenant)
    hang_up_with_the_room_alive(world, tenant, live)
    error(act(live.agent.api(), live.id, "recording", {"action": "resume"}), 409, "call_not_connected")
    fence(tenant)
    assert not [e for e in tenant.events(type="call.recording_resumed") if e["conversation_id"] == live.id]


def test_an_outbound_call_plays_the_default_hold_music(world, tenant):
    agent, _cid, _stream, _lead = answered_outbound(tenant)
    leg = agent.phone.legs[0]
    body = next(
        c.body for c in world.plivo.callbacks if c.params.get("CallUUID") == leg.call_uuid and "<Multi" in c.body
    )
    room = ET.fromstring(body).find(".//MultiPartyCall")  # noqa: S314 (HS's own reply)
    assert room.findtext("CustomerHoldMusic/Play") == room.findtext("AgentHoldMusic/Play") == MUSIC


def test_a_cold_transfer_hands_the_caller_to_another_agent(tenant):
    target = tenant.agent("Target").login()
    live = connect(tenant)
    tid = ok(act(live.agent.api(), live.id, "transfers", {"mode": "cold", "target": to_agent(target)}))["transfer_id"]
    leg = target.phone.wait_ring()
    assert (hs_header(leg, "HsKind"), hs_header(leg, "HsConversation")) == ("transfer", live.id)
    assert hs_header(leg, "HsOffer")
    leg.wait_state("in_progress")
    live.leg.wait_state("completed")
    assert leg.mpc == live.caller.mpc and live.caller.state == "in_progress"
    done = live.stream.wait_for("call.transferred", conversation_id=live.id)
    assert (done["data"]["transfer_id"], done["data"]["mode"], done["agent_id"]) == (tid, "cold", live.agent.id)
    assert done["data"]["from_agent_id"] == live.agent.id


def test_a_warm_transfer_completes_on_request(tenant):
    target = tenant.agent("Target").login()
    live = connect(tenant)
    tid = ok(act(live.agent.api(), live.id, "transfers", {"mode": "warm", "target": to_agent(target)}))["transfer_id"]
    leg = target.phone.wait_ring()
    leg.wait_state("in_progress")
    assert live.leg.state == "in_progress"
    ok(act(live.agent.api(), live.id, f"transfers/{tid}/complete"))
    live.leg.wait_state("completed")
    assert leg.mpc == live.caller.mpc and live.caller.state == "in_progress"
    assert live.stream.wait_for("call.transferred", conversation_id=live.id)["data"]["mode"] == "warm"


def test_cancelling_a_warm_transfer_keeps_the_first_agent(tenant):
    target = tenant.agent("Target").login()
    live = connect(tenant)
    tid = ok(act(live.agent.api(), live.id, "transfers", {"mode": "warm", "target": to_agent(target)}))["transfer_id"]
    leg = target.phone.wait_ring()
    leg.wait_state("in_progress")
    ok(act(live.agent.api(), live.id, f"transfers/{tid}/cancel"))
    leg.wait_state("completed")
    assert live.leg.state == live.caller.state == "in_progress"


def test_cancelling_a_transfer_while_its_target_rings_misses_the_offer_with_the_caller_still_on(tenant):
    target = tenant.agent("Target").login()
    target.phone.on_ring("ignore")
    live = connect(tenant)
    tid = ok(act(live.agent.api(), live.id, "transfers", {"mode": "warm", "target": to_agent(target)}))["transfer_id"]
    target.phone.wait_ring()
    offer = live.stream.wait_for("offer.created", agent_id=target.id)["data"]["offer_id"]
    ok(act(live.agent.api(), live.id, f"transfers/{tid}/cancel"))

    def outcome(events):
        mine = [e for e in events if e["type"] != "offer.created" and e["data"].get("offer_id") == offer]
        return [(e["type"], e["data"].get("reason"), e["data"].get("strike")) for e in mine]

    tenant.world.wait(lambda: outcome(live.stream.events), what="the cancelled offer's outcome")
    fence(tenant)
    assert (outcome(tenant.events()), live.caller.state) == ([("offer.missed", None, 0)], "in_progress")


def test_a_plivo_outage_on_cancelling_a_transfer_is_a_503_and_a_retry_cancels(world, tenant):
    target = tenant.agent("Target").login()
    live = connect(tenant)
    tid = ok(act(live.agent.api(), live.id, "transfers", {"mode": "warm", "target": to_agent(target)}))["transfer_id"]
    leg = target.phone.wait_ring().wait_state("in_progress")
    world.plivo.reject_next(f"/Call/{leg.call_uuid}/", 503)
    error(act(live.agent.api(), live.id, f"transfers/{tid}/cancel"), 503, "dependency_unavailable")
    assert leg.state == "in_progress"
    ok(act(live.agent.api(), live.id, f"transfers/{tid}/cancel"))
    leg.wait_state("completed")


def test_a_plivo_outage_on_a_hangup_is_a_503_and_a_retry_ends_the_call(world, tenant):
    live = connect(tenant)
    world.wait(world.plivo.idle, what="the agent's join handled")
    world.plivo.reject_next(f"/Call/{live.leg.call_uuid}/", 503)
    error(act(live.agent.api(), live.id, "hangup"), 503, "dependency_unavailable")
    assert (live.caller.state, live.leg.state) == ("in_progress", "in_progress")
    ok(act(live.agent.api(), live.id, "hangup"))
    live.caller.wait_state("completed")
    live.leg.wait_state("completed")


def test_a_transfer_to_a_queue_offers_its_agents(tenant):
    target = tenant.agent("Target").login()
    other_q = tenant.queue(name="Billing", agents=[target.id])
    live = connect(tenant)
    ok(
        act(
            live.agent.api(),
            live.id,
            "transfers",
            {"mode": "cold", "target": {"type": "queue", "queue_id": other_q["id"]}},
        )
    )
    assert hs_header(target.phone.wait_ring(), "HsConversation") == live.id


def test_a_cold_transfer_back_to_the_calls_own_queue_is_answered_again(world, tenant):
    live = connect(tenant)
    back = {"type": "queue", "queue_id": live.queue["id"]}
    ok(act(live.agent.api(), live.id, "transfers", {"mode": "cold", "target": back}))
    leg = live.agent.phone.wait_ring()
    leg.wait_state("in_progress")
    live.stream.wait_for("offer.accepted", conversation_id=live.id, offer_id=hs_header(leg, "HsOffer"))
    assert live.caller.state == "in_progress"
    assert not [c.status for c in world.plivo.callbacks if live.caller.call_uuid in c.url and c.status >= 500]


def test_a_tenant_key_transfers_to_a_number(world, tenant):
    number, phone = lead(world)
    live = connect(tenant)
    ok(act(tenant.api, live.id, "transfers", {"mode": "cold", "target": {"type": "number", "number": number}}))
    customer_two = phone.wait_ring()
    customer_two.wait_state("in_progress")
    assert customer_two.mpc == live.caller.mpc
    assert live.stream.wait_for("call.transferred", conversation_id=live.id)["data"]["mode"] == "cold"


@pytest.mark.parametrize("wrapped", ["before", "after"])
@pytest.mark.parametrize("first", ["caller", "phone"])
def test_a_call_transferred_to_a_number_closes_once_both_ends_hang_up(world, tenant, first, wrapped):
    number, phone = lead(world)
    live = connect(tenant)
    ok(act(tenant.api, live.id, "transfers", {"mode": "cold", "target": {"type": "number", "number": number}}))
    far = phone.wait_ring().wait_state("in_progress")
    live.leg.wait_state("completed")
    live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id, agent_id=live.agent.id)
    if wrapped == "before":
        ok(wrap_up(live.agent, live.id, disposition(tenant)["id"]))
    for leg in (live.caller, far) if first == "caller" else (far, live.caller):
        leg.hangup()
        leg.wait_state("completed")
    live.stream.wait_for("call.ended", conversation_id=live.id)
    if wrapped == "after":
        ok(wrap_up(live.agent, live.id, disposition(tenant)["id"]))
    closed = live.stream.wait_for("conversation.closed", conversation_id=live.id)
    fence(tenant)
    ended = [e for e in tenant.events(type="call.ended") if e["conversation_id"] == live.id]
    assert [e["data"]["result"] for e in ended] == [closed["data"]["result"]] == ["answered"]


def test_an_agent_cannot_reach_a_number_by_transfer_or_conference_without_direct_dial(world, tenant):
    number, phone = lead(world)
    live = connect(tenant)
    target = {"type": "number", "number": number}
    error(act(live.agent.api(), live.id, "transfers", {"mode": "cold", "target": target}), 403, "dial_requires_server")
    error(act(live.agent.api(), live.id, "participants", {"target": target}), 403, "dial_requires_server")
    assert phone.legs == [] and live.leg.state == "in_progress"


def test_an_outbound_call_takes_hold_recording_pause_and_transfer(tenant):
    settings(tenant, recording={"outbound": True, "allow_pause": True})
    target = tenant.agent("Target").login()
    agent, cid, stream, customer = answered_outbound(tenant)
    ok(act(agent.api(), cid, "hold", {"on": True}))
    stream.wait_for("call.held", conversation_id=cid)
    ok(act(agent.api(), cid, "recording", {"action": "pause"}))
    stream.wait_for("call.recording_paused", conversation_id=cid)
    ok(act(agent.api(), cid, "transfers", {"mode": "cold", "target": to_agent(target)}))
    leg = target.phone.wait_ring()
    leg.wait_state("in_progress")
    assert leg.mpc == customer.mpc
    stream.wait_for("call.transferred", conversation_id=cid)


def test_an_outbound_call_refuses_a_move_to_a_queue_or_voicemail_and_keeps_the_lead(tenant):
    q = tenant.queue(name="Billing")
    agent, cid, _stream, customer = answered_outbound(tenant)
    for target in ({"type": "queue", "queue_id": q["id"]}, voicemail_action(q)):
        error(act(agent.api(), cid, "transfers", {"mode": "cold", "target": target}), 409, "transfer_failed")
    tenant.world.wait(tenant.world.plivo.idle, what="the Plivo simulator to go idle")
    assert customer.state == "in_progress" and not customer.held


def test_the_close_names_the_agent_who_wrapped_up_last(tenant):
    settings(tenant, wrap_up={"mode": "manual", "seconds": 60})
    target = tenant.agent("Target").login()
    live = connect(tenant)
    ok(act(live.agent.api(), live.id, "transfers", {"mode": "cold", "target": to_agent(target)}))
    target.phone.wait_ring().wait_state("in_progress")
    live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id, agent_id=live.agent.id)
    ok(wrap_up(live.agent, live.id, disposition(tenant, "First")["id"]))
    live.caller.hangup()
    live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id, agent_id=target.id)
    assert not seen(live.stream, "conversation.closed", conversation_id=live.id)
    ok(wrap_up(target, live.id, disposition(tenant, "Second")["id"]))
    closed = live.stream.wait_for("conversation.closed", conversation_id=live.id)
    assert closed["agent_id"] == target.id
    assert {d["agent_id"] for d in closed["data"]["dispositions"]} == {live.agent.id, target.id}


def test_a_transfer_to_another_tenants_agent_is_422(world, tenant):
    live = connect(tenant)
    foreign = world.new_tenant("Other").agent().login()
    error(act(live.agent.api(), live.id, "transfers", {"mode": "cold", "target": to_agent(foreign)}), 422)
    assert foreign.phone.legs == []


def test_conference_add_and_remove_an_agent(tenant):
    third = tenant.agent("Third").login()
    live = connect(tenant)
    pid = ok(act(live.agent.api(), live.id, "participants", {"target": to_agent(third)}))["participant_id"]
    leg = third.phone.wait_ring()
    leg.wait_state("in_progress")
    assert leg.mpc == live.caller.mpc
    assert live.stream.wait_for("call.participant_added", conversation_id=live.id)["data"]["participant_id"] == pid
    ok(live.agent.api().delete(f"/api/v1/calls/{live.id}/participants/{pid}"))
    leg.wait_state("completed")
    assert live.stream.wait_for("call.participant_removed", conversation_id=live.id)["data"]["participant_id"] == pid


def test_a_tenant_key_adds_a_phone_participant(world, tenant):
    number, phone = lead(world)
    live = connect(tenant)
    ok(act(tenant.api, live.id, "participants", {"target": {"type": "number", "number": number}}))
    extra = phone.wait_ring()
    extra.wait_state("in_progress")
    assert extra.mpc == live.caller.mpc and live.leg.state == "in_progress"


def test_a_conference_takes_at_most_two_extra_participants(world, tenant):
    live = connect(tenant)
    for _ in range(2):
        number, phone = lead(world)
        ok(act(tenant.api, live.id, "participants", {"target": {"type": "number", "number": number}}))
        phone.wait_ring().wait_state("in_progress")
    refused(act(tenant.api, live.id, "participants", {"target": {"type": "number", "number": lead(world)[0]}}))


def test_recording_pause_and_resume(tenant):
    settings(tenant, recording={"inbound": True, "allow_pause": True})
    live = connect(tenant)
    ok(act(live.agent.api(), live.id, "recording", {"action": "pause"}))
    assert (
        live.stream.wait_for("call.recording_paused", conversation_id=live.id)["data"]["by_agent_id"] == live.agent.id
    )
    ok(act(live.agent.api(), live.id, "recording", {"action": "resume"}))
    live.stream.wait_for("call.recording_resumed", conversation_id=live.id)


def test_a_plivo_outage_on_a_control_is_a_503_and_a_retry_succeeds(world, tenant):
    settings(tenant, recording={"inbound": True, "allow_pause": True})
    live = connect(tenant)
    world.plivo.reject_next("/Record/Pause/", 503)
    err = error(act(live.agent.api(), live.id, "recording", {"action": "pause"}), 503, "dependency_unavailable")
    assert "rejected" not in err["message"]
    ok(act(live.agent.api(), live.id, "recording", {"action": "pause"}))
    live.stream.wait_for("call.recording_paused", conversation_id=live.id)


def test_resume_starts_a_recording_when_none_runs(tenant):
    settings(tenant, recording={"inbound": False, "allow_pause": True})
    live = connect(tenant)
    ok(act(live.agent.api(), live.id, "recording", {"action": "resume"}))
    live.stream.wait_for("call.recording_resumed", conversation_id=live.id)
    live.caller.hangup()
    assert live.stream.wait_for("recording.ready", conversation_id=live.id)["data"]["recording_id"]


def test_a_pause_refused_by_the_calls_state_logs_no_error(tenant, caplog):
    settings(tenant, recording={"inbound": False, "allow_pause": True})
    live = connect(tenant)
    caplog.clear()
    error(act(live.agent.api(), live.id, "recording", {"action": "pause"}), 409, "recording_control_failed")
    logged = [(r.levelno, r.getMessage()) for r in caplog.records]
    assert any("recording_control_failed" in m for _, m in logged), "HS's log was not captured"
    assert not [m for level, m in logged if level >= logging.ERROR]


def test_hangup_ends_the_call(tenant):
    live = connect(tenant)
    ok(act(tenant.api, live.id, "hangup"))
    live.caller.wait_state("completed")
    live.leg.wait_state("completed")
    live.stream.wait_for("call.ended", conversation_id=live.id)


def emitted(tenant, type_: str, cid: str) -> list[dict]:
    """Every `type_` event of the conversation so far."""
    fence(tenant)
    return [e for e in tenant.events(type=type_) if e["conversation_id"] == cid]


def minutes_later(world, *agents) -> None:
    """Every Redis key nothing refreshes has expired, as on a call minutes old; the agents' desks kept pinging."""
    world.wait(world.plivo.idle, what="the call settled")
    world.expire_cache(300)
    for agent in agents:
        agent.heartbeat()


def hours_later(world, *agents) -> None:
    """Two hours pass: every Redis key whose TTL runs out by then has expired, and each agent's desk signed in again
    and kept pinging."""
    world.wait(world.plivo.idle, what="the call settled")
    world.expire_cache(7200)
    for agent in agents:
        agent.login(available=False).heartbeat()


def test_the_agents_hangup_ends_a_call_minutes_old(world, tenant):
    live = connect(tenant)
    minutes_later(world, live.agent)
    ok(act(live.agent.api(), live.id, "hangup"))
    live.caller.wait_state("completed")
    live.leg.wait_state("completed")
    assert live.stream.wait_for("call.ended", conversation_id=live.id)["data"]["ended_by"] == "agent"


@pytest.mark.parametrize("third", ["agent", "number", "supervisor"])
def test_the_agents_hangup_on_a_call_minutes_old_ends_it_for_everyone_on_it(world, tenant, third):
    floor = team(tenant, "Floor")
    live = connect(tenant, tenant.agent("Agent", teams=[floor]).login())
    agents = [live.agent]
    if third == "number":
        number, phone = lead(world)
        ok(act(tenant.api, live.id, "participants", {"target": {"type": "number", "number": number}}))
    elif third == "agent":
        agents.append(tenant.agent("Third").login())
        ok(act(live.agent.api(), live.id, "participants", {"target": to_agent(agents[-1])}))
    else:
        agents.append(tenant.agent("Sup", role="supervisor", teams=[floor]).login())
        ok(act(agents[-1].api(), live.id, "monitor", {"mode": "listen"}))
    extra = (phone if third == "number" else agents[-1].phone).wait_ring().wait_state("in_progress")
    minutes_later(world, *agents)
    ok(act(live.agent.api(), live.id, "hangup"))
    for leg in (live.caller, live.leg, extra):
        leg.wait_state("completed")


def test_the_agents_hangup_ends_a_call_hours_old(world, tenant):
    live = connect(tenant)
    hours_later(world, live.agent)
    ok(act(live.agent.api(), live.id, "hangup"))
    live.caller.wait_state("completed")
    live.leg.wait_state("completed")
    world.wait(lambda: emitted(tenant, "call.ended", live.id), what="call.ended")
    assert [e["data"]["ended_by"] for e in emitted(tenant, "call.ended", live.id)] == ["agent"]


def test_the_agents_hangup_on_a_call_hours_old_ends_their_leg_and_leaves_the_others_talking(world, tenant):
    """Past two hours the call's cache is gone, so the agent's leave can no longer end the room for the rest."""
    floor = team(tenant, "Floor")
    live = connect(tenant, tenant.agent("Agent", teams=[floor]).login())
    number, phone = lead(world)
    ok(act(tenant.api, live.id, "participants", {"target": {"type": "number", "number": number}}))
    extra = phone.wait_ring().wait_state("in_progress")
    sup = tenant.agent("Sup", role="supervisor", teams=[floor]).login()
    ok(act(sup.api(), live.id, "monitor", {"mode": "listen"}))
    watching = sup.phone.wait_ring().wait_state("in_progress")
    hours_later(world, live.agent, sup)
    ok(act(live.agent.api(), live.id, "hangup"))
    live.leg.wait_state("completed")
    world.wait(world.plivo.idle, what="the agent's leave handled")
    assert (live.caller.state, extra.state, watching.state) == ("in_progress",) * 3


def test_a_hangup_on_a_call_that_already_ended_is_refused(tenant):
    live = connect(tenant)
    live.caller.hangup()
    live.stream.wait_for("call.ended", conversation_id=live.id)
    error(act(tenant.api, live.id, "hangup"), 409, "call_not_connected")


@pytest.mark.parametrize(
    "mode,outcome",
    [
        ("warm", "consulting"),
        ("warm", "cancelled"),
        ("warm", "completed"),
        ("warm", "failed"),
        ("cold", "completed"),
        ("cold", "failed"),
    ],
)
def test_a_hangup_minutes_after_a_transfer_does_what_the_agent_leaving_does(world, tenant, mode, outcome):
    target = tenant.agent("Target").login()
    target.phone.on_ring("decline" if outcome == "failed" else "answer")
    live = connect(tenant)
    tid = ok(act(live.agent.api(), live.id, "transfers", {"mode": mode, "target": to_agent(target)}))["transfer_id"]
    leg = target.phone.wait_ring().wait_state("completed" if outcome == "failed" else "in_progress")
    if outcome == "cancelled":
        ok(act(live.agent.api(), live.id, f"transfers/{tid}/cancel"))
    elif outcome == "completed" and mode == "warm":
        ok(act(live.agent.api(), live.id, f"transfers/{tid}/complete"))
    if outcome in ("completed", "failed"):
        live.stream.wait_for("call.transferred", conversation_id=live.id)
    minutes_later(world, live.agent, target)
    on_call = target if outcome == "completed" else live.agent
    ok(act(on_call.api(), live.id, "hangup"))
    live.leg.wait_state("completed")
    if outcome == "consulting":
        live.stream.wait_for("call.transferred", conversation_id=live.id, agent_id=live.agent.id)
        assert (live.caller.state, leg.state) == ("in_progress", "in_progress")
        ok(act(target.api(), live.id, "hangup"))
    for x in (live.caller, leg):
        x.wait_state("completed")


def test_the_agents_hangup_during_a_warm_consult_hands_the_caller_to_the_target(tenant):
    target = tenant.agent("Target").login()
    live = connect(tenant)
    ok(act(live.agent.api(), live.id, "transfers", {"mode": "warm", "target": to_agent(target)}))
    leg = target.phone.wait_ring().wait_state("in_progress")
    tenant.world.wait(tenant.world.plivo.idle, what="the consult's join handled")
    ok(act(live.agent.api(), live.id, "hangup"))
    live.leg.wait_state("completed")
    done = live.stream.wait_for("call.transferred", conversation_id=live.id, agent_id=live.agent.id)
    assert (done["data"]["mode"], done["data"]["status"]) == ("warm", "completed")
    assert (live.caller.state, leg.state) == ("in_progress", "in_progress")


def test_a_cold_transfer_target_who_answered_keeps_the_caller_when_the_first_agent_hangs_up(world, tenant):
    target = tenant.agent("Target").login()
    live = connect(tenant)
    world.wait(world.plivo.idle, what="the agent's join handled")
    world.plivo.reject_next(f"/Call/{live.leg.call_uuid}/", 503)  # the handover's own hangup of the first agent
    ok(act(live.agent.api(), live.id, "transfers", {"mode": "cold", "target": to_agent(target)}))
    leg = target.phone.wait_ring().wait_state("in_progress")
    world.wait(world.plivo.idle, what="the target's join handled")
    assert live.leg.state == "in_progress"
    ok(act(live.agent.api(), live.id, "hangup"))
    live.leg.wait_state("completed")
    done = live.stream.wait_for("call.transferred", conversation_id=live.id, agent_id=live.agent.id)
    assert (done["data"]["mode"], done["data"]["status"]) == ("cold", "completed")
    assert (live.caller.state, leg.state) == ("in_progress", "in_progress")


def test_ending_the_call_while_a_cold_transfer_rings_withdraws_that_offer(world, tenant):
    agent, target = tenant.agent().login(), tenant.agent("Target").login()
    target.phone.on_ring("ignore")
    number, _ = tenant.inbound_line(agent, target, strategy="Simultaneous")
    stream = tenant.stream(after=0)
    fresh_second(world)
    world.plivo.call(number)  # the target's first leg loses the race and ends as the agent answers
    cid = hs_header(agent.phone.wait_ring(), "HsConversation")
    stream.wait_for("offer.accepted", conversation_id=cid)
    ok(act(agent.api(), cid, "transfers", {"mode": "cold", "target": to_agent(target)}))
    target.phone.wait_ring()
    leg = target.phone.wait_ring()
    ok(act(agent.api(), cid, "hangup"))
    leg.wait_state("completed")
    stream.wait_for("offer.withdrawn", offer_id=hs_header(leg, "HsOffer"))
    world.wait(lambda: activity(tenant, target) == "idle", what="the transfer target to read idle")


def test_a_transferred_call_gives_two_wrap_ups_and_one_close(tenant):
    settings(tenant, wrap_up={"mode": "manual", "seconds": 60})
    target = tenant.agent("Target").login()
    live = connect(tenant)
    ok(act(live.agent.api(), live.id, "transfers", {"mode": "cold", "target": to_agent(target)}))
    target.phone.wait_ring().wait_state("in_progress")
    live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id, agent_id=live.agent.id)
    done = disposition(tenant, "Done")["id"]
    ok(wrap_up(live.agent, live.id, done))
    live.caller.hangup()
    live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id, agent_id=target.id)
    assert not seen(live.stream, "conversation.closed", conversation_id=live.id)
    ok(wrap_up(target, live.id, done))
    closed = live.stream.wait_for("conversation.closed", conversation_id=live.id)
    wrapped = seen(live.stream, "conversation.wrapped_up", conversation_id=live.id)
    assert {e["agent_id"] for e in wrapped} == {live.agent.id, target.id}
    assert int(closed["id"]) > max(int(e["id"]) for e in wrapped) and {
        d["agent_id"] for d in closed["data"]["dispositions"]
    } == {live.agent.id, target.id}
    fence(tenant)
    assert [e["conversation_id"] for e in tenant.events(type="conversation.closed")].count(live.id) == 1


def test_transfer_targets_list_free_agents_and_their_teams(tenant):
    billing = team(tenant, "Billing")
    free = tenant.agent("Free", teams=[billing]).login()
    live = connect(tenant)
    targets = ok(live.agent.api().get("/api/v1/transfer-targets"))
    agents = [a["id"] for a in targets["agents"]]
    assert free.id in agents and live.agent.id not in agents
    assert billing in [t["id"] for t in targets["teams"]]


def test_a_transfer_to_a_team_offers_its_members(tenant):
    billing = team(tenant, "Billing")
    member = tenant.agent("Member", teams=[billing]).login()
    live = connect(tenant)
    ok(act(live.agent.api(), live.id, "transfers", {"mode": "cold", "target": {"type": "team", "team_id": billing}}))
    leg = member.phone.wait_ring()
    assert (hs_header(leg, "HsKind"), hs_header(leg, "HsConversation")) == ("transfer", live.id)


def test_a_transfer_to_voicemail_takes_a_message(tenant):
    mailbox = tenant.queue(name="Billing mailbox")
    live = connect(tenant)
    target = voicemail_action(mailbox, "Leave a message for billing")
    ok(act(live.agent.api(), live.id, "transfers", {"mode": "cold", "target": target}))
    vm = live.stream.wait_for("voicemail.received", conversation_id=live.id, timeout=10)
    assert "Leave a message for billing" in live.caller.said and vm["data"]["mailbox"]["queue_id"] == mailbox["id"]
    live.leg.wait_state("completed")


def test_a_warm_transfer_completes_when_the_first_agent_hangs_up(world, tenant):
    target = tenant.agent("Target").login()
    live = connect(tenant)
    tid = ok(act(live.agent.api(), live.id, "transfers", {"mode": "warm", "target": to_agent(target)}))["transfer_id"]
    leg = target.phone.wait_ring().wait_state("in_progress")
    world.wait(lambda: leg.mpc == live.caller.mpc, what="the consult in the caller's room")
    world.wait(world.plivo.idle, what="the consult's join answered")
    live.leg.hangup()
    done = live.stream.wait_for("call.transferred", conversation_id=live.id)
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    fence(tenant)
    assert (done["data"]["transfer_id"], done["data"]["mode"]) == (tid, "warm")
    assert live.caller.state == leg.state == "in_progress" and leg.mpc == live.caller.mpc
    assert not [e for e in tenant.events(type="conversation.closed") if e["conversation_id"] == live.id]


def test_a_warm_transfer_to_a_number_keeps_the_caller_when_the_first_agent_hangs_up(world, tenant):
    number, phone = lead(world)
    live = connect(tenant)
    body = {"mode": "warm", "target": {"type": "number", "number": number}}
    tid = ok(act(tenant.api, live.id, "transfers", body))["transfer_id"]
    leg = phone.wait_ring().wait_state("in_progress")
    world.wait(lambda: leg.mpc == live.caller.mpc, what="the consult in the caller's room")
    world.wait(world.plivo.idle, what="the consult's join answered")
    live.leg.hangup()
    done = live.stream.wait_for("call.transferred", conversation_id=live.id)
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    fence(tenant)
    assert (done["data"]["transfer_id"], done["data"]["target"]) == (tid, {"type": "external", "id": number})
    assert live.caller.state == leg.state == "in_progress" and leg.mpc == live.caller.mpc
    assert not [e for e in tenant.events(type="conversation.closed") if e["conversation_id"] == live.id]


def test_a_warm_transfer_to_a_number_completes_on_request(world, tenant):
    number, phone = lead(world)
    live = connect(tenant)
    body = {"mode": "warm", "target": {"type": "number", "number": number}}
    tid = ok(act(tenant.api, live.id, "transfers", body))["transfer_id"]
    leg = phone.wait_ring().wait_state("in_progress")
    world.wait(lambda: leg.mpc == live.caller.mpc, what="the consult in the caller's room")
    world.wait(world.plivo.idle, what="the consult's join answered")
    ok(act(live.agent.api(), live.id, f"transfers/{tid}/complete"))
    live.leg.wait_state("completed")
    done = live.stream.wait_for("call.transferred", conversation_id=live.id)
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    fence(tenant)
    assert (done["data"]["transfer_id"], done["data"]["status"], done["data"]["mode"]) == (tid, "completed", "warm")
    assert done["data"]["target"] == {"type": "external", "id": number}
    assert live.caller.state == leg.state == "in_progress" and leg.mpc == live.caller.mpc
    assert len([e for e in tenant.events(type="call.transferred") if e["conversation_id"] == live.id]) == 1


def test_a_warm_transfer_to_a_number_still_ringing_is_not_completed_and_keeps_the_call(world, tenant):
    settings(tenant, ring_timeout_sec=120)
    number, phone = lead(world)
    phone.on_ring("ignore")
    live = connect(tenant)
    body = {"mode": "warm", "target": {"type": "number", "number": number}}
    tid = ok(act(tenant.api, live.id, "transfers", body))["transfer_id"]
    phone.wait_ring()
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    error(act(live.agent.api(), live.id, f"transfers/{tid}/complete"), 409, "transfer_failed")
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    assert live.leg.state == live.caller.state == "in_progress"


def test_a_second_transfer_while_one_rings_is_refused_and_the_target_rings_once(world, tenant):
    settings(tenant, ring_timeout_sec=120)
    target = tenant.agent("Target").login()
    target.phone.on_ring("ignore")
    live = connect(tenant)
    body = {"mode": "warm", "target": to_agent(target)}
    ok(act(live.agent.api(), live.id, "transfers", body))
    target.phone.wait_ring()
    error(act(live.agent.api(), live.id, "transfers", body), 409, "transfer_failed")
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    assert [leg.state for leg in target.phone.legs] == ["ringing"]


def test_a_declined_transfer_names_its_target_and_a_new_one_can_follow(tenant):
    target = tenant.agent("Target").login()
    target.phone.on_ring("decline")
    live = connect(tenant)
    body = {"mode": "warm", "target": to_agent(target)}
    tid = ok(act(live.agent.api(), live.id, "transfers", body))["transfer_id"]
    failed = live.stream.wait_for("call.transferred", conversation_id=live.id)
    assert (failed["data"]["transfer_id"], failed["data"]["status"]) == (tid, "failed")
    assert failed["data"]["target"] == {"type": "agent", "id": target.id}
    target.phone.on_ring("answer")
    ok(act(live.agent.api(), live.id, "transfers", body))
    target.phone.wait_ring()
    target.phone.wait_ring().wait_state("in_progress")


def test_a_team_transfer_never_rings_the_transferring_agent_and_a_miss_keeps_the_caller(tenant):
    billing = team(tenant, "Billing")
    member = tenant.agent("Member", teams=[billing]).login()
    member.phone.on_ring("decline")
    agent = tenant.agent("Agent", teams=[billing]).login()
    live = connect(tenant, agent)
    ok(act(agent.api(), live.id, "transfers", {"mode": "warm", "target": {"type": "team", "team_id": billing}}))
    member.phone.wait_ring()
    failed = live.stream.wait_for("call.transferred", conversation_id=live.id)
    assert failed["data"]["status"] == "failed" and failed["data"]["target"] == {"type": "team", "id": billing}
    assert len(agent.phone.legs) == 1 and live.leg.state == live.caller.state == "in_progress"


def test_a_warm_number_transfer_still_ringing_is_not_completed_to_another_agent_on_the_call(world, tenant, monkeypatch):
    settings(tenant, ring_timeout_sec=120)
    third = tenant.agent("Third").login()
    number, phone = lead(world)
    phone.on_ring("ignore")
    live = connect(tenant)
    ok(act(live.agent.api(), live.id, "participants", {"target": to_agent(third)}))
    extra = third.phone.wait_ring().wait_state("in_progress")
    body = {"mode": "warm", "target": {"type": "number", "number": number}}
    with monkeypatch.context() as m:  # the number rings the full 120 s, so a slow run cannot time it out
        m.setattr(world.plivo, "time_factor", 1.0)
        tid = ok(act(tenant.api, live.id, "transfers", body))["transfer_id"]
        target = phone.wait_ring()
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    error(act(live.agent.api(), live.id, f"transfers/{tid}/complete"), 409, "transfer_failed")
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    assert live.leg.state == live.caller.state == extra.state == "in_progress" and target.state == "ringing"
    assert (live.caller.held, live.leg.held, extra.held) == (True, False, False)
    error(act(tenant.api, live.id, "transfers", body), 409, "transfer_failed")


def test_a_transfer_whose_cleanup_was_lost_gives_way_past_the_ring_timeout_and_a_live_consult_does_not(world, tenant):
    settings(tenant, ring_timeout_sec=5)
    consulted, third = tenant.agent("Consulted").login(), tenant.agent("Third").login()
    consult = connect(tenant)
    ok(act(consult.agent.api(), consult.id, "transfers", {"mode": "warm", "target": to_agent(consulted)}))
    consulted.phone.wait_ring().wait_state("in_progress")
    target = tenant.agent("Target").login()
    target.phone.on_ring("decline")
    stuck = connect(tenant)
    lost = world.plivo.fault(
        "delay",
        lambda r: (
            r.params.get("EventName") == "AddParticipantByAPIActionCompleted"
            and r.params.get("AddParticipantAPIRequestOutcome") == "unsuccessful"
        ),
        account=tenant.auth_id,
    )
    body = {"mode": "warm", "target": to_agent(target)}
    ok(act(stuck.agent.api(), stuck.id, "transfers", body))
    world.wait(lambda: lost.hits, what="the failed dial's callback lost")
    error(act(stuck.agent.api(), stuck.id, "transfers", body), 409, "transfer_failed")
    target.phone.on_ring("ignore")
    world.wait(
        lambda: act(stuck.agent.api(), stuck.id, "transfers", body).status_code == 201,
        timeout=20,
        what="a new transfer once past the ring timeout",
    )
    world.wait(lambda: len(target.phone.legs) == 2, what="the target rung again")
    to_third = {"mode": "warm", "target": to_agent(third)}
    error(act(consult.agent.api(), consult.id, "transfers", to_third), 409, "transfer_failed")
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    assert third.phone.legs == [] and consult.caller.state == "in_progress"


def test_a_failed_warm_transfer_to_a_number_names_its_target_external(world, tenant):
    number = lead(world, "busy")[0]
    live = connect(tenant)
    hangup = world.plivo.fault(
        "delay",
        lambda r: (r.params.get("EventName"), r.params.get("To")) == ("Hangup", number[1:]),
        account=tenant.auth_id,
    )
    body = {"mode": "warm", "target": {"type": "number", "number": number}}
    tid = ok(act(tenant.api, live.id, "transfers", body))["transfer_id"]
    failed = live.stream.wait_for("call.transferred", conversation_id=live.id)["data"]
    hangup.release()
    assert (failed["transfer_id"], failed["status"], failed["mode"]) == (tid, "failed", "warm")
    assert failed["target"] == {"type": "external", "id": number}


def test_a_conference_keeps_the_caller_and_earlier_participants_off_hold(world, tenant):
    live = connect(tenant)
    legs = []
    for _ in range(2):
        number, phone = lead(world)
        ok(act(tenant.api, live.id, "participants", {"target": {"type": "number", "number": number}}))
        legs.append(phone.wait_ring().wait_state("in_progress"))
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    assert not live.caller.held and not legs[0].held


@pytest.mark.parametrize("end", ["cancel", "no_answer", "decline"])
def test_a_transfer_nobody_takes_leaves_the_caller_held_until_the_agent_resumes(world, tenant, end):
    target = tenant.agent("Target").login()
    target.phone.on_ring("decline" if end == "decline" else "ignore")
    live = connect(tenant)
    tid = ok(act(live.agent.api(), live.id, "transfers", {"mode": "cold", "target": to_agent(target)}))["transfer_id"]
    leg = target.phone.wait_ring()
    world.wait(lambda: live.caller.held, what="the caller held while the target rings")
    if end == "cancel":
        ok(act(live.agent.api(), live.id, f"transfers/{tid}/cancel"))
    leg.wait_state("completed")
    resumed_by_the_agent(world, live)


def resumed_by_the_agent(world, live):
    """HS keeps the caller on hold after a transfer that ends without a hand-over; the agent resumes."""
    world.wait(world.plivo.idle, what="the transfer's end handled")
    assert live.caller.held and live.leg.state == "in_progress"
    ok(act(live.agent.api(), live.id, "hold", {"on": False}))
    world.wait(lambda: not live.caller.held, what="the agent's resume")


@pytest.mark.parametrize("end", ["cancel", "target_hangs_up"])
def test_a_warm_consult_that_ends_without_a_hand_over_leaves_the_caller_held_until_the_agent_resumes(
    world, tenant, end
):
    target = tenant.agent("Target").login()
    live = connect(tenant)
    tid = ok(act(live.agent.api(), live.id, "transfers", {"mode": "warm", "target": to_agent(target)}))["transfer_id"]
    leg = target.phone.wait_ring().wait_state("in_progress")
    world.wait(lambda: leg.mpc == live.caller.mpc, what="the consult in the caller's room")
    world.wait(world.plivo.idle, what="the consult's join answered")
    assert live.caller.held
    if end == "cancel":
        ok(act(live.agent.api(), live.id, f"transfers/{tid}/cancel"))
    else:
        leg.hangup()
    leg.wait_state("completed")
    resumed_by_the_agent(world, live)


@pytest.mark.parametrize("mode", ["cold", "warm"])
def test_a_completed_transfer_resumes_the_caller(world, tenant, mode):
    target = tenant.agent("Target").login()
    target.phone.on_ring("answer", after=1.0)
    live = connect(tenant)
    tid = ok(act(live.agent.api(), live.id, "transfers", {"mode": mode, "target": to_agent(target)}))["transfer_id"]
    leg = target.phone.wait_ring()
    world.wait(lambda: live.caller.held, what="the caller held while the target rings")
    leg.wait_state("in_progress")
    if mode == "warm":
        world.wait(lambda: leg.mpc == live.caller.mpc, what="the consult in the caller's room")
        world.wait(world.plivo.idle, what="the consult's join answered")
        ok(act(live.agent.api(), live.id, f"transfers/{tid}/complete"))
    live.leg.wait_state("completed")
    world.wait(lambda: not live.caller.held, what="the caller off hold")
    assert leg.mpc == live.caller.mpc


def test_a_recording_pause_that_plivo_answers_409_is_refused_with_no_event(world, tenant):
    settings(tenant, recording={"inbound": True, "allow_pause": True})
    live = connect(tenant)
    world.plivo.reject_next("/Record/Pause/", 409)
    error(act(live.agent.api(), live.id, "recording", {"action": "pause"}), 409, "recording_control_failed")
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    fence(tenant)
    assert not tenant.events(type="call.recording_paused") and not tenant.events(type="call.recording_resumed")


@pytest.mark.parametrize("mode", ["cold", "warm"])
@pytest.mark.parametrize("to", ["agent", "number"])
def test_a_transfer_dials_its_target_once_and_emits_one_completed_event(world, tenant, mode, to):
    number, phone = lead(world)
    target = tenant.agent("Target").login()
    live = connect(tenant)
    tid = ok(
        act(
            tenant.api if to == "number" else live.agent.api(),
            live.id,
            "transfers",
            {"mode": mode, "target": to_agent(target) if to == "agent" else {"type": "number", "number": number}},
        )
    )["transfer_id"]
    legs = (target.phone if to == "agent" else phone).wait_ring().wait_state("in_progress")
    if mode == "warm":
        world.wait(lambda: legs.mpc == live.caller.mpc, what="the consult in the caller's room")
        world.wait(world.plivo.idle, what="the consult's join answered")
        ok(act(live.agent.api(), live.id, f"transfers/{tid}/complete"))
    live.stream.wait_for("call.transferred", conversation_id=live.id)
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    dialled = (target.phone if to == "agent" else phone).legs
    done = emitted(tenant, "call.transferred", live.id)
    assert (len(dialled), [(e["data"]["transfer_id"], e["data"]["status"]) for e in done]) == (1, [(tid, "completed")])


def warm_consult_held(world, live, target):
    """A warm transfer to `target`, requested; returns (transfer id, the target's first leg)."""
    tid = ok(act(live.agent.api(), live.id, "transfers", {"mode": "warm", "target": to_agent(target)}))["transfer_id"]
    return tid, target.phone.wait_ring()


@pytest.mark.parametrize("end", ["cancelled", "rings_out", "declined"])
def test_a_warm_transfer_that_nobody_answers_leaves_the_caller_held_until_the_agent_resumes(world, tenant, end):
    settings(tenant, ring_timeout_sec=5)
    target = tenant.agent("Target").login()
    target.phone.on_ring("decline" if end == "declined" else "ignore")
    live = connect(tenant)
    tid, leg = warm_consult_held(world, live, target)
    world.wait(lambda: live.caller.held, what="the caller held while the target rings")
    if end == "cancelled":
        ok(act(live.agent.api(), live.id, f"transfers/{tid}/cancel"))
    leg.wait_state("completed", timeout=10)
    resumed_by_the_agent(world, live)
    assert live.caller.state == "in_progress"


def test_a_cold_transfer_to_a_queue_queues_the_caller_there_and_routes_on_with_no_transfer_event(world, tenant):
    target = tenant.agent("Target").login()
    other = tenant.queue(name="Billing", agents=[target.id])
    live = connect(tenant)
    ok(
        act(
            live.agent.api(),
            live.id,
            "transfers",
            {"mode": "cold", "target": {"type": "queue", "queue_id": other["id"]}},
        )
    )
    leg = target.phone.wait_ring().wait_state("in_progress")
    live.stream.wait_for("offer.accepted", conversation_id=live.id, agent_id=target.id)
    live.leg.wait_state("completed")
    queued = [e["data"]["queue_id"] for e in emitted(tenant, "conversation.queued", live.id)]
    assert queued == [live.queue["id"], other["id"]]
    assert not emitted(tenant, "call.transferred", live.id)
    assert leg.mpc == live.caller.mpc and live.caller.state == "in_progress"


def test_a_transfer_to_voicemail_leaves_the_message_in_that_mailbox_with_no_transfer_event(tenant):
    mailbox = tenant.queue(name="Billing mailbox")
    live = connect(tenant)
    ok(
        act(
            live.agent.api(),
            live.id,
            "transfers",
            {"mode": "cold", "target": voicemail_action(mailbox, "Leave a message for billing")},
        )
    )
    vm = live.stream.wait_for("voicemail.received", conversation_id=live.id, timeout=10)
    assert vm["data"]["mailbox"]["queue_id"] == mailbox["id"]
    live.leg.wait_state("completed")
    assert not emitted(tenant, "call.transferred", live.id)
    assert len(emitted(tenant, "voicemail.received", live.id)) == 1


@pytest.mark.parametrize(
    "mode,outcome",
    [("warm", "consulting"), ("warm", "cancelled"), ("warm", "completed"), ("cold", "completed"), ("cold", "failed")],
)
def test_a_hangup_from_the_agents_phone_after_a_transfer_ends_the_legs_the_agent_leaving_ends(
    world, tenant, mode, outcome
):
    target = tenant.agent("Target").login()
    target.phone.on_ring("decline" if outcome == "failed" else "answer")
    live = connect(tenant)
    tid = ok(act(live.agent.api(), live.id, "transfers", {"mode": mode, "target": to_agent(target)}))["transfer_id"]
    leg = target.phone.wait_ring().wait_state("completed" if outcome == "failed" else "in_progress")
    if outcome == "cancelled":
        ok(act(live.agent.api(), live.id, f"transfers/{tid}/cancel"))
    elif outcome == "completed" and mode == "warm":
        ok(act(live.agent.api(), live.id, f"transfers/{tid}/complete"))
    if outcome in ("completed", "failed"):
        live.stream.wait_for("call.transferred", conversation_id=live.id)
    world.wait(world.plivo.idle, what="the transfer settled")
    (leg if outcome == "completed" else live.leg).hangup()
    live.leg.wait_state("completed")
    if outcome == "consulting":
        live.stream.wait_for("call.transferred", conversation_id=live.id, agent_id=live.agent.id)
        assert (live.caller.state, leg.state) == ("in_progress", "in_progress")
        leg.hangup()
    for x in (live.caller, leg):
        x.wait_state("completed")
    live.stream.wait_for("call.ended", conversation_id=live.id)
    assert len(emitted(tenant, "call.ended", live.id)) == 1


@pytest.mark.parametrize("who", ["caller", "agent"])
def test_an_answered_call_whose_room_end_arrives_before_the_exit_reads_ended_by_system(world, tenant, who):
    live = connect(tenant)
    world.wait(world.plivo.idle, what="the agent's join handled")
    quitter = live.caller if who == "caller" else live.leg
    exit_ = world.plivo.fault(
        "delay",
        lambda r: (
            (r.params.get("EventName"), r.params.get("ParticipantCallUUID")) == ("ParticipantExit", quitter.call_uuid)
        ),
        account=tenant.auth_id,
    )
    quitter.hangup()
    world.wait(lambda: exit_.hits, what="the quitter's exit held back")
    live.stream.wait_for("call.ended", conversation_id=live.id)
    exit_.release()
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    ended = emitted(tenant, "call.ended", live.id)
    assert [(e["data"]["result"], e["data"]["ended_by"]) for e in ended] == [("answered", "system")]
