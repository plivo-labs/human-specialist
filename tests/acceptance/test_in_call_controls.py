"""In-call controls in both directions: hold, transfers, conference, recording, hangup."""

import logging
import math
import threading
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from xml.sax.saxutils import escape

import psycopg2
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
    menu,
    ok,
    refused,
    replayed,
    run_jobs_until,
    seen,
    settings,
    team,
    unavailable,
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
    done = live.stream.wait_for("call.transferred", conversation_id=live.id, status="completed")
    assert (done["data"]["transfer_id"], done["data"]["mode"], done["agent_id"]) == (tid, "cold", live.agent.id)
    assert done["data"]["from_agent_id"] == live.agent.id


def test_a_warm_transfer_completes_on_request(tenant):
    target = tenant.agent("Target").login()
    live = connect(tenant)
    tid = ok(act(live.agent.api(), live.id, "transfers", {"mode": "warm", "target": to_agent(target)}))["transfer_id"]
    leg = target.phone.wait_ring()
    leg.wait_state("in_progress")
    assert live.leg.state == "in_progress"
    live.stream.wait_for("offer.accepted", conversation_id=live.id, agent_id=target.id)
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


def consult(tenant, live, *, heard: bool = True) -> tuple[str, object]:
    """A warm transfer from `live`'s agent to a new agent who answers. Returns its id and the target's leg."""
    target = tenant.agent("Target").login()
    tid = ok(act(live.agent.api(), live.id, "transfers", {"mode": "warm", "target": to_agent(target)}))["transfer_id"]
    leg = target.phone.wait_ring().wait_state("in_progress")
    if heard:
        live.stream.wait_for("offer.accepted", conversation_id=live.id, agent_id=target.id)
    else:
        tenant.world.wait(lambda: leg.mpc == live.caller.mpc, what="the target in the caller's room")
    return tid, leg


def told(tenant, live) -> list[str]:
    return [e["data"]["status"] for e in tenant.events(type="call.transferred") if e["conversation_id"] == live.id]


def test_a_complete_whose_agent_hangup_plivo_refuses_answers_the_refusal_and_keeps_the_agent(world, tenant):
    live = connect(tenant)
    tid, leg = consult(tenant, live)
    world.plivo.reject_next(f"/Call/{live.leg.call_uuid}/", 400)
    error(act(live.agent.api(), live.id, f"transfers/{tid}/complete"), 422, "plivo_refused")
    fence(tenant)
    assert "completed" not in told(tenant, live) and activity(tenant, live.agent) == "on_call"
    assert live.leg.state == leg.state == "in_progress"
    ok(act(live.agent.api(), live.id, f"transfers/{tid}/complete"))
    live.leg.wait_state("completed")
    live.stream.wait_for("call.transferred", conversation_id=live.id, status="completed")


def test_a_complete_whose_agent_hangup_reply_plivo_lost_is_finished_once_by_the_agent_legs_hangup(world, tenant):
    live = connect(tenant)
    tid, leg = consult(tenant, live)
    world.plivo.fault("lose", lambda r: r.method == "DELETE" and live.leg.call_uuid in r.path, account=tenant.auth_id)
    error(act(live.agent.api(), live.id, f"transfers/{tid}/complete"), 503, "dependency_unavailable")
    live.leg.wait_state("completed")
    live.stream.wait_for("call.transferred", conversation_id=live.id, status="completed")
    world.run_jobs("plivo")
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    fence(tenant)
    assert told(tenant, live).count("completed") == 1 and leg.state == "in_progress"


def test_a_cancel_while_a_complete_waits_on_plivo_is_refused_and_the_complete_finishes_once(world, tenant):
    live = connect(tenant)
    tid, leg = consult(tenant, live)
    slow = {"account": tenant.auth_id, "after_sec": 3}
    hangup = world.plivo.fault("slow", lambda r: r.method == "DELETE" and live.leg.call_uuid in r.path, **slow)
    with ThreadPoolExecutor(1) as pool:
        completing = pool.submit(act, live.agent.api(), live.id, f"transfers/{tid}/complete")
        world.wait(lambda: hangup.hits, what="the agent's hangup on its way to Plivo")
        error(act(live.agent.api(), live.id, f"transfers/{tid}/cancel"), 409, "transfer_completing")
        assert completing.result(15).status_code == 204
    live.leg.wait_state("completed")
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    fence(tenant)
    assert (told(tenant, live).count("completed"), "cancelled" in told(tenant, live)) == (1, False)
    assert leg.state == "in_progress"


def test_a_cancel_while_a_completes_hangup_retries_is_refused_and_the_transfer_completes_once_plivo_answers(
    world, tenant
):
    live = connect(tenant)
    tid, leg = consult(tenant, live)
    world.plivo.reject_next(f"/Call/{live.leg.call_uuid}/", 503)
    error(act(live.agent.api(), live.id, f"transfers/{tid}/complete"), 503, "dependency_unavailable")
    error(act(live.agent.api(), live.id, f"transfers/{tid}/cancel"), 409, "transfer_completing")
    world.run_jobs("plivo")
    live.leg.wait_state("completed")
    live.stream.wait_for("call.transferred", conversation_id=live.id, status="completed")
    fence(tenant)
    assert told(tenant, live).count("completed") == 1 and "cancelled" not in told(tenant, live)
    assert leg.state == "in_progress"


def test_a_completes_hangup_plivo_never_answers_stops_after_its_retries_and_a_cancel_then_stops_the_transfer(
    world, tenant
):
    live = connect(tenant)
    tid, leg = consult(tenant, live)
    unanswered = {"account": tenant.auth_id, "times": 4, "status": 503}
    world.plivo.fault("reject", lambda r: r.method == "DELETE" and live.leg.call_uuid in r.path, **unanswered)
    error(act(live.agent.api(), live.id, f"transfers/{tid}/complete"), 503, "dependency_unavailable")
    for _ in range(3):
        error(act(live.agent.api(), live.id, f"transfers/{tid}/cancel"), 409, "transfer_completing")
        world.run_jobs("plivo")
    ok(act(live.agent.api(), live.id, f"transfers/{tid}/cancel"))
    leg.wait_state("completed")
    live.stream.wait_for("call.transferred", conversation_id=live.id, status="cancelled")
    assert live.leg.state == live.caller.state == "in_progress"


def test_a_cancel_after_plivo_refused_a_completes_hangup_stops_the_transfer(world, tenant):
    live = connect(tenant)
    tid, leg = consult(tenant, live)
    world.plivo.reject_next(f"/Call/{live.leg.call_uuid}/", 400)
    error(act(live.agent.api(), live.id, f"transfers/{tid}/complete"), 422, "plivo_refused")
    ok(act(live.agent.api(), live.id, f"transfers/{tid}/cancel"))
    leg.wait_state("completed")
    live.stream.wait_for("call.transferred", conversation_id=live.id, status="cancelled")
    assert live.leg.state == "in_progress"


def unheard(world, tenant, live):
    """A warm transfer whose target answered into the caller's room, its join held back from HS. Returns the held
    join, the transfer's id and the target's leg."""
    known = {live.leg.call_uuid, live.caller.call_uuid}

    def targets_join(r) -> bool:
        return r.params.get("EventName") == "ParticipantJoin" and r.params.get("ParticipantCallUUID") not in known

    held = world.plivo.fault("delay", targets_join, account=tenant.auth_id)
    tid, leg = consult(tenant, live, heard=False)
    world.wait(lambda: held.hits, what="the target's join held")
    return held, tid, leg


def test_a_complete_before_hs_heard_the_targets_join_that_plivo_refuses_keeps_the_agent(world, tenant):
    live = connect(tenant)
    held, tid, leg = unheard(world, tenant, live)

    def agent_off(r) -> bool:
        return r.method == "DELETE" and (live.leg.call_uuid in r.path or "/Participant/" in r.path)

    world.plivo.fault("reject", agent_off, account=tenant.auth_id, status=400)
    error(act(live.agent.api(), live.id, f"transfers/{tid}/complete"), 422, "plivo_refused")
    held.release()
    live.stream.wait_for("call.transferred", conversation_id=live.id, status="accepted")
    fence(tenant)
    assert "completed" not in told(tenant, live) and activity(tenant, live.agent) == "on_call"
    assert live.leg.state == leg.state == "in_progress"
    ok(act(live.agent.api(), live.id, f"transfers/{tid}/complete"))
    live.leg.wait_state("completed")
    live.stream.wait_for("call.transferred", conversation_id=live.id, status="completed")


def test_a_complete_before_hs_heard_the_targets_join_hands_over_once_plivo_takes_the_agent_off(world, tenant):
    live = connect(tenant)
    held, tid, leg = unheard(world, tenant, live)
    ok(act(live.agent.api(), live.id, f"transfers/{tid}/complete"))
    live.leg.wait_state("completed")
    held.release()
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    fence(tenant)
    assert told(tenant, live).count("completed") == 1 and leg.state == "in_progress"


def test_a_plivo_outage_on_any_leg_of_a_hangup_is_a_503_and_a_retry_ends_the_call(world, tenant):
    target = tenant.agent("Target").login()
    live = connect(tenant)
    ok(act(live.agent.api(), live.id, "participants", {"target": to_agent(target)}))
    leg = target.phone.wait_ring().wait_state("in_progress")
    world.plivo.reject_next(f"/Call/{live.leg.call_uuid}/", 404)
    world.plivo.reject_next(f"/Call/{leg.call_uuid}/", 503)
    error(act(live.agent.api(), live.id, "hangup"), 503, "dependency_unavailable")
    ok(act(live.agent.api(), live.id, "hangup"))
    live.leg.wait_state("completed")
    leg.wait_state("completed")


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
    """The agent wraps up and is offered the caller again only after it submits, the room it left ended or not."""
    live = connect(tenant)
    back = {"type": "queue", "queue_id": live.queue["id"]}
    ok(act(live.agent.api(), live.id, "transfers", {"mode": "cold", "target": back}))
    assert activity(tenant, live.agent) == "wrap_up"
    live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id, agent_id=live.agent.id)
    world.wait(lambda: seen(live.stream, "conversation.queued", conversation_id=live.id)[1:], what="queued again")
    world.run_jobs("route_agent")
    again = [e for e in emitted(tenant, "offer.created", live.id) if e["agent_id"] == live.agent.id][1:]
    assert not again, "offered again before its wrap-up was submitted"
    ok(wrap_up(live.agent, live.id, disposition(tenant)["id"]))
    leg = live.agent.phone.wait_ring()
    leg.wait_state("in_progress")
    live.stream.wait_for("offer.accepted", conversation_id=live.id, offer_id=hs_header(leg, "HsOffer"))
    assert live.caller.state == "in_progress"
    assert not [c.status for c in world.plivo.callbacks if live.caller.call_uuid in c.url and c.status >= 500]


def test_an_agent_who_takes_back_the_caller_it_handed_to_a_queue_wraps_both_up_with_one_submit(world, tenant):
    settings(tenant, acw_blocks_routing=False)
    live, done = connect(tenant), disposition(tenant)["id"]
    back = {"type": "queue", "queue_id": live.queue["id"]}
    ok(act(live.agent.api(), live.id, "transfers", {"mode": "cold", "target": back}))
    leg = live.agent.phone.wait_ring().wait_state("in_progress")
    live.stream.wait_for("offer.accepted", conversation_id=live.id, offer_id=hs_header(leg, "HsOffer"))
    error(wrap_up(live.agent, live.id, done), 422, "not_in_wrap_up")
    live.caller.hangup()
    world.wait(lambda: seen(live.stream, "conversation.wrap_up_started", conversation_id=live.id)[1:], what="wrap-up 2")
    ok(wrap_up(live.agent, live.id, done))
    live.stream.wait_for("conversation.closed", conversation_id=live.id)
    assert len(replayed(tenant, "conversation.wrapped_up", conversation_id=live.id)) == 1


def test_a_warm_target_that_answers_after_the_first_agent_hangs_up_takes_the_caller(world, tenant):
    target = tenant.agent("Target").login()
    target.phone.on_ring("answer", after=1.0)
    live = connect(tenant)
    ok(act(live.agent.api(), live.id, "transfers", {"mode": "warm", "target": to_agent(target)}))
    target.phone.wait_ring()
    live.leg.hangup()
    leg = target.phone.wait_ring().wait_state("in_progress")
    world.wait(lambda: leg.mpc == live.caller.mpc, what="the target in the caller's room")
    done = live.stream.wait_for("call.transferred", conversation_id=live.id, status="completed")
    assert (done["data"]["mode"], done["data"]["from_agent_id"]) == ("warm", live.agent.id)
    assert live.caller.state == "in_progress"


def test_a_warm_target_that_never_answers_after_the_first_agent_hangs_up_returns_the_caller_to_the_queue(world, tenant):
    target, backup = tenant.agent("Target").login(), tenant.agent("Backup")
    target.phone.on_ring("ignore")
    live = connect(tenant, also=(backup,))
    ok(act(live.agent.api(), live.id, "transfers", {"mode": "warm", "target": to_agent(target)}))
    ringing = target.phone.wait_ring()
    live.leg.hangup()
    ringing.wait_state("completed")
    backup.login()
    leg = backup.phone.wait_ring().wait_state("in_progress")
    world.wait(lambda: leg.mpc == live.caller.mpc, what="the backup agent in the caller's room")
    assert live.caller.state == "in_progress"


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


def test_an_agent_cannot_redirect_a_call_without_direct_dial_and_a_tenant_key_can(world, tenant):
    app = world.plivo.app("<Response><Speak>Welcome to self service</Speak></Response>")
    live = connect(tenant)
    redirect = {"mode": "cold", "target": {"type": "redirect", "url": app}}
    error(act(live.agent.api(), live.id, "transfers", redirect), 403, "dial_requires_server")
    assert live.leg.state == "in_progress"
    ok(act(tenant.api, live.id, "transfers", redirect))
    world.wait(lambda: "Welcome to self service" in live.caller.said, what="the tenant app")


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
    stream.wait_for("call.transferred", conversation_id=cid, status="completed")


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


def test_an_offers_id_names_its_ring_its_events_and_the_stint_it_becomes(world, tenant):
    third = tenant.agent("Third").login()
    live = connect(tenant)
    [created] = replayed(tenant, "offer.created", conversation_id=live.id)
    [accepted] = replayed(tenant, "offer.accepted", conversation_id=live.id)
    assert hs_header(live.leg, "HsOffer") == created["data"]["offer_id"] == accepted["data"]["offer_id"]
    pid = ok(act(live.agent.api(), live.id, "participants", {"target": to_agent(third)}))["participant_id"]
    leg = third.phone.wait_ring()
    assert hs_header(leg, "HsOffer") == pid
    leg.wait_state("in_progress")
    world.wait(world.plivo.idle, what="the participant's join answered")
    ok(live.agent.api().delete(f"/api/v1/calls/{live.id}/participants/{pid}"))
    leg.wait_state("completed")


def test_removing_a_participant_whose_ring_was_declined_is_422(tenant):
    third = tenant.agent("Third").login()
    third.phone.on_ring("decline")
    live = connect(tenant)
    pid = ok(act(live.agent.api(), live.id, "participants", {"target": to_agent(third)}))["participant_id"]
    live.stream.wait_for("offer.rejected", offer_id=pid)
    error(live.agent.api().delete(f"/api/v1/calls/{live.id}/participants/{pid}"), 422, "participant_not_on_call")


def test_removing_a_participant_whose_offer_ended_while_its_leg_still_rings_hangs_the_leg_up(world, tenant):
    third = tenant.agent("Third").login()
    third.phone.on_ring("ignore")
    live = connect(tenant)
    pid = ok(act(live.agent.api(), live.id, "participants", {"target": to_agent(third)}))["participant_id"]
    leg = third.phone.wait_ring()
    world.wait(world.plivo.idle, what="the ring's legs bound")
    world.run_jobs("offer_timeout")
    live.stream.wait_for("offer.missed", offer_id=pid)
    ok(live.agent.api().delete(f"/api/v1/calls/{live.id}/participants/{pid}"))
    leg.wait_state("completed")


def test_cancelling_a_transfer_whose_only_target_declined_is_422_not_in_transfer(tenant):
    target = tenant.agent("Target").login()
    target.phone.on_ring("decline")
    live = connect(tenant)
    tid = ok(act(live.agent.api(), live.id, "transfers", {"mode": "warm", "target": to_agent(target)}))["transfer_id"]
    live.stream.wait_for("call.transferred", conversation_id=live.id, status="failed")
    error(act(live.agent.api(), live.id, f"transfers/{tid}/cancel"), 422, "not_in_transfer")


def test_removing_a_participant_agent_while_it_rings_cancels_its_offer(tenant):
    third = tenant.agent("Third").login()
    third.phone.on_ring("ignore")
    live = connect(tenant)
    pid = ok(act(live.agent.api(), live.id, "participants", {"target": to_agent(third)}))["participant_id"]
    leg = third.phone.wait_ring()
    ok(live.agent.api().delete(f"/api/v1/calls/{live.id}/participants/{pid}"))
    leg.wait_state("completed")
    assert live.stream.wait_for("offer.withdrawn", offer_id=pid)["data"]["reason"] == "cancelled"
    assert live.stream.wait_for("call.participant_removed", conversation_id=live.id)["data"]["participant_id"] == pid
    assert live.leg.state == "in_progress"


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
    paused = live.stream.wait_for("call.recording_paused", conversation_id=live.id)
    assert (paused["agent_id"], paused["data"]["by_agent_id"]) == (live.agent.id, live.agent.id)
    ok(act(tenant.api, live.id, "recording", {"action": "resume"}))
    resumed = live.stream.wait_for("call.recording_resumed", conversation_id=live.id)
    assert (resumed["agent_id"], resumed["data"]["by_agent_id"]) == (live.agent.id, None), "the call's agent"


def test_a_plivo_outage_on_a_control_is_a_503_and_a_retry_succeeds(world, tenant):
    settings(tenant, recording={"inbound": True, "allow_pause": True})
    live = connect(tenant)
    world.plivo.reject_next("/Record/Pause/", 503, account=tenant.auth_id)
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


def test_a_pause_that_one_members_track_refuses_fails_with_no_event_and_a_retry_pauses_it(world, tenant):
    settings(tenant, recording={"inbound": True, "allow_pause": True})
    live = connect(tenant)
    track = world.plivo.fault(
        "reject", lambda r: "/Participant/" in r.path and "/Record/Pause" in r.path, account=tenant.auth_id, status=400
    )
    failed = error(act(live.agent.api(), live.id, "recording", {"action": "pause"}), 422, "plivo_refused")
    assert failed["details"]["status"] == 400
    fence(tenant)
    assert not tenant.events(type="call.recording_paused")
    ok(act(live.agent.api(), live.id, "recording", {"action": "pause"}))
    fence(tenant)
    assert len(tenant.events(type="call.recording_paused")) == 1
    assert [r.path for r in world.plivo.requests].count(track.hits[0].path) == 2


def test_a_resume_starts_the_track_of_a_member_who_joined_with_none(world, tenant):
    settings(tenant, recording={"inbound": False, "allow_pause": True})
    live = connect(tenant)
    ok(act(live.agent.api(), live.id, "recording", {"action": "resume"}))
    number, phone = lead(world)
    ok(act(tenant.api, live.id, "participants", {"target": {"type": "number", "number": number}}))
    extra = phone.wait_ring()
    world.wait(lambda: extra.mpc == live.caller.mpc, what="the participant in the caller's room")
    for action in ("pause", "resume"):
        ok(act(live.agent.api(), live.id, "recording", {"action": action}))
    mine = [r for r in world.plivo.recordings.values() if r.account.auth_id == tenant.auth_id]
    assert extra.call_uuid in {r.call_uuid for r in mine if r.kind == "participant"}


def test_a_resume_whose_start_timed_out_is_a_503_and_its_retry_starts_one_recording(world, tenant):
    settings(tenant, recording={"inbound": False, "allow_pause": True})
    live = connect(tenant)
    start = f"name_{live.caller.mpc}/Record"

    def starting(r) -> bool:
        return r.method == "POST" and r.path.rstrip("/").endswith(start)

    world.plivo.fault("timeout", starting, account=tenant.auth_id)
    error(act(live.agent.api(), live.id, "recording", {"action": "resume"}), 503, "dependency_unavailable")
    run_jobs_until(world, lambda: tenant.events(type="call.recording_resumed"), "the retried resume")
    fence(tenant)
    mine = [r for r in world.plivo.recordings.values() if r.account.auth_id == tenant.auth_id]
    assert len([r for r in world.plivo.requests if starting(r)]) == 1 and len([r for r in mine if r.kind == "mpc"]) == 1
    assert len(tenant.events(type="call.recording_resumed")) == 1


def test_a_resume_never_starts_a_second_recording_in_a_room_that_records(world, tenant):
    settings(tenant, recording={"inbound": True, "allow_pause": True})
    live = connect(tenant)
    room = f"name_{live.caller.mpc}"
    ok(act(live.agent.api(), live.id, "recording", {"action": "resume"}))
    assert not [
        r for r in world.plivo.requests if r.method == "POST" and room in r.path and r.path.endswith("/Record/")
    ]
    world.plivo.reject_next(f"{room}/Record/Resume", 409, account=tenant.auth_id)
    error(act(live.agent.api(), live.id, "recording", {"action": "resume"}), 422, "plivo_refused")
    mine = [r for r in world.plivo.recordings.values() if r.account.auth_id == tenant.auth_id]
    assert len([r for r in mine if r.kind == "mpc"]) == 1


def test_a_pause_that_a_resume_replaced_while_plivo_stalled_it_answers_204_and_the_call_records(world, tenant):
    settings(tenant, recording={"inbound": True, "allow_pause": True})
    live = connect(tenant)
    paused = f"name_{live.caller.mpc}/Record/Pause"
    stall = world.plivo.fault("stall", lambda r: r.path.rstrip("/").endswith(paused), account=tenant.auth_id)
    answers = {}

    def send(client, action: str) -> None:
        answers[action] = act(client, live.id, "recording", {"action": action})

    pause = threading.Thread(target=send, args=(live.agent.api(), "pause"))
    pause.start()
    world.wait(lambda: stall.hits, what="the pause stalled at Plivo")
    resume = threading.Thread(target=send, args=(tenant.api, "resume"))
    resume.start()
    stall.release()
    pause.join(15)
    resume.join(15)
    ok(answers["pause"])
    ok(answers["resume"])
    toggles = [r.path for r in world.plivo.requests if r.path.endswith(("/Record/Pause/", "/Record/Resume/"))]
    last = dict(path.rsplit("/Record/", 1) for path in toggles if tenant.auth_id in path)
    assert len(last) > 1 and set(last.values()) == {"Resume/"}
    fence(tenant)
    assert len(tenant.events(type="call.recording_resumed")) == 1


def test_a_pause_of_a_queued_calls_recording_is_recorded_and_told_once(world, tenant):
    settings(tenant, recording={"inbound": False, "allow_pause": True})
    number, _ = tenant.inbound_line(unavailable(tenant))
    stream = tenant.stream(after=0)
    caller = world.plivo.call(number)
    cid = stream.wait_for("conversation.queued")["conversation_id"]
    world.wait(lambda: caller.mpc, what="the caller seated in its queue's room")
    ok(act(tenant.api, cid, "recording", {"action": "resume"}))
    for _ in range(2):
        ok(act(tenant.api, cid, "recording", {"action": "pause"}))
    fence(tenant)
    assert [len(tenant.events(type=f"call.recording_{a}d")) for a in ("resume", "pause")] == [1, 1]


def queue_transfer(tenant, live) -> None:
    target = tenant.agent("Target").login()
    billing = tenant.queue(name="Billing", agents=[target.id])
    moved = {"mode": "cold", "target": {"type": "queue", "queue_id": billing["id"]}}
    ok(act(live.agent.api(), live.id, "transfers", moved))
    live.stream.wait_for("offer.accepted", conversation_id=live.id, agent_id=target.id)


def caller_hangs_up(tenant, live) -> None:
    live.caller.hangup()
    live.stream.wait_for("call.ended", conversation_id=live.id)


def recording_state(world, cid: str):
    """The call's stored recording state: the room and the action Plivo last confirmed there."""
    with closing(psycopg2.connect(world._env["DATABASE_URL"])) as db, db.cursor() as cur:
        cur.execute("SELECT routing -> 'recording' FROM hs.conversation WHERE id = %s", (cid,))
        return cur.fetchone()[0]


@pytest.mark.parametrize("leave", [queue_transfer, caller_hangs_up], ids=["transfer", "hangup"])
@pytest.mark.parametrize("action", ["pause", "resume"])
def test_a_pause_or_resume_plivo_confirms_after_the_caller_left_its_room_tells_nothing(world, tenant, leave, action):
    settings(tenant, recording={"inbound": True, "allow_pause": True})
    live = connect(tenant)
    if action == "resume":
        ok(act(live.agent.api(), live.id, "recording", {"action": "pause"}))
        live.stream.wait_for("call.recording_paused", conversation_id=live.id)
    tracks = []

    def track_of_the_caller_or_agent_toggled_last(r) -> bool:
        if r.method == "POST" and "/Participant/" in r.path and f"/Record/{action.capitalize()}" in r.path:
            tracks.append(r)
            return len(tracks) == 2
        return False

    stall = world.plivo.fault("stall", track_of_the_caller_or_agent_toggled_last, account=tenant.auth_id)
    with ThreadPoolExecutor(1) as pool:
        asking = pool.submit(act, live.agent.api(), live.id, "recording", {"action": action})
        world.wait(lambda: stall.hits, what="the last track stalled at Plivo")
        leave(tenant, live)
        kept = recording_state(world, live.id)
        stall.release()
        assert asking.result(15).status_code == 204
    fence(tenant)
    assert not tenant.events(type=f"call.recording_{action}d") and recording_state(world, live.id) == kept


def test_a_hold_that_a_resume_replaced_while_plivo_stalled_it_answers_204_and_the_caller_is_resumed(world, tenant):
    live = connect(tenant)
    stall = world.plivo.fault("stall", lambda r: r.method == "POST" and "hold" in r.params, account=tenant.auth_id)
    answers = {}

    def send(client, on: bool) -> None:
        answers[on] = act(client, live.id, "hold", {"on": on})

    holding = threading.Thread(target=send, args=(live.agent.api(), True))
    holding.start()
    world.wait(lambda: stall.hits, what="the hold stalled at Plivo")
    resuming = threading.Thread(target=send, args=(tenant.api, False))
    resuming.start()
    stall.release()
    holding.join(15)
    resuming.join(15)
    ok(answers[False])
    ok(answers[True])
    assert not live.caller.held


@pytest.mark.parametrize(
    ("action", "bodies", "stalls", "refuses"),
    [
        pytest.param(
            "hold",
            ({"on": True}, {"on": False}),
            lambda r: r.params.get("hold") is True,
            lambda r: r.params.get("hold") is False,
            id="hold",
        ),
        pytest.param(
            "recording",
            ({"action": "pause"}, {"action": "resume"}),
            lambda r: r.path.endswith("/Record/Pause/"),
            lambda r: r.path.endswith("/Record/Resume/"),
            id="recording",
        ),
    ],
)
def test_controls_a_later_one_replaced_answer_204_and_only_the_later_one_answers_plivos_refusal(
    world, tenant, action, bodies, stalls, refuses
):
    settings(tenant, recording={"inbound": True, "allow_pause": True})
    live = connect(tenant)
    stall = world.plivo.fault("stall", lambda r: r.method == "POST" and stalls(r), account=tenant.auth_id)
    world.plivo.fault("reject", lambda r: r.method == "POST" and refuses(r), account=tenant.auth_id, status=400)
    answers, threads = {}, []

    def send(name: str, client, body: dict) -> None:
        threads.append(threading.Thread(target=lambda: answers.update({name: act(client, live.id, action, body)})))
        threads[-1].start()

    send("first", live.agent.api(), bodies[0])
    world.wait(lambda: stall.hits, what=f"Plivo holding its answer to the first {action}")
    send("last", live.agent.api(), bodies[1])
    stall.release()
    for thread in threads:
        thread.join(15)
    ok(answers["first"])
    error(answers["last"], 422, "plivo_refused")


def test_a_pause_that_plivo_finishes_on_its_backoff_and_the_clients_retry_emit_one_event(world, tenant):
    settings(tenant, recording={"inbound": True, "allow_pause": True})
    live = connect(tenant)
    world.plivo.reject_next("/Record/Pause/", 503, account=tenant.auth_id)
    error(act(live.agent.api(), live.id, "recording", {"action": "pause"}), 503, "dependency_unavailable")
    run_jobs_until(world, lambda: tenant.events(type="call.recording_paused"), "the pause on its backoff")
    ok(act(live.agent.api(), live.id, "recording", {"action": "pause"}))
    fence(tenant)
    assert len(tenant.events(type="call.recording_paused")) == 1


def test_a_pause_of_a_paused_call_is_a_204_with_no_second_event(tenant):
    settings(tenant, recording={"inbound": True, "allow_pause": True})
    live = connect(tenant)
    for _ in range(2):
        ok(act(live.agent.api(), live.id, "recording", {"action": "pause"}))
    fence(tenant)
    assert len(tenant.events(type="call.recording_paused")) == 1


def test_hangup_ends_the_call(tenant):
    live = connect(tenant)
    ok(act(tenant.api, live.id, "hangup"))
    live.caller.wait_state("completed")
    live.leg.wait_state("completed")
    live.stream.wait_for("call.ended", conversation_id=live.id)


def test_ending_the_call_while_a_cold_transfer_rings_withdraws_that_offer(world, tenant):
    agent, target = tenant.agent().login(), tenant.agent("Target").login()
    target.phone.on_ring("ignore")
    number, _ = tenant.inbound_line(agent, target, strategy="simultaneous")
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
    assert not replayed(tenant, "conversation.closed", conversation_id=live.id)
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


def test_a_warm_transfer_to_a_number_keeps_the_caller_when_the_first_agent_hangs_up(world, tenant):
    number, phone = lead(world)
    live = connect(tenant)
    body = {"mode": "warm", "target": {"type": "number", "number": number}}
    tid = ok(act(tenant.api, live.id, "transfers", body))["transfer_id"]
    leg = phone.wait_ring().wait_state("in_progress")
    world.wait(lambda: leg.mpc == live.caller.mpc, what="the consult in the caller's room")
    world.wait(world.plivo.idle, what="the consult's join answered")
    live.leg.hangup()
    done = live.stream.wait_for("call.transferred", conversation_id=live.id, status="completed")
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    fence(tenant)
    assert (done["data"]["transfer_id"], done["data"]["target"]) == (tid, {"type": "number", "number": number})
    assert live.caller.state == leg.state == "in_progress" and leg.mpc == live.caller.mpc
    assert not [e for e in tenant.events(type="conversation.closed") if e["conversation_id"] == live.id]


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
    error(act(live.agent.api(), live.id, "recording", {"action": "resume"}), 409, "call_not_live")
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
    assert (outcome(tenant.events()), live.caller.state) == ([("offer.withdrawn", "cancelled", None)], "in_progress")


def test_a_failed_rings_outcome_for_a_leg_whose_offer_ended_before_redis_heard_is_answered(world, tenant):
    target = tenant.agent("Target").login()
    target.phone.on_ring("ignore")
    live = connect(tenant)
    replica = world.replica(gated=True)
    tid = ok(act(live.agent.api(), live.id, "transfers", {"mode": "warm", "target": to_agent(target)}))["transfer_id"]
    target.phone.wait_ring()
    offer = live.stream.wait_for("offer.created", agent_id=target.id)["data"]["offer_id"]
    world.wait(world.plivo.idle, what="the ring's leg bound")

    def outcome(c) -> bool:
        return c.params.get("AddParticipantAPIRequestOutcome") == "unsuccessful" and f"/plivo/{tenant.id}/" in c.url

    def tombstone(sent: bytes) -> bool:
        return b"EVALSHA" in sent and offer.encode() in sent and b"withdrawn" in sent

    with replica.redis_held(tombstone) as held:
        ok(act(replica.client(live.agent.agent_token), live.id, f"transfers/{tid}/cancel"))
        world.wait(held, what="the ended offer's tombstone on its way to Redis")
        world.wait(lambda: [c for c in world.plivo.callbacks if outcome(c)], what="the failed ring's outcome answered")
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    assert [c.status for c in world.plivo.callbacks if outcome(c)] == [200]


def test_a_plivo_outage_on_a_hangup_is_a_503_and_a_retry_ends_the_call(world, tenant):
    live = connect(tenant)
    world.wait(world.plivo.idle, what="the agent's join handled")
    world.plivo.reject_next(f"/Call/{live.leg.call_uuid}/", 503)
    error(act(live.agent.api(), live.id, "hangup"), 503, "dependency_unavailable")
    assert (live.caller.state, live.leg.state) == ("in_progress", "in_progress")
    ok(act(live.agent.api(), live.id, "hangup"))
    live.caller.wait_state("completed")
    live.leg.wait_state("completed")


@pytest.mark.parametrize("to", ["queue", "voicemail"])
def test_an_outbound_call_moved_to_a_queue_or_voicemail_takes_the_lead_there_and_lets_the_agent_go(tenant, to):
    settings(tenant, wrap_up={"mode": "auto", "seconds": 60})
    billing = tenant.agent("Billing").login()
    q = tenant.queue(name="Billing", agents=[billing.id])
    agent, cid, stream, customer = answered_outbound(tenant)
    target = {"type": "queue", "queue_id": q["id"]} if to == "queue" else voicemail_action(q, "Leave it for billing")
    ok(act(agent.api(), cid, "transfers", {"mode": "cold", "target": target}))
    agent.phone.legs[0].wait_state("completed")
    if to == "queue":
        stream.wait_for("conversation.wrap_up_started", conversation_id=cid, agent_id=agent.id, mode="manual")
        leg = billing.phone.wait_ring().wait_state("in_progress")
        tenant.world.wait(lambda: leg.mpc == customer.mpc, what="the lead with the queue's agent")
        assert customer.state == "in_progress"
    else:
        vm = stream.wait_for("voicemail.received", conversation_id=cid, timeout=10)["data"]
        assert vm["mailbox"]["queue_id"] == q["id"] and "Leave it for billing" in customer.said
    assert [e["data"]["status"] for e in emitted(tenant, "call.transferred", cid)] == ["initiated"]


def test_an_outbound_call_moved_to_a_queue_tells_call_ended_once_per_room_to_the_agent_on_it(world, tenant):
    billing = tenant.agent("Billing").login()
    q = tenant.queue(name="Billing", agents=[billing.id])
    agent, cid, stream, customer = answered_outbound(tenant)
    ok(act(agent.api(), cid, "transfers", {"mode": "cold", "target": {"type": "queue", "queue_id": q["id"]}}))
    leg = billing.phone.wait_ring().wait_state("in_progress")
    world.wait(lambda: leg.mpc == customer.mpc, what="the lead with the queue's agent")
    customer.hangup()
    stream.wait_for("call.ended", conversation_id=cid, agent_id=billing.id)
    world.wait(world.plivo.idle, what="the lead's hangup delivered")
    fence(tenant)
    kinds = ("call.ended", "offer.accepted")
    told = [(e["type"], e["agent_id"]) for e in tenant.events() if e["conversation_id"] == cid and e["type"] in kinds]
    assert told == [("call.ended", agent.id), ("offer.accepted", billing.id), ("call.ended", billing.id)]


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
    assert not replayed(tenant, "conversation.closed", conversation_id=live.id)
    ok(wrap_up(target, live.id, disposition(tenant, "Second")["id"]))
    closed = live.stream.wait_for("conversation.closed", conversation_id=live.id)
    assert closed["agent_id"] == target.id
    assert {d["agent_id"] for d in closed["data"]["dispositions"]} == {live.agent.id, target.id}


def test_a_pause_refused_by_the_calls_state_logs_no_error(tenant, caplog):
    settings(tenant, recording={"inbound": False, "allow_pause": True})
    live = connect(tenant)
    caplog.clear()
    refused = error(act(live.agent.api(), live.id, "recording", {"action": "pause"}), 422, "plivo_refused")
    assert refused["details"]["status"] == 409
    logged = [(r.levelno, r.getMessage()) for r in caplog.records]
    assert any("Record/Pause" in m for _, m in logged), "HS's log was not captured"
    assert not [m for level, m in logged if level >= logging.ERROR]


def emitted(tenant, type_: str, cid: str) -> list[dict]:
    """Every `type_` event of the conversation so far."""
    fence(tenant)
    return [e for e in tenant.events(type=type_) if e["conversation_id"] == cid]


def minutes_later(world, *agents) -> None:
    """Five minutes pass for what HS stores. Agents whose desk expired sign in again."""
    world.wait(world.plivo.idle, what="the call settled")
    world.travel(300)
    for agent in agents:
        agent.heartbeat()


def hours_later(world, *agents) -> None:
    """Three hours pass for what HS stores, past every call-state and session expiry. Each agent signs in again."""
    world.wait(world.plivo.idle, what="the call settled")
    world.travel(3 * 3600)
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


def test_a_hangup_on_a_call_that_already_ended_is_refused(tenant):
    live = connect(tenant)
    live.caller.hangup()
    live.stream.wait_for("call.ended", conversation_id=live.id)
    error(act(tenant.api, live.id, "hangup"), 409, "call_not_live")


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
        live.stream.wait_for("offer.accepted", conversation_id=live.id, agent_id=target.id)
        ok(act(live.agent.api(), live.id, f"transfers/{tid}/complete"))
    if outcome in ("completed", "failed"):
        live.stream.wait_for("call.transferred", conversation_id=live.id, status=outcome)
    minutes_later(world, live.agent, target)
    on_call = target if outcome == "completed" else live.agent
    ok(act(on_call.api(), live.id, "hangup"))
    live.leg.wait_state("completed")
    if outcome == "consulting":
        live.stream.wait_for("call.transferred", conversation_id=live.id, agent_id=live.agent.id, status="completed")
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
    done = live.stream.wait_for("call.transferred", conversation_id=live.id, agent_id=live.agent.id, status="completed")
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
    done = live.stream.wait_for("call.transferred", conversation_id=live.id, agent_id=live.agent.id, status="completed")
    assert (done["data"]["mode"], done["data"]["status"]) == ("cold", "completed")
    assert (live.caller.state, leg.state) == ("in_progress", "in_progress")


@pytest.mark.parametrize("finished_by", ["the_agents_retry", "hs"])
def test_a_hangup_whose_agent_leg_failed_then_ended_meanwhile_still_ends_every_leg(world, tenant, finished_by):
    target = tenant.agent("Target").login()
    live = connect(tenant)
    ok(act(live.agent.api(), live.id, "participants", {"target": to_agent(target)}))
    leg = target.phone.wait_ring().wait_state("in_progress")
    world.wait(world.plivo.idle, what="the participant's join handled")
    world.plivo.reject_next(f"/Call/{live.leg.call_uuid}/", 503)
    error(act(live.agent.api(), live.id, "hangup"), 503, "dependency_unavailable")
    world.advance(1)
    live.leg.wait_state("completed")
    world.wait(world.plivo.idle, what="the agent's exit handled")
    if finished_by == "the_agents_retry":
        ok(act(live.agent.api(), live.id, "hangup"))
    else:
        world.advance(10)
    for x in (live.caller, leg):
        x.wait_state("completed")


def test_a_control_sent_once_the_hangup_handed_the_caller_back_is_refused(world, tenant):
    settings(tenant, recording={"inbound": False, "allow_pause": True})
    app = world.plivo.app('<Response><Speak>Please rate your call</Speak><Wait length="60"/></Response>')
    live = connect(tenant, after_agent_action={"type": "redirect", "url": app})
    ok(act(live.agent.api(), live.id, "hangup"))
    world.wait(lambda: "Please rate your call" in live.caller.said, what="the after-agent action")
    error(act(tenant.api, live.id, "recording", {"action": "resume"}), 409, "call_not_live")
    assert not [r for r in world.plivo.requests_to("/Record") if live.id in r.path]


def test_the_agents_hangup_whose_after_agent_action_hangs_up_reads_ended_by_agent(world, tenant):
    live = connect(tenant, after_agent_action={"type": "hangup"})
    ok(act(live.agent.api(), live.id, "hangup"))
    live.caller.wait_state("completed")
    run_jobs_until(world, lambda: seen(live.stream, "call.ended", conversation_id=live.id), "call.ended")
    assert [e["data"]["ended_by"] for e in emitted(tenant, "call.ended", live.id)] == ["agent"]


def test_a_recording_control_after_the_hangup_is_refused_as_not_live_where_pausing_is_off(world, tenant):
    settings(tenant, recording={"inbound": False, "allow_pause": False})
    live = connect(tenant)
    hang_up_with_the_room_alive(world, tenant, live)
    error(act(live.agent.api(), live.id, "recording", {"action": "resume"}), 409, "call_not_live")


@pytest.mark.parametrize("room_end_first", [False, True], ids=["exit_first", "room_end_first"])
@pytest.mark.parametrize("via", ["queue", "redirect_to_enqueue"])
def test_the_agents_hangup_hands_the_caller_back_to_a_queue_whichever_room_callback_comes_first(
    world, tenant, monkeypatch, via, room_end_first
):
    monkeypatch.setattr(world.plivo, "exit_after_events", room_end_first)
    nxt = tenant.agent("Next").login()
    nxt.phone.on_ring("answer")
    back = tenant.queue(agents=[nxt.id])
    action = {"type": "queue", "queue_id": back["id"]}
    if via == "redirect_to_enqueue":
        app = world.plivo.app(f"<Response><Redirect>{escape(back['enqueue_url'])}</Redirect></Response>")
        action = {"type": "redirect", "url": app}
    live = connect(tenant, after_agent_action=action)
    ok(act(live.agent.api(), live.id, "hangup"))
    leg = nxt.phone.wait_ring()
    assert hs_header(leg, "HsConversation") == live.id
    leg.wait_state("in_progress")
    world.wait(lambda: leg.mpc == live.caller.mpc, what="the caller in a room with the next agent")


@pytest.mark.parametrize("to", ["agent", "number"])
def test_call_ended_is_told_to_the_agent_who_has_the_call_when_it_ends(world, tenant, to):
    live = connect(tenant)
    if to == "agent":
        told = tenant.agent("Target").login()
        tid = ok(act(live.agent.api(), live.id, "transfers", {"mode": "warm", "target": to_agent(told)}))["transfer_id"]
        told.phone.wait_ring().wait_state("in_progress")
        live.stream.wait_for("offer.accepted", conversation_id=live.id, agent_id=told.id)
        ok(act(live.agent.api(), live.id, f"transfers/{tid}/complete"))
    else:
        told, (number, phone) = live.agent, lead(world)
        ok(act(tenant.api, live.id, "transfers", {"mode": "cold", "target": {"type": "number", "number": number}}))
        phone.wait_ring().wait_state("in_progress")
    live.leg.wait_state("completed")
    live.stream.wait_for("call.transferred", conversation_id=live.id, status="completed")
    live.caller.hangup()
    live.stream.wait_for("call.ended", conversation_id=live.id)
    assert [e["agent_id"] for e in emitted(tenant, "call.ended", live.id)] == [told.id]


def test_a_caller_its_queue_hands_to_another_agent_tells_call_ended_once_per_room_to_the_agent_on_it(world, tenant):
    nxt = tenant.agent("Next").login()
    nxt.phone.on_ring("answer")
    back = tenant.queue(agents=[nxt.id])
    live = connect(tenant, after_agent_action={"type": "queue", "queue_id": back["id"]})
    ok(act(live.agent.api(), live.id, "hangup"))
    leg = nxt.phone.wait_ring().wait_state("in_progress")
    world.wait(lambda: leg.mpc == live.caller.mpc, what="the caller in a room with the next agent")
    live.caller.hangup()
    live.stream.wait_for("call.ended", conversation_id=live.id, agent_id=nxt.id)
    assert [e["agent_id"] for e in emitted(tenant, "call.ended", live.id)] == [live.agent.id, nxt.id]


def test_a_caller_moved_to_a_menu_tells_call_ended_to_its_agent_and_not_again_when_it_hangs_up_there(world, tenant):
    live = connect(tenant)
    target = {"type": "menu", "menu_id": menu(tenant, {})["id"]}
    ok(act(live.agent.api(), live.id, "transfers", {"mode": "cold", "target": target}))
    world.wait(lambda: "Press a key" in live.caller.said, what="the caller in the menu")
    live.caller.hangup()
    ok(wrap_up(live.agent, live.id, disposition(tenant)["id"]))
    run_jobs_until(world, lambda: seen(live.stream, "conversation.closed", conversation_id=live.id), "the call closed")
    assert [e["agent_id"] for e in emitted(tenant, "call.ended", live.id)] == [live.agent.id]


@pytest.mark.parametrize("moved_by", ["transfer", "hangup"])
def test_a_caller_who_hangs_up_unserved_in_a_second_queue_tells_the_first_agent_no_second_call_ended(
    world, tenant, moved_by
):
    second = tenant.queue(name="Second")
    to_second = {"type": "queue", "queue_id": second["id"]}
    live = connect(tenant, after_agent_action=to_second if moved_by == "hangup" else None)
    if moved_by == "transfer":
        ok(act(live.agent.api(), live.id, "transfers", {"mode": "cold", "target": to_second}))
    else:
        ok(act(live.agent.api(), live.id, "hangup"))
    world.wait(lambda: live.caller.mpc == f"hs-{live.id}-1", what="the caller waiting in its second room")
    live.caller.hangup()
    world.wait(world.plivo.idle, what="the caller's hangup delivered")
    assert [e["agent_id"] for e in emitted(tenant, "call.ended", live.id)] == [live.agent.id, None]


@pytest.mark.parametrize("wrap_up_ends", ["submitted", "timed_out"])
def test_the_agents_hangup_hands_the_caller_back_though_its_queue_exit_reaches_hs_after_the_wrap_up_ended(
    world, tenant, monkeypatch, wrap_up_ends
):
    settings(tenant, wrap_up={"mode": "auto" if wrap_up_ends == "timed_out" else "manual", "seconds": 1})
    app = world.plivo.app("<Response><Speak>Please rate your call</Speak></Response>")
    live = connect(tenant, after_agent_action={"type": "redirect", "url": app})
    monkeypatch.setattr(world.plivo, "exit_after_events", True)
    exits, send = threading.Event(), world.plivo._send

    def held(acc, url, params, *args, **kwargs):
        if "/actions/queue-exit" in url:
            exits.wait(10)
        return send(acc, url, params, *args, **kwargs)

    monkeypatch.setattr(world.plivo, "_send", held)
    ok(act(live.agent.api(), live.id, "hangup"))
    live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id)
    if wrap_up_ends == "submitted":
        ok(wrap_up(live.agent, live.id, disposition(tenant)["id"]))
    live.stream.wait_for("conversation.wrapped_up", conversation_id=live.id)
    exits.set()
    world.wait(lambda: "Please rate your call" in live.caller.said, what="the after-agent action")


@pytest.mark.parametrize("finished_by", ["the_agents_retry", "hs", "hs_once_its_retries_ran_out"])
def test_a_leg_plivo_refused_on_a_hangup_is_sent_again_until_it_ends_and_then_the_caller_is_handed_back(
    world, tenant, finished_by
):
    app = world.plivo.app("<Response><Speak>Please rate your call</Speak></Response>")
    target = tenant.agent("Target").login()
    live = connect(tenant, after_agent_action={"type": "redirect", "url": app})
    ok(act(live.agent.api(), live.id, "participants", {"target": to_agent(target)}))
    leg = target.phone.wait_ring().wait_state("in_progress")
    world.wait(world.plivo.idle, what="the participant's join handled")
    world.advance(120)  # the room check after the participant's ring runs while it is on the call
    refusing = world.plivo.fault(
        "reject",
        lambda r: r.method == "DELETE" and leg.call_uuid in r.path,
        account=tenant.auth_id,
        times=math.inf,
        status=403,
    )
    error(act(live.agent.api(), live.id, "hangup"), 422, "plivo_refused")
    live.leg.wait_state("completed")
    if finished_by == "the_agents_retry":
        error(act(live.agent.api(), live.id, "hangup"), 422, "plivo_refused")
    world.wait(world.plivo.idle, what="the agent's exit handled")
    assert (live.caller.state, leg.state) == ("in_progress", "in_progress")
    if finished_by == "hs_once_its_retries_ran_out":
        for _ in range(6):
            world.advance(600)
        assert (live.caller.state, leg.state) == ("in_progress", "in_progress")
    refusing.times = 0
    if finished_by == "the_agents_retry":
        ok(act(live.agent.api(), live.id, "hangup"))
    else:
        world.advance(600 if finished_by == "hs_once_its_retries_ran_out" else 10)
    leg.wait_state("completed")
    world.wait(lambda: "Please rate your call" in live.caller.said, what="the after-agent action")


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
    done = live.stream.wait_for("call.transferred", conversation_id=live.id, status="completed")
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    fence(tenant)
    assert (done["data"]["transfer_id"], done["data"]["status"], done["data"]["mode"]) == (tid, "completed", "warm")
    assert done["data"]["target"] == {"type": "number", "number": number}
    assert live.caller.state == leg.state == "in_progress" and leg.mpc == live.caller.mpc
    told = [e["data"]["status"] for e in tenant.events(type="call.transferred") if e["conversation_id"] == live.id]
    assert told.count("completed") == 1


def test_cancelling_a_completed_transfer_to_a_number_is_refused_and_keeps_the_number_on_the_call(world, tenant):
    number, phone = lead(world)
    live = connect(tenant)
    body = {"mode": "warm", "target": {"type": "number", "number": number}}
    tid = ok(act(tenant.api, live.id, "transfers", body))["transfer_id"]
    leg = phone.wait_ring().wait_state("in_progress")
    world.wait(lambda: leg.mpc == live.caller.mpc, what="the consult in the caller's room")
    world.wait(world.plivo.idle, what="the consult's join answered")
    ok(act(live.agent.api(), live.id, f"transfers/{tid}/complete"))
    live.leg.wait_state("completed")
    world.wait(world.plivo.idle, what="the hand-over settled")
    error(act(tenant.api, live.id, f"transfers/{tid}/cancel"), 422, "transfer_completed")
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    assert live.caller.state == leg.state == "in_progress" and leg.mpc == live.caller.mpc


def test_a_warm_transfer_to_a_number_still_ringing_is_not_completed_and_keeps_the_call(world, tenant):
    settings(tenant, ring_timeout_sec=120)
    number, phone = lead(world)
    phone.on_ring("ignore")
    live = connect(tenant)
    body = {"mode": "warm", "target": {"type": "number", "number": number}}
    tid = ok(act(tenant.api, live.id, "transfers", body))["transfer_id"]
    phone.wait_ring()
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    error(act(live.agent.api(), live.id, f"transfers/{tid}/complete"), 409, "transfer_not_open")
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
    error(act(live.agent.api(), live.id, "transfers", body), 409, "transfer_in_progress")
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    assert [leg.state for leg in target.phone.legs] == ["ringing"]


def test_a_declined_transfer_names_its_target_and_a_new_one_can_follow(tenant):
    target = tenant.agent("Target").login()
    target.phone.on_ring("decline")
    live = connect(tenant)
    body = {"mode": "warm", "target": to_agent(target)}
    tid = ok(act(live.agent.api(), live.id, "transfers", body))["transfer_id"]
    failed = live.stream.wait_for("call.transferred", conversation_id=live.id, status="failed")
    assert (failed["data"]["transfer_id"], failed["data"]["status"]) == (tid, "failed")
    assert failed["data"]["target"] == {"type": "agent", "agent_id": target.id}
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
    failed = live.stream.wait_for("call.transferred", conversation_id=live.id, status="failed")
    assert failed["data"]["status"] == "failed" and failed["data"]["target"] == {"type": "team", "team_id": billing}
    assert len(agent.phone.legs) == 1 and live.leg.state == live.caller.state == "in_progress"


def test_a_team_transfer_rung_again_after_its_agent_hangs_up_names_the_team_when_it_fails(tenant):
    billing = team(tenant, "Billing")
    member = tenant.agent("Member", teams=[billing]).login()
    member.phone.on_ring("ignore")
    live = connect(tenant)
    ok(act(live.agent.api(), live.id, "transfers", {"mode": "warm", "target": {"type": "team", "team_id": billing}}))
    member.phone.wait_ring()
    member.phone.on_ring("decline")
    live.leg.hangup()
    member.phone.wait_ring()
    failed = live.stream.wait_for("call.transferred", conversation_id=live.id, status="failed")["data"]
    assert (failed["status"], failed["from_agent_id"]) == ("failed", live.agent.id)
    assert failed["target"] == {"type": "team", "team_id": billing}


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
    error(act(live.agent.api(), live.id, f"transfers/{tid}/complete"), 409, "transfer_not_open")
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    world.wait(lambda: live.caller.held, what="the caller held while the number rings")
    assert live.leg.state == live.caller.state == extra.state == "in_progress" and target.state == "ringing"
    assert (live.caller.held, live.leg.held, extra.held) == (True, False, False)
    error(act(tenant.api, live.id, "transfers", body), 409, "transfer_in_progress")


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
    target.phone.on_ring("ignore")
    world.wait(
        lambda: act(stuck.agent.api(), stuck.id, "transfers", body).status_code == 201,
        timeout=20,
        what="a new transfer, past the ring timeout at the latest",
    )
    world.wait(lambda: len(target.phone.legs) == 2, what="the target rung again")
    to_third = {"mode": "warm", "target": to_agent(third)}
    error(act(consult.agent.api(), consult.id, "transfers", to_third), 409, "transfer_in_progress")
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    assert third.phone.legs == [] and consult.caller.state == "in_progress"


def test_a_failed_warm_transfer_to_a_number_names_its_number_target(world, tenant):
    number = lead(world, "busy")[0]
    live = connect(tenant)
    hangup = world.plivo.fault(
        "delay",
        lambda r: (r.params.get("EventName"), r.params.get("To")) == ("Hangup", number[1:]),
        account=tenant.auth_id,
    )
    body = {"mode": "warm", "target": {"type": "number", "number": number}}
    tid = ok(act(tenant.api, live.id, "transfers", body))["transfer_id"]
    failed = live.stream.wait_for("call.transferred", conversation_id=live.id, status="failed")["data"]
    hangup.release()
    assert (failed["transfer_id"], failed["status"], failed["mode"]) == (tid, "failed", "warm")
    assert failed["target"] == {"type": "number", "number": number}


def test_a_second_participant_holds_the_caller_and_the_first_participant(world, tenant):
    live = connect(tenant)
    legs = []
    for _ in range(2):
        number, phone = lead(world)
        ok(act(tenant.api, live.id, "participants", {"target": {"type": "number", "number": number}}))
        legs.append(phone.wait_ring().wait_state("in_progress"))
        world.wait(lambda: live.caller.held, what="the first participant's ring answered before the second is added")
    world.run_jobs("plivo")
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    assert live.caller.held and legs[0].held and not legs[1].held


def first_answer_late(world, tenant, live, *, hidden: bool = False):
    """Two phone participants join while Plivo's answer to the first one's ring waits, until the second one's ring
    holds the caller. Returns their participant ids and legs, and the faults to `release`: that answer, then with
    `hidden` the first leg's own callbacks, so HS learns of the first leg only from the late answer."""
    (first, one), (second, two) = lead(world), lead(world)

    def about_the_first(r) -> bool:
        return first[1:] in (r.params.get("To"), r.params.get("ParticipantCallTo"))

    faults = [world.plivo.fault("stall", lambda r: r.params.get("to") == first, account=tenant.auth_id)]
    if hidden:
        faults.append(world.plivo.fault("delay", about_the_first, account=tenant.auth_id, times=math.inf))
    ids, legs = [], []
    for number, phone in ((first, one), (second, two)):
        target = {"target": {"type": "number", "number": number}}
        ids.append(ok(act(tenant.api, live.id, "participants", target))["participant_id"])
        legs.append(phone.wait_ring().wait_state("in_progress"))
        world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    world.wait(lambda: live.caller.held, what="the second participant's ring holding the caller")
    return ids, legs, faults


def release(world, faults) -> None:
    """Release each fault in turn, and let HS send what it does about each before the next."""
    for fault in faults:
        fault.release()
        world.run_jobs("plivo")
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")


@pytest.mark.parametrize("seen", [True, False], ids=["first-leg-seen", "first-leg-unseen"])
def test_a_first_participants_ring_answered_after_the_seconds_holds_the_first_and_not_the_second(world, tenant, seen):
    live = connect(tenant)
    _, legs, faults = first_answer_late(world, tenant, live, hidden=not seen)
    release(world, faults)
    assert live.caller.held and legs[0].held and not legs[1].held


@pytest.mark.parametrize("leaves", ["hangs_up", "removed"])
def test_a_participants_ring_answered_after_it_left_holds_only_the_caller(world, tenant, leaves):
    live = connect(tenant)
    ids, legs, faults = first_answer_late(world, tenant, live)
    if leaves == "hangs_up":
        legs[0].hangup()
    else:
        ok(tenant.api.delete(f"/api/v1/calls/{live.id}/participants/{ids[0]}"))
    live.stream.wait_for("call.participant_removed", participant_id=ids[0])
    release(world, faults)
    assert live.caller.held and not legs[1].held


def test_a_participants_ring_answered_after_the_agent_switched_to_it_keeps_it_off_hold(world, tenant):
    live = connect(tenant)
    ids, legs, faults = first_answer_late(world, tenant, live)
    world.wait(lambda: legs[0].held, what="the second participant's ring holding the first")
    ok(act(live.agent.api(), live.id, "switch", {"participant_id": ids[0]}))
    release(world, faults)
    assert (live.caller.held, legs[0].held, legs[1].held) == (True, False, True)


def test_a_participants_ring_answered_after_a_warm_transfer_completed_leaves_the_caller_off_hold(world, tenant):
    target = tenant.agent("Target").login()
    live = connect(tenant)
    number, phone = lead(world)
    stalled = world.plivo.fault("stall", lambda r: r.params.get("to") == number, account=tenant.auth_id)
    ok(act(tenant.api, live.id, "participants", {"target": {"type": "number", "number": number}}))
    phone.wait_ring().wait_state("in_progress")
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    tid = ok(act(live.agent.api(), live.id, "transfers", {"mode": "warm", "target": to_agent(target)}))["transfer_id"]
    target.phone.wait_ring().wait_state("in_progress")
    live.stream.wait_for("call.transferred", conversation_id=live.id, status="accepted")
    ok(act(live.agent.api(), live.id, f"transfers/{tid}/complete"))
    live.stream.wait_for("call.transferred", conversation_id=live.id, status="completed")
    world.wait(lambda: not live.caller.held, what="the caller off hold with the transfer's target")
    release(world, [stalled])
    assert not live.caller.held


def test_a_participants_ring_answered_after_a_warm_consult_began_holds_only_the_caller(world, tenant):
    target = tenant.agent("Target").login()
    live = connect(tenant)
    number, phone = lead(world)
    stalled = world.plivo.fault("stall", lambda r: r.params.get("to") == number, account=tenant.auth_id)
    ok(act(tenant.api, live.id, "participants", {"target": {"type": "number", "number": number}}))
    extra = phone.wait_ring().wait_state("in_progress")
    ok(act(live.agent.api(), live.id, "transfers", {"mode": "warm", "target": to_agent(target)}))
    consult = target.phone.wait_ring().wait_state("in_progress")
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    world.wait(lambda: live.caller.held, what="the transfer's ring holding the caller")
    stalled.release()
    world.run_jobs("plivo")
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    assert (live.caller.held, extra.held, consult.held) == (True, False, False)


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
    world.wait(lambda: live.caller.held, what="the caller held while the target consults")
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


def test_a_transfer_whose_ring_plivo_refuses_never_holds_the_caller(world, tenant):
    target = tenant.agent("Target").login()
    live = connect(tenant)
    world.plivo.fault(
        "reject", lambda r: r.method == "POST" and r.path.endswith("/Participant/"), account=tenant.auth_id, status=400
    )
    act(live.agent.api(), live.id, "transfers", {"mode": "cold", "target": to_agent(target)})
    live.stream.wait_for("offer.missed", conversation_id=live.id, agent_id=target.id)
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    fence(tenant)
    assert not live.caller.held and not tenant.events(type="call.held")
    assert not target.phone.legs


def test_a_transfer_that_ends_before_plivo_answers_its_ring_leaves_the_caller_held_until_the_agent_resumes(
    world, tenant
):
    target = tenant.agent("Target").login()
    target.phone.on_ring("decline")
    live = connect(tenant)
    stalled = world.plivo.fault(
        "stall",
        lambda r: r.method == "POST" and r.path.endswith("/Participant/"),
        account=tenant.auth_id,
        after=lambda r: "/call-status" in r.path and r.params.get("EventName") == "Hangup",
    )
    ok(act(live.agent.api(), live.id, "transfers", {"mode": "cold", "target": to_agent(target)}))
    target.phone.wait_ring().wait_state("completed")
    world.wait(lambda: stalled.hits and stalled.released.is_set(), what="Plivo's reply to the ring, after its end")
    world.wait(lambda: live.caller.held, what="the caller held once Plivo takes the ring")
    resumed_by_the_agent(world, live)


def test_a_resume_sent_before_plivo_answers_a_declined_transfers_ring_keeps_the_caller_off_hold(world, tenant):
    target = tenant.agent("Target").login()
    target.phone.on_ring("decline")
    live = connect(tenant)
    stalled = world.plivo.fault(
        "stall", lambda r: r.method == "POST" and r.path.endswith("/Participant/"), account=tenant.auth_id
    )
    ok(act(live.agent.api(), live.id, "transfers", {"mode": "cold", "target": to_agent(target)}))
    target.phone.wait_ring().wait_state("completed")
    live.stream.wait_for("offer.rejected", conversation_id=live.id, agent_id=target.id)
    ok(act(live.agent.api(), live.id, "hold", {"on": False}))
    stalled.release()
    world.run_jobs("plivo")
    fence(tenant)
    assert stalled.hits and not live.caller.held and not tenant.events(type="call.held")


def test_a_recording_pause_that_plivo_answers_409_is_refused_with_no_event(world, tenant):
    settings(tenant, recording={"inbound": True, "allow_pause": True})
    live = connect(tenant)
    world.plivo.reject_next("/Record/Pause/", 409, account=tenant.auth_id)
    refused = error(act(live.agent.api(), live.id, "recording", {"action": "pause"}), 422, "plivo_refused")
    assert refused["details"]["status"] == 409
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
    live.stream.wait_for("call.transferred", conversation_id=live.id, status="completed")
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    dialled = (target.phone if to == "agent" else phone).legs
    done = [e for e in emitted(tenant, "call.transferred", live.id) if e["data"]["status"] == "completed"]
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


def test_a_cold_transfer_to_a_queue_queues_the_caller_there_and_routes_on(world, tenant):
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
    assert [e["data"]["status"] for e in emitted(tenant, "call.transferred", live.id)] == ["initiated"]
    assert leg.mpc == live.caller.mpc and live.caller.state == "in_progress"


def test_a_transfer_to_voicemail_leaves_the_message_in_that_mailbox(tenant):
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
    assert [e["data"]["status"] for e in emitted(tenant, "call.transferred", live.id)] == ["initiated"]
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
        live.stream.wait_for("offer.accepted", conversation_id=live.id, agent_id=target.id)
        ok(act(live.agent.api(), live.id, f"transfers/{tid}/complete"))
    if outcome in ("completed", "failed"):
        live.stream.wait_for("call.transferred", conversation_id=live.id, status=outcome)
    world.wait(world.plivo.idle, what="the transfer settled")
    (leg if outcome == "completed" else live.leg).hangup()
    live.leg.wait_state("completed")
    if outcome == "consulting":
        live.stream.wait_for("call.transferred", conversation_id=live.id, agent_id=live.agent.id, status="completed")
        assert (live.caller.state, leg.state) == ("in_progress", "in_progress")
        leg.hangup()
    for x in (live.caller, leg):
        x.wait_state("completed")
    live.stream.wait_for("call.ended", conversation_id=live.id)
    assert len(emitted(tenant, "call.ended", live.id)) == 1


@pytest.mark.parametrize(("who", "ended_by"), [("caller", "customer"), ("agent", "agent")])
def test_an_answered_call_whose_room_end_arrives_before_the_exit_reads_who_ended_it_once(world, tenant, who, ended_by):
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
    if who == "caller":
        live.stream.wait_for("call.ended", conversation_id=live.id)
    exit_.release()
    live.stream.wait_for("call.ended", conversation_id=live.id)
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    ended = emitted(tenant, "call.ended", live.id)
    assert [(e["data"]["result"], e["data"]["ended_by"]) for e in ended] == [("answered", ended_by)]


def test_an_answered_call_whose_terminating_exit_never_comes_reads_system_after_the_close_delay(world, tenant):
    live = connect(tenant)
    world.wait(world.plivo.idle, what="the agent's join handled")
    lost = world.plivo.fault(
        "drop",
        lambda r: r.params.get("EventName") == "ParticipantExit" and r.params.get("IsMPCTerminationSource") == "true",
        account=tenant.auth_id,
        times=math.inf,
    )
    live.leg.hangup()
    world.wait(lambda: lost.hits, what="the terminating exit dropped")
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    assert not emitted(tenant, "call.ended", live.id)
    world.travel(11)
    ended = emitted(tenant, "call.ended", live.id)
    assert [(e["data"]["result"], e["data"]["ended_by"]) for e in ended] == [("answered", "system")]
