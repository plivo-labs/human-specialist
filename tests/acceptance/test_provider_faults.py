"""Plivo loses replies, repeats callbacks and delivers them late. A ring or a send happens once, a repeated or
stale callback changes nothing it should not, and a conversation outlives the room it left."""

import math
from urllib.parse import quote, urlsplit

from tests.acceptance.channels import messages, plivo_sent, reply, take, text_line
from tests.acceptance.conftest import (
    activity,
    connect,
    dial,
    disposition,
    error,
    fence,
    hs_header,
    lead,
    menu,
    ok,
    plivo_post,
    queue_action,
    run_jobs_until,
    seen,
    settings,
    unavailable,
    voicemail_action,
    wrap_up,
)


def joins_of(agent):
    return lambda r: (
        r.params.get("EventName") == "ParticipantJoin"
        and r.params.get("ParticipantCallUUID") in {leg.call_uuid for leg in agent.phone.legs}
    )


def settle(world, tenant) -> None:
    """Every callback sent and answered, every event committed so far readable, and the simulator idle again
    after whatever HS sent Plivo meanwhile."""
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    fence(tenant)
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")


def emitted(tenant, type_: str, **match) -> list[dict]:
    """Events of `type_` replayable from GET /events whose top-level fields equal `match`; after `settle`, an
    empty list means none was emitted."""
    return [e for e in tenant.events(type=type_) if all(e.get(k) == v for k, v in match.items())]


def test_a_lost_add_member_reply_rings_no_agent_twice(world, tenant):
    a, b = tenant.agent("A").login(), tenant.agent("B").login()
    for agent in (a, b):
        agent.phone.on_ring("answer", after=0.5)
    stream = tenant.stream(after=0)
    number, _ = tenant.inbound_line(a, b, agents_at_once=1, retries=1)
    lost = world.plivo.fault(
        "lose", lambda r: r.method == "POST" and r.path.rstrip("/").endswith("/Participant"), account=tenant.auth_id
    )
    caller = world.plivo.call(number)
    accepted = stream.wait_for("offer.accepted", timeout=10)
    world.advance(10)
    settle(world, tenant)
    legs = a.phone.legs + b.phone.legs
    assert lost.hits and len(legs) == 1, f"{len(legs)} legs rang for one ring"
    assert accepted["agent_id"] == (a if a.phone.legs else b).id
    assert legs[0].state == caller.state == "in_progress" and legs[0].mpc == caller.mpc


def test_repeated_ring_and_room_callbacks_change_nothing_twice(world, tenant):
    first, second = tenant.agent("First").login(), tenant.agent("Second").login()
    first.phone.on_ring("decline")
    twice = world.plivo.fault(
        "duplicate",
        lambda r: "/call-status" in r.path or "/mpc-status" in r.path,
        account=tenant.auth_id,
        times=math.inf,
    )
    live = connect(tenant, second, also=[first], agents_at_once=1, retries=1)
    live.caller.hangup()
    live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id)
    settle(world, tenant)
    assert twice.hits and (len(first.phone.legs), len(second.phone.legs)) == (1, 1)
    for type_ in ("offer.rejected", "offer.accepted", "conversation.wrap_up_started"):
        assert len(emitted(tenant, type_, conversation_id=live.id)) == 1, type_


def test_a_failed_add_member_misses_its_offer_when_its_call_status_is_lost(world, tenant):
    first, second = tenant.agent("First").login(), tenant.agent("Second").login()
    first.phone.on_ring("decline")
    stream = tenant.stream(after=0)
    number, _ = tenant.inbound_line(first, second, agents_at_once=1, retries=1)
    lost = world.plivo.fault("drop", lambda r: "/call-status" in r.path, account=tenant.auth_id, times=math.inf)
    world.plivo.call(number)
    stream.wait_for("offer.missed", agent_id=first.id)
    second.phone.wait_ring()
    assert lost.hits


def test_a_member_whose_join_was_lost_is_kicked_by_the_room_check(world, tenant):
    settings(tenant, ring_timeout_sec=5)
    first, second = tenant.agent("First").login(), tenant.agent("Second").login()
    stream = tenant.stream(after=0)
    number, _ = tenant.inbound_line(first, second, agents_at_once=1, retries=1, wait_timeout_sec=600)
    lost = world.plivo.fault("drop", joins_of(first), account=tenant.auth_id)
    caller = world.plivo.call(number)
    stale = first.phone.wait_ring().wait_state("in_progress")
    world.wait(lambda: stale.mpc, what="the first leg in the room, its join lost")
    world.advance(10)
    stream.wait_for("offer.missed", agent_id=first.id)
    fresh = second.phone.wait_ring().wait_state("in_progress")
    stream.wait_for("offer.accepted", agent_id=second.id)
    world.advance(40)
    stale.wait_state("completed")
    assert lost.hits and caller.state == fresh.state == "in_progress" and fresh.mpc == caller.mpc


def test_a_room_whose_end_was_lost_lets_its_conversation_close(world, tenant):
    settings(tenant, ring_timeout_sec=5, wrap_up={"mode": "manual", "seconds": 300})
    live = connect(tenant)
    caller = live.caller.call_uuid
    lost = world.plivo.fault(
        "drop",
        lambda r: (
            r.path.endswith("/hangup")
            or r.params.get("EventName") == "MPCEnd"
            or (r.params.get("EventName") == "ParticipantExit" and r.params.get("ParticipantCallUUID") == caller)
        ),
        account=tenant.auth_id,
        times=math.inf,
    )
    live.caller.hangup()
    live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id)
    world.advance(40)
    ok(wrap_up(live.agent, live.id, disposition(tenant)["id"]))
    live.stream.wait_for("conversation.closed", conversation_id=live.id)
    assert lost.hits


def caller_end(r) -> bool:
    return r.path.endswith("/hangup") or r.params.get("EventName") in ("ParticipantExit", "MPCEnd")


def test_a_queued_caller_whose_end_was_lost_is_abandoned_at_its_rings_room_check(world, tenant):
    agent = tenant.agent().login()
    agent.phone.unregister()
    number, _ = tenant.inbound_line(agent, wait_timeout_sec=600)
    stream = tenant.stream(after=0)
    lost = world.plivo.fault("drop", caller_end, account=tenant.auth_id, times=math.inf)
    caller = world.plivo.call(number)
    cid = stream.wait_for("offer.missed", agent_id=agent.id)["conversation_id"]
    caller.hangup()
    caller.wait_state("completed")
    world.run_jobs("missed_mpc_event")
    assert stream.wait_for("conversation.closed", conversation_id=cid)["data"]["result"] == "abandoned"
    assert lost.hits


def test_a_queued_caller_whose_end_was_lost_and_who_never_rang_ends_at_its_wait_timeout(world, tenant):
    number, _ = tenant.inbound_line(unavailable(tenant), wait_timeout_sec=600)
    stream = tenant.stream(after=0)
    lost = world.plivo.fault("drop", caller_end, account=tenant.auth_id, times=math.inf)
    caller = world.plivo.call(number)
    cid = stream.wait_for("conversation.queued")["conversation_id"]
    caller.hangup()
    caller.wait_state("completed")
    world.advance(540)
    assert ok(tenant.api.get(f"/api/v1/conversations/{cid}"))["state"] == "active"
    world.advance(660)
    assert stream.wait_for("conversation.closed", conversation_id=cid)["data"]["result"] == "queue_timeout"
    assert lost.hits


def test_an_agent_hangs_up_a_call_that_ended_while_its_callbacks_were_lost(world, tenant):
    settings(tenant, ring_timeout_sec=5, wrap_up={"mode": "manual", "seconds": 300})
    live = connect(tenant)
    world.advance(40)
    lost = world.plivo.fault("drop", lambda r: True, account=tenant.auth_id, times=math.inf)
    live.caller.hangup()
    settle(world, tenant)
    assert ok(tenant.api.get(f"/api/v1/conversations/{live.id}"))["state"] == "active"
    assert activity(tenant, live.agent) == "on_call"
    ok(live.agent.api().post(f"/api/v1/calls/{live.id}/hangup"))
    live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id)
    ok(wrap_up(live.agent, live.id, disposition(tenant)["id"]))
    live.stream.wait_for("conversation.closed", conversation_id=live.id)
    world.wait(lambda: activity(tenant, live.agent) == "idle", what="the agent free again")
    assert lost.hits


def test_a_lost_send_reply_sends_the_message_once(world, tenant):
    agent, customer = tenant.agent().login(), "+14155550301"
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, agent)
    world.plivo.text(number, customer, "Hi")
    cid = take(stream, agent)
    lost = world.plivo.fault("lose", lambda r: r.method == "POST" and "/Message" in r.path, account=tenant.auth_id)
    sent = reply(agent, cid, "On its way")
    stream.wait_for("message.status_changed", conversation_id=cid, message_id=sent["id"], status="delivered")
    world.advance(10)
    assert lost.hits and [m.text for m in plivo_sent(world, number, customer)].count("On its way") == 1
    assert [m["status"] for m in messages(agent.api(), cid) if m["text"] == "On its way"] == ["delivered"]


def test_a_send_plivo_never_took_or_reported_fails_once_checked(world, tenant):
    agent, customer = tenant.agent().login(), "+14155550302"
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, agent)
    world.plivo.text(number, customer, "Hi")
    cid = take(stream, agent)
    lost = world.plivo.fault("reject", lambda r: r.method == "POST" and "/Message" in r.path, account=tenant.auth_id)
    sent = reply(agent, cid, "On its way")
    failed = {"message_id": sent["id"], "status": "failed"}
    run_jobs_until(world, lambda: seen(stream, "message.status_changed", **failed), "the unheard send to fail")
    assert lost.hits and not plivo_sent(world, number, customer)
    assert [m["status"] for m in messages(agent.api(), cid) if m["text"] == "On its way"] == ["failed"]


def test_a_duplicate_join_keeps_the_agent(world, tenant):
    agent = tenant.agent().login()
    twice = world.plivo.fault("duplicate", joins_of(agent), account=tenant.auth_id)
    live = connect(tenant, agent)
    settle(world, tenant)
    assert twice.hits and live.leg.state == live.caller.state == "in_progress" and live.leg.mpc == live.caller.mpc
    assert len(emitted(tenant, "offer.accepted", conversation_id=live.id)) == 1


def test_a_stale_join_after_a_re_offer_is_kicked(world, tenant):
    settings(tenant, ring_timeout_sec=5)
    first, second = tenant.agent("First").login(), tenant.agent("Second").login()
    stream = tenant.stream(after=0)
    number, _ = tenant.inbound_line(first, second, agents_at_once=1, retries=1, wait_timeout_sec=600)
    late = world.plivo.fault("delay", joins_of(first), account=tenant.auth_id)
    caller = world.plivo.call(number)
    stale = first.phone.wait_ring().wait_state("in_progress")
    world.wait(lambda: stale.mpc, what="the first leg in the room, its join not yet sent")
    world.advance(10)
    stream.wait_for("offer.missed", agent_id=first.id)
    fresh = second.phone.wait_ring().wait_state("in_progress")
    stream.wait_for("offer.accepted", agent_id=second.id)
    late.release()
    stale.wait_state("completed")
    settle(world, tenant)
    assert late.hits and caller.state == fresh.state == "in_progress" and fresh.mpc == caller.mpc
    assert not emitted(tenant, "offer.accepted", agent_id=first.id)


def test_a_leg_rung_for_an_offer_withdrawn_in_flight_ends_though_its_first_hangup_found_no_call(world, tenant):
    agent = tenant.agent().login()
    agent.phone.on_ring("ignore")
    stream = tenant.stream(after=0)
    number, _ = tenant.inbound_line(agent, wait_timeout_sec=600)
    ring = world.plivo.fault(
        "stall", lambda r: r.method == "POST" and r.path.rstrip("/").endswith("/Participant"), account=tenant.auth_id
    )
    status = world.plivo.fault("delay", lambda r: "/call-status" in r.path, account=tenant.auth_id, times=math.inf)
    caller = world.plivo.call(number)
    orphan = agent.phone.wait_ring()
    caller.hangup()
    stream.wait_for("offer.withdrawn", agent_id=agent.id)
    unborn = world.plivo.fault(
        "reject", lambda r: r.method == "DELETE" and orphan.call_uuid in r.path, account=tenant.auth_id, status=404
    )
    ring.release()
    world.wait(lambda: unborn.hits, what="a hangup sent before Plivo created the leg")
    orphan.answer()
    world.wait(lambda: orphan.mpc, what="the withdrawn offer's leg in a room")
    settle(world, tenant)
    status.release()
    orphan.wait_state("completed")
    agent.phone.on_ring("answer")
    world.plivo.call(number)
    fresh = agent.phone.wait_ring().wait_state("in_progress")
    stream.wait_for("offer.accepted", agent_id=agent.id)
    settle(world, tenant)
    assert [leg for leg in agent.phone.legs if leg.state != "completed"] == [fresh]


def test_a_move_to_a_menu_does_not_close_the_conversation(world, tenant):
    agent = tenant.agent().login()
    sales = tenant.queue(name="Sales", agents=[agent.id])
    m = menu(tenant, {"1": queue_action(sales)}, greeting="Press 1 for sales")
    to_menu = {"9": {"type": "menu", "menu_id": m["id"]}}
    waiting = tenant.queue(name="Waiting", agents=[unavailable(tenant).id], dtmf_actions=to_menu, wait_timeout_sec=600)
    number = tenant.number()
    tenant.route_number(number, queue_action(waiting))
    stream = tenant.stream(after=0)
    caller = world.plivo.call(number)
    cid = stream.wait_for("conversation.queued", queue_id=waiting["id"])["conversation_id"]
    room = world.wait(lambda: caller.mpc, what="the caller in the queue's room")
    left = ("ParticipantExit", "MPCEnd")
    late = world.plivo.fault(
        "delay",
        lambda r: r.params.get("MPCName") == room and r.params.get("EventName") in left,
        account=tenant.auth_id,
        times=2,
    )
    caller.press("9")
    world.wait(lambda: "Press 1 for sales" in caller.said, what="the menu greeting")
    caller.press("1")
    leg = agent.phone.wait_ring()
    assert hs_header(leg, "HsConversation") == cid
    stream.wait_for("offer.accepted", conversation_id=cid)
    late.release()
    settle(world, tenant)
    assert len(late.hits) == 2 and caller.state == leg.state == "in_progress" and leg.mpc == caller.mpc
    assert not emitted(tenant, "call.ended", conversation_id=cid)
    assert not emitted(tenant, "conversation.closed", conversation_id=cid)


def test_a_recording_delivered_after_a_move_is_kept(world, tenant):
    settings(tenant, recording={"inbound": True})
    live = connect(tenant)
    late = world.plivo.fault(
        "delay", lambda r: r.params.get("EventName") == "MPCRecordingCompleted", account=tenant.auth_id
    )
    target = {"mode": "cold", "target": voicemail_action(tenant.queue(name="Mailbox"))}
    ok(live.agent.api().post(f"/api/v1/calls/{live.id}/transfers", json=target))
    voicemail = live.stream.wait_for("voicemail.received", conversation_id=live.id, timeout=10)["data"]

    def call_recordings():
        ready = emitted(tenant, "recording.ready", conversation_id=live.id)
        return [e["data"]["recording_id"] for e in ready if e["data"]["recording_id"] != voicemail["recording_id"]]

    settle(world, tenant)
    assert late.hits and not call_recordings(), "the call's recording was stored before its callback arrived"
    late.release()
    [rid] = world.wait(call_recordings, what="recording.ready for the room the caller left")
    rows = ok(tenant.api.get("/api/v1/recordings", params={"conversation_id": live.id}))["data"]
    assert rid in [r["id"] for r in rows]


def test_a_warm_transfer_completes_when_the_primary_hangs_up(world, tenant):
    target = tenant.agent("Target").login()
    live = connect(tenant)
    exits = world.plivo.fault(
        "duplicate",
        lambda r: (
            r.params.get("EventName") == "ParticipantExit" and r.params.get("ParticipantCallUUID") == live.leg.call_uuid
        ),
        account=tenant.auth_id,
    )
    body = {"mode": "warm", "target": {"type": "agent", "agent_id": target.id}}
    tid = ok(live.agent.api().post(f"/api/v1/calls/{live.id}/transfers", json=body))["transfer_id"]
    leg = target.phone.wait_ring().wait_state("in_progress")
    world.wait(lambda: leg.mpc == live.caller.mpc, what="the consult in the caller's room")
    world.wait(world.plivo.idle, what="the consult's join answered")
    live.leg.hangup()
    done = live.stream.wait_for("call.transferred", conversation_id=live.id, status="completed")
    settle(world, tenant)
    assert exits.hits and (done["data"]["transfer_id"], done["data"]["mode"]) == (tid, "warm")
    assert live.caller.state == leg.state == "in_progress" and leg.mpc == live.caller.mpc
    assert not emitted(tenant, "conversation.closed", conversation_id=live.id)


def test_a_repeated_recording_or_transcript_callback_keeps_one_recording_announced_once(world, tenant):
    settings(tenant, recording={"inbound": True})
    live = connect(tenant)
    media = ("/recording", "/transcription")
    twice = world.plivo.fault(
        "duplicate", lambda r: urlsplit(r.path).path.endswith(media), account=tenant.auth_id, times=math.inf
    )
    live.caller.hangup()
    rid = live.stream.wait_for("recording.ready", conversation_id=live.id)["data"]["recording_id"]
    run_jobs_until(world, lambda: seen(live.stream, "transcript.ready", recording_id=rid), "transcript.ready")
    settle(world, tenant)
    assert twice.hits
    assert [e["data"]["recording_id"] for e in emitted(tenant, "recording.ready", conversation_id=live.id)] == [rid]
    assert len(emitted(tenant, "transcript.ready", conversation_id=live.id)) == 1
    rows = ok(tenant.api.get("/api/v1/recordings", params={"conversation_id": live.id}))["data"]
    assert [r["id"] for r in rows] == [rid]


def test_a_transcript_that_comes_before_its_recording_is_fetched_once_the_recording_lands(world, tenant):
    settings(tenant, recording={"inbound": True})
    live = connect(tenant)
    late = world.plivo.fault(
        "delay",
        lambda r: r.params.get("EventName", "").endswith("RecordingCompleted"),
        account=tenant.auth_id,
        times=math.inf,
    )
    live.caller.hangup()
    settle(world, tenant)
    assert late.hits and not emitted(tenant, "recording.ready", conversation_id=live.id)
    late.release()
    rid = live.stream.wait_for("recording.ready", conversation_id=live.id)["data"]["recording_id"]
    run_jobs_until(world, lambda: seen(live.stream, "transcript.ready", recording_id=rid), "transcript.ready")
    got = ok(tenant.api.get(f"/api/v1/recordings/{rid}/transcript"))
    assert got["complete"] is True and world.plivo.transcript in [s["text"] for s in got["segments"]]


def test_another_tenants_callback_naming_this_recording_leaves_its_transcript_fetch_alone(world, tenant):
    settings(tenant, recording={"inbound": True})
    live, other = connect(tenant), world.new_tenant("Other")
    theirs = connect(other)
    late = world.plivo.fault(
        "delay",
        lambda r: r.params.get("EventName", "").endswith("RecordingCompleted"),
        account=tenant.auth_id,
        times=math.inf,
    )
    live.caller.hangup()
    settle(world, tenant)
    track = next(
        r.id for r in world.plivo.recordings.values() if r.account.auth_id == tenant.auth_id and r.kind == "participant"
    )
    forged = {"recording_id": track, "transcription": ""}
    assert plivo_post(world, other.id, f"transcription?c={theirs.id}", forged, other.auth_token).status_code == 200
    late.release()
    rid = live.stream.wait_for("recording.ready", conversation_id=live.id)["data"]["recording_id"]
    run_jobs_until(world, lambda: seen(live.stream, "transcript.ready", recording_id=rid), "transcript.ready")


def test_a_direct_plivo_call_that_gets_no_answer_is_a_503(world, tenant):
    number = tenant.number()
    down = world.plivo.fault(
        "reject", lambda r: "/Number/" in r.path, account=tenant.auth_id, status=502, times=math.inf
    )
    put = tenant.api.put(f"/api/v1/numbers/{quote(number)}", json={"open_action": {"type": "hangup"}})
    error(put, 503, "plivo_unavailable")
    assert down.hits


def test_a_plivo_read_that_answers_a_502_or_503_is_retried_twice(world, tenant):
    number = tenant.number()

    def read(r) -> bool:
        return r.method == "GET" and "/Number/" in r.path

    blip = world.plivo.fault("reject", read, account=tenant.auth_id, status=502)
    world.plivo.fault("reject", read, account=tenant.auth_id, status=503)
    ok(tenant.api.put(f"/api/v1/numbers/{quote(number)}", json={"open_action": {"type": "hangup"}}))
    assert blip.hits


def lead_status(event: str | None = None):
    """The lead's call statuses (only `event`, if given); the agent leg's Make Call hangup shares /hangup without
    an EventName."""

    def match(r) -> bool:
        name = r.params.get("EventName")
        return urlsplit(r.path).path.endswith("/hangup") and name is not None and event in (None, name)

    return match


def adding_the_lead(r) -> bool:
    return r.method == "POST" and r.path.rstrip("/").endswith("/Participant")


def test_a_lead_that_joins_before_hs_knows_its_leg_stays_on_the_call(world, tenant):
    number, phone = lead(world)
    stream = tenant.stream(after=0)
    lost = world.plivo.fault("lose", adding_the_lead, account=tenant.auth_id)
    late = world.plivo.fault("delay", lead_status(), account=tenant.auth_id, times=math.inf)
    _, cid = dial(tenant, to=number)
    leg = world.wait(lambda: phone.legs and phone.legs[0].mpc and phone.legs[0], what="the lead in the room")
    settle(world, tenant)
    late.release()
    stream.wait_for("call.answered", conversation_id=cid)
    settle(world, tenant)
    assert lost.hits and late.hits and leg.state == "in_progress"


def test_a_lead_hs_learns_of_only_after_its_agent_hung_up_is_hung_up(world, tenant):
    agent = tenant.agent().login()
    number, phone = lead(world, "no_answer")
    stream = tenant.stream(after=0)
    world.plivo.fault("lose", adding_the_lead, account=tenant.auth_id)
    late = world.plivo.fault("delay", lead_status(), account=tenant.auth_id, times=math.inf)
    _, cid = dial(tenant, agent, to=number)
    ringing = world.wait(lambda: phone.legs and phone.legs[0], what="the lead to ring")
    agent.phone.legs[0].hangup()
    assert stream.wait_for("call.failed", conversation_id=cid)["data"]["result"] == "cancelled"
    late.release()
    world.wait(
        lambda: any(r.method == "DELETE" and ringing.call_uuid in r.path for r in world.plivo.requests),
        what="the lead hung up",
    )


def test_a_leads_hangup_that_beats_its_answered_status_ends_an_answered_call(world, tenant):
    number, phone = lead(world)
    stream = tenant.stream(after=0)
    late = world.plivo.fault("delay", lead_status("Answered"), account=tenant.auth_id, after=lead_status("Hangup"))
    _, cid = dial(tenant, to=number)
    leg = world.wait(lambda: phone.legs and phone.legs[0].mpc and phone.legs[0], what="the lead in the room")
    leg.hangup()
    assert stream.wait_for("call.ended", conversation_id=cid)["data"]["result"] == "answered"
    settle(world, tenant)
    assert late.hits and not emitted(tenant, "call.failed", conversation_id=cid)


def test_a_dial_whose_reply_and_end_were_lost_fails_once_its_lead_stopped_ringing(world, tenant):
    number, phone = lead(world, "no_answer")
    stream = tenant.stream(after=0)
    world.plivo.fault("lose", adding_the_lead, account=tenant.auth_id)
    gone = world.plivo.fault("drop", lead_status("Hangup"), account=tenant.auth_id)
    _, cid = dial(tenant, to=number, ring_timeout_sec=5)
    world.wait(lambda: phone.legs and phone.legs[0].state == "completed", what="the lead to stop ringing")
    settle(world, tenant)
    assert gone.hits and not emitted(tenant, "call.failed", conversation_id=cid)
    run_jobs_until(world, lambda: seen(stream, "call.failed", conversation_id=cid), "call.failed")


def test_a_dial_whose_lead_never_answered_and_whose_end_was_lost_fails_once_checked(world, tenant):
    number, phone = lead(world, "no_answer")
    stream = tenant.stream(after=0)
    gone = world.plivo.fault("drop", lead_status("Hangup"), account=tenant.auth_id)
    _, cid = dial(tenant, to=number, ring_timeout_sec=5)
    world.wait(lambda: phone.legs and phone.legs[0].state == "completed", what="the lead to stop ringing")
    settle(world, tenant)
    assert gone.hits and not emitted(tenant, "call.failed", conversation_id=cid)
    run_jobs_until(world, lambda: seen(stream, "call.failed", conversation_id=cid), "call.failed")


def test_a_dial_whose_agent_never_answered_and_whose_end_was_lost_frees_the_agent_once_checked(world, tenant):
    agent = tenant.agent().login()
    agent.phone.on_ring("ignore")
    stream = tenant.stream(after=0)
    gone = world.plivo.fault(
        "drop",
        lambda r: urlsplit(r.path).path.endswith("/hangup") and r.params.get("Event") == "Hangup",
        account=tenant.auth_id,
    )
    _, cid = dial(tenant, agent)
    agent.phone.wait_ring().wait_state("completed")
    settle(world, tenant)
    assert gone.hits and not emitted(tenant, "call.failed", conversation_id=cid)
    run_jobs_until(world, lambda: seen(stream, "call.failed", conversation_id=cid), "call.failed")
    assert activity(tenant, agent) == "idle"


def test_a_track_that_comes_after_its_calls_last_recording_was_deleted_is_deleted_at_plivo_too(world, tenant):
    settings(tenant, recording={"inbound": True})
    late = world.plivo.fault(
        "delay",
        lambda r: r.params.get("EventName") == "ParticipantRecordingCompleted",
        account=tenant.auth_id,
        times=math.inf,
    )
    live = connect(tenant)
    live.caller.hangup()
    rid = live.stream.wait_for("recording.ready", conversation_id=live.id)["data"]["recording_id"]
    ok(tenant.api.delete(f"/api/v1/recordings/{rid}"))
    settle(world, tenant)
    late.release()
    mine = [r for r in world.plivo.recordings.values() if r.account.auth_id == tenant.auth_id]
    tracks = [r for r in mine if r.kind == "participant"]
    world.wait(lambda: all(r.deleted for r in tracks), what="the late tracks deleted at Plivo")
    assert late.hits and tracks


def test_a_lead_bound_late_after_a_transfer_made_the_call_live_stays_on_it(world, tenant):
    target = tenant.agent("Target").login()
    number, phone = lead(world)
    stream = tenant.stream(after=0)
    world.plivo.fault("lose", adding_the_lead, account=tenant.auth_id)
    late = world.plivo.fault("delay", lead_status(), account=tenant.auth_id, times=math.inf)
    agent, cid = dial(tenant, to=number)
    customer = world.wait(lambda: phone.legs and phone.legs[0].mpc and phone.legs[0], what="the lead in the room")
    to_target = {"mode": "cold", "target": {"type": "agent", "agent_id": target.id}}
    ok(agent.api().post(f"/api/v1/calls/{cid}/transfers", json=to_target))
    target.phone.wait_ring().wait_state("in_progress")
    stream.wait_for("call.transferred", conversation_id=cid, status="completed")
    late.release()
    settle(world, tenant)
    assert customer.state == "in_progress"
    assert not any(r.method == "DELETE" and customer.call_uuid in r.path for r in world.plivo.requests)


def test_a_leads_room_exit_before_its_answered_status_ends_an_answered_call(world, tenant):
    number, phone = lead(world)
    stream = tenant.stream(after=0)
    answered, ended = lead_status("Answered"), lead_status("Hangup")
    late = world.plivo.fault("delay", lambda r: answered(r) or ended(r), account=tenant.auth_id, times=math.inf)
    _, cid = dial(tenant, to=number)
    leg = world.wait(lambda: phone.legs and phone.legs[0].mpc and phone.legs[0], what="the lead in the room")
    settle(world, tenant)
    leg.hangup()
    settle(world, tenant)
    late.release()
    assert stream.wait_for("call.ended", conversation_id=cid)["data"]["result"] == "answered"


def test_an_agent_leaving_the_room_before_the_lead_answers_cancels_the_call_without_wrap_up(world, tenant):
    agent = tenant.agent().login()
    number, phone = lead(world, "no_answer")
    stream = tenant.stream(after=0)
    late = world.plivo.fault(
        "delay",
        lambda r: (
            urlsplit(r.path).path.endswith("/hangup") and "Hangup" in (r.params.get("Event"), r.params.get("EventName"))
        ),
        account=tenant.auth_id,
        times=math.inf,
    )
    _, cid = dial(tenant, agent, to=number)
    ringing = world.wait(lambda: phone.legs and phone.legs[0], what="the lead to ring")
    agent.phone.legs[0].hangup()
    assert stream.wait_for("call.failed", conversation_id=cid)["data"]["result"] == "cancelled"
    ringing.wait_state("completed")
    late.release()
    settle(world, tenant)
    assert late.hits and not emitted(tenant, "conversation.wrap_up_started", conversation_id=cid)


def test_an_uncertain_dial_whose_agent_answered_leaves_its_lead_to_its_own_end(world, tenant):
    agent = tenant.agent().login()
    number, _ = lead(world, "no_answer")
    stream = tenant.stream(after=0)
    lost = world.plivo.fault(
        "lose", lambda r: r.method == "POST" and r.path.rstrip("/").endswith("/Call"), account=tenant.auth_id
    )
    late = world.plivo.fault("delay", lead_status("Hangup"), account=tenant.auth_id)
    _, cid = dial(tenant, agent, to=number)
    world.wait(lambda: late.hits, what="the lead to stop ringing")
    world.advance(60)
    settle(world, tenant)
    assert lost.hits and not emitted(tenant, "call.failed", conversation_id=cid)
    late.release()
    assert stream.wait_for("call.failed", conversation_id=cid)["data"]["result"] == "no_answer"


def test_a_late_tracks_refused_erase_is_retried_by_deleting_its_recording_again(world, tenant):
    settings(tenant, recording={"inbound": True})
    late = world.plivo.fault(
        "delay",
        lambda r: r.params.get("EventName") == "ParticipantRecordingCompleted",
        account=tenant.auth_id,
        times=math.inf,
    )
    live = connect(tenant)
    live.caller.hangup()
    rid = live.stream.wait_for("recording.ready", conversation_id=live.id)["data"]["recording_id"]
    ok(tenant.api.delete(f"/api/v1/recordings/{rid}"))
    refused = world.plivo.fault(
        "reject",
        lambda r: r.method == "DELETE" and "/Recording/" in r.path,
        account=tenant.auth_id,
        status=400,
        times=2,
    )
    settle(world, tenant)
    late.release()
    world.wait(lambda: len(refused.hits) == 2, what="the late tracks' erases refused")
    settle(world, tenant)
    tracks = [
        r for r in world.plivo.recordings.values() if r.account.auth_id == tenant.auth_id and r.kind == "participant"
    ]
    assert tracks and not any(r.deleted for r in tracks)
    ok(tenant.api.delete(f"/api/v1/recordings/{rid}"))
    assert all(r.deleted for r in tracks)
