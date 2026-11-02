"""Inbound routing as configuration: number entry point, hours, actions, queues, offers and overflow."""

import random
import xml.etree.ElementTree as ET

import pytest

from tests.acceptance.conftest import (
    MUSIC,
    answer_params,
    connect,
    disposition,
    error,
    fence,
    finish,
    hangup_action,
    hours,
    hs_header,
    lead,
    ok,
    plivo_post,
    queue_action,
    refused,
    run_jobs_until,
    say,
    seen,
    set_status,
    settings,
    skill,
    status_changes,
    unavailable,
    voicemail_action,
    wrap_up,
)

OUTCOMES = ("offer.accepted", "offer.missed", "offer.rejected", "offer.withdrawn")


def line(tenant, open_action: dict, **fields) -> str:
    number = tenant.number()
    tenant.route_number(number, open_action, **fields)
    return number


def outcomes(stream, tenant, agent, leg) -> list[dict]:
    """Every outcome of the offer `leg` carried to `agent`, once one has arrived and every earlier event is replayable."""
    offer = hs_header(leg, "HsOffer")

    def mine(events):
        return [
            e for e in events if e["type"] in OUTCOMES and (e["agent_id"], e["data"]["offer_id"]) == (agent.id, offer)
        ]

    tenant.world.wait(lambda: mine(stream.events), what=f"an outcome of offer {offer}")
    fence(tenant)
    return mine(tenant.events())


def closed_line(tenant, closed_action: dict, **hours_fields) -> str:
    q = tenant.queue(name="Q", agents=[tenant.agent().login().id])
    h = hours(tenant, **({"weekly": False} | hours_fields))
    return line(tenant, queue_action(q), business_hours_id=h["id"], closed_action=closed_action)


def test_an_open_entry_point_routes_into_its_queue(world, tenant):
    agent = tenant.agent().login()
    q = tenant.queue(name="Sales", agents=[agent.id])
    number = line(tenant, queue_action(q), business_hours_id=hours(tenant)["id"], closed_action=hangup_action("Closed"))
    stream = tenant.stream(after=0)
    world.plivo.call(number, from_="+14155550142")
    started = stream.wait_for("conversation.started")
    assert (started["data"]["hours"], started["data"]["number"], started["direction"]) == ("open", number, "inbound")
    assert started["data"]["entry_point_id"]
    queued = stream.wait_for("conversation.queued", conversation_id=started["conversation_id"])
    assert (queued["data"]["queue_id"], queued["data"]["from"], queued["data"]["to"]) == (
        q["id"],
        "+14155550142",
        number,
    )
    assert hs_header(agent.phone.wait_ring(), "HsConversation") == started["conversation_id"]


def test_closed_hours_render_the_closed_action(world, tenant):
    number = closed_line(tenant, hangup_action("We are closed"))
    stream = tenant.stream(after=0)
    caller = world.plivo.call(number)
    caller.wait_state("completed")
    assert "We are closed" in caller.said
    assert stream.wait_for("conversation.started")["data"]["hours"] == "closed"
    assert stream.wait_for("conversation.after_hours")["data"]["number"] == number
    assert not seen(stream, "conversation.queued")


def test_a_holiday_closes_an_open_weekday(world, tenant):
    caller = world.plivo.call(closed_line(tenant, hangup_action("Happy holidays"), weekly=True, holiday=[]))
    caller.wait_state("completed")
    assert "Happy holidays" in caller.said


def test_business_hours_status_follows_weekly_and_holiday_slots(tenant):
    cases = [
        (hours(tenant), True),
        (hours(tenant, weekly=False), False),
        (hours(tenant, weekly=True, holiday=[]), False),
        (hours(tenant, weekly=False, holiday=[{"start": "08:00", "end": "16:00"}]), True),
        (hours(tenant, weekly=True, holiday=[{"start": "00:00", "end": "01:00"}]), False),
    ]
    got = [ok(tenant.api.get(f"/api/v1/business-hours/{h['id']}/status"))["open"] for h, _ in cases]
    assert got == [expected for _, expected in cases]


def test_after_hours_voicemail_takes_a_message(world, tenant):
    q = tenant.queue(name="Support")
    number = line(
        tenant, queue_action(q), business_hours_id=hours(tenant, weekly=False)["id"], closed_action=voicemail_action(q)
    )
    stream = tenant.stream(after=0)
    caller = world.plivo.call(number, from_="+14155550150")
    vm = stream.wait_for("voicemail.received", timeout=10)
    assert "Leave a message" in caller.said and vm["data"]["recording_id"]
    assert (vm["data"]["mailbox"]["queue_id"], vm["data"]["from"]) == (q["id"], "+14155550150")


def test_an_agents_voicemail_reaches_that_agents_stream(world, tenant):
    q = tenant.queue(name="Support")
    me = tenant.agent("Owner").login()
    action = voicemail_action(q) | {"mailbox": {"agent_id": me.id}}
    number = line(tenant, queue_action(q), business_hours_id=hours(tenant, weekly=False)["id"], closed_action=action)
    other = tenant.agent("Other").login()
    stream, unrelated = tenant.stream(token=me.stream_token, after=0), tenant.stream(token=other.stream_token, after=0)
    world.plivo.call(number, from_="+14155550150")
    vm = stream.wait_for("voicemail.received", timeout=10)
    assert vm["data"]["mailbox"]["agent_id"] == me.id
    status_changes(tenant, other, "Away")
    unrelated.wait_for("agent.status_changed", timeout=10)
    assert not seen(unrelated, "voicemail.received")


def test_after_hours_forward_dials_the_number(world, tenant):
    target, phone = lead(world)
    world.plivo.call(closed_line(tenant, {"type": "forward", "number": target}))
    assert phone.wait_ring().to == target


def test_a_wait_timeout_overflows_to_the_next_queue(world, tenant):
    backup = tenant.agent("Backup").login()
    second = tenant.queue(name="Team", agents=[backup.id])
    first = tenant.queue(
        name="Owner", agents=[unavailable(tenant).id], wait_timeout_sec=1, overflow_action=queue_action(second)
    )
    stream = tenant.stream(after=0)
    world.plivo.call(line(tenant, queue_action(first)))
    over = stream.wait_for("conversation.overflowed", timeout=10)
    assert (over["data"]["reason"], over["data"]["queue_id"]) == ("wait_timeout", first["id"])
    assert hs_header(backup.phone.wait_ring(), "HsConversation") == over["conversation_id"]


def test_a_wait_timeout_can_overflow_to_a_transcribed_voicemail(world, tenant):
    q = tenant.queue(name="Q", agents=[unavailable(tenant).id], wait_timeout_sec=1)
    vm = voicemail_action(q, "Nobody is free", transcribe=True)
    ok(tenant.api.patch(f"/api/v1/queues/{q['id']}", json={"overflow_action": vm}))
    stream = tenant.stream(after=0)
    caller = world.plivo.call(line(tenant, queue_action(q)))
    received = stream.wait_for("voicemail.received", timeout=10)
    assert "Nobody is free" in caller.said and received["data"]["mailbox"]["queue_id"] == q["id"]
    assert stream.wait_for("conversation.overflowed")["data"]["reason"] == "wait_timeout"
    stream.wait_for("transcript.ready", recording_id=received["data"]["recording_id"])
    ok(tenant.api.get(f"/api/v1/recordings/{received['data']['recording_id']}/transcript"))


@pytest.mark.parametrize(
    ("leaves", "result", "ended_by"),
    [
        ("hangs_up", "abandoned", "customer"),
        ("waits_out", "queue_timeout", "system"),
        ("waits_out_api_down", "queue_timeout", "system"),
    ],
)
def test_a_caller_who_leaves_a_queue_unanswered_ends_once_with_the_true_cause(world, tenant, leaves, result, ended_by):
    waits = leaves.startswith("waits_out")
    q = tenant.queue(name="Q", agents=[unavailable(tenant).id], wait_timeout_sec=1 if waits else 600)
    world.plivo.exit_after_events = True  # the room's end reaches HS before the queue exit, as it can live
    stream = tenant.stream(after=0)
    caller = world.plivo.call(line(tenant, queue_action(q)))
    if leaves == "waits_out_api_down":  # the call's end is read from Plivo's callbacks, never its call API
        world.plivo.fault("lose", lambda r: caller.call_uuid in r.path, account=tenant.auth_id, times=10)
    stream.wait_for("conversation.queued")
    if leaves == "hangs_up":
        caller.hangup()
    caller.wait_state("completed", timeout=10)
    closed = run_jobs_until(world, lambda: seen(stream, "conversation.closed"), "the conversation closed")[0]
    world.run_jobs()
    fence(tenant)
    ended = [(e["data"]["result"], e["data"]["ended_by"]) for e in tenant.events() if e["type"] == "call.ended"]
    waiting = ok(tenant.api.get(f"/api/v1/queues/{q['id']}/stats"))["waiting"]
    assert (ended, closed["data"]["result"], waiting) == ([(result, ended_by)], result, 0)


def test_a_voicemail_transcribed_before_its_recording_is_stored_still_carries_its_text(world, tenant, monkeypatch):
    monkeypatch.setattr(world.plivo, "transcribe_first", True)
    q = tenant.queue(name="Q", agents=[unavailable(tenant).id], wait_timeout_sec=1)
    vm = voicemail_action(q, "Nobody is free", transcribe=True)
    ok(tenant.api.patch(f"/api/v1/queues/{q['id']}", json={"overflow_action": vm}))
    stream = tenant.stream(after=0)
    world.plivo.call(line(tenant, queue_action(q)))
    rid = stream.wait_for("voicemail.received", timeout=10)["data"]["recording_id"]
    run_jobs_until(world, lambda: seen(stream, "transcript.ready", recording_id=rid), "transcript.ready")
    segments = ok(tenant.api.get(f"/api/v1/recordings/{rid}/transcript"))["segments"]
    assert [s["text"] for s in segments] == [world.plivo.transcript]


def test_a_full_queue_overflows_as_enqueue_failed(world, tenant):
    q = tenant.queue(
        name="Small",
        agents=[unavailable(tenant).id],
        capacity=50,
        wait_timeout_sec=1800,
        overflow_action=hangup_action("Too busy"),
    )
    number = line(tenant, queue_action(q))
    stream = tenant.stream(after=0)
    for _ in range(50):
        world.plivo.call(number)
    world.wait(lambda: len(seen(stream, "conversation.queued")) == 50, timeout=30, what="50 waiting")
    caller = world.plivo.call(number)
    assert stream.wait_for("conversation.overflowed", reason="enqueue_failed")["data"]["queue_id"] == q["id"]
    world.wait(lambda: "Too busy" in caller.said, what="overflow action")


def test_a_call_every_agent_declines_takes_the_overflow_action(world, tenant):
    agents = [tenant.agent(n).login() for n in ("A", "B")]
    for a in agents:
        a.phone.on_ring("decline")
    number, _ = tenant.inbound_line(*agents, retries=0, overflow_action=hangup_action("Sorry, nobody answered"))
    stream = tenant.stream(after=0)
    caller = world.plivo.call(number)
    assert stream.wait_for("conversation.overflowed")["data"]["reason"] == "assignment_failed"
    world.wait(lambda: "Sorry, nobody answered" in caller.said, what="overflow action")
    assert len(seen(stream, "offer.rejected")) == 2


def test_a_decline_reoffers_the_next_agent_until_retries_run_out(world, tenant):
    a, b, c = (tenant.agent(n).login() for n in ("A", "B", "C"))
    a.phone.on_ring("decline")
    b.phone.on_ring("decline")
    number, _ = tenant.inbound_line(a, b, c, agents_at_once=1, retries=1, overflow_action=hangup_action("Nobody"))
    stream = tenant.stream(after=0)
    world.plivo.call(number)
    assert stream.wait_for("conversation.overflowed")["data"]["reason"] == "assignment_failed"
    assert (len(a.phone.legs), len(b.phone.legs), len(c.phone.legs)) == (1, 1, 0)


def test_a_ring_timeout_reoffers_the_next_agent_while_retries_allow(world, tenant):
    settings(tenant, ring_timeout_sec=5)
    first, second = tenant.agent("First").login(), tenant.agent("Second").login()
    first.phone.on_ring("timeout")
    number, _ = tenant.inbound_line(first, second, agents_at_once=1, retries=1)
    stream = tenant.stream(after=0)
    world.plivo.call(number)
    second.phone.wait_ring(timeout=10)
    assert stream.wait_for("offer.missed", agent_id=first.id)["data"]["strike"] == 1


@pytest.mark.parametrize("fault", ["offline", "fail", "busy"])
def test_a_ring_that_fails_while_the_caller_waits_is_a_miss_with_no_strike(world, tenant, fault):
    agent = tenant.agent().login()
    if fault == "offline":
        agent.phone.unregister()
    else:
        agent.phone.on_ring(fault)
    stream = tenant.stream(after=0)
    caller = world.plivo.call(tenant.inbound_line(agent, wait_timeout_sec=600)[0])
    offer = stream.wait_for("offer.created", agent_id=agent.id)["data"]["offer_id"]

    def mine(events):
        ours = [e for e in events if e["type"] in OUTCOMES and e["data"]["offer_id"] == offer]
        return [(e["type"], e["data"].get("strike")) for e in ours]

    world.wait(lambda: mine(stream.events), what="the failed offer's outcome")
    fence(tenant)
    assert (mine(tenant.events()), caller.state) == ([("offer.missed", 0)], "in_progress")


def test_a_round_whose_rings_all_fail_offers_the_next_round(world, tenant):
    a, b, c = (tenant.agent(n).login() for n in ("A", "B", "C"))
    a.phone.unregister()
    b.phone.unregister()
    number, _ = tenant.inbound_line(a, b, c, agents_at_once=2, retries=1)
    stream = tenant.stream(after=0)
    world.plivo.call(number)
    c.phone.wait_ring(timeout=10)
    stream.wait_for("offer.created", agent_id=c.id)
    offered = [e["agent_id"] for e in seen(stream, "offer.created")]
    assert (set(offered[:2]), offered[2:]) == ({a.id, b.id}, [c.id])


def test_simultaneous_rings_every_agent_and_withdraws_the_rest(tenant):
    winner, other = tenant.agent("Winner").login(), tenant.agent("Other").login()
    other.phone.on_ring("ignore")
    live = connect(tenant, winner, also=[other], strategy="Simultaneous")
    assert live.stream.wait_for("offer.withdrawn", agent_id=other.id)["data"]["reason"] == "taken"
    other.phone.legs[0].wait_state("completed")
    assert live.caller.mpc == live.leg.mpc


def test_availability_offers_the_longest_available_agent_first(world, tenant):
    longest = tenant.agent("Longest").login()
    newest = tenant.agent("Newest").login()
    number, _ = tenant.inbound_line(newest, longest, agents_at_once=1)
    world.plivo.call(number)
    longest.phone.wait_ring()
    assert newest.phone.legs == []


def test_lowest_conversations_handled_prefers_fewer_calls(world, tenant):
    settings(tenant, wrap_up={"mode": "manual", "seconds": 1})
    veteran = tenant.agent("Veteran").login()
    finish(connect(tenant, veteran))
    rookie = tenant.agent("Rookie").login()
    number, _ = tenant.inbound_line(veteran, rookie, strategy="Lowest Conversations Handled", agents_at_once=1)
    before = len(veteran.phone.legs)
    world.plivo.call(number)
    rookie.phone.wait_ring()
    assert len(veteran.phone.legs) == before


def test_lowest_conversations_handled_resets_at_sign_in(world, tenant):
    settings(tenant, wrap_up={"mode": "manual", "seconds": 1})
    a, b = tenant.agent("A").login(), tenant.agent("B").login()
    for agent in (b, b, a):
        finish(connect(tenant, agent))
    ok(set_status(tenant, b, "Offline"))
    b.login()
    number, _ = tenant.inbound_line(a, b, strategy="Lowest Conversations Handled", agents_at_once=1)
    before = len(a.phone.legs)
    world.plivo.call(number)
    b.phone.wait_ring()
    assert len(a.phone.legs) == before


def test_lowest_conversation_handle_time_prefers_less_time_on_calls(world, tenant):
    settings(tenant, wrap_up={"mode": "manual", "seconds": 1})
    slow, quick = tenant.agent("Slow").login(), tenant.agent("Quick").login()
    long_call = connect(tenant, slow)
    for _ in range(2):
        finish(connect(tenant, quick))
    finish(long_call)
    number, _ = tenant.inbound_line(slow, quick, strategy="Lowest Conversation Handle Time", agents_at_once=1)
    before = len(slow.phone.legs)
    world.plivo.call(number)
    quick.phone.wait_ring()
    assert len(slow.phone.legs) == before


def test_skills_filter_who_is_offered(world, tenant):
    spanish = skill(tenant, "Spanish")
    novice = tenant.agent("Novice", skills=[{"skill_id": spanish, "level": 3}]).login()
    expert = tenant.agent("Expert", skills=[{"skill_id": spanish, "level": 7}]).login()
    needs = [{"skill_id": spanish, "operator": ">=", "level": 5}]
    world.plivo.call(line(tenant, queue_action(tenant.queue(name="ES", agents=[novice.id, expert.id], skills=needs))))
    expert.phone.wait_ring()
    assert novice.phone.legs == []


# per operator against a needed level of 5: the levels of an agent just inside and just outside it
EDGE = {"=": (5, 6), "!=": (6, 5), "<": (4, 5), "<=": (5, 6), ">": (6, 5), ">=": (5, 4)}


@pytest.mark.parametrize("op", EDGE)
def test_a_freed_agent_takes_a_waiting_caller_only_when_its_skill_level_qualifies(world, tenant, op):
    spanish = skill(tenant)
    inside, outside = (tenant.agent(f"L{n}", skills=[{"skill_id": spanish, "level": n}]).login() for n in EDGE[op])
    for a in (inside, outside):
        a.set_status("Away")
    needs = [{"skill_id": spanish, "operator": op, "level": 5}]
    urgent, uq = tenant.inbound_line(inside, outside, skills=needs, priority=1, wait_timeout_sec=600)
    plain, pq = tenant.inbound_line(outside, priority=9, wait_timeout_sec=600)
    stream = tenant.stream(after=0)
    calls = []
    for i, (number, q) in enumerate(((urgent, uq), (plain, pq))):
        world.plivo.call(number, from_=f"+1415555022{i}")
        calls.append(stream.wait_for("conversation.queued", queue_id=q["id"])["conversation_id"])
    skilled_call, plain_call = calls
    outside.set_status("Available")  # the urgent caller does not qualify it, so it takes the plain one
    assert hs_header(outside.phone.wait_ring(), "HsConversation") == plain_call
    inside.set_status("Available")
    assert hs_header(inside.phone.wait_ring(), "HsConversation") == skilled_call


def stuck_to_last(world, tenant, mode: str, caller_id: str):
    """Two agents on a sticky queue, where `last` took a first call from `caller_id` after `usual`
    declined it. Returns (number, usual, last, stream)."""
    settings(tenant, wrap_up={"mode": "manual", "seconds": 1})
    usual, last = tenant.agent("Usual").login(), tenant.agent("Last").login()
    number, _ = tenant.inbound_line(usual, last, agents_at_once=1, retries=1, sticky={"mode": mode})
    usual.phone.on_ring("decline")
    stream = tenant.stream(after=0)
    caller = world.plivo.call(number, from_=caller_id)
    cid = hs_header(last.phone.wait_ring(), "HsConversation")
    caller.hangup()
    stream.wait_for("conversation.wrap_up_started", conversation_id=cid)
    ok(wrap_up(last, cid, disposition(tenant)["id"]))
    usual.phone.on_ring("answer")
    return number, usual, last, stream


def test_loose_sticky_offers_the_last_agent_first(world, tenant):
    number, usual, last, _ = stuck_to_last(world, tenant, "loose", "+14155550177")
    rings = len(usual.phone.legs)
    world.plivo.call(number, from_="+14155550177")
    last.phone.wait_ring()
    assert len(usual.phone.legs) == rings


def test_strict_sticky_waits_for_the_last_agent(world, tenant):
    number, usual, last, stream = stuck_to_last(world, tenant, "strict", "+14155550178")
    last.set_status("Away")
    rings = len(usual.phone.legs)
    world.plivo.call(number, from_="+14155550178")
    world.wait(lambda: seen(stream, "conversation.queued")[1:], what="second call waiting")
    last.set_status("Available")
    last.phone.wait_ring()
    assert len(usual.phone.legs) == rings


def test_auto_accept_marks_the_offer_leg(world, tenant):
    agent = tenant.agent().login()
    number, _ = tenant.inbound_line(agent, auto_accept={"enabled": True, "delay": 2})
    stream = tenant.stream(after=0)
    world.plivo.call(number)
    assert hs_header(agent.phone.wait_ring(), "HsAutoAnswer") == "2"
    assert stream.wait_for("offer.created", agent_id=agent.id)["data"]["auto_accept"] is True


def test_an_offer_leg_carries_the_public_headers(world, tenant):
    agent = tenant.agent().login()
    agent.phone.on_ring("ignore")
    number, q = tenant.inbound_line(agent)
    stream = tenant.stream(after=0)
    world.plivo.call(number, from_="+14155550160")
    leg = agent.phone.wait_ring()
    offer = stream.wait_for("offer.created", agent_id=agent.id)
    assert (hs_header(leg, "HsKind"), hs_header(leg, "HsOffer")) == ("offer", offer["data"]["offer_id"])
    assert hs_header(leg, "HsConversation") == offer["conversation_id"] and "X-PH-HsAutoAnswer" not in leg.headers
    assert (offer["data"]["queue_id"], offer["data"]["from"], offer["data"]["auto_accept"]) == (
        q["id"],
        "+14155550160",
        False,
    )


def test_a_queue_change_applies_to_the_next_call(world, tenant):
    before, after = tenant.agent("Before").login(), tenant.agent("After").login()
    number, q = tenant.inbound_line(before)
    ok(tenant.api.patch(f"/api/v1/queues/{q['id']}", json={"agents": [after.id]}))
    world.plivo.call(number)
    after.phone.wait_ring()
    assert before.phone.legs == []


def test_a_queue_created_with_only_a_name_takes_calls(world, tenant):
    q = tenant.queue(name="Just a name")
    stream = tenant.stream(after=0)
    caller = world.plivo.call(line(tenant, queue_action(q)))
    assert stream.wait_for("conversation.started")["data"]["hours"] == "none"
    assert stream.wait_for("conversation.queued")["data"]["queue_id"] == q["id"]
    world.wait(lambda: caller.mpc, what="caller waiting in the queue's room")
    assert caller.state == "in_progress"


def test_an_agent_who_hangs_up_first_gives_no_overflow(world, tenant):
    live = connect(tenant, overflow_action=hangup_action("Overflowed"))
    live.leg.hangup()
    live.caller.wait_state("completed")
    live.stream.wait_for("call.ended", conversation_id=live.id)
    fence(tenant)
    assert not tenant.events(type="conversation.overflowed") and "Overflowed" not in live.caller.said


def test_a_caller_who_leaves_while_ringing_withdraws_the_offer(world, tenant):
    agent = tenant.agent().login()
    agent.phone.on_ring("ignore")
    number, q = tenant.inbound_line(agent)
    stream = tenant.stream(after=0)
    caller = world.plivo.call(number)
    agent.phone.wait_ring()
    caller.hangup()
    assert stream.wait_for("offer.withdrawn", agent_id=agent.id)["data"]["reason"] == "caller_left"
    assert stream.wait_for("call.ended")["data"]["queue_id"] == q["id"]


@pytest.mark.parametrize(
    ("first", "outcome"), [("caller_exit", ("offer.withdrawn", "caller_left")), ("ring_end", ("offer.missed", None))]
)
def test_a_failed_ring_reads_caller_left_from_the_callers_own_exit_in_either_order(world, tenant, first, outcome):
    agent = tenant.agent().login()
    agent.phone.unregister()
    stream = tenant.stream(after=0)

    def hold(event):
        return world.plivo.fault("delay", lambda r: r.params.get("EventName") == event, account=tenant.auth_id)

    room_end, ring_end = hold("MPCEnd"), hold("Hangup") if first == "caller_exit" else None
    caller = world.plivo.call(tenant.inbound_line(agent, wait_timeout_sec=600)[0])
    offer = stream.wait_for("offer.created", agent_id=agent.id)["data"]["offer_id"]

    def mine(events):
        ours = [e for e in events if e["type"] in OUTCOMES and e["data"]["offer_id"] == offer]
        return [(e["type"], e["data"].get("reason")) for e in ours]

    world.wait(lambda: ring_end.delayed if ring_end else mine(stream.events), what="the failed ring's hangup")
    caller.hangup()
    stream.wait_for("call.ended")
    if ring_end:
        ring_end.release()
    world.wait(lambda: mine(stream.events), what="the failed ring's outcome")
    room_end.release()
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    fence(tenant)
    ended = [e["data"]["result"] for e in tenant.events(type="call.ended")]
    assert (mine(tenant.events()), ended) == ([outcome], ["abandoned"])


def test_an_agent_who_picks_up_after_the_caller_left_gets_the_offer_withdrawn(world, tenant):
    agent = tenant.agent().login()
    agent.phone.on_ring("answer", after=0.2)
    stream = tenant.stream(after=0)
    caller = world.plivo.call(tenant.inbound_line(agent)[0])
    leg = agent.phone.wait_ring()
    world.plivo.reject_next(f"/Call/{leg.call_uuid}/", 503)  # the cancel is lost, so the agent picks up
    caller.hangup()
    leg.wait_state("in_progress").wait_state("completed")
    assert [(e["type"], e["data"].get("reason")) for e in outcomes(stream, tenant, agent, leg)] == [
        ("offer.withdrawn", "caller_left")
    ]


def test_an_agent_who_picks_up_after_the_caller_moved_queues_gets_the_offer_withdrawn(world, tenant):
    agent = tenant.agent().login()
    agent.phone.on_ring("answer", after=1.0)
    second = tenant.queue(name="Second", agents=[unavailable(tenant).id])
    first = tenant.queue(name="First", agents=[agent.id], dtmf_actions={"9": queue_action(second)})
    stream = tenant.stream(after=0)
    caller = world.plivo.call(line(tenant, queue_action(first)))
    leg = agent.phone.wait_ring()
    for _ in range(2):  # the queue exit and the room's end both cancel; both are lost, so the agent picks up
        world.plivo.reject_next(f"/Call/{leg.call_uuid}/", 503)
    caller.press("9")
    stream.wait_for("conversation.queued", queue_id=second["id"])
    leg.wait_state("in_progress")
    assert [(e["type"], e["data"].get("reason")) for e in outcomes(stream, tenant, agent, leg)] == [
        ("offer.withdrawn", "caller_left")
    ]


def test_a_caller_key_press_in_queue_runs_its_dtmf_action(world, tenant):
    q = tenant.queue(name="Q", agents=[unavailable(tenant).id], dtmf_actions={"9": hangup_action("You pressed nine")})
    stream = tenant.stream(after=0)
    caller = world.plivo.call(line(tenant, queue_action(q)))
    stream.wait_for("conversation.queued")
    caller.press("9")
    assert stream.wait_for("conversation.overflowed")["data"]["reason"] == "dtmf"
    world.wait(lambda: "You pressed nine" in caller.said, what="dtmf action")


def test_queue_defaults_and_read_only_fields(world, tenant):
    q = tenant.queue(name="Defaults")
    got = (q["strategy"], q["agents_at_once"], q["retries"], q["priority"], q["capacity"], q["wait_timeout_sec"])
    assert got == ("Availability", 3, 3, 5, 50, 20)
    assert q["enqueue_url"].startswith(f"{world.base_url}/api/v1/queues/{q['id']}/enqueue")
    tenant.api.patch(f"/api/v1/queues/{q['id']}", json={"enqueue_url": "https://evil.example/x"})
    assert ok(tenant.api.get(f"/api/v1/queues/{q['id']}"))["enqueue_url"] == q["enqueue_url"]
    assert tenant.queue(name="All at once", strategy="Simultaneous", retries=3)["retries"] == 0


def test_queue_fields_out_of_range_are_422(tenant):
    bad_skill = [{"skill_id": skill(tenant), "operator": "LIKE", "level": 1}]
    attempts = [
        {"agents_at_once": 0},
        {"agents_at_once": 11},
        {"retries": 4},
        {"priority": 11},
        {"capacity": 49},
        {"capacity": 101},
        {"wait_timeout_sec": 1801},
        {"strategy": "Round robin"},
        {"skills": bad_skill},
    ]
    assert [b for b in attempts if tenant.api.post("/api/v1/queues", json={"name": "Q", **b}).status_code != 422] == []


def test_a_repeated_answer_for_one_call_gets_the_same_xml(world, tenant):
    number, _ = tenant.inbound_line(tenant.agent().login())
    params = answer_params(number)
    first = plivo_post(world, tenant.id, "answer", params, tenant.auth_token)
    again = plivo_post(world, tenant.id, "answer", params, tenant.auth_token)
    assert first.status_code == again.status_code == 200 and "xml" in first.headers["content-type"]
    assert first.text == again.text
    fence(tenant)
    assert len(tenant.events(type="conversation.started")) == 1


def test_number_entry_points_attach_list_and_delete(world, tenant):
    app_id = ok(world.operator.get(f"/api/v1/tenants/{tenant.id}"))["plivo_app_id"]
    number, _ = tenant.inbound_line(tenant.agent())
    updates = [r for r in world.plivo.requests_to(f"/Number/{number.lstrip('+')}") if r.method == "POST"]
    assert updates and updates[-1].params["app_id"] == app_id
    assert number in [n["number"] for n in ok(tenant.api.get("/api/v1/numbers"))["data"]]
    ok(tenant.api.delete(f"/api/v1/numbers/{number}"))
    error(tenant.api.get(f"/api/v1/numbers/{number}"), 404)
    stranger = f"+1555{random.randint(1_000_000, 9_999_999)}"
    refused(tenant.api.put(f"/api/v1/numbers/{stranger}", json={"open_action": hangup_action()}))


def room_xml(world, caller) -> ET.Element:
    """The MultiPartyCall element HS answered the caller's leg with."""
    bodies = [cb.body for cb in world.plivo.callbacks if cb.params.get("CallUUID") == caller.call_uuid]
    body = next(b for b in bodies if "<MultiPartyCall" in b)
    return ET.fromstring(body).find(".//MultiPartyCall")  # noqa: S314 (HS's own reply)


def test_a_hold_music_url_with_a_query_string_still_seats_the_caller(world, tenant):
    music = {"customer": "https://music.example.test/wait.mp3?track=1&loop=true"}
    number, _ = tenant.inbound_line(unavailable(tenant), hold_music=music)
    caller = world.plivo.call(number)
    world.wait(lambda: caller.mpc or caller.state == "completed", what="the caller seated or dropped")
    assert caller.mpc and room_xml(world, caller).findtext("CustomerHoldMusic/Play") == music["customer"]


def test_a_queue_without_hold_music_plays_the_default_track(world, tenant):
    number, _ = tenant.inbound_line(unavailable(tenant))
    caller = world.plivo.call(number)
    world.wait(lambda: caller.mpc or caller.state == "completed", what="the caller seated or dropped")
    room = room_xml(world, caller)
    assert caller.mpc and room.findtext("CustomerHoldMusic/Play") == room.findtext("AgentHoldMusic/Play") == MUSIC


def test_a_voicemail_left_in_an_agents_mailbox_reaches_that_agent(world, tenant):
    owner = tenant.agent("Owner").login(available=False)
    mine = tenant.stream(owner.stream_token, after=0)
    number = tenant.number()
    tenant.route_number(
        number, {"type": "voicemail", "greeting": say("Leave a message"), "mailbox": {"agent_id": owner.id}}
    )
    world.plivo.call(number)
    assert mine.wait_for("voicemail.received", timeout=10)["data"]["mailbox"]["agent_id"] == owner.id


@pytest.mark.xfail(strict=True, reason="bug: a 5xx on add-member overflows the caller while the agent rings")
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
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    fence(tenant)
    legs = a.phone.legs + b.phone.legs
    assert lost.hits and len(legs) == 1, f"{len(legs)} legs rang for one ring"
    assert accepted["agent_id"] == (a if a.phone.legs else b).id
    assert legs[0].state == caller.state == "in_progress" and legs[0].mpc == caller.mpc


def test_a_call_every_agent_declines_reaches_the_overflow_and_closes_once(world, tenant):
    agents = [tenant.agent(n).login() for n in ("A", "B")]
    for a in agents:
        a.phone.on_ring("decline")
    number, q = tenant.inbound_line(*agents, retries=0, overflow_action=hangup_action("Sorry, nobody answered"))
    stream = tenant.stream(after=0)
    caller = world.plivo.call(number)
    caller.wait_state("completed", timeout=10)
    closed = run_jobs_until(world, lambda: seen(stream, "conversation.closed"), "the conversation closed")[0]
    fence(tenant)
    ended = [(e["data"]["result"], e["data"]["ended_by"]) for e in tenant.events() if e["type"] == "call.ended"]
    waiting = ok(tenant.api.get(f"/api/v1/queues/{q['id']}/stats"))["waiting"]
    assert "Sorry, nobody answered" in caller.said
    assert (len(ended), closed["data"]["result"], waiting) == (1, ended[0][0], 0)
    assert len(seen(stream, "conversation.overflowed")) == 1


def test_a_caller_nobody_can_be_offered_again_waits_out_the_timeout_with_one_true_ending(world, tenant):
    agents = [tenant.agent(n).login() for n in ("A", "B")]
    for a in agents:
        a.phone.on_ring("decline")
    number, q = tenant.inbound_line(*agents, retries=3, wait_timeout_sec=2)
    stream = tenant.stream(after=0)
    caller = world.plivo.call(number)
    caller.wait_state("completed", timeout=10)
    closed = run_jobs_until(world, lambda: seen(stream, "conversation.closed"), "the conversation closed")[0]
    world.run_jobs()
    fence(tenant)
    offered = sorted(e["agent_id"] for e in tenant.events(type="offer.created"))
    ended = [(e["data"]["result"], e["data"]["ended_by"]) for e in tenant.events() if e["type"] == "call.ended"]
    waiting = ok(tenant.api.get(f"/api/v1/queues/{q['id']}/stats"))["waiting"]
    assert offered == sorted(a.id for a in agents) and [len(a.phone.legs) for a in agents] == [1, 1]
    assert (ended, closed["data"]["result"], waiting) == ([("queue_timeout", "system")], "queue_timeout", 0)
    assert [e["data"]["reason"] for e in tenant.events(type="conversation.overflowed")] == ["wait_timeout"]


def test_a_call_on_a_holiday_runs_the_closed_action_and_never_queues(world, tenant):
    box = tenant.queue(name="Box")
    number = closed_line(tenant, voicemail_action(box, "Closed for the holiday"), weekly=True, holiday=[])
    stream = tenant.stream(after=0)
    caller = world.plivo.call(number)
    started = stream.wait_for("conversation.started")
    received = stream.wait_for("voicemail.received", timeout=10)
    fence(tenant)
    assert started["data"]["hours"] == "closed" and "Closed for the holiday" in caller.said
    assert stream.wait_for("conversation.after_hours")["data"]["number"] == number
    assert received["conversation_id"] == started["conversation_id"] and not seen(stream, "conversation.queued")


def test_a_holiday_with_open_slots_routes_a_call_on_a_day_that_is_otherwise_closed(world, tenant):
    agent = tenant.agent().login()
    h = hours(tenant, weekly=False, holiday=[{"start": "00:00", "end": "23:59"}])
    q = tenant.queue(name="Q", agents=[agent.id])
    number = line(tenant, queue_action(q), business_hours_id=h["id"], closed_action=hangup_action("Closed"))
    caller = world.plivo.call(number)
    assert hs_header(agent.phone.wait_ring(), "HsKind") == "offer"
    assert "Closed" not in caller.said
