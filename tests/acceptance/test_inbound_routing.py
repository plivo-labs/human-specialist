"""Inbound routing as configuration: number entry point, hours, actions, queues, offers and overflow."""

import math
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
    hangup_action,
    hours,
    hs_header,
    lead,
    ok,
    plivo_post,
    queue_action,
    refused,
    replayed,
    run_jobs_until,
    say,
    seen,
    settings,
    skill,
    status_changes,
    team,
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
    assert not replayed(tenant, "conversation.queued")


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


def test_a_voicemails_transcription_callback_keeps_its_transcript_and_announces_it(world, tenant):
    # Plivo refuses every transcript read, so only the callback can have kept the text.
    world.plivo.fault(
        "reject", lambda r: "/Transcription/" in r.path, account=tenant.auth_id, times=math.inf, status=404
    )
    q = tenant.queue(name="Support")
    vm = voicemail_action(q, transcribe=True)
    number = line(tenant, queue_action(q), business_hours_id=hours(tenant, weekly=False)["id"], closed_action=vm)
    stream = tenant.stream(after=0)
    world.plivo.call(number)
    rid = stream.wait_for("voicemail.received", timeout=10)["data"]["recording_id"]
    stream.wait_for("transcript.ready", recording_id=rid)
    got = ok(tenant.api.get(f"/api/v1/recordings/{rid}/transcript"))
    assert (got["complete"], [s["text"] for s in got["segments"]]) == (True, [world.plivo.transcript])


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


def test_simultaneous_rings_every_agent_and_withdraws_the_rest(tenant):
    winner, other = tenant.agent("Winner").login(), tenant.agent("Other").login()
    other.phone.on_ring("ignore")
    live = connect(tenant, winner, also=[other], strategy="simultaneous")
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


def test_skills_filter_who_is_offered(world, tenant):
    spanish = skill(tenant, "Spanish")
    novice = tenant.agent("Novice", skills=[{"skill_id": spanish, "level": 3}]).login()
    expert = tenant.agent("Expert", skills=[{"skill_id": spanish, "level": 7}]).login()
    needs = [{"skill_id": spanish, "operator": ">=", "level": 5}]
    world.plivo.call(line(tenant, queue_action(tenant.queue(name="ES", agents=[novice.id, expert.id], skills=needs))))
    expert.phone.wait_ring()
    assert novice.phone.legs == []


def stuck_to_last(world, tenant, mode: str, caller_id: str, **sticky):
    """Two agents on a sticky queue, where `last` took a first call from `caller_id` after `usual`
    declined it. Returns (number, usual, last, stream)."""
    settings(tenant, wrap_up={"mode": "manual", "seconds": 1})
    usual, last = tenant.agent("Usual").login(), tenant.agent("Last").login()
    number, _ = tenant.inbound_line(usual, last, agents_at_once=1, retries=1, sticky={"mode": mode, **sticky})
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


def test_sticky_with_an_expiry_offers_the_last_agent_inside_it(world, tenant):
    number, usual, last, _ = stuck_to_last(world, tenant, "loose", "+14155550179", expiry_hours=1)
    rings = len(usual.phone.legs)
    world.plivo.call(number, from_="+14155550179")
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


def test_an_hours_change_applies_to_the_next_call(world, tenant):
    q = tenant.queue(name="Q", agents=[unavailable(tenant).id])
    h = hours(tenant)
    number = line(tenant, queue_action(q), business_hours_id=h["id"], closed_action=hangup_action("Now closed"))
    ok(tenant.api.patch(f"/api/v1/business-hours/{h['id']}", json={"slots": []}))
    caller = world.plivo.call(number)
    caller.wait_state("completed")
    assert "Now closed" in caller.said


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
    assert got == ("availability", 3, 3, 5, 50, 20)
    assert q["enqueue_url"].startswith(f"{world.base_url}/api/v1/queues/{q['id']}/enqueue")
    tenant.api.patch(f"/api/v1/queues/{q['id']}", json={"enqueue_url": "https://evil.example/x"})
    assert ok(tenant.api.get(f"/api/v1/queues/{q['id']}"))["enqueue_url"] == q["enqueue_url"]
    assert tenant.queue(name="All at once", strategy="simultaneous", retries=3)["retries"] == 0


def test_queue_fields_out_of_range_are_422(tenant):
    bad_skill = [{"skill_id": skill(tenant), "operator": "LIKE", "level": 1}]
    attempts = [
        {"agents_at_once": 11},
        {"retries": 11},
        {"priority": 11},
        {"capacity": 0},
        {"capacity": 10_001},
        {"wait_timeout_sec": 86_401},
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


@pytest.mark.parametrize("change", ["open_action", "hours"])
def test_a_repeated_answer_for_a_seated_call_gets_its_room_again_after_its_number_or_hours_changed(
    world, tenant, change
):
    q = tenant.queue(name="Q", agents=[unavailable(tenant).id])
    h = hours(tenant)
    number = line(tenant, queue_action(q), business_hours_id=h["id"], closed_action=hangup_action("Closed"))
    params = answer_params(number)
    first = plivo_post(world, tenant.id, "answer", params, tenant.auth_token)
    assert "<MultiPartyCall" in first.text
    if change == "open_action":
        tenant.route_number(number, hangup_action("Moved"), business_hours_id=h["id"])
    else:
        ok(tenant.api.patch(f"/api/v1/business-hours/{h['id']}", json={"slots": []}))
    again = plivo_post(world, tenant.id, "answer", params, tenant.auth_token)
    assert again.text == first.text


def test_a_repeated_queue_exit_gets_the_same_xml_and_one_overflow(world, tenant):
    nobody = hangup_action("Nobody is free")
    q = tenant.queue(name="Q", agents=[unavailable(tenant).id], wait_timeout_sec=1800, overflow_action=nobody)
    caller = world.plivo.call(line(tenant, queue_action(q)))
    world.wait(lambda: caller.mpc, what="the caller waiting in the queue's room")
    url = room_xml(world, caller).get("onExitActionUrl").partition("#")[0]
    timed_out = {"CallUUID": caller.call_uuid, "ParticipantExitCause": "Member Wait Timeout Triggered"}
    signed = [world.plivo.sign(url, timed_out, tenant.auth_token) for _ in range(2)]  # a retry carries a new nonce
    first, again = [world.anon().post(url, data=timed_out, headers=headers) for headers in signed]
    assert first.status_code == again.status_code == 200 and first.text == again.text
    assert "Nobody is free" in first.text
    fence(tenant)
    assert len(tenant.events(type="conversation.overflowed")) == 1


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


def test_a_waiting_caller_hears_the_queues_hold_music(world, tenant):
    music = {"customer": "https://music.example.test/wait.mp3", "agent": "https://music.example.test/agent.mp3"}
    number, _ = tenant.inbound_line(unavailable(tenant), hold_music=music)
    plain, _ = tenant.inbound_line(unavailable(tenant, "Away-too"))
    caller, other = world.plivo.call(number), world.plivo.call(plain)
    world.wait(lambda: caller.mpc and other.mpc, what="both callers waiting in their queue's room")
    room = room_xml(world, caller)
    assert room.findtext("CustomerHoldMusic/Play") == music["customer"]
    assert room.findtext("AgentHoldMusic/Play") == music["agent"]


def test_a_hold_music_url_with_a_query_string_still_seats_the_caller(world, tenant):
    music = {"customer": "https://music.example.test/wait.mp3?track=1&loop=true"}
    number, _ = tenant.inbound_line(unavailable(tenant), hold_music=music)
    caller = world.plivo.call(number)
    world.wait(lambda: caller.mpc or caller.state == "completed", what="the caller seated or dropped")
    assert caller.mpc and room_xml(world, caller).findtext("CustomerHoldMusic/Play") == music["customer"]


def waiting(world, stream, number: str, from_: str) -> str:
    """Place a call from `from_` and return its conversation id once it waits in the queue."""
    world.plivo.call(number, from_=from_)
    return stream.wait_for("conversation.queued", **{"from": from_})["conversation_id"]


@pytest.mark.parametrize(("order", "rung"), [("fifo", 0), ("lifo", 1)])
def test_a_waiting_caller_rings_an_agent_freed_by_wrap_up(world, tenant, order, rung):
    settings(tenant, wrap_up={"mode": "manual", "seconds": 300})
    live = connect(tenant, dequeue_order=order, wait_timeout_sec=600)
    cids = [waiting(world, live.stream, live.number, f"+1415555020{i}") for i in range(2)]
    live.caller.hangup()
    live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id)
    ok(wrap_up(live.agent, live.id, disposition(tenant)["id"]))
    assert hs_header(live.agent.phone.wait_ring(), "HsConversation") == cids[rung]


def test_a_freed_agent_takes_the_higher_priority_queue_first(world, tenant):
    settings(tenant, wrap_up={"mode": "manual", "seconds": 300})
    live = connect(tenant)
    low, high = (
        line(tenant, queue_action(tenant.queue(name=n, agents=[live.agent.id], priority=p, wait_timeout_sec=600)))
        for n, p in (("Low", 9), ("High", 1))
    )
    waiting(world, live.stream, low, "+14155550211")
    urgent = waiting(world, live.stream, high, "+14155550212")
    live.caller.hangup()
    live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id)
    ok(wrap_up(live.agent, live.id, disposition(tenant)["id"]))
    assert hs_header(live.agent.phone.wait_ring(), "HsConversation") == urgent


def test_an_agent_in_manual_wrap_up_is_not_offered(world, tenant):
    settings(tenant, wrap_up={"mode": "manual", "seconds": 300})
    live = connect(tenant)
    other = tenant.agent("Other").login()
    ok(tenant.api.patch(f"/api/v1/queues/{live.queue['id']}", json={"agents": [live.agent.id, other.id]}))
    live.caller.hangup()
    live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id)
    world.plivo.call(live.number)
    cid = hs_header(other.phone.wait_ring(), "HsConversation")
    live.stream.wait_for("offer.accepted", conversation_id=cid)
    assert [e["agent_id"] for e in seen(live.stream, "offer.created", conversation_id=cid)] == [other.id]
    assert len(live.agent.phone.legs) == 1


def test_a_team_queue_offers_the_teams_members(world, tenant):
    t = team(tenant, "Sales")
    member = tenant.agent("Member", teams=[t]).login()
    outsider = tenant.agent("Outsider", teams=[team(tenant, "Support")]).login()
    stream = tenant.stream(after=0)
    world.plivo.call(line(tenant, queue_action(tenant.queue(name="Sales", teams=[t]))))
    assert hs_header(member.phone.wait_ring(), "HsKind") == "offer"
    stream.wait_for("offer.created", agent_id=member.id)
    assert outsider.phone.legs == []
    assert [e["agent_id"] for e in seen(stream, "offer.created")] == [member.id]


def test_a_custom_status_routes_by_its_availability_flag(world, tenant):
    statuses = "/api/v1/agent-statuses"
    ok(tenant.api.post(statuses, json={"name": "Ready", "availability": True}))
    lunch = ok(tenant.api.post(statuses, json={"name": "Lunch", "availability": False}))
    on, off = tenant.agent("On").login(), tenant.agent("Off").login()
    on.set_status("Ready")
    off.set_status("Lunch")
    number, _ = tenant.inbound_line(on, off, wait_timeout_sec=600)
    stream = tenant.stream(after=0)
    world.plivo.call(number)
    stream.wait_for("offer.accepted", agent_id=on.id)
    assert off.phone.legs == []
    ok(tenant.api.patch(f"{statuses}/{lunch['id']}", json={"availability": True}))
    world.plivo.call(number)
    assert hs_header(off.phone.wait_ring(), "HsKind") == "offer"


def offers(tenant, cid: str) -> list[str]:
    """The agents offered `cid`, one entry per offer, once routing has settled."""
    return sorted(e["agent_id"] for e in replayed(tenant, "offer.created", conversation_id=cid))


@pytest.mark.parametrize("freed_by", ["status", "sign-in"])
def test_a_waiting_caller_rings_the_agent_who_turns_available(world, tenant, freed_by):
    agent = tenant.agent()
    if freed_by == "status":
        agent.login().set_status("Away")
    number, _ = tenant.inbound_line(agent, wait_timeout_sec=600)
    stream = tenant.stream(after=0)
    world.plivo.call(number)
    cid = stream.wait_for("conversation.queued")["conversation_id"]
    assert offers(tenant, cid) == [] and agent.phone.legs == []
    if freed_by == "status":
        agent.set_status("Available")
    else:
        agent.login()
    assert hs_header(agent.phone.wait_ring(), "HsConversation") == cid
    stream.wait_for("offer.created", agent_id=agent.id, conversation_id=cid)
    assert offers(tenant, cid) == [agent.id] and len(agent.phone.legs) == 1


# The levels of 4, 5 and 6 that a "level 5" requirement admits, and per operator one agent just inside it
# and one just outside.
ADMITS = {"=": {5}, "!=": {4, 6}, "<": {4}, "<=": {4, 5}, ">": {6}, ">=": {5, 6}}
EDGE = {"=": (5, 6), "!=": (6, 5), "<": (4, 5), "<=": (5, 6), ">": (6, 5), ">=": (5, 4)}


def skilled(tenant, spanish: str, level: int):
    return tenant.agent(f"Level-{level}", skills=[{"skill_id": spanish, "level": level}])


@pytest.mark.parametrize("op", ADMITS)
def test_a_skill_operator_offers_only_the_levels_it_admits(world, tenant, op):
    spanish = skill(tenant)
    agents = {level: skilled(tenant, spanish, level).login() for level in (4, 5, 6)}
    for a in agents.values():
        a.phone.on_ring("ignore")
    needs = [{"skill_id": spanish, "operator": op, "level": 5}]
    q = tenant.queue(name="ES", strategy="simultaneous", agents=[a.id for a in agents.values()], skills=needs)
    stream = tenant.stream(after=0)
    world.plivo.call(line(tenant, queue_action(q)))
    cid = stream.wait_for("conversation.queued")["conversation_id"]
    for level in ADMITS[op]:
        stream.wait_for("offer.created", agent_id=agents[level].id, conversation_id=cid)
    assert offers(tenant, cid) == sorted(agents[level].id for level in ADMITS[op])
    assert {level: len(a.phone.legs) for level, a in agents.items()} == {lv: int(lv in ADMITS[op]) for lv in agents}


@pytest.mark.parametrize("op", EDGE)
def test_a_freed_agent_takes_a_waiting_caller_only_when_its_skill_level_qualifies(world, tenant, op):
    spanish = skill(tenant)
    inside, outside = (skilled(tenant, spanish, level).login() for level in EDGE[op])
    for a in (inside, outside):
        a.set_status("Away")
    needs = [{"skill_id": spanish, "operator": op, "level": 5}]
    urgent, _ = tenant.inbound_line(inside, outside, skills=needs, priority=1, wait_timeout_sec=600)
    plain, _ = tenant.inbound_line(outside, priority=9, wait_timeout_sec=600)
    stream = tenant.stream(after=0)
    skilled_call, plain_call = (waiting(world, stream, n, f"+1415555022{i}") for i, n in enumerate((urgent, plain)))
    outside.set_status("Available")
    assert hs_header(outside.phone.wait_ring(), "HsConversation") == plain_call
    inside.set_status("Available")
    assert hs_header(inside.phone.wait_ring(), "HsConversation") == skilled_call
    stream.wait_for("offer.created", agent_id=inside.id, conversation_id=skilled_call)
    assert offers(tenant, skilled_call) == [inside.id] and offers(tenant, plain_call) == [outside.id]
    assert (len(inside.phone.legs), len(outside.phone.legs)) == (1, 1)


@pytest.mark.parametrize("busy_with", ["ringing", "call"])
def test_an_agent_ringing_or_on_a_call_is_not_offered_a_second_caller(world, tenant, busy_with):
    busy, other = tenant.agent("Busy").login(), tenant.agent("Other")
    busy.phone.on_ring("ignore" if busy_with == "ringing" else "answer")
    number, _ = tenant.inbound_line(busy, other, wait_timeout_sec=600)
    stream = tenant.stream(after=0)
    world.plivo.call(number, from_="+14155550231")
    first = hs_header(busy.phone.wait_ring(), "HsConversation")
    if busy_with == "call":
        stream.wait_for("offer.accepted", conversation_id=first)
    other.login()
    world.plivo.call(number, from_="+14155550232")
    second = hs_header(other.phone.wait_ring(), "HsConversation")
    stream.wait_for("offer.created", agent_id=other.id, conversation_id=second)
    assert offers(tenant, second) == [other.id] and offers(tenant, first) == [busy.id]
    assert second != first and (len(busy.phone.legs), len(other.phone.legs)) == (1, 1)


def test_an_agent_in_auto_wrap_up_is_not_offered(world, tenant):
    settings(tenant, wrap_up={"mode": "auto", "seconds": 300})
    live = connect(tenant)
    other = tenant.agent("Other").login()
    ok(tenant.api.patch(f"/api/v1/queues/{live.queue['id']}", json={"agents": [live.agent.id, other.id]}))
    live.caller.hangup()
    live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id)
    world.plivo.call(live.number)
    cid = hs_header(other.phone.wait_ring(), "HsConversation")
    live.stream.wait_for("offer.created", agent_id=other.id, conversation_id=cid)
    assert offers(tenant, cid) == [other.id]
    assert (len(live.agent.phone.legs), len(other.phone.legs)) == (1, 1)


def test_a_freed_team_member_takes_the_team_queues_waiting_caller(world, tenant):
    sales = team(tenant, "Sales")
    member = tenant.agent("Member", teams=[sales]).login()
    outsider = tenant.agent("Outsider", teams=[team(tenant, "Support")]).login()
    for a in (member, outsider):
        a.set_status("Away")
    team_line = line(tenant, queue_action(tenant.queue(name="Sales", teams=[sales], priority=1, wait_timeout_sec=600)))
    plain, _ = tenant.inbound_line(outsider, priority=9, wait_timeout_sec=600)
    stream = tenant.stream(after=0)
    team_call, plain_call = (waiting(world, stream, n, f"+1415555024{i}") for i, n in enumerate((team_line, plain)))
    outsider.set_status("Available")
    assert hs_header(outsider.phone.wait_ring(), "HsConversation") == plain_call
    member.set_status("Available")
    assert hs_header(member.phone.wait_ring(), "HsConversation") == team_call
    stream.wait_for("offer.created", agent_id=member.id, conversation_id=team_call)
    assert offers(tenant, team_call) == [member.id] and offers(tenant, plain_call) == [outsider.id]
    assert (len(member.phone.legs), len(outsider.phone.legs)) == (1, 1)


def test_availability_rings_agents_at_once_of_the_longest_available(world, tenant):
    first, second, third = (tenant.agent(n).login() for n in ("First", "Second", "Third"))
    for a in (first, second, third):
        a.phone.on_ring("ignore")
    number, _ = tenant.inbound_line(third, second, first, agents_at_once=2)
    stream = tenant.stream(after=0)
    world.plivo.call(number)
    cid = stream.wait_for("conversation.queued")["conversation_id"]
    for a in (first, second):
        stream.wait_for("offer.created", agent_id=a.id, conversation_id=cid)
    assert offers(tenant, cid) == sorted([first.id, second.id])
    assert [len(a.phone.legs) for a in (first, second, third)] == [1, 1, 0]


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
    [later] = status_changes(tenant, other, "Away")
    unrelated.wait_for("agent.status_changed", id=later["id"], timeout=10)
    assert not [e for e in unrelated.events if e["type"] == "voicemail.received"]


@pytest.mark.parametrize(
    ("leaves", "result", "ended_by"),
    [
        ("hangs_up", "abandoned", "customer"),
        ("waits_out", "queue_timeout", "system"),
        ("waits_out_api_down", "queue_timeout", "system"),
        ("waits_out_hs_first", "queue_timeout", "system"),
        ("waits_out_api_down_hs_first", "queue_timeout", "system"),
    ],
)
def test_a_caller_who_leaves_a_queue_unanswered_ends_once_with_the_true_cause(
    world, tenant, monkeypatch, leaves, result, ended_by
):
    # Plivo's waitTime and HS's queue timeout race once wait_timeout_sec is up; at 600 s the test picks the winner.
    q = tenant.queue(name="Q", agents=[unavailable(tenant).id], wait_timeout_sec=600)
    monkeypatch.setattr(world.plivo, "exit_after_events", True)  # the room's end before the queue exit, as live
    stream = tenant.stream(after=0)
    caller = world.plivo.call(line(tenant, queue_action(q)))
    if "api_down" in leaves:  # the call's end is read from Plivo's callbacks, never its call API
        world.plivo.fault("lose", lambda r: caller.call_uuid in r.path, account=tenant.auth_id, times=10)
    stream.wait_for("conversation.queued")
    if leaves == "hangs_up":
        caller.hangup()
    elif leaves.endswith("hs_first"):
        world.run_jobs("queue_timeout")
    else:
        caller.wait_out()
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


def test_a_failed_rings_add_member_outcome_is_answered_while_the_next_rounds_ring_is_unbound(world, tenant):
    first, second = tenant.agent("A").login(), tenant.agent("B").login(available=False)
    first.phone.unregister()
    second.phone.on_ring("ignore")
    stream = tenant.stream(after=0)

    def outcome(r) -> bool:
        return r.params.get("EventName") == "AddParticipantByAPIActionCompleted"

    def answered() -> list[int]:
        return [cb.status for cb in world.plivo.callbacks if outcome(cb) and f"/plivo/{tenant.id}/" in cb.url]

    failed = world.plivo.fault("delay", outcome, account=tenant.auth_id)
    world.plivo.call(tenant.inbound_line(first, second, retries=3)[0])
    stream.wait_for("offer.missed", agent_id=first.id)
    world.wait(lambda: failed.delayed, what="the failed ring's outcome held")
    world.plivo.fault("delay", lambda r: "/call-status" in r.path, times=math.inf, account=tenant.auth_id)
    stalled = world.plivo.fault("stall", lambda r: r.path.endswith("/Participant/"), account=tenant.auth_id)
    second.set_status("Available")
    world.wait(lambda: stalled.hits, what="the next round's ring sent")
    failed.release()
    world.wait(answered, what="the failed ring's outcome answered")
    world.wait(world.plivo.idle, what="its retries answered")
    assert answered() == [200]


@pytest.mark.parametrize(
    ("first", "outcome"), [("caller_exit", ("offer.withdrawn", "caller_left")), ("ring_end", ("offer.missed", "2020"))]
)
def test_a_failed_ring_reads_caller_left_from_the_callers_own_exit_in_either_order(world, tenant, first, outcome):
    agent = tenant.agent().login()
    agent.phone.unregister()
    stream = tenant.stream(after=0)

    def hold(event):
        return world.plivo.fault("delay", lambda r: r.params.get("EventName") == event, account=tenant.auth_id)

    room_end = hold("MPCEnd")
    # The ring's end reaches HS twice, as its leg's hangup and as its add-member's failure: hold both, and release
    # them before or after the caller's exit, as `first` says.
    ring_end = [hold("Hangup"), hold("AddParticipantByAPIActionCompleted")]
    caller = world.plivo.call(tenant.inbound_line(agent, wait_timeout_sec=600)[0])
    offer = stream.wait_for("offer.created", agent_id=agent.id)["data"]["offer_id"]

    def mine(events):
        ours = [e for e in events if e["type"] in OUTCOMES and e["data"]["offer_id"] == offer]
        return [(e["type"], e["data"].get("reason")) for e in ours]

    world.wait(lambda: all(h.delayed for h in ring_end), what="the ring's end")
    if first == "ring_end":
        for held in ring_end:
            held.release()
        world.wait(world.plivo.idle, what="the ring's end delivered")
        for kind in ("route_agent", "next_round"):
            world.run_jobs(kind)  # the miss's route_agent and next_round run before the caller leaves, never after
    caller.hangup()
    stream.wait_for("call.ended")
    for held in ring_end:
        held.release()
    world.wait(lambda: mine(stream.events), what="the failed ring's outcome")
    room_end.release()
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    fence(tenant)
    ended = [e["data"]["result"] for e in tenant.events(type="call.ended")]
    assert (mine(tenant.events()), ended) == ([outcome], ["abandoned"])


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


def test_a_voicemail_callback_naming_another_tenants_agent_keeps_the_message_in_no_ones_mailbox(world, tenant):
    outsider = world.new_tenant("Other").agent("Outsider")
    number = tenant.number()
    tenant.route_number(number, voicemail_action(tenant.queue()))
    stream = tenant.stream(after=0)
    params = answer_params(number)
    assert plivo_post(world, tenant.id, "answer", params, tenant.auth_token).status_code == 200
    left = {**params, "RecordingID": "left-for-an-outsider", "RecordUrl": "https://example.test/r.mp3"}
    path = f"actions/voicemail?agent_id={outsider.id}"
    assert plivo_post(world, tenant.id, path, left | {"RecordingDuration": "5"}, tenant.auth_token).status_code == 200
    vm = stream.wait_for("voicemail.received", timeout=10)
    assert (vm["agent_id"], vm["data"]["mailbox"]) == (None, {"queue_id": None, "agent_id": None})


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
    assert started["data"]["hours"] == "closed" and "Closed for the holiday" in caller.said
    assert stream.wait_for("conversation.after_hours")["data"]["number"] == number
    assert received["conversation_id"] == started["conversation_id"] and not replayed(tenant, "conversation.queued")


def test_a_holiday_with_open_slots_routes_a_call_on_a_day_that_is_otherwise_closed(world, tenant):
    agent = tenant.agent().login()
    h = hours(tenant, weekly=False, holiday=[{"start": "00:00", "end": "23:59"}])
    q = tenant.queue(name="Q", agents=[agent.id])
    number = line(tenant, queue_action(q), business_hours_id=h["id"], closed_action=hangup_action("Closed"))
    caller = world.plivo.call(number)
    assert hs_header(agent.phone.wait_ring(), "HsKind") == "offer"
    assert "Closed" not in caller.said


def test_an_agent_who_went_away_while_ringing_is_not_offered_the_same_caller_on_return(world, tenant):
    agent = tenant.agent().login()
    agent.phone.on_ring("ignore")
    number, _ = tenant.inbound_line(agent, wait_timeout_sec=600)
    stream = tenant.stream(after=0)
    caller = world.plivo.call(number)
    stream.wait_for("offer.created", agent_id=agent.id)
    agent.set_status("Away")
    stream.wait_for("offer.withdrawn", agent_id=agent.id, reason="agent_offline")
    world.run_jobs("next_round")
    agent.set_status("Available")
    fence(tenant)
    offered = [e["agent_id"] for e in tenant.events(type="offer.created")]
    assert (offered, tenant.events(type="conversation.overflowed"), caller.state) == ([agent.id], [], "in_progress")


def test_an_agent_who_missed_a_waiting_caller_is_offered_it_again_once_300_s_pass_with_no_new_offer(world, tenant):
    agent = tenant.agent().login()
    agent.phone.on_ring("timeout")
    number, _ = tenant.inbound_line(agent, retries=3, wait_timeout_sec=1800)
    stream = tenant.stream(after=0)
    world.plivo.call(number)
    stream.wait_for("offer.missed", agent_id=agent.id)
    for kind in ("route_agent", "next_round"):
        world.run_jobs(kind)

    def offers_after(seconds):
        world.travel(seconds)
        agent.set_status("Away")
        agent.set_status("Available")
        fence(tenant)
        return len(tenant.events(type="offer.created"))

    assert offers_after(290) == 1, "290 s after its offer, the agent's miss still holds"
    assert offers_after(11) == 2, "301 s after its offer, the agent is offered the caller again"


def test_a_round_that_falls_in_a_redis_loss_runs_within_one_sweep_of_redis_returning(world, tenant, monkeypatch):
    settings(tenant, ring_timeout_sec=120)  # the new round's ring outlasts the test, so only the loss's round counts
    monkeypatch.setattr(world.plivo, "time_factor", 1000.0)  # and outlasts a slow run: 120 s rings 33 h, not 12 s
    first, second = tenant.agent().login(), tenant.agent().login()
    for agent in (first, second):
        agent.phone.on_ring("ignore")
    number, _ = tenant.inbound_line(first, second, agents_at_once=1, retries=3, wait_timeout_sec=1800)
    stream = tenant.stream(after=0)
    world.plivo.call(number)
    stream.wait_for("offer.created")
    world.wait(world.plivo.idle, what="the ring's callbacks answered before Redis goes down")
    with world.redis_down():
        world.run_jobs("offer_timeout")
        world.advance(0)
    world.travel_until_rebuilt()
    fence(tenant)
    trail = [(e["type"], e["agent_id"], e["data"].get("reason")) for e in tenant.events() if e["type"][:6] != "call."]
    assert len(tenant.events(type="offer.created")) == 2, trail


def test_after_a_redis_loss_an_agent_on_a_call_stays_full_and_a_free_agent_takes_the_next_call(world, tenant):
    busy, free = tenant.agent("Busy").login(), tenant.agent("Free")
    busy.phone.on_ring("answer")
    number, _ = tenant.inbound_line(busy, free, agents_at_once=1, wait_timeout_sec=600)
    stream = tenant.stream(after=0)
    world.plivo.call(number, from_="+14155550251")
    first = hs_header(busy.phone.wait_ring(), "HsConversation")
    stream.wait_for("offer.accepted", conversation_id=first)
    free.login()
    with world.redis_down():
        pass
    world.travel_until_rebuilt()
    world.plivo.call(number, from_="+14155550252")
    second = hs_header(free.phone.wait_ring(), "HsConversation")
    assert offers(tenant, second) == [free.id] and len(busy.phone.legs) == 1


def test_a_call_ringing_through_a_redis_loss_is_offered_again_and_its_old_leg_answered_late_takes_the_call(
    world, tenant, monkeypatch
):
    settings(tenant, ring_timeout_sec=120)
    monkeypatch.setattr(world.plivo, "time_factor", 1000.0)  # the old leg rings on until it answers, however slow
    agent = tenant.agent().login()
    agent.phone.on_ring("ignore")
    number, _ = tenant.inbound_line(agent, wait_timeout_sec=1800)
    stream = tenant.stream(after=0)
    caller = world.plivo.call(number)
    stream.wait_for("offer.created")
    old, seat = agent.phone.wait_ring(), world.wait(lambda: caller.mpc, what="the caller seated")
    with world.redis_down():
        pass
    world.travel_until_rebuilt()
    fence(tenant)
    assert len(tenant.events(type="offer.created")) == 2, "offered again within one sweep, not at the ring timeout"
    recovered = agent.phone.wait_ring()
    old.answer()
    assert stream.wait_for("offer.accepted")["agent_id"] == agent.id
    withdrawn = stream.wait_for("offer.withdrawn")["data"]
    assert (withdrawn["offer_id"], withdrawn["reason"]) == (hs_header(recovered, "HsOffer"), "taken")
    recovered.wait_state("completed")
    world.wait(world.plivo.idle, what="the late leg's callbacks answered")
    conv = ok(tenant.api.get(f"/api/v1/conversations/{hs_header(old, 'HsConversation')}"))
    legs = {leg["call_uuid"]: leg["role"] for leg in conv["legs"]}
    assert conv["state"] == "active" and legs[old.call_uuid] == "primary"
    assert caller.state == "in_progress" and old.state == "in_progress" and caller.mpc == old.mpc == seat


@pytest.mark.parametrize("ends", ["closed_hours", "overflow", "voicemail"])
def test_a_call_that_hs_ends_reads_ended_by_system(world, tenant, ends):
    if ends == "overflow":
        agent = tenant.agent("A").login()
        agent.phone.on_ring("decline")
        number, _ = tenant.inbound_line(agent, retries=0, overflow_action=hangup_action("Sorry"))
    elif ends == "voicemail":
        q = tenant.queue(name="Support")
        hours_id = hours(tenant, weekly=False)["id"]
        number = line(tenant, queue_action(q), business_hours_id=hours_id, closed_action=voicemail_action(q))
    else:
        number = closed_line(tenant, hangup_action("We are closed"))
    stream = tenant.stream(after=0)
    caller = world.plivo.call(number)
    caller.wait_state("completed", timeout=10)
    run_jobs_until(world, lambda: seen(stream, "conversation.closed"), "the conversation closed")
    world.run_jobs()
    fence(tenant)
    assert [e["data"]["ended_by"] for e in tenant.events() if e["type"] == "call.ended"] == ["system"]
