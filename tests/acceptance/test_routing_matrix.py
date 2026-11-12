"""Who is offered a call and in what order, and what each overflow action does after each trigger."""

import pytest

from tests.acceptance.conftest import (
    connect,
    finish,
    hangup_action,
    hours,
    hs_header,
    lead,
    menu,
    ok,
    queue_action,
    replayed,
    seen,
    set_status,
    settings,
    unavailable,
    voicemail_action,
)


def line(tenant, queue: dict) -> str:
    number = tenant.number()
    tenant.route_number(number, queue_action(queue))
    return number


@pytest.mark.parametrize("status", ["Break", "Offline", "Away", "Lunch"])
def test_an_agent_who_is_not_available_is_never_offered_and_an_available_one_is(world, tenant, status):
    ok(tenant.api.post("/api/v1/agent-statuses", json={"name": "Lunch", "availability": False}))
    out, ready = tenant.agent("Out").login(), tenant.agent("Ready").login()
    ok(set_status(tenant, out, status))
    number, _ = tenant.inbound_line(out, ready)
    world.plivo.call(number)
    assert hs_header(ready.phone.wait_ring(), "HsKind") == "offer"
    world.wait(world.plivo.idle, what="the offer's dials settled")
    assert out.phone.legs == []


def test_a_call_with_only_unavailable_agents_rings_nobody_and_overflows(world, tenant):
    agents = [unavailable(tenant, f"Away-{n}") for n in "12"]
    ok(set_status(tenant, agents[1], "Break"))
    q = tenant.queue(agents=[a.id for a in agents], wait_timeout_sec=1, overflow_action=hangup_action("Nobody here"))
    stream = tenant.stream(after=0)
    caller = world.plivo.call(line(tenant, q))
    assert stream.wait_for("conversation.overflowed", timeout=10)["data"]["reason"] == "wait_timeout"
    world.wait(lambda: "Nobody here" in caller.said, what="the overflow greeting")
    assert not replayed(tenant, "offer.created") and [a.phone.legs for a in agents] == [[], []]


def wait_timeout(tenant):
    return {"agents": [unavailable(tenant).id], "wait_timeout_sec": 1}, "wait_timeout"


def assignment_failed(tenant):
    decliner = tenant.agent("Decliner").login()
    decliner.phone.on_ring("decline")
    return {"agents": [decliner.id], "retries": 0}, "assignment_failed"


def a_hangup(tenant, world):
    return hangup_action("Overflow hangup"), lambda caller, stream: "Overflow hangup" in caller.said


def a_forward(tenant, world):
    target, phone = lead(world)
    return {"type": "forward", "number": target}, lambda caller, stream: phone.legs


def a_menu(tenant, world):
    m = menu(tenant, {"1": hangup_action("One")}, greeting="Overflow menu")
    return {"type": "menu", "menu_id": m["id"]}, lambda caller, stream: "Overflow menu" in caller.said


def a_voicemail(tenant, world):
    q = tenant.queue()
    return voicemail_action(q, "Overflow voicemail"), lambda caller, stream: seen(stream, "voicemail.received")


def a_redirect(tenant, world):
    app = world.plivo.app("<Response><Speak>Overflow app</Speak></Response>")
    return {"type": "redirect", "url": app}, lambda caller, stream: "Overflow app" in caller.said


@pytest.mark.parametrize("trigger", [wait_timeout, assignment_failed])
@pytest.mark.parametrize("action", [a_hangup, a_forward, a_menu, a_voicemail, a_redirect])
def test_each_overflow_action_runs_after_each_trigger(world, tenant, trigger, action):
    fields, reason = trigger(tenant)
    overflow, done = action(tenant, world)
    q = tenant.queue(overflow_action=overflow, **fields)
    stream = tenant.stream(after=0)
    caller = world.plivo.call(line(tenant, q))
    assert stream.wait_for("conversation.overflowed", timeout=10)["data"]["reason"] == reason
    world.wait(lambda: done(caller, stream), timeout=10, what="the overflow action")


@pytest.mark.parametrize("action", [a_hangup, a_forward, a_menu, a_voicemail, a_redirect])
def test_each_overflow_action_runs_when_the_queue_is_full(world, tenant, action):
    overflow, done = action(tenant, world)
    q = tenant.queue(name="Small", agents=[unavailable(tenant).id], capacity=50, wait_timeout_sec=1800)
    ok(tenant.api.patch(f"/api/v1/queues/{q['id']}", json={"overflow_action": overflow}))
    number = line(tenant, q)
    stream = tenant.stream(after=0)
    for _ in range(50):
        world.plivo.call(number)
    world.wait(lambda: len(seen(stream, "conversation.queued")) == 50, timeout=30, what="50 waiting")
    caller = world.plivo.call(number)
    assert stream.wait_for("conversation.overflowed", reason="enqueue_failed")["data"]["queue_id"] == q["id"]
    world.wait(lambda: done(caller, stream), timeout=10, what="the overflow action")


def test_a_second_call_waits_while_the_agent_is_on_the_first_and_is_offered_when_it_ends(world, tenant):
    settings(tenant, wrap_up={"mode": "manual", "seconds": 1})
    first = connect(tenant)
    agent = first.agent
    number, _ = tenant.inbound_line(agent)
    stream = first.stream
    rings, offers = len(agent.phone.legs), len(replayed(tenant, "offer.created"))
    world.plivo.call(number)
    world.wait(lambda: len(seen(stream, "conversation.queued")) == 2, what="the second call waiting")
    world.wait(world.plivo.idle, what="the second call settled")
    assert len(agent.phone.legs) == rings and len(replayed(tenant, "offer.created")) == offers
    finish(first)
    assert hs_header(agent.phone.wait_ring(), "HsKind") == "offer"


def test_two_simultaneous_calls_go_to_two_available_agents_once_each(world, tenant):
    a, b = tenant.agent("A").login(), tenant.agent("B").login()
    number, _ = tenant.inbound_line(a, b)
    stream = tenant.stream(after=0)
    callers = [world.plivo.call(number, from_=f"+1415555030{n}") for n in range(2)]
    world.wait(lambda: len(seen(stream, "offer.accepted")) == 2, what="both calls answered")
    accepted = seen(stream, "offer.accepted")
    assert {e["agent_id"] for e in accepted} == {a.id, b.id}
    assert len({e["conversation_id"] for e in accepted}) == 2
    assert all(c.state == "in_progress" for c in callers)


@pytest.mark.parametrize(("order", "first"), [("fifo", 0), ("lifo", 1)])
def test_equal_priority_callers_are_dequeued_by_the_queue_order(world, tenant, order, first):
    agent = unavailable(tenant)
    q = tenant.queue(agents=[agent.id], dequeue_order=order, wait_timeout_sec=600)
    stream = tenant.stream(after=0)
    number, calls = line(tenant, q), []
    for n in range(2):
        world.plivo.call(number, from_=f"+1415555031{n}")
        queued = world.wait(lambda n=n: seen(stream, "conversation.queued")[n:], what="the caller waiting")
        world.wait(world.plivo.idle, what="the call settled in the queue")
        calls.append(queued[0]["conversation_id"])
    agent.phone.on_ring("ignore")
    agent.set_status("Available")
    assert hs_header(agent.phone.wait_ring(), "HsConversation") == calls[first]


def slot(start: str, end: str) -> dict:
    return {"start": start, "end": end}


@pytest.mark.parametrize(
    ("slots", "is_open"),
    [
        ([slot("09:00", "12:00"), slot("11:00", "14:00")], True),
        ([slot("11:00", "14:00"), slot("09:00", "12:00")], True),
        ([slot("00:00", "13:59"), slot("11:00", "14:00")], True),
        ([slot("09:00", "12:00"), slot("08:00", "10:00")], False),
    ],
    ids=["only the later slot", "only the earlier slot", "both slots", "a slot is closed at its end"],
)
def test_overlapping_slots_are_open_where_either_is_and_a_slot_is_closed_at_its_end(world, tenant, slots, is_open):
    """The local time is 12:00 to 13:00: the first case is past its first slot's end and inside the second."""
    h = hours(tenant, weekly=False, holiday=slots)
    assert ok(tenant.api.get(f"/api/v1/business-hours/{h['id']}/status"))["open"] is is_open
    number, q = tenant.number(), tenant.queue()
    tenant.route_number(number, queue_action(q), business_hours_id=h["id"], closed_action=hangup_action("Closed"))
    caller, stream = world.plivo.call(number), tenant.stream(after=0)
    assert stream.wait_for("conversation.started")["data"]["hours"] == ("open" if is_open else "closed")
    if is_open:
        assert stream.wait_for("conversation.queued")["data"]["queue_id"] == q["id"]
    else:
        caller.wait_state("completed")
        assert "Closed" in caller.said
