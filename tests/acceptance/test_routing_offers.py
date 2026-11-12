"""Who gets a contact: offers an agent loses, capacity and agents at once, sticky routing, the strategies'
counters, routing during wrap-up and its safety close, the order a freed agent takes waiting contacts in,
per-channel timeouts, queue capacity and place in line, a disabled queue, an enqueue's own route, what an offer
carries, ring failures, a text whose offer rounds run out, a monitoring supervisor's dial-out, RONA off, and the
statuses a sign-in, a sign-out and a disconnect set."""

import contextlib
import json
import random
import uuid
from datetime import datetime

import pytest

from tests.acceptance.channels import accept, close, decline, enqueue_text, offered, take, text_line
from tests.acceptance.conftest import (
    connect,
    dial,
    disposition,
    error,
    fence,
    finish,
    hangup_action,
    hs_header,
    lead,
    ok,
    queue_action,
    redirecting_line,
    registered_number,
    replayed,
    seen,
    set_status,
    settings,
    skill,
    team,
    unavailable,
    with_query,
    wrap_up,
)
from tests.acceptance.test_closed_search import hold, stamps
from tests.acceptance.test_tasks import create, later, resolve, task_of
from tests.harness import World

CUSTOMER = "+14155550701"


def line(tenant, queue: dict) -> str:
    number = tenant.number()
    tenant.route_number(number, queue_action(queue))
    return number


def queued(world, stream, number: str, from_: str) -> str:
    """Place a call from `from_` and return its conversation id once it waits in a queue."""
    world.plivo.call(number, from_=from_)
    return stream.wait_for("conversation.queued", **{"from": from_})["conversation_id"]


def status_name(tenant, agent) -> str:
    return ok(tenant.api.get(f"/api/v1/agents/{agent.id}/status"))["status"]["name"]


def done(tenant) -> str:
    return disposition(tenant, f"Done {uuid.uuid4().hex[:6]}")["id"]


def texted(world, tenant, agent, n: int = 1) -> None:
    """The agent takes and closes `n` SMS conversations on a line of its own."""
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, agent)
    did = done(tenant)
    for _ in range(n):
        customer = f"+1415{random.randint(1_000_000, 9_999_999)}"
        world.plivo.text(number, customer, "Hi")
        close(agent, take(stream, agent, **{"from": customer}), did, stream)


def turn(tenant, agent, cid: str) -> dict[str, float]:
    """When the timeline has the agent's turn on `cid` offered, accepted, in wrap-up and wrapped up, and when any
    agent first accepted `cid`."""
    t = stamps(tenant, cid)
    own = {
        "offered": "offer.created",
        "accepted": "offer.accepted",
        "wrap": "conversation.wrap_up_started",
        "wrapped": "conversation.wrapped_up",
    }
    return {k: t[e, agent.id] for k, e in own.items()} | {"first accepted": t["offer.accepted", None]}


def lowest(turns: dict, start: str, end: str):
    """The agent whose turn ran shorter from `start` to `end`, by more than a second plus what millisecond stamps can
    blur, since whole-second scores could tie a closer pair and the agent free longest takes a tie."""
    (pick, low), (_, high) = sorted(((a, t[end] - t[start]) for a, t in turns.items()), key=lambda p: p[1])
    assert high - low > 1.05, f"inconclusive: from {start} to {end} the turns took {low:.3f} s and {high:.3f} s"
    return pick


def rings_least_handle_time(world, tenant, turns: dict, start: str, *wrong: tuple[str, str]) -> None:
    """A call to the agents' queue rings the one whose turn ran shorter from `start` to wrap-up start. Each `wrong`
    measure must favor the other agent, or this history cannot tell it from the right one."""
    pick = lowest(turns, start, "wrap")
    [other] = [a for a in turns if a is not pick]
    for since, until in wrong:
        assert lowest(turns, since, until) is not pick, f"inconclusive: from {since} to {until} also favors {pick.id}"
    queue_line, _ = tenant.inbound_line(*turns, strategy="least_handle_time", agents_at_once=1)
    world.plivo.call(queue_line)
    pick.phone.wait_ring()
    assert other.phone.legs == []


def email(tenant, queue: dict, sender: str):
    body = {"text": "Invoice", "email": {"subject": "Invoice", "message_id": f"<{uuid.uuid4().hex}@customer.example>"}}
    return enqueue_text(tenant.api, queue, "email", sender, "help@acme.example", **body)


def served_last(world, tenant, caller: str, sticky: dict):
    """Usual and Last on a sticky queue, where Last took a first call from `caller` after Usual declined it.
    Returns (number, queue, usual, last)."""
    usual, last = tenant.agent("Usual").login(), tenant.agent("Last").login()
    number, queue = tenant.inbound_line(usual, last, agents_at_once=1, retries=1, sticky=sticky)
    usual.phone.on_ring("decline")
    stream = tenant.stream(after=0)
    call = world.plivo.call(number, from_=caller)
    cid = hs_header(last.phone.wait_ring(), "HsConversation")
    call.hangup()
    stream.wait_for("conversation.wrap_up_started", conversation_id=cid)
    ok(wrap_up(last, cid, done(tenant)))
    usual.phone.on_ring("answer")
    return number, queue, usual, last


def dialed_lead(world, tenant):
    """Dialer called a lead, who hung up. Returns (the lead's number, dialer, an agent who dialed no one). Other signs
    in first, so it is the queue's longest available agent: a call's end keeps the dialer's place."""
    other, dialer = tenant.agent("Other").login(), tenant.agent("Dialer").login()
    stream = tenant.stream(after=0)
    number, _ = lead(world)
    _, cid = dial(tenant, dialer, to=number)
    stream.wait_for("call.answered", conversation_id=cid)
    ok(tenant.api.post(f"/api/v1/calls/{cid}/hangup"))
    stream.wait_for("conversation.wrap_up_started", conversation_id=cid)
    ok(wrap_up(dialer, cid, done(tenant)))
    return number, dialer, other


def test_an_agent_who_loses_a_round_is_offered_the_next_waiting_contact(world, tenant):
    winner, loser = tenant.agent("Winner").login(), tenant.agent("Loser").login()
    stream = tenant.stream(after=0)
    shared, _ = text_line(tenant, winner, loser, agents_at_once=2)
    own, _ = text_line(tenant, loser)
    world.plivo.text(shared, CUSTOMER, "Hi")
    first = offered(stream, winner)
    offered(stream, loser, conversation_id=first["conversation_id"])
    world.plivo.text(own, "+14155550702", "Me too")
    waiting = stream.wait_for("conversation.queued", **{"from": "+14155550702"})["conversation_id"]
    ok(accept(winner, first))
    stream.wait_for("offer.withdrawn", agent_id=loser.id, reason="taken")
    offered(stream, loser, conversation_id=waiting)


def test_an_agent_whose_offer_a_leaving_caller_withdrew_is_offered_the_next_waiting_caller(world, tenant):
    a, b = tenant.agent("A").login(), tenant.agent("B").login()
    for agent in (a, b):
        agent.phone.on_ring("ignore")
    stream = tenant.stream(after=0)
    shared, _ = tenant.inbound_line(a, b, agents_at_once=2)
    own, _ = tenant.inbound_line(b, wait_timeout_sec=600)
    caller = world.plivo.call(shared)
    for agent in (a, b):
        agent.phone.wait_ring()
    waiting = queued(world, stream, own, "+14155550712")
    caller.hangup()
    stream.wait_for("offer.withdrawn", agent_id=b.id, reason="caller_left")
    assert hs_header(b.phone.wait_ring(), "HsConversation") == waiting


def test_an_agent_back_from_away_skips_the_waiting_text_it_declined_and_takes_the_next(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, agent, text={"wait_timeout_sec": 600})
    world.plivo.text(number, CUSTOMER, "Hi")
    first = offered(stream, agent)
    ok(decline(agent, first))
    stream.wait_for("offer.rejected", offer_id=first["data"]["offer_id"])
    world.wait(lambda: not world.advance(0), what="the work the decline set off to finish")
    assert len(replayed(tenant, "offer.created", conversation_id=first["conversation_id"])) == 1
    agent.set_status("Away")
    world.plivo.text(number, "+14155550703", "Me too")
    second = stream.wait_for("conversation.queued", **{"from": "+14155550703"})["conversation_id"]
    agent.set_status("Available")
    offered(stream, agent, conversation_id=second)
    assert len(replayed(tenant, "offer.created", conversation_id=first["conversation_id"])) == 1
    assert not replayed(tenant, "conversation.overflowed", conversation_id=first["conversation_id"])


def test_after_a_redis_loss_a_waiting_text_skips_the_agent_who_declined_it(world, tenant):
    declined, other = tenant.agent("Declined").login(), tenant.agent("Other").login()
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, declined, other, agents_at_once=1, text={"wait_timeout_sec": 600})
    world.plivo.text(number, CUSTOMER, "Hi")
    offer = offered(stream, declined)
    ok(decline(declined, offer))
    offered(stream, other, conversation_id=offer["conversation_id"])
    lose_redis(world)
    later = world.wait(
        lambda: seen(stream, "offer.created", conversation_id=offer["conversation_id"])[2:], what="the next round"
    )
    assert later[0]["agent_id"] == other.id


def test_a_channel_capacity_of_zero_still_holds_one_conversation(world, tenant):
    settings(tenant, capacity={"sms": 0})
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, agent)
    world.plivo.text(number, "+14155550721", "Hi")
    take(stream, agent)
    world.plivo.text(number, "+14155550722", "Me too")
    second = stream.wait_for("conversation.queued", **{"from": "+14155550722"})["conversation_id"]
    assert not replayed(tenant, "offer.created", conversation_id=second)


def test_a_queue_that_offers_to_zero_agents_at_once_rings_nobody(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    number, _ = tenant.inbound_line(agent, agents_at_once=0, wait_timeout_sec=600)
    cid = queued(world, stream, number, "+14155550761")
    assert not replayed(tenant, "offer.created", conversation_id=cid)
    assert agent.phone.legs == []


def test_sticky_offers_the_last_agent_without_the_queue_skills(world, tenant):
    number, queue, usual, last = served_last(world, tenant, "+14155550301", {"mode": "loose"})
    spanish = skill(tenant)
    ok(tenant.api.patch(f"/api/v1/agents/{usual.id}", json={"skills": [{"skill_id": spanish, "level": 7}]}))
    ok(tenant.api.patch(f"/api/v1/queues/{queue['id']}", json={"skills": [{"skill_id": spanish, "level": 5}]}))
    rings = len(usual.phone.legs)
    world.plivo.call(number, from_="+14155550301")
    last.phone.wait_ring()
    assert len(usual.phone.legs) == rings


def test_an_email_is_not_sticky(world, tenant):
    a, b = tenant.agent("A").login(), tenant.agent("B").login()
    stream = tenant.stream(after=0)
    queue = tenant.queue(agents=[a.id, b.id], agents_at_once=1, sticky={"mode": "loose"})
    cid = ok(email(tenant, queue, "ana@customer.example"))["conversation_id"]
    served = a if stream.wait_for("offer.created", conversation_id=cid)["agent_id"] == a.id else b
    close(served, take(stream, served, conversation_id=cid), done(tenant), stream)
    new = ok(email(tenant, queue, "ana@customer.example"))["conversation_id"]
    assert stream.wait_for("offer.created", conversation_id=new)["agent_id"] != served.id


def test_strict_sticky_to_an_agent_who_left_the_queue_routes_normally(world, tenant):
    number, queue, usual, last = served_last(world, tenant, "+14155550303", {"mode": "strict"})
    ok(tenant.api.patch(f"/api/v1/queues/{queue['id']}", json={"agents": [usual.id]}))
    rings = len(usual.phone.legs)
    world.plivo.call(number, from_="+14155550303")
    world.wait(lambda: len(usual.phone.legs) > rings, what="the queue's own agent to ring")
    assert len(last.phone.legs) == 1


def test_strict_sticky_to_a_deleted_agent_routes_normally(world, tenant):
    number, _, usual, last = served_last(world, tenant, "+14155550304", {"mode": "strict"})
    ok(tenant.api.delete(f"/api/v1/agents/{last.id}"))
    rings = len(usual.phone.legs)
    world.plivo.call(number, from_="+14155550304")
    world.wait(lambda: len(usual.phone.legs) > rings, what="the queue's other agent to ring")


def test_strict_sticky_fails_at_once_while_the_last_agent_is_away(world, tenant):
    number, _, usual, last = served_last(world, tenant, "+14155550305", {"mode": "strict"})
    last.set_status("Away")
    stream = tenant.stream(after=0)
    rings = len(usual.phone.legs)
    world.plivo.call(number, from_="+14155550305")
    stream.wait_for("conversation.overflowed", reason="assignment_failed")
    assert len(usual.phone.legs) == rings


def test_a_strict_sticky_text_whose_agent_is_away_leaves_the_queue_at_once(world, tenant):
    usual, last = tenant.agent("Usual").login(), tenant.agent("Last").login()
    stream = tenant.stream(after=0)
    number, queue = text_line(tenant, last, sticky={"mode": "strict"})
    world.plivo.text(number, CUSTOMER, "Hi")
    close(last, take(stream, last), done(tenant), stream)
    ok(tenant.api.patch(f"/api/v1/queues/{queue['id']}", json={"agents": [usual.id, last.id]}))
    last.set_status("Away")
    world.plivo.text(number, CUSTOMER, "Again")
    again = world.wait(lambda: seen(stream, "conversation.queued", **{"from": CUSTOMER})[1:], what="the second text")
    cid = again[0]["conversation_id"]
    assert stream.wait_for("conversation.overflowed", conversation_id=cid)["data"]["reason"] == "assignment_failed"
    assert not replayed(tenant, "offer.created", conversation_id=cid)


def test_outbound_sticky_needs_a_sticky_queue(world, tenant):
    settings(tenant, outbound_sticky={"enabled": True, "hours": 24})
    number, dialer, other = dialed_lead(world, tenant)
    queue_line, _ = tenant.inbound_line(other, dialer, agents_at_once=1, retries=1)
    rings = len(dialer.phone.legs)
    world.plivo.call(queue_line, from_=number)
    other.phone.wait_ring()
    assert len(dialer.phone.legs) == rings


def test_outbound_sticky_with_zero_hours_never_expires(world, tenant):
    settings(tenant, outbound_sticky={"enabled": True, "hours": 0})
    number, dialer, other = dialed_lead(world, tenant)
    queue_line, _ = tenant.inbound_line(other, dialer, agents_at_once=1, retries=1, sticky={"mode": "loose"})
    rings = len(dialer.phone.legs)
    world.plivo.call(queue_line, from_=number)
    world.wait(lambda: len(dialer.phone.legs) > rings, what="the dialer to ring")
    assert other.phone.legs == []


def test_outbound_sticky_with_zero_hours_still_holds_a_year_later():
    # A World of its own: a year's jump in the shared one runs every timer the earlier tests left.
    with contextlib.closing(World()) as world:
        tenant = world.new_tenant()
        settings(tenant, outbound_sticky={"enabled": True, "hours": 0})
        number, dialer, other = dialed_lead(world, tenant)
        world.travel(400 * 24 * 3600)
        for agent in (other, dialer):
            agent.login()
        queue_line, _ = tenant.inbound_line(other, dialer, agents_at_once=1, retries=1, sticky={"mode": "loose"})
        rings = len(dialer.phone.legs)
        world.plivo.call(queue_line, from_=number)
        world.wait(lambda: len(dialer.phone.legs) > rings, what="the dialer to ring")
        assert other.phone.legs == []


def test_an_agents_outbound_sms_makes_the_customers_next_text_sticky(world, tenant):
    settings(tenant, outbound_sticky={"enabled": True, "hours": 24})
    other, sender = tenant.agent("Other").login(), tenant.agent("Sender").login()
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, other, sender, agents_at_once=1, sticky={"mode": "loose"})
    start = {"channel": "sms", "agent_id": sender.id, "from": number, "to": CUSTOMER, "text": "Your order shipped"}
    cid = ok(tenant.api.post("/api/v1/conversations", json=start))["conversation_id"]
    close(sender, cid, done(tenant), stream)
    world.plivo.text(number, CUSTOMER, "Where is it?")
    reply = world.wait(lambda: seen(stream, "conversation.queued", **{"from": CUSTOMER}), what="the reply queued")
    assert stream.wait_for("offer.created", conversation_id=reply[0]["conversation_id"])["agent_id"] == sender.id


def test_a_queue_made_sticky_later_ignores_who_took_the_customer_before(world, tenant):
    number, queue, usual, last = served_last(world, tenant, "+14155550308", None)
    ok(tenant.api.patch(f"/api/v1/queues/{queue['id']}", json={"sticky": {"mode": "loose"}}))
    rings = len(usual.phone.legs)
    world.plivo.call(number, from_="+14155550308")
    world.wait(lambda: len(usual.phone.legs) > rings, what="the queue's longest idle agent to ring")
    assert len(last.phone.legs) == 1


def test_a_transfer_to_an_agent_outside_the_queue_keeps_the_queues_agent_sticky(world, tenant):
    settings(tenant, acw_blocks_routing=False)
    outside = tenant.agent("Outside").login()
    usual, last = tenant.agent("Usual").login(), tenant.agent("Last").login()
    number, _ = tenant.inbound_line(usual, last, agents_at_once=1, retries=1, sticky={"mode": "loose"})
    usual.phone.on_ring("decline")
    stream = tenant.stream(after=0)
    caller = world.plivo.call(number, from_="+14155550309")
    cid = hs_header(last.phone.wait_ring(), "HsConversation")
    stream.wait_for("offer.accepted", conversation_id=cid)
    cold = {"mode": "cold", "target": {"type": "agent", "agent_id": outside.id}}
    ok(last.api().post(f"/api/v1/calls/{cid}/transfers", json=cold))
    stream.wait_for("call.transferred", conversation_id=cid)
    caller.hangup()
    stream.wait_for("conversation.wrap_up_started", conversation_id=cid, agent_id=outside.id)
    usual.phone.on_ring("answer")
    rings = len(usual.phone.legs)
    world.plivo.call(number, from_="+14155550309")
    world.wait(lambda: len(last.phone.legs) > 1, what="the agent the queue holds to ring")
    assert len(usual.phone.legs) == rings


def test_an_inbound_sticky_agent_ends_the_customers_outbound_one():
    # A World of its own: a 2 h jump in the shared one runs every timer the earlier tests left.
    with contextlib.closing(World()) as world:
        tenant = world.new_tenant()
        settings(tenant, outbound_sticky={"enabled": True, "hours": 24}, acw_blocks_routing=False)
        number, dialer, other = dialed_lead(world, tenant)
        last = tenant.agent("Last").login()
        sticky = {"mode": "loose", "expiry_hours": 1}
        line, _ = tenant.inbound_line(other, dialer, last, agents_at_once=1, retries=2, sticky=sticky)
        for agent in (dialer, other):
            agent.phone.on_ring("decline")
        stream = tenant.stream(after=0)
        caller = world.plivo.call(line, from_=number)
        cid = hs_header(last.phone.wait_ring(), "HsConversation")
        stream.wait_for("offer.accepted", conversation_id=cid)
        caller.hangup()
        stream.wait_for("conversation.wrap_up_started", conversation_id=cid)
        world.travel(2 * 3600)
        for agent in (other, last, dialer):
            agent.phone.on_ring("answer")
            agent.login()
        rings = len(dialer.phone.legs)
        world.plivo.call(line, from_=number)
        world.wait(lambda: len(other.phone.legs) > 1, what="the queue's longest idle agent to ring")
        assert len(dialer.phone.legs) == rings


@pytest.mark.parametrize("lost", [False, True])
@pytest.mark.parametrize("channel", ["chat", "email"])
def test_an_agents_call_from_a_chat_or_email_makes_it_the_outbound_sticky_agent_of_the_number_it_dialed(
    world, tenant, channel, lost
):
    settings(tenant, outbound_sticky={"enabled": True, "hours": 24})
    usual, dialer = tenant.agent("Usual").login(), tenant.agent("Dialer").login()
    stream = tenant.stream(after=0)
    queue, sender = tenant.queue(agents=[dialer.id], sticky={"mode": "loose"}), uuid.uuid4().hex[:8]
    if channel == "email":
        cid = ok(email(tenant, queue, f"{sender}@customer.example"))["conversation_id"]
    else:
        cid = ok(enqueue_text(tenant.api, queue, "chat", sender, "support-inbox", text="Hi"))["conversation_id"]
    take(stream, dialer, conversation_id=cid)
    number, phone = lead(world)
    body = {"agent_id": dialer.id, "to": number, "from": registered_number(tenant), "conversation_id": cid}
    ok(tenant.api.post("/api/v1/calls", json=body))
    phone.wait_ring().hangup()
    stream.wait_for("call.ended", conversation_id=cid)
    close(dialer, cid, disposition(tenant)["id"], stream)
    line, _ = tenant.inbound_line(usual, dialer, agents_at_once=1, sticky={"mode": "loose"})
    if lost:
        lose_redis(world)
    rings = (len(usual.phone.legs), len(dialer.phone.legs))
    world.plivo.call(line, from_=number)
    world.wait(lambda: len(dialer.phone.legs) > rings[1], what="the agent who dialed the number to ring")
    assert len(usual.phone.legs) == rings[0]


@pytest.mark.parametrize("which", [0, 1])
def test_after_a_redis_loss_each_number_an_agent_dialed_from_one_chat_routes_back_to_that_agent(world, tenant, which):
    settings(tenant, outbound_sticky={"enabled": True, "hours": 24})
    usual, dialer = tenant.agent("Usual").login(), tenant.agent("Dialer").login()
    stream = tenant.stream(after=0)
    queue = tenant.queue(agents=[dialer.id], sticky={"mode": "loose"})
    cid = ok(enqueue_text(tenant.api, queue, "chat", uuid.uuid4().hex[:8], "support-inbox", text="Hi"))
    cid = cid["conversation_id"]
    take(stream, dialer, conversation_id=cid)
    numbers = []
    for _ in range(2):
        number, phone = lead(world)
        body = {"agent_id": dialer.id, "to": number, "from": registered_number(tenant), "conversation_id": cid}
        ok(tenant.api.post("/api/v1/calls", json=body))
        phone.wait_ring().hangup()
        stream.wait_for("call.ended", conversation_id=cid, to=number)
        numbers.append(number)
    close(dialer, cid, disposition(tenant)["id"], stream)
    line, _ = tenant.inbound_line(usual, dialer, agents_at_once=1, sticky={"mode": "loose"})
    lose_redis(world)
    rings = (len(usual.phone.legs), len(dialer.phone.legs))
    world.plivo.call(line, from_=numbers[which])
    world.wait(lambda: len(dialer.phone.legs) > rings[1], what="the agent who dialed the number to ring")
    assert len(usual.phone.legs) == rings[0]


def test_after_a_redis_loss_a_returning_caller_reaches_its_sticky_agent(world, tenant):
    number, _, usual, last = served_last(world, tenant, "+14155550310", {"mode": "loose"})
    with world.redis_down():
        pass
    world.travel_until_rebuilt()
    rings = len(usual.phone.legs)
    world.plivo.call(number, from_="+14155550310")
    world.wait(lambda: len(last.phone.legs) > 1, what="the sticky agent to ring")
    assert len(usual.phone.legs) == rings


def lose_redis(world) -> None:
    with world.redis_down():
        pass
    world.travel_until_rebuilt()


def test_after_a_redis_loss_with_nobody_signed_in_a_call_waits_out_its_queue_and_a_text_queues(world, tenant):
    number, _ = text_line(tenant, tenant.agent(), wait_timeout_sec=1, overflow_action=hangup_action("Nobody is free"))
    stream = tenant.stream(after=0)
    lose_redis(world)
    world.plivo.call(number, from_="+14155550361")
    cid = stream.wait_for("conversation.queued", **{"from": "+14155550361"})["conversation_id"]
    over = stream.wait_for("conversation.overflowed", conversation_id=cid, timeout=10)["data"]
    assert (over["reason"], over["action"]["type"]) == ("wait_timeout", "hangup")
    world.plivo.text(number, "+14155550362", "Hi")
    stream.wait_for("conversation.queued", **{"from": "+14155550362"})


def test_after_a_redis_loss_a_queue_made_sticky_later_still_ignores_who_took_the_customer_before(world, tenant):
    number, queue, usual, last = served_last(world, tenant, "+14155550311", None)
    ok(tenant.api.patch(f"/api/v1/queues/{queue['id']}", json={"sticky": {"mode": "loose"}}))
    lose_redis(world)
    rings = len(usual.phone.legs)
    world.plivo.call(number, from_="+14155550311")
    world.wait(lambda: len(usual.phone.legs) > rings, what="the queue's longest idle agent to ring")
    assert len(last.phone.legs) == 1


@pytest.mark.parametrize("then", ["agent_deleted", "queue_not_sticky"])
def test_after_a_redis_loss_an_inbound_sticky_agent_still_ends_the_outbound_one(world, tenant, then):
    settings(tenant, outbound_sticky={"enabled": True, "hours": 24}, acw_blocks_routing=False)
    number, dialer, other = dialed_lead(world, tenant)
    last = tenant.agent("Last").login()
    line, queue = tenant.inbound_line(other, dialer, last, agents_at_once=1, retries=2, sticky={"mode": "loose"})
    for agent in (dialer, other):
        agent.phone.on_ring("decline")
    stream = tenant.stream(after=0)
    caller = world.plivo.call(line, from_=number)
    cid = hs_header(last.phone.wait_ring(), "HsConversation")
    stream.wait_for("offer.accepted", conversation_id=cid)
    caller.hangup()
    stream.wait_for("conversation.wrap_up_started", conversation_id=cid)
    ok(wrap_up(last, cid, done(tenant)))
    if then == "agent_deleted":
        ok(tenant.api.delete(f"/api/v1/agents/{last.id}"))
    else:
        ok(tenant.api.patch(f"/api/v1/queues/{queue['id']}", json={"sticky": None}))
    for agent in (dialer, other):
        agent.phone.on_ring("answer")
    other_line, _ = tenant.inbound_line(other, dialer, agents_at_once=1, retries=1, sticky={"mode": "loose"})
    lose_redis(world)
    rings = (len(other.phone.legs), len(dialer.phone.legs))
    world.plivo.call(other_line, from_=number)
    world.wait(lambda: len(other.phone.legs) > rings[0], what="the queue's longest idle agent to ring")
    assert len(dialer.phone.legs) == rings[1]


@pytest.mark.parametrize("ends", ["decline", "ignore"])
def test_a_strict_sticky_call_its_agent_declines_or_misses_fails_without_ringing_anyone_else(world, tenant, ends):
    number, queue, usual, last = served_last(world, tenant, "+14155550312", {"mode": "strict"})
    ok(tenant.api.patch(f"/api/v1/queues/{queue['id']}", json={"wait_timeout_sec": 600}))
    last.phone.on_ring(ends)
    stream = tenant.stream(after=0)
    rings = len(usual.phone.legs)
    world.plivo.call(number, from_="+14155550312")
    cid = hs_header(world.wait(lambda: last.phone.legs[1:], what="the sticky agent to ring")[0], "HsConversation")
    offer = stream.wait_for("offer.created", conversation_id=cid)
    if ends == "ignore":
        world.advance(offer["data"]["ring_timeout_sec"] + 1)
    assert stream.wait_for("conversation.overflowed", conversation_id=cid)["data"]["reason"] == "assignment_failed"
    assert len(usual.phone.legs) == rings
    assert len(replayed(tenant, "offer.created", conversation_id=cid)) == 1


@pytest.mark.parametrize("ends", ["decline", "timeout"])
def test_a_strict_sticky_text_its_agent_declines_or_misses_leaves_the_queue_at_once(world, tenant, ends):
    usual, last = tenant.agent("Usual").login(), tenant.agent("Last").login()
    stream = tenant.stream(after=0)
    number, queue = text_line(tenant, last, sticky={"mode": "strict"})
    world.plivo.text(number, CUSTOMER, "Hi")
    close(last, take(stream, last), done(tenant), stream)
    ok(tenant.api.patch(f"/api/v1/queues/{queue['id']}", json={"agents": [usual.id, last.id]}))
    world.plivo.text(number, CUSTOMER, "Again")
    again = world.wait(lambda: seen(stream, "conversation.queued", **{"from": CUSTOMER})[1:], what="the second text")
    cid = again[0]["conversation_id"]
    offer = offered(stream, last, conversation_id=cid)
    if ends == "decline":
        ok(decline(last, offer))
    else:
        world.advance(offer["data"]["ring_timeout_sec"] + 1)
    assert stream.wait_for("conversation.overflowed", conversation_id=cid)["data"]["reason"] == "assignment_failed"
    assert len(replayed(tenant, "offer.created", conversation_id=cid)) == 1


def test_fewest_handled_counts_texts_not_calls(world, tenant):
    settings(tenant, wrap_up={"mode": "manual", "seconds": 1})
    caller, texter = tenant.agent("Calls").login(), tenant.agent("Texts").login()
    texted(world, tenant, texter)
    for _ in range(2):
        finish(connect(tenant, caller))
    number, _ = tenant.inbound_line(caller, texter, strategy="fewest_handled", agents_at_once=1)
    world.plivo.call(number)
    caller.phone.wait_ring()
    assert texter.phone.legs == []


def test_a_break_does_not_reset_fewest_handled(world, tenant):
    a, b = tenant.agent("A").login(), tenant.agent("B").login()
    texted(world, tenant, b, 2)
    texted(world, tenant, a)
    b.set_status("Break")
    b.set_status("Available")
    number, _ = tenant.inbound_line(a, b, strategy="fewest_handled", agents_at_once=1)
    world.plivo.call(number)
    a.phone.wait_ring()
    assert b.phone.legs == []


def test_fewest_handled_counts_only_conversations_the_agent_took(world, tenant):
    settings(tenant, capacity={"sms": 2})
    worker, joiner, helper = (tenant.agent(name).login() for name in ("Worker", "Joiner", "Helper"))
    texted(world, tenant, helper)
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, worker)
    for customer in ("+14155550341", "+14155550342"):
        world.plivo.text(number, customer, "Hi")
        cid = take(stream, worker, **{"from": customer})
        body = {"target": {"type": "agent", "agent_id": joiner.id}}
        joined = ok(worker.api().post(f"/api/v1/conversations/{cid}/participants", json=body))
        ok(accept(joiner, offered(stream, joiner, conversation_id=cid)))
        ok(worker.api().delete(f"/api/v1/conversations/{cid}/participants/{joined['participant_id']}"))
    queue_line, _ = tenant.inbound_line(helper, joiner, strategy="fewest_handled", agents_at_once=1)
    world.plivo.call(queue_line)
    joiner.phone.wait_ring()
    assert helper.phone.legs == []


def test_fewest_handled_skips_a_text_the_agent_transferred_away(world, tenant):
    a, b = tenant.agent("A").login(), tenant.agent("B").login()
    texted(world, tenant, b)
    stream = tenant.stream(after=0)
    world.plivo.text(text_line(tenant, a)[0], CUSTOMER, "Hi")
    cid = take(stream, a)
    billing = tenant.queue(name="Billing", agents=[unavailable(tenant).id])
    ok(
        a.api().post(
            f"/api/v1/conversations/{cid}/transfers", json={"target": {"type": "queue", "queue_id": billing["id"]}}
        )
    )
    stream.wait_for("conversation.wrap_up_started", conversation_id=cid, agent_id=a.id)
    ok(wrap_up(a, cid, done(tenant)))
    number, _ = tenant.inbound_line(b, a, strategy="fewest_handled", agents_at_once=1)
    world.plivo.call(number)
    a.phone.wait_ring()
    assert b.phone.legs == []


def test_fewest_handled_counts_a_completed_task_and_not_a_reassigned_one(world, tenant):
    b = tenant.agent("B").login()
    stream = tenant.stream(after=0)
    finished = ok(create(tenant.api, target={"type": "agent", "agent_id": b.id}))
    world.wait(lambda: task_of(tenant, finished["id"])["routing_status"] == "accepted", what="B to hold its task")
    ok(resolve(b, finished["id"]))
    stream.wait_for("conversation.wrap_up_started", conversation_id=finished["conversation_id"])
    ok(wrap_up(b, finished["conversation_id"], done(tenant)))
    a, c = tenant.agent("A").login(), tenant.agent("C").login()
    taken = ok(create(tenant.api, target={"type": "agent", "agent_id": a.id}))
    world.wait(lambda: task_of(tenant, taken["id"])["routing_status"] == "accepted", what="A to hold its task")
    ok(tenant.api.patch(f"/api/v1/tasks/{taken['id']}", json={"target": {"type": "agent", "agent_id": c.id}}))
    number, _ = tenant.inbound_line(b, a, strategy="fewest_handled", agents_at_once=1)
    world.plivo.call(number)
    a.phone.wait_ring()
    assert b.phone.legs == []


def test_a_break_and_a_finished_call_keep_an_agents_place_in_an_availability_line(world, tenant):
    first, second = tenant.agent("First").login(), tenant.agent("Second").login()
    first.set_status("Break")
    first.set_status("Available")
    finish(connect(tenant, first))
    number, _ = tenant.inbound_line(second, first, agents_at_once=1)
    world.plivo.call(number)
    first.phone.wait_ring()
    assert second.phone.legs == []


def test_a_closed_text_sends_an_agent_to_the_back_of_an_availability_line(world, tenant):
    first, second = tenant.agent("First").login(), tenant.agent("Second").login()
    texted(world, tenant, first)
    number, _ = tenant.inbound_line(first, second, agents_at_once=1)
    world.plivo.call(number)
    second.phone.wait_ring()
    assert first.phone.legs == []


def test_fewest_handled_resets_at_sign_in_on_texts(world, tenant):
    a, b = tenant.agent("A").login(), tenant.agent("B").login()
    texted(world, tenant, b, 2)
    texted(world, tenant, a)
    ok(set_status(tenant, b, "Offline"))
    b.login()
    number, _ = tenant.inbound_line(a, b, strategy="fewest_handled", agents_at_once=1)
    world.plivo.call(number)
    b.phone.wait_ring()
    assert a.phone.legs == []


def test_least_handle_time_prefers_less_time_on_texts(world, tenant):
    """Slow handled fewer texts and has been idle longer, so only the time spent on them puts Quick first."""
    slow, quick, helper = (tenant.agent(name).login() for name in ("Slow", "Quick", "Helper"))
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, slow)
    world.plivo.text(number, "+14155550231", "Hi")
    held = take(stream, slow)
    texted(world, tenant, helper, 2)
    close(slow, held, done(tenant), stream)
    texted(world, tenant, quick, 2)
    queue_line, _ = tenant.inbound_line(slow, quick, strategy="least_handle_time", agents_at_once=1)
    world.plivo.call(queue_line)
    quick.phone.wait_ring()
    assert slow.phone.legs == []


def test_least_handle_time_runs_from_acceptance_to_wrap_up_start(world, tenant):
    """Late let its offer wait and then sat in wrap-up, so counting either wait would put Steady first."""
    late, steady = tenant.agent("Late").login(), tenant.agent("Steady").login()
    stream = tenant.stream(after=0)
    world.plivo.text(text_line(tenant, late)[0], CUSTOMER, "Hi")
    offer = offered(stream, late)
    hold(world, 4, "the offer to wait")
    ok(accept(late, offer))
    cid = offer["conversation_id"]
    ok(late.api().post(f"/api/v1/conversations/{cid}/close"))
    stream.wait_for("conversation.wrap_up_started", conversation_id=cid, agent_id=late.id)
    world.plivo.text(text_line(tenant, steady)[0], "+14155550232", "Hi")
    held = take(stream, steady)
    hold(world, 2, "Steady's text")
    close(steady, held, done(tenant), stream)
    hold(world, 2, "Late's wrap-up")
    ok(wrap_up(late, cid, done(tenant)))
    turns = {late: turn(tenant, late, cid), steady: turn(tenant, steady, held)}
    rings_least_handle_time(world, tenant, turns, "accepted", ("offered", "wrap"), ("accepted", "wrapped"))


def pass_on(world, tenant, stream, agent, target, cid: str, ring: float = 0) -> None:
    """`agent` transfers text `cid` to `target`, who lets the offer ring `ring` seconds before it takes it, and then
    `agent` wraps up."""
    body = {"target": {"type": "agent", "agent_id": target.id}}
    ok(agent.api().post(f"/api/v1/conversations/{cid}/transfers", json=body))
    offer = offered(stream, target, conversation_id=cid)
    hold(world, ring, "the transfer's ring")
    ok(accept(target, offer))
    stream.wait_for("conversation.wrap_up_started", conversation_id=cid, agent_id=agent.id)
    ok(wrap_up(agent, cid, done(tenant)))


def test_least_handle_time_counts_a_transferred_text_from_the_transfer(world, tenant):
    """First held Second's text longer and Second sat in wrap-up longer, while Third let its offer ring, so counting
    from the first acceptance, from the target's acceptance or to the wrap-up end would put Third first."""
    first, second, third = (tenant.agent(name).login() for name in ("First", "Second", "Third"))
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, first)
    world.plivo.text(number, CUSTOMER, "Hi")
    early = take(stream, first, **{"from": CUSTOMER})
    hold(world, 9, "First's text")
    pass_on(world, tenant, stream, first, second, early)
    hold(world, 4.5, "Second's text")
    ok(second.api().post(f"/api/v1/conversations/{early}/close"))
    stream.wait_for("conversation.wrap_up_started", conversation_id=early, agent_id=second.id)
    world.plivo.text(number, "+14155550233", "Hi")
    late = take(stream, first, **{"from": "+14155550233"})
    pass_on(world, tenant, stream, first, third, late, ring=9)
    close(third, late, done(tenant), stream)
    ok(wrap_up(second, early, done(tenant)))
    turns = {second: turn(tenant, second, early), third: turn(tenant, third, late)}
    wrong = ("accepted", "wrap"), ("first accepted", "wrap"), ("offered", "wrapped")
    rings_least_handle_time(world, tenant, turns, "offered", *wrong)


def test_least_handle_time_counts_texts_not_calls(world, tenant):
    caller, texter = tenant.agent("Calls").login(), tenant.agent("Texts").login()
    live = connect(tenant, caller)
    texted(world, tenant, texter)
    finish(live)
    number, _ = tenant.inbound_line(caller, texter, strategy="least_handle_time", agents_at_once=1)
    world.plivo.call(number)
    caller.phone.wait_ring()
    assert texter.phone.legs == []


def test_an_agent_in_wrap_up_is_offered_when_acw_does_not_block(world, tenant):
    settings(tenant, acw_blocks_routing=False, wrap_up={"mode": "manual", "seconds": 300})
    live = connect(tenant)
    live.caller.hangup()
    live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id)
    world.plivo.call(live.number)
    assert hs_header(live.agent.phone.wait_ring(), "HsKind") == "offer"


def test_with_acw_not_blocking_a_closed_sms_frees_its_slot_at_wrap_up(world, tenant):
    settings(tenant, acw_blocks_routing=False)
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, agent)
    world.plivo.text(number, "+14155550331", "Hi")
    cid = take(stream, agent)
    ok(agent.api().post(f"/api/v1/conversations/{cid}/close"))
    stream.wait_for("conversation.wrap_up_started", conversation_id=cid)
    world.plivo.text(number, "+14155550332", "Me too")
    offered(stream, agent, **{"from": "+14155550332"})


def test_an_automatic_text_wrap_up_the_desk_never_submits_closes_at_the_safety_close(world, tenant):
    settings(tenant, wrap_up={"mode": "auto", "seconds": 60})
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    world.plivo.text(text_line(tenant, agent)[0], CUSTOMER, "Hi")
    cid = take(stream, agent)
    ok(agent.api().post(f"/api/v1/conversations/{cid}/close"))
    stream.wait_for("conversation.wrap_up_started", conversation_id=cid)
    world.advance(60 + 300 - 30)
    assert not replayed(tenant, "conversation.wrapped_up", conversation_id=cid)
    world.advance(60 + 300 + 5)
    assert stream.wait_for("conversation.wrapped_up", conversation_id=cid)["data"]["skipped"]


def test_a_freed_agent_takes_fifo_callers_before_lifo_whatever_the_priority(world, tenant):
    settings(tenant, wrap_up={"mode": "manual", "seconds": 300})
    live = connect(tenant)
    fifo, lifo = (
        line(tenant, tenant.queue(agents=[live.agent.id], priority=p, dequeue_order=o, wait_timeout_sec=600))
        for p, o in ((9, "fifo"), (1, "lifo"))
    )
    first = queued(world, live.stream, fifo, "+14155550311")
    queued(world, live.stream, lifo, "+14155550312")
    live.caller.hangup()
    live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id)
    ok(wrap_up(live.agent, live.id, done(tenant)))
    assert hs_header(live.agent.phone.wait_ring(), "HsConversation") == first


def test_a_queue_times_out_each_text_channel_at_its_own_wait(world, tenant):
    agent = unavailable(tenant)
    stream = tenant.stream(after=0)
    number, queue = text_line(tenant, agent, text={"wait_timeout_sec": {"sms": 60, "email": 86400}})
    world.plivo.text(number, CUSTOMER, "Hello?")
    sms = stream.wait_for("conversation.queued", **{"from": CUSTOMER})["conversation_id"]
    mail = ok(email(tenant, queue, "ana@customer.example"))["conversation_id"]
    stream.wait_for("conversation.queued", conversation_id=mail)
    world.advance(61)
    assert stream.wait_for("conversation.overflowed", conversation_id=sms)["data"]["reason"] == "wait_timeout"
    assert not replayed(tenant, "conversation.overflowed", conversation_id=mail)
    world.advance(86400 - 60)
    assert not replayed(tenant, "conversation.overflowed", conversation_id=mail)
    world.advance(86400 + 1)
    assert stream.wait_for("conversation.overflowed", conversation_id=mail)["data"]["reason"] == "wait_timeout"


def test_a_queue_times_out_tasks_and_callbacks_each_at_its_own_wait(world, tenant):
    stream = tenant.stream(after=0)
    queue = tenant.queue(agents=[unavailable(tenant).id], text={"wait_timeout_sec": {"task": 60, "callback": 600}})
    target = {"type": "queue", "queue_id": queue["id"]}
    task = ok(create(tenant.api, type="task", target=target))["conversation_id"]
    callback = ok(create(tenant.api, type="callback", target=target, customer=CUSTOMER))["conversation_id"]
    world.advance(61)
    assert stream.wait_for("conversation.overflowed", conversation_id=task)["data"]["reason"] == "wait_timeout"
    assert not replayed(tenant, "conversation.overflowed", conversation_id=callback)
    world.advance(601)
    assert stream.wait_for("conversation.overflowed", conversation_id=callback)["data"]["reason"] == "wait_timeout"


def test_each_text_channel_offers_for_its_own_timeout(world, tenant):
    settings(tenant, text={"offer_timeout_sec": {"sms": 20, "email": 600}})
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    number, queue = text_line(tenant, agent)
    world.plivo.text(number, CUSTOMER, "Hi")
    sms = offered(stream, agent)
    mail = offered(stream, agent, conversation_id=ok(email(tenant, queue, "ana@customer.example"))["conversation_id"])
    assert (sms["data"]["ring_timeout_sec"], mail["data"]["ring_timeout_sec"]) == (20, 600)
    world.advance(21)
    stream.wait_for("offer.missed", offer_id=sms["data"]["offer_id"])
    assert not replayed(tenant, "offer.missed", offer_id=mail["data"]["offer_id"])
    world.advance(600 - 30)
    assert not replayed(tenant, "offer.missed", offer_id=mail["data"]["offer_id"])
    world.advance(600 + 1)
    stream.wait_for("offer.missed", offer_id=mail["data"]["offer_id"])


def test_waiting_emails_do_not_fill_a_queue(world, tenant):
    queue = tenant.queue(agents=[unavailable(tenant).id], capacity=50)
    stream = tenant.stream(after=0)
    for i in range(51):
        ok(email(tenant, queue, f"customer{i}@customer.example"))
    world.wait(lambda: len(seen(stream, "conversation.queued")) == 51, timeout=30, what="51 emails waiting")
    assert not replayed(tenant, "conversation.overflowed")


def app_held(world, tenant, from_: str) -> tuple[str, object]:
    """A call the tenant's own Plivo app holds, as a caller's flow does before it hands the call to a queue."""
    number = tenant.number()
    app = '<Response><GetDigits timeout="60"><Speak>One moment</Speak></GetDigits></Response>'
    tenant.route_number(number, {"type": "redirect", "url": world.plivo.app(app)})
    caller = world.plivo.call(number, from_=from_)
    world.wait(lambda: "One moment" in caller.said, what="the tenant app holds the call")
    return number, caller


def test_a_disabled_queue_refuses_new_contacts_as_enqueue_failed_and_keeps_routing_those_it_holds(world, tenant):
    agent = unavailable(tenant)
    q = tenant.queue(agents=[agent.id], wait_timeout_sec=600, overflow_action=hangup_action("Closed"))
    number = line(tenant, q)
    stream = tenant.stream(after=0)
    waiting = queued(world, stream, number, CUSTOMER)
    assert ok(tenant.api.patch(f"/api/v1/queues/{q['id']}", json={"enabled": False}))["enabled"] is False
    caller = world.plivo.call(number, from_="+14155550702")
    assert stream.wait_for("conversation.overflowed", reason="enqueue_failed")["data"]["queue_id"] == q["id"]
    world.wait(lambda: "Closed" in caller.said, what="the overflow action")
    error(enqueue_text(tenant.api, q, "chat", "visitor-71", "inbox", text="Hi"), 409, "enqueue_failed")
    chat = stream.wait_for("conversation.started", channel="chat")["conversation_id"]
    stream.wait_for("conversation.overflowed", conversation_id=chat, reason="enqueue_failed")
    app, held = app_held(world, tenant, "+14155550703")
    body = {"call_uuid": held.call_uuid, "from": "+14155550703", "to": app}
    error(tenant.key_with("calls").post(q["enqueue_url"], json=body), 409, "enqueue_failed")
    [call] = ok(tenant.api.get("/api/v1/conversations", params={"call_uuid": held.call_uuid}))["data"]
    stream.wait_for("conversation.overflowed", conversation_id=call["id"], reason="enqueue_failed")
    assert ok(tenant.api.get(f"/api/v1/queues/{q['id']}/stats"))["waiting"] == 1
    agent.set_status("Available")
    stream.wait_for("offer.accepted", conversation_id=waiting)


def test_a_queue_a_chat_waits_in_refuses_its_delete_until_an_agent_takes_the_chat_even_once_disabled(world, tenant):
    agent = unavailable(tenant)
    stream, q = tenant.stream(after=0), tenant.queue(agents=[agent.id])
    cid = ok(enqueue_text(tenant.api, q, "chat", "visitor-qd", "support-inbox", text="Hi"))["conversation_id"]
    ok(tenant.api.patch(f"/api/v1/queues/{q['id']}", json={"enabled": False}))
    assert error(tenant.api.delete(f"/api/v1/queues/{q['id']}"), 409, "queue_in_use")["details"] == {"waiting": 1}
    agent.set_status("Available")
    take(stream, agent, conversation_id=cid)
    assert tenant.api.delete(f"/api/v1/queues/{q['id']}").status_code == 204


def test_a_task_scheduled_into_a_queue_disabled_meanwhile_fails_its_enqueue_at_its_time(world, tenant):
    q = tenant.queue(agents=[tenant.agent().login().id])
    stream = tenant.stream(after=0)
    task = ok(create(tenant.api, target={"type": "queue", "queue_id": q["id"]}, scheduled_at=later(1)))
    ok(tenant.api.patch(f"/api/v1/queues/{q['id']}", json={"enabled": False}))
    world.advance(3600 + 60)
    cid = task["conversation_id"]
    assert stream.wait_for("conversation.overflowed", conversation_id=cid)["data"]["reason"] == "enqueue_failed"
    assert stream.wait_for("conversation.closed", conversation_id=cid)["data"]["result"] == "enqueue_failed"
    assert task_of(tenant, task["id"])["status"] == "cancelled"


def test_a_text_enqueue_with_its_own_route_is_offered_by_that_route_alone(world, tenant):
    sales, spanish = team(tenant, "Sales"), skill(tenant)
    own = tenant.agent("Queue's own", skills=[{"skill_id": spanish, "level": 5}]).login()
    rep = tenant.agent("Rep", teams=[sales])
    q = tenant.queue(agents=[own.id], skills=[{"skill_id": spanish, "level": 5}])
    stream = tenant.stream(after=0)
    route = {"teams": [sales], "auto_accept": {"enabled": True, "delay": 2}}
    cid = ok(enqueue_text(tenant.api, q, "chat", "visitor-72", "inbox", text="Hi", route=route))["conversation_id"]
    stream.wait_for("conversation.queued", conversation_id=cid)
    rep.login()
    assert offered(stream, rep, conversation_id=cid)["data"]["auto_accept"] is True
    assert not replayed(tenant, "offer.created", agent_id=own.id)
    nobody = enqueue_text(tenant.api, q, "chat", "visitor-73", "inbox", text="Hi", route={"strategy": "simultaneous"})
    error(nobody, 422)


def test_a_calls_json_enqueue_with_its_own_route_rings_that_route_alone(world, tenant):
    sales = team(tenant, "Sales")
    own, rep = tenant.agent("Queue's own").login(), tenant.agent("Rep", teams=[sales]).login()
    q = tenant.queue(agents=[own.id])
    number, caller = app_held(world, tenant, CUSTOMER)
    route = {"teams": [sales], "auto_accept": {"enabled": True, "delay": 2}}
    body = {"call_uuid": caller.call_uuid, "from": CUSTOMER, "to": number, "route": route}
    ok(tenant.key_with("calls").post(q["enqueue_url"], json=body))
    assert hs_header(rep.phone.wait_ring(), "HsAutoAnswer") == "2"
    assert not replayed(tenant, "offer.created", agent_id=own.id)


def test_an_enqueues_own_route_takes_a_flow_nodes_defaults_loosely_sticky_with_no_extra_round(world, tenant):
    sales = team(tenant, "Sales")
    last, other = (tenant.agent(name, teams=[sales]).login() for name in ("Last", "Other"))
    q = tenant.queue(agents=[last.id, other.id], agents_at_once=1)
    stream = tenant.stream(after=0)
    route = {"teams": [sales], "agents_at_once": 1, "auto_accept": {"enabled": True, "delay": 100}}
    error(tenant.api.patch(f"/api/v1/queues/{q['id']}", json={"auto_accept": route["auto_accept"]}), 422)
    first = ok(enqueue_text(tenant.api, q, "chat", "visitor-75", "inbox", text="Hi", route=route))["conversation_id"]
    close(last, take(stream, last, conversation_id=first), done(tenant), stream)
    again = ok(enqueue_text(tenant.api, q, "chat", "visitor-75", "inbox", text="Hi", route=route))["conversation_id"]
    ok(decline(last, offered(stream, last, conversation_id=again)))
    assert stream.wait_for("conversation.overflowed", conversation_id=again)["data"]["reason"] == "assignment_failed"
    assert not replayed(tenant, "offer.created", agent_id=other.id)


def test_a_waiting_call_keeps_the_route_it_was_queued_with_when_its_queue_is_edited(world, tenant):
    first, added = unavailable(tenant, "First"), unavailable(tenant, "Added")
    q = tenant.queue(agents=[first.id], wait_timeout_sec=600)
    number = line(tenant, q)
    stream = tenant.stream(after=0)
    waiting = queued(world, stream, number, CUSTOMER)
    ok(tenant.api.patch(f"/api/v1/queues/{q['id']}", json={"agents": [added.id]}))
    added.set_status("Available")
    assert not replayed(tenant, "offer.created", agent_id=added.id)
    first.set_status("Available")
    stream.wait_for("offer.accepted", conversation_id=waiting, agent_id=first.id)


def test_a_caller_moved_on_by_a_wait_timeout_keeps_its_place_in_the_next_queue(world, tenant):
    agent = unavailable(tenant)
    stream = tenant.stream(after=0)
    second = tenant.queue(name="Second", agents=[agent.id], wait_timeout_sec=0)
    first = tenant.queue(name="First", wait_timeout_sec=60, overflow_action=queue_action(second, hold_position=True))
    early = queued(world, stream, line(tenant, first), "+14155550741")
    queued(world, stream, line(tenant, second), "+14155550742")
    world.advance(61)
    stream.wait_for("conversation.queued", conversation_id=early, queue_id=second["id"])
    agent.set_status("Available")
    assert hs_header(agent.phone.wait_ring(), "HsConversation") == early


def test_a_text_offer_carries_the_conversation_number(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    world.plivo.text(text_line(tenant, agent)[0], CUSTOMER, "My parcel is late")
    offer = offered(stream, agent)
    read = ok(tenant.api.get(f"/api/v1/conversations/{offer['conversation_id']}"))
    assert offer["data"]["conversation_number"] == read["conversation_number"]


def test_an_offer_says_whether_it_came_from_a_queue_or_an_agent_transfer(world, tenant):
    first, target = tenant.agent("First").login(), tenant.agent("Target").login()
    stream = tenant.stream(after=0)
    world.plivo.text(text_line(tenant, first)[0], CUSTOMER, "Hi")
    offer = offered(stream, first)
    assert offer["data"]["via"] == "queue"
    ok(accept(first, offer))
    stream.wait_for("offer.accepted", agent_id=first.id)
    body = {"target": {"type": "agent", "agent_id": target.id}}
    ok(first.api().post(f"/api/v1/conversations/{offer['conversation_id']}/transfers", json=body))
    assert offered(stream, target)["data"]["via"] == "transfer"


def test_a_text_transferred_to_a_queue_is_offered_as_a_transfer(world, tenant):
    first, target = tenant.agent("First").login(), tenant.agent("Billing").login()
    billing = tenant.queue(name="Billing", agents=[target.id])
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, first)
    world.plivo.text(number, CUSTOMER, "Hi")
    cid = take(stream, first)
    body = {"target": {"type": "queue", "queue_id": billing["id"]}}
    ok(first.api().post(f"/api/v1/conversations/{cid}/transfers", json=body))
    assert offered(stream, target, conversation_id=cid)["data"]["via"] == "transfer"


def test_me_lists_a_pending_offer_with_its_customer_route_and_expiry(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    world.plivo.text(text_line(tenant, agent)[0], CUSTOMER, "Hi")
    offer = offered(stream, agent)
    [pending] = ok(agent.api().get("/api/v1/me"))["pending_offers"]
    assert (pending["from"], pending["via"]) == (CUSTOMER, "queue")
    expires = datetime.fromisoformat(pending["expires_at"]) - datetime.fromisoformat(pending["offered_at"])
    assert abs(expires.total_seconds() - offer["data"]["ring_timeout_sec"]) < 1


def test_me_lists_pending_offers_newest_first_with_their_conversation_numbers(world, tenant):
    settings(tenant, capacity={"sms": 2})
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, agent)
    offers = []
    for customer in ("+14155550351", "+14155550352"):
        world.plivo.text(number, customer, "Hi")
        offers.append(offered(stream, agent, **{"from": customer})["data"])
    pending = ok(agent.api().get("/api/v1/me"))["pending_offers"]
    newest_first = [(o["offer_id"], o["conversation_number"]) for o in reversed(offers)]
    assert [(p["offer_id"], p.get("conversation_number")) for p in pending] == newest_first


def test_a_ringing_agent_gets_the_caller_and_the_number_reached_but_no_handoff_context(world, tenant):
    agent, ctx = tenant.agent().login(), {"lead": "L-173"}
    agent.phone.on_ring("ignore")
    q = tenant.queue(agents=[agent.id])
    number = redirecting_line(tenant, with_query(q["enqueue_url"], context=json.dumps(ctx)))
    desk = tenant.stream(token=agent.stream_token, after=0)
    world.plivo.call(number, CUSTOMER)
    offer = desk.wait_for("offer.created", agent_id=agent.id)
    assert (offer["data"]["from"], offer["data"].get("to"), offer["context"]) == (CUSTOMER, number, None)
    agent.phone.wait_ring().answer()
    assert desk.wait_for("offer.accepted", agent_id=agent.id)["context"] == ctx


def test_a_ring_plivo_refuses_ends_the_wait_as_assignment_failed_without_a_strike(world, tenant):
    first, second = tenant.agent("First").login(), tenant.agent("Second").login()
    stream = tenant.stream(after=0)
    sorry = hangup_action("Sorry, nobody can take your call")
    number, _ = tenant.inbound_line(first, second, agents_at_once=1, retries=1, overflow_action=sorry)
    refused = world.plivo.fault(
        "reject",
        lambda r: r.method == "POST" and r.path.rstrip("/").endswith("/Participant"),
        account=tenant.auth_id,
        status=400,
    )
    caller = world.plivo.call(number)
    assert stream.wait_for("conversation.overflowed")["data"]["reason"] == "assignment_failed"
    world.wait(lambda: "Sorry, nobody can take your call" in caller.said, what="the overflow action")
    fence(tenant)
    assert refused.hits and first.phone.legs == second.phone.legs == []
    assert not [e for e in tenant.events(type="offer.missed") if e["data"].get("strike")]


def test_an_sms_whose_offer_rounds_run_out_leaves_the_queue_as_assignment_failed(world, tenant):
    agents = {a.id: a for a in (tenant.agent(n).login() for n in ("A", "B", "C"))}
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, *agents.values(), agents_at_once=1, retries=1)
    world.plivo.text(number, CUSTOMER, "Hi")
    cid = stream.wait_for("conversation.queued")["conversation_id"]
    for n in (1, 2):
        offer = world.wait(lambda n=n: seen(stream, "offer.created", conversation_id=cid)[n - 1 :], what="an offer")[0]
        ok(decline(agents[offer["agent_id"]], offer))
    assert stream.wait_for("conversation.overflowed", conversation_id=cid)["data"]["reason"] == "assignment_failed"
    assert stream.wait_for("conversation.closed", conversation_id=cid)["data"]["result"] == "assignment_failed"
    assert len(replayed(tenant, "offer.created", conversation_id=cid)) == 2


@pytest.mark.parametrize("ends", ["missed", "away"])
def test_an_sms_whose_one_agent_lets_the_offer_end_closes_and_the_next_text_reaches_that_agent(world, tenant, ends):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, agent)
    world.plivo.text(number, CUSTOMER, "Hi")
    offer = offered(stream, agent)
    cid = offer["conversation_id"]
    if ends == "missed":
        world.advance(offer["data"]["ring_timeout_sec"] + 1)
    else:
        agent.set_status("Away")
    assert stream.wait_for("conversation.overflowed", conversation_id=cid)["data"]["reason"] == "assignment_failed"
    closed = stream.wait_for("conversation.closed", conversation_id=cid)["data"]
    assert closed["result"] == "assignment_failed"
    assert [(d["disposition"], d["system"]) for d in closed["dispositions"]] == [("Agent assignment failed", True)]
    agent.set_status("Available")
    world.plivo.text(number, CUSTOMER, "Anyone there?")
    again = world.wait(lambda: seen(stream, "conversation.queued", **{"from": CUSTOMER})[1:], what="the next text")
    assert again[0]["conversation_id"] != cid
    offered(stream, agent, conversation_id=again[0]["conversation_id"])


def test_a_queue_task_whose_one_agent_declines_it_reads_failed(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    task = ok(create(tenant.api, target={"type": "queue", "queue_id": tenant.queue(agents=[agent.id])["id"]}))
    ok(decline(agent, offered(stream, agent, conversation_id=task["conversation_id"])))
    over = stream.wait_for("conversation.overflowed", conversation_id=task["conversation_id"])
    assert over["data"]["reason"] == "assignment_failed"
    stream.wait_for("task.updated", task_id=task["id"], changes={"routing_status": ["pending", "failed"]})


def test_an_sms_whose_one_untried_agent_is_deleted_after_another_declined_it_closes_as_assignment_failed(world, tenant):
    agent, signed_out = tenant.agent("A").login(), tenant.agent("B")
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, agent, signed_out, agents_at_once=1, retries=10)
    world.plivo.text(number, CUSTOMER, "Hi")
    offer = offered(stream, agent)
    cid = offer["conversation_id"]
    ok(decline(agent, offer))
    world.advance(0)
    fence(tenant)
    assert not replayed(tenant, "conversation.overflowed", conversation_id=cid)
    ok(tenant.api.delete(f"/api/v1/agents/{signed_out.id}"))
    assert stream.wait_for("conversation.overflowed", conversation_id=cid)["data"]["reason"] == "assignment_failed"
    assert stream.wait_for("conversation.closed", conversation_id=cid)["data"]["result"] == "assignment_failed"


@pytest.mark.parametrize("ends", ["busy", "unregistered"])
def test_an_agent_whose_ring_ends_busy_or_unregistered_is_not_offered_the_next_caller_at_once(world, tenant, ends):
    agent = unavailable(tenant)
    if ends == "busy":
        agent.phone.on_ring("busy")
    else:
        agent.phone.unregister()
    stream = tenant.stream(after=0)
    first, _ = tenant.inbound_line(agent, retries=0, wait_timeout_sec=0)
    second, _ = tenant.inbound_line(agent, wait_timeout_sec=0)
    rung = queued(world, stream, first, "+14155550751")
    waiting = queued(world, stream, second, "+14155550752")
    agent.set_status("Available")
    world.wait(
        lambda: (
            seen(stream, "offer.rejected", conversation_id=rung) + seen(stream, "offer.missed", conversation_id=rung)
        ),
        what="the first ring to end",
    )
    world.wait(lambda: not world.advance(0), what="the work the first ring's end set off to finish")
    assert not replayed(tenant, "offer.created", conversation_id=waiting)


@pytest.mark.parametrize("mode", ["listen", "whisper"])
def test_a_supervisor_who_listens_or_whispers_cannot_dial_out(world, tenant, mode):
    floor = team(tenant, "Floor")
    sup = tenant.agent("Sup", role="supervisor", teams=[floor]).login()
    live = connect(tenant, tenant.agent("Agent", teams=[floor]).login())
    ok(sup.api().post(f"/api/v1/calls/{live.id}/monitor", json={"mode": mode}))
    sup.phone.wait_ring().wait_state("in_progress")
    live.stream.wait_for("supervisor.joined", conversation_id=live.id)
    body = {"agent_id": sup.id, "to": lead(world)[0], "from": registered_number(tenant)}
    error(tenant.api.post("/api/v1/calls", json=body), 409, "agent_on_call")


def test_rona_off_never_moves_the_agent(world, tenant):
    settings(tenant, ring_timeout_sec=5, rona={"strikes": 0})
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    agent.phone.on_ring("timeout")
    number, _ = tenant.inbound_line(agent, retries=0)
    for n in (1, 2):
        world.plivo.call(number)
        world.wait(lambda n=n: len(seen(stream, "offer.missed")) == n, what="a missed offer")
    fence(tenant)
    assert not [e for e in tenant.events(type="agent.status_changed") if e["data"]["reason"] == "rona"]
    assert status_name(tenant, agent) == "Available"


def test_the_sign_in_and_sign_out_statuses_are_configurable(world, tenant):
    ready, gone = (
        ok(tenant.api.post("/api/v1/agent-statuses", json={"name": name, "availability": False}))
        for name in ("Not ready", "Signed out")
    )
    settings(tenant, sign_in={"status_id": ready["id"]}, sign_out={"status_id": gone["id"]})
    agent = tenant.agent().login(available=False)
    world.wait(lambda: agent.heartbeat() or status_name(tenant, agent) == "Not ready", what="the sign-in status")
    ok(set_status(tenant, agent, "Available"))
    ok(tenant.api.delete(f"/api/v1/agents/{agent.id}/sessions"))
    assert status_name(tenant, agent) == "Signed out"


def test_an_agent_on_break_keeps_break_when_it_signs_out(world, tenant):
    agent = tenant.agent().login()
    agent.set_status("Break")
    ok(tenant.api.delete(f"/api/v1/agents/{agent.id}/sessions"))
    assert status_name(tenant, agent) == "Break"


def test_an_agent_on_break_keeps_break_when_its_desk_disconnects_and_signs_in_when_it_reconnects(world, tenant):
    agent = tenant.agent().login()
    agent.set_status("Break")
    agent.stop_pinging()
    assert status_name(tenant, agent) == "Break"
    agent.heartbeat()
    assert status_name(tenant, agent) == "Break"
    agent.heartbeat()
    world.wait(lambda: status_name(tenant, agent) == "Available", what="the sign-in status after the second ping")


def test_with_no_sign_in_or_sign_out_status_the_agent_keeps_its_status(world, tenant):
    settings(tenant, sign_in={"status_id": None}, sign_out={"status_id": None})
    agent = tenant.agent().login(available=False)
    for _ in range(3):
        agent.heartbeat()
    assert status_name(tenant, agent) == "Offline"
    ok(set_status(tenant, agent, "Available"))
    ok(tenant.api.delete(f"/api/v1/agents/{agent.id}/sessions"))
    assert status_name(tenant, agent) == "Available"
