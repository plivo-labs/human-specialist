"""Many writers on the same conversations at once: calls whose every status callback Plivo sends twice and whose room
events reach HS late and out of order, edits that land on a closed conversation together, and two replicas' rounds
racing for one agent."""

import contextlib
import math
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import pytest

from tests.acceptance.conftest import (
    connect,
    disposition,
    fence,
    finish,
    hs_header,
    ok,
    replayed,
    run_jobs_until,
    seen,
    wrap_up,
)
from tests.acceptance.test_search import search
from tests.harness import World

CALLS, AGENTS = 20, 10
UNTAKEN = ("offer.missed", "offer.rejected", "offer.withdrawn")


@pytest.fixture
def desks():
    """The agents put in this list beat every 5 s until the test ends, as their browsers would, so the presence sweep
    never signs one out on a slow run."""
    desks, stop = [], threading.Event()

    def beat() -> None:
        while not stop.wait(5):
            for desk in desks:
                desk.heartbeat()

    with ThreadPoolExecutor(1) as pool:
        beats = pool.submit(beat)
        yield desks
        stop.set()
        beats.result()


def hang_up(tenant, agent, cid: str, n: int, callers: dict) -> None:
    if n % 3 == 0:
        legs = ok(tenant.api.get(f"/api/v1/conversations/{cid}"))["legs"]
        callers[next(leg["call_uuid"] for leg in legs if leg["role"] == "customer")].hangup()
    elif n % 3 == 1:
        on = (leg for leg in agent.phone.legs if leg.state == "in_progress")
        next(leg for leg in on if hs_header(leg, "HsConversation") == cid).hangup()
    else:
        ok(agent.api().post(f"/api/v1/calls/{cid}/hangup"))


def held_twice(events: list[dict]) -> list[str]:
    """Each offer ends once, and no agent is offered work while it holds an offer, a call or its wrap-up: wrap-up
    blocks routing by default."""
    problems, holding = [], {}
    ends = Counter((e["data"]["offer_id"], e["agent_id"]) for e in events if e["type"] in (*UNTAKEN, "offer.accepted"))
    for e in events:
        agent, cid = e["agent_id"], e["conversation_id"]
        if e["type"] == "offer.created":
            if (n := ends[e["data"]["offer_id"], agent]) != 1:
                problems.append(f"agent {agent}'s offer of {cid} ended {n} times")
            if agent in holding:
                problems.append(f"agent {agent} was offered {cid} while holding {holding[agent]}")
            holding[agent] = cid
        elif e["type"] in (*UNTAKEN, "conversation.wrapped_up") and holding.get(agent) == cid:
            del holding[agent]
    return problems


def test_twenty_calls_over_ten_agents_survive_doubled_callbacks_and_late_reordered_room_events(world, tenant, desks):
    for i in range(AGENTS):
        desks.append(tenant.agent(f"A{i}").login())
    agents = {a.id: a for a in desks}
    number, _ = tenant.inbound_line(*agents.values(), wait_timeout_sec=600)
    done, stream = disposition(tenant)["id"], tenant.stream(after=0)
    rooms = world.plivo.fault("delay", lambda r: "/mpc-status" in r.path, account=tenant.auth_id, times=math.inf)
    doubled = world.plivo.fault("race", lambda r: True, account=tenant.auth_id, times=math.inf)
    callers = {c.call_uuid: c for c in (world.plivo.call(number, from_=f"+1415555{i:04d}") for i in range(CALLS))}
    acted, work = set(), []

    def submit(agent, cid: str) -> None:
        ok(wrap_up(agent, cid, done))

    def drive() -> bool:
        """Room events wait until everything else Plivo had to send went out, then go newest first; each answered
        call is ended and each wrap-up submitted as soon as the stream shows it."""
        if world.plivo.idle():
            rooms.flush()
        for n, e in enumerate(seen(stream, "offer.accepted")):
            if (cid := e["conversation_id"]) not in acted:
                acted.add(cid)
                work.append(pool.submit(hang_up, tenant, agents[e["agent_id"]], cid, n, callers))
        for e in seen(stream, "conversation.wrap_up_started"):
            if (key := (e["conversation_id"], e["agent_id"])) not in acted:
                acted.add(key)
                work.append(pool.submit(submit, agents[e["agent_id"]], e["conversation_id"]))
        return len({e["conversation_id"] for e in seen(stream, "conversation.closed")}) == CALLS

    with ThreadPoolExecutor(AGENTS) as pool:
        world.wait(drive, timeout=60, what="every call closed")
    for job in work:
        job.result()
    legs = [*callers.values(), *(leg for a in agents.values() for leg in a.phone.legs)]
    world.wait(lambda: all(leg.state == "completed" for leg in legs), what="every leg ended")
    rooms.release()
    world.wait(world.plivo.idle, what="the last room events delivered")
    fence(tenant)
    events = [e for e in tenant.events() if e.get("conversation_id")]
    count = Counter((e["type"], e["conversation_id"]) for e in events)
    calls = {e["conversation_id"] for e in events if e["type"] == "conversation.started"}
    assert rooms.hits and doubled.hits and len(calls) == CALLS
    assert {c: (count["call.ended", c], count["conversation.closed", c]) for c in calls} == dict.fromkeys(calls, (1, 1))
    assert [c for c in calls if count["offer.accepted", c] > 1] == []
    assert held_twice(events) == []
    for state in ("active", "in_wrap_up"):
        assert ok(tenant.api.get("/api/v1/conversations", params={"state": state}))["data"] == []
    floor = ok(tenant.api.get("/api/v1/floor"))["agents"]
    assert {r["agent_id"]: r["activity"] for r in floor if r["agent_id"] in agents} == dict.fromkeys(agents, "idle")


def test_notes_and_a_label_added_at_once_to_a_closed_conversation_are_all_found_by_search(world, tenant):
    sale = disposition(tenant, "Sale")["id"]
    cid = finish(connect(tenant), sale)["conversation_id"]
    run_jobs_until(
        world,
        lambda: search(tenant, disposition_id=sale) == [cid] and not world.run_jobs("search_index"),
        "the closed conversation indexed, with no reindex of it left to run",
    )
    words = ("refund", "parcel", "invoice", "courier", "warranty")
    with ThreadPoolExecutor(len(words) + 1) as pool:
        edits = [pool.submit(tenant.api.post, f"/api/v1/conversations/{cid}/notes", json={"text": w}) for w in words]
        label = {"attributes": {"label": "escalated"}}
        edits.append(pool.submit(tenant.api.patch, f"/api/v1/conversations/{cid}", json=label))
    for edit in edits:
        ok(edit.result())
    run_jobs_until(world, lambda: all(search(tenant, text=w) == [cid] for w in words), "every note in the index")
    assert search(tenant, disposition_id=sale) == [cid]
    assert ok(tenant.api.get(f"/api/v1/conversations/{cid}"))["attributes"] == label["attributes"]


def test_two_calls_queued_at_once_on_two_replicas_for_one_free_agent_give_it_one_offer():
    """Each trial queues two held calls at the same moment, one through each HS process and each into its own queue,
    so no queue lock orders them, for a new agent whose one call slot is free."""
    hold = '<Response><GetDigits timeout="120"><Speak>One moment</Speak></GetDigits></Response>'
    with contextlib.closing(World()) as world:
        tenant, replica = world.new_tenant(), world.replica()
        replicas = (tenant.api, replica.client(tenant.key))
        number = tenant.number()
        tenant.route_number(number, {"type": "redirect", "url": world.plivo.app(hold)})
        for trial in range(20):
            agent = tenant.agent(f"Agent {trial}").login()
            agent.phone.on_ring("answer")
            queues = [tenant.queue(agents=[agent.id], wait_timeout_sec=600) for _ in replicas]
            callers = [world.plivo.call(number, from_=f"+1415556{trial:02d}{n:02d}") for n in range(2)]
            for caller in callers:
                world.wait(lambda c=caller: "One moment" in c.said, what="the tenant app holds the call")

            def enqueue(client, queue, caller):
                body = {"call_uuid": caller.call_uuid, "from": caller.from_, "to": number}
                return ok(client.post(f"/api/v1/queues/{queue['id']}/enqueue", json=body))["conversation_id"]

            with ThreadPoolExecutor(2) as pool:
                cids = set(pool.map(enqueue, replicas, queues, callers))
            offered = replayed(tenant, "offer.created", agent_id=agent.id)
            assert len(offered) == 1 and offered[0]["conversation_id"] in cids, f"trial {trial}: {offered}"
            for caller in callers:
                caller.hangup()
