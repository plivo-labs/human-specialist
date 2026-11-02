"""The event replay cursor, a report that reconciles with the calls made, duplicate Plivo callbacks and a cache fault."""

import pytest

from tests.acceptance.conftest import connect, dial, error, finish, lead, ok, run_jobs_until, seen, settings
from tests.acceptance.test_events_and_webhooks import bump
from tests.acceptance.test_reports import settled


def test_replay_resumes_from_after_and_ignores_a_cursor_parameter(tenant):
    events = bump(tenant, "Away", "Busy", "Away")
    paged = ok(tenant.api.get("/api/v1/events", params={"after": 0, "limit": 1}))
    assert [e["id"] for e in paged["data"]] == [events[0]["id"]] and paged["next_cursor"] == events[0]["id"]
    second = ok(tenant.api.get("/api/v1/events", params={"after": paged["next_cursor"], "limit": 1}))
    assert [e["id"] for e in second["data"]] == [events[1]["id"]]
    plain = ok(tenant.api.get("/api/v1/events", params={"limit": 50}))
    cursored = ok(tenant.api.get("/api/v1/events", params={"limit": 50, "cursor": events[-1]["id"]}))
    assert cursored == plain


def test_the_calls_report_reconciles_with_every_call_made(world, tenant):
    settings(tenant, wrap_up={"mode": "auto", "seconds": 1})
    agent = tenant.agent().login()
    behaviours = ["answer", "busy", "answer", "no_answer", "answer", "reject", "answer"]
    leads = [lead(world, b) for b in behaviours]
    stream = tenant.stream(after=0)
    for behaviour, (number, _) in zip(behaviours, leads, strict=True):
        _, cid = dial(tenant, agent, to=number)
        if behaviour == "answer":
            stream.wait_for("call.answered", conversation_id=cid)
            ok(tenant.api.post(f"/api/v1/calls/{cid}/hangup"))
        stream.wait_for("call.ended", conversation_id=cid)
        run_jobs_until(world, lambda cid=cid: seen(stream, "conversation.closed", conversation_id=cid), "closed")
    inbound = 3
    for _ in range(inbound):
        finish(connect(tenant))
    made = sum(len(phone.legs) for _, phone in leads)
    answered = behaviours.count("answer")
    out = settled(tenant, "calls", lambda rs: sum(r["dials"] for r in rs) == made, direction="outbound")
    assert (sum(r["dials"] for r in out), sum(r["connects"] for r in out)) == (len(behaviours), answered)
    both = settled(tenant, "calls", lambda rs: sum(r["connects"] for r in rs) == answered + inbound)
    assert sum(r["connects"] for r in both) == answered + inbound
    closed = ok(tenant.api.get("/api/v1/conversations", params={"state": "closed", "limit": 200}))["data"]
    assert len(closed) == len(behaviours) + inbound


@pytest.fixture
def duplicates(world):
    world.plivo.deliver_twice = True
    yield
    world.wait(world.plivo.idle, what="the simulator to go idle")
    world.plivo.deliver_twice = False


def test_every_plivo_callback_delivered_twice_changes_nothing(world, tenant, duplicates):
    settings(tenant, wrap_up={"mode": "manual", "seconds": 1})
    live = connect(tenant)
    finish(live)
    world.wait(world.plivo.idle, what="the duplicates delivered")
    mine = [e for e in tenant.events() if e["conversation_id"] == live.id]
    counts = {t: sum(e["type"] == t for e in mine) for t in {e["type"] for e in mine}}
    assert {t: n for t, n in counts.items() if n > 1} == {}
    assert counts["call.ended"] == counts["offer.accepted"] == counts["conversation.closed"] == 1


def test_a_cache_fault_on_the_heartbeat_is_503_while_the_agent_reads_stay_available(world, tenant):
    agent = tenant.agent().login()
    world.redis.delete(f"AOMPC:{agent.id}")
    world.redis.hset(f"AOMPC:{agent.id}", "wrong", "type")
    error(agent.api().post("/api/v1/me/heartbeat"), 503, "heartbeat_failed")
    assert ok(tenant.api.get(f"/api/v1/agents/{agent.id}"))["id"] == agent.id
    assert ok(agent.api().get("/api/v1/me"))["id"] == agent.id
    world.redis.delete(f"AOMPC:{agent.id}")
    agent.heartbeat()
