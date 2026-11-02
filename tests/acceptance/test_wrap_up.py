"""Wrap-up per agent: manual and auto modes, extend, and who may submit."""

import time
from datetime import datetime

import pytest

from tests.acceptance.conftest import connect, disposition, error, ok, refused, run_jobs_until, seen, settings, wrap_up


def deadline(event: dict) -> float:
    return datetime.fromisoformat(event["data"]["deadline_at"]).timestamp()


def hung_up(tenant, **wrap):
    """A connected call whose caller hung up; returns (live, the first wrap_up_started)."""
    if wrap:
        settings(tenant, wrap_up=wrap)
    live = connect(tenant)
    live.caller.hangup()
    return live, live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id)


def extend(client, cid: str, seconds: int):
    return client.post(f"/api/v1/conversations/{cid}/wrap-up/extend", json={"seconds": seconds})


def starts(live) -> list[dict]:
    return seen(live.stream, "conversation.wrap_up_started", conversation_id=live.id)


def test_an_agent_hangup_starts_that_agents_wrap_up(tenant):
    live, started = hung_up(tenant, mode="manual", seconds=30)
    assert started["agent_id"] == live.agent.id and started["data"]["mode"] == "manual"
    assert deadline(started) > time.time()


def test_the_agents_disposition_closes_the_conversation(tenant):
    live, _ = hung_up(tenant)
    sale = disposition(tenant, "Sale")
    ok(wrap_up(live.agent, live.id, sale["id"]))
    done = live.stream.wait_for("conversation.wrapped_up", conversation_id=live.id)
    assert (done["agent_id"], done["data"]["system"], done["data"]["skipped"]) == (live.agent.id, False, False)
    closed = live.stream.wait_for("conversation.closed", conversation_id=live.id)
    [entry] = closed["data"]["dispositions"]
    assert (entry["agent_id"], entry["disposition_id"], entry["skipped"]) == (live.agent.id, sale["id"], False)


def test_a_tenant_key_wrap_up_names_an_agent_who_is_in_wrap_up(tenant):
    live, _ = hung_up(tenant)
    sale = disposition(tenant, "Sale")["id"]
    path = f"/api/v1/conversations/{live.id}/wrap-up"
    error(tenant.api.post(path, json={"disposition_id": sale}), 422)
    bystander = tenant.agent("Bystander")
    error(tenant.api.post(path, json={"disposition_id": sale, "agent_id": bystander.id}), 422, "not_in_wrap_up")
    ok(tenant.api.post(path, json={"disposition_id": sale, "agent_id": live.agent.id}))
    live.stream.wait_for("conversation.closed", conversation_id=live.id)


def test_auto_mode_closes_at_the_deadline_as_skipped(tenant):
    before = time.time()
    live, started = hung_up(tenant, mode="auto", seconds=5)
    assert started["data"]["mode"] == "auto" and before + 4 <= deadline(started) <= time.time() + 6
    done = run_jobs_until(
        tenant.world,
        lambda: seen(live.stream, "conversation.wrapped_up", conversation_id=live.id),
        "conversation.wrapped_up",
    )[0]
    assert (done["data"]["system"], done["data"]["skipped"]) == (True, True)
    live.stream.wait_for("conversation.closed", conversation_id=live.id)


def test_manual_mode_waits_for_the_agent_until_the_safety_close(tenant):
    live, _ = hung_up(tenant, mode="manual", seconds=1)
    tenant.world.advance(30)
    query = {"state": "in_wrap_up", "agent_id": live.agent.id}
    assert [c["id"] for c in ok(live.agent.api().get("/api/v1/conversations", params=query))["data"]] == [live.id]
    done = run_jobs_until(
        tenant.world,
        lambda: seen(live.stream, "conversation.wrapped_up", conversation_id=live.id),
        "conversation.wrapped_up",
    )[0]
    assert (done["data"]["system"], done["data"]["skipped"]) == (True, True)


def test_extend_moves_the_deadline_and_reannounces_it(tenant):
    live, started = hung_up(tenant, mode="auto", seconds=10, max_extend_sec=30)
    ok(extend(live.agent.api(), live.id, 10))
    again = tenant.world.wait(lambda: starts(live)[1:], what="re-emitted wrap_up_started")[0]
    assert deadline(again) == pytest.approx(deadline(started) + 10, abs=1)


def test_extend_is_capped_by_max_extend_sec(tenant):
    live, started = hung_up(tenant, mode="auto", seconds=10, max_extend_sec=30)
    ok(extend(live.agent.api(), live.id, 20))
    want = 3 if extend(live.agent.api(), live.id, 20).is_success else 2
    events = tenant.world.wait(lambda: len(starts(live)) >= want and starts(live), what="wrap_up_started events")
    assert deadline(events[-1]) <= deadline(started) + 30 + 1


def test_extend_is_only_for_the_agents_own_auto_wrap_up(tenant):
    manual, _ = hung_up(tenant, mode="manual", seconds=30)
    refused(extend(manual.agent.api(), manual.id, 10))
    auto, _ = hung_up(tenant, mode="auto", seconds=30)
    refused(extend(tenant.api, auto.id, 5))
    other = tenant.agent("Other").login(available=False)
    error(extend(other.api(), auto.id, 5), 404)
