"""Skill level ranges, the idle status and per-queue webhooks, as the contact center's admin and agent desk use them."""

import uuid
from functools import partial

import psycopg2

from tests.acceptance.conftest import connect, error, fence, ok, settings, status_changes, webhook

SKILLS, A = "/api/v1/skills", "/api/v1/agents"


def skill_with(tenant, name: str = "Spanish", **fields) -> dict:
    return ok(tenant.api.post(SKILLS, json={"name": name, **fields}))


def ranged(tenant, lo: int = 2, hi: int = 6) -> dict:
    return skill_with(tenant, is_level_required=True, min_level=lo, max_level=hi)


def levels(tenant, agent) -> list:
    return [s["level"] for s in ok(tenant.api.get(f"{A}/{agent.id}"))["skills"]]


def test_a_skill_with_a_required_level_has_a_range_with_min_below_max(tenant):
    got = ranged(tenant)
    assert (got["is_level_required"], got["min_level"], got["max_level"]) == (True, 2, 6)
    bad = {"name": "Flat", "is_level_required": True, "min_level": 5, "max_level": 5}
    error(tenant.api.post(SKILLS, json=bad), 422)


def test_an_agent_level_must_sit_in_its_skills_range(tenant):
    s = ranged(tenant)["id"]
    body = {"name": "Ann", "role": "agent"}
    error(tenant.api.post(A, json=body | {"skills": [{"skill_id": s, "level": 7}]}), 422, "skill_level_invalid")
    error(tenant.api.post(A, json=body | {"skills": [{"skill_id": s}]}), 422, "skill_level_invalid")
    assert levels(tenant, tenant.agent(skills=[{"skill_id": s, "level": 3}])) == [3]


def test_a_skill_ranges_from_0_to_100_and_an_agent_holds_either_end_but_never_no_level(tenant):
    s = ranged(tenant, 0, 100)["id"]
    assert [levels(tenant, tenant.agent(skills=[{"skill_id": s, "level": n}])) for n in (0, 100)] == [[0], [100]]
    unset = {"name": "Ann", "role": "agent", "skills": [{"skill_id": s}]}
    error(tenant.api.post(A, json=unset), 422, "skill_level_invalid")


def test_a_range_that_leaves_an_agent_outside_is_refused(tenant):
    s = ranged(tenant)["id"]
    agent = tenant.agent(skills=[{"skill_id": s, "level": 5}])
    error(tenant.api.patch(f"{SKILLS}/{s}", json={"min_level": 6, "max_level": 9}), 409, "skill_level_outside_range")
    assert ok(tenant.api.patch(f"{SKILLS}/{s}", json={"min_level": 4, "max_level": 9}))["min_level"] == 4
    assert levels(tenant, agent) == [5]


def test_switching_levels_on_gives_agents_the_minimum_and_off_clears_them(tenant):
    s = skill_with(tenant, is_level_required=False)["id"]
    agent = tenant.agent(skills=[{"skill_id": s}])
    assert levels(tenant, agent) == [None]
    on = {"is_level_required": True, "min_level": 3, "max_level": 8}
    assert ok(tenant.api.patch(f"{SKILLS}/{s}", json=on))["is_level_required"] is True
    assert levels(tenant, agent) == [3]
    ok(tenant.api.patch(f"{SKILLS}/{s}", json={"is_level_required": False}))
    assert levels(tenant, agent) == [None]


def status_name(agent) -> str:
    return ok(agent.api().get("/api/v1/me"))["status"]["name"]


def test_an_idle_desk_takes_the_idle_status_when_tracking_is_on(tenant):
    agent = tenant.agent().login()
    error(agent.api().post("/api/v1/me/idle"), 409, "idle_tracking_off")
    assert settings(tenant, idle={"enabled": True, "after_min": 5})["idle"]["after_min"] == 5
    ok(agent.api().post("/api/v1/me/idle"))
    assert status_name(agent) == "Idle"


def test_an_agent_with_an_active_conversation_stays_as_they_are_when_the_desk_goes_idle(tenant):
    settings(tenant, idle={"enabled": True, "after_min": 5})
    live = connect(tenant)
    ok(live.agent.api().post("/api/v1/me/idle"))
    assert status_name(live.agent) == "Available"


def test_idle_tracking_takes_five_to_five_hundred_minutes_and_leaves_an_agent_on_break_alone(tenant):
    for minutes in (4, 501):
        error(tenant.api.patch("/api/v1/settings", json={"idle": {"enabled": True, "after_min": minutes}}), 422)
    settings(tenant, idle={"enabled": True, "after_min": 30})
    agent = tenant.agent().login()
    agent.set_status("Break")
    ok(agent.api().post("/api/v1/me/idle"))
    assert status_name(agent) == "Break"


def test_a_webhook_for_a_queue_gets_only_that_queues_events(world, tenant):
    sink, agent = world.webhook_sink(), tenant.agent().login(available=False)
    (mine, queue), (other, _) = tenant.inbound_line(agent), tenant.inbound_line(agent)
    hook = webhook(tenant, sink.url, event_types=["conversation.queued"], queue_ids=[queue["id"]])
    assert hook["queue_ids"] == [queue["id"]]
    error(tenant.api.post("/api/v1/webhooks", json={"url": sink.url, "queue_ids": [str(uuid.uuid4())]}), 422)
    world.plivo.call(other)
    world.plivo.call(mine)
    sink.wait_for("conversation.queued", queue_id=queue["id"])
    world.run_jobs()
    assert {d.json["data"]["queue_id"] for d in sink.deliveries} == {queue["id"]}


def test_a_status_change_says_whether_the_new_status_is_available(tenant):
    agent = tenant.agent().login()
    fence(tenant)
    got = status_changes(tenant, agent, "Break", "Available")
    assert [e["data"]["available"] for e in got] == [False, True]


def test_a_webhook_queue_is_the_one_an_event_was_emitted_in(world, tenant):
    agent = tenant.agent().login(available=False)
    (number, a), (_, b) = tenant.inbound_line(agent), tenant.inbound_line(agent)
    sinks = {q["id"]: world.webhook_sink() for q in (a, b)}
    for queue, sink in sinks.items():
        webhook(tenant, sink.url, event_types=["conversation.queued"], queue_ids=[queue])
    with psycopg2.connect(world._env["DATABASE_URL"]) as db, db.cursor() as cur:
        db.autocommit = True
        cur.execute("SELECT pg_advisory_lock(1213420118)")
        world.plivo.call(number)
        world.wait(
            lambda: (
                cur.execute("SELECT count(*) FROM hs.events WHERE type = 'conversation.queued'") or cur.fetchone()[0]
            ),
            what="the queued event",
        )
        cur.execute("UPDATE hs.conversation SET queue_id = %s WHERE queue_id = %s", (b["id"], a["id"]))
        cur.execute("SELECT pg_advisory_unlock(1213420118)")
    sinks[a["id"]].wait_for("conversation.queued")
    world.run_jobs()
    assert sinks[b["id"]].deliveries == []


def test_the_idle_status_is_an_unavailable_one_and_tracking_needs_it(tenant):
    error(tenant.api.patch("/api/v1/settings", json={"idle": {"status_id": tenant.status_id("Available")}}), 422)
    error(tenant.api.patch("/api/v1/settings", json={"idle": {"enabled": True, "status_id": None}}), 422)
    quiet = ok(tenant.api.post("/api/v1/agent-statuses", json={"name": "Quiet"}))["id"]
    settings(tenant, idle={"enabled": True, "status_id": quiet})
    error(tenant.api.patch(f"/api/v1/agent-statuses/{quiet}", json={"availability": True}), 409, "status_in_use")
    loud = ok(tenant.api.post("/api/v1/agent-statuses", json={"name": "Loud"}))["id"]
    settings(tenant, idle={"enabled": True, "status_id": loud.upper()})
    error(tenant.api.patch(f"/api/v1/agent-statuses/{loud}", json={"availability": True}), 409, "status_in_use")


def moved(tenant, agent, status) -> list[dict]:
    got = [e for e in tenant.events(type="agent.status_changed") if e["agent_id"] == agent.id]
    return [e for e in got if e["data"]["from_status"]["id"] == status["id"] and e["data"]["reason"] == "admin"]


def test_deleting_a_status_in_use_says_whether_the_agents_new_status_is_available(tenant):
    for name, available in (("Lunch", False), ("Ready", True)):
        agent = tenant.agent().login()
        status = ok(tenant.api.post("/api/v1/agent-statuses", json={"name": name, "availability": available}))
        ok(tenant.api.put(f"/api/v1/agents/{agent.id}/status", json={"status_id": status["id"]}))
        ok(tenant.api.delete(f"/api/v1/agent-statuses/{status['id']}"))

        tenant.world.wait(partial(moved, tenant, agent, status), what="the admin status change")
        assert [e["data"]["available"] for e in moved(tenant, agent, status)] == [available]
