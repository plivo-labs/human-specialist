"""Writes across tenants in the other direction, a slow webhook, and direct dial without a caller id."""

from tests.acceptance.conftest import error, fence, lead, ok, settings, skill, team, webhook
from tests.acceptance.test_events_and_webhooks import bump, delivered

A = "/api/v1"


def test_a_second_tenant_cannot_write_with_the_first_tenants_ids(world, tenant):
    other = world.new_tenant("Other")
    mine = tenant.agent("Mine")
    foreign_skill = [{"skill_id": skill(tenant), "operator": ">=", "level": 1}]
    attempts = [
        ("POST", f"{A}/queues", {"name": "Q", "agents": [mine.id]}, 422),
        ("POST", f"{A}/queues", {"name": "Q", "teams": [team(tenant)]}, 422),
        ("POST", f"{A}/queues", {"name": "Q", "skills": foreign_skill}, 422),
        ("POST", f"{A}/agents", {"name": "A", "role": "agent", "teams": [team(tenant)]}, 422),
        ("PATCH", f"{A}/agents/{mine.id}", {"name": "Hijacked"}, 404),
        ("DELETE", f"{A}/agents/{mine.id}", None, 404),
        ("PUT", f"{A}/agents/{mine.id}/status", {"status_id": other.status_id("Away")}, 404),
        ("POST", f"{A}/agents/{mine.id}/sessions", None, 404),
        ("POST", f"{A}/calls", {"agent_id": mine.id, "to": lead(world)[0]}, 422),
    ]
    bad = [
        (m, p, r.status_code)
        for m, p, b, want in attempts
        if (r := other.api.request(m, p, json=b)).status_code != want
    ]
    assert not bad and ok(tenant.api.get(f"{A}/agents/{mine.id}"))["name"] == "Mine"


def test_a_webhook_that_answers_too_slowly_rests_30_seconds_then_gets_each_delivery_again(world, tenant):
    sink = world.webhook_sink()
    sink.stall_next()
    hook = webhook(tenant, sink.url)
    [event] = bump(tenant, "Away")
    sink.wait_for("agent.status_changed")
    world.advance(0)
    [later] = bump(tenant, "Busy")
    world.advance(15)
    assert len(delivered(sink, event["id"])) == 1 and not delivered(sink, later["id"]), "called while it rests"
    world.travel(90)
    assert [d.json for d in delivered(sink, event["id"])[1:] + delivered(sink, later["id"])] == [event, later]
    assert ok(tenant.api.get(f"{A}/webhooks/{hook['id']}"))["enabled"] is True


def test_direct_dial_without_a_caller_id_is_refused_for_the_sdk_and_the_agent_token(world, tenant):
    settings(tenant, agent_direct_dial=True)
    agent = tenant.agent().login()
    number, phone = lead(world)
    error(agent.api().post(f"{A}/calls", json={"agent_id": agent.id, "to": number}), 422, "from_not_allowed")
    agent.phone.dial(number).wait_state("completed")
    fence(tenant)
    assert phone.legs == [] and not tenant.events(type="conversation.started")
