"""The live floor, queue depth and the agent-activity report over seeded rows, capacity sets and status events."""

import json
import uuid
from datetime import UTC, datetime, time, timedelta

from sqlalchemy import text

from hs.infra.db import core_engine
from hs.infra.redis import Cache
from hs.reports import api
from hs.routing import constants as rc
from hs.setup import api as setup
from tests.unit._rows import add, agent, principal, tenant, uuid_of

EVENT = text(
    "INSERT INTO events (seq, tenant_id, type, agent_id, created_at, data)"
    " VALUES (:seq, :tenant, 'agent.status_changed', :agent, :at, CAST(:data AS jsonb))"
)


def status_changed(conn, tenant_id, agent_id, name, at, seq=None):
    data = {"from_status": None, "to_status": {"id": "s", "name": name}, "reason": "manual"}
    conn.execute(EVENT, {"seq": seq, "tenant": tenant_id, "agent": agent_id, "at": at, "data": json.dumps(data)})


def call(conn, org, aom, status):
    parent = add(conn, "conversation_parent", organization=org, channel="call", direction_inbound=True,
                 conversation_number=f"N{aom}")  # fmt: skip
    add(conn, "conversation", parent=parent, primary_aom=aom, channel="call", status=status, state="agent_answered",
        generation_type="qnr_generated")  # fmt: skip
    return uuid_of(conn, "conversation_parent", parent)


def get(client, path, who, status=200, **params):
    client.app.dependency_overrides[api.REPORTS.dependency] = lambda: who
    try:
        r = client.get(path, params=params)
    finally:
        client.app.dependency_overrides.clear()
    assert r.status_code == status, r.text
    return r.json()


def waiting(conn, auth_id, queue, status, age):
    add(
        conn, "active_queue_entry", queue_priority=0, queue_uuid=queue, contact_uuid=uuid.uuid4().hex,
        flow_run_uuid=str(uuid.uuid4()), queue_type="fifo", queue_channel="call", contact_phone_or_email="+1",
        qnr_run_uuid=uuid.uuid4().hex, status=status, auth_id=auth_id,
        created_at=datetime.now(UTC) - timedelta(seconds=age),
    )  # fmt: skip


def test_the_floor_shows_status_activity_current_call_queues_and_the_last_event(client):
    with core_engine.begin() as conn:
        org, tenant_id = tenant(conn)
        available = add(conn, "org_statuses", organization=org, status="Available", availability=True)
        team = add(conn, "team", organization=org, name="Red")
        (a, a_id), (b, b_id), (c, c_id), (_, d_id) = (agent(conn, org, agent_status=available) for _ in range(4))
        _, sup = agent(conn, org, team)
        add(conn, "aom_team", organization=org, aom=a, team=team)
        on_call, wrapping = call(conn, org, a, "open"), call(conn, org, b, "in_wrap_up")
        auth_id, sip = conn.execute(
            text(
                "SELECT o.auth_id, a.sip_endpoint FROM account_org_mapping a JOIN organization o ON o.id = a.organization WHERE a.id = :c"
            ),
            {"c": c},
        ).one()
        status_changed(conn, tenant_id, d_id, "Available", datetime.now(UTC), seq=7)
        queue = str(uuid.uuid4())
        for status, age in (
            (rc.CONTACT_STATE_QUEUED, 120),
            (rc.CONTACT_STATE_IN_TRANSIT, 5),
            (rc.CONTACT_STATE_ACCEPTED, 900),
        ):
            waiting(conn, auth_id, queue, status, age)
    ring = rc.agent_capacity_key(auth_id, sip, rc.CHANNEL_CALL)
    Cache.get_instance().client.sadd(ring, "N-offered")
    try:
        floor = get(client, "/api/v1/floor", principal(org, tenant_id))
    finally:
        Cache.get_instance().client.delete(ring)
    rows = {r["agent_id"]: (r["activity"], r["conversation_id"]) for r in floor["agents"]}
    assert rows[a_id] == ("on_call", on_call) and rows[b_id] == ("wrap_up", wrapping)
    assert rows[c_id] == ("ringing", None) and rows[d_id] == ("idle", None)
    assert {r["status"]["name"] for r in floor["agents"] if r["agent_id"] != sup} == {"Available"}
    assert floor["last_event_id"] == "7"
    [q] = floor["queues"]
    assert (q["queue_id"], q["waiting"]) == (queue, 2) and 110 <= q["oldest_wait_sec"] < 300
    mine = get(client, "/api/v1/floor", principal(org, tenant_id, sup, "supervisor"))
    assert {r["agent_id"] for r in mine["agents"]} == {sup, a_id}


def test_agent_activity_sums_each_status_inside_the_window(client):
    now = datetime.now(UTC)
    yesterday = now.date() - timedelta(days=1)
    start = datetime.combine(yesterday, time(), UTC)
    with core_engine.begin() as conn:
        org, tenant_id = tenant(conn)
        _, a_id = agent(conn, org)
        for name, at in (("Offline", start - timedelta(hours=1)), ("Available", now - timedelta(seconds=100)),
                         ("Away", now - timedelta(seconds=40))):  # fmt: skip
            status_changed(conn, tenant_id, a_id, name, at)
    query = {"from": yesterday.isoformat(), "to": now.date().isoformat()}
    [row] = get(client, "/api/v1/reports/agent-activity", principal(org, tenant_id), **query)["data"]
    offline = round((now - timedelta(seconds=100) - start).total_seconds())
    assert row["agent_id"] == a_id and row["statuses"]["Offline"] == offline
    assert row["statuses"]["Available"] == 60 and 40 <= row["statuses"]["Away"] < 100


def test_queue_stats_count_that_queues_waiting_calls_and_404_another_tenants(client):
    with core_engine.begin() as conn:
        org, tenant_id = tenant(conn)
        other, _ = tenant(conn)
        auth_id = conn.execute(text("SELECT auth_id FROM organization WHERE id = :o"), {"o": org}).scalar()
        mine, theirs, idle = (
            str(
                conn.execute(
                    setup.queues.insert().values(organization=o, body={}).returning(setup.queues.c.id)
                ).scalar()
            )
            for o in (org, other, org)
        )
        for queue, status, age in ((mine, rc.CONTACT_STATE_QUEUED, 120), (mine, rc.CONTACT_STATE_ACCEPTED, 900)):
            waiting(conn, auth_id, queue, status, age)
    who = principal(org, tenant_id)
    stats = get(client, f"/api/v1/queues/{mine}/stats", who)
    assert stats["waiting"] == 1 and 110 <= stats["oldest_wait_sec"] < 300
    assert get(client, f"/api/v1/queues/{idle}/stats", who) == {"queue_id": idle, "waiting": 0, "oldest_wait_sec": 0}
    assert get(client, f"/api/v1/queues/{theirs}/stats", who, 404)["error"]["code"] == "queue_not_found"
