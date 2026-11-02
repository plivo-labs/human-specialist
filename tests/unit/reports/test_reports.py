"""The reports over seeded rows: the report SQL, its filters, groups and time zones."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest

from hs.infra.db import core_engine
from hs.reports import api
from tests.unit._rows import add, agent, principal, tenant, uuid_of

AT = datetime(2026, 10, 18, 20, 45, tzinfo=UTC)  # 02:15 on the 19th in Kolkata, 10:45 on the 19th in Kiritimati


@pytest.fixture()
def seeded():
    with core_engine.begin() as conn:
        org, tenant_id = tenant(conn)
        red, blue = (add(conn, "team", organization=org, name=n) for n in ("Red", "Blue"))
        (_, a_id), (_, b_id) = agent(conn, org, red), agent(conn, org, blue)
        sup = agent(conn, org, red)[1]
        sale, auto = (
            uuid_of(conn, "dispositions", add(conn, "dispositions", organization=org, name=n, internal=i))
            for n, i in (("Sale", False), ("Auto-closed", True))
        )
        teams = {t: uuid_of(conn, "team", t) for t in (red, blue)}
        calls = [
            (a_id, teams[red], True, True, 60, 10, sale, AT),
            (a_id, teams[red], True, True, 30, 5, auto, AT),
            (b_id, teams[blue], False, False, 0, 0, None, AT),
            (a_id, teams[red], True, True, 99, 9, sale, AT - timedelta(days=9)),
        ]
        for agent_id, team, inbound, answered, talk, wrap, dispo, at in calls:
            add(
                conn, "closed_conversation_search", conv_number=uuid.uuid4().hex, organization=org,
                conv_created_at=at, direction_inbound=inbound, agent_ids=[agent_id], team_ids=[team],
                is_conv_answered=answered, agent_customer_conv_duration=talk, conv_wrapup_duration=wrap,
                disposition_uuid=dispo,
            )  # fmt: skip
    return {"org": org, "tenant": tenant_id, "a": a_id, "b": b_id, "sup": sup, "red": teams[red], "sale": sale}


def report(client, s, name, who=None, **query):
    client.app.dependency_overrides[api.REPORTS.dependency] = lambda: who or principal(s["org"], s["tenant"])
    query = {"from": "2026-10-18", "to": "2026-10-19", **query}
    try:
        r = client.get(f"/api/v1/reports/{name}", params=query)
    finally:
        client.app.dependency_overrides.clear()
    assert r.status_code == 200, r.text
    return r.json()["data"]


def by_agent(rows):
    return {r["agent_id"]: r for r in rows}


def test_calls_per_agent_with_connect_rate_handle_and_filters(client, seeded):
    s = seeded
    rows = by_agent(report(client, s, "calls", group_by="agent"))
    a, b = rows[s["a"]], rows[s["b"]]
    assert (a["dials"], a["connects"], a["talk_sec"], a["wrap_sec"], a["avg_handle_sec"]) == (2, 2, 90, 15, 52.5)
    assert (b["dials"], b["connects"], b["connect_rate"], b["avg_handle_sec"]) == (1, 0, 0, 0)
    assert list(by_agent(report(client, s, "calls", group_by="agent", direction="outbound"))) == [s["b"]]
    assert list(by_agent(report(client, s, "calls", group_by="agent", team_id=s["red"]))) == [s["a"]]
    [total] = report(client, s, "calls")
    assert (total["dials"], total["agent_id"], total["bucket"]) == (3, None, None)
    sup = principal(s["org"], s["tenant"], s["sup"], "supervisor")
    assert list(by_agent(report(client, s, "calls", sup, group_by="agent"))) == [s["a"]]


def test_buckets_follow_the_time_zone(client, seeded):
    [hour] = report(client, seeded, "calls", group_by="hour", tz="Asia/Kolkata", **{"from": "2026-10-19"})
    assert datetime.fromisoformat(hour["bucket"]) == datetime(2026, 10, 18, 20, 30, tzinfo=UTC)  # 02:00 IST
    [day] = report(client, seeded, "calls", group_by="day", tz="Pacific/Kiritimati", **{"from": "2026-10-19"})
    assert datetime.fromisoformat(day["bucket"]) == datetime(2026, 10, 18, 10, tzinfo=UTC)  # midnight, UTC+14
    assert report(client, seeded, "calls", tz="Pacific/Kiritimati", to="2026-10-18") == []


def test_dispositions_count_system_and_skipped(client, seeded):
    rows = report(client, seeded, "dispositions", group_by="agent", agent_id=seeded["a"])
    got = {r["disposition"]: (r["count"], r["system"], r["skipped"]) for r in rows}
    assert got == {"Sale": (1, False, 0), "Auto-closed": (1, True, 1)}
    assert {r["disposition_id"] for r in rows if r["disposition"] == "Sale"} == {seeded["sale"]}


def test_a_bad_time_zone_is_422(client, seeded):
    seeded_principal = principal(seeded["org"], seeded["tenant"])
    client.app.dependency_overrides[api.REPORTS.dependency] = lambda: seeded_principal
    try:
        r = client.get("/api/v1/reports/calls", params={"from": "2026-10-18", "to": "2026-10-19", "tz": "Mars/Base"})
    finally:
        client.app.dependency_overrides.clear()
    assert (r.status_code, r.json()["error"]["code"]) == (422, "invalid_tz")
