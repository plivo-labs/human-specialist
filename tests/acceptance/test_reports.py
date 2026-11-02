"""Reports over closed conversations and status events, queue stats, and the live floor. Each test seeds
calls through the simulator and compares the report with a hand count."""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from tests.acceptance.conftest import (
    connect,
    dial,
    disposition,
    finish,
    lead,
    ok,
    run_jobs_until,
    seen,
    settings,
    status_changes,
    team,
    unavailable,
)


def window(tz: str = "UTC", **params) -> dict:
    today = datetime.now(ZoneInfo(tz)).date()
    return {
        "from": (today - timedelta(days=1)).isoformat(),
        "to": (today + timedelta(days=1)).isoformat(),
        "tz": tz,
        **params,
    }


def report(tenant, name: str, **params) -> list[dict]:
    return ok(tenant.api.get(f"/api/v1/reports/{name}", params=window(**params)))["data"]


def settled(tenant, name: str, ready, **params) -> list[dict]:
    """The report once ready(rows) holds: a closed call reaches reports through a row written after
    conversation.closed commits."""
    return tenant.world.wait(lambda: ready(rows := report(tenant, name, **params)) and rows, what=f"the {name} report")


def connects(rows: list[dict]) -> int:
    return sum(r["connects"] for r in rows)


def bucket(row: dict, tz: str) -> datetime:
    b = datetime.fromisoformat(row["bucket"])
    return b.astimezone(ZoneInfo(tz)) if b.tzinfo else b


def outbound_closed(tenant, agent, behaviour: str) -> str:
    number, _ = lead(tenant.world, behaviour)
    stream = tenant.stream(after=0)
    _, cid = dial(tenant, agent, to=number)
    if behaviour == "answer":
        stream.wait_for("call.answered", conversation_id=cid)
        ok(tenant.api.post(f"/api/v1/calls/{cid}/hangup"))
    stream.wait_for("call.ended", conversation_id=cid)
    run_jobs_until(
        tenant.world, lambda: seen(stream, "conversation.closed", conversation_id=cid), "conversation.closed"
    )[0]
    return cid


def test_the_calls_report_counts_outbound_dials_and_connects(tenant):
    settings(tenant, wrap_up={"mode": "auto", "seconds": 1})
    agent = tenant.agent().login()
    for behaviour in ("answer", "busy", "no_answer"):
        outbound_closed(tenant, agent, behaviour)
    rows = settled(tenant, "calls", lambda rs: sum(r["dials"] for r in rs) == 3, group_by="agent", direction="outbound")
    [row] = [r for r in rows if r["agent_id"] == agent.id]
    assert (row["dials"], row["connects"]) == (3, 1)
    assert row["connect_rate"] == pytest.approx(1 / 3, abs=0.01)
    assert row["talk_sec"] >= 0 and row["wrap_sec"] >= 0 and row["avg_handle_sec"] >= 0


def test_the_calls_report_counts_inbound_connects_per_agent_and_per_day(tenant):
    a, b = tenant.agent("A").login(), tenant.agent("B").login()
    for agent in (a, a, b):
        finish(connect(tenant, agent))
    reported = settled(tenant, "calls", lambda rs: connects(rs) == 3, group_by="agent", direction="inbound")
    rows = {r["agent_id"]: r for r in reported}
    assert (rows[a.id]["connects"], rows[b.id]["connects"]) == (2, 1)
    assert sum(r["connects"] for r in report(tenant, "calls", group_by="day", agent_id=a.id)) == 2


def test_the_dispositions_report_counts_each_disposition_and_skips(tenant):
    settings(tenant, wrap_up={"mode": "auto", "seconds": 30})
    sale = disposition(tenant, "Sale")
    agent = tenant.agent().login()
    finish(connect(tenant, agent), sale["id"])
    finish(connect(tenant, agent), sale["id"])
    live = connect(tenant, agent)
    live.caller.hangup()
    live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id)
    run_jobs_until(
        tenant.world, lambda: seen(live.stream, "conversation.closed", conversation_id=live.id), "conversation.closed"
    )[0]
    rows = settled(
        tenant, "dispositions", lambda rs: sum(r["count"] for r in rs) == 3, group_by="agent", agent_id=agent.id
    )
    [sold] = [r for r in rows if r["disposition_id"] == sale["id"]]
    assert (sold["count"], sold["system"], sold["skipped"]) == (2, False, 0)
    assert sum(r["count"] for r in rows if r["system"]) == sum(r["skipped"] for r in rows) == 1


def test_the_team_filter_keeps_only_that_teams_calls(tenant):
    red, blue = team(tenant, "Red"), team(tenant, "Blue")
    a, b = tenant.agent("A", teams=[red]).login(), tenant.agent("B", teams=[blue]).login()
    finish(connect(tenant, a))
    finish(connect(tenant, b))
    settled(tenant, "calls", lambda rs: connects(rs) == 2, group_by="agent")
    rows = report(tenant, "calls", group_by="agent", team_id=red)
    assert [r["agent_id"] for r in rows] == [a.id] and rows[0]["connects"] == 1


def test_hour_and_day_buckets_follow_the_time_zone(tenant):
    india, kiribati = "Asia/Kolkata", "Pacific/Kiritimati"
    before = {tz: datetime.now(ZoneInfo(tz)) for tz in (india, kiribati)}
    finish(connect(tenant))
    after = {tz: datetime.now(ZoneInfo(tz)) for tz in (india, kiribati)}
    [hour] = [r for r in settled(tenant, "calls", connects, tz=india, group_by="hour") if r["connects"]]
    b = bucket(hour, india)
    assert b.minute == 0 and (b.date(), b.hour) in {(t.date(), t.hour) for t in (before[india], after[india])}
    [day] = [r for r in report(tenant, "calls", tz=kiribati, group_by="day") if r["connects"]]
    assert bucket(day, kiribati).date() in {before[kiribati].date(), after[kiribati].date()}


def test_the_agent_activity_report_has_seconds_per_status(tenant):
    agent = tenant.agent()
    status_changes(tenant, agent, "Available", "Away")
    [row] = report(tenant, "agent-activity", agent_id=agent.id)
    assert row["agent_id"] == agent.id and row["statuses"]["Available"] >= 0


def test_queue_stats_show_who_is_waiting(world, tenant):
    q = tenant.queue(name="Busy", agents=[unavailable(tenant).id], wait_timeout_sec=600)
    number = tenant.number()
    tenant.route_number(number, {"type": "queue", "queue_id": q["id"]})
    stream = tenant.stream(after=0)
    for _ in range(2):
        world.plivo.call(number)
    world.wait(lambda: len(seen(stream, "conversation.queued")) == 2, what="two waiting")
    stats = ok(tenant.api.get(f"/api/v1/queues/{q['id']}/stats"))
    assert stats["waiting"] == 2 and stats["oldest_wait_sec"] >= 0


def test_the_floor_shows_status_activity_and_the_current_call(world, tenant):
    agent, reader = tenant.agent().login(), tenant.key_with("reports")

    def row():
        return next(a for a in ok(reader.get("/api/v1/floor"))["agents"] if a["agent_id"] == agent.id)

    agent.phone.on_ring("ignore")
    caller = world.plivo.call(tenant.inbound_line(agent)[0])
    agent.phone.wait_ring()
    world.wait(lambda: row()["activity"] == "ringing", what="ringing")
    caller.hangup()
    world.wait(lambda: row()["activity"] == "idle", what="idle")
    live = connect(tenant, agent)
    assert (row()["activity"], row()["status"]["name"], row()["conversation_id"]) == ("on_call", "Available", live.id)
    live.caller.hangup()
    world.wait(lambda: row()["activity"] == "wrap_up", what="wrap_up")


def test_a_supervisor_floor_lists_only_accessible_agents(tenant):
    t = team(tenant, "Mine")
    sup = tenant.agent("Sup", role="supervisor", teams=[t]).login(available=False)
    mine = tenant.agent("Mine", teams=[t]).login()
    theirs = tenant.agent("Theirs", teams=[team(tenant, "Theirs")]).login()
    listed = {a["agent_id"] for a in ok(sup.api().get("/api/v1/floor"))["agents"]}
    assert mine.id in listed and theirs.id not in listed


def test_the_floor_last_event_id_lets_a_stream_continue_without_gaps(tenant):
    agent = tenant.agent()
    status_changes(tenant, agent, "Away")
    floor = ok(tenant.api.get("/api/v1/floor"))
    later = status_changes(tenant, agent, "Available", "Busy")
    stream = tenant.stream(after=floor["last_event_id"])
    tenant.world.wait(lambda: len(seen(stream, "agent.status_changed")) >= 2, what="events after the snapshot")
    assert [e["id"] for e in seen(stream, "agent.status_changed")] == [e["id"] for e in later]


def test_the_recent_calls_list_filters_by_state_agent_direction_and_call_uuid(tenant):
    live, other = connect(tenant), connect(tenant)

    def listed(**q):
        return [c["id"] for c in ok(tenant.api.get("/api/v1/conversations", params=q))["data"]]

    assert live.id in listed(state="active") and live.id not in listed(state="closed")
    assert listed(call_uuid=live.caller.call_uuid) == [live.id]
    assert listed(agent_id=live.agent.id) == [live.id]
    assert live.id in listed(direction="inbound") and live.id not in listed(direction="outbound")
    finish(live)
    assert live.id in listed(state="closed") and other.id in listed(state="active")


def test_a_conversation_shows_its_number_legs_notes_and_recordings(tenant):
    settings(tenant, recording={"inbound": True})
    live = connect(tenant)
    ok(live.agent.api().post(f"/api/v1/conversations/{live.id}/notes", json={"text": "Asked for a refund"}))
    live.caller.hangup()
    rid = live.stream.wait_for("recording.ready", conversation_id=live.id)["data"]["recording_id"]
    c = ok(live.agent.api().get(f"/api/v1/conversations/{live.id}"))
    assert c["conversation_number"] and live.caller.call_uuid in [leg["call_uuid"] for leg in c["legs"]]
    assert "Asked for a refund" in [n["text"] for n in c["notes"]] and rid in c["recording_ids"]
    stranger = tenant.agent("Stranger").login(available=False)
    assert stranger.api().get(f"/api/v1/conversations/{live.id}").status_code == 404
