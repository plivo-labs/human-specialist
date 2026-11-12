"""The conversation and status reports, the floor's activity and queue stats over conversations the conversation
machines closed; the acceptance suite covers them end to end once calls run."""

import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import delete, func, insert, select, text, update

from hs import agents, core, events, hot, jobs, reports, search
from hs import conversations as cv
from hs.core import ApiError, T
from tests.unit.tenants import Tenant


def window() -> reports.Window:
    today = datetime.now(UTC).date()
    return reports.window(today - timedelta(days=1), today + timedelta(days=1), "UTC")


def test_the_floor_shows_ringing_on_call_wrap_up_and_idle():
    t = Tenant()
    agent = t.agent()

    def row():
        [a] = reports.floor(t.key)["agents"]
        return a["activity"], a["conversation_id"]

    assert row() == ("idle", None)
    conv = t.open()
    with core.tx() as db:
        offer = cv.offer(db, cv.lock(db, conv.id), agent)
    assert row() == ("ringing", None)
    with core.tx() as db:
        cv.lock(db, conv.id)
        cv.move(db, offer, "accept")
    assert row() == ("on_call", str(conv.id))
    with core.tx() as db:
        cv.gone(db, cv.lock(db, conv.id))
    assert row() == ("wrap_up", str(conv.id))


def test_the_floor_shows_an_offer_redis_lost_as_idle():
    t = Tenant()
    agent = t.agent()
    conv = t.open()
    with core.tx() as db:
        cv.offer(db, cv.lock(db, conv.id), agent)
    core.redis.delete(hot.OFFERS % (t.id, conv.id))
    [a] = reports.floor(t.key)["agents"]
    assert (a["activity"], a["conversation_id"]) == ("idle", None)


def test_queue_stats_count_the_waiting_and_an_unknown_queue_is_404():
    t = Tenant()
    t.open()
    t.open(customer="+15550003")
    stats = reports.queue_stats(t.queue, t.key)
    assert (stats["waiting"], stats["queue_id"]) == (2, str(t.queue)) and stats["oldest_wait_sec"] >= 0
    assert reports.floor(t.key)["queues"] == [stats]
    with pytest.raises(ApiError) as unknown:
        reports.queue_stats(uuid.uuid4(), t.key)
    assert unknown.value.status == 404


def test_status_history_outlives_event_retention_and_still_reports_its_time():
    t, E = Tenant(), T.events
    agent = t.agent()
    with core.tx() as db:
        db.execute(text("DELETE FROM hook_cursor"))
        start, *moves = (t.doc(db, "status", name=n, availability=False) for n in ("Start", "Away", "Busy"))
        db.execute(update(T.presence).where(T.presence.c.agent_id == agent).values(status_id=start))
        for status in moves:
            agents.set_status(db, t.id, agent, status, "manual")
    H = T.status_history
    with core.read() as db:
        away, busy = db.execute(select(H.c.status, H.c.at, H.c.until).where(H.c.agent_id == agent).order_by(H.c.id))
    assert (away.status, away.until, busy.status, busy.until) == ("Away", busy.at, "Busy", None)
    while events.relay_once():
        pass
    ago = {name: func.now() - func.make_interval(0, 0, 0, days) for name, days in (("Away", 101), ("Busy", 100))}
    with core.tx() as db:
        db.execute(text("DELETE FROM hook_cursor"))
        for name, at in ago.items():
            mine = (E.c.tenant_id == str(t.id), E.c.data["to_status"]["name"].astext == name)
            db.execute(update(E).where(*mine).values(created_at=at))
    events._purge(None)
    with core.read() as db:
        assert db.execute(select(E.c.type).where(E.c.tenant_id == str(t.id))).scalars().all() == []
    with core.tx() as db:
        for name, moved in (("Away", {"at": ago["Away"], "until": ago["Busy"]}), ("Busy", {"at": ago["Busy"]})):
            db.execute(update(H).where(H.c.tenant_id == t.id, H.c.status == name).values(**moved))
    today = datetime.now(UTC).date()
    w = reports.window(today - timedelta(days=102), today, "UTC")
    [row] = reports.agent_activity_report(t.key, w, reports.agents())["data"]
    assert row["statuses"]["Away"] == 86400 and row["statuses"]["Busy"] >= 99 * 86400


def test_a_state_filter_keeps_its_states_whether_or_not_the_pass_stored_their_numbers():
    t = Tenant()
    closed, queued = t.closed(t.agent()), t.open().id
    reports._store(None)

    def listed(*states):
        rows = reports.conversation_rows(t.key, window(), reports.kept([], [], list(states), []), core.Paging(50, None))
        return {r["id"] for r in rows["data"]}

    assert (listed("closed"), listed("queued")) == ({str(closed)}, {str(queued)})
    [row] = reports.conversations_report(t.key, window(), reports.kept([], [], ["queued"], []), None, [])["data"]
    assert row["total"] == 1


def test_the_conversation_reports_read_the_same_numbers_before_and_after_the_pass_stores_them():
    t = Tenant()
    a = t.agent()
    t.closed(a)
    t.closed(a, channel="sms", words=["Hi"])
    t.abandoned()
    every = reports.kept([], [], [], [])

    def numbers():
        grouped = reports.conversations_report(t.key, window(), every, "day", ["channel", "disposition"])["data"]
        return grouped, reports.conversation_rows(t.key, window(), every, core.Paging(50, None))["data"]

    before = numbers()
    assert sorted(r["handle_sec"] is None for r in before[1]) == [False, False, True]
    reports._store(None)
    C = T.conversation
    with core.read() as db:
        assert not db.execute(select(C.c.id).where(C.c.tenant_id == t.id, C.c.transfers.is_(None))).all()
    assert numbers() == before


def test_an_outbound_result_the_stored_map_lacks_takes_its_outbound_default_in_the_close_and_the_reports():
    t = Tenant()
    with core.tx() as db:
        doc = {"system": True, "parent_id": None, "note_required": False}
        unreached = str(t.doc(db, "disposition", name="Customer didn't respond", **doc))
    conv = t.open(direction="outbound", queued=False)
    with core.tx() as db:
        cv.gone(db, cv.lock(db, conv.id), "busy")
    E = T.events
    with core.read() as db:
        closed = select(E.c.data).where(E.c.conversation_id == str(conv.id), E.c.type == "conversation.closed")
        [told] = db.execute(closed).scalar()["dispositions"]
    [row] = reports.conversation_rows(t.key, window(), reports.kept([], [], [], []), core.Paging(50, None))["data"]
    assert told["disposition_id"] == row["disposition_id"] == unreached


def transferred(t: Tenant) -> uuid.UUID:
    """A call open 100 s: the first agent talks 5 to 20 s and submits its wrap-up at 30, and the second, copied in by a
    transfer, talks 40 to 50 s and never submits, its wrap-up due at 70: wrap 10 + 20 s, handle 45 s of talk + wrap."""
    first, second = t.agent(), t.agent()
    cid = t.closed(first)
    at = func.now() - timedelta(minutes=10)
    stamped = {"started_at": at + timedelta(seconds=5), "ended_at": at + timedelta(seconds=20)}
    stamped |= {"wrap_up_until": at + timedelta(seconds=60), "submitted_at": at + timedelta(seconds=30)}
    copied = {"role": "primary", "via": "transfer", "started_at": at + timedelta(seconds=40)}
    copied |= {"ended_at": at + timedelta(seconds=50), "wrap_up_until": at + timedelta(seconds=70)}
    with core.tx() as db:
        db.execute(
            update(T.conversation)
            .where(T.conversation.c.id == cid)
            .values(opened_at=at, closed_at=at + timedelta(seconds=100))
        )
        db.execute(update(T.assignment).where(T.assignment.c.conversation_id == cid).values(**stamped))
        db.execute(
            insert(T.assignment).values(
                **copied, tenant_id=t.id, conversation_id=cid, agent_id=second, channel="call", state="done"
            )
        )
    return cid


def test_each_wrap_up_ends_at_its_submit_or_an_unstamped_ones_own_end_in_search_and_every_report():
    t = Tenant()
    cid = transferred(t)
    with core.read() as db:
        searched = db.execute(search.FACTS, {"c": cid}).one().wrap_sec
    [row] = reports.conversation_rows(t.key, window(), reports.kept([], [], [], []), core.Paging(50, None))["data"]
    assert (searched, row["wrap_sec"]) == (30, 30)


def test_the_queued_refresh_corrects_the_kept_wrap_and_handle_of_a_transferred_call_and_keeps_every_other_fact(
    monkeypatch,
):
    t = Tenant()
    cid = transferred(t)
    C = T.conversation
    # As kept before each wrap-up ended at its own submit: 80 + 50 s of wrap. The offers it missed live on after
    # retention deleted their events.
    old = {"wrap_sec": 130, "handle_sec": 175, "response_sec": 7, "first_reply_sec": 9, "transfers": 1}
    old |= {"missed_offers": 3}
    with core.tx() as db:
        db.execute(update(C).where(C.c.id == cid).values(**old))

    def kept() -> dict:
        with core.read() as db:
            return dict(
                db.execute(select(*(C.c[f] for f in reports.FACTS.split(", "))).where(C.c.id == cid)).one()._mapping
            )

    fixed = kept() | {"wrap_sec": 30, "handle_sec": 75}
    monkeypatch.setattr(reports, "BATCH", 1)
    after = {"after": str(uuid.UUID(int=cid.int - 1))}
    for _ in range(2):
        with core.tx() as db:
            jobs.schedule(db, "report_refresh", after, key="report_refresh")
        jobs.run_now("report_refresh", "report_refresh", wait=True)
        assert kept() == fixed
    start = threading.Barrier(2)

    def run() -> None:
        start.wait()
        reports._refresh(SimpleNamespace(payload=after, rev=0))

    with ThreadPoolExecutor(2) as pool:
        list(pool.map(lambda _: run(), range(2)))
    assert kept() == fixed
    with core.tx() as db:
        db.execute(delete(T.jobs).where(T.jobs.c.kind == "report_refresh"))


def test_hourly_buckets_across_a_daylight_saving_change_count_each_conversation_once_in_the_hour_it_opened():
    """New York springs forward on 2026-03-08 and falls back on 2026-11-01, where 05:30Z and 06:30Z are both 01:30."""
    t = Tenant()
    agent = t.agent()
    days = {date(2026, 3, 8): ("06:30", "07:30", "08:30"), date(2026, 11, 1): ("05:30", "06:30", "07:30")}
    opened = [datetime.fromisoformat(f"{day}T{hhmm}Z") for day, times in days.items() for hhmm in times]
    C, H = T.conversation, T.status_history
    with core.tx() as db:
        for at in opened:
            db.execute(update(C).where(C.c.id == t.abandoned()).values(opened_at=at))
            _changed(db, t, agent, "Available", available=True)
            db.execute(update(H).where(H.c.agent_id == agent, H.c.until.is_(None)).values(at=at))
    every = reports.kept([], [], [], [])
    for day in days:
        w = reports.window(day, day, "America/New_York")
        hours = {at.replace(minute=0): 1 for at in opened if w.start <= at < w.end}
        conversations = reports.conversations_report(t.key, w, every, "hour", [])["data"]
        assert {r["bucket"]: r["total"] for r in conversations} == hours
        available = reports.available_agents_report(t.key, w, reports.agents(), "hour")["data"]
        assert {r["bucket"]: r["agents"] for r in available} == hours


def _changed(db, t: Tenant, agent, name: str, available=False) -> None:
    status = {"id": str(t.doc(db, "status", name=name, availability=available)), "name": name}
    data = {"from_status": None, "to_status": status, "reason": "manual"}
    events.emit(db, "agent.status_changed", str(t.id), data, agent_id=str(agent))


def test_a_status_change_written_as_its_event_alone_shows_in_the_status_reports():
    t = Tenant()
    agent = t.agent()
    with core.tx() as db:
        _changed(db, t, agent, "Away")
    today = datetime.now(UTC).date()
    w = reports.window(today, today, "UTC")
    [row] = reports.agent_status_log(t.key, w, reports.agents(), core.Paging(50, None))["data"]
    assert (row["agent_id"], row["status"], row["available"]) == (str(agent), "Away", False)
    [activity] = reports.agent_activity_report(t.key, w, reports.agents())["data"]
    assert list(activity["statuses"]) == ["Away"]


def test_two_quick_status_changes_for_one_agent_each_end_where_the_next_begins():
    t = Tenant()
    agent = t.agent()
    with core.tx() as db:
        _changed(db, t, agent, "Away")
        _changed(db, t, agent, "Busy")
    H = T.status_history
    mine = select(H.c.status, H.c.at, H.c.until).where(H.c.agent_id == agent)
    with core.read() as db:
        away, busy = db.execute(mine.order_by(H.c.at, H.c.id))
    assert (away.status, away.until, busy.status, busy.until) == ("Away", busy.at, "Busy", None)
    assert away.at < busy.at
