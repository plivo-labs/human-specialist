"""Routing on the migrated database: each strategy's first agent, skill operators both ways, capacity per channel,
wrap-up blocking, sticky agents, rounds and RONA, races for an agent and for a queue's last place, reverse routing,
the queue timeout and the offer routes."""

import threading
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import contextmanager, nullcontext, suppress
from datetime import UTC, datetime, timedelta
from functools import partial
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from redis import Redis, RedisError, ResponseError
from redis import TimeoutError as RedisTimeout
from redis.client import Pipeline
from sqlalchemy import func, insert, select, text, update
from sqlalchemy.exc import OperationalError

from hs import access, agents, config, core, jobs, routing
from hs import app as service
from hs import conversations as cv
from hs.config import models as m
from hs.core import T
from hs.voice import mpc
from tests.harness import AGE

client = TestClient(service.app)
ADMITS = {"": {4, 5, 6}, "=": {5}, "!=": {4, 6}, "<>": {4, 6}, "<": {4}, "<=": {4, 5}, ">": {6}, ">=": {5, 6}}
BLOCKED = text("SELECT pid FROM pg_stat_activity WHERE :pid = ANY(pg_blocking_pids(pid))")
CAP, FULL = "hs:{%s}:cap:%s:%s", "hs:{%s}:full:%s"


class Floor:
    """One tenant with Available, Away and Break statuses, agents with presence, queues, and channels whose rings
    are only recorded."""

    def __init__(self, monkeypatch):
        self.rings = []
        for channel in ("call", "sms"):
            ring = routing.Channel(open=None, ring=lambda db, conv, offers: self.rings.append([o.id for o in offers]))
            monkeypatch.setitem(routing.CHANNELS, channel, ring)
        with core.tx() as db:
            tenant = insert(T.tenant).values(name="t", plivo_auth_id=uuid.uuid4().hex, plivo_token_enc="sealed")
            self.tenant = db.execute(tenant.returning(T.tenant.c.id)).scalar()
            self.available = self.doc(db, "status", {"name": "Available", "availability": True}, "online")
            self.away = self.doc(db, "status", {"name": "Away", "availability": False})
            self.brk = self.doc(db, "status", {"name": "Break", "availability": False})
        routing.hot.rebuilt(self.tenant)
        self.settings(**m.Stored(rona={"status_id": str(self.brk)}).model_dump(mode="json"))

    def doc(self, db, kind: str, body: dict, key: str | None = None) -> uuid.UUID:
        row = insert(T.config).values(tenant_id=self.tenant, kind=kind, key=key, body=body)
        return db.execute(row.returning(T.config.c.id)).scalar()

    def settings(self, **changes) -> None:
        with config.write(access.OPERATOR, self.tenant) as db:
            S = T.tenant.c.settings
            db.execute(update(T.tenant).where(T.tenant.c.id == self.tenant).values(settings=S.concat(changes)))

    def agent(self, *, teams=(), skills=(), available=True, idle_min=0, misses=0) -> uuid.UUID:
        """An agent whose status changed `idle_min` minutes ago."""
        body = {"name": "A", "role": "agent", "teams": [str(t) for t in teams]}
        body["skills"] = [{"skill_id": str(s), "level": level} for s, level in skills]
        with core.tx() as db:
            agent = self.doc(db, "agent", body)
            since = func.now() - timedelta(minutes=idle_min)
            status = self.available if available else self.away
            presence = {"agent_id": agent, "tenant_id": self.tenant, "status_id": status, "since": since}
            db.execute(insert(T.presence).values(**presence, misses=misses))
        return agent

    def queue(self, **fields) -> uuid.UUID:
        with core.tx() as db:
            return self.doc(db, "queue", m.QueueIn(name="Q", **fields).model_dump(mode="json"))

    def open(self, channel: str = "call", customer: str | None = None, direction: str = "inbound"):
        customer = customer or f"+1555{uuid.uuid4().int % 10**7:07d}"
        with core.tx() as db:
            ids = {"call_uuid": uuid.uuid4().hex} if channel == "call" else {}
            return cv.open(
                db, self.tenant, channel=channel, direction=direction, customer=customer, ours="+15550199", **ids
            )

    def enqueue(self, queue, channel: str = "call", customer: str | None = None, **kw) -> uuid.UUID:
        conv = self.open(channel, customer)
        with core.tx() as db:
            routing.enqueue(db, cv.lock(db, conv.id), queue, **kw)
        return conv.id

    def every(self, cid) -> list:
        """The conversation's stints, open offers and ended offers' tombstones, read through the seam."""
        with core.read() as db:
            return cv.rows(db, db.execute(select(T.conversation).where(T.conversation.c.id == cid)).one(), ended=True)

    def offers(self, cid, state: str = "offered") -> list[uuid.UUID]:
        return [a.agent_id for a in self.every(cid) if a.state == state]

    def offer_of(self, cid, agent):
        """The agent's open offer of the conversation, else its latest stint or tombstone."""
        mine = [a for a in self.every(cid) if a.agent_id == agent]
        return next((a for a in mine if a.state == "offered"), mine[-1] if mine else None)

    def conv(self, cid):
        with core.read() as db:
            return db.execute(select(T.conversation).where(T.conversation.c.id == cid)).one()

    def answer(self, cid, agent, event: str = "accept", *, rounds: bool = True, **cols):
        """`event` on the agent's open offer; after a decline or a miss the next round runs, if `rounds`."""
        with core.tx() as db:
            conv = cv.lock(db, cid)
            a = cv.move(db, next(a for a in cv.rows(db, cid, ("offered",)) if a.agent_id == agent), event, **cols)
            if event in ("decline", "miss"):
                routing.reoffer(db, conv, a)
        if rounds:
            jobs.run_now("next_round", str(cid), wait=True)
        return a

    def status(self, agent, status) -> None:
        """The agent's status changes, and its route_agent job runs."""
        with core.tx() as db:
            agents.set_status(db, self.tenant, agent, status, "manual")
        self.route_agent(agent)

    def route_agent(self, agent) -> None:
        with core.tx() as db:
            jobs.schedule(db, "route_agent", {"tenant_id": str(self.tenant), "agent_id": str(agent)}, key=str(agent))
        jobs.run_now("route_agent", str(agent), wait=True)

    def history(
        self, agent, customer: str, *, queue=None, direction="inbound", hours_ago: float = 1, via="queue", route=None
    ):
        """A closed contact whose stint started `hours_ago`, its queue and agent last edited before it."""
        with core.tx() as db:
            started = func.now() - timedelta(hours=hours_ago)
            conv = {"tenant_id": self.tenant, "channel": "call", "direction": direction, "state": "closed"}
            conv |= {"routing": {"route": route.model_dump(mode="json")}} if route else {}
            conv |= {"customer": customer, "queue_id": queue, "queued_at": started if queue else None}
            c = db.execute(insert(T.conversation).values(**conv).returning(*T.conversation.c)).one()
            a = self.stint(db, c.id, agent, started, started + timedelta(minutes=1), via)
            edited = func.least(T.config.c.updated_at, started - timedelta(minutes=1))
            db.execute(update(T.config).where(T.config.c.id.in_([agent, queue])).values(updated_at=edited))
            if not queue:
                cv._sticks(db, a, c)
        self.rebuild()

    def rebuild(self) -> None:
        with core.tx() as db:
            jobs.schedule(db, "sticky_rebuild", {"tenant_id": str(self.tenant), "after": ""}, key=str(self.tenant))
        jobs.run_now("sticky_rebuild", str(self.tenant), wait=True)

    def edit(self, doc) -> None:
        with config.write(access.OPERATOR, self.tenant) as db:
            config.save(db, doc, db.execute(select(T.config.c.body).where(T.config.c.id == doc)).scalar())

    def stint(self, db, cid, agent, started, ended, via="queue", channel="call"):
        row = {"tenant_id": self.tenant, "conversation_id": cid, "agent_id": agent, "role": "primary", "via": via}
        row |= {"channel": channel, "state": "done", "started_at": started, "ended_at": ended, "wrap_up_until": ended}
        return db.execute(insert(T.assignment).values(**row).returning(*T.assignment.c)).one()

    def events(self, type_: str, cid=None) -> list:
        E = T.events
        mine = (E.c.tenant_id == str(self.tenant), E.c.type == type_)
        mine += () if cid is None else (E.c.conversation_id == str(cid),)
        with core.read() as db:
            return db.execute(select(E).where(*mine).order_by(E.c.id)).all()

    def status_of(self, agent):
        with core.read() as db:
            return db.execute(select(T.presence).where(T.presence.c.agent_id == agent)).one()


@pytest.fixture
def floor(monkeypatch) -> Floor:
    return Floor(monkeypatch)


def ten_full_reads(monkeypatch) -> None:
    full, reads = routing.hot.full, []

    def counted(*args):
        if len(reads) == 10:
            raise RuntimeError("route_agent read who is full ten times")
        reads.append(args)
        return full(*args)

    monkeypatch.setattr(routing.hot, "full", counted)


def job(kind: str, key):
    J = T.jobs
    with core.read() as db:
        return db.execute(
            select(J, (J.c.run_at - func.now()).label("wait")).where(J.c.kind == kind, J.c.key == str(key))
        ).first()


def presence_lock(agent):
    return lambda db: db.execute(select(T.presence).where(T.presence.c.agent_id == agent).with_for_update())


def while_held(hold, then, *, waits: bool | None = True):
    """Run `then` while another transaction sits inside `hold(db)`, until `then` waits on that one's locks or
    finishes, as `waits` says it must (None: either); the holder then commits. Returns what `then` returned."""
    ready, release = Future(), threading.Event()

    def holding():
        with core.tx() as db:
            hold(db)
            ready.set_result(db.exec_driver_sql("SELECT pg_backend_pid()").scalar())
            release.wait(10)

    with ThreadPoolExecutor(2) as pool:
        holder = pool.submit(holding)
        pid = ready.result(10)
        result, deadline = pool.submit(then), time.monotonic() + 5
        with core.read() as db:
            while not (blocked := db.execute(BLOCKED, {"pid": pid}).scalar()) and not result.done():
                assert time.monotonic() < deadline, "the second transaction neither waited nor finished"
                time.sleep(0.02)
        release.set()
        holder.result(10)
        assert waits is None or bool(blocked) == waits, (
            f"the second transaction waited on the first one's locks: {bool(blocked)}"
        )
        return result.result(10)


@pytest.mark.parametrize(
    ("strategy", "first"),
    [
        ("availability", "longest_idle"),
        ("simultaneous", "longest_idle"),
        ("fewest_handled", "fewest"),
        ("least_handle_time", "shortest"),
    ],
)
@pytest.mark.parametrize("own", [False, True])
def test_each_strategy_offers_its_first_agent(floor, strategy, first, own):
    """Counted over the text stints since each agent's status change: the longest idle handled 3 texts of 30 s, the
    fewest one text of 40 min, the shortest two texts of 30 s. With `own`, the strategy is the enqueue's own route's
    and the queue's is availability."""
    agents_ = {name: floor.agent(idle_min=120) for name in ("longest_idle", "fewest", "shortest")}
    holder = floor.open()
    stints = {
        "longest_idle": [(15, 0.5), (16, 0.5), (17, 0.5)],
        "fewest": [(5, 40)],
        "shortest": [(10, 0.5), (11, 0.5)],
    }
    with core.tx() as db:
        for name, ends in stints.items():
            for ended_min, minutes in ends:
                ended = func.now() - timedelta(minutes=ended_min)
                floor.stint(db, holder.id, agents_[name], ended - timedelta(minutes=minutes), ended, channel="sms")
    listed = list(agents_.values())
    q = floor.queue(agents=listed, strategy="availability" if own else strategy, agents_at_once=1)
    route = m.Route(agents=listed, strategy=strategy, agents_at_once=1) if own else None
    assert floor.offers(floor.enqueue(q, own=route)) == [agents_[first]]


def test_a_round_offers_agents_at_once_of_the_queues_agents_and_team_members_only(floor):
    sales, support = uuid.uuid4(), uuid.uuid4()
    longest, member, newest = floor.agent(idle_min=30), floor.agent(teams=[sales], idle_min=20), floor.agent()
    floor.agent(teams=[support], idle_min=60)
    q = floor.queue(agents=[longest, newest], teams=[sales], agents_at_once=2)
    assert sorted(floor.offers(floor.enqueue(q))) == sorted([longest, member])
    assert floor.offers(floor.enqueue(q, agents=[newest])) == [newest], "an enqueue's own agents replace the queue's"
    assert [len(offers) for offers in floor.rings] == [2, 1], "each round rings its offers on its channel"


@pytest.mark.parametrize("op", ADMITS)
def test_a_skill_operator_set_through_the_api_offers_only_the_levels_it_admits(floor, op):
    api = admin(floor)
    spanish = api.post("/api/v1/skills", json={"name": "Spanish"}).json()["id"]
    levels = {level: floor.agent(skills=[(spanish, level)]) for level in (4, 5, 6)}
    needs = [{"skill_id": spanish, "operator": op, "level": 5}]
    body = {"name": "Q", "agents": [str(a) for a in levels.values()], "skills": needs, "strategy": "simultaneous"}
    q = uuid.UUID(api.post("/api/v1/queues", json=body).json()["id"])
    assert set(floor.offers(floor.enqueue(q))) == {levels[level] for level in ADMITS[op]}


@pytest.mark.parametrize("op", ADMITS)
def test_a_freed_agent_takes_a_waiting_call_only_at_a_level_its_operator_admits(floor, op):
    spanish = uuid.uuid4()
    levels = {level: floor.agent(skills=[(spanish, level)], available=False) for level in (4, 5, 6)}
    q = floor.queue(agents=list(levels.values()), skills=[{"skill_id": spanish, "operator": op, "level": 5}])
    cid = floor.enqueue(q)
    for level in sorted(levels, key=lambda lv: lv in ADMITS[op]):
        floor.status(levels[level], floor.available)
        assert floor.offers(cid) == ([levels[level]] if level in ADMITS[op] else []), level
        if level in ADMITS[op]:
            break


def test_capacity_counts_per_channel(floor):
    floor.settings(capacity=m.Capacity(sms=2).model_dump())
    agent = floor.agent()
    q = floor.queue(agents=[agent])
    texts = [floor.enqueue(q, "sms") for _ in range(3)]
    assert [floor.offers(cid) for cid in texts] == [[agent], [agent], []]
    calls = [floor.enqueue(q) for _ in range(2)]
    assert [floor.offers(cid) for cid in calls] == [[agent], []], "one call at a time, beside full text"


@pytest.mark.parametrize("blocks", [True, False])
def test_wrap_up_holds_the_slot_only_when_the_tenant_says_so(floor, blocks):
    floor.settings(acw_blocks_routing=blocks)
    agent = floor.agent()
    q = floor.queue(agents=[agent])
    first = floor.enqueue(q)
    floor.answer(first, agent)
    with core.tx() as db:
        cv.gone(db, cv.lock(db, first))
    assert floor.offers(first, "wrap_up") == [agent]
    assert floor.offers(floor.enqueue(q)) == ([] if blocks else [agent])


@pytest.mark.parametrize("blocks", [True, False])
def test_a_wrap_up_handed_back_to_a_queue_holds_the_slot_through_a_sweep_until_its_room_ends(floor, blocks):
    floor.settings(acw_blocks_routing=blocks)
    agent = floor.agent()
    q = floor.queue(agents=[agent])
    first = floor.enqueue(q)
    floor.answer(first, agent)
    with core.tx() as db:
        conv = cv.lock(db, first)
        cv.move(db, cv.primary_agent(db, first), "leave", reason="transferred", held_by_room=conv.room)
        cv.depart(db, conv, {"type": "queue", "queue_id": str(q)})
    sweep()
    waiting = floor.enqueue(q)
    assert floor.offers(waiting) == []
    mpc.mpc_status(first, 0, tenant=SimpleNamespace(id=floor.tenant), form={"EventName": "MPCEnd"})
    sweep()
    floor.route_agent(agent)
    assert floor.offers(waiting) == ([] if blocks else [agent])


def test_a_late_room_end_frees_nothing_of_a_later_stint(floor):
    floor.settings(acw_blocks_routing=False)
    agent = floor.agent()
    q = floor.queue(agents=[agent])
    cid = floor.enqueue(q)
    slot = CAP % (floor.tenant, "call", agent)
    end = partial(mpc.mpc_status, cid, tenant=SimpleNamespace(id=floor.tenant), form={"EventName": "MPCEnd"})
    floor.answer(cid, agent)
    for room in (0, 1):
        with core.tx() as db:
            conv = cv.lock(db, cid)
            assert conv.room == room
            cv.move(db, cv.primary_agent(db, cid), "leave", reason="transferred", held_by_room=room)
            cv.depart(db, conv, {"type": "queue", "queue_id": str(q)})
        if room == 0:
            end(0)
            with core.tx() as db:
                routing.enqueue(db, cv.lock(db, cid), q)
            floor.answer(cid, agent)
    assert set(core.redis.hvals(slot)) == {str(cid)}
    end(0)
    assert set(core.redis.hvals(slot)) == {str(cid)}, "a late duplicate room-0 end frees nothing"
    end(1)
    assert set(core.redis.hvals(slot)) == set()


def test_a_late_room_end_frees_only_the_stint_that_room_held(floor):
    floor.settings(acw_blocks_routing=False)
    agent = floor.agent()
    q = floor.queue(agents=[agent])
    cid = floor.enqueue(q)
    slot = CAP % (floor.tenant, "call", agent)
    end = partial(mpc.mpc_status, cid, tenant=SimpleNamespace(id=floor.tenant), form={"EventName": "MPCEnd"})
    floor.answer(cid, agent)
    for room in (0, 1):
        with core.tx() as db:
            conv = cv.lock(db, cid)
            cv.move(db, cv.primary_agent(db, cid), "leave", reason="transferred", held_by_room=room)
            cv.depart(db, conv, {"type": "queue", "queue_id": str(q)})
            if room == 0:
                cv.offer(db, cv.lock(db, cid), agent, via="direct", force=True)
    end(0)
    assert set(core.redis.hvals(slot)) == {str(cid)}, "room 1 still holds the slot"
    end(1)
    assert set(core.redis.hvals(slot)) == set()


def test_a_duplicate_room_end_frees_nothing_more(floor):
    floor.settings(acw_blocks_routing=False)
    agent = floor.agent()
    q = floor.queue(agents=[agent])
    cid = floor.enqueue(q)
    floor.answer(cid, agent)
    with core.tx() as db:
        conv = cv.lock(db, cid)
        primary = cv.move(db, cv.primary_agent(db, cid), "leave", reason="transferred", held_by_room=conv.room)
        cv.depart(db, conv, {"type": "queue", "queue_id": str(q)})
    slot = CAP % (floor.tenant, "call", agent)
    end = partial(mpc.mpc_status, cid, 0, tenant=SimpleNamespace(id=floor.tenant), form={"EventName": "MPCEnd"})
    end()
    assert set(core.redis.hvals(slot)) == set()
    core.redis.hset(slot, str(primary.id), str(cid))
    end()
    assert set(core.redis.hvals(slot)) == {str(cid)}, "the second end finds no held stint, so it frees nothing"


@pytest.mark.parametrize("mode", ["loose", "strict"])
def test_the_sticky_agent_alone_gets_the_first_round(floor, mode):
    usual, last = floor.agent(idle_min=60), floor.agent()
    q = floor.queue(agents=[usual, last], agents_at_once=2, retries=1, sticky={"mode": mode})
    floor.history(last, "+14155550177", queue=q)
    floor.history(usual, "+14155550177", queue=q, hours_ago=2)
    cid = floor.enqueue(q, customer="+14155550177")
    assert floor.offers(cid) == [last]
    floor.answer(cid, last, "decline")
    if mode == "loose":
        assert floor.offers(cid) == [usual]
    else:
        assert floor.offers(cid) == [] and floor.conv(cid).state == "open", "a strict sticky call gets one round"
        assert floor.events("conversation.overflowed", cid)[0].data["reason"] == "assignment_failed"


@pytest.mark.parametrize("channel", ["call", "sms"])
def test_a_strict_sticky_contact_whose_agent_is_away_fails_at_once_and_no_one_else_takes_it(floor, channel):
    usual, last = floor.agent(idle_min=60), floor.agent(available=False)
    q = floor.queue(agents=[usual, last], sticky={"mode": "strict"})
    floor.history(last, "+14155550178", queue=q)
    cid = floor.enqueue(q, channel, customer="+14155550178")
    floor.route_agent(usual)
    assert floor.offers(cid) == []
    jobs.run_now("queue_timeout", str(cid), wait=True)
    assert floor.events("conversation.overflowed", cid)[0].data["reason"] == "assignment_failed"


@pytest.mark.parametrize(("hours_ago", "sticks"), [(0.5, True), (2, False)])
def test_a_sticky_expiry_bounds_how_far_back_the_last_agent_counts(floor, hours_ago, sticks):
    usual, last = floor.agent(idle_min=60), floor.agent()
    q = floor.queue(agents=[usual, last], agents_at_once=1, sticky={"mode": "loose", "expiry_hours": 1})
    floor.history(last, "+14155550179", queue=q, hours_ago=hours_ago)
    assert floor.offers(floor.enqueue(q, customer="+14155550179")) == [last if sticks else usual]


@pytest.mark.parametrize(("enabled", "sticky"), [(True, True), (True, False), (False, True)])
def test_a_lead_who_calls_back_to_a_sticky_queue_is_offered_the_agent_who_dialed_them(floor, enabled, sticky):
    floor.settings(outbound_sticky={"enabled": enabled, "hours": 24})
    other, dialer = floor.agent(idle_min=60), floor.agent()
    floor.history(dialer, "+14155550180", direction="outbound")
    q = floor.queue(agents=[other, dialer], agents_at_once=1, retries=1, sticky={"mode": "loose"} if sticky else None)
    assert floor.offers(floor.enqueue(q, customer="+14155550180")) == [dialer if enabled and sticky else other]


def test_an_inbound_start_landing_inside_the_rebuild_keeps_the_outbound_agent_ended(floor, monkeypatch):
    floor.settings(outbound_sticky={"enabled": True, "hours": 24})
    other, dialer, last = floor.agent(idle_min=60), floor.agent(), floor.agent()
    q = floor.queue(agents=[other, dialer, last], agents_at_once=1, retries=1, sticky={"mode": "loose"})
    floor.history(dialer, "+14155550183", direction="outbound")
    core.redis.delete(routing.hot.OSTICKY % (floor.tenant, "+14155550183"))
    fill = routing.hot.fill

    def after_an_inbound_start(tenant_id, found, outbound):
        routing.hot.stick(str(floor.tenant), "+14155550183", last, datetime.now(UTC), uuid.uuid4(), None, str(q), 24)
        fill(tenant_id, found, outbound)

    monkeypatch.setattr(routing.hot, "fill", after_an_inbound_start)
    floor.rebuild()
    assert floor.offers(floor.enqueue(q, customer="+14155550183")) == [last]


@pytest.mark.parametrize(("late", "kept"), [(-1, "first"), (0, "first"), (1, "late")])
def test_a_sticky_write_replaces_only_an_older_stints_agent_and_expires_from_its_start(late, kept):
    """A second write for the same stint (0), one primary `late` seconds after it, older (-1) or newer (1)."""
    hot, tenant, at, sid, c = routing.hot, uuid.uuid4(), datetime.now(UTC), uuid.uuid4(), "+14155550191"
    core.redis.set(hot.OSTICKY % (tenant, c), "dialer")
    hot.stick(tenant, c, "first", at, sid, 1, "q", 24)
    hot.stick(tenant, c, "stale", at - timedelta(seconds=1), uuid.uuid4(), 24)
    assert hot.stuck(tenant, "q", c, True)[2] == "first", "a queue's write ends an older outbound agent, bare or late"
    assert core.redis.pexpiretime(hot.OSTICKY % (tenant, c)) == int(at.timestamp() * 1000) + 24 * 3_600_000
    hot.stick(tenant, c, "dialer", at + timedelta(seconds=5), uuid.uuid4(), 24)
    hot.stick(tenant, c, "late", at + timedelta(seconds=late), sid, 1, "q", 24)
    assert hot.stuck(tenant, "q", c, False)[2] == kept
    assert hot.stuck(tenant, "q", c, True)[2] == "dialer", "an older queue write leaves a newer outbound agent"
    became = at + timedelta(seconds=late) if kept == "late" else at
    assert core.redis.pexpiretime(hot.STICKY % (tenant, "q", c)) == int(became.timestamp() * 1000) + 3_600_000
    hot.stick(tenant, "+14155550192", "gone", at - timedelta(hours=2), uuid.uuid4(), 1, "q")
    assert hot.stuck(tenant, "q", "+14155550192", False) is None, "a stint's sticky hours run from its start"


@pytest.mark.parametrize("edited", ["queue", "agent"])
def test_the_rebuild_skips_a_stint_from_before_its_queue_or_agent_was_last_edited(floor, edited):
    usual, last = floor.agent(idle_min=60), floor.agent()
    q = floor.queue(agents=[usual, last], agents_at_once=1, sticky={"mode": "loose"})
    floor.history(last, "+14155550185", queue=q)
    floor.edit(q if edited == "queue" else last)
    core.redis.delete(routing.hot.STICKY % (floor.tenant, q, "+14155550185"))
    floor.rebuild()
    assert floor.offers(floor.enqueue(q, customer="+14155550185")) == [usual]


def test_an_enqueues_own_route_sets_and_rebuilds_the_sticky_agent_its_queue_has_none_for(floor):
    usual, last = floor.agent(idle_min=60), floor.agent()
    q = floor.queue(agents=[usual], agents_at_once=1)
    route = m.Route(agents=[usual, last], agents_at_once=1, sticky={"mode": "loose"})
    floor.history(last, "+14155550189", queue=q, route=route)
    assert floor.offers(floor.enqueue(q, customer="+14155550189", own=route)) == [last]
    assert floor.offers(floor.enqueue(q, customer="+14155550189")) == [usual]


def test_a_queue_edit_keeps_the_sticky_agent_an_enqueues_own_route_set_through_a_rebuild(floor):
    usual, last = floor.agent(idle_min=60), floor.agent()
    q = floor.queue(agents=[usual], agents_at_once=1)
    route = m.Route(agents=[usual, last], agents_at_once=1, sticky={"mode": "loose"})
    floor.history(last, "+14155550190", queue=q, route=route)
    floor.edit(q)
    before = floor.enqueue(q, customer="+14155550190", own=route)
    assert floor.offers(before) == [last]
    floor.answer(before, last, "decline", rounds=False)
    core.redis.delete(routing.hot.STICKY % (floor.tenant, q, "+14155550190"))
    floor.rebuild()
    assert floor.offers(floor.enqueue(q, customer="+14155550190", own=route)) == [last], "the rebuild routes as before"


def test_the_rebuild_sets_each_queues_sticky_agent_for_a_customer(floor):
    usual, one, two = floor.agent(idle_min=60), floor.agent(), floor.agent()
    q1, q2 = (floor.queue(agents=[usual, a], agents_at_once=1, sticky={"mode": "loose"}) for a in (one, two))
    floor.history(one, "+14155550193", queue=q1, hours_ago=2)
    floor.history(two, "+14155550193", queue=q2)
    core.redis.delete(routing.hot.STICKY % (floor.tenant, q1, "+14155550193"))
    floor.rebuild()
    assert [floor.offers(floor.enqueue(q, customer="+14155550193")) for q in (q1, q2)] == [[one], [two]]


def test_the_rebuild_keeps_the_outbound_agent_of_an_outbound_contact_queued_since(floor):
    floor.settings(outbound_sticky={"enabled": True, "hours": 24})
    other, dialer = floor.agent(idle_min=60), floor.agent()
    q = floor.queue(agents=[other, dialer], agents_at_once=1, sticky={"mode": "loose"})
    floor.history(dialer, "+14155550194", direction="outbound")
    with core.tx() as db:
        C = T.conversation
        queued = update(C).where(C.c.tenant_id == floor.tenant, C.c.customer == "+14155550194")
        db.execute(queued.values(queue_id=q, queued_at=func.now()))
    core.redis.delete(routing.hot.OSTICKY % (floor.tenant, "+14155550194"))
    floor.rebuild()
    assert floor.offers(floor.enqueue(q, customer="+14155550194")) == [dialer]


def test_the_rebuild_passes_over_a_newer_stint_of_an_agent_outside_the_queue(floor):
    usual, last, outside = floor.agent(idle_min=60), floor.agent(), floor.agent()
    q = floor.queue(agents=[usual, last], agents_at_once=1, sticky={"mode": "loose"})
    floor.history(last, "+14155550186", queue=q, hours_ago=2)
    floor.history(outside, "+14155550186", queue=q)
    assert floor.offers(floor.enqueue(q, customer="+14155550186")) == [last]


@pytest.mark.parametrize(("hand_over", "warm"), [(cv.promote, True), (mpc.hand_over, True), (mpc.hand_over, False)])
def test_the_rebuild_dates_a_transfer_target_from_when_it_took_over(floor, hand_over, warm):
    first, target, other = floor.agent(), floor.agent(available=False), floor.agent(available=False)
    q = floor.queue(agents=[first, target, other], agents_at_once=1, sticky={"mode": "loose", "expiry_hours": 1})
    call = floor.enqueue(q, customer="+14155550188")
    floor.answer(call, first)
    with core.tx() as db:
        t = cv.offer(db, cv.lock(db, call), target, via="transfer", mode="warm")
        if warm:
            t = cv.move(db, t, "accept")
        else:
            cv.move(db, cv.primary_agent(db, call), "leave")
    floor.status(other, floor.available)
    sms = floor.enqueue(q, "sms", customer="+14155550188")
    floor.answer(sms, first, "decline")
    floor.answer(sms, other)
    with core.tx() as db:
        hand_over(db, cv.lock(db, call), next(a for a in cv.rows(db, call) if a.id == t.id))
        A = T.assignment
        joined = A.c.started_at - timedelta(minutes=30 if warm else 0)
        db.execute(update(A).where(A.c.id == t.id).values(started_at=joined))
    key = routing.hot.STICKY % (floor.tenant, q, "+14155550188")
    live = core.redis.get(key), core.redis.ttl(key)
    core.redis.delete(key)
    floor.rebuild()
    assert core.redis.get(key) == live[0], "the rebuild orders and dates the stint as its live write did"
    assert live[0].endswith(str(target)), "the target took over after the other agent's start"
    assert abs(core.redis.ttl(key) - live[1]) <= 2, "its expiry counts from when it took over"


@pytest.mark.parametrize("newer", [False, True])
def test_the_rebuild_sets_no_agent_from_a_transfer_target_with_no_stint_it_came_from(floor, newer):
    usual, last, moved = floor.agent(idle_min=60), floor.agent(), floor.agent()
    q = floor.queue(agents=[usual, last, moved], agents_at_once=1, sticky={"mode": "loose"})
    floor.history(last, "+14155550189", queue=q, hours_ago=1 if newer else 2)
    floor.history(moved, "+14155550189", queue=q, hours_ago=2 if newer else 1, via="transfer")
    core.redis.delete(routing.hot.STICKY % (floor.tenant, q, "+14155550189"))
    floor.rebuild()
    assert floor.offers(floor.enqueue(q, customer="+14155550189")) == [last if newer else usual]


def test_a_loose_sticky_agent_is_offered_alone_only_in_the_first_round(floor):
    usual, other, last = floor.agent(idle_min=60), floor.agent(idle_min=30), floor.agent()
    q = floor.queue(agents=[usual, other, last], agents_at_once=2, retries=1, sticky={"mode": "loose"})
    floor.history(last, "+14155550187", queue=q)
    cid = floor.enqueue(q, "sms", customer="+14155550187")
    assert floor.offers(cid) == [last]
    floor.answer(cid, last, "decline")
    assert sorted(floor.offers(cid)) == sorted([usual, other]), "the next round offers the queue's agents at once"


@pytest.mark.parametrize("channel", ["call", "sms"])
def test_rounds_offer_the_next_agent_until_retries_run_out_then_the_contact_overflows(floor, channel):
    a, b, c = floor.agent(idle_min=30), floor.agent(idle_min=20), floor.agent(idle_min=10)
    other = floor.queue()
    q = floor.queue(agents=[a, b, c], agents_at_once=1, retries=1, overflow_action={"type": "queue", "queue_id": other})
    cid = floor.enqueue(q, channel)
    assert floor.offers(cid) == [a]
    floor.answer(cid, a, "decline")
    assert floor.offers(cid) == [b]
    floor.answer(cid, b, "decline")
    assert floor.offers(cid) == []
    [over] = floor.events("conversation.overflowed", cid)
    assert (over.data["reason"], floor.conv(cid).result) == ("assignment_failed", "assignment_failed")
    if channel == "call":
        assert floor.conv(cid).state == "open"
        assert job("plivo", f"redirect:{floor.tenant}:{cid}:1").payload["action"]["queue_id"] == str(other)
    else:
        assert floor.conv(cid).state == "closed", "a text that overflows closes"


def test_ten_retries_offer_eleven_rounds_before_the_contact_overflows(floor):
    listed = [floor.agent(idle_min=60 - i) for i in range(12)]
    cid = floor.enqueue(floor.queue(agents=listed, agents_at_once=1, retries=10), "sms")
    for agent in listed[:11]:
        assert floor.offers(cid) == [agent]
        floor.answer(cid, agent, "decline")
    assert (floor.offers(cid), floor.conv(cid).result) == ([], "assignment_failed")


def test_a_queue_of_capacity_one_refuses_its_second_waiting_call_and_one_may_hold_ten_thousand(floor):
    q = floor.queue(capacity=1)
    floor.enqueue(q)
    with pytest.raises(routing.Full):
        floor.enqueue(q)
    assert m.QueueIn(name="Big", capacity=10_000).capacity == 10_000


def test_a_call_waits_a_day_for_an_agent_before_its_queue_times_out(floor):
    cid = floor.enqueue(floor.queue(wait_timeout_sec=86_400))
    assert timedelta(hours=23, minutes=59) < job("queue_timeout", cid).wait <= timedelta(days=1)


def test_no_round_starts_while_an_offer_of_the_last_is_still_out(floor):
    a, b, c = floor.agent(idle_min=30), floor.agent(idle_min=20), floor.agent(idle_min=10)
    q = floor.queue(agents=[a, b, c], agents_at_once=2, retries=1)
    cid = floor.enqueue(q)
    floor.answer(cid, a, "miss")
    assert floor.offers(cid) == [b]
    floor.answer(cid, b, "miss")
    assert floor.offers(cid) == [c]


@pytest.mark.parametrize("channel", ["call", "sms"])
def test_a_call_miss_at_the_strike_limit_moves_the_agent_to_break(floor, channel):
    floor.settings(rona={"strikes": 2, "status_id": str(floor.brk)})
    missing, next_ = floor.agent(idle_min=30, misses=1), floor.agent()
    q = floor.queue(agents=[missing, next_], agents_at_once=1, retries=1)
    cid = floor.enqueue(q, channel)
    if channel == "call":
        with core.tx() as db:
            cv.edit(db, next(iter(cv.rows(db, cv.lock(db, cid).id, ("offered",)))), call_uuid=uuid.uuid4().hex)
    jobs.run_now("offer_timeout", str(floor.offer_of(cid, missing).id), wait=True)
    jobs.run_now("next_round", str(cid), wait=True)
    assert floor.offers(cid) == [next_] and floor.offers(cid, "missed") == [missing]
    presence = floor.status_of(missing)
    if channel == "call":
        assert (presence.status_id, presence.misses) == (floor.brk, 0), "a ring whose leg Plivo named rang out"
        [moved] = floor.events("agent.status_changed")
        assert (moved.agent_id, moved.data["reason"]) == (str(missing), "rona")
    else:
        assert (presence.status_id, presence.misses) == (floor.available, 1), "text offers carry no strikes"


def test_an_agent_who_turns_unavailable_loses_its_offers_and_the_next_agent_gets_them(floor):
    first, second = floor.agent(idle_min=30), floor.agent()
    q = floor.queue(agents=[first, second], agents_at_once=1, retries=1)
    cid = floor.enqueue(q)
    assert floor.offers(cid) == [first]
    floor.status(first, floor.away)
    jobs.run_now("next_round", str(cid), wait=True)
    assert floor.offers(cid) == [second] and floor.offers(cid, "withdrawn") == [first]
    assert floor.events("offer.withdrawn", cid)[0].data["reason"] == "agent_offline"


def test_an_agent_back_before_its_offer_is_withdrawn_keeps_it(floor):
    first, second = floor.agent(idle_min=30), floor.agent()
    cid = floor.enqueue(floor.queue(agents=[first, second], agents_at_once=1, retries=1))
    with core.tx() as db:
        agents.set_status(db, floor.tenant, first, floor.away, "manual")
    back = update(T.presence).where(T.presence.c.agent_id == first).values(status_id=floor.available)
    while_held(lambda db: db.execute(back), lambda: jobs.run_now("route_agent", str(first), wait=True))
    assert (floor.offers(cid), floor.offers(cid, "withdrawn")) == ([first], [])


def test_a_decline_frees_its_agent_for_other_waiting_work_but_not_for_what_it_declined(floor):
    agent, other = floor.agent(idle_min=30), floor.agent()
    declined = floor.enqueue(floor.queue(agents=[agent, other], agents_at_once=1, retries=1))
    waiting = floor.enqueue(floor.queue(agents=[agent]))
    floor.answer(declined, agent, "decline", rounds=False)
    jobs.run_now("route_agent", str(agent), wait=True)
    jobs.run_now("next_round", str(declined), wait=True)
    assert (floor.offers(declined), floor.offers(waiting)) == ([other], [agent])


@pytest.mark.parametrize("channel", ["call", "email", "sms", "chat", "whatsapp"])
def test_a_decline_by_the_only_agent_leaves_a_call_or_an_email_waiting_and_fails_any_other_text(floor, channel):
    agent = floor.agent()
    cid = floor.enqueue(floor.queue(agents=[agent], retries=1, wait_timeout_sec=0), channel)
    floor.answer(cid, agent, "decline")
    jobs.run_now("queue_timeout", str(cid), wait=True)
    jobs.run_now("route_agent", str(agent), wait=True)
    overflowed = [e.data["reason"] for e in floor.events("conversation.overflowed", cid)]
    waits = channel in ("call", "email")
    assert (floor.conv(cid).state, overflowed) == (("queued", []) if waits else ("closed", ["assignment_failed"]))
    assert floor.offers(cid) == []


def test_a_text_that_finds_nobody_left_fails_before_an_agent_who_joins_next_is_offered_it(floor):
    team = uuid.uuid4()
    first = floor.agent(teams=[team])
    cid = floor.enqueue(floor.queue(teams=[team], retries=10), "sms")
    floor.answer(cid, first, "decline")
    joined = floor.agent(teams=[team])
    floor.route_agent(joined)
    jobs.run_now("queue_timeout", str(cid), wait=True)
    overflowed = [e.data["reason"] for e in floor.events("conversation.overflowed", cid)]
    assert (floor.conv(cid).state, overflowed, floor.offer_of(cid, joined)) == ("closed", ["assignment_failed"], None)


@pytest.mark.parametrize("fails", ["strict", "ring_failed"])
def test_an_assignment_failure_due_before_an_offer_leaves_it_open_and_that_offers_end_decides(floor, fails):
    if fails == "strict":
        agent = floor.agent(available=False)
        q = floor.queue(agents=[agent], sticky={"mode": "strict"})
        floor.history(agent, "+14155550179", queue=q)
        cid = floor.enqueue(q, "sms", customer="+14155550179")
        floor.status(agent, floor.available)
    else:
        first, agent = floor.agent(idle_min=30), floor.agent()
        cid = floor.enqueue(floor.queue(agents=[first, agent], agents_at_once=1, retries=1))
        floor.answer(cid, first, "miss", rounds=False, reason="ring_failed")
        floor.route_agent(agent)
    assert (floor.offers(cid), job("queue_timeout", cid).payload["reason"]) == ([agent], "assignment_failed")
    jobs.run_now("queue_timeout", str(cid), wait=True)
    assert (floor.offers(cid), floor.conv(cid).state) == ([agent], "queued")
    floor.answer(cid, agent, "decline")
    assert [e.data["reason"] for e in floor.events("conversation.overflowed", cid)] == ["assignment_failed"]


@pytest.mark.parametrize("ends", ["decline", "miss", "away"])
def test_a_call_whose_refused_ring_met_a_later_offer_fails_when_that_offer_ends_though_rounds_remain(floor, ends):
    first, agent = floor.agent(idle_min=30), floor.agent()
    cid = floor.enqueue(floor.queue(agents=[first, agent], agents_at_once=1, retries=10, wait_timeout_sec=0))
    floor.answer(cid, first, "miss", rounds=False, reason="ring_failed")
    floor.route_agent(agent)
    jobs.run_now("queue_timeout", str(cid), wait=True)
    assert floor.offers(cid) == [agent]
    if ends == "away":
        floor.status(agent, floor.away)
        jobs.run_now("next_round", str(cid), wait=True)
    else:
        floor.answer(cid, agent, ends)
    overflowed = [e.data["reason"] for e in floor.events("conversation.overflowed", cid)]
    assert (floor.offers(cid), overflowed) == ([], ["assignment_failed"])


@pytest.mark.parametrize("wait", [0, 1800])
def test_a_call_whose_every_agent_missed_it_rings_them_again_once_300_s_pass_while_rounds_remain(floor, wait):
    first, second = floor.agent(idle_min=30), floor.agent()
    cid = floor.enqueue(floor.queue(agents=[first, second], agents_at_once=1, retries=10, wait_timeout_sec=wait))
    floor.answer(cid, first, "miss")
    floor.answer(cid, second, "miss")
    for agent in (first, second):
        jobs.run_now("route_agent", str(agent), wait=True)
    assert floor.offers(cid) == []
    pending = job("next_round", cid)
    assert pending and timedelta(seconds=290) < pending.wait <= timedelta(seconds=300)
    age(301)
    jobs.run_now("next_round", str(cid), wait=True)
    assert floor.offers(cid) == [first]


def changed(floor, agent, change: str) -> None:
    p = access.Principal(str(floor.tenant), "key", scopes=frozenset({"admin"}))
    if change == "deleted":
        with core.tx() as db:
            adopted = T.config.c.body.concat({"endpoint_adopted": True})
            db.execute(update(T.config).where(T.config.c.id == agent).values(body=adopted))
        agents.delete_agent(str(agent), p)
    else:
        agents.patch_agent(str(agent), agents.AgentPatch(**{change: []}), p)
    jobs.run_now("config_rounds", str(floor.tenant), wait=True)


@pytest.mark.parametrize("change", ["deleted", "teams", "skills"])
def test_a_text_whose_last_untried_agent_leaves_its_route_fails_once_that_change_commits(floor, change):
    team, spanish = uuid.uuid4(), uuid.uuid4()
    first = floor.agent(teams=[team], skills=[(spanish, 5)], idle_min=30)
    last = floor.agent(teams=[team], skills=[(spanish, 5)], available=False)
    q = floor.queue(teams=[team], skills=[{"skill_id": spanish, "level": 5}], agents_at_once=1, retries=10)
    cid = floor.enqueue(q, "sms")
    floor.answer(cid, first, "decline")
    assert floor.conv(cid).state == "queued"
    changed(floor, last, change)
    jobs.run_now("next_round", str(cid), wait=True)
    overflowed = [e.data["reason"] for e in floor.events("conversation.overflowed", cid)]
    assert (floor.conv(cid).state, overflowed) == ("closed", ["assignment_failed"])


@pytest.mark.parametrize("change", ["another_deleted", "queue_edited"])
def test_a_text_whose_route_still_holds_an_untried_agent_after_an_admin_change_waits_for_it(floor, change):
    first, last, gone = floor.agent(idle_min=30), floor.agent(available=False), floor.agent(available=False)
    q = floor.queue(agents=[first, last, gone], agents_at_once=1, retries=10)
    cid = floor.enqueue(q, "sms")
    floor.answer(cid, first, "decline")
    if change == "another_deleted":
        changed(floor, gone, "deleted")
    else:
        with config.write(access.OPERATOR, floor.tenant) as db:
            body = db.execute(select(T.config.c.body).where(T.config.c.id == q)).scalar()
            config.save(db, q, body | {"agents": [str(first)]})
        jobs.run_now("config_rounds", str(floor.tenant), wait=True)
    jobs.run_now("next_round", str(cid), wait=True)
    assert (floor.conv(cid).state, floor.events("conversation.overflowed", cid)) == ("queued", [])
    floor.status(last, floor.available)
    assert floor.offers(cid) == [last]


def test_a_call_whose_last_untried_agent_is_deleted_keeps_waiting_for_its_tried_agents_to_lapse(floor):
    first, last = floor.agent(idle_min=30), floor.agent(available=False)
    cid = floor.enqueue(floor.queue(agents=[first, last], agents_at_once=1, retries=10, wait_timeout_sec=0))
    floor.answer(cid, first, "miss")
    changed(floor, last, "deleted")
    assert timedelta(seconds=290) < job("next_round", cid).wait <= timedelta(seconds=300)
    age(301)
    jobs.run_now("next_round", str(cid), wait=True)
    assert (floor.offers(cid), floor.events("conversation.overflowed", cid)) == ([first], [])


def age(seconds: float) -> None:
    """`seconds` pass for every Redis key, as world.travel ages them; Postgres stays put."""
    core.redis.eval(AGE, 0, int(seconds * 1000))


def wake(floor, cid, how: str, agent) -> None:
    if how == "route_agent":
        floor.route_agent(agent)
    else:
        with core.tx() as db:
            jobs.schedule(db, "next_round", {"conversation_id": str(cid)}, key=str(cid))
        jobs.run_now("next_round", str(cid), wait=True)


def test_a_call_waiting_as_long_as_a_call_stays_open_keeps_its_round_count(floor):
    first, second = floor.agent(idle_min=30), floor.agent(available=False)
    cid = floor.enqueue(floor.queue(agents=[first, second], agents_at_once=1, retries=1, wait_timeout_sec=86_400))
    floor.answer(cid, first, "decline")
    age(cv.CALL_SESSION.total_seconds() - 1)
    floor.status(second, floor.available)
    assert floor.offers(cid) == [second]
    floor.answer(cid, second, "decline")
    overflowed = [e.data["reason"] for e in floor.events("conversation.overflowed", cid)]
    assert (floor.offers(cid), overflowed) == ([], ["assignment_failed"]), "its second round was its last"


@pytest.mark.parametrize("how", ["route_agent", "next_round"])
@pytest.mark.parametrize(
    ("gap", "since", "again"),
    [(0, 299, False), (0, 301, True), (150, 200, False)],
    ids=["held", "lapsed", "renewed-by-a-later-offer"],
)
def test_a_waiting_call_forgets_who_it_was_offered_to_once_300_s_pass_with_no_new_offer(floor, gap, since, again, how):
    first, second = floor.agent(idle_min=30), floor.agent()
    cid = floor.enqueue(floor.queue(agents=[first, second], agents_at_once=1, retries=3, wait_timeout_sec=1800))
    floor.answer(cid, first, "decline", rounds=False)
    age(gap)
    jobs.run_now("next_round", str(cid), wait=True)
    floor.answer(cid, second, "decline", rounds=False)
    age(since)
    wake(floor, cid, how, first)
    assert floor.offers(cid) == ([first] if again else [])
    if again:
        floor.answer(cid, first, "decline")
        assert floor.offers(cid) == [second], "that offer started a new run, which holds only its agent"


def rolled_back(work) -> None:
    with pytest.raises(RuntimeError), core.tx() as db:
        work(db)
        raise RuntimeError("the transaction fails after its round")


def died_before_commit(work) -> None:
    """`work(db)` in a transaction that ends as a killed process's does: Postgres rolls it back, and nothing else
    runs."""
    with core.engine.connect() as db:
        t = db.begin()
        db.info.update(before_commit=[], after_commit=[], undo=[], tx={})
        work(db)
        t.rollback()
        db.invalidate()  # its connection dies with it, and with the connection this transaction's memo


@pytest.mark.parametrize("how", ["route_agent", "next_round"])
@pytest.mark.parametrize("end", [rolled_back, died_before_commit])
def test_a_round_that_never_commits_counts_no_round_and_tries_no_agent(floor, end, how):
    agent = floor.agent(available=False)
    cid = floor.enqueue(floor.queue(agents=[agent], retries=0))
    with core.tx() as db:
        agents.set_status(db, floor.tenant, agent, floor.available, "manual")
    end(lambda db: routing.route(db, cv.lock(db, cid)))
    if end is died_before_commit:
        sweep()  # the dead round's slot, which keeps the agent full until the sweep gives it back
    wake(floor, cid, how, agent)
    assert (floor.offers(cid), floor.conv(cid).result) == ([agent], None), "its one round is still to run"


@pytest.mark.parametrize("how", ["route_agent", "next_round"])
def test_a_round_rolled_back_renews_no_agents_300_s(floor, how):
    first, second = floor.agent(idle_min=30), floor.agent()
    cid = floor.enqueue(floor.queue(agents=[first, second], agents_at_once=1, retries=3, wait_timeout_sec=1800))
    floor.answer(cid, first, "decline", rounds=False)
    age(299)
    rolled_back(lambda db: routing.route(db, cv.lock(db, cid)))
    age(2)
    wake(floor, cid, how, first)
    assert floor.offers(cid) == [first], "301 s after its offer, and no offer since committed"


def until_redis_answers() -> None:
    deadline = time.monotonic() + 5
    while True:
        try:
            return core.redis.ping()
        except RedisError:
            assert time.monotonic() < deadline, "a probe answers"
            time.sleep(0.02)


@contextmanager
def losing(command: str, key: str):
    """The first `command` on a key named with `key`, sent alone or in a pipeline, runs on Redis and loses its reply."""
    send, execute, lost = Redis.execute_command, Pipeline.execute, []

    def lose(sent: list, replied):
        if not lost and any(c[0] == command and key in str(c[1:]) for c in sent):
            lost.append(sent)
            raise RedisTimeout("Timeout reading from socket")
        return replied

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(core, "REDIS_SHUT", 0.5)
        patch.setattr(Redis, "execute_command", lambda self, *a, **kw: lose([a], send(self, *a, **kw)))
        patch.setattr(Pipeline, "execute", lambda self, *a: lose([c for c, _ in self.command_stack], execute(self, *a)))
        yield
    until_redis_answers()


def lost_reply(command: str, key: str, work) -> None:
    """`work(db)` in a transaction whose `command` loses its reply, as `losing` does, so the transaction rolls back."""
    with losing(command, key), pytest.raises(RedisTimeout), core.tx() as db:
        work(db)


@pytest.mark.parametrize("how", ["route_agent", "next_round"])
def test_a_round_whose_store_ran_but_lost_its_reply_renews_no_agents_300_s(floor, how):
    first, second = floor.agent(idle_min=30), floor.agent()
    cid = floor.enqueue(floor.queue(agents=[first, second], agents_at_once=1, retries=3, wait_timeout_sec=1800))
    floor.answer(cid, first, "decline", rounds=False)
    age(299)
    lost_reply("EVALSHA", ":offers:", lambda db: routing.route(db, cv.lock(db, cid)))
    age(2)
    wake(floor, cid, how, first)
    assert floor.offers(cid) == [first], "301 s after its offer, and the offer whose create timed out rolled back"


def sweep() -> None:
    with core.tx() as db:
        jobs.schedule(db, "sweep", {}, key="sweep")
    jobs.run_now("sweep", "sweep", wait=True)


def slots(floor, agent, channel: str = "call") -> tuple[set, bool]:
    """The conversations Redis holds the agent's slots on the channel for, and whether it counts the agent full."""
    t = floor.tenant
    return set(core.redis.hvals(CAP % (t, channel, agent))), core.redis.sismember(FULL % (t, channel), str(agent))


def lost_its_take_reply(work) -> None:
    lost_reply("EVALSHA", ":cap:", work)


@pytest.mark.parametrize("end", [rolled_back, died_before_commit, lost_its_take_reply])
def test_a_slot_a_round_took_and_never_committed_is_given_back_at_once_or_by_the_agents_next_job(floor, end):
    agent = floor.agent(available=False)
    cid = floor.enqueue(floor.queue(agents=[agent]))
    with core.tx() as db:
        agents.set_status(db, floor.tenant, agent, floor.available, "manual")
    end(lambda db: routing.route(db, cv.lock(db, cid)))
    left = ({str(cid)}, True) if end is died_before_commit else (set(), False)
    assert slots(floor, agent) == left, "only a dead process leaves its slot, which keeps the agent full, never over"
    floor.route_agent(agent)
    assert floor.offers(cid) == [agent]


def test_a_slot_take_that_lost_its_reply_rolls_back_to_the_slot_an_open_offer_holds(floor):
    agent = floor.agent()
    cid = floor.enqueue(floor.queue(agents=[agent]))
    lost_its_take_reply(lambda db: routing.hot.take(db, cv.lock(db, cid), agent, uuid.uuid4(), 1))
    other = floor.enqueue(floor.queue(agents=[agent]))
    assert (floor.offers(cid), floor.offers(other)) == ([agent], []), "the open offer still holds the agent's one slot"


def opened(db, floor, channel: str, direction: str):
    ids = {"call_uuid": uuid.uuid4().hex} if channel == "call" else {}
    customer = f"+1555{uuid.uuid4().int % 10**7:07d}"
    return cv.open(db, floor.tenant, channel=channel, direction=direction, customer=customer, ours="+15550199", **ids)


def dials(db, floor, agent, q):
    return cv.dial(db, opened(db, floor, "call", "outbound"), agent, "sip:agent@example.test")


def texts_from_a_call(db, floor, agent, q):
    return cv.offer(db, opened(db, floor, "sms", "outbound"), agent, via="direct")


def rings_a_caller(db, floor, agent, q):
    routing.enqueue(db, opened(db, floor, "call", "inbound"), q)


@pytest.mark.parametrize(("take", "channel"), [(dials, "call"), (texts_from_a_call, "sms"), (rings_a_caller, "call")])
def test_a_slot_taken_for_a_conversation_whose_process_died_before_its_commit_is_given_back_by_the_next_sweep(
    floor, take, channel
):
    agent = floor.agent()
    q = floor.queue(agents=[agent])
    died_before_commit(lambda db: take(db, floor, agent, q))
    sweep()
    assert floor.offers(floor.enqueue(q, channel)) == [agent]


def test_a_slot_taken_by_a_transaction_still_open_across_a_sweep_holds_the_agent_once_it_commits(floor):
    agent, made = floor.agent(), []
    while_held(lambda db: made.append(dials(db, floor, agent, None)), sweep, waits=False)
    assert slots(floor, agent)[0] == {str(made[0].conversation_id)}
    assert floor.offers(floor.enqueue(floor.queue(agents=[agent]))) == [], "the dial holds the agent's one call slot"


def decline(floor, cid, agent) -> None:
    floor.answer(cid, agent, "decline", rounds=False)


def hang_up(floor, cid, agent) -> None:
    floor.answer(cid, agent)
    with core.tx() as db:
        cv.gone(db, cv.lock(db, cid))


def test_a_decline_rolled_back_after_a_round_took_its_agents_last_slot_leaves_the_agent_within_its_cap(floor):
    agent = floor.agent()
    declined = floor.enqueue(floor.queue(agents=[agent]), "sms")
    q, offer = floor.queue(agents=[agent]), floor.offer_of(declined, agent)
    with pytest.raises(RuntimeError), core.tx() as db:
        routing.reoffer(db, cv.lock(db, declined), cv.move(db, offer, "decline"))
        other = floor.enqueue(q, "sms")
        raise RuntimeError("the decline's transaction fails after that round committed")
    assert (floor.offers(declined), floor.offers(other)) == ([agent], []), "one text at a time at capacity 1"


def gives_refused(work) -> None:
    """`work(db)` in a transaction that commits while Redis refuses every slot give."""
    execute = Pipeline.execute

    def refuse(self, *a):
        if any(c[0] == "HDEL" and ":cap:" in str(c[1:]) for c, _ in self.command_stack):
            raise RedisError("Connection refused")
        return execute(self, *a)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(Pipeline, "execute", refuse)
        with core.tx() as db:
            work(db)


def committed(work) -> list:
    """`work(db)` in a transaction that commits; returns what it queued to run after its commit, not yet run."""
    with core.engine.connect() as db:
        t = db.begin()
        db.info.update(before_commit=(soon := []), after_commit=(late := []), undo=[], tx={})
        work(db)
        for fn in soon:
            fn()
        t.commit()
        db.invalidate()  # no later checkout of this connection reads this transaction's memo
    return late


def died_after_commit(work) -> None:
    """`work(db)` in a transaction that commits, and then its process dies before what it queued to run after."""
    committed(work)


@pytest.mark.parametrize("end", [gives_refused, died_after_commit])
def test_a_slot_give_lost_after_its_commit_keeps_the_agent_full_until_its_job_gives_it_back(floor, end):
    agent = floor.agent()
    declined = floor.enqueue(floor.queue(agents=[agent]))
    waiting = floor.enqueue(floor.queue(agents=[agent]))
    offer = floor.offer_of(declined, agent)
    end(lambda db: routing.reoffer(db, cv.lock(db, declined), cv.move(db, offer, "decline")))
    assert slots(floor, agent) == ({str(declined)}, True), "full, never over"
    jobs.run_now("route_agent", str(agent), wait=True)
    assert floor.offers(waiting) == [agent], "the job the decline committed finds Postgres freed the agent"


@pytest.mark.parametrize(("end", "lost"), [(decline, False), (hang_up, False), (decline, True)])
def test_a_freed_agents_job_run_before_its_slot_is_given_back_offers_it_the_waiting_call(floor, end, lost):
    floor.settings(acw_blocks_routing=False)
    agent = floor.agent()
    first = floor.enqueue(floor.queue(agents=[agent]))
    waiting = floor.enqueue(floor.queue(agents=[agent]))
    execute, early = Pipeline.execute, []

    def job_first(self, *a):
        if early or not any(c[0] == "HDEL" and ":cap:" in str(c[1:]) for c, _ in self.command_stack):
            return execute(self, *a)
        early.append(agent)
        jobs.run_now("route_agent", str(agent), wait=True)
        replied = execute(self, *a)
        if lost:
            raise RedisTimeout("Timeout reading from socket")
        return replied

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(core, "REDIS_SHUT", 0.5)
        patch.setattr(Pipeline, "execute", job_first)
        end(floor, first, agent)
    until_redis_answers()
    jobs.run_now("route_agent", str(agent), wait=True)
    assert early and floor.offers(waiting) == [agent]


def test_a_slot_give_run_after_a_sweep_and_a_new_offer_of_its_conversation_leaves_the_agent_within_its_cap(floor):
    agent = floor.agent()
    q = floor.queue(agents=[agent], retries=1)
    cid = floor.enqueue(q)
    offer = floor.offer_of(cid, agent)
    late = committed(lambda db: routing.reoffer(db, cv.lock(db, cid), cv.move(db, offer, "decline")))
    sweep()
    age(301)
    jobs.run_now("next_round", str(cid), wait=True)
    assert floor.offers(cid) == [agent], "the call's next round offers it to its one agent again once tried lapses"
    for give in late:
        give()
    other = floor.enqueue(q)
    assert (floor.offers(cid), floor.offers(other)) == ([agent], []), "one call at a time"


def route_agent_when_due(agent, until) -> None:
    """Run the agent's route_agent each time it falls due, until `until()` or 2 s pass."""
    J, deadline = T.jobs, time.monotonic() + 2
    while not until() and time.monotonic() < deadline:
        if due := jobs._claim(J.c.kind == "route_agent", J.c.key == str(agent), J.c.run_at <= func.now()):
            jobs.work(due)
        time.sleep(0.05)


@pytest.mark.parametrize("held", [False, True])
def test_a_freed_agents_job_run_before_a_give_whose_process_then_died_offers_it_the_waiting_call(floor, held):
    agent = floor.agent()
    first = floor.enqueue(floor.queue(agents=[agent]))
    waiting = floor.enqueue(floor.queue(agents=[agent]))
    offer = floor.offer_of(first, agent)
    late = committed(lambda db: routing.reoffer(db, cv.lock(db, first), cv.move(db, offer, "decline")))

    def job():
        jobs.run_now("route_agent", str(agent), wait=True)

    if held:
        while_held(lambda db: cv.lock(db, first), job, waits=False)
    else:
        job()
    given = slots(floor, agent)
    for fn in late:
        fn()
        if slots(floor, agent) != given:
            break
    route_agent_when_due(agent, lambda: floor.offers(waiting))
    assert floor.offers(waiting) == [agent], "the job that found the agent full saw Postgres free it, before any sweep"


def hung_up_give_held(floor, agent):
    first = floor.enqueue(floor.queue(agents=[agent]))
    floor.answer(first, agent)
    return committed(lambda db: cv.gone(db, cv.lock(db, first)))


def landing(late: list, step, *, after: bool):
    def land(*args):
        while late and not after:
            late.pop(0)()
        result = step(*args)
        while late:
            late.pop(0)()
        return result

    return land


@pytest.mark.parametrize(("at", "after"), [("full", True), ("settle", False), ("settle", True)])
def test_a_give_landing_while_a_freed_agents_job_finds_it_full_leaves_it_offered_the_waiting_call(
    floor, monkeypatch, at, after
):
    floor.settings(acw_blocks_routing=False)
    agent = floor.agent()
    late = hung_up_give_held(floor, agent)
    waiting = floor.enqueue(floor.queue(agents=[agent]))
    monkeypatch.setattr(routing.hot, at, landing(late, getattr(routing.hot, at), after=after))
    jobs.run_now("route_agent", str(agent), wait=True)
    assert floor.offers(waiting) == [agent], "the job ends only once Postgres holds the agent full"


def test_a_freed_agents_job_waits_out_a_take_of_the_freed_slot_that_rolls_back(floor, monkeypatch):
    floor.settings(acw_blocks_routing=False)
    agent = floor.agent()
    late = hung_up_give_held(floor, agent)
    waiting, other = floor.enqueue(floor.queue(agents=[agent])), floor.open()
    monkeypatch.setattr(routing.hot, "full", landing(late, routing.hot.full, after=True))
    taken = []
    with pytest.raises(RuntimeError), core.tx() as offering:
        conv = cv.lock(offering, other.id)
        take = [lambda: taken.append(routing.hot.take(offering, conv, agent, uuid.uuid4(), 1))]
        monkeypatch.setattr(routing.hot, "audit", landing(take, routing.hot.audit, after=True))
        jobs.run_now("route_agent", str(agent), wait=True)
        raise RuntimeError("the offer that took the freed slot rolls back")
    route_agent_when_due(agent, lambda: floor.offers(waiting))
    assert taken == [True] and floor.offers(waiting) == [agent]


def test_an_agent_with_no_slot_on_a_channel_ends_its_job_while_a_text_waits_there(floor, monkeypatch):
    floor.settings(capacity=m.Capacity(sms=0).model_dump())
    agent = floor.agent()
    floor.enqueue(floor.queue(agents=[agent]), "sms")
    ten_full_reads(monkeypatch)
    floor.route_agent(agent)
    assert job("route_agent", agent) is None, "the job ended instead of picking the text again and again"


def test_a_capacity_cut_whose_cache_invalidation_comes_late_ends_the_agents_job_on_the_stored_capacity(
    floor, monkeypatch
):
    floor.settings(capacity=m.Capacity(sms=2).model_dump())
    agent = floor.agent()
    q = floor.queue(agents=[agent])
    assert floor.offers(floor.enqueue(q, "sms")) == [agent]
    with core.read() as db:
        assert config.tenant_settings(db, floor.tenant).capacity.sms == 2, "the cache holds the old capacity"
    monkeypatch.setattr(config.hot, "changed", lambda tenant_id: None)
    floor.settings(capacity=m.Capacity(sms=1).model_dump())
    core.redis.sadd(FULL % (floor.tenant, "sms"), str(agent))
    floor.enqueue(q, "sms")
    ten_full_reads(monkeypatch)
    floor.route_agent(agent)
    assert job("route_agent", agent) is None, "the job ended on the stored capacity, not the cached one"


def test_a_capacity_raise_whose_cache_invalidation_comes_late_offers_the_agent_on_the_stored_capacity(
    floor, monkeypatch
):
    agent = floor.agent()
    q = floor.queue(agents=[agent])
    assert floor.offers(floor.enqueue(q, "sms")) == [agent]
    with core.read() as db:
        assert config.tenant_settings(db, floor.tenant).capacity.sms == 1, "the cache holds the old capacity"
    with monkeypatch.context() as lost:
        lost.setattr(config.hot, "changed", lambda tenant_id: None)
        floor.settings(capacity=m.Capacity(sms=2).model_dump())
    waiting = floor.enqueue(q, "sms")
    assert floor.offers(waiting) == [], "the cached capacity counts the agent full"
    ten_full_reads(monkeypatch)
    floor.route_agent(agent)
    assert floor.offers(waiting) == [agent], "offered on the stored capacity"
    assert job("route_agent", agent) is None


def test_the_sweep_gives_back_a_slot_no_stint_holds_restores_one_a_stint_holds_and_keeps_an_open_offers(floor):
    stale, ringing, dropped = floor.agent(), floor.agent(), floor.agent()
    rung = floor.enqueue(floor.queue(agents=[ringing]))
    lost = floor.enqueue(floor.queue(agents=[dropped]))
    core.redis.hset(CAP % (floor.tenant, "call", stale), str(uuid.uuid4()), str(floor.open().id))
    core.redis.sadd(FULL % (floor.tenant, "call"), str(stale))
    core.redis.hdel(CAP % (floor.tenant, "call", dropped), str(floor.offer_of(lost, dropped).id))
    core.redis.srem(FULL % (floor.tenant, "call"), str(dropped))
    sweep()
    assert slots(floor, stale) == (set(), False) and job("route_agent", stale), "freed, and offered work"
    assert slots(floor, ringing) == ({str(rung)}, True)
    assert slots(floor, dropped) == ({str(lost)}, True)


def test_an_agent_a_sweep_frees_and_whose_settle_fails_after_its_redis_writes_is_offered_the_waiting_call(floor):
    agent = floor.agent()
    core.redis.hset(CAP % (floor.tenant, "call", agent), str(uuid.uuid4()), str(floor.open().id))
    core.redis.sadd(FULL % (floor.tenant, "call"), str(agent))
    cid = floor.enqueue(floor.queue(agents=[agent]))
    with losing("HDEL", ":cap:"), suppress(RedisError):
        routing._sweep(None)
    assert slots(floor, agent) == (set(), False), "the settle wrote Redis before its transaction failed"
    sweep()
    route_agent_when_due(agent, lambda: floor.offers(cid))
    assert floor.offers(cid) == [agent]


def test_a_sweep_rebuilds_more_tenants_than_one_transaction_holds_locks_for(floor, monkeypatch):
    monkeypatch.setattr(routing, "PAGE", 2)
    row, ids = insert(T.tenant).returning(T.tenant.c.id), {floor.tenant}
    with core.tx() as db:
        for _ in "ab":
            ids.add(db.execute(row.values(name="t", plivo_auth_id=uuid.uuid4().hex, plivo_token_enc="x")).scalar())
    core.redis.delete(routing.hot.EPOCH % floor.tenant)
    mine = text("SELECT count(*) FROM pg_locks WHERE pid = pg_backend_pid() AND locktype = 'advisory'")
    locks, rebuild = [], routing.hot.sweep

    def counted(db, *args):
        locks.append(db.execute(mine).scalar())
        return rebuild(db, *args)

    monkeypatch.setattr(routing.hot, "sweep", counted)
    sweep()
    assert max(locks) <= 2, "no transaction holds more tenant locks than a page"
    assert routing.hot.lost(ids) == set(), "every tenant rebuilt"


def test_a_tenant_rebuilt_after_a_redis_loss_names_each_slots_holder_as_its_audit_does(floor):
    agent = floor.agent()
    cid = floor.enqueue(floor.queue(agents=[agent]))
    floor.answer(cid, agent)
    core.redis.delete(*core.redis.keys(f"hs:{{{floor.tenant}}}:*"))
    with core.tx() as db:
        desks, held = db.execute(agents._DESKS).all(), cv.held(db)
        routing.hot.sweep(db, desks, held, {floor.tenant})
    took = {str(floor.offer_of(cid, agent).id): str(cid)}
    assert core.redis.hgetall(CAP % (floor.tenant, "call", agent)) == took, "held by the stint that took it"
    mine = [d for d in desks if d.tenant_id == floor.tenant]
    assert routing.hot.audit(mine, held, list(routing.CHANNELS))[0] == [], "nothing for the audit to settle"


@pytest.mark.parametrize("settle", [nullcontext, partial(losing, "HGETALL", ":cap:")], ids=["settled", "unsettled"])
def test_a_round_that_fell_in_a_redis_loss_runs_in_the_sweep_that_rebuilds_its_tenant(floor, settle):
    agent = floor.agent(available=False)
    cid = floor.enqueue(floor.queue(agents=[agent]))
    core.redis.delete(*core.redis.keys(f"hs:{{{floor.tenant}}}:*"))
    floor.status(agent, floor.available)
    assert floor.offers(cid) == [], "the round refused the unrebuilt keys, and its job waits out a backoff"
    with settle():
        sweep()
    J = T.jobs
    if due := jobs._claim(J.c.kind == "route_agent", J.c.key == str(agent), J.c.run_at <= func.now()):
        jobs.work(due)
    assert floor.offers(cid) == [agent]


def test_a_sweep_that_fails_while_it_rebuilds_a_tenant_leaves_the_rebuild_and_its_follow_ups_to_the_next(floor):
    agent = floor.agent()
    cid = floor.enqueue(floor.queue(agents=[agent]), "sms")
    core.redis.delete(*core.redis.keys(f"hs:{{{floor.tenant}}}:*"))
    with core.engine.connect() as stalled, stalled.begin():
        stalled.execute(text("LOCK TABLE jobs IN SHARE MODE"))
        with pytest.raises(OperationalError, match="lock timeout"):
            routing._sweep(None)
    sweep()
    assert job("sticky_rebuild", floor.tenant), "the next sweep rebuilt the tenant"
    jobs.run_now("next_round", str(cid), wait=True)
    assert floor.offers(cid) == [agent], "and ran the round its lost offer needed"


def test_a_rebuild_whose_epoch_lost_its_reply_leaves_the_tenant_whole_only_with_its_follow_ups(floor):
    agent = floor.agent()
    cid = floor.enqueue(floor.queue(agents=[agent]), "sms")
    core.redis.delete(*core.redis.keys(f"hs:{{{floor.tenant}}}:*"))
    with losing("EVALSHA", ":epoch"), suppress(RedisError):
        routing._sweep(None)
    sweep()
    assert job("sticky_rebuild", floor.tenant), "the tenant's sticky agents are rebuilt"
    jobs.run_now("next_round", str(cid), wait=True)
    assert floor.offers(cid) == [agent], "and its lost offer's round runs"


def test_a_rebuilt_tenants_waiting_call_skips_the_agent_it_tried_for_what_is_left_of_300_s(floor):
    agent, _ = floor.agent(), floor.agent()
    queue = floor.queue(agents=[agent])
    recent = floor.enqueue(queue)
    declined = floor.answer(recent, agent, "decline")
    old = floor.enqueue(queue)
    floor.answer(old, agent, "decline")
    E, back = T.events, timedelta(seconds=301)
    with core.tx() as db:
        db.execute(update(E).where(E.c.conversation_id == str(old)).values(created_at=E.c.created_at - back))
    lose_and_rebuild(floor)
    kept = routing.hot.rounds(floor.conv(recent))
    assert kept.tried == {str(agent): str(declined.id)} and 0 < kept.ttl <= routing.hot.CALL_TRIED
    assert routing.hot.rounds(floor.conv(old)).tried == {}, "300 s after its last offer a call may ring the agent again"


def test_a_rebuilt_waiting_call_holds_only_the_agents_its_current_run_tried(floor):
    first, second = floor.agent(idle_min=30), floor.agent()
    cid = floor.enqueue(floor.queue(agents=[first, second], agents_at_once=1, retries=3, wait_timeout_sec=1800))
    floor.answer(cid, first, "decline")
    floor.answer(cid, second, "decline", rounds=False)
    age(301)
    E, back = T.events, timedelta(seconds=301)
    with core.tx() as db:
        db.execute(update(E).where(E.c.conversation_id == str(cid)).values(created_at=E.c.created_at - back))
    wake(floor, cid, "next_round", first)
    renewed = floor.answer(cid, first, "decline", rounds=False)
    lose_and_rebuild(floor)
    assert routing.hot.rounds(floor.conv(cid)).tried == {str(first): str(renewed.id)}
    wake(floor, cid, "next_round", second)
    assert floor.offers(cid) == [second], "the run that lapsed took its decline with it"


def lose_and_rebuild(floor) -> None:
    """Redis loses the tenant's keys and a sweep rebuilds them."""
    core.redis.delete(*core.redis.keys(f"hs:{{{floor.tenant}}}:*"))
    with core.tx() as db:
        desks, held = db.execute(agents._DESKS).all(), cv.held(db)
        tried = db.execute(routing._TRIED, {"t": [str(floor.tenant)], "ms": routing.hot.CALL_TRIED}).partitions(2)
        routing.hot.sweep(db, desks, held, {floor.tenant}, tried)


@pytest.mark.parametrize("retained", [True, False])
def test_a_rebuilt_tenants_waiting_text_skips_the_agent_that_missed_it(floor, retained):
    agent = floor.agent()
    cid = floor.enqueue(floor.queue(agents=[agent], text={"wait_timeout_sec": 600}), "sms")
    missed = floor.answer(cid, agent, "miss", reason="timeout")
    if not retained:
        with core.tx() as db:
            E = T.events
            db.execute(E.delete().where(E.c.conversation_id == str(cid), E.c.type == "conversation.queued"))
    lose_and_rebuild(floor)
    assert routing.hot.rounds(floor.conv(cid)).tried == {str(agent): str(missed.id)}


def requeued(floor, again: str, hold: bool):
    """The agent declined the sms in its first queue stay; it then left and joined `again` the same queue or another,
    holding its place if `hold`, and the new stay offers the agent again. Returns the conversation and the agent."""
    agent, waits = floor.agent(), {"wait_timeout_sec": 600}
    first = floor.queue(agents=[agent], text=waits)
    then = first if again == "same" else floor.queue(agents=[agent], text=waits)
    cid = floor.enqueue(first, "sms")
    floor.answer(cid, agent, "decline")
    with core.tx() as db:
        cv.set_state(db, cv.lock(db, cid), "open")
        routing.enqueue(db, cv.lock(db, cid), then, hold=hold)
    assert floor.offers(cid) == [agent], "the new stay offers the agent again"
    return cid, agent


@pytest.mark.parametrize("hold", [True, False])
@pytest.mark.parametrize("again", ["same", "other"])
def test_a_rebuild_forgets_a_decline_from_an_earlier_queue_stay_in_the_same_or_another_queue(floor, again, hold):
    cid, _ = requeued(floor, again, hold)
    lose_and_rebuild(floor)
    assert routing.hot.rounds(floor.conv(cid)).tried == {}


@pytest.mark.parametrize("again", ["same", "other"])
def test_a_rebuild_keeps_a_decline_from_the_current_stay_of_a_conversation_that_held_its_place(floor, again):
    cid, agent = requeued(floor, again, True)
    declined = floor.answer(cid, agent, "decline")
    lose_and_rebuild(floor)
    assert routing.hot.rounds(floor.conv(cid)).tried == {str(agent): str(declined.id)}


def test_a_round_that_reads_its_tried_agents_before_a_rebuild_ends_offers_nothing_to_one_that_declined(
    floor, monkeypatch
):
    agent = floor.agent()
    cid = floor.enqueue(floor.queue(agents=[agent], text={"wait_timeout_sec": 600}), "sms")
    floor.answer(cid, agent, "decline")
    core.redis.delete(*core.redis.keys(f"hs:{{{floor.tenant}}}:*"))
    read = routing.hot.rounds

    def read_then_rebuild(conv):
        was = read(conv)
        sweep()
        return was

    monkeypatch.setattr(routing.hot, "rounds", read_then_rebuild)
    with pytest.raises(ResponseError, match="UNREBUILT"), core.tx() as db:
        routing.route(db, cv.lock(db, cid))
    monkeypatch.setattr(routing.hot, "rounds", read)
    sweep()
    with core.tx() as db:
        routing.route(db, cv.lock(db, cid))
    assert floor.conv(cid).state == "queued" and floor.offers(cid) == [], "the rebuilt stay still skips the agent"


def test_a_freed_agent_whose_slot_take_fails_leaves_the_call_queued_and_takes_it_when_its_job_runs_again(
    floor, monkeypatch
):
    agent = floor.agent(available=False)
    cid = floor.enqueue(floor.queue(agents=[agent]))
    take, failed = routing.hot.take, []

    def fails_once(*args, **kw):
        if not failed:
            failed.append(args)
            raise RedisError("the take never reached Redis")
        return take(*args, **kw)

    monkeypatch.setattr(routing.hot, "take", fails_once)
    floor.status(agent, floor.available)
    assert failed and (floor.conv(cid).state, floor.offers(cid)) == ("queued", [])
    floor.route_agent(agent)
    assert floor.offers(cid) == [agent]


def gate_take(monkeypatch) -> None:
    """route_agent reads what waits and whom it tried, then waits for advisory lock 7 before it locks what it took."""
    gated = str(routing._TAKE).replace(" WHERE id", " WHERE pg_advisory_xact_lock_shared(7)::text = '' AND id")
    assert gated != str(routing._TAKE)
    monkeypatch.setattr(routing, "_TAKE", text(gated))


def test_a_freed_agent_offered_and_declining_a_text_while_its_pick_runs_is_not_offered_it_again(floor, monkeypatch):
    floor.settings(capacity=m.Capacity(sms=2).model_dump())
    agent, other = floor.agent(), floor.agent(idle_min=30)
    cid = floor.enqueue(floor.queue(agents=[agent, other], agents_at_once=1, retries=3), "sms")
    floor.answer(cid, other, "decline", rounds=False)
    gate_take(monkeypatch)
    route, picked = routing.route, []
    monkeypatch.setattr(routing, "route", lambda db, conv, only=None: picked.append(only) or route(db, conv, only=only))

    def offer_and_decline(db):
        db.execute(text("SELECT pg_advisory_xact_lock(7)"))
        cv.move(db, route(db, cv.lock(db, cid))[0], "decline")

    with core.tx() as db:
        jobs.schedule(db, "route_agent", {"tenant_id": str(floor.tenant), "agent_id": str(agent)}, key=str(agent))
    claimed = jobs._claim(T.jobs.c.kind == "route_agent", T.jobs.c.key == str(agent))
    while_held(offer_and_decline, lambda: jobs.work(claimed))
    assert picked == [str(agent)], "the pick read the offers from before that offer and decline committed"
    assert floor.offers(cid) == []


def test_a_freed_agent_whose_pick_was_offered_to_another_agent_meanwhile_is_offered_the_next_call(floor, monkeypatch):
    agent, other = floor.agent(available=False), floor.agent()
    q = floor.queue(agents=[agent])
    taken, waiting = floor.enqueue(q), floor.enqueue(q)
    with core.tx() as db:
        agents.set_status(db, floor.tenant, agent, floor.available, "manual")
    gate_take(monkeypatch)

    def offer_to_other(db):
        db.execute(text("SELECT pg_advisory_xact_lock(7)"))
        cv.offer(db, cv.lock(db, taken), other)

    while_held(offer_to_other, lambda: jobs.run_now("route_agent", str(agent), wait=True))
    assert (floor.offers(taken), floor.offers(waiting)) == ([other], [agent])


@pytest.mark.parametrize("cause", cv.BUSY_OR_UNREGISTERED)
def test_a_freed_agent_whose_ring_ends_busy_or_unregistered_while_its_job_runs_is_not_offered_the_next_call(
    floor, monkeypatch, cause
):
    agent = floor.agent(available=False)
    q = floor.queue(agents=[agent])
    rung, waiting = floor.enqueue(q), floor.enqueue(q)
    full = routing.hot.full

    def ring_ends_before_the_next_pick(*args):
        if args[1:] == tuple(routing.CHANNELS) and floor.offers(rung) == [agent]:
            floor.answer(rung, agent, "miss", rounds=False, reason=cause)
        return full(*args)

    monkeypatch.setattr(routing.hot, "full", ring_ends_before_the_next_pick)
    floor.status(agent, floor.available)
    assert floor.offer_of(rung, agent).state == "missed", "the first ring ended while the job ran"
    assert floor.offers(waiting) == []


@pytest.mark.parametrize("give_held", [False, True])
@pytest.mark.parametrize("cause", cv.BUSY_OR_UNREGISTERED)
def test_an_agent_whose_ring_ends_busy_or_unregistered_after_a_status_change_takes_no_call_until_the_next(
    floor, cause, give_held
):
    agent = floor.agent(available=False)
    q = floor.queue(agents=[agent])
    rung, waiting = floor.enqueue(q), floor.enqueue(q)
    floor.status(agent, floor.available)
    with core.tx() as db:
        ready = floor.doc(db, "status", {"name": "Ready", "availability": True})
        agents.set_status(db, floor.tenant, agent, ready, "manual")

    def ring_ends(db):
        routing.reoffer(db, cv.lock(db, rung), cv.move(db, floor.offer_of(rung, agent), "miss", reason=cause))

    after = committed(ring_ends)
    for give in [] if give_held else after:
        give()
    assert slots(floor, agent)[1] == give_held, "Redis counts the agent full until the ring end's commit hooks run"
    jobs.run_now("route_agent", str(agent), wait=True)
    assert floor.offers(waiting) == [], "the change came before the ring ended"
    floor.status(agent, floor.available)
    assert floor.offers(waiting) == [agent]


@pytest.mark.parametrize("text_ends_first", [True, False])
@pytest.mark.parametrize("cause", cv.BUSY_OR_UNREGISTERED)
def test_an_agent_whose_ring_ends_busy_or_unregistered_takes_the_next_call_once_its_text_ends_after_it(
    floor, cause, text_ends_first
):
    floor.settings(acw_blocks_routing=False)
    agent = floor.agent(available=False)
    text = floor.enqueue(floor.queue(agents=[agent]), "sms")
    q = floor.queue(agents=[agent])
    rung, waiting = floor.enqueue(q), floor.enqueue(q)
    floor.status(agent, floor.available)
    ends = [
        partial(hang_up, floor, text, agent),
        partial(floor.answer, rung, agent, "miss", rounds=False, reason=cause),
    ]
    for end in ends if text_ends_first else ends[::-1]:
        end()
    jobs.run_now("route_agent", str(agent), wait=True)
    assert floor.offers(waiting) == ([] if text_ends_first else [agent])


def test_a_freed_agent_is_offered_the_call_its_pick_passed_over_while_another_transaction_held_it(floor):
    agent = floor.agent(available=False)
    cid = floor.enqueue(floor.queue(agents=[agent]))
    while_held(lambda db: cv.lock(db, cid), lambda: floor.status(agent, floor.available), waits=False)
    assert floor.offers(cid) == [], "the pick passed over the held call"
    route_agent_when_due(agent, lambda: floor.offers(cid))
    assert floor.offers(cid) == [agent], "the agent's own route_agent came back for it"


def test_a_call_declined_while_its_other_agent_is_busy_waits_for_that_agent(floor):
    free, busy = floor.agent(idle_min=30), floor.agent()
    other = floor.enqueue(floor.queue(agents=[busy]))
    cid = floor.enqueue(floor.queue(agents=[free, busy], agents_at_once=1, retries=1, wait_timeout_sec=20))
    floor.answer(cid, free, "decline")
    assert (floor.conv(cid).state, floor.offers(cid)) == ("queued", [])
    floor.answer(other, busy, "decline")
    jobs.run_now("route_agent", str(busy), wait=True)
    assert floor.offers(cid) == [busy]


def test_a_freed_agent_is_not_offered_a_call_whose_rounds_are_spent(floor):
    first, freed = floor.agent(idle_min=30), floor.agent()
    cid = floor.enqueue(floor.queue(agents=[first, freed], agents_at_once=1, retries=0))
    floor.answer(cid, first, "decline", rounds=False)
    floor.route_agent(freed)
    assert (floor.offers(cid), floor.conv(cid).result) == ([], "assignment_failed")


def test_a_miss_takes_no_other_agents_presence_row(floor):
    missing, next_ = floor.agent(idle_min=30), floor.agent()
    cid = floor.enqueue(floor.queue(agents=[missing, next_], agents_at_once=1, retries=1))
    timeout = str(floor.offer_of(cid, missing).id)
    while_held(presence_lock(next_), lambda: jobs.run_now("offer_timeout", timeout, wait=True), waits=False)
    jobs.run_now("next_round", str(cid), wait=True)
    assert floor.offers(cid) == [next_]


def test_a_sticky_round_locks_its_fallback_agents_with_the_sticky_one(floor):
    usual, last = floor.agent(idle_min=60), floor.agent()
    q = floor.queue(agents=[usual, last], agents_at_once=1, sticky={"mode": "loose"})
    floor.history(last, "+14155550181", queue=q)
    cid = while_held(presence_lock(usual), lambda: floor.enqueue(q, customer="+14155550181"))
    assert floor.offers(cid) == [last]


def test_a_stint_start_that_rolls_back_leaves_the_sticky_agent_as_it_was(floor):
    usual, last = floor.agent(idle_min=60), floor.agent()
    q = floor.queue(agents=[usual, last], agents_at_once=1, retries=1, sticky={"mode": "loose"})
    floor.history(last, "+14155550182", queue=q)
    cid = floor.enqueue(q, customer="+14155550182")
    floor.answer(cid, last, "decline")
    key = routing.hot.STICKY % (floor.tenant, q, "+14155550182")
    with pytest.raises(RuntimeError), core.tx() as db:
        cv.lock(db, cid)
        cv.move(db, next(a for a in cv.rows(db, cid, ("offered",)) if a.agent_id == usual), "accept")
        raise RuntimeError("the process dies before COMMIT")
    assert core.redis.get(key).endswith(str(last)), "nothing reached Redis before COMMIT"
    floor.answer(cid, usual)
    assert core.redis.get(key).endswith(str(usual)), "the accept that commits makes its agent sticky"


@pytest.mark.parametrize(("order", "taken"), [("fifo", 1), ("lifo", 2)])
def test_a_freed_agent_takes_the_most_urgent_queue_then_its_dequeue_order(floor, order, taken):
    agent = floor.agent(available=False)
    low, high = (floor.queue(agents=[agent], priority=p, dequeue_order=order) for p in (9, 1))
    waiting = [floor.enqueue(low), floor.enqueue(high), floor.enqueue(high)]
    floor.status(agent, floor.available)
    assert [floor.offers(cid) for cid in waiting] == [[agent] if i == taken else [] for i in range(3)]


def test_a_freed_agent_fills_its_capacity_on_every_channel(floor):
    floor.settings(capacity=m.Capacity(sms=2).model_dump())
    agent = floor.agent(available=False)
    q = floor.queue(agents=[agent])
    waiting = [floor.enqueue(q, "sms") for _ in range(3)] + [floor.enqueue(q)]
    floor.status(agent, floor.available)
    assert [len(floor.offers(cid)) for cid in waiting] == [1, 1, 0, 1]


def test_a_round_beside_another_rounds_open_offer_to_its_agent_skips_that_agent_without_waiting(floor):
    agent, rival = floor.agent(), floor.open()
    q = floor.queue(agents=[agent])
    cid = while_held(lambda db: cv.offer(db, cv.lock(db, rival.id), agent), lambda: floor.enqueue(q), waits=False)
    assert floor.offers(cid) == [] and floor.offers(rival.id) == [agent]


def test_two_enqueues_racing_for_a_queues_last_place_admit_one(floor):
    q = floor.queue(capacity=50, overflow_action={"type": "hangup"})
    with core.tx() as db:
        full = {"tenant_id": floor.tenant, "channel": "call", "direction": "inbound", "state": "queued", "queue_id": q}
        db.execute(insert(T.conversation), [full | {"customer": f"+1{i}"} for i in range(49)])
    first, second = floor.open(), floor.open()

    def refused():
        with core.tx() as db:
            try:
                routing.enqueue(db, cv.lock(db, second.id), q)
            except routing.Full as e:
                return e.action
        return "queued"

    assert while_held(lambda db: routing.enqueue(db, cv.lock(db, first.id), q), refused)["type"] == "hangup"
    assert (floor.conv(first.id).state, floor.conv(second.id).state) == ("queued", "open")
    [over] = floor.events("conversation.overflowed")
    assert (over.conversation_id, over.data["reason"], over.data["queue_id"]) == (
        str(second.id),
        "enqueue_failed",
        str(q),
    )


@pytest.mark.parametrize("channel", ["call", "sms"])
def test_a_queue_timeout_overflows_a_call_and_closes_a_text(floor, channel):
    q = floor.queue(wait_timeout_sec=20, text={"wait_timeout_sec": 60})
    cid = floor.enqueue(q, channel)
    assert timedelta(seconds=55 if channel == "sms" else 15) < job("queue_timeout", cid).wait
    jobs.run_now("queue_timeout", str(cid), wait=True)
    [over] = floor.events("conversation.overflowed", cid)
    conv = floor.conv(cid)
    assert (over.data["reason"], conv.result) == ("wait_timeout", "queue_timeout")
    if channel == "call":
        assert conv.state == "open" and job("plivo", f"redirect:{floor.tenant}:{cid}:1").payload["action"] == {
            "type": "hangup"
        }
    else:
        assert conv.state == "closed"


def test_a_key_press_overflow_ends_as_a_queue_exit_and_tells_its_action(floor):
    q = floor.queue(agents=[floor.agent(available=False)])
    cid = floor.enqueue(q)
    with core.tx() as db:
        routing.overflowed(db, cv.lock(db, cid), q, "dtmf", {"type": "hangup"})
    [over] = floor.events("conversation.overflowed", cid)
    assert (over.data["reason"], over.data["action"], floor.conv(cid).result) == (
        "dtmf",
        {"type": "hangup"},
        "queue_exit",
    )


def test_a_queue_timeout_claimed_before_an_answer_and_a_new_enqueue_does_nothing(floor):
    q = floor.queue(wait_timeout_sec=20)
    cid = floor.enqueue(q)
    claimed = jobs._claim(T.jobs.c.kind == "queue_timeout", T.jobs.c.key == str(cid))
    with core.tx() as db:
        routing.enqueue(db, cv.set_state(db, cv.lock(db, cid), "live"), q)
    jobs.work(claimed)
    assert floor.conv(cid).state == "queued" and not floor.events("conversation.overflowed", cid)


def test_a_queue_without_a_timeout_drops_the_timer_of_the_queue_before(floor):
    first, second = floor.queue(wait_timeout_sec=20), floor.queue(wait_timeout_sec=0)
    cid = floor.enqueue(first)
    with core.tx() as db:
        cv.depart(db, cv.lock(db, cid), {"type": "queue", "queue_id": str(second)})
        routing.enqueue(db, cv.lock(db, cid), second)
    assert job("queue_timeout", cid) is None


@pytest.mark.parametrize("leaves", ["taken", "closed"])
def test_a_queue_a_conversation_waits_in_refuses_its_delete_until_the_conversation_is_taken_or_closed(floor, leaves):
    agent, api = floor.agent(available=False), admin(floor)
    q = floor.queue(agents=[agent])
    cid = floor.enqueue(q, "sms")
    refused = api.delete(f"/api/v1/queues/{q}")
    assert refused.status_code == 409 and refused.json()["error"]["details"] == {"waiting": 1}
    assert refused.json()["error"]["code"] == "queue_in_use"
    if leaves == "taken":
        floor.status(agent, floor.available)
        assert api.delete(f"/api/v1/queues/{q}").status_code == 409, "a conversation offered still waits"
        floor.answer(cid, agent)
    else:
        with core.tx() as db:
            cv.gone(db, cv.lock(db, cid))
    assert api.delete(f"/api/v1/queues/{q}").status_code == 204


def test_a_queue_whose_conversations_ended_deletes_while_others_wait_in_other_queues(floor, monkeypatch):
    q, other, elsewhere = floor.queue(), floor.queue(), Floor(monkeypatch)
    with core.tx() as db:
        cv.gone(db, cv.lock(db, floor.enqueue(q, "sms")))
    floor.enqueue(other, "sms")
    elsewhere.enqueue(elsewhere.queue(), "sms")
    assert admin(floor).delete(f"/api/v1/queues/{q}").status_code == 204


def in_turn(lock, *runs) -> list[Future]:
    ready, release, started = Future(), threading.Event(), []

    def holding():
        with core.tx() as db:
            db.execute(lock)
            ready.set_result(db.exec_driver_sql("SELECT pg_backend_pid()").scalar())
            release.wait(10)

    with ThreadPoolExecutor(1 + len(runs)) as pool:
        pool.submit(holding)
        pid, deadline = ready.result(10), time.monotonic() + 5
        try:
            for run in runs:
                started.append(pool.submit(run))
                with core.read() as db:
                    while not (waiter := db.execute(BLOCKED, {"pid": pid}).scalar()):
                        assert time.monotonic() < deadline, "a run did not wait on the one before it"
                        time.sleep(0.02)
                pid = waiter
        finally:
            release.set()
    return started


@pytest.mark.parametrize("order", [("enqueue", "delete"), ("delete", "enqueue")], ids=["enqueue", "delete"])
def test_an_enqueue_racing_its_queues_delete_lands_wholly_before_or_after_it(floor, order):
    q, conv, api = floor.queue(), floor.open("sms"), admin(floor)

    def enqueue():
        with core.tx() as db:
            routing.enqueue(db, cv.lock(db, conv.id), q)

    runs = {"enqueue": enqueue, "delete": lambda: api.delete(f"/api/v1/queues/{q}")}
    held = select(T.config.c.id).where(T.config.c.id == q).with_for_update()
    done = dict(zip(order, in_turn(held, *(runs[k] for k in order)), strict=True))
    if order[0] == "enqueue":
        assert (floor.conv(conv.id).state, done["delete"].result().status_code) == ("queued", 409)
    else:
        assert (done["delete"].result().status_code, done["enqueue"].exception().code) == (204, "queue_not_found")
        assert floor.conv(conv.id).state == "open"


def admin(floor) -> TestClient:
    with core.tx() as db:
        key = access.new_key(db, floor.tenant, ["admin"])["api_key"]
    return TestClient(service.app, headers={"Authorization": f"Bearer {key}"})


def bearer(floor, agent) -> dict:
    with core.tx() as db:
        return {"Authorization": f"Bearer {access.mint(db, floor.tenant, agent).agent_token}"}


def test_an_agent_answers_only_its_own_open_text_offer_through_the_api(floor):
    first, second = floor.agent(idle_min=30), floor.agent()
    q = floor.queue(agents=[first, second], agents_at_once=1, retries=1)
    cid = floor.enqueue(q, "sms")
    offer = floor.offer_of(cid, first).id
    assert client.post(f"/api/v1/offers/{offer}/accept", headers=bearer(floor, second)).status_code == 404
    assert client.post(f"/api/v1/offers/{offer}/decline", headers=bearer(floor, first)).status_code == 204
    jobs.run_now("next_round", str(cid), wait=True)
    taken = client.post(f"/api/v1/offers/{offer}/accept", headers=bearer(floor, first))
    assert (taken.status_code, taken.json()["error"]["code"]) == (422, "offer_not_open")
    assert floor.offers(cid) == [second]
    mine = floor.offer_of(cid, second).id
    assert client.post(f"/api/v1/offers/{mine}/accept", headers=bearer(floor, second)).status_code == 204
    assert floor.offers(cid, "active") == [second] and floor.conv(cid).state == "live"
    call = floor.offer_of(floor.enqueue(q), first).id
    assert client.post(f"/api/v1/offers/{call}/decline", headers=bearer(floor, first)).status_code == 404


def test_accepting_a_text_transfer_makes_its_target_the_primary(floor):
    first, target = floor.agent(), floor.agent()
    cid = floor.enqueue(floor.queue(agents=[first]), "sms")
    floor.answer(cid, first)
    with core.tx() as db:
        transfer = cv.offer(db, cv.lock(db, cid), target, via="transfer").id
    assert client.post(f"/api/v1/offers/{transfer}/accept", headers=bearer(floor, target)).status_code == 204
    assert (floor.offers(cid, "wrap_up"), floor.offers(cid, "active")) == ([first], [target])
    assert floor.offer_of(cid, target).role == "primary"
