"""Open offers across transactions, replicas and a Redis loss: what a waiter sees, what a late write may change,
and what the rebuild counts. Offers are read and ended through hs.conversations only, so these hold wherever offers
live."""

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext, suppress

import pytest
from redis import Redis, ResponseError
from redis.client import Pipeline
from sqlalchemy import text

from hs import agents, core, hot, jobs, routing
from hs import conversations as cv
from tests.unit.test_routing import CAP, FULL, Floor, age, job, sweep, while_held

# The sweep's exclusive request on the slots lock, queued behind a round's shared hold.
WAITING = "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND classid = 5107 AND NOT granted"


@pytest.fixture
def floor(monkeypatch) -> Floor:
    return Floor(monkeypatch)


def opened(cid) -> list:
    """The agents with an open offer of the conversation, read under its lock."""
    with core.tx() as db:
        return sorted(a.agent_id for a in cv.rows(db, cv.lock(db, cid).id, ("offered",)))


def end(cid, agent, event: str) -> None:
    """`event` on the agent's open offer; a decline or a miss schedules the next round, which is left to run."""
    with core.tx() as db:
        conv = cv.lock(db, cid)
        a = cv.move(db, next(a for a in cv.rows(db, cid, ("offered",)) if a.agent_id == agent), event)
        if event in ("decline", "miss"):
            routing.reoffer(db, conv, a)


def kept(tenant, cid) -> dict:
    """Every Redis hash the conversation's routing keeps, by key."""
    keys = core.redis.keys(f"hs:{{{tenant}}}:*:{cid}")
    return {k: core.redis.hgetall(k) for k in keys if core.redis.type(k) == "hash"}


def rounds(tenant, cid) -> dict:
    """The conversation's routing hashes but its offers, whose late creates count only with a timer."""
    return {k: v for k, v in kept(tenant, cid).items() if ":offers:" not in k}


class Rollback(Exception):
    pass


def writes(work, *, rolls_back: bool = False) -> list:
    """The Redis writes `work(db)` sends in one transaction, besides the capacity keys' takes and gives, to send
    again later; with `rolls_back`, the transaction then rolls back."""
    send, execute, sent = Redis.execute_command, Pipeline.execute, []

    def record(commands: list) -> None:
        sent.extend(c for c in commands if not any(k in str(c[1:]) for k in (":cap:", ":full:")) and c[0] not in READS)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(Redis, "execute_command", lambda self, *a, **kw: record([a]) or send(self, *a, **kw))
        patch.setattr(
            Pipeline, "execute", lambda self, *a: record([c for c, _ in self.command_stack]) or execute(self, *a)
        )
        with pytest.raises(Rollback) if rolls_back else nullcontext(), core.tx() as db:
            work(db)
            if rolls_back:
                core.before_commit(db, fail_at_commit)
    return sent


def fail_at_commit() -> None:
    raise Rollback


READS = ("HGETALL", "HGET", "HMGET", "HVALS", "PTTL", "SMEMBERS", "SISMEMBER", "EXISTS", "GET", "MGET")


def test_a_transaction_that_waited_for_a_conversations_lock_sees_the_offer_made_while_it_waited(floor):
    agent = floor.agent(available=False)
    cid = floor.enqueue(floor.queue(agents=[agent]))
    with core.tx() as db:
        agents.set_status(db, floor.tenant, agent, floor.available, "manual")
    assert while_held(lambda db: routing.route(db, cv.lock(db, cid)), lambda: opened(cid)) == [agent]


def test_an_offer_round_whose_transaction_rolled_back_offers_nothing_when_its_redis_writes_land_late(floor):
    agent = floor.agent()
    q = floor.queue(agents=[agent])
    cid = floor.open().id
    late = writes(lambda db: routing.enqueue(db, cv.lock(db, cid), q), rolls_back=True)
    assert any(c[:2] == ("EVALSHA", hot._CREATE.sha) for c in late), "the rolled-back round sent its offer's create"
    for command in late:
        core.redis.execute_command(*command)
    assert opened(cid) == []
    with core.read() as db:
        assert cv.desk(db, floor.tenant, agent) == []
    assert not core.redis.sismember(FULL % (floor.tenant, "call"), str(agent))
    with core.tx() as db:
        routing.enqueue(db, cv.lock(db, cid), q)
    assert opened(cid) == [agent]


def test_a_round_store_delivered_after_a_newer_round_changes_nothing_the_newer_round_keeps(floor):
    first, second = floor.agent(idle_min=30), floor.agent()
    cid = floor.open().id
    q = floor.queue(agents=[first, second], agents_at_once=1, retries=3, wait_timeout_sec=1800)
    late = writes(lambda db: routing.enqueue(db, cv.lock(db, cid), q))
    end(cid, first, "decline")
    jobs.run_now("next_round", str(cid), wait=True)
    assert opened(cid) == [second]
    newer = kept(floor.tenant, cid)
    for command in late:
        core.redis.execute_command(*command)
    assert kept(floor.tenant, cid) == newer, "the older round's store changed what the newer round keeps"


def test_a_round_store_from_before_a_redis_loss_changes_nothing_once_the_recovered_cycle_reaches_its_round(floor):
    first, second = floor.agent(idle_min=30), floor.agent()
    cid = floor.enqueue(floor.queue(agents=[first, second], agents_at_once=1, retries=3, wait_timeout_sec=1800))
    end(cid, first, "decline")
    late = writes(lambda db: routing.route(db, cv.lock(db, cid)))
    assert opened(cid) == [second]
    core.redis.delete(*core.redis.keys(f"hs:{{{floor.tenant}}}:*"))
    sweep()
    jobs.run_now("next_round", str(cid), wait=True)
    recovered, offered = rounds(floor.tenant, cid), opened(cid)
    for command in late:
        core.redis.execute_command(*command)
    assert rounds(floor.tenant, cid) == recovered, "a store from before the loss changed the recovered cycle's rounds"
    assert opened(cid) == offered


def test_a_slot_taken_before_a_redis_loss_counts_once_its_round_commits_after_the_rebuild(floor):
    """A round holds its agent's one slot when Redis loses the tenant's keys. The sweep that rebuilds the tenant must
    count that round's offer, so a call queued after the rebuild finds the agent full."""
    agent = floor.agent()
    q = floor.queue(agents=[agent])
    first, second = floor.open(), floor.open()

    def round_then_loss(db):
        routing.enqueue(db, cv.lock(db, first.id), q)
        core.redis.delete(*core.redis.keys(f"hs:{{{floor.tenant}}}:*"))

    while_held(round_then_loss, sweep, waits=None)
    with core.tx() as db:
        routing.enqueue(db, cv.lock(db, second.id), q)
    assert (opened(first.id), opened(second.id)) == ([agent], []), "one offer at capacity 1"


def test_a_rival_replicas_round_between_a_redis_loss_and_an_open_rounds_commit_takes_no_slot(floor):
    """One replica's round takes the agent's one slot and Redis loses the tenant's keys before it commits. The
    sweep waits for that round to end before it rebuilds, and a second replica's round in that window is refused
    rather than offered the slot the loss freed; once the round commits, the rebuild counts it."""
    agent = floor.agent()
    q, other = floor.queue(agents=[agent]), floor.queue(agents=[agent])
    first, second, third = floor.open(), floor.open(), floor.open()
    lost, done = threading.Event(), threading.Event()

    def round_then_loss():
        with core.tx() as db:
            routing.enqueue(db, cv.lock(db, first.id), q)
            core.redis.delete(*core.redis.keys(f"hs:{{{floor.tenant}}}:*"))
            lost.set()
            done.wait(10)

    with ThreadPoolExecutor(2) as pool:
        held = pool.submit(round_then_loss)
        assert lost.wait(10)
        rebuilt = pool.submit(sweep)
        with core.read() as db:
            deadline = time.monotonic() + 5
            while not db.execute(text(WAITING)).scalar():
                assert time.monotonic() < deadline, "the sweep never waited for the open round"
                time.sleep(0.02)
        with pytest.raises(ResponseError), core.tx() as db:
            routing.enqueue(db, cv.lock(db, second.id), other)
        done.set()
        held.result(10), rebuilt.result(10)
    with core.tx() as db:
        routing.enqueue(db, cv.lock(db, third.id), q)
    assert (opened(first.id), opened(second.id), opened(third.id)) == ([agent], [], []), "one offer at capacity 1"


@pytest.mark.parametrize("claimed", [False, True], ids=["lost_before_the_accept", "lost_after_the_accept"])
def test_an_accept_and_a_redis_loss_before_its_commit_leave_the_agent_one_conversation(floor, claimed):
    agent = floor.agent()
    first, second = floor.enqueue(floor.queue(agents=[agent])), floor.open()
    read, go = threading.Event(), threading.Event()

    def accept():
        with core.tx() as db:
            [offer] = cv.rows(db, cv.lock(db, first).id, ("offered",))
            if claimed:
                cv.move(db, offer, "accept")
            read.set()
            go.wait(10)
            if not claimed:
                cv.move(db, offer, "accept")

    def rival():
        with suppress(ResponseError) if claimed else nullcontext(), core.tx() as db:
            routing.enqueue(db, cv.lock(db, second.id), floor.queue(agents=[agent]))

    with ThreadPoolExecutor(3) as pool:
        accepting = pool.submit(accept)
        assert read.wait(10)
        core.redis.delete(*core.redis.keys(f"hs:{{{floor.tenant}}}:*"))
        rebuilt = pool.submit(sweep)
        with core.read() as db:
            deadline = time.monotonic() + 5
            while claimed and not db.execute(text(WAITING)).scalar():
                assert time.monotonic() < deadline, "the sweep never waited for the accept"
                time.sleep(0.02)
        if not claimed:
            rebuilt.result(10)
            deadline = time.monotonic() + 5
            while not core.redis.exists(hot.EPOCH % floor.tenant):
                assert time.monotonic() < deadline, "the sweep never rebuilt the tenant"
                time.sleep(0.02)
        rivals = pool.submit(rival)
        if not claimed:
            rivals.result(10)
        go.set()
        accepting.result(10), rebuilt.result(10), rivals.result(10)
    with core.read() as db:
        took = [a.agent_id for a in cv.rows(db, first, ("active",))]
    assert (took, opened(second.id)) == (([agent], []) if claimed else ([], [agent])), "one conversation at capacity 1"


@pytest.mark.parametrize("lost", [False, True], ids=["redis_keeps_the_offer", "redis_then_loses_it"])
def test_an_accept_that_commits_between_the_holder_reads_keeps_its_slot(floor, monkeypatch, lost):
    agent = floor.agent()
    cid = floor.enqueue(floor.queue(agents=[agent]))
    states, accepted, strays = hot.states, [], []

    def accept_between(offers: list) -> list:
        if not accepted:
            accepted.append(cid)
            end(cid, agent, "accept")
            if lost:
                core.redis.delete(*core.redis.keys(f"hs:{{{floor.tenant}}}:*"))
        return states(offers)

    monkeypatch.setattr(hot, "states", accept_between)
    with core.tx() as db:
        assert {r.conv for r in cv.held(db, None, agent, lost=strays)} == {str(cid)}
    assert strays == [], "an accepted offer is not a lost one"


def test_a_ringing_offer_keeps_its_slot_through_a_sweep_and_its_agents_job(floor):
    agent = floor.agent()
    q = floor.queue(agents=[agent])
    rung, waiting = floor.enqueue(q), floor.enqueue(q)
    sweep()
    floor.route_agent(agent)
    t = floor.tenant
    assert (opened(rung), opened(waiting)) == ([agent], [])
    assert set(core.redis.hvals(CAP % (t, "call", agent))) == {str(rung)}
    assert core.redis.sismember(FULL % (t, "call"), str(agent))


@pytest.mark.parametrize(("channel", "again"), [("task", False), ("callback", False), ("sms", True)])
def test_a_decliner_is_offered_again_26_h_later_only_on_a_channel_that_forgets_whom_it_tried(floor, channel, again):
    """A conversation keeps whom its routing tried for the channel's life: 360 h for a task, 75 h for a callback,
    25 h for other text."""
    first, away = floor.agent(), floor.agent(available=False)
    cid = floor.enqueue(floor.queue(agents=[first, away], agents_at_once=1, retries=3), channel)
    end(cid, first, "decline")
    jobs.run_now("next_round", str(cid), wait=True)
    assert opened(cid) == [], "its other agent is away"
    age(26 * 3600)
    floor.route_agent(first)
    assert opened(cid) == ([first] if again else [])


def test_a_text_offer_redis_lost_is_replaced_within_one_sweep_and_leaves_no_phantom(floor):
    agent = floor.agent()
    cid = floor.enqueue(floor.queue(agents=[agent]), "sms")
    with core.tx() as db:
        [old] = cv.rows(db, cv.lock(db, cid).id, ("offered",))
    core.redis.delete(*core.redis.keys(f"hs:{{{floor.tenant}}}:*"))
    sweep()
    jobs.run_now("next_round", str(cid), wait=True)
    with core.tx() as db:
        [new] = cv.rows(db, cv.lock(db, cid).id, ("offered",))
    assert new.id != old.id, "the sweep that rebuilt the tenant ran the call's next round"
    assert job("offer_timeout", old.id) is None, "the lost offer's timer is retired"
    with core.read() as db:
        assert [a.id for a in cv.desk(db, floor.tenant, agent)] == [new.id]


def test_an_offer_whose_key_alone_redis_lost_is_offered_again_within_one_sweep_and_leaves_the_desk(floor):
    """Redis can evict one key under memory pressure while the timer, the epoch and the routing keys stay."""
    agent = floor.agent()
    cid = floor.enqueue(floor.queue(agents=[agent]))
    with core.tx() as db:
        [old] = cv.rows(db, cv.lock(db, cid).id, ("offered",))
    core.redis.delete(hot.OFFERS % (floor.tenant, cid))
    with core.read() as db:
        assert cv.desk(db, floor.tenant, agent) == [], "the desk lists only offers Redis still holds"
    sweep()
    assert job("next_round", cid) is not None, "the sweep scheduled the call's round"
    jobs.run_now("next_round", str(cid), wait=True)
    assert job("offer_timeout", old.id) is None, "the lost offer's timer is retired"
    with core.read() as db:
        assert [(a.agent_id, a.id != old.id) for a in cv.desk(db, floor.tenant, agent)] == [(agent, True)]
