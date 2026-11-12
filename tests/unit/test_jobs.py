import contextlib
import json
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, event, func, insert, select, text, true, update
from sqlalchemy.exc import TimeoutError as PoolTimeout
from sqlalchemy.pool import NullPool

import hs.routing  # noqa: F401
from hs import access, core, events, hot, jobs, plivo
from hs import app as service
from hs import conversations as cv
from hs.core import ApiError
from tests.unit.tenants import Tenant


def kind() -> str:
    return f"t_{uuid.uuid4().hex[:8]}"


def schedule(kind: str, key: str, payload: dict) -> None:
    with core.tx() as db:
        jobs.schedule(db, kind, payload, key=key)


def row(kind: str, key: str):
    J = core.T.jobs
    due = (J.c.run_at <= func.now()).label("due")
    parked = J.c.locked_until.is_not_distinct_from(jobs.INFINITY).label("parked")
    with core.read() as db:
        return db.execute(select(J, due, parked).where(J.c.kind == kind, J.c.key == key)).first()


def in_flight(kind: str, key: str) -> threading.Thread:
    """run_now(wait=True) on another thread."""
    thread = threading.Thread(target=jobs.run_now, args=(kind, key), kwargs={"wait": True})
    thread.start()
    return thread


def test_run_now_with_wait_returns_the_handler_result_and_deletes_the_row():
    k = kind()
    jobs.handler(k)(lambda job: job.payload["n"] * 2)
    schedule(k, "w", {"n": 21})
    assert jobs.run_now(k, "w", wait=True) == 42
    assert row(k, "w") is None and jobs.run_now(k, "w", wait=True) is None


def test_a_keyed_schedule_replaces_the_row_so_only_the_last_payload_runs():
    k, seen = kind(), []
    jobs.handler(k)(lambda job: seen.append(job.payload["n"]))
    schedule(k, "x", {"n": 1})
    schedule(k, "x", {"n": 2})
    jobs.run_now(k, "x", wait=True)
    assert seen == [2] and row(k, "x") is None


def test_a_reschedule_during_a_run_runs_again_after_it_with_the_newest_payload():
    k, sent, started, release = kind(), [], threading.Event(), threading.Event()

    def send(job):
        sent.append(job.payload["hold"])
        if job.payload["hold"]:
            started.set()
            release.wait(5)

    jobs.handler(k)(send)
    schedule(k, "c", {"hold": True})
    first = in_flight(k, "c")
    assert started.wait(5)
    schedule(k, "c", {"hold": False})
    assert jobs.run_now(k, "c", wait=True) is None and sent == [True], "the newer payload waits for the run in flight"
    release.set()
    first.join(5)
    assert row(k, "c").locked_until is None, "the finished run released the row instead of deleting it"
    jobs.run_now(k, "c", wait=True)
    assert sent == [True, False] and row(k, "c") is None


def test_a_run_whose_lease_expired_changes_nothing():
    k, started, release = kind(), threading.Event(), threading.Event()

    def run(job):
        if job.attempts == 1:
            started.set()
            release.wait(5)
            return
        raise RuntimeError("second claim fails")

    jobs.handler(k)(run)
    schedule(k, "l", {})
    first = in_flight(k, "l")
    assert started.wait(5)
    with core.tx() as db:
        J = core.T.jobs
        db.execute(update(J).where(J.c.kind == k).values(locked_until=text("now() - interval '1 second'")))
    jobs.run_now(k, "l", wait=True)
    release.set()
    first.join(5)
    stale = row(k, "l")
    assert stale is not None and stale.attempts == 2 and stale.last_error == "RuntimeError: second claim fails"


def test_a_failing_job_backs_off_then_parks():
    k = kind()
    jobs.handler(k, backoff=(0,))(lambda job: 1 / 0)
    schedule(k, "p", {})
    jobs.run_now(k, "p", wait=True)
    retry = row(k, "p")
    assert retry.attempts == 1 and retry.locked_until is None and retry.last_error.startswith("ZeroDivisionError")
    jobs.run_now(k, "p", wait=True)
    assert row(k, "p").parked and jobs.run_now(k, "p", wait=True) is None


def test_a_recurring_job_is_rescheduled_instead_of_deleted():
    k = kind()
    jobs.every(k, 60)
    jobs.handler(k)(lambda job: None)
    schedule(k, k, {})
    jobs.run_now(k, k, wait=True)
    again = row(k, k)
    assert again.attempts == 0 and again.locked_until is None and not again.due


def test_a_handler_without_a_retry_is_refused():
    with pytest.raises(ValueError):
        jobs.handler(kind(), backoff=())


def test_the_runner_keeps_claiming_while_jobs_are_due_instead_of_sleeping_between_full_rounds(monkeypatch):
    k, J = kind(), core.T.jobs
    waiting = select(func.count()).where(J.c.kind == k, J.c.locked_until.is_(None), J.c.run_at <= func.now())
    seen: list[int] = []

    class Wake(threading.Event):
        def wait(self, timeout=None):
            if timeout == jobs.POLL:
                with core.read() as db:
                    seen.append(db.execute(waiting).scalar())
            return super().wait(timeout)

    monkeypatch.setattr(jobs, "_handlers", {})
    monkeypatch.setattr(jobs, "LANES", {"control": jobs.LANES["control"]})
    monkeypatch.setitem(jobs._wakes, "control", Wake())
    jobs.handler(k)(lambda job: time.sleep(0.05))
    with core.tx() as db:
        for n in range(8 * jobs.LANES["control"]):
            jobs.schedule(db, k, {}, key=str(n))
    stop, deadline = jobs.start(), time.monotonic() + 10
    try:
        while time.monotonic() < deadline:
            with core.read() as db:
                if not (left := db.execute(select(J.c.id).where(J.c.kind == k)).first()):
                    break
            time.sleep(0.02)
    finally:
        stop()
    assert left is None and [n for n in seen if n] == []


def test_a_stopped_runner_finishes_the_jobs_it_holds_and_leaves_the_rest_unclaimed(monkeypatch):
    k, J, ran, release = kind(), core.T.jobs, [], threading.Event()
    held = jobs.LANES["control"]
    entered = threading.Barrier(held + 1)

    def hold(job):
        ran.append(job.key)
        entered.wait()
        release.wait(5)

    monkeypatch.setattr(jobs, "_handlers", {})
    jobs.handler(k)(hold)
    with core.tx() as db:
        for n in range(2 * held):
            jobs.schedule(db, k, {}, key=str(n))
    stop = jobs.start()
    entered.wait(timeout=5)
    closer = threading.Thread(target=stop)
    closer.start()
    closer.join(0.3)
    assert closer.is_alive(), "stop returned while its handlers were still running"
    release.set()
    closer.join(5)
    assert not closer.is_alive()
    with core.read() as db:
        left = db.execute(select(J.c.key, J.c.locked_until, J.c.attempts).where(J.c.kind == k)).all()
    assert sorted(ran + [r.key for r in left], key=int) == [str(n) for n in range(2 * held)], "each ran once or waits"
    assert len(ran) == held and all(r.locked_until is None and r.attempts == 0 for r in left)


def test_a_job_claimed_as_the_runner_stops_runs_before_it_stops(monkeypatch):
    k, ran, due, stopping, entered = kind(), [], jobs._due, threading.Event(), threading.Event()

    def claim_once_stopping(lane):
        entered.set()
        stopping.wait(5)
        return due(lane)

    monkeypatch.setattr(jobs, "_handlers", {})
    monkeypatch.setattr(jobs, "LANES", {"control": 1})
    monkeypatch.setattr(jobs, "_due", claim_once_stopping)
    jobs.handler(k)(lambda job: ran.append(job.attempts))
    schedule(k, "s", {})
    closer = threading.Thread(target=jobs.start())
    closer.start()
    assert entered.wait(5), "the worker never reached its claim"
    stopping.set()
    closer.join(5)
    assert ran == [1] and row(k, "s") is None


def plan_nodes(plan: dict):
    yield plan
    for child in plan.get("Plans", ()):
        yield from plan_nodes(child)


def test_a_lanes_claim_reads_none_of_another_lanes_backlog(monkeypatch):
    backlog, timer, J, sent = kind(), kind(), core.T.jobs, []
    monkeypatch.setattr(jobs, "_handlers", {})
    jobs.handler(backlog, lane="slow")(lambda job: None)
    jobs.handler(timer)(lambda job: None)
    copied = [c for c in J.c if c.name != "id"]
    copies = select(*copied).join_from(J, func.generate_series(1, 50000).alias(), true()).where(J.c.kind == backlog)
    with core.tx() as db:
        jobs.schedule(db, backlog, {}, delay=-3600)
        db.execute(insert(J).from_select(copied, copies))
        jobs.schedule(db, timer, {})
        db.execute(text("ANALYZE jobs"))

    def capture(conn, cursor, statement, parameters, context, executemany):
        sent.append((statement, parameters))

    event.listen(core.engine, "before_cursor_execute", capture)
    try:
        assert jobs._claim(lane="control").kind == timer
    finally:
        event.remove(core.engine, "before_cursor_execute", capture)
    try:
        [(claim, parameters)] = [s for s in sent if s[0].lstrip().startswith("UPDATE")]
        with core.engine.connect() as db, db.begin() as explaining:
            [plan] = db.exec_driver_sql("EXPLAIN (ANALYZE, FORMAT JSON) " + claim, parameters).scalar()
            explaining.rollback()
        scans = [n for n in plan_nodes(plan["Plan"]) if n.get("Relation Name") == "jobs"]
        read = sum(n["Actual Rows"] + n.get("Rows Removed by Filter", 0) for n in scans)
        assert read < 1000, f"the control claim read {read} rows behind 50,000 due slow ones: {json.dumps(plan)}"
    finally:
        with core.tx() as db:
            db.execute(delete(J).where(J.c.kind.in_([backlog, timer])))


def test_a_claim_back_after_its_lease_ran_out_leaves_the_job_to_the_claim_that_took_it_over(monkeypatch):
    k, ran, real, stalled = kind(), [], jobs.read, [True]
    jobs.handler(k)(lambda job: ran.append(job.attempts))
    schedule(k, "s", {})
    monkeypatch.setattr(jobs, "LEASE", timedelta(seconds=1))
    deadline = time.monotonic() + 10

    @contextlib.contextmanager
    def slow_reply():
        with real() as db:
            yield db
        if stalled and stalled.pop():
            while not ran and time.monotonic() < deadline:
                time.sleep(0.02)
                jobs.run_now(k, "s", wait=True)

    monkeypatch.setattr(jobs, "read", slow_reply)
    assert jobs.run_now(k, "s", wait=True) is None
    assert len(ran) == 1 and ran[0] > 1, "only a claim that took the expired lease over ran it"


def test_retention_purges_refusals_past_a_few_minutes_and_keeps_parked_failures():
    k, J = kind(), core.T.jobs
    jobs.handler(k, backoff=(0,))(lambda job: 1 / 0 if job.key == "failed" else ApiError(422, "refused"))
    for key in ("old", "new", "failed"):
        schedule(k, key, {})
        jobs.run_now(k, key, wait=True)
    jobs.run_now(k, "failed", wait=True)
    aged = update(J).where(J.c.kind == k, J.c.key != "new").values(run_at=text("now() - interval '10 minutes'"))
    with core.tx() as db:
        db.execute(aged)
        jobs.schedule(db, "retention", {}, key="retention")
    jobs.run_now("retention", "retention", wait=True)
    assert row(k, "old") is None and row(k, "new").parked and row(k, "failed").parked


def test_retention_purges_idempotency_claims_older_than_a_day_and_keeps_every_text_nonce():
    tenant = uuid.uuid4()
    with core.tx() as db:
        for key, principal, age in (
            ("stale", "key:x", "25 hours"),
            ("fresh", "key:x", "23 hours"),
            ("nonce", "plivo-v2", "400 days"),
        ):
            claim = {"tenant": tenant, "principal": principal, "key": key, "age": age}
            db.execute(
                text(
                    "INSERT INTO idempotency (tenant_id, principal, key_sha256, fingerprint, created_at)"
                    " VALUES (:tenant, :principal, :key, '', now() - CAST(:age AS interval))"
                ),
                claim,
            )
        jobs.schedule(db, "retention", {}, key="retention")
    jobs.run_now("retention", "retention", wait=True)
    with core.read() as db:
        kept = set(db.execute(text("SELECT key_sha256 FROM idempotency WHERE tenant_id = :t"), {"t": tenant}).scalars())
    assert kept == {"fresh", "nonce"}


def test_retention_deletes_tokens_expired_over_a_day_ago_and_keeps_the_rest():
    with core.tx() as db:
        tenant = db.execute(
            text("INSERT INTO tenant (name, plivo_auth_id, plivo_token_enc) VALUES ('t', :a, 'x') RETURNING id"),
            {"a": uuid.uuid4().hex},
        ).scalar()
        for sha, kind, expired in (
            ("stale", "agent", "25 hours"),
            ("fresh", "stream", "23 hours"),
            ("key", "key", None),
        ):
            db.execute(
                text(
                    "INSERT INTO credential (sha256, tenant_id, kind, expires_at)"
                    " VALUES (:sha, :t, :kind, now() - CAST(:expired AS interval))"
                ),
                {"sha": f"{tenant}-{sha}", "t": tenant, "kind": kind, "expired": expired},
            )
        jobs.schedule(db, "retention", {}, key="retention")
    jobs.run_now("retention", "retention", wait=True)
    with core.read() as db:
        kept = set(db.execute(text("SELECT kind FROM credential WHERE tenant_id = :t"), {"t": tenant}).scalars())
    assert kept == {"stream", "key"}


def test_an_offer_times_out_on_time_while_every_plivo_worker_waits_on_plivo(monkeypatch):
    t, J = Tenant(), core.T.jobs
    agent, conv, release = t.agent(), t.open(), threading.Event()
    sending = threading.Barrier(jobs.LANES["plivo"] + 1)

    def hang(tenant, payload):
        sending.wait()
        release.wait()
        return {}

    monkeypatch.setitem(plivo.OPS, "hang", plivo.Op(False, hang))
    monkeypatch.setattr(jobs, "_handlers", {k: jobs._handlers[k] for k in ("plivo", "offer_timeout")})
    with core.tx() as db:
        db.execute(delete(J).where(J.c.kind.in_(("plivo", "offer_timeout"))))
        for n in range(jobs.LANES["plivo"]):
            jobs.schedule(db, "plivo", {"op": "hang", "tenant_id": str(t.id)}, key=f"hang:{n}")
        offer = cv.offer(db, cv.lock(db, conv.id), agent)
    stop = jobs.start()
    try:
        sending.wait(timeout=5)
        with core.tx() as db:
            jobs.schedule(db, "offer_timeout", due(offer), key=str(offer.id))
        deadline = time.monotonic() + 1
        while time.monotonic() < deadline and (state := offer_state(offer)) == "offered":
            time.sleep(0.05)
        assert state == "missed"
    finally:
        release.set()
        stop()


@contextlib.contextmanager
def outside():
    """Connections from no pool, so the test steers and watches without taking one of HS's."""
    engine = create_engine(core.engine.url, poolclass=NullPool, future=True)
    try:
        yield engine
    finally:
        engine.dispose()


def due(offer) -> dict:
    """The offer's timer payload, due now."""
    ids = {"tenant_id": str(offer.tenant_id), "conversation_id": str(offer.conversation_id)}
    return ids | {
        "assignment_id": str(offer.id),
        "agent_id": str(offer.agent_id),
        "channel": offer.channel,
        "timeout": 0,
    }


def offer_state(offer) -> str:
    """The offer's state as Redis keeps it, read without a database connection."""
    return json.loads(core.redis.hget(hot.OFFERS % (offer.tenant_id, offer.conversation_id), str(offer.id)))["state"]


def times_out_now(observer, t: Tenant, offer, conv) -> str:
    """Make the offer's timeout due; return its state once missed, or after 3 s, well inside the 10 s a checkout waits."""
    with observer.begin() as db:
        jobs.schedule(db, "offer_timeout", due(offer), key=str(offer.id))
    deadline = time.monotonic() + 3
    while (state := offer_state(offer)) == "offered" and time.monotonic() <= deadline:
        time.sleep(0.05)
    return state


def test_a_control_job_and_a_request_get_a_connection_while_plugin_hooks_and_plivo_stall(monkeypatch):
    t, J, release = Tenant(), core.T.jobs, threading.Event()
    agent, conv = t.agent(), t.open()
    with core.tx() as db:
        key = access.new_key(db, t.id, ["admin"])["api_key"]
        offer = cv.offer(db, cv.lock(db, conv.id), agent)
        db.execute(delete(J).where(J.c.kind.in_(("plivo", "offer_timeout"))))
        for n in range(jobs.LANES["plivo"]):
            jobs.schedule(db, "plivo", {"op": "hang", "tenant_id": str(t.id)}, key=f"hang:{n}")
    hooks = core.settings.db_pool_size + core.settings.db_max_overflow
    sending, hooked = threading.Barrier(jobs.LANES["plivo"] + 1), threading.Semaphore(0)

    def hang(tenant, payload):
        sending.wait()
        release.wait()
        return {}

    def stall(e: dict) -> None:
        hooked.release()
        release.wait()

    monkeypatch.setitem(plivo.OPS, "hang", plivo.Op(False, hang))
    monkeypatch.setattr(jobs, "_handlers", {k: jobs._handlers[k] for k in ("plivo", "offer_timeout")})
    monkeypatch.setattr(events, "HOOKS", {f"stall:{n}:{uuid.uuid4().hex}": stall for n in range(hooks)})
    api = TestClient(service.app, headers={"Authorization": f"Bearer {key}"})
    stops = [jobs.start()]
    with outside() as observer, ThreadPoolExecutor(1) as pool:
        try:
            sending.wait(timeout=5)
            stops.append(events.start())
            with core.tx() as db:
                events.emit(db, "call.held", t.id, {"by_agent_id": None}, conversation_id=conv.id)
            assert all(hooked.acquire(timeout=10) for _ in range(hooks)), "a hook never took the event"
            answer = pool.submit(api.get, "/api/v1/queues")
            assert times_out_now(observer, t, offer, conv) == "missed", "the offer timeout waited for a connection"
            assert answer.result(timeout=2).status_code == 200
        finally:
            release.set()
            for stop in reversed(stops):
                stop()


def test_a_control_job_gets_a_connection_while_ordinary_work_holds_every_one_it_may(monkeypatch):
    t, release, inside = Tenant(), threading.Event(), threading.Semaphore(0)
    agent, conv = t.agent(), t.open()
    with core.tx() as db:
        offer = cv.offer(db, cv.lock(db, conv.id), agent)
        db.execute(delete(core.T.jobs).where(core.T.jobs.c.kind == "offer_timeout"))
    total = core.settings.db_pool_size + core.settings.db_max_overflow

    def hold() -> None:
        with contextlib.suppress(PoolTimeout), core.read():
            inside.release()
            release.wait()

    monkeypatch.setattr(jobs, "_handlers", {"offer_timeout": jobs._handlers["offer_timeout"]})
    holders = [threading.Thread(target=hold) for _ in range(total)]
    stop = jobs.start()
    with outside() as observer:
        try:
            for h in holders:
                h.start()
            ordinary = total - getattr(core, "CONTROL_RESERVE", 0)
            assert all(inside.acquire(timeout=10) for _ in range(ordinary)), "ordinary work never took the pool"
            assert times_out_now(observer, t, offer, conv) == "missed", "the offer timeout waited for a connection"
        finally:
            release.set()
            for h in holders:
                h.join()
            stop()
