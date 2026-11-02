"""The `jobs` table: every timer and async task. The runner claims due rows with FOR UPDATE SKIP LOCKED and
leases them through `locked_until`; success deletes the row, failure backs off, and the last failure parks it."""

import logging
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from datetime import datetime, timedelta
from functools import partial

from sqlalchemy import (
    BigInteger,
    Column,
    Index,
    Integer,
    Table,
    Text,
    delete,
    func,
    literal_column,
    or_,
    select,
    text,
    update,
)
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP, insert
from sqlalchemy.orm import Session

from hs.infra.db import DBBase, core_engine
from hs.infra.redis import Cache
from hs.infra.settings import settings

log = logging.getLogger(__name__)

jobs = Table(
    "jobs",
    DBBase.metadata,
    Column("id", BigInteger, primary_key=True),
    Column("kind", Text, nullable=False),
    Column("key", Text),
    Column("run_at", TIMESTAMP(timezone=True), nullable=False),
    Column("payload", JSONB, nullable=False),
    Column("attempts", Integer, nullable=False, server_default="0"),
    Column("locked_until", TIMESTAMP(timezone=True)),
    Column("last_error", Text),
    Index("uq_jobs_kind_key", "kind", "key", unique=True, postgresql_where=text("key IS NOT NULL")),
    Index("ix_jobs_due", "run_at", postgresql_where=text("locked_until IS NULL OR locked_until <> 'infinity'")),
)

DEFAULT_BACKOFF = (5, 30, 120, 600)
LEASE = timedelta(seconds=60)
WORKERS = 8
POLL = 0.2
INFINITY = literal_column("'infinity'::timestamptz")
PURGES = (
    "DELETE FROM events WHERE id IN (SELECT id FROM events"
    " WHERE created_at < now() - make_interval(days => :days) LIMIT 5000)",
    "DELETE FROM jobs WHERE id IN (SELECT id FROM jobs"
    " WHERE locked_until = 'infinity' AND run_at < now() - make_interval(days => :days) LIMIT 5000)",
)

_handlers: dict[str, tuple[Callable[[dict], None], tuple[float, ...]]] = {}


def execute(session: Session | None, *statements) -> None:
    """Run in the caller's session (commits with it), else in a short transaction of its own."""
    with nullcontext(session) if session is not None else core_engine.begin() as conn:
        for statement in statements:
            conn.execute(statement)


def handler(kind: str, backoff: tuple[float, ...] = DEFAULT_BACKOFF):
    """Register the function that runs jobs of `kind`; failure n waits backoff[n-1], one more failure parks."""

    def register(fn: Callable[[dict], None]) -> Callable[[dict], None]:
        _handlers[kind] = (fn, backoff)
        return fn

    return register


def schedule(
    kind: str,
    payload: dict,
    *,
    delay: float = 0.0,
    run_at: datetime | None = None,
    key: str | None = None,
    session: Session | None = None,
) -> None:
    when = run_at if run_at is not None else func.now() + timedelta(seconds=delay)
    upsert = insert(jobs).values(kind=kind, key=key, run_at=when, payload=payload)
    upsert = upsert.on_conflict_do_update(
        index_elements=[jobs.c.kind, jobs.c.key],
        index_where=jobs.c.key.isnot(None),
        set_={"run_at": when, "payload": payload, "attempts": 0, "locked_until": None, "last_error": None},
    )
    # A re-schedule replaces the row, so a run in flight cannot delete its successor.
    replaced = [] if key is None else [delete(jobs).where(jobs.c.kind == kind, jobs.c.key == key)]
    execute(session, *replaced, upsert)


def cancel(kind: str, key: str) -> bool:
    with core_engine.begin() as conn:
        return conn.execute(delete(jobs).where(jobs.c.kind == kind, jobs.c.key == key)).rowcount > 0


def claim(conn):
    """Lease one due job of a kind this process can run, or None. The claim counts the attempt, so a job whose
    run kills the process still parks once its attempts are spent."""
    due = (
        select(jobs.c.id)
        .where(
            jobs.c.kind.in_(list(_handlers)),
            jobs.c.run_at <= func.now(),
            or_(jobs.c.locked_until.is_(None), jobs.c.locked_until != INFINITY),  # ix_jobs_due's predicate
            or_(jobs.c.locked_until.is_(None), jobs.c.locked_until < func.now()),
        )
        .order_by(jobs.c.run_at)
        .limit(1)
        .with_for_update(skip_locked=True)
        .scalar_subquery()
    )
    leased = update(jobs).where(jobs.c.id == due).values(locked_until=func.now() + LEASE, attempts=jobs.c.attempts + 1)
    return conn.execute(
        leased.returning(jobs.c.id, jobs.c.kind, jobs.c.payload, jobs.c.attempts, jobs.c.locked_until)
    ).first()


class Retry(RuntimeError):
    """An expected wait, not a failure: the job runs again on its backoff and logs no traceback."""


def work(job) -> None:
    """Run one leased job. Completion is fenced on the lease: a worker whose lease expired changes nothing."""
    fn, backoff = _handlers[job.kind]
    mine = (jobs.c.id == job.id, jobs.c.locked_until == job.locked_until)
    if job.attempts > len(backoff) + 1:
        execute(None, update(jobs).where(*mine).values(locked_until=INFINITY, last_error="no attempt finished"))
        return
    try:
        fn(job.payload)
    except Exception as e:
        failed = log.warning if isinstance(e, Retry) else log.exception
        failed("job %s %s failed (attempt %s): %s", job.kind, job.id, job.attempts, e)
        values = {"last_error": f"{type(e).__name__}: {e}"[:2000], "locked_until": INFINITY}
        if job.attempts <= len(backoff):
            values |= {"locked_until": None, "run_at": func.now() + timedelta(seconds=backoff[job.attempts - 1])}
        done = update(jobs).where(*mine).values(**values)
    else:
        done = delete(jobs).where(*mine)
    execute(None, done)


def retention() -> None:
    """Once a day across all workers: events older than EVENT_RETENTION_DAYS, and parked jobs as old. The gate
    holds an hour while a pass runs, so a failed pass is retried then; a finished one holds it a day."""
    gate = Cache.get_instance().client
    if not gate.set("jobs:retention", 1, nx=True, ex=3600):
        return
    days = {"days": settings.event_retention_days}
    for purge in PURGES:
        while True:
            with core_engine.begin() as conn:
                if not conn.execute(text(purge), days).rowcount:
                    break
    gate.set("jobs:retention", 1, ex=86400)


def _fill(pool: ThreadPoolExecutor, slots: threading.Semaphore) -> None:
    while slots.acquire(blocking=False):
        job = None
        try:
            with core_engine.begin() as conn:
                job = claim(conn)
        finally:
            if job is None:
                slots.release()
        if job is None:
            return
        pool.submit(work, job).add_done_callback(partial(_done, job, slots))


def _done(job, slots: threading.Semaphore, future) -> None:
    slots.release()
    if not future.cancelled() and (error := future.exception()):
        log.error("job %s %s could not be completed", job.kind, job.id, exc_info=error)


def background(name: str, step: Callable[[], None]) -> Callable[[], None]:
    """Run `step` every POLL seconds on a daemon thread; returns its stop function."""
    stop = threading.Event()

    def loop() -> None:
        while not stop.wait(POLL):
            try:
                step()
            except Exception:
                log.exception("%s pass failed", name)

    thread = threading.Thread(target=loop, name=name, daemon=True)
    thread.start()

    def close() -> None:
        stop.set()
        thread.join(5)

    return close


def start() -> Callable[[], None]:
    """Start the job runner on this worker; returns its stop function."""
    slots, pool = threading.Semaphore(WORKERS), ThreadPoolExecutor(WORKERS, thread_name_prefix="job")

    def step() -> None:
        try:
            retention()
        except Exception:
            log.exception("retention pass failed")
        _fill(pool, slots)

    stop = background("job runner", step)

    def close() -> None:
        stop()
        pool.shutdown(wait=False, cancel_futures=True)

    return close
