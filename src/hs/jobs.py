"""The `jobs` table: every timer, recurring pass and provider command. The runner claims due rows with FOR UPDATE
SKIP LOCKED and leases them through `locked_until`; success deletes the row, failure backs off, and the last failure
parks it (`locked_until = infinity`). A handler that returns the ApiError refusing it parks its row at once, the
refusal kept as `payload.refused` for whoever waits on the job."""

import logging
import random
import threading
import time
from collections.abc import Callable
from datetime import datetime, timedelta
from functools import cache

from prometheus_client import Counter
from sqlalchemy import Interval, and_, bindparam, case, delete, func, literal_column, or_, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Connection, Row

from hs.core import CONTROL_LANE, CONTROL_RESERVE, ApiError, T, read, settings, tx

log = logging.getLogger(__name__)
DEFAULT_BACKOFF = (5, 30, 120, 600)
# A runner frozen for over LEASE minus Plivo's 5 s timeout between its claim and its send can send a superseded
# convergent op after the newer one; if that is ever seen, read the provider's state before such a send.
LEASE = timedelta(seconds=60)
# A lane claims only rows stamped with it, so a slow endpoint never holds up a timer. A claim takes only kinds this
# process registered, so a newer release's kinds wait for it; a row inserted without a lane reads control, which runs it.
LANES = {"plivo": 8, "webhook": 4, "slow": 4, "control": CONTROL_RESERVE}
POLL = 0.2
INFINITY = literal_column("'infinity'::timestamptz")

PARKED = Counter("hs_jobs_parked", "Jobs parked after their last attempt", ("kind",))
# Parked jobs past EVENT_RETENTION_DAYS (refused ones past 5 minutes: only a route waiting on one reads it), and
# idempotency records and tokens expired over a day ago, in batches. A text callback's nonce is kept: Plivo's V2
# signature never expires and never covers the body, so the nonce is all that refuses a replay.
PURGES = (
    text(
        "DELETE FROM jobs WHERE id IN (SELECT id FROM jobs WHERE locked_until = 'infinity'"
        " AND (run_at < now() - make_interval(days => :days)"
        " OR (payload ? 'refused' AND run_at < now() - interval '5 minutes')) LIMIT 5000)"
    ),
    text(
        "DELETE FROM idempotency WHERE ctid = ANY(ARRAY(SELECT ctid FROM idempotency"
        " WHERE created_at < now() - interval '1 day' AND principal <> 'plivo-v2' LIMIT 5000))"
    ),
    text(
        "DELETE FROM credential WHERE id IN (SELECT id FROM credential"
        " WHERE expires_at < now() - interval '1 day' LIMIT 5000)"
    ),
)

_handlers: dict[str, tuple[Callable[[Row], object], tuple[int, ...]]] = {}
_lanes: dict[str, str] = {}
_every: dict[str, float] = {}
_unparked: set[str] = set()
_wakes = {lane: threading.Event() for lane in LANES}


def parked():
    return T.jobs.c.locked_until == INFINITY


class NotYet(Exception):
    """What the handler waits on is not there yet: its job backs off as on a failure, logged without a traceback."""


def schedule(
    db: Connection,
    kind: str,
    payload: dict,
    *,
    delay: float = 0,
    at: datetime | None = None,
    key: str | None = None,
    rev: int | None = None,
) -> None:
    """A keyed schedule rewrites its row in place and bumps `rev`: a run in flight then neither deletes it nor
    finishes after it, since it only releases the row and the newest payload runs next. The earlier claims count
    as one, so the next claim knows a create may have been sent, unless a refusal ended them. With `rev`, a run
    reschedules its own row only while no schedule replaced it."""
    params = {"j_kind": kind, "j_key": key, "j_payload": payload, "j_rev": rev, "j_at": at}
    params |= {"j_lane": _lanes[kind], "j_delay": timedelta(seconds=delay)}
    db.execute(_scheduling(key is not None, at is not None, rev is not None), params)


# The statements a call runs most are built once: SQLAlchemy keeps a built statement's cache key.
@cache
def _scheduling(keyed: bool, at: bool, fenced: bool):
    J = T.jobs
    when = bindparam("j_at", type_=J.c.run_at.type) if at else func.now() + bindparam("j_delay", type_=Interval)
    row = insert(J).values(**{c: bindparam(f"j_{c}") for c in ("kind", "lane", "key", "payload")}, run_at=when)
    if not keyed:
        return row
    claimed = case((J.c.payload.has_key("refused"), 0), else_=func.least(J.c.attempts, 1))
    fields = {"run_at": row.excluded.run_at, "payload": row.excluded.payload, "rev": J.c.rev + 1, "attempts": claimed}
    return row.on_conflict_do_update(
        index_elements=[J.c.kind, J.c.key],
        index_where=J.c.key.isnot(None),
        set_=fields | {"last_error": None, "locked_until": case((parked(), None), else_=J.c.locked_until)},
        where=J.c.rev == bindparam("j_rev") if fenced else None,
    )


@cache
def _cancelling():
    return delete(T.jobs).where(T.jobs.c.kind == bindparam("j_kind"), T.jobs.c.key == bindparam("j_key"))


def cancel(db: Connection, kind: str, key: str) -> bool:
    return db.execute(_cancelling(), {"j_kind": kind, "j_key": key}).rowcount > 0


def handler(kind: str, backoff: tuple[int, ...] = DEFAULT_BACKOFF, lane: str = "control", parks: bool = True):
    """Register `fn(job)` for `kind`, run by a worker of `lane`. It opens its own transactions; `job.attempts` counts
    claims, this one included, and the claims before a reschedule as one. Failure n waits backoff[n-1]; one more
    failure parks it. Unless `parks`, every failure waits a random 20% to 100% of the last backoff instead: a replica
    whose attempts all fail then cannot line its polls up with the retries and claim each one. Its NotYet logs only
    the first of a run of failures, and the run that ends them."""
    if not backoff:
        raise ValueError(f"job kind {kind} needs a backoff: a rescheduled job's next claim is at least its second")
    if lane not in LANES:
        raise ValueError(f"job kind {kind} names lane {lane}, not one of {', '.join(LANES)}")

    def register(fn: Callable[[Row], object]) -> Callable[[Row], object]:
        _handlers[kind], _lanes[kind] = (fn, backoff), lane
        if not parks:
            _unparked.add(kind)
        return fn

    return register


def every(kind: str, seconds: float) -> None:
    """`kind` recurs: one row keyed by its kind, rescheduled after each run, so one replica runs each pass."""
    _every[kind] = seconds


def _claim(*where, lane: str | None = None) -> Row | None:
    """Lease one job for its caller to run at once: one of `lane`'s due jobs, else one matching `where`. A claim
    back after its lease ran out is left to whoever claims it next, so it never runs twice at once."""
    leasing = _due_of(lane, tuple(_handlers)) if lane else _leasing(*where)
    asked = time.monotonic()
    with read() as db:
        job = db.execute(leasing).first()
    # Callers run the handler straight after this and nothing re-leases an unexpired row: within LEASE, it is ours.
    if job is not None and time.monotonic() - asked >= LEASE.total_seconds():
        log.warning("job %s %s claimed past its lease; left to the next claim", job.kind, job.key or job.id)
        return None
    return job


def _leasing(*where):
    J = T.jobs
    free = or_(J.c.locked_until.is_(None), and_(~parked(), J.c.locked_until < func.now()))
    one = select(J.c.id).where(*where, free).order_by(J.c.run_at).limit(1).with_for_update(skip_locked=True)
    leased = update(J).where(J.c.id == one.scalar_subquery())
    return leased.values(locked_until=func.now() + LEASE, attempts=J.c.attempts + 1).returning(*J.c)


@cache
def _due_of(lane: str, kinds: tuple[str, ...]):
    J = T.jobs
    return _leasing(J.c.lane == lane, J.c.kind.in_(kinds), J.c.run_at <= func.now())


def _next(job: Row, backoff: tuple[int, ...], error: str | None, result: object) -> dict | None:
    """The row's next state; None deletes it."""
    if isinstance(result, ApiError):
        refused = {"status": result.status, "code": result.code, "message": result.message, "details": result.details}
        return {"locked_until": INFINITY, "payload": T.jobs.c.payload.concat({"refused": refused})}
    if job.kind in _every:
        return {"run_at": func.now() + timedelta(seconds=_every[job.kind]), "locked_until": None, "attempts": 0}
    if error is None:
        return None
    if job.kind in _unparked:
        wait = timedelta(seconds=backoff[-1] * random.uniform(0.2, 1))
        return {"run_at": func.now() + wait, "locked_until": None, "attempts": 0}
    if job.attempts <= len(backoff):
        return {"run_at": func.now() + timedelta(seconds=backoff[job.attempts - 1]), "locked_until": None}
    return {"locked_until": INFINITY}


def work(job: Row) -> object:
    """Run one leased job and return what its handler returned. The write after it is fenced on the lease and on
    `rev`: a stale worker changes nothing, and a reschedule meanwhile only releases the row. Since that run is then
    never retried, an ApiError its handler raised is returned as its outcome."""
    fn, backoff = _handlers[job.kind]
    result = raised = None
    error = "no attempt finished" if job.attempts > len(backoff) + 1 else None
    failing = job.kind in _unparked and job.last_error is not None
    if error is None:
        try:
            result = fn(job)
        except NotYet as e:
            if not failing:
                log.info("job %s %s not ready (attempt %s): %s", job.kind, job.key or job.id, job.attempts, e)
            error = f"NotYet: {e}"[:2000]
        except Exception as e:
            log.exception("job %s %s failed (attempt %s)", job.kind, job.key or job.id, job.attempts)
            error, raised = f"{type(e).__name__}: {e}"[:2000], e
        else:
            if failing:
                log.info("job %s %s ran after failing: %s", job.kind, job.key or job.id, job.last_error)
    J, after = T.jobs, _next(job, backoff, error, result)
    mine = (J.c.id == job.id, J.c.locked_until == job.locked_until)
    with read() as db:
        if after is None:
            applied = db.execute(_done(), {"j_id": job.id, "j_lease": job.locked_until, "j_rev": job.rev}).rowcount
        else:
            applied = db.execute(update(J).values(**after, last_error=error).where(*mine, J.c.rev == job.rev)).rowcount
        replaced = not applied and db.execute(update(J).where(*mine).values(locked_until=None)).rowcount
    if applied and error and after["locked_until"] is INFINITY:
        PARKED.labels(job.kind).inc()
    return raised if replaced and isinstance(raised, ApiError) else result


@cache
def _done():
    J = T.jobs
    mine = (J.c.id == bindparam("j_id"), J.c.locked_until == bindparam("j_lease"), J.c.rev == bindparam("j_rev"))
    return delete(J).where(*mine)


def run_now(kind: str, key: str, *where, wait: bool = False) -> object | None:
    """With `wait`, claim that job here unless someone holds it or it fails `where`, run it and return its handler's
    result. Without, wake its lane: an idle worker claims the lane's due jobs, this one among them, else the next one
    freed does."""
    if not wait:
        _wakes[_lanes[kind]].set()
        return None
    job = _claim(T.jobs.c.kind == kind, T.jobs.c.key == key, *where)
    return None if job is None else work(job)


def _due(lane: str) -> Row | None:
    try:
        return _claim(lane=lane)
    except Exception:
        log.exception("job lane %s could not claim", lane)
        return None


def _worker(lane: str, turn: threading.Lock, stop: threading.Event) -> None:
    """Claim a due job of `lane`, run it, repeat; with none due, wait for a wake or POLL. Holding `turn` while it
    looks, one idle worker of a lane polls at a time, so a wake or a due job costs one claim, not one per worker."""
    CONTROL_LANE.on = lane == "control"
    while True:
        with turn:
            job = None
            while job is None and not stop.is_set():
                _wakes[lane].clear()
                if (job := _due(lane)) is None:
                    _wakes[lane].wait(POLL)
        if job is None:
            return
        try:
            work(job)
        except Exception:
            log.exception("job %s %s could not be completed", job.kind, job.id)


def start() -> Callable[[], None]:
    """Seed the recurring jobs and start each lane's workers on this process; returns the stop function, which lets
    every worker finish the job it holds."""
    with tx() as db:
        for kind in _every:
            row = insert(T.jobs).values(kind=kind, lane=_lanes[kind], key=kind, run_at=func.now(), payload={})
            db.execute(row.on_conflict_do_nothing())
    stop, workers = threading.Event(), []
    for lane, n in LANES.items():
        args = (lane, threading.Lock(), stop)
        workers += [threading.Thread(target=_worker, args=args, name=f"job {lane}", daemon=True) for _ in range(n)]
    for thread in workers:
        thread.start()

    def close() -> None:
        stop.set()
        for wake in _wakes.values():
            wake.set()
        for thread in workers:
            thread.join()

    return close


every("retention", 86400)


@handler("retention", lane="slow")
def _retention(job: Row) -> None:
    for purge in PURGES:
        while True:
            with read() as db:
                if not db.execute(purge, {"days": settings.event_retention_days}).rowcount:
                    break
