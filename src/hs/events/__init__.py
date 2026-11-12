"""Events: the catalog, the outbox and its relay, the stream fan-out, webhooks and plugin hooks.

`emit` inserts an unstamped row in the caller's transaction. One relay at a time stamps `seq`, the public id, in
commit order, copies the conversation's direction and context onto the row and queues one webhook job, envelope
included, per matching subscription. Each process reads the stamped rows once per wake and hands each to its open
streams that may see it; each plugin hook follows its own cursor."""

import asyncio
import json
import logging
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor, wait
from contextlib import contextmanager
from dataclasses import dataclass, field
from functools import cache, partial
from typing import NamedTuple

from fastapi import Request
from prometheus_client import Gauge
from sqlalchemy import bindparam, insert, select, text
from sqlalchemy.engine import Connection, Row

from hs import access, core, hot, jobs
from hs.access import Principal
from hs.events.catalog import CATALOG

log = logging.getLogger(__name__)
HOOKS: dict[str, Callable[[dict], None]] = {}
GRANTS = ("key:events", "agent", "stream")
POLL, AUTH_EVERY, PING, BATCH, WAKE_PAGE, PAGE, HOOK_PAGE, HOOK_RETRY = 0.2, 2.0, 15, 500, 1000, 200, 50, (1, 2, 5)
RETRIES = (10, 60, 300, 1800, 7200, 21600, 43200)
EXPIRED = 'data: {"type":"auth.expired"}\n\n'
BACKLOG = 1000  # frames a stream may fall behind before it is ended; its client resumes with Last-Event-ID
SCAN = 10  # so a long scoped replay hands its thread back between slices
CHECK_READ = 2 * core.POOL_TIMEOUT + int(core.engine.url.query["connect_timeout"]) + core.STATEMENT_TIMEOUT
STALE = 2 * CHECK_READ + AUTH_EVERY
LAG = Gauge("hs_hook_lag", "Events stamped after a plugin hook's cursor", ("hook",))

# The envelope, as every outlet sends it: the seq is the id, and the time is UTC to the millisecond.
ENVELOPE = (
    "SELECT seq::text AS id, type, to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"')"
    " AS created_at, tenant_id, conversation_id, agent_id, direction, context, data"
)
COLUMNS = ENVELOPE + " FROM events"
RELAY_LOCK = text("SELECT pg_try_advisory_xact_lock(1213420118)")
# nextval sits in a sorted target list, which Postgres evaluates after the sort: seq follows id.
STAMP_ROWS = """
WITH batch AS (
    SELECT e.id, nextval('event_seq') AS seq, c.direction, c.context
    FROM (SELECT id, conversation_id FROM events WHERE seq IS NULL ORDER BY id LIMIT :batch) e
    LEFT JOIN conversation c ON c.id = CAST(e.conversation_id AS uuid)
    ORDER BY e.id
), stamped AS (
    UPDATE events SET seq = b.seq, direction = b.direction, context = b.context FROM batch b WHERE events.id = b.id
    RETURNING events.*
), envelopes AS ("""
STAMP_JOBS = """ FROM stamped), deliveries AS (
    INSERT INTO jobs (kind, lane, run_at, payload)
    SELECT 'webhook', 'webhook', now(), jsonb_build_object('webhook_id', w.id, 'body', to_jsonb(e) - 'queue_id')
    FROM envelopes e JOIN webhooks w ON w.tenant_id = e.tenant_id AND w.enabled
        AND (cardinality(w.event_types) = 0 OR e.type = ANY(w.event_types))
        AND (cardinality(w.queue_ids) = 0 OR e.queue_id = ANY(w.queue_ids))
)
SELECT count(*) FROM stamped
"""
STAMP = text(STAMP_ROWS + ENVELOPE + ", queue_id" + STAMP_JOBS)
HEAD = text("SELECT coalesce(max(seq), 0) FROM events WHERE seq IS NOT NULL")
NEW = text(COLUMNS + " WHERE seq > :after ORDER BY seq LIMIT :limit")
SELECT = text(
    COLUMNS + " WHERE tenant_id = :tenant AND seq > :after AND (CAST(:head AS bigint) IS NULL OR seq <= :head)"
    " AND (CAST(:types AS text[]) IS NULL OR type = ANY(CAST(:types AS text[])))"
    " AND (CAST(:conversation AS text) IS NULL OR conversation_id = :conversation) ORDER BY seq LIMIT :limit"
)
FACTS = text(
    "SELECT c.id::text AS id, ARRAY(SELECT a.agent_id::text FROM assignment a WHERE a.conversation_id = c.id"
    " AND a.state IN ('offered', 'active', 'wrap_up') AND a.agent_id IS NOT NULL) AS agents,"
    " ARRAY(SELECT jsonb_build_array(c.tenant_id, j.key, j.payload->>'agent_id') FROM jobs j"
    " WHERE j.kind = 'offer_timeout' AND j.payload->>'conversation_id' = c.id::text) AS timers,"
    " ARRAY(SELECT jsonb_array_elements_text(coalesce(c.routing->'route'->'teams', q.body->'teams'))) AS teams,"
    " ARRAY(SELECT a.agent_id::text FROM assignment a WHERE a.agent_id IS NOT NULL AND a.started_at IS NOT NULL"
    " AND (a.role = 'primary' OR a.state = 'active') AND a.conversation_id = ANY(c.id"
    " || ARRAY(SELECT s.conversation_id FROM task s WHERE s.origin_id = c.id))) AS worked,"
    " ARRAY(SELECT a.agent_id::text FROM assignment a WHERE a.conversation_id = c.id AND a.agent_id IS NOT NULL"
    " AND a.started_at IS NOT NULL) AS took,"
    " ARRAY(SELECT a.agent_id::text FROM assignment a WHERE a.conversation_id = c.id AND a.agent_id IS NOT NULL"
    " AND (a.state = 'active' OR a.state = 'wrap_up' AND a.role = 'primary')) AS here"
    " FROM conversation c LEFT JOIN config q ON q.id = c.queue_id AND q.kind = 'queue'"
    " WHERE c.id = ANY(CAST(:ids AS uuid[]))"
)
# Events about a record only its readers may see (a task's) never reach an offer or a queue.
READERS = {"task.created", "task.updated"}
NOTES = {"note.added", "note.updated", "note.deleted"}
LIVE = text(
    "SELECT c.sha256, c.tenant_id::text, c.kind, CASE WHEN c.kind = 'key' THEN c.id::text END AS key_id,"
    " c.agent_id::text, a.body->>'role' AS role, c.scopes, "
    + access.SCOPE.format(role="a.body->>'role'")
    + access.CREDENTIALS
    + " AND c.sha256 = ANY(CAST(:shas AS text[]))"
).bindparams(permissions=json.dumps(access.PERMISSIONS))
# The same for the callers plugin resolvers named, by the role each was given, in the order asked.
AGENT = text("SELECT " + access.SCOPE.format(role="x.role") + access.WHO + " ORDER BY x.n").bindparams(
    permissions=json.dumps(access.PERMISSIONS)
)
HOOK_START = text(
    "INSERT INTO hook_cursor (name, seq) SELECT :name, coalesce(max(seq), 0) FROM events WHERE seq IS NOT NULL"
    " ON CONFLICT DO NOTHING"
)
# A pass or a replay holds its hook by a lease on the cursor, so no connection is checked out while the plugin runs,
# and `_keep` renews it, so a call however slow keeps the hook from every other holder. Only the holder moves it.
# A call already running when its lease lapses cannot be recalled and may end after the next holder's begins; a plugin that must never see that drops an id at or below the highest it applied.
HOOK_LEASE = 60
TAKE = (
    "UPDATE hook_cursor SET held_by = :token, held_until = now() + make_interval(secs => :lease) WHERE name = :name"
    " AND (held_until IS NULL OR held_until < now())"
)
HOOK_TAKE = text(TAKE + " RETURNING seq")
# A replay takes the hook only while its claim still owns the job row.
REPLAY_TAKE = text(TAKE + " AND EXISTS (SELECT 1 FROM jobs WHERE id = :id AND locked_until = :claim) RETURNING seq")  # noqa: S608
HOOK_RENEW = text(
    "UPDATE hook_cursor SET held_until = now() + make_interval(secs => :lease)"
    " WHERE held_by = ANY(CAST(:tokens AS uuid[])) RETURNING held_by"
)
HOOK_GIVE = text(
    "UPDATE hook_cursor SET seq = :seq, held_by = NULL, held_until = NULL, updated_at = now()"
    " WHERE name = :name AND held_by = :token"
)
# A replay's job row, fenced on this claim's lease. Waiting runs it again in a second, as a new rev the runner
# keeps and not as an attempt.
REPLAY_LATER = text(
    "UPDATE jobs SET run_at = now() + interval '1 second', rev = rev + 1, attempts = attempts - 1"
    " WHERE id = :id AND locked_until = :claim"
)
# A conversation not yet closed keeps every event: its routing rebuild and its search fill read them.
PURGE = text(
    "DELETE FROM events WHERE id IN (SELECT id FROM events e WHERE created_at < now() - make_interval(days => :days)"
    " AND seq <= coalesce((SELECT min(seq) FROM hook_cursor WHERE name = ANY(:hooks)"
    " OR greatest(updated_at, held_until) > now() - make_interval(secs => :lease)), seq) AND NOT EXISTS (SELECT"
    " FROM conversation c WHERE c.id = CAST(e.conversation_id AS uuid) AND c.state <> 'closed') LIMIT 5000)"
)


def _id(value) -> str | None:
    return None if value is None else str(uuid.UUID(str(value)))


def emit(db: Connection, type_: str, tenant_id, data: dict, *, conversation_id=None, agent_id=None) -> None:
    """Record an event in the caller's transaction, so it commits with the change it tells of."""
    body = CATALOG[type_].model_validate(data).model_dump(mode="json", by_alias=True)
    row = {"e_tenant": _id(tenant_id), "e_type": type_, "e_conv": _id(conversation_id), "e_agent": _id(agent_id)}
    db.execute(_recording(), row | {"e_data": body})


@cache
def _recording():
    """Built once, as every state change runs it: SQLAlchemy keeps a built statement's cache key."""
    E, C = core.T.events, core.T.conversation
    conv = bindparam("e_conv", type_=E.c.conversation_id.type)
    queue = select(C.c.queue_id).where(C.c.id == conv).scalar_subquery()
    values = {"tenant_id": bindparam("e_tenant"), "type": bindparam("e_type"), "agent_id": bindparam("e_agent")}
    return insert(E).values(**values, conversation_id=conv, data=bindparam("e_data"), queue_id=queue)


def _envelopes(db: Connection, statement, params: dict) -> list[dict]:
    return [dict(r._mapping) for r in db.execute(statement, params)]


def frame(e: dict) -> str:
    return f"id: {e['id']}\ndata: {json.dumps(e, separators=(',', ':'))}\n\n"


class Scope(NamedTuple):
    agents: frozenset[str]
    teams: frozenset[str]

    def within(self, wider: "Scope") -> bool:
        return self.agents <= wider.agents and self.teams <= wider.teams


class Answer(NamedTuple):
    asked_at: float
    principal: Principal | None


class Auth(NamedTuple):
    """A stream's authorization: its principal, and what it sees (None for the whole tenant: a key or an admin)."""

    principal: Principal
    scope: Scope | None


def _authorize(callers: list[tuple[str, Principal | None]]) -> list[Auth | None]:
    found = {i: p for i, (_, p) in enumerate(callers) if p}
    ps = found.values()
    who = {"agents": [p.agent_id for p in ps], "tenants": [p.tenant_id for p in ps], "roles": [p.role for p in ps]}
    with core.read() as db:
        rows = {r.sha256: r for r in db.execute(LIVE, {"shas": [access.digest(token) for token, _ in callers]})}
        docs = dict(zip(found, db.execute(AGENT, who).all() if found else (), strict=True))
    out: list[Auth | None] = []
    for i, (token, p) in enumerate(callers):
        if r := rows.get(access.digest(token)):
            p = Principal(r.tenant_id, r.kind, r.key_id, r.agent_id, r.role, frozenset(r.scopes))
        else:
            r = docs.get(i)
        if p is None or not any(access.holds_grant(p, grant) for grant in GRANTS):
            out.append(None)
        else:
            s = access.Seen.of(p, r.grants, r.mates, r.teams)
            out.append(Auth(p, None if s.agents is None else Scope(s.agents, s.teams)))
    return out


def authorize(token: str, request: Request | None) -> Auth | None:
    return _authorize([(token, None if token.startswith(core.OURS) else access.resolve(token, request))])[0]


def facts(db: Connection, rows: list[dict]) -> dict[str, tuple[frozenset, ...]]:
    ids = sorted({e["conversation_id"] for e in rows if e["conversation_id"]})
    found = db.execute(FACTS, {"ids": ids}).all() if ids else []
    timers = [(t, r.id, o) for r in found for t, o, _ in r.timers]
    states = hot.standing(db, timers, blind=True)
    held = {o for (_, _, o), s in zip(timers, states, strict=True) if s in ("offered", "active")}
    agents = {r.id: frozenset(r.agents).union(a for _, o, a in r.timers if o in held) for r in found}
    return {r.id: (agents[r.id], *map(frozenset, (r.teams, r.worked, r.took, r.here))) for r in found}


def visible(e: dict, scope: Scope | None, known: dict, reader: str | None) -> bool:
    agents, teams, worked, took, _ = known.get(e["conversation_id"], ((),) * 5)
    if scope is None:
        return True
    sup = e["type"].startswith("supervisor.") and not e["data"]["visible"] and e["data"]["supervisor_id"]
    if sup and reader != sup and (sup not in scope.agents or reader in agents or reader in took):
        return False
    if e["agent_id"] in scope.agents:
        return True
    if e["type"] in READERS:
        return not scope.agents.isdisjoint(worked)
    untaken = not took and not scope.teams.isdisjoint(teams)
    return untaken or not scope.agents.isdisjoint(took if e["type"] in NOTES else agents)


def shown(e: dict, agent_id: str | None, known: dict) -> dict:
    """The envelope as `agent_id` gets it: an agent gets the handoff context only while it is on the conversation,
    in wrap-up as its primary included."""
    if e["context"] is None or agent_id is None or agent_id in known.get(e["conversation_id"], ((),) * 5)[4]:
        return e
    return e | {"context": None}


def read(
    tenant_id: str, after: int, limit: int, *, head=None, scope=None, agent=None, types=None, conversation_id=None
) -> tuple[list[dict], int | None]:
    """The tenant's stamped events with after < seq <= head, oldest first, at most `limit`; with `scope` only those
    it sees, and as `agent` gets them (`shown`). It scans at most SCAN pages, each on its own connection, and returns
    the seq to read on after, or None once none is left up to `head`. An unscoped read ends on its first page."""
    params = {"tenant": tenant_id, "head": head, "types": types, "conversation": conversation_id, "limit": limit}
    out: list[dict] = []
    for _ in range(SCAN):
        with core.read() as db:
            rows = _envelopes(db, SELECT, params | {"after": after})
            known = facts(db, rows) if scope is not None or agent else {}
        out += [shown(e, agent, known) for e in rows if visible(e, scope, known, agent)]
        if len(out) >= limit:
            return out[:limit], int(out[limit - 1]["id"])
        if len(rows) < limit:
            return out, None
        after = int(rows[-1]["id"])
    return out, after


def relay_once() -> bool:
    """Stamp one batch unless another relay holds the lock; True when the batch was full."""
    with core.tx() as db:
        return bool(db.execute(RELAY_LOCK).scalar()) and db.execute(STAMP, {"batch": BATCH}).scalar() == BATCH


@dataclass(eq=False)
class Stream:
    """One open SSE response. The fan-out and check threads call `send` and `end`; the response's event loop runs
    `frames`."""

    auth: Auth
    token: str
    request: Request | None  # what a plugin resolver reads again; HS's own credentials are checked by SHA-256
    loop: asyncio.AbstractEventLoop
    queue: asyncio.Queue = field(default_factory=asyncio.Queue)
    checked: float = field(default_factory=time.monotonic)  # the last check that found it unchanged
    lagged: bool = False
    revoked: bool = False

    @property
    def tenant(self) -> str:
        return self.auth.principal.tenant_id

    def confirmed(self) -> bool:
        return not self.revoked and time.monotonic() - self.checked <= STALE

    def send(self, item: tuple[int, str]) -> None:
        """From any thread: an (id, frame) pair."""
        self.loop.call_soon_threadsafe(self._offer, item)

    def end(self) -> None:
        """From any thread: no frame is yielded after this, and the client gets auth.expired at once."""
        self.revoked = True
        leave(self)
        self.loop.call_soon_threadsafe(self._offer, None)

    def _offer(self, item) -> None:
        """An (id, frame) pair, or None to end the stream: a client this far behind resumes with Last-Event-ID."""
        if not self.lagged:
            self.lagged = self.queue.qsize() >= BACKLOG
            self.queue.put_nowait(None if self.lagged else item)

    async def frames(self, cursor: int | None):
        """Events after `cursor` (by default from now): up to the fan-out's position at join from the table, then the
        live feed. Its authorization is checked before every frame, and at least every AUTH_EVERY while idle."""
        start = join(self)
        cursor = start if cursor is None else cursor
        sent, pinged = max(cursor, start), time.monotonic()
        try:
            yield "retry: 3000\n: ping\n\n"
            while cursor < start and self.confirmed():
                who = {"head": start, "scope": self.auth.scope, "agent": self.auth.principal.agent_id}
                scan = partial(read, self.tenant, cursor, PAGE, **who)
                batch, after = await self.loop.run_in_executor(_replays, scan)
                for e in batch:
                    if self.confirmed():
                        yield frame(e)
                cursor = start if after is None else after
            while self.confirmed():
                try:
                    async with asyncio.timeout(AUTH_EVERY):
                        item = await self.queue.get()
                except TimeoutError:
                    item = ""
                if item is None or not self.confirmed():
                    break
                if item and item[0] > sent:
                    sent = item[0]
                    yield item[1]
                elif time.monotonic() - pinged >= PING:
                    pinged = time.monotonic()
                    yield ": ping\n\n"
            if self.revoked:
                yield EXPIRED
            elif not self.confirmed():
                log.warning("events: no stream check finished in %s s, so a stream ended to resume", STALE)
        finally:
            leave(self)


_lock = threading.Lock()
_streams: dict[str, set[Stream]] = {}
_last = 0  # the newest seq this process has handed to its streams
_wakes: list[threading.Event] = []
# 16 at once holds about 450 plugin streams a replica at a resolver's 2 s bound; past that, size it by streams times
# latency.
_askers = ThreadPoolExecutor(16, "stream check")
_asking: dict[Stream, Future] = {}
# A shift of stale desks resuming at once queues here, holding no connection.
_replays = ThreadPoolExecutor(4, "replay")


def join(s: Stream) -> int:
    """Fan out to `s` from now on; returns the position after which it gets every event."""
    with _lock:
        _streams.setdefault(s.tenant, set()).add(s)
        return _last


def leave(s: Stream) -> None:
    with _lock:
        _streams.get(s.tenant, set()).discard(s)


def _poll() -> bool:
    """Read the newly stamped events once and hand each to the streams that may see it; True when the page was
    full. The position moves only once every stream has its share, under the lock a joining stream takes."""
    global _last
    with core.read() as db:
        rows = _envelopes(db, NEW, {"after": _last, "limit": WAKE_PAGE})
        known = facts(db, rows)
    with _lock:
        for e in rows:
            readers = _streams.get(e["tenant_id"], ())
            if takers := [s for s in readers if visible(e, s.auth.scope, known, s.auth.principal.agent_id)]:
                item, bare = (int(e["id"]), frame(e)), None
                for s in takers:
                    if (mine := shown(e, s.auth.principal.agent_id, known)) is not e:
                        bare = bare or (int(e["id"]), frame(mine))
                    s.send(item if mine is e else bare)
        _last = int(rows[-1]["id"]) if rows else _last
    for wake in _wakes if rows else ():
        wake.set()
    return len(rows) == WAKE_PAGE


def _check(ours: bool = True) -> None:
    with _lock:
        streams = [s for mine in _streams.values() for s in mine if s.token.startswith(core.OURS) == ours]
    now = time.monotonic()
    if not (found := [(s, Answer(now, None)) for s in streams] if ours else _answered(streams)):
        return
    current = _authorize([(s.token, a.principal) for s, a in found])
    for (s, a), auth in zip(found, current, strict=True):
        wider = auth and (auth.scope is None or (s.auth.scope is not None and s.auth.scope.within(auth.scope)))
        if wider and auth.principal == s.auth.principal:
            s.auth, s.checked = auth, a.asked_at
        else:
            s.end()


def _ask(s: Stream) -> Answer:
    return Answer(time.monotonic(), access.resolve(s.token, s.request))


def _answered(streams: list[Stream]) -> list[tuple[Stream, Answer]]:
    live = set(streams)
    for s in [s for s, f in _asking.items() if s not in live and f.cancel()]:
        del _asking[s]
    for s in streams:
        if s not in _asking:
            _asking[s] = _askers.submit(_ask, s)
    wait([_asking[s] for s in streams], AUTH_EVERY)
    done = [s for s, f in _asking.items() if f.done()]
    return [(s, f.result()) for s, f in ((s, _asking.pop(s)) for s in done) if s in live]


def _safe(step: Callable, *args) -> object:
    """`step(*args)`, or False once it raised: a loop's pass logs its failure and goes on."""
    try:
        return step(*args)
    except Exception:
        log.exception("events: %s%s failed", step.__name__, args[:1])
        return False


def _pump(stop: threading.Event) -> None:
    """The relay and the fan-out, on one thread."""
    while not stop.wait(0 if _safe(relay_once) | _safe(_poll) else POLL):
        pass


def _watch(stop: threading.Event, ours: bool) -> None:
    while not stop.wait(AUTH_EVERY):
        _safe(_check, ours)


def _fresh(token: str) -> bool:
    """Each stamp predates its statement, so the lease outlives `renewed` by HOOK_LEASE. A call starting no later than
    half that has time to end before another holder could take the hook. The stamp is read before the clock, so a
    freeze between the two reads only ages it."""
    return _held[token].renewed + HOOK_LEASE / 2 >= time.monotonic()


def _call(name: str, fn: Callable[[dict], None], e: dict, token: str) -> bool | None:
    for delay in (0, *HOOK_RETRY):
        time.sleep(delay)
        if not _fresh(token):
            return None
        try:
            fn(e)
        except Exception:
            log.exception("hook %s failed on event %s", name, e["id"])
        else:
            return True
    return False


@dataclass
class _Hold:
    name: str
    taken: float
    renewed: float


_held: dict[str, _Hold] = {}


@contextmanager
def _lease(name: str) -> Iterator[str]:
    token, now = str(uuid.uuid4()), time.monotonic()
    _held[token] = _Hold(name, now, now)
    try:
        yield token
    finally:
        del _held[token]


def _renew(sent: float) -> None:
    with core.read() as db:
        for token in db.execute(HOOK_RENEW, {"tokens": list(_held), "lease": HOOK_LEASE}).scalars():
            if hold := _held.get(str(token)):
                hold.renewed = sent


def _keep(stop: threading.Event) -> None:
    """Renew this process's hook leases, and warn once of each hold that outlives a lease: a call is stuck there, and
    the hook's later events wait for it."""
    now = time.monotonic()
    while not stop.wait(HOOK_LEASE / 4):
        last, now = now, time.monotonic()
        for name in [h.name for h in list(_held.values()) if last < h.taken + HOOK_LEASE <= now]:
            log.warning("hook %s has held its cursor past %s s, so its later events wait", name, HOOK_LEASE)
        _safe(_renew, now)


def hook_pass(name: str, fn: Callable[[dict], None], stop: threading.Event | None = None) -> int:
    """Deliver up to HOOK_PAGE events after the hook's cursor, in seq order, unless another holder has the hook; a
    pass ends once `stop` is set or a call could not start. An event that fails every in-place retry is parked in a
    `hook` job with its envelope, and the cursor moves on."""
    with _lease(name) as token:
        with core.read() as db:
            if (cursor := db.execute(HOOK_TAKE, {"name": name, "lease": HOOK_LEASE, "token": token}).scalar()) is None:
                return 0
            rows, failed = _envelopes(db, NEW, {"after": cursor, "limit": HOOK_PAGE}), []
        for e in rows:
            if (stop and stop.is_set()) or (delivered := _call(name, fn, e, token)) is None:
                break
            if not delivered:
                failed.append(e)
            cursor = int(e["id"])
        with core.tx() as db:
            if db.execute(HOOK_GIVE, {"name": name, "seq": cursor, "token": token}).rowcount:
                for e in failed:
                    jobs.schedule(db, "hook", {"hook": name, "envelope": e}, key=f"{name}:{e['id']}")
    LAG.labels(name).set(max(_last - cursor, 0))
    return len(rows)


def _follow(name: str, fn: Callable[[dict], None], wake: threading.Event, stop: threading.Event) -> None:
    """One hook's loop: a pass whenever the fan-out saw new events, and at least every second."""
    while not stop.is_set():
        wake.clear()
        if _safe(hook_pass, name, fn, stop) != HOOK_PAGE:
            wake.wait(1.0)


@jobs.handler("hook", backoff=RETRIES, lane="slow")
def _replay(job: Row) -> None:
    """A parked event, delivered again while this claim still owns its row and holds its hook, so never beside a
    pass or another replay. Once delivered, the row is deleted as the hook is given back."""
    name, delivered = job.payload["hook"], False
    with _lease(name) as token:
        mine = {"id": job.id, "claim": job.locked_until, "name": name, "lease": HOOK_LEASE, "token": token}
        with core.read() as db:
            if (seq := db.execute(REPLAY_TAKE, mine).scalar()) is None:
                db.execute(REPLAY_LATER, mine)
                return
        try:
            if _fresh(token):
                HOOKS[name](job.payload["envelope"])
                delivered = True
            else:
                with core.tx() as db:
                    db.execute(REPLAY_LATER, mine)
        finally:
            with core.tx() as db:
                if db.execute(HOOK_GIVE, {"name": name, "seq": seq, "token": token}).rowcount and delivered:
                    jobs.cancel(db, "hook", job.key)


jobs.every("event_retention", 86400)


@jobs.handler("event_retention", lane="slow")
def _purge(job: Row) -> None:
    """Events older than EVENT_RETENTION_DAYS that every running hook's cursor has passed: this process's, and any
    other moved or held within a lease, as an older replica's is during a rolling deploy. Webhook jobs carry their
    envelope, so a parked delivery outlives its event."""
    keep = {"days": core.settings.event_retention_days, "hooks": list(HOOKS), "lease": HOOK_LEASE}
    while True:
        with core.tx() as db:
            if not db.execute(PURGE, keep).rowcount:
                return


def start() -> Callable[[], None]:
    global _last
    with core.tx() as db:
        _last = db.execute(HEAD).scalar()
        for name in HOOKS:
            db.execute(HOOK_START, {"name": name})
    stop = threading.Event()
    _wakes[:] = [threading.Event() for _ in HOOKS]
    threads = [threading.Thread(target=f, args=(stop,), name=f.__name__, daemon=True) for f in (_pump, _keep)]
    for ours, name in ((True, "_watch hs"), (False, "_watch plugin")):
        threads.append(threading.Thread(target=_watch, args=(stop, ours), name=name, daemon=True))
    for (name, fn), wake in zip(HOOKS.items(), _wakes, strict=True):
        threads.append(threading.Thread(target=_follow, args=(name, fn, wake, stop), name=f"hook {name}", daemon=True))
    for thread in threads:
        thread.start()

    def close() -> None:
        stop.set()
        for wake in _wakes:
            wake.set()
        for thread in threads:
            thread.join(5)

    return close


# The routes and the delivery job use this module's functions, so they load once it is complete.
from hs.events import webhooks  # noqa: E402, F401
from hs.events.api import router, webhook_docs  # noqa: E402, F401
