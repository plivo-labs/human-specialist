"""Black-box harness: a real HS process, a fake Plivo, and helpers.

Acceptance tests import only from here and talk to HS only over HTTP (public API, SSE, webhooks).
`World` is session-scoped (one HS process, one simulator, one migrated database); every test isolates
itself by creating its own tenant(s), so tests never share state, and `begin_test` keeps an earlier test's
jobs out of `run_jobs`.

This file is the contract between the simulator and the acceptance suite. Change a signature only
together with every caller.

HS may branch only on what Plivo documents: hangup cause codes, event names, and the one exit cause
"Member Wait Timeout Triggered". The simulator's other exit and termination causes are invented ("sim:...").
"""

from __future__ import annotations

import contextlib
import copy
import hashlib
import importlib
import itertools
import json
import logging
import os
import secrets
import shutil
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit
from uuid import uuid4

import httpx
import psycopg2
from psycopg2 import sql

from tests._gate import gate
from tests.harness.sim import PlivoSim, SimCall, SimCallback, SimPhone, SimRequest, free_socket, serve, slow

__all__ = [
    "ALL_SCOPES",
    "SEARCH_URL",
    "Agent",
    "Delivery",
    "EventStream",
    "PlivoSim",
    "SimCall",
    "SimCallback",
    "SimPhone",
    "SimRequest",
    "Tenant",
    "WebhookSink",
    "World",
    "require_search",
]

ALL_SCOPES = ("admin", "sessions", "calls", "reports", "events", "recordings", "messages")
# The test stack's OpenSearch (docker/compose.test.yaml).
SEARCH_URL = os.environ.get("HS_TEST_SEARCH_URL", "http://127.0.0.1:59200")
# HS bounds one Plivo request at 10 s, and an API call may wait out another request's before its own.
API_TIMEOUT = 30
REPO = Path(__file__).resolve().parents[2]
log = logging.getLogger(__name__)
AGE = (
    "for _, k in ipairs(redis.call('KEYS', '*')) do local t = redis.call('PTTL', k)"
    " if t > tonumber(ARGV[1]) then redis.call('PEXPIRE', k, t - ARGV[1])"
    " elseif t >= 0 then redis.call('DEL', k) end end"
)
TIMESTAMPS = (
    "SELECT c.table_name::text, array_agg(c.column_name::text) FROM information_schema.columns c"
    " JOIN information_schema.tables t USING (table_schema, table_name) WHERE c.table_schema = 'hs'"
    " AND t.table_type = 'BASE TABLE' AND c.is_generated = 'NEVER' AND c.data_type LIKE 'timestamp%'"
    " GROUP BY c.table_name"
)
QUIET = (
    "SELECT NOT EXISTS (SELECT 1 FROM hs.jobs WHERE locked_until IS DISTINCT FROM 'infinity'"
    " AND (locked_until > now() OR (run_at <= now() AND (NOT %(held)s OR key IS DISTINCT FROM kind))))"
    " AND NOT EXISTS (SELECT 1 FROM hs.events WHERE seq IS NULL)"
)
NEXT = (
    "SELECT min(run_at) - now() FROM hs.jobs WHERE locked_until IS DISTINCT FROM 'infinity'"
    " AND key IS DISTINCT FROM kind"
)
# jobs.every keys a recurring pass's row by its kind. The shift may still update a row under FOR KEY SHARE, while
# the runner's FOR UPDATE SKIP LOCKED claim skips it.
HOLD_PASSES = "SELECT 1 FROM hs.jobs WHERE key = kind FOR KEY SHARE"
# Every job waits parked while a stop's recurring passes run, so one falling due meanwhile cannot run first: the
# runner never claims a row leased to infinity, and HS rescheduling one clears its lease. A stop finds none leased.
PARK = (
    "UPDATE hs.jobs SET locked_until = 'infinity' WHERE key IS DISTINCT FROM kind"
    " AND locked_until IS DISTINCT FROM 'infinity' RETURNING id, rev, run_at <= now()"
)
# A row HS rescheduled since has a new rev and is HS's own again, even parked by a refusal: travel never frees it.
UNPARK = "UPDATE hs.jobs SET locked_until = NULL WHERE (id, rev) IN %s AND locked_until = 'infinity'"
REDUE = (
    "UPDATE hs.jobs SET run_at = now() WHERE kind = 'route_agent' AND key = %s"
    " AND (locked_until IS NULL OR locked_until <= now())"
)
PARKED = "SELECT 1 FROM hs.jobs WHERE kind = %s AND key = %s AND locked_until = 'infinity'"
ROUTE_JOB = (
    "SELECT run_at - now(), locked_until - now(), attempts, last_error FROM hs.jobs"
    " WHERE kind = 'route_agent' AND key = %s"
)


def require_search() -> None:
    """Skip an OpenSearch variant when nothing answers at SEARCH_URL, or fail it under HS_REQUIRE_STACK=1 (CI)."""
    try:
        httpx.get(SEARCH_URL, timeout=2 * slow())
    except httpx.TransportError:
        gate([f"OpenSearch at {SEARCH_URL}"], "search index")


def _ok(r: httpx.Response) -> Any:
    if not r.is_success:
        raise AssertionError(f"{r.request.method} {r.request.url} -> {r.status_code}: {r.text}")
    return r.json() if r.content else None


def _matches(item: dict, type: str, match: dict) -> bool:
    data = item.get("data") or {}
    return item.get("type") == type and all(v in (item.get(k, ...), data.get(k, ...)) for k, v in match.items())


def _wait(cond: threading.Condition, find: Callable[[], Any], timeout: float) -> Any:
    with cond:
        return cond.wait_for(find, timeout * slow())


class World:
    """One running HS (uvicorn thread, real lifespan: job runner, relay, sweeper) + PlivoSim.

    HS env: PUBLIC_BASE_URL=base_url, PLIVO_API_URL=plivo.url, WEBHOOK_PRIVATE_HOSTS=127.0.0.1,localhost,
    a fresh copy of a database migrated with `alembic upgrade head`, REDIS_URL of a redis-server this world starts,
    and SEARCH_URL only when `search_url` is given (search reads Postgres without it; with it, HS reaches the index
    through a gate `search_down` can close, and the world deletes its tenants' indices when it ends).
    """

    base_url: str
    operator: httpx.Client  # Authorization: Bearer <HS_OPERATOR_KEY>
    plivo: PlivoSim
    cors_origin = "https://desk.example.test"  # the one origin in HS's CORS_ALLOWED_ORIGINS

    def __init__(self, search_url: str | None = None) -> None:
        self.search_url = search_url
        self._stack = contextlib.ExitStack()
        self._jobs_after = 0
        self._desks: set[Agent] = set()  # desks still pinging, which travel beats
        self._signed_in: set[Agent] = set()
        self._streams: list[EventStream] = []
        try:
            self._start()
        except BaseException:
            self._stack.close()
            raise

    def _start(self) -> None:
        admin = urlsplit(os.environ.get("HS_TEST_DATABASE_URL", "postgresql://hs:hs@127.0.0.1:56432/hs"))
        name = f"hs_accept_{uuid4().hex}"
        # HS's own 3 s connect timeout is too tight for a Docker-forwarded Postgres on a loaded machine.
        query = admin.query if "connect_timeout" in admin.query else f"{admin.query}&connect_timeout=15".lstrip("&")
        database_url = urlunsplit(admin._replace(path=f"/{name}", query=query))
        self.plivo = PlivoSim()
        self._stack.callback(self.plivo.close)
        self._redis = _Redis()
        self._stack.callback(self._redis.stop)
        sock = free_socket()
        self.base_url = f"http://127.0.0.1:{sock.getsockname()[1]}"
        self._env = {
            "DATABASE_URL": database_url,
            "PUBLIC_BASE_URL": self.base_url,
            "HS_OPERATOR_KEY": f"hs_op_{secrets.token_urlsafe(24)}",
            "HS_SECRET_KEY": secrets.token_urlsafe(32),
            "PLIVO_API_URL": self.plivo.url,
            "PLIVO_MEDIA_URL": self.plivo.media_url,
            "METRICS_PORT": str(_free_port()),
            "WEBHOOK_PRIVATE_HOSTS": "127.0.0.1,localhost",
            "CORS_ALLOWED_ORIGINS": self.cors_origin,
            "EVENT_RETENTION_DAYS": "90",
            "REDIS_URL": f"redis://127.0.0.1:{self._redis.port}/0",
            "DB_POOL_SIZE": os.environ.get("DB_POOL_SIZE", "4"),
            "DB_MAX_OVERFLOW": os.environ.get("DB_MAX_OVERFLOW", "12"),
        }
        if self.search_url:
            self._gate = _SearchGate(self.search_url)
            self._stack.callback(self._gate.close)
            self._env["SEARCH_URL"] = self._gate.url
        before = dict(os.environ)
        self._stack.callback(lambda: (os.environ.clear(), os.environ.update(before)))
        os.environ.pop("SEARCH_URL", None)
        os.environ.update(self._env)
        _copy_template(admin.geturl(), database_url)
        # Only once the CREATE succeeded: a failed one must not drop a database another world owns.
        drop = sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name))
        self._stack.callback(_admin_sql, admin.geturl(), drop)
        self._db = psycopg2.connect(database_url)
        self._db.autocommit = True
        self._stack.callback(self._db.close)
        # travel's shift gives up a lock wait first and retries, so an HS transaction waiting on its jobs lock is
        # never the deadlock victim.
        self._clock = psycopg2.connect(database_url, options="-c lock_timeout=100")
        self._stack.callback(self._clock.close)
        self._hold = psycopg2.connect(database_url)
        self._stack.callback(self._hold.close)
        if self.search_url:
            self._stack.callback(self._drop_indices)
        self._app = os.environ.get("HS_TEST_APP", "hs.app:app")
        if "hs.core" in sys.modules:
            # This process imported HS already (a contract test listing its routes), so its settings are frozen:
            # boot HS in its own process with this World's env.
            sock.close()
            self._spawn(self.base_url, dict(os.environ))
        else:
            module, _, attr = self._app.partition(":")
            app = getattr(importlib.import_module(module), attr)
            # HS must never reach real Plivo from a test.
            hs = sys.modules["hs.core"].settings
            if (hs.plivo_api_url, hs.plivo_media_url) != (self.plivo.url, self.plivo.media_url):
                raise RuntimeError(f"HS would call {hs.plivo_api_url}, not the Plivo simulator")
            self._stack.callback(serve(app, sock)[1])
        self.operator = self.client(self._env["HS_OPERATOR_KEY"])

    def _spawn(self, base_url: str, env: dict[str, str]) -> subprocess.Popen:
        """Boot HS in a process of its own with `env`, listening at `base_url`."""
        port = str(urlsplit(base_url).port)
        uvicorn = [sys.executable, "-m", "uvicorn", self._app, "--port", port, "--log-level", "warning"]
        proc = subprocess.Popen(uvicorn, cwd=REPO, env=env)
        _outlive_never(proc)
        self._stack.callback(proc.wait, 10)
        self._stack.callback(proc.terminate)
        self.wait(lambda: _up(base_url) or proc.poll() is not None, timeout=30, what="HS to boot")
        if proc.poll() is not None:
            raise RuntimeError("HS exited during boot")
        return proc

    def replica(self, redis_user: str | None = None, *, gated: bool = False) -> World:
        """Start a second HS process on this world's database and Redis, with its own port and job runner, and return
        a view of this world whose clients, tenants and agents talk to it. Plivo still calls the first. `redis_user`
        is the Redis ACL user it signs in as, one the test made with world.redis("ACL", "SETUSER", ...). A `gated`
        replica reaches Redis through a gate that `redis_held` can close."""
        view = copy.copy(self)
        view.base_url = f"http://127.0.0.1:{_free_port()}"
        env = {k: v for k, v in os.environ.items() if k != "SEARCH_URL"} | self._env
        port = self._redis.port
        if gated:
            view._redis_gate = _RedisGate(port)
            self._stack.callback(view._redis_gate.close)
            port = view._redis_gate.port
        env["REDIS_URL"] = f"redis://{f'{redis_user}:-@' if redis_user else ''}127.0.0.1:{port}/0"
        # libpq names the replica's sessions, so `kill` can wait for Postgres to end them.
        view._appname = f"hs-replica-{urlsplit(view.base_url).port}"
        env |= {"METRICS_PORT": str(_free_port()), "PGAPPNAME": view._appname}
        view._proc = self._spawn(view.base_url, env)
        view.operator = view.client(self._env["HS_OPERATOR_KEY"])
        return view

    @contextlib.contextmanager
    def redis_held(self, match: Callable[[bytes], bool]) -> Iterator[Callable[[], bool]]:
        """The first whole command this replica sends Redis inside the block that `match` accepts is held, and its
        connection closed. Yields a check that it is held. The block's end replays it on a new connection and returns
        once Redis has answered it."""
        gate = self._redis_gate
        gate.held, gate.match = None, match
        try:
            yield lambda: gate.held is not None
        finally:
            gate.match, held, gate.held = None, gate.held, None
            if held:
                with socket.create_connection(("127.0.0.1", self._redis.port)) as redis:
                    redis.sendall(held)
                    if (reply := redis.recv(65536)).startswith(b"-"):
                        raise RuntimeError(f"Redis refused the held command: {reply!r}")

    def kill(self) -> None:
        """This replica's process dies at once, as in a crash: nothing it would undo, roll back or run after a commit
        runs. Returns once Postgres has ended its sessions, and with them their transactions."""
        self._proc.kill()
        self._proc.wait(10)

        def ended() -> bool:
            with self._db.cursor() as cur:
                cur.execute("SELECT count(*) FROM pg_stat_activity WHERE application_name = %s", (self._appname,))
                return cur.fetchone()[0] == 0

        self.wait(ended, what="Postgres to end the killed replica's sessions")

    @contextlib.contextmanager
    def stall(self, table: str) -> Iterator[Callable[[], bool]]:
        """Writes to hs.<table> wait inside the block, as behind a slow transaction, each until its lock_timeout.
        Yields a check that some session waits on the stall now."""
        with contextlib.closing(psycopg2.connect(self._env["DATABASE_URL"])) as conn, conn, conn.cursor() as cur:
            cur.execute(sql.SQL("LOCK TABLE hs.{} IN SHARE MODE").format(sql.Identifier(table)))

            def waiting() -> bool:
                with self._db.cursor() as seen:
                    seen.execute(
                        "SELECT 1 FROM pg_stat_activity WHERE %s = ANY(pg_blocking_pids(pid))",
                        (conn.get_backend_pid(),),
                    )
                    return seen.fetchone() is not None

            yield waiting

    @contextlib.contextmanager
    def passes_held(self) -> Iterator[None]:
        """No recurring pass (the sweep, retention) starts inside the block, and none still runs once it begins."""
        with contextlib.closing(psycopg2.connect(self._env["DATABASE_URL"])) as conn, conn, conn.cursor() as cur:
            cur.execute(HOLD_PASSES)

            def idle() -> bool:
                with self._db.cursor() as seen:
                    seen.execute(
                        "SELECT 1 FROM hs.jobs WHERE key = kind AND locked_until > now() AND locked_until <> 'infinity'"
                    )
                    return seen.fetchone() is None

            self.wait(idle, what="a recurring pass still running to end")
            yield

    def redis(self, *args: str) -> str:
        """Send one command to this world's Redis; returns the raw reply. A refused command raises."""
        return self._redis.cli(*args)

    @contextlib.contextmanager
    def redis_down(self) -> Iterator[None]:
        """Redis is unreachable inside the block and comes back empty on the same port: a Redis loss."""
        self._redis.stop()
        try:
            yield
        finally:
            self._redis.start()

    def travel_until_rebuilt(self) -> None:
        """Travel 15 s sweep intervals, at most three, until every live tenant's epoch key is back after a Redis loss:
        a sweep Redis refuses as it returns, with HS's breaker still open, runs again the next interval."""
        for _ in range(3):
            self.travel(15)
            with self._db.cursor() as cur:
                cur.execute("SELECT id FROM hs.tenant WHERE deleted_at IS NULL")
                epochs = [f"hs:{{{t}}}:epoch" for (t,) in cur.fetchall()]
            if int(self.redis("EXISTS", *epochs)) == len(epochs):
                return
        raise AssertionError("the sweep has not rebuilt every tenant after three intervals")

    @contextlib.contextmanager
    def search_down(self) -> Iterator[None]:
        """Every search index write fails inside the block and works again after it: the index answers 503, or
        without one the search table is missing."""
        self._search_down(True)
        try:
            yield
        finally:
            self._search_down(False)

    def _search_down(self, down: bool) -> None:
        if self.search_url:
            self._gate.down = down
            return
        names = ("search", "search_down") if down else ("search_down", "search")
        with self._db.cursor() as cur:
            cur.execute(sql.SQL("ALTER TABLE hs.{} RENAME TO {}").format(*map(sql.Identifier, names)))

    def _drop_indices(self) -> None:
        """Delete every index naming one of this world's tenants, best-effort: HS creates one per tenant and never
        deletes it, and a shared search node refuses new indices past its shard limit."""
        with self._db.cursor() as cur:
            cur.execute("SELECT id::text FROM hs.tenant")
            tenants = [t for (t,) in cur.fetchall()]
        try:
            listed = httpx.get(
                f"{self.search_url}/_cat/indices", params={"format": "json", "h": "index"}, timeout=30 * slow()
            )
            mine = [i["index"] for i in listed.raise_for_status().json() if any(t in i["index"] for t in tenants)]
            # OpenSearch refuses a request line over 4 KB.
            for names in itertools.batched(mine, 50, strict=False):
                url = f"{self.search_url}/{','.join(names)}"
                httpx.delete(url, params={"ignore_unavailable": "true"}, timeout=30 * slow()).raise_for_status()
        except httpx.HTTPError:
            log.warning("this world's search indices were not deleted", exc_info=True)

    def close(self) -> None:
        self._stack.close()

    def anon(self) -> httpx.Client:
        """Client with no credentials."""
        return httpx.Client(base_url=self.base_url, timeout=API_TIMEOUT * slow())

    def client(self, token: str) -> httpx.Client:
        """Client sending `Authorization: Bearer <token>`."""
        return httpx.Client(
            base_url=self.base_url, headers={"Authorization": f"Bearer {token}"}, timeout=API_TIMEOUT * slow()
        )

    def new_tenant(self, name: str = "Acme", **fields: Any) -> Tenant:
        """Create a Plivo sub-account in the simulator, then POST /api/v1/tenants with the operator key."""
        sub = self.plivo.subaccount(name)
        body = {"name": name, "plivo_auth_id": sub.auth_id, "plivo_auth_token": sub.auth_token, **fields}
        created = _ok(self.operator.post("/api/v1/tenants", json=body))
        key = created["api_key"]
        return Tenant(created["tenant_id"], name, sub.auth_id, sub.auth_token, key, self.client(key), self)

    def webhook_sink(self) -> WebhookSink:
        """A local HTTP receiver for webhook deliveries."""
        sink = WebhookSink()
        self._stack.callback(sink.close)
        return sink

    def begin_test(self) -> None:
        """From now on run_jobs and advance see only jobs created after this call, so a test never runs or waits on
        an earlier test's timers. An earlier test's desks stop pinging."""
        with self._db.cursor() as cur:
            cur.execute("SELECT coalesce(max(id), 0) FROM jobs")
            self._jobs_after = cur.fetchone()[0]
        self._desks.clear()

    def end_test(self) -> None:
        """Sign the test's agents out and close its streams: a signed-in agent stays a desk the sweep reads for its
        hour-long session, and a Redis loss would rebuild every earlier test's."""
        agents, streams, self._signed_in, self._streams = self._signed_in, self._streams, set(), []
        for stream in streams:
            stream.close()
        for agent in agents:
            # A test that removed the agent, its tenant or the tenant's key signed it out already.
            if (r := agent.tenant.api.delete(f"/api/v1/agents/{agent.id}/sessions")).status_code not in (204, 401, 404):
                _ok(r)

    def parked(self, kind: str, key: str) -> bool:
        """The job of this kind and key gave up after its last retry and waits for a sweep."""
        with self._db.cursor() as cur:
            cur.execute(PARKED, (kind, key))
            return cur.fetchone() is not None

    def run_jobs(self, kind: str | None = None) -> int:
        """Make this test's pending jobs (optionally of one kind) due now, wait until the runner has processed
        them; return how many ran. Used instead of waiting for real timers. A recurring pass, whose row is older than
        the test, runs only when its kind is named."""
        return self._run_due(kind, None)

    def advance(self, seconds: float) -> int:
        """Run this test's jobs due within `seconds` from now (as run_jobs does) and return how many ran; nothing
        else moves: no clock is shifted and no other timer fires."""
        return self._run_due(None, seconds)

    def travel(self, seconds: float) -> None:
        """`seconds` pass for what HS stores, in time order. Travel stops at each pending job's run_at inside the jump
        in turn, and at its end: every finite timestamp in schema hs and every Redis TTL move back by the time to it (a
        key whose TTL runs out is deleted). Each stop does what that moment would have seen: each desk still pinging
        beats, the recurring passes due by then run (the presence sweep, retention), and then the jobs due there, each
        until the job runner and the simulator are idle; a job falling due meanwhile runs only after them. A recurring
        pass is no stop of its own, or a 15 s sweep would stop the jump every 15 s. HS's Python clock and the
        simulator's do not move, so time held only in memory stands still. A desk whose agent token expired in the jump
        stops beating. The time travel itself takes counts toward `seconds`, so a slow stop never stretches the jump.

        The job runner claims nothing while a stop moves time. Redis ages just before Postgres commits, so travel
        assumes no other actor touches HS meanwhile; the harness has no background desk pinger."""
        if seconds < 0:
            raise ValueError(f"travel moves time forward only, not by {seconds} s")
        until = time.monotonic() + seconds
        self._settle()
        while time.monotonic() < until:
            parked: list[list[tuple[int, int]]] = []
            try:
                with self._hold, self._hold.cursor() as hold:
                    hold.execute(HOLD_PASSES)
                    step, parked = self._stop(until)
                    until -= step.total_seconds()
                    for desk in list(self._desks):
                        if (beat := desk.api().post("/api/v1/me/heartbeat")).status_code == 401:
                            self._desks.discard(desk)
                        else:
                            _ok(beat)
                self._settle()
                while parked:
                    self._unpark(parked[0])
                    del parked[0]
                    self._settle()
            finally:
                for rows in parked:
                    self._unpark(rows)

    def _unpark(self, rows: list[tuple[int, int]]) -> None:
        if rows:
            with self._db.cursor() as cur:
                cur.execute(UNPARK, (tuple(rows),))

    def _stop(self, until: float) -> tuple[timedelta, list[list[tuple[int, int]]]]:
        """Move every stored time back to the next job's run_at, or by what is left until monotonic time `until` if
        none falls due sooner, and park every job, in one transaction whose SHARE lock keeps the job runner from
        claiming; returns how far, and the parked (id, rev) pairs to let go in turn: those due there, then the rest.
        Its lock waits time out and retry, so an HS transaction waiting on it never loses a deadlock to it, up to 20
        tries or 30 s."""
        deadline, blocked = time.monotonic() + 30 * slow(), "the job runner or the simulator, still busy"
        for _ in range(20):
            table, aged = "jobs", False
            try:
                with self._clock, self._clock.cursor() as cur:
                    cur.execute("LOCK TABLE hs.jobs IN SHARE MODE")
                    if (left := timedelta(seconds=until - time.monotonic())) <= timedelta(0):
                        return timedelta(0), []
                    if self._quiet(cur, held=True):
                        cur.execute(NEXT)
                        step = left if (due := cur.fetchone()[0]) is None else min(left, due)
                        cur.execute(TIMESTAMPS)
                        for table, columns in cur.fetchall():
                            cur.execute(_shift(table, columns), {"gap": step})
                        cur.execute(PARK)
                        parked = cur.fetchall()
                        # Last before the commit, so a retry never ages Redis twice.
                        if self._redis.up():
                            self.redis("EVAL", AGE, "0", str(round(step.total_seconds() * 1000)))
                        aged = True
                        return step, [[(i, r) for i, r, ready in parked if ready == due] for due in (True, False)]
            except psycopg2.Error as e:
                if aged:
                    raise RuntimeError(f"travel aged Redis by {step} but Postgres did not commit the shift") from e
                if not isinstance(e, psycopg2.errors.LockNotAvailable):
                    raise
                blocked = f"hs.{table}, {(e.diag.context or e.diag.message_primary or '').strip()}"
            self._settle(held=True, deadline=deadline)
            if time.monotonic() > deadline:
                break
        raise RuntimeError(f"travel could not move time in 20 tries or 30 s: blocked by {blocked}")

    def _quiet(self, cur: Any, held: bool = False) -> bool:
        """Nothing due or running, every event relayed (the events API shows only those) and the simulator idle;
        `held`: the recurring passes, held due, do not count."""
        cur.execute(QUIET, {"held": held})
        return self.plivo.idle() and cur.fetchone()[0]

    def _settle(self, held: bool = False, deadline: float = float("inf")) -> None:
        def quiet() -> bool:
            with self._db.cursor() as cur:
                return self._quiet(cur, held) or time.monotonic() > deadline

        self.wait(quiet, timeout=30, what="the simulator and the job runner to go idle")

    def _run_due(self, kind: str | None, within: float | None) -> int:
        """Drain first: the simulator idle (no engine work, no callback queued or in flight), then no matching
        job held by the runner. Only then make the rows due, and wait for each to finish: its row gone, or claimed
        or rescheduled since and its lease released (backed off, parked or due again)."""
        self.wait(self.plivo.idle, timeout=10, what="the Plivo simulator to go idle")
        args = {"kind": kind, "within": within, "after": self._jobs_after}
        match = (
            " (id > %(after)s OR key = %(kind)s) AND (%(kind)s::text IS NULL OR kind = %(kind)s)"
            " AND (%(within)s::float8 IS NULL OR run_at <= now() + %(within)s * interval '1 second')"
            # A flush must not fire the 24 h call-session backstop: advance() or its kind runs it.
            " AND (%(kind)s::text IS NOT NULL OR %(within)s::float8 IS NOT NULL OR kind <> 'call_session')"
        )

        def unlocked() -> bool:
            with self._db.cursor() as cur:
                cur.execute(
                    "SELECT count(*) FROM hs.jobs WHERE" + match + " AND locked_until > now()"
                    " AND locked_until <> 'infinity'",
                    args,
                )
                return cur.fetchone()[0] == 0

        # A webhook delivery holds its job for up to HS's 10 s delivery deadline.
        self.wait(unlocked, timeout=15, what="the job runner to release the jobs it holds")
        with self._db.cursor() as cur:
            cur.execute(
                "UPDATE hs.jobs SET run_at = now() WHERE" + match + " AND locked_until IS DISTINCT FROM 'infinity'"
                " RETURNING id, attempts, rev, CASE WHEN key = kind THEN run_at END",
                args,
            )
            due = {i: (attempts, rev, at) for i, attempts, rev, at in cur.fetchall()}

        def ran(i: int, attempts: int, rev: int, at: Any) -> bool:
            """Claimed or rescheduled since; a recurring pass that ran resets its attempts and moves its run_at on."""
            before, before_rev, due_at = due[i]
            return attempts > before or rev > before_rev or (due_at is not None and at > due_at)

        def finished() -> bool:
            with self._db.cursor() as cur:
                cur.execute(
                    "SELECT id, attempts, rev, run_at, locked_until IS NULL OR locked_until <= now()"
                    " OR locked_until = 'infinity' FROM hs.jobs WHERE id = ANY(%s)",
                    (list(due),),
                )
                return all(free and ran(*row) for *row, free in cur.fetchall())

        self.wait(finished, timeout=10, what=f"the job runner to finish {len(due)} {kind or 'due'} job(s)")
        return len(due)

    def wait(self, predicate: Callable[[], Any], timeout: float = 5.0, what: str = "") -> Any:
        """Poll `predicate` until truthy; return its value or raise AssertionError(what)."""
        deadline = time.monotonic() + timeout * slow()
        while not (value := predicate()):
            if time.monotonic() > deadline:
                raise AssertionError(what or "condition not met")
            time.sleep(0.02)
        return value


def _up(base_url: str) -> bool:
    try:
        httpx.get(f"{base_url}/status", timeout=1)
    except httpx.TransportError:
        return False
    return True


def _free_port() -> int:
    sock = free_socket()
    port = sock.getsockname()[1]
    sock.close()
    return port


def _shift(table: str, columns: list[str]) -> sql.Composed:
    """An UPDATE moving each of the table's timestamp columns back by %(gap)s; NULL and infinity stay put."""
    moved = sql.SQL(", ").join(sql.SQL("{0} = {0} - %(gap)s").format(sql.Identifier(c)) for c in columns)
    return sql.SQL("UPDATE hs.{} SET {}").format(sql.Identifier(table), moved)


WATCH = """import os, signal, sys, time
boss, pid = int(sys.argv[1]), int(sys.argv[2])
try:
    while os.getppid() == boss:
        os.kill(pid, 0)
        time.sleep(0.5)
    os.kill(pid, signal.SIGTERM)
    for _ in range(50):
        os.kill(pid, 0)
        time.sleep(0.1)
    os.kill(pid, signal.SIGKILL)
except OSError:
    pass
"""


def _outlive_never(proc: subprocess.Popen) -> None:
    """`proc` ends once this process dies, even by kill -9: a watcher of its own (macOS has no PR_SET_PDEATHSIG) polls
    for the parent and then ends the child, so a killed test run leaves no HS or redis-server holding connections."""
    subprocess.Popen(
        [sys.executable, "-c", WATCH, str(os.getpid()), str(proc.pid)], start_new_session=True, stdin=subprocess.DEVNULL
    )


class _Redis:
    """A redis-server of one world's own, keeping nothing on disk, so a restart comes back empty."""

    def __init__(self, port: int | None = None) -> None:
        if not (shutil.which("redis-server") and shutil.which("redis-cli")):
            raise RuntimeError(
                "redis-server and redis-cli must be on PATH: brew install redis, or apt-get install redis-server"
            )
        # A port nobody gave us may be taken between picking and binding: pick again. A given port raises.
        for _ in range(1 if port else 3):
            self.port = port or _free_port()
            try:
                self.start()
            except RuntimeError as e:
                failed = e
            else:
                return
        raise failed

    def start(self) -> None:
        argv = ["redis-server", "--port", str(self.port), "--bind", "127.0.0.1", "--save", "", "--appendonly", "no"]
        self._proc = subprocess.Popen(argv, stdout=subprocess.DEVNULL)
        _outlive_never(self._proc)
        deadline = time.monotonic() + 10 * slow()
        while self._proc.poll() is None and time.monotonic() < deadline:
            with contextlib.suppress(subprocess.CalledProcessError):
                # Another server on a reused port answers PING too.
                if f"process_id:{self._proc.pid}" in self.cli("INFO", "server").split():
                    return
            time.sleep(0.02)
        self._proc.kill()
        raise RuntimeError(f"redis-server did not start on port {self.port}")

    def up(self) -> bool:
        return self._proc.poll() is None

    def stop(self) -> None:
        self._proc.terminate()
        self._proc.wait(10)

    def cli(self, *args: str) -> str:
        argv = ["redis-cli", "-e", "-p", str(self.port), *args]
        return subprocess.run(argv, capture_output=True, text=True, check=True).stdout.strip()


def _copy_template(admin: str, database_url: str) -> None:
    """Create `database_url`'s database from a template migrated with `alembic upgrade head`: one per Postgres and
    content of alembic/ (whose migrations import nothing from src), renamed into place only once fully migrated."""
    # A template of an older alembic/ is never dropped; prune them here if they pile up.
    digest = hashlib.sha256(b"".join(p.read_bytes() for p in sorted((REPO / "alembic").rglob("*.py"))))
    template, url = f"hs_accept_tpl_{digest.hexdigest()[:16]}", urlsplit(database_url)
    building = f"{template}_new"
    conn = psycopg2.connect(admin)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(hashtext(%s))", (template,))
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (template,))
            if not cur.fetchone():
                cur.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(building)))
                cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(building)))
                # A subprocess, so alembic's env.py reads this env rather than settings an earlier import cached.
                migrate = [sys.executable, "-m", "alembic", "-c", str(REPO / "alembic.ini"), "upgrade", "head"]
                env = {**os.environ, "DATABASE_URL": urlunsplit(url._replace(path=f"/{building}"))}
                done = subprocess.run(migrate, cwd=REPO, env=env, capture_output=True, text=True, check=False)
                if done.returncode:
                    raise RuntimeError(f"alembic upgrade head failed:\n{done.stderr}")
                rename = sql.SQL("ALTER DATABASE {} RENAME TO {}")
                cur.execute(rename.format(sql.Identifier(building), sql.Identifier(template)))
            create = sql.SQL("CREATE DATABASE {} TEMPLATE {}")
            cur.execute(create.format(sql.Identifier(url.path.lstrip("/")), sql.Identifier(template)))
    finally:
        conn.close()  # also releases the advisory lock


def _admin_sql(url: str, statement: sql.Composable) -> None:
    conn = psycopg2.connect(url)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(statement)
    finally:
        conn.close()


@dataclass(eq=False)
class Tenant:
    id: str
    name: str
    auth_id: str  # its Plivo sub-account in the simulator
    auth_token: str = field(repr=False)
    key: str = field(repr=False)  # tenant API key with ALL_SCOPES (from registration)
    api: httpx.Client = field(repr=False)  # client using `key`
    world: World = field(repr=False)

    def key_with(self, *scopes: str) -> httpx.Client:
        """Mint a tenant key with only these scopes (operator API) and return a client for it."""
        minted = _ok(self.world.operator.post(f"/api/v1/tenants/{self.id}/api-keys", json={"scopes": list(scopes)}))
        return self.world.client(minted["api_key"])

    def number(self, prefix: str = "1415") -> str:
        """Buy a fresh E.164 number on this sub-account in the simulator (not yet routed in HS)."""
        return self.world.plivo.buy_number(self.auth_id, prefix)

    def agent(self, name: str = "Agent", role: str = "agent", **fields: Any) -> Agent:
        """POST /api/v1/agents; the simulator registers a softphone on the new agent's endpoint."""
        created = _ok(self.api.post("/api/v1/agents", json={"name": name, "role": role, **fields}))
        endpoint = self.world.plivo.endpoint(self.auth_id, created.get("endpoint_id"))
        username = endpoint["username"]
        return Agent(created["id"], self, username, self.world.plivo.phone(username))

    def status_id(self, name: str) -> str:
        """Id of a presence status by name (GET /api/v1/agent-statuses)."""
        statuses = _ok(self.api.get("/api/v1/agent-statuses", params={"limit": 200}))["data"]
        return next(s["id"] for s in statuses if s["name"] == name)

    def queue(self, **fields: Any) -> dict:
        """POST /api/v1/queues; returns the created queue."""
        return _ok(self.api.post("/api/v1/queues", json={"name": f"Queue {secrets.token_hex(3)}", **fields}))

    def route_number(self, number: str, open_action: dict, **fields: Any) -> dict:
        """PUT /api/v1/numbers/{number}."""
        return _ok(self.api.put(f"/api/v1/numbers/{quote(number)}", json={"open_action": open_action, **fields}))

    def inbound_line(self, *agents: Agent, **queue_fields: Any) -> tuple[str, dict]:
        """Shortcut: a number whose open action is a queue holding `agents`. Returns (number, queue)."""
        number, queue = self.number(), self.queue(agents=[a.id for a in agents], **queue_fields)
        self.route_number(number, {"type": "queue", "queue_id": queue["id"]})
        return number, queue

    def events(self, after: int = 0, **params: Any) -> list[dict]:
        """GET /api/v1/events?after=, following next_cursor to the end."""
        out, cursor = [], after
        while True:
            page = _ok(self.api.get("/api/v1/events", params={"after": cursor, "limit": 200, **params}))
            out += page["data"]
            if not page.get("next_cursor"):
                return out
            cursor = page["next_cursor"]

    def stream(self, token: str | None = None, after: int | None = None, query: bool = False) -> EventStream:
        """Open GET /api/v1/events/stream (tenant key unless `token`; `query` sends it as ?access_token=, as
        EventSource must) in a background reader."""
        stream = EventStream(f"{self.world.base_url}/api/v1/events/stream", token or self.key, after, query)
        self.world._stack.callback(stream.close)
        self.world._streams.append(stream)
        return stream


@dataclass(eq=False)
class Agent:
    id: str
    tenant: Tenant = field(repr=False)
    endpoint_username: str
    phone: SimPhone = field(repr=False)
    agent_token: str | None = field(default=None, repr=False)
    stream_token: str | None = field(default=None, repr=False)
    _api: httpx.Client | None = field(default=None, repr=False)

    def login(self, available: bool = True) -> Agent:
        """POST /agents/{id}/sessions, then the Browser SDK login with its Plivo token: once that succeeds the browser
        is the agent's only device, so the SIP registration goes. With `available`, send heartbeats until the agent is
        Available."""
        session = _ok(self.tenant.api.post(f"/api/v1/agents/{self.id}/sessions"))
        self.agent_token, self.stream_token = session["agent_token"], session["stream_token"]
        if self.phone.login(session["plivo"]["access_token"]):
            self.phone.registered = False
        self._api = self.tenant.world.client(self.agent_token)
        self.tenant.world._signed_in.add(self)
        if available:
            what = f"agent {self.id} to become Available"
            self.tenant.world.wait(lambda: self.heartbeat() or self._status() == "Available", what=what)
            self._routed()
        return self

    def api(self) -> httpx.Client:
        """Client with this agent's token (after login)."""
        return self._api

    def heartbeat(self) -> None:
        """Beat once; from now on this desk is pinging, so world.travel beats it too."""
        _ok(self.api().post("/api/v1/me/heartbeat"))
        self.tenant.world._desks.add(self)

    @contextlib.contextmanager
    def presence_cache_fault(self) -> Iterator[None]:
        """HS's presence store is down inside the block, and comes back empty."""
        with self.tenant.world.redis_down():
            yield

    def set_status(self, name: str) -> None:
        """PUT /me/status by status name."""
        _ok(self.api().put("/api/v1/me/status", json={"status_id": self.tenant.status_id(name)}))
        self._routed()

    def _routed(self) -> None:
        """Wait out the route_agent job a status change schedules: left to the job poll under load, it would offer
        this agent work a test hands out afterwards. A failed try backs off longer than the wait, so it is made due
        again."""

        job: list = []

        def done() -> bool:
            with self.tenant.world._db.cursor() as cur:
                cur.execute(ROUTE_JOB, (self.id,))
                job[:] = cur.fetchall()
                for due_in, _, attempts, error in job:
                    if attempts and due_in > timedelta(0):
                        log.warning("agent %s's route_agent job failed (%s); made due again", self.id, error)
                        cur.execute(REDUE, (self.id,))
            return not job

        try:
            self.tenant.world.wait(done, what=f"agent {self.id}'s route_agent job to run")
        except AssertionError as e:
            raise AssertionError(f"{e}; its row (due in, leased for, attempts, last error): {job}") from None

    def expire_tokens(self) -> None:
        """Expire this agent's agent and stream tokens now, as if their TTL ran out: their hs.credential rows,
        found by SHA-256, expire. HS's SSE loop must re-check the stream's token at least every 2 s to send
        auth.expired."""
        digests = [hashlib.sha256(token.encode()).hexdigest() for token in (self.agent_token, self.stream_token)]
        with self.tenant.world._db.cursor() as cur:
            cur.execute("UPDATE hs.credential SET expires_at = now() WHERE sha256 = ANY(%s)", (digests,))
            assert cur.rowcount == 2, "HS must keep each token's SHA-256 in hs.credential"

    def stop_pinging(self) -> None:
        """This desk stops beating and 55 s pass: HS's 40 s limit and one 15 s sweep. A stop in the jump may run the
        sweep before the 40 s are up, so only then has a sweep surely seen the desk silent. Returns once the API
        shows an Available agent Offline (any other status stays) and the disconnect has cleared the session's
        beat count in Redis, which only a handled disconnect does."""
        world, beats = self.tenant.world, f"hs:{{{self.tenant.id}}}:beats:{self.id}"
        world._desks.discard(self)
        was = self._status()
        want = "Offline" if was == "Available" else was
        assert world.redis("EXISTS", beats) == "1", f"desk {self.id} must have beaten before it stops pinging"
        world.travel(55)
        world.wait(
            lambda: self._status() == want and world.redis("EXISTS", beats) == "0",
            what=f"agent {self.id} to go {want} with its beat count cleared",
        )

    def _status(self) -> str:
        return _ok(self.tenant.api.get(f"/api/v1/agents/{self.id}/status"))["status"]["name"]


class EventStream:
    def __init__(self, url: str, token: str, after: int | None = None, query: bool = False):
        self.events: list[dict] = []  # parsed `data:` envelopes, in arrival order; each has `id`, `type`, ...
        self.status: int | None = None  # HTTP status of the stream response (0: connection failed)
        self._cond, self._closed, self._ended = threading.Condition(), False, threading.Event()
        self._client = httpx.Client(timeout=httpx.Timeout(10, read=None))
        params = {} if after is None else {"after": after}
        headers = {} if query else {"Authorization": f"Bearer {token}"}
        if query:
            params["access_token"] = token
        threading.Thread(target=self._read, args=(url, headers, params), daemon=True).start()
        if not _wait(self._cond, lambda: self.status is not None, 10):
            raise AssertionError(f"{url} did not answer")

    def _read(self, url: str, headers: dict, params: dict) -> None:
        try:
            with self._client.stream("GET", url, params=params, headers=headers) as r:
                self._set_status(r.status_code)
                data: list[str] = []
                for line in r.iter_lines():
                    if line.startswith("data:"):
                        data.append(line[5:].removeprefix(" "))
                    elif not line and data:
                        event, data = json.loads("\n".join(data)), []
                        with self._cond:
                            self.events.append(event)
                            self._cond.notify_all()
        except Exception:
            if not self._closed:
                log.exception("event stream reader failed")
        finally:
            self._set_status(self.status or 0)
            self._ended.set()

    def _set_status(self, status: int) -> None:
        with self._cond:
            self.status = status
            self._cond.notify_all()

    def wait_for(self, type: str, timeout: float = 5.0, **match: Any) -> dict:
        """First event of `type` whose top-level or `data` fields equal `match`."""
        event = _wait(self._cond, lambda: next((e for e in self.events if _matches(e, type, match)), None), timeout)
        if event is None:
            raise AssertionError(f"no {type} event matching {match}; got {[e.get('type') for e in self.events]}")
        return event

    def wait_closed(self, timeout: float = 5.0) -> None:
        """Return once the server has ended the stream."""
        if not self._ended.wait(timeout * slow()):
            raise AssertionError(f"the event stream is still open after {timeout}s")

    def close(self) -> None:
        self._closed = True
        self._client.close()


@dataclass
class Delivery:
    headers: dict[str, str]
    body: bytes
    json: dict


class _RedisGate:
    """Passes each connection on to Redis at `port`, one whole command at a time, keeping as `held` the first command
    `match` accepts and closing the connection that sent it."""

    def __init__(self, port: int) -> None:
        self.match: Callable[[bytes], bool] | None = None
        self.held: bytes | None = None
        self._lock, self._closed = threading.Lock(), False
        self._threads: list[threading.Thread] = []
        self._ends: set[socket.socket] = set()
        self._server = socket.create_server(("127.0.0.1", 0))
        self.port = self._server.getsockname()[1]
        self._start(self._serve, port)

    def _start(self, target: Callable, *args: Any, ends: tuple = ()) -> bool:
        """Runs `target` on a thread `close` joins, with `ends` the sockets `close` shuts; False once closing."""
        with self._lock:
            if self._closed:
                return False
            self._ends.update(ends)
            thread = threading.Thread(target=target, args=args, daemon=True)
            self._threads.append(thread)
            thread.start()
            return True

    def _serve(self, port: int) -> None:
        with contextlib.suppress(OSError):
            while True:
                down = self._server.accept()[0]
                if not self._start(self._link, down, port):
                    down.close()
                    return

    def _link(self, down: socket.socket, port: int) -> None:
        """One connection: a Redis that refuses it (as while it is down) or a side that closes ends this one only."""
        with contextlib.suppress(OSError), down, socket.create_connection(("127.0.0.1", port)) as up:
            if not self._start(self._pipe, up, down, ends=(down, up)):
                return
            try:
                rest = b""
                while data := down.recv(65536):
                    rest += data
                    while end := _command_end(rest):
                        command, rest = rest[:end], rest[end:]
                        if self._claims(command):
                            return
                        up.sendall(command)
            finally:
                with self._lock:
                    self._ends -= {down, up}
                for end in (down, up):  # wakes the other direction's recv, which a close alone may not
                    with contextlib.suppress(OSError):
                        end.shutdown(socket.SHUT_RDWR)

    def _pipe(self, src: socket.socket, dst: socket.socket) -> None:
        with contextlib.suppress(OSError):
            while data := src.recv(65536):
                dst.sendall(data)
            dst.shutdown(socket.SHUT_RDWR)

    def _claims(self, command: bytes) -> bool:
        with self._lock:
            if hit := bool(self.match and self.match(command)):
                self.match, self.held = None, command
            return hit

    def close(self) -> None:
        """Ends every connection and thread of the gate; raises if a thread outlives 5 s. Once closing, no thread
        starts, so the threads it joins are all there are."""
        with self._lock:
            self._closed, ends = True, list(self._ends)
        with contextlib.suppress(OSError):
            socket.create_connection(("127.0.0.1", self.port)).close()  # wakes the listener's accept
        self._server.close()
        for end in ends:
            with contextlib.suppress(OSError):
                end.shutdown(socket.SHUT_RDWR)
        for thread in self._threads:
            thread.join(5)
        if alive := [t for t in self._threads if t.is_alive()]:
            raise RuntimeError(f"{len(alive)} Redis gate threads outlived its close")


def _command_end(buf: bytes) -> int:
    """Where the first whole RESP command in `buf` ends, an array of bulk strings as clients send them; 0 if none."""
    at = buf.find(b"\r\n") + 2
    if at < 2:
        return 0
    for _ in range(int(buf[1 : at - 2])):
        if (eol := buf.find(b"\r\n", at)) < 0:
            return 0
        at = eol + 2 + int(buf[at + 1 : eol]) + 2
    return at if at <= len(buf) else 0


class _SearchGate:
    """Passes HS's search index requests on to `upstream`; while `down`, it answers each one 503 instead."""

    def __init__(self, upstream: str) -> None:
        self.down, client = False, httpx.Client(base_url=upstream, timeout=30)
        gate = self

        class Handler(BaseHTTPRequestHandler):
            def forward(self) -> None:
                body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                status, content = 503, b""
                if not gate.down:
                    kind = {"Content-Type": self.headers.get("Content-Type", "application/json")}
                    r = client.request(self.command, self.path, content=body, headers=kind)
                    status, content = r.status_code, r.content
                self.send_response(status)
                for name, value in (("Content-Type", "application/json"), ("Content-Length", str(len(content)))):
                    self.send_header(name, value)
                self.end_headers()
                self.wfile.write(content)

            do_GET = do_PUT = do_POST = do_DELETE = forward

            def log_message(self, *args: Any) -> None:
                pass

        self._client, self._server = client, ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self._server.serve_forever, args=(0.05,), daemon=True).start()
        self.url = f"http://127.0.0.1:{self._server.server_port}"

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._client.close()


class WebhookSink:
    def __init__(self) -> None:
        self.deliveries: list[Delivery] = []
        self._status, self._headers, self._cond = 200, {}, threading.Condition()
        self._stall, self._released = False, threading.Event()
        sink = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                status, headers = sink._status, sink._headers  # read before a waiter can change them
                body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                try:
                    parsed = json.loads(body)
                except ValueError:
                    parsed = {}
                with sink._cond:
                    sink.deliveries.append(Delivery(dict(self.headers), body, parsed))
                    sink._cond.notify_all()
                    stalled, sink._stall = sink._stall, False
                if stalled:
                    sink._released.wait(30)
                self.send_response(status)
                for name, value in {**headers, "Content-Length": "0"}.items():
                    self.send_header(name, value)
                self.end_headers()

            def log_message(self, *args: Any) -> None:
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self._server.serve_forever, args=(0.05,), daemon=True).start()
        self.url = f"http://127.0.0.1:{self._server.server_port}/hook"

    def respond_with(self, status: int) -> None:
        """Status code for following deliveries (default 200)."""
        self._status, self._headers = status, {}

    def stall_next(self) -> None:
        """The next delivery is recorded but never answered, so the sender's deadline decides; later ones answer."""
        self._stall = True

    def redirect_to(self, url: str) -> None:
        """Following deliveries get `302 Location: url` (and are still logged)."""
        self._status, self._headers = 302, {"Location": url}

    def wait_for(self, type: str, timeout: float = 5.0, **match: Any) -> Delivery:
        delivery = _wait(
            self._cond, lambda: next((d for d in self.deliveries if _matches(d.json, type, match)), None), timeout
        )
        if delivery is None:
            raise AssertionError(
                f"no {type} delivery matching {match}; got {[d.json.get('type') for d in self.deliveries]}"
            )
        return delivery

    def close(self) -> None:
        self._released.set()
        self._server.shutdown()
        self._server.server_close()
