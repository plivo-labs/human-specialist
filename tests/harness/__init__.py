"""Black-box harness: a real HS process, a fake Plivo, and helpers.

Acceptance tests import only from here and talk to HS only over HTTP (public API, SSE, webhooks).
`World` is session-scoped (one HS process, one simulator, one migrated database); every test isolates
itself by creating its own tenant(s), so tests never share state.

This file is the contract between the simulator and the acceptance suite. Change a signature only
together with every caller.

HS may branch only on what Plivo documents: hangup cause codes, event names, and the one exit cause
"Member Wait Timeout Triggered". The simulator's other exit and termination causes are invented ("sim:...").
"""

from __future__ import annotations

import contextlib
import hashlib
import importlib
import json
import logging
import os
import secrets
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit

import httpx
import psycopg2
import redis
from psycopg2 import sql

from tests.harness.sim import SLOW, PlivoSim, SimCall, SimCallback, SimPhone, SimRequest, free_socket, serve

__all__ = [
    "ALL_SCOPES",
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
]

ALL_SCOPES = ("admin", "sessions", "calls", "reports", "events", "recordings")
REPO = Path(__file__).resolve().parents[2]
log = logging.getLogger(__name__)


def _ok(r: httpx.Response) -> Any:
    if not r.is_success:
        raise AssertionError(f"{r.request.method} {r.request.url} -> {r.status_code}: {r.text}")
    return r.json() if r.content else None


def _matches(item: dict, type: str, match: dict) -> bool:
    data = item.get("data") or {}
    return item.get("type") == type and all(v in (item.get(k, ...), data.get(k, ...)) for k, v in match.items())


def _wait(cond: threading.Condition, find: Callable[[], Any], timeout: float) -> Any:
    with cond:
        return cond.wait_for(find, timeout * SLOW)


class World:
    """One running HS (uvicorn thread, real lifespan: job runner, relay, sweeper) + PlivoSim.

    HS env: PUBLIC_BASE_URL=base_url, PLIVO_API_URL=plivo.url, WEBHOOK_PRIVATE_HOSTS=127.0.0.1,localhost,
    a fresh database migrated with `alembic upgrade head`, a dedicated Redis db.
    """

    base_url: str
    operator: httpx.Client  # Authorization: Bearer <HS_OPERATOR_KEY>
    plivo: PlivoSim
    cors_origin = "https://desk.example.test"  # the one origin in HS's CORS_ALLOWED_ORIGINS

    def __init__(self) -> None:
        self._stack = contextlib.ExitStack()
        try:
            self._start()
        except BaseException:
            self._stack.close()
            raise

    def _start(self) -> None:
        admin = urlsplit(os.environ.get("HS_TEST_DATABASE_URL", "postgresql://hs:hs@127.0.0.1:56432/hs"))
        name = f"hs_accept_{secrets.token_hex(4)}"
        _admin_sql(admin.geturl(), sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        drop = sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name))
        self._stack.callback(_admin_sql, admin.geturl(), drop)
        database_url = urlunsplit(admin._replace(path=f"/{name}"))
        redis_db = os.environ.get("HS_TEST_REDIS_DB", "10")
        cache = urlsplit(os.environ.get("HS_TEST_REDIS_URL", "redis://127.0.0.1:57379"))
        redis_url = urlunsplit(cache._replace(path=f"/{redis_db}"))
        self.redis = redis.Redis.from_url(redis_url)
        self.redis.flushdb()
        self.plivo = PlivoSim()
        self._stack.callback(self.plivo.close)
        sock = free_socket()
        self.base_url = f"http://127.0.0.1:{sock.getsockname()[1]}"
        operator_key = f"hs_op_{secrets.token_urlsafe(24)}"
        os.environ.update(
            DATABASE_URL=database_url,
            REDIS_URL=redis_url,
            PUBLIC_BASE_URL=self.base_url,
            HS_OPERATOR_KEY=operator_key,
            HS_SECRET_KEY=secrets.token_urlsafe(32),
            PLIVO_API_URL=self.plivo.url,
            PLIVO_MEDIA_URL=self.plivo.media_url,
            METRICS_PORT=str(_free_port()),
            WEBHOOK_PRIVATE_HOSTS="127.0.0.1,localhost",
            CORS_ALLOWED_ORIGINS=self.cors_origin,
            EVENT_RETENTION_DAYS="90",
            WEBHOOK_DEADLINE="3",
        )
        # A subprocess, so alembic's env.py reads this env rather than settings an earlier import cached.
        migrate = [sys.executable, "-m", "alembic", "-c", str(REPO / "alembic.ini"), "upgrade", "head"]
        done = subprocess.run(migrate, cwd=REPO, capture_output=True, text=True, check=False)
        if done.returncode:
            raise RuntimeError(f"alembic upgrade head failed:\n{done.stderr}")
        self._db = psycopg2.connect(database_url)
        self._db.autocommit = True
        self._stack.callback(self._db.close)
        module, _, attr = os.environ.get("HS_TEST_APP", "hs.app:app").partition(":")
        if "hs.infra.settings" in sys.modules:
            # This process imported HS already (a contract test listing its routes), so its settings are frozen:
            # boot HS in its own process with this World's env.
            port = str(sock.getsockname()[1])
            sock.close()
            uvicorn = [sys.executable, "-m", "uvicorn", f"{module}:{attr}", "--port", port, "--log-level", "warning"]
            proc = subprocess.Popen(uvicorn, cwd=REPO, env=dict(os.environ))
            self._stack.callback(proc.wait, 10)
            self._stack.callback(proc.terminate)
            self.wait(lambda: _up(self.base_url) or proc.poll() is not None, timeout=30, what="HS to boot")
            if proc.poll() is not None:
                raise RuntimeError("HS exited during boot")
        else:
            app = getattr(importlib.import_module(module), attr)
            # HS must never reach real Plivo from a test.
            hs = sys.modules["hs.infra.settings"].settings
            if (hs.plivo_api_url, hs.plivo_media_url) != (self.plivo.url, self.plivo.media_url):
                raise RuntimeError(f"HS would call {hs.plivo_api_url}, not the Plivo simulator")
            self._stack.callback(serve(app, sock)[1])
        self.operator = self.client(operator_key)

    def close(self) -> None:
        self._stack.close()

    def anon(self) -> httpx.Client:
        """Client with no credentials."""
        return httpx.Client(base_url=self.base_url, timeout=10 * SLOW)

    def client(self, token: str) -> httpx.Client:
        """Client sending `Authorization: Bearer <token>`."""
        return httpx.Client(base_url=self.base_url, headers={"Authorization": f"Bearer {token}"}, timeout=10 * SLOW)

    def new_tenant(self, name: str = "Acme") -> Tenant:
        """Create a Plivo sub-account in the simulator, then POST /api/v1/tenants with the operator key."""
        sub = self.plivo.subaccount(name)
        body = {"name": name, "plivo_auth_id": sub.auth_id, "plivo_auth_token": sub.auth_token}
        created = _ok(self.operator.post("/api/v1/tenants", json=body))
        key = created["api_key"]
        return Tenant(created["tenant_id"], name, sub.auth_id, sub.auth_token, key, self.client(key), self)

    def webhook_sink(self) -> WebhookSink:
        """A local HTTP receiver for webhook deliveries."""
        sink = WebhookSink()
        self._stack.callback(sink.close)
        return sink

    def run_jobs(self, kind: str | None = None) -> int:
        """Make pending jobs (optionally of one kind) due now, wait until the runner has processed them;
        return how many ran. Used instead of waiting for real timers."""
        return self._run_due(kind, None)

    def advance(self, seconds: float) -> int:
        """Run the jobs due within `seconds` from now (as run_jobs does) and return how many ran; nothing
        else moves: no clock is shifted and no other timer fires."""
        return self._run_due(None, seconds)

    def expire_cache(self, seconds: float) -> None:
        """Let `seconds` pass for Redis alone: every key whose TTL runs out by then expires now. No job runs."""
        for key in self.redis.scan_iter():
            if 0 < self.redis.pttl(key) <= seconds * 1000:
                self.redis.pexpire(key, 1)

    def _run_due(self, kind: str | None, within: float | None) -> int:
        """Drain first: the simulator idle (no engine work, no callback queued or in flight), then no matching
        job held by the runner. Only then make the rows due, and wait for each to run or fail."""
        self.wait(self.plivo.idle, timeout=10, what="the Plivo simulator to go idle")
        args = {"kind": kind, "within": within}
        match = (
            " (%(kind)s::text IS NULL OR kind = %(kind)s)"
            " AND (%(within)s::float8 IS NULL OR run_at <= now() + %(within)s * interval '1 second')"
        )

        def unlocked() -> bool:
            with self._db.cursor() as cur:
                cur.execute(
                    "SELECT count(*) FROM jobs WHERE" + match + " AND locked_until > now()"
                    " AND locked_until <> 'infinity'",
                    args,
                )
                return cur.fetchone()[0] == 0

        self.wait(unlocked, timeout=10, what="the job runner to release the jobs it holds")
        with self._db.cursor() as cur:
            cur.execute(
                "UPDATE jobs SET run_at = now() WHERE" + match + " AND locked_until IS DISTINCT FROM 'infinity'"
                " RETURNING id, attempts",
                args,
            )
            due = dict(cur.fetchall())

        def processed() -> bool:
            with self._db.cursor() as cur:
                cur.execute("SELECT id, attempts FROM jobs WHERE id = ANY(%s)", (list(due),))
                return all(attempts > due[i] for i, attempts in cur.fetchall())

        self.wait(processed, timeout=10, what=f"the job runner to process {len(due)} {kind or 'due'} job(s)")
        return len(due)

    def wait(self, predicate: Callable[[], Any], timeout: float = 5.0, what: str = "") -> Any:
        """Poll `predicate` until truthy; return its value or raise AssertionError(what)."""
        deadline = time.monotonic() + timeout * SLOW
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

    def number(self) -> str:
        """Buy a fresh E.164 number on this sub-account in the simulator (not yet routed in HS)."""
        return self.world.plivo.buy_number(self.auth_id)

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
        if available:
            what = f"agent {self.id} to become Available"
            self.tenant.world.wait(lambda: self.heartbeat() or self._status() == "Available", what=what)
        return self

    def api(self) -> httpx.Client:
        """Client with this agent's token (after login)."""
        return self._api

    def heartbeat(self) -> None:
        _ok(self.api().post("/api/v1/me/heartbeat"))

    def set_status(self, name: str) -> None:
        """PUT /me/status by status name."""
        _ok(self.api().put("/api/v1/me/status", json={"status_id": self.tenant.status_id(name)}))

    def expire_tokens(self) -> None:
        """Expire this agent's agent and stream tokens now, as if their TTL ran out: every Redis key whose
        name contains a token's SHA-256 hex gets a 1 ms TTL. HS must key tokens that way, and its SSE loop
        must re-check the stream's token about once a second to send auth.expired."""
        world = self.tenant.world
        for token in (self.agent_token, self.stream_token):
            keys = list(world.redis.scan_iter(match=f"*{hashlib.sha256(token.encode()).hexdigest()}*"))
            assert keys, "no Redis key is named after the token's SHA-256"
            for key in keys:
                world.redis.pexpire(key, 1)

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
        if not self._ended.wait(timeout * SLOW):
            raise AssertionError(f"the event stream is still open after {timeout}s")

    def close(self) -> None:
        self._closed = True
        self._client.close()


@dataclass
class Delivery:
    headers: dict[str, str]
    body: bytes
    json: dict


class WebhookSink:
    def __init__(self) -> None:
        self.deliveries: list[Delivery] = []
        self._status, self._headers, self._delay, self._cond = 200, {}, 0.0, threading.Condition()
        self._stall, self._released = False, threading.Event()
        sink = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                status, headers, delay = (
                    sink._status,
                    sink._headers,
                    sink._delay,
                )  # read before a waiter can change them
                body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                try:
                    parsed = json.loads(body)
                except ValueError:
                    parsed = {}
                with sink._cond:
                    sink.deliveries.append(Delivery(dict(self.headers), body, parsed))
                    sink._cond.notify_all()
                time.sleep(delay)
                with sink._cond:
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

    def answer_after(self, seconds: float) -> None:
        """Following deliveries are recorded at once but answered only after `seconds`."""
        self._delay = seconds

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
