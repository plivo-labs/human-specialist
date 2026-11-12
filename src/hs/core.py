"""What every domain shares: settings, the database (reflected `hs` tables, `tx`, `read`), the Redis client, the
error envelope and the request middleware, list paging and logging."""

import base64
import binascii
import json
import logging
import logging.handlers
import os
import queue
import re
import sys
import threading
import time
import uuid
import warnings
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from http import HTTPStatus
from types import SimpleNamespace
from typing import NamedTuple
from urllib.parse import urlsplit

from fastapi import FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from prometheus_client import Histogram
from pydantic import BaseModel
from redis import ConnectionError as RedisDown
from redis import Redis, RedisError, ResponseError
from redis import TimeoutError as RedisTimeout
from redis.client import Pipeline
from sqlalchemy import MetaData, create_engine, event
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.engine import Connection, make_url
from sqlalchemy.exc import OperationalError, SAWarning
from sqlalchemy.exc import TimeoutError as PoolTimeout
from starlette.datastructures import MutableHeaders
from starlette.exceptions import HTTPException
from starlette.requests import ClientDisconnect

PUBLIC = "/api/v1"
log = logging.getLogger(__name__)


def _env(name: str, default: str | None = None, cast=str.strip):
    return field(default_factory=lambda: cast(os.environ[name] if default is None else os.getenv(name, default)))


def _csv(v: str) -> tuple[str, ...]:
    return tuple(p.strip() for p in v.split(",") if p.strip())


def _slash(v: str) -> str:
    return v.strip().rstrip("/")


def _at_least(name: str, low: int):
    def cast(v: str) -> int:
        if int(v) < low:
            raise RuntimeError(f"{name} must be at least {low}, got {v}")
        return int(v)

    return cast


# DNS labels that neither start nor end with "-", or an IP literal.
_HOST = re.compile(r"(?!-)(?!.*-\.)(?!.*\.-)(?!.*-$)[A-Za-z0-9.-]+|[0-9A-Fa-f:.]+")


def _base_url(v: str) -> str:
    """Plivo calls back and signs over this URL, so a malformed one stops the start instead of failing a call."""
    url = _slash(v)
    try:
        parts = urlsplit(url)
        host, _ = (parts.hostname or "").encode("idna").decode(), parts.port
    except ValueError as e:
        raise RuntimeError(f"PUBLIC_BASE_URL is not a valid URL: {e}") from e
    if parts.scheme not in ("http", "https") or re.search(r"[\s\x00-\x1f]", url) or not _HOST.fullmatch(host):
        raise RuntimeError("PUBLIC_BASE_URL must be an http or https URL with a host, such as https://hs.example.com")
    return url


@dataclass(frozen=True)
class Settings:
    database_url: str = _env("DATABASE_URL")
    redis_url: str = _env("REDIS_URL")
    db_pool_size: int = _env("DB_POOL_SIZE", "20", _at_least("DB_POOL_SIZE", 1))
    db_max_overflow: int = _env("DB_MAX_OVERFLOW", "10", _at_least("DB_MAX_OVERFLOW", 0))
    public_base_url: str = _env("PUBLIC_BASE_URL", cast=_base_url)
    operator_key: str = _env("HS_OPERATOR_KEY")
    secret_key: str = _env("HS_SECRET_KEY")
    plivo_api_url: str = _env("PLIVO_API_URL", "https://api.plivo.com", _slash)
    plivo_media_url: str = _env("PLIVO_MEDIA_URL", "https://media.plivo.com", _slash)
    cors_allowed_origins: tuple[str, ...] = _env("CORS_ALLOWED_ORIGINS", "", _csv)
    event_retention_days: int = _env("EVENT_RETENTION_DAYS", "90", int)
    webhook_private_hosts: tuple[str, ...] = _env("WEBHOOK_PRIVATE_HOSTS", "", _csv)
    search_url: str = _env("SEARCH_URL", "", _slash)
    metrics_port: int = _env("METRICS_PORT", "9100", int)
    log_level: str = _env("LOG_LEVEL", "INFO", lambda v: v.strip().upper())


_required = {"DATABASE_URL", "REDIS_URL", "PUBLIC_BASE_URL", "HS_OPERATOR_KEY", "HS_SECRET_KEY"}
if missing := sorted(name for name in _required if not os.getenv(name, "").strip()):
    raise RuntimeError(f"missing required environment variables: {', '.join(missing)}")
if short := [k for k in ("HS_OPERATOR_KEY", "HS_SECRET_KEY") if len(os.environ[k].strip().encode()) < 32]:
    raise RuntimeError(f"{', '.join(short)} must be at least 32 bytes")
settings = Settings()

# The default 20 + 10, with the 2 that count conversation numbers, fits three replicas in the 97 connections Postgres's
# defaults leave past its superuser reserve, but is under AnyIO's 40 threads: in a burst a request waits pool_timeout
# for a connection, then answers 503. Timeouts bound every statement, lock and idle transaction. No ping on checkout: after a Postgres restart each request that already
# holds a pooled connection fails once, and that failure discards the rest of the pool, so later requests reconnect.
_url = make_url(settings.database_url)
POOL_TIMEOUT, STATEMENT_TIMEOUT = 10, 5
CONNECT = {
    "target_session_attrs": "read-write",
    "options": f"-c search_path=hs -c statement_timeout={STATEMENT_TIMEOUT * 1000} -c lock_timeout=3000"
    " -c idle_in_transaction_session_timeout=10000",
}
# A connect_timeout in DATABASE_URL replaces the 3 s default.
engine = create_engine(
    _url.set(query={"connect_timeout": "3", **_url.query}),
    future=True,
    hide_parameters=True,
    pool_size=settings.db_pool_size,
    max_overflow=settings.db_max_overflow,
    pool_recycle=540,
    pool_timeout=POOL_TIMEOUT,
    connect_args=CONNECT,
)


@event.listens_for(engine, "handle_error")
def _demoted_primary(context) -> None:
    """A write refused as read-only (SQLSTATE 25006) means a failover demoted this server: treated as a disconnect, so
    the pool is discarded and the next checkout reconnects to the new primary."""
    if getattr(context.original_exception, "pgcode", None) == "25006":
        context.is_disconnect = True


CHECKOUT = Histogram("hs_db_checkout_seconds", "Wait for a pooled database connection, a timed-out wait included")
REDIS_SECONDS = Histogram(
    "hs_redis_seconds",
    "One Redis command or pipeline, a timed-out one included",
    buckets=(0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
)
# A Redis that stops answering costs each call in flight one REDIS_TIMEOUT; then calls fail fast, so none holds a
# pooled database connection waiting on it.
REDIS_TIMEOUT, REDIS_SHUT = 0.5, 2.0
_breaker = threading.Lock()
_probe_at: float | None = None  # None while closed
_probing = False
_closes = 0
_undoing: ContextVar[bool] = ContextVar("undoing", default=False)


class _Refused(RedisDown):
    pass


def _guarded(send: Callable, *args, **kwargs):
    """`send()` behind the breaker. A timeout opens it: every call fails fast until `_probe_at`, when one goes through
    as the probe. A probe that times out keeps it open; any other outcome closes it. Only the probe changes an open
    breaker, and only a call sent since it last closed can open it. A transaction's undo is sent even while it is
    open."""
    global _probe_at, _probing, _closes
    with _breaker:
        closes, probe = _closes, _probe_at is not None and not _undoing.get()
        if probe:
            if _probing or time.monotonic() < _probe_at:
                raise _Refused(f"Redis did not answer within {REDIS_TIMEOUT} s; it is asked again shortly")
            _probing = True
    start, timed_out = time.perf_counter(), False
    try:
        return send(*args, **kwargs)
    except RedisTimeout:
        timed_out = True
        raise
    finally:
        REDIS_SECONDS.observe(time.perf_counter() - start)
        if probe or timed_out:
            with _breaker:
                closed_as_sent = _probe_at is None and _closes == closes
                if probe and not timed_out:
                    log.warning("Redis answers again")
                    _probe_at, _probing, _closes = None, False, _closes + 1
                elif probe or closed_as_sent:
                    if closed_as_sent:
                        log.warning("Redis timed out: calls fail fast, and one probes every %s s", REDIS_SHUT)
                    _probe_at, _probing = time.monotonic() + REDIS_SHUT, False


class _Pipeline(Pipeline):
    def execute(self, raise_on_error: bool = True):
        return _guarded(super().execute, raise_on_error)


class _Redis(Redis):
    def execute_command(self, *args, **options):
        return _guarded(super().execute_command, *args, **options)

    def pipeline(self, transaction=True, shard_hint=None) -> _Pipeline:
        return _Pipeline(self.connection_pool, self.response_callbacks, transaction, shard_hint)


redis = _Redis.from_url(
    settings.redis_url,
    decode_responses=True,
    socket_connect_timeout=REDIS_TIMEOUT,
    socket_timeout=REDIS_TIMEOUT,
    health_check_interval=30,
)
T = SimpleNamespace()


def check_versions() -> None:
    """Refuse a server older than the SQL (CYCLE, Postgres 14) and the newest command options (EXPIRE ... NX and GT,
    Redis 7) the code uses."""
    with read() as db:
        postgres = int(db.exec_driver_sql("SHOW server_version_num").scalar())
    if postgres < 140000:
        raise RuntimeError(f"Postgres {postgres // 10000} is too old: human-specialist needs Postgres 14 or later")
    try:
        redis.execute_command("EXPIRE", "hs:{probe}", 1, "GT")
    except ResponseError as e:
        # Redis 6.2 takes EXPIRE's options for extra arguments. An unknown command is one renamed or disabled, not an
        # old server.
        if "wrong number of arguments" in str(e):
            raise RuntimeError(f"This Redis is too old ({e}): human-specialist needs Redis 7 or later") from e
        log.warning("Redis refused the EXPIRE GT probe, so its version is unchecked", exc_info=True)
    except RedisError:
        log.warning("Redis did not answer the EXPIRE GT probe, so its version is unchecked", exc_info=True)


def check_eviction() -> None:
    """Managed Redis often refuses CONFIG GET, so read INFO."""
    try:
        policy = redis.info("memory").get("maxmemory_policy")
    except RedisError:
        log.warning("Redis did not answer INFO memory, so its maxmemory-policy is unchecked", exc_info=True)
        return
    if policy != "noeviction":
        log.error("Redis maxmemory-policy is %s, not noeviction: see docs/configuration.md#run-redis", policy)


def init() -> None:
    """Reflect the `hs` tables into `T`, uuid columns as `uuid.UUID`. Alembic owns the DDL; the code never restates it."""
    meta = MetaData()
    with warnings.catch_warnings():
        # Expression indexes stay Alembic's; queries never need them reflected.
        warnings.filterwarnings("ignore", "Skipped unsupported reflection of expression-based index", SAWarning)
        meta.reflect(bind=engine, schema="hs")
    for table in meta.tables.values():
        for column in table.c:
            if isinstance(column.type, UUID):
                column.type = UUID(as_uuid=True)
        T.__dict__[table.name] = table


# The last CONTROL_RESERVE connections serve only threads the job runner marks in CONTROL_LANE, so timers never wait
# behind other work.
CONTROL_RESERVE = 8
CONTROL_LANE = threading.local()
if (_total := settings.db_pool_size + settings.db_max_overflow) < 2 * CONTROL_RESERVE:
    raise RuntimeError(
        f"DB_POOL_SIZE + DB_MAX_OVERFLOW must be at least {2 * CONTROL_RESERVE}, got {_total}: {CONTROL_RESERVE} are"
        " kept for timers, and requests and other work need at least as many"
    )
_ordinary = threading.BoundedSemaphore(_total - CONTROL_RESERVE)


@contextmanager
def _connect() -> Iterator[Connection]:
    ordinary, start = not getattr(CONTROL_LANE, "on", False), time.perf_counter()
    if ordinary and not _ordinary.acquire(timeout=POOL_TIMEOUT):
        CHECKOUT.observe(time.perf_counter() - start)
        raise PoolTimeout(f"no connection outside the control lane's reserve came free in {POOL_TIMEOUT} s")
    try:
        try:
            db = engine.connect()
        finally:
            CHECKOUT.observe(time.perf_counter() - start)
        db.info["memo"] = {}  # the pool keeps Connection.info across checkouts
        with db:
            yield db
    finally:
        if ordinary:
            _ordinary.release()


@contextmanager
def tx() -> Iterator[Connection]:
    """One transaction. `before_commit` callables run in order just before COMMIT, and one that fails rolls it back;
    `after_commit` callables run in order after COMMIT, never after a rollback. On failure the `undoable` undos run
    newest first before the rollback, each sent once even while the Redis breaker is open, so its row locks still
    keep out the next writer of what they restore. A process that dies runs none of them: what they would undo must
    be recognizable as stale."""
    after: list[Callable[[], None]] = []
    undos: list[Callable[[], None]] = []
    with _connect() as db, db.begin():
        db.info["before_commit"], db.info["after_commit"], db.info["undo"], db.info["tx"] = [], after, undos, {}
        try:
            yield db
            for fn in db.info["before_commit"]:
                fn()
        except BaseException:
            undoing = _undoing.set(True)
            try:
                for fn in reversed(undos):
                    try:
                        fn()
                    except Exception:
                        log.exception("undo %s failed", getattr(fn, "__qualname__", fn))
            finally:
                _undoing.reset(undoing)
            raise
        finally:
            del db.info["before_commit"], db.info["after_commit"], db.info["undo"], db.info["tx"]
    for fn in after:
        try:
            fn()
        except Exception:
            log.exception("after-commit %s failed", getattr(fn, "__qualname__", fn))


def before_commit(db: Connection, fn: Callable[[], None]) -> None:
    db.info["before_commit"].append(fn)


def after_commit(db: Connection, fn: Callable[[], None]) -> None:
    db.info["after_commit"].append(fn)


def undoable(db: Connection, write: Callable[[], object], undo: Callable[[], None]) -> object:
    """`write()`'s reply from Redis; `undo()` reverses the write if the transaction fails. A write whose reply is
    lost may have run, so it keeps its undo, which must be right whether or not the write ran. A write the breaker
    refused never reached Redis, so it leaves nothing to undo."""
    db.info["undo"].append(undo)
    try:
        return write()
    except _Refused:
        db.info["undo"].remove(undo)
        raise


@contextmanager
def read() -> Iterator[Connection]:
    """No transaction: each statement commits on its own, with a snapshot of its own as READ COMMITTED gives
    anyway. Work that needs a lock across statements, an advisory lock or SET LOCAL takes `tx()`."""
    with _connect() as db:
        # psycopg2's own flag sends nothing, where SQLAlchemy's AUTOCOMMIT level sends a SET on every checkin.
        raw = db.connection.dbapi_connection
        raw.autocommit = True
        try:
            yield db
        finally:
            if not raw.closed:
                raw.autocommit = False


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str = "", details: dict | None = None):
        super().__init__(message or code)
        self.status, self.code, self.message, self.details = status, code, message, details or {}


class Error(BaseModel):
    code: str
    message: str
    details: dict = {}


class ErrorEnvelope(BaseModel):
    error: Error
    request_id: str


REQUEST_ID: ContextVar[str | None] = ContextVar("request_id", default=None)
_SAFE_ID = re.compile(r"[A-Za-z0-9._-]{1,64}")
LATENCY = Histogram(
    "hs_http_request_response_seconds",
    "Response time per route",
    ("endpoint", "method", "response_code"),
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)


def render_error(request: Request, status: int, code: str, message: str = "", details: dict | None = None) -> Response:
    rid = getattr(request.state, "request_id", None) or uuid.uuid4().hex
    body = {"error": {"code": code, "message": message, "details": details or {}}, "request_id": rid}
    return JSONResponse(body, status, headers={"X-Request-Id": rid})


def _api_error(request: Request, exc: ApiError) -> Response:
    if exc.status >= 500:
        log.warning("%s %s answered %s %s: %s", request.method, request.url.path, exc.status, exc.code, exc)
    response = render_error(request, exc.status, exc.code, exc.message, exc.details)
    if exc.status == 429:
        response.headers["Retry-After"] = str(exc.details.get("retry_after", 1))
    return response


def _invalid(request: Request, exc: RequestValidationError) -> Response:
    if any(e["type"] == "json_invalid" for e in exc.errors()):
        return render_error(request, 400, "invalid_json", "the body is not valid JSON")
    fields = [{"loc": list(e["loc"]), "msg": e["msg"], "type": e["type"]} for e in exc.errors()]
    return render_error(request, 422, "validation_failed", "the request is invalid", {"fields": fields})


def _http(request: Request, exc: HTTPException) -> Response:
    if isinstance(exc.__cause__, ClientDisconnect):  # FastAPI's body read wraps a caller gone mid-body in a 400
        return Response(status_code=499)
    return render_error(
        request, exc.status_code, HTTPStatus(exc.status_code).phrase.lower().replace(" ", "_"), str(exc.detail)
    )


def _unhandled(request: Request, exc: Exception) -> Response:
    if isinstance(exc, ClientDisconnect):
        return Response(status_code=499)
    log.error("unhandled error on %s %s", request.method, request.url.path, exc_info=exc)
    if isinstance(exc, OperationalError | PoolTimeout):
        return render_error(request, 503, "dependency_unavailable", "the database did not answer; try again")
    if isinstance(exc, RedisError):
        return render_error(request, 503, "dependency_unavailable", "the hot-state store did not answer; try again")
    return render_error(request, 500, "internal_error", "internal server error")


def _answer(render: Callable[[Request, Exception], Response]):
    async def handle(request: Request, exc: Exception) -> Response:
        response = render(request, exc)
        if request.url.path.startswith("/plivo/"):
            rid = getattr(request.state, "request_id", None)
            return Response(status_code=response.status_code, headers={"X-Request-Id": rid} if rid else None)
        return response

    return handle


class _Edge:
    """Request ids and latency as plain ASGI, since `@app.middleware` gives every request a task and streams of its
    own. The caller's X-Request-Id only if it is short and safe to log and echo; otherwise a new one. Latency runs to
    the response's start, by route template, not raw path, so ids and scanners cannot mint new series."""

    def __init__(self, app) -> None:
        self.app = app

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        sent = next((v.decode("latin-1") for k, v in scope["headers"] if k == b"x-request-id"), "")
        rid = sent if _SAFE_ID.fullmatch(sent) else uuid.uuid4().hex
        REQUEST_ID.set(rid)
        scope.setdefault("state", {})["request_id"] = rid
        start, timed = time.perf_counter(), False

        def observe(status: int) -> None:
            nonlocal timed
            timed = True
            endpoint = getattr(scope.get("route"), "path", "<unmatched>")
            LATENCY.labels(endpoint, scope["method"], str(status)).observe(time.perf_counter() - start)

        async def stamped(message) -> None:
            if message["type"] == "http.response.start":
                MutableHeaders(scope=message)["X-Request-Id"] = rid
                observe(message["status"])
            await send(message)

        try:
            await self.app(scope, receive, stamped)
        finally:
            if not timed:
                observe(500)


class _ApiCors(CORSMiddleware):
    """CORS on /api/v1 only: exact origins, bearer tokens, no credentials mode."""

    def __init__(self, app) -> None:
        super().__init__(
            app,
            allow_origins=list(settings.cors_allowed_origins),
            allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
            allow_headers=["Authorization", "Content-Type", "Idempotency-Key", "Last-Event-ID"],
            expose_headers=["Retry-After", "X-Request-Id"],
        )

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] == "http" and scope["path"].startswith(PUBLIC):
            return await super().__call__(scope, receive, send)
        return await self.app(scope, receive, send)


def install(app: FastAPI) -> None:
    """The error envelope (a bare status on /plivo routes), request ids, latency metrics and CORS."""
    if "*" in settings.cors_allowed_origins:
        raise RuntimeError("CORS_ALLOWED_ORIGINS takes exact origins, never *")
    for exc, render in {ApiError: _api_error, RequestValidationError: _invalid, HTTPException: _http}.items():
        app.add_exception_handler(exc, _answer(render))
    app.add_exception_handler(Exception, _answer(_unhandled))
    app.add_middleware(_Edge)
    app.add_middleware(_ApiCors)


class Page[M](BaseModel):
    data: list[M]
    next_cursor: str | None


class Paging(NamedTuple):
    limit: int
    after: str | None  # the sort key of the last row the client saw


def paging(limit: int = Query(50, ge=1, le=200), cursor: str | None = None) -> Paging:
    """`?limit=&cursor=`: fetch `limit + 1` rows after `after` and hand them to `page`."""
    try:
        return Paging(limit, base64.b64decode(cursor, b"-_", validate=True).decode() if cursor else None)
    except (binascii.Error, UnicodeDecodeError) as e:
        raise ApiError(422, "invalid_cursor", "cursor is not one this API returned") from e


def page(rows: list[dict], paged: Paging, key: str = "id") -> dict:
    more, rows = len(rows) > paged.limit, rows[: paged.limit]
    cursor = base64.urlsafe_b64encode(str(rows[-1][key]).encode()).decode() if more else None
    return {"data": rows, "next_cursor": cursor}


def capped(value: dict | None, limit: int = 2048, field: str = "context") -> dict:
    """A caller's free-form object, at most `limit` bytes of JSON and without NUL (jsonb cannot hold it)."""
    raw = json.dumps(value or {}, separators=(",", ":"))
    if len(raw.encode()) > limit:
        raise ApiError(422, f"{field}_too_large", f"{field} is at most {limit} bytes")
    if "\\u0000" in raw:
        raise ApiError(422, f"{field}_invalid", f"{field} may not contain a NUL character")
    return value or {}


# HS's own credentials are one of these prefixes plus secrets.token_urlsafe(32).
KEY_PREFIX, AGENT_PREFIX, STREAM_PREFIX = "hs_tk_", "hs_at_", "hs_st_"
OURS = (KEY_PREFIX, AGENT_PREFIX, STREAM_PREFIX)


def _encodable(name: str) -> str:
    """A pattern for `name` with any character percent-encoded, as a query string may send it (`%73ig` is `sig`).
    The line is never decoded, so an encoded CR or LF cannot split it."""
    return "".join(f"(?:{re.escape(c)}|%{ord(c.lower()):x}|%{ord(c.upper()):x})" for c in name)


_NAMES = "|".join(map(_encodable, ("signature", "password", "secret", "api_key", "api-key", "token", "credential")))
# Group 1 stays (an HS prefix, an auth scheme, a sensitive name and its separator); the value after it goes. `key` and
# `sig` count only as a word of their own (`?key=`, `X-Auth-Key`), so `idempotency_key` and `signal` keep their values,
# as does `Idempotency-Key`, a retry id. `api-key` stays listed for `api%2Dkey`, whose encoded hyphen ends in a letter.
# The name after its keyword is capped at 32 characters, which keeps a run of keywords linear, so a longer name's
# value is not redacted; a non-overlapping tokenizer lifts the cap if a real key needs it.
_SECRET = re.compile(
    rf"(?i)({KEY_PREFIX}|{AGENT_PREFIX}|{STREAM_PREFIX}|\b(?:bearer|basic)\s+|(?:{_NAMES}"
    rf"|(?<!\w)(?:{_encodable('sig')}|(?<!idempotency-){_encodable('key')})\b)[\w%-]{{0,32}}['\"]?\s*[:=]\s*)"
    r"('[^']*'|\"[^\"]*\"|[^\s&,;'\"]+)"
)


def redact(text: str) -> str:
    return re.sub(r"(://)[^/?#\s]*@", r"\1[REDACTED]@", _SECRET.sub(r"\1[REDACTED]", text))


class _Json(logging.Formatter):
    """No token reaches a log line: the message is redacted once rendered with its args, and so is the traceback."""

    def format(self, record: logging.LogRecord) -> str:
        line = {"time": self.formatTime(record), "level": record.levelname, "logger": record.name}
        line |= {"message": redact(record.getMessage()), "request_id": REQUEST_ID.get()}
        if record.exc_info:
            line["error"] = redact(self.formatException(record.exc_info))
        return json.dumps(line, default=str)


def setup_logging() -> None:
    """JSON lines on stdout through a queue and one writer thread: a stalled stdout costs memory, not requests.
    uvicorn's lines, its traceback of an unhandled error included, are rendered the same way but written at once,
    as uvicorn ends on SIGTERM without draining a queue."""
    lines = queue.SimpleQueue()
    handler = logging.handlers.QueueHandler(lines)
    handler.setFormatter(_Json())
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(settings.log_level)
    # httpx logs each request's URL at INFO, and a recording's media URL is a credential.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    direct = logging.StreamHandler(sys.stdout)
    direct.setFormatter(_Json())
    for uvicorn in map(logging.getLogger, ("uvicorn", "uvicorn.access")):
        if uvicorn.handlers:  # uvicorn empties them for --no-access-log, which stays off
            uvicorn.handlers = [direct]
    logging.handlers.QueueListener(lines, logging.StreamHandler(sys.stdout)).start()
