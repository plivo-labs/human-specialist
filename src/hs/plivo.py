"""Plivo: REST as a tenant, provider writes as durable `plivo` jobs, signed callbacks, callback URLs and XML.

A write is a command scheduled in the transaction that decided it. A create (ring, dial, send) is sent only on its
first claim: a later claim cannot know whether an earlier one sent it, so it runs the op's `uncertain`, which
reconciles from callbacks, timers or a provider read. Convergent ops retry on a backoff and then park."""

import base64
import hashlib
import hmac
import itertools
import json
import logging
import re
import time
import xml.etree.ElementTree as ET
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from functools import cache, partial
from http.cookiejar import DefaultCookiePolicy
from typing import NamedTuple
from urllib.parse import parse_qsl, unquote_plus, urlencode, urlsplit
from uuid import UUID

import httpx
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives.hashes import SHA256
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from fastapi import Depends, Request, Response
from fastapi.concurrency import run_in_threadpool
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Connection, Row

from hs import hot, jobs
from hs.core import ApiError, T, after_commit, read, redact, settings, tx

log = logging.getLogger(__name__)
# Plivo retries a callback that answers 5xx or times out, five times.
RETRY = "#rc=5&ct=2000&rd=1000&rp=5xx,ct"
TIMEOUT = 10  # seconds one Plivo request may take, unless its caller sets its own
HTTP = httpx.Client(timeout=5)
# One client serves every tenant, so a cookie one tenant's answer sets must never ride on another's request.
HTTP.cookies.jar.set_policy(DefaultCookiePolicy(allowed_domains=[]))


class Refused(ApiError):
    """Plivo answered 4xx, or the request never left: nothing was created."""


class Uncertain(ApiError):
    """The request was sent and its outcome is unknown: 5xx, read timeout or reset. A route that meets one answers
    503; a job reconciles or retries. `str()` is the cause, for the log."""

    def __init__(self, cause: str):
        super().__init__(503, "plivo_unavailable", "Plivo did not answer")
        self.args = (cause,)


def _ignore(*_) -> None:
    return None


@dataclass(frozen=True)
class Op:
    create: bool
    send: Callable[[Row, dict], dict]  # (tenant, payload) -> Plivo's reply
    done: Callable[[dict, dict], None] = _ignore  # (payload, reply)
    failed: Callable[[dict, Refused], None] = _ignore
    uncertain: Callable[[dict], None] = _ignore
    teardown: bool = False  # cleanup that still runs once its tenant is deleted


OPS: dict[str, Op] = {}
# Polls of a command the job pool runs: past Plivo's 5 s timeout.
WAITS = 120


def command(db: Connection, op: str, subject: str, payload: dict, *, wait: bool = False) -> str:
    """Schedule Plivo write `op` keyed `<op>:<tenant>:<subject>`, since a subject may be an id from a callback, which
    any tenant can sign; it runs once `db` commits. With `wait` the caller runs it itself (`jobs.run_now("plivo", key,
    wait=True)` returns a `Refused` as Plivo's verdict) and the poll takes it only if that process died first."""
    key = f"{op}:{payload['tenant_id']}:{subject}"
    jobs.schedule(db, "plivo", payload | {"op": op}, key=key, delay=10 if wait else 0)
    if not wait:
        after_commit(db, partial(jobs.run_now, "plivo", key))
    return key


def verdict(*keys: str | None, request: str | None = None) -> None:
    """Wait for each command's outcome, running it here unless someone holds it. A refusal is the answer, the job
    pool's too; a command Plivo did not answer keeps retrying on its backoff and is a 503, which the client may retry.
    A run here that a newer schedule of the key replaced is never retried, so its own failure is the answer; had it
    succeeded, it frees the newer one, which runs here as soon as a read finds it free and unfailed: a claim straight
    after that read never runs a failed command before its backoff. The outcome is then the newer command's. With
    `request`, only the caller's own command, the one whose payload carries it, runs here and is waited on: once a
    newer schedule replaced it, the answer is its own run's, or success if it was never sent or the job pool ran it."""
    J = T.jobs
    mine = () if request is None else (J.c.payload["request"].astext == request,)
    for key in filter(None, keys):
        if refused := jobs.run_now("plivo", key, *mine, wait=True):
            raise refused
        for _ in range(WAITS):
            with read() as db:
                outcome = select(J.c.last_error, J.c.payload["refused"].label("refused"), J.c.locked_until)
                job = db.execute(outcome.where(J.c.kind == "plivo", J.c.key == key, *mine)).first()
            if job is None:
                break
            if job.refused:
                raise Refused(**job.refused)
            if job.last_error:
                raise ApiError(503, "dependency_unavailable", "Plivo did not answer; try again")
            if job.locked_until is None and (refused := jobs.run_now("plivo", key, *mine, wait=True)):
                raise refused
            time.sleep(0.05)
        else:
            raise ApiError(503, "dependency_unavailable", "Plivo did not answer; try again")


@jobs.handler("plivo", backoff=(1, 5, 30), lane="plivo")
def _run(job: Row) -> Refused | None:
    payload, op, tenant = job.payload, OPS[job.payload["op"]], account(job.payload["tenant_id"], fresh=True)
    if (tenant is None or tenant.deleted) and not op.teardown:
        log.info("plivo %s: its tenant is deleted; dropped", job.key)
        return None
    if op.create and job.attempts > 1:
        log.warning("plivo %s: an earlier claim may have sent it; reconciling", job.key)
        op.uncertain(payload)
        return None
    try:
        reply = op.send(tenant, payload)
    except Refused as e:
        if not op.create and (e.status == 429 or e.code == "plivo_unavailable"):
            raise
        log.warning("plivo %s refused: %s", job.key, e.code)
        op.failed(payload, e)
        return e
    except Uncertain:
        if not op.create:
            raise
        log.warning("plivo %s: outcome unknown; reconciling", job.key, exc_info=True)
        op.uncertain(payload)
        return None
    op.done(payload, reply)
    return None


def rest(tenant: Row, method: str, path: str, *, timeout_sec: float = TIMEOUT, **params) -> dict:
    """One request to /v1/Account/{auth_id}/{path} as the tenant. A DELETE of something already gone answers
    {"gone": True}. A GET that could not connect or met a 502, 503 or 504 is sent twice more: a read changes nothing
    at Plivo, and a write is never sent again here, since a lost answer can hide one that took effect."""
    auth = (tenant.plivo_auth_id, decrypt(tenant.plivo_token_enc))
    # Plivo's multiparty call API refuses a POST without a JSON body, so a POST with no parameters sends {}.
    body = {"params": params} if method == "GET" else {"json": params if method == "POST" else params or None}
    url = f"{settings.plivo_api_url}/v1/Account/{auth[0]}/{path}"
    for wait in (0.2, 0.4, None) if method == "GET" else (None,):
        try:
            r = HTTP.request(method, url, auth=auth, timeout=timeout_sec, **body)
            if wait is None or r.status_code not in (502, 503, 504):
                break
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout) as e:
            if wait is None:
                raise Refused(503, "plivo_unavailable", "Plivo did not answer") from e
        except httpx.TransportError as e:
            raise Uncertain(f"{method} {path}: {type(e).__name__}") from e
        time.sleep(wait)
    if r.is_success:
        return r.json() if r.content else {}
    if method == "DELETE" and r.status_code == 404:
        return {"gone": True}
    details = {"status": r.status_code, "errors": _reason(r, auth, params)}
    log.warning("plivo %s %s answered %s: %s", method, path, r.status_code, details["errors"])
    if r.status_code >= 500:
        raise Uncertain(f"{method} {path}: {r.status_code}")
    if r.status_code == 429:
        wait = r.headers.get("Retry-After", "")
        details["retry_after"] = int(wait) if wait.isdigit() else 1
        raise Refused(429, "carrier_rate_limited", "Plivo is rate limiting this account", details)
    if r.status_code == 401:
        raise Refused(422, "plivo_credentials_rejected", "Plivo refused these credentials", details)
    raise Refused(422, "plivo_refused", "Plivo refused the request", details)


def listed(tenant, path: str) -> Iterator[dict]:
    """Every object Plivo lists at `path`, a page of 20 at a time."""
    for offset in itertools.count(0, 20):
        objects = rest(tenant, "GET", path, limit=20, offset=offset).get("objects", [])
        yield from objects
        if len(objects) < 20:
            return


def held(tenant: Row, path: str, what: str) -> dict:
    """The Plivo account's `what` at `path`, as Plivo reads it; 422 `<what>_not_in_account` when it holds none."""
    try:
        return rest(tenant, "GET", path)
    except Refused as e:
        if e.details.get("status") == 404:
            raise ApiError(422, f"{what}_not_in_account", f"the Plivo account does not hold this {what}") from e
        raise


# HS sends Plivo the token only in the auth header, so this guards an accidental echo, not a hostile Plivo.
_CREDENTIAL = re.compile(r"key|token|secret|signature|password|auth", re.I)
_FIELD = re.compile(r"[a-z_]{1,19}")


def _decodings(text: str) -> list[str] | None:
    """`text` and each URL-decoding of it until one changes nothing; None if three decodes do not settle it."""
    levels = [text]
    for _ in range(4):
        levels.append(unquote_plus(levels[-1]))
        if levels[-1] == levels[-2]:
            return levels[:-1]
    return None


def _reason(r: httpx.Response, auth: tuple[str, str], params: dict) -> dict:
    """Plivo's own error fields (field name to message), bounded, minus any name or message that could spell a
    credential the request sent, by the rule docs/api.md states. Nothing is relayed when a credential is not text or a
    number, does not settle or decodes to under 8 characters, since HS cannot look for it reliably."""
    try:
        body = r.json()
    except ValueError:
        return {}
    sent = [*auth, r.request.headers["authorization"].partition(" ")[2]]
    sent += [v for k, v in params.items() if _CREDENTIAL.search(k)]
    forms = [_decodings(str(s)) if type(s) in (str, int, float) else None for s in sent if s != ""]
    if any(f is None or len(f[-1]) < 8 for f in forms):
        return {}
    needles = {level.casefold() for f in forms for level in f}

    def holds(text: str) -> bool:
        levels = _decodings(text)
        spelled = levels is None or any(n in level for level in map(str.casefold, levels) for n in needles)
        return spelled or any(redact(level) != level for level in levels[1:])

    out = {}
    for k, v in list(body.items())[:10] if isinstance(body, dict) else ():
        msgs = [v] if isinstance(v, str) else v if isinstance(v, list) else []
        safe = ["[REDACTED]" if holds(m) else redact(m)[:200] for m in msgs[:5] if isinstance(m, str)]
        if safe and _FIELD.fullmatch(k) and not _CREDENTIAL.search(k) and k != "api_id" and not holds(k):
            out[k] = safe if isinstance(v, list) else safe[0]
    names, messages = "".join(out), "".join("".join(v) for v in out.values())
    both = "".join(k + "".join(v) for k, v in out.items())
    return {} if any(map(holds, (names, messages, both))) else out


@cache
def _fernet() -> Fernet:
    hkdf = HKDF(SHA256(), 32, salt=None, info=b"plivo-token")
    return Fernet(base64.urlsafe_b64encode(hkdf.derive(settings.secret_key.encode())))


def encrypt(token: str) -> str:
    return _fernet().encrypt(token.encode()).decode()


def decrypt(sealed: str) -> str:
    return _fernet().decrypt(sealed.encode()).decode()


def _hmac(token: str, message: str) -> str:
    return base64.b64encode(hmac.new(token.encode(), message.encode(), hashlib.sha256).digest()).decode()


def v3(token: str, url: str, query: list, form: list, nonce: str) -> str:
    """Plivo's V3 signature: the URL, its sorted query, a dot, the sorted POST params, then the nonce."""
    q = "&".join(f"{k}={v}" for k, v in sorted(query))
    base = url + ("?" + q if q or form else "") + ("." if q and form else "") + "".join(k + v for k, v in sorted(form))
    return _hmac(token, f"{base}.{nonce}")


def v2(token: str, url: str, nonce: str) -> str:
    """Plivo's V2 signature (messaging): the URL without its query, then the nonce. It does not cover the body."""
    parts = urlsplit(url)
    return _hmac(token, f"{parts.scheme}://{parts.netloc}{parts.path}{nonce}")


class Account(NamedTuple):
    id: UUID
    plivo_auth_id: str
    plivo_token_enc: str
    plivo_app_id: str | None
    deleted: bool


def account(tenant_id, *, fresh: bool = False) -> Account | None:
    """The tenant's Plivo account through the config cache, or with `fresh` from its row, deleted or not; None for an
    id that names no tenant."""
    try:
        tid = UUID(str(tenant_id))
    except ValueError:
        return None
    t = T.tenant

    def load() -> dict | None:
        cols = (t.c.plivo_auth_id, t.c.plivo_token_enc, t.c.plivo_app_id, t.c.deleted_at.isnot(None).label("deleted"))
        with read() as db:
            row = db.execute(select(*cols).where(t.c.id == tid)).first()
        return row and dict(row._mapping)

    found = load() if fresh else json.loads(hot.cached(tid, "tenant", load))
    return found and Account(tid, **found)


async def form(request: Request) -> dict:
    return dict((await request.form()).items())


FORM = Depends(form)


async def voice_signed(request: Request) -> Account:
    """Dependency of /plivo/{tenant_id} routes: the tenant whose token signed this request (V3), checked over
    PUBLIC_BASE_URL and the raw path, since a Host header can rewrite request.url."""
    signatures = request.headers.get("x-plivo-signature-v3", "")
    nonce = request.headers.get("x-plivo-signature-v3-nonce", "")
    tenant_id = request.path_params["tenant_id"]
    tenant = await run_in_threadpool(account, tenant_id) if signatures and nonce else None
    if tenant and not tenant.deleted:
        form = (await request.form()).multi_items() if request.method == "POST" else []
        query = parse_qsl(request.scope["query_string"].decode(), keep_blank_values=True)
        url = settings.public_base_url + request.scope["path"]
        expected = v3(decrypt(tenant.plivo_token_enc), url, query, form, nonce)
        if any(hmac.compare_digest(expected, s.strip()) for s in signatures.split(",")):
            return tenant
    raise ApiError(401, "unauthorized", "bad Plivo signature")


async def text_signed(request: Request) -> Account:
    """The tenant whose token signed this text callback (V2); `TEXT` checks its nonce."""
    signature = request.headers.get("x-plivo-signature-v2", "")
    nonce = request.headers.get("x-plivo-signature-v2-nonce", "")
    tenant_id = request.path_params["tenant_id"]
    tenant = await run_in_threadpool(account, tenant_id) if signature and nonce else None
    if tenant and not tenant.deleted:
        expected = v2(decrypt(tenant.plivo_token_enc), settings.public_base_url + request.scope["path"], nonce)
        if hmac.compare_digest(expected, signature.strip()):
            return tenant
    raise ApiError(401, "unauthorized", "bad Plivo signature")


def _text_tx(request: Request, _: Account = Depends(text_signed)) -> Iterator[Connection]:
    """The transaction of a text callback. Its first statement records the V2 nonce, so a callback that fails takes
    its nonce with it and Plivo's retry runs, and a replay of one that committed is refused (401)."""
    seen = {"tenant_id": request.path_params["tenant_id"], "principal": "plivo-v2", "fingerprint": ""}
    sha = hashlib.sha256(request.headers["x-plivo-signature-v2-nonce"].encode()).hexdigest()
    with tx() as db:
        if not db.execute(insert(T.idempotency).values(**seen, key_sha256=sha).on_conflict_do_nothing()).rowcount:
            raise ApiError(401, "unauthorized", "bad Plivo signature")
        yield db


# The text callback router depends on it, so every text route runs in it; a handler takes it as `db: Connection = TEXT`.
# Function scope commits before the response: a failed commit answers 500, and Plivo retries.
TEXT = Depends(_text_tx, scope="function")


def e164(number: str) -> str:
    """Plivo sends PSTN numbers without their "+"."""
    return f"+{number}" if number.isdigit() else number


def utc(plivo_time: str | None) -> str | None:
    """A time Plivo sends in UTC, `YYYY-MM-DD HH:MM:SS`, as ISO 8601: a MultiPartyCall's, its members' and a
    recording's. A Call's hangup sends StartTime, AnswerTime and EndTime in the account's zone instead."""
    return f"{plivo_time.replace(' ', 'T')}+00:00" if plivo_time else None


def fetchable(url: str) -> bool:
    """Only Plivo's media origin or an https Plivo host gets the tenant's credentials: a callback can name any URL."""
    u, media = urlsplit(url), urlsplit(settings.plivo_media_url)
    return (u.scheme, u.netloc) == (media.scheme, media.netloc) or (
        u.scheme == "https" and (u.hostname or "").endswith(".plivo.com")
    )


def url(tenant_id: str, path: str, **query) -> str:
    """An absolute /plivo/{tenant_id}/{path} callback URL; None values are dropped."""
    q = urlencode({k: v for k, v in query.items() if v is not None})
    return f"{settings.public_base_url}/plivo/{tenant_id}/{path}" + (f"?{q}" if q else "") + RETRY


def _element(tag: str, attrs: dict, *children) -> ET.Element:
    values = {k: str(v).lower() if isinstance(v, bool) else str(v) for k, v in attrs.items() if v is not None}
    element = ET.Element(tag, values)
    for child in children:
        if isinstance(child, tuple):
            element.append(_element(*child))
        else:
            element.text = str(child)
    return element


def xml(*elements: tuple) -> Response:
    """A Plivo XML document from `(tag, attrs, *children)` tuples; a str child is the element's text."""
    root = ET.Element("Response")
    root.extend(_element(*e) for e in elements)
    return Response(ET.tostring(root, encoding="unicode"), media_type="text/xml")


def register_app(tenant: Row, **extra) -> str:
    """Point the tenant's Plivo Application at HS, creating it on first use; returns its app id."""
    base = f"{settings.public_base_url}/plivo/{tenant.id}"
    urls = {"answer_url": f"{base}/answer", "answer_method": "POST", "hangup_url": f"{base}/hangup"}
    urls |= {"hangup_method": "POST", "message_url": f"{base}/message", "message_method": "POST"} | extra
    if tenant.plivo_app_id:
        rest(tenant, "POST", f"Application/{tenant.plivo_app_id}/", **urls)
        return tenant.plivo_app_id
    return rest(tenant, "POST", "Application/", app_name=f"hs-{tenant.id}", **urls)["app_id"]
