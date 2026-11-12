"""Who is calling and what they may touch: the operator key, tenant keys, agent and stream tokens, agent passwords,
plugin resolvers, grants, tenant-scoped conversations and body ids, and Idempotency-Key on /api/v1 writes."""

import base64
import hashlib
import hmac
import json
import logging
import re
import secrets
import uuid
from collections.abc import Callable, Iterable
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Literal, NamedTuple
from uuid import UUID

from fastapi import APIRouter, Depends, Request, Response, params
from fastapi.concurrency import run_in_threadpool
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, Field, TypeAdapter
from sqlalchemy import func, select, text, update
from sqlalchemy.engine import Connection, Row
from starlette.routing import Match

from hs import core
from hs.core import AGENT_PREFIX, KEY_PREFIX, PUBLIC, STREAM_PREFIX, ApiError, Page, Paging, T, page, paging, read, tx

log = logging.getLogger(__name__)
router = APIRouter()
SCOPES = ("admin", "sessions", "calls", "reports", "events", "recordings", "messages")
ROLES = {"sup": ("admin", "supervisor"), "admin": ("admin",)}
GRANTS = {"op", "agent", "stream", *ROLES, *(f"key:{s}" for s in SCOPES)}
# A response naming one of these fields, or carrying an HS token, is a secret: never stored, never replayed.
SECRET = re.compile(rb'"(?:agent_token|password|api_key|secret)"\s*:|hs_(?:tk|at|st)_')


@dataclass(frozen=True)
class Principal:
    tenant_id: str
    kind: str  # op, key, agent, stream, or what a plugin resolver names
    key_id: str | None = None
    agent_id: str | None = None
    role: str | None = None  # an agent's: admin, supervisor or agent
    scopes: frozenset[str] = frozenset()
    credential_id: str | None = field(default=None, compare=False)


# The operator belongs to no tenant; the nil uuid matches no tenant's rows.
OPERATOR = Principal(str(uuid.UUID(int=0)), "op")
# Plugin resolvers, asked in order for a caller that carries no HS credential.
RESOLVERS: list[Callable[[Request], Principal | None]] = []

# A live credential and, for an agent's, its agent document `a`; the caller adds the match on c.sha256.
CREDENTIALS = (
    " FROM credential c JOIN tenant t ON t.id = c.tenant_id AND t.deleted_at IS NULL"
    " LEFT JOIN config a ON a.id = c.agent_id AND a.tenant_id = c.tenant_id AND a.kind = 'agent'"
    " WHERE c.revoked_at IS NULL AND (c.expires_at IS NULL OR c.expires_at > now())"
    " AND (c.kind = 'key' OR a.id IS NOT NULL)"
)
_CREDENTIAL = text(
    "SELECT c.id, c.tenant_id, c.kind, c.scopes, c.agent_id, a.body->>'role' AS role"
    + CREDENTIALS
    + " AND c.sha256 = :sha"
)


def digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def resolve(token: str, request: Request | None = None) -> Principal | None:
    """The caller, or None. The operator key, then HS's own credentials; an hs_ token is HS's alone, so one that is
    unknown, revoked, expired, or of a deleted tenant or agent never reaches a plugin. Any other caller goes to the
    resolvers in order: a Principal accepts, None declines to the next, anything else rejects."""
    if token and hmac.compare_digest(token.encode(), core.settings.operator_key.encode()):
        return OPERATOR
    if token.startswith(core.OURS):
        with read() as db:
            row = db.execute(_CREDENTIAL, {"sha": digest(token)}).first()
        if row is None:
            return None
        key_id, agent_id = str(row.id) if row.kind == "key" else None, row.agent_id and str(row.agent_id)
        return Principal(str(row.tenant_id), row.kind, key_id, agent_id, row.role, frozenset(row.scopes), str(row.id))
    for resolver in RESOLVERS if request is not None else ():
        try:
            found = resolver(request)
            if not isinstance(found, Principal | None):
                raise TypeError(f"a resolver returned {type(found).__name__}, not a Principal")
        except Exception as e:
            # Names only: a plugin's exception text or traceback can hold the credential it was handed.
            log.error(
                "resolver %s rejected the caller: %s",
                getattr(resolver, "__qualname__", type(resolver).__qualname__),
                type(e).__name__,
            )
            return None
        if found is not None:
            return found
    return None


def bearer(request: Request) -> str:
    scheme, _, token = request.headers.get("authorization", "").partition(" ")
    return token.strip() if scheme.lower() == "bearer" else ""


def credential(request: Request, stream: bool) -> str:
    """The token a caller presents: its bearer, else on a stream route a query stream token, since EventSource
    cannot send headers. The stream route reads it here too, so it checks the very credential `require` took."""
    token = bearer(request)
    if not token and stream:
        token = request.query_params.get("access_token", "")
        token = token if token.startswith(STREAM_PREFIX) else ""
    return token


def holds_grant(p: Principal, grant: str) -> bool:
    if grant.startswith("key:"):
        return p.kind == "key" and grant.removeprefix("key:") in p.scopes
    if grant in ROLES:
        return p.kind == "agent" and p.role in ROLES[grant]
    return p.kind == grant


def require(*grants: str) -> params.Depends:
    """The caller's Principal when it holds one of `grants` (the route table's codes): 401 for a missing or unknown
    credential, 403 for one this route does not take."""
    if not grants or not set(grants) <= GRANTS:
        raise ValueError(f"unknown grants {grants}")

    def principal(request: Request) -> Principal:
        if (p := resolve(credential(request, "stream" in grants), request)) is None:
            raise ApiError(401, "unauthorized", "missing or invalid credential")
        if not any(holds_grant(p, grant) for grant in grants):
            raise ApiError(403, "forbidden", "this credential cannot use this route")
        return p

    return Depends(principal)


_ADMIN = text("SELECT 1" + CREDENTIALS + " AND c.id = :id AND a.body->>'role' = 'admin'")


def still_admin(db: Connection, p: Principal) -> None:
    """401 unless `p`'s HS agent token is still live and its agent still an admin. Call it after taking the lock a
    demotion holds, so a demotion the caller waited for wins. Keys and plugin callers are not checked here."""
    if p.kind == "agent" and p.credential_id and db.execute(_ADMIN, {"id": p.credential_id}).first() is None:
        raise ApiError(401, "unauthorized", "missing or invalid credential")


LEVELS = ("org", "team", "self")
_DOWNLOADS = dict.fromkeys(("download_list", "download_conversation", "download_recording"), True)
PERMISSIONS = {
    "admin": {"view": "org", **_DOWNLOADS, "monitor": True, "barge": True},
    "supervisor": {"view": "team", **_DOWNLOADS, "monitor": True, "barge": True},
    "agent": {"view": "self", **_DOWNLOADS, "monitor": False, "barge": False},
}
# For agent document `a` of tenant `t` and the role in {role}: the role's grants, and at the team level the agent's
# teams and the agents sharing one. A statement binds :permissions to PERMISSIONS, for settings that never named them.
_GRANTS = "coalesce(t.settings->'permissions', CAST(:permissions AS jsonb))->({role})"
_TEAM = f"{_GRANTS}->>'view' = 'team'"
_TEAMS = "ARRAY(SELECT jsonb_array_elements_text(a.body->'teams'))"
SCOPE = (
    f"{_GRANTS} AS grants, ARRAY(SELECT jsonb_array_elements_text(a.body->'teams') WHERE {_TEAM}) AS teams,"  # noqa: S608
    f" ARRAY(SELECT m.id::text FROM config m WHERE {_TEAM} AND m.kind = 'agent' AND m.tenant_id = a.tenant_id"
    f" AND m.body->'teams' ?| {_TEAMS}) AS mates"
)
# One row per (agent, tenant, role) of the arrays, in their order, with the `t` and `a` that SCOPE reads.
WHO = (
    " FROM unnest(CAST(:agents AS uuid[]), CAST(:tenants AS uuid[]), CAST(:roles AS text[])) WITH ORDINALITY"
    " x (agent, tenant, role, n) LEFT JOIN tenant t ON t.id = x.tenant"
    " LEFT JOIN config a ON a.id = x.agent AND a.tenant_id = x.tenant AND a.kind = 'agent'"
)
_QUEUED = (
    f", ARRAY(SELECT q.id::text FROM config q WHERE {_TEAM} AND q.kind = 'queue' AND q.tenant_id = a.tenant_id"  # noqa: S608
    f" AND q.body->'teams' ?| {_TEAMS}) AS queues"
)
_SEEN = text("SELECT " + (SCOPE + _QUEUED).format(role="x.role") + WHO).bindparams(permissions=json.dumps(PERMISSIONS))
ROUTED = (
    "CASE WHEN c.routing ? 'route' THEN c.routing->'route'->'teams' ?| CAST(:teams AS text[])"
    " ELSE c.queue_id = ANY(CAST(:queues AS uuid[])) END"
)
_VISIBLE = text(
    "SELECT c.*, (:all OR EXISTS (SELECT 1 FROM assignment a WHERE a.conversation_id = c.id"  # noqa: S608
    " AND a.agent_id = ANY(CAST(:seen AS uuid[])) AND a.state = 'active')) AS controls"
    " FROM conversation c WHERE c.id = :cid AND c.tenant_id = :tenant AND (:all OR EXISTS ("
    " SELECT 1 FROM assignment a WHERE a.conversation_id = c.id AND a.agent_id = ANY(CAST(:seen AS uuid[]))"
    " AND (a.state = 'active' OR a.started_at IS NOT NULL))"
    " OR (:offer AND (EXISTS (SELECT 1 FROM jobs j WHERE j.kind = 'offer_timeout'"
    " AND j.payload->>'conversation_id' = c.id::text AND j.payload->>'agent_id' = ANY(CAST(:seen AS text[]))"
    " AND j.run_at > now() AND j.attempts = 0) OR EXISTS (SELECT 1 FROM assignment a WHERE a.conversation_id = c.id"
    " AND a.agent_id = CAST(:me AS uuid) AND a.state = 'offered' AND a.via = 'outbound')))"
    " OR (" + ROUTED + " AND NOT EXISTS (SELECT 1 FROM assignment a"
    " WHERE a.conversation_id = c.id AND a.agent_id IS NOT NULL AND a.started_at IS NOT NULL)))"
)
_LOCK = text("SELECT 1 FROM conversation WHERE id = :cid AND tenant_id = :tenant FOR NO KEY UPDATE")


class Seen(NamedTuple):
    """What a caller sees by its role's view level, and its role's grants. `agents` is None for the whole tenant (a
    key, or the org level); at the team level it is the caller and the agents sharing one of its `teams`, and the
    caller also sees what no agent took in the `queues` routing to one; at the self level it is the caller alone."""

    grants: dict
    agents: frozenset[str] | None
    teams: frozenset[str]
    queues: list[str]

    @classmethod
    def of(cls, p: Principal, grants: dict | None, mates: list, teams: list, queues: list = ()) -> "Seen":
        whole = p.kind == "key" or (grants or {}).get("view") == "org"
        agents = None if whole else frozenset({p.agent_id, *mates} - {None})
        return cls(grants or {}, agents, frozenset(teams), list(queues))


def seen(db: Connection, p: Principal) -> Seen:
    """`p`'s Seen, from the tenant's settings as they are now: none for a key, else one statement once per
    transaction."""
    if p.kind == "key":
        return Seen.of(p, None, [], [])
    if (memo := db.info.get("memo")) is None:
        memo = {}
    if (key := ("seen", p)) not in memo:
        r = db.execute(_SEEN, {"agents": [p.agent_id], "tenants": [p.tenant_id], "roles": [p.role]}).one()
        memo[key] = Seen.of(p, r.grants, r.mates, r.teams, r.queues)
    return memo[key]


def visible_agents(db: Connection, p: Principal) -> frozenset[str] | None:
    """The agents whose work `p` may see, None for all of the tenant's (`Seen.agents`)."""
    return seen(db, p).agents


def visible_queues(db: Connection, p: Principal) -> list[str]:
    return seen(db, p).queues


def permits(db: Connection, p: Principal, grant: str) -> bool:
    return p.kind == "key" or seen(db, p).grants.get(grant) is True


def conversation(
    db: Connection, p: Principal, cid: UUID, *, lock: bool = False, control: bool = False, offer: bool = False
) -> Row:
    """The conversation when `p` may see it, else 404, another tenant's included: one an agent of `visible_agents`
    is active on, or for a read without `control`, ever was or no agent took on a route to one of its teams
    (`ROUTED`). `offer`, which only reads pass, adds one it holds an offer on before its ring timeout, and the
    caller's own dial before its leg answers. `lock` takes the row lock first, as `conversations.lock` does, so the
    check holds for the rest of the transaction."""
    s = seen(db, p)
    args = {"cid": cid, "tenant": p.tenant_id, "all": s.agents is None, "teams": sorted(s.teams), "offer": offer}
    args |= {"seen": sorted(s.agents or ()), "queues": s.queues, "me": p.agent_id}
    if lock:
        db.execute(_LOCK, args)
    if (row := db.execute(_VISIBLE, args).first()) is None:
        raise ApiError(404, "conversation_not_found", "no such conversation")
    return controlled(row) if control else row


def controlled(conv: Row) -> Row:
    """`conv`, as `conversation` read it, when its caller may control it, else 404: the `control` check, later in a
    transaction that holds the lock."""
    if not conv.controls:
        raise ApiError(404, "conversation_not_found", "no such conversation")
    return conv


def hung_up(conv: Row) -> dict | None:
    """The mark of the agent's API hangup since a queue or a transfer's successor last took the call (the room its
    teardown ends, and the legs it ends there), else None. The call takes no more controls, and an end of it no
    party caused reads ended_by agent. A queue's routing starts afresh, without this mark."""
    return conv.routing.get("hung_up")


def owned(db: Connection, p: Principal, kind: str, ids: Iterable[UUID]) -> None:
    """422 foreign_id unless every id names a `kind` document of the caller's tenant."""
    if not (wanted := set(ids)):
        return
    C = T.config
    found = select(C.c.id).where(C.c.tenant_id == p.tenant_id, C.c.kind == kind, C.c.id.in_(wanted))
    if missing := wanted - set(db.execute(found).scalars()):
        details = {"kind": kind, "ids": sorted(map(str, missing))}
        raise ApiError(422, "foreign_id", "an id in the body names nothing of this tenant", details)


SCRYPT = {"n": 2**14, "r": 8, "p": 1}


def hashed(password: str) -> str:
    salt = secrets.token_bytes(16)
    key = hashlib.scrypt(password.encode(), salt=salt, dklen=32, **SCRYPT)
    return "$".join(["scrypt", *map(str, SCRYPT.values()), *(base64.b64encode(b).decode() for b in (salt, key))])


def matches(password: str, stored: str) -> bool:
    """Whether `password` is the one `stored`, from `hashed`, holds, by the cost `stored` was made with."""
    _, n, r, p, salt, key = stored.split("$")
    want = base64.b64decode(key)
    got = hashlib.scrypt(password.encode(), salt=base64.b64decode(salt), n=int(n), r=int(r), p=int(p), dklen=len(want))
    return hmac.compare_digest(got, want)


DUMMY_HASH = hashed(secrets.token_urlsafe(32))


class Tokens(NamedTuple):
    agent_token: str
    stream_token: str
    expires_at: datetime
    renewable_until: datetime


def mint(db: Connection, tenant_id: str, agent_id: str, renewable_until: datetime | None = None) -> Tokens:
    """A fresh agent token and stream token, valid an hour; only their SHA-256 is kept. Their agent token mints the
    next session until `renewable_until`, 12 hours from now for the first session of a chain."""
    now = datetime.now(UTC)
    agent, stream = (prefix + secrets.token_urlsafe(32) for prefix in (AGENT_PREFIX, STREAM_PREFIX))
    tokens = Tokens(agent, stream, now + timedelta(hours=1), renewable_until or now + timedelta(hours=12))
    rows = [
        {"sha256": digest(token), "tenant_id": tenant_id, "kind": kind, "agent_id": agent_id}
        | {"expires_at": tokens.expires_at, "renewable_until": tokens.renewable_until}
        for kind, token in (("agent", tokens.agent_token), ("stream", tokens.stream_token))
    ]
    db.execute(T.credential.insert(), rows)
    return tokens


def new_key(db: Connection, tenant_id: str, scopes: Iterable[str] = SCOPES) -> dict:
    """A tenant API key; the reply is the only place its secret ever appears."""
    K, key = T.credential, KEY_PREFIX + secrets.token_urlsafe(32)
    values = {"sha256": digest(key), "tenant_id": tenant_id, "kind": "key", "prefix": key[:12]}
    made = K.insert().values(**values, scopes=sorted(set(scopes)))
    row = db.execute(made.returning(K.c.id, K.c.prefix, K.c.scopes, K.c.created_at)).one()
    return {**row._mapping, "id": str(row.id), "api_key": key}


def revoke(db: Connection, tenant_id: str, *, agent_id: str | None = None, key_id: str | None = None) -> int:
    """Revoke the agent's tokens, or the key `key_id`, or with neither every credential of the tenant; open streams
    close at their next check. Returns how many were live."""
    K = T.credential
    q = update(K).where(K.c.tenant_id == tenant_id, K.c.revoked_at.is_(None)).values(revoked_at=func.now())
    if agent_id:
        q = q.where(K.c.agent_id == agent_id)
    if key_id:
        q = q.where(K.c.id == key_id, K.c.kind == "key")
    return db.execute(q).rowcount


class KeyIn(BaseModel):
    scopes: list[Literal[SCOPES]] = Field(min_length=1)


class Key(BaseModel):
    id: str
    prefix: str
    scopes: list[str]
    created_at: datetime


class MintedKey(Key):
    api_key: str


OP = require("op")


def live_tenant(db: Connection, tenant_id) -> Row:
    """The tenant's row; 404 for an unknown or deleted one."""
    t = T.tenant
    if (row := db.execute(select(t).where(t.c.id == tenant_id, t.c.deleted_at.is_(None))).first()) is None:
        raise ApiError(404, "tenant_not_found", "no such tenant")
    return row


@router.post("/tenants/{tenant_id}/api-keys", status_code=201, response_model=MintedKey)
def mint_key(tenant_id: UUID, body: KeyIn, _: Principal = OP):
    with tx() as db:
        live_tenant(db, tenant_id)
        key = new_key(db, tenant_id, body.scopes)
        created(db, key["id"])
        return key


@router.get("/tenants/{tenant_id}/api-keys", response_model=Page[Key])
def list_keys(tenant_id: UUID, _: Principal = OP, p: Paging = Depends(paging)):
    try:
        after = UUID(p.after) if p.after else None
    except ValueError as e:
        raise ApiError(422, "invalid_cursor", "cursor is not one this API returned") from e
    K = T.credential
    q = select(K.c.id, K.c.prefix, K.c.scopes, K.c.created_at)
    q = q.where(K.c.tenant_id == tenant_id, K.c.kind == "key", K.c.revoked_at.is_(None))
    with read() as db:
        live_tenant(db, tenant_id)
        rows = db.execute((q.where(K.c.id > after) if after else q).order_by(K.c.id).limit(p.limit + 1)).all()
    return page([{**r._mapping, "id": str(r.id)} for r in rows], p)


@router.delete("/tenants/{tenant_id}/api-keys/{key_id}", status_code=204)
def revoke_key(tenant_id: UUID, key_id: UUID, _: Principal = OP) -> Response:
    with tx() as db:
        if not revoke(db, tenant_id, key_id=key_id):
            raise ApiError(404, "api_key_not_found", "no such API key")
    return Response(status_code=204)


# The idempotency slot of the request being served: tenant_id, principal, key_sha256, the fingerprint, and the
# token, the created_at it claimed the row with, which a takeover renews.
CLAIM: ContextVar[dict | None] = ContextVar("idempotency_claim", default=None)
# "METHOD /route/path" -> the success body of a create, rebuilt from its resource id for a takeover after a crash.
VIEWS: dict[str, Callable[[Connection, Principal, str], dict]] = {}
# A key's secret cannot be rebuilt, so the secret rule answers that takeover with the key's id.
VIEWS[f"POST {PUBLIC}/tenants/{{tenant_id}}/api-keys"] = lambda db, p, rid: {"id": rid, "api_key": None}

_CLAIM = text(
    "INSERT INTO idempotency (tenant_id, principal, key_sha256, fingerprint)"
    " VALUES (:tenant_id, :principal, :key_sha256, :fingerprint) ON CONFLICT DO NOTHING RETURNING created_at"
)
# Seconds after which an unanswered claim belongs to a process that died.
TAKEOVER = 120
# `body` keeps the exact response text as a JSON string; NULL marks a secret.
_HELD = text(
    "SELECT fingerprint, status, body #>> '{}' AS body, resource_id,"
    " created_at < now() - make_interval(secs => :takeover) AS stale FROM idempotency"
    " WHERE tenant_id = :tenant_id AND principal = :principal AND key_sha256 = :key_sha256 FOR UPDATE"
)
_RETAKE = text(
    "UPDATE idempotency SET created_at = now()"
    " WHERE tenant_id = :tenant_id AND principal = :principal AND key_sha256 = :key_sha256 RETURNING created_at"
)
_CREATED = text(
    "UPDATE idempotency SET resource_id = :resource_id"
    " WHERE tenant_id = :tenant_id AND principal = :principal AND key_sha256 = :key_sha256 AND created_at = :token"
)
_STORE = text(
    "UPDATE idempotency SET status = :status, body = CAST(:body AS jsonb), resource_id = coalesce(resource_id, :rid)"
    " WHERE tenant_id = :tenant_id AND principal = :principal AND key_sha256 = :key_sha256 AND created_at = :token"
)
_FREE = text(
    "DELETE FROM idempotency WHERE tenant_id = :tenant_id AND principal = :principal AND key_sha256 = :key_sha256"
    " AND created_at = :token AND status IS NULL AND resource_id IS NULL"
)


def created(db: Connection, resource_id: str) -> None:
    """Call in the route's own transaction once it has made `resource_id`. The request's answer, a refusal included,
    is then its outcome for good, and a retry after a crash answers from the route's VIEWS entry; neither runs it
    again."""
    if (claim := CLAIM.get()) is not None and not db.execute(_CREATED, claim | {"resource_id": resource_id}).rowcount:
        raise ApiError(409, "idempotency_in_progress", "a later request with this Idempotency-Key took it over")


def claimed() -> dict | None:
    """The claim as a JSON-safe command payload field."""
    claim = CLAIM.get()
    return claim and {k: str(claim[k]) for k in ("tenant_id", "principal", "key_sha256", "token")}


def refused(db: Connection, error: ApiError, claim: dict | None) -> None:
    """Record, in the transaction that stores a command's refusal, that refusal as the stored answer of the request
    that scheduled it (`claim`, from `claimed`), so it replays though that request died before answering."""
    if claim is not None:
        shown = {"code": error.code, "message": error.message, "details": error.details}
        sent = json.dumps({"error": shown, "request_id": core.REQUEST_ID.get()}, separators=(",", ":"))
        db.execute(_STORE, claim | {"status": error.status, "body": json.dumps(sent), "rid": None})


def _refuse(request: Request, code: str, message: str, details: dict | None = None) -> Response:
    return core.render_error(request, 409, code, message, details)


def _claim(request: Request, p: Principal, claim: dict) -> Response | None:
    """None when this request runs the route (a new claim, or the takeover of a dead one), else its answer."""
    with tx() as db:
        if (token := db.execute(_CLAIM, claim).scalar()) is not None:
            claim["token"] = token
            return None
        held = db.execute(_HELD, claim | {"takeover": TAKEOVER}).first()
        if held is not None and held.fingerprint != claim["fingerprint"]:
            return _refuse(request, "idempotency_conflict", "this Idempotency-Key was used with another request")
        if held is None or (held.status is None and not held.stale):
            return _refuse(request, "idempotency_in_progress", "the first request with this key is still running")
        if held.status is None and held.resource_id is None:
            # Age alone marks a claim dead, so a request still running after 120 s overlaps its takeover;
            # the token stops only its `created` and its stored answer, so a route that never calls `created` runs twice.
            claim["token"] = db.execute(_RETAKE, claim).scalar()
            return None
        if held.status is None:
            route = next(r for r in request.app.routes if r.matches(request.scope)[0] is Match.FULL)
            view = VIEWS[f"{request.method} {route.path}"](db, p, held.resource_id)
            if not SECRET.search(json.dumps(jsonable_encoder(view)).encode()):
                shown = TypeAdapter(route.response_model).validate_python(view) if route.response_model else view
                sent = json.dumps(jsonable_encoder(shown)).encode()
                return Response(sent, route.status_code or 200, media_type="application/json")
        if held.body is None:  # a secret, stored or rebuilt
            msg = "the response held a secret and is not replayed"
            return _refuse(request, "idempotency_replay_secret", msg, {"id": held.resource_id})
        return Response(held.body, held.status, media_type="application/json" if held.body else None)


def _store(claim: dict, status: int, sent: bytes) -> None:
    secret = SECRET.search(sent) is not None
    try:
        doc = json.loads(sent) if secret else {}
    except ValueError:
        doc = {}
    rid = (doc.get("id") or doc.get("tenant_id")) if isinstance(doc, dict) else None
    body = None if secret else json.dumps(sent.decode(errors="replace"))
    with tx() as db:
        db.execute(_STORE, claim | {"status": status, "body": body, "rid": rid and str(rid)})


def _free(claim: dict) -> int:
    with tx() as db:
        return db.execute(_FREE, claim).rowcount


async def idempotency(request: Request, call_next) -> Response:
    """HTTP middleware: Idempotency-Key on /api/v1 writes, per tenant, credential kind and identity (a key, or an
    agent across its agent tokens) and key. A repeat answers the stored response; a different body is 409; a
    response that held a secret keeps only its status and id and replays as 409. An error answer or a crash frees the
    key only if the route had not yet called `created`."""
    key = request.headers.get("idempotency-key")
    if not key or request.method not in ("POST", "PUT", "PATCH", "DELETE") or not request.url.path.startswith(PUBLIC):
        return await call_next(request)
    if (p := await run_in_threadpool(resolve, bearer(request), request)) is None:
        return await call_next(request)
    target = f"{request.method} {request.scope['path']}?{request.scope['query_string'].decode()} "
    # The caller's grants too, so one whose role or scopes changed since gets 409, never the stored answer.
    grants = f"{p.role} {','.join(sorted(p.scopes))} "
    fingerprint = hashlib.sha256((target + grants).encode() + await request.body()).hexdigest()
    who = p.key_id or p.agent_id
    if not who and p.kind != "op":
        msg = "this caller has no stable identity to hold an Idempotency-Key"
        return core.render_error(request, 422, "idempotency_unsupported", msg)
    # The kind too, so a stream token never reaches its agent's stored responses.
    claim = {"tenant_id": p.tenant_id, "principal": f"{p.kind}:{who or ''}", "key_sha256": digest(key)}
    claim["fingerprint"] = fingerprint
    if (answer := await run_in_threadpool(_claim, request, p, claim)) is not None:
        return answer
    CLAIM.set(claim)
    try:
        response = await call_next(request)
    except BaseException:
        await run_in_threadpool(_free, claim)
        raise
    if response.status_code >= 400 and await run_in_threadpool(_free, claim):
        return response
    sent = b"".join([chunk async for chunk in response.body_iterator])
    await run_in_threadpool(_store, claim, response.status_code, sent)
    return Response(sent, response.status_code, headers=dict(response.headers))
