"""Who is calling: the operator key, tenant keys, agent and stream tokens, and Plivo's V3 signature; and what
the caller may touch: tenant-scoped ids and calls.

Routes declare `Depends(require(...))`; the event stream re-checks its token with `resolve` about once a second.
"""

import base64
import hashlib
import hmac
import json
import logging
import re
import secrets
import time
import uuid
from dataclasses import dataclass
from functools import cache
from typing import NamedTuple
from urllib.parse import parse_qsl

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives.hashes import SHA256
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from fastapi import Request
from fastapi.concurrency import run_in_threadpool
from sqlalchemy import TIMESTAMP, Column, ForeignKey, Integer, Table, Text, bindparam, func, text
from sqlalchemy.dialects.postgresql import ARRAY, UUID

from hs.errors import ApiError
from hs.infra.db import DBBase, core_engine
from hs.infra.redis import Cache
from hs.infra.settings import settings

SCOPES = ("admin", "sessions", "calls", "reports", "events", "recordings")
KEY_PREFIX, AGENT_PREFIX, STREAM_PREFIX = "hs_tk_", "hs_at_", "hs_st_"
TOKEN_TTL = 3600
# How far each role sees other agents' calls: fixed RBAC levels, as a constant.
LEVEL = {"admin": "org", "supervisor": "team", "agent": "self"}
CODES = {"op", "agent", "sup", "stream", "plivo", *(f"key:{s}" for s in SCOPES)}

api_keys = Table(
    "api_keys",
    DBBase.metadata,
    Column("id", UUID, primary_key=True, server_default=text("gen_random_uuid()")),
    Column("organization", Integer, ForeignKey("organization.id"), nullable=False, index=True),
    Column("prefix", Text, nullable=False),
    Column("sha256", Text, nullable=False, unique=True),
    Column("scopes", ARRAY(Text), nullable=False),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=func.now()),
    Column("revoked_at", TIMESTAMP(timezone=True)),
)
_KEY = text(
    "SELECT k.id::text, k.scopes, o.uuid::text, o.id FROM api_keys k JOIN organization o ON o.id = k.organization"
    " WHERE k.sha256 = :sha AND k.revoked_at IS NULL AND o.is_deleted IS NOT TRUE AND o.is_active"
)
_TENANT = text(
    "SELECT id, auth_id, plivo_token_enc FROM organization"
    " WHERE uuid = :tenant AND is_deleted IS NOT TRUE AND is_active"
)
_CALL = text(
    "SELECT cp.id, cp.conversation_number, cp.contact_uuid, o.name, o.auth_id FROM conversation_parent cp"
    " JOIN organization o ON o.id = cp.organization"
    " WHERE cp.uuid = :id AND cp.organization = :org AND cp.is_deleted IS NOT TRUE"
)
# The agent and, for a team-level role, its teammates: is any of them on the conversation (with `live`: on it
# now, not left, removed or in wrap-up)?
_ON_CALL = text(
    "WITH seen AS (SELECT id FROM account_org_mapping WHERE uuid = :agent UNION SELECT mate.aom"
    " FROM aom_team me JOIN aom_team mate ON mate.team = me.team AND mate.is_deleted IS NOT TRUE"
    " WHERE :team AND me.is_deleted IS NOT TRUE AND me.aom = (SELECT id FROM account_org_mapping WHERE uuid = :agent))"
    " SELECT EXISTS (SELECT 1 FROM conversation WHERE parent = :parent AND primary_aom IN (SELECT id FROM seen)"
    " AND (NOT :live OR (is_deleted IS NOT TRUE AND status IN ('open', 'on_hold', 'waiting')))"
    " UNION ALL SELECT 1 FROM conversation_secondary_aom_mapping WHERE conversation = :parent"
    " AND aom IN (SELECT id FROM seen) AND (NOT :live OR (is_deleted IS NOT TRUE AND secondary_agent_state NOT IN"
    " ('participant_did_not_answered', 'participant_removed', 'participant_removed_conference'))))"
)
# One atomic step, so a session minted meanwhile is not left unindexed.
_REVOKE = (
    "for _, k in ipairs(redis.call('SMEMBERS', KEYS[1])) do redis.call('DEL', k) end return redis.call('DEL', KEYS[1])"
)


def _ids(sql: str):
    return text(sql).bindparams(bindparam("ids", expanding=True))


AGENTS = _ids(
    "SELECT uuid::text, id FROM account_org_mapping WHERE organization = :org AND is_deleted IS NOT TRUE AND uuid IN :ids"
)
TEAMS = _ids("SELECT uuid::text, id FROM team WHERE organization = :org AND is_deleted IS NOT TRUE AND uuid IN :ids")
SKILLS = _ids("SELECT uuid::text, id FROM skills WHERE organization = :org AND is_deleted IS NOT TRUE AND uuid IN :ids")
# Each name character may arrive percent-encoded (`%73ig=` reads as `sig=`); the line itself stays raw, so an
# encoded CR or LF cannot split it.
_SECRET_NAMES = "|".join("".join(f"(?:{c}|%{ord(c):02x})" for c in n) for n in ("access_token", "sig"))
_SECRET_QUERY = re.compile(rf"(?<![\w%])((?:{_SECRET_NAMES})=)[^&\s\"]+", re.IGNORECASE)
# A Browser SDK login registers the SIP user `<sub>_<iss>`; an endpoint username is alphanumeric, never with "_".
_SDK_USER = re.compile(r"^((?:sip:)?[A-Za-z0-9]+)_[A-Za-z0-9]+(?=@phone\.plivo\.com)")


@dataclass(frozen=True)
class Principal:
    tenant_id: str  # organization.uuid; "" for the operator
    org_id: int  # organization.id
    kind: str  # operator | key | agent | stream | plivo
    scopes: frozenset[str] = frozenset()
    agent_id: str | None = None  # account_org_mapping.uuid
    role: str | None = None  # admin | supervisor | agent
    key_id: str | None = None


class Call(NamedTuple):
    conversation_id: str
    conversation_number: str
    call_uuid: str  # the caller's CallUUID, which the voice engine keys every call on
    req_meta: dict


def redis_client():
    return Cache.get_instance().client


def as_uuid(value) -> str | None:
    """The canonical text of a UUID, else None: ids are compared on the uuid columns, never as text."""
    try:
        return str(uuid.UUID(str(value)))
    except ValueError:
        return None


def digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _token_key(kind: str, token: str) -> str:
    return f"{'AGT' if kind == 'agent' else 'STR'}:{digest(token)}"


def _sessions(agent_id: str) -> str:
    return f"AGS:{agent_id}"


def _ring(endpoint: str) -> str:
    return f"RING:{endpoint}"


def close_tenant(tenant_id: str) -> None:
    """Refuse the tenant's agent and stream tokens until the last of them would have expired anyway."""
    redis_client().set(f"TOFF:{tenant_id}", 1, ex=TOKEN_TTL)


def resolve(token: str) -> Principal | None:
    """The principal behind a bearer token; None when it is unknown, expired, revoked or its tenant deleted."""
    if hmac.compare_digest(token.encode(), settings.operator_key.encode()):
        return Principal("", 0, "operator")
    if token.startswith(KEY_PREFIX):
        with core_engine.connect() as conn:
            row = conn.execute(_KEY, {"sha": digest(token)}).first()
        return Principal(row[2], row[3], "key", frozenset(row[1]), key_id=row[0]) if row else None
    kind = {AGENT_PREFIX: "agent", STREAM_PREFIX: "stream"}.get(token[: len(AGENT_PREFIX)])
    raw = redis_client().get(_token_key(kind, token)) if kind else None
    if raw is None:
        return None
    s = json.loads(raw)
    if redis_client().exists(f"TOFF:{s['tenant_id']}"):
        return None
    return Principal(s["tenant_id"], s["org_id"], kind, agent_id=s["agent_id"], role=s["role"])


def _grants(p: Principal, code: str) -> bool:
    if code.startswith("key:"):
        return p.kind == "key" and code.removeprefix("key:") in p.scopes
    if code == "sup":
        return p.kind == "agent" and LEVEL.get(p.role) in ("org", "team")
    return p.kind == {"op": "operator"}.get(code, code)


def bearer(request: Request) -> str:
    scheme, _, token = request.headers.get("authorization", "").partition(" ")
    return token.strip() if scheme.lower() == "bearer" else ""


def require(*auth: str, secret: bool = False):
    """Dependency: the caller's Principal when its credential is one of `auth` (the route table's codes); 401 for
    a missing or bad credential, 403 for a good one this route does not take. `secret`: the response carries a
    secret, so an idempotent replay answers 409 with the resource id only."""
    if not auth or not set(auth) <= CODES:
        raise ValueError(f"unknown auth codes {auth}")

    async def principal(request: Request) -> Principal:
        if secret:
            request.state.secret = True
        if "plivo" in auth and "tenant_id" in request.path_params:
            return await plivo(request, request.path_params["tenant_id"])
        token = bearer(request)
        if not token and "stream" in auth:
            # EventSource cannot send headers, so only the stream token may ride in the query.
            token = request.query_params.get("access_token", "")
            token = token if token.startswith(STREAM_PREFIX) else ""
        found = await run_in_threadpool(resolve, token) if token else None
        if found is None:
            raise ApiError(401, "unauthorized", "missing or invalid credential")
        if not any(_grants(found, code) for code in auth):
            raise ApiError(403, "forbidden", "this credential cannot use this route")
        return found

    return principal


# Every /plivo/{tenant_id} route runs this; a route that needs the tenant declares it too (FastAPI caches it).
plivo_signed = require("plivo")


def v3_signature(auth_token: str, url: str, query: list, form: list, nonce: str) -> str:
    """Plivo's V3 signature (the SDK's algorithm): HMAC-SHA256 over the URL, its sorted query and the sorted
    POST params, then the nonce."""
    q = "&".join(f"{k}={v}" for k, v in sorted(query))
    base = url + ("?" + q if q or form else "") + ("." if q and form else "") + "".join(k + v for k, v in sorted(form))
    mac = hmac.new(auth_token.encode(), f"{base}.{nonce}".encode(), hashlib.sha256)
    return base64.b64encode(mac.digest()).decode()


async def plivo(request: Request, tenant_id: str) -> Principal:
    """A request Plivo signed with this tenant's token, over PUBLIC_BASE_URL + the raw path and query (the Host
    header can rewrite request.url, so it is never read)."""
    signatures = request.headers.get("x-plivo-signature-v3", "")
    nonce = request.headers.get("x-plivo-signature-v3-nonce", "")
    tenant = await run_in_threadpool(_tenant, tenant_id) if signatures and nonce else None
    if tenant is not None:
        org_id, _, sealed = tenant
        form = (await request.form()).multi_items() if request.method == "POST" else []
        url = settings.public_base_url + request.scope["path"]
        query = parse_qsl(request.scope["query_string"].decode(), keep_blank_values=True)
        expected = v3_signature(decrypt(sealed), url, query, form, nonce)
        if any(hmac.compare_digest(expected, s.strip()) for s in signatures.split(",")):
            return Principal(tenant_id, org_id, "plivo")
    raise ApiError(401, "unauthorized", "bad Plivo signature")


def _tenant(tenant_id: str):
    """(org id, auth id, sealed token) of a live tenant, else None."""
    if (tenant := as_uuid(tenant_id)) is None:
        return None
    with core_engine.connect() as conn:
        return conn.execute(_TENANT, {"tenant": tenant}).first()


def tenant_plivo(tenant_id: str) -> tuple[str, str]:
    """(auth_id, auth_token) to act at Plivo for this tenant."""
    if (tenant := _tenant(tenant_id)) is None:
        raise ApiError(404, "tenant_not_found", "no such tenant")
    return tenant[1], decrypt(tenant[2])


@cache
def _fernet() -> Fernet:
    hkdf = HKDF(SHA256(), 32, salt=None, info=b"plivo-token")
    return Fernet(base64.urlsafe_b64encode(hkdf.derive(settings.secret_key.encode())))


def encrypt(value: str) -> str:
    return _fernet().encrypt(value.encode()).decode()


def decrypt(sealed: str) -> str:
    return _fernet().decrypt(sealed.encode()).decode()


def mint_tokens(tenant_id: str, org_id: int, agent_id: str, role: str, ring: tuple[str, str]) -> tuple[str, str, int]:
    """(agent token, stream token, expiry epoch): fresh opaque tokens, kept in Redis by their SHA-256. `ring` is the
    agent's (endpoint, Browser SDK user): while the tokens live, Plivo rings the agent at the SDK user."""
    expires = int(time.time()) + TOKEN_TTL
    payload = json.dumps({"tenant_id": tenant_id, "org_id": org_id, "agent_id": agent_id, "role": role})
    agent, stream = AGENT_PREFIX + secrets.token_urlsafe(32), STREAM_PREFIX + secrets.token_urlsafe(32)
    values = {_token_key("agent", agent): payload, _token_key("stream", stream): payload, _ring(ring[0]): ring[1]}
    pipe = redis_client().pipeline()
    for key, value in values.items():
        pipe.set(key, value, ex=TOKEN_TTL)
    pipe.sadd(_sessions(agent_id), *values).expire(_sessions(agent_id), TOKEN_TTL).execute()
    return agent, stream, expires


def revoke_sessions(agent_id: str) -> None:
    """End every agent and stream token of the agent; its open streams close at their next token check."""
    redis_client().eval(_REVOKE, 1, _sessions(agent_id))


def ring_address(endpoint: str) -> str:
    """Where Plivo rings the agent on `endpoint`: its Browser SDK user while a session lives (a JWT login leaves the
    endpoint unregistered), else the endpoint, where a SIP device registers."""
    return redis_client().get(_ring(endpoint)) or endpoint


def agent_address(address: str) -> str:
    """The agent's endpoint for a Browser SDK user Plivo reports; any other address as it is."""
    return _SDK_USER.sub(r"\1", address)


def load_call(conversation_id, principal: Principal, live: bool = False) -> Call:
    """The conversation's call, when the principal may act on it: 404 unless it is the tenant's and, for an agent
    token, the agent is on it (a supervisor: or an agent of its teams; an admin: any). Call controls pass `live`:
    the agent must be on the call now; reads accept anyone who ever was."""
    level, cid = LEVEL.get(principal.role), as_uuid(conversation_id)
    with core_engine.connect() as conn:
        row = cid and conn.execute(_CALL, {"id": cid, "org": principal.org_id}).first()
        on_it = row is not None and (
            principal.kind == "key"
            or (principal.kind == "agent" and level == "org")
            or (
                principal.kind == "agent"
                and conn.execute(
                    _ON_CALL, {"parent": row[0], "agent": principal.agent_id, "team": level == "team", "live": live}
                ).scalar()
            )
        )
    if not on_it:
        raise ApiError(404, "conversation_not_found", "no such conversation")
    meta = {"org_uuid": principal.tenant_id, "org_name": row[3], "auth_id": row[4], "aom_uuid": principal.agent_id}
    return Call(cid, row[1], row[2], meta)


def owned(query, ids: list, principal: Principal, field: str | None = None) -> dict[str, int]:
    """{uuid: row id} for `ids` inside the principal's tenant; `query` selects (uuid, id) for `:ids` in `:org`
    (AGENTS, TEAMS, SKILLS or the caller's own). A miss is 404 for a path id, 422 foreign_id for a body `field`."""
    ids = [as_uuid(i) or str(i) for i in ids]
    valid = [i for i in ids if as_uuid(i)]
    with core_engine.connect() as conn:
        found = dict(conn.execute(query, {"ids": valid, "org": principal.org_id}).all()) if valid else {}
    if missing := [i for i in ids if i not in found]:
        if field is None:
            raise ApiError(404, "not_found", "no such resource")
        raise ApiError(422, "foreign_id", f"unknown id in {field}", {"field": field, "ids": missing})
    return found


def _scrub(line: str) -> str:
    return _SECRET_QUERY.sub(r"\1[REDACTED]", line)


def redact_access_token(record: logging.LogRecord) -> bool:
    """A logging filter: the stream token and a recording URL's signature never reach a log line. It edits the args in place, as
    uvicorn's access formatter unpacks them."""
    if isinstance(record.args, tuple):
        record.args = tuple(_scrub(a) if isinstance(a, str) else a for a in record.args)
    if isinstance(record.msg, str):
        record.msg = _scrub(record.msg)
    return True
