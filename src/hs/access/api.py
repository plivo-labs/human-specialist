"""Tenants (Plivo sub-accounts) and their API keys, for the operator; agent sessions, for a tenant key."""

import logging
import secrets
import time
import uuid
from datetime import UTC, datetime
from typing import Literal
from uuid import UUID

import httpx
from fastapi import APIRouter, Depends, Response
from pydantic import BaseModel, Field
from sqlalchemy import text

from hs.access import (
    KEY_PREFIX,
    SCOPES,
    TOKEN_TTL,
    Principal,
    as_uuid,
    close_tenant,
    decrypt,
    digest,
    encrypt,
    mint_tokens,
    require,
    revoke_sessions,
    tenant_plivo,
)
from hs.admin import seeds
from hs.errors import ApiError
from hs.infra.db import core_engine
from hs.infra.redis import HeartbeatPresenceCache
from hs.infra.settings import settings
from hs.paging import Page, Paging, page, paging
from hs.plivo import rest

log = logging.getLogger(__name__)
router = APIRouter()
# Mounted with POST /agents (admin): before agents exist nothing can open a session.
sessions_router = APIRouter()
OP, OP_SECRET = Depends(require("op")), Depends(require("op", secret=True))
SESSIONS, SESSIONS_SECRET = Depends(require("key:sessions")), Depends(require("key:sessions", secret=True))

_REGISTERED = text("SELECT uuid::text FROM organization WHERE auth_id = :auth_id AND is_deleted IS NOT TRUE")
_NEW_TENANT = text(
    "INSERT INTO organization (uuid, created_at, updated_at, is_deleted, is_active, name, auth_id, auth_token,"
    " plivo_token_enc, skip_contact_creation, skip_case_creation, allow_acw_post_transfer)"
    " VALUES (:tenant, now(), now(), false, true, :name, :auth_id, '', :sealed, false, false, false) RETURNING id"
)
_TENANTS = text(
    "SELECT o.uuid::text AS id, o.name, o.auth_id AS plivo_auth_id, s.plivo_app_id, o.created_at, o.id AS org_id,"
    " o.plivo_token_enc FROM organization o LEFT JOIN org_setting s ON s.organization = o.id"
    " WHERE o.is_deleted IS NOT TRUE AND (CAST(:tenant AS uuid) IS NULL OR o.uuid = :tenant)"
    " AND (CAST(:after AS uuid) IS NULL OR o.uuid > :after) ORDER BY o.uuid LIMIT :limit"
)
_RENAME = text(
    "UPDATE organization SET name = COALESCE(:name, name), plivo_token_enc = :sealed, updated_at = now() WHERE id = :org"
)
_DELETE_TENANT = text(
    "UPDATE organization SET is_deleted = true, is_active = false, updated_at = now() WHERE id = :org"
)
_AGENTS = text(
    "SELECT uuid::text, sip_endpoint_id FROM account_org_mapping WHERE organization = :org AND is_deleted IS NOT TRUE"
)
_NEW_KEY = text(
    "INSERT INTO api_keys (organization, prefix, sha256, scopes) VALUES (:org, :prefix, :sha, :scopes)"
    " RETURNING id::text, prefix, scopes, created_at"
)
_KEYS = text(
    "SELECT id::text, prefix, scopes, created_at FROM api_keys WHERE organization = :org AND revoked_at IS NULL"
    " AND (CAST(:after AS uuid) IS NULL OR id > :after) ORDER BY id LIMIT :limit"
)
_REVOKE_KEYS = text(
    "UPDATE api_keys SET revoked_at = now() WHERE organization = :org AND revoked_at IS NULL"
    " AND (CAST(:key AS uuid) IS NULL OR id = :key)"
)
_SESSION_AGENT = text(
    "SELECT aom.sip_endpoint, r.name, COALESCE((s.voice_settings->>'agent_direct_dial')::boolean, false), s.plivo_app_id"
    " FROM account_org_mapping aom LEFT JOIN role r ON r.id = aom.role"
    " LEFT JOIN org_setting s ON s.organization = aom.organization"
    " WHERE aom.uuid = :agent AND aom.organization = :org AND aom.is_deleted IS NOT TRUE"
)


class TenantIn(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    plivo_auth_id: str = Field(min_length=1)
    plivo_auth_token: str = Field(min_length=1)
    fallback_answer_url: str | None = None


class TenantPatch(BaseModel):
    name: str | None = Field(None, min_length=1, max_length=200)
    plivo_auth_token: str | None = Field(None, min_length=1)


class Tenant(BaseModel):
    id: str
    name: str
    plivo_auth_id: str
    plivo_app_id: str | None
    created_at: datetime


class Registered(BaseModel):
    tenant_id: str
    plivo_app_id: str
    api_key: str


class KeyIn(BaseModel):
    scopes: list[Literal[SCOPES]] = Field(min_length=1)


class Key(BaseModel):
    id: str
    prefix: str
    scopes: list[str]
    created_at: datetime


class MintedKey(Key):
    api_key: str


class PlivoLogin(BaseModel):
    endpoint_username: str
    access_token: str
    expires_at: datetime


class Session(BaseModel):
    agent_token: str
    stream_token: str
    expires_at: datetime
    plivo: PlivoLogin


def _app_urls(tenant_id: str) -> dict:
    base = f"{settings.public_base_url}/plivo/{tenant_id}"
    return {
        "answer_url": f"{base}/answer",
        "answer_method": "POST",
        "hangup_url": f"{base}/hangup",
        "hangup_method": "POST",
    }


def _after(p: Paging) -> str | None:
    if p.after and not as_uuid(p.after):
        raise ApiError(422, "invalid_cursor", "cursor is not one this API returned")
    return p.after


def _tenants(tenant_id: UUID | None, p: Paging) -> list[dict]:
    args = {"tenant": tenant_id and str(tenant_id), "after": _after(p), "limit": p.limit + 1}
    with core_engine.connect() as conn:
        return [dict(r._mapping) for r in conn.execute(_TENANTS, args)]


def _tenant(tenant_id: UUID) -> dict:
    if not (rows := _tenants(tenant_id, Paging(1, None))):
        raise ApiError(404, "tenant_not_found", "no such tenant")
    return rows[0]


def _mint_key(conn, org_id: int, scopes: list[str]) -> dict:
    key = KEY_PREFIX + secrets.token_urlsafe(32)
    args = {"org": org_id, "prefix": key[:12], "sha": digest(key), "scopes": sorted(set(scopes))}
    return dict(conn.execute(_NEW_KEY, args).one()._mapping) | {"api_key": key}


@router.post("/tenants", status_code=201, response_model=Registered)
def register(body: TenantIn, _: Principal = OP_SECRET):
    with core_engine.connect() as conn:
        if existing := conn.execute(_REGISTERED, {"auth_id": body.plivo_auth_id}).scalar():
            raise ApiError(409, "tenant_exists", "this sub-account is already a tenant", {"tenant_id": existing})
    auth = (body.plivo_auth_id, body.plivo_auth_token)
    rest.ok(auth, "GET")
    tenant_id = str(uuid.uuid4())
    app = {"app_name": f"hs-{tenant_id}", **_app_urls(tenant_id)}
    if body.fallback_answer_url:
        app |= {"fallback_answer_url": body.fallback_answer_url, "fallback_method": "POST"}
    with core_engine.begin() as conn:
        args = {"tenant": tenant_id, "name": body.name, "auth_id": body.plivo_auth_id, "sealed": encrypt(auth[1])}
        org_id = conn.execute(_NEW_TENANT, args).scalar_one()
        app_id = rest.ok(auth, "POST", "Application/", **app)["app_id"]
        seeds.seed(conn, org_id, app_id)
        key = _mint_key(conn, org_id, list(SCOPES))
    return {"tenant_id": tenant_id, "plivo_app_id": app_id, "api_key": key["api_key"]}


@router.get("/tenants", response_model=Page[Tenant])
def list_tenants(_: Principal = OP, p: Paging = Depends(paging)):
    return page(_tenants(None, p), p)


@router.get("/tenants/{tenant_id}", response_model=Tenant)
def get_tenant(tenant_id: UUID, _: Principal = OP):
    return _tenant(tenant_id)


@router.patch("/tenants/{tenant_id}", response_model=Tenant)
def patch_tenant(tenant_id: UUID, body: TenantPatch, _: Principal = OP):
    """Rename, replace the Plivo token (checked with Plivo first) and re-apply the app URLs from PUBLIC_BASE_URL."""
    t = _tenant(tenant_id)
    auth = (t["plivo_auth_id"], body.plivo_auth_token or decrypt(t["plivo_token_enc"]))
    if body.plivo_auth_token:
        rest.ok(auth, "GET")
    rest.ok(auth, "POST", f"Application/{t['plivo_app_id']}/", **_app_urls(tenant_id))
    with core_engine.begin() as conn:
        conn.execute(_RENAME, {"name": body.name, "sealed": encrypt(auth[1]), "org": t["org_id"]})
    return _tenant(tenant_id)


@router.delete("/tenants/{tenant_id}", status_code=204)
def delete_tenant(tenant_id: UUID, _: Principal = OP) -> Response:
    """Refuse every credential of the tenant, then remove its endpoints and application at Plivo."""
    t = _tenant(tenant_id)
    with core_engine.begin() as conn:
        conn.execute(_DELETE_TENANT, {"org": t["org_id"]})
        conn.execute(_REVOKE_KEYS, {"org": t["org_id"], "key": None})
        agents = conn.execute(_AGENTS, {"org": t["org_id"]}).all()
    close_tenant(t["id"])
    for agent_id, _endpoint in agents:
        revoke_sessions(agent_id)
    auth = (t["plivo_auth_id"], decrypt(t["plivo_token_enc"]))
    for path in [f"Endpoint/{e}/" for _, e in agents if e] + [f"Application/{t['plivo_app_id']}/"]:
        try:
            r = rest.call(auth, "DELETE", path)
        except httpx.HTTPError as e:
            log.warning("tenant %s: Plivo DELETE %s failed: %s", tenant_id, path, e)
            continue
        if r.status_code >= 500:
            log.warning("tenant %s: Plivo DELETE %s answered %s", tenant_id, path, r.status_code)
    return Response(status_code=204)


@router.post("/tenants/{tenant_id}/api-keys", status_code=201, response_model=MintedKey)
def mint_key(tenant_id: UUID, body: KeyIn, _: Principal = OP_SECRET):
    org_id = _tenant(tenant_id)["org_id"]
    with core_engine.begin() as conn:
        return _mint_key(conn, org_id, body.scopes)


@router.get("/tenants/{tenant_id}/api-keys", response_model=Page[Key])
def list_keys(tenant_id: UUID, _: Principal = OP, p: Paging = Depends(paging)):
    args = {"org": _tenant(tenant_id)["org_id"], "after": _after(p), "limit": p.limit + 1}
    with core_engine.connect() as conn:
        return page([dict(r._mapping) for r in conn.execute(_KEYS, args)], p)


@router.delete("/tenants/{tenant_id}/api-keys/{key_id}", status_code=204)
def revoke_key(tenant_id: UUID, key_id: UUID, _: Principal = OP) -> Response:
    with core_engine.begin() as conn:
        if not conn.execute(_REVOKE_KEYS, {"org": _tenant(tenant_id)["org_id"], "key": str(key_id)}).rowcount:
            raise ApiError(404, "api_key_not_found", "no such API key")
    return Response(status_code=204)


def _agent(agent_id: UUID, p: Principal):
    with core_engine.connect() as conn:
        row = conn.execute(_SESSION_AGENT, {"agent": str(agent_id), "org": p.org_id}).first()
    if row is None:
        raise ApiError(404, "agent_not_found", "no such agent")
    return row


@sessions_router.post("/agents/{agent_id}/sessions", status_code=201, response_model=Session)
def open_session(agent_id: UUID, p: Principal = SESSIONS_SECRET):
    """Agent and stream tokens plus a Plivo SDK token for the agent's endpoint, which cannot outlive them. Plivo mints
    the SDK token: it refuses one signed anywhere else."""
    sip_endpoint, role, direct_dial, app_id = _agent(agent_id, p)
    if not sip_endpoint:
        raise ApiError(409, "agent_has_no_endpoint", "the agent has no Plivo endpoint")
    username = sip_endpoint.removeprefix("sip:").partition("@")[0]
    auth, start = tenant_plivo(p.tenant_id), int(time.time())
    per = {"voice": {"incoming_allow": True, "outgoing_allow": direct_dial}}
    claims = {"iss": auth[0], "sub": username, "nbf": start, "exp": start + TOKEN_TTL, "per": per, "app": app_id}
    jwt = rest.ok(auth, "POST", "JWT/Token/", **claims)["token"]
    ring = (sip_endpoint, sip_endpoint.replace("@", f"_{auth[0]}@", 1))
    agent_token, stream_token, expires = mint_tokens(p.tenant_id, p.org_id, str(agent_id), role, ring)
    at, jwt_at = datetime.fromtimestamp(expires, UTC), datetime.fromtimestamp(claims["exp"], UTC)
    login = {"endpoint_username": username, "access_token": jwt, "expires_at": jwt_at}
    HeartbeatPresenceCache.reset_ping_count(str(agent_id))  # the sign-in status applies on a session's second ping
    return {"agent_token": agent_token, "stream_token": stream_token, "expires_at": at, "plivo": login}


@sessions_router.delete("/agents/{agent_id}/sessions", status_code=204)
def close_sessions(agent_id: UUID, p: Principal = SESSIONS) -> Response:
    _agent(agent_id, p)
    revoke_sessions(str(agent_id))
    return Response(status_code=204)
