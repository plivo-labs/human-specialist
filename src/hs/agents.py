"""Agents: the people, their Plivo endpoints, sessions, presence and desk.

An agent is a `config` document keyed by its SIP username. Presence is one row per agent with its status; its
desk's beats live in `hot`. A session's second beat applies the sign-in status; a desk silent for 40 s is swept
Offline. Every status change emits `agent.status_changed` and schedules `route_agent`, which offers an available
agent queued work and withdraws the offers of one that left."""

import ipaddress
import secrets
import time
from collections import Counter
from datetime import UTC, datetime
from typing import Annotated, Literal
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, Request, Response
from pydantic import BaseModel, Field
from redis import RedisError
from sqlalchemy import Text, delete, func, insert, literal_column, select, text, update
from sqlalchemy.engine import Connection, Row

from hs import access, config, conversations, events, hot, jobs, plivo
from hs.access import Principal
from hs.config import provision
from hs.config.models import EMAIL_PATTERN, Body, Grants, Named, NameIn, NamePatch, Recording, WrapUp
from hs.core import AGENT_PREFIX, PUBLIC, ApiError, Page, Paging, T, after_commit, page, paging, read, tx
from hs.events.catalog import Channel

router = APIRouter()
SESSIONS, ADMIN, ADMIN_SUP, AGENT = (
    access.require(*g) for g in (("key:sessions",), ("key:admin", "admin"), ("key:admin", "sup"), ("agent",))
)
CONNECTED, DISCONNECTED = "AUTOMATED_EVENT_INTERNET_CONNECTED", "AUTOMATED_EVENT_INTERNET_DISCONNECTED"
SIP = "sip:{}@phone.plivo.com"
# The beat authenticates itself: one statement, no transaction.
_WHO = text(
    "SELECT tenant_id, agent_id FROM credential WHERE sha256 = :sha AND kind = 'agent' AND revoked_at IS NULL"
    " AND (expires_at IS NULL OR expires_at > now())"
)
# Desks routing's sweep checks: agents signed in, and agents a live session may be counting beats for.
_DESKS = text(
    "SELECT p.tenant_id, p.agent_id, s.key IS DISTINCT FROM 'offline' FROM presence p JOIN config s"
    " ON s.id = p.status_id WHERE s.key IS DISTINCT FROM 'offline' OR EXISTS (SELECT 1 FROM credential c"
    "  WHERE c.tenant_id = p.tenant_id AND c.agent_id = p.agent_id AND c.kind = 'agent' AND c.revoked_at IS NULL"
    "  AND (c.expires_at IS NULL OR c.expires_at > now()))"
)
_FREE = text(
    "SELECT a.id::text AS id, a.body->>'name' AS name, t.id::text AS team_id, t.body->>'name' AS team"
    " FROM config a JOIN presence p ON p.agent_id = a.id"
    " JOIN config s ON s.id = p.status_id AND (s.body->>'availability')::boolean"
    " LEFT JOIN config t ON t.tenant_id = a.tenant_id AND t.kind = 'team' AND a.body->'teams' ? t.id::text"
    " WHERE a.tenant_id = :tenant AND a.kind = 'agent' AND a.id <> :me AND 'call:' || a.id <> ALL(CAST(:full AS text[]))"
    " ORDER BY a.body->>'name'"
)
_PICKER = text(
    "SELECT c.kind, c.id::text AS id, c.body->>'name' AS name, coalesce(c.body->'teams', '[]') AS teams,"
    " s.id::text AS status_id, s.body->>'name' AS status, coalesce((s.body->>'availability')::boolean, false) AS ready"
    " FROM config c LEFT JOIN presence p ON p.agent_id = c.id LEFT JOIN config s ON s.id = p.status_id"
    " WHERE c.tenant_id = :tenant AND c.kind = ANY(CAST(:kinds AS text[])) AND c.id <> :me ORDER BY c.body->>'name'"
)
Role = Literal["admin", "supervisor", "agent"]
CallerId = Annotated[str, Field(pattern=r"^\+[1-9]\d{1,14}$")]
Email = Annotated[str, Field(pattern=EMAIL_PATTERN, max_length=254, description="Unique in the tenant, ignoring case.")]
Password = Annotated[str, Field(min_length=8, max_length=128)]


class Status(Named):
    pass


class SkillLevel(Body):
    skill_id: str
    level: int | None = Field(
        None, ge=0, le=100, description="Required inside the skill's range when it asks for a level."
    )


class Agent(Named):
    external_id: str | None
    email: str | None = None
    role: str | None
    teams: list[str]
    skills: list[SkillLevel]
    caller_id: str | None
    endpoint_id: str | None
    endpoint_username: str | None
    status: Status | None


class AgentIn(NameIn):
    external_id: str | None = Field(None, min_length=1, max_length=200)
    email: Email | None = None
    role: Role
    teams: list[str] = []
    skills: list[SkillLevel] = []
    caller_id: CallerId | None = None
    endpoint_id: str | None = Field(
        None,
        pattern=r"^\d{1,32}$",
        description="An endpoint of the tenant's Plivo account to ring, instead of a new one. HS never changes its"
        " password or deletes it.",
    )


class AgentPatch(NamePatch):
    email: Email | None = None
    role: Role | None = None
    teams: list[str] | None = None
    skills: list[SkillLevel] | None = None
    caller_id: CallerId | None = None


class AgentStatus(BaseModel):
    status: Status | None


class StatusSet(Body):
    status_id: str


class PlivoLogin(BaseModel):
    endpoint_username: str
    access_token: str
    expires_at: datetime


class Session(BaseModel):
    agent_token: str
    stream_token: str
    expires_at: datetime
    renewable_until: datetime = Field(description="Until then POST /me/sessions mints the next session.")
    plivo: PlivoLogin


class SignIn(Body):
    email: str = Field(max_length=254)
    password: str = Field(max_length=128)
    tenant_id: UUID | None = Field(None, description="Left out only on an install of one tenant, else 409.")


class PasswordIn(Body):
    password: Password | None = Field(None, description="Omit for one HS makes.")


class NewPassword(BaseModel):
    password: str | None = Field(description="The password HS made, shown only here; null for the one you sent.")


class PasswordChange(Body):
    current_password: str = Field(max_length=128)
    password: Password


class SipCredentials(BaseModel):
    username: str
    password: str


class DeskSettings(BaseModel):
    wrap_up: WrapUp
    agent_direct_dial: bool
    recording: Recording


class PendingOffer(BaseModel):
    offer_id: str
    conversation_id: str
    conversation_number: str
    channel: str
    from_: str | None = Field(alias="from")
    via: str
    offered_at: datetime
    expires_at: datetime


class Me(Agent):
    settings: DeskSettings
    pending_offers: list[PendingOffer]
    grants: Grants = Field(description="The agent's role's grants, so a desk hides what it cannot use.")


class Ping(Body):
    sdk_heartbeat_ts: str | None = None


class Ref(BaseModel):
    id: str
    name: str | None


class TransferAgent(Ref):
    teams: list[str]
    status: Status | None
    availability: Literal["available", "busy", "unavailable"]


class TransferTeam(Ref):
    available_agents: int


class TransferTargets(BaseModel):
    agents: list[TransferAgent]
    teams: list[TransferTeam]
    destinations: list[Ref]


def _agents(tenant_id):
    A, P, S = T.config, T.presence, T.config.alias("s")
    status = (S.c.id.cast(Text).label("status_id"), S.c.body["name"].astext.label("status_name"))
    joined = A.outerjoin(P, P.c.agent_id == A.c.id).outerjoin(S, S.c.id == P.c.status_id)
    return select(A.c.id, A.c.body, *status).select_from(joined).where(A.c.tenant_id == tenant_id, A.c.kind == "agent")


def _agent(row: Row) -> dict:
    status = {"id": row.status_id, "name": row.status_name} if row.status_id else None
    return {**row.body, "id": str(row.id), "status": status}


def agent(db: Connection, tenant_id, agent_id) -> dict | None:
    """The agent as GET /agents/{id} shows it, with its presence status."""
    if (agent_id := config.as_uuid(agent_id)) is None:
        return None
    row = db.execute(_agents(tenant_id).where(T.config.c.id == agent_id)).first()
    return row and _agent(row)


def visible_agent(db: Connection, p: Principal, agent_id) -> dict:
    """The agent, if `p` may see it (a supervisor sees its teams' agents, an agent itself); else 404."""
    seen = access.visible_agents(db, p)
    if (a := agent(db, p.tenant_id, agent_id)) is None or (seen is not None and a["id"] not in seen):
        raise ApiError(404, "agent_not_found", "no such agent")
    return a


def sdk_login(db: Connection, tenant_id, agent_id) -> str | None:
    """The Browser SDK user, `<endpoint username>_<auth id>`, of the agent's live session HS minted a Plivo token
    for. Plivo shows the endpoint unregistered for it."""
    K = T.credential
    live = (K.c.tenant_id == tenant_id, K.c.agent_id == agent_id, K.c.name.is_not(None), K.c.revoked_at.is_(None))
    live += (K.c.expires_at > func.now(),)
    return db.execute(select(K.c.name).where(*live).limit(1)).scalar()


def ring_address(db: Connection, tenant_id, agent: dict) -> str:
    """Where Plivo rings the agent: its SDK login, else its endpoint, where a SIP device or a plugin's console
    registers."""
    return SIP.format(sdk_login(db, tenant_id, agent["id"]) or agent["endpoint_username"])


def _vet(db: Connection, p: Principal, body: AgentIn | AgentPatch, *, partial: bool = False) -> dict:
    """The body as stored, ids made canonical, once its caller id is a routed number and every id it names is the
    tenant's. A partial body leaves out what it does not change; a null caller_id clears it."""
    if body.caller_id and config.by_key(db, p.tenant_id, "number", body.caller_id) is None:
        raise ApiError(422, "from_not_allowed", "caller_id must be a number the tenant routes (PUT /numbers)")
    config.check(db, p, body)
    doc = body.model_dump(mode="json", exclude_unset=partial)
    doc = {k: v for k, v in doc.items() if v is not None or k in ("caller_id", "external_id", "email")}
    if "teams" in doc:
        doc["teams"] = list(dict.fromkeys(str(config.as_uuid(t)) for t in doc["teams"]))
    for s in doc.get("skills", ()):
        s["skill_id"] = str(config.as_uuid(s["skill_id"]))
        want = config.get(db, p.tenant_id, "skill", s["skill_id"])
        if not want.get("is_level_required", True):
            s["level"] = None
        elif s.get("level") is None or not want.get("min_level", 1) <= s["level"] <= want.get("max_level", 10):
            raise ApiError(
                422, "skill_level_invalid", "a level inside the skill's range is required", {"skill_id": s["skill_id"]}
            )
    return doc


def _unique(db: Connection, tenant_id, doc: dict, me=None) -> None:
    """409 agent_exists when another of the tenant's agents has the doc's external_id, or its email in any case."""
    A = T.config
    same = {"external_id": A.c.body["external_id"].astext == doc.get("external_id")}
    same["email"] = func.lower(A.c.body["email"].astext) == func.lower(doc.get("email"))
    for field, match in same.items():
        if doc.get(field) and (taken := db.execute(config.docs(tenant_id, "agent").where(match, A.c.id != me)).first()):
            raise ApiError(409, "agent_exists", f"an agent has this {field}", {"id": str(taken.id)})


@router.get("/agents", response_model=Page[Agent])
def list_agents(external_id: str | None = None, p: Principal = ADMIN_SUP, paged: Paging = Depends(paging)):
    A = T.config
    with read() as db:
        q = _agents(p.tenant_id)
        if (seen := access.visible_agents(db, p)) is not None:
            q = q.where(A.c.id.in_([config.as_uuid(i) for i in seen]))
        if external_id:
            q = q.where(A.c.body["external_id"].astext == external_id)
        return page([_agent(r) for r in config.page_rows(db, q, paged, A.c.id)], paged)


@router.post("/agents", status_code=201, response_model=Agent)
def create_agent(body: AgentIn, p: Principal = ADMIN):
    """An agent on a Plivo endpoint of its own, or on the Plivo account's `endpoint_id`, Offline."""
    adopted = body.endpoint_id is not None
    with read() as db:
        tenant, _ = config.live_tenant(db, p.tenant_id), _vet(db, p, body)
    agent_id = body.id or uuid4()
    made = plivo.held(tenant, f"Endpoint/{body.endpoint_id}/", "endpoint") if adopted else None
    intent = None if adopted else provision.intend(p.tenant_id, "Endpoint", str(agent_id))
    try:
        if made is None:
            endpoint = {"username": "hsagent", "password": secrets.token_hex(16), "alias": str(agent_id)}
            endpoint |= {"app_id": tenant.plivo_app_id} if tenant.plivo_app_id else {}
            made = plivo.rest(tenant, "POST", "Endpoint/", **endpoint)
        with config.write(p) as db:
            config.live_tenant(db, p.tenant_id)
            doc = _vet(db, p, body) | {"endpoint_id": str(made["endpoint_id"]), "endpoint_username": made["username"]}
            _unique(db, p.tenant_id, doc)
            if taken := config.by_key(db, p.tenant_id, "agent", made["username"]):
                raise ApiError(409, "agent_exists", "an agent has this endpoint", {"id": taken[0]})
            row = config.add(db, p.tenant_id, "agent", doc | {"endpoint_adopted": adopted}, made["username"], agent_id)
            access.created(db, str(row.id))
            provision.kept(db, intent)
            offline = config.by_key(db, p.tenant_id, "status", "offline")[0]
            db.execute(insert(T.presence).values(agent_id=row.id, tenant_id=p.tenant_id, status_id=offline))
            return agent(db, p.tenant_id, row.id)
    except Exception:
        provision.unmade(intent, made and f"Endpoint/{made['endpoint_id']}/")
        raise


access.VIEWS[f"POST {PUBLIC}/agents"] = lambda db, p, rid: agent(db, p.tenant_id, rid)


@router.get("/agents/{agent_id}", response_model=Agent)
def get_agent(agent_id: str, p: Principal = ADMIN):
    with read() as db:
        return visible_agent(db, p, agent_id)


@router.patch("/agents/{agent_id}", response_model=Agent)
def patch_agent(agent_id: str, body: AgentPatch, p: Principal = ADMIN):
    """A new role ends the agent's sessions, a renewal in flight included, so its desk signs in again under it."""
    with config.write(p) as db:
        _lock_presence(db, p.tenant_id, config.as_uuid(agent_id))
        row = config.found(db, p.tenant_id, "agent", agent_id, lock=True)
        changes = _vet(db, p, body, partial=True)
        _unique(db, p.tenant_id, changes, row.id)
        config.save(db, row.id, row.body | changes)
        if body.role and body.role != row.body["role"]:
            access.revoke(db, p.tenant_id, agent_id=str(row.id))
        return agent(db, p.tenant_id, row.id)


@router.delete("/agents/{agent_id}", status_code=204)
def delete_agent(agent_id: str, p: Principal = ADMIN) -> Response:
    """Not while an offer or a stint of its is open: the presence lock holds off new offers, which take it too,
    until the row is gone. An endpoint HS made is deleted at Plivo."""
    P = T.presence
    with config.write(p) as db:
        row = config.found(db, p.tenant_id, "agent", agent_id)
        db.execute(select(P.c.agent_id).where(P.c.agent_id == row.id).with_for_update())
        if conversations.desk(db, p.tenant_id, row.id, conversations.ON):
            raise ApiError(409, "agent_on_call", "the agent is on a call")
        db.execute(delete(P).where(P.c.agent_id == row.id))
        db.execute(delete(T.password).where(T.password.c.agent_id == row.id))
        db.execute(delete(T.config).where(T.config.c.id == row.id))
        access.revoke(db, p.tenant_id, agent_id=str(row.id))
        if not row.body.get("endpoint_adopted"):
            provision.drop(db, p.tenant_id, f"Endpoint/{row.body['endpoint_id']}/")
    return Response(status_code=204)


SHIFT = "12 hours"  # a working shift: inside it, a sign-in or an available status keeps the agent's place in line
FIRST_AVAILABLE = literal_column(
    "CASE WHEN coalesce(first_available_at < now() - interval '" + SHIFT + "', true) THEN now()"
    " ELSE first_available_at END"
)


def set_status(db: Connection, tenant_id, agent_id, status_id, reason: str, why: str | None = None) -> bool:
    """Move the agent to one of its tenant's statuses and announce it; False if it had it already. The presence row
    is locked before the status row, the order every transaction takes them in (docs/architecture.md), and the status
    row is held in share mode, so it cannot be deleted under the change."""
    C, P = T.config, T.presence
    held = select(P.c.status_id, C.c.body["name"].astext.label("name")).join(C, C.c.id == P.c.status_id)
    now = db.execute(held.where(P.c.agent_id == agent_id).with_for_update(of=P)).first()
    status = select(C.c.id, C.c.body).where(C.c.id == status_id, C.c.tenant_id == tenant_id, C.c.kind == "status")
    had = now is None or now.status_id == config.as_uuid(status_id)
    if had or (to := db.execute(status.with_for_update(read=True)).first()) is None:
        return False
    first = {"first_available_at": FIRST_AVAILABLE} if to.body["availability"] else {}
    db.execute(update(P).where(P.c.agent_id == agent_id).values(status_id=to.id, since=func.now(), **first))
    data = {"from_status": {"id": str(now.status_id), "name": now.name}, "reason": reason, "reason_for_change": why}
    data["to_status"] = {"id": str(to.id), "name": to.body["name"]}
    data["available"] = to.body["availability"]
    events.emit(db, "agent.status_changed", str(tenant_id), data, agent_id=str(agent_id))
    jobs.schedule(db, "route_agent", {"tenant_id": str(tenant_id), "agent_id": str(agent_id)}, key=str(agent_id))
    return True


def _change(db: Connection, p: Principal, agent_id: str, body: StatusSet, reason: str) -> dict:
    config.check(db, p, body)
    set_status(db, p.tenant_id, agent_id, body.status_id, reason)
    return {"status": agent(db, p.tenant_id, agent_id)["status"]}


SEEDED = {"sign_in": "online", "sign_out": "offline"}


def _edge(db: Connection, tenant_id, edge: str):
    """The status the tenant's sign-in or sign-out sets: the seeded one unless settings name it or switch it off."""
    ref = getattr(config.tenant_settings(db, tenant_id), edge)
    return config.by_key(db, tenant_id, "status", SEEDED[edge])[0] if ref is None else ref.status_id


def _available(db: Connection, agent_id) -> bool:
    C, P = T.config, T.presence
    now = select(C.c.body["availability"].as_boolean()).join(P, P.c.status_id == C.c.id)
    return bool(db.execute(now.where(P.c.agent_id == agent_id)).scalar())


def _signed_out(db: Connection, tenant_id, agent_id, reason: str, why: str | None = None) -> None:
    """An available agent takes the sign-out status; one on Break keeps it. The next session's second beat signs in
    again."""
    if (status := _edge(db, tenant_id, "sign_out")) and _available(db, agent_id):
        set_status(db, tenant_id, agent_id, status, reason, why)
    after_commit(db, lambda: hot.recount(tenant_id, agent_id))


def _lock_presence(db: Connection, tenant_id, agent_id, *, share: bool = False, still=None) -> None:
    P = T.presence
    held = select(P.c.agent_id).where(P.c.agent_id == agent_id, P.c.tenant_id == tenant_id)
    if db.execute(held.with_for_update(read=share)).first() is None:
        raise ApiError(404, "agent_not_found", "no such agent")
    if still is not None and db.execute(still).first() is None:
        raise ApiError(401, "unauthorized", "missing or invalid credential")


def _session(tenant_id, agent_id, renewable_until=None, still=None) -> dict:
    with read() as db:
        found, tenant = agent(db, tenant_id, agent_id), config.live_tenant(db, tenant_id)
        direct = config.tenant_settings(db, tenant_id).agent_direct_dial
    if found is None:
        raise ApiError(404, "agent_not_found", "no such agent")
    username, auth_id, start = found["endpoint_username"], tenant.plivo_auth_id, int(time.time())
    per = {"voice": {"incoming_allow": True, "outgoing_allow": direct}}
    claims = {"iss": auth_id, "sub": username, "nbf": start, "exp": start + 3600, "per": per}
    claims |= {"app": tenant.plivo_app_id} if tenant.plivo_app_id else {}
    # Minted before the session, so only a session that holds a token is marked to ring at its SDK login.
    jwt = plivo.rest(tenant, "POST", "JWT/Token/", **claims)["token"]
    with tx() as db:
        _lock_presence(db, tenant_id, agent_id, share=True, still=still)
        tokens = access.mint(db, tenant_id, found["id"], renewable_until)
        mine = T.credential.c.sha256 == access.digest(tokens.agent_token)
        db.execute(update(T.credential).where(mine).values(name=f"{username}_{auth_id}"))
        after_commit(db, lambda: hot.recount(tenant_id, agent_id))
    expires = datetime.fromtimestamp(claims["exp"], UTC)
    return tokens._asdict() | {"plivo": {"endpoint_username": username, "access_token": jwt, "expires_at": expires}}


@router.post("/agents/{agent_id}/sessions", status_code=201, response_model=Session)
def open_session(agent_id: UUID, p: Principal = SESSIONS):
    """Agent and stream tokens, and a Plivo browser SDK token Plivo mints for the agent's endpoint, which cannot
    outlive them. While the session lives, the agent's calls ring where the browser SDK logs in with it. Its agent
    token renews it (POST /me/sessions)."""
    return _session(p.tenant_id, agent_id)


_SET_PASSWORD = text(
    "INSERT INTO password (agent_id, tenant_id, hash) VALUES (:agent, :tenant, :hash)"
    " ON CONFLICT (agent_id) DO UPDATE SET hash = excluded.hash"
)


def _new_password(db: Connection, tenant_id, agent_id, hash: str, still=None) -> None:
    _lock_presence(db, tenant_id, agent_id, still=still)
    db.execute(_SET_PASSWORD, {"agent": agent_id, "tenant": tenant_id, "hash": hash})
    access.revoke(db, tenant_id, agent_id=str(agent_id))


@router.post("/agents/{agent_id}/password", response_model=NewPassword)
def set_password(agent_id: str, body: PasswordIn | None = None, p: Principal = ADMIN):
    """The agent's sign-in password, or without one in the body a password HS makes and shows only here. HS keeps
    its scrypt hash. Every session of the agent ends."""
    made = None if body and body.password else secrets.token_urlsafe(12)
    hash = access.hashed(made or body.password)
    with config.write(p) as db:
        _new_password(db, p.tenant_id, visible_agent(db, p, agent_id)["id"], hash)
    return {"password": made}


_LOGIN = text(
    "SELECT a.id, w.hash FROM config a JOIN tenant t ON t.id = a.tenant_id AND t.deleted_at IS NULL"
    " JOIN password w ON w.agent_id = a.id AND w.tenant_id = a.tenant_id"
    " WHERE a.tenant_id = :tenant AND a.kind = 'agent' AND lower(a.body->>'email') = lower(:email)"
)
_SOLE = text("SELECT min(id::text) FROM (SELECT id FROM tenant WHERE deleted_at IS NULL LIMIT 2) t HAVING count(*) = 1")
_UNCHANGED = text(
    "SELECT 1 FROM password w WHERE w.agent_id = :agent AND w.tenant_id = :tenant AND w.hash = :hash"
    " AND (CAST(:sha AS text) IS NULL OR EXISTS (SELECT 1 FROM credential k WHERE k.sha256 = :sha"
    "  AND k.agent_id = w.agent_id AND k.revoked_at IS NULL AND k.expires_at > now()))"
)


def _client(request: Request) -> str:
    host = request.client.host if request.client else ""
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return host
    ip = getattr(ip, "ipv4_mapped", None) or ip
    return str(ip if ip.version == 4 else ipaddress.ip_network((ip, 64), strict=False))


def _try(request: Request, account: str) -> list[str]:
    tries = {f"email:{access.digest(account.lower())}": 10, f"ip:{_client(request)}": 100}
    if wait := hot.sign_in_tried(tries, 900):
        raise ApiError(429, "too_many_attempts", "too many failed sign-ins; try again later", {"retry_after": wait})
    return list(tries)


@router.post("/sessions", status_code=201, response_model=Session, openapi_extra={"security": [{}]})
def sign_in(body: SignIn, request: Request):
    """An agent's session for its email and password in one tenant, as POST /agents/{id}/sessions mints it. Only an
    install of one tenant may leave out `tenant_id`; elsewhere that is 409 tenant_required, before anything is checked.
    A wrong email or password is one 401, as slow as the other; 10 failed tries of an email in a tenant, or 100 from
    one client (an IPv6 one by its /64), in 15 minutes are 429 until the window ends."""
    with read() as db:
        if (tenant := body.tenant_id or db.execute(_SOLE).scalar()) is None:
            raise ApiError(409, "tenant_required", "send the tenant_id of your organization to sign in")
        tries = _try(request, f"{tenant}:{body.email}")
        row = db.execute(_LOGIN, {"tenant": tenant, "email": body.email}).one_or_none()
    if not access.matches(body.password, row.hash if row else access.DUMMY_HASH) or row is None:
        raise ApiError(401, "invalid_credentials", "the email or the password is wrong")
    hot.signed_in(*tries)
    still = _UNCHANGED.bindparams(agent=row.id, tenant=tenant, hash=row.hash, sha=None)
    return _session(str(tenant), row.id, still=still)


@router.put("/me/password", status_code=204)
def change_password(body: PasswordChange, request: Request, p: Principal = AGENT) -> Response:
    """The agent's new password, proved by its current one, whose failed tries count as a sign-in's. Every session of
    the agent ends, this one too: the desk signs in again with the new password. A reset or a revoke that lands while
    the change runs wins, and the change is 401."""
    W = T.password
    with read() as db:
        email = visible_agent(db, p, p.agent_id).get("email")
        stored = db.execute(select(W.c.hash).where(W.c.agent_id == p.agent_id, W.c.tenant_id == p.tenant_id)).scalar()
    tries = _try(request, f"{p.tenant_id}:{email or p.agent_id}")
    if stored is None or not access.matches(body.current_password, stored):
        raise ApiError(403, "invalid_credentials", "the current password is wrong")
    hot.signed_in(*tries)
    proof = {"agent": p.agent_id, "tenant": p.tenant_id, "hash": stored, "sha": access.digest(access.bearer(request))}
    hash = access.hashed(body.password)
    with tx() as db:
        _new_password(db, p.tenant_id, p.agent_id, hash, _UNCHANGED.bindparams(**proof))
    return Response(status_code=204)


_CHAIN = text(
    "SELECT renewable_until FROM credential WHERE sha256 = :sha AND kind = 'agent' AND revoked_at IS NULL"
    " AND expires_at > now() AND renewable_until > now()"
)


@router.post("/me/sessions", status_code=201, response_model=Session)
def renew_session(request: Request, p: Principal = AGENT):
    """The next session of the agent's chain, so a desk renews before its tokens expire and never needs a new link.
    The old tokens stay valid to their own expiry. 12 hours after the chain's first session, sign-in or mint, the
    route is 403 sign_in_required; revoking the agent's sessions ends the chain at once."""
    chain = _CHAIN.bindparams(sha=access.digest(access.bearer(request)))
    with read() as db:
        until = db.execute(chain).scalar()
    if until is None:
        raise ApiError(403, "sign_in_required", "this session's renewals have ended; sign in again")
    return _session(p.tenant_id, p.agent_id, until, still=chain)


@router.delete("/me/sessions", status_code=204)
def sign_out(p: Principal = AGENT) -> Response:
    """The agent signs out: as DELETE /agents/{id}/sessions."""
    return close_sessions(UUID(p.agent_id), p)


@router.delete("/agents/{agent_id}/sessions", status_code=204)
def close_sessions(agent_id: UUID, p: Principal = SESSIONS) -> Response:
    """Every token of the agent stops working, its open streams close, and it goes Offline."""
    with tx() as db:
        visible_agent(db, p, agent_id)
        db.execute(select(T.presence.c.agent_id).where(T.presence.c.agent_id == agent_id).with_for_update())
        _signed_out(db, p.tenant_id, agent_id, "admin")
        access.revoke(db, p.tenant_id, agent_id=str(agent_id))
    return Response(status_code=204)


@router.get("/agents/{agent_id}/status", response_model=AgentStatus)
def get_agent_status(agent_id: str, p: Principal = ADMIN_SUP):
    with read() as db:
        return {"status": visible_agent(db, p, agent_id)["status"]}


@router.put("/agents/{agent_id}/status", response_model=AgentStatus)
def set_agent_status(agent_id: str, body: StatusSet, p: Principal = ADMIN_SUP):
    with tx() as db:
        return _change(db, p, visible_agent(db, p, agent_id)["id"], body, "admin")


@router.post("/agents/{agent_id}/sip-credentials", response_model=SipCredentials)
def rotate_sip_credentials(agent_id: str, p: Principal = ADMIN):
    """A new endpoint password, set at Plivo and shown once: HS keeps no SIP password. Only for an endpoint HS made."""
    with read() as db:
        found, tenant = visible_agent(db, p, agent_id), config.live_tenant(db, p.tenant_id)
    if found.get("endpoint_adopted"):
        raise ApiError(409, "endpoint_adopted", "HS did not make this endpoint, so it leaves its password alone")
    password = secrets.token_hex(16)
    plivo.rest(tenant, "POST", f"Endpoint/{found['endpoint_id']}/", password=password)
    with read() as db:
        access.still_admin(db, p)
    return {"username": found["endpoint_username"], "password": password}


@router.get("/me", response_model=Me)
def me(p: Principal = AGENT):
    """The agent, its desk settings, and its open text and task offers, so a desk that reloads can still answer
    them (a call offer rings the phone instead). While Redis does not answer, the desk lists no offers."""
    with read() as db:
        s = config.tenant_settings(db, p.tenant_id)
        desk = s.model_dump(include={"wrap_up", "agent_direct_dial", "recording"})
        try:
            offers = conversations.desk(db, p.tenant_id, p.agent_id)
        except RedisError:
            offers = []
        texts = sorted((a for a in offers if a.channel != "call"), key=lambda a: a.offered_at, reverse=True)
        C = T.conversation
        asked = select(C.c.id, C.c.number, C.c.customer, C.c.answered_at).where(
            C.c.tenant_id == p.tenant_id, C.c.id.in_([a.conversation_id for a in texts])
        )
        convs = {c.id: c for c in db.execute(asked)} if texts else {}
        pending = [
            {"offer_id": str(a.id), "conversation_id": str(a.conversation_id), "channel": a.channel}
            | {"conversation_number": str(convs[a.conversation_id].number)}
            | {"from": convs[a.conversation_id].customer, "via": conversations.shown(a.via, convs[a.conversation_id])}
            | {"offered_at": a.offered_at, "expires_at": a.expires_at}
            for a in texts
        ]
        found = {"settings": desk, "pending_offers": pending, "grants": access.seen(db, p).grants}
        return agent(db, p.tenant_id, p.agent_id) | found


@router.put("/me/status", response_model=AgentStatus)
def set_my_status(body: StatusSet, p: Principal = AGENT):
    with tx() as db:
        return _change(db, p, p.agent_id, body, "manual")


@router.post("/me/idle", response_model=AgentStatus)
def go_idle(p: Principal = AGENT):
    """The desk saw no activity for the settings' idle minutes: an available agent takes the idle status, which
    takes no offers. One on any other status, or with a conversation offered or active on any channel, keeps it."""
    with tx() as db:
        idle = config.tenant_settings(db, p.tenant_id).idle
        if not (idle.enabled and idle.status_id):
            raise ApiError(409, "idle_tracking_off", "idle tracking is off (PATCH /settings idle)")
        db.execute(select(T.presence.c.agent_id).where(T.presence.c.agent_id == p.agent_id).with_for_update())
        if _available(db, p.agent_id) and not conversations.desk(db, p.tenant_id, p.agent_id, ("offered", "active")):
            set_status(db, p.tenant_id, p.agent_id, idle.status_id, "idle")
        return {"status": agent(db, p.tenant_id, p.agent_id)["status"]}


@router.post("/me/heartbeat", status_code=204)
def heartbeat(request: Request, _: Ping | None = None) -> Response:
    """Presence: the second beat of a session applies the sign-in status; 40 s without one sets Offline. An HS
    agent token is checked by the beat itself; a plugin's credential is resolved first."""
    token, P = access.bearer(request), T.presence
    if token.startswith(AGENT_PREFIX):
        query = _WHO.bindparams(sha=access.digest(token))
    elif (p := access.resolve(token, request)) and p.kind == "agent":
        query = select(P.c.tenant_id, P.c.agent_id).where(P.c.agent_id == p.agent_id, P.c.tenant_id == p.tenant_id)
    else:
        raise ApiError(401, "unauthorized", "missing or invalid credential")
    with read() as db:
        who = db.execute(query).first()
    if who is None:
        raise ApiError(401, "unauthorized", "missing or invalid credential")
    try:
        beats, whole = hot.beat(who.tenant_id, who.agent_id)
        if beats == hot.SIGN_IN:
            _sign_in(who.tenant_id, who.agent_id, whole, credential=query)
            hot.recount(who.tenant_id, who.agent_id, hot.SIGN_IN)
    except RedisError as e:
        raise ApiError(503, "heartbeat_failed", "presence is unavailable; beat again") from e
    return Response(status_code=204)


def _sign_in(tenant_id, agent_id, whole: bool, credential) -> None:
    """An agent not available takes the sign-in status. While a Redis loss leaves the tenant's keys unrebuilt, the
    count may be a lost one restarting, so only an agent signed out signs in. The beat's `credential` query runs again
    under the presence lock, which closing the sessions takes first, so a session closed since signs nothing in."""
    C, P = T.config, T.presence
    status = select(C.c.key, C.c.body["availability"].as_boolean().label("available")).join(P, P.c.status_id == C.c.id)
    with tx() as db:
        db.execute(select(P.c.agent_id).where(P.c.agent_id == agent_id).with_for_update())
        if db.execute(credential).first() is None:
            return
        db.execute(
            update(P)
            .where(P.c.agent_id == agent_id)
            .values(signed_in_at=func.now(), first_available_at=FIRST_AVAILABLE, unreachable=False)
        )
        now = db.execute(status.where(P.c.agent_id == agent_id)).first()
        if (
            now
            and not now.available
            and (whole or now.key == "offline")
            and (status := _edge(db, tenant_id, "sign_in"))
        ):
            set_status(db, tenant_id, agent_id, status, "heartbeat", CONNECTED)


@router.get("/transfer-targets", response_model=TransferTargets)
def transfer_targets(channel: Channel = "call", p: Principal = AGENT):
    """Every other agent with its status, `available` when that status is and it has a free slot on the channel,
    `busy` when its slots there are full; and every team with its count of available agents. The unavailable and
    the empty come last, then by name. A call also lists the tenant's saved destinations."""
    kinds = ["agent", "team"] + (["destination"] if channel == "call" else [])
    full = {f.partition(":")[2] for f in hot.full(p.tenant_id, channel)}
    with read() as db:
        rows = db.execute(_PICKER, {"tenant": p.tenant_id, "me": p.agent_id, "kinds": kinds}).all()
    agents = [
        {
            "id": r.id,
            "name": r.name,
            "teams": list(dict.fromkeys(r.teams)),
            "status": r.status_id and {"id": r.status_id, "name": r.status},
            "availability": ("busy" if r.id in full else "available") if r.ready else "unavailable",
        }
        for r in rows
        if r.kind == "agent"
    ]
    free = Counter(t for a in agents if a["availability"] == "available" for t in a["teams"])
    return {
        "agents": sorted(agents, key=lambda a: a["availability"] != "available"),
        "teams": sorted(
            ({"id": r.id, "name": r.name, "available_agents": free[r.id]} for r in rows if r.kind == "team"),
            key=lambda t: not t["available_agents"],
        ),
        "destinations": [{"id": r.id, "name": r.name} for r in rows if r.kind == "destination"],
    }


@jobs.handler("agent_offline")
def _offline(job: Row) -> None:
    """Rechecked under the presence row lock, so a beat since the sweep keeps the agent on."""
    P, tenant_id, agent_id = T.presence, job.payload["tenant_id"], job.payload["agent_id"]
    with tx() as db:
        held = db.execute(select(P.c.agent_id).where(P.c.agent_id == agent_id).with_for_update()).first()
        if held and hot.silent(tenant_id, agent_id):
            _signed_out(db, tenant_id, agent_id, "heartbeat", DISCONNECTED)
