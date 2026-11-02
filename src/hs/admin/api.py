"""The tenant's people and configuration: agents and their Plivo endpoints, teams, skills, roles, presence
statuses, dispositions, settings and the status map; and the agent's own desk under /me."""

import json
import secrets
import uuid
from typing import Literal

from fastapi import APIRouter, BackgroundTasks, Depends, Response
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, ConfigDict, Field, ValidationError, create_model
from sqlalchemy import String, bindparam, cast, column, func, insert, select, table, text, update
from sqlalchemy.exc import IntegrityError

from hs.access import AGENTS, SKILLS, TEAMS, Principal, as_uuid, owned, require, revoke_sessions, tenant_plivo
from hs.admin import rbac, seeds, status
from hs.admin.heartbeat import HeartbeatReqMeta, handle_agent_ping
from hs.errors import ApiError
from hs.infra.constants import DEFAULT_WRAP_UP_TIME
from hs.infra.db import core_engine
from hs.infra.redis import Cache
from hs.paging import Page, Paging, page, paging
from hs.plivo import rest
from hs.routing import constants as rc
from hs.setup.actions import Say
from hs.setup.api import is_registered

router = APIRouter()
ADMIN, ADMIN_SECRET = Depends(require("key:admin")), Depends(require("key:admin", secret=True))
ADMIN_AGENT, ADMIN_SUP, AGENT = (
    Depends(require(*a)) for a in (("key:admin", "agent"), ("key:admin", "sup"), ("agent",))
)
SIP_DOMAIN = "phone.plivo.com"
E164 = r"^\+[1-9]\d{1,14}$"
ONE = Paging(1, None)


class Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


# --- agents -------------------------------------------------------------------------------------------------------

_AGENT_ROWS = text(
    "SELECT aom.id AS cursor, aom.uuid::text AS id, a.name, a.email, r.name AS role, aom.caller_id,"
    " aom.sip_endpoint, aom.sip_endpoint_id AS endpoint_id, st.uuid::text AS status_id, st.status AS status_name,"
    " COALESCE((SELECT json_agg(t.uuid::text ORDER BY t.id) FROM aom_team at JOIN team t ON t.id = at.team"
    "   WHERE at.aom = aom.id AND at.is_deleted IS NOT TRUE AND t.is_deleted IS NOT TRUE), '[]') AS teams,"
    " COALESCE((SELECT json_agg(json_build_object('skill_id', s.uuid::text, 'level', k.skill_level) ORDER BY s.id)"
    "   FROM aom_skills k JOIN skills s ON s.id = k.skill"
    "   WHERE k.aom = aom.id AND k.is_deleted IS NOT TRUE AND s.is_deleted IS NOT TRUE), '[]') AS skills"
    " FROM account_org_mapping aom JOIN account a ON a.id = aom.account"
    " LEFT JOIN role r ON r.id = aom.role LEFT JOIN org_statuses st ON st.id = aom.agent_status"
    " WHERE aom.organization = :org AND aom.is_deleted IS NOT TRUE"
    " AND (CAST(:aid AS int) IS NULL OR aom.id = :aid)"
    " AND (CAST(:email AS text) IS NULL OR lower(a.email) = lower(:email))"
    " AND (CAST(:ids AS uuid[]) IS NULL OR aom.uuid = ANY(CAST(:ids AS uuid[])))"
    " AND (CAST(:after AS int) IS NULL OR aom.id > :after) ORDER BY aom.id LIMIT :limit"
)
_ROLE = text("SELECT id FROM role WHERE organization = :org AND name = :name AND is_deleted IS NOT TRUE")
_NEW_AGENT = text(
    "WITH acc AS (INSERT INTO account (uuid, created_at, updated_at, is_deleted, email, name)"
    "  VALUES (gen_random_uuid(), now(), now(), false, :email, :name) RETURNING id),"
    " aom AS (INSERT INTO account_org_mapping (uuid, created_at, updated_at, is_deleted, account, organization,"
    "  status, role, caller_id, agent_status) SELECT :agent, now(), now(), false, acc.id, :org, 'active', :role,"
    "  :caller_id, (SELECT agent_status_action FROM org_event_actions WHERE organization = :org"
    "  AND event = 'AGENT_LOGGED_OUT') FROM acc RETURNING id)"
    " INSERT INTO aom_routing_params (uuid, created_at, updated_at, is_deleted, aom, available_since,"
    "  contacts_handled, contacts_handle_time) SELECT gen_random_uuid(), now(), now(), false, aom.id, now(), 0, 0"
    " FROM aom RETURNING aom"
)
_APP_ID = text("SELECT plivo_app_id FROM org_setting WHERE organization = :org")
_ENDPOINT = text("UPDATE account_org_mapping SET sip_endpoint = :sip, sip_endpoint_id = :eid WHERE id = :aid")
_PATCH_AGENT = text(
    "UPDATE account_org_mapping SET role = COALESCE(:role, role),"
    " caller_id = CASE WHEN :set_caller THEN :caller_id ELSE caller_id END, updated_at = now() WHERE id = :aid"
)
_RENAME_AGENT = text(
    "UPDATE account SET name = :name, updated_at = now()"
    " WHERE id = (SELECT account FROM account_org_mapping WHERE id = :aid)"
)
_CLEAR_TEAMS = text("DELETE FROM aom_team WHERE aom = :aid")
_ADD_TEAM = text(
    "INSERT INTO aom_team (uuid, created_at, updated_at, is_deleted, organization, aom, team)"
    " VALUES (gen_random_uuid(), now(), now(), false, :org, :aid, :team)"
)
_CLEAR_SKILLS = text("DELETE FROM aom_skills WHERE aom = :aid")
_ADD_SKILL = text(
    "INSERT INTO aom_skills (uuid, created_at, updated_at, is_deleted, aom, skill, skill_level)"
    " VALUES (gen_random_uuid(), now(), now(), false, :aid, :skill, :level)"
)
_DELETE_AGENT = text(
    "WITH aom AS (UPDATE account_org_mapping SET is_deleted = true, status = 'inactive', updated_at = now()"
    " WHERE id = :aid RETURNING account) UPDATE account SET is_deleted = true, updated_at = now()"
    " WHERE id = (SELECT account FROM aom)"
)


class Status(BaseModel):
    id: str
    name: str


class SkillLevel(Strict):
    skill_id: str
    level: int = Field(ge=1, le=10)


Role = Literal["admin", "supervisor", "agent"]


class Agent(BaseModel):
    id: str
    name: str
    external_id: str | None
    role: str | None
    teams: list[str]
    skills: list[SkillLevel]
    caller_id: str | None
    endpoint_id: str | None
    endpoint_username: str | None
    status: Status | None


class AgentIn(Strict):
    name: str = Field(min_length=1, max_length=200)
    external_id: str | None = Field(None, min_length=1, max_length=200)
    role: Role
    teams: list[str] = []
    skills: list[SkillLevel] = []
    caller_id: str | None = Field(None, pattern=E164)


class AgentPatch(Strict):
    name: str | None = Field(None, min_length=1, max_length=200)
    role: Role | None = None
    teams: list[str] | None = None
    skills: list[SkillLevel] | None = None
    caller_id: str | None = Field(None, pattern=E164)


class AgentStatus(BaseModel):
    status: Status | None


class StatusSet(Strict):
    status_id: str


class SipCredentials(BaseModel):
    username: str
    password: str


def _username(sip: str | None) -> str | None:
    return sip.removeprefix("sip:").partition("@")[0] if sip else None


def _agent_rows(p: Principal, pg: Paging = ONE, **where) -> list[dict]:
    args = {"aid": None, "email": None, "ids": None} | where | {"org": p.org_id, "after": _after(pg)}
    with core_engine.connect() as conn:
        rows = [dict(r._mapping) for r in conn.execute(_AGENT_ROWS, args | {"limit": pg.limit + 1})]
    for r in rows:
        local = r["email"].rpartition("@")[0]
        r["external_id"] = None if local == r["id"] else local
        r["endpoint_username"] = _username(r["sip_endpoint"])
        r["status"] = {"id": r["status_id"], "name": r["status_name"]} if r["status_id"] else None
    return rows


def _agent(p: Principal, agent_id: str) -> dict:
    """The tenant's agent; for an agent token, itself or one its role lets it see."""
    aid = _row(AGENTS, agent_id, p)
    agent = _agent_rows(p, aid=aid)[0]
    if agent["id"] != p.agent_id and (seen := rbac.accessible_agents(p)) is not None and agent["id"] not in seen:
        raise ApiError(404, "not_found", "no such resource")
    return agent


def _row(query, public_id: str, p: Principal, field: str | None = None) -> int:
    """The row id behind one of the tenant's public ids: 404 for a path id, 422 for a body `field`."""
    return next(iter(owned(query, [public_id], p, field).values()))


def _email(external_id: str, p: Principal) -> str:
    """An account is global per email, so each agent gets a private one that nothing reads or mails."""
    return f"{external_id}@{p.tenant_id}.invalid"


def _plivo(p: Principal, method: str, path: str, **body) -> dict:
    return rest.ok(tenant_plivo(p.tenant_id), method, path, **body)


def _set_teams_and_skills(conn, aid: int, p: Principal, teams: list[str] | None, skills: list[SkillLevel] | None):
    if teams is not None:
        found = owned(TEAMS, teams, p, "teams")
        conn.execute(_CLEAR_TEAMS, {"aid": aid})
        for team in found.values():
            conn.execute(_ADD_TEAM, {"aid": aid, "team": team, "org": p.org_id})
    if skills is not None:
        found = owned(SKILLS, [s.skill_id for s in skills], p, "skills")
        levels = {as_uuid(s.skill_id): s.level for s in skills}
        conn.execute(_CLEAR_SKILLS, {"aid": aid})
        for skill_id, skill in found.items():
            conn.execute(_ADD_SKILL, {"aid": aid, "skill": skill, "level": levels[skill_id]})


def _check_caller_id(p: Principal, number: str | None) -> None:
    if number and not is_registered(p.org_id, number):
        raise ApiError(422, "from_not_allowed", "caller_id must be a number the tenant routes (PUT /numbers)")


def _role_id(conn, p: Principal, role: str) -> int:
    return conn.execute(_ROLE, {"org": p.org_id, "name": role}).scalar_one()


@router.get("/agents", response_model=Page[Agent])
def list_agents(external_id: str | None = None, p: Principal = ADMIN_SUP, pg: Paging = Depends(paging)):
    seen = rbac.accessible_agents(p)
    email = _email(external_id, p) if external_id else None
    return page(_agent_rows(p, pg, email=email, ids=None if seen is None else sorted(seen)), pg, "cursor")


@router.post("/agents", status_code=201, response_model=Agent)
def create_agent(body: AgentIn, p: Principal = ADMIN):
    """An active member with its own Plivo endpoint, Offline, with the routing row every strategy joins."""
    _check_caller_id(p, body.caller_id)
    agent_id = str(uuid.uuid4())
    email = _email(body.external_id or agent_id, p)
    try:
        with core_engine.begin() as conn:
            args = {"agent": agent_id, "org": p.org_id, "email": email, "name": body.name}
            args |= {"role": _role_id(conn, p, body.role), "caller_id": body.caller_id}
            aid = conn.execute(_NEW_AGENT, args).scalar_one()
            _set_teams_and_skills(conn, aid, p, body.teams, body.skills)
            app_id = conn.execute(_APP_ID, {"org": p.org_id}).scalar()
            endpoint = {"username": "hsagent", "password": secrets.token_hex(16), "alias": agent_id, "app_id": app_id}
            made = _plivo(p, "POST", "Endpoint/", **endpoint)
            sip = f"sip:{made['username']}@{SIP_DOMAIN}"
            conn.execute(_ENDPOINT, {"sip": sip, "eid": str(made["endpoint_id"]), "aid": aid})
    except IntegrityError:
        if not (existing := _agent_rows(p, email=email)):
            raise
        raise ApiError(409, "agent_exists", "an agent has this external_id", {"id": existing[0]["id"]}) from None
    return _agent(p, agent_id)


@router.get("/agents/{agent_id}", response_model=Agent)
def get_agent(agent_id: str, p: Principal = ADMIN):
    return _agent(p, agent_id)


@router.patch("/agents/{agent_id}", response_model=Agent)
def patch_agent(agent_id: str, body: AgentPatch, p: Principal = ADMIN):
    """A new role ends the agent's sessions: its tokens carry the old one."""
    before = _agent(p, agent_id)
    _check_caller_id(p, body.caller_id)
    aid = before["cursor"]
    with core_engine.begin() as conn:
        role = _role_id(conn, p, body.role) if body.role else None
        set_caller = "caller_id" in body.model_fields_set
        conn.execute(_PATCH_AGENT, {"aid": aid, "role": role, "set_caller": set_caller, "caller_id": body.caller_id})
        if body.name:
            conn.execute(_RENAME_AGENT, {"aid": aid, "name": body.name})
        _set_teams_and_skills(conn, aid, p, body.teams, body.skills)
    if body.role and body.role != before["role"]:
        revoke_sessions(before["id"])
    return _agent(p, agent_id)


@router.delete("/agents/{agent_id}", status_code=204)
def delete_agent(agent_id: str, p: Principal = ADMIN) -> Response:
    agent = _agent(p, agent_id)
    auth_id = tenant_plivo(p.tenant_id)[0]
    # Call capacity is held from the offer until the agent is released, so a held slot means a live call.
    if agent["sip_endpoint"] and Cache.get_instance().client.scard(
        rc.agent_capacity_key(auth_id, agent["sip_endpoint"], rc.CHANNEL_CALL)
    ):
        raise ApiError(409, "agent_on_call", "the agent is on a call")
    if agent["endpoint_id"]:
        try:
            _plivo(p, "DELETE", f"Endpoint/{agent['endpoint_id']}/")
        except ApiError as e:
            if e.details.get("status") != 404:
                raise
    with core_engine.begin() as conn:
        conn.execute(_DELETE_AGENT, {"aid": agent["cursor"]})
    revoke_sessions(agent["id"])
    return Response(status_code=204)


def _status_of(p: Principal, agent_id: str) -> dict:
    return {"status": _agent(p, agent_id)["status"]}


def _change(p: Principal, agent_id: str, body: StatusSet, reason: str) -> dict:
    _row(STATUSES, body.status_id, p, "status_id")
    status.change_status(agent_id, body.status_id, reason)
    return _status_of(p, agent_id)


@router.get("/agents/{agent_id}/status", response_model=AgentStatus)
def get_agent_status(agent_id: str, p: Principal = ADMIN_SUP):
    return _status_of(p, agent_id)


@router.put("/agents/{agent_id}/status", response_model=AgentStatus)
def set_agent_status(agent_id: str, body: StatusSet, p: Principal = ADMIN_SUP):
    return _change(p, _agent(p, agent_id)["id"], body, "admin")


@router.post("/agents/{agent_id}/sip-credentials", response_model=SipCredentials)
def rotate_sip_credentials(agent_id: str, p: Principal = ADMIN_SECRET):
    """A new endpoint password, set at Plivo and shown once: HS keeps no SIP password."""
    agent = _agent(p, agent_id)
    if not agent["endpoint_id"]:
        raise ApiError(409, "agent_has_no_endpoint", "the agent has no Plivo endpoint")
    password = secrets.token_hex(16)
    _plivo(p, "POST", f"Endpoint/{agent['endpoint_id']}/", password=password)
    return {"username": agent["endpoint_username"], "password": password}


# --- teams, skills, presence statuses -----------------------------------------------------------------------------


def _table(name: str, *cols: str):
    return table(name, *map(column, ("id", "uuid", "organization", "is_deleted", "created_at", "updated_at", *cols)))


def _owned_query(t):
    """(uuid, id) of the tenant's live rows among `:ids`, the shape `owned` takes."""
    ids = t.c.uuid.in_(bindparam("ids", expanding=True))
    return select(cast(t.c.uuid, String), t.c.id).where(
        t.c.organization == bindparam("org"), t.c.is_deleted.isnot(True), ids
    )


class Named(BaseModel):
    id: str
    name: str


class NameIn(Strict):
    name: str = Field(min_length=1, max_length=200)


class NamePatch(Strict):
    name: str | None = Field(None, min_length=1, max_length=200)


class PresenceStatus(Named):
    availability: bool


class PresenceStatusIn(NameIn):
    availability: bool = False


class PresenceStatusPatch(NamePatch):
    availability: bool | None = None


TEAM, SKILL, STATUS = _table("team", "name"), _table("skills", "name"), _table("org_statuses", "status", "availability")
STATUSES = _owned_query(STATUS)
_STATUS_IN_USE = text(
    "SELECT EXISTS (SELECT 1 FROM org_event_actions WHERE agent_status_action = :id AND is_deleted IS NOT TRUE)"
    " OR EXISTS (SELECT 1 FROM account_org_mapping WHERE agent_status = :id AND is_deleted IS NOT TRUE)"
)


def _unlink_team(conn, row_id: int) -> None:
    conn.execute(text("DELETE FROM aom_team WHERE team = :id"), {"id": row_id})


def _unlink_skill(conn, row_id: int) -> None:
    conn.execute(text("DELETE FROM aom_skills WHERE skill = :id"), {"id": row_id})


def _status_unused(conn, row_id: int) -> None:
    if conn.execute(_STATUS_IN_USE, {"id": row_id}).scalar():
        raise ApiError(409, "status_in_use", "agents or event actions use this status")


def _crud(path: str, t, owned_query, out, new, patch, cols: dict[str, str], on_delete) -> None:
    """List, create, read, update and delete for a tenant table of named rows; `cols` maps API field -> column."""

    def rows(conn, org: int, pg: Paging, row_id: int | None = None) -> list[dict]:
        q = select(
            t.c.id.label("cursor"), cast(t.c.uuid, String).label("id"), *(t.c[c].label(f) for f, c in cols.items())
        )
        q = q.where(t.c.organization == org, t.c.is_deleted.isnot(True)).order_by(t.c.id).limit(pg.limit + 1)
        if row_id is not None:
            q = q.where(t.c.id == row_id)
        if (after := _after(pg)) is not None:
            q = q.where(t.c.id > after)
        return [dict(r) for r in conn.execute(q).mappings()]

    def one(conn, p: Principal, rid: str) -> dict:
        return rows(conn, p.org_id, ONE, _row(owned_query, rid, p))[0]

    def values(body: BaseModel, partial: bool = False) -> dict:
        return {cols[f]: v for f, v in body.model_dump(exclude_unset=partial).items() if v is not None}

    @router.get(path, response_model=Page[out])
    def list_(p: Principal = ADMIN_AGENT, pg: Paging = Depends(paging)):
        with core_engine.connect() as conn:
            return page(rows(conn, p.org_id, pg), pg, "cursor")

    @router.post(path, status_code=201, response_model=out)
    def create(body: new, p: Principal = ADMIN):
        base = {"uuid": func.gen_random_uuid(), "created_at": func.now(), "updated_at": func.now(), "is_deleted": False}
        with core_engine.begin() as conn:
            row_id = conn.execute(insert(t).values(base | values(body) | {"organization": p.org_id}).returning(t.c.id))
            return rows(conn, p.org_id, ONE, row_id.scalar_one())[0]

    @router.get(path + "/{rid}", response_model=out)
    def get(rid: str, p: Principal = ADMIN_AGENT):
        with core_engine.connect() as conn:
            return one(conn, p, rid)

    @router.patch(path + "/{rid}", response_model=out)
    def patch_(rid: str, body: patch, p: Principal = ADMIN):
        with core_engine.begin() as conn:
            row_id = one(conn, p, rid)["cursor"]
            if changes := values(body, partial=True):
                conn.execute(update(t).where(t.c.id == row_id).values(changes | {"updated_at": func.now()}))
            return one(conn, p, rid)

    @router.delete(path + "/{rid}", status_code=204)
    def delete(rid: str, p: Principal = ADMIN) -> Response:
        with core_engine.begin() as conn:
            row_id = one(conn, p, rid)["cursor"]
            on_delete(conn, row_id)
            conn.execute(update(t).where(t.c.id == row_id).values(is_deleted=True, updated_at=func.now()))
        return Response(status_code=204)


_crud("/teams", TEAM, TEAMS, Named, NameIn, NamePatch, {"name": "name"}, _unlink_team)
_crud("/skills", SKILL, SKILLS, Named, NameIn, NamePatch, {"name": "name"}, _unlink_skill)
_crud(
    "/agent-statuses",
    STATUS,
    STATUSES,
    PresenceStatus,
    PresenceStatusIn,
    PresenceStatusPatch,
    {"name": "status", "availability": "availability"},
    _status_unused,
)

_ROLES = text(
    "SELECT id AS cursor, uuid::text AS id, name FROM role WHERE organization = :org AND is_deleted IS NOT TRUE"
    " AND (CAST(:after AS int) IS NULL OR id > :after) ORDER BY id LIMIT :limit"
)


@router.get("/roles", response_model=Page[Named])
def list_roles(p: Principal = ADMIN, pg: Paging = Depends(paging)):
    with core_engine.connect() as conn:
        args = {"org": p.org_id, "after": _after(pg), "limit": pg.limit + 1}
        return page([dict(r._mapping) for r in conn.execute(_ROLES, args)], pg, "cursor")


def _after(pg: Paging) -> int | None:
    """The cursor of the lists here: the integer sort key of the last row."""
    try:
        return None if pg.after is None else int(pg.after)
    except ValueError as e:
        raise ApiError(422, "invalid_cursor", "cursor is not one this API returned") from e


# --- dispositions ---------------------------------------------------------------------------------------------------

DISPOSITION = _table("dispositions", "internal")
DISPOSITIONS = _owned_query(DISPOSITION)
SYSTEM_DISPOSITIONS = DISPOSITIONS.where(DISPOSITION.c.internal.is_(True))
_DISPOSITIONS = text(
    "SELECT d.sequence AS cursor, d.id AS row_id, d.uuid::text AS id, d.name, up.uuid::text AS parent_disposition_id,"
    " COALESCE(d.internal, false) AS system, d.note_required, d.sequence"
    " FROM dispositions d LEFT JOIN dispositions up ON up.id = d.parent_disposition_id"
    " WHERE d.organization = :org AND d.is_deleted IS NOT TRUE AND (:system OR d.internal IS NOT TRUE)"
    " AND (CAST(:did AS int) IS NULL OR d.id = :did)"
    " AND (CAST(:after AS int) IS NULL OR d.sequence > :after) ORDER BY d.sequence LIMIT :limit"
)
# Limit: MAX + 1 can hand two concurrent creates one sequence; a per-tenant sequence would not.
_NEW_DISPOSITION = text(
    "INSERT INTO dispositions (uuid, created_at, updated_at, is_deleted, organization, name, internal, answered, state,"
    " note_required, parent_disposition_id, sequence) VALUES (gen_random_uuid(), now(), now(), false, :org, :name,"
    " :system, NOT :system, :system, :note_required, :parent,"
    " (SELECT COALESCE(MAX(sequence), 0) + 1 FROM dispositions WHERE organization = :org))"
    " RETURNING id"
)
_PATCH_DISPOSITION = text(
    "UPDATE dispositions SET name = COALESCE(:name, name), note_required = COALESCE(:note_required, note_required),"
    " parent_disposition_id = CASE WHEN :set_parent THEN :parent ELSE parent_disposition_id END, updated_at = now()"
    " WHERE id = :did"
)
_DELETE_DISPOSITION = text(
    "UPDATE dispositions SET is_deleted = true, updated_at = now() WHERE id = :did OR parent_disposition_id = :did"
)
_SEQUENCES = text("SELECT sequence FROM dispositions WHERE id IN :ids").bindparams(bindparam("ids", expanding=True))
_SEQUENCE = text("UPDATE dispositions SET sequence = :seq, updated_at = now() WHERE id = :did")


class Disposition(BaseModel):
    id: str
    name: str
    parent_disposition_id: str | None
    system: bool
    note_required: bool
    sequence: int


class DispositionIn(Strict):
    name: str = Field(min_length=1, max_length=200)
    parent_disposition_id: str | None = None
    note_required: bool = False
    system: bool = False


class DispositionPatch(Strict):
    name: str | None = Field(None, min_length=1, max_length=200)
    parent_disposition_id: str | None = None
    note_required: bool | None = None


class Reorder(Strict):
    ids: list[str] = Field(min_length=1)


def _dispositions(conn, p: Principal, pg: Paging = ONE, did: int | None = None) -> list[dict]:
    """Agents never see system dispositions."""
    args = {"org": p.org_id, "system": p.kind != "agent", "did": did, "after": _after(pg), "limit": pg.limit + 1}
    return [dict(r._mapping) for r in conn.execute(_DISPOSITIONS, args)]


def _disposition(conn, p: Principal, disposition_id: str) -> dict:
    if not (rows := _dispositions(conn, p, did=_row(DISPOSITIONS, disposition_id, p))):
        raise ApiError(404, "not_found", "no such resource")
    return rows[0]


def _parent(p: Principal, parent_id: str | None) -> int | None:
    return _row(DISPOSITIONS, parent_id, p, "parent_disposition_id") if parent_id else None


def _locked(d: dict) -> ApiError:
    return ApiError(
        409, "system_disposition", "system dispositions cannot be renamed, moved or deleted", {"id": d["id"]}
    )


@router.get("/dispositions", response_model=Page[Disposition])
def list_dispositions(p: Principal = ADMIN_AGENT, pg: Paging = Depends(paging)):
    with core_engine.connect() as conn:
        return page(_dispositions(conn, p, pg), pg, "cursor")


@router.post("/dispositions", status_code=201, response_model=Disposition)
def create_disposition(body: DispositionIn, p: Principal = ADMIN):
    """`system: true` makes a locked row that the status map can point at."""
    args = body.model_dump(exclude={"parent_disposition_id"}) | {"org": p.org_id}
    with core_engine.begin() as conn:
        did = conn.execute(_NEW_DISPOSITION, args | {"parent": _parent(p, body.parent_disposition_id)}).scalar_one()
        return _dispositions(conn, p, did=did)[0]


@router.post("/dispositions/reorder", status_code=204)
def reorder_dispositions(body: Reorder, p: Principal = ADMIN) -> Response:
    """The listed dispositions take the places they already hold, in the order given; the rest stay put."""
    found = owned(DISPOSITIONS, body.ids, p, "ids")
    ids = list(dict.fromkeys(found[as_uuid(i)] for i in body.ids))
    with core_engine.begin() as conn:
        places = sorted(conn.execute(_SEQUENCES, {"ids": ids}).scalars())
        for did, seq in zip(ids, places, strict=True):
            conn.execute(_SEQUENCE, {"did": did, "seq": seq})
    return Response(status_code=204)


@router.get("/dispositions/{disposition_id}", response_model=Disposition)
def get_disposition(disposition_id: str, p: Principal = ADMIN_AGENT):
    with core_engine.connect() as conn:
        return _disposition(conn, p, disposition_id)


@router.patch("/dispositions/{disposition_id}", response_model=Disposition)
def patch_disposition(disposition_id: str, body: DispositionPatch, p: Principal = ADMIN):
    with core_engine.begin() as conn:
        d = _disposition(conn, p, disposition_id)
        moved = "parent_disposition_id" in body.model_fields_set
        if d["system"] and (moved or body.name not in (None, d["name"])):
            raise _locked(d)
        args = {"did": d["row_id"], "name": body.name, "note_required": body.note_required, "set_parent": moved}
        conn.execute(_PATCH_DISPOSITION, args | {"parent": _parent(p, body.parent_disposition_id)})
        return _disposition(conn, p, disposition_id)


@router.delete("/dispositions/{disposition_id}", status_code=204)
def delete_disposition(disposition_id: str, p: Principal = ADMIN) -> Response:
    """Also deletes its sub-dispositions."""
    with core_engine.begin() as conn:
        if (d := _disposition(conn, p, disposition_id))["system"]:
            raise _locked(d)
        conn.execute(_DELETE_DISPOSITION, {"did": d["row_id"]})
    return Response(status_code=204)


# --- status map -----------------------------------------------------------------------------------------------------

_STATUS_MAP = text("SELECT voice_settings->'status_map' FROM org_setting WHERE organization = :org")
_SYSTEM_NAMES = text(
    "SELECT name, uuid::text FROM dispositions WHERE organization = :org AND internal AND is_deleted IS NOT TRUE"
    " ORDER BY id"
)
_SAVE_STATUS_MAP = text(
    "UPDATE org_setting SET voice_settings = jsonb_set(voice_settings, '{status_map}', CAST(:map AS jsonb)),"
    " updated_at = now() WHERE organization = :org"
)
InboundMap, OutboundMap = (
    create_model(f"{d.title()}StatusMap", __base__=Strict, **dict.fromkeys(seeds.STATUS_MAP_DEFAULTS[d], (str, ...)))
    for d in ("inbound", "outbound")
)


class StatusMap(Strict):
    """Call result -> system disposition id, per direction."""

    inbound: InboundMap
    outbound: OutboundMap


def _default_map(conn, p: Principal) -> dict:
    by_name = dict(conn.execute(_SYSTEM_NAMES, {"org": p.org_id}).all())
    return {d: {r: by_name[n] for r, n in m.items()} for d, m in seeds.STATUS_MAP_DEFAULTS.items()}


@router.get("/status-map", response_model=StatusMap)
def get_status_map(defaults: bool = False, p: Principal = ADMIN):
    with core_engine.connect() as conn:
        base = _default_map(conn, p)
        stored = {} if defaults else conn.execute(_STATUS_MAP, {"org": p.org_id}).scalar() or {}
    return {d: base[d] | stored.get(d, {}) for d in base}


@router.put("/status-map", response_model=StatusMap)
def put_status_map(body: StatusMap, p: Principal = ADMIN):
    mapping = body.model_dump()
    try:
        owned(SYSTEM_DISPOSITIONS, [i for m in mapping.values() for i in m.values()], p, "status_map")
    except ApiError as e:
        raise ApiError(422, "not_system_disposition", "map results to system dispositions only", e.details) from e
    with core_engine.begin() as conn:
        conn.execute(_SAVE_STATUS_MAP, {"org": p.org_id, "map": json.dumps(mapping)})
    return mapping


# --- settings -------------------------------------------------------------------------------------------------------

_SETTINGS = text(
    "SELECT o.auth_id, o.call_recording_inbound, o.call_recording_outbound, o.allow_recording_pause_play,"
    " rs.wrap_up_configuration, rs.reservation_timeout, rs.capacity, s.voice_settings,"
    " st.uuid::text AS rona_status_id, oea.event_metadata AS rona"
    " FROM organization o LEFT JOIN routing_settings rs ON rs.organization = o.id AND rs.is_deleted IS NOT TRUE"
    " LEFT JOIN org_setting s ON s.organization = o.id AND s.is_deleted IS NOT TRUE"
    " LEFT JOIN org_event_actions oea ON oea.organization = o.id AND oea.event = 'AGENT_MISSED_CALLS'"
    "  AND oea.is_deleted IS NOT TRUE"
    " LEFT JOIN org_statuses st ON st.id = oea.agent_status_action WHERE o.id = :org"
)
_SAVE_RECORDING = text(
    "UPDATE organization SET call_recording_inbound = :inbound, call_recording_outbound = :outbound,"
    " allow_recording_pause_play = :allow_pause, updated_at = now() WHERE id = :org"
)
_SAVE_ROUTING = text(
    "UPDATE routing_settings SET wrap_up_configuration = CAST(:wrap_up AS json),"
    " reservation_timeout = CAST(:ring AS json), capacity = CAST(:capacity AS json), updated_at = now()"
    " WHERE organization = :org"
)
_SAVE_VOICE = text(
    "UPDATE org_setting SET voice_settings = voice_settings || CAST(:voice AS jsonb), updated_at = now()"
    " WHERE organization = :org"
)
_SAVE_RONA = text(
    "UPDATE org_event_actions SET agent_status_action = :status, event_metadata = CAST(:meta AS json),"
    " updated_at = now() WHERE organization = :org AND event = 'AGENT_MISSED_CALLS'"
)


class Recording(Strict):
    inbound: bool = False
    outbound: bool = False
    allow_pause: bool = True


class WrapUp(Strict):
    mode: Literal["manual", "auto"] = "manual"
    seconds: int = Field(DEFAULT_WRAP_UP_TIME, ge=1, le=3600)
    max_extend_sec: int = Field(30, ge=0, le=3600)


class Consent(Strict):
    inbound: Say | None = None
    outbound: Say | None = None


class Rona(Strict):
    strikes: int = Field(2, ge=1, le=100)
    status_id: str


class Capacity(Strict):
    call: int = Field(1, ge=1, le=1)


class Monitor(Strict):
    barge_notice: str | None = Field("A supervisor has joined the call", min_length=1, max_length=500)


class Settings(Strict):
    recording: Recording = Recording()
    wrap_up: WrapUp = WrapUp()
    consent: Consent = Consent()
    ring_timeout_sec: int = Field(rc.RESERVATION_TIMEOUT_DEFAULT_SECONDS, ge=5, le=120)
    rona: Rona
    capacity: Capacity = Capacity()
    agent_direct_dial: bool = False
    monitor: Monitor = Monitor()


def merge_patch(target, patch):
    """RFC 7396: objects merge key by key, null removes a key, any other value replaces."""
    if not isinstance(patch, dict):
        return patch
    merged = dict(target) if isinstance(target, dict) else {}
    for key, value in patch.items():
        if value is None:
            merged.pop(key, None)
        else:
            merged[key] = merge_patch(merged.get(key), value)
    return merged


def _settings(p: Principal) -> tuple[str, Settings]:
    """(the tenant's auth id, its settings) from the engine's own columns."""
    with core_engine.connect() as conn:
        r = conn.execute(_SETTINGS, {"org": p.org_id}).one()
    voice = r.voice_settings or {}
    recording = {"inbound": bool(r.call_recording_inbound), "outbound": bool(r.call_recording_outbound)}
    doc = {
        "recording": recording | {"allow_pause": r.allow_recording_pause_play is not False},
        "wrap_up": r.wrap_up_configuration or {},
        "consent": voice.get("consent", {}),
        "rona": {"status_id": r.rona_status_id, "strikes": (r.rona or {}).get("occurrences", 2)},
        "capacity": r.capacity or {},
        **{k: voice[k] for k in ("agent_direct_dial", "monitor") if k in voice},
    }
    if ring := (r.reservation_timeout or {}).get(rc.CHANNEL_CALL):
        doc["ring_timeout_sec"] = ring
    return r.auth_id, Settings.model_validate(doc)


@router.get("/settings", response_model=Settings)
def get_settings(p: Principal = ADMIN):
    return _settings(p)[1]


@router.patch("/settings", response_model=Settings)
def patch_settings(body: dict, p: Principal = ADMIN):
    """A JSON merge patch (RFC 7396) over the settings body; the whole result is validated before anything is saved."""
    auth_id, current = _settings(p)
    try:
        new = Settings.model_validate(merge_patch(current.model_dump(), body))
    except ValidationError as e:
        raise RequestValidationError(e.errors(include_context=False)) from None
    rona_status = _row(STATUSES, new.rona.status_id, p, "rona.status_id")
    d, org = new.model_dump(), {"org": p.org_id}
    voice = {"consent": new.consent.model_dump(mode="json", exclude_none=True)} | {
        k: d[k] for k in ("agent_direct_dial", "monitor")
    }
    routing = {"wrap_up": d["wrap_up"], "capacity": d["capacity"], "ring": {rc.CHANNEL_CALL: d["ring_timeout_sec"]}}
    with core_engine.begin() as conn:
        conn.execute(_SAVE_RECORDING, d["recording"] | org)
        conn.execute(_SAVE_ROUTING, {k: json.dumps(v) for k, v in routing.items()} | org)
        conn.execute(_SAVE_VOICE, {"voice": json.dumps(voice)} | org)
        conn.execute(
            _SAVE_RONA, {"status": rona_status, "meta": json.dumps({"occurrences": d["rona"]["strikes"]})} | org
        )
    Cache.get_instance().client.delete(rc.reservation_timeout_key(auth_id))
    return new


# --- the agent's desk -----------------------------------------------------------------------------------------------


class DeskSettings(BaseModel):
    wrap_up: WrapUp
    agent_direct_dial: bool


class Me(Agent):
    settings: DeskSettings


class Ping(Strict):
    sdk_heartbeat_ts: str | None = None


@router.get("/me", response_model=Me)
def me(p: Principal = AGENT):
    return _agent(p, p.agent_id) | {"settings": _settings(p)[1].model_dump()}


@router.put("/me/status", response_model=AgentStatus)
def set_my_status(body: StatusSet, p: Principal = AGENT):
    return _change(p, p.agent_id, body, "manual")


@router.post("/me/heartbeat", status_code=204)
def heartbeat(background: BackgroundTasks, _: Ping | None = None, p: Principal = AGENT) -> Response:
    """Presence: the second ping of a session applies the sign-in status; 40 s without one sets Offline."""
    if handle_agent_ping(HeartbeatReqMeta(aom_uuid=p.agent_id, org_uuid=p.tenant_id), background):
        raise ApiError(503, "heartbeat_failed", "the heartbeat could not be recorded; try again")
    return Response(status_code=204)
