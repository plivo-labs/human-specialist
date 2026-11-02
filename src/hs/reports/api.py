"""The fixed reports and the live floor: SQL over closed_conversation_search, events and the engine's own
tables."""

from datetime import date, datetime, time, timedelta
from typing import Literal, NamedTuple
from uuid import UUID
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy import text

from hs import events
from hs.access import Principal, require
from hs.conversations.api import visible_agents
from hs.errors import ApiError
from hs.infra.db import core_engine
from hs.infra.redis import Cache
from hs.routing import constants as rc
from hs.setup import api as setup

router = APIRouter()
REPORTS = Depends(require("key:reports", "sup"))
WAITING = [rc.CONTACT_STATE_IN_TRANSIT, rc.CONTACT_STATE_QUEUED, rc.CONTACT_STATE_IN_REVERSE_ROUTING]

# One row per conversation and group: each handling agent (or team) for group_by agent (team), else one row.
# Under an agent (team) group the agent (team) filters apply to that group, not to the whole call.
SOURCE = (
    " FROM closed_conversation_search s CROSS JOIN LATERAL jsonb_array_elements_text(CASE CAST(:group_by AS text)"
    " WHEN 'agent' THEN s.agent_ids WHEN 'team' THEN s.team_ids ELSE '[null]' END) g(key)"
    " CROSS JOIN LATERAL (SELECT CASE WHEN :group_by = 'agent' THEN jsonb_build_array(g.key) ELSE s.agent_ids END"
    " AS agents, CASE WHEN :group_by = 'team' THEN jsonb_build_array(g.key) ELSE s.team_ids END AS teams) w"
)
FILTERS = (
    " WHERE s.organization = :org AND s.conv_created_at >= :start AND s.conv_created_at < :end"
    " AND (CAST(:team AS text) IS NULL OR w.teams ? :team) AND (CAST(:agent AS text) IS NULL OR w.agents ? :agent)"
    " AND (CAST(:visible AS text[]) IS NULL OR w.agents ?| CAST(:visible AS text[]))"
)
BUCKET = (
    "CASE WHEN :group_by IN ('hour', 'day') THEN date_trunc(:group_by, s.conv_created_at AT TIME ZONE :tz)"
    " AT TIME ZONE :tz END"
)
CALLS = text(
    f"SELECT g.key, {BUCKET} AS bucket, count(*) AS dials, count(*) FILTER (WHERE s.is_conv_answered) AS connects,"
    " CAST(coalesce(sum(s.agent_customer_conv_duration), 0) AS bigint) AS talk_sec,"
    " CAST(coalesce(sum(s.conv_wrapup_duration), 0) AS bigint) AS wrap_sec,"
    " coalesce(round((sum(s.agent_customer_conv_duration) + sum(s.conv_wrapup_duration))"
    " / nullif(count(*) FILTER (WHERE s.is_conv_answered), 0), 1), 0) AS avg_handle_sec"
    f"{SOURCE}{FILTERS} AND (CAST(:inbound AS boolean) IS NULL OR s.direction_inbound = :inbound)"
    " GROUP BY 1, 2 ORDER BY 1, 2"
)
# The writer files a disposition marked unanswered under conv_state, so the call's final disposition is either.
# Limit: skipped = a system disposition on a call an agent handled; read the wrap-up's own flag if the
# search row ever stores it.
DISPOSITIONS = text(
    "SELECT g.key, d.uuid::text AS disposition_id, coalesce(d.name, s.disposition) AS disposition,"  # noqa: S608 (constants only)
    " sd.uuid::text AS sub_disposition_id, sd.name AS sub_disposition, coalesce(d.system, false) AS system,"
    " count(*) AS count, count(*) FILTER (WHERE d.system AND s.agent_ids <> '[]') AS skipped"
    f"{SOURCE} LEFT JOIN (SELECT uuid, name, internal AS system FROM dispositions) d"
    " ON d.uuid = CAST(coalesce(s.disposition_uuid, s.conv_state_uuid) AS uuid)"
    f" LEFT JOIN dispositions sd ON sd.uuid = CAST(s.sub_disposition_uuid AS uuid){FILTERS}"
    " GROUP BY 1, 2, 3, 4, 5, 6 ORDER BY 1, 7 DESC, 3"
)

# Each status lasts until the agent's next change; the latest one runs until now. Clipped to the window.
ACTIVITY = text(
    "WITH change AS (SELECT agent_id, data->'to_status'->>'name' AS status, created_at AS since,"
    " lead(created_at) OVER (PARTITION BY agent_id ORDER BY id) AS until FROM events"
    " WHERE tenant_id = :tenant AND type = 'agent.status_changed' AND created_at < :end"
    " AND (CAST(:agent AS text) IS NULL OR agent_id = :agent)"
    " AND (CAST(:visible AS text[]) IS NULL OR agent_id = ANY(CAST(:visible AS text[])))"
    " AND (CAST(:team AS text) IS NULL OR agent_id IN (SELECT a.uuid::text FROM aom_team at"
    " JOIN team t ON t.id = at.team JOIN account_org_mapping a ON a.id = at.aom"
    " WHERE t.uuid = CAST(:team AS uuid) AND t.organization = :org AND at.is_deleted IS NOT TRUE)))"
    " SELECT agent_id, status, CAST(round(sum(extract(epoch FROM least(coalesce(until, now()), :end)"
    " - greatest(since, :start)))) AS bigint) AS seconds FROM change WHERE coalesce(until, now()) > :start"
    " GROUP BY 1, 2 ORDER BY 1, 2"
)
QUEUES = text(
    "SELECT queue_uuid::text AS queue_id, count(*) AS waiting,"
    " CAST(extract(epoch FROM now() - min(created_at)) AS bigint) AS oldest_wait_sec FROM active_queue_entry"
    " WHERE auth_id = (SELECT auth_id FROM organization WHERE id = :org) AND status = ANY(:waiting)"
    " AND deleted_at IS NULL AND (CAST(:queue AS uuid) IS NULL OR queue_uuid = CAST(:queue AS uuid))"
    " GROUP BY 1 ORDER BY 1"
)
# Each agent's current call: a live one before a wrap-up, the latest first.
FLOOR = text(
    "SELECT a.uuid::text AS agent_id, a.sip_endpoint, os.uuid::text AS status_id, os.status AS status_name,"
    " cur.conversation_id, cur.conversation_number, cur.call_status, o.auth_id FROM account_org_mapping a"
    " JOIN organization o ON o.id = a.organization LEFT JOIN org_statuses os ON os.id = a.agent_status"
    " LEFT JOIN LATERAL (SELECT cp.uuid::text AS conversation_id, cp.conversation_number, c.status AS call_status"
    " FROM conversation c JOIN conversation_parent cp ON cp.id = c.parent WHERE c.primary_aom = a.id"
    " AND c.channel = 'call' AND c.is_deleted IS NOT TRUE AND c.status IN ('open', 'on_hold', 'waiting', 'in_wrap_up')"
    " ORDER BY c.status = 'in_wrap_up', c.id DESC LIMIT 1) cur ON TRUE"
    " WHERE a.organization = :org AND a.is_deleted IS NOT TRUE"
    " AND (CAST(:visible AS text[]) IS NULL OR a.uuid::text = ANY(CAST(:visible AS text[]))) ORDER BY a.id"
)


class Window(NamedTuple):
    start: datetime
    end: datetime
    tz: str


def window(from_: date = Query(alias="from"), to: date = Query(), tz: str = "UTC") -> Window:
    """`from` through `to`, whole days in the IANA zone `tz`."""
    try:
        zone = ZoneInfo(tz)
    except (ZoneInfoNotFoundError, ValueError) as e:
        raise ApiError(422, "invalid_tz", "tz is not an IANA time zone") from e
    return Window(datetime.combine(from_, time(), zone), datetime.combine(to + timedelta(days=1), time(), zone), tz)


class Group(BaseModel):
    agent_id: str | None
    team_id: str | None


class CallsRow(Group):
    bucket: datetime | None
    dials: int
    connects: int
    connect_rate: float
    talk_sec: int
    avg_handle_sec: float
    wrap_sec: int


class DispositionsRow(Group):
    disposition_id: str | None
    disposition: str | None
    sub_disposition_id: str | None
    sub_disposition: str | None
    system: bool
    count: int
    skipped: int


class ActivityRow(BaseModel):
    agent_id: str
    statuses: dict[str, int]


class Report[T](BaseModel):
    data: list[T]


class QueueStats(BaseModel):
    queue_id: str
    waiting: int
    oldest_wait_sec: int


class FloorStatus(BaseModel):
    id: str
    name: str


class FloorAgent(BaseModel):
    agent_id: str
    status: FloorStatus | None
    activity: Literal["idle", "ringing", "on_call", "wrap_up"]
    conversation_id: str | None


class Floor(BaseModel):
    agents: list[FloorAgent]
    queues: list[QueueStats]
    last_event_id: str


def _report(sql, principal: Principal, w: Window, group_by, team_id, agent_id, **params) -> list[dict]:
    params |= {"org": principal.org_id, "start": w.start, "end": w.end, "tz": w.tz, "group_by": group_by}
    params |= {"team": team_id and str(team_id), "agent": agent_id and str(agent_id)}
    with core_engine.connect() as conn:
        rows = conn.execute(sql, params | {"visible": visible_agents(principal)}).all()
    return [
        {"agent_id": r.key if group_by == "agent" else None, "team_id": r.key if group_by == "team" else None}
        | r._asdict()
        for r in rows
    ]


@router.get("/reports/calls", response_model=Report[CallsRow])
def calls_report(
    principal: Principal = REPORTS,
    w: Window = Depends(window),
    group_by: Literal["agent", "team", "hour", "day"] | None = None,
    direction: Literal["inbound", "outbound"] | None = None,
    team_id: UUID | None = None,
    agent_id: UUID | None = None,
):
    inbound = None if direction is None else direction == "inbound"
    rows = _report(CALLS, principal, w, group_by, team_id, agent_id, inbound=inbound)
    return {"data": [r | {"connect_rate": r["connects"] / r["dials"]} for r in rows]}


@router.get("/reports/dispositions", response_model=Report[DispositionsRow])
def dispositions_report(
    principal: Principal = REPORTS,
    w: Window = Depends(window),
    group_by: Literal["agent", "team"] | None = None,
    team_id: UUID | None = None,
    agent_id: UUID | None = None,
):
    return {"data": _report(DISPOSITIONS, principal, w, group_by, team_id, agent_id)}


@router.get("/reports/agent-activity", response_model=Report[ActivityRow])
def agent_activity_report(
    principal: Principal = REPORTS,
    w: Window = Depends(window),
    team_id: UUID | None = None,
    agent_id: UUID | None = None,
):
    params = {"tenant": principal.tenant_id, "org": principal.org_id, "start": w.start, "end": w.end}
    params |= {"team": team_id and str(team_id), "agent": agent_id and str(agent_id)}
    statuses: dict[str, dict[str, int]] = {}
    with core_engine.connect() as conn:
        for agent, status, seconds in conn.execute(ACTIVITY, params | {"visible": visible_agents(principal)}):
            statuses.setdefault(agent, {})[status] = seconds
    return {"data": [{"agent_id": agent, "statuses": s} for agent, s in statuses.items()]}


@router.get("/queues/{queue_id}/stats", response_model=QueueStats)
def queue_stats(queue_id: UUID, principal: Principal = REPORTS):
    if setup.get(setup.queues, principal.org_id, queue_id) is None:
        raise ApiError(404, "queue_not_found", "no such queue")
    with core_engine.connect() as conn:
        row = conn.execute(QUEUES, {"org": principal.org_id, "waiting": WAITING, "queue": str(queue_id)}).first()
    return row._asdict() if row else {"queue_id": str(queue_id), "waiting": 0, "oldest_wait_sec": 0}


@router.get("/floor", response_model=Floor)
def floor(principal: Principal = REPORTS):
    # Read first: a change landing during the snapshot is then replayed after it, never skipped.
    last = events.latest(principal.tenant_id)
    with core_engine.connect() as conn:
        agents = conn.execute(FLOOR, {"org": principal.org_id, "visible": visible_agents(principal)}).all()
        queues = conn.execute(QUEUES, {"org": principal.org_id, "waiting": WAITING, "queue": None}).all()
    # Selection reserves call capacity when it offers, under the conversation number: one held for a call the
    # agent is not yet on means it is ringing.
    pipe = Cache.get_instance().client.pipeline(transaction=False)
    for a in agents:
        pipe.smembers(rc.agent_capacity_key(a.auth_id, a.sip_endpoint, rc.CHANNEL_CALL))
    rows = []
    for a, held in zip(agents, pipe.execute(), strict=True):
        live = a.call_status in ("open", "on_hold", "waiting")
        ringing = not live and bool(held - {a.conversation_number})
        activity = "on_call" if live else "ringing" if ringing else "wrap_up" if a.call_status else "idle"
        status = {"id": a.status_id, "name": a.status_name} if a.status_id else None
        current = None if ringing else a.conversation_id
        rows.append({"agent_id": a.agent_id, "status": status, "activity": activity, "conversation_id": current})
    return {"agents": rows, "queues": [q._asdict() for q in queues], "last_event_id": str(last)}
