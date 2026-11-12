"""The fixed reports and the live floor: SQL over conversations, their assignments and messages, the status history and
the live assignment and presence rows. A pass keeps each closed conversation's numbers on its row, so a report over a
long window reads one row per conversation."""

import json
import time as clock
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, datetime, time, timedelta
from typing import Literal, NamedTuple
from uuid import UUID
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.engine import Connection, Row

from hs import access, config, conversations, jobs
from hs.config.models import STATUS_MAP_DEFAULTS, Named
from hs.conversations.api import Channel
from hs.core import ApiError, Page, Paging, page, paging, read, tx

router = APIRouter()
REPORTS = access.require("key:reports", "sup")

# The agents a status report keeps: `agent`, `team`'s, and those whose name holds `name`.
WHO = (
    "(CAST(:agent AS uuid) IS NULL OR h.agent_id = CAST(:agent AS uuid))"
    " AND (CAST(:team AS text) IS NULL AND CAST(:name AS text) IS NULL OR h.agent_id IN (SELECT id FROM config"
    " WHERE tenant_id = :tenant AND kind = 'agent' AND (CAST(:team AS text) IS NULL OR body->'teams' ? :team)"
    " AND (CAST(:name AS text) IS NULL OR strpos(lower(body->>'name'), lower(:name)) > 0)))"
)
CHANGES = "h.tenant_id = :tenant AND h.at >= :start AND h.at < :end"
# The instant the bucket holding `at` starts, so the hour a fall-back repeats is two buckets. A week starts on Sunday.
START = (
    "CASE CAST(:bucket AS text) WHEN 'week' THEN (date_trunc('week', {at} AT TIME ZONE :tz + interval '1 day')"
    " - interval '1 day') AT TIME ZONE :tz ELSE date_trunc(CAST(:bucket AS text), {at}, :tz) END"
)
CHANGED = START.format(at="h.at")
OPENED = START.format(at="f.opened_at")
SAMPLED = START.format(at="s.at")
# Every bucket of the window once, sampled each half hour as no zone's change makes a shorter one. Grouping by it lets
# the planner see how few groups there are, so it hashes the join and the groups instead of sorting every row.
SPANS = (
    f" LEFT JOIN (SELECT {SAMPLED} AS bucket FROM generate_series(:start, :end,"  # noqa: S608 (constants only)
    f" interval '30 minutes') s(at) GROUP BY 1) b ON b.bucket = {OPENED}"
)
# The changes inside the window and each agent's last one before it, each counted for its part inside the window. The
# changes inside are summed on their own: a sum over a union keeps the planner from splitting it across workers.
SECONDS = "extract(epoch FROM least(coalesce(until, now()), :end) - greatest(at, :start))"
ACTIVITY = text(
    f"SELECT CAST(agent_id AS text), status, available, CAST(round(sum(seconds)) AS bigint) AS seconds FROM (SELECT"  # noqa: S608 (constants only)
    f" agent_id, status, available, sum({SECONDS}) AS seconds FROM status_history h WHERE {CHANGES} GROUP BY 1, 2, 3"
    f" UNION ALL SELECT l.agent_id, l.status, l.available, {SECONDS} FROM config g CROSS JOIN LATERAL (SELECT *"
    " FROM status_history h WHERE h.tenant_id = :tenant AND h.agent_id = g.id AND h.at < :start"
    " ORDER BY h.at DESC, h.id DESC LIMIT 1) l WHERE g.tenant_id = :tenant AND g.kind = 'agent'"
    f" AND coalesce(until, now()) > :start) h WHERE {WHO} GROUP BY agent_id, status, available ORDER BY 1, 2"
)
# The agents who changed to an available status in each bucket, not those who stayed available through it.
AVAILABLE_AGENTS = text(
    f"SELECT {CHANGED} AS bucket, count(DISTINCT agent_id) AS agents FROM status_history h"  # noqa: S608 (constants only)
    f" WHERE {CHANGES} AND h.available AND {WHO} GROUP BY 1 ORDER BY 1"
)
# Newest first, below the cursor's (at, id).
STATUS_LOG = text(
    "SELECT h.id, CAST(h.agent_id AS text) AS agent_id, g.body->>'name' AS agent_name, g.body->>'email' AS agent_email,"  # noqa: S608 (constants only)
    " CAST(h.status_id AS text) AS status_id, h.status, h.available,"
    " CAST(round(extract(epoch FROM least(coalesce(h.until, now()), :end) - h.at)) AS bigint) AS duration_sec,"
    f" h.at AS created_at FROM status_history h LEFT JOIN config g ON g.id = h.agent_id WHERE {CHANGES} AND {WHO}"
    " AND (CAST(:at AS timestamptz) IS NULL OR (h.at, h.id) < (:at, CAST(:after AS bigint)))"
    " ORDER BY h.at DESC, h.id DESC LIMIT :limit"
)
FAILED = "('failed_invalid', 'failed_carrier')"
# The results that make a conversation abandoned, and those whose wait counts as unanswered.
ABANDONED = "('enqueue_failed', 'after_hours', 'assignment_failed')"
WAITED = (
    "('queue_timeout', 'assignment_failed', 'queue_exit', 'abandoned', 'rejected', 'after_hours', 'enqueue_failed')"
)
WORKED = (
    " JOIN tenant tn ON tn.id = c.tenant_id CROSS JOIN LATERAL (SELECT min(started_at) AS took,"
    " max(ended_at) FILTER (WHERE wrap_up_until IS NOT NULL) AS wrapped,"
    " sum(extract(epoch FROM coalesce(submitted_at, least(wrap_up_until, c.closed_at)) - ended_at))"
    " FILTER (WHERE wrap_up_until IS NOT NULL) AS wrap,"
    " count(DISTINCT transfer_id) FILTER (WHERE via = 'transfer' AND role = 'primary') AS transfers,"
    " (array_agg(disposition_id ORDER BY offered_at DESC, id DESC) FILTER (WHERE disposition_id IS NOT NULL))[1] AS did"
    " FROM assignment WHERE conversation_id = c.id AND agent_id IS NOT NULL AND started_at IS NOT NULL"
    " AND role IN ('primary', 'secondary')) a CROSS JOIN LATERAL (SELECT min(at) FILTER (WHERE agent_id IS NOT NULL)"
    " AS replied, min(at) FILTER (WHERE body->>'direction' <> c.direction) AS reply,"
    " bool_or(status IN ('sent', 'delivered', 'read')) AS sent,"
    " (array_agg(agent_id IS NOT NULL ORDER BY at DESC, id DESC))[1] AS agent_last FROM entry"
    " WHERE conversation_id = c.id AND c.channel <> 'call' AND kind = 'message' AND status IS DISTINCT FROM 'failed') m"
)
# Its numbers, as search reckons talk, wrap and the first response: handle is talk (none for an outbound call or text
# the customer never got) plus wrap, and none when no agent took it; the first agent message; the transfers an agent
# took over; the last wrap-up's disposition, else the result's system one as conversations map it (the stored map,
# then the default system disposition by name); the offers declined or missed.
NUMBERS = (
    "CAST(CASE WHEN a.took IS NOT NULL THEN CASE WHEN c.direction = 'outbound' AND (c.channel = 'call'"
    " AND c.answered_at IS NULL OR c.channel IN ('sms', 'whatsapp') AND m.sent IS NOT TRUE) THEN 0"
    " ELSE coalesce(extract(epoch FROM a.wrapped - a.took), 0) END + coalesce(a.wrap, 0) END AS int) AS handle_sec,"
    " CAST(a.wrap AS int) AS wrap_sec, CAST(extract(epoch FROM least(m.reply, CASE WHEN c.channel = 'call'"
    " AND (c.direction = 'inbound' OR c.answered_at IS NOT NULL) THEN a.took END) - c.opened_at) AS int) AS response_sec,"
    " CAST(extract(epoch FROM m.replied - c.opened_at) AS int) AS first_reply_sec, CAST(a.transfers AS int) AS transfers,"
    " CASE WHEN c.state = 'closed' THEN coalesce(a.did, CAST(coalesce(tn.settings->'status_map'->c.direction->>c.result,"
    " tn.settings->'status_map'->'inbound'->>c.result) AS uuid), (SELECT d.id FROM config d WHERE d.tenant_id = c.tenant_id"
    " AND d.kind = 'disposition' AND CAST(d.body->>'system' AS boolean) AND d.body->>'name' = CAST(:defaults AS jsonb)"
    "->c.direction->>c.result ORDER BY CAST(d.body->>'sequence' AS int) LIMIT 1)) END AS disposition_id,"
    " CAST((SELECT count(*) FROM events WHERE conversation_id = CAST(c.id AS text)"
    " AND type IN ('offer.missed', 'offer.rejected')) AS int) AS missed_offers"
)
DEFAULTS = json.dumps({d: STATUS_MAP_DEFAULTS["inbound"] | names for d, names in STATUS_MAP_DEFAULTS.items()})
# A closed conversation whose `transfers` is null has no numbers stored yet.
FACTS = "handle_sec, wrap_sec, response_sec, first_reply_sec, transfers, disposition_id, missed_offers"
KEEP = f"(SELECT {NUMBERS} FROM conversation c{WORKED} WHERE c.id = u.id)"  # noqa: S608 (constants only)
FILL = text(
    f"UPDATE conversation u SET ({FACTS}) = {KEEP} WHERE u.id IN (SELECT id FROM conversation"  # noqa: S608 (constants only)
    " WHERE transfers IS NULL AND state = 'closed' LIMIT :batch)"
).bindparams(defaults=DEFAULTS)
# A refresh works out wrap and handle alone: retention may have deleted the events another kept fact counts.
REWRAP = "handle_sec, wrap_sec"
REFRESH = text(
    f"UPDATE conversation u SET ({REWRAP}) = (SELECT {REWRAP} FROM {KEEP} n) WHERE u.id IN (SELECT id"  # noqa: S608 (constants only)
    " FROM conversation WHERE transfers IS NOT NULL AND id > CAST(:after AS uuid) ORDER BY id LIMIT :batch)"
    " RETURNING CAST(u.id AS text)"
).bindparams(defaults=DEFAULTS)
BATCH, BUDGET_S = 500, 20
# Each conversation opened in the window that the filters and the caller's scope keep, with its numbers: off its row,
# else (it is open, or closed since the last pass) worked out here. A live text the agent wrote last is `waiting`.
_IN = (
    "c.tenant_id = :tenant AND c.opened_at >= :start AND c.opened_at < :end"  # noqa: S608
    " AND (CAST(:channels AS text[]) IS NULL OR c.channel = ANY(CAST(:channels AS text[])))"
    " AND (CAST(:directions AS text[]) IS NULL OR c.direction = ANY(CAST(:directions AS text[])))"
    " AND (CAST(:csats AS text[]) IS NULL OR c.csat = ANY(CAST(:csats AS text[])))"
    " AND (:all OR EXISTS (SELECT FROM assignment s WHERE s.conversation_id = c.id"
    " AND s.agent_id = ANY(CAST(:seen AS uuid[])) AND s.started_at IS NOT NULL) OR "
    + access.ROUTED
    + " AND NOT EXISTS (SELECT FROM assignment s WHERE s.conversation_id = c.id AND s.agent_id IS NOT NULL"
    " AND s.started_at IS NOT NULL))"
)
_ROW = "c.id, c.number, c.customer, c.channel, c.direction, c.result, c.opened_at, c.answered_at, c.closed_at, c.csat"
_ROW += ", c.hold_count, c.hold_sec, c.ended_by, c.queue_id"
# Only a closed conversation has its numbers stored, so a state filter without closed reads none of those rows.
STORED = (
    f"SELECT {_ROW}, c.state, {FACTS} FROM conversation c WHERE {_IN} AND c.transfers IS NOT NULL"  # noqa: S608 (constants only)
    " AND (CAST(:states AS text[]) IS NULL OR 'closed' = ANY(CAST(:states AS text[])))"
)
WORKING = (
    f"SELECT {_ROW}, CASE WHEN c.state = 'live' AND m.agent_last THEN 'waiting' ELSE c.state END AS state, {NUMBERS}"  # noqa: S608 (constants only)
    f" FROM conversation c{WORKED} WHERE {_IN} AND c.transfers IS NULL"
)
STATES = " WHERE (CAST(:states AS text[]) IS NULL OR f.state = ANY(CAST(:states AS text[])))"
PAGE = (
    " AND (CAST(:at AS timestamptz) IS NULL OR (f.opened_at, f.id) < (:at, CAST(:after AS uuid)))"
    " ORDER BY f.opened_at DESC, f.id DESC LIMIT :limit"
)
KEYS = (
    "b.bucket, CASE WHEN 'channel' = ANY(CAST(:by AS text[])) THEN f.channel END AS channel,"
    " CASE WHEN 'direction' = ANY(CAST(:by AS text[])) THEN f.direction END AS direction,"
    " CASE WHEN 'state' = ANY(CAST(:by AS text[])) THEN f.state END AS state,"
    " CAST(CASE WHEN 'disposition' = ANY(CAST(:by AS text[])) THEN f.disposition_id END AS text) AS disposition_id,"
    " CASE WHEN 'handle_time' = ANY(CAST(:by AS text[])) THEN CASE WHEN coalesce(f.handle_sec, 0) = 0 THEN 'none'"
    " WHEN f.handle_sec < 60 THEN 'under_1m' WHEN f.handle_sec < 300 THEN '1m_to_5m' ELSE 'over_5m' END END"
    " AS handle_time"
)
COUNTS = {
    "total": "count(*)",
    "replied": "count(f.first_reply_sec)",
    "closed": "count(*) FILTER (WHERE f.state = 'closed')",
    "live": "count(*) FILTER (WHERE f.state = 'live')",
    "waiting": "count(*) FILTER (WHERE f.state = 'waiting')",
    "unassigned": "count(*) FILTER (WHERE f.handle_sec IS NULL)",
    "answered": "count(f.answered_at)",
    "unanswered": f"count(*) FILTER (WHERE f.state = 'closed' AND f.answered_at IS NULL AND f.result NOT IN {FAILED})",
    "failed": f"count(*) FILTER (WHERE f.state = 'closed' AND f.answered_at IS NULL AND f.result IN {FAILED})",
    "abandoned": f"count(*) FILTER (WHERE f.result IN {ABANDONED})",
    "short": "count(*) FILTER (WHERE f.handle_sec < 300)",
    "transferred": "count(*) FILTER (WHERE f.transfers > 0)",
    "transfers": "coalesce(sum(f.transfers), 0)",
    "callbacks": "coalesce(sum(k.n), 0)",
}
MEANS = {
    "avg_csat": ("CASE WHEN f.csat ~ '^[0-9]{1,9}$' THEN CAST(f.csat AS int) END", 2),
    "avg_handle_sec": ("f.handle_sec", 1),
    "avg_wrap_sec": ("f.wrap_sec", 1),
    "avg_first_reply_sec": ("f.first_reply_sec", 1),
    "avg_response_sec": ("f.response_sec", 1),
    "avg_unanswered_wait_sec": (
        f"CASE WHEN f.direction = 'inbound' AND f.result IN {WAITED} THEN extract(epoch FROM f.closed_at - f.opened_at) END",
        1,
    ),
}
PARTS = ", ".join(
    [f"{v} AS {k}" for k, v in COUNTS.items()]
    + [f"sum({expr}) AS {k}_sum, count({expr}) AS {k}_n" for k, (expr, _) in MEANS.items()]
)
SUMS = ", ".join(
    [f"CAST(sum({k}) AS bigint) AS {k}" for k in COUNTS]
    + [f"round(sum({k}_sum) / nullif(sum({k}_n), 0), {places}) AS {k}" for k, (_, places) in MEANS.items()]
)
CALLBACKS = (
    " LEFT JOIN (SELECT origin_id, count(*) AS n FROM task WHERE tenant_id = :tenant AND type = 'callback' GROUP BY 1) k"
    " ON k.origin_id = f.id"
)
# Each branch aggregates before the UNION ALL: a lateral branch makes the union parallel-unsafe, and grouping first
# lets the stored branch run a parallel partial aggregate. The parts then add up.
CONVERSATIONS = text(
    "SELECT g.*, d.body->>'name' AS disposition FROM (SELECT bucket, channel, direction, state, disposition_id,"  # noqa: S608 (constants only)
    f" handle_time, {SUMS} FROM (SELECT {KEYS}, {PARTS} FROM ({STORED}) f{CALLBACKS}{SPANS}{STATES} GROUP BY 1, 2, 3, 4, 5, 6"
    f" UNION ALL SELECT {KEYS}, {PARTS} FROM ({WORKING}) f{CALLBACKS}{SPANS}{STATES} GROUP BY 1, 2, 3, 4, 5, 6) p"
    " GROUP BY 1, 2, 3, 4, 5, 6)"
    " g LEFT JOIN config d ON d.id = CAST(g.disposition_id AS uuid) ORDER BY 1, 2, 3, 4, 5, 6"
).bindparams(defaults=DEFAULTS)
# Without a bucket the window is one group: joining an empty bucket list would cost a step for every row.
TOTALS = text(
    CONVERSATIONS.text.replace(SPANS, "").replace("b.bucket,", "CAST(NULL AS timestamptz) AS bucket,")
).bindparams(defaults=DEFAULTS)
# Newest first, below the cursor's (opened_at, id); the agent is the one who last took it as primary.
ROWS = text(
    f"WITH f AS ((SELECT * FROM ({STORED}) f{STATES}{PAGE}) UNION ALL {WORKING})"  # noqa: S608 (constants only)
    " SELECT CAST(f.id AS text) AS id, f.number AS conversation_number, f.customer, f.channel, f.direction,"
    " f.state, f.result, CASE WHEN f.answered_at IS NOT NULL THEN 'answered' WHEN f.state <> 'closed' THEN NULL"
    f" WHEN f.result IN {FAILED} THEN 'failed' ELSE 'unanswered' END AS resolution,"
    " CAST(f.disposition_id AS text) AS disposition_id, d.body->>'name' AS disposition, x.agent_id, x.agent_name,"
    " f.csat, f.hold_count, f.hold_sec, f.ended_by, f.missed_offers, f.handle_sec, f.wrap_sec, f.response_sec,"
    " CAST(f.queue_id AS text) AS queue_id, q.body->>'name' AS queue_name,"
    f" CASE WHEN f.direction = 'inbound' AND f.result IN {WAITED} THEN CAST(extract(epoch FROM f.closed_at - f.opened_at)"
    f" AS int) END AS unanswered_wait_sec, f.opened_at, f.closed_at FROM (SELECT * FROM f{STATES}{PAGE}) f"
    " LEFT JOIN config d ON d.id = f.disposition_id LEFT JOIN config q ON q.id = f.queue_id"
    " LEFT JOIN LATERAL (SELECT CAST(a.agent_id AS text) AS agent_id, g.body->>'name' AS agent_name FROM assignment a"
    " LEFT JOIN config g ON g.id = a.agent_id WHERE a.conversation_id = f.id AND a.role = 'primary'"
    " AND a.agent_id IS NOT NULL AND a.started_at IS NOT NULL ORDER BY a.started_at DESC, a.id DESC LIMIT 1) x ON TRUE"
    " ORDER BY f.opened_at DESC, f.id DESC"
).bindparams(defaults=DEFAULTS)
QUEUES = text(
    "SELECT c.queue_id::text, q.body->>'name' AS name, count(*) AS waiting,"
    " CAST(extract(epoch FROM now() - min(c.queued_at)) AS bigint) AS oldest_wait_sec FROM conversation c"
    " JOIN config q ON q.id = c.queue_id WHERE c.tenant_id = :tenant AND c.state = 'queued'"
    " AND (CAST(:queues AS uuid[]) IS NULL OR c.queue_id = ANY(CAST(:queues AS uuid[]))) GROUP BY 1, 2 ORDER BY 1"
)
# Each agent's status and its calls: live ones before ringing ones before wrap-ups, the latest first. A ringing call
# read from its offer's timer counts only while Redis holds the offer.
FLOOR = text(
    "SELECT g.id::text AS agent_id, p.status_id::text, st.body->>'name' AS status_name, x.conversation_id, x.state,"
    " x.holder, x.timer FROM config g LEFT JOIN presence p ON p.agent_id = g.id"
    " LEFT JOIN config st ON st.id = p.status_id LEFT JOIN LATERAL (SELECT a.conversation_id::text AS conversation_id,"
    " a.state, a.offered_at, a.id::text AS holder, false AS timer FROM assignment a WHERE a.agent_id = g.id"
    " AND a.channel = 'call' AND a.state IN ('active', 'offered', 'wrap_up') UNION ALL SELECT"
    " j.payload->>'conversation_id', 'offered', j.run_at, j.key, true FROM jobs j WHERE j.kind = 'offer_timeout'"
    " AND j.payload->>'agent_id' = g.id::text AND j.payload->>'channel' = 'call') x ON TRUE"
    " WHERE g.tenant_id = :tenant AND g.kind = 'agent'"
    " AND (CAST(:visible AS text[]) IS NULL OR g.id::text = ANY(CAST(:visible AS text[])))"
    " ORDER BY g.created_at, g.id, array_position(ARRAY['active', 'offered', 'wrap_up'], x.state), x.offered_at DESC"
)
LAST_EVENT = text("SELECT coalesce(max(seq), 0) FROM events WHERE tenant_id = :tenant")
ACTIVITIES = {"active": "on_call", "offered": "ringing", "wrap_up": "wrap_up"}


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


class ActivityRow(BaseModel):
    agent_id: str
    statuses: dict[str, int]
    available_sec: int
    away_sec: int


class AvailableAgentsRow(BaseModel):
    bucket: datetime
    agents: int


class StatusLogRow(BaseModel):
    agent_id: str
    agent_name: str | None
    agent_email: str | None
    status_id: str
    status: str
    available: bool | None
    duration_sec: int
    created_at: datetime


class Report[T](BaseModel):
    data: list[T]


State = Literal["open", "queued", "live", "waiting", "wrap_up", "closed"]


class ConversationsRow(BaseModel):
    """One bucket and `by` value pair; a key it is not grouped by is null."""

    bucket: datetime | None
    channel: str | None
    direction: str | None
    state: State | None
    disposition_id: str | None
    disposition: str | None
    handle_time: Literal["none", "under_1m", "1m_to_5m", "over_5m"] | None
    total: int
    replied: int
    closed: int
    live: int
    waiting: int
    unassigned: int
    answered: int
    unanswered: int
    failed: int
    abandoned: int
    short: int
    transferred: int
    transfers: int
    callbacks: int
    avg_csat: float | None
    avg_handle_sec: float | None
    avg_wrap_sec: float | None
    avg_first_reply_sec: float | None
    avg_response_sec: float | None
    avg_unanswered_wait_sec: float | None


class ConversationRow(BaseModel):
    id: str
    conversation_number: int
    customer: str
    channel: str
    direction: str
    state: State
    result: str | None
    resolution: Literal["answered", "unanswered", "failed"] | None
    disposition_id: str | None
    disposition: str | None
    agent_id: str | None
    agent_name: str | None
    csat: str | None
    hold_count: int
    hold_sec: int
    ended_by: str | None
    missed_offers: int
    queue_id: str | None
    queue_name: str | None
    handle_sec: int | None
    wrap_sec: int | None
    response_sec: int | None
    unanswered_wait_sec: int | None
    opened_at: datetime
    closed_at: datetime | None


class QueueStats(BaseModel):
    queue_id: str
    name: str
    waiting: int
    oldest_wait_sec: int


class FloorAgent(BaseModel):
    agent_id: str
    status: Named | None
    activity: Literal["idle", "ringing", "on_call", "wrap_up"]
    conversation_id: str | None


class Floor(BaseModel):
    agents: list[FloorAgent]
    queues: list[QueueStats]
    last_event_id: str


def _visible(db, p: access.Principal) -> list[str] | None:
    seen = access.visible_agents(db, p)
    return None if seen is None else sorted(seen)


def _queues(db, p: access.Principal) -> list[str] | None:
    return None if access.visible_agents(db, p) is None else access.visible_queues(db, p)


@contextmanager
def _reading() -> Iterator[Connection]:
    """A report's read: one transaction with JIT off, as compiling a report's dozens of expressions costs more than
    it saves."""
    with tx() as db:
        db.execute(text("SET LOCAL jit = off"))
        yield db


class Agents(NamedTuple):
    team: str | None
    agent: str | None
    name: str | None


def agents(team_id: UUID | None = None, agent_id: UUID | None = None, agent_name: str | None = None) -> Agents:
    """The status reports' filters: one team's agents, one agent, and the agents whose name holds `agent_name`."""
    return Agents(team_id and str(team_id), agent_id and str(agent_id), agent_name)


def _statuses(p: access.Principal, w: Window, who: Agents) -> dict:
    return {"tenant": p.tenant_id, "start": w.start, "end": w.end, "tz": w.tz} | who._asdict()


@router.get("/reports/agent-activity", response_model=Report[ActivityRow])
def agent_activity_report(p: access.Principal = REPORTS, w: Window = Depends(window), who: Agents = Depends(agents)):
    """Each agent's seconds in each status, and in available and in away statuses, inside the window."""
    rows: dict[str, dict] = {}
    with _reading() as db:
        for agent, status, available, seconds in db.execute(ACTIVITY, _statuses(p, w, who)):
            row = rows.setdefault(agent, {"agent_id": agent, "statuses": {}, "available_sec": 0, "away_sec": 0})
            row["statuses"][status] = row["statuses"].get(status, 0) + seconds
            if available is not None:
                row["available_sec" if available else "away_sec"] += seconds
    return {"data": list(rows.values())}


@router.get("/reports/available-agents", response_model=Report[AvailableAgentsRow])
def available_agents_report(
    p: access.Principal = REPORTS,
    w: Window = Depends(window),
    who: Agents = Depends(agents),
    bucket: Literal["hour", "day", "week", "month"] = "day",
):
    """Per bucket of the window in `tz`, the agents who changed to an available status in it."""
    with _reading() as db:
        rows = db.execute(AVAILABLE_AGENTS, _statuses(p, w, who) | {"bucket": bucket}).all()
    return {"data": [r._asdict() for r in rows]}


@router.get("/reports/agent-status-log", response_model=Page[StatusLogRow])
def agent_status_log(
    p: access.Principal = REPORTS,
    w: Window = Depends(window),
    who: Agents = Depends(agents),
    paged: Paging = Depends(paging),
):
    """Each status change in the window, newest first, with how long the status lasted inside it."""
    cursor = _cursor(paged, int)
    with _reading() as db:
        rows = db.execute(STATUS_LOG, _statuses(p, w, who) | cursor | {"limit": paged.limit + 1}).all()
    return page([r._asdict() | {"cursor": f"{r.created_at.isoformat()} {r.id}"} for r in rows], paged, key="cursor")


def _cursor(paged: Paging, kind) -> dict:
    """A list's (time, id) cursor as `at` and `after`, the id read by `kind`."""
    at, _, after = (paged.after or "").partition(" ")
    try:
        return {"at": datetime.fromisoformat(at) if at else None, "after": str(kind(after)) if at else None}
    except ValueError as e:
        raise ApiError(422, "invalid_cursor", "cursor is not one this API returned") from e


def kept(
    channel: list[Channel] = Query([]),
    direction: list[Literal["inbound", "outbound"]] = Query([]),
    state: list[State] = Query([]),
    csat: list[str] = Query([]),
) -> dict:
    """The conversation reports' filters: a repeated one keeps any of its values."""
    return {
        "channels": channel or None,
        "directions": direction or None,
        "states": state or None,
        "csats": csat or None,
    }


@router.get("/reports/conversations", response_model=Report[ConversationsRow])
def conversations_report(
    p: access.Principal = REPORTS,
    w: Window = Depends(window),
    filters: dict = Depends(kept),
    bucket: Literal["hour", "day", "week", "month"] | None = None,
    by: list[Literal["channel", "direction", "state", "disposition", "handle_time"]] = Query([], max_length=2),
):
    """The numbers of the conversations opened in the window, per `bucket` of their open time in `tz` (a week starts
    on Sunday) and per value of each `by` key. A supervisor reads the whole tenant's numbers."""
    params = {"tenant": p.tenant_id, "start": w.start, "end": w.end, "tz": w.tz, "bucket": bucket, "by": by}
    with _reading() as db:
        sql = CONVERSATIONS if bucket else TOTALS
        rows = db.execute(sql, params | filters | {"all": True, "seen": [], "teams": [], "queues": []}).all()
    return {"data": [r._asdict() for r in rows]}


@router.get("/reports/conversations/rows", response_model=Page[ConversationRow])
def conversation_rows(
    p: access.Principal = REPORTS,
    w: Window = Depends(window),
    filters: dict = Depends(kept),
    paged: Paging = Depends(paging),
):
    """Each conversation opened in the window, newest first, with its numbers: the conversations the caller's role
    sees, as its conversation list does."""
    cursor = _cursor(paged, UUID)
    params = {"tenant": p.tenant_id, "start": w.start, "end": w.end, "limit": paged.limit + 1}
    with _reading() as db:
        s = access.seen(db, p)
        scope = {"all": s.agents is None, "seen": sorted(s.agents or ()), "teams": sorted(s.teams), "queues": s.queues}
        rows = db.execute(ROWS, params | filters | scope | cursor).all()
    return page([r._asdict() | {"cursor": f"{r.opened_at.isoformat()} {r.id}"} for r in rows], paged, key="cursor")


jobs.every("report_facts", 60)


@jobs.handler("report_facts", lane="slow")
def _store(job: Row) -> None:
    """Keep closed conversations' numbers on their rows, BATCH at a time for at most BUDGET_S seconds a run; a large
    upgrade's backlog drains over several runs."""
    until = clock.monotonic() + BUDGET_S
    while clock.monotonic() < until:
        with tx() as db:
            if db.execute(FILL, {"batch": BATCH}).rowcount < BATCH:
                return


@jobs.handler("report_refresh", lane="slow", parks=False)
def _refresh(job: Row) -> None:
    with tx() as db:
        done = db.execute(REFRESH, {"after": job.payload["after"], "batch": BATCH}).scalars().all()
        if len(done) == BATCH:
            jobs.schedule(db, "report_refresh", {"after": max(done)}, key="report_refresh", rev=job.rev)


@router.get("/queues/{queue_id}/stats", response_model=QueueStats)
def queue_stats(queue_id: UUID, p: access.Principal = REPORTS):
    with read() as db:
        seen = _queues(db, p)
        q = config.get(db, p.tenant_id, "queue", queue_id)
        if q is None or (seen is not None and str(queue_id) not in seen):
            raise ApiError(404, "queue_not_found", "no such queue")
        row = db.execute(QUEUES, {"tenant": p.tenant_id, "queues": [str(queue_id)]}).first()
    return row._asdict() if row else {"queue_id": str(queue_id), "name": q["name"], "waiting": 0, "oldest_wait_sec": 0}


@router.get("/floor", response_model=Floor)
def floor(p: access.Principal = REPORTS):
    with read() as db:
        # Read first: a change landing during the snapshot is then replayed after it, never skipped.
        last = db.execute(LAST_EVENT, {"tenant": p.tenant_id}).scalar()
        found = db.execute(FLOOR, {"tenant": p.tenant_id, "visible": _visible(db, p)}).all()
        ringing = [a.conversation_id for a in found if a.timer]
        offered = {h.holder for h in conversations.held(db, ringing) if h.timer} if ringing else set()
        queues = db.execute(QUEUES, {"tenant": p.tenant_id, "queues": _queues(db, p)}).all()
    agents: dict[str, dict] = {}
    for a in found:
        status = {"id": a.status_id, "name": a.status_name} if a.status_id else None
        idle = {"agent_id": a.agent_id, "status": status, "activity": "idle", "conversation_id": None}
        row = agents.setdefault(a.agent_id, idle)
        if row["activity"] == "idle" and a.state and (not a.timer or a.holder in offered):
            row |= {
                "activity": ACTIVITIES[a.state],
                "conversation_id": None if a.state == "offered" else a.conversation_id,
            }
    return {"agents": list(agents.values()), "queues": [q._asdict() for q in queues], "last_event_id": str(last)}
