"""Agent tasks and callbacks: work an agent does that is not a live contact.

A task is a conversation of its own (channel `task` or `callback`) plus one `task` row. It goes to a queue, routed
like text; to a team, offered to its free members at once but its maker; or to an agent, assigned at once within its
capacity, or past it when the agent made it. Its offers wait 71 hours. It routes now or at a future `scheduled_at`.
Each routing step writes its `routing_status`: pending or scheduled, then accepted, declined when nobody took it, or
failed when nobody could take it or its queue gave up, until it routes again. Its `status` is the work status, todo
until an agent sets another, until resolved or cancelled end it. Its assignee is read from the conversation's
primary assignment, and its activity is the conversation's notes plus one `signal` entry per change. A taken task
left idle 15 days resolves. A callback is a task the agent works by calling the customer with `POST /calls`.
Priority is metadata: it does not change queue order. A target takes the shapes of a text transfer's, and a slot is
counted by routing's own capacity rule."""

from datetime import UTC, datetime
from itertools import islice
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from fastapi.encoders import jsonable_encoder
from pydantic import AwareDatetime, BaseModel, Field, model_validator
from sqlalchemy import func, insert, select, text, update
from sqlalchemy.engine import Connection, Row

from hs import access, config, conversations, events, hot, jobs, routing
from hs.access import Principal
from hs.config.models import E164, Body, File, TeamTarget, Zone
from hs.config.models import TextTarget as Target
from hs.conversations.api import NOTERS, ConversationNote, cursor
from hs.core import PUBLIC, ApiError, Page, Paging, T, page, paging, read, tx

router = APIRouter()
WRITE = access.require("key:calls", "agent")
READ = access.require("key:calls", "agent", "sup")
Kind = Literal["task", "callback", "internal", "follow_up", "others"]
Reach = Literal["call", "chat", "sms", "whatsapp", "email"]
Priority = Literal["low", "medium", "high", "critical"]
Status = Literal["todo", "in_progress", "waiting_on_customer", "resolved", "cancelled"]
ENDS = ("resolved", "cancelled")
Routing = Literal["pending", "scheduled", "accepted", "declined", "failed"]
TRACKED = (
    "status",
    "type",
    "title",
    "description",
    "priority",
    "due_at",
    "scheduled_at",
    "schedule",
    "attachments",
    "target",
)
IDLE_SEC = 15 * 24 * 3600
AT_CAPACITY = ApiError(409, "agent_at_capacity", "the agent has no free slot for a task")

# A title search reads every task of the tenant, an ILIKE scan; add a pg_trgm index on title once a tenant's tasks
# outgrow that.
_TASKS = text(
    "SELECT * FROM (SELECT t.id, t.conversation_id, t.type, t.title, t.description, t.priority,"
    " t.destination AS target, t.due_at, t.scheduled_at, t.schedule, t.attachments, t.origin_id, t.customer,"
    " t.channel, t.routing_status, t.created_by, t.updated_by, t.created_at, t.updated_at, c.number,"
    " CASE WHEN x.state = 'active' OR c.state IN ('wrap_up', 'closed') THEN x.agent_id END AS agent_id,"
    " CASE WHEN t.status IN ('resolved', 'cancelled') THEN t.status WHEN c.result = 'session_timeout' THEN 'resolved'"
    " WHEN c.state IN ('wrap_up', 'closed') THEN 'cancelled' ELSE t.status END AS status FROM task t"
    " JOIN conversation c ON c.id = t.conversation_id"
    " LEFT JOIN LATERAL (SELECT a.agent_id, a.state FROM assignment a WHERE a.conversation_id = c.id"
    "  AND a.role = 'primary' AND a.started_at IS NOT NULL ORDER BY a.started_at DESC LIMIT 1) x ON true"
    " LEFT JOIN LATERAL (SELECT array_agg(CAST(o.payload->>'agent_id' AS uuid)) AS offered FROM jobs o"
    "  WHERE o.kind = 'offer_timeout' AND o.payload->>'conversation_id' = CAST(c.id AS text)) o ON true"
    " WHERE t.tenant_id = :t AND (CAST(:id AS uuid) IS NULL OR t.id = :id)"
    " AND (CAST(:title AS text) IS NULL OR t.title ILIKE '%' || :title || '%')"
    " AND (CAST(:type AS text[]) IS NULL OR t.type = ANY(CAST(:type AS text[])))"
    " AND (CAST(:priority AS text[]) IS NULL OR t.priority = ANY(CAST(:priority AS text[])))"
    " AND (CAST(:cid AS uuid) IS NULL OR t.conversation_id = :cid OR t.origin_id = :cid)"
    " AND (CAST(:seen AS uuid[]) IS NULL OR t.created_by = ANY(CAST(:seen AS uuid[]))"
    "  OR o.offered && CAST(:seen AS uuid[]) OR t.destination->>'agent_id' = ANY(CAST(:seen AS text[]))"
    "  OR t.destination->>'team_id' = ANY(CAST(:teams AS text[])) OR EXISTS (SELECT 1 FROM assignment v"
    "  WHERE v.conversation_id = ANY(coalesce(t.origin_id, c.id) || ARRAY(SELECT s.conversation_id FROM task s"
    "  WHERE s.origin_id = coalesce(t.origin_id, c.id))) AND v.started_at IS NOT NULL"
    "  AND v.agent_id = ANY(CAST(:seen AS uuid[]))))) s"
    " WHERE (CAST(:status AS text[]) IS NULL OR s.status = ANY(CAST(:status AS text[])))"
    " AND (CAST(:routing AS text[]) IS NULL OR s.routing_status = ANY(CAST(:routing AS text[])))"
    " AND (CAST(:after AS bigint) IS NULL OR s.number < :after)"
    " AND (CAST(:agent AS uuid[]) IS NULL OR s.agent_id = ANY(CAST(:agent AS uuid[])))"
    " ORDER BY s.number DESC LIMIT :n"
)
_LOCK = text(
    "SELECT p.agent_id FROM presence p JOIN config a ON a.id = p.agent_id WHERE a.tenant_id = :t"
    " AND a.kind = 'agent' AND (a.id = ANY(CAST(:ids AS uuid[]))"
    " OR a.body->'teams' ? :team AND a.id IS DISTINCT FROM CAST(:maker AS uuid))"
    " ORDER BY p.agent_id FOR SHARE OF p"
)
# Read after the lock; each offer takes its slot. An agent full only with this task's slot on the channel counts as
# free: a reroute releases it.
_FREE = text(
    "SELECT p.agent_id FROM presence p JOIN config s ON s.id = p.status_id WHERE p.agent_id = ANY(CAST(:ids AS uuid[]))"
    " AND (:direct OR (s.body->>'availability')::boolean) AND (:channel || ':' || p.agent_id <> ALL(CAST(:full AS text[]))"
    " OR EXISTS (SELECT 1 FROM assignment b WHERE b.agent_id = p.agent_id AND b.conversation_id = :c"
    " AND b.channel = :channel AND b.state IN ('offered', 'active', 'wrap_up')) OR EXISTS (SELECT 1 FROM jobs j"
    " WHERE j.kind = 'offer_timeout' AND j.payload->>'agent_id' = p.agent_id::text"
    " AND j.payload->>'conversation_id' = CAST(:c AS text) AND j.payload->>'channel' = :channel)) ORDER BY p.agent_id"
)


class Schedule(Body):
    """A local time with its UTC offset, which fixes the instant `scheduled_at` takes, kept with the IANA zone it was
    picked in and the schedule's type, both as the client names them."""

    local_time: AwareDatetime
    timezone: Zone
    type: str


class When(Body):
    scheduled_at: AwareDatetime | None = None
    schedule: Schedule | None = None

    @model_validator(mode="after")
    def _schedule(self):
        if self.schedule:
            if self.scheduled_at:
                raise ValueError("give a schedule or a scheduled_at, not both")
            self.scheduled_at = self.schedule.local_time
        if self.scheduled_at and self.scheduled_at <= datetime.now(UTC):
            raise ValueError("the schedule is in the past")
        return self


class TaskIn(When):
    type: Kind
    title: str
    description: str | None = None
    priority: Priority = "medium"
    target: Target
    due_at: AwareDatetime | None = None
    customer: E164 | None = None
    attachments: list[File] = []
    conversation_id: UUID | None = None
    channel: Reach | None = None

    @model_validator(mode="after")
    def _kind(self):
        if self.type == "callback" and not (self.customer or self.conversation_id):
            raise ValueError("a callback needs the customer's number or the conversation it is raised from")
        if self.type == "internal" and (self.scheduled_at or self.channel):
            raise ValueError("an internal task takes no schedule and no channel")
        if self.type == "follow_up" and not self.channel:
            raise ValueError("a follow-up needs the channel it follows up on")
        return self


class TaskPatch(When):
    """An omitted field stays as it is. `status` resolved or cancelled ends the task; todo, in_progress and
    waiting_on_customer only mark it, in any order, until it ends."""

    type: Kind = None
    title: str = None
    description: str | None = None
    priority: Priority = None
    target: Target = None
    due_at: AwareDatetime | None = None
    attachments: list[File] = None
    channel: Reach | None = None
    status: Status = None
    reroute: bool = Field(False, description="Route it again to its target, changed or not.")


class Task(BaseModel):
    id: UUID
    conversation_id: UUID
    type: Kind
    title: str
    description: str | None
    status: Status
    routing_status: Routing | None
    priority: Priority
    target: Target
    agent_id: UUID | None
    due_at: datetime | None
    scheduled_at: datetime | None
    schedule: Schedule | None
    attachments: list[dict]
    origin_id: UUID | None
    customer: str | None
    channel: Reach | None
    created_by: UUID | None
    updated_by: UUID | None = Field(description="The agent who last made or changed it, told how routing ends.")
    created_at: datetime
    updated_at: datetime


class Note(ConversationNote):
    type: Literal["note"]


class Change(BaseModel):
    type: Literal["change"]
    field: str
    from_: object = Field(alias="from")
    to: object
    agent_id: UUID | None
    created_at: datetime


class TaskDetail(Task):
    activity: list[Annotated[Note | Change, Field(discriminator="type")]]


ALL = {"seen": None, "teams": []}


def _scope(db: Connection, p: Principal) -> dict:
    """The tasks `p` may see and change: all for a key or the org level; else those aimed at it, offered to it or made
    by it, and those whose events' conversation, or a task raised from it, it worked, so whoever hears of a task reads
    it; at the team level also its teammates' and those aimed at its teams."""
    if (seen := access.seen(db, p)).agents is None:
        return ALL
    return {"seen": sorted(seen.agents), "teams": sorted(seen.teams)}


def _tasks(db: Connection, tenant, *, id=None, scope=ALL, n=1, **filters) -> list[dict]:
    args = dict.fromkeys(("status", "routing", "agent", "after", "title", "type", "priority", "cid")) | filters
    return [dict(r._mapping) for r in db.execute(_TASKS, args | {"t": tenant, "id": id, "n": n} | scope)]


def _one(db: Connection, tenant, task_id, scope=ALL) -> dict:
    if not (found := _tasks(db, tenant, id=task_id, scope=scope)):
        raise ApiError(404, "task_not_found", "no such task")
    return found[0]


def _readable(db: Connection, p: Principal, cid: UUID) -> bool:
    task = db.execute(select(T.task.c.id).where(T.task.c.conversation_id == cid)).scalar()
    return task is not None and bool(_tasks(db, p.tenant_id, id=task, scope=_scope(db, p)))


NOTERS["task"] = NOTERS["callback"] = _readable


def _vet(db: Connection, p: Principal, target: BaseModel) -> None:
    """422 foreign_id for a queue, team or agent of another tenant."""
    config.check(db, p, target)
    if isinstance(target, TeamTarget):
        config.owned(db, p, "team", [target.team_id])


def _enqueue(db: Connection, p: Principal, raw: dict) -> Row:
    """The enqueue route takes no task: a task names its own target and starts with POST /tasks."""
    raise ApiError(422, "channel_not_supported", "a task or callback starts with POST /tasks")


def _task(db: Connection, conv: Row) -> Row:
    return db.execute(select(T.task).where(T.task.c.conversation_id == conv.id)).one()


def _ended(db: Connection, conv: Row, a: Row) -> None:
    if a.state == "active":
        _accepted(db, conv, _task(db, conv), a)
    elif conv.state == "open" and not conversations.rows(db, conv.id, conversations.ON):
        _step(db, conv, _task(db, conv), "declined")


def _unserved(db: Connection, conv: Row) -> None:
    _release(db, conv, end=False)
    conversations.set_state(db, conv, "open")
    _step(db, conv, _task(db, conv), "failed")


routing.CHANNELS["task"] = routing.CHANNELS["callback"] = routing.Channel(
    _enqueue, lambda db, conv, o: None, _ended, _unserved
)


def _mine(t: Row) -> bool:
    return t.destination["type"] == "agent" and t.destination["agent_id"] == str(t.created_by)


def _takers(db: Connection, conv: Row, t: Row) -> list[UUID]:
    """Their presence rows are share-locked, so a status change waits for the offer."""
    target, direct = t.destination, t.destination["type"] == "agent"
    who = {"t": conv.tenant_id, "ids": [target["agent_id"]] if direct else [], "team": target.get("team_id")}
    who |= {"maker": t.created_by, "direct": direct, "channel": conv.channel, "c": conv.id}
    ids = db.execute(_LOCK, who).scalars().all()
    if _mine(t):
        return ids
    return db.execute(_FREE, who | {"ids": ids, "full": hot.full(conv.tenant_id, conv.channel)}).scalars().all()


def _route(db: Connection, conv: Row, t: Row, takers: list[UUID] | None = None) -> list[Row] | None:
    t, target = _step(db, conv, t, "pending"), t.destination
    if target["type"] == "queue":
        routing.enqueue(db, conv, UUID(target["queue_id"]), agents=target["agents"])
        return None
    takers = _takers(db, conv, t) if takers is None else takers
    if target["type"] == "agent":
        if made := _assign_unless_taken_meanwhile(db, conv, t, takers):
            _accepted(db, conv, t, made[0])
    else:
        n = config.tenant_settings(db, str(conv.tenant_id)).simultaneous_agent_limit
        offers = (conversations.offer(db, conv, a) for a in takers)
        made = list(islice(filter(None, offers), n))
    if not made:
        _step(db, conv, t, "failed")
    return made


def _assign_unless_taken_meanwhile(db: Connection, conv: Row, t: Row, takers: list[UUID]) -> list[Row]:
    try:
        return [conversations.offer(db, conv, a, via="self" if _mine(t) else "direct") for a in takers]
    except ApiError as e:
        if e.code != "agent_at_capacity":
            raise
        return []


def _accepted(db: Connection, conv: Row, t: Row, a: Row) -> None:
    stints = reversed(conversations.rows(db, conv.id))
    taken_from = next((x.agent_id for x in stints if x.role == "primary" and x.started_at and x.id != a.id), None)
    _step(db, conv, t, "accepted", a.agent_id, taken_from)
    _idle(db, conv)


def _step(db: Connection, conv: Row, t: Row, to: str, by=None, told=None) -> Row:
    K = T.task
    now = db.execute(update(K).where(K.c.id == t.id).values(routing_status=to).returning(*K.c)).one()
    if to not in ("pending", "scheduled"):
        _record(db, conv, now, {"routing_status": [t.routing_status, to]}, by, told or t.updated_by)
    return now


def _idle(db: Connection, conv: Row) -> None:
    jobs.schedule(db, "session_timeout", {"conversation_id": str(conv.id)}, delay=IDLE_SEC, key=str(conv.id))


def _later(t: Row) -> datetime | None:
    return t.scheduled_at if t.scheduled_at and t.scheduled_at > datetime.now(UTC) else None


def _channel(kind: str) -> str:
    return "callback" if kind == "callback" else "task"


def _straight(t: Row) -> bool:
    return t.destination["type"] == "agent" and _later(t) is None


def _arm(db: Connection, conv: Row, t: Row, at: datetime | None) -> None:
    """Route the task in a transaction of its own, at `at` or at once."""
    due = {"task_id": str(t.id), "conversation_id": str(conv.id)}
    jobs.schedule(db, "task_route", due, at=at, key=str(t.id))
    _step(db, conv, t, "scheduled" if at else "pending")


def _release(db: Connection, conv: Row, *, end: bool) -> None:
    """Withdraw the task's open offers and, with `end`, take it from its agent without a wrap-up."""
    jobs.cancel(db, "session_timeout", str(conv.id))
    for a in conversations.by_agent(conversations.rows(db, conv.id, conversations.ON)):
        if a.state == "offered":
            conversations.move(db, a, "withdraw", reason="cancelled")
        elif end:
            conversations.move(db, a, "end", reason="reassigned")


def _reroute(db: Connection, conv: Row, t: Row) -> bool:
    """An agent target due now is read free and locked before the release frees its slot and schedules job rows.
    Returns whether the task_route run for any other target is due now."""
    at, straight = _later(t), _straight(t)
    if straight and not (takers := _takers(db, conv, t)):
        raise AT_CAPACITY
    _release(db, conv, end=True)
    conversations.set_state(db, conv, "open")
    if straight:
        jobs.cancel(db, "task_route", str(t.id))
        if not _route(db, conv, t, takers):
            raise AT_CAPACITY
        return False
    _arm(db, conv, t, at)
    return at is None


def _retype(db: Connection, conv: Row, channel: str) -> Row:
    """The conversation routes on its task type's channel; a slot its agent holds stays on the channel it was taken on."""
    if conv.channel == channel:
        return conv
    C = T.conversation
    return db.execute(update(C).where(C.c.id == conv.id).values(channel=channel).returning(*C.c)).one()


def _record(db: Connection, conv: Row, t: Row, changed: dict, by, agent=None) -> None:
    """Each change is a `signal` entry on the task's conversation; the event goes to the conversation the task was
    raised from if it has one, so that conversation's agent and timeline hear it."""
    for field, (old, new) in changed.items():
        conversations.entry(db, conv, "signal", {"field": field, "from": old, "to": new}, agent_id=by)
    data = {"task_id": str(t.id), "changes": changed, "by_agent_id": by and str(by)}
    events.emit(db, "task.updated", conv.tenant_id, data, conversation_id=t.origin_id or conv.id, agent_id=agent)


@router.post("/tasks", status_code=201, response_model=Task)
def create_task(body: TaskIn, p: Principal = WRITE):
    """Starts `todo`. An agent target takes it at once, routing `accepted`, or its routing reads `failed` when the
    agent has no free slot and did not make it. A team task routes once the task is made, so its routing reads
    `pending` here whatever its routing then finds."""
    with tx() as db:
        config.hold(db, p.tenant_id)
        _vet(db, p, body.target)
        if body.type == "follow_up" and p.agent_id and not body.scheduled_at:
            raise ApiError(422, "schedule_required", "an agent's follow-up needs a schedule")
        C, origin = T.conversation, None
        if body.conversation_id:
            mine = (C.c.id == body.conversation_id, C.c.tenant_id == p.tenant_id)
            if (origin := db.execute(select(C.c.state, C.c.customer, C.c.direction).where(*mine)).first()) is None:
                raise ApiError(422, "foreign_id", "the conversation is not of this tenant", {"kind": "conversation"})
            if origin.state == "closed":
                raise ApiError(422, "conversation_closed", "a task is raised from a conversation until it closes")
        customer = body.customer or (origin and origin.customer) or None
        how = {"direction": origin.direction if origin else "outbound", "customer": customer or ""}
        conv = conversations.open(db, p.tenant_id, channel=_channel(body.type), ours="", **how)
        row = body.model_dump(exclude={"target", "attachments", "conversation_id"}) | jsonable_encoder(
            {
                "destination": body.target,
                "status": "todo",
                "schedule": body.schedule,
                "attachments": _files(body.attachments),
                "customer": customer,
                "origin_id": body.conversation_id,
            }
        )
        ids = {"tenant_id": p.tenant_id, "conversation_id": conv.id, "created_by": p.agent_id, "updated_by": p.agent_id}
        t = db.execute(insert(T.task).values(**row, **ids).returning(*T.task.c)).one()
        data = {"task_id": str(t.id)} | body.model_dump(include={"type", "title", "priority", "due_at", "scheduled_at"})
        agent = getattr(body.target, "agent_id", None)
        events.emit(db, "task.created", p.tenant_id, data, conversation_id=t.origin_id or conv.id, agent_id=agent)
        conversations.entry(db, conv, "signal", {"field": "created", "from": None, "to": None}, agent_id=p.agent_id)
        access.created(db, str(t.id))
        if (at := _later(t)) or body.target.type == "team":
            _arm(db, conv, t, at)
        else:
            _route(db, conv, t)
        return _one(db, p.tenant_id, t.id)


def _files(files: list[File] | None) -> list[dict] | None:
    return files and [f.model_dump(exclude_none=True) for f in files]


def _recovered(db: Connection, p: Principal, task_id: str) -> dict:
    """The answer to a create retried after a crash past its commit, read as GET /tasks/{id} reads it."""
    return Task.model_validate(_one(db, p.tenant_id, task_id, _scope(db, p))).model_dump(mode="json")


access.VIEWS[f"POST {PUBLIC}/tasks"] = _recovered


@router.get("/tasks", response_model=Page[Task])
def list_tasks(
    status: list[Status] | None = Query(None),
    routing_status: list[Routing] | None = Query(None),
    agent_id: list[UUID] | None = Query(None),
    title: str | None = None,
    type: list[Kind] | None = Query(None),
    priority: list[Priority] | None = Query(None),
    conversation_id: UUID | None = None,
    p: Principal = READ,
    paged: Paging = Depends(paging),
):
    """Newest first; a filter given several times takes any of its values. An agent lists the tasks assigned to it,
    aimed at it or made by it; a supervisor also its teammates' and its teams'."""
    with read() as db:
        scope = _scope(db, p)
        found = {"title": title, "type": type, "priority": priority, "cid": conversation_id, "routing": routing_status}
        rows = _tasks(
            db, p.tenant_id, scope=scope, status=status, agent=agent_id, after=cursor(paged), n=paged.limit + 1, **found
        )
    return page(rows, paged, key="number")


@router.get("/tasks/{task_id}", response_model=TaskDetail)
def get_task(task_id: UUID, p: Principal = READ):
    """The task with its activity in the order it was written: each note on its conversation and each change."""
    E = T.entry
    with read() as db:
        t = _one(db, p.tenant_id, task_id, _scope(db, p))
        kept = (E.c.conversation_id == t["conversation_id"], E.c.kind.in_(("note", "signal")), E.c.deleted_at.is_(None))
        written = func.coalesce(E.c.written_at, E.c.at)  # one transaction's entries share its now()
        entries = db.execute(select(E).where(*kept).order_by(written, E.c.id)).all()
    activity = [
        conversations.note(e) | {"type": "note"}
        if e.kind == "note"
        else {"id": e.id, "agent_id": e.agent_id, "created_at": e.at, "type": "change", **e.body}
        for e in entries
    ]
    return t | {"activity": activity}


@router.patch("/tasks/{task_id}", response_model=Task)
def patch_task(task_id: UUID, body: TaskPatch, p: Principal = WRITE):
    """A change of status, priority, due date, schedule or target is activity and `task.updated`. Resolving or
    cancelling ends the task's conversation. A new target or schedule, or `reroute`, takes it from its agent and
    routes it again, a change of its routing status. 409 task_closed once it ended, and task_routing for a new target
    or type or a reroute while an offer or a queue holds it, since a reassignment waits until its routing ends. 409
    agent_at_capacity when the agent it goes to now has no free slot: it stays where it was."""
    K, due_now = T.task, False
    with tx() as db:
        config.hold(db, p.tenant_id)
        cid = db.execute(select(K.c.conversation_id).where(K.c.id == task_id, K.c.tenant_id == p.tenant_id)).scalar()
        if cid is None:
            raise ApiError(404, "task_not_found", "no such task")
        conv = conversations.lock(db, cid)
        before = _one(db, p.tenant_id, task_id, _scope(db, p))
        if before["status"] in ENDS:
            raise ApiError(409, "task_closed", "the task was resolved or cancelled")
        fields = body.model_dump(exclude_unset=True, exclude={"schedule", "attachments", "reroute"})
        if "schedule" in body.model_fields_set or "scheduled_at" in fields:
            fields["schedule"] = jsonable_encoder(body.schedule)
        if body.attachments is not None:
            fields["attachments"] = _files(body.attachments)
        if "target" in fields:
            _vet(db, p, body.target)
            fields["target"] = jsonable_encoder(body.target)
        if (any(fields.get(f, before[f]) != before[f] for f in ("type", "target")) or body.reroute) and (
            conv.state == "queued" or conversations.rows(db, conv.id, ("offered",))
        ):
            raise ApiError(409, "task_routing", "the task is being routed: change its target or type once that ends")
        changed = {f: jsonable_encoder([before[f], v]) for f, v in fields.items() if f in TRACKED and before[f] != v}
        moved = (body.reroute or "target" in changed) and fields.get("type", before["type"]) != "follow_up"
        again = body.status not in ENDS and ("scheduled_at" in changed or moved)
        row = {"destination" if f == "target" else f: v for f, v in fields.items()}
        stamp = {"updated_at": func.now(), "updated_by": p.agent_id}
        t = db.execute(update(K).where(K.c.id == task_id).values(**row, **stamp).returning(*K.c)).one()
        if again and (step := "scheduled" if _later(t) else "pending") != before["routing_status"]:
            changed["routing_status"] = [before["routing_status"], step]
        conv = _retype(db, conv, _channel(t.type))
        if changed:
            _record(db, conv, t, changed, p.agent_id, before["agent_id"])
        if body.status in ENDS:
            _release(db, conv, end=False)
            conversations.gone(db, conv, result=body.status)
        elif again:
            due_now = _reroute(db, conv, t)
    if due_now:
        jobs.run_now("task_route", str(task_id), wait=True)
    with read() as db:
        return _one(db, p.tenant_id, task_id, _scope(db, p))


@jobs.handler("task_route")
def _due(job: Row) -> None:
    """Route the task, at its scheduled time or after a new target. The run deletes its own row in the transaction
    that routes, so a rerun after a crash finds nothing to do. A run whose row a PATCH rewrote since is stale and
    does nothing. A run claimed before its time, by a PATCH's `run_now` after a later PATCH postponed it, arms
    its row again: the new rev keeps the runner from deleting it. A queue that refuses the task, disabled, full or
    deleted by then, closes it as enqueue_failed."""
    J = T.jobs
    with tx() as db:
        conv = conversations.lock(db, UUID(job.payload["conversation_id"]))
        early = (J.c.run_at > func.now()).label("early")
        row = db.execute(select(J.c.rev, early).where(J.c.id == job.id)).first()
        if not (conv and row and row.rev == job.rev and conv.state == "open"):
            return
        if row.early:
            jobs.schedule(db, "task_route", job.payload, at=job.run_at, key=job.key)
        else:
            t = db.execute(select(T.task).where(T.task.c.id == job.payload["task_id"])).one()
            try:
                _route(db, conv, t)
            except ApiError as e:
                if e.code == "queue_not_found":
                    routing.overflowed(db, conv, t.destination["queue_id"], "enqueue_failed", None)
                elif not isinstance(e, routing.Full):
                    raise
                conversations.gone(db, conv)
            jobs.cancel(db, "task_route", job.key)
