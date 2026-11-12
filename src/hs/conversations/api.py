"""The public conversation routes: the list, the read model, notes, attributes, each agent's wrap-up and the customer's
CSAT answer."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Response
from pydantic import BaseModel, Field, StringConstraints
from sqlalchemy import Text, and_, case, cast, func, or_, select, tuple_, update
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.engine import Connection, Row
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import ClauseElement

from hs import access, config, events
from hs import conversations as cv
from hs.config import models as m
from hs.core import PUBLIC, ApiError, Page, Paging, T, capped, page, paging, read, tx
from hs.events.catalog import ClosedDisposition

router = APIRouter()
READ = access.require("key:calls", "agent", "sup")
WRITE = access.require("key:calls", "agent")
KEY = access.require("key:calls")
AGENT = access.require("agent")
State = Literal["active", "in_wrap_up", "closed"]
Shown = Literal["active", "transferring", "in_wrap_up", "closed"]
STATES = {"active": cv.LIVE, "in_wrap_up": ("wrap_up",), "closed": ("closed",)}
Channel = Literal["call", "chat", "sms", "whatsapp", "email", "task", "callback"]


class LastMessage(BaseModel):
    id: str
    direction: Literal["inbound", "outbound"]
    text: str | None
    attachments: int
    agent_id: str | None
    status: str
    created_at: datetime
    template: dict | None = None
    whatsapp: dict | None = None


class Assignment(BaseModel):
    id: str = Field(description="Its offer's offer_id; a participant's participant_id.")
    transfer_id: str | None = Field(description="A transfer target's transfer, which hold and switch take.")
    agent_id: str | None
    number: str | None = Field(description="An external party's number.")
    role: str
    via: str
    mode: str | None = Field(description="A supervisor's listen, whisper or barge; a transfer's warm or cold.")
    state: Literal["active", "wrap_up"]
    started_at: datetime
    ended_at: datetime | None = Field(description="When it left for wrap-up.")
    wrap_up_until: datetime | None


class ConversationSummary(BaseModel):
    id: str
    conversation_number: str
    role: str | None = Field(None, description="How the calling agent came to it: primary or secondary.")
    via: str | None = Field(None, description="queue, transfer, participant, or null for a caller who is no agent.")
    direction: Literal["inbound", "outbound"]
    state: Shown
    number: str | None
    agent_ids: list[str]
    queue_id: str | None = None
    queue_name: str | None = Field(None, description="The queue's name, which a deleted queue keeps.")
    created_at: datetime
    ended_at: datetime | None
    channel: Channel
    awaiting: Literal["customer", "agent"] | None = Field(None, description="Whose turn it is in an open text.")
    waiting: bool = Field(False, description="An agent marked the live text waiting, until the next message.")
    our_address: str | None = Field(None, description="The tenant's number or address the conversation is on.")
    last_message: LastMessage | None = None
    last_activity_at: datetime | None = Field(None, description="Its last entry, stint start or end, or state change.")
    unread_count: int = Field(
        0, description="What no agent wrote since an agent last wrote, read the messages or viewed it; 0 once closed."
    )
    assignments: list[Assignment] = Field([], description="Who is on it now: its stints active or in wrap-up.")


class ConversationLeg(BaseModel):
    call_uuid: str
    role: str | None
    started_at: str | None
    ended_at: str | None


class ConversationNote(BaseModel):
    id: str
    text: str
    attachments: list[dict]
    agent_id: str | None
    created_at: datetime
    updated_at: datetime


class Conversation(ConversationSummary):
    legs: list[ConversationLeg]
    notes: list[ConversationNote]
    recording_ids: list[str]
    context: dict | None
    attributes: dict
    dispositions: list[ClosedDisposition]


Strict = m.Body


NoteText = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=10_000)]


class NoteFile(m.File):
    url: Annotated[str, Field(pattern=r"^https://\S+$", max_length=2000)] | None = None


class NoteBody(Strict):
    text: NoteText
    attachments: list[NoteFile] = []


class NotePatch(NoteBody):
    """An omitted field stays as it is."""

    text: NoteText = None


class ConversationPatch(Strict):
    """An omitted field stays as it is."""

    attributes: dict | None = None
    waiting: bool | None = None
    customer: str | None = Field(None, min_length=1, max_length=320, description="The customer's corrected identifier.")


class WrapUpIn(Strict):
    disposition_id: UUID
    sub_disposition_id: UUID | None = None
    note: str | None = Field(None, max_length=10_000)
    agent_id: UUID | None = None


class ExtendIn(Strict):
    seconds: int = Field(ge=1, le=3600)


class CsatIn(Strict):
    score: str | None = Field(None, min_length=1, max_length=100)


def _took(agents, *only) -> object:
    """Some agent of `agents` was on the conversation in any role, as `access.conversation` reads it; `only` narrows
    the assignment."""
    A, C = T.assignment, T.conversation
    took = (A.c.conversation_id == C.c.id, A.c.started_at.isnot(None), A.c.agent_id.in_(agents))
    return select(A.c.id).where(*took, *only).exists()


@dataclass
class Filters:
    """What GET /conversations filters on; the Postgres search shares it."""

    state: State | None = None
    agent_id: UUID | None = None
    number: str | None = None
    call_uuid: str | None = None
    direction: Literal["inbound", "outbound"] | None = None
    created_after: datetime | None = None
    created_before: datetime | None = None
    channel: Channel | None = None
    conversation_number: int | None = None
    participant_agent_id: UUID | None = None
    attribute: Annotated[list[str] | None, Query(description="key:value of a string attribute; repeat for all")] = None
    sort: Literal["last_activity_at"] | None = None
    active_after: datetime | None = None
    active_before: datetime | None = None


def cursor(paged: Paging) -> int | None:
    """The conversation number a page starts below."""
    if paged.after is not None and not paged.after.isdigit():
        raise ApiError(422, "invalid_cursor", "cursor is not one this API returned")
    return None if paged.after is None else int(paged.after)


def _require_number(f: Filters) -> None:
    if not f.number:
        raise ApiError(422, "number_required", "this reads one customer's history: give its number")


def _activity_cursor(paged: Paging):
    at, _, number = paged.after.rpartition("|")
    try:
        return tuple_(datetime.fromisoformat(at), int(number))
    except ValueError as e:
        raise ApiError(422, "invalid_cursor", "cursor is not one this API returned") from e


def where(db, p: access.Principal, f: Filters, paged: Paging) -> list[ClauseElement]:
    """The conversations `p` may see that match `f`, below the cursor."""
    C, A = T.conversation, T.assignment
    by_activity, activity = f.sort == "last_activity_at", cv.inbox.last_activity()
    if by_activity or f.active_after or f.active_before:
        _require_number(f)
    after, s = None if by_activity else cursor(paged), access.seen(db, p)
    bare = [n.strip().lstrip("+") for n in (f.number or "").split(",")]
    leg = select(A.c.id).where(A.c.conversation_id == C.c.id, A.c.call_uuid == f.call_uuid).exists()
    untaken = select(A.c.id).where(A.c.conversation_id == C.c.id, A.c.agent_id.isnot(None), A.c.started_at.isnot(None))
    teams = cast(sorted(s.teams), ARRAY(Text))
    routed = case(
        (C.c.routing.has_key("route"), C.c.routing["route"]["teams"].has_any(teams)), else_=C.c.queue_id.in_(s.queues)
    )
    untaken = and_(routed, ~untaken.exists())
    on = (A.c.conversation_id == C.c.id, A.c.agent_id == f.participant_agent_id, A.c.state.in_(("active", "wrap_up")))
    kept = [a.partition(":") for a in f.attribute or ()]
    if not all(k and sep for k, sep, _ in kept):
        raise ApiError(422, "invalid_attribute", "an attribute filter reads key:value")
    clauses = (
        C.c.tenant_id == p.tenant_id,
        f.state and C.c.state.in_(STATES[f.state]),
        f.agent_id and _took([f.agent_id], A.c.role.in_(("primary", "secondary"))),
        f.number and C.c.customer.in_([*bare, *(f"+{n}" for n in bare)]),
        f.call_uuid and or_(C.c.call_uuid == f.call_uuid, leg),
        f.direction and C.c.direction == f.direction,
        f.created_after and C.c.opened_at > f.created_after,
        f.created_before and C.c.opened_at < f.created_before,
        f.channel and C.c.channel == f.channel,
        f.conversation_number and C.c.number == f.conversation_number,
        f.participant_agent_id and select(A.c.id).where(*on).exists(),
        *(C.c.attributes.contains({k: v}) for k, _, v in kept),
        f.active_after and activity > f.active_after,
        f.active_before and activity < f.active_before,
        None if s.agents is None else or_(_took(list(s.agents)), untaken),
        after is not None and C.c.number < after,
        by_activity and paged.after is not None and tuple_(activity, C.c.number) < _activity_cursor(paged),
    )
    return [c for c in clauses if isinstance(c, ClauseElement)]


@router.get("/conversations", response_model=Page[ConversationSummary])
def list_conversations(p: access.Principal = READ, f: Filters = Depends(), paged: Paging = Depends(paging)):
    """Newest first. An agent sees what it handled, a supervisor also what its teams handled and what no agent took
    on a route to its teams. `number` takes several, comma-separated: with `limit=1` it reads the newest conversation
    with any of them, and its `our_address`. `participant_agent_id` keeps those that agent is on now, in wrap-up too.
    `sort=last_activity_at`, `active_after` and `active_before` read a customer's history by its last activity, so they
    need `number`."""
    C, by_activity = T.conversation, f.sort == "last_activity_at"
    order = (cv.inbox.last_activity().desc(), C.c.number.desc()) if by_activity else (C.c.number.desc(),)
    with read() as db:
        rows = cv.summary(p.agent_id).where(*where(db, p, f, paged)).order_by(*order)
        rows = cv.inbox.counted(db, p.tenant_id, [dict(r._mapping) for r in db.execute(rows.limit(paged.limit + 1))])
    if not by_activity:
        return page(rows, paged, key="conversation_number")
    for r in rows:
        r["cursor"] = f"{r['last_activity_at'].isoformat()}|{r['conversation_number']}"
    return page(rows, paged, key="cursor")


class Counts(BaseModel):
    total: int
    open: int
    closed: int


@router.get("/conversations/counts", response_model=Counts)
def count_conversations(p: access.Principal = READ, f: Filters = Depends()):
    """How many of a customer's conversations match the list's filters (`number` is required), and how many of
    them are open and closed."""
    _require_number(f)
    C = T.conversation
    with read() as db:
        q = select(func.count(), func.count().filter(C.c.state == "closed")).select_from(C)
        total, closed = db.execute(q.where(*where(db, p, f, Paging(1, None)))).one()
    return {"total": total, "open": total - closed, "closed": closed}


@router.get("/conversations/{conversation_id}", response_model=Conversation)
def get_conversation(conversation_id: UUID, p: access.Principal = READ):
    with read() as db:
        return cv.view(db, access.conversation(db, p, conversation_id, offer=True), p.agent_id)


@router.post("/conversations/{conversation_id}/viewed", status_code=204)
def viewed(conversation_id: UUID, p: access.Principal = AGENT) -> Response:
    """The agent read the conversation: unread_count reads 0 for every viewer until a message, note or voicemail no
    agent wrote arrives, as after an agent reads its messages or writes in it."""
    with read() as db:
        conv = access.conversation(db, p, conversation_id)
    cv.inbox.mark(conv, read=True)
    return Response(status_code=204)


@router.patch("/conversations/{conversation_id}", response_model=Conversation)
def patch_conversation(conversation_id: UUID, body: ConversationPatch, p: access.Principal = WRITE):
    """`attributes` merge one level deep: a key replaces the stored one, null removes it. At most 2 KB, in any state.
    `waiting` marks a live text conversation the caller is on (409 not_text, not_live), until the next message;
    false unmarks it, and a `waiting` that reads as it is now is a no-op. A new `customer` corrects who the customer is:
    replies and the customer's next messages follow it (422 invalid_customer for one that is no address of the
    channel; 409 when that customer already has another open conversation on this line)."""
    C = T.conversation
    with tx() as db:
        conv = access.conversation(db, p, conversation_id, lock=True)
        if body.waiting is not None and body.waiting != cv.waiting(conv):
            cv.wait(db, access.controlled(conv), body.waiting)
        values = {}
        if body.attributes is not None:
            merged = {k: v for k, v in (conv.attributes | body.attributes).items() if v is not None}
            values["attributes"] = capped(merged, field="attributes")
        if body.customer:
            try:
                values["customer"] = m.address(conv.channel, body.customer)
            except ValueError as e:
                raise ApiError(422, "invalid_customer", f"customer is {e}") from None
        if values:
            try:
                with db.begin_nested():
                    conv = db.execute(update(C).where(C.c.id == conv.id).values(**values).returning(*C.c)).one()
            except IntegrityError:
                raise ApiError(
                    409, "conversation_open", "this customer already has an open conversation on this line"
                ) from None
            cv.touched(db, conv)
        return cv.view(db, conv, p.agent_id)


NOTERS: dict[str, Callable[[Connection, access.Principal, UUID], bool]] = {}


def _noted(db: Connection, p: access.Principal, conversation_id: UUID) -> Row:
    C = T.conversation
    mine = select(C).where(C.c.id == conversation_id, C.c.tenant_id == p.tenant_id)
    conv = db.execute(mine.with_for_update(key_share=True)).first()
    if conv and (may := NOTERS.get(conv.channel)) and may(db, p, conv.id):
        return conv
    return access.conversation(db, p, conversation_id)


def _told(db: Connection, p: access.Principal, conv: Row, change: str, note: Row) -> None:
    data = {"note_id": str(note.id), "text": note.body["text"], "attachments": note.body.get("attachments", [])}
    events.emit(db, f"note.{change}", p.tenant_id, data | {"by_agent_id": p.agent_id}, conversation_id=conv.id)


def _changed(conversation_id: UUID, note_id: UUID, p: access.Principal, change: str, **values) -> Row:
    E = T.entry
    with tx() as db:
        conv = _noted(db, p, conversation_id)
        kept = (E.c.id == note_id, E.c.conversation_id == conv.id, E.c.kind == "note", E.c.deleted_at.is_(None))
        if (note := db.execute(update(E).where(*kept).values(**values).returning(*E.c)).first()) is None:
            raise ApiError(404, "note_not_found", "no such note")
        _told(db, p, conv, change, note)
        cv.touched(db, conv)
    return note


@router.post("/conversations/{conversation_id}/notes", status_code=201, response_model=ConversationNote)
def add_note(conversation_id: UUID, body: NoteBody, p: access.Principal = WRITE):
    with tx() as db:
        conv = _noted(db, p, conversation_id)
        note = cv.entry(db, conv, "note", body.model_dump(exclude_none=True), agent_id=p.agent_id)
        access.created(db, str(note.id))
        _told(db, p, conv, "added", note)
    return cv.note(note)


def _replayed(db: Connection, p: access.Principal, note_id: str) -> dict:
    E = T.entry
    note = db.execute(select(E).where(E.c.id == UUID(note_id), E.c.tenant_id == p.tenant_id)).one()
    access.conversation(db, p, note.conversation_id)
    return cv.note(note)


access.VIEWS[f"POST {PUBLIC}/conversations/{{conversation_id}}/notes"] = _replayed


@router.patch("/conversations/{conversation_id}/notes/{note_id}", response_model=ConversationNote)
def edit_note(conversation_id: UUID, note_id: UUID, body: NotePatch, p: access.Principal = WRITE):
    edit = body.model_dump(exclude_unset=True, exclude_none=True)
    stamped = T.entry.c.body.concat(edit).concat(func.jsonb_build_object("updated_at", func.now()))
    return cv.note(_changed(conversation_id, note_id, p, "updated", body=stamped))


@router.delete("/conversations/{conversation_id}/notes/{note_id}", status_code=204)
def delete_note(conversation_id: UUID, note_id: UUID, p: access.Principal = WRITE) -> Response:
    _changed(conversation_id, note_id, p, "deleted", deleted_at=func.now())
    return Response(status_code=204)


def _wrapping(db, conv, agent_id) -> list:
    A = T.assignment
    mine = (A.c.conversation_id == conv.id, A.c.agent_id == agent_id, A.c.role != "monitor")
    on = select(A).where(*mine, A.c.state.in_(("active", "wrap_up"))).order_by(A.c.offered_at.desc(), A.c.id.desc())
    stints = db.execute(on).all() if agent_id else []
    if not stints or stints[0].state != "wrap_up":
        raise ApiError(422, "not_in_wrap_up", "that agent is not in wrap-up on this conversation")
    return [a for a in stints if a.state == "wrap_up"]


@router.post("/conversations/{conversation_id}/wrap-up", status_code=204)
def wrap_up(conversation_id: UUID, body: WrapUpIn, p: access.Principal = WRITE) -> Response:
    """One agent's disposition: an agent token's own, or the named agent's for a tenant key. It closes every
    wrap-up the agent has on the conversation, and is 422 while the agent's newest stint there is live. The
    conversation closes after its last wrap-up once the customer is gone."""
    with tx() as db:
        config.hold(db, p.tenant_id)
        conv = access.conversation(db, p, conversation_id, lock=True)
        newest, *older = _wrapping(db, conv, p.agent_id if p.kind == "agent" else body.agent_id)
        d = config.get(db, p.tenant_id, "disposition", body.disposition_id)
        sub = body.sub_disposition_id and config.get(db, p.tenant_id, "disposition", body.sub_disposition_id)
        gone = not d or any((x or {}).get("deleted") for x in (d, sub))
        if gone or d.get("parent_id") or (body.sub_disposition_id and (sub or {}).get("parent_id") != d["id"]):
            raise ApiError(422, "foreign_id", "unknown disposition", {"field": "disposition_id"})
        if d.get("disabled") or (sub or {}).get("disabled"):
            raise ApiError(422, "disposition_disabled", "this disposition is disabled")
        note = (body.note or "").strip() or None
        if d["note_required"] and not note:
            raise ApiError(422, "note_required", "this disposition needs a note")
        ids = {"disposition_id": body.disposition_id, "sub_disposition_id": body.sub_disposition_id}
        for a in older:
            cv.move(db, a, "fold")
        cv.move(db, newest, "submit", **ids, note=note)
    return Response(status_code=204)


@router.post("/conversations/{conversation_id}/wrap-up/extend", status_code=204)
def extend_wrap_up(conversation_id: UUID, body: ExtendIn, p: access.Principal = WRITE) -> Response:
    """Push the agent's own auto wrap-up deadline back, up to max_extend_sec in all; the stream carries the new
    deadline."""
    if p.kind != "agent":
        raise ApiError(403, "agent_only", "an agent extends its own wrap-up")
    J = T.jobs
    with tx() as db:
        a = _wrapping(db, access.conversation(db, p, conversation_id, lock=True), p.agent_id)[0]
        timer = db.execute(select(J.c.payload).where(J.c.kind == "wrap_up_timeout", J.c.key == str(a.id))).scalar()
        if not timer or timer["mode"] != "auto":
            raise ApiError(409, "not_auto_wrap_up", "only an auto wrap-up can be extended")
        if (granted := min(body.seconds, timer["max_extend_sec"] - timer["extended"])) <= 0:
            raise ApiError(409, "extend_limit_reached", "the wrap-up was extended as far as settings allow")
        a = cv.move(db, a, "extend", wrap_up_until=a.wrap_up_until + timedelta(seconds=granted))
        cv.wrap_up_timer(db, a, a.wrap_up_until, timer | {"extended": timer["extended"] + granted})
    return Response(status_code=204)


@router.post("/conversations/{conversation_id}/csat", status_code=204)
def record_csat(conversation_id: UUID, body: CsatIn, p: access.Principal = KEY) -> Response:
    """The customer's satisfaction answer, as text, from a survey after the agent, in any state: the latest answer
    stands, and a survey that got none (no `score`) reads `No Response` until one comes."""
    C = T.conversation
    with tx() as db:
        conv = access.conversation(db, p, conversation_id, lock=True)
        csat = func.coalesce(body.score, C.c.csat, "No Response")
        conv = db.execute(update(C).where(C.c.id == conv.id).values(csat=csat).returning(*C.c)).one()
        events.emit(db, "conversation.csat_recorded", p.tenant_id, {"score": conv.csat}, conversation_id=conv.id)
        cv.touched(db, conv)
    return Response(status_code=204)
