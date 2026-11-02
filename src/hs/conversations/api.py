"""The public conversation reads (list, one with its legs, notes and recordings) and notes."""

from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends
from pydantic import BaseModel, StringConstraints
from sqlalchemy import text

from hs.access import Principal, load_call, require
from hs.admin.rbac import accessible_agents
from hs.conversations import management
from hs.conversations.models import ConvChunkInsertModel
from hs.errors import ApiError
from hs.infra.context import bg_db_guard, context_aom_uuid, context_org_uuid
from hs.infra.db import core_engine
from hs.paging import Page, Paging, page, paging

router = APIRouter()
CALLS = Depends(require("key:calls", "agent"))

# agent_ids: everyone who handled or joined the call, as load_call counts them.
SUMMARY = (
    "SELECT cp.id AS pk, cp.uuid::text AS id, cp.conversation_number,"
    " CASE WHEN cp.direction_inbound THEN 'inbound' ELSE 'outbound' END AS direction,"
    " CASE WHEN cp.end_at IS NOT NULL THEN 'closed'"
    " WHEN EXISTS (SELECT 1 FROM conversation WHERE parent = cp.id AND status = 'in_wrap_up')"
    " AND NOT EXISTS (SELECT 1 FROM conversation WHERE parent = cp.id AND primary_aom IS NOT NULL"
    " AND status IN ('open', 'on_hold', 'waiting'))"
    " THEN 'in_wrap_up' ELSE 'active' END AS state, cp.contact_identifier AS number,"
    " ARRAY(SELECT a.uuid::text FROM account_org_mapping a WHERE a.id IN (SELECT primary_aom FROM conversation"
    " WHERE parent = cp.id UNION SELECT aom FROM conversation_secondary_aom_mapping WHERE conversation = cp.id)"
    " ORDER BY a.id) AS agent_ids, cp.created_at, cp.end_at AS ended_at FROM conversation_parent cp"
    " WHERE cp.organization = :org AND cp.is_deleted IS NOT TRUE"
)
# The call a CallUUID belongs to: the conversation's own key, or a participant leg under it.
ROOTS = (
    "SELECT CAST(:call_uuid AS text) UNION"
    " SELECT p.call_uuid FROM call_leg l JOIN call_leg p ON p.id = l.parent_call_leg WHERE l.call_uuid = :call_uuid"
)
LIST = text(
    f"SELECT * FROM ({SUMMARY}"  # noqa: S608 (constants only)
    " AND (CAST(:inbound AS boolean) IS NULL OR cp.direction_inbound = :inbound)"
    " AND (CAST(:created_after AS timestamptz) IS NULL OR cp.created_at > :created_after)"
    " AND (CAST(:created_before AS timestamptz) IS NULL OR cp.created_at < :created_before)"
    " AND (CAST(:number AS text) IS NULL OR ltrim(cp.contact_identifier, '+') = ltrim(:number, '+'))"
    f" AND (CAST(:call_uuid AS text) IS NULL OR cp.contact_uuid IN ({ROOTS})"
    f" OR cp.id IN (SELECT parent FROM conversation WHERE contact_uuid IN ({ROOTS})))) s"
    " WHERE (CAST(:state AS text) IS NULL OR s.state = :state)"
    " AND (CAST(:agent AS text) IS NULL OR :agent = ANY(s.agent_ids))"
    " AND (CAST(:visible AS text[]) IS NULL OR s.agent_ids && CAST(:visible AS text[]))"
    " AND (CAST(:before AS integer) IS NULL OR s.pk < :before) ORDER BY s.pk DESC LIMIT :limit"
)
ONE = text(SUMMARY + " AND cp.uuid = CAST(:id AS uuid)")
LEG_ROWS = (
    "WITH root AS (SELECT id FROM call_leg WHERE call_uuid IN (SELECT contact_uuid FROM conversation_parent"
    " WHERE id = :pk UNION SELECT contact_uuid FROM conversation WHERE parent = :pk)),"
    " leg AS (SELECT * FROM call_leg WHERE id IN (SELECT id FROM root) OR parent_call_leg IN (SELECT id FROM root))"
)
# The caller's CallUUID keys both the MPC row and its customer participant row: list it once, with the role.
LEGS = text(
    LEG_ROWS + " SELECT DISTINCT ON (call_uuid) call_uuid, lower(role) AS role, start_time AS started_at,"  # noqa: S608 (constants only)
    " end_time AS ended_at FROM leg ORDER BY call_uuid, role NULLS LAST"
)
NOTES = text(
    'SELECT ch.uuid::text AS id, ch.description AS text, ch."from" AS agent_id,'
    " coalesce(ch.timestamp, ch.created_at) AS created_at"
    " FROM conversation_chunk ch JOIN conversation c ON c.id = ch.conversation WHERE c.parent = :pk"
    " AND ch.type = 'notes' AND ch.disposition IS NULL AND ch.is_deleted IS NOT TRUE ORDER BY created_at, ch.id"
)
RECORDINGS = text(
    "SELECT r.uuid::text FROM conversation_chunk ch JOIN conversation c ON c.id = ch.conversation"
    " JOIN recording r ON r.id = ch.recording WHERE c.parent = :pk ORDER BY r.id"
)


class ConversationSummary(BaseModel):
    id: str
    conversation_number: str
    direction: Literal["inbound", "outbound"]
    state: Literal["active", "in_wrap_up", "closed"]
    number: str | None
    agent_ids: list[str]
    created_at: datetime
    ended_at: datetime | None


class ConversationLeg(BaseModel):
    call_uuid: str
    role: str | None
    started_at: str | None
    ended_at: str | None


class ConversationNote(BaseModel):
    id: str
    text: str
    agent_id: str | None
    created_at: datetime


class Conversation(ConversationSummary):
    legs: list[ConversationLeg]
    notes: list[ConversationNote]
    recording_ids: list[str]


class NoteBody(BaseModel):
    text: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=10_000)]


def visible_agents(principal: Principal) -> list[str] | None:
    """accessible_agents as a bind value: a set does not adapt to a Postgres array."""
    agents = accessible_agents(principal)
    return None if agents is None else list(agents)


@router.get("/conversations", response_model=Page[ConversationSummary])
def list_conversations(
    principal: Principal = CALLS,
    p: Paging = Depends(paging),
    state: Literal["active", "in_wrap_up", "closed"] | None = None,
    agent_id: UUID | None = None,
    number: str | None = None,
    call_uuid: str | None = None,
    direction: Literal["inbound", "outbound"] | None = None,
    created_after: datetime | None = None,
    created_before: datetime | None = None,
):
    if p.after is not None and not p.after.isdigit():
        raise ApiError(422, "invalid_cursor", "cursor is not one this API returned")
    params = {
        "org": principal.org_id,
        "inbound": None if direction is None else direction == "inbound",
        "created_after": created_after,
        "created_before": created_before,
        "number": number,
        "call_uuid": call_uuid,
        "state": state,
        "agent": str(agent_id) if agent_id else None,
        "visible": visible_agents(principal),
        "before": int(p.after) if p.after else None,
        "limit": p.limit + 1,
    }
    with core_engine.connect() as conn:
        rows = [r._asdict() for r in conn.execute(LIST, params)]
    return page(rows, p, key="pk")


@router.get("/conversations/{conversation_id}", response_model=Conversation)
def get_conversation(conversation_id: UUID, principal: Principal = CALLS):
    load_call(str(conversation_id), principal)
    with core_engine.connect() as conn:
        row = conn.execute(ONE, {"org": principal.org_id, "id": str(conversation_id)}).one()._asdict()
        key = {"pk": row["pk"]}
        row["legs"] = [r._asdict() for r in conn.execute(LEGS, key)]
        row["notes"] = [r._asdict() for r in conn.execute(NOTES, key)]
        row["recording_ids"] = conn.execute(RECORDINGS, key).scalars().all()
    return row


@router.post("/conversations/{conversation_id}/notes", status_code=201, response_model=ConversationNote)
def add_note(conversation_id: UUID, body: NoteBody, principal: Principal = CALLS):
    """A note on the conversation, while an agent is on the call; a tenant key writes as that agent."""
    call = load_call(str(conversation_id), principal)
    context_org_uuid.set(principal.tenant_id)
    context_aom_uuid.set(principal.agent_id)
    chunk = ConvChunkInsertModel(type="notes", description=body.text)
    res = bg_db_guard(management.create_conversation_chunk)(call.conversation_number, chunk)
    if res.error:
        raise ApiError(409, "conversation_not_active", "notes need a live call with an agent on it")
    note = res.data
    return {"id": str(note.uuid), "text": body.text, "agent_id": note.from_user, "created_at": note.timestamp}
