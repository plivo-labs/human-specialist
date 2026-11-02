"""How a call ended: the tenant's status map applied wherever HS writes a system disposition, and each agent's
wrap-up (submit, extend) on the public API."""

from datetime import timedelta
from uuid import UUID

from fastapi import APIRouter, Depends, Response
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from hs import jobs
from hs.access import Principal, load_call, require
from hs.admin.seeds import STATUS_MAP_DEFAULTS
from hs.conversations import management
from hs.conversations.models import ConvChunkInsertModel
from hs.errors import ApiError
from hs.events import emit
from hs.infra.context import bg_db_guard, context_aom_uuid, context_org_uuid, get_db_session
from hs.infra.db import core_engine

router = APIRouter()
CALLS = Depends(require("key:calls", "agent"))
WRAP_UP_TIMEOUT = "wrap_up_timeout"

_MAPPED = text(
    "WITH p AS (UPDATE conversation_parent SET result = :result WHERE id = :pid RETURNING organization,"
    " CASE WHEN direction_inbound THEN 'inbound' ELSE 'outbound' END AS direction)"
    " SELECT p.direction, d.name FROM p LEFT JOIN org_setting s ON s.organization = p.organization"
    " LEFT JOIN dispositions d ON d.organization = p.organization AND internal AND d.is_deleted IS NOT TRUE"
    "  AND d.uuid::text = COALESCE(s.voice_settings -> 'status_map' -> p.direction ->> :result,"
    "  s.voice_settings -> 'status_map' -> 'inbound' ->> :result)"
)
_IN_WRAP_UP = text(
    "SELECT EXISTS (SELECT 1 FROM conversation c JOIN account_org_mapping a ON a.id = c.primary_aom"
    " WHERE c.parent = (SELECT id FROM conversation_parent WHERE uuid = CAST(:cid AS uuid))"
    " AND a.uuid = CAST(:agent AS uuid) AND c.status = 'in_wrap_up' AND c.is_deleted IS NOT TRUE)"
)
_DISPOSITION = text(
    "SELECT d.id, d.name, d.note_required, s.id AS sub_id, s.name AS sub_name FROM dispositions d"
    " LEFT JOIN dispositions s ON s.uuid = CAST(:sub AS uuid) AND s.parent_disposition_id = d.id"
    "  AND s.is_deleted IS NOT TRUE"
    " WHERE d.uuid = CAST(:id AS uuid) AND d.organization = :org AND d.parent_disposition_id IS NULL"
    " AND d.is_deleted IS NOT TRUE"
)
_TIMER = text("SELECT run_at, payload FROM jobs WHERE kind = :kind AND key = :key")


def system_disposition(parent_id: int, result: str) -> str:
    """The name (as the engine's chunks take it) of the tenant's system disposition for this call result, from its
    status map or else today's default. The result is kept on the conversation, for conversation.closed."""
    direction, name = get_db_session().execute(_MAPPED, {"pid": parent_id, "result": result}).one()
    # Queue results live in the inbound map only: an outbound call handed to a queue reads them there.
    return name or STATUS_MAP_DEFAULTS[direction].get(result) or STATUS_MAP_DEFAULTS["inbound"][result]


def timer_key(conversation_number: str, agent_id: str) -> str:
    return f"{conversation_number}:{agent_id}"


class Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class WrapUpIn(Strict):
    disposition_id: UUID
    sub_disposition_id: UUID | None = None
    note: str | None = Field(None, max_length=10_000)
    agent_id: UUID | None = None


class ExtendIn(Strict):
    seconds: int = Field(ge=1, le=3600)


def _not_in_wrap_up() -> ApiError:
    return ApiError(422, "not_in_wrap_up", "that agent has no wrap-up on this conversation")


@router.post("/conversations/{conversation_id}/wrap-up", status_code=204)
def wrap_up(conversation_id: UUID, body: WrapUpIn, p: Principal = CALLS) -> Response:
    """One agent's disposition: an agent token's own, or the named agent's for a tenant key. The conversation
    closes after its last wrap-up."""
    agent = p.agent_id if p.kind == "agent" else body.agent_id and str(body.agent_id)
    call = load_call(str(conversation_id), p)
    with core_engine.connect() as conn:
        if not agent or not conn.execute(_IN_WRAP_UP, {"cid": call.conversation_id, "agent": agent}).scalar():
            raise _not_in_wrap_up()
        sub = body.sub_disposition_id and str(body.sub_disposition_id)
        args = {"id": str(body.disposition_id), "sub": sub, "org": p.org_id}
        d = conn.execute(_DISPOSITION, args).one_or_none()
    if d is None or (body.sub_disposition_id and d.sub_id is None):
        raise ApiError(422, "foreign_id", "unknown disposition", {"field": "disposition_id"})
    note = (body.note or "").strip() or None
    if d.note_required and not note:
        raise ApiError(422, "note_required", "this disposition needs a note")
    context_org_uuid.set(p.tenant_id)
    context_aom_uuid.set(agent)
    chunk = ConvChunkInsertModel(
        type="notes",
        disposition=d.name,
        disposition_id=d.id,
        sub_disposition=d.sub_name,
        sub_disposition_id=d.sub_id,
        description=note,
    )
    res = bg_db_guard(management.create_conversation_chunk)(call.conversation_number, chunk)
    if res.error:
        raise ApiError(409, "wrap_up_failed", "the wrap-up cannot be saved in the conversation's current state")
    return Response(status_code=204)


@router.post("/conversations/{conversation_id}/wrap-up/extend", status_code=204)
def extend_wrap_up(conversation_id: UUID, body: ExtendIn, p: Principal = CALLS) -> Response:
    """Push the agent's own auto wrap-up deadline back, up to max_extend_sec in all; the stream carries the new
    deadline."""
    if p.kind != "agent":
        raise ApiError(403, "agent_only", "an agent extends its own wrap-up")
    call = load_call(str(conversation_id), p)
    key = timer_key(call.conversation_number, p.agent_id)
    with Session(core_engine) as s:
        timer = s.execute(_TIMER, {"kind": WRAP_UP_TIMEOUT, "key": key}).one_or_none()
        if timer is None:
            raise _not_in_wrap_up()
        payload = timer.payload
        if payload.get("mode") != "auto":
            raise ApiError(409, "not_auto_wrap_up", "only an auto wrap-up can be extended")
        granted = min(body.seconds, payload["max_extend_sec"] - payload["extended"])
        if granted <= 0:
            raise ApiError(409, "extend_limit_reached", "the wrap-up was extended as far as settings allow")
        deadline = timer.run_at + timedelta(seconds=granted)
        payload = payload | {"extended": payload["extended"] + granted}
        jobs.schedule(WRAP_UP_TIMEOUT, payload, run_at=deadline, key=key, session=s)
        emit(
            "conversation.wrap_up_started",
            tenant_id=p.tenant_id,
            data={"mode": "auto", "deadline_at": deadline},
            conversation_id=call.conversation_id,
            agent_id=p.agent_id,
            session=s,
        )
        s.commit()
    return Response(status_code=204)
