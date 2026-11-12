"""Webhook subscriptions, replay, the per-conversation timeline and the live stream."""

import asyncio
import logging
import secrets
from datetime import UTC, datetime, timedelta
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Header, Query, Request, Response
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import Text, func, select, update
from sqlalchemy.engine import Connection, Row

from hs import access, core, events, jobs, plivo
from hs.access import Principal
from hs.core import ApiError, Page, Paging, page, paging
from hs.events.catalog import CATALOG
from hs.events.webhooks import destination

router = APIRouter()
webhook_docs = APIRouter()  # app.webhooks: one documented POST per event type
EVENTS = access.require("key:events")
STREAM = access.require(*events.GRANTS)
CALLS_SUP = access.require("key:calls", "sup")
EventType = Literal[tuple(CATALOG)]


def _no_token(record: logging.LogRecord) -> bool:
    """uvicorn's access line carries the query, the only place EventSource can send a stream token."""
    record.args = tuple(core.redact(a) if isinstance(a, str) else a for a in record.args or ())
    return True


logging.getLogger("uvicorn.access").addFilter(_no_token)


class WebhookIn(BaseModel):
    url: str
    event_types: list[EventType] = []
    queue_ids: list[UUID] = []
    enabled: bool = True


class WebhookPatch(BaseModel):
    url: str | None = None
    event_types: list[EventType] | None = None
    queue_ids: list[UUID] | None = None
    enabled: bool | None = None


class Webhook(BaseModel):
    id: str
    url: str
    event_types: list[str]
    queue_ids: list[str]
    enabled: bool
    created_at: datetime


class WebhookSecret(Webhook):
    secret: str


class Envelope[T](BaseModel):
    """A webhook delivery body, and the `data:` of an SSE frame."""

    id: str
    type: str
    created_at: str = Field(json_schema_extra={"format": "date-time"})  # as sent, so replay equals the delivery
    tenant_id: str
    conversation_id: str | None
    agent_id: str | None
    direction: Literal["inbound", "outbound"] | None
    context: dict | None
    data: T


for _type, _model in CATALOG.items():

    @webhook_docs.post(_type, name=_type)
    def event(_body: Envelope[_model]) -> None:
        """Signed with X-HS-Signature: v1=<hex HMAC-SHA256(secret, f"{X-HS-Timestamp}.{body}")>."""


def _by_seq(rows: list[dict], limit: int) -> dict:
    """From `limit + 1` rows: a page whose cursor is its last seq, which `after` takes."""
    more, rows = len(rows) > limit, rows[:limit]
    return {"data": rows, "next_cursor": rows[-1]["id"] if more else None}


def _checked(url: str | None) -> None:
    if url is None:
        return
    try:
        destination(url)
    except ValueError as e:
        raise ApiError(422, "webhook_url_refused", str(e)) from e


def _out(row: Row) -> dict:
    return {**row._mapping, "id": str(row.id)}


def _hook(db: Connection, p: Principal, webhook_id: UUID) -> Row:
    W = core.T.webhooks
    row = db.execute(select(W).where(W.c.id == webhook_id, W.c.tenant_id == p.tenant_id)).first()
    if row is None:
        raise ApiError(404, "webhook_not_found", "no such webhook")
    return row


def _set(p: Principal, webhook_id: UUID, **values) -> dict:
    W = core.T.webhooks
    with core.tx() as db:
        _hook(db, p, webhook_id)
        access.owned(db, p, "queue", map(UUID, values.get("queue_ids", ())))
        return _out(db.execute(update(W).where(W.c.id == webhook_id).values(**values).returning(*W.c)).one())


@router.post("/webhooks", status_code=201, response_model=WebhookSecret)
def create_webhook(body: WebhookIn, p: Principal = EVENTS):
    _checked(body.url)
    secret, W = secrets.token_hex(32), core.T.webhooks
    with core.tx() as db:
        access.owned(db, p, "queue", body.queue_ids)
        made = W.insert().values(tenant_id=p.tenant_id, secret=plivo.encrypt(secret), **body.model_dump(mode="json"))
        return _out(db.execute(made.returning(*W.c)).one()) | {"secret": secret}


@router.get("/webhooks", response_model=Page[Webhook])
def list_webhooks(p: Principal = EVENTS, paged: Paging = Depends(paging)):
    W = core.T.webhooks
    key = W.c.id.cast(Text)
    rows = select(W).where(W.c.tenant_id == p.tenant_id, key > (paged.after or "")).order_by(key).limit(paged.limit + 1)
    with core.read() as db:
        return page([_out(r) for r in db.execute(rows)], paged)


@router.get("/webhooks/{webhook_id}", response_model=Webhook)
def get_webhook(webhook_id: UUID, p: Principal = EVENTS):
    with core.read() as db:
        return _out(_hook(db, p, webhook_id))


@router.patch("/webhooks/{webhook_id}", response_model=Webhook)
def patch_webhook(webhook_id: UUID, body: WebhookPatch, p: Principal = EVENTS):
    _checked(body.url)
    return _set(p, webhook_id, **body.model_dump(mode="json", exclude_none=True))


@router.delete("/webhooks/{webhook_id}", status_code=204)
def delete_webhook(webhook_id: UUID, p: Principal = EVENTS) -> Response:
    with core.tx() as db:
        _hook(db, p, webhook_id)
        db.execute(core.T.webhooks.delete().where(core.T.webhooks.c.id == webhook_id))
    return Response(status_code=204)


@router.post("/webhooks/{webhook_id}/rotate-secret", response_model=WebhookSecret)
def rotate_secret(webhook_id: UUID, p: Principal = EVENTS):
    """The old secret stays valid for 24 h; meanwhile deliveries carry both signatures."""
    secret, W = secrets.token_hex(32), core.T.webhooks
    old = {"previous_secret": W.c.secret, "previous_secret_expires_at": func.now() + timedelta(hours=24)}
    return _set(p, webhook_id, secret=plivo.encrypt(secret), **old) | {"secret": secret}


@router.post("/webhooks/{webhook_id}/test", status_code=202)
def test_webhook(webhook_id: UUID, p: Principal = EVENTS) -> Response:
    at = datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    with core.tx() as db:
        hook = str(_hook(db, p, webhook_id).id)
        test = {"id": f"test_{secrets.token_hex(8)}", "type": "webhook.test", "created_at": at}
        body = dict.fromkeys(Envelope.model_fields) | test | {"tenant_id": p.tenant_id, "data": {"webhook_id": hook}}
        jobs.schedule(db, "webhook", {"webhook_id": hook, "body": body})
    return Response(status_code=202)


@router.get("/events", response_model=Page[Envelope[dict]])
def replay(
    p: Principal = EVENTS, after: int = Query(0, ge=0), type: str | None = None, limit: int = Query(50, ge=1, le=200)
):
    return _by_seq(events.read(p.tenant_id, after, limit + 1, types=type and [type])[0], limit)


@router.get("/conversations/{conversation_id}/timeline", response_model=Page[Envelope[dict]])
def timeline(
    conversation_id: UUID, p: Principal = CALLS_SUP, after: int = Query(0, ge=0), limit: int = Query(200, ge=1, le=200)
):
    with core.read() as db:
        access.conversation(db, p, conversation_id)
    found, _ = events.read(p.tenant_id, after, limit + 1, agent=p.agent_id, conversation_id=str(conversation_id))
    return _by_seq(found, limit)


@router.get("/events/stream", response_class=StreamingResponse)
async def stream(
    request: Request,
    _: Principal = STREAM,
    after: int | None = Query(None, ge=0),
    last_event_id: int | None = Header(None),
):
    """Live events; resume with Last-Event-ID or ?after= (the header wins)."""
    token = access.credential(request, stream=True)
    if (auth := await run_in_threadpool(events.authorize, token, request)) is None:
        raise ApiError(401, "unauthorized", "missing or invalid credential")
    cursor = after if last_event_id is None else last_event_id
    feed = events.Stream(auth, token, request, asyncio.get_running_loop())
    return StreamingResponse(feed.frames(cursor), media_type="text/event-stream", headers={"Cache-Control": "no-cache"})
