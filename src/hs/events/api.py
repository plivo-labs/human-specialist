"""Webhook subscriptions, replay, the live stream and the per-conversation timeline."""

import secrets
from datetime import UTC, datetime, timedelta
from typing import Literal
from uuid import UUID

import redis.asyncio
from fastapi import APIRouter, Depends, Header, Query, Request, Response
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import Text, func, insert, select, update

from hs import events
from hs.access import Principal, bearer, encrypt, load_call, require, resolve
from hs.errors import ApiError
from hs.events import sse
from hs.events.catalog import CATALOG
from hs.events.webhooks import destination, webhooks
from hs.infra.db import core_engine
from hs.infra.settings import settings
from hs.jobs import schedule
from hs.paging import Page, Paging, page, paging

router = APIRouter()
catalog = APIRouter()  # app.webhooks: one documented POST per event type
EVENTS = Depends(require("key:events"))
EVENTS_SECRET = Depends(require("key:events", secret=True))
CALLS_SUP = Depends(require("key:calls", "sup"))
STREAM = Depends(require("key:events", "agent", "stream"))


EventType = Literal[tuple(CATALOG)]


class WebhookIn(BaseModel):
    url: str
    event_types: list[EventType] = []
    enabled: bool = True


class WebhookPatch(BaseModel):
    url: str | None = None
    event_types: list[EventType] | None = None
    enabled: bool | None = None


class Webhook(BaseModel):
    id: str
    url: str
    event_types: list[str]
    enabled: bool
    created_at: datetime


class WebhookSecret(Webhook):
    secret: str


def _by_seq(rows: list[dict], limit: int) -> dict:
    """From `limit + 1` rows: a page whose cursor is its last seq, which GET /events takes as `after`."""
    more, rows = len(rows) > limit, rows[:limit]
    return {"data": rows, "next_cursor": rows[-1]["id"] if more else None}


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


def _document(type_: str, model: type[BaseModel]) -> None:
    @catalog.post(type_, name=type_)
    def event(_body: Envelope[model]) -> None:
        """Signed with X-HS-Signature: v1=<hex HMAC-SHA256(secret, f"{X-HS-Timestamp}.{body}")>."""


for _type, _model in CATALOG.items():
    _document(_type, _model)


def _checked(url: str | None) -> None:
    if url is None:
        return
    try:
        destination(url)
    except ValueError as e:
        raise ApiError(422, "webhook_url_refused", str(e)) from e


def _hook(conn, principal: Principal, webhook_id: UUID):
    row = conn.execute(
        select(webhooks).where(webhooks.c.id == str(webhook_id), webhooks.c.tenant_id == principal.tenant_id)
    ).first()
    if row is None:
        raise ApiError(404, "webhook_not_found", "no such webhook")
    return row


def _set(principal: Principal, webhook_id: UUID, **values):
    with core_engine.begin() as conn:
        _hook(conn, principal, webhook_id)
        changed = update(webhooks).where(webhooks.c.id == str(webhook_id)).values(**values)
        return dict(conn.execute(changed.returning(*webhooks.c)).one()._mapping)


@router.post("/webhooks", status_code=201, response_model=WebhookSecret)
def create_webhook(body: WebhookIn, principal: Principal = EVENTS_SECRET):
    _checked(body.url)
    secret = secrets.token_hex(32)
    row = insert(webhooks).values(tenant_id=principal.tenant_id, secret=encrypt(secret), **body.model_dump())
    with core_engine.begin() as conn:
        return {**conn.execute(row.returning(*webhooks.c)).one()._mapping, "secret": secret}


@router.get("/webhooks", response_model=Page[Webhook])
def list_webhooks(principal: Principal = EVENTS, p: Paging = Depends(paging)):
    key = webhooks.c.id.cast(Text)
    rows = select(webhooks).where(webhooks.c.tenant_id == principal.tenant_id, key > (p.after or ""))
    with core_engine.connect() as conn:
        return page([dict(r._mapping) for r in conn.execute(rows.order_by(key).limit(p.limit + 1))], p)


@router.get("/webhooks/{webhook_id}", response_model=Webhook)
def get_webhook(webhook_id: UUID, principal: Principal = EVENTS):
    with core_engine.connect() as conn:
        return dict(_hook(conn, principal, webhook_id)._mapping)


@router.patch("/webhooks/{webhook_id}", response_model=Webhook)
def patch_webhook(webhook_id: UUID, body: WebhookPatch, principal: Principal = EVENTS):
    _checked(body.url)
    return _set(principal, webhook_id, **body.model_dump(exclude_none=True))


@router.delete("/webhooks/{webhook_id}", status_code=204)
def delete_webhook(webhook_id: UUID, principal: Principal = EVENTS) -> Response:
    with core_engine.begin() as conn:
        _hook(conn, principal, webhook_id)
        conn.execute(webhooks.delete().where(webhooks.c.id == str(webhook_id)))
    return Response(status_code=204)


@router.post("/webhooks/{webhook_id}/rotate-secret", response_model=WebhookSecret)
def rotate_secret(webhook_id: UUID, principal: Principal = EVENTS_SECRET):
    """The old secret stays valid for 24 h; meanwhile deliveries carry both signatures."""
    secret = secrets.token_hex(32)
    expires = func.now() + timedelta(hours=24)
    rotated = _set(
        principal,
        webhook_id,
        secret=encrypt(secret),
        previous_secret=webhooks.c.secret,
        previous_secret_expires_at=expires,
    )
    return {**rotated, "secret": secret}


@router.post("/webhooks/{webhook_id}/test", status_code=202)
def test_webhook(webhook_id: UUID, principal: Principal = EVENTS) -> Response:
    with core_engine.connect() as conn:
        hook = _hook(conn, principal, webhook_id)
    body = {
        "id": f"test_{secrets.token_hex(8)}",
        "type": "webhook.test",
        "created_at": events.iso(datetime.now(UTC)),
        "tenant_id": principal.tenant_id,
        "conversation_id": None,
        "agent_id": None,
        "direction": None,
        "context": None,
        "data": {"webhook_id": hook.id},
    }
    schedule("webhook", {"webhook_id": hook.id, "body": body})
    return Response(status_code=202)


@router.get("/events", response_model=Page[Envelope[dict]])
def replay(
    principal: Principal = EVENTS,
    after: int = Query(0, ge=0),
    type: str | None = None,
    limit: int = Query(50, ge=1, le=200),
):
    return _by_seq(events.read(principal.tenant_id, after, limit + 1, type=type), limit)


@router.get("/conversations/{conversation_id}/timeline", response_model=Page[Envelope[dict]])
def timeline(
    conversation_id: UUID,
    principal: Principal = CALLS_SUP,
    after: int = Query(0, ge=0),
    limit: int = Query(200, ge=1, le=200),
):
    load_call(conversation_id, principal)
    rows = events.read(
        principal.tenant_id, after, limit + 1, scope=events.scope(principal), conversation_id=str(conversation_id)
    )
    return _by_seq(rows, limit)


@router.get("/events/stream", response_class=StreamingResponse)
async def stream(
    request: Request,
    principal: Principal = STREAM,
    after: int | None = Query(None, ge=0),
    last_event_id: int | None = Header(None),
):
    """Live events; resume with Last-Event-ID or ?after= (the header wins)."""
    visible = await run_in_threadpool(events.scope, principal)
    cursor = last_event_id if last_event_id is not None else after
    if cursor is None:
        cursor = await run_in_threadpool(events.latest, principal.tenant_id)
    token = bearer(request) or request.query_params.get("access_token", "")

    async def latest() -> int:
        return await run_in_threadpool(events.latest, principal.tenant_id)

    async def read(after: int, head: int) -> list[dict]:
        return await run_in_threadpool(events.read, principal.tenant_id, after, sse.PAGE, head=head, scope=visible)

    async def alive() -> bool:
        """The credential require() accepted is still good; a supervisor's teams are read again with it."""
        nonlocal visible
        found = await run_in_threadpool(resolve, token)
        if found is not None:
            visible = await run_in_threadpool(events.scope, found)
        return found is not None

    async def body():
        # Limit: one Redis pubsub connection per open stream; a per-process subscriber that fans out to the
        # streams when connections run short.
        client = redis.asyncio.Redis.from_url(settings.redis_url)
        pubsub = client.pubsub()
        try:
            await pubsub.subscribe(f"ev:{principal.tenant_id}")

            async def wake():
                return await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)

            async for chunk in sse.frames(latest, read, cursor, alive, wake):
                yield chunk
        finally:
            await pubsub.aclose()
            await client.aclose()

    return StreamingResponse(body(), media_type="text/event-stream", headers={"Cache-Control": "no-cache"})
