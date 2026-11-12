"""SMS and WhatsApp over Plivo: inbound messages, delivery reports, the `send` op and the media proxy.

Plivo's V2 signature covers the URL and the nonce, not the body, so a body is data and never authority: a delivery
report changes only the outbound message its URL names (`message-status/{id}`, inside the signature)."""

import hmac
import json
import logging
import re
import time
from datetime import UTC, datetime
from uuid import UUID

import httpx
from fastapi import APIRouter, Query, Response
from fastapi.responses import StreamingResponse
from sqlalchemy import select, update
from sqlalchemy.engine import Connection
from starlette.background import BackgroundTask

from hs import access, config, events, jobs, plivo, routing, text
from hs import conversations as cv
from hs.core import ApiError, T, read, settings, tx

log = logging.getLogger(__name__)
callbacks = APIRouter(dependencies=[plivo.TEXT])
REPORTS = ("queued", "sent", "delivered", "read", "deleted", "undelivered", "failed")
TIMES = {"queued": "QueuedTime", "sent": "SentTime", "delivered": "DeliveryReportTime", "read": "ReadReceiptTime"}
TIMES["failed"] = "MessageTime"
PICKS = {"list_reply": "ListReply", "button_reply": "ButtonReply"}
SEND_CHECK_DELAY = 300
MEDIA_MAX_BYTES = 100 * 2**20  # WhatsApp's largest media size; a longer file is refused


def _object(form: dict, key: str) -> dict:
    try:
        value = json.loads(form.get(key) or "{}")
    except ValueError:
        value = None
    if not isinstance(value, dict):
        raise ApiError(422, "invalid_message", f"{key} is not a JSON object")
    return value


def _whatsapp(form: dict) -> tuple[object, dict | None]:
    kind = form.get("ContentType")
    if kind in ("text", "media", "image"):
        return form.get("Body") or None, None
    if kind == "button":
        return _object(form, "Button").get("Text"), {"type": "button"}
    if kind == "location":
        return None, {"type": "location", "location": _object(form, "Location")}
    if kind != "interactive":
        raise ApiError(422, "unknown_content_type", "HS takes WhatsApp text, media, buttons, picks and locations")
    picked = _object(form, "Interactive")
    reply = picked.get(PICKS.get(picked.get("Type")), {})
    shown = {k.lower(): reply[k] for k in ("Id", "Title", "Description") if k in reply}
    return reply.get("Title"), {"type": picked.get("Type")} | shown


@callbacks.post("/message")
def inbound(tenant_id: UUID, form: dict = plivo.FORM, db: Connection = plivo.TEXT) -> Response:
    """A customer's SMS or WhatsApp. It joins the open conversation for (channel, from, to); otherwise a number whose
    open action is a queue opens one there, whatever its hours. Any other is dropped."""
    media = sorted((k for k in form if re.fullmatch(r"Media\d+", k)), key=lambda k: int(k[5:]))
    channel = "whatsapp" if form.get("Type") == "whatsapp" else "sms"
    caption = "Body" if form.get("Type") == "mms" else "Text"
    said, whatsapp = _whatsapp(form) if channel == "whatsapp" else (form.get(caption), None)
    msg = {"channel": channel, "from": plivo.e164(form.get("From", "")), "to": plivo.e164(form.get("To", ""))}
    msg |= {"text": said if isinstance(said, str) else None, "provider_id": form.get("MessageUUID")}
    msg |= {"whatsapp": whatsapp, "quoted": _object(form, "Context").get("MessageUUID")}
    msg["attachments"] = [{"url": form[k], "hosted": True, "content_type": form.get("MimeType")} for k in media]
    if text.seen(db, tenant_id, msg["to"], msg["provider_id"]):
        return Response(status_code=200)
    conv = text.find(db, tenant_id, msg)
    number = None if conv else config.by_key(db, tenant_id, "number", msg["to"])
    action = number and number[1]["open_action"]
    if conv is None and (not action or action["type"] != "queue"):
        log.info("a text to tenant %s was dropped: its number opens no queue", tenant_id)
        return Response(status_code=200)
    conv = conv or text.opened(db, tenant_id, msg)
    text.received(db, conv, msg)
    if action and conv.state == "open":
        try:
            routing.enqueue(db, conv, UUID(action["queue_id"]), agents=action["agents"])
        except routing.Full:
            cv.gone(db, conv)
    return Response(status_code=200)


@callbacks.post("/message-status/{message_id}")
def report(tenant_id: UUID, message_id: UUID, form: dict = plivo.FORM, db: Connection = plivo.TEXT) -> Response:
    """Plivo's delivery report for one outbound message; its MessageUUID fills one the send's reply never gave."""
    if (to := form.get("Status", "")) in REPORTS:
        error = {"error_code": form.get("ErrorCode")} if to in ("undelivered", "failed") else {}
        text.status(db, tenant_id, message_id, to, form.get("MessageUUID") or None, _reported(form, to), **error)
    return Response(status_code=200)


def _reported(form: dict, status: str) -> datetime | None:
    raw = form.get(TIMES.get(status, ""), "").removesuffix(" UTC")
    try:
        if raw.replace(".", "", 1).isdigit():  # epoch seconds, or milli-, micro- or nanoseconds
            n = float(raw)
            while n >= 1e12:
                n /= 1000
            return datetime.fromtimestamp(n, UTC)
        at = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return at if at.tzinfo else at.replace(tzinfo=UTC)


def _send(tenant, p: dict) -> dict:
    url = f"{settings.public_base_url}/plivo/{tenant.id}/message-status/{p['entry_id']}"
    reply = plivo.rest(tenant, "POST", "Message/", timeout_sec=30, **p["args"], url=url)
    if not reply.get("message_uuid"):
        raise plivo.Refused(422, "plivo_refused", "Plivo gave the message no id")
    return reply


def _sent(p: dict, reply: dict) -> None:
    """Plivo took the message: it is told as message.sent only now, so a refused one never reads sent."""
    E, uuid = T.entry, reply["message_uuid"][0]
    with tx() as db:
        db.execute(update(E).where(E.c.id == p["entry_id"], E.c.provider_id.is_(None)).values(provider_id=uuid))
        told = p["told"] | {"provider_id": uuid}
        events.emit(db, "message.sent", p["tenant_id"], told, conversation_id=p["conversation_id"])


def _refused(p: dict, e: plivo.Refused) -> None:
    """The message failed. An agent's first message to a customer closes the conversation as failed, with no
    wrap-up, as a refused dial does."""
    E = T.entry
    with tx() as db:
        access.refused(db, text.answer(e), p.get("claim"))
        conv = cv.lock(db, UUID(p["conversation_id"]))
        if db.execute(select(E.c.id).where(E.c.id == p["entry_id"], E.c.status == "pending").with_for_update()).first():
            text.status(db, conv.tenant_id, p["entry_id"], "failed", error_code=e.code)
            if p["first"]:
                text.ended(db, conv, "failed")


def _unknown(p: dict) -> None:
    with tx() as db:
        jobs.schedule(db, "send_check", p, delay=SEND_CHECK_DELAY, key=p["entry_id"])


jobs.handler("send_check")(lambda job: _refused(job.payload, plivo.Refused(503, "plivo_unavailable", "no report")))
plivo.OPS["send"] = plivo.Op(create=True, send=_send, done=_sent, failed=_refused, uncertain=_unknown)


@text.router.get("/messages/{message_id}/media/{n}", openapi_extra={"security": [{}]})
def media(message_id: UUID, n: str, exp: int = 0, sig: str = Query("", max_length=64)) -> Response:
    """A Plivo-hosted file of a customer message, for whoever holds its signed URL (an `<img src>` sends no header),
    fetched from Plivo with the tenant's credentials. `n` is read only once the signature vouches for it."""
    if not sig:
        raise ApiError(401, "unauthorized", "a signed media URL is required")
    if exp < time.time() or not hmac.compare_digest(sig, text.signature(message_id, n, exp)):
        raise ApiError(403, "invalid_signature", "this media URL is invalid or expired")
    E = T.entry
    with read() as db:
        owner = T.tenant.c.id == E.c.tenant_id
        row = db.execute(select(E.c.body, T.tenant).join(T.tenant, owner).where(E.c.id == message_id)).first()
    files, i = row.body["attachments"] if row else [], int(n)
    if i >= len(files) or not files[i].get("hosted") or not plivo.fetchable(files[i]["url"]):
        raise ApiError(404, "media_not_found", "no such file")
    auth = (row.plivo_auth_id, plivo.decrypt(row.plivo_token_enc))
    try:
        r = plivo.HTTP.send(plivo.HTTP.build_request("GET", files[i]["url"], timeout=15), auth=auth, stream=True)
    except httpx.HTTPError as e:
        raise ApiError(503, "plivo_unavailable", "Plivo did not answer") from e
    if not r.is_success:
        r.close()
        raise ApiError(502, "media_unavailable", "Plivo did not serve the file")
    if int(r.headers.get("content-length", 0)) > MEDIA_MAX_BYTES:
        r.close()
        raise ApiError(413, "media_too_large", "the file is over 100 MB")
    return StreamingResponse(_capped(r), media_type=r.headers.get("content-type"), background=BackgroundTask(r.close))


def _capped(r: httpx.Response):
    sent = 0
    for chunk in r.iter_bytes():
        if (sent := sent + len(chunk)) > MEDIA_MAX_BYTES:
            raise ValueError("media body passed its ceiling")  # the client's read fails, never a clean short file
        yield chunk
