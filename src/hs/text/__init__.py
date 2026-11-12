"""Text conversations on chat, SMS, WhatsApp and email: customer messages in, agent messages out, closing, cold
transfers, a second agent and the session timeout.

A customer message lands on the open conversation for its (channel, from, to), or on the email thread its headers
name; only a Plivo inbound or a text enqueue may open one. An agent message is an entry that leaves through Plivo
(SMS and WhatsApp, unless the tenant chose `external`) or as `message.sent` with `delivery: external`, which the
tenant's own gateway delivers and reports on."""

import hashlib
import hmac
import time
import uuid
from datetime import UTC, datetime, timedelta
from itertools import islice
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Response
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, Field, StringConstraints, model_validator
from sqlalchemy import func, select, text, tuple_
from sqlalchemy.engine import Connection, Row

from hs import access, config, events, hot, jobs, routing
from hs import conversations as cv
from hs.access import Principal
from hs.config import models as m
from hs.core import PUBLIC, ApiError, Page, Paging, T, page, paging, read, settings, tx
from hs.events import catalog
from hs.plivo import Refused, command
from hs.plivo import verdict as answered
from hs.text import templates

router = APIRouter()
READ = access.require("key:calls", "agent", "sup")
WRITE = access.require("key:calls", "agent")
GATEWAY = access.require("key:messages")
CHANNELS = cv.TEXT
MEDIA_TTL = 300
Address = Annotated[str, Field(min_length=1, max_length=320)]
Header = Annotated[str, Field(max_length=1000)]
Addresses = Annotated[list[Header], Field(max_length=50)]
SMS_BYTES = 5_000_000

# A statement of its own after routing._LOCK, so its snapshot sees the offers of a transaction that held a lock meanwhile.
_LOAD = text(
    "SELECT p.agent_id, (s.body->>'availability')::boolean AS available, :channel || ':' || p.agent_id"
    " <> ALL(CAST(:full AS text[])) AS room FROM presence p JOIN config s ON s.id = p.status_id"
    " WHERE p.agent_id = ANY(CAST(:ids AS uuid[])) ORDER BY p.agent_id"
)
_MEMBERS = text(
    "SELECT id FROM config WHERE tenant_id = :tenant AND kind = 'agent' AND body->'teams' ? :team AND id <> :primary"
)
# The conversation another one came from, as a reply there on another channel or address opened it.
_CAME = text(
    "SELECT c.id, c.state FROM assignment t JOIN assignment s ON s.id = t.from_assignment JOIN conversation c"
    " ON c.id = s.conversation_id WHERE t.conversation_id = :id AND c.id <> :id LIMIT 1"
)
# Plivo's reports come late and repeated: a status only moves forward, and to failed or undelivered only from pending,
# queued or sent.
_STATUS = text(
    "UPDATE entry SET status = :to, provider_id = coalesce(provider_id, :uuid)"
    " FROM (SELECT ARRAY['pending', 'queued', 'sent', 'delivered', 'read', 'deleted'] AS r) v"
    " WHERE id = :id AND tenant_id = :tenant AND kind = 'message'"
    " AND (CAST(:uuid AS text) IS NULL OR provider_id IS NULL OR provider_id = :uuid)"
    " AND CASE WHEN :to IN ('failed', 'undelivered') THEN status IN ('pending', 'queued', 'sent')"
    " ELSE array_position(v.r, status) < array_position(v.r, CAST(:to AS text)) END"
    " RETURNING id, conversation_id, agent_id, provider_id, at"
)
SKEW = timedelta(minutes=5)


class AttachmentIn(m.File):
    url: Annotated[str, Field(pattern=r"^https://\S+$", max_length=2000)]
    size: int | None = Field(None, ge=0, description="Bytes. An SMS's text and files come to at most 5,000,000.")


class Content(m.Body):
    text: Annotated[str, StringConstraints(strip_whitespace=True, max_length=10_000)] | None = None
    attachments: list[AttachmentIn] = Field([], max_length=10)


class AgentEmail(m.Body):
    subject: Header | None = None
    to: Addresses = Field([], description="To recipients beside the customer.")
    cc: Addresses = []
    bcc: Addresses = []
    reply_to: Addresses = []


class EmailIn(AgentEmail):
    message_id: Header | None = None
    in_reply_to: Header | None = None
    references: list[Header] = Field([], max_length=100)


class Parties(m.Body):
    channel: Literal["chat", "sms", "whatsapp", "email"]
    from_: Address = Field(alias="from")
    to: Address

    @model_validator(mode="after")
    def _addresses(self):
        if self.channel in ("sms", "whatsapp"):
            self.from_, self.to = (a and m.address(self.channel, a) for a in (self.from_, self.to))
        if getattr(self, "email", None) and self.channel != "email":
            raise ValueError("email fields are for the email channel only")
        return self


class Inbound(Parties, Content):
    """A customer message from the tenant's gateway."""

    email: EmailIn | None = None
    provider_id: str | None = Field(None, min_length=1, max_length=200)
    sent_at: datetime | None = Field(None, description="When the customer sent it, if the gateway knows.")


class TextEnqueue(Inbound):
    context: dict | None = None
    agents: list[UUID] = []
    route: m.EnqueueRoute | None = None


Title = Annotated[str, Field(min_length=1, max_length=200)]


class Choice(m.Body):
    title: Title
    description: Title | None = None


class Section(m.Body):
    title: Title | None = None
    rows: list[Choice] = Field(min_length=1, max_length=10)


class Interactive(m.Body):
    """Reply buttons, or a list behind a `button`; HS gives each button and row its id."""

    type: Literal["button", "list"]
    body: str = Field(min_length=1, max_length=1024)
    header: Title | AttachmentIn | None = Field(None, description="Text, or on reply buttons a file's URL.")
    buttons: list[Choice] = Field([], max_length=3)
    button: Title | None = None
    sections: list[Section] = Field([], max_length=10)

    @model_validator(mode="after")
    def _shape(self):
        media = isinstance(self.header, AttachmentIn)
        if not (self.buttons if self.type == "button" else self.button and self.sections and not media):
            raise ValueError("reply buttons need buttons; a list needs its button and sections, and a text header")
        return self


class Location(m.Body):
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)


class AgentMessage(Content):
    template: dict | None = Field(None, description="WhatsApp only, sent to Plivo as given once checked.")
    interactive: Interactive | None = Field(None, description="WhatsApp only.")
    location: Location | None = Field(None, description="WhatsApp only.")
    email: AgentEmail | None = None
    reply_to: UUID | None = Field(None, description="The message of this conversation it answers.")


class Reply(AgentMessage):
    channel: Literal["chat", "sms", "whatsapp", "email"] | None = None
    to: Address | None = Field(None, description="The customer's address on `channel`, else the conversation's.")
    from_: Address | None = Field(
        None, alias="from", description="A sender registered with PUT /numbers, else the conversation's address."
    )


class Start(AgentMessage, Parties):
    channel: Literal["sms", "whatsapp", "email"]
    from_: Address | None = Field(None, alias="from", description="Else the tenant's default_from for the channel.")
    agent_id: UUID
    context: dict | None = None

    @model_validator(mode="after")
    def _mailboxes(self):
        """An email goes from and to addresses, bare or as `Name <local@domain>`; HS keeps the bare one."""
        if self.channel == "email":
            self.from_, self.to = (a and m.address("email", a) for a in (self.from_, self.to))
        if self.channel == "whatsapp" and self.template is None:
            raise ValueError("a WhatsApp start needs a template")
        return self


class Message(BaseModel):
    id: str
    conversation_id: str
    direction: Literal["inbound", "outbound"]
    channel: str
    agent_id: str | None
    text: str | None
    attachments: list[catalog.Attachment]
    email: dict | None
    template: dict | None
    whatsapp: dict | None = Field(description="A WhatsApp's content beyond text: a tap, a pick, buttons, a location.")
    reply_to: str | None
    provider_id: str | None
    status: str
    created_at: datetime


class Started(BaseModel):
    conversation_id: str


class StatusIn(m.Body):
    status: Literal["queued", "sent", "delivered", "read", "deleted", "undelivered", "failed"]
    error_code: str | None = Field(None, max_length=100)
    error_title: str | None = Field(None, max_length=200)
    error_details: str | None = Field(None, max_length=1000)
    reported_at: datetime | None = Field(None, description="When the provider saw the status; else when HS heard.")


class TextTransferIn(m.Body):
    target: m.TextTarget
    mode: Literal["cold", "warm"] = "cold"


class TextParticipantIn(m.Body):
    target: m.AgentTarget


def signature(eid, n, exp: int) -> str:
    return hmac.new(settings.secret_key.encode(), f"message-media.{eid}.{n}.{exp}".encode(), hashlib.sha256).hexdigest()


def _file(f: dict, url: str | None) -> dict:
    return {k: v for k, v in f.items() if k != "hosted"} | {"url": url}


def message(e: Row) -> dict:
    """The read model of a message entry. A Plivo-hosted file shows as a signed media URL, valid MEDIA_TTL seconds;
    its Plivo URL never leaves HS."""
    b, exp, media = e.body, int(time.time()) + MEDIA_TTL, f"{settings.public_base_url}{PUBLIC}/messages/{e.id}/media"
    files = [
        _file(f, f"{media}/{n}?exp={exp}&sig={signature(e.id, n, exp)}" if f.get("hosted") else f["url"])
        for n, f in enumerate(b["attachments"])
    ]
    shown = {k: b.get(k) for k in ("direction", "channel", "text", "email", "template", "whatsapp", "reply_to")}
    ids = {"id": str(e.id), "conversation_id": str(e.conversation_id), "agent_id": e.agent_id and str(e.agent_id)}
    return shown | ids | {"attachments": files, "provider_id": e.provider_id, "status": e.status, "created_at": e.at}


def _stored(db: Connection, tenant_id, ours: str, provider_id: str | None) -> Row | None:
    """The message already stored under this gateway or Plivo id at our address `ours`: a sender mints its ids, so
    two of our addresses may each get one."""
    E, C = T.entry, T.conversation
    if provider_id is None:
        return None
    at = (E.c.tenant_id == tenant_id, E.c.provider_id == provider_id, E.c.kind == "message", C.c.ours == ours)
    return db.execute(select(E).join(C, C.c.id == E.c.conversation_id).where(*at)).first()


def seen(db: Connection, tenant_id, ours: str, provider_id: str | None) -> Row | None:
    """`_stored`, read once every other delivery of the id there has committed or rolled back."""
    if provider_id is not None:
        db.execute(config._SERIAL, {"key": f"message:{tenant_id}:{ours}:{provider_id}"})
    return _stored(db, tenant_id, ours, provider_id)


def find(db: Connection, tenant_id, msg: dict) -> Row | None:
    C, E = T.conversation, T.entry
    mail = msg.get("email") or {}
    refs = [r for r in (*mail.get("references", ()), mail.get("in_reply_to")) if r]
    thread = select(E.c.conversation_id).where(
        E.c.tenant_id == tenant_id, E.c.kind == "message", E.c.body["email"]["message_id"].astext.in_(refs)
    )
    line = (C.c.tenant_id == tenant_id, C.c.state.in_(cv.LIVE), C.c.channel == msg["channel"], C.c.ours == msg["to"])
    for match in ([C.c.id.in_(thread)] if refs else []) + [C.c.customer == msg["from"]]:
        cid = db.execute(select(C.c.id).where(*line, match)).scalar()
        if cid and (conv := cv.lock(db, cid)).state in cv.LIVE:
            return conv
    return None


def opened(db: Connection, tenant_id, msg: dict, context: dict | None = None) -> Row:
    """A new conversation for the message, locked, for the caller to queue; or the one a racing delivery opened."""
    parties = {"customer": msg["from"], "ours": msg["to"]}
    conv = cv.open(db, tenant_id, channel=msg["channel"], direction="inbound", context=context, **parties)
    return cv.lock(db, conv.id)


def received(db: Connection, conv: Row, msg: dict) -> Row | None:
    """Store a customer message and tell of it; None when it carries nothing or its provider id was seen."""
    files = msg.get("attachments") or []
    if not (msg.get("text") or files or msg.get("email") or msg.get("whatsapp")):
        return None
    quoted = _stored(db, conv.tenant_id, conv.ours, msg.get("quoted"))
    content = {"channel": msg["channel"], "text": msg.get("text"), "email": msg.get("email")}
    content |= {"whatsapp": msg.get("whatsapp"), "reply_to": quoted and str(quoted.id)}
    inbound = content | {"direction": "inbound", "attachments": files}
    cols = {"provider_id": msg.get("provider_id"), "status": "received", "at": msg.get("sent_at")}
    if e := cv.entry(db, conv, "message", inbound, **cols):
        shown = [_file(f, None if f.get("hosted") else f["url"]) for f in files]
        data = content | {"message_id": str(e.id), "from": msg["from"], "to": msg["to"], "attachments": shown}
        events.emit(db, "message.received", conv.tenant_id, data, conversation_id=conv.id)
    return e


def _thread(db: Connection, conv: Row, mail: AgentEmail | None) -> dict:
    """The agent's email fields plus the headers that thread it: a new Message-ID at the conversation's domain, in
    reply to the customer's last email."""
    E = T.entry
    header = E.c.body["email"]
    theirs = (E.c.conversation_id == conv.id, E.c.status == "received", header["message_id"].astext.isnot(None))
    prior = db.execute(select(header).where(*theirs).order_by(E.c.at.desc()).limit(1)).scalar() or {}
    refs = [*prior.get("references", []), prior["message_id"]] if prior else []
    ids = {"message_id": f"<{uuid.uuid4()}@{conv.ours.rpartition('@')[2]}>", "in_reply_to": prior.get("message_id")}
    return (mail.model_dump() if mail else {}) | ids | {"references": refs}


def _check_sms_size(db: Connection, conv: Row, body: AgentMessage, first: bool) -> None:
    started_by_agent = (conv.channel, conv.direction) == ("sms", "outbound")
    if started_by_agent and not first and db.execute(_CAME, {"id": conv.id}).first() is None:
        return
    if sum(f.size or 0 for f in body.attachments) + len((body.text or "").encode()) > SMS_BYTES:
        raise ApiError(422, "message_too_large", "an SMS's text and files come to at most 5 MB")


def _whatsapp(body: AgentMessage) -> dict | None:
    if loc := body.location:
        return {"type": "location", "location": {"latitude": f"{loc.latitude:f}", "longitude": f"{loc.longitude:f}"}}
    if not (i := body.interactive):
        return None

    def ids(choices: list) -> list[dict]:
        return [{"id": str(uuid.uuid4())} | c.model_dump(exclude_none=True) for c in choices]

    action = {"buttons": ids(i.buttons)}
    if i.type == "list":
        action = {"buttons": [{"id": str(uuid.uuid4()), "title": i.button}]}
        action["sections"] = [{"title": s.title, "rows": ids(s.rows)} for s in i.sections]
    kind = "media" if isinstance(i.header, AttachmentIn) else "text"
    header = {"header": {"type": kind, kind: getattr(i.header, "url", i.header)}} if i.header else {}
    return {"type": "interactive", "interactive": {"type": i.type, "body": {"text": i.body}, "action": action} | header}


def send(db: Connection, conv: Row, body: AgentMessage, channel: str, agent_id, *, first: bool = False, wait=True):
    """An agent message on `channel`, from the conversation's address to its customer: stored pending and told as
    message.sent, for Plivo delivery once Plivo took it. Returns it and, for Plivo delivery, the send's command key for
    the caller to wait on."""
    E, whatsapp = T.entry, body.template or body.interactive or body.location
    if (whatsapp and channel != "whatsapp") or (body.email and channel != "email"):
        raise ApiError(422, "content_not_for_channel", "templates, interactive and locations are whatsapp's")
    if not (body.text or body.attachments or whatsapp):
        raise ApiError(422, "empty_message", "a message needs text, attachments or whatsapp content")
    if channel == "sms":
        _check_sms_size(db, conv, body, first)
    mine = (E.c.id == body.reply_to, E.c.conversation_id == conv.id, E.c.kind == "message")
    if body.reply_to and db.execute(select(E.c.id).where(*mine)).first() is None:
        raise ApiError(422, "foreign_id", "reply_to names no message of this conversation")
    files = [a.model_dump(exclude_none=True) for a in body.attachments]
    out = {"channel": channel, "text": body.text, "attachments": files, "template": body.template}
    out |= {"whatsapp": _whatsapp(body), "reply_to": body.reply_to and str(body.reply_to)}
    out["email"] = _thread(db, conv, body.email) if channel == "email" else None
    e = cv.entry(db, conv, "message", out | {"direction": "outbound"}, agent_id=agent_id, status="pending")
    delivery = getattr(config.tenant_settings(db, conv.tenant_id).text.delivery, channel, "external")
    data = out | {"message_id": str(e.id), "from": conv.ours, "to": conv.customer, "delivery": delivery}
    data["by_agent_id"] = agent_id
    if delivery == "external":
        events.emit(db, "message.sent", conv.tenant_id, data, conversation_id=conv.id)
        return e, None
    args = {"src": conv.ours, "dst": conv.customer, "type": "mms" if channel == "sms" and files else channel}
    optional = (("text", body.text), ("media_urls", [f["url"] for f in files]), ("template", body.template))
    args |= {k: v for k, v in optional if v}
    if w := out["whatsapp"]:
        args[w["type"]] = w[w["type"]]
    payload = {"tenant_id": str(conv.tenant_id), "conversation_id": str(conv.id), "entry_id": str(e.id)}
    payload["claim"] = access.claimed()
    return e, command(db, "send", str(e.id), payload | {"first": first, "args": args, "told": data}, wait=wait)


def verdict(key: str | None) -> None:
    """Plivo's answer to a send the caller waits for: a refusal is 422 message_refused; a rate limit or an outage
    keeps its own error. An unknown outcome is no error: delivery reports tell the rest."""
    try:
        answered(key)
    except Refused as refused:
        raise answer(refused) from None


def answer(refused: ApiError) -> ApiError:
    """What a send the caller waits for answers when Plivo refuses it."""
    if refused.code != "plivo_refused":
        return refused
    return ApiError(422, "message_refused", "Plivo refused the message", refused.details)


def status(db: Connection, tenant_id, eid, to: str, provider_id=None, reported=None, **error):
    """Move an outbound message's status forward and tell of it, with its `error_code`, `error_title` and
    `error_details`; Plivo's id fills one the send never learned."""
    if e := db.execute(_STATUS, {"id": eid, "tenant": tenant_id, "to": to, "uuid": provider_id}).first():
        now = datetime.now(UTC)
        at = reported if reported and e.at - SKEW <= reported <= now + SKEW else now
        data = {"message_id": str(e.id), "status": to, "provider_id": e.provider_id, "reported_at": at} | error
        events.emit(
            db, "message.status_changed", tenant_id, data, conversation_id=e.conversation_id, agent_id=e.agent_id
        )
        if to in ("delivered", "read"):
            cv.reached(db, e.conversation_id)


def _text(db: Connection, p: Principal, cid: UUID) -> Row:
    """The text conversation, locked, when `p` is a tenant key or an agent active on it."""
    conv = access.conversation(db, p, cid, lock=True, control=True)
    if conv.channel not in CHANNELS:
        raise ApiError(409, "not_text", "this route is for chat, SMS, WhatsApp and email conversations")
    return conv


@router.get("/conversations/{conversation_id}/messages", response_model=Page[Message])
def list_messages(conversation_id: UUID, p: Principal = READ, paged: Paging = Depends(paging)):
    """Oldest first. An agent's read of the last page clears the conversation's unread count."""
    E = T.entry
    written = func.coalesce(E.c.written_at, E.c.at)  # breaks a tie on `at`: one transaction's entries share it
    with read() as db:
        conv = access.conversation(db, p, conversation_id, offer=True)
        mine = (E.c.conversation_id == conv.id, E.c.kind == "message")
        q = select(E).where(*mine)
        if paged.after is not None:
            after = config.as_uuid(paged.after)
            if (seen := after and db.execute(select(E.c.at, written).where(E.c.id == after, *mine)).first()) is None:
                raise ApiError(422, "invalid_cursor", "cursor is not one this API returned")
            q = q.where(tuple_(E.c.at, written, E.c.id) > tuple_(*seen, after))
        rows = db.execute(q.order_by(E.c.at, written, E.c.id).limit(paged.limit + 1)).all()
    if len(rows) <= paged.limit:
        cv.inbox.read_by(p, conv)
    return page([message(e) for e in rows], paged)


@router.post("/conversations/{conversation_id}/messages", status_code=201, response_model=Message)
def reply(conversation_id: UUID, body: Reply, p: Principal = WRITE):
    """An agent's message while its own assignment is active, or files alone in its wrap-up (409 in_wrap_up for text),
    or a tenant key's as the primary agent's (409 not_live without one). `channel`, `to` and `from` move it to another
    line: SMS, WhatsApp or email to the customer, or to another address (a call names one; 422 channel_not_allowed for
    none). Its family is the call or text conversation the switches started from and every one linked from it. The
    message goes in the family's conversation on that line that the agent holds, else in a new linked one the agent
    holds, which takes a slot on the channel (409 agent_at_capacity; 409 conversation_open while another is open on
    the line; a new WhatsApp one needs a template, 422 template_required). The family writes on a channel from one
    sender (422 sender_mismatch), and a switch's sender is registered with PUT /numbers (422 from_not_allowed). In
    wrap-up only files to the customer's own address go, and they stay here; any other switch is 409 in_wrap_up.
    Plivo's refusal is 422 message_refused. A retry sends once only when it reuses its Idempotency-Key."""
    A = T.assignment
    if body.template:
        with read() as db:
            channel, _, ours = _reply_line(db, access.conversation(db, p, conversation_id), body)
        if channel == "whatsapp":
            templates.check(p.tenant_id, ours, body.template)
    with tx() as db:
        root = (body.channel or body.to or body.from_) and _hold(db, p.tenant_id, conversation_id)
        conv = access.conversation(db, p, conversation_id, lock=True)
        mine = (A.c.conversation_id == conv.id, A.c.agent_id == p.agent_id, A.c.state.in_(("active", "wrap_up")))
        if p.agent_id:
            stint = db.execute(select(A).where(*mine).order_by(A.c.state == "wrap_up")).first()
            if stint is None:
                raise ApiError(409, "not_on_conversation", "the agent holds no assignment on this conversation")
        elif (stint := cv.primary_agent(db, conv.id)) is None:
            raise ApiError(409, "not_live", "no agent holds this conversation")
        line, first = _reply_line(db, conv, body), False
        if body.text and stint.state == "wrap_up":
            raise ApiError(409, "in_wrap_up", "an agent in wrap-up may send files, not text")
        if line != (conv.channel, conv.customer, conv.ours):
            conv, first = _switch(db, conv, stint, line, body, root)
        e, key = send(db, conv, body, line[0], str(stint.agent_id), first=first)
        access.created(db, str(e.id))
        sent = message(e)
    verdict(key)
    return sent


def _takers(db: Connection, conv: Row, agents: list, *, direct: bool) -> list[UUID]:
    """The agents that are available with a free slot on the channel, read after their presence rows are
    share-locked in agent order; each offer then takes its slot, so two offers cannot both take a last slot. A direct
    agent short of either is 409."""
    db.execute(routing._LOCK, {"ids": agents})
    args = {"ids": agents, "channel": conv.channel, "full": hot.full(conv.tenant_id, conv.channel)}
    rows = db.execute(_LOAD, args).all()
    if direct and not (rows and rows[0].available):
        raise ApiError(409, "agent_unavailable", "the agent is not available")
    if direct and not rows[0].room:
        raise ApiError(409, "agent_at_capacity", "the agent has no free slot on this channel")
    return [r.agent_id for r in rows if r.available and r.room]


def _may_start(db: Connection, p: Principal, agent_id: UUID) -> None:
    if p.kind == "agent" and not config.tenant_settings(db, p.tenant_id).agent_direct_dial:
        raise ApiError(403, "dial_requires_server", "agents start conversations only with agent_direct_dial on")
    if p.kind == "agent" and p.agent_id != str(agent_id):
        raise ApiError(403, "forbidden", "an agent starts conversations only for itself")


@router.post("/conversations", status_code=201, response_model=Started)
def start(body: Start, p: Principal = WRITE):
    """An agent's SMS, WhatsApp or email to a customer, under POST /calls' rules: an agent token starts one only for
    itself and only with agent_direct_dial on. The agent holds a slot on the channel from the first message, and a
    first message Plivo refuses closes it (422 message_refused)."""
    if body.channel == "whatsapp":
        with read() as db:
            _may_start(db, p, body.agent_id)
        templates.check(p.tenant_id, body.from_, body.template)
    with tx() as db:
        _may_start(db, p, body.agent_id)
        config.owned(db, p, "agent", [body.agent_id])
        ours = body.from_ or config.tenant_settings(db, p.tenant_id).default_from.get(body.channel)
        if not ours or config.by_key(db, p.tenant_id, "number", ours) is None:
            raise ApiError(422, "from_not_allowed", "from must be a number or address registered with PUT /numbers")
        parties = {"customer": body.to, "ours": ours, "context": body.context}
        conv = cv.open(db, p.tenant_id, channel=body.channel, direction="outbound", agent_id=body.agent_id, **parties)
        if conv.state != "open":
            raise ApiError(409, "conversation_open", "this customer already has an open conversation on this line")
        _takers(db, conv, [body.agent_id], direct=True)
        cv.offer(db, conv, body.agent_id, via="direct")
        cv.arm(db, conv)
        _, key = send(db, conv, body, body.channel, str(body.agent_id), first=True)
        access.created(db, str(conv.id))
    verdict(key)
    return {"conversation_id": str(conv.id)}


def _removed(db: Connection, a: Row) -> None:
    who = {"identifier": str(a.agent_id), "alias": None}
    data = {"participant_id": str(a.transfer_id or a.id), "participant": who, "role": a.role}
    cv.emit_event(db, "conversation.participant_removed", a.tenant_id, a.conversation_id, data, a.agent_id)


def _line(db: Connection, conv: Row, which: str, agent_id=None) -> None:
    """The queue's `which` line for the conversation's channel to the customer, as a message from no agent; its first
    `{agent_name}` names `agent_id`, title-cased."""
    t = str(conv.tenant_id)
    q = conv.queue_id and config.get(db, t, "queue", conv.queue_id)
    if (lines := q and q["text"]["messages"].get(conv.channel)) and (line := lines.get(which)):
        name = agent_id and config.get(db, t, "agent", agent_id)["name"]
        text = line.replace("{agent_name}", (name or "").title(), 1)
        send(db, conv, AgentMessage(text=text), conv.channel, None, wait=False)


def _taken(db: Connection, conv: Row, a: Row) -> None:
    if a.state == "active" and a.via == "queue" and conv.answered_at is None:
        _line(db, conv, "assigned", a.agent_id)


def _leaving(db: Connection, conv: Row) -> None:
    _line(db, conv, "closed")
    for a in cv.by_agent(cv.rows(db, conv.id, ("active",))):
        if a.role == "secondary" and a.via in ("participant", "transfer"):
            _removed(db, a)


@router.post("/conversations/{conversation_id}/close", status_code=204)
def close(conversation_id: UUID, p: Principal = WRITE) -> Response:
    """End a text conversation: its offers are withdrawn as cancelled, its agents go to wrap-up, and it closes as
    agent_closed after their wrap-up, or as cancelled when no agent ever took it. A second agent or a warm transfer's
    joined target is told it was removed."""
    with tx() as db:
        conv = _text(db, p, conversation_id)
        _leaving(db, conv)
        cv.gone(db, conv, "agent_closed" if conv.answered_at else "cancelled", why="cancelled")
    return Response(status_code=204)


@router.post("/conversations/{conversation_id}/transfers", status_code=201, response_model=m.TransferOut)
def transfer(conversation_id: UUID, body: TextTransferIn, p: Principal = WRITE):
    """The agent, or each team member, that is available with a free slot gets an offer (an agent that is not is
    409); an agent or team transfer reads `transferring` until it ends. A cold transfer's first accept becomes the
    primary and the current one goes to wrap-up, and keeps it if none accepts. A warm one's target joins as a second
    agent until the transfer completes. A queue takes it now. An agent or team transfer needs a mode transfer.modes allows, else
    409 transfer_mode_not_allowed."""
    t, tid = body.target, uuid.uuid4()
    with tx() as db:
        conv = _text(db, p, conversation_id)
        if (primary := cv.primary_agent(db, conv.id)) is None:
            raise ApiError(409, "not_live", "no agent holds this conversation")
        if t.type == "queue":
            config.owned(db, p, "queue", [t.queue_id])
            config.owned(db, p, "agent", t.agents)
            cv.move(db, primary, "leave", reason="transferred")
            routing.enqueue(db, conv, t.queue_id, agents=t.agents)
            moved = {"transfer_id": tid, "from_assignment": primary.id, "mode": body.mode}
            moved |= {"channel": conv.channel, "tenant_id": conv.tenant_id, "conversation_id": conv.id}
            cv.transferred(db, cv.Offer(**moved), "initiated", target=t.model_dump(mode="json"))
            return {"transfer_id": str(tid)}
        if config.tenant_settings(db, p.tenant_id).transfer.modes not in ("both", body.mode):
            raise ApiError(409, "transfer_mode_not_allowed", "the tenant's transfer modes do not allow this transfer")
        team = None
        if t.type == "agent":
            config.owned(db, p, "agent", [t.agent_id])
            agents = _takers(db, conv, [t.agent_id], direct=True)
        else:
            config.owned(db, p, "team", [t.team_id])
            args = {"tenant": p.tenant_id, "team": str(t.team_id), "primary": primary.agent_id}
            agents = _takers(db, conv, db.execute(_MEMBERS, args).scalars().all(), direct=False)
            team = t.team_id
        offer = {"via": "transfer", "mode": body.mode, "transfer_id": tid, "team": team}
        limit = config.tenant_settings(db, p.tenant_id).simultaneous_agent_limit
        if not (offers := list(islice(filter(None, (cv.offer(db, conv, a, **offer) for a in agents)), limit))):
            raise ApiError(409, "no_agent_available", "no member of that team can take it now")
        cv.transferred(db, offers[0], "initiated")
    return {"transfer_id": str(tid)}


def _target(db: Connection, p: Principal, conversation_id: UUID, tid: UUID, leave: bool = False) -> tuple[Row, list]:
    """To a caller that no longer holds the conversation, a transfer already handed over is an empty list, not a 404."""
    conv = access.conversation(db, p, conversation_id, lock=True)
    every = cv.rows(db, conv.id, ("offered", "active"))
    if not (mine := [a for a in every if a.role == "secondary" and a.transfer_id == tid]):
        return conv, mine
    owners = [a for a in every if a.id == mine[0].from_assignment] + [a for a in mine if leave and a.state == "active"]
    if p.kind == "agent" and p.agent_id not in {str(a.agent_id) for a in owners}:
        raise ApiError(403, "not_conversation_owner", "only the conversation's agent or a tenant key may do this")
    return _text(db, p, conversation_id), mine


@router.post("/conversations/{conversation_id}/transfers/{transfer_id}/cancel", status_code=204)
def cancel_transfer(conversation_id: UUID, transfer_id: UUID, p: Principal = WRITE) -> Response:
    """Withdraw a transfer's open offers and end its warm target, which is told it was removed; 409 once the target has
    taken the conversation. Its joined warm target may cancel it too, and leaves."""
    with tx() as db:
        _, mine = _target(db, p, conversation_id, transfer_id, leave=True)
        if not mine:
            raise ApiError(409, "transfer_not_pending", "the transfer is not pending")
        for a in cv.by_agent(mine):
            if a.state == "active":
                _removed(db, a)
            cv.move_out(db, a, "cancelled")
        cv.transferred(db, mine[0], "cancelled")
    return Response(status_code=204)


@router.post("/conversations/{conversation_id}/transfers/{transfer_id}/complete", status_code=204)
def complete_transfer(conversation_id: UUID, transfer_id: UUID, p: Principal = WRITE) -> Response:
    """The warm target that joined becomes the primary and the current agent goes to wrap-up; 409 before it joined."""
    with tx() as db:
        conv, mine = _target(db, p, conversation_id, transfer_id)
        if not (joined := next((a for a in mine if a.state == "active"), None)) or not cv.promote(db, conv, joined):
            raise ApiError(409, "transfer_not_ready", "no target has joined the conversation")
    return Response(status_code=204)


@router.post("/conversations/{conversation_id}/participants", status_code=201, response_model=m.ParticipantOut)
def add_participant(conversation_id: UUID, body: TextParticipantIn, p: Principal = WRITE):
    """A second agent, available with a free slot (else 409): offered, and once it accepts it holds a slot and may
    reply while the primary stays."""
    with tx() as db:
        conv = _text(db, p, conversation_id)
        config.owned(db, p, "agent", [body.target.agent_id])
        _takers(db, conv, [body.target.agent_id], direct=True)
        a = cv.offer(db, conv, body.target.agent_id, role="secondary", via="participant")
    return {"participant_id": str(a.id)}


@router.delete("/conversations/{conversation_id}/participants/{participant_id}", status_code=204)
def remove_participant(conversation_id: UUID, participant_id: UUID, p: Principal = WRITE) -> Response:
    """The second agent leaves, to wrap-up if it is on; an open offer is withdrawn."""
    with tx() as db:
        conv = _text(db, p, conversation_id)
        theirs = (a for a in cv.rows(db, conv.id) if (a.id, a.via) == (participant_id, "participant"))
        if (a := next(theirs, None)) is None:
            raise ApiError(404, "participant_not_found", "no such participant")
        cv.move(db, a, "withdraw" if a.state == "offered" else "leave", reason="cancelled")
        if a.state == "active":
            _removed(db, a)
    return Response(status_code=204)


@router.post(
    "/messages", status_code=201, response_model=Message, responses={200: {"description": "a repeated provider_id"}}
)
def post_message(body: Inbound, response: Response, p: Principal = GATEWAY):
    """A customer message from the tenant's gateway, on the open conversation for its (channel, from, to) or on its
    email thread. It never opens one (404 no_open_conversation). A repeated provider_id answers 200 with the message
    stored first, and tells of nothing."""
    msg = body.model_dump(by_alias=True)
    if not (body.text or body.attachments or body.email):
        raise ApiError(422, "empty_message", "a message needs text, attachments or email fields")
    with tx() as db:
        if e := seen(db, p.tenant_id, body.to, body.provider_id):
            response.status_code = 200
            return message(e)
        if (conv := find(db, p.tenant_id, msg)) is None:
            raise ApiError(404, "no_open_conversation", "no open conversation for this customer and address")
        return message(received(db, conv, msg) or seen(db, p.tenant_id, body.to, body.provider_id))


@router.post("/messages/{message_id}/status", status_code=204)
def post_status(message_id: UUID, body: StatusIn, p: Principal = GATEWAY) -> Response:
    """A delivery status from the tenant's gateway for an agent message; one that is not forward changes nothing."""
    E = T.entry
    with tx() as db:
        ours = (E.c.id == message_id, E.c.tenant_id == p.tenant_id, E.c.kind == "message", E.c.status != "received")
        if db.execute(select(E.c.id).where(*ours)).first() is None:
            raise ApiError(404, "message_not_found", "no such agent message")
        error = body.model_dump(include={"error_code", "error_title", "error_details"})
        status(db, p.tenant_id, message_id, body.status, None, body.reported_at, **error)
    return Response(status_code=204)


@jobs.handler("session_timeout")
def _session_timeout(job: Row) -> None:
    """A run whose row an accept re-armed after the claim (new rev) is stale and does nothing."""
    J = T.jobs
    with tx() as db:
        conv = cv.lock(db, UUID(job.payload["conversation_id"]))
        rev = db.execute(select(J.c.rev).where(J.c.id == job.id)).scalar()
        if not conv or conv.state == "queued" or rev != job.rev:
            return
        came = db.execute(_CAME, {"id": conv.id}).first()
        if not job.payload.get("extended") and came and came.state != "closed":
            cv.arm(db, conv, extended=True)
        else:
            ended(db, conv, "session_timeout")


def ended(db: Connection, conv: Row, result: str) -> None:
    """The conversation closes as `result` with no wrap-up; its agents' stints end and their slots free."""
    _leaving(db, conv)
    for a in cv.by_agent(cv.rows(db, conv.id, ("active",))):
        cv.move(db, a, "end", reason=result)
    cv.gone(db, conv, result)


def _join(db: Connection, p: Principal, raw: dict) -> tuple[Row, TextEnqueue]:
    """The text enqueue's half here: the conversation its message joins, or a new one for the route to queue."""
    body = config.validate_model(TextEnqueue, raw)
    msg = body.model_dump(by_alias=True)
    if e := seen(db, p.tenant_id, body.to, body.provider_id):
        return cv.lock(db, e.conversation_id), body
    conv = find(db, p.tenant_id, msg) or opened(db, p.tenant_id, msg, body.context)
    received(db, conv, msg)
    return conv, body


def _replayed(db: Connection, p: Principal, rid: str) -> dict:
    E = T.entry
    e = db.execute(select(E).where(E.c.id == UUID(rid), E.c.tenant_id == p.tenant_id)).one()
    access.conversation(db, p, e.conversation_id)
    return jsonable_encoder(message(e))


access.VIEWS[f"POST {PUBLIC}/conversations"] = lambda db, p, rid: {
    "conversation_id": str(access.conversation(db, p, UUID(rid)).id)
}
access.VIEWS[f"POST {PUBLIC}/conversations/{{conversation_id}}/messages"] = _replayed
_body = TextEnqueue.model_json_schema(ref_template="#/components/schemas/{model}")
routing.BODIES.append({k: v for k, v in _body.items() if k != "$defs"})  # its parts are POST /messages' components
for _channel in CHANNELS:
    # A text offer is its offer.created event: there is no leg to ring.
    routing.CHANNELS[_channel] = routing.Channel(
        open=_join, ring=lambda db, conv, offers: None, ended=_taken, queued=lambda db, conv: _line(db, conv, "waiting")
    )


from hs.text.switch import _hold, _reply_line, _switch  # noqa: E402
