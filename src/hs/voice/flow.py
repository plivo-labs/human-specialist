"""The voice entry flow: the answer URL, a queue's enqueue URL, a number's open or closed action, IVR menus,
consent, voicemail, the caller's room in a queue, moves out of a room, and the hangup URL.

A repeated Plivo delivery answers the same XML and changes nothing, resolved by state under the conversation's row
lock: `open` is idempotent on the call's uuid, a queue action enqueues only an `open` conversation, a voicemail is
kept once per recording, a menu URL carries how many inputs the call had made (`n`), and an action emits its events
only when `fresh`."""

import json
from collections.abc import Callable
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from typing import Literal
from urllib.parse import urlencode
from uuid import UUID

from fastapi import APIRouter, Depends, Request, Response
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field
from sqlalchemy import func, insert, select, update
from sqlalchemy.engine import Connection, Row

from hs import access, config, conversations, events, jobs, plivo, routing
from hs.access import Principal
from hs.config import models as m
from hs.core import ApiError, T, capped, read, tx

router = APIRouter()  # under /api/v1
callbacks = APIRouter()  # under /plivo/{tenant_id}
HANGUP = {"type": "hangup"}
END = ("Hangup", {})
WAIT_TIMEOUT = "Member Wait Timeout Triggered"
SEATED = ("queued", "live")
# Plivo's own default is silence, which on hold sounds like a dropped call.
HOLD_MUSIC = "https://s3.amazonaws.com/plivocloud/music.mp3"
# Plivo can post a voicemail's Record action just after the call's hangup.
CLOSE_DELAY = 10
ANONYMOUS = "Anonymous"
WITHHELD = ("anonymous", "restricted", "1000000000")
INITIALIZED = 10  # seconds a seat's room has to say it is initialized before HS reads Plivo's view of it
ROOM_EVENTS = (
    "mpc-state-changes,participant-state-changes,add-participant-api-events,participant-audio-events,"
    "participant-digit-input-events"
)


FORM = plivo.FORM
TENANT = Depends(plivo.voice_signed)
# voice/outbound.py answers the agent legs it dials (query `c`) and agents' own dial-outs (From an endpoint), takes
# every status of its calls' legs (/hangup with `c`), and the room events of its calls the lead has not answered.
DIALED: dict[str, Callable] = {}


def to_int(value: str) -> int | None:
    return int(value) if value.isdigit() else None


def hangup_facts(form: dict, by: str) -> dict:
    """A leg's end, from a Call's hangup or a member's call status. A Call tells its StartTime, AnswerTime and EndTime
    in the account's zone, with no offset: it ended Duration whole seconds after SessionStart, its one UTC time."""
    seconds, start, end = form.get("Duration", ""), form.get("SessionStart"), plivo.utc(form.get("HangupTime"))
    if not end and start and seconds.isdigit():
        with suppress(ValueError):  # a SessionStart that does not parse leaves the end to HS's clock
            end = (datetime.fromisoformat(start).replace(tzinfo=UTC) + timedelta(seconds=int(seconds))).isoformat()
    facts = {"hangup_cause": form.get("HangupCauseName") or form.get("PlivoHangupCause") or None, "ended_by": by}
    facts["hangup_cause_code"] = to_int(form.get("HangupCauseCode") or form.get("PlivoHangupCauseCode") or "")
    return facts | {"duration_sec": to_int(seconds), "ended_at": end}


def emit(db: Connection, type_: str, conv: Row, agent_id=None, **data) -> None:
    events.emit(db, type_, str(conv.tenant_id), data, conversation_id=str(conv.id), agent_id=agent_id)


def _result(db: Connection, conv: Row, result: str) -> None:
    db.execute(update(T.conversation).where(T.conversation.c.id == conv.id).values(result=result))


def said(say: dict | None) -> list[tuple]:
    if not say:
        return []
    if say.get("audio_url"):
        return [("Play", {}, say["audio_url"])]
    return [("Speak", {"voice": say.get("voice"), "language": say.get("language")}, say["text"])]


def _consent(db: Connection, conv: Row) -> list[tuple]:
    notice = getattr(config.tenant_settings(db, str(conv.tenant_id)).consent, conv.direction)
    return said(notice and notice.model_dump())


def _call(db: Connection, tenant_id, form: dict, context: dict | None = None, **started) -> tuple[Row, bool]:
    """The call's conversation, opened on first sight and locked, and whether this request opened it."""
    inbound = form.get("Direction") != "outbound"
    them, us = plivo.e164(form["From" if inbound else "To"]), plivo.e164(form["To" if inbound else "From"])
    raw = form["From"] if inbound else ""
    withheld = raw.strip().lower() in WITHHELD
    parties = {"direction": "inbound" if inbound else "outbound", "customer": ANONYMOUS if withheld else them}
    parties |= {"ours": us, "number": us}
    kept = {"routing": {"caller_id": raw}} if withheld else {}
    row = conversations.open(
        db, tenant_id, channel="call", context=context, call_uuid=form["CallUUID"], **parties, **kept, **started
    )
    # A row this transaction inserted is its alone until COMMIT: it needs no lock.
    new = db.info["tx"].get("opened") == row.id
    return row if new else conversations.lock(db, row.id), new


def _found(db: Connection, tenant_id, form: dict) -> Row | None:
    return conversations.lock(db, tenant=tenant_id, call_uuid=form.get("CallUUID", ""))


def room(db: Connection, conv: Row, queue: dict | None) -> list[tuple]:
    """The caller's seat in its queue's multi-party call. Its name and callback URLs carry the room, so a callback
    from a room the caller has left is told apart. A queue with wait prompts stands in for the room's MPCInitialized
    if it never comes."""
    t, c, r, queue = str(conv.tenant_id), str(conv.id), conv.room, queue or {}
    if queue.get("wait_prompts"):
        seat = {"tenant_id": t, "conversation_id": c, "room": r}
        jobs.schedule(db, "mpc_initialized", seat, delay=INITIALIZED, key=f"{c}:{r}")
    rec = getattr(config.tenant_settings(db, t).recording, conv.direction)
    attrs = {
        "role": "customer",
        "waitTime": queue.get("wait_timeout_sec") or None,
        "startMpcOnEnter": False,
        "endMpcOnExit": True,
        "statusCallbackEvents": ROOM_EVENTS,
        "statusCallbackUrl": plivo.url(t, "mpc-status", c=c, r=r),
        "onExitActionUrl": plivo.url(t, "actions/queue-exit", r=r),
        "record": rec,
        "recordParticipantTrack": rec,
        "recordingCallbackUrl": plivo.url(t, "recording", c=c),
        "transcript": rec,
        "transcriptionUrl": plivo.url(t, "transcription", c=c),
    }
    music = queue.get("hold_music") or {}
    hold = [
        (f"{who.title()}HoldMusic", {}, ("Play", {}, music.get(who) or HOLD_MUSIC)) for who in ("customer", "agent")
    ]
    return [("MultiPartyCall", attrs, f"hs-{c}-{r}", *hold)]


def _menu(conv: Row, menu: dict, attempt: int, n: int) -> list[tuple]:
    """One attempt: the greeting inside GetDigits, then a redirect that reports no input."""
    url = plivo.url(str(conv.tenant_id), "actions/menu", menu_id=menu["id"], attempt=attempt, n=n)
    digits = {"action": url, "method": "POST", "numDigits": 1, "retries": 1, "timeout": menu["input_timeout_sec"]}
    return [("GetDigits", digits, *said(menu["greeting"])), ("Redirect", {"method": "POST"}, url)]


def _count(db: Connection, conv: Row, type_: str) -> int:
    """How many events of this type the conversation has: what a repeated delivery checks."""
    E = T.events
    mine = (E.c.conversation_id == str(conv.id), E.c.type == type_)
    return db.execute(select(func.count()).select_from(E).where(*mine)).scalar()


def _queue(db: Connection, conv: Row, action: dict, fresh: bool) -> list[tuple]:
    """Seat an open call in the queue, or run the queue's overflow when it is full; a seated call (a repeat) gets
    its room again."""
    t = str(conv.tenant_id)
    if conv.state in SEATED:
        return room(db, conv, config.get(db, t, "queue", conv.queue_id))
    if conv.state != "open" or (queue := config.get(db, t, "queue", action["queue_id"])) is None:
        return [END]
    try:
        routing.enqueue(db, conv, UUID(queue["id"]), agents=action.get("agents", []), hold=action.get("hold_position"))
    except routing.Full as e:
        return run(db, conv, e.action, fresh)
    return room(db, conv, queue)


def run(db: Connection, conv: Row, action: dict | None, fresh: bool) -> list[tuple]:
    """The XML of an action for this call; none hangs up."""
    a, t, cid = action or HANGUP, str(conv.tenant_id), str(conv.id)
    match a["type"]:
        case "queue":
            return _queue(db, conv, a, fresh)
        case "menu":
            menu = config.get(db, t, "menu", a["menu_id"])
            return _menu(conv, menu, 1, _count(db, conv, "conversation.menu_input")) if menu else [END]
        case "voicemail":
            done = plivo.url(t, "actions/voicemail", **a["mailbox"])
            record = {"action": done, "method": "POST", "maxLength": a["max_length_sec"], "playBeep": True}
            if a["transcribe"]:
                text = plivo.url(t, "transcription", c=cid)
                record |= {"transcriptionType": "auto", "transcriptionUrl": text, "transcriptionMethod": "POST"}
            return [*said(a["greeting"]), ("Record", record)]
        case "forward":
            _result(db, conv, "forwarded")
            return [("Dial", {}, ("Number", {}, a["number"].removeprefix("+")))]
        case "redirect":
            url = a["url"] + ("&" if "?" in a["url"] else "?") + urlencode({"conversation_id": cid})
            if fresh:
                emit(db, "conversation.redirected", conv, url=url)
            return [("Redirect", {"method": "POST"}, url)]
    return [*said(a.get("greeting")), END]


def _leave(db: Connection, conv: Row, action: dict) -> Row:
    """The caller left its room on Plivo's side, and this answer takes it to `action`: the move `depart`
    schedules is not sent."""
    moved = conversations.depart(db, conv, action)
    jobs.cancel(db, "plivo", f"redirect:{conv.tenant_id}:{conv.id}:{moved.room}")
    jobs.cancel(db, "queue_timeout", str(conv.id))
    return moved


def _stale(db: Connection, conv: Row) -> list[tuple]:
    """A request about a room the caller has left: a seated caller gets its room, else a move is on its way."""
    if conv.state in SEATED:
        return room(db, conv, config.get(db, str(conv.tenant_id), "queue", conv.queue_id))
    return [("Wait", {"length": 10})]


@callbacks.post("/answer")
def answer(c: UUID | None = None, tenant: Row = TENANT, form: dict = FORM, a: UUID | None = None) -> Response:
    """A call to one of the tenant's numbers: consent, then its entry point's open action, or in closed hours its
    closed one. A number with no entry point is hung up. Plivo's retry for a call seated meanwhile gets the same
    room, whatever the number or its hours say by then."""
    if c or form.get("From", "").startswith("sip:"):
        return DIALED["answer"](tenant, form, c, a)
    number = plivo.e164(form.get("To", ""))
    with tx() as db:
        if (seated := _found(db, tenant.id, form)) and seated.state in SEATED:
            return plivo.xml(*_consent(db, seated), *_queue(db, seated, {}, False))
        if (found := config.by_key(db, tenant.id, "number", number)) is None:
            return plivo.xml(END)
        entry_id, entry = found
        hours = entry["business_hours_id"] and config.get(db, tenant.id, "hours", entry["business_hours_id"])
        state = "open" if hours and config.is_open(hours) else "closed" if hours else "none"
        try:
            conv, new = _call(db, tenant.id, form, entry_point_id=entry_id, hours=state)
        except conversations.ForeignCall:
            return plivo.xml(END)
        action = (entry["closed_action"] or HANGUP) if state == "closed" else entry["open_action"]
        if new and state == "closed":
            _result(db, conv, "after_hours")
            emit(db, "conversation.after_hours", conv, number=number, action=action)
        return plivo.xml(*_consent(db, conv), *run(db, conv, action, new))


@callbacks.post("/actions/menu")
def menu(menu_id: UUID, attempt: int = 0, n: int = 0, tenant: Row = TENANT, form: dict = FORM) -> Response:
    """A menu's entry (its menu_url, where a tenant's app sends a call) or one attempt's input: a digit, or none."""
    with tx() as db:
        try:
            conv, new = _call(db, tenant.id, form)
        except conversations.ForeignCall:
            return plivo.xml(END)
        notice = _consent(db, conv) if new else []
        if (m := config.get(db, tenant.id, "menu", menu_id)) is None:
            return plivo.xml(*notice, END)
        if not attempt:
            return plivo.xml(*notice, *_menu(conv, m, 1, _count(db, conv, "conversation.menu_input")))
        digit = form.get("Digits", "")
        option = m["options"].get(digit)
        retry = option is None and attempt <= m["retries"]
        action = option or ({"type": "menu", "menu_id": str(menu_id)} if retry else m["invalid_action"] or HANGUP)
        if fresh := _count(db, conv, "conversation.menu_input") == n:
            heard = digit if option else "invalid" if digit else "no_input"
            data = {"menu_id": str(menu_id), "input": heard, "attempt": attempt, "action": action}
            emit(db, "conversation.menu_input", conv, **data)
        if retry:
            return plivo.xml(*notice, *_menu(conv, m, attempt + 1, n + 1))
        return plivo.xml(*notice, *run(db, conv, action, fresh))


@callbacks.post("/actions/voicemail")
def voicemail(
    queue_id: str | None = None, agent_id: str | None = None, tenant: Row = TENANT, form: dict = FORM
) -> Response:
    """A voicemail's recording ended: keep it on the conversation, once per recording, and hang up. A mailbox id
    counts only if it names the tenant's own queue or agent, and an agent's voicemail is told to that agent."""
    recording, seconds = form.get("RecordingID", ""), form.get("RecordingDuration", "")
    with tx() as db:
        if recording and (conv := _found(db, tenant.id, form)):
            queue = queue_id and config.get(db, tenant.id, "queue", queue_id)
            agent = agent_id and config.get(db, tenant.id, "agent", agent_id)
            mailbox = {"queue_id": queue and queue["id"], "agent_id": agent and agent["id"]}
            body = {"plivo_id": recording, "url": form.get("RecordUrl"), "mailbox": mailbox}
            body["duration"] = to_int(seconds)
            if kept := conversations.entry(db, conv, "voicemail", body, provider_id=recording):
                _result(db, conv, "voicemail")
                data = {"recording_id": str(kept.id), "mailbox": mailbox, "from": conv.customer}
                emit(db, "voicemail.received", conv, mailbox["agent_id"], **data)
    return plivo.xml(END)


@callbacks.post("/actions/move")
def move(r: int, a: str, tenant: Row = TENANT, form: dict = FORM) -> Response:
    """Plivo's fetch for a `redirect` command: the caller left its room for room `r` and action `a`."""
    with tx() as db:
        if (conv := _found(db, tenant.id, form)) is None or conv.state in ("wrap_up", "closed"):
            return plivo.xml(END)
        if r != conv.room:
            return plivo.xml(*_stale(db, conv))
        return plivo.xml(*run(db, conv, json.loads(a), conv.state == "open"))


@callbacks.post("/actions/queue-exit")
def queue_exit(r: int, tenant: Row = TENANT, form: dict = FORM) -> Response:
    """The caller left room `r` on Plivo's side. Plivo retries an exit whose answer it never got, and the exit has
    moved the caller on by then, so the first answer is kept per call and room and a retry gets it again."""
    K = T.idempotency
    kept = {"tenant_id": tenant.id, "principal": "plivo-exit"}
    kept["key_sha256"] = access.digest(f"{form.get('CallUUID', '')}:{r}")
    with tx() as db:
        conv = _found(db, tenant.id, form)
        if before := db.execute(select(K.c.body).where(*(K.c[k] == v for k, v in kept.items()))).scalar():
            return Response(before["xml"], media_type="text/xml")
        answer = plivo.xml(*_exit(db, conv, form, r))
        db.execute(insert(K).values(**kept, fingerprint="", status=200, body={"xml": answer.body.decode()}))
    return answer


def _exit(db: Connection, conv: Row | None, form: dict, r: int) -> list[tuple]:
    """Its wait ran out (the queue's overflow), or the room ended around it once its agents left: it waits for a
    transfer that still rings unless the agent hung up through the API, else the queue's after-agent action runs, or
    the call ends."""
    if conv is None or conv.state in ("wrap_up", "closed"):
        return [END]
    if r != conv.room:
        return _stale(db, conv)
    queue = conv.queue_id and config.get(db, str(conv.tenant_id), "queue", conv.queue_id)
    if queue and conv.state == "queued" and form.get("ParticipantExitCause") == WAIT_TIMEOUT:
        action = queue["overflow_action"]
        routing.overflowed(db, conv, conv.queue_id, "wait_timeout", action)
        return run(db, _leave(db, conv, action or HANGUP), action, True)
    if queue and not access.hung_up(conv) and (held := _ringing(db, conv)):
        return held
    if queue and conv.state == "live" and (after := queue["after_agent_action"]):
        return run(db, _leave(db, conv, after), after, True)
    return [END]


def _ringing(db: Connection, conv: Row) -> list[tuple] | None:
    """The room ended around the caller while its transfer still rang. The caller waits in a new room, where the
    transfer rings again from the same agent's stint, and its queue takes the call if the target never answers. The
    queue's row and the targets' presence rows are locked first, in lock order."""
    if not (rings := [a for a in conversations.rows(db, conv.id, ("offered",)) if a.via == "transfer"]):
        return None
    config.get(db, str(conv.tenant_id), "queue", conv.queue_id, lock=True)
    db.execute(routing._LOCK, {"ids": sorted(t.agent_id for t in rings if t.agent_id)})
    moved = _leave(db, conv, HANGUP)
    # A target whose slot another round took meanwhile is not rung again.
    same = {"via": "transfer", "transfer_id": rings[0].transfer_id, "came": rings[0].from_assignment}
    made = (
        conversations.offer(db, moved, t.agent_id, role=t.role, mode=t.mode, address=t.address, team=t.team, **same)
        for t in rings
    )
    again = list(filter(None, made))
    routing.CHANNELS["call"].ring(db, moved, again)
    return _queue(db, moved, {"type": "queue", "queue_id": str(moved.queue_id)}, True)


class EnqueueIn(BaseModel):
    call_uuid: str = Field(min_length=1, max_length=64)
    from_: m.E164 = Field(alias="from")
    to: m.E164
    agents: list[UUID] = []
    route: m.EnqueueRoute | None = None
    context: dict | None = None
    direction: Literal["inbound", "outbound"] = "inbound"


class Enqueued(BaseModel):
    conversation_id: str


CALLS = access.require("key:calls").dependency
_body = EnqueueIn.model_json_schema(ref_template="#/components/schemas/{model}")
routing.BODIES.append({k: v for k, v in _body.items() if k != "$defs"})  # its parts are the queue's components


async def _enqueuer(request: Request, queue_id: UUID) -> Principal | Row:
    """A tenant key with `calls` (JSON), else the tenant whose token signed Plivo's form, the queue's tenant."""
    if access.bearer(request) or request.headers.get("content-type", "").startswith("application/json"):
        return await run_in_threadpool(CALLS, request)
    C = T.config
    with read() as db:
        tenant_id = db.execute(select(C.c.tenant_id).where(C.c.id == queue_id, C.c.kind == "queue")).scalar()
    if tenant_id is None:
        raise ApiError(401, "unauthorized", "bad Plivo signature")
    # voice_signed reads the tenant from the path, and this path names the queue.
    request.scope["path_params"]["tenant_id"] = str(tenant_id)
    return await plivo.voice_signed(request)


@router.post(
    "/queues/{queue_id}/enqueue",
    response_model=Enqueued,
    responses={200: {"content": {"text/xml": {}}, "description": "Plivo XML for a form request"}},
    # The body is read by hand (Plivo's form or this JSON) so auth runs first; its JSON shapes are declared here.
    openapi_extra={"requestBody": {"content": {"application/json": {"schema": {"anyOf": routing.BODIES}}}}},
)
async def enqueue(request: Request, queue_id: UUID, caller: Principal | Row = Depends(_enqueuer)) -> Response | dict:
    """Hand a live call or a text contact to the queue. A Plivo form request (the queue's enqueue_url, query `agents`,
    `context`, `consent_played=1`) gets XML; a tenant key's JSON gets the conversation's id. JSON with a text
    `channel` is that channel's to read."""
    if isinstance(caller, Principal):
        try:
            raw = await request.json()
        except ValueError as e:
            raise ApiError(400, "invalid_json", "the body is not valid JSON") from e
        channel = raw.get("channel", "call") if isinstance(raw, dict) else "call"
        body = config.validate_model(EnqueueIn, raw) if channel == "call" else None
        if body is None:
            return await run_in_threadpool(_texted, caller, queue_id, channel, raw)
        return await run_in_threadpool(_hand_over, caller, queue_id, body)
    form = await plivo.form(request)
    if form.get("Event") == "Hangup":  # the call's hangup URL is this enqueue URL: the call is over
        return await run_in_threadpool(hangup, caller, form)
    return await run_in_threadpool(_enqueued, caller, queue_id, form, request.query_params)


def _context(raw: str | None) -> dict | None:
    if not raw:
        return None
    try:
        context = json.loads(raw)
    except ValueError:
        context = None
    if not isinstance(context, dict):
        raise ApiError(422, "context_invalid", "context must be a JSON object")
    return capped(context)


def _enqueued(tenant: Row, queue_id: UUID, form: dict, query) -> Response:
    """Query `agents` (the only agents offered), `context` (kept once per conversation) and `consent_played=1`
    (the tenant's app played the notice)."""
    agents = [a for v in query.getlist("agents") for a in v.split(",") if a]
    context = _context(query.get("context"))
    with tx() as db:
        config.owned(db, Principal(str(tenant.id), "plivo"), "agent", agents)
        try:
            conv, new = _call(db, tenant.id, form, context)
        except conversations.ForeignCall:
            return plivo.xml(END)
        if context and conv.context is None:
            db.execute(update(T.conversation).where(T.conversation.c.id == conv.id).values(context=context))
        notice = _consent(db, conv) if new and query.get("consent_played") != "1" else []
        action = {"type": "queue", "queue_id": str(queue_id), "agents": agents}
        return plivo.xml(*notice, *run(db, conv, action, conv.state == "open"))


def _texted(p: Principal, queue_id: UUID, channel: str, raw: dict) -> dict:
    """A text contact: its message joins the open conversation, or opens one this queue takes."""
    if (text := routing.CHANNELS.get(channel)) is None:
        raise ApiError(422, "channel_not_supported", "channel is call, chat, sms, whatsapp or email")
    full = None
    with tx() as db:
        if config.get(db, p.tenant_id, "queue", queue_id) is None:
            raise ApiError(404, "queue_not_found", "no such queue")
        conv, body = text.open(db, p, raw)
        if conv.state == "open":
            config.check(db, p, body)
            try:
                routing.enqueue(db, conv, queue_id, agents=body.agents, own=body.route)
            except routing.Full as e:
                conversations.gone(db, conv)
                full = e
    if full:
        raise full
    return {"conversation_id": str(conv.id)}


def _hand_over(p: Principal, queue_id: UUID, body: EnqueueIn) -> dict:
    """Plivo's verdict on the move is the answer: a call it cannot move leaves the queue at once. A call the queue
    refuses stays with the caller's app, open for its next enqueue."""
    call = {"CallUUID": body.call_uuid, "From": body.from_, "To": body.to, "Direction": body.direction}
    full = None
    with tx() as db:
        if config.get(db, p.tenant_id, "queue", queue_id) is None:
            raise ApiError(404, "queue_not_found", "no such queue")
        config.check(db, p, body)
        conv, _ = _call(db, p.tenant_id, call, body.context)
        try:
            if conv.state == "open":
                routing.enqueue(db, conv, queue_id, agents=body.agents, own=body.route)
        except routing.Full as e:
            full = e
        else:
            move = {"tenant_id": p.tenant_id, "conversation_id": str(conv.id), "room": conv.room}
            move |= {"call_uuid": body.call_uuid, "action": {"type": "queue", "queue_id": str(queue_id)}}
            key = plivo.command(db, "redirect", f"{conv.id}:{conv.room}", move, wait=True)
    if full:
        raise full
    try:
        plivo.verdict(key)
    except plivo.Refused as refused:
        with tx() as db:
            conversations.gone(db, conversations.lock(db, conv.id))
        code = "call_not_live" if refused.details.get("status") == 404 else "transfer_failed"
        raise ApiError(409, code, "Plivo could not move the call into the queue") from refused
    return {"conversation_id": str(conv.id)}


def _told(db: Connection, conv: Row) -> bool:
    """The newest of the conversation's call.ended and conversation.queued events is a call.ended: the caller's last
    room, or its call if it never sat in one, is already told."""
    E = T.events
    mine = (E.c.conversation_id == str(conv.id), E.c.type.in_(("call.ended", "conversation.queued")))
    return db.execute(select(E.c.type).where(*mine).order_by(E.c.id.desc()).limit(1)).scalar() == "call.ended"


def finish_call(db: Connection, conv: Row, hangup: dict, agent_id=None) -> None:
    """call.ended unless the caller's last room is told, naming `agent_id` (one no longer on the call) or else the
    agent on the call; then the caller is gone."""
    result = conv.result or ("rejected" if hangup["hangup_cause_code"] == 3020 else None)
    if hangup["ended_by"] == "system":
        hangup = hangup | {"ended_by": "agent" if access.hung_up(conv) else conv.routing.get("ended_by", "system")}
    live, told = conv.state in conversations.LIVE, _told(db, conv)
    every = conversations.rows(db, conv.id) if live or not (told or agent_id) else []
    if not told:
        on = agent_id or getattr(conversations.on_call(conv, every), "agent_id", None)
        conversations.call_ended(db, conv, hangup, on, result, written=live)
    if live:
        conversations.gone(db, conv, result, every, **({} if told else {"ended_by": hangup["ended_by"]}))


def _awaiting_exit(db: Connection, conv: Row, hangup: dict) -> bool:
    """A hangup nobody owns while an agent is on the call may be the agent's, which only the exit of the member that
    ended the room says; it waits for that exit, or for the close delay."""
    if hangup["ended_by"] != "system" or access.hung_up(conv) or "ended_by" in conv.routing:
        return False
    return conversations.on_call(conv, conversations.rows(db, conv.id)) is not None


@callbacks.post("/hangup")
def hangup(tenant: Row = TENANT, form: dict = FORM, c: UUID | None = None, a: UUID | None = None) -> Response:
    """The caller's leg ended: call.ended, once per room, in whatever state the conversation is, then the caller is
    gone unless someone said so first. V2 may call gone on the customer's MPC exit; call.ended belongs to /hangup.
    A call no queue holds closes 10 s later, after any voicemail it left."""
    if c:
        return DIALED["status"](tenant, form, c, a)
    hangup = hangup_facts(form, "customer" if form.get("HangupSource") == "Caller" else "system")
    with tx() as db:
        if (conv := _found(db, tenant.id, form)) is None:
            return Response(status_code=200)
        if conv.state == "open" or _awaiting_exit(db, conv, hangup):
            payload = {"conversation_id": str(conv.id), "hangup": hangup}
            jobs.schedule(db, "delayed_close", payload, delay=CLOSE_DELAY, key=str(conv.id))
        else:
            finish_call(db, conv, hangup)
    return Response(status_code=200)


@jobs.handler("delayed_close")
def _delayed_close(job: Row) -> None:
    with tx() as db:
        conv = conversations.lock(db, UUID(job.payload["conversation_id"]))
        if conv:
            finish_call(db, conv, job.payload["hangup"])


def redirect_call(tenant: Row, p: dict) -> dict:
    action = json.dumps(p["action"], separators=(",", ":"))
    url = plivo.url(str(tenant.id), "actions/move", r=p["room"], a=action)
    return plivo.rest(tenant, "POST", f"Call/{p['call_uuid']}/", legs="aleg", aleg_url=url, aleg_method="POST")
