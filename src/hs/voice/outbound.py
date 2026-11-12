"""Agent-first outbound calls. POST /calls rings the agent (`dial`); once the agent's leg answers it takes
its seat in the conversation's room and the lead is dialed into it as the customer. An agent's own device may
dial out only with agent_direct_dial, and its leg is then the agent's from the start.

voice/flow.py hands this module the agent leg's answer (/answer with `c`, or from an endpoint) and every status of
both legs (/hangup with `c`: the agent leg's Make Call hangup and the lead's call status), each with `a`, the stint of
the dial it belongs to. The conversation stays `open` until the lead answers; a call that ends before that fails with
the result of its hangup cause. A call from a text conversation has a room of its own on it."""

import re
from contextlib import suppress
from uuid import UUID

from fastapi import APIRouter, Response
from pydantic import BaseModel, Field
from sqlalchemy import select, update
from sqlalchemy.engine import Connection, Row

from hs import access, agents, config, conversations, jobs, plivo
from hs.access import Principal
from hs.config import models as m
from hs.core import PUBLIC, ApiError, T, read, tx
from hs.voice import flow, mpc

router = APIRouter()
CALLS = access.require("key:calls", "agent")
LEAD_RING = 120
# The result of a call that ended before the lead answered, by Plivo's hangup cause; any other is failed_carrier.
RESULTS = {"1000": "cancelled", "3000": "no_answer", "6010": "no_answer", "3010": "busy", "3100": "busy"}
RESULTS |= {"3020": "rejected", "3110": "rejected", "2000": "failed_invalid", "3050": "failed_invalid"}
REJECTED = ("Hangup", {"reason": "rejected"})


class DialIn(m.Body):
    agent_id: UUID
    to: str
    from_: str | None = Field(None, alias="from")
    ring_timeout_sec: int | None = Field(None, ge=5, le=120)
    context: dict | None = None
    conversation_id: UUID | None = Field(
        None, description="The agent's live text, task or callback conversation; the call to `to` joins it."
    )


class Dialing(BaseModel):
    conversation_id: str
    status: str


def _check(db: Connection, p: Principal, body: DialIn) -> tuple[dict, str]:
    if not re.fullmatch(m.E164_PATTERN, body.to):
        raise ApiError(422, "invalid_number", "to must be an E.164 number")
    access.owned(db, p, "agent", [body.agent_id])
    agent = config.get(db, p.tenant_id, "agent", body.agent_id)
    default = config.tenant_settings(db, p.tenant_id).default_from.get("call", "")
    caller_id = body.from_ or agent.get("caller_id") or default
    if not config.by_key(db, p.tenant_id, "number", caller_id):
        raise ApiError(422, "from_not_allowed", "from must be a number registered with PUT /numbers")
    return agent, caller_id


def _open(db: Connection, p: Principal, body: DialIn, agent: dict, caller_id: str, address=None, leg=None):
    """The conversation and the agent's stint on it at `address`, else where the agent rings now: offered while its
    leg rings, active once it has `leg`. A text conversation it joins is locked first, then the agent's presence row is
    share-locked, so a status change in flight is seen, and the stint takes the agent's one call slot, so two dials
    cannot both take it."""
    P, C, t = T.presence, T.conversation, p.tenant_id
    conv = body.conversation_id and access.conversation(db, p, body.conversation_id, lock=True)
    status = db.execute(select(P.c.status_id).where(P.c.agent_id == agent["id"]).with_for_update(read=True)).scalar()
    if not (config.get(db, t, "status", status) or {}).get("availability"):
        raise ApiError(409, "agent_unavailable", "the agent is not available")
    ring = {"lead_ring": body.ring_timeout_sec or LEAD_RING, "lead_from": caller_id, "lead_to": body.to}
    if conv:
        mine = str(getattr(conversations.primary_agent(db, conv.id), "agent_id", None)) == str(agent["id"])
        if conv.channel == "call" or conv.state not in conversations.LIVE or not mine:
            raise ApiError(409, "conversation_not_joinable", "the call joins the agent's live text conversation")
        if mpc.dialed(db, conv):
            raise ApiError(409, "agent_on_call", "the agent is on a call")
        values = {"routing": C.c.routing.concat(ring | {"call_on": True}) - "lead_up", "call_uuid": None}
        values["room"] = C.c.room + 1
    else:
        parties = {"customer": body.to, "ours": caller_id, "context": body.context, "number": caller_id}
        conv = conversations.open(db, t, channel="call", direction="outbound", agent_id=agent["id"], **parties)
        values = {"routing": ring}
    conv = db.execute(update(C).where(C.c.id == conv.id).values(**values).returning(*C.c)).one()
    return conv, conversations.dial(db, conv, agent["id"], address or agents.ring_address(db, t, agent), leg)


def _dialing(cid: str) -> dict:
    return {"conversation_id": cid, "status": "dialing_agent"}


@router.post("/calls", status_code=201, response_model=Dialing)
def dial(body: DialIn, p: Principal = CALLS):
    """Ring the agent's SDK login or registered endpoint; the lead is dialed once the agent answers. An agent token
    dials only for itself, and only with agent_direct_dial on. Plivo's refusal is the answer (422, or 429 with
    Retry-After)."""
    with read() as db:
        if p.kind == "agent" and not config.tenant_settings(db, p.tenant_id).agent_direct_dial:
            raise ApiError(403, "dial_requires_server", "agents dial only with agent_direct_dial on")
        if p.kind == "agent" and p.agent_id != str(body.agent_id):
            raise ApiError(403, "forbidden", "an agent dials only for itself")
        (agent, caller_id), tenant = _check(db, p, body), config.live_tenant(db, p.tenant_id)
        address = agents.ring_address(db, p.tenant_id, agent)
    if address == agents.SIP.format(agent["endpoint_username"]):
        registered = plivo.rest(tenant, "GET", f"Endpoint/{agent['endpoint_id']}/").get("sip_registered")
        if str(registered).lower() != "true":
            raise ApiError(409, "agent_not_registered", "the agent has no registered device")
    with tx() as db:
        conv, a = _open(db, p, body, agent, caller_id, address)
        access.created(db, cid := str(conv.id))
        t, ring = p.tenant_id, config.tenant_settings(db, p.tenant_id).ring_timeout_sec
        call = {f"{u}_url": plivo.url(t, u, c=cid, a=a.id) for u in ("answer", "hangup")}
        call |= {"from": caller_id, "to": a.address, "ring_timeout": ring}
        call["sip_headers"] = f"HsKind=outbound,HsConversation={mpc.sip_header(cid)}"
        dialing = {"tenant_id": t, "conversation_id": cid, "assignment_id": str(a.id), "check": ring + 30}
        dialing["claim"] = access.claimed()
        key = plivo.command(db, "dial", str(a.id), dialing | {"path": "Call/", "params": call}, wait=True)
    plivo.verdict(key)
    return _dialing(cid)


access.VIEWS[f"POST {PUBLIC}/calls"] = lambda db, p, cid: (
    access.conversation(db, p, UUID(cid), offer=True) and _dialing(cid)
)


def _seat(db: Connection, conv: Row, a: Row | None = None) -> Response:
    """The agent's seat in the room. A newly seated agent's `a` also has call.started told and the lead rung into the
    room as its customer."""
    _, attrs, name, *hold = flow.room(db, conv, None)[0]
    room = {k: v for k, v in attrs.items() if k not in mpc.SEAT}
    t, cid, ring = str(conv.tenant_id), str(conv.id), conv.routing["lead_ring"]
    if a:
        ours, to = conv.routing.get("lead_from", conv.ours), conv.routing.get("lead_to", conv.customer)
        flow.emit(db, "call.started", conv, a.agent_id, **{"from": ours, "to": to})
        lead = {re.sub("(?=[A-Z])", "_", k).lower(): v for k, v in room.items()} | {"role": "Customer"}
        lead |= {"from": ours, "to": to, "ring_timeout": ring, "end_mpc_on_exit": True}
        lead["call_status_callback_url"] = plivo.url(t, "hangup", c=cid, a=a.id)
        adding = {"tenant_id": t, "conversation_id": cid, "room": conv.room, "check": ring + 30}
        adding |= {"dial": str(a.id)} if conv.channel != "call" else {}
        adding |= {"path": f"{mpc.room_of(adding)}/Participant/", "params": lead}
        plivo.command(db, "dial", _key(adding), adding)
    return plivo.xml(("MultiPartyCall", room | {"role": "Agent", "startMpcOnEnter": True}, name, *hold))


def answer(tenant: Row, form: dict, c: UUID | None, aid: UUID | None = None) -> Response:
    """The agent's leg answered: a dial of POST /calls (`c`), or the agent's own device dialing out from its endpoint
    or its SDK login, which takes agent_direct_dial and an E.164 number. A repeated answer gets its seat again;
    anything else is rejected."""
    A, call_uuid = T.assignment, form.get("CallUUID", "")
    with tx() as db:
        dialed = select(A.c.id, A.c.conversation_id).where(A.c.call_uuid == call_uuid, A.c.via == "outbound")
        if (seen := db.execute(dialed).first()) and (conv := mpc.find_conv(db, tenant, seen.conversation_id)):
            return _seat(db, conv) if mpc.dialed(db, conv, seen.id) else plivo.xml(REJECTED)
        if c and (conv := mpc.find_conv(db, tenant, c)) and (a := mpc.dialed(db, conv, aid)) and a.state == "offered":
            return _seat(db, conv, conversations.move(db, a, "answer", call_uuid=call_uuid))
        user = mpc.bare_user(form.get("From", "")).removesuffix(f"_{tenant.plivo_auth_id}")
        found = not c and config.by_key(db, tenant.id, "agent", user)
        if not found or not config.tenant_settings(db, tenant.id).agent_direct_dial:
            return plivo.xml(REJECTED)
        p, body = Principal(str(tenant.id), "plivo"), DialIn(agent_id=found[0], to=plivo.e164(form.get("To", "")))
        with suppress(ApiError):
            return _seat(db, *_open(db, p, body, *_check(db, p, body), leg=call_uuid))
        return plivo.xml(REJECTED)


def _hang(db: Connection, conv: Row, call_uuid: str) -> None:
    plivo.command(db, "hangup", call_uuid, {"tenant_id": str(conv.tenant_id), "call_uuid": call_uuid})


def _bind(db: Connection, conv: Row, call_uuid: str) -> Row:
    """The lead's leg, first seen in a status or the add-member reply; one seen once the call is over is hung up."""
    C = T.conversation
    conv = db.execute(update(C).where(C.c.id == conv.id).values(call_uuid=call_uuid).returning(*C.c)).one()
    if conv.state not in conversations.LIVE or not mpc.dialed(db, conv):
        _hang(db, conv, call_uuid)
    return conv


def _up(conv: Row) -> bool:
    """The lead answered. A call joined to a text conversation has no state of its own, so a mark says so."""
    return bool(conv.routing.get("lead_up")) if conv.channel != "call" else conv.state != "open"


def _live(db: Connection, conv: Row, a: Row, speak: bool = False) -> Row | None:
    """The lead answered: the call is live and call.answered told, naming the agent `a` who dialed it, and with
    `speak` the tenant's outbound notice is played to the lead."""
    if conv.channel == "call":
        live = conversations.set_state(db, conv, "live")
    else:
        C = T.conversation
        marked = update(C).where(C.c.id == conv.id, ~C.c.routing.has_key("lead_up"))
        live = db.execute(marked.values(routing=C.c.routing.concat({"lead_up": True})).returning(*C.c)).first()
    if live is None:
        return None
    t, cid = str(live.tenant_id), str(live.id)
    flow.emit(db, "call.answered", live, a.agent_id, call_uuid=live.call_uuid)
    if speak and (notice := config.tenant_settings(db, t).consent.outbound):
        say = notice.model_dump(mode="json", exclude_none=True)
        lead = {"tenant_id": t, "conversation_id": cid, "room": live.room, "call_uuid": live.call_uuid}
        plivo.command(db, "consent", cid, lead | {k.removeprefix("audio_"): v for k, v in say.items()})
    return live


def room(db: Connection, conv: Row, event: str, form: dict) -> bool:
    """A room event of a call that is still `open`, by the status rule: the lead in the room answered (the room
    then takes its exit), and the agent leaving before that cancels the call. True when that ends the event; a call
    joined to a text conversation takes every room event here, since the room never moves its conversation."""
    leg, a = form.get("ParticipantCallUUID"), mpc.dialed(db, conv)
    if a and leg and leg == conv.call_uuid and event in ("ParticipantJoin", "ParticipantExit"):
        _live(db, conv, a, speak=event == "ParticipantJoin")
    elif a and leg and leg == a.call_uuid and event == "ParticipantExit" and not _up(conv):
        _failed(db, conv, a, "cancelled", flow.hangup_facts({}, "agent"), leg)
        return True
    return conv.channel != "call"


def _failed(db: Connection, conv: Row, a: Row, result: str, hangup: dict, ended: str | None = None) -> None:
    """The call ended before the lead answered: call.failed and call.ended, told to the agent too, and it closes
    with `result`; on a text or task conversation only the call ends. The agent's dial `a` is let go without wrap-up,
    and every leg but the one that `ended` is hung up."""
    if conv.channel != "call":
        conversations.end_joined(db, a, hangup, result)
        return
    flow.emit(db, "call.failed", conv, a.agent_id, result=result, hangup_cause_code=hangup["hangup_cause_code"])
    if a.state == "offered":
        conversations.move(db, a, "withdraw", reason="cancelled")
    else:
        conversations.move(db, a, "end", reason=result)
    for leg in {a.call_uuid, conv.call_uuid} - {ended, None}:
        _hang(db, conv, leg)
    C = T.conversation
    conv = db.execute(update(C).where(C.c.id == conv.id).values(result=result).returning(*C.c)).one()
    flow.finish_call(db, conv, hangup, a.agent_id)


def status(tenant: Row, form: dict, c: UUID, aid: UUID | None = None) -> Response:
    """A leg of an outbound call changed. The agent's leg ending before the lead answers cancels the call; once both
    are on, the room takes the agent's end. The lead's leg is bound when first seen. The lead answering, or a hangup
    that says it answered, makes the call live; its end tells call.ended, or fails a call it never answered."""
    call_uuid, event = form.get("CallUUID", ""), form.get("EventName", "Hangup")
    with tx() as db:
        if (conv := mpc.find_conv(db, tenant, c)) is None or (a := mpc.dialed(db, conv, aid)) is None:
            conversations.hang_dial(db, conv, aid, "customer", call_uuid if event != "Hangup" else None)
            return Response()
        if a.state == "offered" or call_uuid == a.call_uuid:
            if not _up(conv) and a.state in ("offered", "active"):  # not once a move has let the agent go
                by = "system" if a.state == "offered" else "agent"
                _failed(db, conv, a, "cancelled", flow.hangup_facts(form, by), call_uuid)
            elif conv.channel != "call" and a.state == "active" and event == "Hangup":
                conversations.end_joined(db, a, flow.hangup_facts(form, "agent"))
            return Response()
        if conv.call_uuid is None and call_uuid:
            conv = _bind(db, conv, call_uuid)
        if call_uuid != conv.call_uuid:
            return Response()
        if (event == "Answered" or form.get("AnswerTime")) and (live := _live(db, conv, a, event == "Answered")):
            conv = live
        if event == "Hangup" and not _up(conv):
            code = form.get("PlivoHangupCauseCode", "")
            _failed(db, conv, a, RESULTS.get(code, "failed_carrier"), flow.hangup_facts(form, "customer"), call_uuid)
        elif event == "Hangup":
            source, agent_left = form.get("PlivoHangupSource"), a.state != "active"
            by = "customer" if source == "Callee" else "agent" if source == "API Request" or agent_left else "system"
            if conv.channel == "call":
                flow.finish_call(db, conv, flow.hangup_facts(form, by))
            else:
                conversations.end_joined(db, a, flow.hangup_facts(form, by))
    return Response()


def _consent(tenant: Row, p: dict) -> dict:
    """The tenant's outbound notice, to the lead once Plivo shows it in the room."""
    members = plivo.rest(tenant, "GET", f"{mpc.room_of(p)}/Participant/").get("objects", [])
    if (member := next((x["member_id"] for x in members if x["call_uuid"] == p["call_uuid"]), None)) is None:
        raise LookupError("the lead is not in the room yet")
    return mpc.speak_to(tenant, p | {"member_id": member})


def _done(p: dict, reply: dict) -> None:
    """The agent's dial keeps its Make Call's request id, which cancels it while the agent rings, and cancels it now
    for a dial that ended first; the lead's leg is bound from the reply unless a status bound it first."""
    with tx() as db:
        conv = conversations.lock(db, UUID(p["conversation_id"]))
        if "assignment_id" in p:
            a = conversations.edit(db, UUID(p["assignment_id"]), ring=(ring := reply["request_uuid"]))
            if a and a.state not in conversations.ON:
                plivo.command(db, "cancel", str(a.id), {"tenant_id": p["tenant_id"], "request_uuid": ring})
        elif (calls := reply.get("calls")) and conv.channel != "call" and not mpc.dialed(db, conv, p.get("dial")):
            conversations.hang_dial(db, conv, p.get("dial"), "customer", calls[0]["call_uuid"])
        elif conv.call_uuid is None and p["room"] == conv.room and calls:
            _bind(db, conv, calls[0]["call_uuid"])
        _check_later(db, p)


def _lost(p: dict, refused: plivo.Refused | None = None) -> None:
    """A dial Plivo refused, or any dial once it would have stopped ringing: a lead Plivo shows in the room answered,
    and a call still waiting on its lead fails, unless this was the agent's dial and the agent answered it. A call
    whose lead or room changed meanwhile is read from Plivo again."""
    cid = UUID(p["conversation_id"])
    while True:
        with read() as db:
            seen = db.execute(select(T.conversation).where(T.conversation.c.id == cid)).one()
        tenant = plivo.account(seen.tenant_id)
        path = f"{mpc.room_of({'conversation_id': cid, 'room': seen.room})}/Participant/"
        members = plivo.rest(tenant, "GET", path).get("objects", []) if not _up(seen) and seen.call_uuid else []
        joined = any(m["call_uuid"] == seen.call_uuid and not m.get("exit_cause") for m in members)
        with tx() as db:
            if refused:
                access.refused(db, refused, p.get("claim"))
            if ((conv := conversations.lock(db, cid)).call_uuid, conv.room) != (seen.call_uuid, seen.room):
                continue
            a = mpc.dialed(db, conv, p.get("assignment_id"))
            if a is None or p.get("room", conv.room) != conv.room:
                return
            answered = "assignment_id" in p and a.state != "offered"
            if not _up(conv) and not answered and not (joined and _live(db, conv, a)):
                _failed(db, conv, a, "failed_carrier", flow.hangup_facts({}, "system"))
            return


def _unknown(p: dict) -> None:
    with tx() as db:
        _check_later(db, p)


def _key(p: dict) -> str:
    """The agent's dial and its lead's are sent and checked apart. A text or task conversation's later dials share
    it, so there the lead's are keyed by their dial."""
    return p.get("assignment_id") or (f"{p['dial']}:customer" if "dial" in p else p["conversation_id"])


def _check_later(db: Connection, p: dict) -> None:
    jobs.schedule(db, "dial_check", p, delay=p["check"], key=_key(p))


jobs.handler("dial_check", lane="plivo")(lambda job: _lost(job.payload))
flow.DIALED |= {"answer": answer, "status": status, "room": room}
plivo.OPS["dial"] = plivo.Op(True, lambda t, p: plivo.rest(t, "POST", p["path"], **p["params"]), _done, _lost, _unknown)
plivo.OPS["consent"] = plivo.Op(False, _consent)
plivo.OPS["cancel"] = plivo.Op(False, lambda t, p: plivo.rest(t, "DELETE", f"Request/{p['request_uuid']}/"))
