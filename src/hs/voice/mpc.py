"""Plivo MultiPartyCall and call-status callbacks, matched to rings and members.

A round's offers ring in one add-member request, so Plivo's first answer wins. Each leg's uuid comes from Plivo's
reply, or from a call-status callback naming the ring (`g`) and the address it dialed, whichever arrives first. A
callback locks its conversation, finds its party (the caller by the call's uuid, a member by its leg's uuid) and
applies the transition that party's state allows, so a repeated or late callback changes nothing. A room event from
a room the caller has left is a no-op, but for its end, which frees the slots it held. A room check after each ring
reads Plivo's own view of the room, for the callbacks HS may never get."""

import json
import logging
import re
from collections import Counter
from math import ceil
from urllib.parse import unquote
from uuid import UUID, uuid4

from fastapi import APIRouter, Response
from sqlalchemy import func, or_, select, text, update
from sqlalchemy.engine import Connection, Row

from hs import agents, config, conversations, hot, jobs, plivo, routing
from hs.config.models import ssml
from hs.core import ApiError, T, read, tx
from hs.voice import flow

log = logging.getLogger(__name__)
callbacks = APIRouter()  # under /plivo/{tenant_id}
REJECTED = "3020"  # the agent turned the ring down; any other end before a join is a miss
LOST_RACE = "9000"  # a sibling leg answered first, and its join withdraws this offer
FAILED = "unsuccessful"  # an add-member request none of whose legs joined
# Room events no branch acts on in any state (an add request only without its outcome): they take no lock.
IDLE = ("ParticipantHold", "ParticipantUnhold", "AddParticipantByAPIActionInitiated")
PROMPTED = ("MPCInitialized", "ParticipantSpeakStopped", "ParticipantAudioStopped")
QUIET = -1  # the prompt number of a caller whose wait prompts have ended
CALLBACK_GRACE_SEC = 30
# Plivo creates a room from whichever party reaches it first, so a ring carries the room's settings: all but these.
SEAT = ("role", "waitTime", "startMpcOnEnter", "endMpcOnExit", "onExitActionUrl")
_HOLE = re.compile(r"\{(position|wait_time|min_wait_time|max_wait_time|wait_minutes)\}")
_PLACE = text(
    "SELECT (SELECT count(*) FROM conversation w WHERE w.tenant_id = c.tenant_id AND w.queue_id = c.queue_id"
    " AND w.channel = 'call' AND w.state = 'queued' AND CASE WHEN :lifo THEN (w.priority, c.queued_at) <="
    " (c.priority, w.queued_at) ELSE (w.priority, w.queued_at) <= (c.priority, c.queued_at) END),"
    " ARRAY(SELECT extract(epoch FROM started_at - queued_at)::float8 FROM assignment WHERE queue_id = c.queue_id"
    " AND channel = 'call' AND started_at >= queued_at AND queued_at BETWEEN c.queued_at - interval '3 hours'"
    " AND c.queued_at ORDER BY queued_at DESC LIMIT 10)"
    " FROM conversation c WHERE c.id = :c"
)


def room_of(p: dict) -> str:
    return f"MultiPartyCall/name_hs-{p['conversation_id']}-{p['room']}"


def _room_of(conv: Row) -> dict:
    return {"tenant_id": str(conv.tenant_id), "conversation_id": str(conv.id), "room": conv.room}


def sip_header(value) -> str:
    """Plivo takes only letters, digits and % in a SIP header value, and a softphone shows 64 encoded characters at
    most, cut short of a half escape."""
    encoded = re.sub(r"[^A-Za-z0-9]", lambda m: f"%{ord(m[0]):02X}", str(value))
    return re.sub(r"%[0-9A-F]?$", "", encoded[:64])


def bare_user(address: str) -> str:
    """An address as Plivo may echo it: without sip:, its domain or a leading +."""
    return address.removeprefix("sip:").partition("@")[0].lstrip("+")


def _tags(db: Connection, conv: Row, a: Row, doc: dict | None, every: list[Row] | None) -> dict:
    t = str(conv.tenant_id)
    mine = {str(x) for x in (doc or {}).get("teams", ())}
    team = getattr(a, "team", None) or (
        a.via == "queue" and next((x for x in conv.routing.get("queue_teams", ()) if x in mine), None)
    )
    tags = {"HsTeam": team and (config.get(db, t, "team", team) or {}).get("name")}
    if a.via == "transfer":
        from_ = next((r for r in every or conversations.rows(db, conv.id) if r.id == a.from_assignment), None)
        passed = from_ and from_.agent_id and config.get(db, t, "agent", from_.agent_id)
        tags |= {"HsFromAgent": passed and passed["name"], "HsConversationNumber": conv.number}
    return {k: v for k, v in tags.items() if v}


def ring(
    db: Connection, conv: Row, offers: list[Row], wait: bool = False, alias: str | None = None, every=None, **extra
) -> str:
    """Dial every offer in one add-member request; each leg's SIP headers name its offer and conversation. `extra`
    sets add-member parameters of its own, as a supervisor's role, mode flags and ring timeout, or the `call_uuid` of
    a live leg that moves in instead of a dial. `every` is the conversation's rows if the caller read them. Returns the
    ring's command key, for a caller who `wait`s on it."""
    t, g = str(conv.tenant_id), uuid4()
    queue = conversations.queue_of(db, conv)
    auto = queue and queue["auto_accept"]
    to, headers = [], []
    for a in offers:
        doc = a.agent_id and config.get(db, t, "agent", a.agent_id)
        address = agents.ring_address(db, t, doc) if a.agent_id else a.address
        conversations.edit(db, a, ring=g, address=address)
        kind = a.via if a.via in ("transfer", "monitor", "participant") else "offer"
        h = {"HsKind": kind, "HsOffer": a.id, "HsConversation": conv.id} | _tags(db, conv, a, doc, every)
        if alias:
            h["HsAlias"] = alias
        if a.via == "queue" and auto and auto["enabled"]:
            h["HsAutoAnswer"] = auto["delay"]
        to.append(address)
        headers.append(",".join(f"{k}={sip_header(v)}" for k, v in h.items()))
    room = flow.room(db, conv, None)[0][1].items()
    params = {re.sub("(?=[A-Z])", "_", k).lower(): v for k, v in room if k not in SEAT}
    raw = conv.routing.get("caller_id") or conv.customer
    shown = (conv.ours, "No Caller ID") if "restricted" in raw.lower() else (raw, raw[:50])
    dialed = {"from": shown[0], "caller_name": shown[1], "to": "<".join(to), "sip_headers": "<".join(headers)}
    params |= {"role": "Agent", "ring_timeout": config.tenant_settings(db, t).ring_timeout_sec}
    params |= ({} if "call_uuid" in extra else dialed) | extra
    params["call_status_callback_url"] = plivo.url(t, "call-status", c=conv.id, g=g)
    payload = {"tenant_id": t, "conversation_id": str(conv.id), "room": conv.room, "ring": str(g), "params": params}
    came = next((str(a.from_assignment) for a in offers if a.from_assignment), None)
    payload |= {"from": came, "resumes": conv.routing.get("resumes", 0), "via": next((a.via for a in offers), None)}
    if payload["via"] == "participant":
        payload["before"] = [str(a.id) for a in conversations.rows(db, conv.id, conversations.ON) if a.ring != g]
    payload["number"] = conv.ours if not (alias or "call_uuid" in extra) else None
    key = plivo.command(db, "ring", str(g), payload, wait=wait)
    _check_later(db, payload)
    return key


def members(db: Connection, conv: Row) -> list[tuple[str | None, Row | None]]:
    """A leg Plivo has bound counts before HS hears its join, as Plivo's room may hold it already. The agent's own
    leg of a call joined to a text or task conversation is no member."""
    on = conversations.rows(db, conv.id, conversations.ON)
    legs = [a for a in on if a.call_uuid and a.role in ("secondary", "external") and a.via != "outbound"]
    return [(None, None), *((str(a.transfer_id or a.id), a) for a in legs)]


def hold(db: Connection, conv: Row, want: list, agent_id, by=None, request=None, wait=False) -> list[str]:
    """A member's commands share its key, so a later one replaces one not sent yet and Plivo ends with the latest."""
    room, keys = {"tenant_id": str(conv.tenant_id), "conversation_id": str(conv.id), "room": conv.room}, []
    for a, on in want:
        p = room | {"call_uuid": (a or conv).call_uuid, "participant_id": a and str(a.transfer_id or a.id)}
        p |= {"args": {"hold": on}, "by": by, "agent_id": str(agent_id), "request": request}
        keys.append(plivo.command(db, "hold", str((a or conv).id), p, wait=wait))
    return keys


def _check_later(db: Connection, p: dict) -> None:
    """Check the ring's room once its legs are done ringing."""
    room = {k: p[k] for k in ("tenant_id", "conversation_id", "room")}
    delay = p["params"]["ring_timeout"] + CALLBACK_GRACE_SEC
    jobs.schedule(db, "missed_mpc_event", room, delay=delay, key=f"{p['conversation_id']}:{p['room']}")


routing.CHANNELS["call"] = routing.Channel(None, ring)  # a call opens on Plivo's answer URL, not through the API


def find_conv(db: Connection, tenant: Row, cid: UUID) -> Row | None:
    return cid and conversations.lock(db, cid, tenant=tenant.id)


def still_live(db: Connection, p: dict, states: tuple[str, ...] = ("live",)) -> Row | None:
    """The call a Plivo command for room `p["room"]` acted on, locked, while it is still in that room and in one of
    the `states` its request allowed; a call joined to a text or task conversation, while it is on."""
    conv = conversations.lock(db, UUID(p["conversation_id"]))
    on = conv.channel == "call" or "call_on" in conv.routing
    return conv if on and conv.state in states and conv.room == p["room"] else None


def dialed(db: Connection, conv: Row, aid=None) -> Row | None:
    """The stint of dial `aid`, else the conversation's newest dial. A text or task conversation outlives each call
    joined to it, so there a dial whose call is over reads None."""
    dials = [a for a in conversations.rows(db, conv.id) if a.via == "outbound"]
    a = next((d for d in dials if str(d.id) == str(aid)), None) if aid else (dials[-1] if dials else None)
    return a if conv.channel == "call" or (a and a.state in conversations.ON) else None


def _primary(every: list) -> Row | None:
    """The active primary stint among rows read after the lock, with no move of a stint since."""
    return next((a for a in every if a.role == "primary" and a.state == "active"), None)


def _leg(db: Connection, conv: Row, call_uuid: str) -> tuple[Row | None, list]:
    """The leg's row, and the rows it was looked for in: the conversation's, then with its offers' tombstones."""

    def mine(rows: list) -> Row | None:
        return next((a for a in rows if a.call_uuid == call_uuid), None)

    if a := mine(every := conversations.rows(db, conv.id)):
        return a, every
    return mine(every := conversations.rows(db, conv, ended=True)), every


def _bind(db: Connection, conv: Row, g: UUID, to: str, call_uuid: str, ended: list) -> Row | None:
    """The ring's unbound leg that dialed `to` takes `call_uuid`. One whose offer is over by then is hung up.
    A bind means Plivo took the ring, so a transfer's offered leg tells `initiated`; `_unbound` holds joins till then.
    `ended` is the rows with tombstones that `_leg` read."""
    rung = (a for a in ended if a.ring == g and a.call_uuid is None)
    if (a := next((x for x in rung if bare_user(x.address) == bare_user(to)), None)) is None:
        return None
    a = conversations.edit(db, a, call_uuid=call_uuid)
    if a.transfer_id and a.state == "offered":
        conversations.transferred(db, a, "initiated")
    elif a.transfer_id and conv.state in flow.SEATED:
        conversations.failed(db, a)
    _hang_up_over(db, conv, a)
    return a


def _pending(db: Connection, op: str, a: Row) -> dict | None:
    """The payload of Plivo command `op` on `a`'s leg while it has yet to run or is running; a parked one is spent."""
    J = T.jobs
    live = J.c.locked_until.is_(None) | (J.c.locked_until != jobs.INFINITY)
    key = J.c.key == f"{op}:{a.tenant_id}:{a.call_uuid}"
    return db.execute(select(J.c.payload).where(J.c.kind == "plivo", key, live)).scalar()


def _hang_up_over(db: Connection, conv: Row, a: Row) -> None:
    """A leg whose offer is over is hung up unless its hangup has yet to run, which would replace that one's outcome.
    Plivo names a leg before it creates it, so a hangup sent then finds no call: each call-status the leg sends
    before its hangup sends another."""
    if a.state in ("declined", "missed", "withdrawn") and _pending(db, "hangup", a) is None:
        payload = {"tenant_id": str(a.tenant_id), "conversation_id": str(conv.id), "call_uuid": a.call_uuid}
        plivo.command(db, "hangup", a.call_uuid, payload)


def _ended(db: Connection, conv: Row, offers: list[Row], event: str, **cols) -> None:
    """The offers end with `event` and routing moves on. Their agents' presence rows are locked first and their
    route_agent jobs after, both in agent order, as a routing round and a closure take them."""
    P, offers = T.presence, sorted(offers, key=lambda a: str(a.agent_id))
    if agents := [a.agent_id for a in offers if a.agent_id]:
        db.execute(select(P.c.agent_id).where(P.c.agent_id.in_(agents)).order_by(P.c.agent_id).with_for_update())
    for a in offers:
        if a := conversations.move(db, a, event, **cols):
            if a.role == "monitor":
                supervisor(db, conv, a, "supervisor.failed")
            routing.reoffer(db, conv, a)


def _kick(db: Connection, conv: Row, subject, member: str) -> None:
    payload = {"tenant_id": str(conv.tenant_id), "conversation_id": str(conv.id), "room": conv.room}
    plivo.command(db, "kick", str(subject), payload | {"member_id": member})


@callbacks.post("/call-status")
def call_status(c: UUID, g: UUID, tenant: Row = flow.TENANT, form: dict = flow.FORM) -> Response:
    """A ringing leg's progress binds it to its offer; its end before a join declines or misses the offer by Plivo's
    cause code, kept on the offer. A joined leg's end is its leave, whether or not the room's exit event comes."""
    call_uuid, code, event = form.get("CallUUID", ""), form.get("PlivoHangupCauseCode"), form.get("EventName")
    with tx() as db:
        if (conv := find_conv(db, tenant, c)) is None:
            return Response()
        if (a := (found := _leg(db, conv, call_uuid))[0]) is None:
            a = _bind(db, conv, g, form.get("To", ""), call_uuid, found[1])
        elif event != "Hangup":
            _hang_up_over(db, conv, a)
        if a and a.state == "offered" and event == "Hangup" and code != LOST_RACE:
            _ended(db, conv, [a], "decline" if code == REJECTED else "miss", reason=code)
        elif a and a.state == "active" and event == "Hangup":
            _left(db, conv, a)
        elif a and a.state == "missed" and event == "Hangup":
            conversations.leg_ended(db, a)
    return Response()


@callbacks.post("/mpc-status")
def mpc_status(c: UUID, r: int, tenant: Row = flow.TENANT, form: dict = flow.FORM) -> Response:
    """An event of room `r`: the caller's (seated, keyed, prompted, gone) or a member's (joined, left)."""
    event, call_uuid = form.get("EventName", ""), form.get("ParticipantCallUUID")
    if event in IDLE and "AddParticipantAPIRequestOutcome" not in form:
        return Response()
    with tx() as db:
        conv = find_conv(db, tenant, c)
        _seen(db, conv, r, event, call_uuid, form)
        if conv is None or r != conv.room:
            if conv and event == "MPCEnd":
                conversations.room_ended(db, conv, r)
            return Response()
        if (conv.state == "open" or conv.channel != "call") and flow.DIALED["room"](db, conv, event, form):
            return Response()
        if call_uuid and call_uuid == conv.call_uuid:
            _caller(db, conv, event, form)
        elif event in ("ParticipantJoin", "ParticipantExit"):
            _member(db, conv, event, form)
        elif event == "MPCEnd" and conv.state in ("open", "live") and not _handed_back(db, conv):
            conversations.gone(db, conv)  # a queued caller's own exit says how it left
        elif event == "MPCStart" or form.get("AddParticipantAPIRequestOutcome") == "successful":
            _quiet(db, conv)
        elif event == "AddParticipantByAPIActionCompleted" and form.get("AddParticipantAPIRequestOutcome") == FAILED:
            legs = set(filter(None, form.get("AddParticipantAPICallUUIDs", "").split(",")))
            dialed = [a for a in conversations.rows(db, conv, ended=True) if a.call_uuid in legs]
            known = {conv.call_uuid, *(a.call_uuid for a in dialed)}
            # A later ring's offer has no call uuid yet; only legs this event names can be unknown. An offer whose end
            # committed before its tombstone reached Redis still holds its leg there.
            unknown = legs - known
            if unknown and unknown - {o["call_uuid"] for o in hot.offers(conv.tenant_id, conv.id).values()}:
                raise ApiError(503, "ring_unbound", "a ring's legs are not known yet")
            to_miss = [a for a in dialed if a.state == "offered"]
            _ended(db, conv, to_miss, "miss", reason=FAILED)
    return Response()


def _seen(db: Connection, conv: Row | None, r: int, event: str, leg: str | None, form: dict) -> None:
    """Apply a member's exit, or hang up a late join to room `r` of a text or task conversation whose dial there is no
    longer on. Each dial there takes the next room, and nothing else moves its room."""
    if conv and event == "ParticipantExit":
        _exited(db, conv, form, r == conv.room)
    elif conv and event == "ParticipantJoin" and conv.channel != "call":
        dials = [a for a in conversations.rows(db, conv.id) if (a.via, a.role) == ("outbound", "secondary")]
        if (i := len(dials) - 1 - (conv.room - r)) >= 0 and dials[i].state not in conversations.ON:
            conversations.hang_dial(db, conv, dials[i].id, form.get("ParticipantRole", "").lower(), leg)


def _exited(db: Connection, conv: Row, form: dict, here: bool) -> None:
    """What an exit says about the call. The caller's leg left when Plivo says, whatever room it left. The member
    whose exit ended the room is who ended the call, by its role, unless HS's own queue timeout ended it; that
    settles a hangup waiting on it."""
    at = form.get("ParticipantCallUUID") == conv.call_uuid and plivo.utc(form.get("ParticipantExitTime"))
    marks = {"left_at": at} if at else {}
    waited_out = form.get("ParticipantExitCause") == flow.WAIT_TIMEOUT
    if here and not waited_out and form.get("IsMPCTerminationSource") == "true":
        by = {"agent": "agent", "customer": "customer"}.get(form.get("ParticipantRole", "").lower(), "system")
        marks["ended_by"] = by
        J = T.jobs
        db.execute(update(J).where(J.c.kind == "delayed_close", J.c.key == str(conv.id)).values(run_at=func.now()))
    if marks:
        conversations.set_marks(db, conv.id, **marks)


def _handed_back(db: Connection, conv: Row) -> bool:
    """A live caller whose queue has an after-agent action: once the room ends around it, its queue exit runs that
    action, whichever reaches HS first, so the call never goes to wrap-up before it. A caller who hung up is gone by
    its hangup URL."""
    queue = conv.state == "live" and conv.queue_id and config.get(db, str(conv.tenant_id), "queue", conv.queue_id)
    return bool(queue and queue["after_agent_action"])


def _waited_out(db: Connection, conv: Row) -> bool:
    """A queued caller whose stint's wait timeout is due, so its room may have ended as Plivo took it to its queue
    exit. The stint's timer holds the wait its room was seated with, whatever its queue says now; one already
    claimed was due, though a failed run moved it on. HS queues a caller before Plivo seats it, so a room that
    ended sooner ended with the caller's hangup."""
    J = T.jobs
    due = (J.c.kind == "queue_timeout", J.c.key == str(conv.id), or_(J.c.run_at <= func.now(), J.c.attempts > 0))
    return conv.state == "queued" and db.execute(select(J.c.id).where(*due)).first() is not None


def _caller(db: Connection, conv: Row, event: str, form: dict) -> None:
    """The caller left the room, pressed a key that has an action while it waits, or is due its next wait prompt. A
    wait that ran out, or a caller its queue hands back, is its queue exit's to end, whichever reaches HS first."""
    if event == "ParticipantExit":
        waited_out = conv.state == "queued" and form.get("ParticipantExitCause") == flow.WAIT_TIMEOUT
        if conv.state in conversations.LIVE and not waited_out and not _handed_back(db, conv):
            conversations.gone(db, conv)
        return
    if conv.state != "queued" or event not in (*PROMPTED, "ParticipantDigitInput"):
        return
    if (q := config.get(db, str(conv.tenant_id), "queue", conv.queue_id)) is None:
        return
    if event == "ParticipantDigitInput" and (action := q["dtmf_actions"].get(form.get("DigitInput", ""))):
        routing.overflowed(db, conv, q["id"], "dtmf", action)
        conversations.depart(db, conv, action, "customer")
    elif event in PROMPTED and q["wait_prompts"]:
        _play(db, conv, q, event == "MPCInitialized", int(form.get("SequenceNumber") or 0), form.get("MemberID", ""))


def _play(db: Connection, conv: Row, q: dict, first: bool, seq: int, member: str) -> None:
    """The caller's first wait prompt, once a stint, or the one after the last it heard, for an event newer than the
    one that chose that; none once the prompts ended or Redis lost how far they got."""
    was, C = hot.prompted(conv), T.conversation
    if was.n == QUIET or (conv.routing.get("prompted") if first else not was.claim or seq <= was.heard):
        return
    if first:
        db.execute(update(C).where(C.c.id == conv.id).values(routing=C.c.routing.concat({"prompted": True})))
    if say := _nth(q["wait_prompts"], n := 0 if first else was.n + 1):
        _prompt(db, conv, q, say, member, hot.fence(conv, n, seq, was))


def _quiet(db: Connection, conv: Row, *, stop: bool = False) -> None:
    q = conv.state == "queued" and conv.queue_id and config.get(db, str(conv.tenant_id), "queue", conv.queue_id)
    if q and q["wait_prompts"]:
        hot.hush(db, conv)
        if stop:
            stopped = _room_of(conv) | {"stop": True, "call_uuid": conv.call_uuid}
            plivo.command(db, "prompt", f"{conv.id}:{conv.room}", stopped)


def _nth(prompts: list[dict], n: int) -> dict | None:
    """The n-th wait prompt: each in order with its repeats, then the loop prompts over and over. A loop prompt's
    repeats are those of the prompt at its own place in the whole list."""
    turns = [say for say in prompts for _ in range(1 + say.get("repeat", 0))]
    loop = [say for say in prompts if say.get("loop")]
    cycle = [say for i, say in enumerate(loop) for _ in range(1 + prompts[i].get("repeat", 0))]
    return turns[n] if n < len(turns) else cycle and cycle[(n - len(turns)) % len(cycle)]


def _typical(took: list[float]) -> float:
    top, times = Counter(took).most_common(1)[0]
    return (sum(took) / len(took) + (top if times > 1 else max(took))) / 2


def _wait(took: list[float], how: str, position: int) -> str:
    """The estimate of the recent waits (`how`: the shortest, the longest or the typical one) times the place in
    line, spoken as minutes and seconds, at least a minute."""
    seconds = int({"min": min, "max": max, "mean": _typical}[how](took)) * position if took else 0
    minutes, seconds = divmod(seconds, 60)
    return " and ".join(f"{n} {unit}" for n, unit in ((minutes, "minutes"), (seconds, "seconds")) if n) or "1 minutes"


def _prompt(db: Connection, conv: Row, q: dict, say: dict, member: str, fence: list) -> None:
    """A wait prompt for the caller in place of the hold music, sent only while Redis holds what `fence` read. A text
    with a placeholder gets its place in line and estimated waits, from the queue's answered calls queued in the 3
    hours up to this caller's entry, newest first, 10 at most."""
    p = _room_of(conv) | {"member_id": member, "stop": False, "fence": fence}
    if say.get("audio_url"):
        p["url"] = say["audio_url"]
    else:
        words = say["text"]
        if _HOLE.search(words):
            position, took = db.execute(_PLACE, {"c": conv.id, "lifo": q["dequeue_order"] == "lifo"}).one()
            words = words.replace("{position}", str(position))
            for key, how in (("wait_time", "mean"), ("min_wait_time", "min"), ("max_wait_time", "max")):
                words = words.replace(f"{{{key}}}", _wait(took, how, position))
            words = words.replace("{wait_minutes}", str(max(1, ceil(sum(took) / len(took) / 60)) if took else 1))
        p |= {"text": words, "voice": say.get("voice"), "language": say.get("language"), "mix": False}
        if say.get("ssml"):
            p |= {"type": "ssml"}
            p["voice"], p["text"] = ssml(words)
    plivo.command(db, "prompt", f"{conv.id}:{conv.room}", p)


def _unbound(db: Connection, conv: Row) -> None:
    """503 while an offered leg of the room, or an outbound call's lead, has no call uuid yet: Plivo retries the
    event, which by then matches."""
    lead = conv.direction == "outbound" and conv.state == "open" and conv.call_uuid is None
    if lead or any(a.call_uuid is None for a in conversations.rows(db, conv.id, ("offered",))):
        raise ApiError(503, "ring_unbound", "a ring's legs are not known yet")


def _member(db: Connection, conv: Row, event: str, form: dict) -> None:
    """A member joined or left. Its leg is matched by uuid and keeps its member id; an unknown leg waits while a ring
    is unbound (503: Plivo retries), else it is a stranger, kicked on join."""
    member = form.get("MemberID", "")
    a, every = _leg(db, conv, form.get("ParticipantCallUUID", ""))
    if a is None:
        _unbound(db, conv)
        if event == "ParticipantJoin" and not _adopt(db, conv, form):
            _kick(db, conv, form.get("ParticipantCallUUID", ""), member)
        return
    if a.member_id is None and member:
        a = conversations.edit(db, a, member_id=member)
    if event == "ParticipantExit":
        if a.state == "offered":
            _ended(db, conv, [a], "miss", **({"member_id": a.member_id} if a.member_id else {}))
        elif a.state == "active":
            _left(db, conv, a)
    elif a.state == "offered":
        _joined(db, conv, a, form, every)
    elif a.state not in ("active", "withdrawn"):  # a withdrawn leg is hung up, again on each call-status it sends
        _kick(db, conv, a.id, member)


def _adopt(db: Connection, conv: Row, form: dict) -> bool:
    if conv.state != "queued":
        return False
    user, K, P = bare_user(form.get("MemberAddress", "")), T.credential, T.presence
    doc = config.by_key(db, str(conv.tenant_id), "agent", user)
    login = select(K.c.agent_id).where(K.c.tenant_id == conv.tenant_id, K.c.name == user).limit(1)
    if (agent := UUID(doc[0]) if doc else db.execute(login).scalar()) is None:
        return False
    db.execute(select(P.c.agent_id).where(P.c.agent_id == agent).with_for_update())
    a = conversations.offer(db, conv, agent, force=True)
    conversations.move(db, a, "accept", call_uuid=form.get("ParticipantCallUUID"), member_id=form.get("MemberID"))
    if conv.answered_at is None:
        flow.emit(db, "call.answered", conv, agent, call_uuid=form.get("ParticipantCallUUID"))
    return True


def _joined(db: Connection, conv: Row, a: Row, form: dict, every: list) -> None:
    """An offer's leg joined. A cold transfer target, or one whose primary left while it rang, takes the call; any
    other offer is accepted. A primary offer while another primary is on the call lost to it, as did a target the
    primary no longer waits on: withdrawn and kicked. `every` is the rows `_leg` read."""
    primary, member = _primary(every), form.get("MemberID", "")
    if a.role == "primary" and primary:
        took = False
    elif a.via == "transfer" and (a.mode == "cold" or not primary):
        if (took := hand_over(db, conv, a, every)) and primary:
            leg = {"tenant_id": str(conv.tenant_id), "call_uuid": primary.call_uuid}
            plivo.command(db, "hangup", primary.call_uuid, leg)
    else:
        if a.role == "monitor" and a.from_assignment:
            _moved(db, a)
        if (took := conversations.move(db, a, "accept")) and a.via == "transfer":
            conversations.transferred(db, took, "accepted")
    if not took:
        conversations.move(db, a, "withdraw", reason="taken")
        _kick(db, conv, a.id, member)
    elif a.role == "monitor":
        supervisor(db, conv, a, "supervisor.joined", announce=a.mode == "barge")
    elif a.role == "primary":
        _quiet(db, conv, stop=True)
        if conv.answered_at is None:
            flow.emit(db, "call.answered", conv, a.agent_id, call_uuid=a.call_uuid)
    elif a.via == "participant":
        conversations.participant(db, took, "call.participant_added", unquote(form.get("X-PH-HsAlias", "")) or None)


def _moved(db: Connection, a) -> None:
    """Supervisor `a` moved here from another call: end its stint there and release the leg and its ring.
    The leg's call-status may still come to the ring that dialed it, and must bind nothing there."""
    came = conversations.desk(db, a.tenant_id, a.agent_id, ("active", "done"), ids=[a.from_assignment])
    there = came and conversations.lock(db, came[0].conversation_id)
    old = there and next((r for r in conversations.rows(db, there.id) if r.id == a.from_assignment), None)
    if old and old.call_uuid == a.call_uuid:
        if old.state == "active":
            supervisor(db, there, old, "supervisor.left")
            conversations.move_out(db, old, "moved")
        conversations.edit(db, old, call_uuid=None, ring=None)


def _left(db: Connection, conv: Row, a: Row) -> None:
    """A member left. A primary's warm consult, or the target a complete hangs the primary up for, takes the call
    over, and a consult that leaves while its primary is on fails its transfer. The last leg besides supervisors to
    leave the caller takes them with it, so the room ends around the caller and its exit route runs as with none on."""
    rows = [r for r in conversations.rows(db, conv.id, conversations.ON) if r.id != a.id]
    targets = [r for r in rows if r.from_assignment == a.id]
    target = any(r.mode == "warm" for r in targets) and completing(db, a)
    if t := next((r for r in targets if r.state == "active" or str(r.id) == target), None):
        hand_over(db, conv, t)
        return
    if a.via == "transfer" and any(r.id == a.from_assignment for r in rows):
        conversations.transferred(db, a, "failed")
    alone = a.role != "monitor" and not any(r.state == "active" and r.role != "monitor" for r in rows)
    for r in conversations.by_agent([a, *(r for r in rows if alone and r.role == "monitor")]):
        if r.role == "monitor" and r.state == "active":
            supervisor(db, conv, r, "supervisor.left")
        conversations.move_out(db, r, "left")
        if r.id != a.id:
            conversations.kick_agent(db, conv, r)
    if a.via == "participant":
        conversations.participant(db, a, "call.participant_removed", reason="left")
    if a.role == "primary" and not alone:
        hold(db, conv, [(m, False) for _, m in members(db, conv)], a.agent_id)


def hand_over(db: Connection, conv: Row, t: Row, every: list | None = None) -> bool:
    """Transfer target `t` takes the call and the caller comes off hold. Rings sent before that hold no one, even if
    Plivo answers them late. False, changing nothing, for a target the call's primary no longer waits on: a repeat, a
    stale join. `every`, if given, is the conversation's rows, read after the lock."""
    every = conversations.rows(db, conv.id) if every is None else every
    primary, consulted = _primary(every), t.state == "active"
    waits = t.from_assignment == primary.id if primary else t.state == "offered"
    if t.state not in ("offered", "active") or not waits:
        return False
    came = primary or next(a for a in every if a.id == t.from_assignment)
    if t.state == "offered":
        t = conversations.move(db, t, "accept")
        conversations.transferred(db, t, "accepted")
    if back := [a for a in every if a.state == "wrap_up" and t.agent_id and a.agent_id == t.agent_id]:
        system = config.system_dispositions(db, str(conv.tenant_id))
        did = next((d.id for d in system if d.body["name"] == "Transferred back to same agent"), None)
        for a in back:
            conversations.move(db, a, "submit", disposition_id=did)
    monitors = [a for a in every if a.role == "monitor" and a.state in conversations.ON and not t.agent_id]
    for r in conversations.by_agent(([primary] if primary else []) + monitors):
        if r.role == "monitor" and r.state == "active":
            supervisor(db, conv, r, "supervisor.left")
        conversations.move_out(db, r, "transferred")
        if r.role == "monitor":
            conversations.kick_agent(db, conv, r)
    if t.agent_id:
        conversations.edit(db, t, role="primary")
    conversations.set_state(db, conv, "live")
    C = T.conversation
    fenced = {"hung_up": None, "resumes": conv.routing.get("resumes", 0) + 1}
    db.execute(update(C).where(C.c.id == conv.id).values(routing=C.c.routing.concat(fenced)))
    hold(db, conv, [(None, False), *([(t, False)] if consulted else [])], t.agent_id or came.agent_id)
    if (conv.routing.get("recording") or {}).get("state") == [conv.room, "pause"]:
        room = {"tenant_id": str(conv.tenant_id), "conversation_id": str(conv.id), "room": conv.room}
        plivo.command(db, "recording", f"{conv.id}:{conv.room}", room | {"action": "resume", "by": None})
    conversations.transferred(db, t, "completed")
    return True


def complete(db: Connection, conv: Row, primary: Row, target: UUID) -> str:
    """Hang the primary's leg up, again on a retry; once Plivo confirms, transfer target `target` takes the call.
    Returns the command to wait on."""
    leg = {"tenant_id": str(conv.tenant_id), "conversation_id": str(conv.id), "call_uuid": primary.call_uuid}
    return plivo.command(db, "complete", primary.call_uuid, leg | {"target": str(target)}, wait=True)


def completing(db: Connection, primary: Row) -> str | None:
    """The transfer target a complete hands the call to once Plivo confirms `primary`'s leg is gone, while that
    hangup has yet to run or is running."""
    return (_pending(db, "complete", primary) or {}).get("target")


def _completed(p: dict, _: dict) -> None:
    with tx() as db:
        conv = conversations.lock(db, UUID(p["conversation_id"]))
        if t := next((a for a in conversations.rows(db, conv.id) if str(a.id) == p["target"]), None):
            hand_over(db, conv, t)


def supervisor(db: Connection, conv: Row, a: Row, type_: str, *, announce: bool = False) -> None:
    """Tell the call's agent what supervisor `a` does. `visible` is false only for a listener that has not revealed
    itself. supervisor.left and supervisor.failed carry the supervisor's own id. `announce` speaks the tenant's barge
    notice to everyone."""
    primary = conversations.primary_agent(db, conv.id)
    agent = a if type_ in ("supervisor.left", "supervisor.failed") else primary
    seen = a.mode != "listen" or conv.routing.get("revealed") == str(a.id)
    flow.emit(db, type_, conv, agent and str(agent.agent_id), supervisor_id=str(a.agent_id), mode=a.mode, visible=seen)
    if announce and (notice := config.tenant_settings(db, str(conv.tenant_id)).monitor.barge_notice):
        payload = {"tenant_id": str(conv.tenant_id), "conversation_id": str(conv.id), "room": conv.room}
        plivo.command(db, "speak", str(a.id), payload | {"member_id": "all", "text": notice})


def _rung(p: dict, reply: dict) -> None:
    """Plivo took the ring: its legs bind, and a transfer from the call's primary holds the caller while its target
    rings and consults, or until the agent resumes if a decline or cancel came first. A call handed over or ended
    before this reply is not held, nor one whose agent took any member off hold after the ring began. A participant's
    ring holds the caller and the legs that were on the call when it rang, and its own leg if another participant
    was added after it: Plivo's replies to two rings may come in either order."""
    g = UUID(p["ring"])
    with tx() as db:
        conv = conversations.lock(db, UUID(p["conversation_id"]))
        every = None
        for leg in reply.get("calls", []):
            if (found := _leg(db, conv, leg["call_uuid"]))[0] is None:
                _bind(db, conv, g, leg["to"], leg["call_uuid"], found[1])
            every = found[1]
        primary = _primary(every) if every is not None else conversations.primary_agent(db, conv.id)
        if not primary or conv.routing.get("resumes", 0) != p.get("resumes"):
            return
        if p.get("from") == str(primary.id):
            hold(db, conv, [(None, True)], primary.agent_id)
        elif p.get("via") == "participant":
            legs, before = [m for _, m in members(db, conv) if m], set(p.get("before", ()))
            later = any(m.via == "participant" and m.ring != g and str(m.id) not in before for m in legs)
            held = [m for m in legs if str(m.id) in before or (m.ring == g and later)]
            hold(db, conv, [(None, True), *((m, True) for m in held)], primary.agent_id)


def _refused(p: dict, _: plivo.Refused) -> None:
    """Plivo refused the ring: its offers are missed."""
    with tx() as db:
        conv = conversations.lock(db, UUID(p["conversation_id"]))
        offers = [a for a in conversations.rows(db, conv.id, ("offered",)) if a.ring == UUID(p["ring"])]
        _ended(db, conv, offers, "miss", reason="ring_failed")


def _unknown(p: dict) -> None:
    with tx() as db:
        _check_later(db, p)


@jobs.handler("missed_mpc_event", lane="plivo")
def _check(job: Row) -> None:
    """Plivo's view of a room whose callbacks HS may have missed: once it ended, its caller is gone unless its queue
    exit says how it left (a caller its queue hands back, or one whose wait may have run out), and a member no offer
    or stint holds is kicked."""
    p, tenant = job.payload, plivo.account(job.payload["tenant_id"])
    status = plivo.rest(tenant, "GET", f"{room_of(p)}/").get("status")
    members = plivo.rest(tenant, "GET", f"{room_of(p)}/Participant/").get("objects", [])
    with tx() as db:
        conv = conversations.lock(db, UUID(p["conversation_id"]))
        ended = status == "Ended" and conv.room == p["room"] and conv.state in conversations.LIVE
        if ended and not _handed_back(db, conv) and not _waited_out(db, conv):
            conversations.gone(db, conv)
        known = {conv.call_uuid, *(a.call_uuid for a in conversations.rows(db, conv.id, conversations.ON))}
        for m in members:
            if not m.get("exit_cause") and m["call_uuid"] not in known:
                plivo.command(db, "kick", m["call_uuid"], p | {"member_id": m["member_id"]})


def _hung_up(p: dict, reply: dict) -> None:
    """Plivo has no such call: a caller whose end HS never heard is gone, and its agents go to wrap-up."""
    if not reply.get("gone") or "conversation_id" not in p:
        return
    with tx() as db:
        conv = conversations.lock(db, UUID(p["conversation_id"]))
        if conv.call_uuid == p["call_uuid"] and conv.state in conversations.LIVE:
            conversations.gone(db, conv)


@jobs.handler("mpc_initialized", lane="plivo")
def _initialized(job: Row) -> None:
    """Plivo's MPCInitialized for a seated caller's room that never sent it, from Plivo's own view of that room: the
    caller hears its first wait prompt there, unless one started or the caller has left the room since."""
    p, C = job.payload, T.conversation
    with read() as db:
        conv = db.execute(select(C).where(C.c.id == UUID(p["conversation_id"]))).one()
    if conv.room != p["room"] or conv.state != "queued" or conv.routing.get("prompted"):
        return
    members = plivo.rest(plivo.account(p["tenant_id"]), "GET", f"{room_of(p)}/Participant/").get("objects", [])
    with tx() as db:
        conv = conversations.lock(db, conv.id)
        me = next((m for m in members if m["call_uuid"] == conv.call_uuid and not m.get("exit_cause")), None)
        if me and conv.room == p["room"]:
            _caller(db, conv, "MPCInitialized", {"MemberID": me["member_id"]})


def speak_to(tenant: Row, p: dict) -> dict:
    kind, args = ("Play", {"url": p["url"]}) if "url" in p else ("Speak", {"text": p["text"]})
    args |= {k: p[k] for k in ("voice", "language", "type", "mix") if p.get(k) not in (None, "")}
    return plivo.rest(tenant, "POST", f"{room_of(p)}/Member/{p['member_id']}/{kind}/", **args)


def _add(tenant: Row, p: dict) -> dict:
    """A supervisor rings only into a room Plivo has not ended, since the add would start a new room by its name."""
    params = p["params"]
    if params["role"] == "Supervisor" and plivo.rest(tenant, "GET", f"{room_of(p)}/").get("status") == "Ended":
        raise plivo.Refused(409, "call_not_live", "the call is not live")
    if p.get("number") and (alias := _number_alias(tenant, p["number"])):
        tagged = (f"{h},HsAlias={sip_header(alias)}" for h in params["sip_headers"].split("<"))
        params = params | {"sip_headers": "<".join(tagged)}
    return plivo.rest(tenant, "POST", f"{room_of(p)}/Participant/", **params)


def _number_alias(tenant: Row, number: str) -> str:
    def load() -> str:
        return plivo.rest(tenant, "GET", f"Number/{number.removeprefix('+')}/").get("alias") or ""

    try:
        return json.loads(hot.cached(tenant.id, f"alias:{number}", load))
    except (plivo.Refused, plivo.Uncertain):
        log.warning("no alias for number %s", number, exc_info=True)
        return ""


# A ring whose outcome is unknown is never sent again: call-status binds its legs, the offer timeout ends the rest
# and the room check kicks a leg that joined unseen.
plivo.OPS["ring"] = plivo.Op(True, _add, _rung, _refused, _unknown)
plivo.OPS["kick"] = plivo.Op(False, lambda t, p: plivo.rest(t, "DELETE", f"{room_of(p)}/Participant/{p['member_id']}/"))
plivo.OPS["hangup"] = plivo.Op(False, lambda t, p: plivo.rest(t, "DELETE", f"Call/{p['call_uuid']}/"), _hung_up)
plivo.OPS["complete"] = plivo.Op(False, plivo.OPS["hangup"].send, _completed)


def _drop(tenant: Row, p: dict) -> dict:
    """A participant's removal hangs its leg up; Plivo having no such call refuses it."""
    if plivo.rest(tenant, "DELETE", f"Call/{p['call_uuid']}/").get("gone"):
        raise plivo.Refused(422, "participant_not_on_call", "the participant's leg is not on the call")
    return {}


def _redirected(p: dict, _: dict) -> None:
    if x := p.get("transfer"):
        with tx() as db:
            a = conversations.offer_row(x | {k: p[k] for k in ("tenant_id", "conversation_id")})
            conversations.transferred(db, a, "initiated", target=p["action"])


plivo.OPS["drop"] = plivo.Op(False, _drop)
# A move Plivo refuses with 404 found no such call, as a hangup's gone reply does.
plivo.OPS["redirect"] = plivo.Op(
    False, flow.redirect_call, _redirected, lambda p, e: _hung_up(p, {"gone": e.details.get("status") == 404})
)
plivo.OPS["speak"] = plivo.Op(create=True, send=speak_to)


def _said(tenant: Row, p: dict) -> dict:
    """The queued check reads Postgres after the claim, so a departure committed before it stops the prompt."""
    if p["stop"]:
        room = plivo.rest(tenant, "GET", f"{room_of(p)}/Participant/").get("objects", [])
        caller = next((m for m in room if m["call_uuid"] == p["call_uuid"] and not m.get("exit_cause")), None)
        for kind in ("Speak", "Play") if caller else ():
            plivo.rest(tenant, "DELETE", f"{room_of(p)}/Member/{caller['member_id']}/{kind}/")
        return {}
    if not hot.claim(p["tenant_id"], p["conversation_id"], p["fence"]):
        return {}
    with read() as db:
        conv = db.execute(select(T.conversation).where(T.conversation.c.id == UUID(p["conversation_id"]))).one()
    if (conv.state, conv.room, hot.stint(conv)) != ("queued", p["room"], p["fence"][1]):
        return {}
    try:
        return speak_to(tenant, p)
    except plivo.Refused:
        hot.unclaim(p["tenant_id"], p["conversation_id"], p["fence"])
        raise


# Speech and the stop share a key, so a stop replaces a prompt not sent yet and runs after one being sent.
plivo.OPS["prompt"] = plivo.Op(False, _said)
