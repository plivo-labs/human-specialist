"""Live call controls: hold, hang up, transfers, a second agent or phone, and a supervisor's listen, whisper and
barge.

A transfer target, a participant and a supervisor are assignment rows (secondary, external, monitor) rung into the
caller's room, and the room's callbacks (mpc.py) take them on and off the call. A transfer's id is its own, carried
on the rows it rings, so it holds when its target is rung again. Plivo writes are commands: a route that answers with
Plivo's verdict runs them itself, so an outage is a 503 while the command retries, and the client's retry converges
on the same state."""

from collections.abc import Callable
from typing import Annotated, Literal
from uuid import UUID, uuid4

from fastapi import APIRouter, Response
from pydantic import Field
from sqlalchemy import func, select, update
from sqlalchemy.engine import Connection, Row

from hs import access, agents, config, conversations, events, hot, jobs, plivo, routing
from hs.access import Principal
from hs.config import models as m
from hs.conversations import ON
from hs.core import ApiError, T, read, tx
from hs.voice import mpc

router = APIRouter()
CALLS = access.require("key:calls", "agent")
SUPERVISE = access.require("sup", "key:calls")
# Plivo's member flags per mode: a listener is muted, a whisper is heard by the agents only, a barge by everyone.
MODES = {
    "listen": {"coach_mode": True, "mute": True},
    "whisper": {"coach_mode": True, "mute": False},
    "barge": {"coach_mode": False, "mute": False},
}
Mode = Literal["listen", "whisper", "barge"]
FIXED_RING_SEC = 45


class Mute(m.Body):
    on: bool


class Member(m.Body):
    participant_id: UUID | None = Field(
        None, description="a participant, or a transfer for its target; none: the caller"
    )


class Hold(Mute):
    participant_id: UUID | Literal["all"] | None = Field(
        None, description="a participant, a transfer for its target, or `all`: every one of them and the caller"
    )


class NumberTarget(m.Body):
    type: Literal["number"]
    number: m.E164


class TransferNumber(NumberTarget):
    number: m.DialNumber


class TransferIn(m.Body):
    mode: Literal["cold", "warm"]
    target: Annotated[
        m.AgentTarget
        | m.TeamTarget
        | m.DestinationTarget
        | TransferNumber
        | m.QueueAction
        | m.VoicemailAction
        | m.MenuAction
        | m.RedirectAction,
        Field(discriminator="type"),
    ]


class ParticipantIn(m.Body):
    target: Annotated[m.AgentTarget | NumberTarget, Field(discriminator="type")]
    alias: str | None = Field(None, max_length=50)


class MonitorIn(m.Body):
    mode: Mode = "listen"
    supervisor_id: UUID | None = None


class MonitorMode(m.Body):
    mode: Mode


def _payload(conv: Row, **more) -> dict:
    return {"tenant_id": str(conv.tenant_id), "conversation_id": str(conv.id), "room": conv.room, **more}


def _call(db: Connection, p: Principal, cid: UUID, joins: bool = False) -> Row:
    """The live call `p` controls, locked: 404 unless it may see it, then 409 once the call is over or hung up, or
    unless `joins`, while it is a call joined to a text or task conversation, then 404 unless it may control it."""
    conv = access.conversation(db, p, cid, lock=True)
    joined = conv.channel != "call"
    if conv.state not in conversations.LIVE or access.hung_up(conv) or (joined and not mpc.dialed(db, conv)):
        raise ApiError(409, "call_not_live", "the call is not live")
    if joined and not joins:
        raise ApiError(409, "call_joined", "a call joined to a text or task conversation takes no such control")
    return access.controlled(conv)


def _owner(p: Principal, a: Row | None) -> None:
    if p.kind == "agent" and str(getattr(a, "agent_id", None)) != p.agent_id:
        raise ApiError(403, "not_conversation_owner", "only the conversation's agent or a tenant key may do this")


def _primary(db: Connection, conv: Row, p: Principal | None = None) -> Row:
    """The stint whose agent has the call: its primary, or the agent's dial of a call joined to a conversation."""
    a = conversations.primary_agent(db, conv.id) if conv.channel == "call" else mpc.dialed(db, conv)
    if a is None or a.state != "active":
        raise ApiError(409, "call_not_connected", "no agent is connected to this call")
    if p:
        _owner(p, a)
    return a


def _dials(db: Connection, p: Principal) -> None:
    if p.kind == "agent" and not config.tenant_settings(db, p.tenant_id).agent_direct_dial:
        raise ApiError(403, "dial_requires_server", "only the server dials numbers while agent_direct_dial is off")


def _leg(db: Connection, conv: Row, call_uuid: str | None, op: str = "hangup") -> str | None:
    """Hang a leg up, again on a retry; the hangup to wait on, None for no leg."""
    return call_uuid and plivo.command(db, op, call_uuid, _payload(conv, call_uuid=call_uuid), wait=True)


def _off(db: Connection, conv: Row, a: Row, reason: str) -> str | None:
    """Take a row off the call and hang up its leg, again on a retry; a leg not bound yet is hung up once it is.
    Returns the hangup to wait on."""
    conversations.move_out(db, a, reason)
    return _leg(db, conv, a.call_uuid)


def _holds(cid: UUID, p: Principal, named: str | None, want: Callable[[str | None], bool | None]) -> Response:
    """Taking any member off hold also keeps the rings sent so far from holding anyone when Plivo answers them late."""
    C, ask = T.conversation, uuid4().hex
    with tx() as db:
        conv = _call(db, p, cid, joins=True)
        primary, on = _primary(db, conv), mpc.members(db, conv)
        if named not in (None, "all", *(k for k, _ in on)):
            raise ApiError(404, "participant_not_found", "no such participant or transfer target on this call")
        held = [(a, w) for k, a in on if (w := want(k)) is not None]
        if any(not w for _, w in held):
            resumes = {"resumes": conv.routing.get("resumes", 0) + 1}
            db.execute(update(C).where(C.c.id == conv.id).values(routing=C.c.routing.concat(resumes)))
        keys = mpc.hold(db, conv, held, primary.agent_id, by=p.agent_id, request=ask, wait=True)
    plivo.verdict(*keys, request=ask)
    return Response(status_code=204)


@router.post("/calls/{cid}/hold", status_code=204)
def hold(cid: UUID, body: Hold, p: Principal = CALLS) -> Response:
    """Put the caller on hold or take it off, or the participant or the transfer's target the body names, or with
    `all` every one of them. Each event names the participant or transfer it holds."""
    named = body.participant_id and str(body.participant_id)
    return _holds(cid, p, named, lambda k: body.on if named in ("all", k) else None)


@router.post("/calls/{cid}/switch", status_code=204)
def switch(cid: UUID, body: Member, p: Principal = CALLS) -> Response:
    """The agent talks with one member alone, the caller unless the body names a participant or a transfer's target:
    every other member goes on hold."""
    named = body.participant_id and str(body.participant_id)
    return _holds(cid, p, named, lambda k: k != named)


@router.post("/calls/{cid}/merge", status_code=204)
def merge(cid: UUID, p: Principal = CALLS) -> Response:
    """Every member comes off hold: the caller, the participants and a transfer's target all talk together."""
    return _holds(cid, p, None, lambda k: False)


@router.post("/calls/{cid}/hangup", status_code=204)
def hangup(cid: UUID, p: Principal = CALLS) -> Response:
    """The agent leaves the call, as when its phone hangs up; a tenant key leaves as the primary. An agent with a
    stint of its own that is not the primary's ends only its own leg. The primary's connected warm consult takes
    the caller. Otherwise the call is torn down: the primary's leg ends first, so an outage there leaves the call as
    it was, then every other leg, and call.ended reads ended_by agent. A retry, or HS on its own, finishes a teardown
    an outage or a refusal cut short, whoever is on the call by then, and sends again each leg Plivo has not confirmed
    gone. A call that is over, or that the teardown handed back, is 409 until a queue takes it again. A call joined to
    a text or task conversation ends as when its agent's phone hangs up, whoever sends it: its agent's leg is hung
    up, and a dial still ringing its agent, or still being placed, is cancelled now."""
    with tx() as db:
        conv, key = access.conversation(db, p, cid, lock=True), None
        if conv.channel != "call":
            leaving = mpc.dialed(db, conv := _call(db, p, cid, joins=True))
            if leaving.state == "active":
                key = _leg(db, conv, leaving.call_uuid)
            else:
                conversations.end_joined(db, leaving, conversations.no_cause("agent", 0), "cancelled")
        else:
            key, leaving = _leave(db, p, conv)
    plivo.verdict(key)
    if leaving is None:
        _tear_down(cid, conv.room)
    return Response(status_code=204)


def _leave(db: Connection, p: Principal, conv: Row) -> tuple[str | None, Row | None]:
    """The hangup or complete the agent's hangup of a call waits on, and the stint that leaves, None for a
    teardown."""
    tearing, live = _tearing(conv, conv.room), conv.state in conversations.LIVE and not access.hung_up(conv)
    if not (live or tearing):
        raise ApiError(409, "call_not_live", "the call is not live")
    primary, leaving, consult = conversations.primary_agent(db, conv.id), None, None
    every = conversations.rows(db, conv.id)
    mine = p.agent_id and next((a for a in reversed(every) if str(a.agent_id) == p.agent_id), None)
    if mine and not tearing and not (primary and mine.id == primary.id):
        leaving = mine
    elif not tearing:
        access.controlled(conv)
        on = (getattr(primary, "id", None), "transfer", "active")
        if consult := primary and next((a.id for a in every if (a.from_assignment, a.via, a.state) == on), None):
            leaving = primary
        else:
            _mark(db, conv, [])
            jobs.schedule(db, "teardown", _payload(conv), delay=10, key=str(conv.id))
    key = mpc.complete(db, conv, primary, consult) if consult else leaving and _leg(db, conv, leaving.call_uuid)
    return key, leaving


def _mark(db: Connection, conv: Row, legs: list[str]) -> None:
    """The teardown's mark: the room it tears down, which is its stint, and the legs it ends there besides the
    primary's and the caller's. A leg stays listed and every pass sends its hangup again, so a leg Plivo refused is
    never forgotten: a pass succeeds only once Plivo has confirmed each one gone."""
    C = T.conversation
    mark = {"room": conv.room, "legs": legs}
    db.execute(update(C).where(C.c.id == conv.id).values(routing=C.c.routing.concat({"hung_up": mark})))


def _tearing(conv: Row, room: int) -> bool:
    """The agent's hangup is tearing down the stint in `room`, and the caller is still in that room."""
    return conv.state in conversations.LIVE and conv.room == room == (access.hung_up(conv) or {}).get("room")


def _tear_down(cid: UUID, room: int) -> None:
    """One pass of the teardown of the stint in `room`. Each transaction stops once the caller has left that room,
    so a pass never acts on a later stint. The primary's leg ends first, then every listed leg and row left."""
    with tx() as db:
        conv = conversations.lock(db, cid)
        if not _tearing(conv, room):
            return
        primary = conversations.primary_agent(db, cid)
        _legs(db, conv, primary)
        key = primary and _leg(db, conv, primary.call_uuid)
    plivo.verdict(key)
    with tx() as db:
        conv = conversations.lock(db, cid)
        keys = _rest(db, conv, primary) if _tearing(conv, room) else []
    plivo.verdict(*keys)


# A teardown outlasts the call session's backstop, so it never parks while its caller may be on.
RETRIES = (5, 30, 120) + (600,) * int(conversations.CALL_SESSION.total_seconds() // 600)
jobs.handler("teardown", RETRIES)(lambda job: _tear_down(UUID(job.payload["conversation_id"]), job.payload["room"]))


def _legs(db: Connection, conv: Row, primary: Row | None) -> list[str]:
    """The legs the mark lists, with every leg on the call now but the primary's added to it."""
    rows = conversations.rows(db, conv.id, ON)
    on = [a.call_uuid for a in rows if a.call_uuid and a.id != getattr(primary, "id", None)]
    legs = list(dict.fromkeys(access.hung_up(conv)["legs"] + on))
    _mark(db, conv, legs)
    return legs


def _rest(db: Connection, conv: Row, primary: Row | None) -> list[str | None]:
    """Every row the leaving primary leaves behind comes off, told of now, since the room may end before its exit
    reaches HS: a ring is withdrawn and a dial still ringing its agent is cancelled. Every listed leg is hung up, and
    the caller's last, unless its queue's after-agent action takes it once the room ends around it."""
    queue = conv.queue_id and config.get(db, str(conv.tenant_id), "queue", conv.queue_id)
    keys, legs = [], _legs(db, conv, primary)
    for a in conversations.by_agent(conversations.rows(db, conv.id, ON)):
        if a.via == "outbound" and a.state == "offered":
            ringing = _payload(conv, request_uuid=str(a.ring))
            keys.append(a.ring and plivo.command(db, "cancel", str(a.id), ringing, wait=True))
        elif not primary or a.id != primary.id:
            if a.role == "monitor" and a.state == "active":
                mpc.supervisor(db, conv, a, "supervisor.left")
            elif a.via == "participant":
                conversations.participant(db, a, "call.participant_removed", reason="removed")
            conversations.move_out(db, a, "cancelled" if a.state == "offered" else "left")
    keys += [_leg(db, conv, leg) for leg in legs]
    if not (queue and queue["after_agent_action"] and conv.state == "live"):
        keys.append(_leg(db, conv, conv.call_uuid))
    return keys


def _free(db: Connection, conv: Row, primary: Row, t, xfer: dict, every: list[Row]) -> list:
    """The free list is read again once its presence rows are share-locked, so a status change in flight is seen. An
    agent wrapping up this call is free for it: its offer takes the slot the wrap-up holds."""
    wrapping = {str(a.agent_id) for a in every if a.state == "wrap_up"}
    full = [f for f in hot.full(conv.tenant_id, "call") if f.removeprefix("call:") not in wrapping]
    args, team = {"tenant": conv.tenant_id, "me": primary.agent_id, "full": full}, getattr(t, "team_id", None)
    want = (lambda r: r.team_id == str(team)) if team else (lambda r: r.id == str(t.agent_id))

    def free() -> list[str]:
        return sorted({r.id for r in db.execute(agents._FREE, args) if want(r)})

    locked, offers = free(), []
    db.execute(routing._LOCK, {"ids": locked})
    limit = config.tenant_settings(db, str(conv.tenant_id)).simultaneous_agent_limit
    for a in (a for a in free() if a in locked):
        if len(offers) < limit and (o := conversations.offer(db, conv, a, team=team, **xfer)):
            offers.append(o)
    if not offers:
        raise ApiError(409, "agent_unavailable", "no agent of that target is available with a free call slot")
    return offers


def _saved(p: Principal, did: UUID) -> TransferNumber | m.RedirectAction:
    with read() as db:
        if (d := config.get(db, p.tenant_id, "destination", did)) is None:
            raise ApiError(422, "foreign_id", "an id in the body names nothing of this tenant", {"kind": "destination"})
    if d["number"]:
        return TransferNumber(type="number", number=d["number"])
    return m.RedirectAction(type="redirect", url=d["url"])


@router.post("/calls/{cid}/transfers", status_code=201, response_model=m.TransferOut)
def transfer(cid: UUID, body: TransferIn, p: Principal = CALLS):
    """Hand the call over; only its agent or a tenant key may. An agent, up to the tenant's simultaneous agent limit of a
    team's free members, or a number is rung into the room: a cold target takes the call when it joins, a warm one
    talks with the agent until the transfer completes. A number rings from the business number, and digits after a
    `#` are sent once it answers. The request answers once Plivo has taken the ring. A queue, voicemail, a menu or
    the tenant's app takes the caller out of the room now, and any supervisor on the call leaves. One transfer at a
    time, in a mode the tenant allows; a cold one to a number always is, a warm one only when both are. An agent
    token needs agent_direct_dial for a number or a redirect: the tenant's app can dial any number. A saved
    destination is the number or redirect it holds, under the same rules."""
    t, tid = body.target, uuid4()
    if t.type == "destination":
        t = _saved(p, t.destination_id)
    if t.type in ("menu", "redirect") and body.mode == "warm":
        raise ApiError(422, "warm_not_supported", "a transfer to a menu or a redirect is cold")
    with tx() as db:
        conv = _call(db, p, cid)
        primary = _primary(db, conv, p)
        config.check(db, p, t)
        modes = config.tenant_settings(db, p.tenant_id).transfer.modes
        checked = t.type in ("agent", "team", "number") and (t.type, body.mode) != ("number", "cold")
        if checked and modes != "both" and (t.type == "number" or modes != body.mode):
            raise ApiError(409, "transfer_mode_not_allowed", "the tenant's transfer modes do not allow this transfer")
        if t.type in ("number", "redirect"):
            _dials(db, p)
        if t.type in ("team", "menu"):
            config.owned(db, p, t.type, [getattr(t, f"{t.type}_id")])
        every = conversations.rows(db, conv.id)
        if any(a.from_assignment == primary.id and a.state in ON for a in every):
            raise ApiError(409, "transfer_in_progress", "this call has a transfer in progress")
        if t.type not in ("agent", "team", "number"):
            for a in (a for a in every if (a.role, a.state) == ("monitor", "active")):
                mpc.supervisor(db, conv, a, "supervisor.left")
            held = conv.room if t.type == "queue" else None
            conversations.move(db, primary, "leave", reason="transferred", held_by_room=held)
            transfer = {"transfer_id": str(tid), "from_assignment": str(primary.id), "mode": "cold", "channel": "call"}
            conversations.depart(db, conv, t.model_dump(mode="json"), "agent", transfer=transfer)
            return {"transfer_id": str(tid)}
        xfer, dial = {"via": "transfer", "mode": body.mode, "transfer_id": tid}, {}
        if t.type == "number":
            number, _, digits = t.number.partition("#")
            offers = [conversations.offer(db, conv, None, role="external", address=number, **xfer)]
            dial = {"from": conv.ours} | ({"send_digits": digits} if digits else {})
        else:
            offers = _free(db, conv, primary, t, xfer, every)
        conversations.open_transfer(db, offers[0])
        key = mpc.ring(db, conv, offers, wait=True, every=every, **dial)
    plivo.verdict(key)
    return {"transfer_id": str(tid)}


def _transfer(db: Connection, p: Principal, conv: Row, tid: UUID) -> tuple[list[Row], Row | None]:
    every = conversations.rows(db, conv, ended=True)
    if not (rows := [a for a in every if a.transfer_id == tid]):
        raise ApiError(404, "transfer_not_found", "no such transfer on this call")
    came = next((a for a in every if a.id == rows[0].from_assignment), None)
    _owner(p, came)
    return rows, came


def _warm(db: Connection, p: Principal, cid: UUID, tid: UUID, states: tuple[str, ...] = ON) -> tuple[Row, Row]:
    conv = _call(db, p, cid)
    if (t := next((a for a in _transfer(db, p, conv, tid)[0] if a.state in states and a.mode == "warm"), None)) is None:
        raise ApiError(409, "transfer_not_open", "only a warm transfer whose target is on the call completes")
    return conv, t


@router.post("/calls/{cid}/transfers/{tid}/complete", status_code=204)
def complete_transfer(cid: UUID, tid: UUID, p: Principal = CALLS) -> Response:
    """A warm transfer's target takes the call, and the agent leaves to wrap-up; only the agent the transfer came
    from, or a tenant key, may complete it. A target on the call takes it once Plivo has hung the agent's leg up, so
    a refusal is the answer and the call stays as it was. A target whose join HS has not heard yet is looked for in
    Plivo's room: one there is on the call, one still ringing is refused. A repeat changes nothing."""
    with tx() as db:
        conv, t = _warm(db, p, cid, tid)
        tenant = access.live_tenant(db, p.tenant_id)
    if not (joined := t.state == "active"):
        members = plivo.rest(tenant, "GET", f"{mpc.room_of(_payload(conv))}/Participant/").get("objects", [])
        joined = any(x["call_uuid"] == t.call_uuid and not x.get("exit_cause") for x in members)
    with tx() as db:
        conv, t = _warm(db, p, cid, tid, ON if joined else ("active",))
        primary = conversations.primary_agent(db, conv.id)
        key = mpc.complete(db, conv, primary, t.id) if primary and t.from_assignment == primary.id else None
    plivo.verdict(key)
    return Response(status_code=204)


@router.post("/calls/{cid}/transfers/{tid}/cancel", status_code=204)
def cancel_transfer(cid: UUID, tid: UUID, p: Principal = CALLS) -> Response:
    """Stop a transfer that has not completed: its rings are withdrawn and a consult is hung up, and a joined agent
    target is told call.participant_removed; the agent keeps the call. Only the agent the transfer came from, or a
    tenant key, may stop it. A caller whose agent left while it rang is queued already, and its queue's next round
    runs. A transfer has completed once its agent target is the primary, or a target is on the call that its agent
    has left, and is completing (409) while Plivo has yet to confirm a complete's hangup of that agent's leg. A retry
    waits on a cancelled leg's hangup Plivo has not confirmed; with nothing left to stop, it is 422."""
    with tx() as db:
        conv = _call(db, p, cid)
        rows, came = _transfer(db, p, conv, tid)
        if any(a.role == "primary" or (a.state == "active" and getattr(came, "state", None) != "active") for a in rows):
            raise ApiError(422, "transfer_completed", "the transfer has completed")
        if came and mpc.completing(db, came):
            raise ApiError(409, "transfer_completing", "the transfer is completing")
        hanging = [f"hangup:{conv.tenant_id}:{a.call_uuid}" for a in rows if a.reason == "cancelled" and a.call_uuid]
        for a in (a for a in rows if a.state == "active" and a.agent_id):
            conversations.participant(db, a, "call.participant_removed", reason="removed")
        J, keys = T.jobs, [_off(db, conv, a, "cancelled") for a in conversations.by_agent(rows) if a.state in ON]
        keys += db.execute(select(J.c.key).where(J.c.kind == "plivo", J.c.key.in_(hanging))).scalars().all()
        if not keys:
            raise ApiError(422, "not_in_transfer", "the call is not in transfer mode")
        if any(a.state in ON for a in rows):
            conversations.transferred(db, rows[0], "cancelled")
            if conv.state == "queued":
                jobs.schedule(db, "next_round", {"conversation_id": str(conv.id)}, key=str(conv.id))
    plivo.verdict(*keys)
    return Response(status_code=204)


@router.post("/calls/{cid}/participants", status_code=201, response_model=m.ParticipantOut)
def add_participant(cid: UUID, body: ParticipantIn, p: Principal = CALLS):
    """Another agent or a phone joins the call, at most two at once; the agent stays the primary and alone may add
    or remove one. It rings for 45 seconds, a phone from the business number. Once Plivo takes the ring everyone else
    on the call goes on hold, so the agent talks with the participant alone until a merge or a switch.
    call.participant_added comes when it joins, with its alias. A call its agent took by transfer, from an agent or
    through a queue, takes none."""
    t = body.target
    with tx() as db:
        conv = _call(db, p, cid)
        primary, every = _primary(db, conv, p), conversations.rows(db, conv.id)
        if any(a.role == "primary" and a.reason == "transferred" for a in every):
            raise ApiError(409, "participant_not_allowed", "a call taken by transfer takes no participants")
        if sum(a.via == "participant" and a.state in ON for a in every) >= 2:
            raise ApiError(409, "too_many_participants", "a call takes at most two extra participants")
        joins = {"via": "participant", "timeout": FIXED_RING_SEC}
        if t.type == "number":
            _dials(db, p)
            a = conversations.offer(db, conv, None, role="external", address=t.number, **joins)
        elif t.agent_id == primary.agent_id:
            raise ApiError(422, "already_on_call", "the call's own agent cannot join it as a participant")
        else:
            config.owned(db, p, "agent", [t.agent_id])
            a = conversations.offer(db, conv, t.agent_id, role="secondary", force=True, **joins)
        dial = {"from": conv.ours} if t.type == "number" else {}
        mpc.ring(db, conv, [a], alias=body.alias, ring_timeout=FIXED_RING_SEC, **dial)
    return {"participant_id": str(a.id)}


@router.delete("/calls/{cid}/participants/{participant_id}", status_code=204)
def remove_participant(cid: UUID, participant_id: str, p: Principal = CALLS) -> Response:
    """The participant leaves the call; an agent goes to wrap-up, and one still ringing has its offer cancelled. Either
    is told removed. One already off the call has its leg hung up, 422 when Plivo has no such call."""
    with tx() as db:
        conv, pid = _call(db, p, cid), config.as_uuid(participant_id)
        _primary(db, conv, p)
        theirs = (a for a in conversations.rows(db, conv, ended=True) if (a.id, a.via) == (pid, "participant"))
        if (a := next(theirs, None)) is None:
            raise ApiError(404, "participant_not_found", "no such participant on this call")
        if a.state in ON:
            conversations.participant(db, a, "call.participant_removed", reason="removed")
            key = _off(db, conv, a, "cancelled" if a.state == "offered" else "removed")
        else:
            key = _leg(db, conv, a.call_uuid, "drop")
    plivo.verdict(key)
    return Response(status_code=204)


def _monitor(db: Connection, p: Principal, cid: UUID, states: tuple[str, ...], left: bool = False) -> tuple[Row, Row]:
    """The call and the supervisor row the caller acts on: its own, or for a tenant key the call's, in `states` if one
    is, else the latest. One not in `states` is 422, unless `left` and a leave ended it: a retry of that leave."""
    conv = _call(db, p, cid)
    rows = conversations.rows(db, conv.id)
    watching = [a for a in rows if a.role == "monitor" and (p.kind != "agent" or str(a.agent_id) == p.agent_id)]
    a = next((a for a in watching if a.state in states), watching[-1] if watching else None)
    if a is None or (a.state not in states and not (left and a.reason == "stopped")):
        raise ApiError(422, "not_monitored", "no supervisor is on this call")
    return conv, a


def _acting(db: Connection, p: Principal, agent_id) -> Principal:
    role = config.get(db, p.tenant_id, "agent", agent_id)["role"]
    return Principal(p.tenant_id, "agent", agent_id=str(agent_id), role=role)


def _granted(db: Connection, p: Principal, mode: str) -> None:
    for grant in ("monitor", "barge") if mode == "barge" else ("monitor",):
        if not access.permits(db, p, grant):
            raise ApiError(403, f"{grant}_not_allowed", f"this caller's role may not {grant}")


@router.post("/calls/{cid}/monitor", status_code=204)
def monitor(cid: UUID, body: MonitorIn, p: Principal = SUPERVISE) -> Response:
    """A supervisor joins the call to listen, whisper to the agent or barge in. Answers 409 `supervisor_busy` if the
    supervisor is on another call, or `omnichannel_not_supported` if the call also texts its caller. A supervisor only
    listening or whispering on another call moves here on the same leg. The answer is Plivo's verdict on the ring."""
    A = T.assignment
    with tx() as db:
        # A supervisor's own token monitors as itself; a tenant key names the supervisor, with that supervisor's access.
        config.owned(db, p, "agent", [body.supervisor_id] if body.supervisor_id else [])
        if p.kind == "agent":
            if body.supervisor_id and str(body.supervisor_id) != p.agent_id:
                raise ApiError(403, "forbidden", "a supervisor token monitors as itself")
            sup = p
        elif body.supervisor_id is None:
            raise ApiError(422, "supervisor_required", "a tenant key names the supervisor_id who monitors")
        else:
            sup = _acting(db, p, body.supervisor_id)
        _granted(db, sup, body.mode)
        conv = _call(db, sup, cid)
        on = conversations.rows(db, conv.id, ON)
        if any(a.role == "monitor" for a in on):
            raise ApiError(409, "supervisor_present", "a supervisor is already on this call")
        texts = (A.c.agent_id.in_([a.agent_id for a in on]), A.c.from_assignment.in_([a.id for a in on]))
        texts += (A.c.channel != "call", A.c.state.in_(("active", "wrap_up")))
        if db.execute(select(A.c.id).where(*texts)).first():
            raise ApiError(409, "omnichannel_not_supported", "a call that also texts its caller cannot be monitored")
        # Serialized on the supervisor's presence row: a second move waits, then sees the first's slot held and is busy.
        db.execute(select(T.presence.c.agent_id).where(T.presence.c.agent_id == sup.agent_id).with_for_update())
        mine = {"role": "monitor", "via": "monitor", "mode": body.mode, "timeout": FIXED_RING_SEC + 30}
        if (a := conversations.offer(db, conv, sup.agent_id, **mine)) is None:
            old = _movable(db, sup)
            a = conversations.offer(db, conv, sup.agent_id, **mine, came=old.id, force=True)
            conversations.edit(db, a, call_uuid=old.call_uuid)
        moved = {"call_uuid": a.call_uuid} if a.call_uuid else {}
        quiet = {"enter_sound": "none", "exit_sound": "none", "ring_timeout": FIXED_RING_SEC}
        key = mpc.ring(db, conv, [a], wait=True, role="Supervisor", **MODES[body.mode], **quiet, **moved)
    plivo.verdict(key)
    return Response(status_code=204)


def _movable(db: Connection, sup: Principal) -> Row:
    """The supervisor's current stint when it is the only call slot held and is listening or whispering; otherwise 409
    `supervisor_busy`."""
    holders = [UUID(h.holder) for h in conversations.held(db, agent=sup.agent_id) if h.channel == "call"]
    old = conversations.desk(db, sup.tenant_id, sup.agent_id, ("active",), ids=holders) if len(holders) == 1 else []
    if not old or old[0].role != "monitor" or old[0].mode == "barge":
        raise ApiError(409, "supervisor_busy", "the supervisor is on a call")
    return old[0]


@router.post("/calls/{cid}/monitor/mute", status_code=204)
def mute_monitor(cid: UUID, body: Mute, p: Principal = SUPERVISE) -> Response:
    """The supervisor mutes or unmutes its own leg, and its mode stays."""
    with tx() as db:
        conv, a = _monitor(db, p, cid, ("active",))
        muted = _payload(conv, member_id=a.member_id, args={"mute": body.on})
        key = plivo.command(db, "mute", str(a.id), muted, wait=True)
    plivo.verdict(key)
    return Response(status_code=204)


@router.post("/calls/{cid}/monitor/reveal", status_code=204)
def reveal_monitor(cid: UUID, p: Principal = SUPERVISE) -> Response:
    """A silent listener lets the agent know it is on the call; from then on its events read visible."""
    C = T.conversation
    with tx() as db:
        conv, a = _monitor(db, p, cid, ("active",))
        shown = C.c.routing.concat({"revealed": str(a.id)})
        conv = db.execute(update(C).where(C.c.id == conv.id).values(routing=shown).returning(*C.c)).one()
        mpc.supervisor(db, conv, a, "supervisor.revealed")
    return Response(status_code=204)


@router.post("/calls/{cid}/monitor/assistance", status_code=204)
def ask_assistance(cid: UUID, p: Principal = CALLS) -> Response:
    """The call's agent asks the supervisor on the call for help."""
    with tx() as db:
        conv = _call(db, p, cid)
        if (a := next((a for a in conversations.rows(db, conv.id, ("active",)) if a.role == "monitor"), None)) is None:
            raise ApiError(422, "not_monitored", "no supervisor is on this call")
        mpc.supervisor(db, conv, a, "supervisor.assistance_requested")
    return Response(status_code=204)


@router.delete("/calls/{cid}/monitor/assistance", status_code=204)
def reject_assistance(cid: UUID, p: Principal = SUPERVISE) -> Response:
    """The supervisor turns the agent's request for help down."""
    with tx() as db:
        conv, a = _monitor(db, p, cid, ("active",))
        mpc.supervisor(db, conv, a, "supervisor.assistance_rejected")
    return Response(status_code=204)


@router.patch("/calls/{cid}/monitor", status_code=204)
def switch_monitor(cid: UUID, body: MonitorMode, p: Principal = SUPERVISE) -> Response:
    """Switch between listen, whisper and barge. The supervisor's mode changes once Plivo has switched it, and a
    barge is announced to everyone."""
    ask = uuid4().hex
    with tx() as db:
        if p.kind == "agent":
            _granted(db, p, body.mode)
        conv, a = _monitor(db, p, cid, ("active",))
        if p.kind != "agent":
            _granted(db, _acting(db, p, a.agent_id), body.mode)
        flags = _payload(conv, member_id=a.member_id, args=MODES[body.mode], request=ask)
        key = plivo.command(db, "coach", str(a.id), flags | {"assignment_id": str(a.id), "mode": body.mode}, wait=True)
    plivo.verdict(key, request=ask)
    return Response(status_code=204)


@router.delete("/calls/{cid}/monitor", status_code=204)
def leave_monitor(cid: UUID, p: Principal = SUPERVISE) -> Response:
    """The supervisor leaves the call, or stops its ring. One on the call leaves once Plivo has hung its leg up, so a
    refusal keeps it on the call. A retry hangs its leg up again."""
    with tx() as db:
        conv, a = _monitor(db, p, cid, ON, left=True)
        if a.state == "active":
            leaving = _payload(conv, call_uuid=a.call_uuid, assignment_id=str(a.id))
            key = plivo.command(db, "leave", a.call_uuid, leaving, wait=True)
        else:
            key = _off(db, conv, a, "stopped")
    plivo.verdict(key)
    return Response(status_code=204)


def _flags(tenant: Row, p: dict) -> dict:
    """Set a room member's hold, mute or coach mode."""
    room, member = mpc.room_of(p), p.get("member_id")
    if member is None:
        members = plivo.rest(tenant, "GET", f"{room}/Participant/").get("objects", [])
        on = (x for x in members if x["call_uuid"] == p["call_uuid"] and not x.get("exit_cause"))
        if (caller := next(on, None)) is None and p.get("participant_id"):
            return {"unchanged": True}
        if caller is None:
            raise plivo.Refused(409, "caller_not_in_room", "the caller is not in the call's room")
        if all(bool(caller.get(k)) == v for k, v in p["args"].items()):
            return {"unchanged": True}
        member = caller["member_id"]
    return plivo.rest(tenant, "POST", f"{room}/Participant/{member}/", **p["args"])


def _held(p: dict, reply: dict) -> None:
    """A retry that finds the caller already as asked still records the hold or its end when the database lacks it:
    Plivo may have applied the first try, whose own record never committed."""
    on, C = p["args"]["hold"], T.conversation
    data = {"by_agent_id": p["by"], "participant_id": p.get("participant_id")}
    with tx() as db:
        if (conv := mpc.still_live(db, p)) is None:
            return
        recorded = data["participant_id"] is None and (conv.held_at is None) == on
        if recorded:
            facts = {"hold_count": C.c.hold_count + 1, "held_at": func.now()} if on else conversations.UNHELD
            db.execute(update(C).where(C.c.id == conv.id).values(**facts))
        if recorded or not reply.get("unchanged"):
            kind = "call.held" if on else "call.resumed"
            events.emit(db, kind, p["tenant_id"], data, conversation_id=p["conversation_id"], agent_id=p["agent_id"])


def _coached(p: dict, _: dict) -> None:
    """Keep the mode Plivo switched a supervisor still on the call to, and tell of a change."""
    A = T.assignment
    with tx() as db:
        conv = conversations.lock(db, UUID(p["conversation_id"]))
        on = (A.c.id == p["assignment_id"], A.c.state == "active", A.c.mode != p["mode"])
        if a := db.execute(update(A).where(*on).values(mode=p["mode"]).returning(*A.c)).first():
            mpc.supervisor(db, conv, a, "supervisor.mode_changed", announce=a.mode == "barge")


def _stopped(p: dict, _: dict) -> None:
    """Plivo hung up a departed supervisor's leg: end its stint unless the exit already did."""
    with tx() as db:
        conv = conversations.lock(db, UUID(p["conversation_id"]))
        on = conversations.rows(db, conv.id, ("active",))
        if a := next((a for a in on if str(a.id) == p["assignment_id"]), None):
            mpc.supervisor(db, conv, a, "supervisor.left")
            conversations.move_out(db, a, "stopped")


plivo.OPS["hold"] = plivo.Op(False, _flags, _held)
plivo.OPS["coach"] = plivo.Op(False, _flags, _coached)
plivo.OPS["mute"] = plivo.Op(False, _flags)
plivo.OPS["leave"] = plivo.Op(False, lambda t, p: plivo.rest(t, "DELETE", f"Call/{p['call_uuid']}/"), _stopped)
