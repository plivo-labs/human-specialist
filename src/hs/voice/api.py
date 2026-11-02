"""The public call routes: click-to-call, the in-call controls of either direction and supervisor monitoring.

Every control resolves the conversation with `load_call(..., live=True)` and acts through the engine's usecases,
which key a call on its contact CallUUID and its MPC `mpc_{run}`."""

from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Response
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text

from hs import events
from hs.access import AGENTS, TEAMS, Call, Principal, load_call, owned, redis_client, require, tenant_plivo
from hs.conversations.api import visible_agents
from hs.errors import UNAVAILABLE, ApiError
from hs.infra.db import core_engine
from hs.infra.log import logger
from hs.infra.redis import Cache
from hs.plivo import voice as pv
from hs.routing import account_read as ar
from hs.routing import constants as rc
from hs.routing.invitation_models import AgentAndContactMapping
from hs.routing.redis_store import RoutingRedis
from hs.setup import api as setup
from hs.setup.actions import E164, QueueAction, VoicemailAction
from hs.voice import agent_leg as voice_agent_leg
from hs.voice import callbacks as voice_callbacks
from hs.voice import constants as vc
from hs.voice import controls, outbound
from hs.voice import hangup as voice_hangup
from hs.voice import monitor as voice_monitor
from hs.voice import participant as voice_participant
from hs.voice import transfer as voice_transfer
from hs.voice.redis_store import VoiceRedis
from hs.voice.transfer_models import AgentDropParams, IncomingTransferReq

router = APIRouter()
CALLS = Depends(require("key:calls", "agent"))
SUPERVISE = Depends(require("sup", "key:calls"))
AGENT = Depends(require("agent"))
# public monitor mode -> the engine's supervisor state on joining, and on switching (a plain monitor state emits
# no mode change)
MODES = {"listen": "monitor", "whisper": "coach", "barge": "barge"}
SWITCHES = MODES | {"listen": "monitor_visible"}

_SIP = text("SELECT sip_endpoint FROM account_org_mapping WHERE uuid = :agent AND organization = :org")
_ALLOW_PAUSE = text("SELECT allow_recording_pause_play IS NOT FALSE FROM organization WHERE id = :org")
_ROLE = text(
    "SELECT r.name FROM account_org_mapping aom JOIN role r ON r.id = aom.role"
    " WHERE aom.uuid = :agent AND aom.organization = :org AND aom.is_deleted IS NOT TRUE"
)
_PRIMARY_AGENTS = text(
    "SELECT DISTINCT a.uuid::text FROM conversation c JOIN account_org_mapping a ON a.id = c.primary_aom"
    " WHERE c.parent = (SELECT id FROM conversation_parent WHERE uuid = :cid)"
)
_FREE_CANDIDATES = text(
    "SELECT aom.uuid::text AS id, acc.name, aom.sip_endpoint, t.uuid::text AS team_id, t.name AS team, o.auth_id"
    " FROM account_org_mapping aom JOIN account acc ON acc.id = aom.account"
    " JOIN organization o ON o.id = aom.organization"
    " JOIN org_statuses st ON st.id = aom.agent_status AND st.availability"
    " LEFT JOIN aom_team at ON at.aom = aom.id AND at.is_deleted IS NOT TRUE"
    " LEFT JOIN team t ON t.id = at.team AND t.is_deleted IS NOT TRUE"
    " WHERE aom.organization = :org AND aom.is_deleted IS NOT TRUE AND aom.uuid <> :me ORDER BY acc.name"
)
_DIRECT_DIAL = text(
    "SELECT COALESCE((voice_settings->>'agent_direct_dial')::boolean, false) FROM org_setting WHERE organization = :org"
)
# The requester's live leg (a key's: the leg of the agent who holds the call), as Plivo reported it joining. These
# rows outlive the call's cache keys.
_LEG = text(
    "SELECT l.call_uuid FROM call_leg c JOIN call_leg l ON l.parent_call_leg = c.id"
    " WHERE c.call_uuid = :call AND COALESCE(l.end_time, '') = ''"
    " AND l.participant = (SELECT sip_endpoint FROM account_org_mapping WHERE organization = :org AND id = COALESCE("
    "(SELECT id FROM account_org_mapping WHERE uuid = :agent), (SELECT primary_aom FROM conversation"
    " WHERE parent = (SELECT id FROM conversation_parent WHERE uuid = :cid) AND primary_aom IS NOT NULL"
    " AND is_deleted IS NOT TRUE AND status IN ('open', 'on_hold', 'waiting') ORDER BY id DESC LIMIT 1)))"
    " ORDER BY l.id DESC LIMIT 1"
)
# The public message of each failure kind (its code). The engine's own error string goes only to the log.
_FAILED = {
    "call_control_failed": "the call is not in a state that allows this action",
    "recording_control_failed": "the recording cannot be paused or resumed in the call's current state",
    "transfer_failed": "the transfer cannot be made in the call's current state",
    "participant_failed": "the participant cannot be added or removed in the call's current state",
    "too_many_participants": "a call takes at most two extra participants",
    "monitor_failed": "monitoring cannot start or change in the call's current state",
    "supervisor_present": "a supervisor is already on this call",
}
# Engine refusals a caller can act on, which answer with a kind of their own.
_OWN_KIND = {
    "maximum secondary participants reached": "too_many_participants",
    vc.MONITOR_EVENT_SUPERVISOR_PRESENT: "supervisor_present",
}


class Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Hold(Strict):
    on: bool


class RecordingAction(Strict):
    action: Literal["pause", "resume"]


class AgentTarget(Strict):
    type: Literal["agent"]
    agent_id: UUID


class TeamTarget(Strict):
    type: Literal["team"]
    team_id: UUID


class NumberTarget(Strict):
    type: Literal["number"]
    number: E164


class TransferIn(Strict):
    mode: Literal["cold", "warm"]
    target: Annotated[
        AgentTarget | TeamTarget | NumberTarget | QueueAction | VoicemailAction, Field(discriminator="type")
    ]


class TransferOut(BaseModel):
    transfer_id: str


class ParticipantIn(Strict):
    target: Annotated[AgentTarget | NumberTarget, Field(discriminator="type")]
    alias: str | None = Field(None, max_length=50)


class ParticipantOut(BaseModel):
    participant_id: str


class MonitorIn(Strict):
    mode: Literal["listen", "whisper", "barge"] = "listen"
    supervisor_id: UUID | None = None


class MonitorMode(Strict):
    mode: Literal["listen", "whisper", "barge"]


class Named(BaseModel):
    id: str
    name: str | None


class TransferTargets(BaseModel):
    agents: list[Named]
    teams: list[Named]


def _done(err: str | None, kind: str = "call_control_failed") -> None:
    """An engine usecase's error, logged and never shown: 503 when Plivo or a data store did not answer, otherwise a
    409 with the kind's fixed message (the call is not in a state that allows the action)."""
    if err is None:
        return
    logger.warning(f"{kind}: {err}")
    if vc.PLIVO_UNAVAILABLE in err or voice_callbacks.is_retryable(err):
        raise ApiError(*UNAVAILABLE)
    kind = _OWN_KIND.get(err, kind)
    raise _refused(409, kind, _FAILED[kind])


def _refused(status: int, code: str, message: str) -> ApiError:
    """A refusal, or a 503 when Redis does not answer: a Redis read returns an outage as a miss."""
    # Known limit: if Redis recovers between the read and this ping, the refusal is still returned; exact per-read
    # errors need Cache.get to report an outage to every caller.
    return ApiError(status, code, message) if Cache.get_instance().get_status() else ApiError(*UNAVAILABLE)


def _mapping(call: Call, ending: bool = False) -> AgentAndContactMapping:
    """The call's agent; none once its room ends or is ending, except for a hangup or leave (`ending`)."""
    m = AgentAndContactMapping.parse_redis(RoutingRedis().get_contact_to_agent_mapping(call.call_uuid))
    if m is None or (not ending and VoiceRedis().room_ended(call.call_uuid, m.qnr_run_uuid)):
        raise _refused(409, "call_not_connected", "no agent is connected to this call")
    return m


def _emit(type_: str, call: Call, m: AgentAndContactMapping, p: Principal, data: dict) -> None:
    events.emit(type_, tenant_id=p.tenant_id, conversation_id=call.conversation_id, agent_id=m.agent_uuid, data=data)


def direct_dial(p: Principal) -> bool:
    """Whether this credential may start a PSTN leg: a tenant key always, an agent token only with agent_direct_dial."""
    if p.kind == "key":
        return True
    with core_engine.connect() as conn:
        return bool(conn.execute(_DIRECT_DIAL, {"org": p.org_id}).scalar())


def _dialing(p: Principal) -> None:
    if not direct_dial(p):
        raise ApiError(403, "dial_requires_server", "only the server dials numbers while agent_direct_dial is off")


def _sip(agent_id: str, p: Principal) -> str:
    with core_engine.connect() as conn:
        return conn.execute(_SIP, {"agent": agent_id, "org": p.org_id}).scalar() or ""


def _members(call: Call, m: AgentAndContactMapping) -> tuple[list[dict], tuple[str, str]]:
    """The MPC's members still on the call, and the pair that signs requests on it."""
    pair = ar.contact_pair(RoutingRedis().get_contact_parameters(call.call_uuid, m.qnr_run_uuid))
    members, err = voice_agent_leg._get_participants(f"mpc_{m.qnr_run_uuid}", *pair)
    _done(err)
    return [x for x in members if not x.get("exit_cause")], pair


def _transfer_req(call: Call, m: AgentAndContactMapping, **fields) -> IncomingTransferReq:
    return IncomingTransferReq(contact_uuid=call.call_uuid, agent_sip=m.agent_sip_endpoint, **fields)


@router.post("/calls", status_code=201, response_model=outbound.Dialing)
def dial(body: outbound.DialIn, p: Principal = CALLS):
    if p.kind == "agent":
        _dialing(p)
        if str(body.agent_id) != p.agent_id:
            raise ApiError(403, "forbidden", "an agent token dials only for its own agent")
    return outbound.dial(p, body)


@router.post("/calls/{cid}/hold", status_code=204)
def hold(cid: UUID, body: Hold, p: Principal = CALLS) -> Response:
    call = load_call(cid, p, live=True)
    m = _mapping(call)
    if (err := controls.handle_hold_call(call.call_uuid, body.on)) != controls.UNCHANGED:
        _done(err)
        _emit("call.held" if body.on else "call.resumed", call, m, p, {"by_agent_id": p.agent_id})
    return Response(status_code=204)


@router.post("/calls/{cid}/recording", status_code=204)
def recording(cid: UUID, body: RecordingAction, p: Principal = CALLS) -> Response:
    call = load_call(cid, p, live=True)
    m = _mapping(call)
    with core_engine.connect() as conn:
        if not conn.execute(_ALLOW_PAUSE, {"org": p.org_id}).scalar():
            raise ApiError(403, "recording_control_off", "settings.recording.allow_pause is off")
    pause = body.action == "pause"
    _done(voice_hangup.handle_call_recording_event(call.call_uuid, pause), "recording_control_failed")
    _emit("call.recording_paused" if pause else "call.recording_resumed", call, m, p, {"by_agent_id": p.agent_id})
    return Response(status_code=204)


@router.post("/calls/{cid}/hangup", status_code=204)
def hangup(cid: UUID, p: Principal = CALLS) -> Response:
    """Hangs up the requester's own leg (a key's: the call's agent's), as when they hang up their phone. The leave
    that follows hands the caller to a transfer target who answered, or ends the call."""
    call = load_call(cid, p, live=True)
    ringing, err = outbound.cancel(p, call)
    if not ringing:
        args = {"call": call.call_uuid, "org": p.org_id, "agent": p.agent_id, "cid": call.conversation_id}
        with core_engine.connect() as conn:
            leg = conn.execute(_LEG, args).scalar()
        hung = pv.PlivoVoiceClient(*tenant_plivo(p.tenant_id)).call_delete(leg, quiet_not_found=True) if leg else None
        _body, status, err = hung or (None, 404, None)
        if status == 404:
            raise ApiError(409, "call_not_connected", "no agent is connected to this call")
    _done(err)
    return Response(status_code=204)


def _transfer_id(call: Call, m: AgentAndContactMapping) -> str:
    """A transfer is named by the transferring agent's own conversation, the id its call.transferred carries."""
    from hs.conversations.store import Conversation

    child = Conversation.get_primary_active_conversation(call.conversation_number, m.agent_uuid)
    if child is None:
        raise ApiError(409, "call_not_connected", "no agent is connected to this call")
    return str(child.uuid)


@router.post("/calls/{cid}/transfers", status_code=201, response_model=TransferOut)
def transfer(cid: UUID, body: TransferIn, p: Principal = CALLS):
    call = load_call(cid, p, live=True)
    m, t = _mapping(call), body.target
    transfer_id = _transfer_id(call, m)
    if t.type in ("queue", "voicemail"):
        setup.check_refs(t, p)
        if outbound.dialed(call.call_uuid, p.tenant_id):
            # An outbound call's contact is the agent's own leg: moving it would leave the lead alone in the room.
            raise ApiError(409, "transfer_failed", _FAILED["transfer_failed"])
        err = voice_transfer.move_caller(call.call_uuid, m, p.tenant_id, t.model_dump(mode="json"))
    else:
        if t.type == "number":
            _dialing(p)
            kind = rc.TRANSFER_TYPE_WARM_EXTERNAL if body.mode == "warm" else rc.TRANSFER_TYPE_EXTERNAL
            req, mode = _transfer_req(call, m, transfer_type=kind, member=t.number), rc.TRANSFER_MODE_BOTH
        else:
            query, ident = (AGENTS, t.agent_id) if t.type == "agent" else (TEAMS, t.team_id)
            owned(query, [ident], p, f"target.{t.type}_id")
            req, mode = _transfer_req(call, m, transfer_type=t.type, identifier=str(ident)), body.mode
        err = voice_transfer.handle_channel_transfer_initiate_generic(rc.CHANNEL_CALL, req, mode)
    _done(err, "transfer_failed")
    return {"transfer_id": transfer_id}


def _in_transfer(call: Call, m: AgentAndContactMapping, tid: UUID) -> dict:
    info = RoutingRedis().get_transfer_in_progress_info(call.call_uuid)
    if info is None or _transfer_id(call, m) != str(tid):
        raise _refused(404, "transfer_not_found", "no such transfer in progress")
    return info


@router.post("/calls/{cid}/transfers/{tid}/complete", status_code=204)
def complete_transfer(cid: UUID, tid: UUID, p: Principal = CALLS) -> Response:
    """A warm transfer hands over: everyone is taken off hold, the target becomes primary and the first agent's
    leg is hung up."""
    call = load_call(cid, p, live=True)
    m = _mapping(call)
    info = _in_transfer(call, m, tid)
    external = info.get("transfer_type") == rc.TRANSFER_TYPE_WARM_EXTERNAL
    if info.get("transfer_mode") != rc.TRANSFER_MODE_WARM and not external:
        raise ApiError(409, "not_a_warm_transfer", "only a warm transfer is completed by request")
    req = _transfer_req(call, m, transfer_type=info.get("transfer_type", ""), member=info.get("target", ""))
    _done(voice_transfer.handle_call_transfer_merge(req), "transfer_failed")
    # A number has no agent to promote; the merge found it joined, and the hangup below hands it the caller.
    if not external:
        leave = AgentDropParams(
            leave_type=rc.LEAVE_TYPE_LEAVE_PRIMARY, contact_uuid=call.call_uuid, channel=rc.CHANNEL_CALL
        )
        _done(voice_transfer.handle_agent_drop(leave, m.agent_uuid), "transfer_failed")
    mpc, pair = f"mpc_{m.qnr_run_uuid}", _members(call, m)[1]
    _done(voice_transfer.hangup_call_for_mpc_member(mpc, m.agent_sip_endpoint, *pair), "transfer_failed")
    return Response(status_code=204)


@router.post("/calls/{cid}/transfers/{tid}/cancel", status_code=204)
def cancel_transfer(cid: UUID, tid: UUID, p: Principal = CALLS) -> Response:
    call = load_call(cid, p, live=True)
    m = _mapping(call)
    mode = _in_transfer(call, m, tid).get("transfer_mode") or rc.TRANSFER_MODE_COLD
    _done(voice_transfer.handle_cancel_transfer(rc.CHANNEL_CALL, _transfer_req(call, m), mode), "transfer_failed")
    return Response(status_code=204)


@router.post("/calls/{cid}/participants", status_code=201, response_model=ParticipantOut)
def add_participant(cid: UUID, body: ParticipantIn, p: Principal = CALLS):
    """A third party joins the conference (two at most): another agent's endpoint, or a number."""
    call = load_call(cid, p, live=True)
    m, t = _mapping(call), body.target
    if t.type == "number":
        _dialing(p)
        member = t.number
    else:
        owned(AGENTS, [t.agent_id], p, "target.agent_id")
        member = _sip(str(t.agent_id), p)
    req = {"contact_uuid": call.call_uuid, "agent_sip": m.agent_sip_endpoint, "member": member}
    _done(voice_participant.add_participant(req | {"participant_alias": body.alias or ""})[1], "participant_failed")
    return {"participant_id": member.removeprefix("+")}


@router.delete("/calls/{cid}/participants/{participant_id}", status_code=204)
def remove_participant(cid: UUID, participant_id: str, p: Principal = CALLS) -> Response:
    call = load_call(cid, p, live=True)
    m = _mapping(call)
    addresses = (participant_id, f"+{participant_id}")
    leg = next((x["call_uuid"] for x in _members(call, m)[0] if x.get("member_address") in addresses), None)
    if leg is None:
        raise ApiError(404, "participant_not_found", "no such participant on this call")
    req = {"contact_uuid": call.call_uuid, "agent_sip": m.agent_sip_endpoint, "member": participant_id}
    _done(voice_participant.remove_participant(req | {"member_call_uuid": leg}), "participant_failed")
    return Response(status_code=204)


@router.get("/transfer-targets", response_model=TransferTargets)
def transfer_targets(p: Principal = AGENT):
    """Available agents with a free call slot, and the teams they are in."""
    with core_engine.connect() as conn:
        rows = conn.execute(_FREE_CANDIDATES, {"org": p.org_id, "me": p.agent_id}).all()
    pipe = redis_client().pipeline()
    for r in rows:
        pipe.scard(rc.agent_capacity_key(r.auth_id, r.sip_endpoint, rc.CHANNEL_CALL))
    free = [r for r, used in zip(rows, pipe.execute(), strict=True) if not used]
    agents = {r.id: {"id": r.id, "name": r.name} for r in free}
    teams = {r.team_id: {"id": r.team_id, "name": r.team} for r in free if r.team_id}
    return {"agents": list(agents.values()), "teams": list(teams.values())}


def _supervisor(cid: UUID, supervisor_id: UUID | None, p: Principal) -> tuple[Call, dict]:
    """The call as the monitoring supervisor may see it, and the engine's request meta acting as them: the token's
    own agent, or the agent a tenant key names (checked with that agent's own access)."""
    if p.kind == "key":
        if supervisor_id is None:
            raise ApiError(422, "supervisor_required", "a tenant key names the supervisor_id who monitors")
        owned(AGENTS, [supervisor_id], p, "supervisor_id")
        with core_engine.connect() as conn:
            role = conn.execute(_ROLE, {"agent": str(supervisor_id), "org": p.org_id}).scalar()
        p = Principal(p.tenant_id, p.org_id, "agent", agent_id=str(supervisor_id), role=role)
    elif supervisor_id is not None and str(supervisor_id) != p.agent_id:
        raise ApiError(403, "forbidden", "a supervisor token monitors as itself")
    call = load_call(cid, p, live=True)
    with core_engine.connect() as conn:
        on_call = set(conn.execute(_PRIMARY_AGENTS, {"cid": call.conversation_id}).scalars())
    allowed = visible_agents(p)
    visible = sorted(on_call if allowed is None else on_call & set(allowed))
    meta = call.req_meta | {"aom_uuid": p.agent_id, "conversation_id": call.conversation_id, "visible": visible}
    return call, meta


def _monitoring(cid: UUID, p: Principal) -> tuple[Call, dict]:
    """The call and the supervisor on it: the token's own agent, or for a tenant key whoever monitors the call."""
    supervisor_id = None
    if p.kind == "key":
        found = VoiceRedis().get_call_to_supervisor_mapping(load_call(cid, p, live=True).call_uuid)[0]
        supervisor_id = UUID(found["supervisor_uuid"]) if found else None
        if supervisor_id is None:
            raise _refused(404, "not_monitored", "no supervisor is on this call")
    call, meta = _supervisor(cid, supervisor_id, p)
    found = VoiceRedis().get_call_to_supervisor_mapping(call.call_uuid)[0]
    if not found or found.get("supervisor_uuid") != meta["aom_uuid"]:
        raise _refused(404, "not_monitored", "this supervisor is not on the call")
    return call, meta


@router.post("/calls/{cid}/monitor", status_code=204)
def monitor(cid: UUID, body: MonitorIn, p: Principal = SUPERVISE) -> Response:
    call, meta = _supervisor(cid, body.supervisor_id, p)
    _mapping(call)
    err = voice_monitor.monitor_conversation(meta, call.conversation_number, meta["visible"], MODES[body.mode])
    _done(err, "monitor_failed")
    return Response(status_code=204)


@router.patch("/calls/{cid}/monitor", status_code=204)
def switch_monitor(cid: UUID, body: MonitorMode, p: Principal = SUPERVISE) -> Response:
    call, meta = _monitoring(cid, p)
    _mapping(call)
    switch = {"accessible_aom_uuids": meta["visible"]}
    err = voice_monitor.update_participant_call_state(meta, call.conversation_number, SWITCHES[body.mode], switch)
    _done(err, "monitor_failed")
    return Response(status_code=204)


@router.delete("/calls/{cid}/monitor", status_code=204)
def leave_monitor(cid: UUID, p: Principal = SUPERVISE) -> Response:
    call, meta = _monitoring(cid, p)
    number = call.conversation_number
    _done(
        voice_monitor.hangup_monitored_conversation(meta, number, f"mpc_{_mapping(call, ending=True).qnr_run_uuid}"),
        "monitor_failed",
    )
    _done(voice_monitor.leave_monitored_conversation(meta, number, {"state": "closed"}), "monitor_failed")
    return Response(status_code=204)
