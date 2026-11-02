"""Click-to-call. One path for both entry points: POST /calls rings the agent with a Make Call, and an
agent's own device may dial out only with agent_direct_dial. At answer the agent's leg becomes the call's contact
and enters `mpc_{run}`, the inbound shape, so hold, recording, transfer and hangup work unchanged; the lead is added
when the agent joins."""

import json
import re
import uuid
from urllib.parse import quote
from xml.sax.saxutils import quoteattr

import httpx
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text

from hs import events
from hs.access import Call, Principal, redis_client, ring_address, tenant_plivo
from hs.conversations import child_update as ccu
from hs.conversations import management as cm
from hs.conversations.models import ConversationDataDict, ConversationStates, ConvParentInsertModel
from hs.errors import ApiError
from hs.infra.db import core_engine
from hs.infra.helpers import get_now
from hs.infra.log import logger
from hs.infra.settings import settings
from hs.plivo import rest
from hs.plivo import voice as pv
from hs.routing import constants as rc
from hs.routing.redis_store import RoutingRedis
from hs.setup.actions import E164_PATTERN
from hs.setup.api import is_registered
from hs.voice import controls
from hs.voice import hangup as voice_hangup
from hs.voice.intake import hold_music
from hs.voice.redis_store import VoiceRedis

DIAL_TTL = 24 * 3600
LEAD_RING_TIMEOUT = 120
REJECT = '<Response><Hangup reason="rejected"/></Response>'
EVENTS = "mpc-state-changes,participant-state-changes,add-participant-api-events,participant-digit-input-events"

_AGENT = text(
    "SELECT aom.uuid::text AS id, aom.sip_endpoint, aom.sip_endpoint_id, aom.caller_id, o.auth_id,"
    " COALESCE(st.availability, false) AS available, o.call_recording_outbound AS record,"
    " COALESCE((rs.reservation_timeout->>'call')::int, 30) AS ring"
    " FROM account_org_mapping aom JOIN organization o ON o.id = aom.organization"
    " LEFT JOIN org_statuses st ON st.id = aom.agent_status"
    " LEFT JOIN routing_settings rs ON rs.organization = o.id AND rs.is_deleted IS NOT TRUE"
    " WHERE aom.uuid = :agent AND aom.organization = :org AND aom.is_deleted IS NOT TRUE"
)
_ENDPOINT_AGENT = text(
    "SELECT uuid::text FROM account_org_mapping WHERE organization = :org AND is_deleted IS NOT TRUE"
    " AND split_part(regexp_replace(sip_endpoint, '^sip:', ''), '@', 1) = :user"
)
_OPENED = text("SELECT uuid::text, conversation_number FROM conversation_parent WHERE contact_uuid = :contact")
# The agent's leg becomes the conversation's contact, as a caller's leg is on an inbound call.
_RECONTACT = text(
    "WITH p AS (UPDATE conversation_parent SET contact_uuid = :call WHERE uuid = :cid RETURNING id)"
    " UPDATE conversation SET contact_uuid = :call WHERE parent = (SELECT id FROM p) AND contact_uuid = :old"
)
_CONSENT = text(
    "SELECT s.voice_settings->'consent'->'outbound' FROM org_setting s JOIN organization o ON o.id = s.organization"
    " WHERE o.auth_id = :auth_id"
)


class DialIn(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)
    agent_id: uuid.UUID
    to: str
    from_: str | None = Field(None, alias="from")
    ring_timeout_sec: int | None = Field(None, ge=5, le=120)
    context: dict | None = None


class Dialing(BaseModel):
    conversation_id: str
    status: str


def hs_header(value: str) -> str:
    """An Hs* SIP header value: Plivo takes only [A-Za-z0-9%], so everything else is percent-encoded."""
    return re.sub(r"[^A-Za-z0-9%]", lambda m: f"%{ord(m[0]):02X}", quote(value, safe=""))


def _key(ref: str) -> str:
    return f"DIAL:{ref}"


def dialed(ref: str, tenant_id: str = "") -> dict | None:
    """The dial by its conversation id or, once answered, by its agent leg (the call's contact): the engine's MPC
    callbacks branch on it. With `tenant_id`, only that tenant's: a callback's query names it, and any tenant signs."""
    raw = redis_client().get(_key(ref))
    dial = json.loads(raw) if raw else None
    return dial if dial and tenant_id in ("", dial["tenant_id"]) else None


def _put(dial: dict, *refs: str) -> None:
    pipe = redis_client().pipeline()
    for ref in refs:
        pipe.set(_key(ref), json.dumps(dial), ex=DIAL_TTL)
    pipe.execute()


def _registered(p: Principal, endpoint_id: str) -> bool:
    try:
        r = rest.call(tenant_plivo(p.tenant_id), "GET", f"Endpoint/{endpoint_id}/")
    except httpx.HTTPError as e:
        raise ApiError(503, "plivo_unavailable", "Plivo did not answer") from e
    return r.is_success and str(r.json().get("sip_registered")).lower() == "true"


def _open(p: Principal, body: DialIn, device: bool = False) -> dict:
    """Check the dial, open the outbound conversation and hold the agent's call slot.
    Returns the dial: what the answer, the lead's add and the hangups read."""
    if not re.fullmatch(E164_PATTERN, body.to):
        raise ApiError(422, "invalid_number", "to must be an E.164 number")
    if body.context is not None and len(context := json.dumps(body.context)) > 2048:
        raise ApiError(422, "context_too_large", "context is at most 2 KB")
    if body.context is not None and "\\u0000" in context:
        # the event relay casts context to jsonb, which cannot hold a NUL
        raise ApiError(422, "context_invalid", "context may not contain a NUL character")
    with core_engine.connect() as conn:
        agent = conn.execute(_AGENT, {"agent": str(body.agent_id), "org": p.org_id}).first()
    if agent is None:
        raise ApiError(422, "foreign_id", "unknown id in agent_id", {"field": "agent_id", "ids": [str(body.agent_id)]})
    caller_id = body.from_ or agent.caller_id
    if not caller_id or not is_registered(p.org_id, caller_id):
        raise ApiError(422, "from_not_allowed", "from must be a number registered with PUT /numbers")
    if not agent.available:
        raise ApiError(409, "agent_unavailable", "the agent is not available")
    if redis_client().scard(rc.agent_capacity_key(agent.auth_id, agent.sip_endpoint, rc.CHANNEL_CALL)):
        raise ApiError(409, "agent_on_call", "the agent is on a call")
    browser = ring_address(agent.sip_endpoint) != agent.sip_endpoint  # Plivo shows no registration for an SDK login
    if not (device or browser or _registered(p, agent.sip_endpoint_id)):
        raise ApiError(409, "agent_not_registered", "the agent has no registered device")
    contact = str(uuid.uuid4())
    conv = ConvParentInsertModel(
        parent_channel=rc.CHANNEL_CALL,
        phone_or_email=body.to,
        parent_contact_uuid=contact,
        org_auth_id=agent.auth_id,
        agent_sip_or_uuid=agent.id,
        direction_inbound=False,
        sender_id=caller_id,
        conversation_data=ConversationDataDict(conversation_custom_data=body.context) if body.context else None,
    )
    if (created := cm.create_parent_conversation(conv)).error:
        logger.error(f"dial: the conversation was not created: {created.error}")
        raise ApiError(409, "conversation_failed", "the call's conversation could not be opened")
    with core_engine.connect() as conn:
        cid, number = conn.execute(_OPENED, {"contact": contact}).one()
    data = {"number": caller_id, "entry_point_id": None, "hours": "none"}
    events.emit("conversation.started", tenant_id=p.tenant_id, conversation_id=cid, agent_id=agent.id, data=data)
    dial = {
        "tenant_id": p.tenant_id,
        "conversation_id": cid,
        "conversation_number": str(number),
        "contact": contact,
        "run_id": str(uuid.uuid4()),
        "agent_id": agent.id,
        "agent_sip": agent.sip_endpoint,
        "auth_id": agent.auth_id,
        "to": body.to,
        "from": caller_id,
        "ring_timeout": body.ring_timeout_sec or LEAD_RING_TIMEOUT,
        "record": bool(agent.record),
        "agent_ring": agent.ring,
    }
    code, _err = RoutingRedis().increase_agent_capacity(agent.auth_id, agent.sip_endpoint, rc.CHANNEL_CALL, 1, number)
    if code == -2:
        _fail(dial, "cancelled", None)
        raise ApiError(409, "agent_on_call", "the agent is on a call")
    _put(dial, cid)
    return dial


def dial(p: Principal, body: DialIn) -> dict:
    """POST /calls: ring the agent's endpoint; the lead is dialled once the agent is in the MPC."""
    dial = _open(p, body)
    base, cid = f"{settings.public_base_url}/plivo/{p.tenant_id}", dial["conversation_id"]
    headers = {"HsKind": "outbound", "HsConversation": cid}
    call = {
        "from": dial["from"],
        "to": ring_address(dial["agent_sip"]),
        "answer_url": f"{base}/answer?dial={cid}",
        "answer_method": "POST",
        "hangup_url": f"{base}/hangup?dial={cid}",
        "hangup_method": "POST",
        "ring_timeout": dial["agent_ring"],
        "sip_headers": ",".join(f"{k}={hs_header(v)}" for k, v in headers.items()),
    }
    try:
        r = rest.call(tenant_plivo(p.tenant_id), "POST", "Call/", **call)
    except httpx.HTTPError as e:
        _fail(dial, "failed_carrier", None)
        raise ApiError(503, "plivo_unavailable", "Plivo did not answer") from e
    if r.status_code == 429:
        _fail(dial, "failed_carrier", None)
        retry = r.headers.get("retry-after", "")
        raise ApiError(429, "carrier_rate_limited", "Plivo refused the dial", {"retry_after": int(retry or 1)})
    if not r.is_success:
        _fail(dial, "failed_carrier", None)
        raise rest.refused(r)
    _put(dial | {"request_uuid": r.json().get("request_uuid")}, cid)
    return {"conversation_id": cid, "status": "dialing_agent"}


def _fail(dial: dict, result: str, code: int | None) -> None:
    """The dial ended before the agent answered: call.failed, call.ended, and the conversation closes."""
    redis_client().delete(_key(dial["conversation_id"]))
    RoutingRedis().decrease_agents_capacity(
        dial["auth_id"], dial["agent_sip"], rc.CHANNEL_CALL, dial["conversation_number"]
    )
    extra = {"hangup_cause_code": str(code or ""), "result": result}
    failed = ConversationStates.outbound_contact_failed.name
    ccu.ChildConversationStateManagement(failed, dial["contact"], get_now(), extra_update_data_dict=extra).invoke()
    _ended(dial, result, code, "system")


def _ended(dial: dict, result: str, code: int | None, by: str) -> None:
    data = {"from": dial["from"], "to": dial["to"], "result": result, "queue_id": None, "hangup_cause": None}
    data |= {"hangup_cause_code": code, "duration_sec": None, "talk_sec": 0, "ended_by": by}
    ids = {"tenant_id": dial["tenant_id"], "conversation_id": dial["conversation_id"], "agent_id": dial["agent_id"]}
    events.emit("call.ended", data=data, **ids)


def cancel(p: Principal, call: Call) -> tuple[bool, str | None]:
    """POST /calls/{id}/hangup while the agent still rings: cancel the Make Call. Its hangup closes the call.
    (whether the agent was ringing, Plivo's error); a 404 means the dial already ended."""
    dial = dialed(call.conversation_id)
    if dial is None or dial.get("call_uuid"):
        return False, None
    if not (request_uuid := dial.get("request_uuid")):
        return True, None
    _body, status, err = pv.PlivoVoiceClient(*tenant_plivo(p.tenant_id)).request_cancel(request_uuid)
    return True, None if status == 404 else err


def answer(p: Principal, form: dict, dial: str | None) -> str:
    """The answer URL of the calls this module owns: an agent leg of POST /calls (`dial`), or a dial-out from one of
    the tenant's endpoints, which only agent_direct_dial allows (to an E.164 number, from the caller ID)."""
    if dial:
        found = dialed(dial, p.tenant_id)
        return REJECT if found is None or found.get("call_uuid") else _agent_answered(found, form["CallUUID"])
    from hs.voice.api import direct_dial

    user = str(form.get("From", "")).removeprefix("sip:").partition("@")[0]
    with core_engine.connect() as conn:
        agent_id = conn.execute(_ENDPOINT_AGENT, {"org": p.org_id, "user": user}).scalar()
    if agent_id is None or not direct_dial(p):
        logger.info(f"refused an endpoint dial-out tenant={p.tenant_id}")
        return REJECT
    try:
        found = _open(p, DialIn(agent_id=uuid.UUID(agent_id), to="+" + str(form.get("To", "")).removeprefix("+")), True)
    except ApiError as e:
        logger.info(f"refused an endpoint dial-out tenant={p.tenant_id}: {e.code}")
        return REJECT
    return _agent_answered(found, form["CallUUID"])


def _agent_answered(dial: dict, call_uuid: str) -> str:
    """The agent's leg answered: it becomes the call's contact, the engine's capacity, mapping and contact record
    are set for the run, and the agent enters `mpc_{run}`."""
    run, cid, auth_id = dial["run_id"], dial["conversation_id"], dial["auth_id"]
    dial |= {"call_uuid": call_uuid}
    _put(dial, cid, call_uuid)
    with core_engine.begin() as conn:
        conn.execute(_RECONTACT, {"call": call_uuid, "cid": cid, "old": dial["contact"]})
    engine = {"agent_sip_endpoint": dial["agent_sip"], "agent_uuid": dial["agent_id"], "auth_id": auth_id}
    engine |= {"flow_run_id": run, "to_number": dial["to"], "from_number": dial["from"]}
    engine["conversation_data"] = {"conversation_number": dial["conversation_number"]}
    if (err := controls.handle_outbound_call(call_uuid, engine)) is not None:
        logger.error(f"outbound answer: capacity or mapping failed call_uuid={call_uuid}: {err}")
        return REJECT
    contact = {"conversation_number": dial["conversation_number"], "flow_run_uuid": run}
    contact |= {"queue_and_route_run_uuid": run, "contact_channel": rc.CHANNEL_CALL, "contact_uuid": call_uuid}
    contact |= {"from_number": dial["to"], "to": dial["from"], "auth_id": auth_id}
    contact["auth_token"] = tenant_plivo(dial["tenant_id"])[1]
    RoutingRedis().store_contact_params(call_uuid, run, json.dumps(contact, separators=(",", ":")))
    if err := VoiceRedis().set_mpc_call_uuids(run, run, [call_uuid], 4 * 3600):
        logger.error(f"outbound answer: could not store the MPC legs call_uuid={call_uuid}: {err}")
    joined = ConversationStates.outbound_contact_agent_joined.name
    ccu.ChildConversationStateManagement(joined, call_uuid, get_now()).invoke()
    data = {"from": dial["from"], "to": dial["to"]}
    events.emit("call.started", tenant_id=dial["tenant_id"], conversation_id=cid, agent_id=dial["agent_id"], data=data)
    record = str(dial["record"]).lower()
    attrs = {
        "role": "Agent",
        "statusCallbackUrl": pv.mpc_status_callback_url(auth_id, call_uuid, run),
        "statusCallbackEvents": EVENTS,
        "startMpcOnEnter": "true",
        "enterSound": "none",
        "relayDTMFInputs": "true",
        "record": record,
        "recordParticipantTrack": record,
        "recordingCallbackUrl": pv.mpc_recording_callback_url(auth_id, call_uuid, run),
        "transcript": record,
        "transcriptionUrl": pv.mpc_transcription_callback_url(auth_id, call_uuid),
    }
    mpc = " ".join(f"{k}={quoteattr(v)}" for k, v in attrs.items())
    return f"<Response><MultiPartyCall {mpc}>mpc_{run}{hold_music()}</MultiPartyCall></Response>"


def hangup(p: Principal, form: dict, dial: str | None) -> tuple[str, str, dict] | None:
    """The hangup URL of the calls this module owns: an agent leg that never answered closes the call as
    cancelled. An answered one has no call-status callback of its own, so it returns the (call, run, callback) of
    the agent leaving for the caller to hand to the engine; Plivo's retries of it share one claim key."""
    found = dialed(dial, p.tenant_id) if dial else None
    if found is None:
        return None
    if not found.get("call_uuid"):
        code = str(form.get("HangupCauseCode", ""))
        _fail(found, "cancelled", int(code) if code.isdigit() else None)
        return None
    left = {"EventName": "Hangup", "Role": "agent", "CallUUID": found["call_uuid"], "To": found["agent_sip"]}
    left |= {"AnswerTime": form.get("AnswerTime", ""), "HangupTime": form.get("EndTime", "")}
    left |= {"PlivoHangupCauseCode": form.get("HangupCauseCode", ""), "PlivoHangupSource": form.get("HangupSource", "")}
    return found["call_uuid"], found["run_id"], left


def on_agent_join(call_uuid: str, run: str, dial: dict) -> str | None:
    """The agent is in the MPC: dial the lead in as its Customer; the MPC ends when either of them leaves."""
    record = dial["record"]
    body = {
        "role": "Customer",
        "from": dial["from"],
        "to": dial["to"],
        "ring_timeout": dial["ring_timeout"],
        "call_status_callback_url": pv.mpc_call_status_callback_url(dial["auth_id"], call_uuid, run),
        "call_status_callback_method": "POST",
        "status_callback_url": pv.mpc_status_callback_url(dial["auth_id"], call_uuid, run),
        "status_callback_method": "POST",
        "status_callback_events": EVENTS,
        "end_mpc_on_exit": True,
        "enter_sound": "none",
        "relay_dtmf_inputs": True,
        "record": record,
        "record_participant_track": record,
        "recording_callback_url": pv.mpc_recording_callback_url(dial["auth_id"], call_uuid, run),
        "recording_callback_method": "POST",
        "transcript": record,
        "transcription_url": pv.mpc_transcription_callback_url(dial["auth_id"], call_uuid),
    }
    client = pv.PlivoVoiceClient(*tenant_plivo(dial["tenant_id"]))
    return client.mpc_add_participant(pv.make_mpc_id(f"mpc_{run}"), body, raw_body=pv.compact_json(body))[2]


def on_customer_join(call_uuid: str, run: str, dial: dict, member_id: str) -> str | None:
    """The lead answered: call.answered, then the outbound consent, once."""
    answered = ConversationStates.outbound_contact_answered.name
    ccu.ChildConversationStateManagement(answered, call_uuid, get_now()).invoke()
    with core_engine.connect() as conn:
        consent = conn.execute(_CONSENT, {"auth_id": dial["auth_id"]}).scalar()
    if not consent or not redis_client().set(f"CONSENT:{call_uuid}", 1, nx=True, ex=DIAL_TTL):
        return None
    client, mpc = pv.PlivoVoiceClient(*tenant_plivo(dial["tenant_id"])), pv.make_mpc_id(f"mpc_{run}")
    if consent.get("audio_url"):
        return client.mpc_start_play_audio(mpc, member_id, {"url": consent["audio_url"]})[2]
    return client.mpc_start_speak(mpc, member_id, {k: v for k, v in consent.items() if v})[2]


def on_customer_failed(call_uuid: str, run: str, dial: dict, code: str) -> str | None:
    """The lead was not reached: call.failed with the cause's result and call.ended; the agent is let go."""
    failed = ConversationStates.outbound_contact_failed.name
    extra = {"hangup_cause_code": code}
    ccu.ChildConversationStateManagement(failed, call_uuid, get_now(), extra_update_data_dict=extra).invoke()
    _ended(dial, ccu.call_result(code), int(code) if code.isdigit() else None, "customer")
    return voice_hangup.end_mpc(f"mpc_{run}", *tenant_plivo(dial["tenant_id"]), call_uuid, run)
