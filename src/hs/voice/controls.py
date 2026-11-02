from contextlib import suppress

from sqlalchemy.exc import SQLAlchemyError

from hs.infra.db import SessionLocal, owned_session
from hs.infra.log import logger
from hs.plivo import voice as pv
from hs.routing import account_read as ar
from hs.routing import active_queue as aq
from hs.routing import constants as rc
from hs.routing.invitation_models import AgentAndContactMapping
from hs.routing.redis_store import RoutingRedis
from hs.voice import agent_leg as voice_agent_leg
from hs.voice import callbacks as voice_callbacks
from hs.voice import constants as vc
from hs.voice import hangup as voice_hangup
from hs.voice.redis_store import VoiceRedis

UNCHANGED = "the caller is already in that hold state"


def handle_hold_call(call_uuid: str, hold_value: bool) -> str | None:
    rr = RoutingRedis()
    mapping = AgentAndContactMapping.parse_redis(rr.get_contact_to_agent_mapping(call_uuid))
    if mapping is None:
        logger.error(f"handle_hold_call: failed to get agent mapping for calluuid call_uuid={call_uuid}")
        return "call not found"
    cp = rr.get_contact_parameters(call_uuid, mapping.qnr_run_uuid)
    if cp is None:
        logger.error("handle_hold_call: error in fetching call parameters from cache")
        return "contact params not found"
    auth_id, auth_token = ar.contact_pair(cp)
    participants, err = voice_agent_leg._get_participants(f"mpc_{mapping.qnr_run_uuid}", auth_id, auth_token)
    if err is not None:
        logger.error(f"handle_hold_call: error in getting participant list from mpc: {err}")
        return err
    customer = next((p for p in participants or [] if str(p.get("role", "")).lower() == vc.MPC_ROLE_CUSTOMER), None)
    if customer is None:
        logger.error(f"handle_hold_call: No customers in the MPC Call uuid={call_uuid}")
        return "no customers in the mpc"
    if bool(customer.get("hold")) == hold_value:
        return UNCHANGED
    member_id = str(customer.get("member_id", ""))
    return voice_hangup.hold_or_unhold_call(hold_value, call_uuid, mapping.qnr_run_uuid, member_id, auth_id, auth_token)


def end_mpc_calls(call_uuid: str, qnr_run_uuid: str, db=None, drop: bool = True) -> tuple[str, str | None]:
    logger.info(f"End MPC calls invoked call_uuid={call_uuid} qnr_run_uuid={qnr_run_uuid}")
    cp = RoutingRedis().get_contact_parameters(call_uuid, qnr_run_uuid)
    if cp is None:
        logger.error(f"error in fetching call parameters from cache call_uuid={call_uuid}")
        return "", rc.CACHE_MISS
    with owned_session(db) as (session, commit):
        try:
            return "", _end_mpc_calls(call_uuid, qnr_run_uuid, cp, session, drop)
        finally:
            commit()


def _emit_dropped(cp: dict, db) -> None:
    """call.ended for a caller who hung up while queued, in the transaction that closes its queue row."""
    from hs.conversations.store import ConversationParent, emit

    data = {"from": cp.get("from_number") or None, "to": cp.get("to") or None, "result": "abandoned"}
    data |= {"queue_id": cp.get("queue_uuid") or None, "hangup_cause": None, "hangup_cause_code": None}
    data |= {"duration_sec": None, "talk_sec": None, "ended_by": "customer"}
    if parent := ConversationParent.get_by_conversation_number(cp.get("conversation_number", "")):
        emit("call.ended", parent, data, session=db)


def drop_abandoned(call_uuid: str, qnr_run_uuid: str) -> str | None:
    """A caller who left the queue room before any agent took the call: its queue row closes as dropped and
    call.ended commits with it. A failed write changes neither and answers a retryable error."""
    rr = RoutingRedis()
    if rr.get_contact_to_agent_mapping(call_uuid) is not None:
        return None
    cp = rr.get_contact_parameters(call_uuid, qnr_run_uuid) or {}
    try:
        with SessionLocal.begin() as db:
            if entry_id := aq.delete_active_queue_entry(db, call_uuid, qnr_run_uuid, rc.CONTACT_STATE_DROPPED):
                _emit_dropped(cp, db)
    except SQLAlchemyError as e:
        logger.error(f"drop_abandoned: queue row left open for a retry call_uuid={call_uuid}: {e}")
        return f"{voice_callbacks._RETRYABLE_QUEUE_PERSISTENCE_PREFIX} queue row not closed"
    if entry_id:
        voice_callbacks._update_unanswered(call_uuid, rc.UNANSWERED_CALL_DROPPED, entry_id)
    return None


def _end_mpc_calls(call_uuid: str, qnr_run_uuid: str, cp: dict, db, drop: bool) -> str | None:
    """An unanswered contact's ringing offers end, and with `drop` it leaves the queue; an answered one has every
    leg hung up."""
    vr = VoiceRedis()
    run = (cp.get("flow_run_uuid", ""), cp.get("queue_and_route_run_uuid", ""))
    try:
        entry = aq.get_current_active_queue_entry_by_contact_uuid(db, call_uuid, qnr_run_uuid)
    except Exception:
        entry = None
    if entry is not None and RoutingRedis().get_contact_to_agent_mapping(call_uuid) is None:  # no agent took it yet
        vr.toggle_active_conference(qnr_run_uuid, "false", 0)
        if drop:
            drop_abandoned(call_uuid, qnr_run_uuid)
        # a caller who leaves mid-offer takes the ringing legs with them
        client = pv.PlivoVoiceClient(*ar.contact_pair(cp))
        for call_id in vr.get_mpc_call_uuids(*run):
            client.call_delete(call_id, quiet_not_found=True)
        return None
    try:
        call_ids = vr.get_mpc_call_uuids(*run)
    except Exception as ge:
        logger.error(f"error in fetching mpc call uuid from cache: {ge}")
        return str(ge)
    try:
        call_ids = [*call_ids, *vr.get_transfer_mpc_call_uuids(qnr_run_uuid)]
    except Exception as te:
        logger.error(f"error in fetching transfer mpc call uuid from cache: {te}")
    serr = vr.set_mpc_callback_event(call_uuid, qnr_run_uuid, vc.MPC_END_INITIATED, "")
    if serr is not None:
        logger.error(f"_end_mpc_calls: unable to set MPC end initialized event in cache CallUUID={call_uuid}: {serr}")
    auth_id, auth_token = ar.contact_pair(cp)
    client = pv.PlivoVoiceClient(auth_id, auth_token) if auth_id and auth_token else None
    if client is None:
        logger.error(f"error initialising plivo client call uuid={call_uuid}")
    errors = []
    for call_id in call_ids:
        if client is None:
            raise RuntimeError("no plivo client to end the MPC calls")
        _b, status, derr = client.call_delete(call_id, quiet_not_found=True)
        if derr is not None and status != 404:  # a 404 leg already ended: every leg the run dialed is listed
            logger.error(f"deleting mpc calls failed call id={call_id}: {derr}")
            errors.append(f"failed to delete MPC call {call_id}: {derr}")
    if errors:
        # Cleanup is best-effort across all legs, but the callback must be retried while any delete failed.
        return pv.worst_error(errors)
    logger.info(f"ended mpc calls successfully call uuid={call_uuid}")
    return None


def handle_outbound_call(call_uuid: str, params: dict) -> str | None:
    """Raises TypeError / KeyError on a malformed conversation_data ."""
    rr = RoutingRedis()
    conversation_data = params.get("conversation_data")
    if not isinstance(conversation_data, dict):
        raise TypeError("conversation_data is not a map")
    conversation_number = conversation_data["conversation_number"]
    if not isinstance(conversation_number, str):
        raise TypeError("conversation_number is not a string")

    def get(key: str) -> str:
        return params.get(key) or ""

    agent_sip = get("agent_sip_endpoint")
    with suppress(Exception):
        rr.delete_reverse_routing_lock(agent_sip, conversation_number, rc.CHANNEL_CALL)
    _code, cerr = rr.increase_agent_capacity(get("auth_id"), agent_sip, rc.CHANNEL_CALL, 1, conversation_number)
    if cerr is not None:
        logger.error(f"Error in increasing agent capacity contact_uuid={call_uuid}: {cerr}")
        return cerr
    mapping = AgentAndContactMapping(
        agent_sip_endpoint=agent_sip,
        from_number=get("from_number"),
        agent_uuid=get("agent_uuid"),
        agent_call_uuid=call_uuid,
        flow_run_uuid=get("flow_run_id"),
        qnr_run_uuid=get("flow_run_id"),
        to_number=get("to_number"),
    )
    merr = rr.set_contact_to_agent_mapping(call_uuid, mapping.to_json(), vc.EXPIRY_DURATION_IN_HOURS_CALL * 3600)
    if merr is not None:
        logger.error(f"Error while storing contact to agent mapping for the outbound call contactUUID={call_uuid}")
    return merr
