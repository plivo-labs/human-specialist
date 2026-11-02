import contextlib
import re

from sqlalchemy.exc import SQLAlchemyError

from hs.admin.account import RoutingSettings
from hs.conversations.models import ConvChunkInsertByContactUUID, ConvGetByContactUUID, NonQNRChunkInsert
from hs.infra.counters import MetricName, increase_counter
from hs.infra.db import owned_session
from hs.infra.log import logger
from hs.plivo import voice as pv
from hs.routing import account_read as ar
from hs.routing import active_queue as aq
from hs.routing import constants as rc
from hs.routing import reverse as rrv
from hs.routing import rona
from hs.routing import sticky as routing_sticky
from hs.routing.invitation_models import AgentAndContactMapping
from hs.routing.redis_store import CAPACITY_ALREADY_CLEAR_ERRS, CAPACITY_OLD_CONTACT_NOT_FOUND, RoutingRedis
from hs.voice import agent_leg as voice_agent_leg
from hs.voice import callbacks as voice_callbacks
from hs.voice import constants as vc
from hs.voice.redis_store import VoiceRedis


def _spawn(fn, *args) -> None:
    # Detached: the worker needs its own DB session, sharing the request one races its commit.
    from hs.infra.bg_spawn import spawn_detached

    spawn_detached(fn, *args, log_context="voice_hangup")


CALL_LEG_PERSISTENCE_RETRYABLE = "call leg persistence retryable"
VOICE_CALLBACK_RETRYABLE_PREFIX = "retryable voice callback:"
TERMINAL_CALLBACK_INVALID = "invalid terminal callback"
RRLOCK_ACQUIRE_FAILED = "failed to acquire reverse routing lock"
RING_TIMEOUT = "6010"
_HANDLED = "successfully handled hangup"
_PHONE_MEMBER = re.compile(r"\+?\d+")
_EXTERNAL_TRANSFERS = (rc.TRANSFER_TYPE_EXTERNAL, rc.TRANSFER_TYPE_WARM_EXTERNAL)
_AGENT_TRANSFERS = (rc.TRANSFER_TYPE_AGENT, rc.TRANSFER_TYPE_TEAM, *_EXTERNAL_TRANSFERS)
# Plivo cause code -> what became of an offer whose agent leg ended unanswered: 3020 Rejected,
# 9000 Lost Race, 6010 Invite Timeout (a RONA strike). Any other is a drop-off: caller_left once the caller is out
# of the room, else a miss without a strike.
_OFFER_OUTCOME = {
    "3020": ("offer.rejected", {}),
    "9000": ("offer.withdrawn", {"reason": "taken"}),  # another agent won the race
    "": ("offer.withdrawn", {"reason": "caller_left"}),  # picked up after the caller left
}
_Reply = tuple[str | None, str | None]
# A number transfer is stored with mode both; its events name the mode the request asked for.
_NUMBER_MODE = {rc.TRANSFER_TYPE_WARM_EXTERNAL: rc.TRANSFER_MODE_WARM, rc.TRANSFER_TYPE_EXTERNAL: rc.TRANSFER_MODE_COLD}


def _event_mode(transfer_info: dict | None) -> str:
    info = transfer_info or {}
    return _NUMBER_MODE.get(info.get("transfer_type", "")) or info.get("transfer_mode") or rc.TRANSFER_MODE_COLD


def retryable_voice_callback_error(error: str) -> str:
    """Tag an infrastructure failure that is safe for the provider to retry."""
    detail = str(error or "voice callback dependency unavailable")
    return detail if is_retryable_voice_callback_error(detail) else f"{VOICE_CALLBACK_RETRYABLE_PREFIX} {detail}"


def provider_voice_callback_error(error: str, status_code: int, *, retry_safe: bool = False) -> str:
    """Only replay-safe operations (reads, desired-state writes) may ask Plivo to retry: a mutating request that
    failed with status 0/5xx may already have been applied."""
    if retry_safe and (not status_code or status_code in (408, 425, 429) or status_code >= 500):
        return retryable_voice_callback_error("provider voice API unavailable")
    return str(error)


def is_retryable_voice_callback_error(error: str | None) -> bool:
    return bool(
        error and (error == CALL_LEG_PERSISTENCE_RETRYABLE or error.startswith(VOICE_CALLBACK_RETRYABLE_PREFIX))
    )


def reconcile_terminal_call_leg(call_uuid: str, params: dict, is_mpc: bool) -> str | None:
    """Synchronously finalize a terminal call leg, creating it if callbacks raced."""
    from hs.infra.context import set_db_session_rollback
    from hs.voice import call_legs as call_management
    from hs.voice.call_models import CallLegInsertModel
    from hs.voice.monitor import _seed_context

    p = params or {}
    if is_mpc:
        leg_uuid, role, member = call_uuid, None, None
        start, end = p.get("mpc_start_time", "") or p.get("mpc_creation_time", ""), p.get("mpc_end_time", "")
    else:
        leg_uuid, member = p.get("participant_call_uuid", ""), p.get("member_address", "")
        role = str(p.get("participant_role", "") or "").strip().lower()
        start, end = p.get("participant_join_time", ""), p.get("participant_exit_time", "")
    if not leg_uuid:
        return f"{TERMINAL_CALLBACK_INVALID}: provider leg UUID is required"
    if not is_mpc and not role:
        return f"{TERMINAL_CALLBACK_INVALID}: participant role is required"
    _seed_context({})
    response = None
    try:
        leg = CallLegInsertModel(
            call_uuid=call_uuid,
            call_leg_uuid=leg_uuid,
            member_address=member,
            start_time=start,
            end_time=end,
            role=role,
        )
        response = call_management.finalize_call_leg(leg, is_parent=is_mpc)
        if not response.error and response.status_code in (200, 201):
            return CALL_LEG_PERSISTENCE_RETRYABLE if _commit_session() is not None else None
        failed = f"persistence failed: {response.error or response.msg}"
    except Exception as e:
        failed = f"persistence raised: {e}"
    set_db_session_rollback()
    _commit_session()
    logger.error(f"terminal call-leg {failed} parent={call_uuid} leg={leg_uuid}")
    if response is not None and response.error in (
        call_management.MSG_CREATE_CALL_LEG_ERROR_ROLE,
        call_management.MSG_CREATE_CALL_LEG_ERROR_MEMBER_ADDRESS,
        call_management.MSG_CREATE_CALL_LEG_ERROR_ACCOUNT_ORG_MAPPING,
        call_management.MSG_CALL_LEG_FOREIGN,
    ):
        return f"{TERMINAL_CALLBACK_INVALID}: {response.error}"
    return CALL_LEG_PERSISTENCE_RETRYABLE


def end_mpc(mpc_name: str, auth_id: str, auth_token: str, call_uuid: str = "", qnr_run_uuid: str = "") -> str | None:
    if not auth_id or not auth_token:
        return "missing credentials"

    def lookup_failed(exc):
        logger.warning(f"end_mpc: terminal cache proof lookup failed call_uuid={call_uuid}: {exc}")

    # A missing conference is the expected answer only once MPCEnd was recorded for this exact call and QNR run.
    vr = VoiceRedis()
    proven = voice_callbacks.has_exact_mpc_end_proof(call_uuid, qnr_run_uuid, vr, lookup_failed)
    if call_uuid and qnr_run_uuid:  # controls read the room as ending from now on
        vr.set_mpc_callback_event(call_uuid, qnr_run_uuid, vc.MPC_END_INITIATED, "")
    client = pv.PlivoVoiceClient(auth_id, auth_token)
    _body, status, err = client.mpc_stop(pv.make_mpc_id(friendly_name=mpc_name), quiet_not_found=proven)
    return None if status == 404 and proven else err


def _customer_member(participants) -> dict | None:
    return next((m for m in participants or [] if str(m.get("role", "")).lower() == vc.MPC_ROLE_CUSTOMER), None)


def get_conversation_number_from_contact_uuid(contact_uuid: str, rr: RoutingRedis) -> tuple[str, str | None]:
    mapping = rr.get_contact_to_agent_mapping(contact_uuid)
    cp = mapping and rr.get_contact_parameters(contact_uuid, mapping.get("QnrRunUuid") or "")
    return ("", rc.CACHE_MISS) if not cp else (cp.get("conversation_number") or "", None)


def hold_or_unhold_call(
    hold_value: bool, call_uuid: str, qnr_run_uuid: str, member_id: str, auth_id: str, auth_token: str
) -> str | None:
    _body, status, err = pv.PlivoVoiceClient(auth_id, auth_token).mpc_update_participant(
        pv.make_mpc_id(friendly_name=f"mpc_{qnr_run_uuid}"), member_id, {"hold": hold_value}
    )
    if err is None:
        return None
    logger.error(f"hold_or_unhold_call: error updating participant call_uuid={call_uuid} member={member_id}: {err}")
    return provider_voice_callback_error(err, status, retry_safe=True)


def _get_agent_name_by_uuid(agent_uuid: str, auth_id: str) -> tuple[str | None, str | None]:
    agent = ar.get_agent_by_uuid(agent_uuid, auth_id)
    return (agent.name, None) if agent else (None, rc.NOT_FOUND)


def handle_call_recording_event(call_uuid: str, pause_recording: bool) -> str | None:
    rr = RoutingRedis()
    mapping = AgentAndContactMapping.parse_redis(rr.get_contact_to_agent_mapping(call_uuid))
    if mapping is None:
        return "call not found"
    if (cp := rr.get_contact_parameters(call_uuid, mapping.qnr_run_uuid)) is None:
        return "contact params not found"
    signing = ar.contact_pair(cp)
    client = pv.PlivoVoiceClient(*signing)
    mpc_name = "mpc_" + mapping.qnr_run_uuid
    mpc_id = pv.make_mpc_id(friendly_name=mpc_name)
    participants, err = voice_agent_leg._get_participants(mpc_name, *signing)
    if err is not None:
        logger.error(f"handle_call_recording_event: participants unavailable call_uuid={call_uuid}: {err}")
        return err
    members = [str(p.get("member_id", "")) for p in participants or []]

    def run(what, call, fallback=None):
        # a failing resume falls back to a fresh start
        err = call()[2]
        if err is not None and fallback is not None:
            err = fallback()[2]
        if err is not None:
            logger.warning(f"handle_call_recording_event: error while {what} call_uuid={call_uuid}: {err}")
        return err

    if pause_recording:
        err = run("pausing recording", lambda: client.mpc_pause_recording(mpc_id))
        for m in members:
            err = err or run("pausing participant recording", lambda: client.mpc_pause_participant_recording(mpc_id, m))
        return err
    start_body = {
        "recording_callback_url": pv.mpc_recording_callback_url(
            cp.get("auth_id", ""), cp.get("contact_uuid", ""), cp.get("queue_and_route_run_uuid", "")
        ),
        "recording_callback_method": "POST",
        "transcript": True,
        "transcription_url": pv.mpc_transcription_callback_url(cp.get("auth_id", ""), cp.get("contact_uuid", "")),
    }
    err = run(
        "resuming recording",
        lambda: client.mpc_resume_recording(mpc_id),
        lambda: client.mpc_start_recording(mpc_id, start_body),
    )
    for m in members:
        err = err or run(
            "starting participant recording",
            lambda: client.mpc_resume_participant_recording(mpc_id, m),
            lambda: client.mpc_start_participant_recording(mpc_id, m, start_body),
        )
    return err


def _commit_session() -> str | None:
    from hs.infra.context import context_db_commit, get_db_session, should_commit_db_session

    try:
        if should_commit_db_session():
            get_db_session().commit()
        else:
            get_db_session().rollback()
            context_db_commit.set(True)
        return None
    except Exception as e:
        # A failed commit leaves the session unusable until rollback.
        with contextlib.suppress(Exception):
            get_db_session().rollback()
        context_db_commit.set(True)
        logger.error(f"voice_hangup: session boundary commit failed: {e}")
        return str(e)


def _active_child(contact_uuid: str) -> dict | None:
    from hs.conversations import management as cm
    from hs.routing.invitation import _ensure_request_context

    try:
        _ensure_request_context()
        resp = cm.get_active_child_for_contact_uuid(ConvGetByContactUUID(contact_uuid=contact_uuid))
        if not resp.error and (resp.status_code or 200) == 200 and resp.data is not None:
            _commit_session()
            return resp.data.model_dump()
        logger.error(f"active child read non-200: {resp.status_code} {resp.error}")
    except Exception:
        logger.error("active child read failed", exc_info=True)
    return None


def _write_chunk(label: str, call) -> None:
    """A chunk write on the request session: committed, or rolled back. Best effort: the terminal leg is the fact."""
    from hs.infra.context import set_db_session_rollback
    from hs.routing.invitation import _ensure_request_context

    try:
        _ensure_request_context()
        resp = call()
        if not resp.error:
            _commit_session()
            return
        logger.error(f"{label} chunk not created: {resp.status_code} {resp.error}")
    except Exception:
        logger.error(f"{label} chunk failed", exc_info=True)
    set_db_session_rollback()
    _commit_session()


def _signal_chunk(contact_uuid: str, description: str, identifier: str, id_type: str, message: str) -> None:
    _spawn(_signal_chunk_committed, contact_uuid, description, identifier, id_type, message)


def _signal_chunk_committed(contact_uuid: str, description: str, identifier: str, id_type: str, message: str) -> None:
    from hs.conversations.transfer import signal_chunk

    signal_chunk(contact_uuid, rc.CHANNEL_CALL, description, identifier, id_type, message)
    _commit_session()


def _delete_secondary(payload: dict, member: str) -> str | None:
    from hs.conversations.transfer import remove_secondary

    err = remove_secondary(**payload)
    err = _commit_session() or err
    if err is not None:
        logger.error(f"failed to delete secondary agent for the conversation sip={member}: {err}")
    return err


def handle_reverse_routing_and_agent_capacity(
    agent_sip: str, channel: str, conversation_id: str, db=None
) -> tuple[str, str | None]:
    if _PHONE_MEMBER.fullmatch(str(agent_sip or "")):
        # An external transfer leg is no agent: no aom row and no capacity set, so nothing to reverse route.
        return "failed to run reverse routing", rc.NOT_FOUND
    rr = RoutingRedis()
    if not rr.acquire_reverse_routing_lock_bool(agent_sip, conversation_id, channel):
        return "handled reverse routing and agent capacity", RRLOCK_ACQUIRE_FAILED
    try:
        return _reverse_route(rr, agent_sip, channel, conversation_id, db)
    finally:
        # Always released, or it blocks every retry for this (agent, conversation) for its 1 h TTL.
        try:
            rr.delete_reverse_routing_lock(agent_sip, conversation_id, channel)
        except Exception as e:
            logger.error(f"reverse routing: lock not released agent={agent_sip} conversationId={conversation_id}: {e}")


def _reverse_route(rr: RoutingRedis, agent_sip: str, channel: str, conversation_id: str, db):
    """Hand the agent's freed call slot to its next queued contact, or release the slot when there is none."""
    with owned_session(db) as (db, commit):
        contact, _err = rrv.get_contact(agent_sip, [channel], db)
        commit()
    cp = rr.get_contact_parameters(contact.contact_uuid, contact.qnr_run_uuid) if contact is not None else None
    if cp is None:
        org = ar.get_org_details_for_agent_sip(agent_sip)
        if org is None:
            logger.error(f"reverse routing: org authid not found agent={agent_sip}")
            return "failed to run reverse routing", rc.NOT_FOUND
        derr = rr.decrease_agents_capacity(org.auth_id, agent_sip, rc.CHANNEL_CALL, conversation_id)
        # Already clear: the synchronous wrap-up released it, so the async re-run is a quiet no-op.
        return ("capacity already clear", None) if derr in CAPACITY_ALREADY_CLEAR_ERRS else ("", derr)
    is_transfer = (rr.get_transfer_in_progress_info(cp.get("contact_uuid", "")) or {}).get("transfer_type") == (
        rc.TRANSFER_TYPE_FLOW
    )
    kind = f"{rc.REVERSE_ROUTING} {rc.TRANSFER_TYPE_FLOW}" if is_transfer else rc.REVERSE_ROUTING
    auth_id, new_conversation = cp.get("auth_id", ""), cp.get("conversation_number", "")
    settings = RoutingSettings.get_org_routing_settings(auth_id)
    capacity = (getattr(settings, "capacity", None) or {}).get(rc.CHANNEL_CALL, 0)
    uerr = rr.update_agents_capacity(auth_id, agent_sip, rc.CHANNEL_CALL, conversation_id, new_conversation, capacity)
    if uerr == CAPACITY_OLD_CONTACT_NOT_FOUND:
        # The release half was a no-op (a racing hangup already released it) and the Lua aborts the whole swap:
        # recover with an add-only acquire; capacity still gates over-allocation.
        uerr = rr.increase_agent_capacity(auth_id, agent_sip, rc.CHANNEL_CALL, capacity, new_conversation)[1]
    if uerr is not None:
        logger.error(f"reverse routing: capacity not updated agent={agent_sip} new={new_conversation}: {uerr}")
        return "", uerr
    try:
        voice_agent_leg.add_agents_to_mpc(cp, [agent_sip], "", is_transfer, kind, "")
    except Exception as e:
        logger.error(f"reverse routing: mpc not initiated contact uuid={cp.get('contact_uuid', '')}: {e}")
        primary_error = str(e) or "unable to initiate mpc contact"
        try:
            derr = rr.decrease_agents_capacity(auth_id, agent_sip, rc.CHANNEL_CALL, new_conversation)
        except Exception as cleanup_error:
            return "", f"{primary_error}; capacity rollback raised: {cleanup_error}"
        # The rollback is compensation, not the result: the add failure keeps the callback retryable.
        if derr is not None and derr not in CAPACITY_ALREADY_CLEAR_ERRS:
            return "", f"{primary_error}; capacity rollback failed: {derr}"
        return "", primary_error
    return "handled reverse routing and agent capacity", None


def emit_offer_outcome(call_uuid: str, qnr_run_uuid: str, agent: str, cause_code: str, leg_offer: str = "") -> None:
    """The outcome of the offer the leg carried (its HsOffer header): by the time a hangup lands, the run may
    have offered the same agent again. The header is caller input: one that names no offer this run made this
    agent falls back to the run's current offer."""
    from hs.conversations.store import claim_offer_outcome, emit_by_number

    rr = RoutingRedis()
    leg_offer = voice_agent_leg.decode_sip_header_value(leg_offer)[0]
    offer_id = leg_offer if rr.is_offer_of(qnr_run_uuid, agent, leg_offer) else rr.get_offer(qnr_run_uuid, agent)
    if not offer_id or not (aom := ar.get_aom_for_sip_endpoint(agent)):
        return None
    if (cp := rr.get_contact_parameters(call_uuid, qnr_run_uuid)) is None:
        logger.error(f"emit_offer_outcome: contact parameters not found call_uuid={call_uuid}")
        return None
    if not claim_offer_outcome(offer_id, agent):
        return None
    number = cp.get("conversation_number", "")
    if cause_code == RING_TIMEOUT:  # a RONA strike: offer.missed carries its count
        return rona.missed(agent, offer_id, number)
    if cause_code not in _OFFER_OUTCOME and not _caller_left(call_uuid, qnr_run_uuid):
        return rona.missed(agent, offer_id, number, strike=False)
    event, data = _OFFER_OUTCOME.get(cause_code, _OFFER_OUTCOME[""])
    emit_by_number(event, number, {"offer_id": offer_id, **data}, aom)


def _caller_left(call_uuid: str, qnr_run_uuid: str) -> bool:
    """The caller is out of the room this ring was for: the room ended, or the caller's own exit closed its queue
    row, whichever callback came first. A read that fails proves neither, so the ring reads as missed."""
    if VoiceRedis().get_mpc_callback_event(call_uuid, qnr_run_uuid, vc.MPC_END_EVENT)[0]:
        return True
    try:
        with owned_session(None) as (db, _):
            return aq.left_unanswered(db, call_uuid, qnr_run_uuid)
    except SQLAlchemyError as e:
        logger.error(f"_caller_left: queue row unreadable call_uuid={call_uuid}: {e}")
        return False


def handle_agent_hangup_event(call_uuid: str, qnr_run_uuid: str, params: dict, db) -> _Reply:
    """An agent / supervisor / losing-invite leg hung up: fan out the leave handling, then free the agent's
    call capacity, reverse-routing a queued contact to them unless the hangup cause says not to."""
    rr, vr, p = RoutingRedis(), VoiceRedis(), params or {}
    to, role = p.get("to", ""), str(p.get("role", "") or "").strip().lower()
    cause_code = p.get("plivo_hangup_cause_code", "")
    unanswered = not str(p.get("answer_time", "") or "").strip()
    from hs.voice import monitor, outbound

    if role == vc.MPC_ROLE_CUSTOMER and (dial := outbound.dialed(call_uuid)):
        # the lead of a dial: unreached, the call fails; answered, its exit ends the MPC
        err = outbound.on_customer_failed(call_uuid, qnr_run_uuid, dial, cause_code) if unanswered else None
        return "handled the lead's hangup", err
    if role == vc.MPC_ROLE_AGENT and unanswered:
        _spawn(emit_offer_outcome, call_uuid, qnr_run_uuid, to, cause_code, p.get("offer_id", ""))
    transfer_info = rr.get_transfer_in_progress_info(call_uuid) or {}
    transfer_type = transfer_info.get("transfer_type", "") or ""
    mapping = AgentAndContactMapping.parse_redis(rr.get_contact_to_agent_mapping(call_uuid))
    supervisor_mapping, reason = None, ""
    if mapping is None:
        reason = "agent mapping not found"
    elif mapping.agent_sip_endpoint != to:
        added = str(p.get("add_participant_header", "") or "").strip().lower() == vc.HEADER_PARTICIPANT_EXISTS_VALUE
        # Parallel invites leave unanswered losing legs once another agent wins: ordinary exits, not transfer
        # secondaries, whose cleanup would report a false transfer error and could target the winner.
        losing_invite = role == vc.MPC_ROLE_AGENT and unanswered and not transfer_type and not added
        reason = "losing invitation hangup" if losing_invite else "secondary agent hangup"
        sup, sup_err = vr.get_call_to_supervisor_mapping(call_uuid)
        if sup_err is None and sup is not None and sup.get("supervisor_sip") == to and role == vc.MPC_ROLE_SUPERVISOR:
            supervisor_mapping, reason, sup_uuid = sup, "supervisor hangup", sup.get("supervisor_uuid", "")
            call_mapping, cm_err = vr.get_supervisor_to_call_mapping(sup_uuid)
            if cm_err is None and call_mapping is not None and call_mapping.get("call_uuid") != call_uuid:
                reason = ""  # the supervisor already monitors another call
            voice_callbacks.mark_unsafe_callback_effect(db, "supervisor_leave_external_effects")
            # Already deleted, or monitoring another call: the errors are benign.
            vr.delete_supervisor_to_call_mapping(sup_uuid, call_uuid)
            vr.delete_call_to_supervisor_mapping(call_uuid, sup_uuid)
            # A supervisor who hung up their own phone is off the conversation too, or a rejoin reads them present.
            # Marked first, as a leave is: a join of this leg still in flight then takes itself off after its commit.
            leg = p.get("call_uuid", "")
            if merr := monitor.mark_leave_requested(call_uuid, leg):
                logger.error(f"hangup: a hung-up supervisor leg not marked as left leg={leg}: {merr}")
            dropped = (call_uuid, leg, sup.get("conversation_number", ""), to, sup_uuid)
            voice_callbacks.stage_or_run(db, monitor.drop_supervisor_leg, *dropped, spawn=_spawn)
        elif not losing_invite:
            voice_callbacks.mark_unsafe_callback_effect(db, "secondary_agent_leave_fanout")
            secondary = (call_uuid, qnr_run_uuid, p, mapping.agent_sip_endpoint, dict(transfer_info))
            _spawn(handle_secondary_agent_leave, *secondary)
    else:
        conv = _active_child(call_uuid)
        if conv is None:
            reason = "conversationNotFound"
        elif conv.get("primary_agent") == "":
            reason = "conversationNotStarted"
        elif conv.get("update_capacity_on_resolve") is True:
            reason = "normal hangup"
        voice_callbacks.mark_unsafe_callback_effect(db, "primary_agent_leave_fanout")
        _spawn(handle_primary_agent_leave, call_uuid, qnr_run_uuid, p, mapping, dict(transfer_info))
    if reason == "":
        return _HANDLED, None
    reverse = cause_code not in vc.HANGUP_CODES_NO_REVERSE_ROUTING
    if not reverse:
        if transfer_type == rc.TRANSFER_TYPE_WARM_EXTERNAL or _PHONE_MEMBER.fullmatch(str(to or "")):
            return _HANDLED, None
        voice_callbacks.mark_unsafe_callback_effect(db, "hangup_capacity_and_events")
        if (org := ar.get_org_details_for_agent_sip(to)) is None:
            logger.error(f"hangup: org not found agentSipEndpoint={to}")
            return None, rc.NOT_FOUND
    if reason == "supervisor hangup":
        conversation_number = supervisor_mapping.get("conversation_number", "")
    elif (cp := rr.get_contact_parameters(call_uuid, qnr_run_uuid)) is None:
        logger.error(f"hangup: contact parameters not in cache call_uuid={call_uuid}")
        return None, rc.CACHE_MISS
    else:
        conversation_number = cp.get("conversation_number", "")
    if not reverse:
        derr = rr.decrease_agents_capacity(org.auth_id, to, rc.CHANNEL_CALL, conversation_number)
        if derr in CAPACITY_ALREADY_CLEAR_ERRS:
            return _HANDLED, None
        if derr is not None:
            logger.error(f"hangup: capacity not decreased agentSipEndpoint={to}: {derr}")
            return None, retryable_voice_callback_error(derr)
        return _HANDLED, None
    voice_callbacks.mark_unsafe_callback_effect(db, "hangup_reverse_routing")
    res, rerr = handle_reverse_routing_and_agent_capacity(to, rc.CHANNEL_CALL, conversation_number, db)
    if rerr == RRLOCK_ACQUIRE_FAILED:
        return res, None
    if rerr == rc.NOT_FOUND and _PHONE_MEMBER.fullmatch(str(to or "")):
        return _HANDLED, None
    if rerr is not None and rerr.startswith("known_error:"):
        # Queue exhaustion / no valid contact is deterministic: untagged, so the claim is kept, not retried.
        increase_counter(MetricName.REVERSE_ROUTING_FAILURE, {"output_state": "hangup_reverse_routing"})
        return None, rerr
    if rerr is not None:
        logger.error(f"hangup: reverse routing failed: {rerr}")
        # A non-phone member with no organization mapping is a permanent data defect: a retry can't repair it.
        return None, rerr if rerr == rc.NOT_FOUND else retryable_voice_callback_error(rerr)
    return res, None


def handle_primary_agent_leave(
    call_uuid: str, qnr_run_uuid: str, params: dict, agent_mapping: AgentAndContactMapping, transfer_info: dict
) -> str | None:
    """The primary agent left. With nobody to take over the MPC ends; with a transfer target waiting
    (the to-be-primary agent) the conversation moves to them: transfer child, mapping, sticky routing,
    customer unhold, transfer chunks."""
    rr, vr, p = RoutingRedis(), VoiceRedis(), params or {}
    to, mpc_name = p.get("to", ""), f"mpc_{qnr_run_uuid}"
    transfer_type = (transfer_info or {}).get("transfer_type", "") or ""
    cp = rr.get_contact_parameters(call_uuid, qnr_run_uuid)
    if cp is None or p.get("plivo_hangup_cause_code", "") == vc.HANGUP_CODE_MPC_ENDED:  # no customer left either
        return None
    to_be_primary = AgentAndContactMapping.parse_redis(rr.get_to_be_primary_agent(agent_mapping.qnr_run_uuid))
    if to_be_primary is None or to_be_primary.agent_sip_endpoint == to:
        err = end_mpc(mpc_name, *ar.contact_pair(cp), call_uuid, qnr_run_uuid)
        if err is not None:
            logger.error(f"handle_primary_agent_leave: could not end MPC qnrRunUUID={qnr_run_uuid}: {err}")
        return err
    qnr_config, cfg_err = routing_sticky.parse_queue_and_route_config(
        rr.get_queue_and_route_config(cp.get("flow_run_uuid", ""), qnr_run_uuid)
    )
    if qnr_config is None and agent_mapping.is_inbound:  # an outbound call has no queue config
        logger.error(f"handle_primary_agent_leave: no queue and route config CallUUID={call_uuid}: {cfg_err}")
        return None
    recording_enabled, _pause_play, rec_err = ar.get_organisation_record_value(
        cp.get("auth_id", ""), agent_mapping.is_inbound and cp.get("inbound", True)
    )
    if rec_err is not None:
        logger.error(f"handle_primary_agent_leave: call record value unavailable: {rec_err}")
        return None
    err = handle_call_recording_event(call_uuid, not recording_enabled)
    if err is not None:
        logger.error(f"handle_primary_agent_leave: recording event failed: {err}")
    if transfer_type == "":
        return None
    # Not room_ended(): a failed read counts as not ended, so a Redis blip does not strand the hand-over.
    if any(vr.get_mpc_callback_event(call_uuid, qnr_run_uuid, e)[0] for e in (vc.MPC_END_INITIATED, vc.MPC_END_EVENT)):
        return None
    from hs.conversations.transfer import transfer_child

    child_type = rc.TRANSFER_TYPE_EXTERNAL if transfer_type == rc.TRANSFER_TYPE_WARM_EXTERNAL else transfer_type
    mode = _event_mode(transfer_info)
    _child_uuid, terr = transfer_child(call_uuid, child_type, rc.CHANNEL_CALL, to_be_primary.agent_sip_endpoint, mode)
    _commit_session()
    if terr is not None:
        logger.error(
            f"handle_primary_agent_leave: transfer conversation not created sip={to_be_primary.agent_sip_endpoint}: {terr}"
        )
    else:
        to_be_primary.previous_primary_agent = to
        ttl = vc.EXPIRY_DURATION_IN_HOURS_CALL * 3600
        if rr.set_contact_to_agent_mapping(call_uuid, to_be_primary.to_json(), ttl) is not None:
            logger.error("handle_primary_agent_leave: contact to agent mapping not updated")
            return None
        if qnr_config and qnr_config.sticky_routing_type != rc.STICKY_NO_STICKY_ROUTING:
            new_agent = to_be_primary.agent_uuid
            in_assignment, sticky_err = routing_sticky.is_agent_in_routing_assignment(qnr_config, new_agent)
            sticky = (new_agent, agent_mapping.from_number, qnr_config, cp.get("auth_id", ""))
            if sticky_err is not None or (in_assignment and routing_sticky.set_inbound_sticky_mapping(*sticky)):
                logger.error(f"handle_primary_agent_leave: sticky routing not updated toBePrimaryAgent={new_agent}")
    signing = ar.contact_pair(cp)
    customer = _customer_member(voice_agent_leg._get_participants(mpc_name, *signing)[0])
    if customer is not None:
        hold_or_unhold_call(False, call_uuid, qnr_run_uuid, str(customer.get("member_id", "")), *signing)
    handle_primary_agent_leave_transfer_chunks(call_uuid, agent_mapping.agent_uuid, cp, transfer_info, to_be_primary)
    rr.delete_transfer_in_progress(call_uuid)
    if transfer_type in _EXTERNAL_TRANSFERS:
        from hs.voice.monitor import hangup_monitored_conversation

        merr = hangup_monitored_conversation(
            {"auth_id": cp.get("auth_id", "")}, cp.get("conversation_number", ""), mpc_name
        )
        if merr is not None:
            logger.error(f"handle_primary_agent_leave: error in dropping supervisor: {merr}")


def handle_primary_agent_leave_transfer_chunks(
    call_uuid: str, agent_uuid: str, cp: dict, transfer_info: dict, to_be_primary: AgentAndContactMapping
) -> None:
    transfer_type = (transfer_info or {}).get("transfer_type", "") or ""

    def name_of(uuid) -> str | None:
        name, err = _get_agent_name_by_uuid(uuid, cp.get("auth_id", ""))
        if err is not None:
            logger.error(f"transfer chunks: agent not found uuid={uuid}")
        return None if err is not None else name or ""

    if (transfer_info or {}).get("transfer_mode") == "warm":
        if (name := name_of(agent_uuid)) is not None:
            message = rc.AGENT1_LEAVING_MSG.replace("<name>", name, 1)
            _signal_chunk(call_uuid, rc.SIGNAL_AGENT1_LEAVING, agent_uuid, rc.TRANSFER_TYPE_AGENT, message)
        return
    if transfer_type in _EXTERNAL_TRANSFERS:
        message, kind = vc.TRANSFER_SUCCESSFUL_MSG_EXTERNAL, rc.TRANSFER_TYPE_EXTERNAL
        warm = transfer_type == rc.TRANSFER_TYPE_WARM_EXTERNAL
        agent2 = to_be_primary.agent_sip_endpoint if warm else cp.get("to", "")
    else:
        message, kind = rc.TRANSFER_SUCCESSFUL_MSG, rc.TRANSFER_TYPE_AGENT
        agent2 = name_of(to_be_primary.agent_uuid)
        agent2 = "Agent2" if agent2 is None else agent2
    if transfer_type == rc.TRANSFER_TYPE_TEAM:
        message, kind = rc.TRANSFER_SUCCESSFUL_MSG_TEAM, rc.TRANSFER_TYPE_TEAM
    if (name1 := name_of(agent_uuid)) is not None:
        message = message.replace("<Agent1>", name1, 1).replace("<Agent2>", agent2 or "", 1)
        _signal_chunk(call_uuid, rc.SIGNAL_TRANSFER_SUCCESSFUL, agent_uuid, kind, message)


def handle_secondary_agent_leave(
    call_uuid: str, qnr_run_uuid: str, params: dict, primary_agent_sip: str, transfer_info: dict
) -> None:
    rr, vr, p = RoutingRedis(), VoiceRedis(), params or {}
    to = p.get("to", "")
    transfer_type = (transfer_info or {}).get("transfer_type", "") or ""
    if p.get("add_participant_header", "") == vc.HEADER_PARTICIPANT_EXISTS_VALUE:
        err = handle_external_participant_leave(call_uuid, qnr_run_uuid, p)
        if err is not None:
            logger.error(f"handle_secondary_agent_leave: participant leave failed: {err}")
        return
    warm_external = transfer_type == rc.TRANSFER_TYPE_WARM_EXTERNAL
    if transfer_type == "" or (p.get("answer_time", "") == "" and not warm_external):
        return
    if (cp := rr.get_contact_parameters(call_uuid, qnr_run_uuid)) is None:
        logger.error("handle_secondary_agent_leave: contact parameters not in cache")
        return
    participants, perr = voice_agent_leg._get_participants(f"mpc_{qnr_run_uuid}", *ar.contact_pair(cp))
    if perr is not None:
        logger.error(f"handle_secondary_agent_leave: participant list unavailable: {perr}")
    customer = _customer_member(participants)
    is_cust_in_mpc = customer is not None and not customer.get("exit_cause")
    payload = {
        "conversation_number": cp.get("conversation_number", ""),
        "ignore_event": perr is not None or (participants is not None and not is_cust_in_mpc),
        "channel": rc.CHANNEL_CALL,
    }
    if warm_external:
        digits = p.get("digits_sent", "")
        payload.update(participant_identifier=f"{to}#{digits}" if digits else to, type=vc.PARTICIPANT_TYPE_EXTERNAL)
    else:
        payload["agent_sip_or_uuid"] = to
    _delete_secondary(payload, to)
    if is_cust_in_mpc:
        vr.set_agent_switch_role(qnr_run_uuid, primary_agent_sip, "")
        rr.delete_to_be_primary_agent(qnr_run_uuid)
        rr.delete_transfer_in_progress(call_uuid)
    dropped, kind = {"name": to, "uuid": to}, rc.TRANSFER_TYPE_WARM_EXTERNAL
    if not warm_external:
        dropped, kind = (
            (voice_agent_leg.get_account_details_for_sip_endpoints([to])[0] or [None])[0],
            rc.TRANSFER_TYPE_AGENT,
        )
    if dropped:
        message = rc.AGENT2_DROPPED_MSG.replace("<name>", dropped.get("name", ""), 1)
        _signal_chunk(call_uuid, rc.SIGNAL_AGENT2_DROPPED, dropped.get("uuid", ""), kind, message)


def handle_external_participant_leave(call_uuid: str, qnr_run_uuid: str, params: dict) -> str | None:
    p = params or {}
    to, answered = p.get("to", ""), p.get("answer_time", "") != ""
    if not answered and p.get("plivo_hangup_cause_code", "") in (
        vc.HANGUP_CODE_CALL_CANCELLED_BY_API,
        vc.HANGUP_CODE_NORMAL_HANGUP,
        vc.HANGUP_CODE_MPC_ENDED,
    ):
        return None
    cp = RoutingRedis().get_contact_parameters(call_uuid, qnr_run_uuid)
    if cp is None:
        return rc.CACHE_MISS
    alias = None
    if raw := p.get("add_participant_alias_name", ""):
        alias, err = voice_agent_leg.decode_sip_header_value(raw)
        if err is not None:
            logger.error(f"Failed to Decode participant alias name participant_alias={raw}")
    if not answered:
        message = f"{to if alias is None else alias} declined the call or failed to pick up"
        _signal_chunk(call_uuid, "participant_did_not_answered", to, "", message)
        return None
    state = "participant_removed" if p.get("plivo_hangup_source", "") == "API Request" else "participant_left"
    payload = {
        "conversation_number": cp.get("conversation_number", ""),
        "participant_identifier": to,
        "channel": rc.CHANNEL_CALL,
        "type": vc.PARTICIPANT_TYPE_ADD_PARTICIPANT,
        "participant_alias": alias or "",
        "secondary_agent_state": state,
    }
    err = _delete_secondary(payload, to)
    if err is None and (err := VoiceRedis().remove_current_participant(call_uuid, to)) is not None:
        logger.error("handle_external_participant_leave: participant not removed from cache")
    return err


def handle_mpc_end_event(call_uuid: str, params: dict, qnr_run_uuid: str = "") -> tuple[str, str | None]:
    # The durable MPCEnd leg is the terminal fact; the chunk is optional fan-out and never releases the claim
    # (a replay would replace the exact MPCEnd proof and could re-arm routing on a dead conference).
    if (leg_error := reconcile_terminal_call_leg(call_uuid, params, is_mpc=True)) is not None:
        return "", leg_error
    from hs.conversations import child_update as ccu
    from hs.voice import outbound

    ended = "Outgoing_call_ended" if outbound.dialed(call_uuid) else "Incoming_call_ended"
    chunk = NonQNRChunkInsert(
        parent_contact_uuid=call_uuid, type="signal", description=ended, channel=rc.CHANNEL_CALL, message_id=""
    )
    mapping = AgentAndContactMapping.parse_redis(RoutingRedis().get_contact_to_agent_mapping(call_uuid))
    # An answered call lives in its agent's room: a late MPCEnd from a room the caller left (for a menu or another
    # queue) must not end the live conversation.
    if mapping is None or mapping.qnr_run_uuid == qnr_run_uuid:
        _write_chunk("MPCEnd", lambda: ccu.add_chunk_to_non_qnr_child(chunk))
    if qnr_run_uuid and ended == "Incoming_call_ended":
        # the room ended while queued or ringing: cancel the ringing offers; the caller's exit closes the queue row
        from hs.voice.controls import end_mpc_calls

        if err := end_mpc_calls(call_uuid, qnr_run_uuid, drop=False)[1]:
            logger.error(f"handle_mpc_end_event: could not end the MPC calls call_uuid={call_uuid}: {err}")
    return "successfully handled end mpc", None


def drop_exited_caller(call_uuid: str, params: dict, qnr_run_uuid: str) -> str | None:
    """The caller left its queue room: a wait timeout is the queue exit's to close, any other exit is unanswered.
    Safe to repeat, so a retried exit finishes a drop its first delivery never reached."""
    caller = str(params.get("participant_role", "")).lower() == vc.MPC_ROLE_CUSTOMER
    if not qnr_run_uuid or not caller or params.get("participant_exit_cause", "") == vc.EXIT_WAIT_TIMEOUT:
        return None
    from hs.voice.controls import drop_abandoned

    return drop_abandoned(call_uuid, qnr_run_uuid)


def handle_participant_exit_event(call_uuid: str, params: dict, qnr_run_uuid: str = "") -> _Reply:
    p = params or {}
    if (leg_error := reconcile_terminal_call_leg(call_uuid, p, is_mpc=False)) is not None:
        return None, leg_error
    if err := drop_exited_caller(call_uuid, p, qnr_run_uuid):
        return None, err
    if p.get("is_mpc_termination_source", "") == "true":
        from hs.conversations import management as cm

        chunk = ConvChunkInsertByContactUUID(
            contact_uuid=call_uuid,
            type="hangup",
            description="Incoming_call_hangup",
            channel=rc.CHANNEL_CALL,
            attributes=[{"hangup_source": p.get("participant_role", "")}],
        )
        _write_chunk("ParticipantExit", lambda: cm.create_hangup_conversation_chunk(chunk))
    return "Successfully updated with hangup source", None


def handle_add_participant_action_completed(call_uuid: str, qnr_run_uuid: str, params: dict, db) -> _Reply:
    """Plivo finished dialing a participant into the MPC. An agent's routing dial that succeeded resets the
    active-conference toggle; one that failed retries routing, or fails the queue entry and sends the call
    to the queue's exit action. A failed transfer dial unholds the customer and tells the transferring agent."""
    rr, vr, p, done = RoutingRedis(), VoiceRedis(), params or {}, "Handled AddParticipantByAPIActionCompleted"
    if (cp := rr.get_contact_parameters(call_uuid, qnr_run_uuid)) is None:
        return "", rc.CACHE_MISS
    transfer_info = rr.get_transfer_in_progress_info(cp.get("contact_uuid", ""))
    transfer_type = (transfer_info or {}).get("transfer_type", "") or ""
    routing_dial = transfer_info is None or transfer_type == rc.TRANSFER_TYPE_FLOW
    outcome = (p.get("add_participant_api_request_outcome", "") or "").lower()
    if (p.get("participant_role", "") or "").lower() != vc.MPC_ROLE_AGENT:
        return done, None
    if outcome == "successful" and routing_dial and vr.toggle_active_conference(qnr_run_uuid, "false", 0) is not None:
        logger.error(f"handle_add_participant_action_completed: active conference not reset call_uuid={call_uuid}")
    if outcome != "unsuccessful":
        return done, None
    if p.get("add_participant_header", "") != "":
        vr.remove_current_participant(call_uuid, p.get("to", ""))
        return done, None
    if not routing_dial:
        return _transfer_dial_failed(call_uuid, qnr_run_uuid, cp, transfer_info, db, rr, vr)
    if p.get("add_participant_api_request_outcome_reason", "") != "":
        return done, None
    try:
        entry = aq.get_current_active_queue_entry_by_contact_uuid(db, call_uuid, qnr_run_uuid)
    except Exception:
        logger.error("handle_add_participant_action_completed: active queue lookup failed", exc_info=True)
        return "", retryable_voice_callback_error("active queue lookup unavailable")
    if entry is None or not entry.status:
        return done, None
    if entry.status == rc.CONTACT_STATE_IN_REVERSE_ROUTING:
        queue_error = aq.update_queue_status(db, call_uuid, qnr_run_uuid, rc.CONTACT_STATE_IN_TRANSIT, strict=True)
        if aq.is_no_live_queue_status(queue_error):
            return done, None
        if queue_error is not None:
            logger.error(f"handle_add_participant_action_completed: status not moved back to in_transit: {queue_error}")
            return "", retryable_voice_callback_error("queue status persistence unavailable")
    route = (call_uuid, qnr_run_uuid, False, transfer_type, "", "", "", db)
    res, rerr = voice_callbacks.route_and_add_to_mpc(*route)
    if rerr is None:
        return done, None
    if voice_callbacks.is_retryable(rerr):
        return "", rerr  # no cleanup or transfer-back until the transient routing dependency recovers
    if rerr.startswith("known_error:"):
        increase_counter(MetricName.MPC_ADD_AGENTS_FAILURE, {"output_state": "retry_incoming_call"})
    try:
        active_queue_id = aq.delete_active_queue_entry(db, call_uuid, qnr_run_uuid, rc.CONTACT_STATE_FAILED)
    except Exception:
        logger.error(f"Could not update queue status call UUID={call_uuid}", exc_info=True)
        return "", retryable_voice_callback_error("queue status persistence unavailable")
    voice_callbacks.mark_unsafe_callback_effect(db, "add_participant_failure_cleanup")
    vr.toggle_active_conference(qnr_run_uuid, "false", 0)
    voice_callbacks._update_unanswered(call_uuid, rc.UNANSWERED_ASSIGNMENT_FAILED, active_queue_id)
    _msg, terr = voice_callbacks.send_to_queue_exit(call_uuid, cp, res)
    if terr is not None:
        logger.error(f"handle_add_participant_action_completed: call not handed back calluuid={call_uuid}: {terr}")
    return "", rerr


def _transfer_dial_failed(call_uuid, qnr_run_uuid, cp, transfer_info, db, rr, vr) -> tuple[str, str | None]:
    from hs.conversations.store import Conversation, emit

    transfer_type = transfer_info.get("transfer_type", "") or ""
    target_type = rc.TRANSFER_TYPE_EXTERNAL if transfer_type in _EXTERNAL_TRANSFERS else transfer_type
    vr.delete_transfer_mpc_call_uuids(qnr_run_uuid)
    if (mapping := AgentAndContactMapping.parse_redis(rr.get_contact_to_agent_mapping(call_uuid))) is None:
        return "", rc.CACHE_MISS
    conversation_number, _err = get_conversation_number_from_contact_uuid(call_uuid, rr)
    if transfer_type not in _AGENT_TRANSFERS:
        # Retryable provider work first, so a failure replays without duplicating the visible effects below.
        signing = ar.contact_pair(cp)
        participants, participant_error = voice_agent_leg._get_participants(f"mpc_{qnr_run_uuid}", *signing)
        if participant_error is not None:
            return "", participant_error
        customer = _customer_member(participants)
        if customer is not None:
            uerr = hold_or_unhold_call(False, call_uuid, qnr_run_uuid, str(customer.get("member_id", "")), *signing)
            if uerr is not None:
                return "", uerr
    rr.delete_transfer_in_progress(call_uuid)
    voice_callbacks.mark_unsafe_callback_effect(db, "transfer_failure_event_and_signal")
    if child := Conversation.get_primary_active_conversation(conversation_number, mapping.agent_uuid):
        data = {
            "transfer_id": str(child.uuid),
            "mode": _event_mode(transfer_info),
            "from_agent_id": mapping.agent_uuid,
            "target": {"type": target_type, "id": transfer_info.get("target", "")},
            "status": "failed",
        }
        emit("call.transferred", child.parent_conversation, data, mapping.agent_uuid)
    _signal_chunk(call_uuid, rc.SIGNAL_TRANSFER_FAILED, "", "", rc.TRANSFER_FAILED_MSG)
    return "Handled AddParticipantByAPIActionCompleted", None


def _release_capacity(params, channel: str, output_state: str) -> str | None:
    _res, err = handle_reverse_routing_and_agent_capacity(
        params.agent_sip_endpoint, channel, params.conversation_number
    )
    if err is not None and err.startswith("known_error:"):
        increase_counter(MetricName.REVERSE_ROUTING_FAILURE, {"output_state": output_state})
    return err


def voice_handle_wrap_up(params) -> str | None:
    if not params.release_agent_capacity:
        return None
    err = _release_capacity(params, params.channel, "wrapup_reverse_routing")
    if err is not None:
        logger.error(f"wrap up: reverse routing failed ContactUUID={params.contact_uuid}: {err}")
    return err


def voice_handle_resolve(params) -> str | None:
    if not params.release_agent_capacity:
        return None
    err = _release_capacity(params, rc.CHANNEL_CALL, "resolve_reverse_routing")
    if err is not None:
        logger.error(f"resolve: reverse routing failed AgentUUID={params.agent_uuid}: {err}")
    return err
