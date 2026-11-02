import uuid
from contextlib import contextmanager, suppress

from fastapi.encoders import jsonable_encoder
from sqlalchemy import text

from hs.access import ring_address
from hs.conversations import transfer as conversation_transfer
from hs.conversations.models import ConvChannel
from hs.infra.counters import MetricName, increase_counter
from hs.infra.db import core_engine
from hs.infra.log import logger
from hs.plivo import voice as pv
from hs.routing import account_read as ar
from hs.routing import constants as rc
from hs.routing.invitation_models import AgentAndContactMapping
from hs.routing.redis_store import RoutingRedis
from hs.voice import agent_leg as voice_agent_leg
from hs.voice import callbacks as voice_callbacks
from hs.voice import constants as vc
from hs.voice import hangup as voice_hangup
from hs.voice import transfer as voice_transfer
from hs.voice.outbound import hs_header
from hs.voice.redis_store import VoiceRedis

BARGE_NOTICE = "A supervisor has joined the call"


def _spawn(target, *args) -> None:
    # Detached spawn (hs/infra/bg_spawn.py): keeps log context, but the worker gets its OWN DB
    # session -- sharing the request Session across threads raced its COMMIT against the
    # worker's SAVEPOINT (psycopg2 NoActiveSqlTransaction).
    from hs.infra.bg_spawn import spawn_detached

    spawn_detached(target, *args, log_context="voice_monitor")


def _title(name: str) -> str:
    return " ".join(w[:1].upper() + w[1:].lower() if w else w for w in (name or "").split(" "))


def _seed_context(req_meta: dict) -> None:
    from hs.infra.context import context_aom_uuid, context_org_uuid
    from hs.routing.invitation import _ensure_request_context

    _ensure_request_context()
    with suppress(ValueError, TypeError):
        if req_meta.get("aom_uuid"):
            context_aom_uuid.set(uuid.UUID(str(req_meta["aom_uuid"])))
        if req_meta.get("org_uuid"):
            context_org_uuid.set(uuid.UUID(str(req_meta["org_uuid"])))


@contextmanager
def _context_session(req_meta: dict):
    """The conversation use cases' request context for this caller, committed on the way out."""
    _seed_context(req_meta)
    try:
        yield
    finally:
        voice_hangup._commit_session()


def get_parent_conversation_detail(
    req_meta: dict, conversation_number: str, has_supervisor_admin: bool, filter_active: bool
) -> tuple[dict | None, str | None]:
    from hs.conversations import management as cm

    with _context_session(req_meta):
        try:
            resp = cm.get_parent_conversation_internal(conversation_number, has_supervisor_admin, filter_active)
        except Exception as e:
            logger.error(f"Error while fetching parent conversation: {e}")
            return None, "failed to fetch parent conversation"
    if resp.error or (resp.status_code or 200) != 200:
        logger.error(f"Error while fetching parent conversation statusCode={resp.status_code}: {resp.error}")
        return None, "failed to fetch parent conversation"
    return jsonable_encoder(resp.data), None


def _occupied_blocks(req_meta: dict, channel: str) -> bool:
    """Whether the supervisor is busy on the channel (or that can't be read)."""
    from hs.conversations import management as cm

    with _context_session(req_meta):
        try:
            resp = cm.get_aom_occupied_state(ConvChannel(channel=channel))
        except Exception as e:
            logger.error(f"_occupied_blocks: Error while checking if aom is occupied: {e}")
            return True
    if resp.error or (resp.status_code or 200) != 200:
        return True
    occupied = jsonable_encoder(resp.data) or {}
    return bool(occupied.get("is_aom_occupied")) or occupied.get("secondary_agent_state") == vc.SECONDARY_STATE_BARGE


def check_if_mpc_is_active(mpc_name: str, auth_id: str, auth_token: str) -> str | None:
    """Gates a supervisor leg on the MPC being readable with the pair and not ended. Plivo's
    own answer decides: an MPC the pair does not own reads as 404, and an ended MPC still
    reads 200 with its end time set, where an add would start a new MPC under the same name
    and dial the supervisor into it alone."""
    if not auth_id or not auth_token:
        return "missing credentials"
    body, _status, err = pv.get_mpc(mpc_name, auth_id, auth_token, single_attempt=False)
    if err is not None:
        return err
    return "mpc is ended" if (body or {}).get("end_time") else None


def _mpc_name_for(mapping: AgentAndContactMapping, call_uuid: str) -> str:
    if mapping.qnr_run_uuid == "":
        return f"main_mpc_{mapping.flow_run_uuid}_{call_uuid}"
    return f"mpc_{mapping.qnr_run_uuid}"


def add_supervisor_to_mpc(req_meta: dict, auth_token: str, contact_uuid: str, supervisor_sip: str) -> str | None:
    """Dials the supervisor into the contact's MPC, muted in coach mode."""
    rr = RoutingRedis()
    mapping = AgentAndContactMapping.parse_redis(rr.get_contact_to_agent_mapping(contact_uuid))
    if mapping is None:
        logger.error(f"could not get contact and agent mapping contactUUID={contact_uuid}")
        return rc.CACHE_MISS
    auth_id, run_uuid = req_meta.get("auth_id", ""), mapping.qnr_run_uuid or mapping.flow_run_uuid
    mpc_name = _mpc_name_for(mapping, contact_uuid)
    aerr = check_if_mpc_is_active(mpc_name, auth_id, auth_token)
    if aerr is not None:
        logger.error(f"MPC is not active mpcName={mpc_name}: {aerr}")
        return "MPC is not active"
    # A POST /calls dial is outbound; a queued call keeps its direction (an outbound hand-off) in its contact params.
    inbound = mapping.is_inbound and (rr.get_contact_parameters(contact_uuid, mapping.qnr_run_uuid) or {}).get(
        "inbound", True
    )
    _tok, record_value, gerr = ar.get_organisation_auth_token_and_record_value(auth_id, inbound)
    if gerr is not None:
        logger.error(
            f"add_supervisor_to_mpc: Error occurred while fetching record value from org table calluuid={contact_uuid}: {gerr}"
        )
    from_identifier, contact_name = voice_agent_leg.handle_restricted_number(
        mapping.from_number, mapping.to_number, mapping.from_number, contact_uuid, "add_supervisor_to_mpc"
    )
    leg = {"from": from_identifier, "to": ring_address(supervisor_sip), "caller_name": (contact_name or "")[:50]}
    body = {
        **leg,
        "sip_headers": f"HsKind=monitor,HsConversation={hs_header(req_meta.get('conversation_id', ''))}",
        "call_status_callback_url": pv.mpc_call_status_callback_url(auth_id, contact_uuid, run_uuid),
        "call_status_callback_method": "POST",
        "status_callback_url": pv.mpc_status_callback_url(auth_id, contact_uuid, run_uuid),
        "status_callback_method": "POST",
        "status_callback_events": vc.MPC_AGENT_STATUS_CALLBACK_EVENTS,
        "role": "Supervisor",
        "coach_mode": True,
        "enter_sound": "none",
        "exit_sound": "none",
        "mute": True,
        "stay_alone": False,
        "relay_dtmf_inputs": False,
        "ring_timeout": "45",
        "recording_callback_method": "POST",
        "create_mpc_with_single_participant": False,
        "record": record_value,
        "record_participant_track": record_value,
        "recording_callback_url": pv.mpc_recording_callback_url(auth_id, contact_uuid, run_uuid),
        "transcript": record_value,
        "transcription_url": pv.mpc_transcription_callback_url(auth_id, contact_uuid),
    }
    client = pv.PlivoVoiceClient(auth_id, auth_token)
    reply, _s, err = client.mpc_add_participant(
        pv.make_mpc_id(friendly_name=mpc_name), body, raw_body=pv.compact_json(body)
    )
    if (leg := ((reply or {}).get("calls") or [{}])[0].get("call_uuid")) and err is None:
        err = VoiceRedis().set_supervisor_leg(contact_uuid, req_meta.get("aom_uuid", ""), leg)
    if err is not None:
        logger.error(f"add_supervisor_to_mpc: failed to add supervisor to MPC callUUID={contact_uuid}: {err}")
        increase_counter(MetricName.MPC_FAILURE, {"output_state": "add_supervisor"})
        # A leave reaches a leg only through its record: a leg that failed to record is not left ringing.
        if leg and (herr := client.call_delete(leg, quiet_not_found=True)[2]) is not None:
            logger.error(f"add_supervisor_to_mpc: an unrecorded supervisor leg not hung up leg={leg}: {herr}")
    return err


_BARGE_NOTICE = text(
    "SELECT COALESCE(s.voice_settings->'monitor'->>'barge_notice', :default) FROM org_setting s"
    " JOIN organization o ON o.id = s.organization WHERE o.auth_id = :auth_id"
)


def barge_notice(mpc_name: str, auth_id: str, auth_token: str) -> str | None:
    """settings.monitor.barge_notice, spoken to everyone on the MPC before a supervisor barges in."""
    with core_engine.connect() as conn:
        notice = conn.execute(_BARGE_NOTICE, {"auth_id": auth_id, "default": BARGE_NOTICE}).scalar()
    if not notice:
        return None
    client = pv.PlivoVoiceClient(auth_id, auth_token)
    return client.mpc_start_speak(pv.make_mpc_id(friendly_name=mpc_name), "all", {"text": notice})[2]


def _active_member(mpc_name: str, aom_sip: str, auth_id: str, auth_token: str):
    """(the participant still on the MPC at aom_sip or None, error, whether the participant list was read)."""
    participants, perr = voice_agent_leg._get_participants(mpc_name, auth_id, auth_token)
    if perr is not None or not participants:
        logger.error(f"Could not get the list of participants mpcName={mpc_name}: {perr}")
        return None, perr, False
    member = next((p for p in participants if p.get("member_address", "") == aom_sip and not p.get("exit_cause")), None)
    return member, None, True


def _update_supervisor_leg(
    body: dict, missing: str, failed: str, mpc_name: str, aom_sip: str, auth_id: str, auth_token: str
) -> str | None:
    """Apply body to the supervisor's leg on the MPC; `missing` / `failed` are the not-on-call / update errors."""
    member, perr, read = _active_member(mpc_name, aom_sip, auth_id, auth_token)
    if not read:
        return perr
    if member is None:
        logger.error(missing)
        return "participant not found"
    _b, _s, err = pv.PlivoVoiceClient(auth_id, auth_token).mpc_update_participant(
        pv.make_mpc_id(friendly_name=mpc_name), str(member.get("member_id", "")), body
    )
    if err is not None:
        logger.error(f"{failed}: {err}")
    return err


def pre_req_details_for_monitoring(
    req_meta: dict, conversation_number: str, call_uuid: str, accessible_aom_uuids, check_conversation_access: bool
):
    """(conversation, supervisor aom, org auth token, None), or the refusal as the last item."""
    conv_details, err = get_parent_conversation_detail(req_meta, conversation_number, True, True)
    if err is not None:
        return None, None, None, vc.MONITOR_EVENT_CONVERSATION_CLOSED
    participants = conv_details.get("conv_participants_aom_uuids") or []
    if req_meta.get("aom_uuid", "") in participants:
        logger.info(f"Supervisor is already part of the conversation conversationNumber={conversation_number}")
        return conv_details, None, None, vc.MONITOR_EVENT_SUPERVISOR_PRESENT
    if check_conversation_access and not set(participants) & set(accessible_aom_uuids or []):
        return conv_details, None, None, vc.MONITOR_EVENT_CONVERSATION_NOT_ACCESSIBLE
    if call_uuid == "":
        call_uuid = (conv_details.get("conv_channel_to_contact_uuid_map") or {}).get(rc.CHANNEL_CALL, "")
        if not call_uuid:
            logger.error(f"Error while getting callUUID conversationNumber={conversation_number}")
            return conv_details, None, None, "could not get callUUID"
    supervisor_mapping, _serr = VoiceRedis().get_call_to_supervisor_mapping(call_uuid)
    if supervisor_mapping is not None and supervisor_mapping.get("supervisor_uuid") != req_meta.get("aom_uuid"):
        logger.error(f"Another supervisor is already part of the conversation conversationNumber={conversation_number}")
        return conv_details, None, None, vc.MONITOR_EVENT_SUPERVISOR_PRESENT
    aom_details = ar.get_agent_by_uuid(req_meta.get("aom_uuid", ""), req_meta.get("auth_id", ""))
    if aom_details is None:
        return conv_details, None, None, "could not get aom details"
    auth_token, terr = ar.get_organisation_auth_token(req_meta.get("auth_id", ""))
    if terr is not None:
        return conv_details, aom_details, None, "could get organization details"
    return conv_details, aom_details, auth_token, None


def handle_capacity_and_err_events(req_meta: dict, channel: str, aom_details, conv_details: dict) -> str | None:
    rr, number, aom_uuid = RoutingRedis(), conv_details.get("conversation_number", ""), req_meta.get("aom_uuid", "")
    with suppress(Exception):
        rr.delete_reverse_routing_lock(aom_details.sip_endpoint, number, channel)
    _code, cerr = rr.increase_agent_capacity(req_meta.get("auth_id", ""), aom_details.sip_endpoint, channel, 1, number)
    if cerr is None:
        return None
    logger.error(f"Error while increasing the capacity for the channel aomUUID={aom_uuid} channel={channel}")
    return "capacity full" if _occupied_blocks(req_meta, channel) else "switch call monitoring"


def handle_call_monitoring(
    req_meta: dict,
    call_uuid: str,
    conv_details: dict,
    aom_details,
    auth_token: str,
    state: str = vc.SECONDARY_STATE_MONITOR,
) -> str | None:
    vr = VoiceRedis()
    name = aom_details.name
    shared = {
        "parent_contact_uuid": conv_details.get("conv_parent_contact_uuid", ""),
        "supervisor_name": _title(name or ""),
        "supervisor_sip": aom_details.sip_endpoint,
        "conversation_number": conv_details.get("conversation_number", ""),
        "state": state,
    }
    ids = f"aomUUID={aom_details.uuid} callUUID={call_uuid}"
    err = vr.set_supervisor_to_call_mapping(aom_details.uuid, {"call_uuid": call_uuid, **shared})
    if err is not None:
        logger.error(f"Error while storing the supervisor to call mapping {ids}: {err}")
        return err
    err = vr.set_call_to_supervisor_mapping(call_uuid, {"supervisor_uuid": aom_details.uuid, **shared})
    if err is not None:
        logger.error(f"Error while storing the call to supervisor mapping {ids}: {err}")
        return err
    aerr = add_supervisor_to_mpc(req_meta, auth_token, call_uuid, aom_details.sip_endpoint)
    if aerr is not None:
        logger.error(f"Error while adding supervisor to MPC callUUID={call_uuid} aomUUID={aom_details.uuid}: {aerr}")
        vr.delete_supervisor_to_call_mapping(aom_details.uuid, call_uuid)
        return aerr
    return None


def _release_monitor_capacity(req_meta: dict, aom_details, conversation_number: str, label: str):
    try:
        auth_id = req_meta.get("auth_id", "")
        RoutingRedis().decrease_agents_capacity(auth_id, aom_details.sip_endpoint, rc.CHANNEL_CALL, conversation_number)
    except Exception as ce:
        logger.error(f"{label}Error while decreasing the capacity: {ce}")


_PREREQ_FAILED = "Error while getting pre requisite details for monitoring conversationNumber={}"


def monitor_conversation(
    req_meta: dict, conversation_number: str, accessible_aom_uuids, state: str = vc.SECONDARY_STATE_MONITOR
) -> str | None:
    conv_details, aom_details, auth_token, err = pre_req_details_for_monitoring(
        req_meta, conversation_number, "", accessible_aom_uuids, True
    )
    if err is not None:
        logger.error(_PREREQ_FAILED.format(conversation_number))
        return err
    if list(conv_details.get("conv_channels_list") or []) != [rc.CHANNEL_CALL]:
        return "We currently support monitoring for voice channels only"
    cerr = handle_capacity_and_err_events(req_meta, rc.CHANNEL_CALL, aom_details, conv_details)
    if cerr is not None:
        return cerr
    call_uuid = (conv_details.get("conv_channel_to_contact_uuid_map") or {}).get(rc.CHANNEL_CALL, "")
    merr = handle_call_monitoring(req_meta, call_uuid, conv_details, aom_details, auth_token, state)
    if merr is not None:
        logger.error(f"Error while handling supervisor call monitoring aomUUID={aom_details.uuid} callUUID={call_uuid}")
        _release_monitor_capacity(req_meta, aom_details, conv_details.get("conversation_number", ""), "")
    return merr


def _update_supervisor_state(conversation_number: str, req_meta: dict, state: str, **extra) -> str | None:
    return conversation_transfer.update_secondary(
        conversation_number=conversation_number,
        agent_sip_or_uuid=req_meta.get("aom_uuid", ""),
        secondary_agent_state=state,
        **extra,
        type=vc.PARTICIPANT_TYPE_SUPERVISOR,
    )


def _conv_details_and_mappings(req_meta: dict, conversation_number: str):
    """(call uuid, agent mapping, supervisor mapping, org pair, conversation, None), or the error as the last item."""
    conv_details, err = get_parent_conversation_detail(req_meta, conversation_number, False, True)
    if err is not None:
        return "", None, None, None, None, err
    call_uuid = (conv_details.get("conv_channel_to_contact_uuid_map") or {}).get(rc.CHANNEL_CALL, "")
    if not call_uuid:
        logger.error("could not find call channel in the conversation")
        return "", None, None, None, conv_details, "no call channel found"
    mapping = AgentAndContactMapping.parse_redis(RoutingRedis().get_contact_to_agent_mapping(call_uuid))
    if mapping is None:
        return "", None, None, None, conv_details, rc.CACHE_MISS
    auth_token, terr = ar.get_organisation_auth_token(req_meta.get("auth_id", ""))
    if terr is not None:
        return "", None, None, None, conv_details, terr
    supervisor_mapping, serr = VoiceRedis().get_call_to_supervisor_mapping(call_uuid)
    if supervisor_mapping is None:
        return "", None, None, None, conv_details, serr or rc.CACHE_MISS
    return call_uuid, mapping, supervisor_mapping, (req_meta.get("auth_id", ""), auth_token), conv_details, None


def monitor_conversation_actions(req_meta: dict, conversation_number: str, actions: dict) -> str | None:
    if actions.get("mute") is not None:
        return handle_supervisor_mute_unmute(req_meta, bool(actions["mute"]), conversation_number)
    if actions.get("notify"):
        return handle_notify_action(req_meta, conversation_number)
    return None


def handle_supervisor_mute_unmute(req_meta: dict, mute: bool, conversation_number: str) -> str | None:
    call_uuid, mapping, supervisor_mapping, auth, _conv, err = _conv_details_and_mappings(req_meta, conversation_number)
    if err is not None:
        logger.error(
            f"handle_supervisor_mute_unmute: error while muting the supervisor call state "
            f"conversationNumber={conversation_number}"
        )
        return err
    mpc_name, supervisor_sip = _mpc_name_for(mapping, call_uuid), supervisor_mapping.get("supervisor_sip", "")
    return _update_supervisor_leg(
        {"mute": mute},
        f"participant not found on the call aomSIPEndpoint={supervisor_sip} mpcName={mpc_name} callUUID={call_uuid}",
        f"error in updating status of participant callUUID={call_uuid}",
        mpc_name,
        supervisor_sip,
        *auth,
    )


def handle_notify_action(req_meta: dict, conversation_number: str) -> str | None:
    _conv, err = get_parent_conversation_detail(req_meta, conversation_number, False, True)
    if err is not None:
        return err
    call_mapping, cerr = VoiceRedis().get_supervisor_to_call_mapping(req_meta.get("aom_uuid", ""))
    if call_mapping is None:
        return cerr or rc.CACHE_MISS
    previous = call_mapping.get("conversation_number", "")
    uerr = _update_supervisor_state(previous, req_meta, "monitor_visible")
    if uerr is not None:
        logger.error(
            f"Notify action: Error while updating the existing call with monitor visible state "
            f"aomUUID={req_meta.get('aom_uuid', '')}"
        )
    return uerr


def update_participant_call_state(req_meta: dict, conversation_number: str, state: str, payload: dict) -> str | None:
    if state not in vc.PARTICIPANT_STATE_SETTINGS:
        return "invalid participant state update requested"
    coach_mode, mute = vc.PARTICIPANT_STATE_SETTINGS[state]
    call_uuid, mapping, supervisor_mapping, auth, conv_details, err = _conv_details_and_mappings(
        req_meta, conversation_number
    )
    tag, number = "update_participant_call_state", f"conversationNumber={conversation_number}"
    if err is not None:
        logger.error(f"{tag}: error while updating the supervisor call state {number}")
        return err
    participants = conv_details.get("conv_participants_aom_uuids") or []
    if not (set(participants) & set(payload.get("accessible_aom_uuids") or [])):
        return "you do not have permission to access this conversation"
    mpc_name, supervisor_sip = _mpc_name_for(mapping, call_uuid), supervisor_mapping.get("supervisor_sip", "")
    if state == vc.SECONDARY_STATE_BARGE and (nerr := barge_notice(mpc_name, *auth)) is not None:
        return nerr
    uerr = _update_supervisor_leg(
        {"mute": mute, "coach_mode": coach_mode},
        f"participant not found on the call aomSIPEndpoint={supervisor_sip} mpcName={mpc_name}",
        f"error in updating mute/unmute, coachMode status of participant callUUID={call_uuid}",
        mpc_name,
        supervisor_sip,
        *auth,
    )
    if uerr is not None:
        logger.error(
            f"{tag}: error while updating supervisor setting on MPC mute={mute} coachMode={coach_mode}: {uerr}"
        )
        return uerr
    extra = {"agent_requested_coach": bool(payload.get("agent_requested_coach"))}
    serr = _update_supervisor_state(supervisor_mapping.get("conversation_number", ""), req_meta, state, **extra)
    if serr is not None:
        logger.error(f"{tag}: error while updating the secondary agent details {number}")
    return serr


def leave_monitored_conversation(req_meta: dict, conversation_number: str, leave_request: dict) -> str | None:
    aom_details = ar.get_agent_by_uuid(req_meta.get("aom_uuid", ""), req_meta.get("auth_id", ""))
    if aom_details is None:
        return "could not get aom details"
    conv_details, err = get_parent_conversation_detail(req_meta, conversation_number, False, False)
    if err is not None:
        return "could not get conversation details"
    state = leave_request.get("state", "")
    if state == "in_wrap_up":
        return "wrap up states are not implemented"
    if state != "closed":
        logger.error(f"Invalid state received state={state} conversationNumber={conversation_number}")
        return "invalid state"
    if conversation_transfer.remove_secondary(**_supervisor_off(conversation_number, aom_details.sip_endpoint)):
        logger.error(f"failed to delete secondary aom mapping of supervisor conversationNumber={conversation_number}")
        return "could not delete secondary agent mapping"
    parent = conv_details.get("conv_parent_contact_uuid", "")
    monitoring = (vc.SECONDARY_STATE_MONITOR, vc.SECONDARY_STATE_MONITOR_PAUSED)
    name = aom_details.name
    agent_message = f"{_title(name or '')} {vc.SUPERVISOR_LEFT_MSG}"
    attribute = [{"agent_message": agent_message, "supervisor_message": f"You have {vc.SUPERVISOR_LEFT_MSG}"}]
    left = (parent, attribute, leave_request.get("agent_state") not in monitoring)
    _spawn(_create_supervisor_left_chunk, req_meta, *left, conversation_number)
    return None


def _supervisor_off(conversation_number: str, supervisor_sip: str) -> dict:
    """What takes a supervisor off the conversation. active=False: the monitored agent may have closed their
    conversation already. missing_ok: a leg whose join has not landed has nothing to remove."""
    off = {"conversation_number": conversation_number, "agent_sip_or_uuid": supervisor_sip, "ignore_event": True}
    return off | {"type": vc.PARTICIPANT_TYPE_SUPERVISOR, "active": False, "missing_ok": True}


def mark_leave_requested(call_uuid: str, leg: str) -> str | None:
    merr = VoiceRedis().set_mpc_callback_event(call_uuid, "", f"{vc.SUPERVISOR_LEAVE_REQUESTED}:{leg}", "1")
    return None if merr in (None, vc.VALUE_EXISTS_ERROR) else merr


def _leave_requested(call_uuid: str, leg: str) -> bool:
    return bool(leg) and bool(
        VoiceRedis().get_mpc_callback_event(call_uuid, "", f"{vc.SUPERVISOR_LEAVE_REQUESTED}:{leg}")[0]
    )


def _drop_if_left(call_uuid: str, leg: str, conversation_number: str, supervisor_sip: str, supervisor: str) -> None:
    """A leave that ran while this leg's join was in flight found nothing to remove: remove it now it is committed."""
    if _leave_requested(call_uuid, leg):
        drop_supervisor_leg(call_uuid, leg, conversation_number, supervisor_sip, supervisor)


def drop_supervisor_leg(call_uuid: str, leg: str, conversation_number: str, supervisor_sip: str, supervisor: str):
    """Takes the supervisor off the conversation for this leg only: a rejoin stores its own leg, and the row
    is then the rejoin's."""
    current, err = VoiceRedis().get_supervisor_leg(call_uuid, supervisor)
    if err is None and current != leg:
        return
    if err := err or voice_hangup._delete_secondary(
        _supervisor_off(conversation_number, supervisor_sip), supervisor_sip
    ):
        ids = f"conversationNumber={conversation_number} supervisor={supervisor} leg={leg}"
        logger.error(f"drop_supervisor_leg: a left supervisor not removed {ids}: {err}")
        increase_counter(MetricName.MPC_FAILURE, {"output_state": "remove_left_supervisor"})


def _create_supervisor_left_chunk(
    req_meta: dict, parent_contact_uuid: str, attribute, create_chunk: bool, conversation_number: str
) -> None:
    from hs.conversations import child_update as ccu
    from hs.conversations.models import NonQNRChunkInsert

    failure = (
        f"Error while creating conversation chunk for supervisor left event conversationNumber={conversation_number}"
    )
    with _context_session(req_meta):
        try:
            chunk = NonQNRChunkInsert(
                parent_contact_uuid=parent_contact_uuid,
                description="SUPERVISOR_LEFT",
                attributes=attribute,
                type="signal_msg",
                channel=rc.CHANNEL_CALL,
                is_inbound_chunk=True,
                create_chunk=create_chunk,
            )
            resp = ccu.add_chunk_to_non_qnr_child(non_qnr_chunk=chunk)
            if resp.error:
                logger.error(f"{failure}: {resp.error}")
        except Exception as e:
            logger.error(f"{failure}: {e}")


def hangup_monitored_conversation(req_meta: dict, conversation_number: str, mpc_name: str) -> str | None:
    vr, label = VoiceRedis(), "hangup_monitored_conversation"
    conv_details, err = get_parent_conversation_detail(req_meta, conversation_number, False, False)
    if err is not None:
        logger.error(f"{label}: error while fetching conversation details conversationNumber={conversation_number}")
        return err
    call_uuid = conv_details.get("conv_parent_contact_uuid", "")
    supervisor, serr = vr.get_call_to_supervisor_mapping(call_uuid)
    if supervisor is None:
        logger.error(f"{label}: error while fetching supervisor for a call_uuid call_uuid={call_uuid}")
        return serr or rc.CACHE_MISS
    call_mapping, cerr = vr.get_supervisor_to_call_mapping(supervisor.get("supervisor_uuid", ""))
    if call_mapping is None:
        logger.error(f"{label}: fail to find call_uuid for this supervisor call_uuid={call_uuid}")
        return cerr or rc.CACHE_MISS
    if call_mapping.get("call_uuid") != call_uuid:
        return "Supervisor is not on this call"
    auth_token, terr = ar.get_organisation_auth_token(req_meta.get("auth_id", ""))
    if terr is not None:
        logger.error(f"{label}: fail to get auth token for org call_uuid={call_uuid}")
        return terr
    signing = (req_meta.get("auth_id", ""), auth_token)
    leg, lerr = vr.get_supervisor_leg(call_uuid, supervisor.get("supervisor_uuid", ""))
    if lerr is not None:
        return lerr
    if leg:
        # Cancels a ringing leg or ends a joined one; a join that lands later sees the mark and is dropped.
        if merr := mark_leave_requested(call_uuid, leg):
            return merr
        _b, status, herr = pv.PlivoVoiceClient(*signing).call_delete(leg, quiet_not_found=True)
        herr = None if status == 404 else herr
    else:
        herr = voice_transfer.hangup_call_for_mpc_member(mpc_name, supervisor.get("supervisor_sip", ""), *signing)
    if herr is not None:
        logger.error(f"{label}: fail drop supervisor call_uuid={call_uuid}: {herr}")
    return herr


def handle_supervisor_join_event(
    call_uuid: str, qnr_run_uuid: str, member_address: str, mpc_name: str, leg: str = "", db=None
) -> str | None:
    if _leave_requested(call_uuid, leg):  # the supervisor left before this leg joined: it does not join
        pair = ar.contact_pair(RoutingRedis().get_contact_parameters(call_uuid, qnr_run_uuid))
        _b, status, herr = pv.PlivoVoiceClient(*pair).call_delete(leg, quiet_not_found=True)
        if herr is not None and status != 404:
            logger.error(f"handle_supervisor_join_event: a left supervisor's leg not hung up leg={leg}: {herr}")
        return None
    supervisor_mapping, _serr = VoiceRedis().get_call_to_supervisor_mapping(call_uuid)
    if supervisor_mapping is None or supervisor_mapping.get("supervisor_sip") != member_address:
        logger.error(
            f"handle_supervisor_join_event: Looks like some other supervisor is already monitoring, may be a race "
            f"issue. Removing the supervisor from the MPC supervisorSIP={member_address} callUUID={call_uuid}"
        )
        _spawn(_remove_racing_supervisor, mpc_name, member_address, call_uuid)
        return None
    conversation_number = supervisor_mapping.get("conversation_number", "")
    state = supervisor_mapping.get("state") or vc.SECONDARY_STATE_MONITOR
    if state != vc.SECONDARY_STATE_MONITOR:
        coach_mode, mute = vc.PARTICIPANT_STATE_SETTINGS[state]
        pair = ar.contact_pair(RoutingRedis().get_contact_parameters(call_uuid, qnr_run_uuid))
        if state == vc.SECONDARY_STATE_BARGE and (nerr := barge_notice(mpc_name, *pair)) is not None:
            return nerr
        body = {"mute": mute, "coach_mode": coach_mode}
        if uerr := _update_supervisor_leg(body, "supervisor left", "supervisor mode", mpc_name, member_address, *pair):
            return uerr
    aerr = conversation_transfer.add_secondary(
        conversation_number=conversation_number,
        agent_sip_or_uuid=supervisor_mapping.get("supervisor_sip", ""),
        secondary_agent_state=state,
        type=vc.PARTICIPANT_TYPE_SUPERVISOR,
    )
    if aerr is not None:
        logger.error(
            f"Error while adding the supervisor as a secondary participant of the conversation "
            f"aomUUID={supervisor_mapping.get('supervisor_uuid', '')}"
        )
        return "technical error: could not add supervisor to the conversation"
    cleanup = (call_uuid, leg, conversation_number, member_address, supervisor_mapping.get("supervisor_uuid", ""))
    voice_callbacks.stage_or_run(db, _drop_if_left, *cleanup, spawn=_spawn)
    return None


def _remove_racing_supervisor(mpc_name: str, member_address: str, call_uuid: str) -> None:
    sip = f"supervisorSIP={member_address}"
    org = ar.get_org_details_for_agent_sip(member_address)
    if org is None:
        logger.error(f"_remove_racing_supervisor: could not get org details {sip}")
        return
    auth_token, terr = ar.get_organisation_auth_token(org.auth_id)
    if terr is not None:
        logger.error(f"_remove_racing_supervisor: could not get the org token {sip}: {terr}")
        return
    herr = voice_transfer.hangup_call_for_mpc_member(mpc_name, member_address, org.auth_id, auth_token)
    if herr is not None:
        logger.error(f"_remove_racing_supervisor: error removing member from the call {sip}: {herr}")
