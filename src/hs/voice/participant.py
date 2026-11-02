import uuid as _uuid

from hs.access import ring_address
from hs.infra.log import logger
from hs.plivo import voice as pv
from hs.routing import account_read as ar
from hs.routing import constants as rc
from hs.routing.invitation_models import AgentAndContactMapping
from hs.routing.redis_store import RoutingRedis
from hs.voice import constants as vc
from hs.voice.redis_store import VoiceRedis


def _get_mpc_name(mapping: AgentAndContactMapping, contact_uuid: str) -> str:
    if mapping.qnr_run_uuid:
        return f"mpc_{mapping.qnr_run_uuid}"
    return f"main_mpc_{mapping.flow_run_uuid}_{contact_uuid}"


def _alias_header_value(participant_alias: str) -> str:
    from hs.voice.agent_leg import encode_sip_header_value

    alias = participant_alias
    if len(participant_alias) > vc.MAX_PARTICIPANT_ALIAS_LEN:
        alias = participant_alias[: vc.MAX_PARTICIPANT_ALIAS_LEN] + "..."
    return encode_sip_header_value(alias)


def hold_unhold_members(
    mpc_name: str,
    auth_id: str,
    auth_token: str,
    contact_uuid: str,
    agent_sip: str,
    hold_value: bool,
    member_call_uuid: str = "",
) -> str | None:
    from hs.voice.agent_leg import _get_participants

    participants, err = _get_participants(mpc_name, auth_id, auth_token)
    if err is not None:
        logger.error(msg=f"hold_unhold_members: error in getting participants contact_uuid={contact_uuid}: {err}")
        return err
    members = []
    for p in participants or []:
        if not p.get("exit_cause") and p.get("member_address", "") != agent_sip:
            if member_call_uuid != "" and p.get("call_uuid", "") != member_call_uuid:
                continue
            members.append(p)
    client = pv.PlivoVoiceClient(auth_id, auth_token)
    mpc_id = pv.make_mpc_id(friendly_name=mpc_name)
    for member in members:
        _b, _s, uerr = client.mpc_update_participant(mpc_id, str(member.get("member_id", "")), {"hold": hold_value})
        if uerr is not None:
            logger.error(f"hold_unhold_members: hold update failed contact_uuid={contact_uuid}: {uerr}")
            return uerr
    return None


def validate_add_participant_req_owner(
    req: dict,
    validate_participant_count: bool,
    routing_redis: RoutingRedis | None = None,
    voice_redis: VoiceRedis | None = None,
):
    from hs.conversations.transfer import transfer_allowed

    rr = routing_redis or RoutingRedis()
    vr = voice_redis or VoiceRedis()
    contact_uuid = req.get("contact_uuid", "")
    if validate_participant_count:
        try:
            count = vr.get_current_participant_count(contact_uuid)
        except Exception as e:
            logger.error(
                msg=f"validate_add_participant_req_owner: Error while fetching current participant count "
                f"contact_uuid={contact_uuid}: {e}"
            )
            return None, None, str(e)
        if count == vc.MAX_SECONDARY_PARTICIPANTS:
            logger.error(msg="validate_add_participant_req_owner: Maximum secondary participants reached")
            return None, None, "maximum secondary participants reached"
    org = ar.get_org_details_for_agent_sip(req.get("agent_sip", ""))
    if org is None:
        logger.error(
            msg=f"validate_add_participant_req_owner: failed to get org details for agent sip agent_sip={req.get('agent_sip', '')}"
        )
        return None, None, rc.NOT_FOUND
    mapping = AgentAndContactMapping.parse_redis(rr.get_contact_to_agent_mapping(contact_uuid))
    if mapping is None:
        logger.error(msg="validate_add_participant_req_owner: Error while fetching the contact to agent mapping")
        return None, None, rc.CACHE_MISS
    if mapping.agent_sip_endpoint != req.get("agent_sip", ""):
        logger.error(
            msg="validate_add_participant_req_owner: Request agent is not same as the mapped agent for this conversation"
        )
        return mapping, None, "not the owner of the channel"
    _allowed, add_allowed, conv_details = transfer_allowed(contact_uuid, "call", True, True)
    if not add_allowed:
        logger.error(msg="validate_add_participant_req_owner: Add participant is not allowed for this contact_uuid")
        return None, None, "add participant is not allowed"
    qnr_run_uuid = mapping.qnr_run_uuid
    if not mapping.is_inbound:
        qnr_run_uuid = mapping.flow_run_uuid
    contact_params = rr.get_contact_parameters(contact_uuid, qnr_run_uuid)
    if mapping.is_inbound and contact_params is None:
        logger.error(msg="validate_add_participant_req_owner: Error while fetching contact parameters")
        return None, None, rc.CACHE_MISS
    if not mapping.is_inbound and contact_params is None:
        contact_params = {
            "from_number": (conv_details or {}).get("from", ""),
            "contact_uuid": contact_uuid,
            "contact_channel": (conv_details or {}).get("parent_channel", ""),
            "flow_run_uuid": mapping.flow_run_uuid,
            "queue_and_route_run_uuid": mapping.flow_run_uuid,
            "queue_uuid": str(_uuid.uuid4()),
            "auth_id": org.auth_id,
            "auth_token": getattr(org, "auth_token", "") or "",
            "to": (conv_details or {}).get("to", ""),
            "conversation_number": (conv_details or {}).get("conversation_number", ""),
        }
        import json as _json

        try:
            rr.store_contact_params(
                contact_uuid, mapping.flow_run_uuid, _json.dumps(contact_params, separators=(",", ":"))
            )
        except Exception as se:
            logger.error(msg="validate_add_participant_req_owner: Error while storing contact parameters")
            return None, None, str(se)
    return mapping, contact_params, None


def add_participant(
    req: dict, routing_redis: RoutingRedis | None = None, voice_redis: VoiceRedis | None = None
) -> tuple[str | None, str | None]:
    contact_uuid = req.get("contact_uuid", "")
    mapping, contact_params, err = validate_add_participant_req_owner(
        req, True, routing_redis=routing_redis, voice_redis=voice_redis
    )
    if err is not None:
        logger.error(msg=f"add_participant: failed validation for add participant: {err}")
        return None, err
    from_number = mapping.to_number
    if not mapping.is_inbound:
        from_number = mapping.from_number
        mapping.qnr_run_uuid = mapping.flow_run_uuid
    mpc_name = _get_mpc_name(mapping, contact_uuid)
    alias = _alias_header_value(req.get("participant_alias", "") or "")
    sip_headers = "Add_Participant=true" + (f",Participant_Alias={alias}" if alias else "")
    logger.info(msg=f"add_participant: adding participant to MPC with sip headers sipHeaders={sip_headers}")
    auth_id = contact_params.get("auth_id", "")
    signing_auth_id, signing_auth_token = ar.contact_pair(contact_params)
    cs_url = pv.mpc_call_status_callback_url(auth_id, contact_uuid, mapping.qnr_run_uuid)
    st_url = pv.mpc_status_callback_url(auth_id, contact_uuid, mapping.qnr_run_uuid)
    rec_url = pv.mpc_recording_callback_url(auth_id, contact_uuid, mapping.qnr_run_uuid)
    tr_url = pv.mpc_transcription_callback_url(auth_id, contact_uuid)
    _tok, record_value, gerr = ar.get_organisation_auth_token_and_record_value(auth_id, mapping.is_inbound)
    if gerr is not None:
        logger.error(msg="add_participant: failed to get org record value for agent sip")
        return None, gerr
    body = {
        "from": from_number,
        "to": ring_address(req.get("member", "")),
        "sip_headers": sip_headers,
        "call_status_callback_url": cs_url,
        "call_status_callback_method": "POST",
        "record": record_value,
        "status_callback_url": st_url,
        "status_callback_method": "POST",
        "status_callback_events": "mpc-state-changes,participant-state-changes,add-participant-api-events",
        "role": vc.MPC_ROLE_AGENT_TITLE,
        "relay_dtmf_inputs": False,
        "ring_timeout": 45,
        "recording_callback_url": rec_url,
        "recording_callback_method": "POST",
        "create_mpc_with_single_participant": False,
        "record_participant_track": record_value,
        "transcript": record_value,
        "transcription_url": tr_url,
    }
    resp_body, _status, aerr = pv.PlivoVoiceClient(signing_auth_id, signing_auth_token).mpc_add_participant(
        pv.make_mpc_id(friendly_name=mpc_name), body, raw_body=pv.compact_json(body)
    )
    calls = (resp_body or {}).get("calls") or []
    if aerr is not None or not calls:
        logger.error(msg=f"add_participant: failed to add agent to MPC: {aerr}")
        from hs.infra.counters import MetricName, increase_counter

        increase_counter(MetricName.MPC_FAILURE, {"output_state": "add_participant"})
        return None, aerr
    leg_call_uuid = str(calls[0].get("call_uuid", ""))
    logger.info(msg=f"add_participant: added participant to MPC legCallUUID={leg_call_uuid}")
    # counted from the add, so a quick next add sees it; a failed add-completed callback drops it
    (voice_redis or VoiceRedis()).set_current_participant(contact_uuid, req.get("member", ""))
    return leg_call_uuid, None


def remove_participant(
    req: dict, routing_redis: RoutingRedis | None = None, voice_redis: VoiceRedis | None = None
) -> str | None:
    _mapping, contact_params, err = validate_add_participant_req_owner(
        req, False, routing_redis=routing_redis, voice_redis=voice_redis
    )
    if err is not None:
        logger.error(msg=f"remove_participant: Failed to validate owner of the call: {err}")
        return err
    auth_id, auth_token = ar.contact_pair(contact_params)
    logger.info(msg="remove_participant: removing participant from MPC")
    if not auth_id or not auth_token:
        return "failed to create plivo client"
    _b, _s, derr = pv.PlivoVoiceClient(auth_id, auth_token).call_delete(req.get("member_call_uuid", ""))
    if derr is not None:
        logger.error(msg=f"remove_participant: failed to remove participant from MPC: {derr}")
        return derr
    return None


def merge_participant(
    req: dict, routing_redis: RoutingRedis | None = None, voice_redis: VoiceRedis | None = None
) -> str | None:
    contact_uuid = req.get("contact_uuid", "")
    mapping, contact_params, err = validate_add_participant_req_owner(
        req, False, routing_redis=routing_redis, voice_redis=voice_redis
    )
    if err is not None:
        logger.error(msg=f"merge_participant: Failed to validate owner of the call: {err}")
        return err
    mpc_name = _get_mpc_name(mapping, contact_uuid)
    logger.info(msg="Merge Participant: merging participant to MPC")
    herr = hold_unhold_members(
        mpc_name, *ar.contact_pair(contact_params), contact_uuid, req.get("agent_sip", ""), False
    )
    member_identifier = req.get("member", "") or ""
    if member_identifier.startswith("+"):
        member_identifier = member_identifier[1:]
    if herr is not None:
        logger.error(msg=f"merge_participant: failed to unhold the participants: {herr}")
        _update_secondary_participant(member_identifier, contact_uuid, contact_params, "add_participant_merge_failed")
        return herr
    _update_secondary_participant(member_identifier, contact_uuid, contact_params, "add_participant_merge_success")
    return None


def _update_secondary_participant(member: str, contact_uuid: str, contact_params: dict, agent_state: str) -> str | None:
    from hs.conversations.transfer import update_secondary

    uerr = update_secondary(
        participant_identifier=member,
        conversation_number=contact_params.get("conversation_number", ""),
        type=vc.PARTICIPANT_TYPE_ADD_PARTICIPANT,
        secondary_agent_state=agent_state,
    )
    if uerr is not None:
        logger.error(
            msg=f"_update_secondary_participant: failed to update secondary participant contactUUID={contact_uuid}"
        )
        return uerr
    return None


def _owner_params_for(req: dict, routing_redis: RoutingRedis | None = None):
    rr = routing_redis or RoutingRedis()
    contact_uuid = req.get("contact_uuid", "")
    mapping = AgentAndContactMapping.parse_redis(rr.get_contact_to_agent_mapping(contact_uuid))
    if mapping is None:
        logger.error(msg="_owner_params_for: Failed to validate owner of the call")
        return None, None, rc.CACHE_MISS
    if mapping.agent_sip_endpoint != req.get("agent_sip", ""):
        logger.error(msg="_owner_params_for: Request agent is not same as the mapped agent for this conversation")
        return None, None, "not the owner of the channel"
    qnr_run_uuid = mapping.qnr_run_uuid
    if not mapping.is_inbound:
        qnr_run_uuid = mapping.flow_run_uuid
    contact_params = rr.get_contact_parameters(contact_uuid, qnr_run_uuid)
    if contact_params is None:
        logger.error(msg="_owner_params_for: Failed to get contact parameters")
        return None, None, rc.CACHE_MISS
    return mapping, contact_params, None


def hold_participant(req: dict, routing_redis: RoutingRedis | None = None) -> str | None:
    mapping, contact_params, err = _owner_params_for(req, routing_redis)
    if err is not None:
        return err
    contact_uuid = req.get("contact_uuid", "")
    logger.info(msg="hold_participant: holding participant")
    herr = hold_unhold_members(
        _get_mpc_name(mapping, contact_uuid),
        *ar.contact_pair(contact_params),
        contact_uuid,
        req.get("agent_sip", ""),
        bool(req.get("hold_value", False)),
        req.get("member_call_uuid", "") or "",
    )
    if herr is not None:
        logger.error(msg=f"hold_participant: failed to hold the participants: {herr}")
        return herr
    return None


def switch_participant(req: dict, routing_redis: RoutingRedis | None = None) -> str | None:
    mapping, contact_params, err = _owner_params_for(req, routing_redis)
    if err is not None:
        return err
    contact_uuid = req.get("contact_uuid", "")
    mpc_name = _get_mpc_name(mapping, contact_uuid)
    logger.info(msg="switch_participant: switching participant")
    switch_value = bool(req.get("switch_value", False))
    signing = ar.contact_pair(contact_params)
    herr = hold_unhold_members(mpc_name, *signing, contact_uuid, req.get("agent_sip", ""), switch_value)
    if herr is not None:
        logger.error(msg=f"switch_participant: failed to hold all the participants: {herr}")
        return herr
    herr = hold_unhold_members(
        mpc_name,
        *signing,
        contact_uuid,
        req.get("agent_sip", ""),
        not switch_value,
        req.get("member_call_uuid", "") or "",
    )
    if herr is not None:
        logger.error(msg=f"switch_participant: failed to unhold the participant with memberCallUUID: {herr}")
        return herr
    logger.info(msg="switch_participant: successfully switched the participants")
    return None
