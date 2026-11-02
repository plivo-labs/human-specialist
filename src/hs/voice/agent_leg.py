import re
import time
import urllib.parse
import uuid
from contextlib import closing
from datetime import datetime, timedelta

from hs.access import agent_address, ring_address
from hs.infra.context import context_db_session, set_db_session_rollback
from hs.infra.counters import MetricName, increase_counter
from hs.infra.helpers import is_valid_phone_number
from hs.infra.log import logger
from hs.plivo import voice as pv
from hs.routing import account_read as ar
from hs.routing import active_queue as aq
from hs.routing import constants as rc
from hs.routing import sticky as routing_sticky
from hs.routing.invitation_models import AgentAndContactMapping
from hs.routing.redis_store import RoutingRedis

# module imports: both import this module the same way
from hs.voice import callbacks as voice_callbacks
from hs.voice import constants as vc
from hs.voice import hangup as voice_hangup
from hs.voice.redis_store import VoiceRedis


class AddAgentsToMPCError(Exception):
    pass


def encode_sip_header_value(value: str) -> str:
    """Plivo takes SIP header values in [A-Za-z0-9%] only: every other byte is percent-encoded."""
    encoded = "".join(c if c.isascii() and c.isalnum() else "".join(f"%{b:02X}" for b in c.encode()) for c in value)
    # the 64-character cut must not leave a partial %XX escape
    return re.sub(r"%[0-9A-F]?$", "", encoded[:64])


def decode_sip_header_value(encoded: str) -> tuple[str, str | None]:
    s = encoded.replace("%2E", ".")
    if re.search(r"%(?![0-9A-Fa-f]{2})", s):
        return "", "invalid URL escape"
    return urllib.parse.unquote(s), None


def extract_phone_number_and_extension_if_any(number: str, omit_plus: bool) -> tuple[str, str]:
    if not re.search(r"\+?[0-9]+#?[0-9]+", number or ""):
        return number, ""
    parts = number.split("#")
    ph_number, extension = parts if len(parts) == 2 else (number, "")
    return (ph_number.removeprefix("+") if omit_plus else ph_number), extension


def handle_restricted_number(
    from_number: str, to_number: str, contact_name: str, call_uuid: str, log_context: str
) -> tuple[str, str]:
    if to_number.startswith("sip:") and from_number.startswith("sip:"):
        # NANP has no exchange starting with 0, so this restricted-caller number is never a real subscriber.
        return "+19090909090", vc.RESTRICTED
    # Plivo rejects a SIP address as the agent leg's From: keep a dialable user part, else ring from the dialed number.
    if from_number.startswith("sip:"):
        user = from_number[4:].split("@", 1)[0].split(";", 1)[0]
        return (user if re.fullmatch(r"\+?[1-9]\d{6,14}", user) else to_number), contact_name
    if vc.RESTRICTED not in (from_number or "").lower():
        return from_number, contact_name
    logger.info(f"{log_context}: fromIdentifier was Restricted, updated to match To number call_uuid={call_uuid}")
    return to_number, "No Caller ID"


def _spawn(target, *args) -> None:
    # Detached spawn (hs/infra/bg_spawn.py): keeps log context, but the worker gets its OWN DB
    # session -- sharing the request Session across threads raced its COMMIT against the
    # worker's SAVEPOINT (psycopg2 NoActiveSqlTransaction).
    from hs.infra.bg_spawn import spawn_detached

    spawn_detached(target, *args, log_context="voice_agent_leg")


def get_account_details_for_sip_endpoints(sips: list[str]) -> tuple[list[dict], str | None]:
    rows = ar.get_account_rows_for_sip_endpoints(sips)
    if rows is None:
        return [], "error fetching account details for sip endpoints"
    return [row._asdict() for row in rows], None


_GET_CONTACTS_FAILED = "getting the contact failed"


def _get_contact_name_once(contact_uuid: str) -> str | None:
    """The parent conversation's contact name, read in an isolated read transaction."""
    from hs.conversations.store import Conversation
    from hs.infra.db import SessionLocal

    with closing(SessionLocal()) as lookup_db:
        token = context_db_session.set(lookup_db)
        try:
            conversation = Conversation.get_conversation_for_child_contact_uuid(contact_uuid)
            return conversation.parent_conversation.contact_name if conversation else None
        finally:
            # A lower layer may answer a SQL error while leaving the transaction aborted: never leak it.
            context_db_session.reset(token)
            lookup_db.rollback()


def get_contact_details_for_contact_uuid(contact_uuid: str) -> dict | None:
    """Optional display data, retried once; None (never an abort) when it stays unavailable."""
    for attempt in range(2):
        if attempt:
            logger.warning(f"contact details not ready; retrying once contact_uuid={contact_uuid}")
            time.sleep(0.05)
        try:
            name = _get_contact_name_once(contact_uuid)
            if name:
                return {"name": name}
            reason = _GET_CONTACTS_FAILED
        except Exception as exc:
            reason = str(exc) or _GET_CONTACTS_FAILED
    logger.warning(
        f"contact details unavailable after bounded retry; using caller identifier contact_uuid={contact_uuid} "
        f"reason={reason}"
    )
    return None


def _get_participants(mpc_name: str, auth_id: str, auth_token: str):
    body, status, err = pv.PlivoVoiceClient(auth_id, auth_token).mpc_list_participants(
        pv.make_mpc_id(friendly_name=mpc_name)
    )
    if err is not None:
        return None, voice_hangup.provider_voice_callback_error(err, status, retry_safe=True)
    if body is None:
        return None, voice_hangup.retryable_voice_callback_error("provider participants response unavailable")
    objects = body.get("objects") or []
    return [p | {"member_address": agent_address(p.get("member_address") or "")} for p in objects], None


def stop_wait_prompt_for_customer(cp: dict) -> str | None:
    mpc_name = "mpc_" + cp.get("queue_and_route_run_uuid", "")
    signing = ar.contact_pair(cp)
    participants, err = _get_participants(mpc_name, *signing)
    if err is not None:
        logger.error(
            f"stop_wait_prompt_for_customer: error in getting MPC participants "
            f"qnr_run_uuid={cp.get('queue_and_route_run_uuid', '')}: {err}"
        )
        return err
    customer = next(
        (p for p in participants if str(p.get("role", "")).lower() == vc.MPC_ROLE_CUSTOMER and not p.get("exit_cause")),
        None,
    )
    if customer is None:
        logger.error(f"stop_wait_prompt_for_customer: failed to get customer ID call_uuid={cp.get('contact_uuid', '')}")
    elif member_id := str(customer.get("member_id", "")):
        client, mpc_id = pv.PlivoVoiceClient(*signing), pv.make_mpc_id(friendly_name=mpc_name)
        client.mpc_stop_play_audio(mpc_id, member_id)
        client.mpc_stop_speak(mpc_id, member_id)
    return None


def hangup_call_for_mpc_member(mpc_name: str, member_address: str, auth_id: str, auth_token: str) -> str | None:
    participants, err = _get_participants(mpc_name, auth_id, auth_token)
    if err is not None:
        logger.error(f"hangup_call_for_mpc_member: failed to get participants mpc={mpc_name}: {err}")
        return err
    for p in participants:
        if member_address == p.get("member_address") and not p.get("exit_cause"):
            _body, _status, derr = pv.PlivoVoiceClient(auth_id, auth_token).call_delete(str(p.get("call_uuid", "")))
            if derr is not None:
                logger.error(
                    f"hangup_call_for_mpc_member: failed to trigger hangup for member "
                    f"member_call_uuid={p.get('call_uuid', '')}: {derr}"
                )
                return derr
    return None


def add_participants_to_mpc_utility(
    auth_id: str, auth_token: str, call_uuid: str, mpc_name: str, params: dict
) -> tuple[dict | None, str | None]:
    # Plivo rejects a CallerName over 50 characters; a long SIP caller address must not cost the call its agent.
    params["caller_name"] = (params.get("caller_name") or "")[:50]
    body, status, err = pv.PlivoVoiceClient(auth_id, auth_token).mpc_add_participant(
        pv.make_mpc_id(friendly_name=mpc_name), params, raw_body=pv.compact_json(params)
    )
    if err is not None:
        logger.error(f"add_participants_to_mpc_utility: failed to add participant to MPC calluuid={call_uuid}: {err}")
        return None, voice_hangup.provider_voice_callback_error(err, status, retry_safe=False)
    return body, None


def _sip_header(base: str, **fields) -> str:
    """base,Key=value for each non-empty field, in the given order."""
    return base + "".join(f",{k}={v}" for k, v in fields.items() if v)


def _conversation_id(conversation_number: str) -> str:
    from hs.conversations.store import ConversationParent

    parent = ConversationParent.get_by_conversation_number(conversation_number)
    return str(parent.uuid) if parent else ""


def _caller_name(cp: dict, agent_sip: str) -> str:
    """The contact's name for the agent's caller ID, else the caller's number."""
    call_uuid, from_number = cp.get("contact_uuid", ""), cp.get("from_number", "")
    # Only an account leg shows the contact's name: a dialed phone number (external transfer) keeps the number.
    if not ar.get_account_rows_for_sip_endpoints([agent_sip]):
        logger.error(f"_caller_name: Could not get agent details for sip endpoint agent_sip={agent_sip}")
        return from_number
    details = get_contact_details_for_contact_uuid(call_uuid)
    if details is None:
        logger.warning(
            f"_caller_name: contact name unavailable; routing with caller identifier "
            f"contact_uuid={call_uuid}: {_GET_CONTACTS_FAILED}"
        )
        return from_number
    return details.get("name", "")[:50] or from_number


def _number_alias(cp: dict) -> str:
    """The dialed Plivo number's alias (shown to the agent), if the destination is a phone number."""
    from hs.plivo.numbers import get_plivo_number

    destination = cp.get("to", "")
    if not is_valid_phone_number(destination):
        logger.info("Skipping Plivo number metadata lookup for a non-phone destination")
        return ""
    try:
        # A record read: the tenant sees every number in its tree, a subaccount only its own.
        return get_plivo_number(cp.get("auth_id", ""), cp.get("auth_token", ""), destination).get("alias") or ""
    except Exception as e:
        logger.error(f"Error in fetching number details: {e}")
        return ""


def _transferring_agent_name(cp: dict, current_primary_agent_sip: str) -> str:
    call_uuid = cp.get("contact_uuid", "")
    if current_primary_agent_sip:
        details, err = get_account_details_for_sip_endpoints([current_primary_agent_sip])
        if err is not None or not details:
            logger.error(
                f"Error fetching agent details for current primary agent sip endpoint "
                f"sip={current_primary_agent_sip}: {err or 'no account row'}"
            )
            return ""
        return details[0]["name"]
    from hs.conversations import management as cm
    from hs.routing.invitation import _ensure_request_context

    try:
        _ensure_request_context()
        resp = cm.get_agent_details(call_uuid)
        if not resp.error and resp.data is not None:
            return (resp.data if isinstance(resp.data, dict) else resp.data.model_dump()).get("agent_name") or ""
    except Exception as e:
        logger.error(f"get_agent_details_from_contact_uuid in-process failed contact={call_uuid}: {e}")
    logger.error(f"Error fetching agent1 details in transfer flow contactUUID={call_uuid}: getting the agents failed")
    return ""


def add_agents_to_mpc(
    cp: dict, agents: list[str], current_primary_agent_sip: str, is_transfer: bool, transfer_type: str, team_id: str
) -> None:
    """Dial the offered agents into the caller's MPC, then record the invite (retry count, invited
    agents, routing events, the agent legs' call uuids). transfer_type may instead name reverse
    routing. Raises AddAgentsToMPCError."""
    rr = RoutingRedis()
    auth_id, call_uuid = cp.get("auth_id", ""), cp.get("contact_uuid", "")
    flow_run_uuid, qnr_run_uuid = cp.get("flow_run_uuid", ""), cp.get("queue_and_route_run_uuid", "")
    conversation_number = cp.get("conversation_number", "")
    run_ids = f"flowRunUUID={flow_run_uuid} qnrRunUUID={qnr_run_uuid}"
    org = ar.get_org_for_auth_id(auth_id)
    if org is None:
        logger.error(f"add_agents_to_mpc: Could not fetch org value auth Id={auth_id}")
        raise AddAgentsToMPCError("could not fetch org value")
    raw_cfg = rr.get_queue_and_route_config(flow_run_uuid, qnr_run_uuid)
    qnr_config, cfg_err = routing_sticky.parse_queue_and_route_config(raw_cfg)
    if qnr_config is None and not is_transfer:  # a transfer off an outbound call has no queue config
        logger.error(f"add_agents_to_mpc: Could not fetch queue and route config {run_ids}")
        raise AddAgentsToMPCError(cfg_err)
    agent_teams, team_name = [""] * len(agents), ""
    if not is_transfer:
        teams = ar.get_team_names_for_sips_in_queue(list(agents), qnr_run_uuid)
        if teams is None:
            logger.error(f"add_agents_to_mpc: Could not get agent details for team name contactUuid={call_uuid}")
        else:
            agent_teams = [encode_sip_header_value(t) for t in teams]
    elif transfer_type == rc.TRANSFER_TYPE_TEAM:
        team_name = encode_sip_header_value(ar.get_team_name(team_id) or "")
    contact_name = _caller_name(cp, agents[0])
    name_tag = _number_alias(cp)
    alias = encode_sip_header_value(name_tag)
    agent1_name = _transferring_agent_name(cp, current_primary_agent_sip) if is_transfer else ""
    ring_timeout, rt_err = rr.get_reservation_timeout(auth_id, rc.CHANNEL_CALL)
    if rt_err is not None:
        ring_timeout = 45
        logger.error(
            f"add_agents_to_mpc: Unable to fetch reservation timeout value, so taking default value Ring "
            f"timeout={ring_timeout} calluuid={call_uuid}"
        )
    # One offer per ring attempt, shared by its parallel legs; stored before the add, since a join can beat its reply.
    offer_id = str(uuid.uuid4())
    if oerr := rr.set_offer(qnr_run_uuid, agents, offer_id, vc.CALL_RESERVATION_MAX_TIMEOUT_MINUTES * 60):
        raise AddAgentsToMPCError(oerr)
    _auth_token_db, record_value, rec_err = ar.get_organisation_auth_token_and_record_value(
        auth_id, cp.get("inbound", True)
    )
    if rec_err is not None:
        logger.error(
            f"add_agents_to_mpc: Error occurred while fetching record value from org table calluuid={call_uuid}"
        )
    # The public headers (HsKind, HsOffer, HsConversation, HsAutoAnswer) every agent's softphone reads.
    public = _sip_header(
        f",HsKind={'transfer' if is_transfer else 'offer'}",
        HsOffer=encode_sip_header_value(offer_id),
        HsConversation=encode_sip_header_value(_conversation_id(conversation_number)),
        HsAutoAnswer=str(qnr_config.auto_accept_delay) if not is_transfer and qnr_config.allow_auto_accept else "",
    )
    if is_transfer:
        base = f"header1=Incoming%20call%20transfer,header2={conversation_number}"
        header = _sip_header(base, FromAgent=encode_sip_header_value(agent1_name), AliasName=alias, TeamName=team_name)
        headers = [header + public] * len(agents)
    else:
        headers = [_sip_header("header1=Incoming%20call", AliasName=alias, TeamName=t) + public for t in agent_teams]
    for agent in agents:
        rr.delete_reverse_routing_lock(agent, conversation_number, rc.CHANNEL_CALL)
    sip_headers = "<".join(headers)
    logger.info(f"sip headers have been formed: {sip_headers}")
    external = is_transfer and transfer_type in (rc.TRANSFER_TYPE_EXTERNAL, rc.TRANSFER_TYPE_WARM_EXTERNAL)
    from_identifier = cp.get("to" if external else "from_number", "")
    targets = [ring_address(a) for a in agents]
    agents_string, extension = "<".join(targets), ""
    if len(agents) == 1:
        agents_string, extension = extract_phone_number_and_extension_if_any(targets[0], False)
        logger.info(
            "add_agents_to_mpc: agents to be added after processing agents array "
            f"contactUuid={call_uuid} count={len(agents)} extension={bool(extension)}"
        )
    from_identifier, contact_name = handle_restricted_number(
        from_identifier, cp.get("to", ""), contact_name, call_uuid, "add_agents_to_mpc"
    )
    # Insertion order is the wire order; a false record/transcript and empty send_digits
    # are omitted.
    params = {
        "role": vc.MPC_ROLE_AGENT_TITLE,
        "from": from_identifier,
        "to": agents_string,
        "caller_name": contact_name,
        "call_status_callback_url": pv.mpc_call_status_callback_url(auth_id, call_uuid, qnr_run_uuid),
        "call_status_callback_method": "POST",
        "sip_headers": sip_headers,
        "ring_timeout": str(ring_timeout),
        "delay_dial": 0,
        "recording_callback_url": pv.mpc_recording_callback_url(auth_id, call_uuid, qnr_run_uuid),
        "recording_callback_method": "POST",
        "status_callback_url": pv.mpc_status_callback_url(auth_id, call_uuid, qnr_run_uuid),
        "status_callback_method": "POST",
        **({"record": True} if record_value else {}),
        "status_callback_events": vc.MPC_AGENT_STATUS_CALLBACK_EVENTS,
        "create_mpc_with_single_participant": False,
        **({"send_digits": extension} if extension else {}),
        "transcription_url": pv.mpc_transcription_callback_url(auth_id, call_uuid),
        **({"transcript": True} if record_value else {}),
        "record_participant_track": record_value,
    }
    logger.info(
        f"add_agents_to_mpc: params sent to add a participant to the MPC contactUuid={call_uuid} "
        f"addParticipantParams={params}"
    )
    signing = ar.contact_pair(cp)
    res, err = add_participants_to_mpc_utility(*signing, call_uuid, f"mpc_{qnr_run_uuid}", params)
    if err is not None:
        logger.error(f"add_agents_to_mpc: failed to add agents to MPC calluuid={call_uuid}: {err}")
        increase_counter(MetricName.MPC_FAILURE, {"output_state": "add_agent"})
        raise AddAgentsToMPCError(err)
    flow_transfer = rc.TRANSFER_TYPE_FLOW in (transfer_type or "")
    attempt_count = 1
    if not is_transfer or flow_transfer:
        new_val = rr.set_retry_val(flow_run_uuid, qnr_run_uuid)
        if new_val is None:
            logger.error(f"add_agents_to_mpc: Could not set retry value in routing {run_ids}")
        attempt_count = int(new_val or 0)
    rr.set_invited_agents(
        flow_run_uuid, qnr_run_uuid, agents, vc.CALL_RESERVATION_MAX_TIMEOUT_MINUTES * 60, attempt_count
    )
    logger.info(f"add_agents_to_mpc: MPC add participant response call_uuid={call_uuid} response={res}")
    auto_accept = not is_transfer and qnr_config.allow_auto_accept
    _emit_offers(cp, agents, offer_id, int(ring_timeout), auto_accept)
    call_uuid_list = [c.get("call_uuid", "") for c in ((res or {}).get("calls") or [])]
    if is_transfer:
        set_err = VoiceRedis().set_transfer_mpc_call_uuids(qnr_run_uuid, call_uuid_list, ring_timeout)
        if set_err is not None:
            logger.error(f"add_agents_to_mpc: error in storing channeltransfer mpc call uuids: {set_err}")
    else:
        set_err = VoiceRedis().set_mpc_call_uuids(flow_run_uuid, qnr_run_uuid, call_uuid_list, ring_timeout)
        if set_err is not None:
            logger.error(f"add_agents_to_mpc: error in storing mpc call uuids: {set_err}")


def _emit_offers(cp: dict, agents: list[str], offer_id: str, ring_timeout: int, auto_accept: bool) -> None:
    """offer.created for each offered agent (a phone number target is no agent)."""
    from hs.conversations.store import ConversationParent, emit

    if not (parent := ConversationParent.get_by_conversation_number(cp.get("conversation_number", ""))):
        return
    data = {"offer_id": offer_id, "queue_id": cp.get("queue_uuid") or None, "from": cp.get("from_number") or None}
    data |= {"ring_timeout_sec": ring_timeout, "auto_accept": auto_accept}
    for agent in agents:
        if aom := ar.get_aom_for_sip_endpoint(agent):
            emit("offer.created", parent, data, aom)


def _update_child_conversation_voice_accept(contact_uuid: str, agent_sip: str) -> str | None:
    from hs.conversations import child_update as ccu
    from hs.conversations.models import ConversationUpdateByContactUUID
    from hs.routing.invitation import _ensure_request_context

    _ensure_request_context()
    try:
        resp = ccu.update_child_conversation(
            ConversationUpdateByContactUUID(contact_uuid=contact_uuid, agent_sip_or_uuid=agent_sip)
        )
    except Exception as e:
        set_db_session_rollback()
        logger.error(
            f"voice update_child_conversation in-process failed contact_uuid={contact_uuid}: {e}", exc_info=True
        )
        return voice_hangup.retryable_voice_callback_error("child conversation persistence unavailable")
    if resp.status_code == 200:
        return None
    logger.error(
        f"voice update_child_conversation non-200 ({resp.status_code}) contact_uuid={contact_uuid} error={resp.error}"
    )
    if not resp.status_code or resp.status_code >= 500:
        return voice_hangup.retryable_voice_callback_error("child conversation persistence unavailable")
    return "updating the child conversation failed"


def set_agent_for_contact_and_update_conversation(
    call_uuid: str, qnr_run_uuid: str, cp: dict, member_address: str, participant_call_uuid: str
) -> str | None:
    from hs.conversations.store import claim_offer_outcome, emit_by_number

    rr, label = RoutingRedis(), "set_agent_for_contact_and_update_conversation"
    ids = f"contact UUID={call_uuid} Agent id={member_address}"
    agent_uuid = ar.get_aom_for_sip_endpoint(member_address)
    if agent_uuid is None:
        logger.error(f"{label}: aom not found for primary agent agent_sip={member_address}")
        return "aom not found"
    mapping = AgentAndContactMapping(
        agent_sip_endpoint=member_address,
        from_number=cp.get("from_number", ""),
        qnr_run_uuid=qnr_run_uuid,
        agent_uuid=agent_uuid,
        agent_call_uuid=participant_call_uuid,
        is_inbound=True,
        to_number=cp.get("to", ""),
    )
    raw_cfg = rr.get_queue_and_route_config(cp.get("flow_run_uuid", ""), qnr_run_uuid)
    qnr_config, cfg_err = routing_sticky.parse_queue_and_route_config(raw_cfg)
    if qnr_config is None:
        logger.error(f"{label}: unable to get queue and route config agent_sip={member_address}")
        return cfg_err
    conv_err = _update_child_conversation_voice_accept(call_uuid, member_address)
    if conv_err is not None:
        logger.error(f"{label}: error in updating conversation {ids}")
        return conv_err
    # Publish the primary-identity marker only after the durable update: a callback released for a
    # transient failure must replay as the same primary join, not read a half-written marker as a second agent.
    ttl = vc.EXPIRY_DURATION_IN_HOURS_CALL * 3600
    if rr.set_contact_to_agent_mapping(call_uuid, mapping.to_json(), ttl) is not None:
        logger.error(f"{label}: error in storing agent to contact mapping in cache")
        return voice_hangup.retryable_voice_callback_error("agent mapping cache persistence unavailable")
    if qnr_config.sticky_routing_type != rc.STICKY_NO_STICKY_ROUTING:
        in_assignment, sticky_err = routing_sticky.is_agent_in_routing_assignment(qnr_config, mapping.agent_uuid)
        if sticky_err is not None:
            logger.error(
                f"sticky routing: error in getting teams for agent agent_sip={member_address} "
                f"customer_number={mapping.from_number}"
            )
        if (
            in_assignment
            and routing_sticky.set_inbound_sticky_mapping(
                mapping.agent_uuid, mapping.from_number, qnr_config, cp.get("auth_id", "")
            )
            is not None
        ):
            logger.error(f"{label}: unable to update sticky routing cache agent_sip={member_address}")
    if (offer_id := rr.get_offer(qnr_run_uuid, member_address)) and claim_offer_outcome(offer_id, member_address):
        emit_by_number("offer.accepted", cp.get("conversation_number", ""), {"offer_id": offer_id}, agent_uuid)
    return None


def _end_failed_primary_join(db, call_uuid: str, qnr_run_uuid: str, member_address: str, set_err: str) -> str | None:
    """Tear the MPC down after a permanent set-agent failure, then mark the queue entry FAILED. The result is the
    teardown error, else the set-agent error: the join failed either way."""
    # The teardown has no provider idempotency key: mark the callback unsafe before trying it,
    # so a later commit failure keeps the claim instead of replaying the teardown.
    voice_callbacks.mark_unsafe_callback_effect(db, "primary_join_set_agent_failure_teardown")
    end_err = None
    try:
        voice_callbacks._end_mpc_calls_seam(call_uuid, qnr_run_uuid)
    except Exception as e:
        end_err = str(e)
    logger.error(
        f"_end_failed_primary_join: error in ending MPC calls contact UUID={call_uuid} Agent "
        f"id={member_address}: {end_err}"
    )
    # Once the MPC is gone the contact must not linger QUEUED. Best-effort: it never masks the result.
    if end_err is None:
        try:
            aq.delete_active_queue_entry(db, call_uuid, qnr_run_uuid, rc.CONTACT_STATE_FAILED)
        except Exception as e:
            logger.error(f"_end_failed_primary_join: could not mark queue entry FAILED call_uuid={call_uuid}: {e}")
    return end_err or set_err


def handle_primary_agent_join(
    call_uuid: str, qnr_run_uuid: str, cp: dict, params: dict, transfer_info: dict, db, *, replay=False
) -> str | None:
    """Handle the primary agent joining the MPC: reset the active-conference toggle, load the active
    queue entry, then set the agent on the contact and update the conversation. On success the queue
    entry is cleared (ACCEPTED) and the accepted routing events are staged for after the commit;
    on a permanent set-agent failure the MPC is torn down and the queue entry is marked FAILED. A
    retryable failure keeps the live MPC and queue row: the controller rolls back and releases only
    this claim. A replay of an accepted join only re-stages the effects. Returns an error string on
    failure, else None."""
    rr, label, member_address = RoutingRedis(), "handle_primary_agent_join", params.get("member_address", "")
    if VoiceRedis().toggle_active_conference(qnr_run_uuid, "false", 0) is not None:
        logger.error(f"{label}: error in resetting active conference call_uuid={call_uuid} qnr_run_uuid={qnr_run_uuid}")
    active_entry = aq.get_current_active_queue_entry_by_contact_uuid(db, call_uuid, qnr_run_uuid)
    if active_entry is None:
        if replay:
            logger.info(f"{label}: exact replay of an accepted primary join; ignoring call UUID={call_uuid}")
            return None
        logger.error(f"{label}: Could not fetch queue status call UUID={call_uuid}")
        # The caller left while this leg rang and the agent picked up before HS's cancel landed: withdraw the offer.
        voice_hangup.emit_offer_outcome(call_uuid, qnr_run_uuid, member_address, "")
        return "unable to fetch queue status"
    if not replay:
        leg = params.get("participant_call_uuid", "")
        set_err = set_agent_for_contact_and_update_conversation(call_uuid, qnr_run_uuid, cp, member_address, leg)
        if set_err is not None:
            if voice_hangup.is_retryable_voice_callback_error(set_err):
                return set_err
            return _end_failed_primary_join(db, call_uuid, qnr_run_uuid, member_address, set_err)
    logger.info(f"{label}: MPC started, so updating active queue entry Contact uuid={call_uuid}")
    try:
        aq.delete_active_queue_entry(db, call_uuid, qnr_run_uuid, rc.CONTACT_STATE_ACCEPTED)
    except Exception as e:
        logger.error(f"{label}: Could not update queue status call UUID={call_uuid}: {e}")
        return voice_hangup.retryable_voice_callback_error("queue status persistence unavailable")

    # (callback, run it detached when there is no session to stage on, *args)
    node, queue = cp.get("node_uuid", ""), cp.get("queue_uuid", "")
    effects = [
        (rr.set_recompute_flag_for_queue_position_calculation, False, node, queue),
        (stop_wait_prompt_for_customer, False, dict(cp)),
    ]
    if transfer_info.get("transfer_type", "") == rc.TRANSFER_TYPE_FLOW:
        effects.append((rr.delete_transfer_in_progress, False, call_uuid))
    for callback, detached, *args in effects:
        voice_callbacks.stage_or_run(db, callback, *args, spawn=_spawn if detached else None)
    return None


_ADD_SECONDARY_FAILED = "{}: failed to add secondary agent for the conversation call_uuid={} agent sip={}"


def handle_secondary_agent_join(
    call_uuid: str, qnr_run_uuid: str, cp: dict, params: dict, agent_mapping, transfer_info: dict
) -> str | None:
    """A transfer target joined and becomes the primary-to-be, who keeps the caller once the primary leaves: a warm
    transfer (or warm external) also adds them as a secondary participant; a cold or external transfer hangs up the
    current primary."""
    from hs.conversations.transfer import add_secondary, signal_chunk

    rr = RoutingRedis()
    member_address = params.get("member_address", "")
    transfer_type = transfer_info.get("transfer_type", "")
    if not transfer_type:
        logger.error(
            f"handle_secondary_agent_join: mpc not in transfer call_uuid={call_uuid} qnr_run_uuid={qnr_run_uuid}"
        )
    external = transfer_type == rc.TRANSFER_TYPE_EXTERNAL
    warm_external = transfer_type == rc.TRANSFER_TYPE_WARM_EXTERNAL
    cold = bool(transfer_type) and transfer_info.get("transfer_mode", "") == rc.TRANSFER_MODE_COLD
    if external or warm_external:
        name, identifier, identifier_type = member_address, member_address, transfer_type
    else:
        det = (get_account_details_for_sip_endpoints([member_address])[0] or [{}])[0]
        name, identifier, identifier_type = det.get("name"), det.get("uuid", ""), rc.TRANSFER_TYPE_AGENT
    signal_message = "" if name is None else rc.TRANSFER_ACCEPTED_MSG.replace("<name>", name, 1)
    agent_uuid = ar.get_aom_for_sip_endpoint(member_address)
    # Only a leg the transfer dialed succeeds the primary, and the first to join keeps the slot (another team
    # member's later join leaves it); a queue transfer offers the caller afresh, so its run has no successor.
    if transfer_type not in ("", rc.TRANSFER_TYPE_FLOW) and params.get("hs_kind") == "transfer":
        if agent_uuid is None and not (external or warm_external):
            logger.error(f"handle_secondary_agent_join: aom not found for secondary agent agent_sip={member_address}")
            return "aom not found"
        promoted = AgentAndContactMapping(
            agent_sip_endpoint=member_address,
            agent_call_uuid=params.get("participant_call_uuid", ""),
            agent_uuid=agent_uuid or "",
            from_number=cp.get("from_number", ""),
            qnr_run_uuid=qnr_run_uuid,
        )
        ttl = vc.EXPIRY_DURATION_IN_HOURS_ASYNC * 3600
        claimed = rr.set_to_be_primary_agent(qnr_run_uuid, promoted.to_json(), ttl, nx=True)
        if not claimed and rr.get_to_be_primary_agent(qnr_run_uuid) is None:
            logger.error(
                f"failed to update to be primary agent call_uuid={call_uuid} to be primary agent_sip={member_address}"
            )
            return voice_hangup.retryable_voice_callback_error("primary-agent cache persistence unavailable")
    if not (cold or external):
        payload = {"conversation_number": cp.get("conversation_number", ""), "secondary_agent_state": "transferred"}
        if warm_external:
            digits = params.get("digits_sent", "")
            payload["participant_identifier"] = member_address + (f"#{digits}" if digits else "")
            payload["type"] = vc.PARTICIPANT_TYPE_EXTERNAL
        else:
            payload["agent_sip_or_uuid"] = member_address
        # This use case can emit chunks and events before returning and has no callback idempotency key.
        voice_callbacks.mark_unsafe_callback_effect(None, "add_secondary_agent")
        add_err = add_secondary(**payload)
        if add_err is not None:
            logger.error(_ADD_SECONDARY_FAILED.format("handle_secondary_agent_join", call_uuid, member_address))
            return add_err
    else:
        voice_callbacks.mark_unsafe_callback_effect(None, "provider_hangup_primary_agent")
        signing = ar.contact_pair(cp)
        if hangup_call_for_mpc_member(f"mpc_{qnr_run_uuid}", agent_mapping.agent_sip_endpoint, *signing) is not None:
            logger.error(f"failed to hangup primary agent for cold transfer call_uuid={call_uuid}")
    if (offer_id := rr.get_offer(qnr_run_uuid, member_address)) and agent_uuid:
        from hs.conversations.store import claim_offer_outcome, emit_by_number

        if claim_offer_outcome(offer_id, member_address):
            emit_by_number("offer.accepted", cp.get("conversation_number", ""), {"offer_id": offer_id}, agent_uuid)
    # Transfer-accepted goes out only after the secondary write reached its result, keeping causal order.
    logger.info(f"handle_secondary_agent_join: saving signal chunk signalMsg={signal_message}")
    if signal_message and identifier:
        voice_callbacks.mark_unsafe_callback_effect(None, "secondary_join_signal")
        signal = (rc.SIGNAL_TRANSFER_ACCEPTED, identifier, identifier_type, signal_message)
        _spawn(signal_chunk, cp.get("contact_uuid", ""), rc.CHANNEL_CALL, *signal)
    return None


def handle_secondary_participant_join(call_uuid: str, cp: dict, params: dict) -> str | None:
    from hs.conversations.transfer import add_secondary

    vr = VoiceRedis()
    member_address = params.get("member_address", "")
    alias, derr = decode_sip_header_value(params.get("add_participant_alias_name", ""))
    if derr is not None:
        logger.info(f"participant_alias={params.get('add_participant_alias_name', '')}")
    # Cache desired state first: this write is idempotent and can safely retry
    # without replaying the non-idempotent secondary-participant use case.
    if vr.set_current_participant(call_uuid, member_address) is not None:
        logger.error(
            f"handle_secondary_participant_join: failed to set current add participant contactUUID={call_uuid}"
        )
        return voice_hangup.retryable_voice_callback_error("participant cache persistence unavailable")
    voice_callbacks.mark_unsafe_callback_effect(None, "add_secondary_participant")
    add_err = add_secondary(
        conversation_number=cp.get("conversation_number", ""),
        participant_identifier=member_address,
        secondary_agent_state="add_participant_success",
        type=vc.PARTICIPANT_TYPE_ADD_PARTICIPANT,
        participant_alias=alias,
    )
    if add_err is None:
        return None
    logger.error(_ADD_SECONDARY_FAILED.format("handle_secondary_participant_join", call_uuid, member_address))
    # It may have partially emitted chunks/events, so it is not retried: undo the optimistic cache state.
    if vr.remove_current_participant(call_uuid, member_address) is not None:
        logger.error(
            f"handle_secondary_participant_join: failed to compensate participant cache contactUUID={call_uuid}"
        )
    return add_err


def handle_agent_join(call_uuid: str, qnr_run_uuid: str, params: dict, db) -> str | None:
    rr = RoutingRedis()
    member_address, call_leg = params.get("member_address", ""), params.get("participant_call_uuid", "")
    logger.info(f"Agent participant join event invoked Contact uuid={call_uuid}")
    answered = f"{vc.CALL_ANSWERED_EVENT}:{member_address}"
    voice_callbacks.push_voice_event_metric(call_uuid, qnr_run_uuid, answered, "AgentJoinDuration")
    cp = rr.get_contact_parameters(call_uuid, qnr_run_uuid)
    if cp is None:
        logger.error("handle_agent_join: error in fetching call parameters from cache")
        increase_counter(MetricName.AGENT_JOIN_FAILURE, {"failure_type": "agent_join_failure"})
        return "contact params not found"
    transfer_info = rr.get_transfer_in_progress_info(call_uuid) or {}
    mapping = AgentAndContactMapping.parse_redis(rr.get_contact_to_agent_mapping(call_uuid))
    # A mapping for this exact run, agent and call leg is a replay of this primary join, not a second agent.
    leg_key = (qnr_run_uuid, member_address, call_leg)
    replay = (
        mapping is not None
        and all(leg_key)
        and (mapping.qnr_run_uuid, mapping.agent_sip_endpoint, mapping.agent_call_uuid) == leg_key
    )
    if mapping is None or replay:
        kind, failed = "primary_agent_join_failure", "failed to handle primary agent join"
        if replay:
            failed = "failed to resume matching primary agent join"
        err = handle_primary_agent_join(call_uuid, qnr_run_uuid, cp, params, transfer_info, db, replay=replay)
    elif params.get("add_participant_header", "") == vc.HEADER_PARTICIPANT_EXISTS_VALUE:
        kind, failed = "secondary_participant_join_failure", "failed to handle secondary agent join"
        err = handle_secondary_participant_join(call_uuid, cp, params)
    else:
        kind, failed = "secondary_agent_join_failure", "failed to handle secondary agent join"
        err = handle_secondary_agent_join(call_uuid, qnr_run_uuid, cp, params, mapping, transfer_info)
    if err is not None:
        logger.error(f"handle_agent_join: {failed} call_uuid={call_uuid}")
        increase_counter(MetricName.AGENT_JOIN_FAILURE, {"failure_type": kind})
    return err


def handle_participant_join(call_uuid: str, qnr_run_uuid: str, params: dict, db) -> tuple[str, str | None]:
    from hs.voice import outbound

    role, ids = str(params.get("participant_role", "")).lower(), f"callUUID={call_uuid} qnrRunUUID={qnr_run_uuid}"
    dial, leg = outbound.dialed(call_uuid), params.get("participant_call_uuid", "")
    if dial and role == vc.MPC_ROLE_AGENT and leg == call_uuid:
        # the dialling agent entered its own MPC: the lead is added now
        voice_callbacks.mark_unsafe_callback_effect(db, "outbound_add_lead")
        if (err := outbound.on_agent_join(call_uuid, qnr_run_uuid, dial)) is not None:
            return "", err
    elif dial and role == vc.MPC_ROLE_CUSTOMER:
        voice_callbacks.mark_unsafe_callback_effect(db, "outbound_lead_answered")
        if (err := outbound.on_customer_join(call_uuid, qnr_run_uuid, dial, params.get("member_id", ""))) is not None:
            return "", err
    elif role == vc.MPC_ROLE_AGENT:
        err = handle_agent_join(call_uuid, qnr_run_uuid, params, db)
        if err is not None:
            logger.error(f"handle_participant_join: failed to handle agent join event {ids}: {err}")
            return "", err
    elif role == vc.MPC_ROLE_SUPERVISOR:
        from hs.voice.monitor import handle_supervisor_join_event

        # The supervisor helper can write conversations, call Plivo and emit events before returning: keep the claim if a
        # later step fails, so a retry can't duplicate those effects.
        voice_callbacks.mark_unsafe_callback_effect(db, "supervisor_join_external_effects")
        member, mpc_name = str(params.get("member_address", "")), str(params.get("mpc_name", ""))
        err = handle_supervisor_join_event(call_uuid, qnr_run_uuid, member, mpc_name, leg, db)
        if err is not None:
            logger.error(f"handle_participant_join: failed to handle supervisor join event {ids}: {err}")
            return "", err
    elif role == vc.MPC_ROLE_CUSTOMER:
        joined = (vc.MPC_INITIALISED_CALLBACK_EVENT, "CustomerJoinDuration")
        voice_callbacks.push_voice_event_metric(call_uuid, qnr_run_uuid, *joined)
    else:
        logger.info(f"handle_participant_join: unhandled participant role in Participant Join Event {ids} role={role}")
    leg_fields = ("participant_call_uuid", "member_address", "participant_join_time", "participant_exit_time")
    call_leg = (call_uuid, *(params.get(f, "") for f in leg_fields), params.get("participant_role", ""), False)
    voice_callbacks.stage_or_run(db, voice_callbacks._create_call_leg_seam, *call_leg, spawn=_spawn)
    return "Participant joined to MPC", None


_ANSWER_TIME = re.compile(
    r"\s*(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?"
    r"(?:\s*(Z|UTC|GMT|[+-]\d{2}(?::?\d{2})?)(?:\s+[A-Za-z]{1,10})?)?\s*",
    re.IGNORECASE,
)


def parse_answer_time(answer_time: str) -> int | None:
    """Parse supported Plivo timestamps to exact Unix nanoseconds."""
    match = _ANSWER_TIME.fullmatch(answer_time or "")
    if match is None:
        return None
    date_part, time_part, fraction, offset = match.groups()
    offset_delta = timedelta()
    if offset and offset.upper() not in {"Z", "UTC", "GMT"}:
        compact = offset.replace(":", "")
        hours, minutes = int(compact[1:3]), int(compact[3:5] or 0)
        if hours > 23 or minutes > 59:
            return None
        offset_delta = (1 if compact[0] == "+" else -1) * timedelta(hours=hours, minutes=minutes)
    try:
        utc_naive = datetime.strptime(f"{date_part} {time_part}", "%Y-%m-%d %H:%M:%S") - offset_delta
    except (OverflowError, ValueError):
        return None
    seconds = int((utc_naive - datetime(1970, 1, 1)).total_seconds())
    return seconds * 1_000_000_000 + int(((fraction or "") + "0" * 9)[:9])


def handle_call_answered(call_uuid: str, qnr_run_uuid: str, params: dict) -> str | None:
    answer_time = params.get("answer_time", "")
    ns = parse_answer_time(answer_time)
    if ns is None:
        logger.warning(
            f"Provider answer timestamp is missing or invalid; acknowledging callback without timing "
            f"call_uuid={call_uuid} value_present={bool(answer_time)}"
        )
        return None
    answered = f"{vc.CALL_ANSWERED_EVENT}:{params.get('to', '')}"
    cache_error = VoiceRedis().set_mpc_event_with_dependency(call_uuid, qnr_run_uuid, answered, timestamp=str(ns))
    if cache_error not in (None, vc.VALUE_EXISTS_ERROR):
        return voice_hangup.retryable_voice_callback_error("answer timestamp cache persistence unavailable")
    return None
