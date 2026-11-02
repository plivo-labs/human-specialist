import functools
import json
import time
import uuid
from contextlib import suppress

from hs.conversations import transfer as cxfer
from hs.infra.counters import MetricName, increase_counter
from hs.infra.log import logger
from hs.plivo import voice as pv
from hs.routing import account_read as ar
from hs.routing import acw as routing_acw
from hs.routing import constants as rc
from hs.routing import get_agents as routing_get_agents
from hs.routing import invitation as ri
from hs.routing import selection as routing_selection
from hs.routing import sticky as routing_sticky
from hs.routing.invitation_models import AgentAndContactMapping
from hs.routing.redis_store import RoutingRedis
from hs.voice import agent_leg as voice_agent_leg
from hs.voice import callbacks as voice_callbacks
from hs.voice import constants as vc
from hs.voice import hangup as voice_hangup
from hs.voice.redis_store import VoiceRedis
from hs.voice.transfer_models import TransferInfo


class _Refused(Exception):
    """A transfer step's refusal: the error string the flow returns."""


def _ok(err) -> None:
    if err is not None:
        raise _Refused(err)


def _refusal_returned(flow):
    """The flow's refusal as its return value: the routes reply with that string."""

    @functools.wraps(flow)
    def run(*args, **kwargs):
        try:
            return flow(*args, **kwargs)
        except _Refused as refused:
            return refused.args[0]

    return run


def hold_unhold_customer(hold_value, call_uuid, qnr_run_uuid, auth_id, auth_token) -> str | None:
    participants, err = voice_agent_leg._get_participants(f"mpc_{qnr_run_uuid}", auth_id, auth_token)
    if err is not None:
        logger.error(f"hold_unhold_customer: error in getting participant list from mpc: {err}")
        return err
    customer = next((p for p in participants or [] if str(p.get("role", "")).lower() == vc.MPC_ROLE_CUSTOMER), None)
    if customer is None:
        return None
    member = str(customer.get("member_id", ""))
    herr = voice_hangup.hold_or_unhold_call(hold_value, call_uuid, qnr_run_uuid, member, auth_id, auth_token)
    if herr is not None:
        logger.error(f"hold_unhold_customer: error in updating status of customer participant: {herr}")
    return herr


def delete_transfer_mpc_calls(call_uuid, qnr_run_uuid, auth_id, auth_token, voice_redis=None) -> str | None:
    vr = voice_redis or VoiceRedis()
    try:
        transferred = vr.get_transfer_mpc_call_uuids(qnr_run_uuid)
    except Exception as e:
        logger.error(f"delete_transfer_mpc_calls: error in fetching transferred call ids from cache: {e}")
        return str(e)
    logger.info(
        f"delete_transfer_mpc_calls: transferred calls from DB {transferred} call_uuid={call_uuid} "
        f"qnrRunUuid={qnr_run_uuid}"
    )
    if not auth_id or not auth_token:
        logger.error(f"delete_transfer_mpc_calls: error initialising plivo client call uuid={call_uuid}")
        return "missing credentials"
    client = pv.PlivoVoiceClient(auth_id, auth_token)
    deleted = [client.call_delete(call_id) for call_id in transferred]
    # A 404 leg already ended.
    return pv.worst_error([err for _b, status, err in deleted if err is not None and status != 404])


def _member_address_matches(participant_addr: str, requested_forms) -> bool:
    """MPC reports member_address in whichever form the leg was dialed with — bare phone
    ("1555..."), "+"-prefixed phone, or SIP URI with/without the "sip:" scheme — while the hangup
    request carries its own form. Normalize both sides instead of requiring byte equality, or a
    SIP-addressed request never matches a bare-phone participant and the hangup is skipped."""

    def forms(v):
        v = str(v or "").strip()
        return {v, v[4:] if v.startswith("sip:") else v, v[1:] if v.startswith("+") else v}

    return any(forms(participant_addr) & forms(req) for req in requested_forms)


def hangup_call_for_mpc_member(mpc_name, member_address, auth_id, auth_token, extra_member_forms=()) -> str | None:
    participants, err = voice_agent_leg._get_participants(mpc_name, auth_id, auth_token)
    if err is not None or not participants:
        logger.error(f"hangup_call_for_mpc_member: failed to get participants mpcName={mpc_name}: {err}")
        return err
    forms = (member_address, *extra_member_forms)
    for p in participants:
        if p.get("exit_cause") or not _member_address_matches(p.get("member_address", ""), forms):
            continue
        if not auth_id or not auth_token:
            return "missing credentials"
        if (derr := pv.PlivoVoiceClient(auth_id, auth_token).call_delete(str(p.get("call_uuid", "")))[2]) is not None:
            logger.error(
                f"hangup_call_for_mpc_member: failed to trigger hangup for member "
                f"member_call_uuid={p.get('call_uuid', '')}: {derr}"
            )
            return derr
    return None


def validate_transfer_req_owner(transfer_req, channel, fetch_conv_details, rr):
    """The requesting agent must own the conversation. A missing or foreign contact-to-agent mapping is
    rebuilt from the conversation record. Returns (mapping, conversation, allowed mode, error)."""
    contact, channel = transfer_req.contact_uuid, channel or rc.CHANNEL_CALL
    parsed = AgentAndContactMapping.parse_redis(rr.get_contact_to_agent_mapping(contact))
    mapping = parsed or AgentAndContactMapping()
    allowed, _add, conv = cxfer.transfer_allowed(contact, channel, fetch_conv_details)

    def fail(err):
        return mapping, conv, rc.TRANSFER_MODE_BOTH, err

    if not allowed:
        return fail("case and contact should be created before transferring the conversation")
    if parsed is None or mapping.agent_sip_endpoint != transfer_req.agent_sip:
        run_uuid = mapping.qnr_run_uuid or str(uuid.uuid4())
        mapping = AgentAndContactMapping(
            agent_sip_endpoint=conv.get("agent_sip_endpoint", ""),
            from_number=conv.get("from", ""),
            agent_uuid=conv.get("agent_uuid", ""),
            qnr_run_uuid=run_uuid,
        )
        err = rr.set_contact_to_agent_mapping(contact, mapping.to_json(), rc.TRANSFER_KEY_TTL_SECONDS)
        if err is not None:
            return fail(err)
        if mapping.agent_sip_endpoint != transfer_req.agent_sip:
            return fail("not the owner of the channel")
    if rr.get_contact_parameters(contact, mapping.qnr_run_uuid) is None:
        return fail("contact params not found in cache")
    if (settings := ar.get_org_transfer_settings_for_agent_sip(transfer_req.agent_sip)) is None:
        return fail("error fetching org transfer settings")
    if not settings.allow_transfer_type and settings.default_transfer in (rc.TRANSFER_MODE_COLD, rc.TRANSFER_MODE_WARM):
        return mapping, conv, settings.default_transfer, None
    return mapping, conv, rc.TRANSFER_MODE_BOTH, None


def _contact_info(rr, contact_uuid, mapping, missing) -> dict:
    if (cp := rr.get_contact_parameters(contact_uuid, mapping.qnr_run_uuid)) is None:
        raise _Refused(missing)
    return cp


def _agent_mapping(rr, contact_uuid, missing) -> AgentAndContactMapping:
    if (mapping := AgentAndContactMapping.parse_redis(rr.get_contact_to_agent_mapping(contact_uuid))) is None:
        raise _Refused(missing)
    return mapping


def _resolve_destination_name(req, auth_id: str) -> str:
    if req.transfer_type == rc.TRANSFER_TYPE_AGENT:
        name, nerr = voice_hangup._get_agent_name_by_uuid(req.identifier, auth_id)
        if nerr is not None:
            logger.error(
                f"_resolve_destination_name: error resolving the agent name transfer_type={req.transfer_type}: {nerr}"
            )
        return (name or "") if nerr is None else ""
    if req.transfer_type == rc.TRANSFER_TYPE_TEAM:
        return ar.get_team_name(req.identifier) or ""
    return req.member if req.transfer_type in (rc.TRANSFER_TYPE_EXTERNAL, rc.TRANSFER_TYPE_WARM_EXTERNAL) else ""


@_refusal_returned
def handle_channel_transfer_initiate_generic(channel, req, transfer_mode, db=None, rr=None):
    rr, contact = rr or RoutingRedis(), req.contact_uuid
    agent_mapping, _conv, allowed_mode, err = validate_transfer_req_owner(req, channel, True, rr)
    if err is not None:
        logger.error(f"handle_channel_transfer_initiate: validate owner failed contact={contact}: {err}")
        return err
    if req.transfer_type != rc.TRANSFER_TYPE_EXTERNAL and allowed_mode not in (rc.TRANSFER_MODE_BOTH, transfer_mode):
        return f"{transfer_mode} transfer is not allowed"
    qnr_run = agent_mapping.qnr_run_uuid
    contact_info = _contact_info(rr, contact, agent_mapping, "error in fetching contact parameters from cache")
    target = req.identifier or req.member
    info = TransferInfo(transfer_type=req.transfer_type, transfer_mode=transfer_mode, target=target, at=time.time())
    claim = functools.partial(rr.set_transfer_in_progress, contact, info.to_json(), rc.TRANSFER_KEY_TTL_SECONDS)
    # One transfer at a time: a second request while the first rings would dial its target again.
    # Known limit: two requests that both find an abandoned key can both replace it.
    if not claim(nx=True) and not (_abandoned(rr, contact, agent_mapping, contact_info) and claim()):
        return "failed to set transfer in progress"
    rr.delete_to_be_primary_agent(qnr_run)
    if channel == rc.CHANNEL_CALL:
        sip = agent_mapping.agent_sip_endpoint
        _res, err = voice_callbacks.route_and_add_to_mpc(
            contact, qnr_run, True, req.transfer_type, req.identifier, req.member, sip, db
        )
        if err is not None and err.startswith("known_error:"):
            increase_counter(MetricName.MPC_ADD_AGENTS_FAILURE, {"output_state": "transfer_call"})
        if err is not None:
            logger.error(f"handle_channel_transfer_initiate: call transfer failed contact={contact}: {err}")
    if err is not None:
        rr.delete_transfer_in_progress(contact)
        return err
    if channel == rc.CHANNEL_CALL:
        herr = hold_unhold_customer(True, contact, qnr_run, *ar.contact_pair(contact_info))
        if herr is not None:
            logger.error(
                f"handle_channel_transfer_initiate: failed to hold unhold the customer in transfer "
                f"contact={contact}: {herr}"
            )
    msg = rc.TRANSFER_INITIATED_MSG.replace(
        "<name>", _resolve_destination_name(req, contact_info.get("auth_id", "")), 1
    )
    cxfer.signal_chunk(contact, channel, rc.SIGNAL_TRANSFER_INITIATED, req.identifier, req.transfer_type, msg)


def _abandoned(rr, contact, mapping, contact_info) -> bool:
    """Whether the call's transfer key outlived its transfer, as a crash or a lost callback leaves it: set more than
    a ring timeout ago, with no target leg ringing and no agent on the call but the transferring one."""
    ring, err = rr.get_reservation_timeout(contact_info.get("auth_id", ""), rc.CHANNEL_CALL)
    age = time.time() - (rr.get_transfer_in_progress_info(contact) or {}).get("at", 0)
    if err is not None or age <= ring or VoiceRedis().get_transfer_mpc_call_uuids(mapping.qnr_run_uuid):
        return False
    members, err = voice_agent_leg._get_participants(f"mpc_{mapping.qnr_run_uuid}", *ar.contact_pair(contact_info))
    live = [m for m in members or [] if not m.get("exit_cause")]
    agents = {m.get("member_address") for m in live if str(m.get("role", "")).lower() == vc.MPC_ROLE_AGENT}
    return err is None and agents <= {mapping.agent_sip_endpoint}


def _set_sticky_agent(agent_uuid, cp, rr) -> None:
    """The new owner becomes the contact's sticky agent: outbound when no flow is named, else per its queue config."""
    number = cp.get("from_number", "")
    if not cp.get("flow_name", ""):
        routing_sticky.set_outbound_sticky_mapping(agent_uuid, number, cp.get("auth_id", ""))
    elif raw_cfg := rr.get_queue_and_route_config(cp.get("flow_run_uuid", ""), cp.get("queue_and_route_run_uuid", "")):
        config, err = routing_sticky.parse_queue_and_route_config(raw_cfg)
        if err is not None:
            logger.error(f"_set_sticky_agent: error while getting qnrconfig agent={agent_uuid}: {err}")
        else:
            routing_sticky.set_inbound_sticky_mapping(agent_uuid, number, config, cp.get("auth_id", ""))


def handle_cancel_transfer(channel, req, transfer_mode, rr=None) -> str | None:
    rr, contact = rr or RoutingRedis(), req.contact_uuid
    agent_mapping, _conv, _allowed, err = validate_transfer_req_owner(req, channel, False, rr)
    if err is not None:
        return err
    if (contact_info := rr.get_contact_parameters(contact, agent_mapping.qnr_run_uuid)) is None:
        return "error in fetching contact parameters from cache"
    if not rr.is_transfer_in_progress(contact)[0]:
        return "conversation is not in transfer mode"
    if channel == rc.CHANNEL_CALL:
        pair = ar.contact_pair(contact_info)
        if (derr := delete_transfer_mpc_calls(contact, agent_mapping.qnr_run_uuid, *pair)) is not None:
            logger.error(f"handle_cancel_transfer: transferred calls deletion failed ContactUUID={contact}: {derr}")
            return derr
    cxfer.signal_chunk(contact, channel, rc.SIGNAL_CANCEL_TRANSFER, "", "", rc.CANCEL_TRANSFER_MSG)
    if channel != rc.CHANNEL_CALL:
        cxfer.set_transfer_state(contact, channel, rc.TRANSFER_STATE_TRANSFER_FAILED)
    return None


# leave type -> (refusal unless the requester is its owner: the secondary to leave it, else the primary;
# ignore_event; (drop signal, its message))
_NOT_PRIMARY = "requester is not the primary agent for the conversation"
_AGENT2_DROPPED = rc.SIGNAL_AGENT2_DROPPED, rc.AGENT2_DROPPED_MSG
_CLOSED = rc.SIGNAL_AGENT1_CLOSED_CONVERSATION, rc.AGENT1_CLOSED_CONVERSATION_MSG
_SECONDARY_DROPS = {
    rc.LEAVE_TYPE_DROP_SECONDARY: (_NOT_PRIMARY, False, _AGENT2_DROPPED),
    rc.LEAVE_TYPE_CLOSE_CONVERSATION: ("not the primary agent for the conversation", True, _CLOSED),
    rc.LEAVE_TYPE_LEAVE_SECONDARY: ("not secondary agent for the given conversation", False, _AGENT2_DROPPED),
}


@_refusal_returned
def handle_agent_drop(agent_drop_params, agent_uuid, rr=None):
    rr = rr or RoutingRedis()
    contact, channel, lt = agent_drop_params.contact_uuid, agent_drop_params.channel, agent_drop_params.leave_type
    if not rr.is_transfer_in_progress(contact)[0]:
        return "conversation is not in transfer mode"
    mapping = _agent_mapping(rr, contact, "unable to get agent to contact mapping")
    contact_param = rr.get_contact_parameters(contact, mapping.qnr_run_uuid)
    if (to_be_primary := rr.get_to_be_primary_agent(mapping.qnr_run_uuid)) is None:
        return "no to be primary agent found"
    if (org := ar.get_org_details_for_agent_sip(mapping.agent_sip_endpoint)) is None:
        return "org authid not found got agent sip-endpoint"
    if (agent := ar.get_agent_by_uuid(agent_uuid, org.auth_id)) is None:
        return "failed to get agent details"
    primary, promoted = mapping.agent_uuid, to_be_primary.get("AgentUUID", "")
    if lt == rc.LEAVE_TYPE_LEAVE_PRIMARY:
        if primary != agent_uuid:
            return _NOT_PRIMARY
        _ok(cxfer.transfer_child(contact, rc.TRANSFER_TYPE_AGENT, channel, promoted, rc.TRANSFER_MODE_WARM)[1])
        mapping.agent_uuid, mapping.previous_primary_agent = promoted, agent_uuid
        mapping.agent_sip_endpoint = to_be_primary.get("AgentSipEndpoint", "")
        _ok(rr.set_contact_to_agent_mapping(contact, mapping.to_json(), rc.TRANSFER_KEY_TTL_SECONDS))
        if contact_param is not None:
            _set_sticky_agent(promoted, contact_param, rr)
        msg = rc.AGENT1_LEAVING_MSG.replace("<name>", agent.name or "", 1)
        cxfer.signal_chunk(contact, channel, rc.SIGNAL_AGENT1_LEAVING, agent_uuid, rc.TRANSFER_TYPE_AGENT, msg)
    elif lt in _SECONDARY_DROPS:
        refusal, ignore_event, (signal, msg) = _SECONDARY_DROPS[lt]
        if (promoted if lt == rc.LEAVE_TYPE_LEAVE_SECONDARY else primary) != agent_uuid:
            return refusal
        conversation_number = (contact_param or {}).get("conversation_number", "")
        secondary = {"agent_sip_or_uuid": promoted, "channel": channel, "ignore_event": ignore_event}
        _ok(cxfer.remove_secondary(conversation_number=conversation_number, **secondary))
        ri._handle_reverse_routing_inprocess(to_be_primary.get("AgentSipEndpoint", ""), channel, conversation_number)
        if (dropped := ar.get_agent_by_uuid(promoted, org.auth_id)) is not None:
            msg = msg.replace("<name>", dropped.name or "", 1)
            cxfer.signal_chunk(contact, channel, signal, promoted, rc.TRANSFER_TYPE_AGENT, msg)
    else:
        return "invalid leave type"
    rr.delete_to_be_primary_agent(mapping.qnr_run_uuid)
    rr.delete_transfer_in_progress(contact)


def _owned_call(req, rr, prefix):
    mapping, _conv, _allowed, err = validate_transfer_req_owner(req, rc.CHANNEL_CALL, False, rr)
    if err is not None:
        logger.error(f"{prefix}Failed to validate owner of the call contactUUID={req.contact_uuid}: {err}")
        raise _Refused(err)
    return mapping


def _call_participants(req, mapping, rr, label, mpc_error):
    if (contact_info := rr.get_contact_parameters(req.contact_uuid, mapping.qnr_run_uuid)) is None:
        logger.error(f"{label}: unable to get contact parameters contactUUID={req.contact_uuid}")
        raise _Refused(rc.CACHE_MISS)
    pair = ar.contact_pair(contact_info)
    participants, err = voice_agent_leg._get_participants(f"mpc_{mapping.qnr_run_uuid}", *pair)
    if err is not None:
        logger.error(f"{mpc_error}: {err}")
        raise _Refused(err)
    return contact_info, pair, participants or []


def _hold(hold, req, agent_mapping, participant, pair, error) -> None:
    member = str(participant.get("member_id", ""))
    err = voice_hangup.hold_or_unhold_call(hold, req.contact_uuid, agent_mapping.qnr_run_uuid, member, *pair)
    if err is not None:
        logger.error(f"{error}: {err}")
        raise _Refused(err)


def _aom_row(member_address, label, where):
    if (row := ar.get_aom_row_for_sip(member_address)) is None:
        logger.error(f"{label}: error in getting AOM for agent sip {where}")
        raise _Refused(rc.NOT_FOUND)
    return row


@_refusal_returned
def handle_call_switch(req, rr=None, voice_redis=None):
    rr, vr, label = rr or RoutingRedis(), voice_redis or VoiceRedis(), "handle_call_switch"
    contact = req.contact_uuid
    agent_mapping = _owned_call(req, rr, "")
    to_agent = vr.get_agent_switch_role(agent_mapping.qnr_run_uuid, req.agent_sip)[0] not in ("", vc.MPC_ROLE_AGENT)
    switch_role = vc.MPC_ROLE_AGENT if to_agent else vc.MPC_ROLE_CUSTOMER
    if (err := vr.set_agent_switch_role(agent_mapping.qnr_run_uuid, req.agent_sip, switch_role)) is not None:
        logger.error(f"handle_call_switch: unable to set the role for switch call contactUUID={contact}: {err}")
        return err
    mpc_error = f"error in getting participant list from mpc contactUUID={contact}"
    _info, pair, participants = _call_participants(req, agent_mapping, rr, label, mpc_error)
    for p in participants:
        member_address = str(p.get("member_address", ""))
        if str(p.get("role", "")).lower() == vc.MPC_ROLE_CUSTOMER:
            _hold(to_agent, req, agent_mapping, p, pair, f"{label}: error in updating status of customer participant")
        elif req.agent_sip != member_address and not p.get("exit_cause"):
            error = f"{label}: error in updating status of agent participant member={p.get('member_id', '')}"
            _hold(not to_agent, req, agent_mapping, p, pair, error)


@_refusal_returned
def handle_call_transfer_merge(req, rr=None):
    rr, label, contact = rr or RoutingRedis(), "handle_call_transfer_merge", req.contact_uuid
    agent_mapping = _owned_call(req, rr, f"{label}: ")
    mpc_error = f"{label}: error in getting participant list from mpc"
    contact_info, pair, participants = _call_participants(req, agent_mapping, rr, label, mpc_error)
    external, ttl = req.transfer_type == rc.TRANSFER_TYPE_WARM_EXTERNAL, vc.EXPIRY_DURATION_IN_HOURS_ASYNC * 3600
    live = [p for p in participants if not p.get("exit_cause")]
    # Only an agent-role leg is the dialed number: the caller's own leg has the same address when they are the target.
    agents = [p for p in live if str(p.get("role", "")).lower() == vc.MPC_ROLE_AGENT]
    number = next((p for p in agents if _member_address_matches(p.get("member_address", ""), (req.member,))), None)
    if external and number is None:
        return "the number has not joined"
    for p in live:
        member_address = str(p.get("member_address", ""))
        if req.agent_sip == member_address:
            continue
        _hold(False, req, agent_mapping, p, pair, f"{label}: error in updating status of participant")
        if str(p.get("role", "")).lower() != vc.MPC_ROLE_AGENT or (external and p is not number):
            continue
        to_be_primary = AgentAndContactMapping(
            AgentCallUuid=str(p.get("call_uuid", "")),
            AgentSipEndpoint=member_address,
            FromNumber=contact_info.get("from_number", ""),
            QnrRunUuid=contact_info.get("queue_and_route_run_uuid", ""),
        )
        row = None if external else _aom_row(member_address, label, f"call_uuid={contact}")
        if row is not None:
            to_be_primary.agent_uuid = row.uuid
        if not rr.set_to_be_primary_agent(agent_mapping.qnr_run_uuid, to_be_primary.to_json(), ttl):
            kind = "for warm external transfer " if external else ""
            logger.error(f"failed to update to be primary agent {kind}call_uuid={contact}")
            return "failed to update to be primary agent"


@_refusal_returned
def handle_call_transfer_warm_hangup(req, rr=None):
    rr, label = rr or RoutingRedis(), "handle_call_transfer_warm_hangup"
    agent_mapping = _owned_call(req, rr, f"{label}: ")
    mpc_error = f"{label}: error in getting participant list from mpc contactUUID={req.contact_uuid}"
    contact_info, pair, participants = _call_participants(req, agent_mapping, rr, label, mpc_error)
    member, _ext = voice_agent_leg.extract_phone_number_and_extension_if_any(req.member, True)
    qnr_run = contact_info.get("queue_and_route_run_uuid", "")
    # Matched against both the request's member and its phone-extracted form (see _member_address_matches).
    for p in participants:
        if not _member_address_matches(p.get("member_address", ""), (member, req.member)):
            continue
        if p.get("role", "") == vc.MPC_ROLE_CUSTOMER:
            if (err := delete_transfer_mpc_calls(req.contact_uuid, qnr_run, *pair)) is not None:
                logger.error(f"{label}: Failed to delete transfer MPC calls call_uuid={req.contact_uuid}: {err}")
        err = hangup_call_for_mpc_member(f"mpc_{qnr_run}", member, *pair, extra_member_forms=(req.member,))
        if err is not None:
            logger.error(f"{label}: Failed to hangup member call call_uuid={req.contact_uuid}: {err}")
        return err


def handle_outbound_call_transfer_success(call_uuid, params, rr=None) -> str | None:
    rr = rr or RoutingRedis()
    if (mapping := AgentAndContactMapping.parse_redis(rr.get_contact_to_agent_mapping(call_uuid))) is None:
        logger.error(f"could not get contact to agent mapping callUUID={call_uuid}")
        return rc.CACHE_MISS
    agent_sip = params.get("agent_sip_endpoint", "") or ""
    if (aom_details := ar.get_account_rows_for_sip_endpoints([agent_sip])) is None:
        return rc.NOT_FOUND
    if not aom_details:
        raise IndexError("no agent for the SIP endpoint")
    mapping.agent_sip_endpoint, mapping.agent_uuid, mapping.agent_call_uuid = agent_sip, aom_details[0].uuid, call_uuid
    mapping.flow_run_uuid = params.get("flow_run_id", "") or ""
    merr = rr.set_contact_to_agent_mapping(call_uuid, mapping.to_json(), vc.EXPIRY_DURATION_IN_HOURS_CALL * 3600)
    if merr is not None:
        logger.error(f"Error while storing the contact and agent mapping callUUID={call_uuid}: {merr}")
    return merr


def _release_reverse_routing_locks(agents, conversation_number, rr) -> None:
    for agent in agents or []:
        with suppress(Exception):
            rr.delete_reverse_routing_lock(agent, conversation_number, rc.CHANNEL_CALL)


def get_agents_for_team(params, rr=None) -> tuple[list | None, str | None]:
    rr = rr or RoutingRedis()
    agents, err = routing_get_agents.get_agents(
        params.call_uuid,
        rc.CHANNEL_CALL,
        params.auth_id,
        params.conversation_number,
        transfer_to=params.transfer_destination_id,
        transfer_type=rc.TRANSFER_TYPE_TEAM,
        routing_redis=rr,
    )
    if err is not None:
        logger.error(f"get_agents_for_team: Could not fetch agents calluuid={params.call_uuid}: {err}")
        return None, err
    _release_reverse_routing_locks(agents, params.conversation_number, rr)
    return agents, None


def get_agent_by_capacity(params, rr=None) -> tuple[list | None, str | None]:
    rr = rr or RoutingRedis()
    if (agent_details := ar.get_agent_by_uuid(params.aom_uuid, params.auth_id)) is None:
        logger.error(f"get_agent_by_capacity: Could not fetch agent details aomUuid={params.aom_uuid}")
        return None, rc.NOT_FOUND
    agents = [agent_details.sip_endpoint]
    try:
        acw = routing_acw.get_agents_on_active_call_and_in_acw(params.auth_id, agents)
    except Exception as e:
        logger.error(f"Error while fetching Agents in ACW and active call: {e}")
        return None, str(e)
    try:
        agents = routing_selection.get_available_agents_by_capacity(
            agents, rc.CHANNEL_CALL, 1, 1, params.auth_id, acw, params.conversation_number, routing_redis=rr
        )
    except Exception as e:
        logger.error(f"get_agent_by_capacity: Could not fetch agents calluuid={params.call_uuid}: {e}")
        return None, str(e)
    _release_reverse_routing_locks(agents, params.conversation_number, rr)
    return agents, None


def move_caller(call_uuid, mapping, tenant_id, action: dict) -> str | None:
    """Take the caller out of the room to run a queue or voicemail action; the room ends with its caller, which
    ends the agent's leg."""
    from hs.intake import flow
    from hs.setup.actions import plivo_url

    if (cp := RoutingRedis().get_contact_parameters(call_uuid, mapping.qnr_run_uuid)) is None:
        return "contact params not found"
    flow.save(call_uuid, move=json.dumps(action), transfer="1" if action["type"] == "queue" else "")
    url = plivo_url(tenant_id, "actions/move", call_uuid=call_uuid)
    return pv.PlivoVoiceClient(*ar.contact_pair(cp)).call_update(call_uuid, {"legs": "aleg", "aleg_url": url})[2]
