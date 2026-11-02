from hs.admin.account import AccountOrgMapping, Organization
from hs.admin.lookups import (
    BeforeTransferStateCache,
    fetch_aom_by_uuid_or_sip_and_set_context,
    fetch_org_by_id_and_set_context,
)
from hs.conversations import models as mc
from hs.conversations.store import Conversation, ConversationChunk, ConversationParent, ConvSecondaryAgentMapping, emit
from hs.infra import constants as ne
from hs.infra import context as ctx
from hs.infra.error_messages import GENERIC_ERROR_RESPONSE
from hs.infra.helpers import get_now
from hs.infra.log import logger
from hs.infra.models import UseCaseResponse, fail

_CHILD_NOT_FOUND = "Child conversation not found for contact uuid"
_AOM_NOT_FOUND = "Agent not found for given sip_endpoint or aom_uuid"
_NOT_IN_ORG = "Agent and conversation are not in the same organization"
_NO_ACTIVE_CHILD = "No active conversation found for parent conversation"
_PARENT_NOT_FOUND = "Parent conversation not found"
_NO_SECONDARY = "No secondary to remove"
_NO_AGENT_SIP_OR_UUID = "Agent sip-endpoint or uuid cannot be empty for outbound conversation"
_EMPTY_SECONDARY_STATE = "Cannot update with secondary mapping with empty secondary agent state"
_SIGNAL = mc.ConversationChunkTypes.signal.name
# supervisor state -> (agent message template, supervisor message, signal chunk description)
_SUPERVISOR_SIGNALS = {
    ne.MONITOR_VISIBLE: (ne.AGENT_MESSAGE_MONITORING, ne.SUPERVISOR_MESSAGE_MONITORING, "SUPERVISOR_MONITORING"),
    ne.MONITOR_PAUSED: (ne.AGENT_MESSAGE_MONITORING_PAUSED, ne.SUPERVISOR_MESSAGE_MONITORING_PAUSED, "SUPERVISOR_LEFT"),
    ne.COACH: (ne.AGENT_MESSAGE_COACHING, ne.SUPERVISOR_MESSAGE_COACHING, "SUPERVISOR_COACHING"),
    ne.BARGE: (ne.AGENT_MESSAGE_BARGING, ne.SUPERVISOR_MESSAGE_BARGING, "SUPERVISOR_BARGING"),
}
SUPERVISOR_MODES = {ne.MONITOR: "listen", ne.MONITOR_VISIBLE: "listen", ne.COACH: "whisper", ne.BARGE: "barge"}
_S = mc.SecondaryAgentStateEnum
_PARTICIPANT_EVENTS = {
    _S.add_participant_success: "call.participant_added",
    _S.participant_removed: "call.participant_removed",
    _S.participant_left: "call.participant_removed",
}


def _wrap_up_transferred_child(conv: mc.ConvTransferModel, child, aom, now) -> None:
    """The transferred child goes to wrap-up: transferred then in-wrap-up custom states, the transfer chunk, and the
    in-wrap-up status change for its agent."""
    from hs.conversations import child_update as ccu
    from hs.conversations import management as cm
    from hs.conversations.management import PostConversationStatusChange

    T, S, C = mc.ConversationTransferTypes, mc.ConversationCustomStatusMappingStates, Conversation
    org_id = child.parent_conversation.organization
    custom_state = ccu.get_conversation_custom_state_by_name
    transferred = custom_state(org_id=org_id, state_name=S.transferred.value)
    update_dict = {
        C.state.name: mc.ConversationStates.transferred.name,
        C.status.name: mc.ConversationStatus.in_wrap_up.name,
        C.wrap_up_start_at.name: now,
    }
    if transferred:
        update_dict[C.custom_status_mapping.name] = transferred.id
    C.update_conversation(child.id, update_dict, now=now, update_status_change_timestamp_for_state=S.transferred.value)
    wrap_up = custom_state(org_id=org_id, state_name=S.in_wrap_up.value)
    wrapped = {C.custom_status_mapping.name: wrap_up.id}
    C.update_conversation(child.id, wrapped, now=now, update_status_change_timestamp_for_state=wrap_up.state)
    to_user = str(aom.uuid) if aom else None
    if conv.transfer_type == T.external.name and conv.agent_sip_or_uuid:
        to_user = conv.agent_sip_or_uuid
    chunk = mc.ConvChunkInsertModel(
        type=mc.ConversationChunkTypes.transfer.name,
        from_user=str(child.primary_agent.uuid),
        to_user=to_user or ne.EMPTY_TO_USER,
        channel_meta={"transfer_type": conv.transfer_type},
    )
    cm.create_chunk_with_related_data(chunk.build_db_model(conversation=child))
    PostConversationStatusChange(
        aom_uuid=child.primary_agent.uuid,
        aom=child.primary_agent,
        conversation=child,
        changed_status=mc.ConversationStatus.in_wrap_up.name,
    ).invoke()


def _clear_the_way_for(aom, conv: mc.ConvTransferModel, child) -> UseCaseResponse:
    """Before the target agent gets the conversation: close the one they already hold for this contact (a transfer
    back) and drop their secondary-participant mapping (a warm transfer's)."""
    from hs.conversations import child_update as ccu
    from hs.conversations import management as cm

    parent = child.parent_conversation
    number = parent.conversation_number
    if already := Conversation.get_primary_active_conversation(number, aom.uuid, contact_uuid=child.contact_uuid):
        notes = mc.ConvChunkInsertModel(
            type=mc.ConversationChunkTypes.notes.name,
            from_user=str(ctx.context_aom_uuid.get()),
            to_user=already.parent_conversation.contact_identifier,
            disposition=ne.DISPOSITION_FOR_TRANSFER_BACK_TO_SAME_AGENT,
        )
        cm.create_chunk_with_related_data(notes.build_db_model(conversation=already))
        ccu.close_conversation(already)
    mapping = mc.ConvSecAgentMappingRemove(
        agent_sip_or_uuid=conv.agent_sip_or_uuid,
        conversation_number=number,
        secondary_agent=aom,
        parent_conversation=parent,
    )
    return remove_secondary_participant(mapping, bypass_all_events=True)


def _evict_barging_supervisor(parent, agent) -> None:
    if not (barge := ConvSecondaryAgentMapping.get_monitor_barge_secondary_conversation_by_conv_id(parent.id)):
        return
    ConvSecondaryAgentMapping.delete_mapping(parent.id, aom_id=barge.aom, states=ne.MONITOR_BARGE_COACH_STATE_LIST)
    if barge.aom and barge.agent.uuid:
        emit("supervisor.left", parent, {"supervisor_id": str(barge.agent.uuid), "mode": "barge"}, agent.uuid)


def _join_queue_transfer(conv: mc.ConvInsertModel, aom) -> None:
    """A queue-transfer child takes the first human child's sender (and starts now), and claims the parent's
    unrouted transfer chunk for its agent."""
    if first_non_bot := Conversation.get_first_non_bot_by_contact_uuid(conv.parent_contact_uuid):
        conv.start_at, conv.sender_id = get_now(), first_non_bot.sender_id
    unrouted = ConversationChunk.get_conversation_chunks_by_conv_number(
        conv.parent.conversation_number,
        filter_empty_to_user=True,
        filter_chunk_types=[mc.ConversationChunkTypes.transfer.name],
    )
    if unrouted and aom:
        ConversationChunk.update_conversation_chunk_by_id(unrouted[0].id, {"to_user": str(aom.uuid)})


def _signal_primary(mapping, label: str, description: str, attributes) -> None:
    """A signal chunk on the conversation's non-QnR child."""
    from hs.conversations import management as cm

    parent = mapping.parent_conversation
    non_qnr_child = Conversation.get_active_non_qnr_child_by_contact_uuid(parent.parent_contact_uuid)
    logger.info(f"{label}: non qnr child id={getattr(non_qnr_child, 'id', None)}")
    chunk = mc.NonQNRChunkInsert(type=_SIGNAL, parent_contact_uuid=parent.parent_contact_uuid)
    chunk.description, chunk.attributes = description, attributes
    cm.create_chunk_with_related_data(chunk.build_db_model(conversation=non_qnr_child))


ERROR_NO_AGENT_CAPACITY = (
    "You have reached your open conversation limit. Wrap-up the open conversations for this channel to continue."
)


def check_if_transfer_allowed(
    contact_uuid: str, channel=None, include_conv_details=False, check_add_participant_allowed=False
):

    conversation = Conversation.get_single_active_conversation_for_parent(contact_uuid, channel=channel)
    if not conversation:
        return fail("Conversation not found", 404)
    parent = conversation.parent_conversation
    org = Organization.get_by_id(parent.organization)
    ctx.context_org_uuid.set(org.uuid)
    if org.allow_acw_post_transfer:
        return UseCaseResponse(data={"transfer_allowed": True})
    data = {"transfer_allowed": True}
    if channel and include_conv_details:
        first_child = Conversation.get_first_child_conversation_for_parent(parent.conversation_number)
        outbound_call = channel == mc.ConversationChannels.call.name and bool(
            Conversation.get_child_by_conv_parent(parent.id, channel, False, False, True)
        )
        data["conversation_details"] = {
            "first_channel": first_child.channel,
            "parent_channel": parent.parent_channel,
            "is_inbound": parent.source_direction_inbound,
            "from": parent.contact_identifier,
            "to": conversation.sender_id,
            "agent_sip_endpoint": conversation.primary_agent.sip_endpoint,
            "agent_uuid": str(conversation.primary_agent.uuid),
            "is_outbound_call": outbound_call,
            "conversation_number": parent.conversation_number,
        }
    if check_add_participant_allowed:
        data["add_participant_allowed"] = check_if_add_participant_allowed(conversation)
    return UseCaseResponse(data=data)


def check_if_add_participant_allowed(conversation: mc.ConversationModel):
    G = mc.ConversationGenerationTypes
    return conversation.generation_type not in (G.transfer_generated.name, G.transfer_qnr_generated.name)


def update_transfer_conversation(update_conv: mc.UpdateConvTransferModel) -> UseCaseResponse:
    logger.info(f"in update_transfer_conversation, req : {update_conv}")
    child = Conversation.get_active_transferable_by_contact_uuid(
        update_conv.parent_contact_uuid or update_conv.contact_uuid, update_conv.channel
    )
    if not child:
        return fail(_CHILD_NOT_FOUND, log=f"in update_transfer_conversation, error : {_CHILD_NOT_FOUND}")
    transferring = mc.ConversationStates.transferring.name
    if update_conv.state == transferring:
        if child.state == transferring:
            return UseCaseResponse(msg="success")
        if not BeforeTransferStateCache.set(update_conv.contact_uuid, update_conv.channel, child.state):
            logger.error("in update_transfer_conversation, error : failed to update the before transfer state in cache")
            return UseCaseResponse(error="Unable to update conversation state")
        Conversation.update_conversation(child.id, {Conversation.state.name: transferring}, now=get_now())
    if update_conv.state == mc.ConversationStates.transfer_failed.name:
        update_failed_transfer_state(child, update_conv.contact_uuid, update_conv.channel)
    return UseCaseResponse(msg="success")


def update_failed_transfer_state(transfer_conv, contact_uuid, channel) -> None:
    if not (before := BeforeTransferStateCache.get(contact_uuid, channel)):
        logger.error("in update_transfer_conversation, error : previous state not found in cache")
        return
    if transfer_conv.state == mc.ConversationStates.transferring.name:
        logger.info(f"in update_transfer_conversation, updating BFS - {before} for transferring child: {transfer_conv}")
        Conversation.update_conversation(transfer_conv.id, {Conversation.state.name: before}, now=get_now())
    BeforeTransferStateCache.delete(contact_uuid, channel)


def transfer_child_conversation(conv: mc.ConvTransferModel, mode: str = "cold") -> UseCaseResponse:
    T = mc.ConversationTransferTypes
    logger.info(f"in transfer_child_conversation contact_uuid={conv.contact_uuid}")
    now = get_now()
    child = Conversation.get_active_transferable_by_contact_uuid(
        conv.contact_uuid, conv.channel, conv.parent_contact_uuid
    )
    if not child:
        return fail(_CHILD_NOT_FOUND)
    response_child, parent, agent = child.build_res_model(), child.parent_conversation, child.primary_agent
    aom = fetch_aom_by_uuid_or_sip_and_set_context(conv.agent_sip_or_uuid) if conv.agent_sip_or_uuid else None
    if not aom:
        fetch_org_by_id_and_set_context(parent.organization)
    if aom and parent.organization != aom.organization:
        return fail(_NOT_IN_ORG)
    to_agent = conv.transfer_type in (T.agent.name, T.team.name)
    if to_agent and not aom:
        return fail(_AOM_NOT_FOUND)
    if to_agent and agent.uuid == aom.uuid:
        return fail("Cannot transfer conversation to same primary agent")
    _wrap_up_transferred_child(conv, child, aom, now)
    if to_agent and (removed := _clear_the_way_for(aom, conv, child)).error:
        log = f"in transfer_child_conversation mapping removal error : {removed.error}"
        return fail(removed.error, 422, log=log, rollback=True)
    handle_non_transferred_conversations(child)
    if conv.transfer_type != T.flow.name:
        generated = mc.ConversationGenerationTypes.transfer_generated.name
        initiated = mc.ConversationStates.external_transfer_initiated.name
        new_child = mc.ConvInsertModel(
            generation_type=generated,
            state=mc.DEFAULT_GEN_TYPE_STATE_MAPPING.get(generated) if aom else initiated,
            parent_contact_uuid=parent.parent_contact_uuid,
            parent=parent,
            active_queue_entry_id=None if conv.transfer_type == T.external.name else child.active_queue_entry_id,
            sender_id=child.sender_id,
            start_at=get_now(),
            direction_inbound=conv.direction_inbound,
            contact_uuid=child.contact_uuid,
            channel=child.channel,
        )
        created = create_child_conversation(new_child, aom=aom, outbound_queue_transfer=True)
        if created.error:
            return fail(created.error, 422, rollback=True)
        response_child = created.data
    if conv.transfer_type == T.flow.name:
        _evict_barging_supervisor(parent, agent)
    data = {
        "transfer_id": str(child.uuid),
        "mode": mode,
        "from_agent_id": str(agent.uuid),
        "target": {"type": conv.transfer_type, "id": str(aom.uuid) if aom else conv.agent_sip_or_uuid},
        "status": "completed",
    }
    emit("call.transferred", parent, data, agent.uuid)
    response_data = response_child or "success"
    logger.info(f"in transfer_child_conversation response : {response_data}")
    BeforeTransferStateCache.delete(str(parent.parent_contact_uuid), conv.channel)
    ConversationParent.update_last_activity_at(conversation_id=parent.id, now=now)
    return UseCaseResponse(status_code=201, msg="Child conversation successfully transferred", data=response_data)


def create_child_conversation(
    conv: mc.ConvInsertModel, aom=None, response_model=True, outbound_queue_transfer=False
) -> UseCaseResponse:
    from hs.conversations.management import PostConversationStatusChange

    G, S = mc.ConversationGenerationTypes, mc.ConversationCustomStatusMappingStates
    call = mc.ConversationChannels.call.name
    kind, outbound = conv.generation_type, G.outbound_generated.name
    logger.info(f"in create_child_conversation generation_type={conv.generation_type}")
    if not aom:
        if kind == outbound and not conv.agent_sip_or_uuid:
            return fail(_NO_AGENT_SIP_OR_UUID, log=f"in create_conversation error : {_NO_AGENT_SIP_OR_UUID}")
        if conv.agent_sip_or_uuid:
            aom = fetch_aom_by_uuid_or_sip_and_set_context(conv.agent_sip_or_uuid)
        if kind == outbound and not aom:
            return fail("Agent is required for outbound conversation")
    if not conv.parent:
        if conv.parent_contact_uuid:
            parent = ConversationParent.get_by_contact_uuid(conv.parent_contact_uuid)
            if not parent and kind == G.transfer_qnr_generated.name:
                # a queue transfer of an outbound conversation: adopt its non-QnR child's parent
                if non_qnr := Conversation.get_active_non_qnr_child_by_contact_uuid(conv.parent_contact_uuid):
                    parent = non_qnr.parent_conversation
                    conv.parent_contact_uuid = parent.parent_contact_uuid
                    conv.contact_uuid = non_qnr.contact_uuid
                    outbound_queue_transfer = True
        elif conv.parent_conversation_number:
            parent = ConversationParent.get_by_conversation_number(conv.parent_conversation_number)
        else:
            return fail(_PARENT_NOT_FOUND, log="Either parent conversation number or contact uuid is required")
        if not parent:
            return fail(_PARENT_NOT_FOUND)
        conv.parent = parent
    if aom and conv.parent.organization != aom.organization:
        return fail(_NOT_IN_ORG)
    if (
        not outbound_queue_transfer
        and kind not in (G.qnr_generated.name, G.transfer_qnr_generated.name)
        and (conv.contact_uuid or conv.channel)
    ):
        return fail(
            "Channel and contact UUID are not required for non channel switch generated conversation", rollback=True
        )
    if kind in mc.QNR_CHILDREN and not conv.active_queue_entry_id:
        return fail("Queue UUID required for QNR generated conversation", rollback=True)
    if kind == G.transfer_qnr_generated.name:
        _join_queue_transfer(conv, aom)
    custom = S.in_progress_human.value if aom else S.new.value
    conversation_to_create = conv.build_db_model(
        primary_aom=aom, active_queue_id=conv.active_queue_entry_id, conv_priority="medium", custom_conv_state=custom
    )
    if kind == outbound and conv.sender_id and conv.customer_contact:
        curr_conv = Conversation.get_active_conversation(
            channel=conv.channel, sender_id=conv.sender_id, customer_contact=conv.customer_contact
        )
        if curr_conv and curr_conv.channel != call:
            return fail(
                "Child conversation with the sender_id, customer_number and channel already open",
                data=GENERIC_ERROR_RESPONSE.get("ERROR_CHILD_CONVERSATION_EXISTS"),
            )

    # A given start_at becomes now: a child must never start before its row's created_at.
    if conversation_to_create.start_at:
        conversation_to_create.start_at = get_now()
    created: mc.ConversationModel = Conversation.create_conversation(conversation_to_create)
    created.parent_conversation = conv.parent
    if created.generation_type not in mc.NON_QNR_CHILDREN and created.primary_aom:
        org_uuid, org_name = ctx.context_org_uuid.get(), ctx.get_context_org_name()
        if not org_uuid:
            org = Organization.get_by_id(created.parent_conversation.organization)
            org_uuid, org_name = org.uuid, org.name
        PostConversationStatusChange(
            aom_uuid=created.primary_agent.uuid,
            org_name=org_name,
            org_uuid=org_uuid,
            conversation=created,
            changed_status=mc.ConversationStatus.open.name,
        ).invoke()
    logger.info(f"in create_child_conversation response : {created}")
    data = created.build_res_model() if response_model else created
    return UseCaseResponse(status_code=201, msg="Child conversation added successfully", data=data)


def increase_agent_capacity(aom, channel: str, conversation_number: str) -> UseCaseResponse:
    from hs.routing import capacity as routing_capacity

    try:
        msg, err = routing_capacity.increase_agent_capacity(aom.sip_endpoint, channel, conversation_number)
        status_code = 200 if err is None else 400
    except Exception:
        logger.error("in-process increase_agent_capacity raised", exc_info=True)
        msg, status_code = "", 500
    if status_code != 200:
        return UseCaseResponse(
            status_code=status_code,
            error=ERROR_NO_AGENT_CAPACITY,
            data=GENERIC_ERROR_RESPONSE.get("AGENT_NO_CAPACITY_ERROR_RESPONSE"),
        )
    return UseCaseResponse(status_code=200, data=msg)


def enrich_secondary_participant(mapping: mc.ConvSecAgentMappingInsert) -> UseCaseResponse:
    child = Conversation.get_primary_active_conversation(
        conversation_number=mapping.conversation_number, channel=mapping.channel or None
    )
    if not child:
        return fail(_NO_ACTIVE_CHILD, 404)
    mapping.active_child_conversation = child
    if not mapping.parent_conversation:
        mapping.parent_conversation = child.parent_conversation
    if not mapping.secondary_agent and mapping.agent_sip_or_uuid:
        if not (secondary_agent := fetch_aom_by_uuid_or_sip_and_set_context(mapping.agent_sip_or_uuid)):
            return fail(_AOM_NOT_FOUND, 404, log=f"in enrich_secondary_participant error :{_AOM_NOT_FOUND}")
        if secondary_agent.organization != child.parent_conversation.organization:
            return fail("Secondary agent not in organization of conversation", 422)
        if secondary_agent.uuid == child.primary_agent.uuid:
            return fail("Cannot add same agent as a secondary", 422)
        mapping.secondary_agent = secondary_agent
    return UseCaseResponse(status_code=200, data=mapping)


def handle_events_and_chunks_add_secondary_participant(mapping, previous_mapping=None):
    S, state = mc.SecondaryAgentStateEnum, mapping.secondary_agent_state
    if not (message := S.get_conversation_message(state.name)):
        return
    merged = previous_mapping and previous_mapping.secondary_agent_state == S.add_participant_merge_success
    if merged and state in (S.participant_left, S.participant_removed):
        message = S.get_conversation_message(f"{state.name}_conference")
    if "participant_name" in message:
        message = message.format(participant_name=mapping.participant_alias or mapping.participant_identifier)
    attributes = [mc.SignalAttributes(signal_message=message, is_actionable=False)]
    _signal_primary(mapping, "handle_add_secondary_participant", state.name.upper(), attributes)
    if event := _PARTICIPANT_EVENTS.get(state):
        participant = {"identifier": mapping.participant_identifier, "alias": mapping.participant_alias}
        data = {"participant_id": mapping.participant_identifier, "participant": participant, "role": "participant"}
        emit(event, mapping.parent_conversation, data, mapping.active_child_conversation.primary_agent.uuid)


def create_secondary_participant(mapping: mc.ConvSecAgentMappingInsert) -> UseCaseResponse:
    now = get_now()
    if (enriched := enrich_secondary_participant(mapping)).error:
        return enriched
    mapping_created = ConvSecondaryAgentMapping.create_mapping(mapping.build_db_model())
    logger.info(f"in create_secondary_participant response : {mapping_created}")
    parent, child, agent = mapping.parent_conversation, mapping.active_child_conversation, mapping.secondary_agent
    added = UseCaseResponse(status_code=201, msg="Secondary participant added successfully for conversation")
    if mapping.type == mc.SecAgentTypes.add_participant:
        handle_events_and_chunks_add_secondary_participant(mapping)
        return added
    if mapping.type == mc.SecAgentTypes.external_transfer:
        return added
    if mode := SUPERVISOR_MODES.get(mapping.secondary_agent_state):
        data = {"supervisor_id": str(agent.uuid), "mode": mode}
        emit("supervisor.joined", parent, data, child.primary_agent.uuid)
    ConversationParent.update_last_activity_at(conversation_id=parent.id, now=now)
    return added


def update_secondary_participant(mapping: mc.ConvSecAgentMappingInsert):
    now = get_now()
    if (enriched := enrich_secondary_participant(mapping)).error:
        return enriched
    if not mapping.secondary_agent_state:
        return fail(_EMPTY_SECONDARY_STATE, log=f"in update_secondary_participant error :{_EMPTY_SECONDARY_STATE}")
    parent, agent = mapping.parent_conversation, mapping.secondary_agent
    who = {
        "aom_id": agent.id if agent else None,
        "participant_identifier": None if agent else mapping.participant_identifier,
        # a supervisor's mode is set on their own row only, as in a removal
        "states": ne.MONITOR_BARGE_COACH_STATE_LIST if mapping.type == mc.SecAgentTypes.supervisor_participant else (),
    }
    existing = ConvSecondaryAgentMapping.get_secondary_conv_by_identifiers(
        conversation_id=parent.id, **who, active=True
    )
    ConvSecondaryAgentMapping.update_secondary_agent_state(parent.id, mapping.secondary_agent_state, **who, now=now)
    if mapping.type == mc.SecAgentTypes.add_participant:
        handle_events_and_chunks_add_secondary_participant(mapping)
    else:
        previous_state = existing.secondary_agent_state if existing else None
        create_chunks_on_secondary_mapping_update(mapping, previous_state)
    return UseCaseResponse(status_code=200, msg="Secondary participant updated successfully for conversation")


def create_chunks_on_secondary_mapping_update(mapping, existing_secondary_agent_state):
    state = mapping.secondary_agent_state
    if state not in _SUPERVISOR_SIGNALS:
        return
    agent_template, supervisor_message, description = _SUPERVISOR_SIGNALS[state]
    if state == ne.COACH and mapping.agent_requested_coach:
        agent_template = ne.AGENT_MESSAGE_COACHING_ACCEPTED
    primary_agent, name_of = (
        mapping.active_child_conversation.primary_agent,
        AccountOrgMapping.get_agent_name_by_aom_uuid,
    )
    supervisor_name = name_of(str(mapping.secondary_agent.uuid)) or "Unknown Agent"
    attributes = [
        mc.MonitorSignalAttributes(
            agent_message=agent_template.format(agent_name=supervisor_name, supervisor_name=supervisor_name),
            supervisor_message=supervisor_message,
            supervisor_name=supervisor_name,
            agent_name=name_of(str(primary_agent.uuid)) or "Unknown Agent",
        )
    ]
    # Pausing a plain monitor leaves no chunk.
    if existing_secondary_agent_state != ne.MONITOR or state != ne.MONITOR_PAUSED:
        _signal_primary(mapping, "create_chunks_on_secondary_mapping_update", description, attributes)
    if mode := SUPERVISOR_MODES.get(state):
        data = {"supervisor_id": str(mapping.secondary_agent.uuid), "mode": mode}
        emit("supervisor.mode_changed", mapping.parent_conversation, data, primary_agent.uuid)


def remove_secondary_participant(mapping, bypass_all_events=False, active=True) -> UseCaseResponse:
    now = get_now()
    if not mapping.parent_conversation:
        if not (child := Conversation.get_primary_active_conversation(mapping.conversation_number, active=active)):
            return fail(_NO_ACTIVE_CHILD)
        mapping.parent_conversation, mapping.active_child_conversation = child.parent_conversation, child
    if not mapping.secondary_agent and mapping.agent_sip_or_uuid:
        if not (secondary_agent := fetch_aom_by_uuid_or_sip_and_set_context(mapping.agent_sip_or_uuid)):
            return fail(_AOM_NOT_FOUND)
        mapping.secondary_agent = secondary_agent
    parent, agent = mapping.parent_conversation, mapping.secondary_agent
    aom_id = agent.id if agent else None
    # A supervisor's removal leaves the same agent's other rows on the call alone, such as a transfer they took.
    states = ne.MONITOR_BARGE_COACH_STATE_LIST if mapping.type == mc.SecAgentTypes.supervisor_participant else ()
    secondary_mapping = ConvSecondaryAgentMapping.get_secondary_conv_by_identifiers(
        conversation_id=parent.id, aom_id=aom_id, participant_identifier=mapping.participant_identifier, states=states
    )
    if not mapping.secondary_agent_state and secondary_mapping:
        mapping.secondary_agent_state = secondary_mapping.secondary_agent_state
    delete_mapping_count = ConvSecondaryAgentMapping.delete_mapping(
        conversation_id=parent.id,
        aom_id=aom_id,
        participant_identifier=None if agent else mapping.participant_identifier,
        secondary_state=mapping.secondary_agent_state.name if mapping.secondary_agent_state else None,
        active=active,
        states=states,
    )
    logger.info(f"in remove_secondary_participant mapping deleted count : {delete_mapping_count}")
    if delete_mapping_count < 1:
        logger.error(f"in remove_secondary_participant error : {_NO_SECONDARY}")
        # A msg, not an error: cold transfers have no secondary, and an empty delete must not roll them back.
        return UseCaseResponse(status_code=422, msg=_NO_SECONDARY)
    removed = UseCaseResponse(status_code=200, msg="Secondary participant removed successfully for conversation")
    if mapping.type == mc.SecAgentTypes.add_participant:
        handle_events_and_chunks_add_secondary_participant(mapping, secondary_mapping)
        return removed
    ConversationParent.update_last_activity_at(conversation_id=parent.id, now=now)
    if mapping.secondary_agent_state in ne.MONITOR_BARGE_COACH_STATE_LIST:
        data = {"supervisor_id": str(agent.uuid), "mode": SUPERVISOR_MODES.get(mapping.secondary_agent_state, "listen")}
        monitored = mapping.active_child_conversation
        emit("supervisor.left", parent, data, monitored.primary_agent.uuid if monitored else None)
    if bypass_all_events or mapping.secondary_conversation:
        return removed
    if child := Conversation.get_active_transferable_by_contact_uuid(parent.parent_contact_uuid, mapping.channel):
        update_failed_transfer_state(child, parent.parent_contact_uuid, mapping.channel)
    return removed


def handle_non_transferred_conversations(conv_transfer: mc.ConversationModel) -> UseCaseResponse:
    from hs.conversations import child_update as ccu
    from hs.conversations.management import AgentConversationSetting, PostConversationStatusChange

    logger.info("in handle_non_transferred_conversations")
    # Close the transferring agent's other open children: an open child holds the agent's capacity
    # until it closes.
    conversations = Conversation.get_active_non_transfer_child_conversations(
        conv_transfer.parent, conv_transfer.id, conv_transfer.primary_aom
    )
    if conversations:
        logger.info(f"in handle_non_transferred_conversations, {len(conversations)} conversations found")
    else:
        logger.info("in handle_non_transferred_conversations, no conversations found to close")
    for conv in conversations or []:
        parent = conv.parent_conversation
        capacity_on_resolve = AgentConversationSetting.set_or_get_setting(
            parent.organization, parent.conversation_number
        )
        if conv.contact_uuid == parent.parent_contact_uuid and conv.state != mc.ConversationStates.transferred.name:
            ConversationParent.update_conversation(
                conversation_id=conv.parent, update_dict={ConversationParent.flow_closure_flag.name: True}
            )
        ccu.close_conversation(conv)
        PostConversationStatusChange(
            aom_uuid=str(conv.primary_agent.uuid),
            aom=conv.primary_agent,
            conversation=conv,
            changed_status=mc.ConversationStatus.closed.name,
            timeout_type=mc.TimeoutTypes.session_timeout,
            send_conversation_closed_event=False,
            update_capacity_on_resolve=capacity_on_resolve,
        ).invoke()
        logger.info(f"in handle_non_transferred_conversations, closed conversation id: {conv.id}")
    return UseCaseResponse(status_code=200, msg="Closed conversations from other channels")


def _in_process(call, failure, failed: str, non_ok: str, ok: int = 200, default: int = 200):
    """Run a use case in-process: its response, or (None, failure) on an exception (logged as `failed`) or a status
    other than ok (a missing status counts as default; logged as `<non_ok>: <status> <error>`)."""
    from hs.routing.invitation import _ensure_request_context

    try:
        _ensure_request_context()
        resp = call()
    except Exception:
        logger.error(failed, exc_info=True)
        return None, failure
    if resp.error or (resp.status_code or default) != ok:
        logger.error(f"{non_ok}: {resp.status_code} {resp.error}")
        return None, failure
    return resp, None


def transfer_allowed(contact_uuid, channel, include_conv_details=False, check_add_participant=False):
    """(transfer allowed, add participant allowed, conversation details), all falsy when the check fails."""
    resp, err = _in_process(
        lambda: check_if_transfer_allowed(contact_uuid, channel or None, include_conv_details, check_add_participant),
        True,
        "check_if_transfer_allowed in-process failed",
        f"check_if_transfer_allowed in-process non-200 for {contact_uuid}",
    )
    data = {} if err is not None else resp.data or {}
    allowed, add_allowed = data.get("transfer_allowed"), data.get("add_participant_allowed")
    return bool(allowed), bool(add_allowed), data.get("conversation_details") or {}


def set_transfer_state(contact_uuid, channel, state) -> None:
    update = lambda: mc.UpdateConvTransferModel(contact_uuid=contact_uuid, channel=channel, state=state)  # noqa: E731
    _in_process(
        lambda: update_transfer_conversation(update()),
        None,
        "update_transfer_conversation in-process failed",
        f"update_transfer_conversation in-process non-200 for {contact_uuid}",
    )


def signal_chunk(contact_uuid, channel, description, identifier, transfer_type, signal_message) -> None:
    from hs.conversations import child_update as ccu
    from hs.routing.invitation import _ensure_request_context

    signal = {"signal_message": signal_message, "identifier": identifier, "identifier_type": transfer_type}
    try:
        _ensure_request_context()
        resp = ccu.add_chunk_to_non_qnr_child(
            mc.NonQNRChunkInsert(
                parent_contact_uuid=contact_uuid,
                type=_SIGNAL,
                description=description,
                channel=channel,
                attributes=[signal],
                message_id="",
            )
        )
    except Exception:
        logger.error(f"signal_chunk: chunk creation failed for {description}", exc_info=True)
        return
    if resp.error or (resp.status_code or 201) != 201:
        logger.error(f"signal_chunk: chunk creation failed for {description} ({resp.status_code} {resp.error})")


def add_secondary(**mapping) -> str | None:
    # This use case emits mapping/chunk/event effects before it returns: with no idempotency key or outbox, an
    # exception or 5xx is an ambiguous partial success that a callback replay could duplicate.
    return _in_process(
        lambda: create_secondary_participant(mc.ConvSecAgentMappingInsert(**mapping)),
        "adding the secondary agent failed",
        "create_secondary_participant in-process failed",
        "create_secondary_participant in-process non-201",
        ok=201,
        default=0,
    )[1]


def update_secondary(**mapping) -> str | None:
    return _in_process(
        lambda: update_secondary_participant(mc.ConvSecAgentMappingInsert(**mapping)),
        "updating the secondary agent failed",
        "update_secondary_participant in-process failed",
        "update_secondary_participant in-process non-200",
    )[1]


def remove_secondary(active=True, missing_ok=False, **mapping) -> str | None:
    # Monitor-leave passes active=False so it can remove a supervisor mapping after the primary leg closes.
    def remove():
        resp = remove_secondary_participant(mc.ConvSecAgentMappingRemove(**mapping), active=active)
        return UseCaseResponse(status_code=200) if missing_ok and resp.msg == _NO_SECONDARY else resp

    return _in_process(
        remove,
        "removing the secondary agent failed",
        "remove_secondary_participant in-process failed",
        "remove_secondary_participant in-process non-200",
        default=0,
    )[1]


def transfer_child(contact_uuid, transfer_type, channel, agent_sip_or_uuid=None, mode="cold"):
    """(the new child's contact uuid, None), or ("", why the transfer failed)."""
    conv = lambda: mc.ConvTransferModel(  # noqa: E731
        contact_uuid=contact_uuid,
        transfer_type=transfer_type,
        channel=channel,
        agent_sip_or_uuid=agent_sip_or_uuid or None,
    )
    resp, err = _in_process(
        lambda: transfer_child_conversation(conv(), mode),
        "creating the transfer conversation failed",
        "transfer_child_conversation in-process failed",
        "transfer_child_conversation in-process non-201",
        ok=201,
        default=0,
    )
    return ("", err) if err is not None else (str(getattr(resp.data, "contact_uuid", None) or ""), None)
