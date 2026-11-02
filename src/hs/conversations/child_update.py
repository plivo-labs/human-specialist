from functools import wraps

from sqlalchemy import select

from hs import jobs
from hs.admin import lookups as uh
from hs.admin.lookups import fetch_aom_by_uuid_or_sip_and_set_context, fetch_org_by_id_and_set_context
from hs.conversations.models import (
    CUSTOMER_COMMUNICATION_CHUNK_TYPES,
    NON_QNR_CHILDREN,
    QNR_CHILDREN,
    ConvChunkInsertModel,
    ConversationChannels,
    ConversationChunkTypes,
    ConversationCustomStatusMappingModel,
    ConversationCustomStatusMappingStates,
    ConversationGenerationTypes,
    ConversationModel,
    ConversationParentStates,
    ConversationStates,
    ConversationStatus,
    ConversationUpdateByContactUUID,
    NonQNRChunkInsert,
    SecondaryAgentStateEnum,
    TimeoutTypes,
    UpdateParentByConversationNumber,
)
from hs.conversations.store import (
    RESOLVE_STATUSES,
    ActiveQueueEntry,
    Conversation,
    ConversationChunk,
    ConversationCustomStatusMapping,
    ConversationParent,
    ConvSecondaryAgentMapping,
    Recording,
    emit,
)
from hs.conversations.wrap_up_models import ResolveConversationParams
from hs.infra.constants import INCOMING_CALL_ENDED, OUTGOING_CALL_ENDED, SYSTEM_USER_UUID
from hs.infra.context import (
    add_background,
    context_aom_uuid,
    context_api_id,
    context_org_uuid,
    get_context_org_name,
    get_db_session,
    set_db_session_rollback,
)
from hs.infra.helpers import get_now
from hs.infra.log import logger
from hs.infra.models import UseCaseResponse, fail
from hs.routing import constants as rc
from hs.routing import rona


def _commit_parent_lock_transaction(parent_conv, phase: str) -> None:
    """Commit the parent decision/write section before any orchestration seam.

    The parent identifier and phase are safe operational metadata; payload/chunk content is
    deliberately excluded. A failed commit propagates, so no timer, runner callback, response,
    or background fan-out can observe an uncommitted decision or run while the lock is held.
    """
    api_id = context_api_id.get("default")
    parent_id = getattr(parent_conv, "id", None)
    conversation_number = getattr(parent_conv, "conversation_number", None)
    logger.info(
        f"parent_lock_transaction commit_start api_id={api_id} parent_id={parent_id} "
        f"conversation_number={conversation_number} phase={phase}"
    )
    get_db_session().commit()
    logger.info(
        f"parent_lock_transaction commit_complete api_id={api_id} parent_id={parent_id} "
        f"conversation_number={conversation_number} phase={phase}"
    )


def _rollback_db_transaction_on_error(fn):
    """Rollback lock-owning closure paths before propagating an unexpected failure."""

    @wraps(fn)
    def _wrapped(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception:
            # Do not rely on a later exception handler crossing the same ContextVar or
            # worker-thread boundary. Roll back here, where the lock-owning session lives.
            try:
                set_db_session_rollback()
            except Exception as rollback_error:
                logger.error(f"parent_lock_transaction rollback marker failed: {rollback_error}")
            try:
                get_db_session().rollback()
            except Exception as rollback_error:
                logger.error(f"parent_lock_transaction rollback failed: {rollback_error}")
            raise

    return _wrapped


ORPHANED_FLOW_LEG_CLOSE_AFTER_SEC = 60 * 60 * 24


def _orphaned_flow_legs_expired(parent_conv, now) -> bool:
    """True when a parent waiting ONLY on flow-owned (non-qnr) legs has been inactive past
    the orphan TTL. A call flow that ends without its closure signal leaves such a
    parent waiting for a signal that never comes; this staleness check is what lets the
    parent-close retry finally reclaim it. A live flow leg keeps refreshing
    last_activity_at, so an active flow conversation can never be reclaimed early.
    Fail-safe: missing timestamps or comparison errors report NOT expired."""
    try:
        last_activity = parent_conv.last_activity_at or parent_conv.parent_start_at
        if last_activity is None:
            return False
        return (now - last_activity).total_seconds() > ORPHANED_FLOW_LEG_CLOSE_AFTER_SEC
    except Exception as e:
        logger.error(
            f"orphaned-flow-leg staleness check failed for {getattr(parent_conv, 'conversation_number', '?')}: {e}"
        )
        return False


def _handle_session_timeout_post_status_change(active_agent_conversations: list):
    # Release session-resolved primary-agent capacity through the close/wrap-up path in the owning session.
    from hs.admin.account import Organization
    from hs.conversations.management import PostConversationStatusChange

    org_map = {}
    for conv in active_agent_conversations or []:
        if not getattr(conv, "primary_aom", None) or not getattr(conv, "primary_agent", None):
            continue
        try:
            org_id = conv.parent_conversation.organization
            org = org_map.get(org_id)
            if org is None:
                org = Organization.get_by_id(org_id)
                org_map[org_id] = org
            PostConversationStatusChange(
                aom_uuid=str(conv.primary_agent.uuid),
                aom=conv.primary_agent,
                org_name=org.name,
                org_uuid=org.uuid,
                conversation=conv,
                changed_status=ConversationStatus.closed.name,
                timeout_type=TimeoutTypes.session_timeout,
                update_capacity_on_resolve=False,
                send_conversation_closed_event=True,
            ).invoke()
        except Exception as e:
            logger.error(f"session-timeout post-status-change failed conv={getattr(conv, 'id', None)}: {e}")
    logger.info(f"session-timeout capacity-release processed {len(active_agent_conversations or [])} conversation(s)")


def _handle_capacity_clear_on_call_drop(conversation, aom_uuid, aom_sip_endpoint):
    """The agent's call ended: its child goes to wrap-up (armed and announced), and its capacity is released now
    or at close, per the tenant's release-on-resolve setting."""
    from hs.admin.account import AOMRoutingParams
    from hs.conversations import timers, wrap_up
    from hs.conversations.management import AgentConversationSetting

    parent = conversation.parent_conversation
    number = parent.conversation_number
    release = AgentConversationSetting.set_or_get_setting(parent.organization, number)
    if conversation.status in RESOLVE_STATUSES:
        logger.info(f"conversation is already in wrapup state, skip the flow for conversion :  {number}")
    else:
        state = ConversationCustomStatusMappingStates.in_wrap_up.value
        wrap = {"status": ConversationStatus.in_wrap_up.name, "wrap_up_start_at": get_now()}
        wrap["custom_status_mapping"] = get_conversation_custom_state_by_name(parent.organization, state).id
        Conversation.update_conversation(conversation.id, wrap, update_status_change_timestamp_for_state=state)
        timers.start_wrap_up(conversation, aom_uuid, context_org_uuid.get(), get_context_org_name())
        handle_time = Conversation.get_contacts_handle_time(parent.id, conversation.primary_aom)
        AOMRoutingParams.update_routing_params(conversation.primary_aom, handle_time)
    data = ResolveConversationParams.for_child(conversation, aom_sip_endpoint, aom_uuid, release_agent_capacity=release)
    add_background(wrap_up.resolve_conversation, data)
    logger.info(f"completed capacity clear on call drop for conversation {number}")


def _seconds(start, end) -> int | None:
    return int((end - start).total_seconds()) if start else None


def _emit_call_ended(conv) -> None:
    """call.ended for a call an agent answered; who ended it is the MPC's termination source, kept as a hangup chunk."""
    parent, now = conv.parent_conversation, get_now()
    hangups = ConversationChunk.get_conversation_chunks_by_conv_number(
        parent.conversation_number, filter_chunk_types=[ConversationChunkTypes.hangup.name]
    )
    sources = [str(getattr(a, "hangup_source", "")).lower() for c in hangups for a in c.attributes or []]
    entry = ActiveQueueEntry.get_by_id(conv.active_queue_entry_id) if conv.active_queue_entry_id else None
    customer, number = parent.contact_identifier, conv.sender_id
    inbound = parent.source_direction_inbound
    data = {
        "from": customer if inbound else number,
        "to": number if inbound else customer,
        "result": "answered",
        "queue_id": str(entry.queue_uuid) if entry else None,
        "hangup_cause": None,
        "hangup_cause_code": None,
        "duration_sec": _seconds(parent.parent_start_at, now),
        "talk_sec": _seconds(conv.start_at, now),
        "ended_by": sources[-1] if sources and sources[-1] in ("customer", "agent") else "system",
    }
    emit("call.ended", parent, data, agent=conv.primary_agent.uuid if conv.primary_agent else None)


def _call_ended_chunk_flow(non_qnr_chunk, non_qnr_child):
    """A call-ended signal: call.ended, then the answered agent's child goes to wrap-up and a phone's transfer child,
    with no agent to wrap it up, closes; None when no agent's child is open."""
    from hs.conversations import management as cm

    conv = Conversation.get_child_by_conv_parent(
        non_qnr_child.parent,
        ConversationChannels.call.name,
        inbound_direction=non_qnr_chunk.description == INCOMING_CALL_ENDED,
        filter_active=not non_qnr_chunk.skip_for_active_child,
    )
    children = Conversation.get_all_active_conversations_for_parent(
        non_qnr_child.parent_conversation.parent_contact_uuid
    )
    phone = next((c for c in children if c.state == ConversationStates.external_transfer_initiated.name), None)
    if conv or phone:
        _emit_call_ended(conv or phone)
    if phone:
        close_child(phone)
    if conv is None:
        logger.info(
            f"No answered-agent conversation for call-ended chunk; persisting without an agent event contact_uuid={non_qnr_child.contact_uuid}"
        )
        return None
    _handle_capacity_clear_on_call_drop(conv, conv.primary_agent.uuid, conv.primary_agent.sip_endpoint)
    return UseCaseResponse(
        status_code=200, msg=cm.CONVERSATION_STATE_TO_WRAP_UP_UPDATED_SUCCESSFULLY, data=non_qnr_child.contact_uuid
    )


def _recording_chunk_event(non_qnr_chunk, chunk, non_qnr_child):
    if non_qnr_chunk.is_main_recording or not chunk.recording:
        return
    recording = Recording.get_by_id(chunk.recording)
    duration = recording.recording_duration or ""
    data = {"recording_id": str(recording.uuid), "duration_sec": int(duration) if duration.isdigit() else None}
    # in the recording's own transaction: a failed commit is retried by the provider, and so is the event
    emit("recording.ready", non_qnr_child.parent_conversation, data, session=get_db_session())


ERROR_CONVERSATION_NOT_FOUND = "Conversation not found"
ERROR_AOM_NOT_FOUND = "Agent not found for given sip_endpoint or aom_uuid"
ERROR_AOM_AND_CONV_NOT_IN_ORG = "Agent and conversation are not in the same organization"
ERROR_AOM_AND_CONV_ALREADY_LINKED = "Another agent already linked to the conversation"
ERROR_NON_QNR_CHILD_NOT_FOUND = "Non QNR child conversation not found"
ERROR_PARENT_CONVERSATION_NOT_FOUND = "Parent conversation not found"
MSG_UPDATE_CONVERSATION_SUCCESS = "Conversation updated successfully"
MSG_PARENT_CONVERSATION_CLOSURE_WAIT_FOR_OTHERS = "Wait for other conversations to get closed before closing parent"
MSG_UPDATE_PARENT_CONVERSATION_SUCCESS = "Parent conversation updated successfully"
MSG_ADD_CHUNK_TO_NON_QNR_CHILD_SUCCESS = "Chunk added to non QNR child successfully"


def get_conversation_custom_state_by_name(org_id: int, state_name: str, use_cache: bool = True):
    if use_cache:
        custom_state = uh.cached_custom_state(org_id, state_name)
        if custom_state:
            return custom_state
    custom_state_list = ConversationCustomStatusMapping.get_all_states_by_org(org_id=org_id)
    custom_state_model_list = []
    result = None
    for state in custom_state_list:
        custom_state_model_list.append(ConversationCustomStatusMappingModel(id=state.id, state=state.state))
        if state.state == state_name:
            result = ConversationCustomStatusMappingModel(id=state.id, state=state.state)
    if use_cache:
        uh.cache_custom_states(org_id, custom_state_model_list)
    return result


def update_custom_state_for_multiple_conversations(
    child_ids: list[int], custom_state_id: int, custom_state_name: str, now=None, commit_transaction: bool = True
):
    now = now or get_now()
    if not child_ids:
        logger.info("in update_custom_state_for_conversations: no child_ids to update")
        return
    for conv_id in child_ids:
        Conversation.update_conversation(
            conv_id,
            update_dict={Conversation.custom_status_mapping.name: custom_state_id},
            now=now,
            update_status_change_timestamp_for_state=custom_state_name,
            commit_transaction=commit_transaction,
        )


@_rollback_db_transaction_on_error
def close_conversation(
    conversation: ConversationModel,
    state: ConversationStates = None,
    resolve_for_agent=False,
    is_destination_transfer=False,
    flow_ended=False,
) -> ConversationModel:
    now = get_now()
    # Acquire the parent lock before child writes to serialize with parent closure.
    ConversationParent.get_by_conversation_number(conversation.parent_conversation.conversation_number, for_update=True)
    update = {
        Conversation.status.name: ConversationStatus.closed.name,
        Conversation.updated_at.name: now,
        Conversation.end_at.name: now,
    }
    if state:
        update[Conversation.state.name] = state.name
    custom_conv_state = None
    if resolve_for_agent:
        custom_conv_state = get_conversation_custom_state_by_name(
            org_id=conversation.parent_conversation.organization,
            state_name=ConversationCustomStatusMappingStates.resolved.value,
            use_cache=False,
        )
        update[Conversation.custom_status_mapping.name] = custom_conv_state.id
    logger.info(
        f"in close_conversation :: for conv_id {conversation.id} update: {update} resolve_for_agent: "
        f"{resolve_for_agent}"
    )
    Conversation.update_conversation(
        conversation.id,
        update,
        now,
        update_status_change_timestamp_for_state=custom_conv_state.state if custom_conv_state else None,
        commit_transaction=False,
    )
    conversation = conversation.model_copy(update=update)
    if resolve_for_agent:
        agent_convs = Conversation.get_active_conversations_by_parent_for_aom(
            conversation.parent, conversation.primary_aom
        )
        logger.info(
            f"in close_conversation: closing all conversations for aom {conversation.primary_aom} parent "
            f"{conversation.parent_conversation.conversation_number} agent_convs {len(agent_convs)}"
        )
        if agent_convs:
            Conversation.update_active_conversations_by_parent_for_aom(
                conversation.parent, conversation.primary_aom, update, now
            )
            child_ids = [conv.id for conv in agent_convs]
            update_custom_state_for_multiple_conversations(
                child_ids,
                custom_conv_state.id,
                ConversationCustomStatusMappingStates.resolved.value,
                now,
                commit_transaction=False,
            )
    if is_destination_transfer:
        _commit_parent_lock_transaction(conversation.parent_conversation, "destination_transfer_child_close")
        logger.info("in close_conversation: skipping closing parent conversation for queue transfer")
        return conversation
    parent_conversation_number = conversation.parent_conversation.conversation_number
    update_obj = UpdateParentByConversationNumber(
        conversation_number=parent_conversation_number, state=ConversationParentStates.closed, is_flow_ended=flow_ended
    )
    response = update_parent_conversation_by_conversation_number(update=update_obj)
    if response.error:
        if response.error == MSG_PARENT_CONVERSATION_CLOSURE_WAIT_FOR_OTHERS:
            # Expected under parallel child completion: this child is
            # durably closed while the parent correctly remains open.
            logger.info(
                f"in close_conversation: parent remains open while other children are active: "
                f"{conversation.parent_conversation.conversation_number}"
            )
        else:
            logger.error(
                f"in close_conversation error in updating parent: "
                f"{conversation.parent_conversation.conversation_number} response: {response.error}"
            )
    else:
        logger.info(
            f"in close_conversation successfully updated the parent: "
            f"{conversation.parent_conversation.conversation_number} response: {response.msg}"
        )
    return conversation


def close_child(conversation, resolve_for_agent=False) -> None:
    """HS has no flow to end the call's conversation: it closes with its last agent child, unless a transfer
    destination is still queued or ringing."""
    others = Conversation.get_all_active_conversations_for_parent(conversation.parent_conversation.parent_contact_uuid)
    pending = [c for c in others if c.id != conversation.id and c.generation_type not in NON_QNR_CHILDREN]
    close_conversation(conversation, resolve_for_agent=resolve_for_agent, flow_ended=not pending)


def close_parent_conversation(parent_conversation, now=None):
    now = now or get_now()
    ConversationParent.update_conversation(
        parent_conversation.id, {ConversationParent.end_at.name: now}, now, commit_transaction=False
    )
    ConvSecondaryAgentMapping.delete_all_secondary_mappings(parent_conversation.id)
    data, agent = _closed_event(parent_conversation, now)
    emit("conversation.closed", parent_conversation, data, agent, get_db_session())
    _commit_parent_lock_transaction(parent_conversation, "parent_closed")
    # Write the closed-conversation search row once the close writes commit.
    from hs.conversations import search as ccsm

    add_background(ccsm.safe_upsert, parent_conversation.conversation_number)
    return parent_conversation.model_copy(update={"parent_end_at": now})


def _closed_event(parent, now) -> tuple[dict, str | None]:
    """conversation.closed's data, and the last agent who handled the conversation."""
    (cp,) = ConversationParent.get_parent_conversation_preloaded(parent.conversation_number)
    agent, dispositions, recordings = None, [], []
    # in creation order, whatever order the rows loaded in, so the last child with an agent handled it last
    for child in sorted(cp.child_convs, key=lambda c: c.id):
        aom = child.primary_agent
        agent = str(aom.uuid) if aom else agent
        for chunk in child.chunks:
            if chunk.recording:
                recordings.append(chunk.recording)
            if chunk.type != ConversationChunkTypes.notes.name or not chunk.disposition:
                continue
            dispositions.append(
                {
                    "agent_id": str(aom.uuid) if aom else None,
                    "disposition_id": str(chunk.dispo.uuid) if chunk.dispo else None,
                    "sub_disposition_id": str(chunk.sub_dispo.uuid) if chunk.sub_dispo else None,
                    "system": bool(chunk.dispo and chunk.dispo.internal),
                    # the wrap-up timer's close: written by HS, not the agent, on a child that went to wrap-up
                    "skipped": bool(aom and child.wrap_up_start_at and chunk.from_user != str(aom.uuid)),
                }
            )
    ids = get_db_session().query(Recording.uuid).filter(Recording.id.in_(recordings)).all() if recordings else []
    # a column read: the result was written in this session by SQL the loaded row has not seen
    result = get_db_session().scalar(select(ConversationParent.result).where(ConversationParent.id == cp.id))
    data = {
        "result": result or ("answered" if agent else "abandoned"),
        "dispositions": dispositions,
        "recording_ids": [str(row[0]) for row in ids],
        "duration_sec": _seconds(cp.created_at, now),
    }
    return data, agent


def close_parent_and_non_qnr(parent_conv, all_conversations, active_agent_conversations, now=None) -> UseCaseResponse:
    from hs.admin.account import Organization
    from hs.conversations.management import AgentConversationSetting

    now = now or get_now()
    org = Organization.get_by_id(parent_conv.organization)
    Conversation.update_all_non_qnr_for_parent(
        parent_conv.conversation_number,
        update_dict={Conversation.status: ConversationStatus.closed.name, Conversation.end_at: now},
        now=now,
    )
    # This call writes the parent end marker and explicitly commits the complete
    # parent/child decision section. Everything below is post-commit orchestration.
    close_parent_conversation(parent_conv, now=now)
    custom_state = get_conversation_custom_state_by_name(
        org_id=org.id, state_name=ConversationCustomStatusMappingStates.resolved.value
    )
    for conv in all_conversations:
        if conv.end_at is None:
            # BackgroundTasks wraps sync work in bg_db_guard, and this adapter's
            # default commit_transaction=True commits each post-close metadata stamp.
            add_background(
                Conversation.update_conversation,
                conv.id,
                update_dict={Conversation.custom_status_mapping.name: custom_state.id},
                now=now,
                update_status_change_timestamp_for_state=custom_state.state,
            )
    if len(all_conversations) > 0:
        # The parent/child close decision was already committed above, so no parent
        # lock is held across this separate post-close transaction.
        get_db_session().commit()
        logger.info(
            f"post_parent_close_transaction commit_complete parent_id={parent_conv.id} "
            f"conversation_number={parent_conv.conversation_number}"
        )
    logger.info("in close_parent_and_non_qnr, deleting update agent capacity on resolve flag from cache")
    AgentConversationSetting.delete_setting(parent_conv.conversation_number)
    return UseCaseResponse(status_code=200, msg=MSG_UPDATE_PARENT_CONVERSATION_SUCCESS)


@_rollback_db_transaction_on_error
def update_parent_conversation_by_conversation_number(update: UpdateParentByConversationNumber) -> UseCaseResponse:
    method_name = "in update_parent_conversation_by_conversation_number"
    logger.info(f"{method_name} update: {update}")
    if update.state != ConversationParentStates.closed.name:
        logger.error(f"{method_name} error : no implementation for this state, update {update}")
        return UseCaseResponse(status_code=422, error="No implementation for this state")
    parent_conv = None
    if update.conversation_number:
        parent_conv = ConversationParent.get_by_conversation_number(
            update.conversation_number, active=True, for_update=True
        )
    if not parent_conv and update.parent_contact_uuid:
        logger.info(f"{method_name} querying parent conv by contact_uuid: {update.parent_contact_uuid}")
        parent_conv = ConversationParent.get_by_contact_uuid(
            update.parent_contact_uuid, only_open_conv=True, for_update=True
        )
    if not parent_conv and update.conversation_number:
        # An already-closed parent is a successful closure retry; persist the flow-ended flag when needed.
        closed_parent = ConversationParent.get_by_conversation_number(update.conversation_number)
        if closed_parent:
            if update.is_flow_ended and not closed_parent.flow_closure_flag:
                ConversationParent.update_conversation(
                    closed_parent.id,
                    update_dict={ConversationParent.flow_closure_flag.name: update.is_flow_ended},
                    now=get_now(),
                )
            logger.info(
                f"{method_name} parent already closed: {closed_parent.conversation_number}; treating closure as done"
            )
            return UseCaseResponse(msg="Parent conversation already closed")
    if not parent_conv:
        logger.error(f"{method_name} error: {ERROR_PARENT_CONVERSATION_NOT_FOUND}")
        return UseCaseResponse(status_code=404, error=ERROR_PARENT_CONVERSATION_NOT_FOUND)
    now = get_now()
    is_ok_to_close_conversation = False
    update_custom_state_resolved = False
    run_session_timeout_post_status_change = False
    if update.is_flow_ended:
        update_dict = {ConversationParent.flow_closure_flag.name: update.is_flow_ended}
        ConversationParent.update_conversation(
            parent_conv.id, update_dict=update_dict, now=now, commit_transaction=False
        )
        parent_conv = parent_conv.model_copy(update=update_dict)
    all_conversations = Conversation.get_all_active_conversations_for_parent(parent_conv.parent_contact_uuid)
    active_agent_conversations = [conv for conv in all_conversations if conv.generation_type not in NON_QNR_CHILDREN]
    active_non_agent_conversations = [conv for conv in active_agent_conversations if not conv.primary_aom]
    if (
        update.is_flow_ended
        and active_agent_conversations
        and len(active_agent_conversations) == len(active_non_agent_conversations)
    ):
        update_dict = {Conversation.status.name: ConversationStatus.closed.name, Conversation.end_at.name: now}
        Conversation.update_all_active_conversations_by_parent_id(
            parent_conv_id=parent_conv.id, update_dict=update_dict, now=now, only_non_bot=True
        )
        is_ok_to_close_conversation = True
        update_custom_state_resolved = True
    session_resolved_conversations = [
        conv for conv in active_agent_conversations if conv.status == ConversationStatus.session_resolved.name
    ]
    if active_agent_conversations and (
        len(active_agent_conversations) == len(session_resolved_conversations) or parent_conv.is_hidden
    ):
        update_dict = {Conversation.end_at.name: now}
        Conversation.update_all_active_conversations_by_parent_id(
            parent_conv_id=parent_conv.id, update_dict=update_dict, now=now, only_non_bot=True
        )
        is_ok_to_close_conversation = True
        update_custom_state_resolved = True
        run_session_timeout_post_status_change = True
    if is_ok_to_close_conversation or not active_agent_conversations:
        if parent_conv.flow_closure_flag or len(all_conversations) == 0:
            is_ok_to_close_conversation = True
        elif _orphaned_flow_legs_expired(parent_conv, now):
            # Close inactive orphan parents and their flow-owned children together after the orphan TTL.
            # A remaining agent-owned child or recent activity must prevent this forced closure.
            logger.info(
                f"{method_name} closing orphaned parent (flow legs inactive past TTL): "
                f"{parent_conv.conversation_number}"
            )
            is_ok_to_close_conversation = True
    if update_custom_state_resolved:
        child_ids = [conv.id for conv in active_agent_conversations if conv.primary_aom]
        # Avoid Redis/cache traffic while the parent row is locked; this database-only
        # lookup remains inside the transaction and the normal post-commit path can warm
        # or read the cache safely.
        custom_state = get_conversation_custom_state_by_name(
            org_id=parent_conv.organization,
            state_name=ConversationCustomStatusMappingStates.resolved.value,
            use_cache=False,
        )
        update_custom_state_for_multiple_conversations(
            child_ids, custom_state.id, custom_state.state, now, commit_transaction=False
        )
    if is_ok_to_close_conversation:
        logger.info(f"{method_name} closing parent: {parent_conv.conversation_number}")
        response = close_parent_and_non_qnr(parent_conv, all_conversations, active_agent_conversations, now)
        if run_session_timeout_post_status_change:
            try:
                logger.info(
                    f"{method_name} calling handle_session_timeout_post_status_change with params "
                    + f"convs_len: {len(active_agent_conversations)}"
                )
                _handle_session_timeout_post_status_change(active_agent_conversations)
            except Exception as e:
                logger.error(f"error in co routine: {e}")
        return response
    _commit_parent_lock_transaction(parent_conv, "parent_close_deferred")
    if update.is_flow_ended or not active_agent_conversations:
        # Poll deferred closure after FLOW ends or no agent children remain; orphan-only parents need this trigger.
        key = update.conversation_number or update.parent_contact_uuid
        jobs.schedule("parent_close", update.model_dump(mode="json"), delay=3600, key=key)
        logger.info(f"{method_name} scheduled callback for parent closure: {update.conversation_number}")
    logger.info(
        f"{method_name} for parent: {parent_conv.conversation_number} waiting for other conversations to "
        f"wrap-up - Total open conversations: {len(all_conversations)} agent conversations: "
        f"{len(active_agent_conversations)}"
    )
    return UseCaseResponse(status_code=422, error=MSG_PARENT_CONVERSATION_CLOSURE_WAIT_FOR_OTHERS)


def add_chunk_to_non_qnr_child(non_qnr_chunk: NonQNRChunkInsert) -> UseCaseResponse:
    from hs.conversations import management as cm

    logger.info(f"in add_chunk_to_non_qnr_child request : {non_qnr_chunk}")
    filter_active_child = True
    if non_qnr_chunk.type in [
        ConversationChunkTypes.recording.name,
        ConversationChunkTypes.hangup.name,
        ConversationChunkTypes.voicemail.name,
        ConversationChunkTypes.signal.name,
    ]:
        filter_active_child = False
    if non_qnr_chunk.chunk_for_hangup_node:
        filter_active_child = False
    non_qnr_child = Conversation.get_active_non_qnr_child_by_contact_uuid(
        non_qnr_chunk.parent_contact_uuid, filter_active=filter_active_child
    )
    if not non_qnr_child:
        return fail(ERROR_NON_QNR_CHILD_NOT_FOUND)
    error, org = fetch_org_by_id_and_set_context(non_qnr_child.parent_conversation.organization)
    if error:
        logger.error(f"in add_chunk_to_non_qnr_child error : {error}")
        return UseCaseResponse(status_code=422, msg=error)
    if not non_qnr_chunk.disposition and non_qnr_chunk.type in CUSTOMER_COMMUNICATION_CHUNK_TYPES:
        state = (
            ConversationCustomStatusMappingStates.waiting_on_customer.value
            if not non_qnr_chunk.is_inbound_chunk
            else ConversationCustomStatusMappingStates.in_progress_human.value
        )
        custom_conv_state = get_conversation_custom_state_by_name(org_id=org.id, state_name=state)
        update_dict = {Conversation.custom_status_mapping.name: custom_conv_state.id}
        Conversation.update_conversation(
            conversation_id=non_qnr_child.id, update_dict=update_dict, update_status_change_timestamp_for_state=state
        )
    if non_qnr_chunk.type == ConversationChunkTypes.signal.name and non_qnr_chunk.description in [
        OUTGOING_CALL_ENDED,
        INCOMING_CALL_ENDED,
    ]:
        if (early := _call_ended_chunk_flow(non_qnr_chunk, non_qnr_child)) is not None:
            return early
    logger.info(f"Building db model for conversation id={non_qnr_child.id}")
    if non_qnr_chunk.description == SecondaryAgentStateEnum.participant_did_not_answered.name:
        non_qnr_chunk.description = SecondaryAgentStateEnum.participant_did_not_answered.name.upper()
    chunk = non_qnr_chunk.build_db_model(conversation=non_qnr_child)
    logger.info(f"Creating {non_qnr_chunk.type} chunk for conversation id={non_qnr_child.id}")
    if non_qnr_chunk.create_chunk:
        cm.create_chunk_with_related_data(chunk)
        db = get_db_session()
        db.commit()
    logger.info("in add_chunk_to_non_qnr_child success")
    if non_qnr_chunk.type == ConversationChunkTypes.recording.name:
        _recording_chunk_event(non_qnr_chunk, chunk, non_qnr_child)
    logger.info(f"Updating last activity for conversation id {non_qnr_child.parent_conversation.id}")
    ConversationParent.update_last_activity_at(conversation_id=non_qnr_child.parent_conversation.id)
    return UseCaseResponse(status_code=201, msg=MSG_ADD_CHUNK_TO_NON_QNR_CHILD_SUCCESS, data=chunk)


_CS = ConversationCustomStatusMappingStates


def update_child_conversation(update: ConversationUpdateByContactUUID) -> UseCaseResponse:
    from hs.conversations.management import PostConversationStatusChange

    logger.info(f"in update_child_conversation request : {update}")
    now = get_now()
    if update.state:
        return ChildConversationStateManagement(
            state=update.state,
            child_contact_uuid=update.contact_uuid,
            now=now,
            extra_update_data_dict=update.model_dump(),
        ).invoke()
    aom = None
    conversation = None
    if update.agent_sip_or_uuid:
        if update.child_conversation_id:
            conversation = Conversation.get_by_id(update.child_conversation_id)
        else:
            conversation = Conversation.get_active_unanswered_qnr_child_by_contact_uuid(
                update.contact_uuid, channel=update.channel
            )
        if not conversation:
            # Recover only the same active QNR child and AOM without re-emitting acceptance events.
            # Different agents and terminal conversations retain the not-found result.
            accepted_aom = fetch_aom_by_uuid_or_sip_and_set_context(update.agent_sip_or_uuid)
            accepted = None
            if accepted_aom:
                accepted = Conversation.get_conversation_for_child_contact_uuid(
                    update.contact_uuid, aom_id=accepted_aom.id
                )
            exact_accepted_replay = bool(
                accepted
                and str(accepted.primary_aom) == str(accepted_aom.id)
                and accepted.generation_type in QNR_CHILDREN
                and accepted.state == ConversationStates.agent_answered.name
                and accepted.end_at is None
                and (not update.channel or accepted.channel == update.channel)
                and accepted.parent_conversation.parent_end_at is None
            )
            if exact_accepted_replay:
                logger.info(
                    f"update_child_conversation: exact same-agent accepted replay; skipping duplicate effects "
                    f"contact_uuid={update.contact_uuid}"
                )
                return UseCaseResponse(status_code=200, msg=MSG_UPDATE_CONVERSATION_SUCCESS, data=accepted)
            logger.error(
                f"in update_child_conversation for qnr_generated error:{ERROR_CONVERSATION_NOT_FOUND} "
                f"contact_uuid : {update.contact_uuid}"
            )
            return UseCaseResponse(status_code=404, error=ERROR_CONVERSATION_NOT_FOUND)
        aom = fetch_aom_by_uuid_or_sip_and_set_context(update.agent_sip_or_uuid)
        if not aom:
            logger.error(
                f"in update_child_conversation for qnr_generated error : {ERROR_AOM_NOT_FOUND} "
                f"agent_sip_or_uuid : {update.agent_sip_or_uuid}"
            )
            return UseCaseResponse(status_code=400, error=ERROR_AOM_NOT_FOUND)
        if aom.organization != conversation.parent_conversation.organization:
            logger.error(
                f"in update_child_conversation for qnr_generated error : {ERROR_AOM_AND_CONV_NOT_IN_ORG} "
                f"agent_sip_or_uuid : {update.agent_sip_or_uuid}"
            )
            return UseCaseResponse(status_code=400, error=ERROR_AOM_AND_CONV_NOT_IN_ORG)
        if str(conversation.primary_aom) == str(aom.id):
            logger.error(
                f"in update_child_conversation for qnr_generated error : {ERROR_AOM_AND_CONV_ALREADY_LINKED} "
                f"agent_sip_or_uuid : {update.agent_sip_or_uuid}"
            )
            return UseCaseResponse(status_code=400, error=ERROR_AOM_AND_CONV_ALREADY_LINKED)
        is_transfer = conversation.generation_type == ConversationGenerationTypes.transfer_qnr_generated.name
        logger.info(f"in update_child_conversation: child_conversation id={conversation.id}, aom: {aom.id}")
        open_state = get_conversation_custom_state_by_name(org_id=aom.organization, state_name=_CS.open.value)
        progress_state = get_conversation_custom_state_by_name(
            org_id=aom.organization, state_name=_CS.in_progress_human.value
        )
        update_dict = {
            Conversation.primary_aom.name: aom.id,
            Conversation.state.name: ConversationStates.agent_answered.name,
            Conversation.custom_status_mapping.name: progress_state.id,
            Conversation.start_at.name: now,
        }
        if is_transfer:
            last_empty_to_user_transfer_chunk = ConversationChunk.get_conversation_chunks_by_conv_number(
                conversation.parent_conversation.conversation_number,
                filter_empty_to_user=True,
                filter_chunk_types=[ConversationChunkTypes.transfer.name],
            )
            logger.info(
                "in update_child_conversation: "
                + f"last_empty_to_user_transfer_chunk - {last_empty_to_user_transfer_chunk}"
            )
            if last_empty_to_user_transfer_chunk:
                ConversationChunk.update_conversation_chunk_by_id(
                    last_empty_to_user_transfer_chunk[0].id, {"to_user": str(aom.uuid)}
                )
        Conversation.update_conversation(
            conversation_id=conversation.id,
            now=now,
            update_dict={Conversation.custom_status_mapping.name: open_state.id},
            update_status_change_timestamp_for_state=open_state.state,
        )
        Conversation.update_conversation(
            conversation_id=conversation.id,
            update_dict=update_dict,
            now=now,
            update_status_change_timestamp_for_state=progress_state.state,
        )
        db = get_db_session()
        rona.reset(str(context_org_uuid.get()), str(aom.uuid))
        db.commit()
        PostConversationStatusChange(conversation=conversation, changed_status=ConversationStatus.open.name).invoke()
        ConversationParent.update_last_activity_at(conversation_id=conversation.parent_conversation.id, now=now)
    elif update.reason_for_unanswered:
        if update.child_conversation_id:
            conversation = Conversation.get_by_id(update.child_conversation_id)
        else:
            conversation = Conversation.get_active_unanswered_qnr_child_by_contact_uuid(
                update.contact_uuid, channel=update.channel, active_queue_id=update.active_queue_id
            )
        from hs.admin.seeds import STATUS_MAP_DEFAULTS
        from hs.conversations.outcomes import system_disposition

        result = update.reason_for_unanswered
        if conversation:
            fetch_org_by_id_and_set_context(conversation.parent_conversation.organization)
            disposition = system_disposition(conversation.parent_conversation.id, result)
        else:
            disposition = STATUS_MAP_DEFAULTS["inbound"][result]
        non_qnr_chunk = NonQNRChunkInsert(
            type=ConversationChunkTypes.notes.name, disposition=disposition, parent_contact_uuid=update.contact_uuid
        )
        response = add_chunk_to_non_qnr_child(non_qnr_chunk=non_qnr_chunk)
        if response.error:
            return response
        if conversation:
            # A caller who hung up while queued ends the flow: no agent or later action would close it.
            close_conversation(conversation=conversation, flow_ended=result == rc.UNANSWERED_CALL_DROPPED)
    logger.info(f"in update_child_conversation for qnr_generated {MSG_UPDATE_CONVERSATION_SUCCESS}")
    return UseCaseResponse(status_code=200, msg=MSG_UPDATE_CONVERSATION_SUCCESS)


# Plivo hangup cause code -> the outbound call result; any other failure is the carrier's.
_FAILED_CALL_RESULTS = {
    **dict.fromkeys(("3000", "6010"), "no_answer"),
    **dict.fromkeys(("3010", "3100"), "busy"),
    **dict.fromkeys(("3020", "3110"), "rejected"),
    **dict.fromkeys(("2000", "3050", "3120"), "failed_invalid"),
}


def call_result(code: str) -> str:
    """The outbound result of a failed call's Plivo hangup cause code."""
    return _FAILED_CALL_RESULTS.get(code, "failed_carrier")


_S = ConversationStates
_CALL_STATES = (
    _S.outbound_contact_answered.name,
    _S.outbound_contact_agent_joined.name,
    _S.outbound_contact_failed.name,
)


class ChildConversationStateManagement:
    # the states each channel and each generation type may move a child into, and the handler of each
    CHANNEL_STATES = {ConversationChannels.call.name: _CALL_STATES}
    GENERATION_TYPE_STATES = {ConversationGenerationTypes.outbound_generated.name: _CALL_STATES}
    STATE_HANDLERS = {
        _S.outbound_contact_answered.name: "_state_outbound_contact_answered",
        _S.outbound_contact_agent_joined.name: "_state_outbound_contact_agent_joined",
        _S.outbound_contact_failed.name: "_state_outbound_contact_failed",
    }

    def __init__(self, state, child_contact_uuid, now, conversation_child=None, extra_update_data_dict=None):
        self.state = state
        self.child_contact_uuid = child_contact_uuid
        self.conversation_child = conversation_child
        self.now = now
        self.update_dict = self.event = self.aom = self.response = None
        self.extra_update_data_dict = extra_update_data_dict or {}

    def _close_with_status(self, state, aom_uuid=None):
        """Close the child in `state` and run the closed-status hand-off (capacity release per the org setting)."""
        from hs.conversations.management import AgentConversationSetting, PostConversationStatusChange

        parent = self.conversation_child.parent_conversation
        release = AgentConversationSetting.set_or_get_setting(parent.organization, parent.conversation_number)
        close_conversation(self.conversation_child, state=state)
        PostConversationStatusChange(
            aom_uuid=aom_uuid,
            conversation=self.conversation_child,
            aom=self.aom,
            changed_status=ConversationStatus.closed.name,
            update_capacity_on_resolve=release,
        ).invoke()

    def _state_outbound_contact_answered(self):
        from hs.conversations import timers

        number = self.conversation_child.parent_conversation.conversation_number
        self.update_dict = {Conversation.start_at.name: self.now, Conversation.state.name: self.state}
        self.event = ("call.answered", {"call_uuid": self.conversation_child.contact_uuid})
        # Arm outbound call session expiry so abandoned legs can resolve and release their parents.
        actor = (context_aom_uuid.get(), context_org_uuid.get(), get_context_org_name())
        timers.schedule_session_timeout(number, ConversationChannels.call.name, *actor)

    def _state_outbound_contact_agent_joined(self):
        self.update_dict = {Conversation.state.name: self.state}

    def _state_outbound_contact_failed(self):
        from hs.conversations import management as cm

        parent, failed = self.conversation_child.parent_conversation, ConversationStates.outbound_contact_failed
        non_qnr_conv = Conversation.get_active_non_qnr_child_by_contact_uuid(parent.parent_contact_uuid)
        code = self.extra_update_data_dict.get("hangup_cause_code") or ""
        result = self.extra_update_data_dict.get("result") or call_result(code)
        self.event = ("call.failed", {"result": result, "hangup_cause_code": int(code) if code.isdigit() else None})
        from hs.conversations.outcomes import system_disposition

        notes = ConvChunkInsertModel(
            type=ConversationChunkTypes.notes.name,
            from_user=SYSTEM_USER_UUID,
            to_user=parent.contact_identifier,
            disposition=system_disposition(parent.id, result),
            channel=self.conversation_child.channel,
        )
        cm.create_chunk_with_related_data(notes.build_db_model(conversation=self.conversation_child))
        self._close_with_status(failed)
        close_conversation(non_qnr_conv, state=failed)

    def invoke(self) -> UseCaseResponse:
        where = "in ChildConversationStateManagement invoke"
        log = (
            f"in ChildConversationStateManagement: invoking state {self.state} for contact_uuid "
            f"{self.child_contact_uuid} and child {self.conversation_child}"
        )
        logger.debug(log)
        if not self.conversation_child:
            self.conversation_child = Conversation.get_outbound_child_by_contact_uuid(self.child_contact_uuid)
        child = self.conversation_child
        if not child:
            not_found = ERROR_CONVERSATION_NOT_FOUND
            return fail(not_found, 404, log=f"{where} uuid {self.child_contact_uuid} not found")
        self.aom = fetch_aom_by_uuid_or_sip_and_set_context(str(child.primary_agent.uuid))
        if self.state not in self.CHANNEL_STATES.get(child.channel, ()):
            log = f"{where} state {self.state} not supported for channel {child.channel}"
            return fail("state not supported for channel", 422, log=log)
        if self.state not in self.GENERATION_TYPE_STATES.get(child.generation_type, ()):
            log = f"{where} state {self.state} not supported for generation_type {child.generation_type}"
            return fail("state not supported for generation_type", 422, log=log)
        handler = self.STATE_HANDLERS.get(self.state)
        if not handler:
            return fail("No function found to invoke", 404, log=f"{where} state {self.state} not found")
        getattr(self, handler)()
        if self.response and self.response.error:
            logger.error(f"{where} error {self.response.error}")
            return self.response
        if self.update_dict:
            Conversation.update_conversation(conversation_id=child.id, update_dict=self.update_dict, now=self.now)
            ConversationParent.update_last_activity_at(conversation_id=child.parent_conversation.id, now=self.now)
        if self.event:
            event_type, data = self.event
            emit(event_type, child.parent_conversation, data, child.primary_agent.uuid)
        logger.info(f"{where} {MSG_UPDATE_CONVERSATION_SUCCESS}")
        return UseCaseResponse(status_code=200, msg=MSG_UPDATE_CONVERSATION_SUCCESS)
