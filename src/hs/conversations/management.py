import datetime

from hs import jobs
from hs.admin.account import AccountOrgMapping, AOMRoleProfile, Organization, RoutingSettings
from hs.admin.account_models import AOMOccupiedState
from hs.admin.dispositions import Dispositions
from hs.admin.lookups import is_valid_aom_user_for_chunk
from hs.conversations.models import (
    CHANNELS_FOR_SESSION_TIMEOUT,
    OPEN_STATUSES,
    RESOLVE_STATUSES,
    ConvChannel,
    ConversationChannels,
    ConversationChunkTypes,
    ConversationGenerationTypes,
    ConversationStates,
    ConversationStatus,
    ConversationVariablesRes,
    ConvGetByContactUUID,
    ConvInsertModel,
    ConvParentAndCustomerConvDataRes,
)
from hs.conversations.store import Conversation, ConversationChunk, ConversationParent, CustomerConversationData, emit
from hs.conversations.wrap_up_models import WrapUpConversationParams
from hs.infra.constants import (
    BARGE,
    CHUNKS_CACHE_KEY,
    CONVERSATION_IDENTIFIER,
    CONVERSATION_SESSION_EXTENDED_TIMEOUT_API,
    CONVERSATION_SESSION_TIMEOUT_API,
    DISPOSITION_FOR_AUTO_CLOSED,
    EXPIRY_FOR_CONVERSATION_SESSION_TIMEOUT_CONCURRENT_REQUEST_SEC,
    EXPIRY_FOR_CONVERSATION_TO_FLOW_MAPPING_IN_SECONDS,
    EXPIRY_FOR_WRAPUP_TIMEOUT_CONCURRENT_REQUEST_SEC,
    MONITOR_BARGE_COACH_STATE_LIST,
    ROLE_ADMIN,
    ROLE_SUPERVISOR,
    SYSTEM_USER_UUID,
    WRAPUP_TIMEOUT_API,
)
from hs.infra.context import (
    context_aom_uuid,
    context_org_uuid,
    get_context_org_name,
    get_db_session,
    set_db_session_rollback,
)
from hs.infra.helpers import (
    build_cache_key_without_integration,
    get_now,
    get_number_in_e164_format,
    get_time_difference_in_seconds,
    get_utc,
    is_valid_phone_number,
    sanitize_string_input,
)
from hs.infra.log import logger
from hs.infra.models import UseCaseResponse, fail
from hs.infra.redis import Cache
from hs.voice.call_models import CallRecordingType

_AGENT_CONV_SETTING_PREFIX = "ACS"


def _emit_wrapped_up(conversation, chunk, agent_uuid, skipped=False) -> None:
    from hs.infra.context import get_db_session

    dispo = Dispositions.get_by_id(chunk.disposition_id) if chunk.disposition_id else None
    data = {
        "disposition_id": str(dispo.uuid) if dispo else None,
        "disposition": chunk.disposition,
        "sub_disposition": chunk.sub_disposition,
        "note": chunk.description,
        "system": skipped or bool(dispo and dispo.internal),
        "skipped": skipped,
    }
    emit("conversation.wrapped_up", conversation.parent_conversation, data, agent_uuid, get_db_session())


class AgentConversationSetting:
    @staticmethod
    def get_cache_key(conversation_id, key):
        return f"{_AGENT_CONV_SETTING_PREFIX}:{conversation_id}:{key}"

    @staticmethod
    def set_or_get_setting(org_id, conversation_id, key="release_capacity_on_resolve") -> bool:
        cache_key = AgentConversationSetting.get_cache_key(conversation_id, key)
        update_capacity = Cache.get_instance().get(cache_key)
        log = f"in set_or_get_setting for cache_key: {cache_key} and key: {key}, update_capacity: {update_capacity}"
        logger.info(log)
        if update_capacity is None:
            routing_setting = RoutingSettings.get_by_org_id(org_id)
            if routing_setting:
                update_capacity = routing_setting.release_capacity_on_resolve
                update_capacity = "false" if update_capacity is None else str(update_capacity)
                Cache.get_instance().set(cache_key, update_capacity)
        return update_capacity.lower() == "true" if update_capacity else False

    @staticmethod
    def delete_setting(conversation_id, key="release_capacity_on_resolve"):
        return Cache.get_instance().delete(AgentConversationSetting.get_cache_key(conversation_id, key))


def get_cache_key(from_number, to_number, channel, prefix=CHUNKS_CACHE_KEY):
    return build_cache_key_without_integration(prefix, f"{from_number}:{to_number}", channel)


# The customer-facing dividers persisted as `signal` chunks that survive a refresh.


ERROR_CONVERSATION_NUMBER_REQUIRED = "Conversation number required"
ERROR_CONVERSATION_NOT_FOUND = "Conversation not found"
ERROR_PARENT_CONVERSATION_NOT_FOUND = "Parent conversation not found"
ERROR_DESC_DISPO_BOTH_CANNOT_BE_EMPTY = "Description and disposition both cannot be empty"
ERROR_AOM_NOT_FOUND = "Agent not found for given sip_endpoint or aom_uuid"
ERROR_DISPO_CANNOT_BE_EMPTY_FOR_WRAP_UP = "Disposition cannot be empty for wrapup conversation"
ERROR_CANNOT_ADD_CHUNK_CONV_RESOLVED_STATE = "Error cannot add chunk for conversation in resolved state."
ERROR_ORG_NOT_FOUND = "Organization not found"
ERROR_CONVERSATIONS_NOT_FOUND = "No conversations found for case"
ERROR_WHILE_FETCHING_AGENT_DETAILS_FROM_CONTACT_UUID = "Error while fetching agent details from contact_uuid"
ERROR_WHILE_FILTERING_AGENTS_IN_ACW = "Error while filtering agents in ACW"
ERROR_AOM_ALREADY_A_PRIMARY_AOM_FOR_A_CONVERSATION = "Agent is already a primary aom for a conversation"
MSG_ADD_CONVERSATION_CHUNK_SUCCESS = "Conversation chunk added successfully"
CONVERSATION_STATE_TO_WRAP_UP_UPDATED_SUCCESSFULLY = "Conversation status update to in_wrap_up"
AOM_STATUS_FETCHED_SUCCESSFULLY = "Aom occupied details fetched successfully"
AGENT_DETAILS_FETCHED_SUCCESSFULLY_FROM_CONTACT_UUID = "Agent details fetched successfully from contact_uuid"


def create_hangup_conversation_chunk(chunk):
    conversation = Conversation.get_conversation_for_hangup_chunk(chunk.contact_uuid)
    if not conversation:
        logger.error(f"in create_hangup_conversation_chunk:{ERROR_CONVERSATION_NOT_FOUND}")
        return UseCaseResponse(status_code=404, error=ERROR_CONVERSATION_NOT_FOUND)
    if conversation.direction_inbound:
        from_user = conversation.parent_conversation.contact_identifier
        to_user = SYSTEM_USER_UUID
    else:
        from_user = SYSTEM_USER_UUID
        to_user = conversation.parent_conversation.contact_identifier
    if not chunk.from_user:
        chunk.from_user = from_user
    if not chunk.to_user:
        chunk.to_user = to_user
    chunk_to_create = chunk.build_db_model(conversation=conversation)
    create_chunk_with_related_data(chunk_to_create)
    return UseCaseResponse(status_code=201, msg=MSG_ADD_CONVERSATION_CHUNK_SUCCESS)


def create_chunk_with_related_data(chunk) -> bool:
    """Write the chunk (and its recording row) in a savepoint; a failure is logged and reported False, and does not
    abort the caller's transaction."""
    try:
        db = get_db_session()
        with db.begin_nested():
            if chunk.type == ConversationChunkTypes.recording.name and chunk.attributes and not chunk.recording:
                from hs.conversations.store import Recording
                from hs.voice.call_store import CallLeg

                conversation = Conversation.get_by_id(chunk.conversation) if chunk.conversation else None
                parent_call_leg = CallLeg.get_by_call_uuid(conversation.contact_uuid if conversation else None)
                chunk.recording = Recording.create_from_chunk(chunk, parent_call_leg.id if parent_call_leg else None).id
            ConversationChunk.create_chunk(chunk)
    except Exception as e:
        logger.error(
            f"in create_chunk_with_related_data, error: {e} (chunk type={getattr(chunk, 'type', None)} "
            f"conversation={getattr(chunk, 'conversation', None)} — best-effort write LOST)"
        )
        return False
    return True


ERROR_INVALID_STATUS_CHANGE = "Status change is not valid"


ERROR_CONV_NOT_IN_WRAP_UP = "Conversation is not in wrapup state"
ERROR_WRAPUP_TIMEOUT_INITIATED_BY_ANOTHER_REQUEST = "Wrapup timeout initiated by another request"
MSG_CONV_TIMEOUT_HANDLED = "Conversation timeout handled successfully"


ERROR_CONV_SESSION_TIMEOUT_INITIATED_BY_ANOTHER_REQUEST = "Conversation session timeout initiated by another request"
ERROR_CONV_SESSION_EXTENDED = "Conversation session timeout extended as call conversation is still open"
MSG_CONVERSATION_SESSION_TIMEOUT_PARENT_CLOSURE_FAILED = "Failed to close parent after child session timeout"


def handle_conversation_session_timeout(conversation_number: str, channel: str, payload) -> UseCaseResponse:
    from hs.admin.lookups import ApiLockManager
    from hs.conversations import timers
    from hs.infra.context import context_api_id, db_session_commit

    api_name = CONVERSATION_SESSION_EXTENDED_TIMEOUT_API if payload.extended else CONVERSATION_SESSION_TIMEOUT_API
    api_lock = ApiLockManager(
        api_name=api_name,
        unique_identifier=f"{context_aom_uuid.get()}:{conversation_number}:{channel}",
        api_run_id=context_api_id.get(),
        expiry_in_seconds=EXPIRY_FOR_CONVERSATION_SESSION_TIMEOUT_CONCURRENT_REQUEST_SEC,
    )
    if not api_lock.acquire_lock_for_api():
        # CAS loser: another request holds the timeout lock and will close the conversation.
        logger.info(
            f"in handle_conversation_session_timeout skipped : "
            f"{ERROR_CONV_SESSION_TIMEOUT_INITIATED_BY_ANOTHER_REQUEST}"
        )
        return UseCaseResponse(status_code=422, error=ERROR_CONV_SESSION_TIMEOUT_INITIATED_BY_ANOTHER_REQUEST)
    now = get_now()
    conversation = Conversation.get_primary_active_conversation(
        conversation_number, context_aom_uuid.get(), channel=channel, asc_order=True
    )
    if not conversation:
        # Expected stray: session-timeout timers cannot be cancelled, so one fires after every
        # normally-completed conversation and correctly finds nothing.
        logger.info(
            "in handle_conversation_session_timeout :"
            + f"{ERROR_CONVERSATION_NOT_FOUND}, conversation_number : "
            + f"{conversation_number}, channel: {channel}"
        )
        return UseCaseResponse(status_code=404, error=ERROR_CONVERSATION_NOT_FOUND)
    if conversation and conversation.state == ConversationStates.transferred.name:
        logger.error(
            "in handle_conversation_session_timeout error :"
            + (
                f"Conversation is transferred, wait for transferred child's session timeout conversation_number : "
                f"{conversation_number}, channel: {channel}"
            )
        )
        return UseCaseResponse(status_code=404, error="Conversation is transferred")
    if not payload.extended:
        call_conv = Conversation.get_child_by_conv_parent(
            parent_id=conversation.parent, channel=ConversationChannels.call.name, filter_active=True, filter_qnr=True
        )
        if call_conv:
            logger.info(
                f"in handle_conversation_session_timeout, extending conversation session timeout for "
                f"{channel}, conversation : {conversation_number}"
            )
            actor = (context_aom_uuid.get(), context_org_uuid.get(), get_context_org_name())
            timers.schedule_session_timeout(conversation_number, channel, *actor, extended=True)
            set_db_session_rollback()
            return UseCaseResponse(status_code=400, error=ERROR_CONV_SESSION_EXTENDED)
    if conversation.status in OPEN_STATUSES:
        logger.info(
            f"in handle_conversation_session_timeout, auto-closing conversation of channel: {channel} "
            f"conversation_number : {conversation_number}, conversation id: {conversation.id}"
        )
        Conversation.update_conversation(
            conversation.id, update_dict={Conversation.status.name: ConversationStatus.session_resolved.name}, now=now
        )
        db_session_commit()
    return handle_session_timeout_auto_closure(conversation)


def handle_session_timeout_auto_closure(conversation, is_flow_ended=False):
    from hs.conversations import child_update as ccu
    from hs.conversations.models import ConvChunkInsertModel, UpdateParentByConversationNumber

    conversation_number = conversation.parent_conversation.conversation_number
    response = ccu.update_parent_conversation_by_conversation_number(
        update=UpdateParentByConversationNumber(
            conversation_number=conversation_number, state=ConversationStatus.closed.name, is_flow_ended=is_flow_ended
        )
    )
    if response.error:
        logger.info(
            f"in handle_conversation_session_timeout, error response when tried to close parent "
            f"withconversation_number: {conversation_number}, error: {response.error}"
        )
        return UseCaseResponse(status_code=422, error=MSG_CONVERSATION_SESSION_TIMEOUT_PARENT_CLOSURE_FAILED)
    logger.info(
        f"in handle_conversation_session_timeout, creating conversation chunk for {conversation.channel} "
        f"conversation : {conversation_number}"
    )
    chunk_insert = ConvChunkInsertModel(
        type=ConversationChunkTypes.notes.name,
        from_user=SYSTEM_USER_UUID,
        to_user=conversation.parent_conversation.contact_identifier,
        disposition=DISPOSITION_FOR_AUTO_CLOSED,
        channel=conversation.channel,
    )
    chunk = chunk_insert.build_db_model(conversation=conversation)
    create_chunk_with_related_data(chunk)
    logger.info(f"in handle_conversation_session_timeout, successfully closed the conversation: {conversation_number}")
    return UseCaseResponse(status_code=200, msg=MSG_CONV_TIMEOUT_HANDLED)


def handle_conversation_wrapup_timeout(conversation_number: str) -> UseCaseResponse:
    from hs.admin.lookups import ApiLockManager
    from hs.conversations.models import ConvChunkInsertModel, TimeoutTypes
    from hs.conversations.outcomes import system_disposition
    from hs.infra.context import context_api_id

    # per agent: a transferred call has one wrap-up timer per agent, and they can fire together
    if not ApiLockManager(
        api_name=WRAPUP_TIMEOUT_API,
        unique_identifier=f"{conversation_number}:{context_aom_uuid.get()}",
        api_run_id=context_api_id.get(),
        expiry_in_seconds=EXPIRY_FOR_WRAPUP_TIMEOUT_CONCURRENT_REQUEST_SEC,
    ).acquire_lock_for_api():
        return fail(ERROR_WRAPUP_TIMEOUT_INITIATED_BY_ANOTHER_REQUEST, 422)
    conversation = Conversation.get_primary_active_conversation(conversation_number, context_aom_uuid.get())
    where = f"conversation_number : {conversation_number}"
    if not conversation:
        # INFO: same expected-stray semantics as the session-timeout variant above.
        logger.info(f"in handle_conversation_timeout :{ERROR_CONVERSATION_NOT_FOUND} {where}")
        return UseCaseResponse(status_code=404, error=ERROR_CONVERSATION_NOT_FOUND)
    if conversation.status != ConversationStatus.in_wrap_up.name:
        log = (
            f"in handle_conversation_timeout error {ERROR_CONV_NOT_IN_WRAP_UP} current conv status : "
            f"{conversation.status} {where}"
        )
        return fail(ERROR_CONV_NOT_IN_WRAP_UP, 404, log=log)
    closed = ConversationStatus.closed.name
    is_valid, suggested = check_status_move(conversation, closed)
    if not is_valid:
        log = (
            f"in handle_conversation_wrapup_timeout error : {ERROR_INVALID_STATUS_CHANGE} for conversation: "
            f"{conversation.id} suggested status change: {suggested}"
        )
        data = f"suggested status change : {suggested}" if suggested else None
        return fail(ERROR_INVALID_STATUS_CHANGE, 422, data, log=log, rollback=True)
    chunk = ConvChunkInsertModel(
        type=ConversationChunkTypes.notes.name,
        from_user=SYSTEM_USER_UUID,
        to_user=conversation.parent_conversation.contact_identifier,
        disposition=system_disposition(conversation.parent_conversation.id, "answered"),
    )
    auto_closed = chunk.build_db_model(conversation=conversation)
    create_chunk_with_related_data(auto_closed)
    _emit_wrapped_up(conversation, auto_closed, context_aom_uuid.get(), skipped=True)
    timeout = TimeoutTypes.wrap_up_timeout
    _close_wrapped_up(conversation, conversation_number, timeout_type=timeout, send_conversation_closed_event=True)
    return UseCaseResponse(status_code=200, msg=MSG_CONV_TIMEOUT_HANDLED)


def _close_wrapped_up(conversation, conversation_number, **post):
    """Close a wrapped-up child, then run the closed hand-off for each of the agent's active children."""
    from hs.conversations import child_update as ccu

    active_convs = Conversation.get_active_by_conv_number_for_aom(conversation_number, context_aom_uuid.get(), None)
    organization = conversation.parent_conversation.organization
    release = AgentConversationSetting.set_or_get_setting(organization, conversation_number)
    ccu.close_child(conversation, resolve_for_agent=True)
    for conv in active_convs:
        PostConversationStatusChange(
            conversation=conv, changed_status=ConversationStatus.closed.name, update_capacity_on_resolve=release, **post
        ).invoke()


ERROR_AGENT_CANNOT_BE_SET_FOR_INBOUND_CONVERSATION = "Agent sip-endpoint or uuid cannot be set for inbound conversation"
ERROR_AGENT_SIP_OR_UUID_CANNOT_BE_EMPTY = "Agent sip-endpoint or uuid cannot be empty for outbound conversation"
ERROR_AGENT_REQUIRED_FOR_OUTBOUND = "Agent is required for outbound conversation"
ERROR_AOM_NOT_IN_ORG = "Agent not in organization"
ERROR_PARENT_CONVERSATION_NOT_FOUND_FOR_TRANSFER = "Parent transaction not found for transfer conversation"
MSG_ADD_PARENT_CONVERSATION_SUCCESS = "Parent conversation added successfully"


def _create_parent_with_unique_number(conversation_parent_to_create, conv, org, contact_response, phone_or_email):
    from sqlalchemy.exc import IntegrityError

    from hs.infra.context import get_db_session

    last_err = None
    for attempt in range(5):
        try:
            with get_db_session().begin_nested():
                return ConversationParent.create_conversation(conversation_parent_to_create)
        except IntegrityError as e:
            last_err = e
            if "conversation_number" not in str(e.orig):
                raise
            logger.error(f"conversation_number collision (attempt {attempt + 1}/5) — regenerating")
            conversation_parent_to_create = conv.build_db_model(
                org=org, contact=contact_response, contact_identifier=phone_or_email
            )
    raise last_err


def create_parent_conversation(conv) -> UseCaseResponse:
    from hs.admin.lookups import fetch_aom_by_uuid_or_sip_and_set_context, fetch_org_by_auth_id_and_set_context
    from hs.conversations import child_update as ccu
    from hs.conversations import transfer as cxfer
    from hs.infra.context import get_db_session

    logger.info(f"in create_parent_conversation channel={conv.parent_channel}")
    now = get_now()
    aom = None
    is_http_request = conv.http_request_uuid is not None
    if not conv.direction_inbound and not conv.agent_sip_or_uuid and not conv.http_request_uuid:
        logger.error(f"in create_conversation error : {ERROR_AGENT_SIP_OR_UUID_CANNOT_BE_EMPTY}")
        return UseCaseResponse(status_code=400, error=ERROR_AGENT_SIP_OR_UUID_CANNOT_BE_EMPTY)
    if conv.direction_inbound and conv.agent_sip_or_uuid:
        logger.error(f"in create_conversation error : {ERROR_AGENT_CANNOT_BE_SET_FOR_INBOUND_CONVERSATION}")
        return UseCaseResponse(status_code=400, error=ERROR_AGENT_CANNOT_BE_SET_FOR_INBOUND_CONVERSATION)
    if conv.agent_sip_or_uuid:
        aom = fetch_aom_by_uuid_or_sip_and_set_context(conv.agent_sip_or_uuid)
    if not conv.direction_inbound and not aom and not is_http_request:
        return fail(ERROR_AGENT_REQUIRED_FOR_OUTBOUND)
    error, org = fetch_org_by_auth_id_and_set_context(conv.org_auth_id)
    if error:
        logger.error(f"in create_parent_conversation error from fetch_org_by_auth_id_and_set_context {error}")
        return UseCaseResponse(status_code=422, error=error)
    if aom and org.id != aom.organization:
        return fail(ERROR_AOM_NOT_IN_ORG)
    conversation_parent = None
    response_data = None
    if not conv.transfer_conversation:
        contact_response = None
        phone_or_email = get_number_in_e164_format(conv.phone_or_email)
        conversation_parent_to_create = conv.build_db_model(
            org=org, contact=contact_response, contact_identifier=phone_or_email
        )
        conversation_parent = _create_parent_with_unique_number(
            conversation_parent_to_create, conv, org, contact_response, phone_or_email
        )
        if conv.flow_run_uuid is not None:
            logger.info(
                f"in create_parent_conversation:: mapping conversation_number: "
                f"{conversation_parent.conversation_number}to flow_run_uuid: {conv.flow_run_uuid} in cache"
            )
            cache_key = build_cache_key_without_integration(
                identifier_name=CONVERSATION_IDENTIFIER,
                unique_id=conversation_parent.conversation_number,
                entity_name="flow_run_uuid",
            )
            Cache.get_instance().set(cache_key, conv.flow_run_uuid, EXPIRY_FOR_CONVERSATION_TO_FLOW_MAPPING_IN_SECONDS)
        customer_conversation_data_to_create = conv.build_customer_conversation_db_entity(conversation_parent.id)
        customer_conversation_data = CustomerConversationData.create_customer_conversation_data(
            customer_conversation_data_to_create
        )
        response_data = ConvParentAndCustomerConvDataRes.build_response(conversation_parent, customer_conversation_data)
        child_non_qnr_to_create = ConvInsertModel(
            generation_type=ConversationGenerationTypes.non_qnr_generated.name,
            state=ConversationStates.bot_answered.name,
            sender_id=conv.sender_id,
            parent_contact_uuid=conversation_parent.parent_contact_uuid,
            parent=conversation_parent,
            direction_inbound=conv.direction_inbound,
        )
        response_child = cxfer.create_child_conversation(child_non_qnr_to_create)
        response_data = response_child.data
        if response_child.error:
            logger.error(f"in create_parent_conversation error from create_child_conversation {response_child.error}")
            set_db_session_rollback()
            return UseCaseResponse(status_code=422, error=response_child.error)
        if not conv.direction_inbound and not is_http_request:
            child_outbound_to_create = ConvInsertModel(
                generation_type=ConversationGenerationTypes.outbound_generated.name,
                parent_contact_uuid=conversation_parent.parent_contact_uuid,
                parent=conversation_parent,
                start_at=now,
                direction_inbound=conv.direction_inbound,
                sender_id=conv.sender_id,
            )
            response_child = cxfer.create_child_conversation(child_outbound_to_create, aom=aom)
            if response_child.error:
                logger.error(
                    f"in create_parent_conversation error from create_child_conversation {response_child.error}"
                )
                set_db_session_rollback()
                return UseCaseResponse(status_code=422, error=response_child.error)
            response_data = response_child.data
    else:
        if conv.transfer_outbound:
            conversation_parent = ConversationParent.get_by_contact_uuid(conv.parent_contact_uuid)
            if not conversation_parent:
                conversation_child = Conversation.get_conversation_for_child_contact_uuid(conv.parent_contact_uuid)
                if conversation_child:
                    conversation_parent = conversation_child.parent_conversation
            child_contact_uuid = conv.child_contact_uuid
        else:
            existing_non_qnr_child = Conversation.get_active_non_qnr_child_by_contact_uuid(conv.parent_contact_uuid)
            child_contact_uuid = conv.parent_contact_uuid
            conversation_child = Conversation.get_conversation_for_child_contact_uuid(conv.parent_contact_uuid)
            logger.info(
                "in create_parent_conversation, fetched existing_non_qnr_child - "
                + f" {existing_non_qnr_child}, child_contact_uuid: {child_contact_uuid}, conversation"
                + f"_child: {conversation_child} for parent contact_uuid: {conv.parent_contact_uuid}"
            )
            if conversation_child:
                conversation_parent = conversation_child.parent_conversation
                child_contact_uuid = conversation_child.contact_uuid
                logger.info(
                    f"in create_parent_conversation, fetched conversation_child, updated child_contact_uuid - "
                    f"{child_contact_uuid}, conversation_parent - {conversation_parent}"
                )
                if not existing_non_qnr_child:
                    existing_non_qnr_child = Conversation.get_active_non_qnr_child_by_contact_uuid(
                        conversation_parent.parent_contact_uuid
                    )
            if not existing_non_qnr_child:
                return fail(ERROR_PARENT_CONVERSATION_NOT_FOUND_FOR_TRANSFER)
            conversation_parent = existing_non_qnr_child.parent_conversation
            ccu.close_conversation(existing_non_qnr_child, is_destination_transfer=True)
        sender_id = conv.sender_id
        if sender_id and not is_valid_phone_number(sender_id):
            sender_id = Conversation.get_last_used_number(
                conv.parent_channel,
                None,
                to_numbers=[conversation_parent.contact_identifier],
                conversation_number=conversation_parent.conversation_number,
                direction_inbound=not conv.transfer_outbound,
            )
            logger.info(f"in create_parent_conversation, fetched last user sender id - {sender_id}")
        child_transfer_non_qnr_to_create = ConvInsertModel(
            generation_type=ConversationGenerationTypes.transfer_non_qnr_generated.name,
            state=ConversationStates.bot_answered.name,
            sender_id=sender_id,
            parent_contact_uuid=conversation_parent.parent_contact_uuid,
            parent=conversation_parent,
            direction_inbound=conv.direction_inbound,
            contact_uuid=child_contact_uuid,
            channel=conv.parent_channel if conv.parent_channel else None,
        )
        response_child = cxfer.create_child_conversation(child_transfer_non_qnr_to_create, outbound_queue_transfer=True)
        response_data = response_child.data
        if response_child.error:
            logger.error(f"in create_parent_conversation error from create_child_conversation {response_child.error}")
            set_db_session_rollback()
            return UseCaseResponse(status_code=422, error=response_child.error)
    if conversation_parent:
        logger.info("in create_parent_conversation, get or set update agent capacity on resolve flag")
        AgentConversationSetting.set_or_get_setting(str(org.id), conversation_parent.conversation_number)
    logger.info(f"in create_parent_conversation response : {response_data}")
    get_db_session().commit()
    return UseCaseResponse(status_code=201, msg=MSG_ADD_PARENT_CONVERSATION_SUCCESS, data=response_data)


def get_parent_conversation_internal(
    conversation_number: str, has_supervisor_admin: bool = False, active: bool = False
) -> UseCaseResponse:
    if not conversation_number:
        return fail(ERROR_CONVERSATION_NUMBER_REQUIRED)
    conversation_parent = ConversationParent.get_parent_conversation_preloaded(conversation_number, active)
    if not conversation_parent:
        return UseCaseResponse(status_code=404, error=ERROR_CONVERSATION_NOT_FOUND)
    cp = conversation_parent[0]
    conv_variables = ConversationVariablesRes(
        conversation_number=cp.conversation_number,
        contact_identifier=cp.contact_identifier,
        conv_parent_contact_uuid=cp.contact_uuid,
    )
    conv_variables.conv_case_id = cp.case_id
    conv_variables.conv_contact_id = cp.crm_contact_id
    conv_variables.conv_created_date = str(get_utc(cp.created_at).timestamp())
    conversation_first_qnr_child = Conversation.get_first_non_bot_by_contact_uuid(cp.contact_uuid)
    if conversation_first_qnr_child:
        conv_variables.conv_duration = str(get_utc(conversation_first_qnr_child.start_at).timestamp())
    if cp.end_at:
        conv_variables.conv_duration = str(get_time_difference_in_seconds(cp.end_at, cp.created_at))
    agent_emails = set()
    channels = set()
    channel_to_contact_uuid_map = {}
    primary_and_secondary_aom = [
        str(aom.agent.uuid) for aom in cp.secondary_participants if (aom and aom.agent and aom.is_deleted is False)
    ]
    cp.child_convs = sorted(
        cp.child_convs, key=lambda d: d.start_at.replace(tzinfo=None) if d.start_at else datetime.datetime.now(tz=None)
    )
    # One batched lookup for every child's agent name/email, not one per child.
    child_primary_aom_uuids = [str(child.primary_agent.uuid) for child in cp.child_convs if child.primary_aom]
    agent_user_details = AccountOrgMapping.get_user_details_by_aom_uuids(
        child_primary_aom_uuids, organization_id=cp.organization
    )
    for child in cp.child_convs:
        channels.add(child.channel)
        channel_to_contact_uuid_map[child.channel] = child.contact_uuid
        conv_variables.direction_inbound = child.direction_inbound
        if (
            child.status in OPEN_STATUSES
            and child.channel == ConversationChannels.call.name
            and child.contact_uuid != cp.contact_uuid
        ):
            conv_variables.conv_parent_contact_uuid = child.contact_uuid
        if child.primary_aom:
            child_primary_aom_uuid = str(child.primary_agent.uuid)
            user_details = agent_user_details.get(child_primary_aom_uuid)
            email = user_details.email if user_details else None
            if email:
                agent_emails.add(email)
            primary_and_secondary_aom.append(child_primary_aom_uuid)
            if child.status in OPEN_STATUSES:
                agent_name = user_details.name if user_details else None
                conv_variables.conv_current_agent_name = agent_name.title() if agent_name else None
        for chunk in child.chunks:
            if chunk.type == ConversationChunkTypes.notes.name and chunk.disposition:
                conv_variables.conv_disposition = chunk.disposition
                conv_variables.conv_wrapup_note = chunk.description if chunk.description else None
        if child.direction_inbound:
            conv_variables.conv_source = child.customer_contact
            conv_variables.conv_destination = child.sender_id
        else:
            conv_variables.conv_source = child.sender_id
            conv_variables.conv_destination = child.customer_contact
    conv_variables.conv_channel_to_contact_uuid_map = channel_to_contact_uuid_map
    conv_variables.conv_contact_name = cp.contact_name
    conv_variables.conv_channels_list = list(channels)
    conv_variables.conv_agents_email = list(agent_emails)
    conv_variables.conv_participants_aom_uuids = primary_and_secondary_aom
    if has_supervisor_admin:
        role_list = AOMRoleProfile.get_role_names_by_aom_uuids([str(aom) for aom in primary_and_secondary_aom])
        conv_variables.has_supervisor_or_admin = any(role in [ROLE_ADMIN, ROLE_SUPERVISOR] for role in role_list)
    return UseCaseResponse(
        status_code=200, msg="Parent Conversation fetched Successfully", data=conv_variables.model_dump()
    )


def get_agent_details(contact_uuid):
    try:
        conversation_parent = ConversationParent.get_conversation_parent_for_child_contact_uuid(contact_uuid)
        if conversation_parent is None:
            log = "error in fetching conversation_parent from child contact_uuid: conversation_parent not found"
            return fail(ERROR_CONVERSATIONS_NOT_FOUND, None, log=log)
        conversation = Conversation.get_first_child_conversation_for_parent(conversation_parent.conversation_number)
        if conversation is None:
            log = (
                "error in fetching last active conversation along with account from conversation_number: "
                "conversation not found"
            )
            return fail(ERROR_CONVERSATIONS_NOT_FOUND, None, log=log)
        logger.info(f"last active conversation found for contact_uuid: {contact_uuid}")
        msg = AGENT_DETAILS_FETCHED_SUCCESSFULLY_FROM_CONTACT_UUID
        return UseCaseResponse(status_code=200, msg=msg, data=conversation.build_agent_details_response())
    except Exception:
        log = "exception in fetching last active conversation along with account from contact_uuid"
        return fail(ERROR_WHILE_FETCHING_AGENT_DETAILS_FROM_CONTACT_UUID, None, log=log)


def filter_active_agents_in_acw(agents: list[str], channel: str) -> UseCaseResponse:
    try:
        return UseCaseResponse(data=Conversation.filter_agents_in_acw(agents, channel))
    except Exception:
        log = "error in fetching active agents in acw"
        return fail(ERROR_WHILE_FILTERING_AGENTS_IN_ACW, None, log=log)


def get_active_child_for_contact_uuid(get_conv: ConvGetByContactUUID) -> UseCaseResponse:
    logger.info(f"in get_qnr_child_for_contact_uuid request : {get_conv}")
    conversation = Conversation.get_single_active_conversation_for_parent(get_conv.contact_uuid)
    if not conversation:
        error = ERROR_CONVERSATION_NOT_FOUND
        return fail(error, 404, log=f"in get_qnr_child_for_contact_uuid error : {error}")
    parent = conversation.parent_conversation
    release = AgentConversationSetting.set_or_get_setting(str(parent.organization), parent.conversation_number)
    data = conversation.build_res_model(update_capacity_on_resolve=release)
    return UseCaseResponse(status_code=200, data=data)


def get_aom_occupied_state(conv_channel: ConvChannel) -> UseCaseResponse:
    aom_uuid, org_uuid = context_aom_uuid.get(), context_org_uuid.get()
    org = Organization.get_by_uuid(str(org_uuid)) if org_uuid else None
    if org is None:
        return fail(ERROR_ORG_NOT_FOUND, 404, log=ERROR_ORG_NOT_FOUND)
    routing_setting = RoutingSettings.get_by_org_id(str(org.id))
    active = routing_setting is None or not routing_setting.release_capacity_on_resolve
    aom_state = AOMOccupiedState()
    if Conversation.get_active_conversations_for_primary_aom(aom_uuid, conv_channel, active):
        aom_state.is_aom_occupied = True
        msg = ERROR_AOM_ALREADY_A_PRIMARY_AOM_FOR_A_CONVERSATION
        return UseCaseResponse(status_code=200, msg=msg, data=aom_state.model_dump())
    states = ConversationParent.get_active_secondary_conversation_mappings_with_state(aom_uuid, conv_channel)
    # a barge, or any non-supervisor state, occupies the agent and wins over monitor/coach; a barge wins over all
    for value in (states or {}).values():
        busy = value == BARGE or value not in MONITOR_BARGE_COACH_STATE_LIST
        if not aom_state.secondary_agent_state or (busy and aom_state.secondary_agent_state != BARGE):
            aom_state.secondary_agent_state = value
        aom_state.is_aom_occupied = aom_state.is_aom_occupied or bool(value and busy)
    return UseCaseResponse(status_code=200, msg=AOM_STATUS_FETCHED_SUCCESSFULLY, data=aom_state.model_dump())


def create_conversation_chunk(conversation_number: str, chunk) -> UseCaseResponse:
    """The acting agent's note or wrap-up disposition on a live call; the last agent's disposition closes it."""
    if not (sanitize_string_input(chunk.disposition) or sanitize_string_input(chunk.description)):
        return fail(ERROR_DESC_DISPO_BOTH_CANNOT_BE_EMPTY)
    aom_uuid = context_aom_uuid.get()
    if not aom_uuid:
        if not (primary := Conversation.get_primary_active_conversation(conversation_number)):
            return fail(ERROR_AOM_NOT_FOUND)
        aom_uuid = primary.primary_agent.uuid
    aom = AccountOrgMapping.get_by_uuid(aom_uuid, True)
    chunk.from_user = chunk.from_user or str(aom_uuid)
    chunk.is_inbound_chunk = not is_valid_aom_user_for_chunk(chunk.from_user)
    conversation = Conversation.get_primary_active_conversation(
        conversation_number, aom_uuid
    ) or ConversationParent.get_secondary_active_conversation(conversation_number, aom_uuid)
    if not conversation:
        return fail(ERROR_CONVERSATION_NOT_FOUND, 404)
    if chunk.description and not chunk.disposition and conversation.status in RESOLVE_STATUSES:
        return fail(ERROR_CANNOT_ADD_CHUNK_CONV_RESOLVED_STATE, 422)
    if not chunk.to_user and not chunk.is_inbound_chunk:
        chunk.to_user = conversation.parent_conversation.contact_identifier
    row = chunk.build_db_model(conversation=conversation)
    create_chunk_with_related_data(row)
    if (
        conversation.is_primary_agent(context_aom_uuid.get())
        and conversation.status == ConversationStatus.in_wrap_up.name
    ):
        if not sanitize_string_input(row.disposition):
            return fail(ERROR_DISPO_CANNOT_BE_EMPTY_FOR_WRAP_UP, rollback=True)
        is_valid, suggested = check_status_move(conversation, ConversationStatus.closed.name)
        if not is_valid:
            data = f"suggested status change : {suggested}" if suggested else None
            return fail(ERROR_INVALID_STATUS_CHANGE, 422, data, rollback=True)
        _emit_wrapped_up(conversation, row, aom.uuid)
        _close_wrapped_up(conversation, conversation_number, aom=aom)
    else:
        ConversationParent.update_last_activity_at(conversation_id=conversation.parent_conversation.id)
    return UseCaseResponse(status_code=201, msg=MSG_ADD_CONVERSATION_CHUNK_SUCCESS, data=row)


_NEXT_STATUS = {
    ConversationStatus.open.name: [
        ConversationStatus.session_resolved.name,
        ConversationStatus.in_wrap_up.name,
        ConversationStatus.closed.name,
    ],
    ConversationStatus.session_resolved.name: [ConversationStatus.in_wrap_up.name, ConversationStatus.closed.name],
    ConversationStatus.in_wrap_up.name: [ConversationStatus.closed.name],
    ConversationStatus.closed.name: [],
}


def check_status_move(conversation, to: str) -> tuple[bool, str | None]:
    """(allowed, the status to suggest instead)."""
    allowed = _NEXT_STATUS[conversation.status]
    return (True, "") if to in allowed else (False, allowed[0] if allowed else None)


class PostConversationStatusChange:
    """The effects of a child entering a status; the caller names the acting agent and org, else the request's."""

    def __init__(
        self,
        *,
        aom_uuid=None,
        org_name=None,
        org_uuid=None,
        conversation,
        changed_status,
        aom=None,
        timeout_type=None,
        send_conversation_closed_event=False,
        update_capacity_on_resolve=False,
    ):
        self.aom_uuid = context_aom_uuid.get() if aom_uuid is None else aom_uuid
        self.aom = aom if aom else AccountOrgMapping.get_by_uuid(self.aom_uuid)
        self.org_name = get_context_org_name() if org_name is None else org_name
        self.org_uuid = context_org_uuid.get() if org_uuid is None else org_uuid
        self.conversation = conversation
        self.changed_status = changed_status
        self.timeout_type = timeout_type
        self.send_conversation_closed_event = send_conversation_closed_event
        self.update_capacity_on_resolve = update_capacity_on_resolve

    def invoke(self):
        get_db_session().commit()
        handler = {
            ConversationStatus.open.name: self._open,
            ConversationStatus.in_wrap_up.name: self._in_wrap_up,
            ConversationStatus.closed.name: self._closed,
        }.get(self.changed_status)
        if handler:
            handler()

    def _open(self):
        from hs.conversations import timers

        if self.conversation.channel in CHANNELS_FOR_SESSION_TIMEOUT:
            actor = (self.aom_uuid, self.org_uuid, self.org_name)
            timers.schedule_session_timeout(
                self.conversation.parent_conversation.conversation_number, self.conversation.channel, *actor
            )

    def _in_wrap_up(self):
        from hs.conversations import timers

        timers.start_wrap_up(self.conversation, self.aom_uuid, self.org_uuid, self.org_name)

    def _closed(self):
        child, parent = self.conversation, self.conversation.parent_conversation
        keep_capacity = self.update_capacity_on_resolve and child.status not in OPEN_STATUSES
        wrapup_data = {
            "agent_sip_endpoint": self.aom.sip_endpoint,
            "phone_or_email": child.customer_contact or parent.contact_identifier,
            "channel": child.channel,
            "contact_uuid": child.contact_uuid,
            "parent_contact_uuid": parent.parent_contact_uuid,
            "agent_uuid": str(self.aom.uuid),
            "direction": "inbound" if parent.source_direction_inbound else "outbound",
            "timeout_type": self.timeout_type.value if self.timeout_type else "",
            "send_conversation_closed_event": self.send_conversation_closed_event,
            "sender_id": child.sender_id or "",
            "release_agent_capacity": not keep_capacity,
            "conversation_number": parent.conversation_number,
        }
        self.run_wrap_up_conversation_inprocess(wrapup_data)
        # the delayed capacity-release safety net
        jobs.schedule("capacity_clearance", wrapup_data, delay=5)

    @staticmethod
    def run_wrap_up_conversation_inprocess(wrapup_data: dict) -> None:
        from hs.conversations import wrap_up

        try:
            err = wrap_up.handle_wrap_up_voice(WrapUpConversationParams.model_validate(wrapup_data))
            if err is not None:
                from hs.routing.reverse import _is_known_routing_outcome

                # an expected empty-queue outcome surfaces through this relay too: not an error
                log = logger.info if _is_known_routing_outcome(err) else logger.error
                log(msg=f"in-process wrap_up_conversation returned error (ignored): {err}")
        except Exception:
            logger.error("in-process wrap_up_conversation raised (ignored)", exc_info=True)


ERROR_CALL_UUID_OR_CALL_LEG_UUID_REQUIRED = "Call UUID or Call Leg UUID is required"
ERROR_CALL_LEG_NOT_FOUND = "Call Leg not found"
ERROR_RECORDING_CREATION_FAILED = "Recording Creation Failed"
SUCCESS_RECORDING_CREATION = "Recording Created Successfully"
ERROR_RECORDING_DUPLICATE = "Recording already exists"


def create_recording(call_recording_request) -> UseCaseResponse:
    from hs.conversations.store import Recording
    from hs.voice.call_store import CallLeg

    if not call_recording_request.call_uuid and not call_recording_request.call_leg_uuid:
        logger.error("in create_recording error :  call_uuid or call_leg_uuid is required")
        return UseCaseResponse(status_code=400, error=ERROR_CALL_UUID_OR_CALL_LEG_UUID_REQUIRED)
    is_participant_recording = False
    if call_recording_request.recording_type == CallRecordingType.PARTICIPANT.value:
        is_participant_recording = True
    call_leg = CallLeg.get_by_call_uuid(call_recording_request.call_leg_uuid, participant_leg=is_participant_recording)
    if not call_leg:
        logger.error(
            "in create_recording error : call leg not found"
            + f" call_uuid : {call_recording_request.call_uuid} call_leg_uuid : {call_recording_request.call_leg_uuid}"
        )
        return UseCaseResponse(status_code=422, error=ERROR_CALL_LEG_NOT_FOUND)
    try:
        existing_recording = Recording.get_by_call_leg_id(call_leg.id)
        if existing_recording:
            logger.info(
                "create_recording: Recording already exists for "
                + f"call_leg_uuid: {call_recording_request.call_leg_uuid}"
            )
            return UseCaseResponse(status_code=422, error=ERROR_RECORDING_DUPLICATE)
        recording = call_recording_request.build_db_model(call_leg.id)
        Recording.create_recording(recording)
    except Exception as e:
        logger.error(
            "in create_recording error : recording creation failed"
            + f" call_uuid : {call_recording_request.call_uuid} call_leg_uuid : {call_recording_request.call_leg_uuid}"
            + f" error : {e}"
        )
        return UseCaseResponse(status_code=400, error=ERROR_RECORDING_CREATION_FAILED)
    return UseCaseResponse(status_code=200, msg=SUCCESS_RECORDING_CREATION)
