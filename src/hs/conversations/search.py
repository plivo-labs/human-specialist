"""Build the denormalized closed-conversation search row.

Derived analytics (durations, first-response time, CSAT, disposition/state, per-chunk document,
channel/sender/contact aggregation, agent_details) written as one `ClosedConversationSearch` row,
computed once at conversation-close (see hs/conversations/child_update.py), so the cost (incl. the
agents' teams and skills read) is paid once, not per list query. Per-agent `teams` and `skills` are
the agent's own `aom_team` / `aom_skills` rows at close.
"""

from sqlalchemy import bindparam, func, text

from hs.admin.account import AccountOrgMapping
from hs.admin.dispositions import Dispositions
from hs.conversations.models import CONVERSATION_ANSWERED_STATES, ConversationChunkTypes, FeedbackChunkAttributes
from hs.conversations.search_store import FULLTEXT_SOURCE_FIELDS, ClosedConversationSearch
from hs.conversations.store import AGENT_SPECIFIC_CHUNK_TYPES, Conversation, ConversationParent
from hs.infra.constants import ANONYMOUS_NUMBER, DISPOSITION_FOR_AUTO_CLOSED, EMPTY_TO_USER, SYSTEM_USER_UUID
from hs.infra.context import get_db_session
from hs.infra.helpers import (
    get_time_difference_in_seconds,
    get_utc,
    is_anonymous_identifier,
    is_valid_contact_identifier_for_chunk,
)
from hs.infra.log import logger

# The default conversation state when no closing disposition sets one.
DISPOSITION_FOR_CLOSED_STATE = "Customer answered"

# Each agent's teams and skills, keyed by account_org_mapping id.
_AGENT_TEAMS_SKILLS = text(
    "SELECT a.id::text, a.uuid::text, coalesce((SELECT jsonb_agg(jsonb_build_object('team_id', t.uuid::text,"
    " 'team_name', t.name)) FROM aom_team at JOIN team t ON t.id = at.team WHERE at.aom = a.id"
    " AND at.is_deleted IS NOT TRUE AND t.is_deleted IS NOT TRUE), '[]'),"
    " coalesce((SELECT jsonb_agg(jsonb_build_object('skill_id', s.uuid::text, 'skill_name', s.name,"
    " 'skill_level', k.skill_level)) FROM aom_skills k JOIN skills s ON s.id = k.skill WHERE k.aom = a.id"
    " AND k.is_deleted IS NOT TRUE AND s.is_deleted IS NOT TRUE), '[]')"
    " FROM account_org_mapping a WHERE a.id::text IN :aoms"
).bindparams(bindparam("aoms", expanding=True))


def _agent_teams_skills(aoms: list[str]) -> dict:
    rows = get_db_session().execute(_AGENT_TEAMS_SKILLS, {"aoms": aoms})
    return {"data": {aom: {"agent_id": uuid, "teams": teams, "skills": skills} for aom, uuid, teams, skills in rows}}


# The seeded name of the "customer didn't respond" system disposition.
DISPOSITION_FOR_UNANSWERED = "Customer didn't respond"


def _get_account_attr_for_chunk_user(aom_uuid, memory_for_account, attr="name"):
    """Resolve a chunk from/to/agent value to an account attribute (name/email), memoised; a
    contact-identifier value (phone/email) is returned verbatim."""
    if aom_uuid in memory_for_account:
        account = memory_for_account[aom_uuid]
        return getattr(account, attr, None) if account else None
    if aom_uuid == EMPTY_TO_USER:
        return None
    if is_valid_contact_identifier_for_chunk(aom_uuid):
        return aom_uuid
    account = AccountOrgMapping.get_account_by_aom_uuid(aom_uuid)
    memory_for_account[aom_uuid] = account
    return getattr(account, attr, None) if account else None


def build_row(conversation_parent, org_uuid=None, agent_team_skills=None, internal_dispositions=None):
    """Build (not persist) a ClosedConversationSearch row for a closed conversation_parent."""
    conv = conversation_parent
    if org_uuid is None and getattr(conv, "org", None) is not None:
        org_uuid = str(conv.org.uuid)
    row = ClosedConversationSearch()

    row.conv_number = conv.conversation_number
    row.organization = conv.organization
    row.org_uuid = str(org_uuid) if org_uuid else None
    if is_anonymous_identifier(str(conv.contact_identifier)):
        row.contact_identifier = ANONYMOUS_NUMBER
    else:
        row.contact_identifier = conv.contact_identifier
    row.case_id = conv.case_id
    row.crm_contact_id = conv.crm_contact_id
    row.parent_channel = conv.channel
    row.direction_inbound = conv.direction_inbound
    row.customer_name = conv.contact_name

    conv_created_at_utc = get_utc(conv.created_at)
    row.conv_created_at = conv_created_at_utc  # real ts (sort/range)
    row.conv_created_at_str = conv_created_at_utc.isoformat()  # doc field
    row.conv_created_date = conv_created_at_utc.strftime("%Y-%m-%d")
    row.conv_created_time = conv_created_at_utc.strftime("%H:%M:%S.%f%z")
    row.conv_total_duration = get_time_difference_in_seconds(conv.end_at, conv_created_at_utc) if conv.end_at else 0
    row.conv_updated_at_ts = get_utc(conv.updated_at) if conv.updated_at else None
    row.conv_updated_at = row.conv_updated_at_ts.isoformat() if row.conv_updated_at_ts else None

    if conv.customer_conversation_data:
        ccd = conv.customer_conversation_data
        row.customer_data = getattr(ccd, "customer_data", None)
        row.custom_data = getattr(ccd, "conversation_custom_data", None)
        row.customer_custom_data = getattr(ccd, "customer_custom_data", None)
        row.external_identifer = getattr(ccd, "external_identifier", None)
        row.client_data = getattr(ccd, "client_data", None)

    # accumulators
    row.conv_message_count = 0
    row.conv_wrapup_duration = 0
    row.agent_customer_conv_duration = 0
    row.conv_response_time = 0
    conv_states, conv_channels, sender_ids, customer_contacts = set(), set(), set(), set()
    conv_chunks, chunk_descriptions, chunk_messagetypes = [], set(), set()
    agent_ids, team_ids = [], set()

    # exclude soft-deleted children (other closed-conv paths filter is_deleted too — avoids
    # persisting deleted conversations into the snapshot)
    sorted_convs = sorted(
        [c for c in conv.child_convs if not getattr(c, "is_deleted", False)], key=lambda c: c.created_at
    )
    list_of_aoms = []
    for child in sorted_convs:
        if child.primary_aom and str(child.primary_aom) not in list_of_aoms:
            list_of_aoms.append(str(child.primary_aom))

    agent_mapping = {SYSTEM_USER_UUID: "bot"}
    primary_csat_time = None
    conv_start_at = None
    sync_first_response_at = None
    async_first_response_at = None
    message_delivered = False
    active_queue_entry_id = None
    if not internal_dispositions:
        disposition_list = [DISPOSITION_FOR_AUTO_CLOSED, DISPOSITION_FOR_UNANSWERED, DISPOSITION_FOR_CLOSED_STATE]
        internal_dispositions = Dispositions.get_by_disposition_list(org_uuid, disposition_list) or []
    disposition_dict = {d.name: d for d in internal_dispositions}
    auto_close_disposition = disposition_dict.get(DISPOSITION_FOR_AUTO_CLOSED)
    unanswered_disposition = disposition_dict.get(DISPOSITION_FOR_UNANSWERED)
    answered_disposition = disposition_dict.get(DISPOSITION_FOR_CLOSED_STATE)
    disposition_chunk = None

    for child in sorted_convs:
        conv_states.add(child.state)
        conv_channels.add(child.channel)
        if child.primary_agent:
            aid = str(child.primary_agent.uuid)
            if aid not in agent_ids:
                agent_ids.append(aid)
        if child.sender_id:
            # index every channel's raw sender_id for search, not only phone numbers.
            # the row stores the value verbatim (no normalization), so phone-format validation is
            # not needed here — gating on it dropped all non-phone senders and logged noisy errors.
            sender_ids.add(child.sender_id)
        customer_contacts.add(child.customer_contact)

        if child.start_at and child.end_at and child.primary_aom:
            if conv_start_at is None:
                conv_start_at = child.start_at
            if child.wrap_up_start_at:
                row.conv_wrapup_duration += (
                    child.end_at.replace(microsecond=0) - child.wrap_up_start_at.replace(microsecond=0)
                ).total_seconds()
                resolve_time = child.wrap_up_start_at
                if child.contact_creation_start_at:
                    resolve_time = min(child.contact_creation_start_at, resolve_time)
                if child.case_creation_start_at:
                    resolve_time = min(child.case_creation_start_at, resolve_time)
                row.agent_customer_conv_duration = (
                    resolve_time.replace(microsecond=0) - conv_start_at.replace(microsecond=0)
                ).total_seconds()
                if (
                    child.channel == "call"
                    and not child.direction_inbound
                    and child.state not in ["outbound_contact_answered", "transferred"]
                ):
                    row.agent_customer_conv_duration = 0
            if (
                child.channel == "call"
                and sync_first_response_at is None
                and (
                    child.direction_inbound
                    or (not child.direction_inbound and child.state in ["outbound_contact_answered", "transferred"])
                )
            ):
                sync_first_response_at = child.start_at

        current_agent_uuid = None
        if child.primary_aom and child.primary_agent:
            current_agent_uuid = str(child.primary_agent.uuid)
            agent_mapping[current_agent_uuid] = AccountOrgMapping.get_account_by_aom_uuid(current_agent_uuid)
        if (
            active_queue_entry_id is None
            and child.generation_type == "qnr_generated"
            and child.primary_aom
            and child.active_queue_entry_id
        ):
            active_queue_entry_id = child.active_queue_entry_id

        # ordered by message time for a deterministic final wrap-up/disposition; skip soft-deleted
        for chunk in sorted(child.chunks, key=lambda ch: ch.timestamp or ch.created_at):
            if getattr(chunk, "is_deleted", False):
                continue
            if chunk.channel_meta and chunk.channel_meta.get("status") == "failed":
                continue
            chunk_dict = {
                "type": chunk.type,
                "disposition": chunk.disposition,
                "sub_disposition": chunk.sub_disposition,
                "description": chunk.description,
                "attributes": chunk.attributes,
                "timestamp": chunk.timestamp.strftime("%Y-%m-%d %H:%M:%S") if chunk.timestamp else None,
                "channel": chunk.channel,
                "conversation_id": conv.conversation_number,
                "message_id": chunk.message_id,
                "from": _get_account_attr_for_chunk_user(chunk.from_user, agent_mapping, "email"),
                "to": _get_account_attr_for_chunk_user(chunk.to_user, agent_mapping, "email"),
                "agent_name": None,
                "agent_email": None,
                "agent_uuid": None,
                "channel_meta": chunk.channel_meta,
                "chunk_uuid": str(chunk.uuid),
            }
            if current_agent_uuid:
                chunk_dict["agent_name"] = _get_account_attr_for_chunk_user(current_agent_uuid, agent_mapping, "name")
                chunk_dict["agent_email"] = _get_account_attr_for_chunk_user(current_agent_uuid, agent_mapping, "email")
                chunk_dict["agent_uuid"] = current_agent_uuid
            conv_chunks.append(chunk_dict)
            if chunk.type != ConversationChunkTypes.signal.name:
                chunk_messagetypes.add(chunk.type)
                if chunk.description:
                    chunk_descriptions.add(chunk.description)
            if chunk.type == ConversationChunkTypes.notes.name and chunk.disposition:
                disposition_chunk = chunk
                row.wrap_up_note = chunk.description if chunk.description else None
            if chunk.type not in AGENT_SPECIFIC_CHUNK_TYPES:
                row.conv_message_count += 1
            if chunk.type == ConversationChunkTypes.customer_feedback.name and chunk.attributes:
                attrs = FeedbackChunkAttributes.model_validate(chunk.attributes[0])
                if attrs.feedback_type == "CSAT":
                    if not primary_csat_time:
                        primary_csat_time = chunk.timestamp
                        row.csat_feedback = chunk.description if chunk.description else "No Response"
                    elif chunk.description:
                        if (
                            row.csat_feedback != "No Response" and primary_csat_time < chunk.timestamp
                        ) or row.csat_feedback == "No Response":
                            row.csat_feedback = chunk.description
                            primary_csat_time = chunk.timestamp
            if chunk.message_id and (async_first_response_at is None or async_first_response_at > chunk.created_at):
                if not message_delivered:
                    message_delivered = (chunk.channel_meta or {}).get("status", "") in ["delivered", "sent", "read"]
                if (child.direction_inbound and chunk.to_user == conv.contact_identifier) or (
                    not child.direction_inbound and chunk.from_user == conv.contact_identifier
                ):
                    async_first_response_at = chunk.timestamp
            if (
                chunk.type == ConversationChunkTypes.hangup.name
                and chunk.attributes
                and isinstance(chunk.attributes, list)
                and len(chunk.attributes) > 0
            ):
                row.hangup_source = chunk.attributes[0].get("hangup_source", None)

    row.is_conv_answered = any(state in CONVERSATION_ANSWERED_STATES for state in conv_states)

    conv_dispo_state = None
    if disposition_chunk:
        row.disposition = disposition_chunk.disposition
        if disposition_chunk.disposition_id:
            if disposition_chunk.dispo and disposition_chunk.dispo.answered:
                disposition = disposition_chunk.dispo
                conv_dispo_state = answered_disposition
                if row.disposition == DISPOSITION_FOR_AUTO_CLOSED:
                    disposition = auto_close_disposition
                if not row.is_conv_answered:
                    conv_dispo_state = unanswered_disposition
            else:
                disposition = None
                conv_dispo_state = disposition_chunk.dispo
            row.disposition = disposition.name if disposition else None
            row.disposition_uuid = str(disposition.uuid) if disposition else None
            row.conv_state = conv_dispo_state.name if conv_dispo_state else None
            row.conv_state_uuid = str(conv_dispo_state.uuid) if conv_dispo_state else None
            if disposition_chunk.sub_disposition_id and disposition_chunk.sub_dispo:
                row.sub_disposition_uuid = str(disposition_chunk.sub_dispo.uuid)
                row.sub_disposition = disposition_chunk.sub_dispo.name

    if sync_first_response_at and async_first_response_at:
        row.conv_response_time = (
            min(sync_first_response_at, async_first_response_at).replace(microsecond=0)
            - conv.created_at.replace(microsecond=0)
        ).total_seconds()
    elif sync_first_response_at:
        row.conv_response_time = (
            sync_first_response_at.replace(microsecond=0) - conv.created_at.replace(microsecond=0)
        ).total_seconds()
    elif async_first_response_at:
        row.conv_response_time = (
            async_first_response_at.replace(microsecond=0) - conv.created_at.replace(microsecond=0)
        ).total_seconds()

    if not message_delivered and not row.direction_inbound and row.parent_channel == "message":
        row.agent_customer_conv_duration = 0

    # finalize collections (sets -> lists)
    row.conv_states = list(conv_states)
    row.conv_channels = list(conv_channels)
    row.conv_child_sender_ids = list(sender_ids)
    row.conv_child_customer_contacts = [c for c in customer_contacts if c is not None]
    row.conv_chunks = conv_chunks
    row.conv_chunk_descriptions = list(chunk_descriptions)
    row.conv_chunk_message_types = list(chunk_messagetypes)

    agent_details = []
    if list_of_aoms:
        agent_team_skills = agent_team_skills or _agent_teams_skills(list_of_aoms)
        data = agent_team_skills.get("data", {}) if isinstance(agent_team_skills, dict) else {}
        agent_details = [data.get(aid) for aid in list_of_aoms]
        for agent in agent_details:
            if agent and agent.get("teams"):
                for team in agent["teams"]:
                    tid = team.get("team_id") if isinstance(team, dict) else None
                    if tid:
                        team_ids.add(str(tid))
    row.agent_details = agent_details
    row.agent_ids = agent_ids
    row.team_ids = list(team_ids)

    # full-text search vector over the record's text fields
    row.search_vector = func.to_tsvector("simple", _search_text(row))
    return row


def _search_text(row) -> str:
    parts = []
    for field in FULLTEXT_SOURCE_FIELDS:
        val = getattr(row, field, None)
        if val is None:
            continue
        if isinstance(val, (list, tuple, set)):
            parts.extend(str(v) for v in val if v is not None)
        else:
            parts.append(str(val))
    return " ".join(parts)


def _load_closed_parent(conversation_number):
    """Eager-load the closed conversation_parent ORM graph the builder needs."""
    from sqlalchemy.orm import joinedload

    db = get_db_session()
    return (
        db.query(ConversationParent)
        .filter(
            ConversationParent.conversation_number == conversation_number,
            ConversationParent.is_deleted.is_(False),
            ConversationParent.end_at.is_not(None),
        )
        .options(
            joinedload(ConversationParent.child_convs).joinedload(Conversation.chunks),
            joinedload(ConversationParent.child_convs).joinedload(Conversation.primary_agent),
            joinedload(ConversationParent.customer_conversation_data),
            joinedload(ConversationParent.org),
        )
        .first()
    )


def upsert_for_conversation(conversation_number, parent=None) -> bool:
    """Build + upsert the search row for a closed conversation. Returns True if a row was
    written, False if the conversation isn't closed/loadable."""
    if parent is None:
        parent = _load_closed_parent(conversation_number)
    if not parent:
        logger.info(f"[closed-conv-search] {conversation_number} not closed/loadable — skipping upsert")
        return False
    row = build_row(parent)
    db = get_db_session()
    # SAVEPOINT-isolate the write: a failed delete/add flush rolls back only this nested block,
    # leaving the surrounding session usable (so a failed snapshot can never break the close
    # flow or abort the rest of a backfill loop — preserving the best-effort guarantee).
    with db.begin_nested():
        db.query(ClosedConversationSearch).filter(ClosedConversationSearch.conv_number == conversation_number).delete(
            synchronize_session=False
        )
        db.add(row)
    return True


def safe_upsert(conversation_number) -> bool:
    """Best-effort upsert — NEVER raises (the search store must not break conversation close)."""
    try:
        return upsert_for_conversation(conversation_number)
    except Exception as e:
        logger.error(f"[closed-conv-search] safe_upsert failed for {conversation_number}: {e}")
        return False


# ---- read side: query the search store ----


# nested field aliases -> the extracted JSONB-array columns
