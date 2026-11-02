from typing import Optional

"""Offline golden tests for the conversation read models (``hs/conversations/models.py``), without a
live DB or Redis. A model's fields are emitted by walking ``reversed(cls.__mro__)``, so the JSON key
order is a property of the class hierarchy (not of the input dict); these tests pin it.

Facts pinned here:

- ``ConversationChunkRes``: ``attributes`` FIRST, ``type``
  SECOND, … final keys ``from_user_name, to_user_name, contact_uuid``. The wire
  names for the ``from_user``/``to_user`` fields are the aliases ``from``/``to``.
- ``ConversationRes`` / ``ConversationParentChildModel``:
  PARENT fields FIRST, then child (``ConvModelRes``) fields, then
  ``run_reverse_routing, update_capacity_on_resolve``.
- ``unviewed_count`` is NOT coerced: cache MISS → JSON number ``0``; cache HIT
  (a raw Redis string assigned post-construction) → JSON string ``"3"``
  (``validate_assignment`` is off).
- ``parent_start_at`` placeholder = ``datetime.max`` @ UTC renders
  ``'9999-12-31T23:59:59.999999+00:00'`` with a **T** separator via
  ``jsonable_encoder`` (the space form is only the Redis ``lv`` value).
- ``ConversationVariablesRes``: single-inheritance declaration order;
  ``conv_created_date``/``conv_duration`` are epoch-float-string fields
  (``Optional[str]``).
- the agent's aggregated chunk = ``ConversationChunkRes`` order + ``hide_conversation``
  at the tail.
- the chunk list and chunk view payload key order.
- enums / constants: ``ConversationStates``, generation types, ``OPEN_STATUSES``,
  ``QNR_CHILDREN``, ``DEFAULT_GEN_TYPE_STATE_MAPPING``.

``AOMOccupiedState`` lives in ``hs/admin/account_models.py``; it is imported lazily so this
module collects without it.
"""

import datetime
import json

import pytest
from fastapi.encoders import jsonable_encoder

from hs.conversations.models import (
    DEFAULT_GEN_TYPE_STATE_MAPPING,
    NON_QNR_CHILDREN,
    OPEN_STATUSES,
    QNR_CHILDREN,
    AgentDetailsRes,
    ConversationChunkRes,
    ConversationGenerationTypes,
    ConversationParentChildModel,
    ConversationRes,
    ConversationStates,
    ConversationStatus,
    ConversationVariablesRes,
)

# ======================================================================================
# Golden key orders
# ======================================================================================

# ConversationChunkRes JSON key order. NOTE the wire names are the
# `from`/`to` aliases (fields are `from_user`/`to_user`).
EXPECTED_CHUNK_KEY_ORDER = [
    "attributes",
    "type",
    "message_id",
    "sender_id",
    "customer_contact",
    "from",
    "to",
    "disposition",
    "disposition_id",
    "sub_disposition",
    "sub_disposition_id",
    "description",
    "channel",
    "timestamp",
    "channel_meta",
    "retry_chunk_uuid",
    "reply_to",
    "is_first_chunk_for_conv",
    "is_inbound_chunk",
    "uuid",
    "from_user_name",
    "to_user_name",
    "contact_uuid",
]

# ConversationRes reversed-MRO order: PARENT first.
EXPECTED_CONVERSATION_RES_KEY_ORDER = [
    "parent_channel",
    "flow_closure_flag",
    "conversation_number",
    "crm_contact_id",
    "contact_identifier",
    "case_id",
    "parent_contact_uuid",
    "parent_end_at",
    "parent_start_at",
    "source_direction_inbound",
    "last_activity_at",
    "flow_run_uuid",
    "client_data",
    "contact_name",
    "is_hidden",
    "state",
    "generation_type",
    "status",
    "channel",
    "updated_at",
    "start_at",
    "end_at",
    "primary_agent",
    "wrap_up_start_at",
    "contact_uuid",
    "child_direction_inbound",
    "sender_id",
    "run_reverse_routing",
    "update_capacity_on_resolve",
]

# ConversationParentChildModel reversed-MRO order.
EXPECTED_PARENT_CHILD_KEY_ORDER = [
    "parent_channel",
    "flow_closure_flag",
    "conversation_number",
    "crm_contact_id",
    "contact_identifier",
    "case_id",
    "parent_contact_uuid",
    "parent_end_at",
    "parent_start_at",
    "source_direction_inbound",
    "last_activity_at",
    "flow_run_uuid",
    "client_data",
    "contact_name",
    "is_hidden",
    "child_conversations",
    "closed_conversations",
    "contact_uuid",
    "channel",
    "status",
    "updated_at",
    "start_at",
    "end_at",
    "meta",
    "agent_id",
    "unviewed_count",
    "last_message",
    "templates_data",
    "linked_conversation",
    "agent_secondary_conversation_state",
    "conversation_other_participant_state",
    "secondary_participants",
    "secondary_participants_details",
]

# ConversationVariablesRes declaration order.
EXPECTED_CONV_VARIABLES_KEY_ORDER = [
    "conversation_number",
    "contact_identifier",
    "conv_agents_email",
    "conv_created_date",
    "conv_channels_list",
    "conv_duration",
    "conv_url",
    "conv_disposition",
    "conv_wrapup_note",
    "conv_contact_id",
    "conv_case_id",
    "conv_source",
    "conv_destination",
    "conv_contact_name",
    "conv_channel_to_contact_uuid_map",
    "conv_parent_contact_uuid",
    "has_supervisor_or_admin",
    "conv_current_agent_name",
    "conv_participants_aom_uuids",
    "direction_inbound",
]


def _build_chunk(**overrides) -> ConversationChunkRes:
    data = {"type": "chat", "from": "agentbot", "to": "+15550001111", "timestamp": "2026-06-03T10:00:00+00:00"}
    data.update(overrides)
    return ConversationChunkRes(**data)


# ======================================================================================
# ConversationChunkRes — `attributes` FIRST … final from_user_name/to_user_name/contact_uuid
# ======================================================================================
def test_chunk_res_field_order_attributes_first():
    """pydantic field order: `attributes` FIRST, `type` SECOND, …, final fields
    from_user_name, to_user_name, contact_uuid (reversed-MRO over
    ConvChunkInsertModel)."""
    keys = list(ConversationChunkRes.model_fields.keys())
    assert keys[0] == "attributes"
    assert keys[1] == "type"
    assert keys[-3:] == ["from_user_name", "to_user_name", "contact_uuid"]


def test_chunk_res_wire_key_order_by_alias():
    """jsonable_encoder(..., by_alias=True) — the served form — emits the `from`/`to`
    aliases and the exact wire order with `attributes` FIRST."""
    enc = jsonable_encoder(_build_chunk(), by_alias=True)
    assert list(enc.keys()) == EXPECTED_CHUNK_KEY_ORDER
    assert "from" in enc and "to" in enc
    assert "from_user" not in enc and "to_user" not in enc


def test_chunk_res_dot_json_by_alias_matches():
    """`.model_dump_json(by_alias=True)` yields the same key order."""
    parsed = json.loads(_build_chunk().model_dump_json(by_alias=True))
    assert list(parsed.keys()) == EXPECTED_CHUNK_KEY_ORDER


def test_chunk_res_no_conversation_number():
    """The chunk (hence `last_message`) carries NO conversation_number; only contact_uuid survives."""
    fields = set(ConversationChunkRes.model_fields.keys())
    assert "conversation_number" not in fields
    assert "contact_uuid" in fields


def test_chunk_res_timestamp_redeclared_once_in_place():
    """`timestamp`/`channel_meta` are redeclared on ConversationChunkRes but appear
    ONCE, in their original ConvChunkInsertModel position (no duplicate trailing
    keys)."""
    keys = list(ConversationChunkRes.model_fields.keys())
    assert keys.count("timestamp") == 1
    assert keys.count("channel_meta") == 1
    # they sit mid-list, not at the tail
    assert keys.index("timestamp") < keys.index("uuid")


# ======================================================================================
# ConversationRes — reversed-MRO PARENT-first full order (POST /child/active)
# ======================================================================================
def _build_conversation_res(**overrides) -> ConversationRes:
    data = {
        "conversation_number": "12345678901",
        "channel": "chat",
        "status": "open",
        "state": "agent_answered",
        "generation_type": "qnr_generated",
        "updated_at": "2026-06-03T10:00:00+00:00",
    }
    data.update(overrides)
    return ConversationRes(**data)


def test_conversation_res_field_order_parent_first():
    assert list(ConversationRes.model_fields.keys()) == EXPECTED_CONVERSATION_RES_KEY_ORDER


def test_conversation_res_wire_key_order():
    enc = jsonable_encoder(_build_conversation_res())
    assert list(enc.keys()) == EXPECTED_CONVERSATION_RES_KEY_ORDER
    # parent block first, child markers last
    assert list(enc.keys())[0] == "parent_channel"
    assert list(enc.keys())[-2:] == ["run_reverse_routing", "update_capacity_on_resolve"]


def test_conversation_res_child_sub_order():
    """Child (ConvModelRes) sub-order is state, generation_type, status, channel
    They appear in that relative order after the parent
    block and before the two trailing flags."""
    keys = list(ConversationRes.model_fields.keys())
    assert keys.index("state") < keys.index("generation_type") < keys.index("status") < keys.index("channel")
    assert keys.index("is_hidden") < keys.index("state")  # parent before child
    assert keys.index("channel") < keys.index("run_reverse_routing")


# ======================================================================================
# ConversationParentChildModel — /active element order + placeholder start + unviewed_count
# ======================================================================================
def test_parent_child_model_field_order():
    assert list(ConversationParentChildModel.model_fields.keys()) == EXPECTED_PARENT_CHILD_KEY_ORDER


def test_parent_start_at_max_renders_with_T_separator():
    """The active-conv placeholder parent_start_at = datetime.max @ UTC renders
    `9999-12-31T23:59:59.999999+00:00` (T separator) via jsonable_encoder — the
    space form is only the Redis `lv` value."""
    latest = datetime.datetime.max.replace(tzinfo=datetime.UTC)
    m = ConversationParentChildModel(conversation_number="12345678901")
    m.parent_start_at = latest
    enc = jsonable_encoder(m)
    assert enc["parent_start_at"] == "9999-12-31T23:59:59.999999+00:00"
    assert "T" in enc["parent_start_at"]
    assert " " not in enc["parent_start_at"]


def test_unviewed_count_cache_miss_is_number_zero():
    """Cache MISS → default int 0 → JSON number."""
    enc = jsonable_encoder(ConversationParentChildModel(conversation_number="12345678901"))
    assert enc["unviewed_count"] == 0
    assert isinstance(enc["unviewed_count"], int)


# ======================================================================================
# ConversationVariablesRes — declaration order + epoch-float-string fields
# ======================================================================================
def test_conv_variables_res_declaration_order():
    assert list(ConversationVariablesRes.model_fields.keys()) == EXPECTED_CONV_VARIABLES_KEY_ORDER


def test_conv_variables_res_epoch_float_string_fields():
    """conv_created_date / conv_duration are Optional[str] — the usecase assigns
    epoch-second float-as-string values (e.g. "1717400000.0"); the model never
    coerces to ISO."""
    assert ConversationVariablesRes.model_fields["conv_created_date"].annotation == Optional[str]
    assert ConversationVariablesRes.model_fields["conv_duration"].annotation == Optional[str]
    m = ConversationVariablesRes(
        conversation_number="12345678901",
        contact_identifier="+15550001111",
        conv_parent_contact_uuid="cu-1",
        conv_created_date="1717400000.0",
        conv_duration="123.5",
    )
    enc = jsonable_encoder(m)
    assert enc["conv_created_date"] == "1717400000.0"
    assert enc["conv_duration"] == "123.5"
    assert isinstance(enc["conv_created_date"], str)


# ======================================================================================
# AgentDetailsRes — single agent_name
# ======================================================================================
def test_agent_details_res_single_agent_name():
    assert list(AgentDetailsRes.model_fields.keys()) == ["agent_name"]
    enc = jsonable_encoder(AgentDetailsRes(agent_name=None))
    assert enc == {"agent_name": None}


# ======================================================================================
# enums / constants
# ======================================================================================
def test_open_statuses():
    assert [ConversationStatus.open.name, ConversationStatus.waiting.name] == OPEN_STATUSES
    assert OPEN_STATUSES == ["open", "waiting"]


def test_qnr_children_sets():
    assert [
        ConversationGenerationTypes.qnr_generated.name,
        ConversationGenerationTypes.transfer_qnr_generated.name,
        ConversationGenerationTypes.task_generated.name,
    ] == QNR_CHILDREN
    assert [
        ConversationGenerationTypes.non_qnr_generated.name,
        ConversationGenerationTypes.transfer_non_qnr_generated.name,
    ] == NON_QNR_CHILDREN


def test_conversation_states_enum_values():
    """State integer values are byte-stable; a representative sample is pinned."""
    assert ConversationStates.agent_answered.value == 1
    assert ConversationStates.bot_answered.value == 5
    assert ConversationStates.transferred.value == 6
    assert ConversationStates.transferring.value == 15
    assert ConversationStates.transfer_failed.value == 16


def test_default_gen_type_state_mapping():
    """The generation-type → default-state mapping is byte-stable (used to derive
    the child state); a representative sample is pinned."""
    assert (
        DEFAULT_GEN_TYPE_STATE_MAPPING[ConversationGenerationTypes.qnr_generated.name]
        == ConversationStates.agent_unanswered.name
    )
    assert (
        DEFAULT_GEN_TYPE_STATE_MAPPING[ConversationGenerationTypes.non_qnr_generated.name]
        == ConversationStates.bot_answered.name
    )
    assert (
        DEFAULT_GEN_TYPE_STATE_MAPPING[ConversationGenerationTypes.outbound_generated.name]
        == ConversationStates.outbound_contact_unanswered.name
    )
    assert (
        DEFAULT_GEN_TYPE_STATE_MAPPING[ConversationGenerationTypes.channel_switch.name]
        == ConversationStates.agent_answered.name
    )


# ======================================================================================
# AOMOccupiedState (hs/admin/account_models.py); guarded import.
# ======================================================================================
def test_aom_occupied_state_field_order_if_available():
    """Pins AOMOccupiedState's 7-key order: id, created_at, updated_at, is_deleted, uuid,
    is_aom_occupied, secondary_agent_state. Skips if the module is unavailable."""
    try:
        from hs.admin.account_models import AOMOccupiedState
    except Exception:
        pytest.skip("hs.admin.account_models.AOMOccupiedState unavailable")
    assert list(AOMOccupiedState.model_fields.keys()) == [
        "id",
        "created_at",
        "updated_at",
        "is_deleted",
        "uuid",
        "is_aom_occupied",
        "secondary_agent_state",
    ]


# ── contact-identifier formatting ────────────────


@pytest.mark.parametrize(
    "raw, stored",
    [
        ("14155551234", "14155551234"),
        (" +1 (415) 555-1234 ", "+1 (415) 555-1234"),
        ("Bob@Example.com", "Bob@Example.com"),
    ],
)
def test_parent_insert_phone_or_email_keeps_format(raw, stored):
    from hs.conversations.models import ConvParentInsertModel

    insert = ConvParentInsertModel(parent_channel="call", phone_or_email=raw, parent_contact_uuid="c1", org_auth_id="A")
    assert insert.phone_or_email == stored


def test_child_insert_customer_contact_and_sender_id_keep_format(monkeypatch):
    import hs.setup.number as number
    from hs.conversations.models import ConvInsertModel

    monkeypatch.setattr(number.Number, "get_by_number", staticmethod(lambda n: None))

    def child(customer_contact, sender_id=None):
        conv = ConvInsertModel(
            generation_type="qnr_generated", channel="message", customer_contact=customer_contact, sender_id=sender_id
        )
        return conv.customer_contact, conv.sender_id

    assert child("14155551234", "14155550000") == ("14155551234", "14155550000")  # no "+" added
    assert child(" Anonymous ") == ("anonymous", None)  # lowercased, not mapped to "Anonymous"
