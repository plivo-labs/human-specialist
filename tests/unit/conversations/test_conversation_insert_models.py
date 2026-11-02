"""ConvInsertModel / NonQNRChunkInsert build_db_model: the conversation + chunk ORM rows a task
create writes, plus the key branches (primary_aom human-agent resolution, channel fallback,
inbound-chunk from/to swap, disposition lookup, and the guarded outbound
custom-state/priority path). Offline.
"""

from types import SimpleNamespace
from unittest.mock import patch

import tests.conftest  # noqa: F401 — eager data_adapter imports (AbstractConcreteBase registry)
from hs.conversations.models import ConvInsertModel, NonQNRChunkInsert


def _parent(**kw):
    base = {
        "id": 42,
        "parent_channel": "chat",
        "parent_contact_uuid": "pcu-1",
        "contact_identifier": "cust-1",
        "organization": 7,
    }
    base.update(kw)
    return SimpleNamespace(**base)


def _conv_insert(**kw):
    base = {
        "generation_type": "task_generated",
        "state": "agent_answered",
        "channel": "chat",
        "status": "open",
        "contact_uuid": "task-uuid-1",
        "direction_inbound": True,
    }
    base.update(kw)
    m = ConvInsertModel(**base)
    m.parent = kw.pop("parent", _parent())
    return m


# --- ConvInsertModel.build_db_model -------------------------------------------


def test_conv_insert_maps_core_columns():
    conv = _conv_insert().build_db_model(primary_aom=None)
    assert conv.parent == 42
    assert conv.channel == "chat"
    assert conv.generation_type == "task_generated"
    assert conv.state == "agent_answered"
    assert conv.status == "open"
    assert conv.contact_uuid == "task-uuid-1"
    assert conv.primary_aom is None
    assert conv.customer_contact == "cust-1"  # falls back to parent.contact_identifier
    assert conv.direction_inbound is True


def test_conv_insert_primary_agent():
    conv = _conv_insert().build_db_model(primary_aom=SimpleNamespace(id=11, organization=7))
    assert conv.primary_aom == 11


def test_conv_insert_channel_falls_back_to_parent():
    m = _conv_insert(channel=None)
    m.parent = _parent(parent_channel="message")
    conv = m.build_db_model(primary_aom=None)
    assert conv.channel == "message"


def test_conv_insert_contact_uuid_falls_back_to_parent():
    m = _conv_insert(contact_uuid=None)
    conv = m.build_db_model(primary_aom=None)
    assert conv.contact_uuid == "pcu-1"  # parent.parent_contact_uuid


def test_conv_insert_outbound_custom_state_is_guarded():
    # The custom_conv_state / conv_priority tail resolves against the org tables (the
    # transfer_generated create passes both); this DTO-level pin keeps the lookups
    # LAZY (called only when the args are passed — the task-create path stays DB-free).
    with patch(
        "hs.conversations.store.ConversationCustomStatusMapping.get_by_state_and_org",
        return_value=SimpleNamespace(id=4),
    ) as gs:
        conv = _conv_insert().build_db_model(primary_aom=None, custom_conv_state="in_progress_human")
        assert conv.custom_status_mapping == 4 and gs.called
    with patch(
        "hs.conversations.store.ConversationPriority.get_by_name_and_org", return_value=SimpleNamespace(id=2)
    ) as gp:
        conv = _conv_insert().build_db_model(primary_aom=None, conv_priority="high")
        assert conv.priority == 2 and gp.called


# --- NonQNRChunkInsert.build_db_model -----------------------------------------


def _chunk_insert(**kw):
    base = {
        "type": "signal",
        "description": "TASK_PENDING",
        "parent_contact_uuid": "pcu-1",
        "message_id": "task-uuid-1",
        "channel": "chat",
    }
    base.update(kw)
    return NonQNRChunkInsert(**base)


def _conv_for_chunk(channel="chat", contact_identifier="cust-1"):
    return SimpleNamespace(
        id=99, channel=channel, parent_conversation=SimpleNamespace(contact_identifier=contact_identifier)
    )


def test_chunk_insert_maps_core_columns_outbound_signal():
    chunk = _chunk_insert().build_db_model(_conv_for_chunk())
    assert chunk.conversation == 99
    assert chunk.type == "signal"
    assert chunk.description == "TASK_PENDING"
    assert chunk.message_id == "task-uuid-1"
    assert chunk.channel == "chat"
    assert chunk.from_user == "adminbot-00000000-0000-4000-a000-000000000000"  # bot
    assert chunk.to_user == "cust-1"  # parent contact_identifier
    assert chunk.timestamp is not None


def test_chunk_insert_inbound_swaps_from_to():
    chunk = _chunk_insert(is_inbound_chunk=True).build_db_model(_conv_for_chunk())
    assert chunk.from_user == "cust-1"
    assert chunk.to_user == "adminbot-00000000-0000-4000-a000-000000000000"


def test_chunk_insert_channel_falls_back_to_conversation():
    chunk = _chunk_insert(channel=None).build_db_model(_conv_for_chunk(channel="email"))
    assert chunk.channel == "email"


def test_chunk_insert_disposition_lookup_sets_id():
    from hs.infra.context import context_org_uuid

    token = context_org_uuid.set("org-uuid-1")
    try:
        with patch("hs.admin.dispositions.Dispositions.get_by_name", return_value=SimpleNamespace(id=88)) as gbn:
            chunk = _chunk_insert(disposition="Resolved").build_db_model(_conv_for_chunk())
        gbn.assert_called_once()
        assert chunk.disposition_id == 88
    finally:
        context_org_uuid.reset(token)
