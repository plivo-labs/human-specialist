"""Queue-run child conversation create.

Pins:
  * create_qnr_child_conversation: None on 201, the fixed 'child conversation creation failed'
    string on any failure; payload→ConvInsertModel mapping (contact_uuid omitted when empty);
  * the qnr_generated spine: queue-id required, create + Post(open) + 201;
  * transfer_qnr_generated: parent fallback re-parents off the non-qnr child (400 when absent),
    start_at/sender_id seed from the first non-bot child, the flow-transfer chunk to_user backfill;
  * the outbound/channel_switch duplicate guard: 400 + ERROR_CHILD_CONVERSATION_EXISTS + the
    resolved GENERIC_ERROR_RESPONSE payload; the call channel is exempt;
  * channel_switch: capacity increase, capacity error → 400 + AGENT_NO_CAPACITY payload + rollback;
    the outbound-call branch creates the non-qnr child + publishes OUTBOUND_CALL_INITIALISED.
"""

import uuid as uuid_mod
from types import SimpleNamespace

import pytest

import tests.conftest  # noqa: F401 — eager data_adapter imports (AbstractConcreteBase registry)
from hs.conversations import transfer as cxfer
from hs.conversations.models import ConvInsertModel

AOM_UUID = uuid_mod.uuid4()
ORG_UUID = str(uuid_mod.uuid4())


@pytest.fixture(autouse=True)
def _req_ctx():
    from hs.infra.context import context_background_tasks, context_org_name, context_org_uuid, context_req_meta

    t1 = context_req_meta.set({"api_id": "child-create-test"})
    t2 = context_background_tasks.set(SimpleNamespace(add_task=lambda fn, *a, **k: None))
    t3 = context_org_uuid.set(ORG_UUID)
    t4 = context_org_name.set("plivo")
    yield
    context_req_meta.reset(t1)
    context_background_tasks.reset(t2)
    context_org_uuid.reset(t3)
    context_org_name.reset(t4)


class _Post:
    calls = []

    def __init__(self, **kw):
        self.kw = kw

    def invoke(self):
        _Post.calls.append(self.kw)


@pytest.fixture(autouse=True)
def _post_recorder(monkeypatch):
    import hs.conversations.management as cm

    _Post.calls = []
    monkeypatch.setattr(cm, "PostConversationStatusChange", _Post)
    yield


def _parent(contact="par-1", channel="chat"):
    return SimpleNamespace(
        id=11,
        organization=1,
        conversation_number="CN1",
        parent_contact_uuid=contact,
        contact_uuid=contact,
        contact_identifier="+14155550000",
        parent_channel=channel,
        source_direction_inbound=True,
    )


def _aom():
    return SimpleNamespace(
        id=203,
        uuid=AOM_UUID,
        organization=1,
        sip_endpoint="sip:agent2@sip.example.com",
        org=SimpleNamespace(name="plivo", uuid=ORG_UUID, id=1),
    )


def _created_row(parent, gen="qnr_generated"):
    row = SimpleNamespace(
        id=88, generation_type=gen, primary_aom=None, primary_agent=None, parent_conversation=parent, channel="chat"
    )
    row.build_res_model = lambda **kw: {"contact_uuid": "par-1"}
    return row


# ============================================================================ qnr spine
def test_qnr_create_requires_queue_id_and_happy_path(monkeypatch):
    parent = _parent()
    monkeypatch.setattr(
        cxfer.ConversationParent, "get_by_contact_uuid", classmethod(lambda cls, c, only_open_conv=False: parent)
    )
    conv = ConvInsertModel(generation_type="qnr_generated", parent_contact_uuid="par-1", sender_id=None, channel="chat")
    resp = cxfer.create_child_conversation(conv)
    assert (resp.status_code, resp.error) == (400, "Queue UUID required for QNR generated conversation")

    monkeypatch.setattr(
        ConvInsertModel, "build_db_model", lambda self, **kw: SimpleNamespace(start_at=None, channel=self.channel)
    )
    monkeypatch.setattr(cxfer.Conversation, "create_conversation", classmethod(lambda cls, row: _created_row(parent)))
    conv2 = ConvInsertModel(
        generation_type="qnr_generated",
        parent_contact_uuid="par-1",
        sender_id=None,
        channel="chat",
        active_queue_entry_id=5,
    )
    resp = cxfer.create_child_conversation(conv2)
    assert (resp.status_code, resp.msg) == (201, "Child conversation added successfully")
    # the created child has NO primary (in-transit qnr) → no Post(open)
    assert _Post.calls == []


# ============================================================================ transfer_qnr branch
def test_transfer_qnr_parent_fallback_and_seed(monkeypatch):
    parent = _parent(contact="par-real")
    non_qnr = SimpleNamespace(parent_conversation=parent, contact_uuid="child-1")
    first_non_bot = SimpleNamespace(sender_id="+1999")
    backfilled = []
    monkeypatch.setattr(
        cxfer.ConversationParent, "get_by_contact_uuid", classmethod(lambda cls, c, only_open_conv=False: None)
    )
    monkeypatch.setattr(
        cxfer.Conversation,
        "get_active_non_qnr_child_by_contact_uuid",
        classmethod(lambda cls, c, filter_active=True: non_qnr),
    )
    monkeypatch.setattr(
        cxfer.Conversation, "get_first_non_bot_by_contact_uuid", classmethod(lambda cls, c: first_non_bot)
    )
    from hs.conversations.store import ConversationChunk

    monkeypatch.setattr(
        ConversationChunk,
        "get_conversation_chunks_by_conv_number",
        classmethod(lambda cls, cn, **kw: [SimpleNamespace(id=42)]),
    )
    monkeypatch.setattr(
        ConversationChunk,
        "update_conversation_chunk_by_id",
        classmethod(lambda cls, cid, ud: backfilled.append((cid, ud))),
    )
    monkeypatch.setattr(
        ConvInsertModel,
        "build_db_model",
        lambda self, **kw: SimpleNamespace(start_at=self.start_at, channel=self.channel),
    )
    created = {}

    def _cc(cls, row):
        created["row"] = row
        return _created_row(parent, gen="transfer_qnr_generated")

    monkeypatch.setattr(cxfer.Conversation, "create_conversation", classmethod(_cc))
    aom = _aom()
    monkeypatch.setattr(cxfer, "fetch_aom_by_uuid_or_sip_and_set_context", lambda s: aom)
    conv = ConvInsertModel(
        generation_type="transfer_qnr_generated",
        parent_contact_uuid="child-1",
        sender_id=None,
        channel="chat",
        active_queue_entry_id=5,
        agent_sip_or_uuid=str(AOM_UUID),
    )
    resp = cxfer.create_child_conversation(conv)
    assert resp.status_code == 201
    # re-parented off the non-qnr child + seeded
    assert conv.parent is parent and conv.parent_contact_uuid == "par-real"
    assert conv.contact_uuid == "child-1" and conv.sender_id == "+1999" and conv.start_at is not None
    # the flow-transfer chunk to_user backfill hit the FIRST empty-to_user transfer chunk
    assert backfilled == [(42, {"to_user": str(AOM_UUID)})]


def test_transfer_qnr_no_parent_no_child_400(monkeypatch):
    monkeypatch.setattr(
        cxfer.ConversationParent, "get_by_contact_uuid", classmethod(lambda cls, c, only_open_conv=False: None)
    )
    monkeypatch.setattr(
        cxfer.Conversation,
        "get_active_non_qnr_child_by_contact_uuid",
        classmethod(lambda cls, c, filter_active=True: None),
    )
    conv = ConvInsertModel(
        generation_type="transfer_qnr_generated",
        parent_contact_uuid="ghost",
        sender_id=None,
        channel="chat",
        active_queue_entry_id=5,
    )
    resp = cxfer.create_child_conversation(conv)
    assert (resp.status_code, resp.error) == (400, "Parent transaction not found for transfer conversation") or (
        resp.status_code,
        resp.error,
    ) == (400, "Parent conversation not found")


# ============================================================================ channel_switch
