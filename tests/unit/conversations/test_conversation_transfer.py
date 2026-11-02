"""Conversation transfer rows and caches (offline decision-tree pins):
* transfer/check: 404 / allow_acw fast-path / crm-contact gate / the conversation_details shape
  (no fail-open empty details) / add_participant gating;
* PATCH transfer/update: the BeforeTransferStateCache halves (set on transferring, restore +
  delete on transfer_failed) + the error strings;
* POST /transfer: guards (400s), the cold/agent happy path (transferred+in_wrap_up update shapes,
  transfer chunk, Post(in_wrap_up, is_transfer_resolve=True), secondary cleanup with
  bypass_all_events, the transfer_generated child create args, CONVERSATION_TRANSFERRED
  event-callback, TRANSFER_SUCCESS_PRIMARY, BTS delete), external (no child-queue id, the
  external number as chunk to_user, primary-left event ON), flow (no child create + monitor-barge
  eviction with MONITOR_FAILURE);
* create_child_conversation: transfer_generated (TRANSFER_SUCCESS_SECONDARY with the contact swap
  + Post(open));
* secondary participants: enrich guards (404/422), warm create (mapping row
  + TRANSFER_INITIATED_PRIMARY + SECONDARY_PARTICIPANT_ADDED), remove (delete count guard 422, BTS
  restore via update_failed_transfer_state, TRANSFER_SECONDARY_LEFT + SECONDARY_PARTICIPANT_REMOVED,
  bypass_all_events silence);
* handle_non_transferred_conversations: census close + flow_closure_flag + Post(closed,
  session_timeout);
* PostConversationStatusChange: in_wrap_up → resolve params (is_transfer_resolve, release per
  AgentConversationSetting) + PRIMARY_AGENT_LEFT gating + last-child handle-time/wrap-up-timer;
  closed → the per-channel wrap-up dispatch + the capacity clearance; self-task skip;
* the transfer entry points: fixed failure strings, check fails closed on error, the signal chunk
  never raises, the flow-201-carries-old-child contact_uuid extraction, defaults wiring.
"""

import uuid as uuid_mod
from types import SimpleNamespace

import pytest

import tests.conftest  # noqa: F401 — eager data_adapter imports (AbstractConcreteBase registry)
from hs.conversations import child_update as ccu
from hs.conversations import transfer as cxfer
from hs.conversations.models import (
    ConvSecAgentMappingInsert,
    ConvSecAgentMappingRemove,
    ConvTransferModel,
    UpdateConvTransferModel,
)
from hs.infra.models import UseCaseResponse

AOM_UUID = uuid_mod.uuid4()
OLD_AOM_UUID = uuid_mod.uuid4()


ORG_UUID = str(uuid_mod.uuid4())


@pytest.fixture(autouse=True)
def _req_ctx():
    from hs.infra.context import (
        context_aom_uuid,
        context_background_tasks,
        context_org_name,
        context_org_uuid,
        context_req_meta,
    )

    t1 = context_req_meta.set({"api_id": "conversation-transfer-test"})
    bg_calls = []
    bg = SimpleNamespace(add_task=lambda fn, *a, **k: bg_calls.append((fn, a, k)))
    t2 = context_background_tasks.set(bg)
    t3 = context_org_uuid.set(ORG_UUID)
    t4 = context_aom_uuid.set(str(AOM_UUID))
    t5 = context_org_name.set("plivo")  # pre-set: get_context_org_name must not hit the DB
    yield bg_calls
    context_req_meta.reset(t1)
    context_background_tasks.reset(t2)
    context_org_uuid.reset(t3)
    context_aom_uuid.reset(t4)
    context_org_name.reset(t5)


class _Post:
    """PostConversationStatusChange recorder (patched into hs.conversations.management)."""

    calls = []

    def __init__(self, **kw):
        self.kw = kw

    def invoke(self):
        _Post.calls.append(self.kw)


@pytest.fixture(autouse=True)
def _recorders(monkeypatch):
    import hs.conversations.management as cm

    _Post.calls = []
    monkeypatch.setattr(cm, "create_chunk_with_related_data", lambda chunk: None)
    yield


PARENT_UUID = uuid_mod.uuid4()


def _parent(org=1, cn="CN1", contact="par-1"):
    return SimpleNamespace(
        id=11,
        uuid=PARENT_UUID,
        organization=org,
        conversation_number=cn,
        parent_contact_uuid=contact,
        contact_uuid=contact,
        contact_identifier="+14155550000",
        contact_name="Cust",
        crm_contact_id=None,
        case_id=None,
        parent_channel="chat",
        source_direction_inbound=True,
        flow_closure_flag=False,
    )


def _aom(org=1, aom_id=203, aom_uuid=AOM_UUID, sip="sip:agent2@sip.example.com"):
    # org carries the FULL OrganizationModel field set — ConvSecAgentMappingRemove(secondary_agent=
    # aom) pydantic-validates it (AOMFullModel.org) on the transfer path.
    org_ns = SimpleNamespace(
        id=org,
        uuid=ORG_UUID,
        name="plivo",
        auth_id="MA_X",
        auth_token="tok",
        skip_contact_creation=True,
        skip_case_creation=True,
        allow_acw_post_transfer=False,
        is_deleted=False,
        created_at=None,
        updated_at=None,
    )
    return SimpleNamespace(id=aom_id, uuid=aom_uuid, organization=org, sip_endpoint=sip, org=org_ns)


def _conv(
    channel="chat",
    generation_type="qnr_generated",
    conv_id=77,
    contact="c-1",
    state="agent_answered",
    status="open",
    primary=None,
    parent=None,
    aqe=5,
    child_dir=True,
):
    parent = parent or _parent()
    primary = primary or _aom(aom_id=101, aom_uuid=OLD_AOM_UUID, sip="sip:agent1@sip.example.com")
    c = SimpleNamespace(
        id=conv_id,
        uuid=f"conv-{conv_id}",
        channel=channel,
        generation_type=generation_type,
        contact_uuid=contact,
        state=state,
        status=status,
        parent=parent.id,
        parent_conversation=parent,
        primary_agent=primary,
        primary_aom=primary.id,
        customer_contact=None,
        sender_id=None,
        end_at=None,
        active_queue_entry_id=aqe,
        child_direction_inbound=child_dir,
    )
    c.build_res_model = lambda **kw: SimpleNamespace(contact_uuid=c.contact_uuid)
    return c


_TRANSFERRED = SimpleNamespace(id=6, state="transferred")
_IN_WRAP_UP = SimpleNamespace(id=7, state="in_wrap_up")


def _patch_custom_states(mp):
    mp.setattr(
        ccu,
        "get_conversation_custom_state_by_name",
        lambda org_id, state_name: _TRANSFERRED if state_name == "transferred" else _IN_WRAP_UP,
    )


def _patch_org(mp, allow_acw=False, skip_contact=True, skip_case=True):
    org = SimpleNamespace(
        id=1,
        uuid=ORG_UUID,
        name="plivo",
        allow_acw_post_transfer=allow_acw,
        skip_contact_creation=skip_contact,
        skip_case_creation=skip_case,
    )
    mp.setattr(cxfer.Organization, "get_by_id", classmethod(lambda cls, i: org))
    return org


# ============================================================================ transfer/check
def test_check_404_when_no_active_conversation(monkeypatch):
    monkeypatch.setattr(
        cxfer.Conversation, "get_single_active_conversation_for_parent", classmethod(lambda cls, c, channel=None: None)
    )
    resp = cxfer.check_if_transfer_allowed("par-1")
    assert (resp.status_code, resp.error) == (404, "Conversation not found")


def test_check_acw_fast_path_skips_details(monkeypatch):
    conv = _conv()
    monkeypatch.setattr(
        cxfer.Conversation, "get_single_active_conversation_for_parent", classmethod(lambda cls, c, channel=None: conv)
    )
    _patch_org(monkeypatch, allow_acw=True)
    resp = cxfer.check_if_transfer_allowed("par-1", channel="chat", include_conv_details=True)
    assert resp.data == {"transfer_allowed": True}  # NO conversation_details on the ACW path


def test_check_conv_details_shape_and_add_participant(monkeypatch):
    conv = _conv(state="agent_answered")
    first = SimpleNamespace(channel="chat")
    monkeypatch.setattr(
        cxfer.Conversation, "get_single_active_conversation_for_parent", classmethod(lambda cls, c, channel=None: conv)
    )
    monkeypatch.setattr(
        cxfer.Conversation, "get_first_child_conversation_for_parent", classmethod(lambda cls, cn: first)
    )
    _patch_org(monkeypatch)

    resp = cxfer.check_if_transfer_allowed(
        "par-1", channel="chat", include_conv_details=True, check_add_participant_allowed=True
    )
    d = resp.data
    assert d["transfer_allowed"] is True
    assert d["conversation_details"] == {
        "first_channel": "chat",
        "parent_channel": "chat",
        "is_inbound": True,
        "from": "+14155550000",
        "to": None,
        "agent_sip_endpoint": "sip:agent1@sip.example.com",
        "agent_uuid": str(OLD_AOM_UUID),
        "is_outbound_call": False,
        "conversation_number": "CN1",
    }
    assert d["add_participant_allowed"] is True
    # transfer_generated child → add_participant NOT allowed
    assert cxfer.check_if_add_participant_allowed(_conv(generation_type="transfer_generated")) is False


# ============================================================================ PATCH transfer/update
def test_update_transfer_400_when_no_transferable(monkeypatch):
    monkeypatch.setattr(
        cxfer.Conversation, "get_active_transferable_by_contact_uuid", classmethod(lambda cls, c, ch=None, p=None: None)
    )
    resp = cxfer.update_transfer_conversation(
        UpdateConvTransferModel(contact_uuid="par-1", channel="chat", state="transferring")
    )
    assert (resp.status_code, resp.error) == (400, "Child conversation not found for contact uuid")


def test_update_transfer_transferring_sets_bts_and_state(monkeypatch):
    conv = _conv(state="agent_answered")
    bts, updates = {}, []
    monkeypatch.setattr(
        cxfer.Conversation, "get_active_transferable_by_contact_uuid", classmethod(lambda cls, c, ch=None, p=None: conv)
    )
    monkeypatch.setattr(
        cxfer.Conversation,
        "update_conversation",
        classmethod(lambda cls, cid, ud, now=None, **kw: updates.append((cid, ud))),
    )
    monkeypatch.setattr(
        cxfer.BeforeTransferStateCache, "set", classmethod(lambda cls, c, ch, s: bts.update({(c, ch): s}) or True)
    )
    resp = cxfer.update_transfer_conversation(
        UpdateConvTransferModel(contact_uuid="par-1", channel="chat", state="transferring")
    )
    assert resp.msg == "success"
    assert bts == {("par-1", "chat"): "agent_answered"}
    assert updates == [(77, {"state": "transferring"})]
    # cache-set failure → the error envelope, NO row update
    updates.clear()
    monkeypatch.setattr(cxfer.BeforeTransferStateCache, "set", classmethod(lambda cls, c, ch, s: False))
    resp = cxfer.update_transfer_conversation(
        UpdateConvTransferModel(contact_uuid="par-1", channel="chat", state="transferring")
    )
    assert resp.error == "Unable to update conversation state" and updates == []


def test_update_transfer_failed_restores_bts(monkeypatch):
    conv = _conv(state="transferring")
    updates, deleted = [], []
    monkeypatch.setattr(
        cxfer.Conversation, "get_active_transferable_by_contact_uuid", classmethod(lambda cls, c, ch=None, p=None: conv)
    )
    monkeypatch.setattr(
        cxfer.Conversation, "update_conversation", classmethod(lambda cls, cid, ud, now=None, **kw: updates.append(ud))
    )
    monkeypatch.setattr(cxfer.BeforeTransferStateCache, "get", classmethod(lambda cls, c, ch: "agent_answered"))
    monkeypatch.setattr(
        cxfer.BeforeTransferStateCache, "delete", classmethod(lambda cls, c, ch: deleted.append((c, ch)))
    )
    resp = cxfer.update_transfer_conversation(
        UpdateConvTransferModel(contact_uuid="par-1", channel="chat", state="transfer_failed")
    )
    assert resp.msg == "success"
    assert updates == [{"state": "agent_answered"}] and deleted == [("par-1", "chat")]


def test_update_transfer_repeated_start_keeps_bts(monkeypatch):
    conv = _conv(state="agent_answered")
    bts, updates = {}, []

    def _update(cls, cid, ud, now=None, **kw):
        updates.append(ud)
        conv.state = ud["state"]

    def _req(state):
        return UpdateConvTransferModel(contact_uuid="par-1", channel="chat", state=state)

    monkeypatch.setattr(
        cxfer.Conversation, "get_active_transferable_by_contact_uuid", classmethod(lambda cls, c, ch=None, p=None: conv)
    )
    monkeypatch.setattr(cxfer.Conversation, "update_conversation", classmethod(_update))
    monkeypatch.setattr(
        cxfer.BeforeTransferStateCache, "set", classmethod(lambda cls, c, ch, s: bts.update({(c, ch): s}) or True)
    )
    monkeypatch.setattr(cxfer.BeforeTransferStateCache, "get", classmethod(lambda cls, c, ch: bts.get((c, ch))))
    monkeypatch.setattr(
        cxfer.BeforeTransferStateCache, "delete", classmethod(lambda cls, c, ch: bts.pop((c, ch), None))
    )
    for _ in range(3):
        assert cxfer.update_transfer_conversation(_req("transferring")).msg == "success"
    # only the first start snapshots and writes; redelivered starts are no-ops
    assert bts == {("par-1", "chat"): "agent_answered"} and updates == [{"state": "transferring"}]
    assert cxfer.update_transfer_conversation(_req("transfer_failed")).msg == "success"
    assert (conv.state, bts) == ("agent_answered", {})


def test_bts_cache_key_shape():
    assert cxfer.BeforeTransferStateCache._get_key("par-demo-alice", "chat") == "bts:par-demo-alice:chat"


# ============================================================================ POST /transfer
def _patch_transfer_commons(monkeypatch, conv, new_aom):
    _patch_custom_states(monkeypatch)
    updates = []
    monkeypatch.setattr(
        cxfer.Conversation, "get_active_transferable_by_contact_uuid", classmethod(lambda cls, c, ch=None, p=None: conv)
    )
    monkeypatch.setattr(
        cxfer.Conversation,
        "update_conversation",
        classmethod(
            lambda cls, conversation_id=None, update_dict=None, now=None, **kw: updates.append((kw or {}, update_dict))
        ),
    )
    monkeypatch.setattr(cxfer.Conversation, "get_primary_active_conversation", classmethod(lambda cls, *a, **k: None))
    monkeypatch.setattr(cxfer, "fetch_aom_by_uuid_or_sip_and_set_context", lambda s: new_aom)
    monkeypatch.setattr(cxfer, "fetch_org_by_id_and_set_context", lambda i: ("", None))
    monkeypatch.setattr(
        cxfer.ConversationParent,
        "update_last_activity_at",
        classmethod(lambda cls, conversation_id=None, now=None: None),
    )
    monkeypatch.setattr(cxfer.AccountOrgMapping, "get_name_by_aom_uuid", classmethod(lambda cls, u: "Cara"))
    monkeypatch.setattr(cxfer.BeforeTransferStateCache, "delete", classmethod(lambda cls, c, ch: None))
    monkeypatch.setattr(
        cxfer,
        "remove_secondary_participant",
        lambda m, bypass_all_events=False, active=True: UseCaseResponse(status_code=200, msg="ok"),
    )
    monkeypatch.setattr(cxfer, "handle_non_transferred_conversations", lambda c: UseCaseResponse(status_code=200))
    import hs.conversations.management as cm

    monkeypatch.setattr(cm, "PostConversationStatusChange", _Post)
    return updates


def test_transfer_child_guards(monkeypatch):
    monkeypatch.setattr(
        cxfer.Conversation, "get_active_transferable_by_contact_uuid", classmethod(lambda cls, c, ch=None, p=None: None)
    )
    resp = cxfer.transfer_child_conversation(ConvTransferModel(contact_uuid="par-1", channel="chat"))
    assert (resp.status_code, resp.error) == (400, "Child conversation not found for contact uuid")

    conv = _conv()
    monkeypatch.setattr(
        cxfer.Conversation, "get_active_transferable_by_contact_uuid", classmethod(lambda cls, c, ch=None, p=None: conv)
    )
    monkeypatch.setattr(cxfer, "fetch_org_by_id_and_set_context", lambda i: ("", None))
    monkeypatch.setattr(cxfer, "fetch_aom_by_uuid_or_sip_and_set_context", lambda s: None)
    resp = cxfer.transfer_child_conversation(
        ConvTransferModel(contact_uuid="par-1", channel="chat", transfer_type="agent", agent_sip_or_uuid="sip:nope@x")
    )
    assert (resp.status_code, resp.error) == (400, "Agent not found for given sip_endpoint or aom_uuid")

    same = _aom(aom_id=101, aom_uuid=OLD_AOM_UUID)
    monkeypatch.setattr(cxfer, "fetch_aom_by_uuid_or_sip_and_set_context", lambda s: same)
    resp = cxfer.transfer_child_conversation(
        ConvTransferModel(
            contact_uuid="par-1", channel="chat", transfer_type="agent", agent_sip_or_uuid=str(OLD_AOM_UUID)
        )
    )
    assert (resp.status_code, resp.error) == (400, "Cannot transfer conversation to same primary agent")

    other_org = _aom(org=2)
    monkeypatch.setattr(cxfer, "fetch_aom_by_uuid_or_sip_and_set_context", lambda s: other_org)
    resp = cxfer.transfer_child_conversation(
        ConvTransferModel(contact_uuid="par-1", channel="chat", transfer_type="agent", agent_sip_or_uuid=str(AOM_UUID))
    )
    assert (resp.status_code, resp.error) == (400, "Agent and conversation are not in the same organization")


def test_transfer_child_agent_happy_path(monkeypatch, emitted):
    conv = _conv()
    new_aom = _aom()
    updates = _patch_transfer_commons(monkeypatch, conv, new_aom)
    chunks = []
    import hs.conversations.management as cm

    monkeypatch.setattr(cm, "create_chunk_with_related_data", lambda chunk: chunks.append(chunk))
    created = {}

    def _fake_create(child, aom=None, outbound_queue_transfer=False, **kw):
        created.update(child=child, aom=aom, oqt=outbound_queue_transfer, **kw)
        return UseCaseResponse(status_code=201, data={"contact_uuid": "c-1"})

    monkeypatch.setattr(cxfer, "create_child_conversation", _fake_create)
    resp = cxfer.transfer_child_conversation(
        ConvTransferModel(contact_uuid="par-1", channel="chat", transfer_type="agent", agent_sip_or_uuid=str(AOM_UUID))
    )
    assert (resp.status_code, resp.msg) == (201, "Child conversation successfully transferred")
    assert resp.data == {"contact_uuid": "c-1"}
    # two updates: transferred (state+status+wrap_up_start+custom) then the in_wrap_up custom chain
    _first_kw, first_ud = updates[0]
    assert first_ud["state"] == "transferred" and first_ud["status"] == "in_wrap_up"
    assert first_ud["custom_status_mapping"] == _TRANSFERRED.id and "wrap_up_start_at" in first_ud
    _second_kw, second_ud = updates[1]
    assert second_ud == {"custom_status_mapping": _IN_WRAP_UP.id}
    # transfer chunk: from old primary to the new aom, channel_meta carries transfer_type
    assert len(chunks) == 1
    post = _Post.calls[0]
    assert post["changed_status"] == "in_wrap_up"
    # child create: transfer_generated under the same parent, queue id carried, oqt=True
    child = created["child"]
    assert child.generation_type == "transfer_generated" and child.state == "agent_answered"
    assert child.active_queue_entry_id == 5 and created["oqt"] is True and created["aom"] is new_aom
    # CONVERSATION_TRANSFERRED event callback + call.transferred from the OLD agent
    [(kind, data, kw)] = emitted
    assert kind == "call.transferred" and kw["agent_id"] == str(OLD_AOM_UUID)
    assert data == {
        "transfer_id": "conv-77",
        "mode": "cold",
        "from_agent_id": str(OLD_AOM_UUID),
        "target": {"type": "agent", "id": str(AOM_UUID)},
        "status": "completed",
    }


def test_transfer_child_external_path(monkeypatch):
    conv = _conv()
    _patch_transfer_commons(monkeypatch, conv, None)
    monkeypatch.setattr(cxfer, "fetch_aom_by_uuid_or_sip_and_set_context", lambda s: None)
    created = {}

    def _fake_create(child, aom=None, outbound_queue_transfer=False, **kw):
        created.update(child=child, aom=aom)
        return UseCaseResponse(status_code=201, data={"contact_uuid": "c-1"})

    monkeypatch.setattr(cxfer, "create_child_conversation", _fake_create)
    resp = cxfer.transfer_child_conversation(
        ConvTransferModel(
            contact_uuid="par-1", channel="chat", transfer_type="external", agent_sip_or_uuid="+14155553333"
        )
    )
    assert resp.status_code == 201
    child = created["child"]
    # external: no aom → external_transfer_initiated state, queue id dropped, primary-left ON
    assert child.state == "external_transfer_initiated" and child.active_queue_entry_id is None
    assert created["aom"] is None


def test_transfer_child_flow_skips_child_and_evicts_monitor(monkeypatch, emitted):
    conv = _conv()
    _patch_transfer_commons(monkeypatch, conv, None)
    monkeypatch.setattr(cxfer, "fetch_aom_by_uuid_or_sip_and_set_context", lambda s: None)
    barger_uuid = uuid_mod.uuid4()
    mapping = SimpleNamespace(aom=301, agent=SimpleNamespace(uuid=barger_uuid))
    deleted = []
    monkeypatch.setattr(
        cxfer.ConvSecondaryAgentMapping,
        "get_monitor_barge_secondary_conversation_by_conv_id",
        classmethod(lambda cls, cid: mapping),
    )
    monkeypatch.setattr(
        cxfer.ConvSecondaryAgentMapping,
        "delete_mapping",
        classmethod(lambda cls, cid, aom_id=None, **kw: deleted.append((cid, aom_id)) or 1),
    )
    create_called = []
    monkeypatch.setattr(cxfer, "create_child_conversation", lambda *a, **k: create_called.append(1))
    resp = cxfer.transfer_child_conversation(
        ConvTransferModel(contact_uuid="par-1", channel="chat", transfer_type="flow")
    )
    assert resp.status_code == 201
    assert resp.data.contact_uuid == "c-1"  # flow 201 carries the OLD child res
    assert create_called == []  # flow type creates NO child here
    assert deleted == [(11, 301)]
    left = [d for t, d, _ in emitted if t == "supervisor.left"]
    assert left == [{"supervisor_id": str(barger_uuid), "mode": "barge"}]


# ============================================================================ create_child
def test_create_child_transfer_generated_happy(monkeypatch):
    parent = _parent()
    new_aom = _aom()
    from hs.conversations.models import ConvInsertModel

    monkeypatch.setattr(
        ConvInsertModel, "build_db_model", lambda self, **kw: SimpleNamespace(start_at=None, channel=self.channel)
    )
    created_row = _conv(generation_type="transfer_generated", conv_id=88, primary=new_aom, parent=parent, contact="c-1")
    monkeypatch.setattr(cxfer.Conversation, "create_conversation", classmethod(lambda cls, row: created_row))
    monkeypatch.setattr(
        cxfer.ConvSecondaryAgentMapping,
        "get_monitor_barge_secondary_conversation_by_conv_id",
        classmethod(lambda cls, cid: None),
    )
    import hs.conversations.management as cm

    monkeypatch.setattr(cm, "PostConversationStatusChange", _Post)
    child = ConvInsertModel(
        generation_type="transfer_generated",
        state="agent_answered",
        parent_contact_uuid="par-1",
        channel="chat",
        contact_uuid="c-1",
        active_queue_entry_id=5,
    )
    child.parent = parent
    resp = cxfer.create_child_conversation(child, aom=new_aom, outbound_queue_transfer=True)
    assert (resp.status_code, resp.msg) == (201, "Child conversation added successfully")
    # Post(open) for the qnr-side child with a primary
    assert _Post.calls and _Post.calls[0]["changed_status"] == "open"


# ============================================================================ secondary participants
def test_enrich_secondary_guards(monkeypatch):
    monkeypatch.setattr(cxfer.Conversation, "get_primary_active_conversation", classmethod(lambda cls, *a, **k: None))
    resp = cxfer.enrich_secondary_participant(
        ConvSecAgentMappingInsert(conversation_number="CN1", agent_sip_or_uuid="sip:x@y")
    )
    assert (resp.status_code, resp.error) == (404, "No active conversation found for parent conversation")

    conv = _conv()
    monkeypatch.setattr(cxfer.Conversation, "get_primary_active_conversation", classmethod(lambda cls, *a, **k: conv))
    monkeypatch.setattr(cxfer, "fetch_aom_by_uuid_or_sip_and_set_context", lambda s: None)
    resp = cxfer.enrich_secondary_participant(
        ConvSecAgentMappingInsert(conversation_number="CN1", agent_sip_or_uuid="sip:x@y")
    )
    assert (resp.status_code, resp.error) == (404, "Agent not found for given sip_endpoint or aom_uuid")

    monkeypatch.setattr(cxfer, "fetch_aom_by_uuid_or_sip_and_set_context", lambda s: _aom(org=2))
    resp = cxfer.enrich_secondary_participant(
        ConvSecAgentMappingInsert(conversation_number="CN1", agent_sip_or_uuid="sip:x@y")
    )
    assert (resp.status_code, resp.error) == (422, "Secondary agent not in organization of conversation")

    monkeypatch.setattr(
        cxfer, "fetch_aom_by_uuid_or_sip_and_set_context", lambda s: _aom(aom_id=101, aom_uuid=OLD_AOM_UUID)
    )
    resp = cxfer.enrich_secondary_participant(
        ConvSecAgentMappingInsert(conversation_number="CN1", agent_sip_or_uuid="sip:x@y")
    )
    assert (resp.status_code, resp.error) == (422, "Cannot add same agent as a secondary")


def test_create_secondary_warm_transfer(monkeypatch, emitted):
    conv = _conv()
    sec = _aom()
    rows, parent_updates = [], []
    monkeypatch.setattr(cxfer.Conversation, "get_primary_active_conversation", classmethod(lambda cls, *a, **k: conv))
    monkeypatch.setattr(cxfer, "fetch_aom_by_uuid_or_sip_and_set_context", lambda s: sec)
    monkeypatch.setattr(ConvSecAgentMappingInsert, "build_db_model", lambda self, **kw: {"row": True})
    monkeypatch.setattr(
        cxfer.ConvSecondaryAgentMapping,
        "create_mapping",
        classmethod(lambda cls, m: rows.append(m) or SimpleNamespace(aom=sec.id)),
    )
    monkeypatch.setattr(
        cxfer.ConversationParent,
        "update_conversation",
        classmethod(lambda cls, cid, ud: parent_updates.append((cid, ud))),
    )
    monkeypatch.setattr(
        cxfer.ConversationParent,
        "update_last_activity_at",
        classmethod(lambda cls, conversation_id=None, now=None: None),
    )
    resp = cxfer.create_secondary_participant(
        ConvSecAgentMappingInsert(
            conversation_number="CN1",
            agent_sip_or_uuid=str(AOM_UUID),
            channel="chat",
            secondary_agent_state="transferred",
        )
    )
    assert (resp.status_code, resp.msg) == (201, "Secondary participant added successfully for conversation")
    assert rows == [{"row": True}]
    assert parent_updates == []
    assert emitted == []  # a warm-transfer secondary is no catalog event


def test_remove_secondary_participant(monkeypatch, emitted):
    conv = _conv()
    sec = _aom()
    monkeypatch.setattr(cxfer.Conversation, "get_primary_active_conversation", classmethod(lambda cls, *a, **k: conv))
    monkeypatch.setattr(cxfer, "fetch_aom_by_uuid_or_sip_and_set_context", lambda s: sec)
    monkeypatch.setattr(
        cxfer.ConvSecondaryAgentMapping,
        "get_secondary_conv_by_identifiers",
        classmethod(lambda cls, **kw: SimpleNamespace(secondary_agent_state=None)),
    )
    monkeypatch.setattr(cxfer.ConvSecondaryAgentMapping, "delete_mapping", classmethod(lambda cls, **kw: 0))
    resp = cxfer.remove_secondary_participant(
        ConvSecAgentMappingRemove(conversation_number="CN1", agent_sip_or_uuid=str(AOM_UUID))
    )
    assert (resp.status_code, resp.msg) == (422, "No secondary to remove")

    deletes = []
    monkeypatch.setattr(
        cxfer.ConvSecondaryAgentMapping, "delete_mapping", classmethod(lambda cls, **kw: deletes.append(kw) or 1)
    )
    monkeypatch.setattr(
        cxfer.ConversationParent,
        "update_last_activity_at",
        classmethod(lambda cls, conversation_id=None, now=None: None),
    )
    transferable = _conv(state="transferring")
    monkeypatch.setattr(
        cxfer.Conversation,
        "get_active_transferable_by_contact_uuid",
        classmethod(lambda cls, c, ch=None, p=None: transferable),
    )
    restored = []
    monkeypatch.setattr(cxfer, "update_failed_transfer_state", lambda conv_, c, ch: restored.append(c))
    resp = cxfer.remove_secondary_participant(
        ConvSecAgentMappingRemove(conversation_number="CN1", agent_sip_or_uuid=str(AOM_UUID), channel="chat")
    )
    assert (resp.status_code, resp.msg) == (200, "Secondary participant removed successfully for conversation")
    assert deletes[0]["aom_id"] == sec.id and deletes[0]["active"] is True
    assert restored == ["par-1"]  # BTS restore via update_failed_transfer_state
    assert emitted == []  # a transfer secondary leaving is no catalog event

    # bypass_all_events → row delete only
    restored.clear()
    resp = cxfer.remove_secondary_participant(
        ConvSecAgentMappingRemove(conversation_number="CN1", agent_sip_or_uuid=str(AOM_UUID), channel="chat"),
        bypass_all_events=True,
    )
    assert resp.status_code == 200 and restored == []


# ============================================================================ census close
def test_handle_non_transferred_conversations(monkeypatch):
    transfer_child = _conv(conv_id=77, contact="par-1")  # the FLOW-initiating child
    sibling = _conv(conv_id=78, contact="par-1", channel="task", state="agent_answered")
    closed, parent_updates = [], []
    monkeypatch.setattr(
        cxfer.Conversation, "get_active_non_transfer_child_conversations", classmethod(lambda cls, p, t, a: [sibling])
    )
    import hs.conversations.management as cm

    monkeypatch.setattr(cm.AgentConversationSetting, "set_or_get_setting", staticmethod(lambda org, cn: True))
    monkeypatch.setattr(
        cxfer.ConversationParent,
        "update_conversation",
        classmethod(lambda cls, conversation_id=None, update_dict=None: parent_updates.append(update_dict)),
    )
    monkeypatch.setattr(ccu, "close_conversation", lambda c, **kw: closed.append(c.id))
    monkeypatch.setattr(cm, "PostConversationStatusChange", _Post)
    resp = cxfer.handle_non_transferred_conversations(transfer_child)
    assert resp.msg == "Closed conversations from other channels"
    assert closed == [78]
    assert parent_updates == [{"flow_closure_flag": True}]  # contact==parent_contact & not transferred
    post = _Post.calls[0]
    assert post["changed_status"] == "closed" and str(post["timeout_type"]) in (
        "TimeoutTypes.session_timeout",
        "session_timeout",
    )
    assert post["send_conversation_closed_event"] is False and post["update_capacity_on_resolve"] is True


# ============================================================================ Post handlers
def _post(conv, changed, **kw):
    import hs.conversations.management as cm

    return cm.PostConversationStatusChange(
        aom_uuid=str(OLD_AOM_UUID),
        org_name="plivo",
        org_uuid=ORG_UUID,
        conversation=conv,
        changed_status=changed,
        aom=conv.primary_agent,
        **kw,
    )


def test_post_in_wrap_up_arms_the_wrap_up_timer(monkeypatch, _req_ctx):
    from hs.conversations import timers

    conv = _conv(channel="call")
    armed = []
    monkeypatch.setattr(timers, "start_wrap_up", lambda c, *actor: armed.append(c))
    _post(conv, "in_wrap_up")._in_wrap_up()
    assert armed == [conv]
    assert not _req_ctx, "a call resolves on its drop, not here"


def test_post_closed_dispatches_in_process_wrap_up(monkeypatch):
    import hs.conversations.management as cm
    from hs.conversations import wrap_up

    calls = {}
    monkeypatch.setattr(wrap_up, "handle_wrap_up_voice", lambda body, *a, **k: calls.setdefault("call", body) and None)
    seam = []
    monkeypatch.setattr(cm.jobs, "schedule", lambda kind, payload, delay: seam.append((kind, payload, delay)))
    conv = _conv(channel="call", status="open")
    p = _post(conv, "closed", timeout_type=None, update_capacity_on_resolve=True)
    p._closed()
    body = calls["call"]
    # update_capacity_on_resolve flips False for an OPEN conv → release_agent_capacity=True
    assert body.release_agent_capacity is True and body.conversation_number == "CN1"
    assert body.agent_uuid == str(OLD_AOM_UUID) and body.direction == "inbound"
    [(kind, payload, delay)] = seam  # the delayed capacity-release safety net
    assert (kind, payload["conversation_number"], delay) == ("capacity_clearance", "CN1", 5)


# ============================================================================ seam adapters
def test_adapter_failure_contracts(monkeypatch):
    err_resp = UseCaseResponse(status_code=400, error="boom")
    monkeypatch.setattr(cxfer, "create_secondary_participant", lambda m: err_resp)
    added = cxfer.add_secondary(
        conversation_number="CN1", agent_sip_or_uuid="sip:x@y", channel="chat", secondary_agent_state="transferred"
    )
    assert added == "adding the secondary agent failed"

    monkeypatch.setattr(cxfer, "remove_secondary_participant", lambda m, active=True: err_resp)
    removed = cxfer.remove_secondary(conversation_number="CN1", agent_sip_or_uuid=str(AOM_UUID), channel="chat")
    assert removed == "removing the secondary agent failed"
    nothing = UseCaseResponse(status_code=422, msg="No secondary to remove")
    monkeypatch.setattr(cxfer, "remove_secondary_participant", lambda m, active=True: nothing)
    removal = {"conversation_number": "CN1", "agent_sip_or_uuid": str(AOM_UUID), "channel": "chat"}
    assert cxfer.remove_secondary(**removal) == "removing the secondary agent failed"
    assert cxfer.remove_secondary(missing_ok=True, **removal) is None  # nothing to remove counts as removed

    monkeypatch.setattr(cxfer, "transfer_child_conversation", lambda c: err_resp)
    assert cxfer.transfer_child("par-1", "agent", "chat", "sip:x@y") == (
        "",
        "creating the transfer conversation failed",
    )

    monkeypatch.setattr(cxfer, "check_if_transfer_allowed", lambda *a, **k: err_resp)
    assert cxfer.transfer_allowed("par-1", "chat", True, False) == (False, False, {})


def test_adapter_success_contracts(monkeypatch):
    ok201 = UseCaseResponse(status_code=201, msg="ok")
    monkeypatch.setattr(cxfer, "create_secondary_participant", lambda m: ok201)
    added = cxfer.add_secondary(
        conversation_number="CN1", agent_sip_or_uuid="sip:x@y", channel="chat", secondary_agent_state="transferred"
    )
    assert added is None

    # the 201 data is the new child's model; "success" (flow, data=str) extracts "" (the
    # child_uuid-or-contact fallback then applies upstream)
    for data, want in ((SimpleNamespace(contact_uuid="c-8"), "c-8"), ("success", "")):
        monkeypatch.setattr(
            cxfer, "transfer_child_conversation", lambda c, mode, _d=data: UseCaseResponse(status_code=201, data=_d)
        )
        assert cxfer.transfer_child("par-1", "agent", "chat") == (want, None)

    monkeypatch.setattr(
        cxfer,
        "check_if_transfer_allowed",
        lambda *a, **k: UseCaseResponse(
            status_code=200,
            data={
                "transfer_allowed": True,
                "add_participant_allowed": True,
                "conversation_details": {"conversation_number": "CN1"},
            },
        ),
    )
    assert cxfer.transfer_allowed("par-1", "chat", True, True) == (True, True, {"conversation_number": "CN1"})


def test_transfer_child_treats_a_blank_agent_as_none(monkeypatch):
    seen = []
    monkeypatch.setattr(
        cxfer,
        "transfer_child_conversation",
        lambda c, mode: seen.append(c.agent_sip_or_uuid) or UseCaseResponse(status_code=201, data="success"),
    )
    cxfer.transfer_child("par-1", "agent", "chat", "")
    assert seen == [None]


def test_signal_chunk_adapter_never_raises(monkeypatch):
    def _boom(chunk):
        raise RuntimeError("db down")

    monkeypatch.setattr(ccu, "add_chunk_to_non_qnr_child", _boom)
    assert cxfer.signal_chunk("par-1", "chat", "TRANSFER_INITIATED", "id-1", "agent", "msg") is None

    sent = []
    monkeypatch.setattr(
        ccu, "add_chunk_to_non_qnr_child", lambda chunk: sent.append(chunk) or UseCaseResponse(status_code=201)
    )
    cxfer.signal_chunk("par-1", "chat", "TRANSFER_ACCEPTED", str(AOM_UUID), "agent", "Cara accepted")
    chunk = sent[0]
    assert chunk.type == "signal" and chunk.description == "TRANSFER_ACCEPTED"
    assert chunk.parent_contact_uuid == "par-1" and chunk.create_chunk is True and chunk.message_id == ""
    assert not (chunk.is_inbound_chunk or chunk.is_main_recording)
    attrs = chunk.attributes[0]
    sm = attrs.get("signal_message") if isinstance(attrs, dict) else attrs.signal_message
    ident = attrs.get("identifier") if isinstance(attrs, dict) else attrs.identifier
    itype = attrs.get("identifier_type") if isinstance(attrs, dict) else attrs.identifier_type
    assert (sm, ident, itype) == ("Cara accepted", str(AOM_UUID), "agent")


# ============================================================================ the HTTP route


# ============================================================================ secondary signal chunks
def _signal_recorders(monkeypatch):
    import hs.conversations.management as cm

    chunks, non_qnr = [], SimpleNamespace(parent_conversation=_parent())
    monkeypatch.setattr(
        cxfer.Conversation, "get_active_non_qnr_child_by_contact_uuid", classmethod(lambda cls, c: non_qnr)
    )
    monkeypatch.setattr(
        cxfer.mc.NonQNRChunkInsert,
        "build_db_model",
        lambda self, conversation=None: SimpleNamespace(get_model=lambda: (self.description, self.attributes)),
    )
    monkeypatch.setattr(cm, "create_chunk_with_related_data", chunks.append)
    monkeypatch.setattr(cxfer.AccountOrgMapping, "get_agent_name_by_aom_uuid", classmethod(lambda cls, u: "Sam"))
    return chunks


def _secondary(state, **kw):
    return SimpleNamespace(
        secondary_agent_state=state,
        parent_conversation=_parent(),
        active_child_conversation=_conv(),
        secondary_agent=_aom(),
        participant_identifier="+14085550000",
        participant_alias=None,
        agent_requested_coach=False,
        **kw,
    )


def test_add_participant_signal_and_event(monkeypatch, emitted):
    chunks = _signal_recorders(monkeypatch)
    state = cxfer.mc.SecondaryAgentStateEnum.add_participant_success
    cxfer.handle_events_and_chunks_add_secondary_participant(_secondary(state))
    [(description, attributes)] = [chunk.get_model() for chunk in chunks]
    assert description == "ADD_PARTICIPANT_SUCCESS"
    assert attributes[0].signal_message == "+14085550000 joined the conversation"
    [(kind, data, kw)] = emitted
    assert (kind, kw["agent_id"]) == ("call.participant_added", str(OLD_AOM_UUID))
    assert data["participant_id"] == "+14085550000" and data["role"] == "participant"


def test_supervisor_signal_persists_unless_a_monitor_pauses(monkeypatch, emitted):
    chunks = _signal_recorders(monkeypatch)
    update = cxfer.create_chunks_on_secondary_mapping_update
    mapping = _secondary("monitor_paused")
    update(mapping, "coach")
    [(description, attributes)] = [chunk.get_model() for chunk in chunks]
    assert description == "SUPERVISOR_LEFT" and attributes[0].agent_message == "Sam left the conversation"
    update(mapping, "monitor")
    assert len(chunks) == 1 and emitted == []  # pausing is no mode
    update(_secondary("barge"), "monitor")
    [(kind, data, kw)] = emitted
    assert (kind, data, kw["agent_id"]) == (
        "supervisor.mode_changed",
        {"supervisor_id": str(AOM_UUID), "mode": "barge"},
        str(OLD_AOM_UUID),
    )
