"""Child conversation update (accept / unanswered / outbound states), as decision-tree pins:
* the request-model mutual-exclusion validator;
* accept guards: 404 no-conversation, 400 no-aom / org-mismatch / already-linked;
* accept happy path: update_dict shape (primary_aom + agent_answered + start_at + in_progress_human),
  the chat/call/message OPEN pre-write, PostConversationStatusChange(open),
  last_activity bump;
* task accept → the task routing status update (accepted); manager error → rollback + propagate;
* unanswered: task failed-vs-declined mapping (the notes chunk is still created for tasks);
  no-task → notes chunk + close;
* the seam adapters: None on 200; the fixed 'updating the child conversation failed'
  string on any non-200/exception; unanswered swallows;
* ChildConversationStateManagement: channel/generation-type validation 422s, 404 no child,
  outbound_contact_answered prepares OUTBOUND_CALL_CUSTOMER_ADDED + start_at/state update_dict;
* get_conversation_custom_state_by_name: cache-hit short-circuit vs DB refresh + cache fill.
"""

import uuid as uuid_mod
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import tests.conftest  # noqa: F401 — eager data_adapter imports (AbstractConcreteBase registry)
from hs.conversations import child_update as ccu
from hs.conversations.models import ConversationUpdateByContactUUID
from hs.infra.models import UseCaseResponse
from hs.routing import invitation as ri

AOM_UUID = uuid_mod.uuid4()


@pytest.fixture(autouse=True)
def _req_ctx():
    from hs.infra.context import context_background_tasks, context_req_meta

    t1 = context_req_meta.set({"api_id": "child-update-test"})
    bg = SimpleNamespace(add_task=lambda *a, **k: None)
    t2 = context_background_tasks.set(bg)
    yield
    context_req_meta.reset(t1)
    context_background_tasks.reset(t2)


PARENT_UUID = uuid_mod.uuid4()


def _parent(org=1, cn="CN1", contact_uuid="par-1"):
    return SimpleNamespace(
        id=11,
        uuid=PARENT_UUID,
        organization=org,
        conversation_number=cn,
        parent_contact_uuid=contact_uuid,
        contact_uuid=contact_uuid,
        contact_identifier="+14155550000",
        contact_name="Cust",
        crm_contact_id=None,
        parent_channel="chat",
        is_hidden=False,
        flow_closure_flag=False,
        flow_run_uuid=None,
        parent_end_at=None,
        source_direction_inbound=True,
    )


def _conv(
    channel="chat",
    generation_type="qnr_generated",
    primary_aom=None,
    conv_id=77,
    contact_uuid="c-1",
    state="agent_unanswered",
    status="open",
):
    parent = _parent()
    c = SimpleNamespace(
        id=conv_id,
        channel=channel,
        generation_type=generation_type,
        primary_aom=primary_aom,
        contact_uuid=contact_uuid,
        state=state,
        status=status,
        parent=parent.id,
        parent_conversation=parent,
        primary_agent=None,
        customer_contact=None,
        sender_id=None,
        end_at=None,
    )
    c.copy = lambda update=None, _c=c: SimpleNamespace(**{**_c.__dict__, **(update or {})})
    return c


def _aom(org=1):
    return SimpleNamespace(
        id=203,
        uuid=AOM_UUID,
        organization=org,
        org=SimpleNamespace(name="plivo", uuid=str(uuid_mod.uuid4()), id=org),
        sip_endpoint="sip:agent1@sip.example.com",
    )


_OPEN = SimpleNamespace(id=2, state="open")
_IPH = SimpleNamespace(id=4, state="in_progress_human")


def _patch_custom_states(mp):
    mp.setattr(
        ccu, "get_conversation_custom_state_by_name", lambda org_id, state_name: _OPEN if state_name == "open" else _IPH
    )


# ---------------------------------------------------------------------------- model validation
def test_model_rejects_mixed_update_parameters():
    for kw in (
        {"agent_sip_or_uuid": "s", "reason_for_unanswered": "r"},
        {"state": "outbound_contact_answered", "agent_sip_or_uuid": "s"},
        {"reason_for_unanswered": "r", "status": "open"},
    ):
        with pytest.raises(Exception):
            ConversationUpdateByContactUUID(contact_uuid="c1", **kw)


# ---------------------------------------------------------------------------- accept guards
def test_accept_404_when_no_unanswered_qnr_child(monkeypatch):
    monkeypatch.setattr(
        ccu.Conversation,
        "get_active_unanswered_qnr_child_by_contact_uuid",
        classmethod(lambda cls, c, channel=None, active_queue_id=0: None),
    )
    monkeypatch.setattr(ccu, "fetch_aom_by_uuid_or_sip_and_set_context", lambda s: None)
    resp = ccu.update_child_conversation(
        ConversationUpdateByContactUUID(contact_uuid="c-1", agent_sip_or_uuid="sip:x@y")
    )
    assert (resp.status_code, resp.error) == (404, "Conversation not found")


def test_accept_exact_same_agent_replay_is_idempotent(monkeypatch):
    aom = _aom()
    accepted = _conv(channel="call", primary_aom=aom.id, state="agent_answered")
    monkeypatch.setattr(
        ccu.Conversation,
        "get_active_unanswered_qnr_child_by_contact_uuid",
        classmethod(lambda cls, c, channel=None, active_queue_id=0: None),
    )
    monkeypatch.setattr(
        ccu.Conversation, "get_conversation_for_child_contact_uuid", classmethod(lambda cls, c, aom_id=None: accepted)
    )
    monkeypatch.setattr(ccu, "fetch_aom_by_uuid_or_sip_and_set_context", lambda s: aom)
    monkeypatch.setattr(
        ccu.Conversation,
        "update_conversation",
        classmethod(
            lambda *args, **kwargs: pytest.fail("same-agent replay must not rewrite or re-emit accepted state")
        ),
    )

    resp = ccu.update_child_conversation(
        ConversationUpdateByContactUUID(
            contact_uuid="c-1", agent_sip_or_uuid="sip:agent1@sip.example.com", channel="call"
        )
    )

    assert resp.status_code == 200
    assert resp.error is None
    assert resp.data is accepted


def test_accept_replay_does_not_adopt_different_or_terminal_child(monkeypatch):
    aom = _aom()
    candidate = _conv(channel="call", primary_aom=aom.id + 1, state="agent_answered")
    monkeypatch.setattr(
        ccu.Conversation,
        "get_active_unanswered_qnr_child_by_contact_uuid",
        classmethod(lambda cls, c, channel=None, active_queue_id=0: None),
    )
    monkeypatch.setattr(
        ccu.Conversation, "get_conversation_for_child_contact_uuid", classmethod(lambda cls, c, aom_id=None: candidate)
    )
    monkeypatch.setattr(ccu, "fetch_aom_by_uuid_or_sip_and_set_context", lambda s: aom)

    resp = ccu.update_child_conversation(
        ConversationUpdateByContactUUID(
            contact_uuid="c-1", agent_sip_or_uuid="sip:agent1@sip.example.com", channel="call"
        )
    )

    assert (resp.status_code, resp.error) == (404, "Conversation not found")


def test_accept_400_when_aom_missing(monkeypatch):
    monkeypatch.setattr(
        ccu.Conversation,
        "get_active_unanswered_qnr_child_by_contact_uuid",
        classmethod(lambda cls, c, channel=None, active_queue_id=0: _conv()),
    )
    monkeypatch.setattr(ccu, "fetch_aom_by_uuid_or_sip_and_set_context", lambda s: None)
    resp = ccu.update_child_conversation(
        ConversationUpdateByContactUUID(contact_uuid="c-1", agent_sip_or_uuid="sip:x@y")
    )
    assert (resp.status_code, resp.error) == (400, "Agent not found for given sip_endpoint or aom_uuid")


def test_accept_400_on_org_mismatch_and_already_linked(monkeypatch):
    conv = _conv()
    monkeypatch.setattr(
        ccu.Conversation,
        "get_active_unanswered_qnr_child_by_contact_uuid",
        classmethod(lambda cls, c, channel=None, active_queue_id=0: conv),
    )
    monkeypatch.setattr(ccu, "fetch_aom_by_uuid_or_sip_and_set_context", lambda s: _aom(org=2))
    resp = ccu.update_child_conversation(
        ConversationUpdateByContactUUID(contact_uuid="c-1", agent_sip_or_uuid="sip:x@y")
    )
    assert (resp.status_code, resp.error) == (400, "Agent and conversation are not in the same organization")

    aom = _aom()
    conv2 = _conv(primary_aom=aom.id)
    monkeypatch.setattr(
        ccu.Conversation,
        "get_active_unanswered_qnr_child_by_contact_uuid",
        classmethod(lambda cls, c, channel=None, active_queue_id=0: conv2),
    )
    monkeypatch.setattr(ccu, "fetch_aom_by_uuid_or_sip_and_set_context", lambda s: aom)
    resp = ccu.update_child_conversation(
        ConversationUpdateByContactUUID(contact_uuid="c-1", agent_sip_or_uuid="sip:x@y")
    )
    assert (resp.status_code, resp.error) == (400, "Another agent already linked to the conversation")


def test_accept_guards_compare_a_str_agent_id_with_the_int_primary_aom(monkeypatch):
    """The agent model's id is a str, the child's primary_aom an int: both replay guards must still fire."""
    aom = _aom()
    aom.id = str(aom.id)
    accepted = _conv(channel="call", primary_aom=int(aom.id), state="agent_answered")
    lookup = classmethod(lambda cls, c, channel=None, active_queue_id=0: None)
    monkeypatch.setattr(ccu.Conversation, "get_active_unanswered_qnr_child_by_contact_uuid", lookup)
    monkeypatch.setattr(
        ccu.Conversation, "get_conversation_for_child_contact_uuid", classmethod(lambda cls, c, aom_id=None: accepted)
    )
    monkeypatch.setattr(ccu, "fetch_aom_by_uuid_or_sip_and_set_context", lambda s: aom)
    update = ConversationUpdateByContactUUID(contact_uuid="c-1", agent_sip_or_uuid="sip:x@y", channel="call")
    assert ccu.update_child_conversation(update).data is accepted

    linked = _conv(primary_aom=int(aom.id))
    monkeypatch.setattr(
        ccu.Conversation,
        "get_active_unanswered_qnr_child_by_contact_uuid",
        classmethod(lambda cls, c, channel=None, active_queue_id=0: linked),
    )
    resp = ccu.update_child_conversation(update)
    assert (resp.status_code, resp.error) == (400, "Another agent already linked to the conversation")


# ---------------------------------------------------------------------------- accept happy path
class _AcceptHarness:
    def __init__(self, monkeypatch, conv=None, aom=None, first_non_qnr_channel="chat"):
        self.conv = conv or _conv()
        self.aom = aom or _aom()
        self.updates = []
        self.last_activity = []
        self.post_status = MagicMock()
        m = monkeypatch
        m.setattr(
            ccu.Conversation,
            "get_active_unanswered_qnr_child_by_contact_uuid",
            classmethod(lambda cls, c, channel=None, active_queue_id=0: self.conv),
        )
        m.setattr(
            ccu.Conversation,
            "update_conversation",
            classmethod(
                lambda cls, conversation_id, update_dict, now=None, update_status_change_timestamp_for_state=None: (
                    self.updates.append((conversation_id, dict(update_dict), update_status_change_timestamp_for_state))
                )
            ),
        )
        m.setattr(
            ccu.ConversationParent,
            "update_last_activity_at",
            classmethod(lambda cls, conversation_id, now=None: self.last_activity.append(conversation_id)),
        )
        m.setattr(ccu, "fetch_aom_by_uuid_or_sip_and_set_context", lambda s: self.aom)
        _patch_custom_states(m)

        import hs.conversations.management as cm

        m.setattr(
            cm, "PostConversationStatusChange", lambda **kw: SimpleNamespace(invoke=lambda: self.post_status(**kw))
        )
        m.setattr(ccu, "get_db_session", lambda: SimpleNamespace(commit=lambda: None))


def test_accept_happy_path_chat(monkeypatch):
    h = _AcceptHarness(monkeypatch)
    resp = ccu.update_child_conversation(
        ConversationUpdateByContactUUID(contact_uuid="c-1", agent_sip_or_uuid="sip:agent1@sip.example.com")
    )
    assert resp.status_code == 200 and resp.msg == "Conversation updated successfully"
    # chat ⇒ TWO update_conversation calls: the OPEN pre-write, then the main dict
    assert len(h.updates) == 2
    open_write, main_write = h.updates
    assert open_write[1] == {"custom_status_mapping": _OPEN.id} and open_write[2] == "open"
    assert main_write[1]["primary_aom"] == h.aom.id
    assert main_write[1]["state"] == "agent_answered"
    assert "start_at" in main_write[1]
    assert main_write[1]["custom_status_mapping"] == _IPH.id and main_write[2] == "in_progress_human"
    # PostConversationStatusChange(open) invoked; last_activity bumped
    h.post_status.assert_called_once()
    assert h.post_status.call_args.kwargs["changed_status"] == "open"
    assert h.last_activity == [h.conv.parent_conversation.id]


def test_accept_transfer_meta_and_chunk_backfill(monkeypatch):
    conv = _conv(generation_type="transfer_qnr_generated")
    h = _AcceptHarness(monkeypatch, conv=conv)
    backfills = []
    monkeypatch.setattr(
        ccu.ConversationChunk,
        "get_conversation_chunks_by_conv_number",
        classmethod(lambda cls, cn, filter_empty_to_user=False, filter_chunk_types=None: [SimpleNamespace(id=901)]),
    )
    monkeypatch.setattr(
        ccu.ConversationChunk,
        "update_conversation_chunk_by_id",
        classmethod(lambda cls, cid, d, now=None: backfills.append((cid, d))),
    )
    resp = ccu.update_child_conversation(
        ConversationUpdateByContactUUID(contact_uuid="c-1", agent_sip_or_uuid="sip:agent1@sip.example.com")
    )
    assert resp.status_code == 200
    assert backfills == [(901, {"to_user": str(h.aom.uuid)})]


# ---------------------------------------------------------------------------- unanswered branch
def test_unanswered_no_task_creates_notes_chunk_and_closes(monkeypatch):
    conv = _conv()
    monkeypatch.setattr(
        ccu.Conversation,
        "get_active_unanswered_qnr_child_by_contact_uuid",
        classmethod(lambda cls, c, channel=None, active_queue_id=0: conv),
    )
    monkeypatch.setattr(ccu, "fetch_org_by_id_and_set_context", lambda org_id: ("", SimpleNamespace(id=1)))
    monkeypatch.setattr("hs.conversations.outcomes.system_disposition", lambda parent_id, result: f"Mapped {result}")
    chunks = []
    monkeypatch.setattr(
        ccu,
        "add_chunk_to_non_qnr_child",
        lambda non_qnr_chunk: (chunks.append(non_qnr_chunk), UseCaseResponse(status_code=201))[1],
    )
    closed = []
    monkeypatch.setattr(ccu, "close_conversation", lambda conversation, **kw: closed.append(conversation.id))
    resp = ccu.update_child_conversation(
        ConversationUpdateByContactUUID(contact_uuid="c-1", reason_for_unanswered="queue_timeout")
    )
    assert resp.status_code == 200
    assert len(chunks) == 1 and chunks[0].type == "notes" and chunks[0].disposition == "Mapped queue_timeout"
    assert closed == [conv.id]


def test_unanswered_seam_swallows_everything(monkeypatch):
    monkeypatch.setattr(ccu, "update_child_conversation", lambda update: (_ for _ in ()).throw(RuntimeError("boom")))
    assert ri._update_unanswered_inprocess("c-1", "Queue timeout") is None
    monkeypatch.setattr(ccu, "update_child_conversation", lambda update: UseCaseResponse(status_code=422, error="x"))
    assert ri._update_unanswered_inprocess("c-1", "Queue timeout", "12") is None


# ---------------------------------------------------------------------------- state manager
def test_state_manager_404_and_validation_422s(monkeypatch):
    monkeypatch.setattr(ccu.Conversation, "get_outbound_child_by_contact_uuid", classmethod(lambda cls, c: None))
    resp = ccu.ChildConversationStateManagement(
        state="outbound_contact_answered", child_contact_uuid="c-1", now=None
    ).invoke()
    assert (resp.status_code, resp.error) == (404, "Conversation not found")

    child = _conv(channel="chat", generation_type="outbound_generated")
    child.primary_agent = SimpleNamespace(uuid=AOM_UUID)
    monkeypatch.setattr(ccu.Conversation, "get_outbound_child_by_contact_uuid", classmethod(lambda cls, c: child))
    monkeypatch.setattr(ccu, "fetch_aom_by_uuid_or_sip_and_set_context", lambda s: _aom())
    resp = ccu.ChildConversationStateManagement(
        state="outbound_contact_answered", child_contact_uuid="c-1", now=None
    ).invoke()
    assert (resp.status_code, resp.error) == (422, "state not supported for channel")

    child2 = _conv(channel="call", generation_type="qnr_generated")
    child2.primary_agent = SimpleNamespace(uuid=AOM_UUID)
    monkeypatch.setattr(ccu.Conversation, "get_outbound_child_by_contact_uuid", classmethod(lambda cls, c: child2))
    resp = ccu.ChildConversationStateManagement(
        state="outbound_contact_answered", child_contact_uuid="c-1", now=None
    ).invoke()
    assert (resp.status_code, resp.error) == (422, "state not supported for generation_type")


def test_state_outbound_contact_answered_happy(monkeypatch, emitted):
    from hs.conversations import timers

    monkeypatch.setattr(timers, "schedule_session_timeout", lambda *a, **kw: None)
    child = _conv(channel="call", generation_type="outbound_generated")
    child.primary_agent = SimpleNamespace(uuid=AOM_UUID)
    monkeypatch.setattr(ccu.Conversation, "get_outbound_child_by_contact_uuid", classmethod(lambda cls, c: child))
    monkeypatch.setattr(ccu, "fetch_aom_by_uuid_or_sip_and_set_context", lambda s: _aom())
    updates = []
    monkeypatch.setattr(
        ccu.Conversation,
        "update_conversation",
        classmethod(
            lambda cls, conversation_id, update_dict, now=None, update_status_change_timestamp_for_state=None: (
                updates.append(dict(update_dict))
            )
        ),
    )
    bumped = []
    monkeypatch.setattr(
        ccu.ConversationParent,
        "update_last_activity_at",
        classmethod(lambda cls, conversation_id, now=None: bumped.append(conversation_id)),
    )
    resp = ccu.ChildConversationStateManagement(
        state="outbound_contact_answered", child_contact_uuid="c-1", now="NOW"
    ).invoke()
    assert resp.status_code == 200 and resp.msg == "Conversation updated successfully"
    assert updates == [{"start_at": "NOW", "state": "outbound_contact_answered"}]
    assert bumped == [child.parent_conversation.id]
    assert [(t, d, kw["agent_id"]) for t, d, kw in emitted] == [("call.answered", {"call_uuid": "c-1"}, str(AOM_UUID))]


# ---------------------------------------------------------------------------- custom-state cache
def test_custom_state_cache_hit_skips_db(monkeypatch):
    import hs.admin.lookups as uh

    monkeypatch.setattr(uh, "cached_custom_state", lambda org_id, state_name: SimpleNamespace(id=9, state=state_name))
    db_called = []
    monkeypatch.setattr(
        ccu.ConversationCustomStatusMapping,
        "get_all_states_by_org",
        classmethod(lambda cls, org_id: db_called.append(1) or []),
    )
    got = ccu.get_conversation_custom_state_by_name(1, "open")
    assert got.id == 9 and db_called == []


def test_custom_state_cache_miss_reads_db_and_fills_cache(monkeypatch):
    import hs.admin.lookups as uh

    monkeypatch.setattr(uh, "cached_custom_state", lambda org_id, state_name: None)
    filled = []
    monkeypatch.setattr(uh, "cache_custom_states", lambda org_id, states: filled.append([s.state for s in states]))
    rows = [SimpleNamespace(id=2, state="open"), SimpleNamespace(id=4, state="in_progress_human")]
    monkeypatch.setattr(
        ccu.ConversationCustomStatusMapping, "get_all_states_by_org", classmethod(lambda cls, org_id: rows)
    )
    got = ccu.get_conversation_custom_state_by_name(1, "in_progress_human")
    assert got.id == 4 and got.state == "in_progress_human"
    assert filled == [["open", "in_progress_human"]]


# ------------------------------------------------- outbound-answer session-timeout arming
# Without a session timeout on OUTBOUND call conversations an abandoned dropped call leaves
# the child open forever and the parent-close timer retries without end.
# _state_outbound_contact_answered must arm the call session timeout, and a scheduling
# failure must NOT fail the answer callback (best-effort).
def _outbound_answer_env(monkeypatch):
    child = _conv(channel="call", generation_type="outbound_generated")
    child.primary_agent = SimpleNamespace(uuid=AOM_UUID)
    monkeypatch.setattr(ccu.Conversation, "get_outbound_child_by_contact_uuid", classmethod(lambda cls, c: child))
    monkeypatch.setattr(ccu, "fetch_aom_by_uuid_or_sip_and_set_context", lambda s: _aom())
    monkeypatch.setattr(
        ccu.Conversation,
        "update_conversation",
        classmethod(
            lambda cls, conversation_id, update_dict, now=None, update_status_change_timestamp_for_state=None: None
        ),
    )
    monkeypatch.setattr(
        ccu.ConversationParent, "update_last_activity_at", classmethod(lambda cls, conversation_id, now=None: None)
    )
    return child


def test_state_outbound_contact_answered_arms_call_session_timeout(monkeypatch):
    child = _outbound_answer_env(monkeypatch)
    armed = []
    from hs.conversations import timers

    monkeypatch.setattr(timers, "schedule_session_timeout", lambda *a, **kw: armed.append(a))
    # the job runs as this agent and org, so all three must reach it
    from hs.infra.context import context_aom_uuid, context_org_name, context_org_uuid

    t_aom = context_aom_uuid.set(AOM_UUID)
    t_org_name = context_org_name.set("acme")
    t_org_uuid = context_org_uuid.set("org-uuid-1")
    try:
        resp = ccu.ChildConversationStateManagement(
            state="outbound_contact_answered", child_contact_uuid="c-1", now="NOW"
        ).invoke()
    finally:
        context_aom_uuid.reset(t_aom)
        context_org_name.reset(t_org_name)
        context_org_uuid.reset(t_org_uuid)
    assert resp.status_code == 200
    assert armed == [(child.parent_conversation.conversation_number, "call", AOM_UUID, "org-uuid-1", "acme")]


def _record_child_lookups(monkeypatch, order):
    """All three child lookups miss, recording the order they were consulted in."""
    monkeypatch.setattr(
        ccu.Conversation,
        "get_active_non_qnr_child_by_contact_uuid",
        classmethod(lambda cls, c, filter_active=True: order.append(("nonqnr", filter_active)) or None),
    )


def _recording_chunk(**flags):
    from hs.conversations.models import NonQNRChunkInsert

    return NonQNRChunkInsert(
        parent_contact_uuid="ct-1", is_inbound_chunk=True, channel="call", type="recording", description="", **flags
    )


# ---------------------------------------------------------------------------- call_failure
def test_model_accepts_failure_context_with_state():
    from pydantic import ValidationError

    upd = ConversationUpdateByContactUUID(
        contact_uuid="c-1",
        state="outbound_contact_failed",
        failure_reason="error in increasing agent capacity",
        hangup_cause_code="3010",
        hangup_source="Carrier",
    )
    assert upd.hangup_cause_code == "3010"
    with pytest.raises(ValidationError):
        ConversationUpdateByContactUUID(contact_uuid="c-1", state="outbound_contact_failed", nonsense="x")
    with pytest.raises(ValidationError):
        ConversationUpdateByContactUUID(contact_uuid="c-1", state="outbound_contact_failed", hangup_cause_code="")


def _run_outbound_failed(monkeypatch, emitted, extra_update_data_dict):
    import hs.conversations.management as cm

    child = _conv(channel="call", generation_type="outbound_generated")
    child.primary_agent = SimpleNamespace(uuid=AOM_UUID)
    non_qnr = _conv(channel="chat", conv_id=88)
    monkeypatch.setattr(ccu.Conversation, "get_outbound_child_by_contact_uuid", classmethod(lambda cls, c: child))
    monkeypatch.setattr(
        ccu.Conversation, "get_active_non_qnr_child_by_contact_uuid", classmethod(lambda cls, c: non_qnr)
    )
    monkeypatch.setattr(ccu, "fetch_aom_by_uuid_or_sip_and_set_context", lambda s: _aom())

    class _Chunk:
        def __init__(s, **kw):
            pass

        def build_db_model(s, conversation=None):
            return s

        def get_model(s):
            return {"chunk": "CHUNK"}

    import hs.conversations.outcomes as outcomes

    monkeypatch.setattr(ccu, "NonQNRChunkInsert", _Chunk)
    monkeypatch.setattr(ccu, "ConvChunkInsertModel", _Chunk)
    monkeypatch.setattr(outcomes, "system_disposition", lambda pid, result: emitted.append(("disposition", result, 0)))
    monkeypatch.setattr(cm, "create_chunk_with_related_data", lambda c: None)
    monkeypatch.setattr(ccu, "close_conversation", lambda conv, state=None: None)
    monkeypatch.setattr(cm.AgentConversationSetting, "set_or_get_setting", staticmethod(lambda org, cn: False))

    class _PSC:
        def __init__(s, **kw):
            pass

        def invoke(s):
            return None

    monkeypatch.setattr(cm, "PostConversationStatusChange", _PSC)
    resp = ccu.ChildConversationStateManagement(
        state="outbound_contact_failed",
        child_contact_uuid="c-1",
        now="NOW",
        extra_update_data_dict=extra_update_data_dict,
    ).invoke()
    return resp, [(t, d) for t, d, _ in emitted]


def test_state_outbound_contact_failed_forwards_call_failure(monkeypatch, emitted):
    # Round-trip through the real request model, exactly as the route does
    # (update.model_dump() is what becomes extra_update_data_dict).
    update = ConversationUpdateByContactUUID(
        contact_uuid="c-1",
        state="outbound_contact_failed",
        failure_reason="error in increasing agent capacity",
        hangup_cause_code="8011",
        hangup_source="Error",
    )
    resp, events = _run_outbound_failed(monkeypatch, emitted, update.model_dump())
    assert resp.status_code == 200
    assert events[-1] == ("call.failed", {"result": "failed_carrier", "hangup_cause_code": 8011})


@pytest.mark.parametrize(("code", "result"), [("3010", "busy"), ("6010", "no_answer"), ("3020", "rejected")])
def test_state_outbound_contact_failed_maps_the_cause_to_a_result(monkeypatch, emitted, code, result):
    update = ConversationUpdateByContactUUID(
        contact_uuid="c-1", state="outbound_contact_failed", hangup_cause_code=code
    )
    _resp, events = _run_outbound_failed(monkeypatch, emitted, update.model_dump())
    assert events == [("disposition", result), ("call.failed", {"result": result, "hangup_cause_code": int(code)})]


def test_state_outbound_contact_failed_without_context(monkeypatch, emitted):
    update = ConversationUpdateByContactUUID(contact_uuid="c-1", state="outbound_contact_failed")
    resp, events = _run_outbound_failed(monkeypatch, emitted, update.model_dump())
    assert resp.status_code == 200
    assert events[-1] == ("call.failed", {"result": "failed_carrier", "hangup_cause_code": None})


# ---------------------------------------------------------------------------- accept drain
class _DrainCache:
    """redis list fake: lrange snapshot + head-guarded ltrim (no delete: the accept path must never DEL)."""

    def __init__(self, items):
        self.items = list(items)

    def lrange(self, key, start=0, end=-1):
        return list(self.items)

    def ltrim_if_head_matches(self, key, start, expected_head):
        if not self.items or self.items[0] != expected_head:
            return False
        self.items = self.items[start:]
        return True
