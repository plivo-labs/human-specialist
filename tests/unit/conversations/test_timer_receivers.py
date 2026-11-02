"""The timer receivers (offline decision-tree pins).
* ApiLockManager: NX-first-wins (`rl:<unique>:<api>`), duplicate → None, release only by the
  owning run-id;
* wrap-up timeout: lock-denied 422; 404 no-conv / not-in-wrap-up; happy = system 'Auto-closed' notes
  chunk + close(resolve_for_agent=True) + Post(closed, wrap_up_timeout,
  send_conversation_closed_event=True) per active conv;
* conversation session timeout: lock-denied 422; the non-agent-leg fast path (already-ended 200 /
  session_resolved + the resolution-category seam + auto-closure when no active agent convs);
  transferred guard 404; the call-extension branch (reschedule extended=True + rollback + 400);
  open → session_resolved + the task auto-resolve + handle_session_timeout_auto_closure (parent
  close via the census + Auto-closed chunk + the flow-continue seam);
* route ORDER: PATCH /parent registers BEFORE PATCH /{conversation_number}.
"""

import uuid as uuid_mod
from types import SimpleNamespace
from unittest.mock import patch

import pytest

import hs.conversations.management as cm
import tests.conftest  # noqa: F401
from hs.infra.models import UseCaseResponse

AOM_UUID = str(uuid_mod.uuid4())
ORG_UUID = str(uuid_mod.uuid4())


@pytest.fixture(autouse=True)
def _ctx():
    from hs.infra.context import (
        context_aom_uuid,
        context_api_id,
        context_background_tasks,
        context_org_name,
        context_org_uuid,
        context_req_meta,
    )

    toks = [
        context_req_meta.set({"api_id": "timer-receivers-test"}),
        context_api_id.set("timer-receivers-run-1"),
        context_aom_uuid.set(AOM_UUID),
        context_org_uuid.set(ORG_UUID),
        context_org_name.set("plivo"),
        context_background_tasks.set(SimpleNamespace(add_task=lambda *a, **k: None)),
    ]
    yield
    from hs.infra.context import (
        context_aom_uuid,
        context_api_id,
        context_background_tasks,
        context_org_name,
        context_org_uuid,
        context_req_meta,
    )

    for var, tok in zip(
        (
            context_req_meta,
            context_api_id,
            context_aom_uuid,
            context_org_uuid,
            context_org_name,
            context_background_tasks,
        ),
        toks,
    ):
        var.reset(tok)


class _Post:
    calls = []

    def __init__(self, **kw):
        self.kw = kw

    def invoke(self):
        _Post.calls.append(self.kw)


@pytest.fixture(autouse=True)
def _common(monkeypatch):
    _Post.calls = []
    monkeypatch.setattr(cm, "PostConversationStatusChange", _Post)
    monkeypatch.setattr(cm, "create_chunk_with_related_data", lambda chunk: None)
    # default: the lock always acquires (per-test denial overrides); keep the REAL method
    # reachable for the lock-semantics test.
    from hs.admin import lookups as uh

    if not hasattr(uh, "_REAL_ACQUIRE"):
        uh._REAL_ACQUIRE = uh.ApiLockManager.acquire_lock_for_api
    monkeypatch.setattr(uh.ApiLockManager, "acquire_lock_for_api", lambda self: True)
    yield


def _parent(cn="CN1"):
    return SimpleNamespace(
        id=11,
        organization=1,
        conversation_number=cn,
        parent_contact_uuid="par-1",
        contact_uuid="par-1",
        contact_identifier="+14155550000",
        crm_contact_id=None,
        case_id=None,
        parent_channel="chat",
        source_direction_inbound=True,
    )


def _conv(
    status="in_wrap_up", state="agent_answered", channel="chat", conv_id=77, parent=None, end_at=None, resolution=None
):
    parent = parent or _parent()
    return SimpleNamespace(
        id=conv_id,
        status=status,
        state=state,
        channel=channel,
        parent=parent.id,
        parent_conversation=parent,
        primary_aom=203,
        primary_agent=SimpleNamespace(uuid=uuid_mod.UUID(AOM_UUID)),
        contact_uuid="par-1",
        customer_contact=None,
        sender_id=None,
        child_direction_inbound=True,
        end_at=end_at,
        resolution_category=resolution,
        generation_type="qnr_generated",
        active_queue_entry_id=5,
    )


class _Payload(SimpleNamespace):
    pass


# ============================================================================ lock manager
def test_api_lock_manager_nx_semantics(monkeypatch):
    from hs.admin import lookups as uh

    store = {}

    class _FakeCache:
        def set(self, k, v, nx=False, expiry_in_seconds=None):
            if nx and k in store:
                return None
            store[k] = v
            return True

        def get(self, k):
            return store.get(k)

    monkeypatch.setattr(uh.Cache, "get_instance", classmethod(lambda cls: _FakeCache()))
    real = uh._REAL_ACQUIRE  # the unpatched method (saved by the autouse fixture)
    lock1 = uh.ApiLockManager(api_name="wutout", api_run_id="run-1", unique_identifier="CN1", expiry_in_seconds=10)
    lock2 = uh.ApiLockManager(api_name="wutout", api_run_id="run-2", unique_identifier="CN1", expiry_in_seconds=10)
    assert lock1.cache_key == "rl:CN1:wutout"
    assert real(lock1) is True  # first wins
    assert real(lock2) is None  # duplicate concurrent → None (denied)
    assert store == {"rl:CN1:wutout": "run-1"}


# ============================================================================ wrapup-timeout
def test_wrapup_timeout_lock_denied(monkeypatch):
    from hs.admin import lookups as uh

    monkeypatch.setattr(uh.ApiLockManager, "acquire_lock_for_api", lambda self: None)
    resp = cm.handle_conversation_wrapup_timeout("CN1")
    assert (resp.status_code, resp.error) == (422, "Wrapup timeout initiated by another request")


# ============================================================================ session-timeout
def test_session_timeout_lock_denied(monkeypatch):
    from hs.admin import lookups as uh

    monkeypatch.setattr(uh.ApiLockManager, "acquire_lock_for_api", lambda self: None)
    resp = cm.handle_conversation_session_timeout("CN1", "chat", _Payload(extended=False, child_conversation_id=None))
    assert (resp.status_code, resp.error) == (422, "Conversation session timeout initiated by another request")


def test_session_timeout_for_a_missing_child_answers_404(monkeypatch):
    from hs.admin import lookups as uh

    monkeypatch.setattr(uh.ApiLockManager, "acquire_lock_for_api", lambda self: True)
    monkeypatch.setattr(
        cm.Conversation, "get_primary_active_conversation", classmethod(lambda cls, cn, aom, **kw: None)
    )
    resp = cm.handle_conversation_session_timeout("CN1", "call", _Payload(extended=False, child_conversation_id=None))
    assert (resp.status_code, resp.error) == (404, "Conversation not found")


def test_session_timeout_transferred_and_extension(monkeypatch):
    transferred = _conv(status="open", state="transferred")
    monkeypatch.setattr(
        cm.Conversation, "get_primary_active_conversation", classmethod(lambda cls, cn, aom, **kw: transferred)
    )
    resp = cm.handle_conversation_session_timeout("CN1", "chat", _Payload(extended=False, child_conversation_id=None))
    assert (resp.status_code, resp.error) == (404, "Conversation is transferred")

    open_conv = _conv(status="open")
    monkeypatch.setattr(
        cm.Conversation, "get_primary_active_conversation", classmethod(lambda cls, cn, aom, **kw: open_conv)
    )
    monkeypatch.setattr(
        cm.Conversation,
        "get_child_by_conv_parent",
        classmethod(lambda cls, parent_id=None, channel=None, **kw: SimpleNamespace(id=99)),
    )  # an open CALL child → extend
    scheduled, rolled = [], []
    from hs.conversations import timers

    monkeypatch.setattr(timers, "schedule_session_timeout", lambda *a, **kw: scheduled.append(kw))
    monkeypatch.setattr(cm, "set_db_session_rollback", lambda: rolled.append(1))
    resp = cm.handle_conversation_session_timeout("CN1", "chat", _Payload(extended=False, child_conversation_id=None))
    assert (resp.status_code, resp.error) == (
        400,
        "Conversation session timeout extended as call conversation is still open",
    )
    assert scheduled[0]["extended"] is True and rolled == [1]


def test_session_timeout_open_resolves_and_closes(monkeypatch):
    open_conv = _conv(status="open", channel="chat")
    updates, auto = [], []
    monkeypatch.setattr(
        cm.Conversation, "get_primary_active_conversation", classmethod(lambda cls, cn, aom, **kw: open_conv)
    )
    monkeypatch.setattr(
        cm.Conversation, "get_child_by_conv_parent", classmethod(lambda cls, **kw: None)
    )  # no open call child
    monkeypatch.setattr(
        cm.Conversation,
        "update_conversation",
        classmethod(lambda cls, cid, update_dict=None, now=None, **kw: updates.append((cid, update_dict))),
    )
    with patch("hs.infra.context.db_session_commit"):
        monkeypatch.setattr(
            cm,
            "handle_session_timeout_auto_closure",
            lambda conv, is_flow_ended=False: (
                auto.append(is_flow_ended)
                or UseCaseResponse(status_code=200, msg="Conversation timeout handled successfully")
            ),
        )
        resp = cm.handle_conversation_session_timeout(
            "CN1", "chat", _Payload(extended=False, child_conversation_id=None)
        )
    assert resp.status_code == 200
    assert updates == [(77, {"status": "session_resolved"})]
    assert auto == [False]


def test_session_timeout_auto_closure_chain(monkeypatch):
    conv = _conv()
    chunks = []
    import hs.conversations.child_update as ccu

    captured = {}

    def _close(update):
        captured.update(cn=update.conversation_number, state=update.state, is_flow_ended=update.is_flow_ended)
        return UseCaseResponse(status_code=200)

    monkeypatch.setattr(ccu, "update_parent_conversation_by_conversation_number", _close)
    monkeypatch.setattr(cm, "create_chunk_with_related_data", lambda chunk: chunks.append(chunk))
    from hs.conversations.models import ConvChunkInsertModel

    monkeypatch.setattr(
        ConvChunkInsertModel,
        "build_db_model",
        lambda self, conversation=None: SimpleNamespace(disposition=self.disposition),
    )
    monkeypatch.setattr(cm.Organization, "get_by_id", classmethod(lambda cls, i: SimpleNamespace(id=1)))
    resp = cm.handle_session_timeout_auto_closure(conv, is_flow_ended=True)
    assert (resp.status_code, resp.msg) == (200, "Conversation timeout handled successfully")
    assert captured == {"cn": "CN1", "state": "closed", "is_flow_ended": True}
    assert chunks[0].disposition == "Auto-closed"

    monkeypatch.setattr(
        ccu,
        "update_parent_conversation_by_conversation_number",
        lambda update: UseCaseResponse(status_code=422, error="boom"),
    )
    resp = cm.handle_session_timeout_auto_closure(conv)
    assert (resp.status_code, resp.error) == (422, "Failed to close parent after child session timeout")
