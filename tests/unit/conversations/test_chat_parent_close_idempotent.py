"""A flow's parent close (state: closed, is_flow_ended) must be idempotent against a parent that a session/wrap-up timeout
already closed: the active-only lookups miss it and the close used to 404 with
'Parent conversation not found' even though the parent exists. An already-closed
parent is a success — at most the flow-ended flag still needs to land.
"""

import types

import tests.conftest  # noqa: F401
from hs.conversations import child_update as ccu
from hs.conversations.models import UpdateParentByConversationNumber

CONV_NUMBER = "001-1977457783"


def _update(is_flow_ended=True):
    return UpdateParentByConversationNumber(
        conversation_number=CONV_NUMBER, parent_contact_uuid=None, state="closed", is_flow_ended=is_flow_ended
    )


def _closed_parent(flow_closure_flag=False):
    return types.SimpleNamespace(
        id=193320, conversation_number=CONV_NUMBER, flow_closure_flag=flow_closure_flag, end_at="2026-07-14T12:24:36Z"
    )


def _patch_lookups(monkeypatch, closed_parent):
    """Active-only lookup misses (the timeout already closed the parent); the
    unfiltered re-check finds `closed_parent` (or None for a truly unknown number)."""

    def _by_number(_conversation_number, org_uuid=None, active=False, for_update=False):
        return None if active else closed_parent

    monkeypatch.setattr(ccu.ConversationParent, "get_by_conversation_number", staticmethod(_by_number))


def test_already_closed_parent_is_a_success_and_lands_the_flow_ended_flag(monkeypatch):
    calls = {}
    _patch_lookups(monkeypatch, _closed_parent(flow_closure_flag=False))

    def _update_conversation(parent_id, update_dict=None, now=None, **_kw):
        calls["parent_id"], calls["update_dict"] = parent_id, update_dict

    monkeypatch.setattr(ccu.ConversationParent, "update_conversation", staticmethod(_update_conversation))
    response = ccu.update_parent_conversation_by_conversation_number(_update(is_flow_ended=True))
    assert response.error is None
    assert calls["parent_id"] == 193320
    assert calls["update_dict"] == {"flow_closure_flag": True}


def test_already_closed_parent_does_not_restamp_an_existing_flag(monkeypatch):
    _patch_lookups(monkeypatch, _closed_parent(flow_closure_flag=True))

    def _boom(*_a, **_k):
        raise AssertionError("must not rewrite an already-stamped parent")

    monkeypatch.setattr(ccu.ConversationParent, "update_conversation", staticmethod(_boom))
    response = ccu.update_parent_conversation_by_conversation_number(_update(is_flow_ended=True))
    assert response.error is None


def test_already_closed_parent_without_flow_end_is_a_plain_noop(monkeypatch):
    _patch_lookups(monkeypatch, _closed_parent(flow_closure_flag=False))

    def _boom(*_a, **_k):
        raise AssertionError("nothing to update when the flow did not end")

    monkeypatch.setattr(ccu.ConversationParent, "update_conversation", staticmethod(_boom))
    response = ccu.update_parent_conversation_by_conversation_number(_update(is_flow_ended=False))
    assert response.error is None


def test_truly_unknown_conversation_still_404s(monkeypatch):
    _patch_lookups(monkeypatch, None)
    response = ccu.update_parent_conversation_by_conversation_number(_update())
    assert response.status_code == 404
    assert response.error == ccu.ERROR_PARENT_CONVERSATION_NOT_FOUND
