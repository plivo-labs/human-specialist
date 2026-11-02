"""Session-timeout capacity release.

_handle_session_timeout_post_status_change must hand PostConversationStatusChange a timeout_type
that survives the callee's `self.timeout_type.value` wrapup_data expression. A plain str
(TimeoutTypes.session_timeout.name) fails there ('str' object has no attribute 'value') and
skips the agent capacity release.
"""

import uuid as uuid_mod
from types import SimpleNamespace
from unittest.mock import patch

import pytest

import tests.conftest  # noqa: F401 — eager data_adapter imports (AbstractConcreteBase registry)
from hs.conversations import child_update as ccu


@pytest.fixture(autouse=True)
def _req_ctx():
    from hs.infra.context import context_req_meta

    t = context_req_meta.set({"api_id": "session-timeout-test"})
    yield
    context_req_meta.reset(t)


def _conv():
    parent = SimpleNamespace(organization=1, conversation_number="CN1")
    agent = SimpleNamespace(uuid=uuid_mod.uuid4())
    return SimpleNamespace(id=77, parent_conversation=parent, primary_aom=5, primary_agent=agent)


def test_session_timeout_seam_timeout_type_survives_value_access():
    conv = _conv()
    captured = {}

    class _FakePost:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def invoke(self):
            captured["invoked"] = True

    org = SimpleNamespace(name="org", uuid=str(uuid_mod.uuid4()))
    with (
        patch("hs.conversations.management.PostConversationStatusChange", _FakePost),
        patch("hs.admin.account.Organization") as org_da,
    ):
        org_da.get_by_id.return_value = org
        ccu._handle_session_timeout_post_status_change([conv])

    assert captured.get("invoked") is True
    # The callee builds wrapup_data with `self.timeout_type.value`; a `.name` string here
    # raises AttributeError inside the close flow and the capacity release is skipped.
    assert captured["timeout_type"].value == "session_timeout"
    assert captured["changed_status"] == "closed"
    assert captured["update_capacity_on_resolve"] is False
    assert captured["send_conversation_closed_event"] is True
