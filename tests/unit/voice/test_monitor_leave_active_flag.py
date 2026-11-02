"""Monitor-leave must remove the supervisor's secondary mapping with active=False.

On monitor-leave the primary agent may already have closed their leg, so with active=True the
get_primary_active_conversation / delete_mapping lookup misses and the supervisor's
ConvSecondaryAgentMapping is never deleted (a leak that breaks later get_primary_active_conversation
filtering). Monitor-leave and a late join's cleanup pass False; transfer/hangup pass True. This test
pins that remove_secondary threads `active` through (default True) and that monitor-leave passes
False.
"""

import pytest

import hs.conversations.transfer as ct
import tests.conftest  # noqa: F401 — eager data_adapter imports (AbstractConcreteBase registry)
from hs.infra.models import UseCaseResponse


@pytest.fixture(autouse=True)
def _ctx():
    from hs.infra.context import context_api_id, context_req_meta

    t = [context_req_meta.set({"api_id": "mon-leave"}), context_api_id.set("mon-leave")]
    yield
    from hs.infra.context import context_api_id, context_req_meta

    for var, tok in zip((context_req_meta, context_api_id), t):
        var.reset(tok)


def _run_delete(monkeypatch, **mapping):
    captured = {}
    monkeypatch.setattr("hs.routing.invitation._ensure_request_context", lambda: None, raising=False)

    def _fake_remove(mapping, bypass_all_events=False, active=True):
        captured["active"] = active
        return UseCaseResponse(status_code=200)

    monkeypatch.setattr(ct, "remove_secondary_participant", _fake_remove)
    err = ct.remove_secondary(**mapping)
    assert err is None
    return captured["active"]


def test_default_active_true_for_transfer_hangup(monkeypatch):
    active = _run_delete(monkeypatch, conversation_number="1", agent_sip_or_uuid="sip:a")
    assert active is True


def test_monitor_leave_payload_active_false(monkeypatch):
    # exactly what voice_monitor.leave_monitored_conversation sends
    active = _run_delete(
        monkeypatch,
        conversation_number="1",
        agent_sip_or_uuid="sip:sup",
        ignore_event=True,
        type="supervisor_participant",
        active=False,
    )
    assert active is False
