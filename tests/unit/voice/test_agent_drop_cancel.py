"""Transfer agent-drop + cancel-transfer paths.

Pins the remaining large handlers (hs/voice/transfer.py):
  * handle_agent_drop — guards (agent/transfer/mapping/to-be-primary/org) + the four
    leave types (LEAVE_PRIMARY promote, DROP_SECONDARY, CLOSE_CONVERSATION, LEAVE_SECONDARY) +
    invalid-leave-type + the terminal cleanup.
  * _delete_secondary_agent_mapping_helper — delete error fork.
  * handle_cancel_transfer — validate / contact-info / not-in-progress / call-delete-err /
    async already-accepted / async success.
"""

from types import SimpleNamespace

import pytest

import hs.voice.transfer as ct
import tests.conftest  # noqa: F401 — eager data_adapter imports (AbstractConcreteBase registry)
from hs.routing import constants as rc
from hs.routing.invitation_models import AgentAndContactMapping
from tests.unit._invite_seams import patch_invite_seams

CP = {
    "auth_id": "AUTH1",
    "from_number": "+1",
    "flow_name": "",
    "conversation_number": "555",
    "flow_run_uuid": "flow-1",
    "queue_and_route_run_uuid": "qnr-1",
}
_TBP = {"AgentUUID": "aom-b", "AgentSipEndpoint": "sip:b"}
_UNSET = object()


def _mapping(agent_uuid="aom-a"):
    return AgentAndContactMapping(QnrRunUuid="qnr-1", AgentUUID=agent_uuid, AgentSipEndpoint="sip:a")


class FakeRR:
    def __init__(
        self, in_progress=True, mapping=_UNSET, cp=_UNSET, tbp=_UNSET, set_err=None, accepted="", call_delete=None
    ):
        self._in_progress = in_progress
        self._mapping = _mapping().to_json() if mapping is _UNSET else mapping
        self._cp = dict(CP) if cp is _UNSET else cp
        self._tbp = dict(_TBP) if tbp is _UNSET else tbp
        self._set_err = set_err
        self._accepted = accepted
        self.deleted = []

    # agent-drop + cancel reads
    def is_transfer_in_progress(self, ct_):
        return (self._in_progress, None)

    def get_contact_to_agent_mapping(self, ct_):
        return self._mapping

    def get_contact_parameters(self, ct_, qnr):
        return self._cp

    def get_to_be_primary_agent(self, qnr):
        return self._tbp

    def set_contact_to_agent_mapping(self, ct_, j, ttl):
        return self._set_err

    def get_queue_and_route_config(self, p, q):
        return None

    def is_transfer_invite_accepted(self, ct_):
        return (self._accepted, None)

    def delete_to_be_primary_agent(self, qnr):
        self.deleted.append("tbp")

    def delete_transfer_in_progress(self, ct_):
        self.deleted.append("tip")


def _deps(monkeypatch, **over):
    monkeypatch.setattr(ct.cxfer, "transfer_child", over.get("child", lambda *a, **k: ("child-1", None)))
    monkeypatch.setattr(ct.cxfer, "signal_chunk", lambda *a, **k: None)
    monkeypatch.setattr(ct.cxfer, "remove_secondary", over.get("del_sec", lambda **m: None))
    monkeypatch.setattr(ct.cxfer, "transfer_allowed", lambda *a: (True, True, {}))
    monkeypatch.setattr(ct.cxfer, "set_transfer_state", lambda *a: None)
    patch_invite_seams(monkeypatch, handle_reverse_routing=lambda sip, ch, conv: None)


def _drop(leave_type, contact_uuid="ct-1", channel="chat"):
    return SimpleNamespace(contact_uuid=contact_uuid, channel=channel, leave_type=leave_type)


@pytest.fixture(autouse=True)
def _patch_common(monkeypatch):
    monkeypatch.setattr(ct.ar, "get_agent_by_uuid", lambda u, _auth: SimpleNamespace(name="Agent " + str(u)))
    monkeypatch.setattr(
        ct.ar, "get_org_details_for_agent_sip", lambda sip: SimpleNamespace(name="acme", auth_id="AUTH1")
    )
    monkeypatch.setattr(ct.routing_sticky, "set_outbound_sticky_mapping", lambda *a, **k: None)
    monkeypatch.setattr(ct.routing_sticky, "set_inbound_sticky_mapping", lambda *a, **k: None)
    _deps(monkeypatch)


# ============================== handle_agent_drop guards =============================


def test_drop_agent_not_found_is_error(monkeypatch):
    monkeypatch.setattr(ct.ar, "get_agent_by_uuid", lambda u, _auth: None)
    assert (
        ct.handle_agent_drop(_drop(rc.LEAVE_TYPE_LEAVE_PRIMARY), "aom-a", rr=FakeRR()) == "failed to get agent details"
    )


def test_drop_not_in_transfer_is_error():
    assert (
        ct.handle_agent_drop(_drop(rc.LEAVE_TYPE_LEAVE_PRIMARY), "aom-a", rr=FakeRR(in_progress=False))
        == "conversation is not in transfer mode"
    )


def test_drop_mapping_missing_is_error():
    assert (
        ct.handle_agent_drop(_drop(rc.LEAVE_TYPE_LEAVE_PRIMARY), "aom-a", rr=FakeRR(mapping=None))
        == "unable to get agent to contact mapping"
    )


def test_drop_to_be_primary_missing_is_error():
    assert (
        ct.handle_agent_drop(_drop(rc.LEAVE_TYPE_LEAVE_PRIMARY), "aom-a", rr=FakeRR(tbp=None))
        == "no to be primary agent found"
    )


# ============================== leave types ================================================


def test_drop_leave_primary_not_primary_requester_is_error():
    # requester aom-x is not the mapped primary (aom-a)
    assert (
        ct.handle_agent_drop(_drop(rc.LEAVE_TYPE_LEAVE_PRIMARY), "aom-x", rr=FakeRR())
        == "requester is not the primary agent for the conversation"
    )


def test_drop_leave_primary_child_error_returns_err(monkeypatch):
    _deps(monkeypatch, child=lambda *a, **k: ("", "child failed"))
    assert ct.handle_agent_drop(_drop(rc.LEAVE_TYPE_LEAVE_PRIMARY), "aom-a", rr=FakeRR()) == "child failed"


def test_drop_leave_primary_set_mapping_error_returns_err():
    assert (
        ct.handle_agent_drop(_drop(rc.LEAVE_TYPE_LEAVE_PRIMARY), "aom-a", rr=FakeRR(set_err="redis set failed"))
        == "redis set failed"
    )


def test_drop_leave_primary_success_promotes_and_cleans_up():
    rr = FakeRR()
    assert ct.handle_agent_drop(_drop(rc.LEAVE_TYPE_LEAVE_PRIMARY), "aom-a", rr=rr) is None
    assert rr.deleted == ["tbp", "tip"]  # terminal cleanup ran in order


def test_drop_secondary_not_primary_requester_is_error():
    assert (
        ct.handle_agent_drop(_drop(rc.LEAVE_TYPE_DROP_SECONDARY), "aom-x", rr=FakeRR())
        == "requester is not the primary agent for the conversation"
    )


def test_drop_secondary_success():
    rr = FakeRR()
    assert ct.handle_agent_drop(_drop(rc.LEAVE_TYPE_DROP_SECONDARY), "aom-a", rr=rr) is None
    assert rr.deleted == ["tbp", "tip"]


def test_drop_secondary_helper_delete_error_returns_err(monkeypatch):
    # _delete_secondary_agent_mapping_helper :400-401 — delete_secondary error bubbles up.
    _deps(monkeypatch, del_sec=lambda **m: "delete secondary failed")
    assert ct.handle_agent_drop(_drop(rc.LEAVE_TYPE_DROP_SECONDARY), "aom-a", rr=FakeRR()) == "delete secondary failed"


def test_drop_close_conversation_not_primary_is_error():
    assert (
        ct.handle_agent_drop(_drop(rc.LEAVE_TYPE_CLOSE_CONVERSATION), "aom-x", rr=FakeRR())
        == "not the primary agent for the conversation"
    )


def test_drop_close_conversation_success():
    rr = FakeRR()
    assert ct.handle_agent_drop(_drop(rc.LEAVE_TYPE_CLOSE_CONVERSATION), "aom-a", rr=rr) is None
    assert rr.deleted == ["tbp", "tip"]


def test_drop_leave_secondary_not_secondary_is_error():
    # requester aom-a is the primary, not the to-be-primary secondary (aom-b)
    assert (
        ct.handle_agent_drop(_drop(rc.LEAVE_TYPE_LEAVE_SECONDARY), "aom-a", rr=FakeRR())
        == "not secondary agent for the given conversation"
    )


def test_drop_leave_secondary_success():
    rr = FakeRR()
    assert ct.handle_agent_drop(_drop(rc.LEAVE_TYPE_LEAVE_SECONDARY), "aom-b", rr=rr) is None


@pytest.mark.parametrize(
    "leave_type, requester",
    [
        (rc.LEAVE_TYPE_LEAVE_PRIMARY, "aom-a"),
        (rc.LEAVE_TYPE_DROP_SECONDARY, "aom-a"),
        (rc.LEAVE_TYPE_CLOSE_CONVERSATION, "aom-a"),
        (rc.LEAVE_TYPE_LEAVE_SECONDARY, "aom-b"),
    ],
)
def test_drop_succeeds_for_every_leave_type(leave_type, requester):
    assert ct.handle_agent_drop(_drop(leave_type), requester, rr=FakeRR()) is None


def test_drop_by_a_stranger_is_refused():
    assert ct.handle_agent_drop(_drop(rc.LEAVE_TYPE_DROP_SECONDARY), "aom-x", rr=FakeRR()) is not None


def test_drop_invalid_leave_type_is_error():
    assert ct.handle_agent_drop(_drop("teleport"), "aom-a", rr=FakeRR()) == "invalid leave type"


# ============================== handle_cancel_transfer ===============================


def _req(**kw):
    base = {"contact_uuid": "ct-1", "agent_sip": "sip:a"}
    base.update(kw)
    return SimpleNamespace(**base)


def _patch_validate(monkeypatch, err=None):
    monkeypatch.setattr(ct, "validate_transfer_req_owner", lambda req, ch, fetch, rr: (_mapping(), {}, "", err))


def test_cancel_validate_error_returns_err(monkeypatch):
    _patch_validate(monkeypatch, err="not the owner of the channel")
    assert ct.handle_cancel_transfer("chat", _req(), "warm", rr=FakeRR()) == "not the owner of the channel"


def test_cancel_contact_info_missing_is_error(monkeypatch):
    _patch_validate(monkeypatch)
    assert (
        ct.handle_cancel_transfer("chat", _req(), "warm", rr=FakeRR(cp=None))
        == "error in fetching contact parameters from cache"
    )


def test_cancel_not_in_progress_is_error(monkeypatch):
    _patch_validate(monkeypatch)
    assert (
        ct.handle_cancel_transfer("chat", _req(), "warm", rr=FakeRR(in_progress=False))
        == "conversation is not in transfer mode"
    )


def test_cancel_call_delete_error_returns_err(monkeypatch):
    _patch_validate(monkeypatch)
    monkeypatch.setattr(ct, "delete_transfer_mpc_calls", lambda *a, **k: "delete failed")
    assert ct.handle_cancel_transfer(rc.CHANNEL_CALL, _req(), "warm", rr=FakeRR()) == "delete failed"
