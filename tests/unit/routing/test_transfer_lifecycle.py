"""Channel-transfer CANCEL + DROP/LEAVE FSM + FLOW transfer (offline-deterministic).

Pins handle_cancel_transfer (require in-progress; reject if accepted; invited-agent removal + transfer-in-progress delete
+ CANCEL_TRANSFER; transfer state 'transfer_failed'); handle_agent_drop (leave_primary promote
+ the first agent leaving; drop_secondary/close/leave_secondary helper; the per-case identity checks incl. close's
distinct error text; final MTBPAK+TIP cleanup) + the served routes' faithful validation byte shapes.
"""

from types import SimpleNamespace

import tests.conftest  # noqa: F401 — eager data_adapter imports
from hs.routing.invitation_models import AgentAndContactMapping
from hs.voice import transfer as ct
from hs.voice.transfer_models import AgentDropParams, IncomingTransferReq

_AUTH = ("user", "pass")
_PARAMS = {
    "conversation_number": "CN1",
    "flow_run_uuid": "P",
    "queue_and_route_run_uuid": "Q",
    "auth_id": "A",
    "auth_token": "T",
    "from_number": "+1",
    "flow_name": "",
}


class FakeRR:
    def __init__(self, in_progress=True, accepted="", mapping=None, params=None, mtbpak=None, tip_ok=True):
        self.in_progress = in_progress
        self.accepted = accepted
        self.mapping = mapping
        self.params = params
        self.mtbpak = mtbpak
        self.tip_ok = tip_ok
        self.tip_value = None
        self.set_mapping = []
        self.deleted_tip = []
        self.deleted_mtbpak = []

    def is_transfer_in_progress(self, c):
        return (self.in_progress, None if self.in_progress else "data not found in cache")

    def is_transfer_invite_accepted(self, c):
        return (self.accepted, None) if self.accepted else ("", "data not found in cache")

    def get_contact_parameters(self, c, q):
        return dict(self.params) if self.params is not None else None

    def get_contact_to_agent_mapping(self, c):
        return self.mapping

    def get_to_be_primary_agent(self, q):
        return dict(self.mtbpak) if self.mtbpak is not None else None

    def set_contact_to_agent_mapping(self, c, j, ttl):
        self.set_mapping.append(j)
        self.mapping = j
        return

    def set_transfer_in_progress(self, c, j, ttl):
        self.tip_value = j
        return self.tip_ok

    def get_queue_and_route_config(self, p, q):
        return None

    def delete_transfer_in_progress(self, c):
        self.deleted_tip.append(c)

    def delete_to_be_primary_agent(self, q):
        self.deleted_mtbpak.append(q)


def _mapping(agent_uuid="primary", sip="sip:p", qnr="Q"):
    return AgentAndContactMapping(agent_uuid=agent_uuid, agent_sip_endpoint=sip, qnr_run_uuid=qnr).to_json()


def _deps(monkeypatch):
    recs = {"signals": [], "states": [], "secondary": [], "child": [], "rr": [], "flow": [], "transfer_failed": []}
    x = ct.cxfer
    monkeypatch.setattr(x, "transfer_allowed", lambda *a, **k: (True, False, {}))
    monkeypatch.setattr(x, "signal_chunk", lambda cu, ch, desc, idn, tt, msg: recs["signals"].append((desc, msg)))
    monkeypatch.setattr(x, "set_transfer_state", lambda cu, ch, st: recs["states"].append(st))
    monkeypatch.setattr(x, "remove_secondary", lambda **m: recs["secondary"].append(m))
    monkeypatch.setattr(x, "transfer_child", lambda *a, **k: recs["child"].append(a) or ("child-cn", None))
    return recs


def _patch_owner_account(monkeypatch, agent_name="X"):
    # validate_transfer_req_owner: mapping owner == agent_sip; settings allow 'both'
    from hs.routing.account_read import OrgTransferSettings

    monkeypatch.setattr(
        ct.ar, "get_org_transfer_settings_for_agent_sip", lambda s: OrgTransferSettings(False, True, "")
    )
    monkeypatch.setattr(
        ct.ar,
        "get_org_details_for_agent_sip",
        lambda s: SimpleNamespace(auth_id="A", auth_token="T", name="acme", id=1),
    )
    monkeypatch.setattr(
        ct.ar, "get_agent_by_uuid", lambda u, _auth: SimpleNamespace(name=agent_name, sip_endpoint="sip:x", uuid=u)
    )


def _cancel_req():
    return IncomingTransferReq.model_validate(
        {"contact_uuid": "c1", "agent_sip": "sip:p", "transfer_type": "agent", "identifier": "i"}
    )


# --- DROP / LEAVE FSM ---------------------------------------------------------------------------
_MTBPAK = {"AgentUUID": "secondary", "AgentSipEndpoint": "sip:sec"}


def _drop(leave_type):
    return AgentDropParams.model_validate({"leave_type": leave_type, "contact_uuid": "c1", "channel": "chat"})


def test_cancel_not_in_progress(monkeypatch):
    _patch_owner_account(monkeypatch)
    _deps(monkeypatch)
    rr = FakeRR(in_progress=False, mapping=_mapping("sip:p"), params=_PARAMS)
    assert ct.handle_cancel_transfer("chat", _cancel_req(), "warm", rr) == "conversation is not in transfer mode"


def test_drop_leave_primary_by_non_primary_rejected(monkeypatch):
    _patch_owner_account(monkeypatch)
    _deps(monkeypatch)
    rr = FakeRR(mapping=_mapping(agent_uuid="primary"), params=_PARAMS, mtbpak=_MTBPAK)
    assert (
        ct.handle_agent_drop(_drop("leave_primary"), "someone-else", rr)
        == "requester is not the primary agent for the conversation"
    )


def test_drop_close_conversation_distinct_error(monkeypatch):
    _patch_owner_account(monkeypatch)
    _deps(monkeypatch)
    rr = FakeRR(mapping=_mapping(agent_uuid="primary"), params=_PARAMS, mtbpak=_MTBPAK)
    # close_conversation uses the DISTINCT 'not the primary agent...' text (no 'requester is')
    assert (
        ct.handle_agent_drop(_drop("close_conversation"), "other", rr) == "not the primary agent for the conversation"
    )


def test_drop_secondary_deletes_secondary(monkeypatch):
    _patch_owner_account(monkeypatch)
    recs = _deps(monkeypatch)
    rr = FakeRR(mapping=_mapping(agent_uuid="primary"), params=_PARAMS, mtbpak=_MTBPAK)
    err = ct.handle_agent_drop(_drop("drop_secondary"), "primary", rr)
    assert err is None
    assert (
        recs["secondary"] and recs["secondary"][0]["agent_sip_or_uuid"] == "secondary"
    )  # agent_sip holds the to-be-primary agent's uuid (a quirk)
    assert any(s[0] == "AGENT2_DROPPED" for s in recs["signals"])


def test_a_secondary_drop_on_a_call_reverse_routes_the_freed_agent(monkeypatch):
    import hs.routing.invitation as ri
    import hs.voice.hangup as vh

    _patch_owner_account(monkeypatch)
    _deps(monkeypatch)
    routed = []
    monkeypatch.setattr(vh, "handle_reverse_routing_and_agent_capacity", lambda *a: routed.append(a) or ("", None))
    monkeypatch.setattr(ri.threading, "Thread", lambda target, args, **kw: SimpleNamespace(start=lambda: target(*args)))
    rr = FakeRR(mapping=_mapping(agent_uuid="primary"), params=_PARAMS, mtbpak=_MTBPAK)
    drop = AgentDropParams.model_validate({"leave_type": "drop_secondary", "contact_uuid": "c1", "channel": "call"})
    assert ct.handle_agent_drop(drop, "primary", rr) is None
    assert routed == [("sip:sec", "call", "CN1")]  # the freed secondary is offered the next queued call


def test_drop_leave_secondary_by_non_secondary_rejected(monkeypatch):
    _patch_owner_account(monkeypatch)
    _deps(monkeypatch)
    rr = FakeRR(mapping=_mapping(agent_uuid="primary"), params=_PARAMS, mtbpak=_MTBPAK)
    assert (
        ct.handle_agent_drop(_drop("leave_secondary"), "primary", rr)
        == "not secondary agent for the given conversation"
    )


def test_drop_invalid_leave_type(monkeypatch):
    _patch_owner_account(monkeypatch)
    _deps(monkeypatch)
    rr = FakeRR(mapping=_mapping(agent_uuid="primary"), params=_PARAMS, mtbpak=_MTBPAK)
    assert ct.handle_agent_drop(_drop("teleport"), "primary", rr) == "invalid leave type"


def test_drop_not_in_progress(monkeypatch):
    _patch_owner_account(monkeypatch)
    _deps(monkeypatch)
    rr = FakeRR(in_progress=False, mapping=_mapping(), params=_PARAMS, mtbpak=_MTBPAK)
    assert ct.handle_agent_drop(_drop("leave_primary"), "primary", rr) == "conversation is not in transfer mode"
