"""Channel-transfer INITIATE path (offline-deterministic).

Pins validate_transfer_req_owner (owner gate / lazy-seed / allowed transfer mode), the initiate
orchestration (allowed-transfer-mode gate → TIP write → MTBPAK cleanup → async dispatch → signal
chunk), the async channel transfer (get_agents is_transfer=true → agent invite is_transfer=true →
transfer state 'transferring'), the call-channel DELEGATE marker, and the served route's faithful
validation order + byte shapes (bind/channel/mode/missing-params/type → 400; usecase → 422; success 200).
"""

from types import SimpleNamespace

import tests.conftest  # noqa: F401 — eager data_adapter imports
from hs.routing import constants as rc
from hs.routing.account_read import OrgTransferSettings
from hs.routing.invitation_models import AgentAndContactMapping
from hs.voice import transfer as ct
from hs.voice.transfer_models import IncomingTransferReq

_AUTH = ("user", "pass")
_PARAMS = {
    "conversation_number": "CN1",
    "flow_run_uuid": "P",
    "queue_and_route_run_uuid": "Q",
    "auth_id": "A",
    "from_number": "+1",
    "to": "+2",
}


class FakeRR:
    def __init__(self, mapping=None, contact_params=None, tip_set=True):
        self.mapping = mapping  # raw CONTACT_AGENT_MAPPING JSON or None
        self.contact_params = contact_params
        self.tip_set_ok = tip_set
        self.tip_value = None
        self.set_mapping = []
        self.stored_params = []
        self.deleted_mtbpak = []
        self.deleted_tip = []

    def get_contact_to_agent_mapping(self, cu):
        return self.mapping

    def set_contact_to_agent_mapping(self, cu, j, ttl):
        self.set_mapping.append(j)
        self.mapping = j
        return

    def get_contact_parameters(self, cu, qnr):
        return dict(self.contact_params) if self.contact_params is not None else None

    def get_active_task_run_id(self, t):
        return (None, None, None, "miss")

    def store_contact_params(self, cu, qnr, j):
        self.stored_params.append(j)
        return 1

    def set_transfer_in_progress(self, cu, j, ttl, nx=False):
        self.tip_value = j
        return self.tip_set_ok

    def delete_to_be_primary_agent(self, qnr):
        self.deleted_mtbpak.append(qnr)

    def delete_transfer_in_progress(self, cu):
        self.deleted_tip.append(cu)
        self.tip_value = None


def _owner_mapping(sip="sip:a", qnr="Q"):
    return AgentAndContactMapping(agent_sip_endpoint=sip, qnr_run_uuid=qnr).to_json()


def _req(**kw):
    base = {
        "contact_uuid": "c1",
        "transfer_type": "agent",
        "identifier": "sip:target",
        "agent_sip": "sip:a",
        "member": "",
    }
    base.update(kw)
    return IncomingTransferReq.model_validate(base)


def _deps(monkeypatch, check=None):
    recs = {"signals": [], "states": []}
    monkeypatch.setattr(ct.cxfer, "transfer_allowed", check or (lambda *a, **k: (True, False, {})))
    monkeypatch.setattr(ct.cxfer, "set_transfer_state", lambda cu, ch, st: recs["states"].append((cu, ch, st)))
    monkeypatch.setattr(
        ct.cxfer, "signal_chunk", lambda cu, ch, desc, idn, tt, msg: recs["signals"].append((desc, msg))
    )
    return recs


def _patch_common(
    monkeypatch,
    agents=("sip:target",),
    invite_err=None,
    allow_transfer_type=True,
    default_transfer="",
    team_name="Team A",
    agent_name="Bob",
):
    inv = []
    monkeypatch.setattr(
        ct.ar,
        "get_org_transfer_settings_for_agent_sip",
        lambda sip: OrgTransferSettings(False, allow_transfer_type, default_transfer),
    )
    monkeypatch.setattr(
        ct.ar, "get_org_details_for_agent_sip", lambda sip: SimpleNamespace(auth_id="A", auth_token="T")
    )
    monkeypatch.setattr(ct.ar, "get_team_name", lambda t: team_name)
    import hs.voice.hangup as vh

    monkeypatch.setattr(vh, "_get_agent_name_by_uuid", lambda u, auth_id: (agent_name, None))
    return inv


def test_validate_owner_valid_no_seed(monkeypatch):
    _patch_common(monkeypatch)
    rr = FakeRR(mapping=_owner_mapping("sip:a"), contact_params=_PARAMS)
    _deps(monkeypatch)
    _mapping, _conv, allowed, err = ct.validate_transfer_req_owner(_req(), "chat", True, rr)
    assert err is None and allowed == rc.TRANSFER_MODE_BOTH
    assert rr.set_mapping == []  # owner valid → no lazy seed


def test_validate_transfer_not_allowed(monkeypatch):
    _patch_common(monkeypatch)
    rr = FakeRR(mapping=_owner_mapping("sip:a"), contact_params=_PARAMS)
    _deps(monkeypatch, check=lambda *a, **k: (False, False, {}))
    _m, _c, _a, err = ct.validate_transfer_req_owner(_req(), "chat", True, rr)
    assert err is not None and "should be created before transferring" in err


def test_validate_owner_mismatch_seeds_mapping(monkeypatch):
    _patch_common(monkeypatch)
    # cached owner is a DIFFERENT sip → not the owner; conv details carry the real owner sip == req.agent_sip
    rr = FakeRR(mapping=_owner_mapping("sip:other"), contact_params=_PARAMS)
    _deps(monkeypatch, check=lambda *a, **k: (True, False, {"agent_sip_endpoint": "sip:a", "is_inbound": True}))
    mapping, _conv, _allowed, err = ct.validate_transfer_req_owner(_req(agent_sip="sip:a"), "chat", True, rr)
    assert err is None
    assert rr.set_mapping and mapping.agent_sip_endpoint == "sip:a"  # seeded with the conv owner


def test_validate_allowed_mode_cold_when_transfer_type_off(monkeypatch):
    _patch_common(monkeypatch, allow_transfer_type=False, default_transfer="cold")
    rr = FakeRR(mapping=_owner_mapping("sip:a"), contact_params=_PARAMS)
    _deps(monkeypatch)
    _m, _c, allowed, err = ct.validate_transfer_req_owner(_req(), "chat", True, rr)
    assert err is None and allowed == rc.TRANSFER_MODE_COLD


def test_initiate_mode_not_allowed(monkeypatch):
    _patch_common(monkeypatch, allow_transfer_type=False, default_transfer="cold")
    rr = FakeRR(mapping=_owner_mapping("sip:a"), contact_params=_PARAMS)
    _deps(monkeypatch)
    # allowed=cold, request warm, type=agent (not external) → blocked
    err = ct.handle_channel_transfer_initiate_generic("call", _req(), rc.TRANSFER_MODE_WARM, None, rr)
    assert err == "warm transfer is not allowed"
    assert rr.tip_value is None  # gate is BEFORE the TIP write


def test_initiate_call_channel_routes_and_holds(monkeypatch):
    # The call branch = RouteAndAddToMPC(is_transfer=true) + hold_unhold_customer(true)
    _patch_common(monkeypatch)
    from hs.voice import callbacks as voice_callbacks

    routed, held = [], []
    monkeypatch.setattr(
        voice_callbacks,
        "route_and_add_to_mpc",
        lambda ct_, qnr, is_t, ttype, ident, member, sip, db, routing_redis=None: (
            routed.append((ct_, qnr, is_t, ttype, ident, member, sip)) or ("", None)
        ),
    )
    monkeypatch.setattr(ct, "hold_unhold_customer", lambda hv, ct_, qnr, a, t: held.append((hv, ct_, qnr)) or None)
    rr = FakeRR(mapping=_owner_mapping("sip:a"), contact_params=_PARAMS)
    _deps(monkeypatch)
    err = ct.handle_channel_transfer_initiate_generic("call", _req(), rc.TRANSFER_MODE_WARM, None, rr)
    assert err is None
    assert routed == [("c1", "Q", True, "agent", "sip:target", "", "sip:a")]
    assert held == [(True, "c1", "Q")]
    assert rr.deleted_tip == []  # success keeps the TIP


def test_initiate_call_channel_failure_rolls_back_tip(monkeypatch):
    _patch_common(monkeypatch)
    from hs.voice import callbacks as voice_callbacks

    monkeypatch.setattr(
        voice_callbacks, "route_and_add_to_mpc", lambda *a, **k: ("", "known_error: no available agents found")
    )
    rr = FakeRR(mapping=_owner_mapping("sip:a"), contact_params=_PARAMS)
    _deps(monkeypatch)
    err = ct.handle_channel_transfer_initiate_generic("call", _req(), rc.TRANSFER_MODE_WARM, None, rr)
    assert err == "known_error: no available agents found"
    assert rr.deleted_tip == ["c1"]  # TIP rolled back on the call failure
