"""Call-channel transfer: the initiate/cancel/queue call branches (RouteAndAddToMPC+hold_unhold_customer
/ DeleteTransferMPCCalls-no-TIP-delete / the call-uuid juggle + aleg redirect + recording flip),
handle_call_switch (AS role toggle + selective holds), handle_call_transfer_merge (unhold + MTBPAK
promotion; warm_external sip-only-continue), handle_call_transfer_warm_hangup (member normalize +
customer-first transfer-calls delete + member hangup), the outbound queue/success pair (the
case/contact gate; the mapping rewrite + the no-agent IndexError), and the two agents reads
(team/capacity + per-agent RRLOCK deletes).
"""

import json

import pytest

import hs.voice.transfer as ct
import tests.conftest  # noqa: F401 — eager data_adapter imports (AbstractConcreteBase registry)
from hs.routing import constants as rc
from hs.routing.invitation_models import AgentAndContactMapping
from hs.voice import constants as vc
from hs.voice.transfer_models import GetAgentsByCapacity, GetAgentsParams, IncomingTransferReq

CP = {
    "contact_uuid": "c-1",
    "auth_id": "AUTH1",
    "auth_token": "tok",
    "queue_and_route_run_uuid": "qnr-1",
    "flow_run_uuid": "flow-1",
    "conversation_number": "5550001111",
    "from_number": "+14150000001",
}


def _mapping(sip="sip:a", qnr="qnr-1"):
    return AgentAndContactMapping(AgentSipEndpoint=sip, QnrRunUuid=qnr, AgentUUID="aom-a")


def _req(**kw):
    base = {"contact_uuid": "c-1", "transfer_type": "agent", "identifier": "dest-1", "agent_sip": "sip:a", "member": ""}
    base.update(kw)
    return IncomingTransferReq.model_validate(base)


class FakeRR:
    def __init__(self, cp=CP):
        self.cp = cp
        self.mapping = None
        self.calls = {"tbp": [], "del_lock": [], "set_mapping": [], "del_tip": []}

    def get_contact_parameters(self, ct_, qnr):
        return self.cp

    def get_contact_to_agent_mapping(self, ct_):
        return self.mapping

    def set_to_be_primary_agent(self, qnr, mapping_json, ttl):
        self.calls["tbp"].append((qnr, mapping_json, ttl))
        return True

    def delete_reverse_routing_lock(self, sip, conv, channel):
        self.calls["del_lock"].append((sip, conv, channel))

    def set_contact_to_agent_mapping(self, ct_, mapping_json, ttl):
        self.calls["set_mapping"].append((ct_, mapping_json, ttl))
        return

    def delete_transfer_in_progress(self, ct_):
        self.calls["del_tip"].append(ct_)
        return True

    def set_transfer_in_progress(self, ct_, info, ttl):
        return True


class FakeVR:
    def __init__(self, role=""):
        self.role = role
        self.set_roles = []

    def get_agent_switch_role(self, qnr, sip):
        return self.role, None

    def set_agent_switch_role(self, qnr, sip, value):
        self.set_roles.append((qnr, sip, value))
        return


def _owner_ok(monkeypatch, mapping=None):
    m = mapping or _mapping()
    monkeypatch.setattr(
        ct, "validate_transfer_req_owner", lambda req, channel, fetch, rr: (m, {}, rc.TRANSFER_MODE_BOTH, None)
    )
    return m


def _participants(monkeypatch, parts):
    from hs.voice import agent_leg as voice_agent_leg

    monkeypatch.setattr(voice_agent_leg, "_get_participants", lambda name, a, t: (list(parts), None))


def _holds(monkeypatch):
    from hs.voice import hangup as voice_hangup

    holds = []
    monkeypatch.setattr(
        voice_hangup, "hold_or_unhold_call", lambda hv, ct_, qnr, member, a, t: holds.append((hv, member)) or None
    )
    return holds


# ============================================================================ switch
def test_switch_role_toggle_to_customer(monkeypatch):
    _owner_ok(monkeypatch)
    _participants(
        monkeypatch,
        [
            {"role": "customer", "member_id": "1"},
            {"role": "agent", "member_id": "2", "member_address": "sip:a", "exit_cause": ""},
        ],
    )
    holds = _holds(monkeypatch)
    vr = FakeVR(role="")  # ''/agent → customer talk: customer unheld, agents held
    err = ct.handle_call_switch(_req(), rr=FakeRR(), voice_redis=vr)
    assert err is None
    assert vr.set_roles == [("qnr-1", "sip:a", vc.MPC_ROLE_CUSTOMER)]
    assert holds == [(False, "1")]  # customer unheld; the self-agent leg untouched


def test_switch_role_toggle_to_agent_holds_customer(monkeypatch):
    _owner_ok(monkeypatch)
    _participants(
        monkeypatch,
        [
            {"role": "customer", "member_id": "1"},
            {"role": "agent", "member_id": "2", "member_address": "+1555000", "exit_cause": ""},
        ],
    )
    holds = _holds(monkeypatch)
    vr = FakeVR(role="customer")  # customer → agent talk: customer held, agents unheld
    err = ct.handle_call_switch(_req(), rr=FakeRR(), voice_redis=vr)
    assert err is None
    assert vr.set_roles == [("qnr-1", "sip:a", vc.MPC_ROLE_AGENT)]
    assert holds == [(True, "1"), (False, "2")]


# ============================================================================ merge
def test_merge_promotes_agent(monkeypatch):
    _owner_ok(monkeypatch)
    _participants(
        monkeypatch,
        [{"role": "Agent", "member_id": "2", "call_uuid": "leg-2", "member_address": "sip:other@x", "exit_cause": ""}],
    )
    holds = _holds(monkeypatch)

    class Row:
        uuid = "aom-other"

    monkeypatch.setattr(ct.ar, "get_aom_row_for_sip", lambda sip: Row())
    from hs.voice import hangup as voice_hangup

    monkeypatch.setattr(voice_hangup, "get_conversation_number_from_contact_uuid", lambda ct_, rr: ("555", None))
    rr = FakeRR()
    err = ct.handle_call_transfer_merge(_req(), rr=rr)
    assert err is None
    assert holds == [(False, "2")]  # other leg unheld
    qnr, tbp_json, ttl = rr.calls["tbp"][0]
    assert qnr == "qnr-1" and ttl == vc.EXPIRY_DURATION_IN_HOURS_ASYNC * 3600
    tbp = json.loads(tbp_json)
    assert tbp["AgentUUID"] == "aom-other" and tbp["AgentCallUuid"] == "leg-2"
    assert tbp["AgentSipEndpoint"] == "sip:other@x" and tbp["QnrRunUuid"] == "qnr-1"


def test_merge_warm_external_sip_only(monkeypatch):
    _owner_ok(monkeypatch)
    _participants(
        monkeypatch,
        [{"role": "agent", "member_id": "2", "call_uuid": "leg-2", "member_address": "+18005550000", "exit_cause": ""}],
    )
    _holds(monkeypatch)
    monkeypatch.setattr(ct.ar, "get_aom_row_for_sip", lambda sip: pytest.fail("warm_external must not resolve the aom"))
    rr = FakeRR()
    err = ct.handle_call_transfer_merge(_req(transfer_type="warm_external", member="+18005550000"), rr=rr)
    assert err is None
    tbp = json.loads(rr.calls["tbp"][0][1])
    assert tbp["AgentUUID"] == "" and tbp["AgentSipEndpoint"] == "+18005550000"


def test_merge_warm_external_is_not_satisfied_by_the_callers_own_leg(monkeypatch):
    _owner_ok(monkeypatch)
    customer = {"role": "Customer", "member_id": "1", "call_uuid": "leg-1", "member_address": "+18005550000"}
    _participants(monkeypatch, [customer | {"exit_cause": ""}])
    holds = _holds(monkeypatch)
    rr = FakeRR()
    err = ct.handle_call_transfer_merge(_req(transfer_type="warm_external", member="+18005550000"), rr=rr)
    assert err == "the number has not joined" and holds == [] and not rr.calls.get("tbp")


# ============================================================================ warm hangup
def test_warm_hangup_customer_match_deletes_transfer_calls_first(monkeypatch):
    _owner_ok(monkeypatch)
    _participants(
        monkeypatch, [{"role": "customer", "member_id": "1", "member_address": "18005550000", "exit_cause": ""}]
    )
    seq = []
    monkeypatch.setattr(
        ct, "delete_transfer_mpc_calls", lambda ct_, qnr, a, t, voice_redis=None: seq.append("del") or None
    )
    monkeypatch.setattr(
        ct,
        "hangup_call_for_mpc_member",
        lambda mpc, member, a, t, extra_member_forms=(): seq.append(("hang", mpc, member)) or None,
    )
    err = ct.handle_call_transfer_warm_hangup(_req(member="+18005550000"), rr=FakeRR())
    assert err is None
    assert seq == ["del", ("hang", "mpc_qnr-1", "18005550000")]  # omit_plus normalize


def test_warm_hangup_agent_match_skips_transfer_delete(monkeypatch):
    _owner_ok(monkeypatch)
    _participants(monkeypatch, [{"role": "agent", "member_id": "2", "member_address": "sip:b@x", "exit_cause": ""}])
    seq = []
    monkeypatch.setattr(ct, "delete_transfer_mpc_calls", lambda *a, **k: seq.append("del") or None)
    monkeypatch.setattr(
        ct, "hangup_call_for_mpc_member", lambda mpc, member, a, t, extra_member_forms=(): seq.append("hang") or None
    )
    err = ct.handle_call_transfer_warm_hangup(_req(member="sip:b@x"), rr=FakeRR())
    assert err is None and seq == ["hang"]


# ============================================================================ cancel (call)
def test_cancel_call_deletes_transfer_calls_keeps_tip(monkeypatch):
    _owner_ok(monkeypatch)
    deleted, signals = [], []
    monkeypatch.setattr(
        ct, "delete_transfer_mpc_calls", lambda ct_, qnr, a, t, voice_redis=None: deleted.append(ct_) or None
    )
    rr = FakeRR()
    rr.is_transfer_in_progress = lambda ct_: (True, None)
    monkeypatch.setattr(ct.cxfer, "signal_chunk", lambda *a: signals.append(a))
    err = ct.handle_cancel_transfer(rc.CHANNEL_CALL, _req(), "warm", rr)
    assert err is None
    assert deleted == ["c-1"]
    assert rr.calls["del_tip"] == []  # the call cancel KEEPS the TIP
    assert signals and signals[0][2] == rc.SIGNAL_CANCEL_TRANSFER


def test_outbound_success_rewrites_mapping(monkeypatch):
    rr = FakeRR()
    rr.mapping = _mapping().to_json()
    monkeypatch.setattr(ct.ar, "get_account_rows_for_sip_endpoints", lambda sips: [type("A", (), {"uuid": "aom-b"})()])
    err = ct.handle_outbound_call_transfer_success(
        "ct-9", {"agent_sip_endpoint": "sip:b@x", "flow_run_id": "flow-2"}, rr=rr
    )
    assert err is None
    ct_, mapping_json, ttl = rr.calls["set_mapping"][0]
    assert ct_ == "ct-9" and ttl == vc.EXPIRY_DURATION_IN_HOURS_CALL * 3600
    m = json.loads(mapping_json)
    assert (m["AgentSipEndpoint"], m["AgentUUID"], m["AgentCallUuid"], m["FlowRunUUID"]) == (
        "sip:b@x",
        "aom-b",
        "ct-9",
        "flow-2",
    )


def test_outbound_success_empty_aom_rows_raises(monkeypatch):
    rr = FakeRR()
    rr.mapping = _mapping().to_json()
    monkeypatch.setattr(ct.ar, "get_account_rows_for_sip_endpoints", lambda sips: [])
    with pytest.raises(IndexError):  # no agent row for the SIP endpoint
        ct.handle_outbound_call_transfer_success("ct-9", {"agent_sip_endpoint": "sip:b@x"}, rr=rr)


# ============================================================================ agents reads
def test_get_agents_for_team_deletes_locks(monkeypatch):
    from hs.routing import get_agents as routing_get_agents

    seen = []
    monkeypatch.setattr(
        routing_get_agents, "get_agents", lambda *a, **k: seen.append((a, k)) or (["sip:x", "sip:y"], None)
    )
    rr = FakeRR()
    agents, err = ct.get_agents_for_team(
        GetAgentsParams.model_validate(
            {
                "call_uuid": "ct",
                "flow_run_uuid": "flow",
                "auth_id": "AUTH1",
                "transfer_destination_id": "team-1",
                "conversation_number": "555",
            }
        ),
        rr=rr,
    )
    assert err is None and agents == ["sip:x", "sip:y"]
    # a team transfer, not inbound: no run (its retry count only feeds inbound routing events)
    args, kw = seen[0]
    assert args == ("ct", rc.CHANNEL_CALL, "AUTH1", "555")
    assert kw == {"transfer_to": "team-1", "transfer_type": rc.TRANSFER_TYPE_TEAM, "routing_redis": rr}
    assert rr.calls["del_lock"] == [("sip:x", "555", "call"), ("sip:y", "555", "call")]


def test_get_agent_by_capacity_chain(monkeypatch):
    from hs.routing import acw as routing_acw
    from hs.routing import selection as routing_selection

    monkeypatch.setattr(ct.ar, "get_agent_by_uuid", lambda u, _auth: type("A", (), {"sip_endpoint": "sip:x"})())
    monkeypatch.setattr(routing_acw, "get_agents_on_active_call_and_in_acw", lambda auth, agents: "ACW")
    caps = []
    monkeypatch.setattr(
        routing_selection,
        "get_available_agents_by_capacity",
        lambda agents, channel, cap, lim, auth, acw, conv, routing_redis=None: (
            caps.append((agents, channel, cap, lim, acw, conv)) or ["sip:x"]
        ),
    )
    rr = FakeRR()
    agents, err = ct.get_agent_by_capacity(
        GetAgentsByCapacity.model_validate(
            {
                "call_uuid": "ct",
                "flow_run_uuid": "flow",
                "auth_id": "AUTH1",
                "aom_uuid": "aom-x",
                "conversation_number": "555",
            }
        ),
        rr=rr,
    )
    assert err is None and agents == ["sip:x"]
    assert caps == [(["sip:x"], "call", 1, 1, "ACW", "555")]
    assert rr.calls["del_lock"] == [("sip:x", "555", "call")]


# ============================================================================ helpers
def test_hold_unhold_customer_first_break(monkeypatch):
    _participants(monkeypatch, [{"role": "Customer", "member_id": "1"}, {"role": "customer", "member_id": "2"}])
    holds = _holds(monkeypatch)
    err = ct.hold_unhold_customer(True, "ct", "qnr", "A", "T")
    assert err is None and holds == [(True, "1")]  # FIRST customer only (break)


def test_delete_transfer_mpc_calls_counts_a_gone_leg_as_deleted(monkeypatch):
    deleted = []

    class FakePV:
        def __init__(self, a, t):
            pass

        def call_delete(self, cu):
            deleted.append(cu)
            return "", 404, "not found"

    class VR:
        def get_transfer_mpc_call_uuids(self, qnr):
            return ["t1", "t2"]

    monkeypatch.setattr(ct.pv, "PlivoVoiceClient", FakePV)
    err = ct.delete_transfer_mpc_calls("ct", "qnr", "A", "T", voice_redis=VR())
    assert err is None and deleted == ["t1", "t2"]


def test_hangup_call_for_mpc_member_matches_active(monkeypatch):
    _participants(
        monkeypatch,
        [
            {"member_address": "sip:b@x", "exit_cause": "hangup", "call_uuid": "old"},
            {"member_address": "sip:b@x", "exit_cause": "", "call_uuid": "leg-9"},
        ],
    )
    deleted = []

    class FakePV:
        def __init__(self, a, t):
            pass

        def call_delete(self, cu):
            deleted.append(cu)
            return "", 204, None

    monkeypatch.setattr(ct.pv, "PlivoVoiceClient", FakePV)
    err = ct.hangup_call_for_mpc_member("mpc_q", "sip:b@x", "A", "T")
    assert err is None and deleted == ["leg-9"]  # only the ACTIVE matching leg
