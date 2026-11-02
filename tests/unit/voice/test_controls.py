"""Voice controls: handle_hold_call (mapping/cp misses, first-customer hold, case-insensitive role match),
end_mpc_calls (the AQE un-answered branch — ringing legs cancelled, the queue row dropped unless the room's
end called it; the answered branch — MPCCALLSKEY+MTCK deletes, MPCEndInitiated marker,
swallowed per-call errors, the no-client RuntimeError), handle_outbound_call (the unchecked
conversation_data asserts, the org_capacity=1 acquire, the mapping write), the call-event publish
(sip→aom resolve), and _end_mpc_calls_seam.
"""

import json

import pytest

import hs.voice.controls as vctl
import tests.conftest  # noqa: F401 — eager data_adapter imports (AbstractConcreteBase registry)
from hs.routing import constants as rc
from hs.voice import constants as vc

BEN = "sip:ben@x.sip.plivo.com"
CP = {
    "contact_uuid": "c-1",
    "conversation_number": "5550001111",
    "auth_id": "AUTH1",
    "auth_token": "tok",
    "flow_run_uuid": "flow-1",
    "queue_and_route_run_uuid": "qnr-1",
    "from_number": "+14150000001",
    "node_uuid": "n-1",
    "queue_uuid": "q-1",
    "flow_name": "flow",
}


def _mapping(sip=BEN, qnr="qnr-1", agent_uuid="aom-ben"):
    return {
        "AgentSipEndpoint": sip,
        "FromNumber": "+14150000001",
        "QnrRunUuid": qnr,
        "AgentUUID": agent_uuid,
        "AgentCallUuid": "leg-1",
        "PreviousPrimaryAgent": "",
        "FlowRunUUID": "flow-1",
        "ToNumber": "+18000000001",
        "IsInbound": True,
    }


class FakeRR:
    def __init__(self, *, mapping=None, cp=CP, cap_err=None, map_err=None):
        self.mapping = mapping
        self.cp = cp
        self.cap_err = cap_err
        self.map_err = map_err
        self.calls = {"del_lock": [], "cap": [], "set_mapping": []}

    def get_contact_to_agent_mapping(self, contact_uuid):
        return self.mapping

    def get_contact_parameters(self, contact_uuid, qnr_run_uuid):
        return self.cp

    def delete_reverse_routing_lock(self, sip, conv, channel):
        self.calls["del_lock"].append((sip, conv, channel))
        return

    def increase_agent_capacity(self, auth_id, agent, channel, org_capacity, conversation_id):
        self.calls["cap"].append((auth_id, agent, channel, org_capacity, conversation_id))
        return (-2, self.cap_err) if self.cap_err else (0, None)

    def set_contact_to_agent_mapping(self, contact_uuid, mapping_json, ttl):
        self.calls["set_mapping"].append((contact_uuid, mapping_json, ttl))
        return self.map_err


class FakeVR:
    def __init__(self, *, call_uuids=(), transfer_uuids=(), list_raises=None, transfer_raises=None):
        self.call_uuids = list(call_uuids)
        self.transfer_uuids = list(transfer_uuids)
        self.list_raises = list_raises
        self.transfer_raises = transfer_raises
        self.calls = {"toggle": [], "set_event": []}

    def toggle_active_conference(self, qnr, is_active, expiry):
        self.calls["toggle"].append((qnr, is_active, expiry))
        return

    def get_mpc_call_uuids(self, flow_run_uuid, qnr_run_uuid):
        if self.list_raises:
            raise RuntimeError(self.list_raises)
        return list(self.call_uuids)

    def get_transfer_mpc_call_uuids(self, qnr_run_uuid):
        if self.transfer_raises:
            raise RuntimeError(self.transfer_raises)
        return list(self.transfer_uuids)

    def set_mpc_callback_event(self, call_uuid, qnr_run_uuid, event_name, timestamp=""):
        self.calls["set_event"].append((call_uuid, qnr_run_uuid, event_name, timestamp))
        return


class FakePlivoClient:
    deleted = []
    delete_err = None
    delete_status = 204

    def __init__(self, auth_id, auth_token):
        self.auth = (auth_id, auth_token)

    def call_delete(self, call_uuid, *, quiet_not_found=False):
        FakePlivoClient.deleted.append(call_uuid)
        return ("", FakePlivoClient.delete_status, FakePlivoClient.delete_err)


@pytest.fixture(autouse=True)
def _reset_fake_plivo():
    FakePlivoClient.deleted = []
    FakePlivoClient.delete_err = None
    FakePlivoClient.delete_status = 204


# ============================================================================ handle_hold_call
def _hold(monkeypatch, rr, participants=(), perr=None, herr=None, hold_value=True):
    from hs.voice import agent_leg as voice_agent_leg
    from hs.voice import hangup as voice_hangup

    holds = []
    monkeypatch.setattr(voice_agent_leg, "_get_participants", lambda name, a, t: (list(participants), perr))
    monkeypatch.setattr(
        voice_hangup,
        "hold_or_unhold_call",
        lambda hv, ct, qnr, member, a, t: holds.append((hv, ct, qnr, member, a, t)) or herr,
    )
    monkeypatch.setattr(vctl, "RoutingRedis", lambda: rr)
    err = vctl.handle_hold_call("c-1", hold_value)
    return err, holds


def test_hold_mapping_miss(monkeypatch):
    err, holds = _hold(monkeypatch, FakeRR(mapping=None))
    assert err == "call not found" and holds == []


def test_hold_contact_params_miss(monkeypatch):
    err, holds = _hold(monkeypatch, FakeRR(mapping=_mapping(), cp=None))
    assert err == "contact params not found" and holds == []


def test_hold_participants_error_propagates(monkeypatch):
    err, holds = _hold(monkeypatch, FakeRR(mapping=_mapping()), perr="mpc not found")
    assert err == "mpc not found" and holds == []


def test_hold_no_customers(monkeypatch):
    err, holds = _hold(monkeypatch, FakeRR(mapping=_mapping()), participants=[{"role": "agent", "member_id": "10"}])
    assert err == "no customers in the mpc" and holds == []


def test_hold_first_customer_equalfold(monkeypatch):
    # case-insensitive: 'Customer' matches; FIRST customer row's member id is used
    err, holds = _hold(
        monkeypatch,
        FakeRR(mapping=_mapping()),
        participants=[
            {"role": "agent", "member_id": "10"},
            {"role": "Customer", "member_id": "21", "hold": True},
            {"role": "customer", "member_id": "22", "hold": True},
        ],
        hold_value=False,
    )
    assert err is None
    assert holds == [(False, "c-1", "qnr-1", "21", "AUTH1", "tok")]


def test_hold_update_error_propagates(monkeypatch):
    err, holds = _hold(
        monkeypatch,
        FakeRR(mapping=_mapping()),
        participants=[{"role": "customer", "member_id": "21"}],
        herr="update failed",
    )
    assert err == "update failed" and len(holds) == 1


# ============================================================================ end_mpc_calls
class _Entry:
    id = 41
    status = "QUEUED"


def _end(monkeypatch, rr, vr, *, entry="raise", drop=True):
    """Drive end_mpc_calls with the AQE read and the queue-row drop recorded."""
    from hs.routing import active_queue as aq_mod

    calls = {"dropped": []}

    def _get_entry(db, contact_uuid, run=""):
        if entry == "raise":
            raise RuntimeError("not found")
        return entry

    monkeypatch.setattr(aq_mod, "get_current_active_queue_entry_by_contact_uuid", _get_entry)
    monkeypatch.setattr(vctl, "drop_abandoned", lambda c, q: calls["dropped"].append((c, q)))
    monkeypatch.setattr(vctl.pv, "PlivoVoiceClient", FakePlivoClient)
    monkeypatch.setattr(vctl, "RoutingRedis", lambda: rr)
    monkeypatch.setattr(vctl, "VoiceRedis", lambda: vr)
    res, err = vctl.end_mpc_calls("c-1", "qnr-1", db=object(), drop=drop)
    return res, err, calls


def test_end_contact_params_miss(monkeypatch):
    monkeypatch.setattr(vctl, "RoutingRedis", lambda: FakeRR(cp=None))
    monkeypatch.setattr(vctl, "VoiceRedis", lambda: FakeVR())
    res, err = vctl.end_mpc_calls("c-1", "qnr-1", db=object())
    assert (res, err) == ("", "not found in cache")


@pytest.mark.parametrize("drop", [True, False])
def test_an_unanswered_room_cancels_its_rings_and_drops_the_caller_unless_the_room_end_called(monkeypatch, drop):
    vr = FakeVR(call_uuids=["leg-1"])
    res, err, calls = _end(monkeypatch, FakeRR(), vr, entry=_Entry(), drop=drop)
    assert (res, err, vr.calls["toggle"], FakePlivoClient.deleted) == ("", None, [("qnr-1", "false", 0)], ["leg-1"])
    assert (calls["dropped"], vr.calls["set_event"]) == ([("c-1", "qnr-1")] if drop else [], [])


def test_end_answered_branch_deletes_calls(monkeypatch):
    vr = FakeVR(call_uuids=["leg-1", "leg-2"], transfer_uuids=["tleg-1"])
    res, err, calls = _end(monkeypatch, FakeRR(), vr)
    assert (res, err) == ("", None)
    assert vr.calls["set_event"] == [("c-1", "qnr-1", vc.MPC_END_INITIATED, "")]
    assert FakePlivoClient.deleted == ["leg-1", "leg-2", "tleg-1"]
    assert calls["dropped"] == []  # no AQE → nothing to drop


def test_end_answered_delete_errors_continue_cleanup_but_fail_callback(monkeypatch):
    FakePlivoClient.delete_status = 503
    FakePlivoClient.delete_err = "non 2xx response (503)"
    vr = FakeVR(call_uuids=["leg-1", "leg-2"])
    res, err, _calls = _end(monkeypatch, FakeRR(), vr)
    assert res == ""
    assert err == "failed to delete MPC call leg-1: non 2xx response (503)"
    assert FakePlivoClient.deleted == ["leg-1", "leg-2"]  # loop continues past errors


def test_end_answered_404_leg_already_ended(monkeypatch):
    FakePlivoClient.delete_status = 404
    FakePlivoClient.delete_err = "non 2xx response (404)"
    vr = FakeVR(call_uuids=["lost-invite", "leg-1"])

    res, err, _calls = _end(monkeypatch, FakeRR(), vr)

    assert (res, err) == ("", None)
    assert FakePlivoClient.deleted == ["lost-invite", "leg-1"]


def test_end_call_list_error(monkeypatch):
    res, err, _calls = _end(monkeypatch, FakeRR(), FakeVR(list_raises="redis down"))
    assert res == "" and err == "redis down"


def test_end_transfer_list_error_logged_only(monkeypatch):
    vr = FakeVR(call_uuids=["leg-1"], transfer_raises="redis down")
    res, err, _calls = _end(monkeypatch, FakeRR(), vr)
    assert (res, err) == ("", None)
    assert FakePlivoClient.deleted == ["leg-1"]


@pytest.mark.parametrize(
    "auth_id,auth_token",
    [("", ""), ("MATESTAUTHID00000001", ""), ("", "token-only")],
    ids=["both", "no-token", "no-id"],
)
def test_end_without_client_raises(monkeypatch, auth_id, auth_token):
    # Either cred empty + non-empty call list → no client, no delete → raise
    cp = dict(CP, auth_id=auth_id, auth_token=auth_token)
    with pytest.raises(RuntimeError):
        _end(monkeypatch, FakeRR(cp=cp), FakeVR(call_uuids=["leg-1"]))


def test_end_nil_client_empty_list_succeeds(monkeypatch):
    # BOTH creds empty + EMPTY list → the loop never runs → 200 (no error)
    cp = dict(CP, auth_id="", auth_token="")
    res, err, _calls = _end(monkeypatch, FakeRR(cp=cp), FakeVR())
    assert (res, err) == ("", None)


# ============================================================================ outbound
def _outbound_params(**over):
    p = {
        "agent_sip_endpoint": BEN,
        "agent_uuid": "aom-ben",
        "auth_id": "AUTH1",
        "flow_run_id": "flow-9",
        "to_number": "+18000000001",
        "from_number": "+14150000001",
        "conversation_data": {"conversation_number": "7770001111"},
    }
    p.update(over)
    return p


def test_outbound_conversation_data_asserts(monkeypatch):
    monkeypatch.setattr(vctl, "RoutingRedis", FakeRR)
    with pytest.raises(TypeError):
        vctl.handle_outbound_call("ct-1", _outbound_params(conversation_data=None))
    with pytest.raises(KeyError):
        vctl.handle_outbound_call("ct-1", _outbound_params(conversation_data={}))
    with pytest.raises(TypeError):
        vctl.handle_outbound_call("ct-1", _outbound_params(conversation_data={"conversation_number": 777}))


def test_outbound_happy_chain_and_mapping_bytes(monkeypatch):
    rr = FakeRR()
    monkeypatch.setattr(vctl, "RoutingRedis", lambda: rr)
    err = vctl.handle_outbound_call("ct-1", _outbound_params())
    assert err is None
    assert rr.calls["del_lock"] == [(BEN, "7770001111", rc.CHANNEL_CALL)]
    # the org_capacity=1 QUIRK: the repo acquire rides limit 1, NOT the org's call capacity
    assert rr.calls["cap"] == [("AUTH1", BEN, rc.CHANNEL_CALL, 1, "7770001111")]
    ct, mapping_json, ttl = rr.calls["set_mapping"][0]
    assert ct == "ct-1" and ttl == vc.EXPIRY_DURATION_IN_HOURS_CALL * 3600
    assert mapping_json == json.dumps(
        {
            "AgentSipEndpoint": BEN,
            "FromNumber": "+14150000001",
            "QnrRunUuid": "flow-9",
            "AgentUUID": "aom-ben",
            "AgentCallUuid": "ct-1",
            "PreviousPrimaryAgent": "",
            "FlowRunUUID": "flow-9",
            "ToNumber": "+18000000001",
            "IsInbound": False,
        },
        separators=(",", ":"),
    )


def test_outbound_capacity_error_stops(monkeypatch):
    rr = FakeRR(cap_err="error in increasing agent capacity")
    monkeypatch.setattr(vctl, "RoutingRedis", lambda: rr)
    err = vctl.handle_outbound_call("ct-1", _outbound_params())
    assert err == "error in increasing agent capacity"
    assert rr.calls["set_mapping"] == []


def test_outbound_mapping_error_returned(monkeypatch):
    rr = FakeRR(map_err="redis down")
    monkeypatch.setattr(vctl, "RoutingRedis", lambda: rr)
    err = vctl.handle_outbound_call("ct-1", _outbound_params())
    assert err == "redis down"


# ============================================================================ the end-MPC-calls seam
def test_end_mpc_calls_seam_delegates(monkeypatch):
    from hs.voice import callbacks as voice_callbacks

    calls = []
    monkeypatch.setattr(vctl, "end_mpc_calls", lambda ct, qnr: calls.append((ct, qnr)) or ("", None))
    assert voice_callbacks._end_mpc_calls_seam("ct-9", "qnr-9") is None
    assert calls == [("ct-9", "qnr-9")]
    monkeypatch.setattr(vctl, "end_mpc_calls", lambda ct, qnr: ("", "not found in cache"))
    with pytest.raises(RuntimeError):
        voice_callbacks._end_mpc_calls_seam("ct-9", "qnr-9")
