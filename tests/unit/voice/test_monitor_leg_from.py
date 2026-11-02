from types import SimpleNamespace

import pytest

import hs.voice.monitor as vmon
from hs.routing.invitation_models import AgentAndContactMapping


class _RR:
    def __init__(self, raw):
        self.raw = raw

    def get_contact_to_agent_mapping(self, ct):
        return self.raw


@pytest.mark.parametrize(
    "caller, sent_from", [("sip:+14155550123@phone.plivo.com", "+14155550123"), ("+Restricted", "+16505550000")]
)
def test_the_supervisor_leg_rings_from_a_number_plivo_accepts(monkeypatch, caller, sent_from):
    sent = []

    class _Client:
        def __init__(self, *a):
            pass

        def mpc_add_participant(self, mpc_id, body, raw_body=None):
            sent.append(body)
            return None, 201, None

    monkeypatch.setattr(vmon, "check_if_mpc_is_active", lambda *a: None)
    monkeypatch.setattr(vmon.ar, "get_organisation_auth_token_and_record_value", lambda *a: ("t", False, None))
    monkeypatch.setattr(vmon.pv, "PlivoVoiceClient", _Client)
    monkeypatch.setattr(vmon.ar, "get_org_for_auth_id", lambda auth: SimpleNamespace(org_uuid="T1"))
    raw = AgentAndContactMapping(from_number=caller, to_number="+16505550000", flow_run_uuid="run-1").to_json()
    monkeypatch.setattr(vmon, "RoutingRedis", lambda: _RR(raw))
    assert vmon.add_supervisor_to_mpc({"auth_id": "MA1"}, "tok", "ct-1", "sip:sup@x") is None
    assert sent[0]["from"] == sent_from


@pytest.mark.parametrize(
    ("is_inbound", "cp", "asked"), [(True, {}, True), (True, {"inbound": False}, False), (False, None, False)]
)
def test_the_supervisor_leg_records_by_the_calls_direction(monkeypatch, is_inbound, cp, asked):
    """A queued call's stored direction (an outbound hand-off is outbound); POST /calls is outbound."""
    flags = []

    class _Client:
        def __init__(self, *a):
            pass

        def mpc_add_participant(self, mpc_id, body, raw_body=None):
            return None, 201, None

    class _Leg(_RR):
        def get_contact_parameters(self, ct, run):
            return cp

    monkeypatch.setattr(vmon, "check_if_mpc_is_active", lambda *a: None)
    monkeypatch.setattr(
        vmon.ar,
        "get_organisation_auth_token_and_record_value",
        lambda auth, inbound: flags.append(inbound) or ("t", 0, None),
    )
    monkeypatch.setattr(vmon.pv, "PlivoVoiceClient", _Client)
    monkeypatch.setattr(vmon.ar, "get_org_for_auth_id", lambda auth: SimpleNamespace(org_uuid="T1"))
    raw = AgentAndContactMapping(from_number="+14155550123", qnr_run_uuid="q-1", is_inbound=is_inbound).to_json()
    monkeypatch.setattr(vmon, "RoutingRedis", lambda: _Leg(raw))
    assert vmon.add_supervisor_to_mpc({"auth_id": "MA1"}, "tok", "ct-1", "sip:sup@x") is None
    assert flags == [asked]


def test_a_supervisor_leg_that_cannot_be_recorded_is_hung_up_and_the_monitor_fails(monkeypatch):
    hung = []

    class _Client:
        def __init__(self, *a):
            pass

        def mpc_add_participant(self, mpc_id, body, raw_body=None):
            return {"calls": [{"call_uuid": "leg-7"}]}, 201, None

        def call_delete(self, call_uuid, **kw):
            hung.append(call_uuid)
            return None, 204, None

    class _VR:
        def set_supervisor_leg(self, ct, supervisor, leg):
            return "error while storing the supervisor leg"

    monkeypatch.setattr(vmon, "check_if_mpc_is_active", lambda *a: None)
    monkeypatch.setattr(vmon.ar, "get_organisation_auth_token_and_record_value", lambda *a: ("t", False, None))
    monkeypatch.setattr(vmon.pv, "PlivoVoiceClient", _Client)
    monkeypatch.setattr(vmon, "VoiceRedis", _VR)
    monkeypatch.setattr(vmon.ar, "get_org_for_auth_id", lambda auth: SimpleNamespace(org_uuid="T1"))
    raw = AgentAndContactMapping(from_number="+14155550123", flow_run_uuid="run-1").to_json()
    monkeypatch.setattr(vmon, "RoutingRedis", lambda: _RR(raw))
    err = vmon.add_supervisor_to_mpc({"auth_id": "MA1"}, "tok", "ct-1", "sip:sup@x")
    assert (err, hung) == ("error while storing the supervisor leg", ["leg-7"])
