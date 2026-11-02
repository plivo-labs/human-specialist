"""The add/remove/owner-hold paths of hs/voice/participant.py (test_monitor_participant covers the rest):

  * add_participant      — MPC body assembly, inbound/outbound from-number, add success +
                          customer-hold, and every failure fork (validate / record-value /
                          add-fail / hold-fail).
  * remove_participant   — owner-validate gate, missing-creds guard, call_delete fork.
  * _owner_params_for    — owner mapping + contact-params resolution (the hold/switch gate).
  * hold_participant     — the owner-gated hold passthrough.

Reuses the FakeRR / FakeVR / Org / _owner_mapping fixtures' shape from test_monitor_participant.
"""

import pytest

import hs.voice.participant as vpart
import tests.conftest  # noqa: F401 — eager data_adapter imports (AbstractConcreteBase registry)
from hs.routing.invitation_models import AgentAndContactMapping


class Org:
    auth_id = "AUTH1"
    auth_token = "tok"


class FakeRR:
    def __init__(self, mapping=None, cp=None):
        self.mapping = mapping
        self.cp = cp
        self.cp_calls = []  # (contact_uuid, qnr_run_uuid) passed to get_contact_parameters

    def get_contact_to_agent_mapping(self, ct):
        return self.mapping

    def get_contact_parameters(self, ct, qnr):
        self.cp_calls.append((ct, qnr))
        return self.cp


def _mapping(sip="sip:a@x", inbound=True, to="+1to", frm="+1from"):
    return AgentAndContactMapping(
        AgentSipEndpoint=sip, QnrRunUuid="qnr-1", FlowRunUUID="flow-1", IsInbound=inbound, ToNumber=to, FromNumber=frm
    )


def _params(auth_id="AUTH1", auth_token="tok"):
    return {"auth_id": auth_id, "auth_token": auth_token, "conversation_number": "555"}


def _patch_validate(monkeypatch, mapping, params, err):
    monkeypatch.setattr(
        vpart,
        "validate_add_participant_req_owner",
        lambda req, v, routing_redis=None, voice_redis=None, deps=None: (mapping, params, err),
    )


def _patch_callback_urls(monkeypatch):
    for fn in (
        "mpc_call_status_callback_url",
        "mpc_status_callback_url",
        "mpc_recording_callback_url",
        "mpc_transcription_callback_url",
    ):
        monkeypatch.setattr(vpart.pv, fn, lambda *a, **k: "https://cb")
    monkeypatch.setattr(vpart.pv, "make_mpc_id", lambda friendly_name: "mpc-id")
    monkeypatch.setattr(vpart.pv, "compact_json", lambda body: b"{}")


class _FakeClient:
    """PlivoVoiceClient stand-in. add_resp drives mpc_add_participant; del_err drives call_delete."""

    def __init__(self, auth_id, auth_token, add_resp=("{}", 200, None), del_err=None, captured=None):
        self._add_resp = add_resp
        self._del_err = del_err
        self._captured = captured if captured is not None else {}

    def mpc_add_participant(self, mpc_id, body, raw_body=None):
        self._captured["body"] = body
        self._captured["mpc_id"] = mpc_id
        return self._add_resp

    def call_delete(self, call_uuid):
        self._captured["deleted"] = call_uuid
        return "", 204, self._del_err


# ============================== add_participant =======================================


def test_add_participant_validation_failure_returns_error(monkeypatch):
    _patch_validate(monkeypatch, None, None, "not the owner of the channel")
    leg, err = vpart.add_participant({"contact_uuid": "ct"}, routing_redis=FakeRR(), voice_redis=None)
    assert (leg, err) == (None, "not the owner of the channel")


def test_add_participant_inbound_happy_path_holds_nobody_and_returns_leg(monkeypatch):
    _patch_validate(monkeypatch, _mapping(inbound=True), _params(), None)
    _patch_callback_urls(monkeypatch)
    monkeypatch.setattr(
        vpart.ar, "get_organisation_auth_token_and_record_value", lambda auth_id, inbound: ("tok", True, None)
    )
    captured = {}
    add_resp = ({"calls": [{"call_uuid": "leg-77"}]}, 201, None)
    monkeypatch.setattr(
        vpart.pv, "PlivoVoiceClient", lambda a, t: _FakeClient(a, t, add_resp=add_resp, captured=captured)
    )
    held = []
    monkeypatch.setattr(vpart, "hold_unhold_members", lambda *a, **k: held.append(a) or None)

    leg, err = vpart.add_participant(
        {"contact_uuid": "ct", "member": "+1member", "agent_sip": "sip:a@x", "participant_alias": "Ben"},
        routing_redis=FakeRR(),
        voice_redis=None,
    )
    assert (leg, err) == ("leg-77", None)
    assert captured["body"]["from"] == "+1to"  # inbound -> from = mapping.to_number
    assert captured["body"]["to"] == "+1member"
    assert captured["body"]["record"] is True  # record_value threaded into the MPC body
    assert held == []


def test_add_participant_outbound_uses_from_number(monkeypatch):
    _patch_validate(monkeypatch, _mapping(inbound=False), _params(), None)
    _patch_callback_urls(monkeypatch)
    monkeypatch.setattr(
        vpart.ar, "get_organisation_auth_token_and_record_value", lambda auth_id, inbound: ("tok", False, None)
    )
    captured = {}
    add_resp = ({"calls": [{"call_uuid": "leg-9"}]}, 201, None)
    monkeypatch.setattr(
        vpart.pv, "PlivoVoiceClient", lambda a, t: _FakeClient(a, t, add_resp=add_resp, captured=captured)
    )
    leg, err = vpart.add_participant(
        {"contact_uuid": "ct", "member": "+1m", "agent_sip": "sip:a@x"}, routing_redis=FakeRR(), voice_redis=None
    )
    assert (leg, err) == ("leg-9", None)
    assert captured["body"]["from"] == "+1from"  # outbound -> from = mapping.from_number


def test_add_participant_record_value_error_returns_error(monkeypatch):
    _patch_validate(monkeypatch, _mapping(), _params(), None)
    _patch_callback_urls(monkeypatch)
    monkeypatch.setattr(
        vpart.ar,
        "get_organisation_auth_token_and_record_value",
        lambda auth_id, inbound: ("", False, "org record lookup failed"),
    )
    leg, err = vpart.add_participant(
        {"contact_uuid": "ct", "agent_sip": "sip:a@x"}, routing_redis=FakeRR(), voice_redis=None
    )
    assert (leg, err) == (None, "org record lookup failed")


def test_add_participant_mpc_add_failure_returns_error_and_no_calls(monkeypatch):
    _patch_validate(monkeypatch, _mapping(), _params(), None)
    _patch_callback_urls(monkeypatch)
    monkeypatch.setattr(
        vpart.ar, "get_organisation_auth_token_and_record_value", lambda auth_id, inbound: ("tok", True, None)
    )
    monkeypatch.setattr(vpart.pv, "PlivoVoiceClient", lambda a, t: _FakeClient(a, t, add_resp=("", 502, "plivo 502")))
    leg, err = vpart.add_participant(
        {"contact_uuid": "ct", "agent_sip": "sip:a@x"}, routing_redis=FakeRR(), voice_redis=None
    )
    assert (leg, err) == (None, "plivo 502")


def test_add_participant_empty_calls_list_is_failure(monkeypatch):
    _patch_validate(monkeypatch, _mapping(), _params(), None)
    _patch_callback_urls(monkeypatch)
    monkeypatch.setattr(
        vpart.ar, "get_organisation_auth_token_and_record_value", lambda auth_id, inbound: ("tok", True, None)
    )
    # add 'succeeds' (err None) but returns no calls -> still treated as a failure (returns aerr=None).
    monkeypatch.setattr(
        vpart.pv, "PlivoVoiceClient", lambda a, t: _FakeClient(a, t, add_resp=({"calls": []}, 201, None))
    )
    leg, _err = vpart.add_participant(
        {"contact_uuid": "ct", "agent_sip": "sip:a@x"}, routing_redis=FakeRR(), voice_redis=None
    )
    assert leg is None  # no leg created


# ============================== remove_participant ===================================


def test_remove_participant_validation_failure_returns_error(monkeypatch):
    _patch_validate(monkeypatch, None, None, "not the owner of the channel")
    assert vpart.remove_participant({"contact_uuid": "ct"}, routing_redis=FakeRR()) == "not the owner of the channel"


@pytest.mark.parametrize(
    "auth_id,auth_token",
    [("", ""), ("MATESTAUTHID00000001", ""), ("", "token-only")],
    ids=["both", "no-token", "no-id"],
)
def test_remove_participant_missing_creds_cannot_make_client(monkeypatch, auth_id, auth_token):
    _patch_validate(monkeypatch, _mapping(), {"auth_id": auth_id, "auth_token": auth_token}, None)
    assert (
        vpart.remove_participant({"contact_uuid": "ct", "member_call_uuid": "leg-1"}, routing_redis=FakeRR())
        == "failed to create plivo client"
    )


def test_remove_participant_call_delete_failure(monkeypatch):
    _patch_validate(monkeypatch, _mapping(), _params(), None)
    monkeypatch.setattr(vpart.pv, "PlivoVoiceClient", lambda a, t: _FakeClient(a, t, del_err="plivo delete 500"))
    assert (
        vpart.remove_participant({"contact_uuid": "ct", "member_call_uuid": "leg-1"}, routing_redis=FakeRR())
        == "plivo delete 500"
    )


def test_remove_participant_success_deletes_the_member_leg(monkeypatch):
    _patch_validate(monkeypatch, _mapping(), _params(), None)
    captured = {}
    monkeypatch.setattr(vpart.pv, "PlivoVoiceClient", lambda a, t: _FakeClient(a, t, captured=captured))
    assert (
        vpart.remove_participant({"contact_uuid": "ct", "member_call_uuid": "leg-42"}, routing_redis=FakeRR()) is None
    )
    assert captured["deleted"] == "leg-42"  # the member leg is the one removed


# ============================== _owner_params_for + hold_participant ===========


def test_owner_params_for_missing_mapping_is_redis_nil():
    mapping, params, err = vpart._owner_params_for(
        {"contact_uuid": "ct", "agent_sip": "s"}, routing_redis=FakeRR(mapping=None)
    )
    assert (mapping, params, err) == (None, None, "not found in cache")


def test_owner_params_for_owner_mismatch():
    rr = FakeRR(mapping=_mapping(sip="sip:other@x").to_json())
    mapping, params, err = vpart._owner_params_for({"contact_uuid": "ct", "agent_sip": "sip:a@x"}, routing_redis=rr)
    assert err == "not the owner of the channel"


def test_owner_params_for_missing_contact_params_is_redis_nil():
    rr = FakeRR(mapping=_mapping(sip="sip:a@x").to_json(), cp=None)
    mapping, params, err = vpart._owner_params_for({"contact_uuid": "ct", "agent_sip": "sip:a@x"}, routing_redis=rr)
    assert err == "not found in cache"


def test_owner_params_for_outbound_keys_params_by_flow_run():
    # outbound mapping -> contact params are fetched keyed by the FLOW run, not the QNR run.
    rr = FakeRR(mapping=_mapping(sip="sip:a@x", inbound=False).to_json(), cp={"auth_id": "A"})
    mapping, params, err = vpart._owner_params_for({"contact_uuid": "ct", "agent_sip": "sip:a@x"}, routing_redis=rr)
    assert err is None
    assert rr.cp_calls == [("ct", "flow-1")]


def test_hold_participant_passthrough_success(monkeypatch):
    monkeypatch.setattr(vpart, "_owner_params_for", lambda req, rr=None: (_mapping(inbound=True), _params(), None))
    seen = {}
    monkeypatch.setattr(
        vpart,
        "hold_unhold_members",
        lambda mpc, a, t, ct, sip, hold, member="": seen.update(hold=hold, member=member) or None,
    )
    err = vpart.hold_participant(
        {"contact_uuid": "ct", "agent_sip": "s", "hold_value": True, "member_call_uuid": "leg-5"}
    )
    assert err is None
    assert seen == {"hold": True, "member": "leg-5"}  # hold value + member leg threaded through


def test_hold_participant_owner_error_short_circuits(monkeypatch):
    monkeypatch.setattr(vpart, "_owner_params_for", lambda req, rr=None: (None, None, "not found in cache"))
    called = []
    monkeypatch.setattr(vpart, "hold_unhold_members", lambda *a, **k: called.append(1))
    assert vpart.hold_participant({"contact_uuid": "ct", "agent_sip": "s"}) == "not found in cache"
    assert called == []  # owner gate fails -> no hold attempted


def test_hold_participant_hold_failure_is_returned(monkeypatch):
    monkeypatch.setattr(vpart, "_owner_params_for", lambda req, rr=None: (_mapping(), _params(), None))
    monkeypatch.setattr(vpart, "hold_unhold_members", lambda *a, **k: "hold boom")
    assert vpart.hold_participant({"contact_uuid": "ct", "agent_sip": "s"}) == "hold boom"
