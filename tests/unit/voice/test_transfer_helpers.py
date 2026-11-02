"""The small MPC helpers in hs/voice/transfer.py that the call-transfer handlers delegate to:
* hold_unhold_customer       — find the customer leg, hold/unhold it
* delete_transfer_mpc_calls  — fetch-err / missing-creds / delete-each
* hangup_call_for_mpc_member — participant match + creds guard + delete fork
"""

import pytest

import hs.voice.transfer as ct
import tests.conftest  # noqa: F401 — eager data_adapter imports (AbstractConcreteBase registry)
from hs.voice import constants as vc


class FakePV:
    """PlivoVoiceClient stand-in: call_delete / call_update return (body, status, err)."""

    def __init__(self, auth_id, auth_token, delete_err=None, update=("{}", 200, None), captured=None):
        self._delete_err = delete_err
        self._update = update
        self._captured = captured if captured is not None else {}

    def call_delete(self, call_uuid):
        self._captured.setdefault("deleted", []).append(call_uuid)
        return "", 204, self._delete_err

    def call_update(self, call_uuid, body):
        self._captured["update_body"] = body
        return self._update


def _patch_participants(monkeypatch, participants, err=None):
    import hs.voice.agent_leg as val

    monkeypatch.setattr(val, "_get_participants", lambda name, a, t: (participants, err))


# ============================== hold_unhold_customer ==================================


def test_hold_unhold_customer_participant_fetch_error_returns_error(monkeypatch):
    _patch_participants(monkeypatch, None, err="mpc unreachable")
    assert ct.hold_unhold_customer(True, "cu", "qnr", "A", "T") == "mpc unreachable"


def test_hold_unhold_customer_holds_the_customer_leg(monkeypatch):
    _patch_participants(monkeypatch, [{"role": vc.MPC_ROLE_CUSTOMER, "member_id": "9"}])
    import hs.voice.hangup as vh

    seen = {}
    monkeypatch.setattr(
        vh, "hold_or_unhold_call", lambda hold, cu, qnr, mid, a, t: seen.update(hold=hold, mid=mid) or None
    )
    assert ct.hold_unhold_customer(True, "cu", "qnr", "A", "T") is None
    assert seen == {"hold": True, "mid": "9"}  # only the customer leg is held


def test_hold_unhold_customer_propagates_hold_error(monkeypatch):
    _patch_participants(monkeypatch, [{"role": vc.MPC_ROLE_CUSTOMER, "member_id": "9"}])
    import hs.voice.hangup as vh

    monkeypatch.setattr(vh, "hold_or_unhold_call", lambda *a, **k: "hold failed")
    assert ct.hold_unhold_customer(True, "cu", "qnr", "A", "T") == "hold failed"


# ============================== delete_transfer_mpc_calls =============================


class FakeVR:
    def __init__(self, transferred=None, raise_fetch=False):
        self._transferred = transferred or []
        self._raise = raise_fetch

    def get_transfer_mpc_call_uuids(self, qnr):
        if self._raise:
            raise RuntimeError("cache down")
        return self._transferred


def test_delete_transfer_mpc_calls_fetch_exception_returns_error():
    assert ct.delete_transfer_mpc_calls("cu", "qnr", "A", "T", voice_redis=FakeVR(raise_fetch=True)) == "cache down"


@pytest.mark.parametrize(
    "auth_id,auth_token",
    [("", ""), ("MATESTAUTHID00000001", ""), ("", "token-only")],
    ids=["both", "no-token", "no-id"],
)
def test_delete_transfer_mpc_calls_missing_creds(auth_id, auth_token):
    assert (
        ct.delete_transfer_mpc_calls("cu", "qnr", auth_id, auth_token, voice_redis=FakeVR(transferred=["c1"]))
        == "missing credentials"
    )


def test_delete_transfer_mpc_calls_deletes_each_transferred_call(monkeypatch):
    captured = {}
    monkeypatch.setattr(ct.pv, "PlivoVoiceClient", lambda a, t: FakePV(a, t, captured=captured))
    assert ct.delete_transfer_mpc_calls("cu", "qnr", "A", "T", voice_redis=FakeVR(transferred=["c1", "c2"])) is None
    assert captured["deleted"] == ["c1", "c2"]  # every transferred MPC call is deleted


# ============================== hangup_call_for_mpc_member ============================


def test_hangup_member_participant_fetch_error_returns_error(monkeypatch):
    _patch_participants(monkeypatch, None, err="mpc err")
    assert ct.hangup_call_for_mpc_member("mpc", "+1m", "A", "T") == "mpc err"


@pytest.mark.parametrize(
    "auth_id,auth_token",
    [("", ""), ("MATESTAUTHID00000001", ""), ("", "token-only")],
    ids=["both", "no-token", "no-id"],
)
def test_hangup_member_missing_creds(monkeypatch, auth_id, auth_token):
    _patch_participants(monkeypatch, [{"member_address": "+1m", "exit_cause": "", "call_uuid": "x"}])
    assert ct.hangup_call_for_mpc_member("mpc", "+1m", auth_id, auth_token) == "missing credentials"


def test_hangup_member_delete_error_returns_error(monkeypatch):
    _patch_participants(monkeypatch, [{"member_address": "+1m", "exit_cause": "", "call_uuid": "x"}])
    monkeypatch.setattr(ct.pv, "PlivoVoiceClient", lambda a, t: FakePV(a, t, delete_err="del 500"))
    assert ct.hangup_call_for_mpc_member("mpc", "+1m", "A", "T") == "del 500"


def test_hangup_member_success_deletes_matching_leg(monkeypatch):
    _patch_participants(monkeypatch, [{"member_address": "+1m", "exit_cause": "", "call_uuid": "leg-9"}])
    captured = {}
    monkeypatch.setattr(ct.pv, "PlivoVoiceClient", lambda a, t: FakePV(a, t, captured=captured))
    assert ct.hangup_call_for_mpc_member("mpc", "+1m", "A", "T") is None
    assert captured["deleted"] == ["leg-9"]
