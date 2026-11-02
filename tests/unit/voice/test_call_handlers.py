"""The error/guard forks of the three call-transfer handlers in hs/voice/transfer.py, with
validate_transfer_req_owner and the voice seams mocked:
  * handle_call_switch               — validate / set-role / contact-info / participants /
                                       customer-hold / agent-hold / aom-row-miss
  * handle_call_transfer_merge       — validate / contact-info / participants / hold /
                                       warm-external-MTBPA-fail / aom-row-miss / MTBPA-fail
  * handle_call_transfer_warm_hangup — validate / contact-info / participants /
                                       transfer-calls-delete-err / member-hangup-err
"""

from types import SimpleNamespace

import hs.voice.transfer as ct
import tests.conftest  # noqa: F401 — eager data_adapter imports (AbstractConcreteBase registry)
from hs.routing import constants as rc
from hs.routing.invitation_models import AgentAndContactMapping
from hs.voice import constants as vc

CP = {"auth_id": "AUTH1", "auth_token": "tok", "queue_and_route_run_uuid": "qnr-1", "from_number": "+1"}


def _req(**kw):
    base = {"contact_uuid": "ct-1", "agent_sip": "sip:a", "transfer_type": rc.TRANSFER_TYPE_AGENT, "member": "+1m"}
    base.update(kw)
    return SimpleNamespace(**base)


def _mapping():
    return AgentAndContactMapping(QnrRunUuid="qnr-1", AgentSipEndpoint="sip:a", AgentUUID="aom-a")


_UNSET = object()


class FakeRR:
    def __init__(self, cp=_UNSET, tbp_ok=True):
        self._cp = dict(CP) if cp is _UNSET else cp  # cp=None means "return None" (missing params)
        self._tbp_ok = tbp_ok
        self.tbp = []

    def get_contact_parameters(self, ct_, qnr):
        return self._cp

    def set_to_be_primary_agent(self, qnr, mapping_json, ttl):
        self.tbp.append((qnr, mapping_json, ttl))
        return self._tbp_ok


class FakeVR:
    def __init__(self, role="", set_err=None):
        self._role = role
        self._set_err = set_err

    def get_agent_switch_role(self, qnr, sip):
        return self._role, None

    def set_agent_switch_role(self, qnr, sip, role):
        return self._set_err


def _patch_validate(monkeypatch, err=None, mapping=None):
    monkeypatch.setattr(
        ct, "validate_transfer_req_owner", lambda req, ch, fetch, rr: (mapping or _mapping(), {}, "", err)
    )


def _patch_participants(monkeypatch, participants, err=None):
    import hs.voice.agent_leg as val

    monkeypatch.setattr(val, "_get_participants", lambda name, a, t: (participants, err))


def _patch_hold(monkeypatch, ret=None):
    import hs.voice.hangup as vh

    monkeypatch.setattr(vh, "hold_or_unhold_call", lambda hold, cu, qnr, mid, a, t: ret)
    monkeypatch.setattr(vh, "get_conversation_number_from_contact_uuid", lambda cu, rr: ("555", None))


# ============================== handle_call_switch ===================================


def test_switch_validate_error_returns_err(monkeypatch):
    _patch_validate(monkeypatch, err="not the owner of the channel")
    assert ct.handle_call_switch(_req(), rr=FakeRR(), voice_redis=FakeVR()) == "not the owner of the channel"


def test_switch_set_role_error_returns_err(monkeypatch):
    _patch_validate(monkeypatch)
    assert (
        ct.handle_call_switch(_req(), rr=FakeRR(), voice_redis=FakeVR(set_err="role set failed")) == "role set failed"
    )


def test_switch_contact_info_missing_is_redis_nil(monkeypatch):
    _patch_validate(monkeypatch)
    assert ct.handle_call_switch(_req(), rr=FakeRR(cp=None), voice_redis=FakeVR()) == "not found in cache"


def test_switch_participants_error_returns_err(monkeypatch):
    _patch_validate(monkeypatch)
    _patch_participants(monkeypatch, None, err="mpc err")
    assert ct.handle_call_switch(_req(), rr=FakeRR(), voice_redis=FakeVR()) == "mpc err"


def test_switch_customer_hold_error_returns_err(monkeypatch):
    _patch_validate(monkeypatch)
    _patch_participants(monkeypatch, [{"role": vc.MPC_ROLE_CUSTOMER, "member_id": "1", "member_address": "+c"}])
    _patch_hold(monkeypatch, ret="hold failed")
    assert ct.handle_call_switch(_req(), rr=FakeRR(), voice_redis=FakeVR()) == "hold failed"


def test_switch_agent_hold_error_returns_err(monkeypatch):
    _patch_validate(monkeypatch)
    _patch_participants(monkeypatch, [{"role": "agent", "member_id": "2", "member_address": "sip:b", "exit_cause": ""}])
    _patch_hold(monkeypatch, ret="agent hold failed")
    assert ct.handle_call_switch(_req(), rr=FakeRR(), voice_redis=FakeVR()) == "agent hold failed"


# ============================== handle_call_transfer_merge ===========================


def test_merge_validate_error_returns_err(monkeypatch):
    _patch_validate(monkeypatch, err="owner err")
    assert ct.handle_call_transfer_merge(_req(), rr=FakeRR()) == "owner err"


def test_merge_contact_info_missing_is_redis_nil(monkeypatch):
    _patch_validate(monkeypatch)
    assert ct.handle_call_transfer_merge(_req(), rr=FakeRR(cp=None)) == "not found in cache"


def test_merge_participants_error_returns_err(monkeypatch):
    _patch_validate(monkeypatch)
    _patch_participants(monkeypatch, None, err="mpc err")
    assert ct.handle_call_transfer_merge(_req(), rr=FakeRR()) == "mpc err"


def test_merge_unhold_error_returns_err(monkeypatch):
    _patch_validate(monkeypatch)
    _patch_participants(
        monkeypatch,
        [{"role": "agent", "member_id": "2", "member_address": "sip:b", "exit_cause": "", "call_uuid": "leg-2"}],
    )
    _patch_hold(monkeypatch, ret="unhold failed")
    assert ct.handle_call_transfer_merge(_req(), rr=FakeRR()) == "unhold failed"


def test_merge_warm_external_to_be_primary_failure(monkeypatch):
    _patch_validate(monkeypatch)
    _patch_participants(
        monkeypatch,
        [
            {
                "role": vc.MPC_ROLE_AGENT,
                "member_id": "2",
                "member_address": "sip:b",
                "exit_cause": "",
                "call_uuid": "leg-2",
            }
        ],
    )
    _patch_hold(monkeypatch, ret=None)
    assert (
        ct.handle_call_transfer_merge(
            _req(transfer_type=rc.TRANSFER_TYPE_WARM_EXTERNAL, member="sip:b"), rr=FakeRR(tbp_ok=False)
        )
        == "failed to update to be primary agent"
    )


def test_merge_aom_row_missing_is_error(monkeypatch):
    _patch_validate(monkeypatch)
    _patch_participants(
        monkeypatch,
        [
            {
                "role": vc.MPC_ROLE_AGENT,
                "member_id": "2",
                "member_address": "sip:b",
                "exit_cause": "",
                "call_uuid": "leg-2",
            }
        ],
    )
    _patch_hold(monkeypatch, ret=None)
    monkeypatch.setattr(ct.ar, "get_aom_row_for_sip", lambda addr: None)
    assert ct.handle_call_transfer_merge(_req(), rr=FakeRR()) == "not found"


def test_merge_to_be_primary_failure_after_aom(monkeypatch):
    _patch_validate(monkeypatch)
    _patch_participants(
        monkeypatch,
        [
            {
                "role": vc.MPC_ROLE_AGENT,
                "member_id": "2",
                "member_address": "sip:b",
                "exit_cause": "",
                "call_uuid": "leg-2",
            }
        ],
    )
    _patch_hold(monkeypatch, ret=None)
    monkeypatch.setattr(ct.ar, "get_aom_row_for_sip", lambda addr: SimpleNamespace(uuid="aom-b"))
    assert ct.handle_call_transfer_merge(_req(), rr=FakeRR(tbp_ok=False)) == "failed to update to be primary agent"


# ============================== handle_call_transfer_warm_hangup =====================


def _patch_extract(monkeypatch):
    import hs.voice.agent_leg as val

    monkeypatch.setattr(val, "extract_phone_number_and_extension_if_any", lambda m, flag: (m, ""))


def test_warm_hangup_validate_error_returns_err(monkeypatch):
    _patch_validate(monkeypatch, err="owner err")
    assert ct.handle_call_transfer_warm_hangup(_req(), rr=FakeRR()) == "owner err"


def test_warm_hangup_contact_info_missing_is_redis_nil(monkeypatch):
    _patch_validate(monkeypatch)
    assert ct.handle_call_transfer_warm_hangup(_req(), rr=FakeRR(cp=None)) == "not found in cache"


def test_warm_hangup_participants_error_returns_err(monkeypatch):
    _patch_validate(monkeypatch)
    _patch_extract(monkeypatch)
    _patch_participants(monkeypatch, None, err="mpc err")
    assert ct.handle_call_transfer_warm_hangup(_req(), rr=FakeRR()) == "mpc err"


def test_warm_hangup_still_hangs_up_the_member_when_the_transfer_leg_delete_fails(monkeypatch):
    _patch_validate(monkeypatch)
    _patch_extract(monkeypatch)
    _patch_participants(monkeypatch, [{"member_address": "+1m", "role": vc.MPC_ROLE_CUSTOMER}])
    monkeypatch.setattr(ct, "delete_transfer_mpc_calls", lambda *a, **k: "delete failed")
    hung_up = []
    monkeypatch.setattr(ct, "hangup_call_for_mpc_member", lambda *a, **k: hung_up.append(a[1]))
    assert ct.handle_call_transfer_warm_hangup(_req(member="+1m"), rr=FakeRR()) is None
    assert hung_up == ["+1m"]


def test_warm_hangup_member_hangup_error(monkeypatch):
    _patch_validate(monkeypatch)
    _patch_extract(monkeypatch)
    _patch_participants(monkeypatch, [{"member_address": "+1m", "role": "agent"}])  # not customer
    monkeypatch.setattr(ct, "hangup_call_for_mpc_member", lambda *a, **k: "hangup failed")
    assert ct.handle_call_transfer_warm_hangup(_req(member="+1m"), rr=FakeRR()) == "hangup failed"
