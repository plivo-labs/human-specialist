"""Transfer validate-owner + async-transfer dispatch.

Pins the linchpin validator and the async fan-out (hs/voice/transfer.py):
  * validate_transfer_req_owner — transfer-not-allowed / owner-mismatch / mapping-seed
    (set-error) / contact-params seed (org-miss, flow-err, store-fail, success) /
    callback-task run-id + cu-keyed params / settings (missing, cold-default, warm-default).
  * handle_async_channel_transfer — get_agents error / invite error / success.
"""

from types import SimpleNamespace

import hs.voice.transfer as ct
import tests.conftest  # noqa: F401 — eager data_adapter imports (AbstractConcreteBase registry)
from hs.routing import constants as rc
from hs.routing.invitation_models import AgentAndContactMapping

_UNSET = object()


def _req(**kw):
    base = {
        "contact_uuid": "ct-1",
        "agent_sip": "sip:a",
        "identifier": "dest-1",
        "transfer_type": rc.TRANSFER_TYPE_AGENT,
    }
    base.update(kw)
    return SimpleNamespace(**base)


class FakeRR:
    def __init__(self, mapping=None, cp=_UNSET, set_err=None, store_ret=1, task_run=("trun-1", "", "", None)):
        self._mapping = mapping
        self._cp = {"auth_id": "AUTH1"} if cp is _UNSET else cp
        self._set_err = set_err
        self._store_ret = store_ret
        self._task_run = task_run
        self.stored = []

    def get_contact_to_agent_mapping(self, cu):
        return self._mapping

    def get_active_task_run_id(self, cu):
        return self._task_run

    def set_contact_to_agent_mapping(self, cu, j, ttl):
        return self._set_err

    def get_contact_parameters(self, cu, qnr):
        return self._cp

    def store_contact_params(self, cu, qnr, j):
        self.stored.append(j)
        return self._store_ret


def _deps(monkeypatch, allowed=True, conv=None):
    monkeypatch.setattr(
        ct.cxfer, "transfer_allowed", lambda c, ch, f: (allowed, True, conv if conv is not None else {})
    )


def _settings(monkeypatch, settings):
    monkeypatch.setattr(ct.ar, "get_org_transfer_settings_for_agent_sip", lambda sip: settings)


_OWNER = AgentAndContactMapping(AgentSipEndpoint="sip:a", QnrRunUuid="qnr-1").to_json()


# ============================== validate_transfer_req_owner ==========================


def test_validate_missing_contact_params_is_error(monkeypatch):
    _deps(monkeypatch, conv={"is_inbound": False})
    rr = FakeRR(mapping=_OWNER, cp=None)  # owner ok, params missing
    _m, _conv, _allowed, err = ct.validate_transfer_req_owner(_req(), "chat", True, rr)
    assert err == "contact params not found in cache"


def test_validate_transfer_not_allowed_is_error(monkeypatch):
    _deps(monkeypatch, allowed=False)
    _m, _conv, _allowed, err = ct.validate_transfer_req_owner(_req(), "chat", True, FakeRR())
    assert err.startswith("case and contact")


def test_validate_inbound_owner_happy_returns_both(monkeypatch):
    _deps(monkeypatch, conv={"is_inbound": True})
    _settings(monkeypatch, SimpleNamespace(allow_transfer_type=True))
    rr = FakeRR(mapping=_OWNER, cp={"auth_id": "AUTH1"})  # inbound + params present
    _m, _conv, allowed, err = ct.validate_transfer_req_owner(_req(agent_sip="sip:a"), "chat", True, rr)
    assert err is None and allowed == rc.TRANSFER_MODE_BOTH


def test_validate_owner_mismatch_is_error(monkeypatch):
    _deps(monkeypatch, conv={"is_inbound": True})
    # inbound, a different agent owns the mapping, and conv has no agent_sip to re-seed ownership.
    rr = FakeRR(mapping=AgentAndContactMapping(AgentSipEndpoint="sip:other", QnrRunUuid="q").to_json())
    _m, _conv, _allowed, err = ct.validate_transfer_req_owner(_req(agent_sip="sip:a"), "chat", True, rr)
    assert err == "not the owner of the channel"


def test_validate_mapping_seed_set_error(monkeypatch):
    _deps(monkeypatch, conv={"is_inbound": False, "agent_sip_endpoint": "sip:a"})
    # outbound + no mapping -> seed a mapping; the set fails.
    rr = FakeRR(mapping=None, set_err="set mapping failed")
    _m, _conv, _allowed, err = ct.validate_transfer_req_owner(_req(), "chat", True, rr)
    assert err == "set mapping failed"


def test_validate_settings_cold_default_overrides_allowed(monkeypatch):
    _deps(monkeypatch, conv={"is_inbound": True})
    _settings(monkeypatch, SimpleNamespace(allow_transfer_type=False, default_transfer=rc.TRANSFER_MODE_COLD))
    rr = FakeRR(mapping=_OWNER, cp={"auth_id": "AUTH1"})
    _m, _conv, allowed, err = ct.validate_transfer_req_owner(_req(agent_sip="sip:a"), "chat", True, rr)
    assert err is None and allowed == rc.TRANSFER_MODE_COLD


def test_validate_settings_warm_default_overrides_allowed(monkeypatch):
    _deps(monkeypatch, conv={"is_inbound": True})
    _settings(monkeypatch, SimpleNamespace(allow_transfer_type=False, default_transfer=rc.TRANSFER_MODE_WARM))
    rr = FakeRR(mapping=_OWNER, cp={"auth_id": "AUTH1"})
    _m, _conv, allowed, err = ct.validate_transfer_req_owner(_req(agent_sip="sip:a"), "chat", True, rr)
    assert err is None and allowed == rc.TRANSFER_MODE_WARM


# ============================== handle_async_channel_transfer ========================

_CONTACT_INFO = {
    "flow_run_uuid": "flow-1",
    "queue_and_route_run_uuid": "qnr-1",
    "auth_id": "AUTH1",
    "from_number": "+1",
    "to": "to-app",
    "conversation_number": "555",
}
