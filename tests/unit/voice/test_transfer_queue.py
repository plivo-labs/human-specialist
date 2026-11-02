"""Transfer agent-read/outbound ERROR forks (hs/voice/transfer.py):
* get_agents_for_team — get_agents error
* get_agent_by_capacity — agent-miss / acw-exc / capacity-exc
* handle_outbound_call_transfer_success — mapping-miss / aom-rows-None
"""

import pytest

import hs.voice.transfer as ct
import tests.conftest  # noqa: F401 — eager data_adapter imports (AbstractConcreteBase registry)
from hs.routing.invitation_models import AgentAndContactMapping
from hs.voice.transfer_models import GetAgentsByCapacity, GetAgentsParams


class FakeRR:
    def __init__(self, tip_ok=True, mapping=None):
        self._tip_ok = tip_ok
        self.mapping = mapping
        self.deleted_tip = []
        self.set_mapping = []
        self.del_locks = []

    def set_transfer_in_progress(self, ct_, info, ttl):
        return self._tip_ok

    def delete_transfer_in_progress(self, ct_):
        self.deleted_tip.append(ct_)

    def get_contact_to_agent_mapping(self, ct_):
        return self.mapping

    def set_contact_to_agent_mapping(self, ct_, j, ttl):
        self.set_mapping.append((ct_, j, ttl))
        return

    def delete_reverse_routing_lock(self, sip, conv, ch):
        self.del_locks.append((sip, conv, ch))


def _deps(monkeypatch, **over):
    monkeypatch.setattr(ct.cxfer, "transfer_child", over.get("child", lambda *a, **k: ("child-1", None)))
    monkeypatch.setattr(ct.cxfer, "signal_chunk", lambda *a, **k: None)
    monkeypatch.setattr(ct.cxfer, "transfer_allowed", lambda *a: (True, True, {}))


@pytest.fixture(autouse=True)
def _default_deps(monkeypatch):
    _deps(monkeypatch)


# ============================== agent reads — error forks =====================


def test_get_agents_for_team_get_agents_error(monkeypatch):
    from hs.routing import get_agents as routing_get_agents

    monkeypatch.setattr(routing_get_agents, "get_agents", lambda *a, **k: (None, "router down"))
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
        rr=FakeRR(),
    )
    assert (agents, err) == (None, "router down")


def test_get_agent_by_capacity_agent_missing(monkeypatch):
    monkeypatch.setattr(ct.ar, "get_agent_by_uuid", lambda u, _auth: None)
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
        rr=FakeRR(),
    )
    assert (agents, err) == (None, "not found")


def test_get_agent_by_capacity_acw_exception(monkeypatch):
    from hs.routing import acw as routing_acw

    monkeypatch.setattr(ct.ar, "get_agent_by_uuid", lambda u, _auth: type("A", (), {"sip_endpoint": "sip:x"})())
    monkeypatch.setattr(
        routing_acw,
        "get_agents_on_active_call_and_in_acw",
        lambda auth, agents: (_ for _ in ()).throw(RuntimeError("acw down")),
    )
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
        rr=FakeRR(),
    )
    assert agents is None and "acw down" in err


def test_get_agent_by_capacity_selection_exception(monkeypatch):
    from hs.routing import acw as routing_acw
    from hs.routing import selection as routing_selection

    monkeypatch.setattr(ct.ar, "get_agent_by_uuid", lambda u, _auth: type("A", (), {"sip_endpoint": "sip:x"})())
    monkeypatch.setattr(routing_acw, "get_agents_on_active_call_and_in_acw", lambda auth, agents: "ACW")
    monkeypatch.setattr(
        routing_selection,
        "get_available_agents_by_capacity",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("capacity down")),
    )
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
        rr=FakeRR(),
    )
    assert agents is None and "capacity down" in err


# ============================== outbound forks ================================


def test_outbound_call_transfer_success_mapping_missing(monkeypatch):
    assert (
        ct.handle_outbound_call_transfer_success("ct-9", {"agent_sip_endpoint": "sip:b"}, rr=FakeRR(mapping=None))
        == "not found in cache"
    )


def test_outbound_call_transfer_success_aom_rows_none(monkeypatch):
    rr = FakeRR(mapping=AgentAndContactMapping(AgentSipEndpoint="sip:a").to_json())
    monkeypatch.setattr(ct.ar, "get_account_rows_for_sip_endpoints", lambda sips: None)
    assert ct.handle_outbound_call_transfer_success("ct-9", {"agent_sip_endpoint": "sip:b"}, rr=rr) == "not found"
