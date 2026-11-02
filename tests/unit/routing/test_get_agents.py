"""get_agents forward-router decision tree (offline-deterministic, HIGH risk).

Pins the control flow: the load order, the retry-limit hard-error, the
sticky-first handling (hit returns / strict sticky 'known_error:' wrap / non-strict + (None,None) fall
through), the retry>0 skip_agents OVERWRITE with the invited set, the TRANSFER branch, the
agent-transfer gate skip, the default-Simultaneous, the empty-list-no-error vs None-with-error
split, and the inbound-Call-only analytics branch. Sticky / DB-selection / capacity are monkeypatched
(each is exhaustively tested on its own) so this isolates get_agents' ORCHESTRATION. No stack.
"""

import json
import types

import pytest

import tests.conftest  # noqa: F401 — eager data_adapter imports (AbstractConcreteBase registry)
from hs.routing import constants as rc
from hs.routing import get_agents as G
from hs.routing.selection import ActiveAgentsOnCallAndACW

AUTH = "AUTHID"
FLOW, RUN = "flow1", "run1"
SIP_A, SIP_B = "sip:a@x", "sip:b@x"


class FakeRR:
    """Stands in for RoutingRedis for the get_agents reads (contact params / retry / qnr-config /
    invited agents). Sticky / selection / capacity are monkeypatched separately. `capacity_code`
    feeds the REAL capacity filter when the boundary test exercises it (default '0' = acquired)."""

    def __init__(self, contact_params=None, retry=0, qnr_raw=None, invited=(), capacity_code="0"):
        self._contact_params = contact_params
        self._retry = retry
        self._qnr_raw = qnr_raw
        self._invited = list(invited)
        self._capacity_code = capacity_code

    def get_contact_parameters(self, contact_uuid, qnr_run_uuid):
        return self._contact_params

    def get_retry_val(self, flow, run):
        return self._retry

    def get_queue_and_route_config(self, flow, run):
        return self._qnr_raw

    def get_invited_agents(self, flow, run):
        return list(self._invited)

    def update_agent_capacity(self, auth_id, agent, channel, org_capacity, old_contact="", new_contact=""):
        return self._capacity_code  # only used by the REAL capacity filter in the boundary test


def _qnr_raw(**over):
    d = {
        "queue": "q1",
        "routing": rc.SIMULTANEOUS_ROUTING,
        "agent_limit": 3,
        "retry_limit": 2,
        "routing_teams": [],
        "finalRoutingAgents": ["fa1"],
        "sticky_routing_options": "No sticky routing",
    }
    d.update(over)
    return json.dumps(d)


def _settings(capacity=None, sim_limit=10, osr_enabled=True):
    return types.SimpleNamespace(
        capacity=capacity if capacity is not None else {"chat": 10, "call": 1},
        simultaneous_agent_limit=sim_limit,
        outbound_sticky_routing={"enabled": osr_enabled},
    )


@pytest.fixture()
def patched(monkeypatch):
    """Default wiring: settings present; sticky falls through (None,None); selection returns [A,B];
    capacity returns the input unchanged. Each test overrides what it needs. Records the args passed
    to the DB-selection + capacity seams so the wiring can be asserted."""
    rec = {"selection": None, "capacity": None}
    monkeypatch.setattr(G.RoutingSettings, "get_org_routing_settings", lambda auth_id: _settings())
    monkeypatch.setattr(G, "get_sticky_routing_agents", lambda *a, **k: (None, None))
    import hs.routing.acw as racw

    monkeypatch.setattr(
        racw, "get_agents_on_active_call_and_in_acw", lambda auth_id, agents: ActiveAgentsOnCallAndACW()
    )

    def fake_selection(agent_routing_list, routing_teams, skills, skip, routing_type, auth_id):
        rec["selection"] = {
            "agent_routing_list": list(agent_routing_list),
            "routing_teams": list(routing_teams),
            "skills": tuple(skills),
            "skip": list(skip),
            "routing_type": routing_type,
        }
        return [SIP_A, SIP_B]

    monkeypatch.setattr(G, "get_agents_for_contact", fake_selection)

    def fake_capacity(
        agents, channel, capacity, agent_limit, auth_id, acw=None, conversation_id="", routing_redis=None, **k
    ):
        rec["capacity"] = {
            "agents": list(agents),
            "channel": channel,
            "capacity": capacity,
            "agent_limit": agent_limit,
            "auth_id": auth_id,
            "acw": acw,
        }
        return list(agents)

    monkeypatch.setattr(G, "get_available_agents_by_capacity", fake_capacity)
    return rec


def _call(rr, *, channel="chat", is_inbound=True, is_transfer=False, t_id="", t_type="", skip=()):
    return G.get_agents(
        "c1",
        channel,
        AUTH,
        "conv1",
        FLOW,
        RUN,
        transfer_to=t_id if is_transfer else None,
        transfer_type=t_type,
        skip_agents=list(skip),
        contact_identifier="+1-555",
        contact_params=rr.get_contact_parameters("c1", RUN) if is_inbound else None,
        routing_redis=rr,
    )


# ---------------------------------------------------------------------------------------------
# read-error hard aborts
# ---------------------------------------------------------------------------------------------
def test_settings_missing_aborts(patched, monkeypatch):
    monkeypatch.setattr(G.RoutingSettings, "get_org_routing_settings", lambda auth_id: None)
    rr = FakeRR(contact_params={"x": 1}, qnr_raw=_qnr_raw())
    assert _call(rr) == (None, "error fetching org routing settings")


def test_qnr_config_missing_aborts(patched):
    rr = FakeRR(contact_params={"x": 1}, qnr_raw=None)  # non-transfer, MISSING qnr config
    assert _call(rr) == (None, "not found in cache")  # the cache miss passes through


# ---------------------------------------------------------------------------------------------
# retry limit
# ---------------------------------------------------------------------------------------------
def test_retry_limit_reached_is_known_outcome_not_error_log(patched, monkeypatch):
    infos = []
    errors = []
    monkeypatch.setattr(G.logger, "info", lambda *a, **k: infos.append(k.get("msg") or (a[0] if a else "")))
    monkeypatch.setattr(G.logger, "error", lambda *a, **k: errors.append(k.get("msg") or (a[0] if a else "")))
    rr = FakeRR(contact_params={"x": 1}, retry=3, qnr_raw=_qnr_raw(retry_limit=2))
    assert _call(rr) == (None, "known_error: retry limit reached")
    assert any("retry limit reached" in msg for msg in infos)
    assert not any("retry limit reached" in msg for msg in errors)


def test_retry_zero_never_trips_limit(patched):
    rr = FakeRR(contact_params={"x": 1}, retry=0, qnr_raw=_qnr_raw(retry_limit=0))
    agents, err = _call(rr)
    assert err is None and agents == [SIP_A, SIP_B]  # retry==0 skips the limit check


# ---------------------------------------------------------------------------------------------
# sticky-first handling
# ---------------------------------------------------------------------------------------------
def test_sticky_hit_returns_and_skips_db(patched, monkeypatch):
    monkeypatch.setattr(G, "get_sticky_routing_agents", lambda *a, **k: ([SIP_A], None))
    rr = FakeRR(contact_params={"x": 1}, qnr_raw=_qnr_raw())
    agents, err = _call(rr)
    assert (agents, err) == ([SIP_A], None)
    assert patched["selection"] is None  # DB selection NOT consulted on a hit


def test_sticky_strictly_bind_error_is_known_error(patched, monkeypatch):
    monkeypatch.setattr(G, "get_sticky_routing_agents", lambda *a, **k: (None, "boom"))
    rr = FakeRR(contact_params={"x": 1}, qnr_raw=_qnr_raw(sticky_routing_options=rc.STICKY_STRICTLY_BIND))
    assert _call(rr) == (None, "known_error: boom")


def test_sticky_strictly_bind_waits_for_an_unavailable_agent(patched, monkeypatch):
    monkeypatch.setattr(G, "get_sticky_routing_agents", lambda *a, **k: (None, G.STRICT_WAITS[0]))
    rr = FakeRR(contact_params={"x": 1}, qnr_raw=_qnr_raw(sticky_routing_options=rc.STICKY_STRICTLY_BIND))
    assert _call(rr) == ([], None)


def test_sticky_loosely_bind_error_falls_through(patched, monkeypatch):
    monkeypatch.setattr(G, "get_sticky_routing_agents", lambda *a, **k: (None, "ignored"))
    rr = FakeRR(contact_params={"x": 1}, qnr_raw=_qnr_raw(sticky_routing_options=rc.STICKY_LOOSELY_BIND))
    agents, err = _call(rr)
    assert err is None and agents == [SIP_A, SIP_B]  # non-strict err → normal routing
    assert patched["selection"] is not None


def test_sticky_none_falls_through(patched):
    rr = FakeRR(contact_params={"x": 1}, qnr_raw=_qnr_raw())
    agents, err = _call(rr)
    assert (agents, err) == ([SIP_A, SIP_B], None)


# ---------------------------------------------------------------------------------------------
# retry>0 skip_agents OVERWRITE with the invited set
# ---------------------------------------------------------------------------------------------
def test_retry_gt_0_overwrites_skip_with_invited(patched):
    rr = FakeRR(contact_params={"x": 1}, retry=1, qnr_raw=_qnr_raw(retry_limit=5), invited=["sip:inv1@x", "sip:inv2@x"])
    _call(rr, skip=["caller-skip"])
    assert set(patched["selection"]["skip"]) == {"sip:inv1@x", "sip:inv2@x"}  # caller skip discarded


def test_retry_0_keeps_caller_skip(patched):
    rr = FakeRR(contact_params={"x": 1}, retry=0, qnr_raw=_qnr_raw(retry_limit=5), invited=["sip:inv@x"])
    _call(rr, skip=["caller-skip"])
    assert patched["selection"]["skip"] == ["caller-skip"]  # retry==0 keeps the caller's list


# ---------------------------------------------------------------------------------------------
# transfer branches
# ---------------------------------------------------------------------------------------------
def test_transfer_team_uses_identifier_as_team_simultaneous(patched):
    rr = FakeRR(contact_params=None)
    agents, err = _call(rr, is_inbound=False, is_transfer=True, t_id="team-uuid", t_type=rc.TRANSFER_TYPE_TEAM)
    assert err is None and agents == [SIP_A, SIP_B]
    sel = patched["selection"]
    assert sel["routing_teams"] == ["team-uuid"] and sel["routing_type"] == rc.SIMULTANEOUS_ROUTING
    assert patched["capacity"]["agent_limit"] == 10  # simultaneous_agent_limit, not node limit


def test_non_inbound_transfer_reads_no_retry_count(patched, monkeypatch):
    rr = FakeRR(contact_params=None)
    monkeypatch.setattr(rr, "get_retry_val", lambda *a: pytest.fail("retry count read for a non-inbound transfer"))
    agents, err = _call(rr, is_inbound=False, is_transfer=True, t_id="team-uuid", t_type=rc.TRANSFER_TYPE_TEAM)
    assert err is None and agents == [SIP_A, SIP_B]


def test_transfer_type_agent_skips_db_selection_but_runs_capacity(patched, monkeypatch):
    # an agent transfer SKIPS get_agents_for_contact (the DB selection), but the active-call gate +
    # the capacity filter run for ANY non-empty agent list — so the transfer-target SIP IS
    # capacity-filtered.
    monkeypatch.setattr(G.ar, "get_agent_sip_by_uuid", lambda uuid, _auth: SIP_A)
    rr = FakeRR(contact_params=None)
    agents, err = _call(rr, is_inbound=False, is_transfer=True, t_id="agent-uuid", t_type=rc.TRANSFER_TYPE_AGENT)
    assert (agents, err) == ([SIP_A], None)
    assert patched["selection"] is None  # get_agents_for_contact SKIPPED
    assert patched["capacity"] is not None  # but the capacity filter RUNS
    assert patched["capacity"]["agents"] == [SIP_A]  # on the transfer-target SIP


def test_transfer_type_agent_not_found(patched, monkeypatch):
    monkeypatch.setattr(G.ar, "get_agent_sip_by_uuid", lambda uuid, _auth: None)
    rr = FakeRR(contact_params=None)
    assert _call(rr, is_inbound=False, is_transfer=True, t_id="nope", t_type=rc.TRANSFER_TYPE_AGENT) == (
        None,
        "agent not found",
    )


def test_transfer_type_agent_empty_string_sip_proceeds(patched, monkeypatch):
    # A present-but-empty-string sip ('' not None) → '' is appended and the call PROCEEDS to the
    # capacity gate; only None ('agent not found') aborts. get_agents checks `sip is None`, so '' →
    # agents=[''] → capacity filter runs (NOT a hard abort).
    monkeypatch.setattr(G.ar, "get_agent_sip_by_uuid", lambda uuid, _auth: "")
    rr = FakeRR(contact_params=None)
    agents, err = _call(rr, is_inbound=False, is_transfer=True, t_id="agent-uuid", t_type=rc.TRANSFER_TYPE_AGENT)
    assert err is None and agents == [""]  # proceeded with [''], capacity ran
    assert patched["capacity"]["agents"] == [""]


# ---------------------------------------------------------------------------------------------
# default routing type + selection/capacity wiring
# ---------------------------------------------------------------------------------------------
def test_default_routing_type_simultaneous_when_empty(patched):
    rr = FakeRR(contact_params={"x": 1}, qnr_raw=_qnr_raw(routing=""))
    _call(rr)
    assert patched["selection"]["routing_type"] == rc.SIMULTANEOUS_ROUTING


def test_capacity_filter_wired_with_channel_capacity_and_node_limit(patched):
    rr = FakeRR(contact_params={"x": 1}, qnr_raw=_qnr_raw(agent_limit=7))
    _call(rr, channel="chat")
    cap = patched["capacity"]
    assert cap["channel"] == "chat" and cap["capacity"] == 10 and cap["agent_limit"] == 7


# ---------------------------------------------------------------------------------------------
# empty-list-no-error (no agents / no capacity) — NOT a hard error
# ---------------------------------------------------------------------------------------------
def test_no_agents_returns_empty_no_error(patched, monkeypatch):
    monkeypatch.setattr(G, "get_agents_for_contact", lambda *a, **k: [])
    rr = FakeRR(contact_params={"x": 1}, qnr_raw=_qnr_raw())
    agents, err = _call(rr)
    assert agents == [] and err is None
    assert patched["capacity"] is None  # capacity gate skipped on 0 candidates


def test_no_capacity_returns_empty_no_error(patched, monkeypatch):
    monkeypatch.setattr(G, "get_available_agents_by_capacity", lambda *a, **k: [])
    rr = FakeRR(contact_params={"x": 1}, qnr_raw=_qnr_raw())
    agents, err = _call(rr)
    assert agents == [] and err is None


# ---------------------------------------------------------------------------------------------
# downstream hard errors (DB / capacity raise → abort)
# ---------------------------------------------------------------------------------------------
def test_selection_db_error_aborts(patched, monkeypatch):
    def _boom(*a, **k):
        raise RuntimeError("db down")

    monkeypatch.setattr(G, "get_agents_for_contact", _boom)
    rr = FakeRR(contact_params={"x": 1}, qnr_raw=_qnr_raw())
    assert _call(rr) == (None, "error fetching agents for contact")


def test_capacity_evalsha_error_aborts(patched, monkeypatch):
    def _boom(*a, **k):
        raise RuntimeError("EVALSHA NOSCRIPT")

    monkeypatch.setattr(G, "get_available_agents_by_capacity", _boom)
    rr = FakeRR(contact_params={"x": 1}, qnr_raw=_qnr_raw())
    assert _call(rr) == (None, "error filtering agents by capacity")


# ---------------------------------------------------------------------------------------------
# Call channel: default voice fetch deferred (raises); injected fetcher works
# ---------------------------------------------------------------------------------------------
def test_call_channel_default_fetcher_defers(patched, monkeypatch):
    # The default fetcher is REAL (routing_acw.get_agents_on_active_call_and_in_acw);
    # a fetch failure is a (None, err) hard abort — NOT a raise out of get_agents.
    import hs.routing.acw as racw

    def boom(auth_id, agents):
        raise racw.AcwFetchError("error while filtering agents in acw")

    monkeypatch.setattr(racw, "get_agents_on_active_call_and_in_acw", boom)
    rr = FakeRR(contact_params={"x": 1}, qnr_raw=_qnr_raw())
    agents, err = _call(rr, channel="call")
    assert agents is None and err == "error while filtering agents in acw"


def test_call_channel_injected_fetcher_runs(patched):
    rr = FakeRR(contact_params={"x": 1}, qnr_raw=_qnr_raw())
    agents, err = _call(rr, channel="call")
    assert err is None and agents == [SIP_A, SIP_B]
    assert isinstance(patched["capacity"]["acw"], ActiveAgentsOnCallAndACW)


# =============================================================================================
# Routing numeric boundaries (retry==retry_limit, agent_limit=0, simultaneous_agent_limit>
# available, Simultaneous retry_limit=0). The retry boundaries are pure get_agents control flow; the
# agent_limit/cap cases exercise the REAL capacity filter THROUGH get_agents (fake Lua via
# FakeRR.update_agent_capacity), so the (returned-agent-count, error) outcome is end-to-end.
# =============================================================================================
def test_routing_numeric_boundaries(monkeypatch):
    # Shared wiring: settings present, sticky falls through, selection returns the 2 candidates; the
    # capacity filter is the REAL one (NOT monkeypatched) so agent_limit boundaries are exercised.
    monkeypatch.setattr(
        G.RoutingSettings, "get_org_routing_settings", lambda auth_id: _settings(capacity={"chat": 10}, sim_limit=5)
    )
    monkeypatch.setattr(G, "get_sticky_routing_agents", lambda *a, **k: (None, None))
    monkeypatch.setattr(G, "get_agents_for_contact", lambda *a, **k: [SIP_A, SIP_B])

    def call(retry, qnr_over, is_transfer=False, t_type=""):
        rr = FakeRR(contact_params={"x": 1}, retry=retry, qnr_raw=_qnr_raw(**qnr_over), capacity_code="0")
        return _call(rr, is_transfer=is_transfer, t_id="team-x", t_type=t_type)

    # retry == retry_limit → the LAST allowed attempt (retry > limit is the rejection, == is allowed).
    agents, err = call(2, {"retry_limit": 2, "agent_limit": 3})
    assert err is None and agents == [SIP_A, SIP_B]

    # retry == retry_limit + 1 → rejected.
    assert call(3, {"retry_limit": 2, "agent_limit": 3}) == (None, "known_error: retry limit reached")

    # Simultaneous forced retry_limit=0 → the first retry (retry=1 > 0) is terminal.
    assert call(1, {"retry_limit": 0, "agent_limit": 3}) == (None, "known_error: retry limit reached")

    # agent_limit == 0 → the capacity filter returns NONE (agent_limit<=0 guard) → empty, no error.
    agents, err = call(0, {"retry_limit": 5, "agent_limit": 0})
    assert agents == [] and err is None

    # simultaneous_agent_limit (5) > available (2): the transfer branch uses sim_limit as the cap; the
    # real filter returns min(limit, available) = 2 with no error.
    agents, err = call(0, {}, is_transfer=True, t_type=rc.TRANSFER_TYPE_TEAM)
    assert err is None and agents == [SIP_A, SIP_B]

    # NEGATIVE retry_limit: treated as unlimited, so it never trips (the >=0 guard),
    # so retry=1 + retry_limit=-1 does NOT reject (proceeds to selection).
    agents, err = call(1, {"retry_limit": -1, "agent_limit": 3})
    assert err is None and agents == [SIP_A, SIP_B]
