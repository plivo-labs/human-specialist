"""Sticky routing decision tree + setters.

Pins get_sticky_routing_agents, set_outbound_sticky_mapping, set_inbound_sticky_mapping and
is_agent_in_routing_assignment. Offline + deterministic: a FakeRoutingRedis stands in for the CASM/OCASM
+ capacity Lua seam, and the account reads are monkeypatched.

The matrix: bind mode × retry × agent status × availability × OCASM/CASM presence + the
email/no-sticky early return + the is_agent_in_routing_assignment edge + the capacity-empty hard error.
The outcomes are asserted as the EXACT (agents, err) tuple, because the (None,None) fall-through vs
(None,'err') hard-error distinction is load-bearing for the caller.
"""

import pytest

import tests.conftest  # noqa: F401 — eager data_adapter imports (AbstractConcreteBase registry)
from hs.intake.models import QueueAndRouteConfigModel
from hs.routing import constants as rc
from hs.routing import sticky as S

QUEUE = "q-uuid-1"
STICKY_UUID = "aom-uuid-sticky"  # the cached OCASM/CASM VALUE (an aom uuid)
STICKY_SIP = "sip:agent@plivo"  # the SIP get_agent_with_status_info returns for that uuid


class FakeRoutingRedis:
    """Stands in for RoutingRedis: an in-memory OCASM/CASM store + a scripted update_agent_capacity
    return code, recording the del-target so the OCASM-vs-CASM delete asymmetry is assertable."""

    def __init__(self, ocasm=None, casm=None, capacity_code=0):
        self.ocasm = dict(ocasm or {})  # contact -> value
        self.casm = dict(casm or {})  # (contact, queue) -> value
        self.capacity_code = capacity_code
        self.deleted = []  # ('OCASM'|'CASM', *args)
        self.sets = []  # ('OCASM'|'CASM', contact, [queue], value, ttl)

    # queue None is the outbound (OCASM) mapping, else the queue's inbound (CASM) one
    def get_sticky_agent(self, contact, queue=None, auth_id=""):
        return self.ocasm.get(contact) if queue is None else self.casm.get((contact, queue))

    def del_sticky_agent(self, contact, queue=None, auth_id=""):  # records the target: the asymmetry under test
        if queue is None:
            self.deleted.append(("OCASM", contact))
            self.ocasm.pop(contact, None)
        else:
            self.deleted.append(("CASM", contact, queue))
            self.casm.pop((contact, queue), None)
        return 1

    def set_sticky_agent(self, contact, value, ttl, queue=None, auth_id=""):
        if queue is None:
            self.sets.append(("OCASM", contact, value, ttl))
            self.ocasm[contact] = value
        else:
            self.sets.append(("CASM", contact, queue, value, ttl))
            self.casm[(contact, queue)] = value
        return True

    # capacity Lua (consumed by get_available_agents_by_capacity)
    def update_agent_capacity(self, auth_id, agent, channel, org_capacity, old_contact="", new_contact=""):
        return self.capacity_code


def _qnr(final=(STICKY_UUID,), teams=(), sticky=rc.STICKY_LOOSELY_BIND):
    return QueueAndRouteConfigModel(
        queue=QUEUE, final_routing_agents=list(final), routing_teams=list(teams), sticky_routing_type=sticky
    )


def _patch_agent(monkeypatch, info=(STICKY_SIP, "active", True), teams=()):
    """Make get_agent_with_status_info return `info` (or None) and get_agent_teams return `teams`."""
    monkeypatch.setattr(S.ar, "get_agent_with_status_info", lambda uuid: info)
    monkeypatch.setattr(S.ar, "get_agent_teams", lambda agent, qp="uuid": list(teams))


def _call(
    rr,
    monkeypatch,
    *,
    sticky_type=rc.STICKY_LOOSELY_BIND,
    retry=0,
    channel=rc.CHANNEL_CHAT,
    outbound_enabled=True,
    qnr=None,
    info=(STICKY_SIP, "active", True),
    teams=(),
):
    _patch_agent(monkeypatch, info=info, teams=teams)
    return S.get_sticky_routing_agents(
        "contact-uuid",
        (qnr or _qnr()).model_copy(update={"sticky_routing_type": sticky_type}),
        "+1-555-0100",
        retry,
        channel,
        10,
        "AUTHID",
        outbound_enabled,
        "conv-1",
        rr,
    )


# ---------------------------------------------------------------------------------------------
# (1) email / no-sticky early return  → (None, None)
# ---------------------------------------------------------------------------------------------
def test_email_is_never_sticky(monkeypatch):
    rr = FakeRoutingRedis(ocasm={"+1-555-0100": STICKY_UUID})
    assert _call(rr, monkeypatch, channel=rc.CHANNEL_EMAIL) == (None, None)


def test_no_sticky_routing_falls_through(monkeypatch):
    rr = FakeRoutingRedis(ocasm={"+1-555-0100": STICKY_UUID})
    assert _call(rr, monkeypatch, sticky_type=rc.STICKY_NO_STICKY_ROUTING) == (None, None)


# ---------------------------------------------------------------------------------------------
# (2/3) OCASM precedence + CASM fall-through  (CASM key keyed on digits+queue)
# ---------------------------------------------------------------------------------------------
def test_ocasm_precedence_over_casm(monkeypatch):
    # Both present → OCASM wins (outbound_sticky_agent=True); CASM is not consulted for the value.
    rr = FakeRoutingRedis(ocasm={"+1-555-0100": STICKY_UUID}, casm={("+1-555-0100", QUEUE): "other-uuid"})
    agents, err = _call(rr, monkeypatch)
    assert (agents, err) == ([STICKY_SIP], None)


def test_casm_used_when_ocasm_empty(monkeypatch):
    # CASM is keyed by digits(contact)+queue: "+1-555-0100" → "15550100".
    casm_key = ("+1-555-0100", QUEUE)
    rr = FakeRoutingRedis(casm={casm_key: STICKY_UUID})
    agents, err = _call(rr, monkeypatch, outbound_enabled=True)  # OCASM enabled but empty
    assert (agents, err) == ([STICKY_SIP], None)


def test_casm_miss_falls_through(monkeypatch):
    rr = FakeRoutingRedis()  # neither OCASM nor CASM → (None, None)
    assert _call(rr, monkeypatch) == (None, None)


def test_outbound_disabled_skips_ocasm_uses_casm(monkeypatch):
    rr = FakeRoutingRedis(ocasm={"+1-555-0100": "should-be-ignored"}, casm={("+1-555-0100", QUEUE): STICKY_UUID})
    agents, err = _call(rr, monkeypatch, outbound_enabled=False)
    assert (agents, err) == ([STICKY_SIP], None)  # OCASM never read → CASM value used


# ---------------------------------------------------------------------------------------------
# (4/5) bind-mode × retry
# ---------------------------------------------------------------------------------------------
def test_strictly_bind_retry_gt_0_is_hard_error(monkeypatch):
    rr = FakeRoutingRedis(ocasm={"+1-555-0100": STICKY_UUID})
    agents, err = _call(rr, monkeypatch, sticky_type=rc.STICKY_STRICTLY_BIND, retry=1)
    assert agents is None and err == "sticky routing: strictly bind, assignment failed as retry > 0"


def test_strictly_bind_retry_0_proceeds(monkeypatch):
    rr = FakeRoutingRedis(ocasm={"+1-555-0100": STICKY_UUID})
    agents, err = _call(rr, monkeypatch, sticky_type=rc.STICKY_STRICTLY_BIND, retry=0)
    assert (agents, err) == ([STICKY_SIP], None)


def test_loosely_bind_retry_gt_0_falls_through(monkeypatch):
    rr = FakeRoutingRedis(ocasm={"+1-555-0100": STICKY_UUID})
    assert _call(rr, monkeypatch, sticky_type=rc.STICKY_LOOSELY_BIND, retry=2) == (None, None)


# ---------------------------------------------------------------------------------------------
# (6) get_agent_with_status_info error → hard error
# ---------------------------------------------------------------------------------------------
def test_agent_lookup_error_is_hard_error(monkeypatch):
    rr = FakeRoutingRedis(ocasm={"+1-555-0100": STICKY_UUID})
    agents, err = _call(rr, monkeypatch, info=None)
    assert agents is None and err == "sticky routing: unable to get agent availability"


# ---------------------------------------------------------------------------------------------
# (7) status != active → DELETE the RIGHT cache + (None, None)   (the delete-target asymmetry)
# ---------------------------------------------------------------------------------------------
def test_inactive_outbound_agent_deletes_ocasm(monkeypatch):
    rr = FakeRoutingRedis(ocasm={"+1-555-0100": STICKY_UUID})
    res = _call(rr, monkeypatch, info=(STICKY_SIP, "inactive", True))
    assert res == (None, None)
    assert rr.deleted == [("OCASM", "+1-555-0100")]  # OCASM-sourced → OCASM deleted


def test_inactive_inbound_agent_deletes_casm(monkeypatch):
    rr = FakeRoutingRedis(casm={("+1-555-0100", QUEUE): STICKY_UUID})
    res = _call(rr, monkeypatch, outbound_enabled=False, info=(STICKY_SIP, "inactive", True))
    assert res == (None, None)
    assert rr.deleted == [("CASM", "+1-555-0100", QUEUE)]  # CASM-sourced → CASM deleted


# ---------------------------------------------------------------------------------------------
# (8) active but NOT available → hard error  (opposite control flow to inactive)
# ---------------------------------------------------------------------------------------------
def test_active_but_unavailable_is_hard_error(monkeypatch):
    rr = FakeRoutingRedis(ocasm={"+1-555-0100": STICKY_UUID})
    agents, err = _call(rr, monkeypatch, info=(STICKY_SIP, "active", False))
    assert agents is None and err == "sticky routing: agent status is marked as unavailable"
    assert rr.deleted == []  # NOT deleted (distinct from the inactive case)


# ---------------------------------------------------------------------------------------------
# (9) is_agent_in_routing_assignment edge
# ---------------------------------------------------------------------------------------------
def test_agent_in_final_routing_agents_hits(monkeypatch):
    rr = FakeRoutingRedis(ocasm={"+1-555-0100": STICKY_UUID})
    agents, err = _call(rr, monkeypatch, qnr=_qnr(final=(STICKY_UUID,)))
    assert (agents, err) == ([STICKY_SIP], None)


def test_agent_not_in_assignment_falls_through(monkeypatch):
    # Not in final_routing_agents and no team overlap → (None, None) fall-through (NOT an error).
    rr = FakeRoutingRedis(ocasm={"+1-555-0100": STICKY_UUID})
    res = _call(rr, monkeypatch, qnr=_qnr(final=("someone-else",), teams=("team-x",)), teams=("team-y",))
    assert res == (None, None)


def test_agent_in_assignment_via_team_overlap_hits(monkeypatch):
    rr = FakeRoutingRedis(ocasm={"+1-555-0100": STICKY_UUID})
    agents, err = _call(rr, monkeypatch, qnr=_qnr(final=("someone-else",), teams=("team-y",)), teams=("team-y",))
    assert (agents, err) == ([STICKY_SIP], None)


def test_get_agent_teams_error_propagates_as_hard_error(monkeypatch):
    rr = FakeRoutingRedis(ocasm={"+1-555-0100": STICKY_UUID})
    _patch_agent(monkeypatch, info=(STICKY_SIP, "active", True))
    monkeypatch.setattr(S.ar, "get_agent_teams", lambda agent, qp="uuid": None)  # read error
    agents, err = S.get_sticky_routing_agents(
        "c", _qnr(final=("nope",), teams=("t1",)), "+1-555-0100", 0, rc.CHANNEL_CHAT, 10, "A", True, "conv", rr
    )
    assert agents is None and err == "sticky routing : No teams found for agent"


# ---------------------------------------------------------------------------------------------
# (12/13) capacity filter (agent_limit=1) — full agent → empty → hard error
# ---------------------------------------------------------------------------------------------
def test_capacity_full_is_hard_error(monkeypatch):
    rr = FakeRoutingRedis(ocasm={"+1-555-0100": STICKY_UUID}, capacity_code=-2)  # -2 = full (non-Call drop)
    agents, err = _call(rr, monkeypatch)
    assert agents is None and err == "sticky routing: cached agent capacity not available"


# ---------------------------------------------------------------------------------------------
# (11) Call channel defers the active-call/ACW voice fetch (raises by default; injectable)
# ---------------------------------------------------------------------------------------------
def test_call_channel_voice_fetch_is_deferred(monkeypatch):
    # The default fetcher is REAL; a fetch failure → the (None, err) hard abort
    # , no longer a NotImplementedError raise.
    import hs.routing.acw as racw

    def boom(auth_id, agents):
        raise racw.AcwFetchError("error while filtering agents in acw")

    monkeypatch.setattr(racw, "get_agents_on_active_call_and_in_acw", boom)
    rr = FakeRoutingRedis(ocasm={"+1-555-0100": STICKY_UUID})
    _patch_agent(monkeypatch, info=(STICKY_SIP, "active", True))
    agents, err = S.get_sticky_routing_agents("c", _qnr(), "+1-555-0100", 0, rc.CHANNEL_CALL, 1, "A", True, "conv", rr)
    assert agents is None and err == "error while filtering agents in acw"


def test_call_channel_with_injected_acw_fetcher(monkeypatch):
    from hs.routing.selection import ActiveAgentsOnCallAndACW

    monkeypatch.setattr(S.routing_acw, "get_agents_on_active_call_and_in_acw", lambda *a: ActiveAgentsOnCallAndACW())
    rr = FakeRoutingRedis(ocasm={"+1-555-0100": STICKY_UUID}, capacity_code=0)
    _patch_agent(monkeypatch, info=(STICKY_SIP, "active", True))
    agents, err = S.get_sticky_routing_agents("c", _qnr(), "+1-555-0100", 0, rc.CHANNEL_CALL, 1, "A", True, "conv", rr)
    assert (agents, err) == ([STICKY_SIP], None)


# =============================================================================================
# is_agent_in_routing_assignment — direct unit
# =============================================================================================
def test_is_agent_in_routing_assignment_direct(monkeypatch):
    assert S.is_agent_in_routing_assignment(_qnr(final=("a1", "a2")), "a2") == (True, None)


def test_is_agent_in_routing_assignment_team(monkeypatch):
    monkeypatch.setattr(S.ar, "get_agent_teams", lambda agent, qp="uuid": ["t2"])
    assert S.is_agent_in_routing_assignment(_qnr(final=("x",), teams=("t1", "t2")), "a") == (True, None)


def test_is_agent_in_routing_assignment_no_match(monkeypatch):
    monkeypatch.setattr(S.ar, "get_agent_teams", lambda agent, qp="uuid": ["t9"])
    assert S.is_agent_in_routing_assignment(_qnr(final=("x",), teams=("t1",)), "a") == (False, None)


def test_is_agent_in_routing_assignment_teams_read_error(monkeypatch):
    monkeypatch.setattr(S.ar, "get_agent_teams", lambda agent, qp="uuid": None)
    ok, err = S.is_agent_in_routing_assignment(_qnr(final=("x",), teams=("t1",)), "a")
    assert ok is False and err == "sticky routing : No teams found for agent"


# =============================================================================================
# set_outbound_sticky_mapping
# =============================================================================================
def _osr_settings(enabled=True, no_expiry=False, unit="", value=2):
    import types

    return types.SimpleNamespace(
        outbound_sticky_routing={
            "enabled": enabled,
            "expiry": {"no_expiry": no_expiry, "expiry_time_unit": unit, "expiry_time_value": value},
        }
    )


def test_set_outbound_sticky_writes_ocasm_when_enabled(monkeypatch):
    rr = FakeRoutingRedis()
    monkeypatch.setattr(S.RoutingSettings, "get_org_routing_settings", lambda auth_id: _osr_settings(True, value=2))
    monkeypatch.setattr(S, "RoutingRedis", lambda: rr)
    err = S.set_outbound_sticky_mapping(STICKY_UUID, "+1-555-0100", "AUTHID")
    assert err is None
    # value = the uuid (no sip→uuid convert since not a sip); ttl = 2h = 7200s.
    assert rr.sets == [("OCASM", "+1-555-0100", STICKY_UUID, 7200)]


def test_set_outbound_sticky_no_expiry_persistent(monkeypatch):
    rr = FakeRoutingRedis()
    monkeypatch.setattr(
        S.RoutingSettings, "get_org_routing_settings", lambda auth_id: _osr_settings(True, no_expiry=True)
    )
    monkeypatch.setattr(S, "RoutingRedis", lambda: rr)
    S.set_outbound_sticky_mapping(STICKY_UUID, "+1-555-0100", "AUTHID")
    assert rr.sets == [("OCASM", "+1-555-0100", STICKY_UUID, 0)]  # no_expiry → ttl 0 (persistent)


def test_set_outbound_sticky_sip_is_converted_to_uuid(monkeypatch):
    rr = FakeRoutingRedis()
    monkeypatch.setattr(S.ar, "get_account_uuid_for_sip_endpoint", lambda sip: "resolved-uuid")
    monkeypatch.setattr(S.RoutingSettings, "get_org_routing_settings", lambda auth_id: _osr_settings(True))
    monkeypatch.setattr(S, "RoutingRedis", lambda: rr)
    S.set_outbound_sticky_mapping("sip:agent@plivo", "+1-555-0100", "AUTHID")
    assert rr.sets[0][2] == "resolved-uuid"  # the OCASM VALUE is the resolved aom uuid, not the sip


def test_set_outbound_sticky_disabled_is_noop(monkeypatch):
    rr = FakeRoutingRedis()
    monkeypatch.setattr(S.RoutingSettings, "get_org_routing_settings", lambda auth_id: _osr_settings(enabled=False))
    monkeypatch.setattr(S, "RoutingRedis", lambda: rr)
    err = S.set_outbound_sticky_mapping(STICKY_UUID, "+1-555-0100", "AUTHID")
    assert err is None and rr.sets == []  # OSR disabled → no OCASM write


def test_set_outbound_sticky_sip_no_row_raises(monkeypatch):
    # A 'sip:' prefix matching NO aom row raises (the handler maps it to a 500).
    rr = FakeRoutingRedis()
    monkeypatch.setattr(S.ar, "get_account_uuid_for_sip_endpoint", lambda sip: None)  # no row
    with pytest.raises(IndexError):
        monkeypatch.setattr(S, "RoutingRedis", lambda: rr)
        S.set_outbound_sticky_mapping("sip:nope@x", "+1-555-0100", "AUTHID")
    assert rr.sets == []


def test_set_outbound_sticky_sip_read_error_returns_422_string(monkeypatch):
    # a real sip-endpoint READ error → an error return → 422 (distinct from no-row).
    rr = FakeRoutingRedis()

    def _boom(sip):
        raise RuntimeError("db down")

    monkeypatch.setattr(S.ar, "get_account_uuid_for_sip_endpoint", _boom)
    monkeypatch.setattr(S, "RoutingRedis", lambda: rr)
    err = S.set_outbound_sticky_mapping("sip:nope@x", "+1-555-0100", "AUTHID")
    assert err == "error while fetching agent uuid from agent sip" and rr.sets == []


def test_capacity_evalsha_error_returns_tuple_not_raise(monkeypatch):
    # A real EVALSHA/Redis error inside the capacity filter → the (None, err) TUPLE (NOT a raised
    # exception), so the caller can abort under strict sticky and fall through under loose sticky.
    class _RaisingRR(FakeRoutingRedis):
        def update_agent_capacity(self, *a, **k):
            raise RuntimeError("EVALSHA NOSCRIPT")

    rr = _RaisingRR(ocasm={"+1-555-0100": STICKY_UUID})
    agents, err = _call(rr, monkeypatch)
    assert agents is None and err == "sticky routing: error in getting capacity for agent"


def test_set_outbound_sticky_missing_settings_errors(monkeypatch):
    rr = FakeRoutingRedis()
    monkeypatch.setattr(S.RoutingSettings, "get_org_routing_settings", lambda auth_id: None)
    monkeypatch.setattr(S, "RoutingRedis", lambda: rr)
    err = S.set_outbound_sticky_mapping(STICKY_UUID, "+1-555-0100", "AUTHID")
    assert err == "error while fetching routing settings"


# =============================================================================================
# set_inbound_sticky_mapping — CASM write + OCASM delete
# =============================================================================================
def test_set_inbound_writes_casm_and_deletes_ocasm(monkeypatch):
    rr = FakeRoutingRedis(ocasm={"+1-555-0100": "old"})
    qnr = QueueAndRouteConfigModel(
        queue=QUEUE, sticky_routing_expiry=False, sticky_routing_expiry_unit="days", sticky_routing_expiry_time=1
    )
    monkeypatch.setattr(S, "RoutingRedis", lambda: rr)
    err = S.set_inbound_sticky_mapping(STICKY_UUID, "+1-555-0100", qnr, "AUTHID")
    assert err is None
    assert rr.sets == [("CASM", "+1-555-0100", QUEUE, STICKY_UUID, 86400)]  # days → 1*24h
    assert rr.deleted == [("OCASM", "+1-555-0100")]  # OCASM cleared
