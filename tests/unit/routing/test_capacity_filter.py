"""get_available_agents_by_capacity + allow_agent_for_routing (the shared capacity FILTER used by sticky
routing and get_agents). Offline: the eager-acquire-per-candidate + agent_limit cap + the
allow_agent_for_routing decision tree. No Redis (a fake RoutingRedis feeds canned acquire codes).
"""

from hs.routing import constants as rc
from hs.routing import selection as S
from hs.routing.selection import ActiveAgentsOnCallAndACW


class _FakeRR:
    """Returns a canned acquire code per agent (by call order) so the filter is tested offline."""

    def __init__(self, codes):
        self._codes = list(codes)
        self.calls = []

    def update_agent_capacity(self, auth_id, agent, channel, capacity, old_contact="", new_contact=""):
        self.calls.append((agent, old_contact, new_contact))
        return self._codes.pop(0)


# ---- allow_agent_for_routing decision tree (pure) -------------------------------------------
def test_non_call_routable_iff_not_occupied():
    acw = ActiveAgentsOnCallAndACW()
    assert S.allow_agent_for_routing(rc.CHANNEL_CHAT, False, acw, "a") is True  # acquired → route
    assert S.allow_agent_for_routing(rc.CHANNEL_CHAT, True, acw, "a") is False  # full → drop


def test_call_occupied_on_active_call_is_dropped():
    acw = ActiveAgentsOnCallAndACW(active_agents_in_call=("a",))
    assert S.allow_agent_for_routing(rc.CHANNEL_CALL, True, acw, "a") is False


def test_call_occupied_not_in_call_not_in_acw_is_routable():
    acw = ActiveAgentsOnCallAndACW(active_agents_in_call=(), agents_in_acw=())
    assert S.allow_agent_for_routing(rc.CHANNEL_CALL, True, acw, "a") is True


def test_call_occupied_in_acw_follows_org_allow_routing_in_acw():
    yes = ActiveAgentsOnCallAndACW(agents_in_acw=("a",), allow_routing_in_acw=True)
    no = ActiveAgentsOnCallAndACW(agents_in_acw=("a",), allow_routing_in_acw=False)
    assert S.allow_agent_for_routing(rc.CHANNEL_CALL, True, yes, "a") is True
    assert S.allow_agent_for_routing(rc.CHANNEL_CALL, True, no, "a") is False


def test_call_not_occupied_requires_registered_sip_when_checked():
    acw = ActiveAgentsOnCallAndACW(agents_with_registered_sip=("reg",))
    # not occupied + sip-check ON: only the registered agent routes.
    assert S.allow_agent_for_routing(rc.CHANNEL_CALL, False, acw, "reg", check_sip_registration=True) is True
    assert S.allow_agent_for_routing(rc.CHANNEL_CALL, False, acw, "unreg", check_sip_registration=True) is False


# ---- get_available_agents_by_capacity (eager-acquire + agent_limit) ------------------------
def test_non_call_keeps_acquired_drops_full_and_caps_at_limit():
    # codes per agent: a1 acquired(0), a2 full(-2 → dropped), a3 acquired(0), a4 acquired(0 but over limit).
    rr = _FakeRR(["0", "-2", "0", "0"])
    got = S.get_available_agents_by_capacity(
        ["a1", "a2", "a3", "a4"], rc.CHANNEL_CHAT, 5, 2, "org", conversation_id="c1", routing_redis=rr
    )
    assert got == ["a1", "a3"]  # a2 dropped (full), capped at agent_limit=2
    # eager-acquire was attempted for a1,a2,a3 (stopped at limit before a4); each acquire used new=c1.
    assert [c[0] for c in rr.calls] == ["a1", "a2", "a3"]
    assert all(old == "" and new == "c1" for _, old, new in rr.calls)


def test_agent_limit_zero_returns_empty_without_acquiring():
    rr = _FakeRR(["0"])
    assert S.get_available_agents_by_capacity(["a1"], rc.CHANNEL_CHAT, 5, 0, "org", routing_redis=rr) == []
    assert rr.calls == []  # agent_limit<=0 → no acquire at all


def test_capacity_zero_coerced_to_one():
    # capacity 0 → 1 (so a single existing conv makes the next full); here a fresh acquire → 0 → kept.
    rr = _FakeRR(["0"])
    assert S.get_available_agents_by_capacity(
        ["a1"], rc.CHANNEL_CHAT, 0, 1, "org", conversation_id="c1", routing_redis=rr
    ) == ["a1"]


# ---- acquire-outcome logging (the lua result is logged) -------------
def _acquire_logs(caplog):
    import logging as _logging

    return [
        r.getMessage() for r in caplog.records if r.levelno == _logging.INFO and "capacity acquire:" in r.getMessage()
    ]


def test_acquire_outcome_logged_per_agent_with_code(caplog):
    import logging as _logging

    rr = _FakeRR(["0", "-2"])
    with caplog.at_level(_logging.DEBUG, logger="human-specialist"):
        S.get_available_agents_by_capacity(
            ["a1", "a2"], rc.CHANNEL_CHAT, 5, 5, "org", conversation_id="c1", routing_redis=rr
        )
    logs = _acquire_logs(caplog)
    # One line per candidate, carrying the lua code + ids — the ONLY log signal that
    # distinguishes a routable agent from one silently dropped for a held slot (-2).
    assert len(logs) == 2
    assert "code=0" in logs[0] and "agent=a1" in logs[0] and "conv=c1" in logs[0]
    assert "code=-2" in logs[1] and "agent=a2" in logs[1] and f"channel={rc.CHANNEL_CHAT}" in logs[1]


def test_acquire_outcome_logged_for_call_transfer_shape(caplog):
    import logging as _logging

    # The transfer-panel shape (get_agent_by_capacity): CALL channel, capacity=1, limit=1,
    # skip-path acw marks the agent in-call → a -2 agent is dropped AND must leave a log trail.
    acw = ActiveAgentsOnCallAndACW(active_agents_in_call=("a1",), agents_with_registered_sip=("a1",))
    rr = _FakeRR(["-2"])
    with caplog.at_level(_logging.DEBUG, logger="human-specialist"):
        got = S.get_available_agents_by_capacity(
            ["a1"], rc.CHANNEL_CALL, 1, 1, "org", acw, conversation_id="c2", routing_redis=rr
        )
    assert got == []
    logs = _acquire_logs(caplog)
    assert len(logs) == 1
    assert "code=-2" in logs[0] and "agent=a1" in logs[0] and "conv=c2" in logs[0]
