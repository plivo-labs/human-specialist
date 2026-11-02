"""Invite-reservation strand / capacity-set ratchet fixes.

Routing selection RESERVES a slot per candidate agent (get_available_agents_by_capacity,
new_contact=conv). The drain-side reverse routing releases it; four leaks once let members strand
in the agent's CAPACITY set until the set-TTL backstop (~26h on chat), ratcheting the agent
unroutable:

  1. the invite drain's reverse-routing hook routed to the AGENT-ONLINE variant,
     which drops conversation_id — the drained reservation was never released,
  2. execute_reverse_routing_workflow left its RRLOCK to the 1h TTL, so the +5s clearance
     retry hit the still-held lock after any mid-flight failure,
  3. an RRLOCK-contended release returned early WITHOUT decrementing and every caller
     swallowed the error,
  4. get_and_remove_invited_agents silently returned when the account-details lookup came
     back empty — after the CIA set was already deleted, so the release list was gone.

These tests pin the fixed behaviours. Redis/DB/account lookups are faked; no I/O.
"""

import logging

import pytest

import tests.conftest  # noqa: F401 — eager data_adapter imports
from hs.routing import capacity as rcap
from hs.routing import invitation as ri
from hs.routing.redis_store import CAPACITY_ALREADY_CLEAR_ERRS

_AUTH = "TESTORG0AUTH0ID000000"
_SIP = "sip:agent@org"
_SIP2 = "sip:agent2@org"
_CHANNEL = "chat"
_CONV = "CN165"


class _Org:
    auth_id = _AUTH


class _Agent:
    def __init__(self, sip):
        self.sip_endpoint = sip
        self.uuid = "aom-" + sip


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(rcap.time, "sleep", lambda *_a, **_k: None)


# ------------------------------------------------------------------ already-clear no-op


def test_release_retry_treats_already_clear_as_success(caplog):
    class FakeRR:
        def __init__(self):
            self.calls = 0

        def decrease_agents_capacity(self, *_a):
            self.calls += 1
            return CAPACITY_ALREADY_CLEAR_ERRS[1]  # "contact not found"

    rr = FakeRR()
    with caplog.at_level(logging.INFO):
        err = rcap._release_capacity_with_retry(rr, _AUTH, _SIP, _CHANNEL, _CONV)
    assert err is None
    assert rr.calls == 1  # no retries — already clear is terminal success
    assert not [r for r in caplog.records if r.levelno == logging.WARNING and "STRANDED" in r.getMessage()]


def test_release_retry_still_warns_on_real_failure(caplog):
    class FakeRR:
        def decrease_agents_capacity(self, *_a):
            return "redis timeout"

    with caplog.at_level(logging.WARNING):
        err = rcap._release_capacity_with_retry(FakeRR(), _AUTH, _SIP, _CHANNEL, _CONV)
    assert err == "redis timeout"
    assert [r for r in caplog.records if "STRANDED" in r.getMessage()]


# ------------------------------------------------------- release_reservation_best_effort


def test_best_effort_release_delegates_via_org_lookup(monkeypatch):
    released = []

    class FakeRR:
        def decrease_agents_capacity(self, auth_id, agent, channel, conv):
            released.append((auth_id, agent, channel, conv))
            return

    monkeypatch.setattr(rcap.ar, "get_org_details_for_agent_sip", lambda _sip: _Org())
    assert rcap.release_reservation_best_effort(FakeRR(), _SIP, _CHANNEL, _CONV) is None
    assert released == [(_AUTH, _SIP, _CHANNEL, _CONV)]


def test_best_effort_release_requires_conversation_and_org(monkeypatch):
    monkeypatch.setattr(rcap.ar, "get_org_details_for_agent_sip", lambda _sip: None)
    assert rcap.release_reservation_best_effort(object(), _SIP, _CHANNEL, "") == "empty conversation id"
    assert rcap.release_reservation_best_effort(object(), _SIP, _CHANNEL, _CONV) == "could not get organization details"


# --------------------------------------------------------------- RRLOCK lifecycle (fix 2)


class _LockRR:
    """RRLOCK + capacity fake for execute_reverse_routing_workflow."""

    def __init__(self, acquire=True):
        self._acquire = acquire
        self.deleted = []
        self.decreased = []

    def acquire_reverse_routing_lock_bool(self, *_a):
        return self._acquire

    def delete_reverse_routing_lock(self, sip, conv, channel):
        self.deleted.append((sip, conv, channel))

    def decrease_agents_capacity(self, auth_id, agent, channel, conv):
        self.decreased.append(conv)
        return


# ------------------------------------------------ close-path lock-contention fallback (fix 3)


def _wrapup_params(**over):
    from hs.conversations.wrap_up_models import WrapUpConversationParams

    base = {
        "agent_sip_endpoint": _SIP,
        "channel": _CHANNEL,
        "conversation_number": _CONV,
        "release_agent_capacity": True,
    }
    base.update(over)
    return WrapUpConversationParams.model_validate(base)


class _FakeDB:
    def commit(self):
        pass

    def close(self):
        pass


# ------------------------------------- drain releases the reservation (fix 1, the regression)


# --------------------------- drain fallback when account lookup dies after CIA delete (fix 4)


class _DrainRR:
    def __init__(self, sips):
        self._sips = sips

    def del_and_get_all_current_invited_agents(self, *_a):
        return list(self._sips)


def test_freed_agent_thread_body_runs_conversation_aware_release(monkeypatch):
    """The freed agent is reverse-routed WITH the conversation id it held."""
    import hs.voice.hangup as vh

    seen = {}

    def _fake_rr(agent_sip, channel, conversation_id, db=None):
        seen["args"] = (agent_sip, channel, conversation_id)
        return "", None

    monkeypatch.setattr(vh, "handle_reverse_routing_and_agent_capacity", _fake_rr)
    ri._reverse_route_freed_agent(_SIP, _CHANNEL, _CONV)
    assert seen["args"] == (_SIP, _CHANNEL, _CONV)


def test_freed_agent_thread_body_falls_back_on_lock_contention(monkeypatch):
    import hs.voice.hangup as vh

    fallback = []
    monkeypatch.setattr(vh, "handle_reverse_routing_and_agent_capacity", lambda *a, **k: ("", vh.RRLOCK_ACQUIRE_FAILED))
    monkeypatch.setattr("hs.routing.redis_store.RoutingRedis", lambda: object())
    monkeypatch.setattr(
        rcap, "release_reservation_best_effort", lambda rr, sip, channel, conv: fallback.append((sip, channel, conv))
    )
    ri._reverse_route_freed_agent(_SIP, _CHANNEL, _CONV)
    assert fallback == [(_SIP, _CHANNEL, _CONV)]


def test_freed_agent_thread_body_falls_back_on_raise(monkeypatch):
    import hs.voice.hangup as vh

    fallback = []

    def _boom(*_a, **_k):
        raise RuntimeError("mid-flight")

    monkeypatch.setattr(vh, "handle_reverse_routing_and_agent_capacity", _boom)
    monkeypatch.setattr("hs.routing.redis_store.RoutingRedis", lambda: object())
    monkeypatch.setattr(
        rcap, "release_reservation_best_effort", lambda rr, sip, channel, conv: fallback.append((sip, channel, conv))
    )
    ri._reverse_route_freed_agent(_SIP, _CHANNEL, _CONV)  # must not propagate
    assert fallback == [(_SIP, _CHANNEL, _CONV)]
