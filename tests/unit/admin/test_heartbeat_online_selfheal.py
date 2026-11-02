"""Heartbeat online-edge self-heal.

If the status change fails exactly at the count==2 online edge and the edge never fires again
(the counter slides past 2, its TTL refreshing on every ping), the agent stays unroutable for up
to the counter's 2h TTL. So: (1) a failed online edge rolls the counter back to N-1 so the NEXT
ping is the edge again; (2) the offline edge ALWAYS resets the counter, even when the agent
already shows unavailable. A rollback (not a marker/reconcile)
is deliberate: an agent who later picks Away by hand must never be forced back online.
"""

import pytest

import hs.admin.heartbeat as hb
import tests.conftest  # noqa: F401 — eager data_adapter imports (AbstractConcreteBase registry)
from hs.admin.heartbeat import HeartbeatReqMeta
from hs.infra.constants import HEARTBEAT_PING_COUNT_ONLINE

AOM = "aom-1"
ORG = "org-1"


class _FakePresence:
    """In-memory AOMPC counter + LST, faithful to the adapter's contract."""

    def __init__(self, start_count=0):
        self.count = start_count
        self.set_calls = []

    def install(self, monkeypatch):
        monkeypatch.setattr(hb.HeartbeatPresenceCache, "incr_ping_count", classmethod(lambda cls, a: self._incr()))
        monkeypatch.setattr(hb.HeartbeatPresenceCache, "set_ping_time", classmethod(lambda cls, a: True))
        monkeypatch.setattr(hb.HeartbeatPresenceCache, "set_ping_count", classmethod(lambda cls, a, c: self._set(c)))
        monkeypatch.setattr(hb.HeartbeatPresenceCache, "mark_online", classmethod(lambda cls, *a: None))
        return self

    def _incr(self):
        self.count += 1
        return self.count

    def _set(self, c):
        self.set_calls.append(c)
        self.count = c
        return True


def _ping(n=1):
    for _ in range(n):
        hb.handle_agent_ping(HeartbeatReqMeta(aom_uuid=AOM, org_uuid=ORG))


def test_failed_online_edge_rolls_counter_back_and_next_ping_retries(monkeypatch):
    # ping#2 → edge → notify FAILS → counter rolled back to N-1 → ping#3 IS the edge again.
    presence = _FakePresence().install(monkeypatch)
    outcomes = iter([False, True])  # first edge fails (the status write errors), second succeeds
    edges = []

    def _edge(rm, online):
        edges.append(presence.count)
        return next(outcomes)

    monkeypatch.setattr(hb, "presence_edge", _edge)
    _ping(2)  # counts 1, 2 → the first edge (fails, rolls back to 1)
    assert edges == [HEARTBEAT_PING_COUNT_ONLINE]
    assert presence.set_calls == [HEARTBEAT_PING_COUNT_ONLINE - 1]
    _ping(1)  # count 1→2 again → the second edge (succeeds)
    assert edges == [HEARTBEAT_PING_COUNT_ONLINE, HEARTBEAT_PING_COUNT_ONLINE]
    _ping(3)  # counts 3,4,5 — edge stays quiet after success
    assert len(edges) == 2
    assert presence.set_calls == [HEARTBEAT_PING_COUNT_ONLINE - 1]  # no further rollbacks


def test_successful_online_edge_never_touches_the_counter(monkeypatch):
    presence = _FakePresence().install(monkeypatch)
    monkeypatch.setattr(hb, "presence_edge", lambda rm, online: True)
    _ping(5)
    assert presence.set_calls == []  # success → the counter is left strictly alone


def test_manual_away_is_never_forced_back_online(monkeypatch):
    # After a SUCCESSFUL edge, later pings must not re-fire it — an agent who picked Away by hand
    # (stored availability false while still pinging) must stay Away.
    presence = _FakePresence().install(monkeypatch)
    fired = []
    monkeypatch.setattr(hb, "presence_edge", lambda rm, online: fired.append(presence.count) or True)
    _ping(2)  # edge fires once, succeeds
    _ping(30)  # agent keeps pinging (maybe Away by hand now)
    assert fired == [HEARTBEAT_PING_COUNT_ONLINE]  # exactly one edge, ever, for the session


def test_stuck_unavailable_agent_recovers_within_the_same_session(monkeypatch):
    # One session: the status change fails at the edge repeatedly, then recovers.
    # Every failed edge rolls back → the edge re-fires on each subsequent ping → first ping after
    # the status write recovers flips the agent routable. No 2h stranding.
    _FakePresence().install(monkeypatch)
    store_up = {"v": False}
    attempts = []

    def _edge(rm, online):
        attempts.append(1)
        return store_up["v"]

    monkeypatch.setattr(hb, "presence_edge", _edge)
    _ping(4)  # edges at ping2/3/4 all fail, each rolls back
    assert len(attempts) == 3
    store_up["v"] = True
    _ping(1)  # the status write works again → this ping's edge lands
    assert len(attempts) == 4
    _ping(3)
    assert len(attempts) == 4  # and the edge stays quiet afterwards


def test_offline_edge_resets_counter_even_when_core_already_unavailable(monkeypatch):
    # pin on the REAL presence_edge (reads mocked): the already-unavailable early-return must
    # still reset the counter — the cross-session half of the poisoning fix. Without it, the
    # next session's pings keep counting 3,4,5… and the online edge can never fire again.
    resets = []
    monkeypatch.setattr("hs.routing.agent_status_read.is_agent_currently_available", lambda a: False)
    monkeypatch.setattr(
        hb.HeartbeatPresenceCache, "reset_ping_count", classmethod(lambda cls, a: resets.append(a) or True)
    )
    assert hb.presence_edge(HeartbeatReqMeta(aom_uuid=AOM, org_uuid=ORG), online=False) is True
    assert resets == [AOM]


def test_availability_read_error_reports_failure_for_retry(monkeypatch):
    monkeypatch.setattr("hs.routing.agent_status_read.is_agent_currently_available", lambda a: None)
    assert hb.presence_edge(HeartbeatReqMeta(aom_uuid=AOM, org_uuid=ORG), online=True) is False


@pytest.mark.parametrize("online", [True, False])
def test_config_read_error_reports_failure_for_retry(monkeypatch, online):
    # A failed config read is not "no config" (True consumes the edge); False retries it.
    def _boom(o, e):
        raise RuntimeError("db down")

    monkeypatch.setattr("hs.routing.agent_status_read.is_agent_currently_available", lambda a: not online)
    monkeypatch.setattr("hs.routing.agent_status_read.get_status_uuid_for_org_event", _boom)
    monkeypatch.setattr(hb.HeartbeatPresenceCache, "reset_ping_count", classmethod(lambda cls, a: True))
    assert hb.presence_edge(HeartbeatReqMeta(aom_uuid=AOM, org_uuid=ORG), online=online) is False
