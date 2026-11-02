"""Heartbeat: synchronous Redis detection and the online-edge debounce.

Offline (Cache mocked, no live Redis): pins the key namespaces + TTLs, the ATOMIC-INCR ping
count, the 40s LST ping-time, the count==2 ONLINE edge, and that every ping ENSURES a watcher
(self-healing arming). The watcher mechanics are in test_heartbeat_offline_watcher.py; the
presence edges in test_heartbeat_presence_edges.py.
"""

import pytest
from fastapi import BackgroundTasks

import hs.admin.heartbeat as hb
import tests.conftest  # noqa: F401 — eager data_adapter imports (AbstractConcreteBase registry)
from hs.access import Principal
from hs.admin import api as admin_api
from hs.admin.heartbeat import CACHE_ERROR, HeartbeatReqMeta, handle_agent_ping
from hs.errors import ApiError
from hs.infra.constants import (
    HEARTBEAT_PING_COUNT_ONLINE,
    HEARTBEAT_PING_COUNT_TTL_SECONDS,
    HEARTBEAT_TIME_TO_MARK_OFFLINE,
)
from hs.infra.redis import Cache, HeartbeatPresenceCache

AOM = "aom-1"


class _FakeCache:
    def __init__(self, incrby_result=1, get_result=None):
        self.calls = []
        self._incrby_result = incrby_result
        self._get_result = get_result

    def incrby(self, key, count):
        self.calls.append(("incrby", key, count))
        return self._incrby_result

    def expire(self, key, secs):
        self.calls.append(("expire", key, secs))
        return True

    def set(self, key, value, expiry_in_seconds=None, nx=False):
        self.calls.append(("set", key, value, expiry_in_seconds))
        return True

    def get(self, key):
        self.calls.append(("get", key))
        return self._get_result


@pytest.fixture()
def install_cache(monkeypatch):
    """Install a fake Cache and no-op the sweeper tracking."""

    def _install(**kw):
        fake = _FakeCache(**kw)
        monkeypatch.setattr(Cache, "get_instance", staticmethod(lambda: fake))
        monkeypatch.setattr(hb.HeartbeatPresenceCache, "mark_online", classmethod(lambda cls, *a: None))
        return fake

    return _install


def test_key_namespaces_are_pinned():
    # LST:<aom> + AOMPC:<aom> — no org prefix / hash-tag.
    assert HeartbeatPresenceCache._ping_time_key(AOM) == "LST:aom-1"
    assert HeartbeatPresenceCache._ping_count_key(AOM) == "AOMPC:aom-1"


def test_heartbeat_tunables_are_pinned():
    assert HEARTBEAT_TIME_TO_MARK_OFFLINE == 40
    assert HEARTBEAT_PING_COUNT_TTL_SECONDS == 2 * 60 * 60
    assert HEARTBEAT_PING_COUNT_ONLINE == 2  # one source for the count==2 flip


def test_incr_ping_count_uses_atomic_incr_then_slides_2h_ttl(install_cache):
    fake = install_cache(incrby_result=2)
    count = HeartbeatPresenceCache.incr_ping_count(AOM)
    assert count == 2
    # atomic INCR (NOT get-then-set) + a 2h EXPIRE on the AOMPC key
    assert ("incrby", "AOMPC:aom-1", 1) in fake.calls
    assert ("expire", "AOMPC:aom-1", HEARTBEAT_PING_COUNT_TTL_SECONDS) in fake.calls


def test_incr_ping_count_returns_none_on_cache_error_and_skips_expire(install_cache):
    fake = install_cache(incrby_result=None)  # Cache swallows the redis error → None
    assert HeartbeatPresenceCache.incr_ping_count(AOM) is None
    assert not any(c[0] == "expire" for c in fake.calls)  # no TTL set when the INCR failed


def test_set_ping_time_writes_lst_unix_seconds_with_40s_ttl(install_cache):
    fake = install_cache()
    assert HeartbeatPresenceCache.set_ping_time(AOM) is True
    sets = [c for c in fake.calls if c[0] == "set"]
    assert len(sets) == 1
    _, key, value, ttl = sets[0]
    assert key == "LST:aom-1"
    assert value.isdigit()  # server unix-seconds string
    assert ttl == HEARTBEAT_TIME_TO_MARK_OFFLINE  # 40s offline window


def test_second_ping_fires_the_online_edge(install_cache, monkeypatch):
    install_cache(incrby_result=2)  # this ping is the 2nd → ONLINE
    edges = []
    monkeypatch.setattr(hb, "presence_edge", lambda rm, online: edges.append(online))
    handle_agent_ping(HeartbeatReqMeta(aom_uuid=AOM, org_uuid="org-1"))
    assert edges == [True]  # exactly one online edge


@pytest.mark.parametrize("count", [1, 3, 5])
def test_non_second_ping_does_not_fire_an_edge(install_cache, monkeypatch, count):
    install_cache(incrby_result=count)
    edges = []
    monkeypatch.setattr(hb, "presence_edge", lambda rm, online: edges.append(online))
    handle_agent_ping(HeartbeatReqMeta(aom_uuid=AOM, org_uuid="org-1"))
    assert edges == []  # online flips ONLY on the 2nd ping (debounce)


def test_ping_writes_both_redis_keys(install_cache, monkeypatch):
    fake = install_cache(incrby_result=1)
    monkeypatch.setattr(hb, "presence_edge", lambda rm, online: None)
    handle_agent_ping(HeartbeatReqMeta(aom_uuid=AOM, org_uuid="org-1"))
    assert ("incrby", "AOMPC:aom-1", 1) in fake.calls  # ping count
    assert any(c[0] == "set" and c[1] == "LST:aom-1" for c in fake.calls)  # ping time (LST)


def test_every_ping_keeps_the_agent_tracked_for_the_sweeper(install_cache, monkeypatch):
    install_cache(incrby_result=4)  # not the online edge
    monkeypatch.setattr(hb, "presence_edge", lambda rm, online: None)
    tracked = []
    monkeypatch.setattr(hb.HeartbeatPresenceCache, "mark_online", classmethod(lambda cls, a, o, n: tracked.append(a)))
    handle_agent_ping(HeartbeatReqMeta(aom_uuid=AOM, org_uuid="org-1"))
    assert tracked == [AOM]


def test_cache_error_returns_the_cache_error_for_422(install_cache):
    install_cache(incrby_result=None)  # redis down → Cache returns None
    err = handle_agent_ping(HeartbeatReqMeta(aom_uuid=AOM, org_uuid="org-1"))
    assert err == CACHE_ERROR  # → controller returns 422


def test_the_route_answers_a_cache_error_with_a_fixed_503(install_cache):
    install_cache(incrby_result=None)
    with pytest.raises(ApiError) as raised:
        admin_api.heartbeat(BackgroundTasks(), None, Principal("t", 1, "agent", agent_id=AOM))
    assert (raised.value.status, raised.value.code) == (503, "heartbeat_failed")
    assert CACHE_ERROR not in raised.value.message
