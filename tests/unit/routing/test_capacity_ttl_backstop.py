"""Agent-capacity TTL backstop.

The CAPACITY set has no TTL, so a member leaked by any path strands the agent unroutable forever.
update_agent_capacity puts a best-effort TTL on the agent-capacity set after every successful mutation
(acquire returns '0', a pure release returns '-3'), sized well above max call+ACW so a live call is never
freed mid-call. These tests pin: TTL on acquire and release, NOT on failed mutations, disable via TTL=0,
and that a failing/missing expire never changes the capacity op's return code.
"""

import fakeredis

from hs.infra.redis import Cache
from hs.routing import constants as rc
from hs.routing.redis_store import RoutingRedis


class _RecordingCache:
    """eval_script returns a canned code; expire records (or optionally raises)."""

    def __init__(self, code="0", expire_raises=False):
        self._code = code
        self._expire_raises = expire_raises
        self.expire_calls = []

    def eval_script(self, src, keys=None, args=None):
        return self._code

    def expire(self, key, expiry_in_seconds):
        if self._expire_raises:
            raise RuntimeError("redis down")
        self.expire_calls.append((key, expiry_in_seconds))
        return True


def _cap_key(auth="o", agent="a", channel="chat"):
    return rc.agent_capacity_key(auth, agent, channel)


def test_acquire_sets_ttl_on_agent_capacity_set():
    cache = _RecordingCache(code="0")
    code, err = RoutingRedis(cache=cache).increase_agent_capacity("o", "a", "chat", 5, "c1")
    assert code == 0 and err is None
    # TTL applied to exactly the agent-capacity set (keys[0]); OCU/CCK untouched.
    # Channel-sized (chat = async ladder), not the flat call-sized base.
    assert cache.expire_calls == [(_cap_key(), rc.agent_capacity_ttl_seconds_for_channel("chat"))]


def test_reassign_move_refreshes_ttl():
    cache = _RecordingCache(code="0")
    assert RoutingRedis(cache=cache).update_agents_capacity("o", "a", "chat", "old", "new", 5) is None
    assert cache.expire_calls == [(_cap_key(), rc.agent_capacity_ttl_seconds_for_channel("chat"))]


def _real_lua(ttl=3600):
    """Actual UPDATE_AGENT_CAPACITY Lua on fakeredis: two reservations, set aged to 60s when TTL is on."""
    cache = Cache()
    cache._client = fakeredis.FakeStrictRedis(decode_responses=True)
    rr = RoutingRedis(cache=cache)
    for conv in ("c1", "c2"):
        assert rr.update_agent_capacity("o", "a", "call", 5, new_contact=conv, ttl_seconds=ttl) == "0"
    if ttl:
        cache._client.expire(_cap_key(channel="call"), 60)
    return rr, cache._client, _cap_key(channel="call")


def test_real_lua_partial_release_refreshes_remaining_capacity_ttl():
    rr, client, key = _real_lua()
    assert rr.update_agent_capacity("o", "a", "call", 0, old_contact="c1", ttl_seconds=3600) == "-3"
    assert client.smembers(key) == {"c2"}
    assert client.pttl(key) > 3_590_000


def test_real_lua_last_release_deletes_capacity_set():
    rr, client, key = _real_lua()
    for conv in ("c1", "c2"):
        assert rr.update_agent_capacity("o", "a", "call", 0, old_contact=conv, ttl_seconds=3600) == "-3"
    assert not client.exists(key)


def test_real_lua_missing_member_release_does_not_extend_ttl():
    rr, client, key = _real_lua()
    assert rr.update_agent_capacity("o", "a", "call", 0, old_contact="nope", ttl_seconds=3600) == "-1"
    assert client.smembers(key) == {"c1", "c2"}
    assert client.pttl(key) <= 60_000


def test_real_lua_capacity_ttl_disabled():
    rr, client, key = _real_lua(ttl=0)
    assert rr.update_agent_capacity("o", "a", "call", 0, old_contact="c1", ttl_seconds=0) == "-3"
    assert client.ttl(key) == -1


def test_capacity_ttl_is_sized_per_channel():
    # call keeps the env-tunable base; long-lived channels get their session-flow lifetime
    # + 1h margin so a SATURATED agent's set cannot expire mid-conversation (scard=0 would
    # read as free capacity -> over-allocation).
    base = rc.AGENT_CAPACITY_TTL_SECONDS
    assert rc.agent_capacity_ttl_seconds_for_channel(rc.CHANNEL_CALL) == base
    assert rc.agent_capacity_ttl_seconds_for_channel(rc.CHANNEL_TASK) == max(
        base, (rc.EXPIRY_DURATION_IN_HOURS_SESSION_TASK + 1) * 3600
    )
    assert rc.agent_capacity_ttl_seconds_for_channel(rc.CHANNEL_CALLBACK) == max(
        base, (rc.EXPIRY_DURATION_IN_HOURS_SESSION_TASK + 1) * 3600
    )
    assert rc.agent_capacity_ttl_seconds_for_channel(rc.CHANNEL_MESSAGE) == max(
        base, (rc.EXPIRY_DURATION_IN_HOURS_CALLBACK + 1) * 3600
    )
    assert rc.agent_capacity_ttl_seconds_for_channel(rc.CHANNEL_CHAT) == max(
        base, (rc.EXPIRY_DURATION_IN_HOURS_ASYNC + 1) * 3600
    )


def test_failed_acquire_does_not_set_ttl():
    # code -2 (capacity occupied) is NOT a successful acquire → no TTL
    cache = _RecordingCache(code="-2")
    code, _ = RoutingRedis(cache=cache).increase_agent_capacity("o", "a", "chat", 5, "c1")
    assert code == -2
    assert cache.expire_calls == []


def test_ttl_zero_disables_backstop(monkeypatch):
    monkeypatch.setattr(rc, "AGENT_CAPACITY_TTL_SECONDS", 0)
    cache = _RecordingCache(code="0")
    RoutingRedis(cache=cache).increase_agent_capacity("o", "a", "chat", 5, "c1")
    assert cache.expire_calls == []


def test_expire_failure_never_breaks_capacity_op():
    # A missing/failing expire (e.g. Redis down, or a cache double without expire) must not change the
    # acquire result — the backstop is best-effort.
    cache = _RecordingCache(code="0", expire_raises=True)
    code, err = RoutingRedis(cache=cache).increase_agent_capacity("o", "a", "chat", 5, "c1")
    assert code == 0 and err is None


def test_ttl_default_is_safely_above_max_call():
    # Guard the sizing invariant: the backstop must be comfortably longer than any legitimate call+ACW.
    assert rc.AGENT_CAPACITY_TTL_SECONDS >= 3600  # >= 1h floor; default 4h
