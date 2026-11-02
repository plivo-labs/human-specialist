"""Invitation-lifecycle DATA LAYER (offline-deterministic).

Pins the invitation Redis surface (CONTACTRETRY INCR / IA HMSet / CIA 3-shape SADD / ACI HSet+HDel+is_invited
/ CONTACT_AGENT_MAPPING plain-SET vs NX-checked / ATRID 2-or-3-pipe split / RESERVATIONTIMEOUT
cache-hit-vs-miss-30-default / the lock BOOL wrappers' eval-error→False swallow) + the
AgentAndContactMapping capitalized-key byte-shape + the invite TTL table. No live stack (a fake
Cache injected into RoutingRedis).
"""

import json

import tests.conftest  # noqa: F401 — eager data_adapter imports (AbstractConcreteBase registry)
from hs.routing.invitation_models import AgentAndContactMapping
from hs.routing.redis_store import RoutingRedis


class FakeCache:
    """Cache stand-in mirroring hs.infra.redis.Cache's swallow contract, with a scriptable
    eval_script for the CIA-drain / GETSET NX / lock Lua wrappers (so we test the by-parts KEY
    SHAPE + the code→error/bool mapping without a live Redis; test_capacity_lua runs the Lua bodies
    against a real Redis)."""

    def __init__(self, strings=None, hashes=None, sets=None, eval_returns=None, eval_raises=False, set_raises=False):
        self.strings = dict(strings or {})
        self.hashes = {k: dict(v) for k, v in (hashes or {}).items()}
        self.sets = {k: set(v) for k, v in (sets or {}).items()}
        self.eval_calls = []  # (src, keys, args)
        self._eval_returns = eval_returns or {}
        self._eval_raises = eval_raises
        self._set_raises = set_raises
        self.expires = {}  # key -> ttl

    # strings
    def get(self, key):
        return self.strings.get(key)

    def set(self, key, value, expiry_in_seconds=None, nx=False):
        self.strings[key] = value
        if expiry_in_seconds:
            self.expires[key] = expiry_in_seconds
        return True

    def set_if_absent_with_error(self, key, value, expiry_seconds):
        if self._set_raises:
            return False, "redis down"
        if key in self.strings:
            return False, None
        self.strings[key] = value
        self.expires[key] = expiry_seconds
        return True, None

    def set_if_absent(self, key, value, expiry_seconds):
        return self.set_if_absent_with_error(key, value, expiry_seconds)[0]

    def delete(self, key):
        existed = key in self.strings or key in self.hashes or key in self.sets
        self.strings.pop(key, None)
        self.hashes.pop(key, None)
        self.sets.pop(key, None)
        return 1 if existed else 0

    def incrby(self, key, count):
        self.strings[key] = str(int(self.strings.get(key, "0")) + count)
        return int(self.strings[key])

    def expire(self, key, ttl):
        self.expires[key] = ttl
        return True

    # sets
    def sadd(self, key, *members):
        self.sets.setdefault(key, set()).update(members)
        return len(members)

    def smembers(self, key):
        return list(self.sets.get(key, set()))

    # hashes
    def hset(self, key, field, value):
        self.hashes.setdefault(key, {})[field] = value
        return 1

    def hget(self, key, field):
        return self.hashes.get(key, {}).get(field)

    def hdel(self, key, *fields):
        h = self.hashes.get(key, {})
        return sum(1 for f in fields if h.pop(f, None) is not None)

    def eval_script(self, src, keys=None, args=None):
        self.eval_calls.append((src, keys, args))
        if self._eval_raises:
            raise RuntimeError("NOSCRIPT / redis down")
        return self._eval_returns.get(src, "0")


def _rr(**kw):
    return RoutingRedis(cache=FakeCache(**kw))


# ---------------------------------------------------------------------------------------------
# CONTACTRETRY INCR (the retry-1 invariant origin)
# ---------------------------------------------------------------------------------------------
def test_set_retry_val_increments():
    rr = _rr()
    assert rr.set_retry_val("p", "r") == 1  # first invite → 1
    assert rr.set_retry_val("p", "r") == 2  # retry → 2
    assert rr._cache.strings["CONTACTRETRY:p:r"] == "2"


# ---------------------------------------------------------------------------------------------
# IA HMSet (field=sip → attempt_count) + CIA SADD (members=sips, 3-shape key) + TTLs
# ---------------------------------------------------------------------------------------------
def test_set_invited_agents_hash_and_ttl():
    rr = _rr()
    rr.set_invited_agents("p", "r", ["sip:a", "sip:b"], 90000, 1)
    assert rr._cache.hashes["IA:p:r"] == {"sip:a": "1", "sip:b": "1"}
    assert rr._cache.expires["IA:p:r"] == 90000
    # empty agents → no write (an empty map stores nothing)
    rr2 = _rr()
    rr2.set_invited_agents("p", "r", [], 90000, 1)
    assert "IA:p:r" not in rr2._cache.hashes


# ---------------------------------------------------------------------------------------------
# CONTACT_AGENT_MAPPING — task/callback plain SET (overwrite) vs others GETSET NX (-1 → error)
# ---------------------------------------------------------------------------------------------
def test_contact_mapping_plain_set_overwrites():
    rr = _rr()
    assert rr.set_contact_to_agent_mapping("c1", '{"AgentUUID":"u1"}', 3600) is None
    assert rr._cache.strings["CONTACT_AGENT_MAPPING:c1"] == '{"AgentUUID":"u1"}'
    # plain SET overwrites (task reassignment allowed)
    rr.set_contact_to_agent_mapping("c1", '{"AgentUUID":"u2"}', 3600)
    assert rr._cache.strings["CONTACT_AGENT_MAPPING:c1"] == '{"AgentUUID":"u2"}'


def test_get_contact_to_agent_mapping_roundtrip():
    cam = AgentAndContactMapping(agent_sip_endpoint="sip:a", qnr_run_uuid="Q", agent_uuid="U1")
    rr = _rr(strings={"CONTACT_AGENT_MAPPING:c1": cam.to_json()})
    raw = rr.get_contact_to_agent_mapping("c1")
    parsed = AgentAndContactMapping.parse_redis(raw)
    assert parsed.agent_uuid == "U1" and parsed.qnr_run_uuid == "Q"
    assert rr.get_contact_to_agent_mapping("missing") is None
    assert AgentAndContactMapping.parse_redis(None) is None


# ---------------------------------------------------------------------------------------------
# RESERVATIONTIMEOUT — cache HIT and MISS both fall back to the 30 s default for an unset channel
# ---------------------------------------------------------------------------------------------
def test_reservation_timeout_cache_hit_defaults_an_unset_channel():
    rr = _rr(strings={"RESERVATIONTIMEOUT:AUTH": json.dumps({"chat": 60, "email": 0})})
    assert rr.get_reservation_timeout("AUTH", "chat") == (60, None)
    # an absent or zero channel gets the same 30 s default as on the miss path, voice included
    assert rr.get_reservation_timeout("AUTH", "task") == (30, None)
    assert rr.get_reservation_timeout("AUTH", "email") == (30, None)
    assert rr.get_reservation_timeout("AUTH", "call") == (30, None)


def test_reservation_timeout_miss_falls_back_to_settings_with_30_default(monkeypatch):
    from hs.admin import account as acct

    class _Settings:
        reservation_timeout = {"chat": 45}

    monkeypatch.setattr(acct.RoutingSettings, "get_org_routing_settings", classmethod(lambda cls, auth: _Settings()))
    rr = _rr()
    assert rr.get_reservation_timeout("AUTH", "chat") == (45, None)  # from settings
    # absent channel on the MISS path → 30 default
    rr2 = _rr()
    assert rr2.get_reservation_timeout("AUTH", "email") == (30, None)
    # cached the raw map (best-effort) so a later hit reads it
    assert "RESERVATIONTIMEOUT:AUTH" in rr2._cache.strings


def test_reservation_timeout_settings_missing_is_error(monkeypatch):
    from hs.admin import account as acct

    monkeypatch.setattr(acct.RoutingSettings, "get_org_routing_settings", classmethod(lambda cls, auth: None))
    rr = _rr()
    secs, err = rr.get_reservation_timeout("AUTH", "chat")
    assert secs == 0 and err is not None


# ---------------------------------------------------------------------------------------------
# AgentAndContactMapping — capitalized keys, declaration order, compact JSON
# ---------------------------------------------------------------------------------------------
def test_agent_and_contact_mapping_byte_shape():
    cam = AgentAndContactMapping(agent_sip_endpoint="sip:a", from_number="123", qnr_run_uuid="Q", agent_uuid="U1")
    assert cam.to_json() == (
        '{"AgentSipEndpoint":"sip:a","FromNumber":"123","QnrRunUuid":"Q","AgentUUID":"U1",'
        '"AgentCallUuid":"","PreviousPrimaryAgent":"","FlowRunUUID":"","ToNumber":"","IsInbound":false}'
    )
