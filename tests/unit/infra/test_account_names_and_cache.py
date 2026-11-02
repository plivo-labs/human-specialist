"""Offline unit tests for the agent-name wrappers, the account models and the Cache redis surface:

- raw-vs-title-case name split: `get_name_*` raw, `get_agent_name_*` title.
- `AOMOccupiedState` 7-key golden order.
- The extended `Cache` redis surface: mget→[None]*n on error, mset pipeline, get→None on error — without
  breaking the existing get/set API.
"""

import pytest

from hs.infra.context import context_req_meta


# --------------------------------------------------------------------------------
# Fixtures / helpers
# --------------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _ctx():
    """A req_meta with an api_id, as a request sets it."""
    token = context_req_meta.set({"api_id": "test-api-id"})
    yield
    context_req_meta.reset(token)


# --------------------------------------------------------------------------------
# hs/admin/account.py wrappers — raw-vs-title-case split, bulk omit, None
# (the account read patched → no live DB)
# --------------------------------------------------------------------------------
UUID4 = "12345678-1234-4234-8234-123456789abc"


def _account(monkeypatch, account):
    import hs.admin.account as da

    found = {UUID4: account} if account else {}
    monkeypatch.setattr(da.AccountOrgMapping, "_accounts", classmethod(lambda cls, *where: found))
    return da.AccountOrgMapping


def test_get_name_by_aom_uuid_returns_raw(monkeypatch):
    from hs.admin.account_models import AgentAccount

    aom = _account(monkeypatch, AgentAccount(name="raw name here", email="e@x", first_name="john", last_name="doe"))
    assert aom.get_name_by_aom_uuid(UUID4) == "raw name here"
    assert aom.get_agent_name_by_aom_uuid(UUID4) == "John Doe"


def test_name_wrappers_without_an_account_return_none(monkeypatch):
    aom = _account(monkeypatch, None)
    assert aom.get_name_by_aom_uuid(UUID4) is None and aom.get_agent_name_by_aom_uuid(UUID4) is None
    assert aom.get_account_by_aom_uuid("not-a-uuid") is None


# --------------------------------------------------------------------------------
# Models — AOMOccupiedState golden 7-key order
# --------------------------------------------------------------------------------
def test_aom_occupied_state_seven_key_golden_order():
    from hs.admin.account_models import AOMOccupiedState

    keys = list(AOMOccupiedState().model_dump().keys())
    assert keys == ["id", "created_at", "updated_at", "is_deleted", "uuid", "is_aom_occupied", "secondary_agent_state"]
    # defaults: occupancy False, secondary state None
    state = AOMOccupiedState().model_dump()
    assert state["is_aom_occupied"] is False
    assert state["secondary_agent_state"] is None


def test_agent_account_get_agent_name_fallback():
    from hs.admin.account_models import AgentAccount

    u = AgentAccount(name="Raw Fallback", email="", first_name="", last_name="")
    assert u.get_agent_name() == "Raw Fallback"  # falls back to raw name
    u2 = AgentAccount(name="raw", email="", first_name="jane", last_name="roe")
    assert u2.get_agent_name() == "Jane Roe"  # title-cased First Last


# --------------------------------------------------------------------------------
# Redis Cache extensions, existing get/set API preserved
# --------------------------------------------------------------------------------
class _FakeRedis:
    def __init__(self):
        self.store = {}
        self.expirations = {}

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value, ex=None, nx=False):
        self.store[key] = value
        if ex:
            self.expirations[key] = ex
        return True

    def mget(self, keys):
        return [self.store.get(k) for k in keys]

    def mset(self, mapping):
        self.store.update(mapping)
        return True

    def delete(self, key):
        return 1 if self.store.pop(key, None) is not None else 0

    def pipeline(self):
        return _FakePipe(self)


class _FakePipe:
    def __init__(self, r):
        self.r = r
        self.ops = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def set(self, key, value, ex=None):
        # real redis-py pipelines support keyed SET+EX in both single-node and cluster mode;
        # Cache.mset now uses it (a pipelined MSET is blocked outright on cluster).
        self.ops.append(("set", key, value, ex))

    def mset(self, mapping):
        self.ops.append(("mset", mapping))
        return self

    def expire(self, key, ttl):
        self.ops.append(("expire", key, ttl))
        return self

    def execute(self):
        for op in self.ops:
            if op[0] == "mset":
                self.r.store.update(op[1])
            elif op[0] == "expire":
                self.r.expirations[op[1]] = op[2]
            elif op[0] == "set":
                self.r.store[op[1]] = op[2]
                if op[3]:
                    self.r.expirations[op[1]] = op[3]
        return [True] * len(self.ops)


def _cache_with_fake(monkeypatch):
    from hs.infra.redis import Cache

    c = Cache()  # fresh instance (production uses the get_instance singleton)
    fake = _FakeRedis()
    monkeypatch.setattr(Cache, "client", property(lambda self: fake))
    return c, fake


def test_cache_get_set_api_preserved():
    from hs.infra.redis import Cache

    # get_instance / get_status / close_redis / client must still exist (lifespan, /status)
    assert hasattr(Cache, "get_instance")
    assert hasattr(Cache, "get_status")
    assert hasattr(Cache, "close_redis")
    inst = Cache.get_instance()
    assert inst is Cache.get_instance()  # singleton


def test_cache_get_set_delete(monkeypatch):
    c, fake = _cache_with_fake(monkeypatch)
    assert c.set("{1}AOMN:u", "Alice", expiry_in_seconds=86400) is True
    assert fake.expirations["{1}AOMN:u"] == 86400
    assert c.get("{1}AOMN:u") == "Alice"
    assert c.delete("{1}AOMN:u") == 1


def test_cache_get_none_on_error(monkeypatch):
    from hs.infra.redis import Cache

    c = Cache()

    class _Boom:
        def get(self, key):
            raise RuntimeError("redis down")

    monkeypatch.setattr(Cache, "client", property(lambda self: _Boom()))
    assert c.get("k") is None


def test_cache_mset_pipeline_with_expiry(monkeypatch):
    c, fake = _cache_with_fake(monkeypatch)
    ok = c.mset({"{1}AOMN:a": "A", "{1}AOMN:b": "B"}, expiry_in_seconds=86400)
    assert ok is True
    assert fake.store["{1}AOMN:a"] == "A"
    assert fake.store["{1}AOMN:b"] == "B"
    # per-key EXPIRE applied (MSET has no TTL option)
    assert fake.expirations["{1}AOMN:a"] == 86400
    assert fake.expirations["{1}AOMN:b"] == 86400


def test_cache_mset_empty_mapping_is_vacuous_success(monkeypatch):
    # empty mapping = nothing to write = True, silently (no redis call, no ERROR log).
    c, fake = _cache_with_fake(monkeypatch)
    assert c.mset({}, expiry_in_seconds=86400) is True
    assert fake.store == {}  # nothing written


def test_cache_mset_no_expiry_plain(monkeypatch):
    c, fake = _cache_with_fake(monkeypatch)
    assert c.mset({"{1}AOMN:a": "A"}) is True
    assert fake.store["{1}AOMN:a"] == "A"
    assert "{1}AOMN:a" not in fake.expirations  # no EXPIRE without expiry arg
