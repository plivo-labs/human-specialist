"""Cache.mget/mset multi-key paths: empty input short-circuits, TTL writes are per-key SET EX in one pipeline."""

import pytest

from hs.infra.redis import Cache


class _FakeClient:
    """Records multi-key command usage; emulates just enough of redis-py for these paths."""

    def __init__(self):
        self.calls = []
        self.pipe = _FakePipeline(self)

    def mget(self, keys):
        self.calls.append(("mget", tuple(keys)))
        return [None] * len(keys)

    def mset(self, mapping):
        self.calls.append(("mset", dict(mapping)))
        return True

    def pipeline(self):
        return self.pipe


class _FakePipeline:
    def __init__(self, parent):
        self.parent = parent
        self.queued = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def set(self, key, value, ex=None):
        self.queued.append(("set", key, value, ex))

    def mset(self, mapping):
        self.queued.append(("mset", dict(mapping)))

    def expire(self, key, ttl):
        self.queued.append(("expire", key, ttl))

    def execute(self):
        self.parent.calls.append(("pipeline_execute", tuple(self.queued)))
        return [True] * len(self.queued)


@pytest.fixture()
def cache():
    c = Cache()  # fresh instance, not the process singleton
    c._client = _FakeClient()
    return c


# ---- mset ---------------------------------------------------------------------------------


def test_mset_with_expiry_pipelines_per_key_set_ex_never_mset(cache):
    ok = cache.mset({"k1": "v1", "k2": "v2"}, expiry_in_seconds=30)
    assert ok is True
    ((name, queued),) = cache._client.calls
    assert name == "pipeline_execute"
    assert ("set", "k1", "v1", 30) in queued and ("set", "k2", "v2", 30) in queued
    assert all(op[0] != "mset" for op in queued)
    assert all(op[0] != "expire" for op in queued), "SET+EX replaces the separate EXPIRE"


def test_mset_without_expiry_on_single_node_keeps_plain_mset(cache):
    assert cache.mset({"k": "v"}) is True
    assert cache._client.calls == [("mset", {"k": "v"})]


def test_mset_empty_mapping_is_vacuous_success_no_calls(cache):
    assert cache.mset({}) is True
    assert cache._client.calls == []


def test_mset_error_returns_false(cache):
    def boom(mapping):
        raise RuntimeError("redis says no")

    cache._client.mset = boom
    assert cache.mset({"k": "v"}) is False
