"""hs/infra/redis.py Cache.set — NX-result propagation (single-replica lock election).

Regression guard for the fix in Cache.set (hs/infra/redis.py): the wrapper must
return the underlying client's result so SET NX accurately reports whether THIS caller
won the key. Previously it discarded the result and always returned True, which silently
defeated single-replica lock election for the heartbeat sweep (HBSWEEP:lock) — see
hs/admin/heartbeat_sweeper._acquire_sweep_lock:9.
"""

import pytest

from hs.infra.redis import Cache


class _FakeRedisClient:
    """Emulates redis-py SET NX semantics: the first NX set wins (True), a later NX set on
    the same key misses (None — set NOT performed)."""

    def __init__(self):
        self.store = {}
        self.calls = []
        self.expiring = set()

    def set(self, key, value, ex=None, nx=False):
        self.calls.append({"key": key, "value": value, "ex": ex, "nx": nx})
        if nx and key in self.store:
            return None  # NX condition not met -> no write
        self.store[key] = value
        if ex is None:
            self.expiring.discard(key)
        else:
            self.expiring.add(key)
        return True

    def persist(self, key):
        if key not in self.store or key not in self.expiring:
            return False
        self.expiring.remove(key)
        return True

    def ttl(self, key):
        if key not in self.store:
            return -2
        return 30 if key in self.expiring else -1


@pytest.fixture()
def cache_with_fake_client():
    cache = Cache()  # fresh instance, not the process singleton
    cache._client = _FakeRedisClient()  # lazy `client` property returns the injected fake
    return cache, cache._client


def test_plain_set_returns_true_on_success(cache_with_fake_client):
    cache, fake = cache_with_fake_client
    assert cache.set("k", "v") is True  # nx=False success path is unchanged by the fix
    assert fake.store["k"] == "v"


def test_nx_set_returns_true_when_key_is_new(cache_with_fake_client):
    cache, _ = cache_with_fake_client
    assert cache.set("LOCK", "owner-a", nx=True) is True  # first holder wins the lock


def test_nx_set_returns_false_when_key_already_held(cache_with_fake_client):
    cache, fake = cache_with_fake_client
    assert cache.set("LOCK", "owner-a", nx=True) is True
    # A second replica attempts the SAME nx key -> redis declines -> wrapper must report False
    # (this is the bug: it used to return True, so every replica thought it held the lock).
    assert cache.set("LOCK", "owner-b", nx=True) is False
    assert fake.store["LOCK"] == "owner-a"  # original holder not overwritten


def test_nx_set_forwards_ttl_and_nx_flag(cache_with_fake_client):
    cache, fake = cache_with_fake_client
    cache.set("LOCK", "1", expiry_in_seconds=14, nx=True)
    assert fake.calls[0]["ex"] == 14 and fake.calls[0]["nx"] is True


def test_set_returns_false_when_client_raises():
    cache = Cache()

    class _Boom:
        def set(self, *a, **k):
            raise RuntimeError("redis unreachable")

    cache._client = _Boom()
    assert cache.set("k", "v") is False  # error swallowed -> False so the caller backs off


def test_persist_removes_ttl_and_accepts_already_persistent_key(cache_with_fake_client):
    cache, fake = cache_with_fake_client
    cache.set("owner", "human_specialist", expiry_in_seconds=30)

    assert cache.persist("owner") is True
    assert fake.ttl("owner") == -1
    assert cache.persist("owner") is True


def test_persist_rejects_missing_key(cache_with_fake_client):
    cache, _ = cache_with_fake_client

    assert cache.persist("missing") is False


def test_persist_returns_false_when_client_raises():
    cache = Cache()

    class _Boom:
        def persist(self, key):
            raise RuntimeError("redis unreachable")

    cache._client = _Boom()

    assert cache.persist("owner") is False


# ============================================== set_if_absent / set_if_absent_with_error
# Contract tests for the recovery lock:
# the orphan-recovery path calls set_if_absent_with_error on the REAL Cache, and its
# three outcomes carry distinct meanings — claimed (winner), lost claim (confirmed
# contention -> park + 200), Redis error (fail OPEN into the winner path).
def test_set_if_absent_claims_then_declines(cache_with_fake_client):
    cache, fake = cache_with_fake_client
    assert cache.set_if_absent("RLOCK", "a", 60) is True
    assert cache.set_if_absent("RLOCK", "b", 60) is False
    assert fake.store["RLOCK"] == "a"
    assert fake.calls[0]["ex"] == 60 and fake.calls[0]["nx"] is True


def test_set_if_absent_with_error_distinguishes_contention_from_failure(cache_with_fake_client):
    cache, _ = cache_with_fake_client
    assert cache.set_if_absent_with_error("RLOCK", "a", 60) == (True, None)  # claimed
    claimed, err = cache.set_if_absent_with_error("RLOCK", "b", 60)
    assert claimed is False and err is None  # contention


def test_set_if_absent_with_error_reports_redis_failure():
    cache = Cache()

    class _Boom:
        def set(self, *a, **k):
            raise RuntimeError("redis unreachable")

    cache._client = _Boom()
    claimed, err = cache.set_if_absent_with_error("RLOCK", "a", 60)
    assert claimed is False and err is not None and "redis unreachable" in err


def test_err_str_is_never_empty_for_a_message_less_exception():
    """Message-less exceptions still produce a non-empty error string."""
    from hs.infra.redis import _err_str

    class _Boom(Exception):
        pass

    class _Silent(Exception):
        def __str__(self):
            return ""

        def __repr__(self):
            return ""

    class _Hostile(Exception):
        def __str__(self):
            raise RuntimeError("cannot stringify")

        __repr__ = __str__

    assert _err_str(_Boom()), "a message-less exception must still produce a truthy error"
    assert _err_str(_Silent()), "empty __str__ AND __repr__ must still produce a truthy error"
    assert _err_str(_Hostile()), "the error reporter must not raise while reporting an error"
    assert _err_str(ValueError("boom")) == "boom"
