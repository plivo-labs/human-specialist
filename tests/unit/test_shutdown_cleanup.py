"""Lifespan shutdown completeness.

hs/app.py's shutdown path used to only close Redis, leaving the SQLAlchemy
core_engine pool open on rolling deploys (half-open TCP to Postgres). This pins
_shutdown_cleanup(): it closes Redis AND disposes core_engine, each in its own
try/except so a failure in one step does not skip the other.
"""

import tests.conftest  # noqa: F401 — eager data_adapter imports (AbstractConcreteBase registry)
from hs.app import _shutdown_cleanup


class _FakeCache:
    def __init__(self, raise_on_close=False):
        self.closed = False
        self._raise_on_close = raise_on_close

    def close_redis(self):
        if self._raise_on_close:
            raise RuntimeError("redis close boom")
        self.closed = True


class _FakeEngine:
    def __init__(self, raise_on_dispose=False):
        self.disposed = False
        self._raise_on_dispose = raise_on_dispose

    def dispose(self):
        if self._raise_on_dispose:
            raise RuntimeError("engine dispose boom")
        self.disposed = True


def _install(monkeypatch, cache, engine):
    import hs.infra.db as db_module
    from hs.infra.redis import Cache

    monkeypatch.setattr(Cache, "get_instance", staticmethod(lambda: cache))
    monkeypatch.setattr(db_module, "core_engine", engine)


def test_happy_path_closes_redis_and_disposes_engine(monkeypatch):
    cache = _FakeCache()
    engine = _FakeEngine()
    _install(monkeypatch, cache, engine)

    _shutdown_cleanup()

    assert cache.closed is True
    assert engine.disposed is True


def test_redis_close_failure_still_disposes_engine(monkeypatch):
    cache = _FakeCache(raise_on_close=True)
    engine = _FakeEngine()
    _install(monkeypatch, cache, engine)

    _shutdown_cleanup()  # must not raise

    assert engine.disposed is True


def test_engine_dispose_failure_does_not_raise(monkeypatch):
    cache = _FakeCache()
    engine = _FakeEngine(raise_on_dispose=True)
    _install(monkeypatch, cache, engine)

    _shutdown_cleanup()  # must not raise

    assert cache.closed is True
