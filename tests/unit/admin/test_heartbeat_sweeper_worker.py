"""hs/admin/heartbeat_sweeper.py — the once-per-process offline-sweep driver.

`sweep_offline_agents()` is covered by test_heartbeat_self_sweep.py; THIS file pins the worker
that drives it:

  * _acquire_sweep_lock     — single-replica election via Redis SET NX + auto-expiry TTL
  * start_heartbeat_sweeper — flag-gated, idempotent (once-per-process) daemon launch
  * _run_loop               — lock-gated sweep dispatch + per-tick error swallowing

Source: hs/admin/heartbeat_sweeper.py.
"""

import pytest

import hs.admin.heartbeat as hb
import hs.admin.heartbeat_sweeper as sweeper
import hs.infra.constants


@pytest.fixture(autouse=True)
def _reset_started():
    # start_heartbeat_sweeper guards re-entry with a process-global; isolate every test.
    sweeper._started = False
    yield
    sweeper._started = False


# --- _acquire_sweep_lock: single-replica election (hs/admin/heartbeat_sweeper.py) --------------


class _FakeCache:
    def __init__(self, ret):
        self._ret = ret
        self.calls = []

    def set(self, key, value, expiry_in_seconds=None, nx=False):
        self.calls.append({"key": key, "value": value, "ttl": expiry_in_seconds, "nx": nx})
        return self._ret


def _patch_cache(monkeypatch, ret):
    import hs.infra.redis as redis_mod

    fake = _FakeCache(ret)
    monkeypatch.setattr(redis_mod.Cache, "get_instance", classmethod(lambda cls: fake))
    return fake


def test_acquire_lock_true_when_this_replica_wins(monkeypatch):
    _patch_cache(monkeypatch, True)
    assert sweeper._acquire_sweep_lock(15) is True  # NX set succeeded -> this replica sweeps this tick


def test_acquire_lock_false_when_another_replica_holds(monkeypatch):
    # SET NX returns falsy when the key already exists -> another replica owns the tick.
    _patch_cache(monkeypatch, None)
    assert sweeper._acquire_sweep_lock(15) is False  # must NOT sweep (no duplicate offline sweeps)


def test_acquire_lock_sets_nx_with_lock_key_and_ttl_below_interval(monkeypatch):
    fake = _patch_cache(monkeypatch, True)
    sweeper._acquire_sweep_lock(15)
    call = fake.calls[0]
    assert call["key"] == hs.infra.constants.HEARTBEAT_SWEEP_LOCK_KEY
    assert call["nx"] is True  # election relies on NX
    assert call["ttl"] == 14  # max(1, interval-1): expires before the next tick


def test_acquire_lock_ttl_never_drops_below_one(monkeypatch):
    # A 1s (or 0s) interval must not produce a non-positive TTL (Redis would reject it).
    fake = _patch_cache(monkeypatch, True)
    sweeper._acquire_sweep_lock(1)
    assert fake.calls[0]["ttl"] == 1  # max(1, 0)


# --- start_heartbeat_sweeper: flag gate + idempotency (hs/admin/heartbeat_sweeper.py) ---------


class _RecordingThread:
    instances = []

    def __init__(self, target=None, name=None, daemon=None):
        self.target, self.name, self.daemon, self.started = target, name, daemon, False
        _RecordingThread.instances.append(self)

    def start(self):
        # Deliberately do NOT invoke target — _run_loop is an infinite loop.
        self.started = True


def _patch_thread(monkeypatch):
    _RecordingThread.instances = []
    monkeypatch.setattr(sweeper.threading, "Thread", _RecordingThread)


def test_start_launches_one_named_daemon_thread(monkeypatch):
    _patch_thread(monkeypatch)
    sweeper.start_heartbeat_sweeper()
    assert len(_RecordingThread.instances) == 1
    t = _RecordingThread.instances[0]
    assert t.target is sweeper._run_loop
    assert t.name == "heartbeat-sweeper"
    assert t.daemon is True  # daemon -> never blocks process shutdown
    assert t.started is True
    assert sweeper._started is True


def test_start_is_idempotent_per_process(monkeypatch):
    _patch_thread(monkeypatch)
    sweeper.start_heartbeat_sweeper()
    sweeper.start_heartbeat_sweeper()  # second call must not spawn a 2nd sweeper
    assert len(_RecordingThread.instances) == 1


# --- _run_loop: lock-gated dispatch + resilience (hs/admin/heartbeat_sweeper.py) --------------


class _StopLoop(Exception):
    """Raised from a patched time.sleep to break the otherwise-infinite loop."""


def _sleep_breaking_after(n_ticks):
    state = {"n": 0}

    def _sleep(_seconds):
        state["n"] += 1
        if state["n"] >= n_ticks:
            raise _StopLoop

    return _sleep, state


def test_run_loop_sweeps_when_lock_acquired(monkeypatch):
    monkeypatch.setattr(hs.infra.constants, "HEARTBEAT_SWEEP_INTERVAL_SECONDS", 15)
    monkeypatch.setattr(sweeper, "_acquire_sweep_lock", lambda interval: True)
    swept = []
    monkeypatch.setattr(hb, "sweep_offline_agents", lambda: swept.append("tick") or 3)
    sleep, _ = _sleep_breaking_after(2)
    monkeypatch.setattr(sweeper.time, "sleep", sleep)

    with pytest.raises(_StopLoop):
        sweeper._run_loop()
    assert swept == ["tick"]  # exactly one sweep dispatched on the held tick


def test_run_loop_skips_sweep_when_lock_not_acquired(monkeypatch):
    monkeypatch.setattr(hs.infra.constants, "HEARTBEAT_SWEEP_INTERVAL_SECONDS", 15)
    monkeypatch.setattr(sweeper, "_acquire_sweep_lock", lambda interval: False)
    swept = []
    monkeypatch.setattr(hb, "sweep_offline_agents", lambda: swept.append("tick"))
    sleep, _ = _sleep_breaking_after(2)
    monkeypatch.setattr(sweeper.time, "sleep", sleep)

    with pytest.raises(_StopLoop):
        sweeper._run_loop()
    assert swept == []  # lock held elsewhere -> this replica stays idle


def test_run_loop_swallows_sweep_errors_and_keeps_ticking(monkeypatch):
    monkeypatch.setattr(hs.infra.constants, "HEARTBEAT_SWEEP_INTERVAL_SECONDS", 15)
    monkeypatch.setattr(sweeper, "_acquire_sweep_lock", lambda interval: True)

    def _boom():
        raise RuntimeError("redis down mid-sweep")

    monkeypatch.setattr(hb, "sweep_offline_agents", _boom)
    sleep, state = _sleep_breaking_after(3)
    monkeypatch.setattr(sweeper.time, "sleep", sleep)

    # The per-tick try/except must absorb the sweep error so the loop survives to later ticks.
    with pytest.raises(_StopLoop):
        sweeper._run_loop()
    assert state["n"] == 3  # reached the 3rd tick despite every sweep raising
