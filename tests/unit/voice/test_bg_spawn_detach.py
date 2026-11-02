"""spawn_detached / _spawn delegation — the background-session-detach guards.

Copying the caller's FULL contextvars context into a daemon thread hands it ``context_db_session``
— the REQUEST's SQLAlchemy Session — so two threads drive one Session: the request thread's COMMIT
races the worker's SAVEPOINT (``psycopg2.errors.NoActiveSqlTransaction``) and best-effort
side-writes (chunk related-data, closed-conv search rows, call legs) are silently dropped.

These tests pin:
  1. the worker NEVER sees the caller's context session (it falls back to its own scoped session);
  2. the logging context (api_id / req_meta) still propagates;
  3. teardown (rollback + remove) happens on the WORKER thread, errors contained.
Plus a drift guard: every usecase ``_spawn`` must delegate to hs.infra.bg_spawn.spawn_detached.
"""

import threading
import time

import pytest

from hs.infra import bg_spawn
from hs.infra import context as cm


def _run_and_wait(fn, *args, timeout=5.0):
    done = threading.Event()
    box = {}

    def target(*a):
        try:
            box["result"] = fn(*a)
        finally:
            done.set()

    bg_spawn.spawn_detached(target, *args)
    assert done.wait(timeout), "spawned target did not finish"
    return box.get("result")


class _MarkerSession:
    """Poses as the request session; the worker must never touch it."""

    def __init__(self):
        self.touched = []

    def __getattr__(self, name):  # any real use inside the worker would record itself
        self.touched.append(name)
        raise AssertionError(f"request session used from worker thread: .{name}")


def test_worker_never_sees_the_request_session():
    marker = _MarkerSession()
    token = cm.context_db_session.set(marker)
    try:
        seen = _run_and_wait(lambda: cm.get_db_session())
    finally:
        cm.context_db_session.reset(token)
    # Detached: the worker got the thread-scoped fallback, not the request session.
    assert seen is not marker
    assert seen is not None
    assert marker.touched == []


def test_worker_keeps_logging_context():
    tok_api = cm.context_api_id.set("api-detach-test")
    tok_meta = cm.context_req_meta.set({"api_id": "api-detach-test"})
    try:
        seen = _run_and_wait(lambda: (cm.context_api_id.get(), cm.context_req_meta.get()))
    finally:
        cm.context_api_id.reset(tok_api)
        cm.context_req_meta.reset(tok_meta)
    assert seen == ("api-detach-test", {"api_id": "api-detach-test"})


def test_worker_session_is_removed_after_run(monkeypatch):
    calls = []
    monkeypatch.setattr(cm, "remove_db_session", lambda: calls.append(threading.get_ident()))
    _run_and_wait(lambda: None)
    for _ in range(200):  # the guard's finally runs right after the target; give it a beat
        if calls:
            break
        time.sleep(0.01)
    # teardown ran, and ON the worker thread (the session's owner), not the caller's.
    assert len(calls) == 1
    assert calls[0] != threading.get_ident()


def test_error_log_runs_inside_the_copied_context(monkeypatch):
    # ctx.run restores the previous context before it raises, so an OUTER
    # except would log the failure without the caller's request metadata. The failure log must
    # run inside the copied context and carry context_req_meta (the logger reads it at call time).
    captured = {}
    monkeypatch.setattr(
        bg_spawn.logger,
        "error",
        lambda *a, **kw: captured.update(kw, msg=a[0] if a else kw.get("msg"), ctx=cm.context_req_meta.get()),
    )
    done = threading.Event()

    def boom():
        try:
            raise RuntimeError("kaboom")
        finally:
            done.set()

    tok = cm.context_req_meta.set({"api_id": "api-ctx-err"})
    try:
        bg_spawn.spawn_detached(boom)
        assert done.wait(5.0)
    finally:
        cm.context_req_meta.reset(tok)
    for _ in range(200):
        if captured:
            break
        time.sleep(0.01)
    assert "kaboom" in captured.get("msg", "")
    assert captured.get("ctx") == {"api_id": "api-ctx-err"}


def test_error_in_target_is_contained_and_still_torn_down(monkeypatch):
    removed = []
    rolled = []
    monkeypatch.setattr(cm, "remove_db_session", lambda: removed.append(True))
    monkeypatch.setattr(cm, "set_db_session_rollback", lambda: rolled.append(True))
    done = threading.Event()

    def boom():
        try:
            raise RuntimeError("kaboom")
        finally:
            done.set()

    bg_spawn.spawn_detached(boom)  # must not raise into the caller
    assert done.wait(5.0)
    for _ in range(200):
        if removed and rolled:
            break
        time.sleep(0.01)
    assert rolled == [True]
    assert removed == [True]


_SPAWN_MODULES = ["hs.voice.agent_leg", "hs.voice.callbacks", "hs.voice.intake", "hs.voice.monitor", "hs.voice.hangup"]


@pytest.mark.parametrize("modname", _SPAWN_MODULES)
def test_every_spawn_delegates_to_spawn_detached(modname, monkeypatch):
    """Drift guard: a module regrowing its own context-copying thread spawner reintroduces the
    cross-thread-session bug. Every _spawn must route through hs.infra.bg_spawn.spawn_detached."""
    import importlib

    mod = importlib.import_module(modname)
    captured = {}
    monkeypatch.setattr(bg_spawn, "spawn_detached", lambda target, *a, **kw: captured.update(t=target, a=a, kw=kw))
    marker = object()
    mod._spawn(lambda *_: None, marker)
    assert captured, f"{modname}._spawn did not delegate to spawn_detached"
    assert captured["a"] == (marker,)
