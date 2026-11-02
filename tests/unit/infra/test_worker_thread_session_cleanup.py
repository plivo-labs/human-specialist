"""The fallback db_session must be torn down on the thread that OWNS it.

The module-level ``db_session`` is a scoped_session keyed by OS thread. Context-less
routers create their session on the AnyIO worker thread running the `def` handler, but
the app-level guard tears down on the event-loop thread — so its remove() hit an empty
slot and worker sessions leaked across requests on reused threads (empirically verified:
stale identity-map reads). threadpool_db_guard (wired into fastapi.routing's def-endpoint
dispatch by _patch_def_endpoint_db_guard) now cleans the owning thread's slot; the
app-level guard gained a fallback_session_active() gate so it no longer lazily creates a
throwaway session on the loop per context-less request.
"""

import threading
from types import SimpleNamespace

import pytest
from starlette.testclient import TestClient

import hs.infra.context as ctx
import tests.conftest  # noqa: F401
from hs.app import app


class _FakeScopedSession:
    """Thread-keyed stand-in for the scoped_session proxy: records which THREAD each
    lifecycle call ran on, and models the registry (has/instantiate/clear) per thread."""

    def __init__(self):
        self.slots = {}  # thread_id -> True (live session)
        self.events = []  # (op, thread_id)
        self.registry = SimpleNamespace(has=lambda: threading.get_ident() in self.slots)

    def touch(self):  # models get_db_session().query(...) first use
        self.slots[threading.get_ident()] = True
        self.events.append(("create", threading.get_ident()))

    def commit(self):
        self.events.append(("commit", threading.get_ident()))

    def rollback(self):
        self.events.append(("rollback", threading.get_ident()))

    def remove(self):
        self.slots.pop(threading.get_ident(), None)
        self.events.append(("remove", threading.get_ident()))


@pytest.fixture()
def fake_scoped(monkeypatch):
    fake = _FakeScopedSession()
    monkeypatch.setattr(ctx, "db_session", fake)
    return fake


def test_guard_cleans_slot_on_owning_thread(fake_scoped):
    seen = {}

    def _handler():
        fake_scoped.touch()
        seen["thread"] = threading.get_ident()
        return "ok"

    ctx.context_db_session.set(None)
    ctx.context_db_commit.set(True)
    assert ctx.threadpool_db_guard(_handler)() == "ok"
    # committed AND removed on the SAME thread that created the session
    assert ("commit", seen["thread"]) in fake_scoped.events
    assert ("remove", seen["thread"]) in fake_scoped.events
    assert fake_scoped.slots == {}  # slot actually cleared


def test_guard_noop_when_thread_never_touched_db(fake_scoped):
    ctx.context_db_session.set(None)
    assert ctx.threadpool_db_guard(lambda: "ok")() == "ok"
    assert fake_scoped.events == []  # no lazy create-just-to-remove


def test_guard_noop_when_contextvar_session_owned_elsewhere(fake_scoped):
    token = ctx.context_db_session.set(object())  # a request's session: get_db owns its lifecycle
    try:
        assert ctx.threadpool_db_guard(lambda: "ok")() == "ok"
        assert fake_scoped.events == []
    finally:
        ctx.context_db_session.reset(token)


def test_guard_rolls_back_on_exception(fake_scoped):
    def _boom():
        fake_scoped.touch()
        raise RuntimeError("handler failed")

    ctx.context_db_session.set(None)
    ctx.context_db_commit.set(True)
    with pytest.raises(RuntimeError):
        ctx.threadpool_db_guard(_boom)()
    ops = [op for op, _ in fake_scoped.events]
    assert ops == ["create", "rollback", "remove"]  # never commit on the failure path
    ctx.context_db_commit.set(True)  # restore for later tests


def test_def_endpoint_leak_fixed_end_to_end(fake_scoped):
    """The empirical leak repro, at the app level: a context-less `def` endpoint creates a
    fallback session on its worker thread; post-fix the request leaves NO live slot behind
    (pre-fix the slot survived — the loop-thread guard could not reach it)."""
    probe = {}

    @app.get("/__test_session_cleanup_probe")
    def _probe():  # plain def => FastAPI threadpool dispatch
        probe["slot_preexisting"] = fake_scoped.registry.has()
        fake_scoped.touch()
        return {"ok": True}

    try:
        with TestClient(app) as c:
            assert c.get("/__test_session_cleanup_probe").status_code == 200
            assert fake_scoped.slots == {}  # cleaned on the worker thread at request end
            assert c.get("/__test_session_cleanup_probe").status_code == 200
            # a reused worker thread must get a FRESH slot, not the last request's session
            assert probe["slot_preexisting"] is False
            assert fake_scoped.slots == {}
    finally:
        app.router.routes = [
            r for r in app.router.routes if getattr(r, "path", None) != "/__test_session_cleanup_probe"
        ]


def test_app_guard_no_longer_creates_throwaway_loop_session(fake_scoped):
    """Before the fix, EVERY context-less request made the loop-thread guard lazily
    create a session just to commit-and-remove it (blocking commit on the event loop).
    The fallback_session_active() gate removes that entirely for no-DB requests."""
    with TestClient(app) as c:
        assert c.get("/status").status_code == 200
    assert fake_scoped.events == []  # zero session lifecycle for a no-DB request
