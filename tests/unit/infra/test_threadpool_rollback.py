"""threadpool_db_guard honors a rollback marker set inside the copied threadpool context: without it, the
loop-side teardown would commit the poisoned transaction."""

import contextvars
import threading

import pytest

from hs.infra import context as cm

# ---------------------------------------------------------------------------------
# Unit pins: each layer in isolation, with the exact copy_context()+thread mechanics
# anyio's to_thread.run_sync applies to a plain-`def` endpoint.
# ---------------------------------------------------------------------------------


class _FakeSession:
    def __init__(self):
        self.rollback_threads = []

    def rollback(self):
        self.rollback_threads.append(threading.get_ident())


def _run_in_copied_context_on_thread(fn):
    """What run_in_threadpool does: run fn inside a COPY of the caller's context, on
    another thread. Marker writes inside fn cannot propagate back to the caller."""
    ctx = contextvars.copy_context()
    out = {}

    def _target():
        try:
            out["result"] = ctx.run(fn)
        except BaseException as e:
            out["error"] = e

    worker = threading.Thread(target=_target)
    worker.start()
    worker.join()
    out["worker_ident"] = worker.ident
    return out


@pytest.fixture()
def _contextvar_session():
    fake = _FakeSession()
    tok_session = cm.context_db_session.set(fake)
    tok_commit = cm.context_db_commit.set(True)
    try:
        yield fake
    finally:
        cm.context_db_session.reset(tok_session)
        cm.context_db_commit.reset(tok_commit)


def test_threadpool_db_guard_honors_marker_set_in_copied_context(_contextvar_session):
    fake = _contextvar_session

    def _body():
        cm.set_db_session_rollback()
        return "ok"

    out = _run_in_copied_context_on_thread(cm.threadpool_db_guard(_body))
    assert out.get("result") == "ok"
    # The caller's context never sees the marker (the trap itself): loop-side teardown
    # would commit. The guard must therefore have rolled back on the worker thread.
    assert cm.should_commit_db_session() is True
    assert fake.rollback_threads == [out["worker_ident"]]


def test_threadpool_db_guard_rolls_back_marked_session_when_body_raises(_contextvar_session):
    fake = _contextvar_session

    def _body():
        cm.set_db_session_rollback()
        raise RuntimeError("boom")

    out = _run_in_copied_context_on_thread(cm.threadpool_db_guard(_body))
    assert isinstance(out.get("error"), RuntimeError)
    assert fake.rollback_threads == [out["worker_ident"]]


def test_threadpool_db_guard_leaves_unmarked_session_alone(_contextvar_session):
    fake = _contextvar_session
    out = _run_in_copied_context_on_thread(cm.threadpool_db_guard(lambda: "ok"))
    assert out.get("result") == "ok"
    assert fake.rollback_threads == []
