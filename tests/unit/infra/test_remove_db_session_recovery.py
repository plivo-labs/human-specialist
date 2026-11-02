"""remove_db_session() teardown must release the session when COMMIT itself dies.

A commit on a dead connection raises OperationalError after SQLAlchemy has moved the transaction
to PREPARED; if the thread-keyed scoped session stayed registered, the next request on that worker
thread would fail on its first statement ("This session is in 'prepared' state").

The first test reproduces the exact SQLAlchemy state with a real engine (sqlite, dialect
do_commit forced to fail) and pins the recovery; the rest pin the call contract on a fake.
"""

import sqlite3
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import InvalidRequestError, OperationalError, PendingRollbackError
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import StaticPool

import hs.infra.context as ctx
import tests.conftest  # noqa: F401


@pytest.fixture()
def clean_context():
    ctx.context_db_session.set(None)
    ctx.context_db_commit.set(True)
    yield
    ctx.context_db_session.set(None)
    ctx.context_db_commit.set(True)


@pytest.fixture()
def real_scoped(monkeypatch, clean_context):
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    scoped = scoped_session(sessionmaker(bind=engine))
    monkeypatch.setattr(ctx, "db_session", scoped)  # the module-level fallback remove_db_session() tears down
    yield engine, scoped
    scoped.remove()
    engine.dispose()


def _kill_commits(monkeypatch, engine):
    def _dead_commit(dbapi_connection):
        raise sqlite3.OperationalError("SSL connection has been closed unexpectedly")

    monkeypatch.setattr(engine.dialect, "do_commit", _dead_commit)


def test_dead_connection_at_commit_no_longer_poisons_the_thread_slot(real_scoped, monkeypatch):
    engine, scoped = real_scoped
    _kill_commits(monkeypatch, engine)

    # --- control: the SQLAlchemy state the prod worker thread was left in ---
    poisoned = scoped()
    assert poisoned.execute(text("select 1")).scalar() == 1  # request did SQL: txn + connection live
    with pytest.raises(OperationalError):
        poisoned.commit()  # dies AFTER the transaction is PREPARED
    with pytest.raises(InvalidRequestError, match="prepared"):
        poisoned.execute(text("select 1"))  # what the NEXT request hit pre-fix
    assert scoped.registry.has()  # ...because nothing cleared the thread slot
    scoped.remove()

    # --- the real sequence with the fix: request SQL, then teardown's commit is the one that dies ---
    request_session = scoped()
    assert request_session.execute(text("select 1")).scalar() == 1
    with pytest.raises(OperationalError):
        ctx.remove_db_session()  # the failed write still surfaces to the caller...
    assert not scoped.registry.has(), "poisoned session left registered on the thread"

    # --- ...and the next request on this thread gets a fresh, working session ---
    monkeypatch.undo()
    fresh = scoped()
    assert fresh is not request_session
    assert fresh.execute(text("select 1")).scalar() == 1


def test_teardown_via_threadpool_db_guard_clears_slot_after_dead_commit(real_scoped, monkeypatch):
    """Same failure through the real wrapper that runs teardown on the AnyIO worker thread."""
    engine, scoped = real_scoped

    def handler():
        scoped().execute(text("select 1"))
        _kill_commits(monkeypatch, engine)
        return "ok"

    with pytest.raises(OperationalError):
        ctx.threadpool_db_guard(handler)()
    assert not scoped.registry.has()


# ----------------------------------------------------------------- call-contract on a fake
class _FakeScoped:
    def __init__(self, commit_exc=None, rollback_exc=None, remove_exc=None):
        self.calls = []
        self.commit_exc, self.rollback_exc, self.remove_exc = commit_exc, rollback_exc, remove_exc
        self.registry = SimpleNamespace(clear=lambda: self.calls.append("registry.clear"), has=lambda: True)

    def commit(self):
        self.calls.append("commit")
        if self.commit_exc:
            raise self.commit_exc

    def rollback(self):
        self.calls.append("rollback")
        if self.rollback_exc:
            raise self.rollback_exc

    def remove(self):
        self.calls.append("remove")
        if self.remove_exc:
            raise self.remove_exc


def _op_err(msg="SSL connection has been closed unexpectedly"):
    return OperationalError("COMMIT", {}, Exception(msg))


@pytest.fixture()
def fake(monkeypatch, clean_context):
    def _make(**kw):
        f = _FakeScoped(**kw)
        monkeypatch.setattr(ctx, "db_session", f)
        return f

    return _make


def test_operational_error_at_commit_rolls_back_releases_and_reraises(fake):
    f = fake(commit_exc=_op_err())
    with pytest.raises(OperationalError):
        ctx.remove_db_session()
    assert f.calls == ["commit", "rollback", "remove"]


def test_pending_rollback_propagates_after_release(fake):
    """A flush failed earlier and the handler swallowed it: the old code logged and returned
    success for a write that never happened. get_db() always propagated this; now both do."""
    f = fake(commit_exc=PendingRollbackError("previous exception during flush"))
    with pytest.raises(PendingRollbackError):
        ctx.remove_db_session()
    assert f.calls == ["commit", "rollback", "remove"]


def test_clean_commit_path_unchanged(fake):
    f = fake()
    ctx.remove_db_session()
    assert f.calls == ["commit", "remove"]


def test_rollback_marker_path_unchanged(fake):
    f = fake()
    ctx.set_db_session_rollback()
    ctx.remove_db_session()
    assert f.calls == ["rollback", "remove"]


def test_rollback_failure_after_dead_commit_still_releases_and_reports_the_commit_error(fake):
    f = fake(commit_exc=_op_err("commit died"), rollback_exc=_op_err("rollback died too"))
    with pytest.raises(OperationalError, match="commit died"):
        ctx.remove_db_session()
    assert f.calls == ["commit", "rollback", "remove"]


def test_remove_failure_still_clears_the_thread_registry(fake):
    f = fake(remove_exc=RuntimeError("close blew up"))
    ctx.remove_db_session()  # teardown must not raise for a close() failure
    assert f.calls == ["commit", "remove", "registry.clear"]


def test_plain_session_without_remove_is_closed(clean_context):
    calls = []
    plain = SimpleNamespace(
        commit=lambda: calls.append("commit"),
        rollback=lambda: calls.append("rollback"),
        close=lambda: calls.append("close"),
    )
    ctx.context_db_session.set(plain)
    ctx.remove_db_session()
    assert calls == ["commit", "close"]
