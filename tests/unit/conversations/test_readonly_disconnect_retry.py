"""ReadOnlySqlTransaction (SQLSTATE 25006) classification + the scoped parent-close FOR UPDATE retry.

Two containment gaps: (1) 25006 is not a socket-level disconnect, so a poisoned connection would
go straight back to the pool and keep failing until pool_recycle; (2) without a bounded retry the
request 500s even when a healthy writer is one reconnect away. These tests pin the
classification predicate, the engine listener registration, and the retry helper's
safety guards (flag kill-switch, pending-mutation refusal, single-retry bound).
"""

import types

import pytest
from sqlalchemy import event
from sqlalchemy.exc import InternalError, OperationalError

from hs.conversations.store import _for_update_first_with_readonly_retry
from hs.infra import db as db_module

try:
    import psycopg2.errors

    ReadOnlySqlTransaction = psycopg2.errors.lookup("25006")
except Exception:  # pragma: no cover - psycopg2 is a hard dep in every env we test in
    ReadOnlySqlTransaction = None


def _readonly_exc():
    return ReadOnlySqlTransaction("cannot execute SELECT FOR UPDATE in a read-only transaction")


def _wrapped_readonly():
    # What SQLAlchemy actually raises at the call site: DBAPIError subclass with .orig.
    return InternalError("SELECT ... FOR UPDATE", {}, _readonly_exc())


def _wrapped_other():
    return OperationalError("SELECT 1", {}, Exception("connection refused"))


class TestIsReadonlyTxnError:
    def test_raw_psycopg2_error_matches(self):
        assert db_module.is_readonly_txn_error(_readonly_exc()) is True

    def test_sqlalchemy_wrapped_error_matches(self):
        assert db_module.is_readonly_txn_error(_wrapped_readonly()) is True

    def test_unrelated_dbapi_error_does_not_match(self):
        assert db_module.is_readonly_txn_error(_wrapped_other()) is False

    def test_none_and_plain_exception_do_not_match(self):
        assert db_module.is_readonly_txn_error(None) is False
        assert db_module.is_readonly_txn_error(Exception("boom")) is False

    def test_asyncpg_style_sqlstate_attribute_matches(self):
        exc = types.SimpleNamespace(sqlstate="25006", orig=None, __cause__=None)
        assert db_module.is_readonly_txn_error(exc) is True


class TestHandleErrorListener:
    def test_listener_is_registered_on_core_engine(self):
        assert event.contains(db_module.core_engine, "handle_error", db_module._readonly_txn_is_disconnect)

    def test_readonly_error_marks_disconnect(self):
        ctx = types.SimpleNamespace(original_exception=_wrapped_readonly(), is_disconnect=False)
        db_module._readonly_txn_is_disconnect(ctx)
        assert ctx.is_disconnect is True

    def test_other_errors_leave_disconnect_untouched(self):
        ctx = types.SimpleNamespace(original_exception=_wrapped_other(), is_disconnect=False)
        db_module._readonly_txn_is_disconnect(ctx)
        assert ctx.is_disconnect is False


class _FakeSession:
    def __init__(self):
        self.new = set()
        self.dirty = set()
        self.deleted = set()
        self.rollbacks = 0

    def rollback(self):
        self.rollbacks += 1


class _FakeQuery:
    """Scripted .first() outcomes: an Exception instance raises, anything else returns."""

    def __init__(self, outcomes):
        self._outcomes = list(outcomes)
        self.session = _FakeSession()
        self.calls = 0

    def first(self):
        self.calls += 1
        outcome = self._outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class TestForUpdateReadonlyRetry:
    def test_clean_first_attempt_never_rolls_back(self):
        q = _FakeQuery(["row"])
        assert _for_update_first_with_readonly_retry(q) == "row"
        assert q.calls == 1
        assert q.session.rollbacks == 0

    def test_readonly_failure_retries_once_after_rollback(self):
        q = _FakeQuery([_wrapped_readonly(), "row"])
        assert _for_update_first_with_readonly_retry(q) == "row"
        assert q.calls == 2
        assert q.session.rollbacks == 1

    def test_retry_is_bounded_to_one(self):
        q = _FakeQuery([_wrapped_readonly(), _wrapped_readonly()])
        with pytest.raises(InternalError):
            _for_update_first_with_readonly_retry(q)
        assert q.calls == 2
        assert q.session.rollbacks == 1

    def test_non_readonly_errors_propagate_immediately(self):
        q = _FakeQuery([_wrapped_other()])
        with pytest.raises(OperationalError):
            _for_update_first_with_readonly_retry(q)
        assert q.calls == 1
        assert q.session.rollbacks == 0

    def test_pending_orm_mutations_refuse_the_retry(self):
        q = _FakeQuery([_wrapped_readonly(), "row"])
        q.session.dirty = {object()}
        with pytest.raises(InternalError):
            _for_update_first_with_readonly_retry(q)
        assert q.calls == 1
        assert q.session.rollbacks == 0
