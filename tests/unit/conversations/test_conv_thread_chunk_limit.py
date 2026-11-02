"""Regression guard for the conversation-thread read hot path.

The chunk fetch ``ConversationChunk.get_conversation_chunks_by_conv_number`` used to ``.all()``
the entire thread with no LIMIT whenever no time window was supplied. The route's default is
``start=0, end=0`` (both falsy), so the common "open a conversation" read took the
unbounded path. The rewrite caps the no-window fetch at ``DEFAULT_CONVERSATION_CHUNK_LIMIT``
(most-recent-first, ORDER BY timestamp DESC) while leaving explicit windows untouched and
letting an explicit caller ``limit`` win.

These tests don't need a live DB: the SQLAlchemy query chain is replaced by a spy that records
whether/which ``.limit()`` is applied, so they pin the exact guard-rail contract.
"""

import pytest

import tests.conftest  # noqa: F401 — eager adapter imports (AbstractConcreteBase registry)
from hs.conversations import store as conv_mod
from hs.conversations.store import DEFAULT_CONVERSATION_CHUNK_LIMIT, ConversationChunk


class _ChainSpy:
    """Chainable stand-in for a SQLAlchemy Query.

    Every builder method (join/filter/order_by/limit/...) returns ``self`` so the repo's fluent
    chain is preserved; ``limit`` and ``order_by`` calls are recorded; ``all()`` returns ``[]``.
    """

    def __init__(self, recorder):
        self._rec = recorder

    def __getattr__(self, name):
        def _method(*args, **kwargs):
            if name == "limit":
                self._rec["limit_calls"].append(args[0] if args else kwargs.get("limit"))
            elif name == "order_by":
                self._rec["order_by_calls"] += 1
            return self

        return _method

    def all(self):
        return []


@pytest.fixture
def spy(monkeypatch):
    rec = {"limit_calls": [], "order_by_calls": 0}
    chain = _ChainSpy(rec)
    monkeypatch.setattr(conv_mod, "get_db_session", lambda: object())
    monkeypatch.setattr(ConversationChunk, "select_query", staticmethod(lambda *a, **k: chain))
    return rec


def test_no_window_applies_default_cap(spy):
    # Route default: start=0/end=0 (both falsy) -> previously an unbounded full-thread .all().
    ConversationChunk.get_conversation_chunks_by_conv_number("1000001", start=0, end=0)
    assert spy["limit_calls"] == [DEFAULT_CONVERSATION_CHUNK_LIMIT]
    assert spy["order_by_calls"] == 1  # ORDER BY timestamp DESC preserved


def test_no_window_via_none_also_capped(spy):
    # The other no-window shape (defaults None/None) must be capped too.
    ConversationChunk.get_conversation_chunks_by_conv_number("1000001")
    assert spy["limit_calls"] == [DEFAULT_CONVERSATION_CHUNK_LIMIT]


def test_explicit_window_is_not_capped(spy):
    # Explicit [start, end] window keeps its exact prior behavior: no default LIMIT.
    ConversationChunk.get_conversation_chunks_by_conv_number("1000001", start=1700000000000, end=1700000600000)
    assert spy["limit_calls"] == []
    assert spy["order_by_calls"] == 1


def test_start_only_window_is_not_capped(spy):
    ConversationChunk.get_conversation_chunks_by_conv_number("1000001", start=1700000000000, end=0)
    assert spy["limit_calls"] == []


def test_end_only_window_is_not_capped(spy):
    ConversationChunk.get_conversation_chunks_by_conv_number("1000001", start=0, end=1700000600000)
    assert spy["limit_calls"] == []


def test_caller_limit_overrides_on_no_window(spy):
    ConversationChunk.get_conversation_chunks_by_conv_number("1000001", start=0, end=0, limit=5)
    assert spy["limit_calls"] == [5]


def test_caller_limit_applies_even_with_explicit_window(spy):
    ConversationChunk.get_conversation_chunks_by_conv_number("1000001", start=1700000000000, end=1700000600000, limit=5)
    assert spy["limit_calls"] == [5]
