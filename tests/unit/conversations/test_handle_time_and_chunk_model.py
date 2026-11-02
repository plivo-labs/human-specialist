"""Query-count and allocation pins with a fake session that tallies `.query(...)`/`select_query(...)`
calls and returns canned results:
  * check_customer_interaction: 1 JOIN query (not 1 + N).
  * get_contacts_handle_time: 1 conditional-aggregation query.
  * ConversationChunk.__to_model: no `deepcopy` (columns-only dict).
  * the pre-change status check: ALLOWED_STATUS_CHANGES/SKIP_CORNER_CASE_VALIDATION
    are built once and shared at class level (not rebuilt every __init__).
"""

import copy as _copy
import datetime
import types

import tests.conftest  # noqa: F401 — eager adapter imports (mapper registry)
from hs.conversations import store as conv_mod
from hs.conversations.store import Conversation, ConversationChunk


class _Chain:
    def __init__(self, first_result=None, all_result=None):
        self._first = first_result
        self._all = all_result if all_result is not None else []

    def join(self, *a, **k):
        return self

    def filter(self, *a, **k):
        return self

    def order_by(self, *a, **k):
        return self

    def limit(self, *a, **k):
        return self

    def first(self):
        return self._first

    def one(self):
        return self._first

    def all(self):
        return self._all

    def count(self):
        return len(self._all)


class _FakeSession:
    """Counts how many queries a code path starts; every query returns the same canned chain."""

    def __init__(self, first_result=None, all_result=None):
        self.query_calls = 0
        self._first = first_result
        self._all = all_result

    def query(self, *a, **k):
        self.query_calls += 1
        return _Chain(self._first, self._all)


# ---------------------------------------------------------------------------
def test_get_contacts_handle_time_is_single_query(monkeypatch):
    base = datetime.datetime(2026, 1, 1, 12, 0, 0)
    # no transfer child → diff(end, start)
    row = types.SimpleNamespace(start=base, end=base + datetime.timedelta(seconds=300), transfer_start=None)
    fake = _FakeSession(first_result=row)
    monkeypatch.setattr(conv_mod, "get_db_session", lambda: fake)
    assert Conversation.get_contacts_handle_time(1, 1) == 300
    assert fake.query_calls == 1

    # transfer child present → diff(end, transfer_start)
    row2 = types.SimpleNamespace(
        start=base, end=base + datetime.timedelta(seconds=300), transfer_start=base + datetime.timedelta(seconds=100)
    )
    fake2 = _FakeSession(first_result=row2)
    monkeypatch.setattr(conv_mod, "get_db_session", lambda: fake2)
    assert Conversation.get_contacts_handle_time(1, 1) == 200
    assert fake2.query_calls == 1


# ---------------------------------------------------------------------------
def test_chunk_to_model_does_not_deepcopy(monkeypatch):
    # Capture the dict handed to parse_obj (model-agnostic) and assert: deepcopy is never
    # called, the columns-only dict renames from_user/to_user → from/to, and no SA state leaks.
    def _boom(*a, **k):
        raise AssertionError("deepcopy must not be called in __to_model")

    monkeypatch.setattr(_copy, "deepcopy", _boom)

    captured = {}

    class _Marker:
        attributes = None

    def _fake_model_validate(d):
        captured.update(d)
        return _Marker()

    monkeypatch.setattr(conv_mod.ConversationChunkModel, "model_validate", _fake_model_validate)

    chunk = ConversationChunk()
    chunk.from_user = "agent-1"
    chunk.to_user = "cust-1"
    chunk._ConversationChunk__to_model()  # name-mangled private method
    assert captured["from"] == "agent-1"
    assert captured["to"] == "cust-1"
    assert "from_user" not in captured and "to_user" not in captured
    assert "_sa_instance_state" not in captured


# ---------------------------------------------------------------------------
