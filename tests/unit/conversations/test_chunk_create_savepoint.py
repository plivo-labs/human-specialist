"""create_chunk_with_related_data must not poison the request session.

The chunk side-write is best-effort (callers always continue), but a failed INSERT/flush
must not leave the shared request session ABORTED for the caller's next statements
("PendingRollbackError during commit"). The DB work runs inside a SAVEPOINT so the
swallow keeps the session healthy.
"""

from contextlib import contextmanager
from types import SimpleNamespace

from sqlalchemy.exc import IntegrityError

import hs.conversations.management as convmgmt
import tests.conftest  # noqa: F401


class FakeSession:
    def __init__(self):
        self.events = []
        self.depth = 0
        self.aborted = False  # models SQLAlchemy's post-failed-flush aborted state

    @contextmanager
    def begin_nested(self):
        self.events.append("savepoint")
        self.depth += 1
        try:
            yield
        except Exception:
            self.events.append("rollback")  # savepoint rollback keeps the session usable
            self.depth -= 1
            raise
        else:
            self.events.append("release")
            self.depth -= 1

    def add(self, _obj):
        self.events.append("add")

    def flush(self):
        self.events.append("flush")

    def commit(self):  # the thing that must NEVER fire mid-savepoint
        self.events.append("commit")


def _wire(monkeypatch, create_raises=None):
    fs = FakeSession()
    monkeypatch.setattr(convmgmt, "get_db_session", lambda: fs)
    monkeypatch.setattr(convmgmt.Conversation, "get_by_id", staticmethod(lambda _id: None))

    def _create(chunk):
        fs.events.append("create_chunk")
        if create_raises:
            if fs.depth == 0:  # a bare failure with no savepoint poisons the session
                fs.aborted = True
            raise create_raises

    monkeypatch.setattr(convmgmt.ConversationChunk, "create_chunk", staticmethod(_create))
    return fs


def _chunk(**kw):
    base = {"conversation": None, "type": "chat", "attributes": None, "recording": None, "description": None}
    base.update(kw)
    return SimpleNamespace(**base)


def test_failed_chunk_insert_rolls_back_savepoint_and_swallows(monkeypatch):
    fs = _wire(monkeypatch, create_raises=IntegrityError("INSERT", {}, Exception("duplicate key value (23505)")))
    # must NOT raise (best-effort semantics preserved)…
    convmgmt.create_chunk_with_related_data(_chunk(conversation=7))
    # …and the failure stays inside the savepoint: session usable, not aborted.
    assert fs.events == ["savepoint", "create_chunk", "rollback"]
    assert fs.aborted is False


def test_happy_path_releases_savepoint(monkeypatch):
    fs = _wire(monkeypatch)
    convmgmt.create_chunk_with_related_data(_chunk())
    assert fs.events == ["savepoint", "create_chunk", "release"]
    assert "commit" not in fs.events
