"""The conversation lock, taken through `conversations.lock` or `access.conversation`, serializes the writers of one
conversation and lets rows that only reference it be inserted while it is held. Taking it writes no row."""

import pytest
from sqlalchemy import insert, text
from sqlalchemy.exc import OperationalError

from hs import access, core
from hs import conversations as cv
from hs.access import Principal
from hs.core import T
from tests.unit.conversations.test_conversations import World

LOCK_NOT_AVAILABLE = "55P03"
LOCKS = {
    "conversations.lock": lambda db, w, cid: cv.lock(db, cid),
    "access.conversation": lambda db, w, cid: access.conversation(db, Principal(str(w.tenant), "key"), cid, lock=True),
}


def blocked(run) -> bool:
    """Whether `run(db)` waits on a lock another transaction holds; it gives up after 1 s."""
    try:
        with core.tx() as db:
            db.execute(text("SET LOCAL lock_timeout = '1s'"))
            run(db)
    except OperationalError as e:
        if e.orig.pgcode != LOCK_NOT_AVAILABLE:
            raise
        return True
    return False


@pytest.mark.parametrize("held", LOCKS)
@pytest.mark.parametrize("taken", LOCKS)
def test_a_held_conversation_lock_makes_a_second_lock_wait_but_not_an_entry_or_assignment_insert(held, taken):
    w = World()
    conv = w.open()
    written = text("SELECT xmin::text FROM conversation WHERE id = :id")
    with core.read() as db:
        before = db.execute(written, {"id": conv.id}).scalar_one()
    named = {"tenant_id": w.tenant, "conversation_id": conv.id}
    entry = insert(T.entry).values(**named, kind="note", body={"text": "Called back"})
    stint = insert(T.assignment).values(**named, role="primary", via="queue", channel="call", state="offered")
    with core.tx() as holder:
        LOCKS[held](holder, w, conv.id)
        assert blocked(lambda db: LOCKS[taken](db, w, conv.id))
        assert not blocked(lambda db: db.execute(entry))
        assert not blocked(lambda db: db.execute(stint))
    assert not blocked(lambda db: LOCKS[taken](db, w, conv.id))
    with core.read() as db:
        assert db.execute(written, {"id": conv.id}).scalar_one() == before, "a lock rewrites no row version"
