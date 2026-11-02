"""active_queue_entry CRUD + state-machine data layer, against the rolled-back test Postgres.

Pins the write/read surface that reverse routing and enqueue sit on: the dual deletion marker
(is_deleted + deleted_at) moving in lockstep, the scoped-vs-unscoped status update split, and the
first-terminal-writer-wins conditional status overwrite.

Skips automatically when the test DB is unreachable (the ``db`` fixture).
"""

import logging
import uuid

import pytest

import tests.conftest  # noqa: F401 — eager data_adapter imports (AbstractConcreteBase registry)
from hs.conversations.store import ActiveQueueAgent, ActiveQueueEntry, ActiveQueueSkillsFilter, ActiveQueueTeam
from hs.infra.helpers import get_now
from hs.routing import active_queue as aq
from hs.routing import constants as rc

_AUTH = "TESTORG0AUTH0ID000000"


def _seed_entry(
    db,
    *,
    queue_uuid=None,
    contact_uuid=None,
    flow_run_uuid=None,
    qnr_run_uuid=None,
    queue_type="first-in-first-out",
    queue_channel=rc.CHANNEL_TASK,
    status=rc.CONTACT_STATE_QUEUED,
    queue_priority=0,
    version=0,
    is_deleted=False,
    deleted_at=None,
    created_at=None,
    auth_id=_AUTH,
):
    """Insert one active_queue_entry row directly (NOT via create_active_queue_entry) so the CRUD
    functions can be exercised against a known fixture. Returns the persisted ORM row."""
    entry = ActiveQueueEntry(
        uuid=uuid.uuid4(),
        queue_priority=queue_priority,
        queue_uuid=queue_uuid or uuid.uuid4(),
        contact_uuid=contact_uuid or ("c-" + uuid.uuid4().hex),
        flow_run_uuid=flow_run_uuid or uuid.uuid4(),
        qnr_run_uuid=qnr_run_uuid or ("qnr-" + uuid.uuid4().hex),
        queue_type=queue_type,
        queue_channel=queue_channel,
        contact_phone_or_email="+15550001111",
        status=status,
        version=version,
        auth_id=auth_id,
        is_deleted=is_deleted,
        deleted_at=deleted_at,
    )
    if created_at is not None:
        entry.created_at = created_at
    db.add(entry)
    db.flush()
    return entry


def _row_state(db, entry_id):
    """Fresh (identity-map-bypassing) column read of the post-update row state."""
    return (
        db.query(ActiveQueueEntry.status, ActiveQueueEntry.is_deleted, ActiveQueueEntry.deleted_at)
        .filter(ActiveQueueEntry.id == entry_id)
        .first()
    )


# =====================================================================================
# child model schema
# =====================================================================================
def test_child_models_schema():
    for cls, tbl, dom in (
        (ActiveQueueTeam, "teams", "team_id"),
        (ActiveQueueSkillsFilter, "skills_filter", "skill_id"),
        (ActiveQueueAgent, "agents", "agent_id"),
    ):
        cols = set(cls.__table__.columns.keys())
        assert cls.__tablename__ == tbl
        # CoreDBBase + the soft-delete deleted_at marker + the FK column + the domain column(s)
        for c in ("id", "uuid", "created_at", "updated_at", "is_deleted", "deleted_at", "active_queue_entry_id", dom):
            assert c in cols, (tbl, c)
    sf = set(ActiveQueueSkillsFilter.__table__.columns.keys())
    assert "operator" in sf and "value" in sf


# =====================================================================================
# create_active_queue_entry — parent + cascaded children
# =====================================================================================
def test_create_active_queue_entry_persists_parent_and_children(db):
    entry = ActiveQueueEntry(
        uuid=uuid.uuid4(),
        queue_priority=2,
        queue_uuid=uuid.uuid4(),
        contact_uuid="c-create",
        flow_run_uuid=uuid.uuid4(),
        qnr_run_uuid="qnr-create",
        queue_type="first-in-first-out",
        queue_channel=rc.CHANNEL_TASK,
        contact_phone_or_email="+1999",
        status=rc.CONTACT_STATE_IN_TRANSIT,
        auth_id=_AUTH,
        is_deleted=False,
    )
    new_id = aq.create_active_queue_entry(
        db, entry, teams=["team-a", "team-b"], skills_filter=[("skill-x", ">", 3)], agents=["agent-1"]
    )
    assert new_id == entry.id and new_id is not None
    # version DB default 0; status persisted as the caller-set IN_TRANSIT (NOT the 'QUEUED' default)
    saved = db.query(ActiveQueueEntry.status, ActiveQueueEntry.version).filter(ActiveQueueEntry.id == new_id).first()
    assert saved.status == rc.CONTACT_STATE_IN_TRANSIT
    assert saved.version == 0
    # children linked by active_queue_entry_id
    teams = db.query(ActiveQueueTeam).filter(ActiveQueueTeam.active_queue_entry_id == new_id).all()
    skills = db.query(ActiveQueueSkillsFilter).filter(ActiveQueueSkillsFilter.active_queue_entry_id == new_id).all()
    agents = db.query(ActiveQueueAgent).filter(ActiveQueueAgent.active_queue_entry_id == new_id).all()
    assert sorted(t.team_id for t in teams) == ["team-a", "team-b"]
    assert len(skills) == 1 and skills[0].operator == ">" and skills[0].value == 3
    assert [a.agent_id for a in agents] == ["agent-1"]


def test_create_active_queue_entry_no_children(db):
    entry = ActiveQueueEntry(
        uuid=uuid.uuid4(),
        queue_priority=0,
        queue_uuid=uuid.uuid4(),
        contact_uuid="c-bare",
        flow_run_uuid=uuid.uuid4(),
        qnr_run_uuid="qnr-bare",
        queue_type="first-in-first-out",
        queue_channel=rc.CHANNEL_EMAIL,
        contact_phone_or_email="a@example.com",
        status=rc.CONTACT_STATE_IN_TRANSIT,
        auth_id=_AUTH,
        is_deleted=False,
    )
    new_id = aq.create_active_queue_entry(db, entry)
    assert new_id is not None
    assert db.query(ActiveQueueTeam).filter(ActiveQueueTeam.active_queue_entry_id == new_id).count() == 0


# =====================================================================================
# get_queue_capacity — per-channel non-deleted count
# =====================================================================================
def test_get_queue_capacity_counts_non_deleted_per_channel(db):
    q = uuid.uuid4()
    _seed_entry(db, queue_uuid=q, queue_channel=rc.CHANNEL_TASK, status=rc.CONTACT_STATE_QUEUED)
    _seed_entry(db, queue_uuid=q, queue_channel=rc.CHANNEL_TASK, status=rc.CONTACT_STATE_IN_TRANSIT)
    _seed_entry(db, queue_uuid=q, queue_channel=rc.CHANNEL_TASK, status=rc.CONTACT_STATE_IN_REVERSE_ROUTING)
    # soft-deleted (both markers) — excluded
    _seed_entry(
        db,
        queue_uuid=q,
        queue_channel=rc.CHANNEL_TASK,
        status=rc.CONTACT_STATE_ACCEPTED,
        is_deleted=True,
        deleted_at=get_now(),
    )
    # other channel — excluded
    _seed_entry(db, queue_uuid=q, queue_channel=rc.CHANNEL_CHAT, status=rc.CONTACT_STATE_QUEUED)
    # other queue — excluded
    _seed_entry(db, queue_uuid=uuid.uuid4(), queue_channel=rc.CHANNEL_TASK, status=rc.CONTACT_STATE_QUEUED)
    assert aq.get_queue_capacity(db, q, rc.CHANNEL_TASK) == 3
    assert aq.get_queue_capacity(db, q, rc.CHANNEL_CHAT) == 1


# =====================================================================================
# update_queue_status — SCOPED (in-flight) vs Unscoped (terminal)
# =====================================================================================
def test_update_queue_status_scoped_skips_soft_deleted(db):
    # a SCOPED status (Queued) must NOT touch a soft-deleted row (deleted_at IS NULL)
    e = _seed_entry(
        db,
        contact_uuid="c-up",
        qnr_run_uuid="qnr-up",
        status=rc.CONTACT_STATE_ACCEPTED,
        is_deleted=True,
        deleted_at=get_now(),
    )
    aq.update_queue_status(db, "c-up", "qnr-up", rc.CONTACT_STATE_QUEUED)
    st = _row_state(db, e.id)
    assert st.status == rc.CONTACT_STATE_ACCEPTED  # untouched (scoped skipped the deleted row)


def test_update_queue_status_scoped_updates_live(db):
    e = _seed_entry(db, contact_uuid="c-up2", qnr_run_uuid="qnr-up2", status=rc.CONTACT_STATE_IN_REVERSE_ROUTING)
    aq.update_queue_status(db, "c-up2", "qnr-up2", rc.CONTACT_STATE_QUEUED)
    assert _row_state(db, e.id).status == rc.CONTACT_STATE_QUEUED


def test_update_queue_status_strict_reports_no_live_row(db):
    err = aq.update_queue_status(db, "missing-contact", "missing-qnr", rc.CONTACT_STATE_QUEUED, strict=True)
    assert err == aq.QUEUE_STATUS_NO_LIVE_ROW


def test_update_queue_status_updates_all_duplicate_live_rows(db):
    # Duplicate live rows can exist: the update covers all of them rather than leaving one stale row.
    first = _seed_entry(
        db, contact_uuid="c-duplicate", qnr_run_uuid="qnr-duplicate", status=rc.CONTACT_STATE_IN_TRANSIT
    )
    second = _seed_entry(
        db, contact_uuid="c-duplicate", qnr_run_uuid="qnr-duplicate", status=rc.CONTACT_STATE_IN_REVERSE_ROUTING
    )

    err = aq.update_queue_status(db, "c-duplicate", "qnr-duplicate", rc.CONTACT_STATE_QUEUED, strict=True)

    assert err is None
    assert _row_state(db, first.id).status == rc.CONTACT_STATE_QUEUED
    assert _row_state(db, second.id).status == rc.CONTACT_STATE_QUEUED


class _FakeNestedTransaction:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False


class _AtomicUpdateQuery:
    def __init__(self, rowcount=1, error=None):
        self.rowcount = rowcount
        self.error = error
        self.filter_calls = 0
        self.update_calls = 0

    def filter(self, *conditions):
        self.filter_calls += 1
        return self

    def update(self, values, synchronize_session=False):
        self.update_calls += 1
        if self.error is not None:
            raise self.error
        return self.rowcount


class _AtomicUpdateSession:
    def __init__(self, query):
        self._query = query
        self.query_calls = 0
        self.nested_calls = 0

    def begin_nested(self):
        self.nested_calls += 1
        return _FakeNestedTransaction()

    def query(self, model):
        self.query_calls += 1
        return self._query


@pytest.mark.parametrize(("rowcount", "expected"), ((0, aq.QUEUE_STATUS_NO_LIVE_ROW), (1, None), (2, None)))
def test_update_queue_status_strict_atomic_rowcount_contract(rowcount, expected):
    query = _AtomicUpdateQuery(rowcount=rowcount)
    session = _AtomicUpdateSession(query)

    result = aq.update_queue_status(session, "contact", "qnr", rc.CONTACT_STATE_QUEUED, strict=True)

    assert result == expected
    assert session.query_calls == 1
    assert session.nested_calls == 1
    assert query.update_calls == 1


def test_update_queue_status_strict_database_exception_is_retryable():
    query = _AtomicUpdateQuery(error=RuntimeError("database unavailable"))
    session = _AtomicUpdateSession(query)

    result = aq.update_queue_status(session, "contact", "qnr", rc.CONTACT_STATE_QUEUED, strict=True)

    assert result == aq.QUEUE_STATUS_PERSISTENCE_RETRYABLE
    assert session.query_calls == 1
    assert query.update_calls == 1


@pytest.mark.parametrize(
    "query", (_AtomicUpdateQuery(rowcount=0), _AtomicUpdateQuery(error=RuntimeError("database unavailable")))
)
def test_update_queue_status_non_strict_preserves_best_effort_contract(query):
    session = _AtomicUpdateSession(query)

    result = aq.update_queue_status(session, "contact", "qnr", rc.CONTACT_STATE_QUEUED)

    assert result is None
    assert session.query_calls == 1
    assert query.update_calls == 1


def test_update_queue_status_terminal_is_unscoped(db):
    # a terminal status (FAILED) is Unscoped → it DOES touch a soft-deleted row
    e = _seed_entry(
        db,
        contact_uuid="c-up3",
        qnr_run_uuid="qnr-up3",
        status=rc.CONTACT_STATE_ACCEPTED,
        is_deleted=True,
        deleted_at=get_now(),
    )
    aq.update_queue_status(db, "c-up3", "qnr-up3", rc.CONTACT_STATE_FAILED)
    assert _row_state(db, e.id).status == rc.CONTACT_STATE_FAILED


# =====================================================================================
# delete_active_queue_entry — soft-delete both markers + conditional status
# =====================================================================================
def test_delete_active_queue_entry_soft_deletes_and_overwrites_in_flight_status(db):
    e = _seed_entry(db, contact_uuid="c-del", qnr_run_uuid="qnr-del", status=rc.CONTACT_STATE_IN_REVERSE_ROUTING)
    rid = aq.delete_active_queue_entry(db, "c-del", "qnr-del", rc.CONTACT_STATE_ACCEPTED)
    assert rid == e.id
    st = _row_state(db, e.id)
    assert st.is_deleted is True and st.deleted_at is not None  # BOTH markers stamped
    assert st.status == rc.CONTACT_STATE_ACCEPTED  # in-flight → overwritten to terminal


def test_delete_active_queue_entry_does_not_relabel_terminal(db):
    # an already-terminal row keeps its prior terminal status (first terminal writer wins)
    e = _seed_entry(db, contact_uuid="c-del2", qnr_run_uuid="qnr-del2", status=rc.CONTACT_STATE_ACCEPTED)
    rid = aq.delete_active_queue_entry(db, "c-del2", "qnr-del2", rc.CONTACT_STATE_TIMEDOUT)
    assert rid == e.id
    st = _row_state(db, e.id)
    assert st.is_deleted is True and st.deleted_at is not None
    assert st.status == rc.CONTACT_STATE_ACCEPTED  # NOT relabeled to TIMEDOUT


def test_delete_active_queue_entry_no_live_row_returns_zero_without_an_error(db, caplog):
    assert aq.delete_active_queue_entry(db, "nope", "nope", rc.CONTACT_STATE_FAILED) == 0
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


# =====================================================================================
# lookups
# =====================================================================================
def test_get_current_in_flight_only(db):
    c = "c-cur"
    _seed_entry(db, contact_uuid=c, qnr_run_uuid="q1", status=rc.CONTACT_STATE_QUEUED)
    got = aq.get_current_active_queue_entry_by_contact_uuid(db, c)
    assert got is not None and got.status == rc.CONTACT_STATE_QUEUED
    # a terminal/soft-deleted-only contact has no "current" entry
    c2 = "c-cur2"
    _seed_entry(
        db, contact_uuid=c2, qnr_run_uuid="q2", status=rc.CONTACT_STATE_ACCEPTED, is_deleted=True, deleted_at=get_now()
    )
    assert aq.get_current_active_queue_entry_by_contact_uuid(db, c2) is None


def test_get_current_for_a_run_ignores_a_newer_runs_entry(db):
    c = "c-runs"
    _seed_entry(
        db, contact_uuid=c, qnr_run_uuid="old", status=rc.CONTACT_STATE_ACCEPTED, is_deleted=True, deleted_at=get_now()
    )
    _seed_entry(db, contact_uuid=c, qnr_run_uuid="new", status=rc.CONTACT_STATE_QUEUED)
    assert aq.get_current_active_queue_entry_by_contact_uuid(db, c, "old") is None
    assert aq.get_current_active_queue_entry_by_contact_uuid(db, c, "new").qnr_run_uuid == "new"
    assert aq.get_current_active_queue_entry_by_contact_uuid(db, c).qnr_run_uuid == "new"


# =====================================================================================
# get_last_n — call-channel ONLY anchor + accepted window + in-queue count
# =====================================================================================
def test_get_last_n_call_only_and_in_queue_count_fifo(db):
    import datetime as _dt

    q = uuid.uuid4()
    anchor_run = "qnr-anchor"
    t0 = get_now()
    # anchor (call, FIFO)
    _seed_entry(
        db,
        queue_uuid=q,
        qnr_run_uuid=anchor_run,
        queue_channel=rc.CHANNEL_CALL,
        queue_type="first-in-first-out",
        status=rc.CONTACT_STATE_QUEUED,
        created_at=t0,
    )
    # accepted historical (call, within window, before anchor)
    _seed_entry(
        db,
        queue_uuid=q,
        qnr_run_uuid="acc1",
        queue_channel=rc.CHANNEL_CALL,
        status=rc.CONTACT_STATE_ACCEPTED,
        created_at=t0 - _dt.timedelta(minutes=30),
    )
    _seed_entry(
        db,
        queue_uuid=q,
        qnr_run_uuid="acc2",
        queue_channel=rc.CHANNEL_CALL,
        status=rc.CONTACT_STATE_ACCEPTED,
        created_at=t0 - _dt.timedelta(minutes=10),
    )
    # in-queue ahead (FIFO counts created_at <= anchor): one earlier QUEUED call contact
    _seed_entry(
        db,
        queue_uuid=q,
        qnr_run_uuid="iq1",
        queue_channel=rc.CHANNEL_CALL,
        status=rc.CONTACT_STATE_QUEUED,
        created_at=t0 - _dt.timedelta(minutes=5),
    )
    # a later contact (created AFTER anchor) must NOT count for FIFO
    _seed_entry(
        db,
        queue_uuid=q,
        qnr_run_uuid="iq2",
        queue_channel=rc.CHANNEL_CALL,
        status=rc.CONTACT_STATE_QUEUED,
        created_at=t0 + _dt.timedelta(minutes=5),
    )
    # a non-call contact must be ignored entirely
    _seed_entry(
        db,
        queue_uuid=q,
        qnr_run_uuid="chatx",
        queue_channel=rc.CHANNEL_CHAT,
        status=rc.CONTACT_STATE_QUEUED,
        created_at=t0,
    )

    accepted, in_queue = aq.get_last_n_active_queue_entries_for_queue_uuid(
        db, q, anchor_run, interval_hours=3, max_entries=10
    )
    assert len(accepted) == 2  # both ACCEPTED call rows in window
    # accepted ordered created_at DESC → acc2 (newer) first
    assert accepted[0].qnr_run_uuid == "acc2"
    # FIFO in-queue: created_at <= anchor → anchor itself + iq1 = 2 (iq2 after anchor excluded)
    assert in_queue == 2


def test_get_last_n_no_call_anchor_returns_empty(db):
    q = uuid.uuid4()
    _seed_entry(
        db, queue_uuid=q, qnr_run_uuid="onlychat", queue_channel=rc.CHANNEL_CHAT, status=rc.CONTACT_STATE_QUEUED
    )
    accepted, in_queue = aq.get_last_n_active_queue_entries_for_queue_uuid(
        db, q, "onlychat", interval_hours=3, max_entries=10
    )
    assert accepted == [] and in_queue == 0


def test_get_last_n_lifo_in_queue_counts_created_at_ge_anchor(db):
    # LIFO branch (queue_type='last-in-first-out'): in-queue counts created_at >= anchor (mirror of the
    # FIFO <= test). Pins active_queue.py's LIFO/FIFO branch + boundary direction.
    import datetime as _dt

    q = uuid.uuid4()
    t0 = get_now()
    _seed_entry(
        db,
        queue_uuid=q,
        qnr_run_uuid="lifo-anchor",
        queue_channel=rc.CHANNEL_CALL,
        queue_type="last-in-first-out",
        status=rc.CONTACT_STATE_QUEUED,
        created_at=t0,
    )
    # earlier contact (created BEFORE anchor) → excluded for LIFO (>= anchor)
    _seed_entry(
        db,
        queue_uuid=q,
        qnr_run_uuid="lbefore",
        queue_channel=rc.CHANNEL_CALL,
        status=rc.CONTACT_STATE_QUEUED,
        created_at=t0 - _dt.timedelta(minutes=5),
    )
    # later contact (created AFTER anchor) → included for LIFO
    _seed_entry(
        db,
        queue_uuid=q,
        qnr_run_uuid="lafter",
        queue_channel=rc.CHANNEL_CALL,
        status=rc.CONTACT_STATE_IN_TRANSIT,
        created_at=t0 + _dt.timedelta(minutes=5),
    )
    _, in_queue = aq.get_last_n_active_queue_entries_for_queue_uuid(
        db, q, "lifo-anchor", interval_hours=3, max_entries=10
    )
    assert in_queue == 2  # anchor + lafter (created_at >= anchor); lbefore excluded


# --- first-match ORDER BY id ASC tie-break (lowest-id row on a multi-match predicate) --------------
def test_get_current_returns_lowest_id_on_multiple_in_flight(db):
    c = "c-cur-multi"
    first = _seed_entry(db, contact_uuid=c, qnr_run_uuid="qm1", status=rc.CONTACT_STATE_QUEUED)
    _seed_entry(db, contact_uuid=c, qnr_run_uuid="qm2", status=rc.CONTACT_STATE_IN_TRANSIT)
    got = aq.get_current_active_queue_entry_by_contact_uuid(db, c)
    assert got is not None and got.id == first.id  # lowest id, not an arbitrary row
