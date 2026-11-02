"""get_best_contact version-CAS pick + check_if_valid_contact.

Row-level Postgres bar (the ``db`` fixture). Pins the reverse-routing pick: FIFO-then-LIFO ordered
scans (queue_priority ASC always; created_at ASC/DESC, id as tie-break), the team/agent membership gate
+ skill filter evaluated in SQL before LIMIT 1, the optimistic-lock CAS flip to IN_REVERSE_ROUTING
(version+1), and the
double-pick guard (check_if_valid_contact + the version conflict). Skips when the _test DB is unreachable.
"""

import datetime as dt
import uuid

import tests.conftest  # noqa: F401 — eager data_adapter imports (AbstractConcreteBase registry)
from hs.conversations.store import ActiveQueueAgent, ActiveQueueEntry, ActiveQueueSkillsFilter, ActiveQueueTeam
from hs.infra.helpers import get_now
from hs.routing import active_queue as aq
from hs.routing import constants as rc
from hs.routing.account_read import AgentSkill

_AUTH = "TESTORG0AUTH0ID000000"
AGENT = "aom-" + uuid.uuid4().hex  # the routing agent's aom UUID (get_best_contact's agent_id)


def _entry(
    *,
    queue_type="first-in-first-out",
    queue_channel=rc.CHANNEL_TASK,
    status=rc.CONTACT_STATE_QUEUED,
    queue_priority=0,
    version=0,
    created_at=None,
    queue_uuid=None,
    contact_uuid=None,
):
    entry = ActiveQueueEntry(
        uuid=uuid.uuid4(),
        queue_priority=queue_priority,
        queue_uuid=queue_uuid or uuid.uuid4(),
        contact_uuid=contact_uuid or ("c-" + uuid.uuid4().hex),
        flow_run_uuid=uuid.uuid4(),
        qnr_run_uuid="qnr-" + uuid.uuid4().hex,
        queue_type=queue_type,
        queue_channel=queue_channel,
        contact_phone_or_email="+15550001111",
        status=status,
        version=version,
        auth_id=_AUTH,
        is_deleted=False,
    )
    if created_at is not None:
        entry.created_at = created_at
    return entry


def _seed_entry(db, **kw):
    entry = _entry(**kw)
    db.add(entry)
    db.flush()
    return entry


def _seed_backlog(db, n, *, queue_type="first-in-first-out", agent_id="aom-other", skill=None):
    """n QUEUED rows in creation order, each targeted at agent_id, optionally all behind one skill filter."""
    base = get_now()
    entries = [_entry(queue_type=queue_type, created_at=base + dt.timedelta(seconds=i)) for i in range(n)]
    db.add_all(entries)
    db.flush()
    db.add_all(ActiveQueueAgent(agent_id=agent_id, active_queue_entry_id=e.id) for e in entries)
    if skill:
        skill_id, operator, value = skill
        db.add_all(
            ActiveQueueSkillsFilter(skill_id=skill_id, operator=operator, value=value, active_queue_entry_id=e.id)
            for e in entries
        )
    db.flush()
    return entries


def _target_agent(db, entry, agent_id=AGENT):
    db.add(ActiveQueueAgent(agent_id=agent_id, active_queue_entry_id=entry.id))
    db.flush()


def _target_team(db, entry, team_id):
    db.add(ActiveQueueTeam(team_id=team_id, active_queue_entry_id=entry.id))
    db.flush()


def _add_skill(db, entry, skill_id, operator, value):
    db.add(ActiveQueueSkillsFilter(skill_id=skill_id, operator=operator, value=value, active_queue_entry_id=entry.id))
    db.flush()


class _Carrier:
    """A minimal (.id, .version) stand-in for the entry get_best_contact returns to check_if_valid_contact."""

    def __init__(self, id_, version):
        self.id = id_
        self.version = version


# =====================================================================================
# check_if_valid_contact
# =====================================================================================
def test_check_if_valid_contact_version(db):
    e = _seed_entry(db, version=3)
    assert aq.check_if_valid_contact(db, _Carrier(e.id, 3)) is True
    assert aq.check_if_valid_contact(db, _Carrier(e.id, 4)) is False  # version bumped by a racer


def test_check_if_valid_contact_false_when_soft_deleted(db):
    e = _seed_entry(db, version=2, status=rc.CONTACT_STATE_IN_REVERSE_ROUTING)
    aq.delete_active_queue_entry(db, e.contact_uuid, e.qnr_run_uuid, rc.CONTACT_STATE_ACCEPTED)
    assert aq.check_if_valid_contact(db, _Carrier(e.id, 2)) is False  # deleted_at IS NULL fails


# =====================================================================================
# get_best_contact — pick + CAS
# =====================================================================================
def test_picks_agent_targeted_and_cas_flip(db):
    e = _seed_entry(db, version=0)
    _target_agent(db, e)
    contact, err = aq.get_best_contact(db, [], [], [rc.CHANNEL_TASK], [], AGENT)
    assert err is None and contact is not None and contact.id == e.id
    assert contact.version == 1 and contact.status == rc.CONTACT_STATE_IN_REVERSE_ROUTING  # in-memory
    row = db.query(ActiveQueueEntry.status, ActiveQueueEntry.version).filter(ActiveQueueEntry.id == e.id).first()
    assert row.status == rc.CONTACT_STATE_IN_REVERSE_ROUTING and row.version == 1  # DB flipped


def test_team_targeted_pick(db):
    e = _seed_entry(db)
    _target_team(db, e, "T1")
    contact, err = aq.get_best_contact(db, ["T1"], [], [rc.CHANNEL_TASK], [], AGENT)
    assert err is None and contact.id == e.id


def test_not_targeted_returns_no_suitable(db):
    e = _seed_entry(db)
    _target_team(db, e, "OTHER")  # not in the agent's teams; no agent target either
    contact, err = aq.get_best_contact(db, ["T1"], [], [rc.CHANNEL_TASK], [], AGENT)
    assert contact is None and err == "known_error: no suitable contact found"


def test_no_queued_rows_returns_known_error(db):
    contact, err = aq.get_best_contact(db, [], [], [rc.CHANNEL_TASK], [], AGENT)
    assert contact is None and err == "known_error: no suitable contact found"


# =====================================================================================
# skill filter
# =====================================================================================
def test_skill_filter_match(db):
    e = _seed_entry(db)
    _target_agent(db, e)
    _add_skill(db, e, "S1", ">", 3)
    contact, err = aq.get_best_contact(db, [], [AgentSkill("S1", 5)], [rc.CHANNEL_TASK], [], AGENT)
    assert err is None and contact.id == e.id


def test_skill_filter_no_match_skips(db):
    e = _seed_entry(db)
    _target_agent(db, e)
    _add_skill(db, e, "S1", ">", 3)
    contact, err = aq.get_best_contact(db, [], [AgentSkill("S1", 2)], [rc.CHANNEL_TASK], [], AGENT)
    assert contact is None and err == "known_error: no suitable contact found"


def test_skill_filter_invalid_level_skipped(db):
    # an agent skill with level None cannot satisfy a filter
    e = _seed_entry(db)
    _target_agent(db, e)
    _add_skill(db, e, "S1", "=", 4)
    contact, err = aq.get_best_contact(db, [], [AgentSkill("S1", None)], [rc.CHANNEL_TASK], [], AGENT)
    assert contact is None and err == "known_error: no suitable contact found"


# =====================================================================================
# ordering: FIFO (priority ASC, created_at ASC) vs LIFO (priority ASC, created_at DESC)
# =====================================================================================
def test_fifo_order_priority_then_oldest(db):
    t0 = get_now()
    e_hi = _seed_entry(db, queue_priority=5, created_at=t0)
    _target_agent(db, e_hi)
    e_lo_new = _seed_entry(db, queue_priority=1, created_at=t0 + dt.timedelta(minutes=5))
    _target_agent(db, e_lo_new)
    e_lo_old = _seed_entry(db, queue_priority=1, created_at=t0 - dt.timedelta(minutes=5))
    _target_agent(db, e_lo_old)
    contact, err = aq.get_best_contact(db, [], [], [rc.CHANNEL_TASK], [], AGENT)
    assert err is None and contact.id == e_lo_old.id  # lowest priority, then oldest created_at


def test_lifo_order_priority_then_newest(db):
    t0 = get_now()
    e_old = _seed_entry(db, queue_type="last-in-first-out", queue_priority=1, created_at=t0 - dt.timedelta(minutes=5))
    _target_agent(db, e_old)
    e_new = _seed_entry(db, queue_type="last-in-first-out", queue_priority=1, created_at=t0 + dt.timedelta(minutes=5))
    _target_agent(db, e_new)
    contact, err = aq.get_best_contact(db, [], [], [rc.CHANNEL_TASK], [], AGENT)
    assert err is None and contact.id == e_new.id  # LIFO: newest created_at first (priority still ASC)


def test_fifo_miss_falls_through_to_lifo(db):
    # FIFO pass finds rows but none targeted (no valid contact) → falls through to the LIFO pass
    e_fifo = _seed_entry(db, queue_type="first-in-first-out")
    _target_team(db, e_fifo, "OTHER")
    e_lifo = _seed_entry(db, queue_type="last-in-first-out")
    _target_agent(db, e_lifo)
    contact, err = aq.get_best_contact(db, ["T1"], [], [rc.CHANNEL_TASK], [], AGENT)
    assert err is None and contact.id == e_lifo.id


# =====================================================================================
# skip_contacts + channel ANY + status-flip-prevents-repick
# =====================================================================================
def test_skip_contacts_excludes(db):
    t0 = get_now()
    e1 = _seed_entry(db, created_at=t0 - dt.timedelta(minutes=5))
    _target_agent(db, e1)
    e2 = _seed_entry(db, created_at=t0)
    _target_agent(db, e2)
    contact, err = aq.get_best_contact(db, [], [], [rc.CHANNEL_TASK], [e1.contact_uuid], AGENT)
    assert err is None and contact.id == e2.id  # e1 (oldest) skipped


def test_channel_any_matches(db):
    e = _seed_entry(db, queue_channel=rc.CHANNEL_CALLBACK)
    _target_agent(db, e)
    contact, err = aq.get_best_contact(db, [], [], [rc.CHANNEL_EMAIL, rc.CHANNEL_CALLBACK, rc.CHANNEL_TASK], [], AGENT)
    assert err is None and contact.id == e.id


def test_status_flip_prevents_repick(db):
    e = _seed_entry(db)
    _target_agent(db, e)
    c1, err1 = aq.get_best_contact(db, [], [], [rc.CHANNEL_TASK], [], AGENT)
    assert err1 is None and c1.id == e.id  # now IN_REVERSE_ROUTING (no longer QUEUED)
    c2, err2 = aq.get_best_contact(db, [], [], [rc.CHANNEL_TASK], [], AGENT)
    assert c2 is None and err2 == "known_error: no suitable contact found"


# =====================================================================================
# version-CAS conflict (double-pick guard)
# =====================================================================================
def test_version_conflict_when_racer_bumped(db):
    e = _seed_entry(db, version=0)
    _target_agent(db, e)
    # A racing process bumps the version AFTER our SELECT view; synchronize_session=False keeps our
    # in-session view of e at version 0, so the CAS WHERE version=0 matches no row → conflict.
    db.query(ActiveQueueEntry).filter(ActiveQueueEntry.id == e.id).update(
        {ActiveQueueEntry.version: 5}, synchronize_session=False
    )
    _contact, err = aq.get_best_contact(db, [], [], [rc.CHANNEL_TASK], [], AGENT)
    assert err == "version conflict: record was updated by another process"


def test_lifo_priority_always_ascending(db):
    # queue_priority is ASC in BOTH passes — only created_at flips. So in
    # the LIFO pass a lower-priority OLDER contact still wins over a higher-priority NEWER one. Catches a
    # queue_priority-DESC-in-LIFO regression (test_lifo_order, using equal priorities, cannot).
    t0 = get_now()
    e_lo_old = _seed_entry(
        db, queue_type="last-in-first-out", queue_priority=1, created_at=t0 - dt.timedelta(minutes=10)
    )
    _target_agent(db, e_lo_old)
    e_hi_new = _seed_entry(db, queue_type="last-in-first-out", queue_priority=5, created_at=t0)
    _target_agent(db, e_hi_new)
    contact, err = aq.get_best_contact(db, [], [], [rc.CHANNEL_TASK], [], AGENT)
    assert err is None and contact.id == e_lo_old.id  # priority 1 < 5 wins despite being older


def test_check_if_valid_contact_end_to_end_after_cas(db):
    # exercise the BUMPED-version read against a REAL CAS'd row (not a _Carrier): get_best_contact flips
    # version 0→1, and the returned contact's version must match the live row.
    e = _seed_entry(db, version=0)
    _target_agent(db, e)
    contact, err = aq.get_best_contact(db, [], [], [rc.CHANNEL_TASK], [], AGENT)
    assert err is None and contact.version == 1
    assert aq.check_if_valid_contact(db, contact) is True  # bumped version matches the CAS'd row
    # a racing accept bumps it again → the picked contact is now stale → invalid
    db.query(ActiveQueueEntry).filter(ActiveQueueEntry.id == e.id).update(
        {ActiveQueueEntry.version: 2}, synchronize_session=False
    )
    assert aq.check_if_valid_contact(db, contact) is False


def test_reverse_pick_query_budget(db):
    # eligibility is evaluated in SQL: one SELECT (+ one CAS UPDATE) per pass, no per-row membership reads.
    from tests.unit._query_counter import count_statements

    fifo = _seed_entry(db)
    _target_agent(db, fifo)
    lifo = _seed_entry(db, queue_type="last-in-first-out")
    _target_agent(db, lifo)
    kinds = lambda stmts: [s.split()[0].upper() for s in stmts]  # noqa: E731
    with count_statements(db) as stmts:
        contact, err = aq.get_best_contact(db, [], [], [rc.CHANNEL_TASK], [], AGENT)
    assert err is None and contact.id == fifo.id and kinds(stmts) == ["SELECT", "UPDATE"]
    with count_statements(db) as stmts:
        contact, err = aq.get_best_contact(db, [], [], [rc.CHANNEL_TASK], [], AGENT)
    assert err is None and contact.id == lifo.id and kinds(stmts) == ["SELECT", "SELECT", "UPDATE"]


# eligibility is applied before LIMIT: an eligible row behind any backlog of unsuitable rows is picked
BACKLOG = 200


def test_reverse_pick_reaches_eligible_fifo_after_foreign_backlog(db):
    _seed_backlog(db, BACKLOG)  # other agent, all older than the target
    target = _seed_entry(db, created_at=get_now() + dt.timedelta(hours=1))
    _target_agent(db, target)
    contact, err = aq.get_best_contact(db, [], [], [rc.CHANNEL_TASK], [], AGENT)
    assert err is None and contact.id == target.id and contact.version == 1


def test_reverse_pick_reaches_eligible_lifo_after_foreign_backlog(db):
    target = _seed_entry(db, queue_type="last-in-first-out", created_at=get_now() - dt.timedelta(hours=1))
    _target_agent(db, target)
    _seed_backlog(db, BACKLOG, queue_type="last-in-first-out")  # newer rows scan first in LIFO
    contact, err = aq.get_best_contact(db, [], [], [rc.CHANNEL_TASK], [], AGENT)
    assert err is None and contact.id == target.id


def test_reverse_pick_reaches_eligible_after_skill_misses(db):
    _seed_backlog(db, BACKLOG, agent_id=AGENT, skill=("S1", ">", 10))  # agent-matched, skill too low
    target = _seed_entry(db, created_at=get_now() + dt.timedelta(hours=1))
    _target_agent(db, target)
    _add_skill(db, target, "S1", ">", 3)
    contact, err = aq.get_best_contact(db, [], [AgentSkill("S1", 5)], [rc.CHANNEL_TASK], [], AGENT)
    assert err is None and contact.id == target.id


def test_reverse_pick_has_stable_equal_timestamp_order(db):
    t0 = get_now()
    fifo = [_seed_entry(db, created_at=t0) for _ in range(3)]
    lifo = [_seed_entry(db, queue_type="last-in-first-out", created_at=t0) for _ in range(3)]
    for e in fifo + lifo:
        _target_agent(db, e)
    contact, err = aq.get_best_contact(db, [], [], [rc.CHANNEL_TASK], [], AGENT)
    assert err is None and contact.id == fifo[0].id  # FIFO tie: smallest id
    contact, err = aq.get_best_contact(db, [], [], [rc.CHANNEL_TASK], [e.contact_uuid for e in fifo], AGENT)
    assert err is None and contact.id == lifo[-1].id  # LIFO tie: largest id


def test_reverse_pick_ignores_soft_deleted_membership(db):
    e = _seed_entry(db)
    _target_agent(db, e)
    db.query(ActiveQueueAgent).filter(ActiveQueueAgent.active_queue_entry_id == e.id).update(
        {ActiveQueueAgent.deleted_at: get_now()}, synchronize_session=False
    )
    contact, err = aq.get_best_contact(db, [], [], [rc.CHANNEL_TASK], [], AGENT)
    assert contact is None and err == "known_error: no suitable contact found"
