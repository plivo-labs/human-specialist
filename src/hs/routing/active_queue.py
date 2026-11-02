import datetime

from sqlalchemy import and_, exists, literal, or_, true

from hs.conversations.store import ActiveQueueAgent, ActiveQueueEntry, ActiveQueueSkillsFilter, ActiveQueueTeam
from hs.infra.helpers import get_now
from hs.infra.log import logger
from hs.routing import constants as rc

_E = ActiveQueueEntry
_IN_FLIGHT_STATUSES = (rc.CONTACT_STATE_IN_TRANSIT, rc.CONTACT_STATE_QUEUED, rc.CONTACT_STATE_IN_REVERSE_ROUTING)

# Stable results for strict callers. A missing live row is an observed state (usually a late MPC callback
# after hangup/soft-delete), not a database failure, and must not be retried; only an UPDATE that raised is.
QUEUE_STATUS_NO_LIVE_ROW = "active_queue_status:no_live_row"
QUEUE_STATUS_PERSISTENCE_RETRYABLE = "active_queue_status:persistence_retryable"


def is_no_live_queue_status(error: str | None) -> bool:
    return error == QUEUE_STATUS_NO_LIVE_ROW


def create_active_queue_entry(
    db,
    entry: ActiveQueueEntry,
    teams: list[str] | None = None,
    skills_filter: list[tuple[str, str, int]] | None = None,
    agents: list[str] | None = None,
) -> int:
    db.add(entry)
    db.flush()
    eid = entry.id
    db.add_all([ActiveQueueTeam(team_id=team_id, active_queue_entry_id=eid) for team_id in teams or []])
    db.add_all(
        ActiveQueueSkillsFilter(skill_id=skill_id, operator=op, value=value, active_queue_entry_id=eid)
        for skill_id, op, value in skills_filter or []
    )
    db.add_all([ActiveQueueAgent(agent_id=agent_id, active_queue_entry_id=eid) for agent_id in agents or []])
    db.flush()
    return eid


def get_queue_capacity(db, queue_uuid: str, channel: str) -> int:
    return _E.active_query(db).filter(_E.queue_uuid == queue_uuid, _E.queue_channel == channel).count()


def _run_rows(db, contact_uuid: str, qnr_run_uuid: str):
    return db.query(_E).filter(_E.contact_uuid == contact_uuid, _E.qnr_run_uuid == qnr_run_uuid)


def update_queue_status(db, contact_uuid: str, qnr_run_uuid: str, status: str, *, strict: bool = False) -> str | None:
    """Update every matching queue row with one atomic SQL UPDATE.

    Duplicate rows are possible until the active-queue uniqueness migration is deployed, so one or more
    affected live rows is a successful durable acknowledgement. In strict mode, zero affected rows and a
    database failure are deliberately different results so callback callers do not retry a queue entry that
    was already soft-deleted by hangup.
    """
    try:
        with db.begin_nested():
            q = _run_rows(db, contact_uuid, qnr_run_uuid)
            if status in _IN_FLIGHT_STATUSES:
                q = q.filter(_E.is_deleted.isnot(True), _E.deleted_at.is_(None))
            rows = q.update({_E.status: status}, synchronize_session=False)
        logger.info(f"update_queue_status: {rows} rows contact_uuid={contact_uuid} status={status}")
        if strict and rows == 0:
            return QUEUE_STATUS_NO_LIVE_ROW
    except Exception as e:
        logger.error(f"update_queue_status error contact_uuid={contact_uuid}: {e}")
        if strict:
            return QUEUE_STATUS_PERSISTENCE_RETRYABLE
    return None


def delete_active_queue_entry(db, contact_uuid: str, qnr_run_uuid: str, status: str) -> int:
    live = _E.active_query(db).filter(_E.contact_uuid == contact_uuid, _E.qnr_run_uuid == qnr_run_uuid)
    row = live.order_by(_E.id.asc()).first()
    if row is None:
        logger.info(f"delete_active_queue_entry: no live row contact_uuid={contact_uuid} qnr_run_uuid={qnr_run_uuid}")
        return 0
    update = {_E.is_deleted: True, _E.deleted_at: get_now()}
    if row.status in _IN_FLIGHT_STATUSES:
        update[_E.status] = status
    rows = live.update(update, synchronize_session=False)
    db.flush()
    logger.info(f"delete_active_queue_entry: {rows} rows soft-deleted contact_uuid={contact_uuid}")
    return row.id if rows else 0


def left_unanswered(db, contact_uuid: str, qnr_run_uuid: str) -> bool:
    """The caller's own exit closed this run's queue row: a hangup, a key press or a wait timeout."""
    left = (rc.CONTACT_STATE_DROPPED, rc.CONTACT_STATE_EXIT, rc.CONTACT_STATE_TIMEDOUT)
    return _run_rows(db, contact_uuid, qnr_run_uuid).filter(_E.status.in_(left)).count() > 0


def _first(db, order, *criteria) -> ActiveQueueEntry | None:
    return db.query(_E).filter(*criteria).order_by(order).first()


def get_current_active_queue_entry_by_contact_uuid(db, contact_uuid: str, run: str = "") -> ActiveQueueEntry | None:
    """The contact's live entry; given its queue run, only that run's: a newer run may already hold the contact."""
    live = (_E.status.in_(list(_IN_FLIGHT_STATUSES)), _E.deleted_at.is_(None))
    return _first(db, _E.id.asc(), _E.contact_uuid == contact_uuid, _E.qnr_run_uuid == (run or _E.qnr_run_uuid), *live)


def check_if_valid_contact(db, entry: ActiveQueueEntry) -> bool:
    return db.query(_E).filter(_E.id == entry.id, _E.version == entry.version, _E.deleted_at.is_(None)).count() > 0


def _eligible_entry_filter(teams, skills, agent_id):
    # Applied before ORDER BY / LIMIT: a backlog of unsuitable rows must not hide an eligible one.
    A, T, S = ActiveQueueAgent, ActiveQueueTeam, ActiveQueueSkillsFilter
    member = exists().where(and_(A.active_queue_entry_id == _E.id, A.agent_id == agent_id, A.deleted_at.is_(None)))
    if teams:
        in_team = and_(T.active_queue_entry_id == _E.id, T.team_id.in_(teams), T.deleted_at.is_(None))
        member = or_(member, exists().where(in_team))
    live_filter = and_(S.active_queue_entry_id == _E.id, S.deleted_at.is_(None))
    satisfied = [
        and_(
            S.skill_id == skill.skill_id,
            or_(
                *(
                    and_(S.operator == op, literal(skill.level).op(op)(S.value) if op else true())
                    for op in rc.SKILL_OPERATORS
                )
            ),
        )
        for skill in skills
    ]
    skill_ok = ~exists().where(live_filter)
    if satisfied:
        skill_ok = or_(skill_ok, exists().where(and_(live_filter, or_(*satisfied))))
    return and_(member, skill_ok)


def _best_contact_pass(db, teams, skills, channel, skip_contacts, agent_id, queue_type, desc):
    q = _E.active_query(db).filter(
        _E.queue_type == queue_type,
        _E.queue_channel.in_(channel),
        _E.status == rc.CONTACT_STATE_QUEUED,
        _eligible_entry_filter(teams, skills, agent_id),
    )
    if skip_contacts:
        q = q.filter(~_E.contact_uuid.in_(skip_contacts))
    age = (_E.created_at.desc(), _E.id.desc()) if desc else (_E.created_at, _E.id)
    entry = q.order_by(_E.queue_priority, *age).first()
    if entry is None:
        return None, None
    claimed = {_E.version: entry.version + 1, _E.status: rc.CONTACT_STATE_IN_REVERSE_ROUTING}
    rows_affected = (
        db.query(_E)
        .filter(_E.id == entry.id, _E.version == entry.version, _E.deleted_at.is_(None))
        .update(claimed, synchronize_session=False)
    )
    db.expunge(entry)
    entry.version, entry.status = entry.version + 1, rc.CONTACT_STATE_IN_REVERSE_ROUTING
    if rows_affected == 0:
        return entry, "version conflict: record was updated by another process"
    return entry, None


def get_best_contact(db, teams, skills, channel, skip_contacts, agent_id):
    for queue_type, desc in (("first-in-first-out", False), ("last-in-first-out", True)):
        entry, err = _best_contact_pass(db, teams, skills, channel, skip_contacts, agent_id, queue_type, desc=desc)
        if entry is not None or err is not None:
            return entry, err
    return None, "known_error: no suitable contact found"


def get_last_n_active_queue_entries_for_queue_uuid(
    db, queue_uuid, qnr_run_uuid: str, interval_hours=None, max_entries=None
):
    from hs.voice import constants as vc

    interval_hours = vc.ESTIMATED_WAIT_INTERVAL_HOURS if interval_hours is None else interval_hours
    max_entries = vc.MAX_ENTRIES_FOR_WAIT_TIME if max_entries is None else max_entries
    queue = (_E.queue_uuid == queue_uuid,)
    call = _E.queue_channel == rc.CHANNEL_CALL
    anchor = db.query(_E).filter(*queue, _E.qnr_run_uuid == qnr_run_uuid, call).order_by(_E.id.asc()).first()
    if anchor is None:
        logger.error(
            f"error in fetching active queue entry by qnr_run_uuid qnr_run_uuid={qnr_run_uuid} queue_uuid={queue_uuid}"
        )
        return [], 0
    end_time = anchor.created_at
    window = (_E.created_at >= end_time - datetime.timedelta(hours=interval_hours), _E.created_at <= end_time)
    accepted = _E.status == rc.CONTACT_STATE_ACCEPTED
    samples = (
        db.query(_E).filter(*queue, *window, accepted, call).order_by(_E.created_at.desc()).limit(max_entries).all()
    )
    ahead = (
        _E.created_at >= anchor.created_at
        if anchor.queue_type == "last-in-first-out"
        else _E.created_at <= anchor.created_at
    )
    return samples, db.query(_E).filter(*queue, _E.status.in_(_IN_FLIGHT_STATUSES), call, ahead).count()
