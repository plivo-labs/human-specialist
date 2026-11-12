import logging
from functools import partial
from itertools import chain

from redis import RedisError
from sqlalchemy import func, select, text
from sqlalchemy.engine import Connection, Row

from hs import hot
from hs.core import T, after_commit

log = logging.getLogger(__name__)
COUNTED = ("message", "note", "voicemail")
_SINCE = text(
    "SELECT CAST(c.id AS text), (SELECT count(*) FROM entry e WHERE e.conversation_id = c.id AND e.kind = ANY(:kinds)"
    " AND e.agent_id IS NULL AND coalesce(e.written_at, e.at) > coalesce((SELECT max(coalesce(x.written_at, x.at))"
    " FROM entry x WHERE x.conversation_id = c.id AND x.kind = ANY(:kinds) AND x.agent_id IS NOT NULL), '-infinity'))"
    " FROM unnest(CAST(:ids AS uuid[])) AS c(id)"
)


def columns() -> list:
    C, E = T.conversation, T.entry
    shown = {"id": E.c.id, "direction": E.c.body["direction"], "text": E.c.body["text"]}
    shown |= {"attachments": func.jsonb_array_length(E.c.body["attachments"]), "agent_id": E.c.agent_id}
    shown |= {"status": E.c.status, "created_at": E.c.at}
    shown |= {"template": E.c.body["template"], "whatsapp": E.c.body["whatsapp"]}
    last = select(func.jsonb_build_object(*chain(*shown.items())))
    last = last.where(E.c.conversation_id == C.c.id, E.c.kind == "message").order_by(E.c.at.desc(), E.c.id.desc())
    return [
        func.nullif(C.c.ours, "").label("our_address"),
        last.limit(1).scalar_subquery().label("last_message"),
        last_activity().label("last_activity_at"),
    ]


def last_activity():
    C, E, A = T.conversation, T.entry, T.assignment
    entered = select(func.max(E.c.at)).where(E.c.conversation_id == C.c.id).scalar_subquery()
    moved = select(func.max(func.greatest(A.c.started_at, A.c.ended_at))).where(A.c.conversation_id == C.c.id)
    return func.greatest(C.c.opened_at, C.c.queued_at, C.c.answered_at, C.c.closed_at, entered, moved.scalar_subquery())


def mark(conv: Row, read: bool) -> None:
    if conv.state != "closed":
        hot.unread(conv.tenant_id, conv.id, read)


def wrote(db: Connection, conv: Row, e: Row) -> None:
    if e.kind in COUNTED:
        after_commit(db, partial(mark, conv, bool(e.agent_id)))


def counted(db: Connection, tenant_id, rows: list[dict]) -> list[dict]:
    """A count Redis lacks is counted from Postgres and not written back: a message may land between the two."""
    live = [r["id"] for r in rows if r["state"] != "closed"]
    try:
        counts = dict(zip(live, hot.unread_counts(tenant_id, live), strict=True))
    except RedisError:
        log.warning("tenant %s counts unread from Postgres: Redis did not answer", tenant_id)
        counts = dict.fromkeys(live)
    if missing := [cid for cid, n in counts.items() if n is None]:
        counts |= dict(db.execute(_SINCE, {"ids": missing, "kinds": list(COUNTED)}).all())
    for r in rows:
        r["unread_count"] = counts.get(r["id"]) or 0
    return rows


def read_by(p, conv: Row) -> None:
    if p.kind == "agent":
        try:
            mark(conv, read=True)
        except RedisError:
            log.warning("conversation %s keeps its unread count: Redis did not answer", conv.id)
