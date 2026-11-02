"""The event outbox. `emit` inserts an unstamped row; the relay stamps `seq` (the public id), fills the
conversation's direction and context, queues one webhook job per matching subscription and wakes the streams."""

import logging
import uuid
from collections.abc import Callable
from datetime import UTC, datetime

from sqlalchemy import BigInteger, Column, Index, Table, Text, func, insert, text
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from hs.events.catalog import CATALOG
from hs.infra.db import DBBase, core_engine
from hs.infra.redis import Cache
from hs.jobs import background, execute

log = logging.getLogger(__name__)

events = Table(
    "events",
    DBBase.metadata,
    Column("id", BigInteger, primary_key=True),
    Column("seq", BigInteger),
    Column("tenant_id", Text, nullable=False),
    Column("type", Text, nullable=False),
    Column("conversation_id", Text),
    Column("agent_id", Text),
    Column("direction", Text),
    Column("context", JSONB),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=func.now()),
    Column("data", JSONB, nullable=False),
    Index("ix_events_tenant_seq", "tenant_id", "seq"),
    Index("ix_events_unstamped", "id", postgresql_where=text("seq IS NULL")),
    Index("ix_events_conversation_seq", "conversation_id", "seq"),
    Index("ix_events_created_at", "created_at"),
    Index(
        "ix_events_queued_queue",
        "tenant_id",
        text("(data->>'queue_id')"),
        postgresql_where=text("type = 'conversation.queued'"),
    ),
)

RELAY_LOCK = 0x48535256
# nextval sits in a sorted target list, which Postgres evaluates after the sort: seq follows id. jsonb refuses
# \u0000, so such a context is dropped rather than failing the batch for every tenant.
STAMP = r"""
WITH batch AS (
    SELECT e.id, nextval('event_seq') AS seq, cp.direction_inbound, ccd.conversation_custom_data AS context
    FROM (SELECT id, conversation_id FROM events WHERE seq IS NULL ORDER BY id LIMIT 500) e
    LEFT JOIN conversation_parent cp ON cp.uuid = e.conversation_id::uuid
    LEFT JOIN customer_conversation_data ccd ON ccd.conversation_parent_id = cp.id
    ORDER BY e.id
), stamped AS (
    UPDATE events SET seq = b.seq,
        context = CASE WHEN strpos(b.context::text, '\u0000') = 0 THEN b.context::jsonb END,
        direction = CASE b.direction_inbound WHEN true THEN 'inbound' WHEN false THEN 'outbound' END
    FROM batch b WHERE events.id = b.id
    RETURNING events.id, events.seq, events.tenant_id, events.type
), deliveries AS (
    INSERT INTO jobs (kind, run_at, payload)
    SELECT 'webhook', now(), jsonb_build_object('event_id', s.id, 'webhook_id', w.id)
    FROM stamped s JOIN webhooks w ON w.tenant_id = s.tenant_id AND w.enabled
        AND (cardinality(w.event_types) = 0 OR s.type = ANY(w.event_types))
)
SELECT tenant_id, max(seq) FROM stamped GROUP BY tenant_id
"""
COLUMNS = "SELECT seq, type, created_at, tenant_id, conversation_id, agent_id, direction, context, data FROM events e"
SELECT_EVENTS = (
    COLUMNS + " WHERE e.tenant_id = :tenant AND e.seq > :after AND (CAST(:head AS bigint) IS NULL OR e.seq <= :head)"
    " AND (CAST(:type AS text) IS NULL OR e.type = :type)"
    " AND (CAST(:conversation AS text) IS NULL OR e.conversation_id = :conversation)"
)
# The agents' own events, the events of conversations they are in, and those of conversations queued on a queue of
# the team of `:sup` (a supervisor); read with every page, so a queue made after the stream opened counts.
IN_SCOPE = """ AND (e.agent_id = ANY(:agents) OR e.conversation_id IN (
    SELECT qe.conversation_id FROM events qe WHERE qe.tenant_id = :tenant AND qe.type = 'conversation.queued'
    AND qe.data->>'queue_id' IN (
        SELECT q.id::text FROM queues q JOIN organization o ON o.id = q.organization AND o.uuid::text = :tenant
        WHERE EXISTS (SELECT 1 FROM jsonb_array_elements_text(q.body->'teams') t(uuid) WHERE t.uuid IN (
            SELECT tm.uuid::text FROM account_org_mapping me JOIN aom_team mt ON mt.aom = me.id
            AND mt.is_deleted IS NOT TRUE JOIN team tm ON tm.id = mt.team WHERE me.uuid = CAST(:sup AS uuid))))
    UNION
    SELECT cp.uuid::text FROM conversation_parent cp JOIN conversation c ON c.parent = cp.id
    JOIN account_org_mapping a ON a.id = c.primary_aom WHERE a.uuid = ANY(CAST(:agents AS uuid[]))
    UNION SELECT cp.uuid::text FROM conversation_secondary_aom_mapping m
    JOIN conversation_parent cp ON cp.id = m.conversation JOIN account_org_mapping a ON a.id = m.aom
    WHERE a.uuid = ANY(CAST(:agents AS uuid[]))))"""
BY_SEQ = " ORDER BY e.seq LIMIT :limit"
TEAMMATES = (
    "SELECT DISTINCT o.uuid::text FROM account_org_mapping me"
    " JOIN aom_team mt ON mt.aom = me.id AND mt.is_deleted IS NOT TRUE"
    " JOIN aom_team ot ON ot.team = mt.team AND ot.is_deleted IS NOT TRUE"
    " JOIN account_org_mapping o ON o.id = ot.aom WHERE me.uuid = CAST(:agent AS uuid)"
)


def _uuid(value) -> str | None:
    return None if value is None else str(uuid.UUID(str(value)))


def emit(
    type: str,
    *,
    tenant_id: str,
    data: dict,
    conversation_id: str | None = None,
    agent_id: str | None = None,
    session: Session | None = None,
) -> None:
    """Record an event. With `session` it commits with the caller's change; without, a failed insert is logged."""
    row = insert(events).values(
        tenant_id=_uuid(tenant_id),
        type=type,
        data=CATALOG[type].model_validate(data).model_dump(mode="json", by_alias=True),
        conversation_id=_uuid(conversation_id),
        agent_id=_uuid(agent_id),
    )
    if session is not None:
        session.execute(row)
        return
    try:
        execute(None, row)
    except SQLAlchemyError:
        log.exception("emit %s failed", type)


def relay_once() -> None:
    """Stamp a batch, if this worker wins the relay lock, then wake the streams of each tenant it touched."""
    with core_engine.begin() as conn:
        if not conn.execute(text("SELECT pg_try_advisory_xact_lock(:k)"), {"k": RELAY_LOCK}).scalar():
            return
        stamped = conn.execute(text(STAMP)).all()
    for tenant_id, seq in stamped:
        Cache.get_instance().client.publish(f"ev:{tenant_id}", seq)


def start() -> Callable[[], None]:
    return background("event relay", relay_once)


def envelope(row) -> dict:
    return {
        "id": str(row.seq),
        "type": row.type,
        "created_at": iso(row.created_at),
        "tenant_id": row.tenant_id,
        "conversation_id": row.conversation_id,
        "agent_id": row.agent_id,
        "direction": row.direction,
        "context": row.context,
        "data": row.data,
    }


def iso(moment: datetime) -> str:
    return moment.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def scope(principal) -> tuple[list[str], str | None] | None:
    """The agents whose events the principal sees (its own; a supervisor's teams too) and the supervisor whose team
    queues' conversations it also sees; None for all of them."""
    if principal.kind == "key" or principal.role == "admin":
        return None
    agents, sup = [principal.agent_id], None
    if principal.role == "supervisor":
        sup = principal.agent_id
        with core_engine.connect() as conn:
            agents += conn.execute(text(TEAMMATES), {"agent": sup}).scalars().all()
    return agents, sup


def read(
    tenant_id: str,
    after: int,
    limit: int,
    *,
    head: int | None = None,
    scope: tuple[list[str], str | None] | None = None,
    type: str | None = None,
    conversation_id: str | None = None,
) -> list[dict]:
    """Stamped events with after < seq <= head, in seq order."""
    params = {"tenant": tenant_id, "after": after, "head": head, "limit": limit, "type": type}
    params |= {"conversation": conversation_id, "agents": scope and scope[0], "sup": scope and scope[1]}
    with core_engine.connect() as conn:
        rows = conn.execute(text(SELECT_EVENTS + (IN_SCOPE if scope is not None else "") + BY_SEQ), params)
        return [envelope(r) for r in rows]


def one(conn, event_id: int) -> dict | None:
    row = conn.execute(text(COLUMNS + " WHERE e.id = :id"), {"id": event_id}).first()
    return envelope(row) if row else None


def latest(tenant_id: str) -> int:
    with core_engine.connect() as conn:
        return conn.execute(
            text("SELECT coalesce(max(seq), 0) FROM events WHERE tenant_id = :t"), {"t": tenant_id}
        ).scalar()
