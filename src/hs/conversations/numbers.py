"""Conversation numbers: ten digits, counted per tenant in its row, from above the highest number it holds. Each is
taken in a statement of its own on two connections of its own, committed at once, so a creation holds no lock on the
count to its commit and never waits on the request pool for a second connection. A creation that rolls back leaves a
gap. The count's commit does not wait for the WAL flush, so a slow disk never queues a tenant's creations on its row:
a creation that commits flushes the WAL past its number's count, so a crash never hands that number out again."""

from sqlalchemy import create_engine, text

from hs import core

_POOL = create_engine(
    core.engine.url,
    future=True,
    hide_parameters=True,
    pool_size=2,
    max_overflow=0,
    pool_recycle=540,
    pool_pre_ping=True,
    pool_timeout=core.POOL_TIMEOUT,
    isolation_level="AUTOCOMMIT",
    connect_args=core.CONNECT | {"options": core.CONNECT["options"] + " -c synchronous_commit=off"},
)
NEXT = text(
    "UPDATE tenant SET conversation_seq = greatest(coalesce(conversation_seq,"
    " (SELECT max(number) FROM conversation WHERE tenant_id = :t), 0), 999999999) + 1"
    " WHERE id = :t RETURNING conversation_seq"
)


def take(tenant_id) -> int:
    with _POOL.connect() as db:
        return db.execute(NEXT, {"t": str(tenant_id)}).scalar_one()
