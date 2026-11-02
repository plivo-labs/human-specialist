import contextlib
import time
import uuid
from datetime import UTC, datetime

from sqlalchemy import TIMESTAMP, Boolean, Column, Integer, create_engine, event
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import AbstractConcreteBase
from sqlalchemy.orm import Session, declarative_base, scoped_session, sessionmaker

from hs.infra.log import logger
from hs.infra.settings import settings

SLOW_QUERY_THRESHOLD_MS = 500
CONNECT_ARGS = {
    "connect_timeout": 3,
    # A host name that resolves to a writer and standbys must land on the writer.
    "target_session_attrs": "read-write",
    # Bound every statement and lock so a hung query cannot freeze a worker. ms.
    "options": "-c statement_timeout=5000 -c lock_timeout=3000 -c idle_in_transaction_session_timeout=10000",
}
# 30 + 20 per process stays above AnyIO's 40-thread limiter, so the threadpool, not the pool, is the limiter.
core_engine = create_engine(
    settings.database_url,
    pool_size=30,
    max_overflow=20,
    pool_recycle=540,
    pool_pre_ping=True,
    pool_timeout=10,
    connect_args=CONNECT_ARGS,
    hide_parameters=True,
)
# SQLSTATE 25006 = ReadOnlySqlTransaction: the statement reached a node that cannot serve writes.
READONLY_SQLSTATE = "25006"
try:
    from psycopg2 import errors as _pg_errors

    _PG_READONLY_ERROR = _pg_errors.lookup(READONLY_SQLSTATE)
except Exception:
    _PG_READONLY_ERROR = None


def is_readonly_txn_error(exc) -> bool:
    """True if exc (or a wrapped original, up to 4 levels) is SQLSTATE 25006: walks .orig, then __cause__."""
    seen = 0
    while exc is not None and seen < 4:
        if _PG_READONLY_ERROR is not None and isinstance(exc, _PG_READONLY_ERROR):
            return True
        if (getattr(exc, "pgcode", None) or getattr(exc, "sqlstate", None)) == READONLY_SQLSTATE:
            return True
        exc = getattr(exc, "orig", None) or exc.__cause__
        seen += 1
    return False


@event.listens_for(core_engine, "handle_error")
def _readonly_txn_is_disconnect(context):
    """A read-only connection fails every later write the same way, so invalidate it like a disconnect and let the
    next checkout dial the writer. The failing statement still raises."""
    if is_readonly_txn_error(context.original_exception):
        context.is_disconnect = True


class RoutingSession(Session):
    # scoped_session's generated proxy forwards Session.get_bind's FULL base signature
    # (mapper, clause, bind=None, _sa_skip_events=..., _sa_skip_for_implicit_returning=...),
    # so a narrower override TypeErrors on every db_session.get_bind() reached through the
    # proxy — i.e. any context-less caller such as acquire_ingestion_lock on the retry rail.
    def get_bind(self, mapper=None, clause=None, **kw):
        return core_engine


SessionLocal = sessionmaker(class_=RoutingSession, expire_on_commit=False)
db_session = scoped_session(sessionmaker(class_=RoutingSession, expire_on_commit=False))


@contextlib.contextmanager
def owned_session(db):
    """(session, commit): the caller's session with a no-op commit, else a session of its own that commit()
    commits and that is closed afterwards."""
    if db is not None:
        yield db, (lambda: None)
        return
    own = SessionLocal()
    try:
        yield own, own.commit
    finally:
        own.close()


def time_now():
    return datetime.now(UTC)


DBBase = declarative_base()


class DB(AbstractConcreteBase, DBBase):
    _concrete_discriminator_name = "_concrete_discriminator"

    @classmethod
    def select_query(cls, db: Session, columns=None):
        if not columns:
            return db.query(cls).filter(cls.is_deleted.is_(False))
        return db.query(*columns).filter(cls.is_deleted.is_(False))


class CoreDBBase:
    id = Column(Integer, primary_key=True, unique=True, autoincrement=True)
    uuid = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, unique=True)
    created_at = Column(TIMESTAMP(timezone=True), default=time_now, nullable=False)
    updated_at = Column(TIMESTAMP(timezone=True), default=time_now, onupdate=time_now, nullable=False)
    is_deleted = Column(Boolean, default=False)

    @classmethod
    def get_by_uuid(cls, uuid):
        from uuid import UUID as _UUID

        from hs.infra.context import get_db_session

        # Guard: a malformed identifier (a non-UUID string such as a placeholder sender_id)
        # would otherwise reach Postgres' uuid cast and raise InvalidTextRepresentation -> 500.
        # Valid UUID objects/strings pass through unchanged; anything else is a clean miss.
        try:
            _UUID(str(uuid))
        except (ValueError, AttributeError, TypeError):
            return None
        db: Session = get_db_session()
        return db.query(cls).filter(cls.uuid == uuid, cls.is_deleted.is_(False)).first()

    @classmethod
    def get_by_id(cls, id):
        from hs.infra.context import get_db_session

        db: Session = get_db_session()
        return db.query(cls).filter(cls.id == id, cls.is_deleted.is_(False)).first()

    @classmethod
    def delete(cls, id):
        from hs.infra.context import get_db_session

        db: Session = get_db_session()
        rows = db.query(cls).filter(cls.id == id, cls.is_deleted.is_(False)).delete()
        db.flush()
        return rows

    @classmethod
    def commit(cls):
        from hs.infra.context import get_db_session

        db: Session = get_db_session()
        db.commit()


@event.listens_for(Engine, "before_cursor_execute")
def before_cursor_execute(conn, cursor, statement, _parameters, context, executemany):
    conn.info.setdefault("query_start_time", []).append(time.time())


@event.listens_for(Engine, "after_cursor_execute")
def after_cursor_execute(conn, cursor, statement, _parameters, context, executemany):
    query_times = conn.info.get("query_start_time", [])
    if not query_times:
        logger.error("No start time recorded for query")
        return
    start_time = query_times.pop()
    total_time = time.time() - start_time
    total_time_ms = total_time * 1000
    if total_time_ms > SLOW_QUERY_THRESHOLD_MS:
        # Bound values stay out of the log: they include recording URLs and callers' numbers.
        logger.warning("Slow query detected. Execution time: %.2f ms, Query: %s", total_time_ms, statement)
