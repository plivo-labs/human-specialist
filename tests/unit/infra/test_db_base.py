"""Offline unit tests for the DB base layer (hs/infra/db.py): `create_engine` does not connect, and
every assertion operates on the SQLAlchemy object graph, not the wire. No Postgres/Redis required.
"""

import time
from types import SimpleNamespace

from sqlalchemy import Column, String
from sqlalchemy.orm import Session, configure_mappers

from hs.infra import db
from hs.infra.db import DB, CoreDBBase, DBBase, RoutingSession, SessionLocal, core_engine, db_session


# Defined at MODULE level (collection time), not inside the test: `DB` is an
# AbstractConcreteBase, so once any test triggers the global configure_mappers()
# (e.g. a live parity test issuing session.query before this module's tests run),
# defining a NEW mapped class would fail with "could not assemble any primary key
# columns". Module-level definition happens during collection, before any test
# executes a query — same reason conftest eager-imports every data_adapter module.
class _Widget(DB, CoreDBBase):
    __tablename__ = "_widget_offline_dbbase_test"
    name = Column(String)


def test_offline_import_has_no_network():
    """`from hs.infra.db import DB` (and friends) must import with no live DB."""
    assert DB is not None
    assert hasattr(DB, "select_query")
    assert hasattr(DBBase, "metadata")


def test_routing_session_get_bind_always_core_engine():
    """RoutingSession.get_bind ALWAYS returns core_engine (crm db deprecated)."""
    rs = RoutingSession.__new__(RoutingSession)  # no connection opened
    assert rs.get_bind() is core_engine
    assert rs.get_bind(mapper=object(), clause=object()) is core_engine


def test_routing_session_get_bind_accepts_scoped_proxy_signature():
    """Regression: scoped_session's generated proxy forwards Session.get_bind's
    FULL base signature (mapper, clause, bind=None, _sa_skip_events,
    _sa_skip_for_implicit_returning). The narrowed (mapper, clause) override raised
    `TypeError: ... unexpected keyword argument 'bind'` on every db_session.get_bind()
    reached through the proxy — exactly what Transcription.acquire_ingestion_lock() does
    when get_db_session() falls back to the module scoped_session (context-less
    transcript-attach retries), turning an already-fetched provider transcript into a 500."""
    rs = RoutingSession.__new__(RoutingSession)  # no connection opened
    assert (
        rs.get_bind(mapper=None, clause=None, bind=None, _sa_skip_events=None, _sa_skip_for_implicit_returning=False)
        is core_engine
    )
    # Byte-for-byte the failing call: through the scoped proxy itself.
    try:
        assert db_session.get_bind() is core_engine
    finally:
        db_session.remove()


def test_sessionmakers_use_routing_session_and_no_expire_on_commit():
    # sessionmaker synthesizes a subclass of the given class_, so assert lineage
    # (and identity by name) rather than object identity.
    assert issubclass(SessionLocal.class_, RoutingSession)
    assert issubclass(db_session.session_factory.class_, RoutingSession)
    # expire_on_commit=False on both (objects stay usable after commit)
    assert SessionLocal.kw.get("expire_on_commit") is False
    assert db_session.session_factory.kw.get("expire_on_commit") is False


def test_core_db_base_columns_and_classmethods():
    """CoreDBBase mixin exposes the soft-delete column set + the shared classmethods."""
    for col in ("id", "uuid", "created_at", "updated_at", "is_deleted"):
        assert hasattr(CoreDBBase, col), col
    for meth in ("get_by_uuid", "get_by_id", "delete", "commit"):
        assert callable(getattr(CoreDBBase, meth)), meth


def test_db_is_abstract_concrete_base_with_discriminator():
    from sqlalchemy.ext.declarative import AbstractConcreteBase

    assert issubclass(DB, AbstractConcreteBase)
    assert DB._concrete_discriminator_name == "_concrete_discriminator"


def test_concrete_model_maps_and_select_query_filters_soft_delete():
    """A `class X(DB, CoreDBBase)` maps cleanly; select_query auto-adds
    `is_deleted IS False` and renders that WHERE clause (offline, no execute).
    (`_Widget` is defined at module level — see comment there.)"""

    configure_mappers()
    assert "_widget_offline_dbbase_test" in DBBase.metadata.tables

    # Build a query against an unbound session — never executed, so no DB needed.
    sess = Session()
    q = _Widget.select_query(sess)
    compiled = str(q.statement.compile())
    assert "is_deleted IS false" in compiled.lower().replace("= false", "is false") or "is_deleted" in compiled.lower()

    # columns-projection branch also applies the soft-delete filter
    q_cols = _Widget.select_query(sess, columns=[_Widget.id])
    assert "is_deleted" in str(q_cols.statement.compile()).lower()


def test_a_slow_query_or_a_failed_statement_never_logs_its_bound_values(monkeypatch):
    seen = []
    monkeypatch.setattr(db, "SLOW_QUERY_THRESHOLD_MS", -1)
    monkeypatch.setattr(db.logger, "warning", lambda msg, *args: seen.append(msg % args))
    conn = SimpleNamespace(info={"query_start_time": [time.time()]})
    db.after_cursor_execute(
        conn, None, "INSERT INTO recording VALUES (%(url)s)", {"url": "https://m.example/r.mp3"}, None, 0
    )
    assert seen and "INSERT INTO recording" in seen[0] and "r.mp3" not in seen[0]
    assert core_engine.hide_parameters
