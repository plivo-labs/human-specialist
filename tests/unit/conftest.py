"""Unit-suite session hygiene.

Much of the usecase code under test reaches the DB through the module-level
scoped session (hs.infra.context.get_db_session -> hs.infra.db.db_session)
rather than the transactional `db` fixture. That session is process-global, so one test
leaving it mid-transaction — or its checked-out connection dying underneath it — poisons
every later test that commits on it (PendingRollbackError / "server closed the connection
unexpectedly", seen as order-dependent failures).

remove() closes the thread's session slot: open work is rolled back, the connection goes
back to the pool (dead ones are invalidated there), and the next test lazily gets a fresh
session on first touch — pool_pre_ping revalidates the connection at checkout.
"""

import pytest

import hs.app  # noqa: F401  # maps every model before the first configure_mappers(), as the app does


@pytest.fixture(scope="session", autouse=True)
def _migrated_db(db_engine):
    """Much of the engine reaches the DB through the app's own engine, so every unit test gets the database."""


@pytest.fixture(autouse=True)
def _reset_global_scoped_session():
    yield
    from hs.infra.db import db_session
    from hs.infra.log_context import context_req_meta

    context_req_meta.set(None)  # a test's request metadata must not reach the next test on this worker

    try:
        db_session.remove()
    except Exception:
        pass


@pytest.fixture(autouse=True)
def emitted(monkeypatch):
    """Events the code under test emitted, as (type, data, kwargs); each data is checked against the catalog."""
    import hs.conversations.store
    import hs.events
    from hs.events.catalog import CATALOG

    seen = []

    def emit(type, *, data, **kwargs):
        CATALOG[type].model_validate(data)
        seen.append((type, data, kwargs))

    monkeypatch.setattr(hs.events, "emit", emit)
    monkeypatch.setattr(hs.conversations.store, "_tenant", str)
    return seen


@pytest.fixture(autouse=True)
def claimed(monkeypatch):
    """Offer outcome claims, kept per test: a fixed test offer id must not stay claimed in Redis for the next run."""
    import hs.conversations.store

    keys = set()

    def claim(offer_id, agent):
        fresh = (offer_id, agent) not in keys
        keys.add((offer_id, agent))
        return fresh

    monkeypatch.setattr(hs.conversations.store, "claim_offer_outcome", claim)
    return keys


@pytest.fixture(autouse=True)
def scheduled(monkeypatch):
    """Jobs the code under test scheduled, as (kind, payload, kwargs); each payload must be JSON."""
    import json

    import hs.jobs

    seen = []

    def schedule(kind, payload, **kwargs):
        json.dumps(payload)
        seen.append((kind, payload, kwargs))

    monkeypatch.setattr(hs.jobs, "schedule", schedule)
    return seen
