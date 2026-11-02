"""The closed_conversation_search write path (upsert / safe_upsert).

The persistence tests run against the local Postgres (skipped if unavailable); they prove the
upsert persists a built row and is idempotent on conv_number. safe_upsert must never raise.
"""

import datetime
from types import SimpleNamespace

import pytest

import tests.conftest  # noqa: F401
from hs.conversations import search as ccsm
from hs.conversations.search_store import ClosedConversationSearch


def _dt(h=9, m=0, s=0):
    return datetime.datetime(2026, 6, 28, h, m, s, tzinfo=datetime.UTC)


def _synthetic_parent(conv_number):
    chunk = SimpleNamespace(
        type="chat",
        disposition=None,
        sub_disposition=None,
        description="hello",
        attributes=None,
        timestamp=_dt(9, 1),
        channel="chat",
        message_id="m1",
        from_user="+15551230000",
        to_user="+15551230000",
        uuid="c0",
        created_at=_dt(9, 1),
        channel_meta={"status": "delivered"},
        disposition_id=None,
        sub_disposition_id=None,
        dispo=None,
        sub_dispo=None,
    )
    child = SimpleNamespace(
        created_at=_dt(9, 0),
        primary_aom=None,
        state="session_resolved",
        channel="chat",
        sender_id=None,
        customer_contact="+15551230000",
        start_at=_dt(9, 0),
        end_at=_dt(9, 5),
        wrap_up_start_at=None,
        contact_creation_start_at=None,
        case_creation_start_at=None,
        direction_inbound=True,
        primary_agent=None,
        generation_type="qnr_generated",
        active_queue_entry_id=None,
        chunks=[chunk],
    )
    return SimpleNamespace(
        conversation_number=conv_number,
        organization=1,
        org=SimpleNamespace(uuid="org-uuid"),
        contact_identifier="+15551230000",
        case_id="C-1",
        crm_contact_id="CRM1",
        channel="chat",
        direction_inbound=True,
        contact_name="Jane Doe",
        created_at=_dt(9, 0),
        end_at=_dt(9, 6),
        updated_at=_dt(9, 6, 30),
        customer_conversation_data=None,
        child_convs=[child],
    )


@pytest.fixture
def _db():
    import sqlalchemy
    from sqlalchemy.exc import OperationalError

    from hs.infra.db import SessionLocal

    db = SessionLocal()
    try:
        db.execute(sqlalchemy.text("select 1"))
    except OperationalError:
        db.close()
        pytest.skip("local Postgres unavailable")
    yield db
    db.rollback()
    db.close()


def test_safe_upsert_never_raises_on_error(monkeypatch):
    def boom(_cn):
        raise RuntimeError("kaboom")

    monkeypatch.setattr(ccsm, "_load_closed_parent", boom)
    assert ccsm.safe_upsert("001-err-000001") is False  # swallowed, no raise


def test_upsert_skips_when_not_closed(monkeypatch):
    monkeypatch.setattr(ccsm, "_load_closed_parent", lambda _cn: None)
    assert ccsm.upsert_for_conversation("001-open-00001") is False


def test_upsert_persists_and_is_idempotent(monkeypatch, _db):
    import uuid as _uuid

    cn = "001-wp-" + _uuid.uuid4().hex[:8]
    monkeypatch.setattr(ccsm, "get_db_session", lambda: _db)
    monkeypatch.setattr(ccsm.Dispositions, "get_by_disposition_list", lambda *a, **k: [])
    monkeypatch.setattr(ccsm.AccountOrgMapping, "get_account_by_aom_uuid", lambda *a, **k: None)
    monkeypatch.setattr(ccsm, "_load_closed_parent", lambda _cn: _synthetic_parent(cn))
    try:
        assert ccsm.upsert_for_conversation(cn) is True
        _db.commit()
        assert _db.query(ClosedConversationSearch).filter_by(conv_number=cn).count() == 1
        # second upsert for the same conv must REPLACE, not duplicate (unique conv_number)
        assert ccsm.upsert_for_conversation(cn) is True
        _db.commit()
        rows = _db.query(ClosedConversationSearch).filter_by(conv_number=cn).all()
        assert len(rows) == 1
        assert rows[0].customer_name == "Jane Doe"
    finally:
        _db.query(ClosedConversationSearch).filter_by(conv_number=cn).delete(synchronize_session=False)
        _db.commit()
