"""get_aom_occupied_state answers 404 ERROR_ORG_NOT_FOUND, not a 500, when the request's org context
resolves to no Organization. Callers already treat any non-2xx as "occupied/blocked".
"""

from types import SimpleNamespace

import pytest

import tests.conftest  # noqa: F401 — eager data_adapter imports
from hs.conversations import management as cm
from hs.conversations.models import ConvChannel


@pytest.fixture
def _org_ctx():
    t_org = cm.context_org_uuid.set("00000000-0000-4000-a000-000000000000")
    t_aom = cm.context_aom_uuid.set("11111111-1111-4111-a111-111111111111")
    yield
    cm.context_org_uuid.reset(t_org)
    cm.context_aom_uuid.reset(t_aom)


def test_missing_org_returns_404_not_500(monkeypatch, _org_ctx):
    # Org not found for the request's org context -> must NOT raise AttributeError('NoneType'.id).
    monkeypatch.setattr(cm.Organization, "get_by_uuid", lambda *a, **k: None)
    resp = cm.get_aom_occupied_state(ConvChannel(channel="chat"))
    assert resp.status_code == 404
    assert resp.error == cm.ERROR_ORG_NOT_FOUND


def test_happy_path_unoccupied_still_200(monkeypatch, _org_ctx):
    # Guard must not break the normal path: org resolves, no active convs -> 200, not occupied.
    monkeypatch.setattr(cm.Organization, "get_by_uuid", lambda *a, **k: SimpleNamespace(id=7))
    monkeypatch.setattr(cm.RoutingSettings, "get_by_org_id", lambda *a, **k: None)
    monkeypatch.setattr(cm.Conversation, "get_active_conversations_for_primary_aom", lambda *a, **k: None)
    monkeypatch.setattr(
        cm.ConversationParent, "get_active_secondary_conversation_mappings_with_state", lambda *a, **k: None
    )
    resp = cm.get_aom_occupied_state(ConvChannel(channel="chat"))
    assert resp.status_code == 200
    assert resp.data["is_aom_occupied"] is False


def _raise_invalid_uuid_dataerror(*_a, **_k):
    """Stands in for the real DB call: `Organization.get_by_uuid(str(None))` binds the
    literal string "None" into a uuid-typed column, and Postgres/psycopg2 rejects it
    with `invalid input syntax for type uuid: "None"`, surfaced by SQLAlchemy as
    DataError -- BEFORE the function ever gets a chance to return None. A mock that
    unconditionally returns None (as in test_missing_org_returns_404_not_500 above)
    does not exercise this: it never proves the guard runs *before* the DB call."""
    from sqlalchemy.exc import DataError

    raise DataError("SELECT ...", {}, Exception('invalid input syntax for type uuid: "None"'))


def test_missing_org_uuid_context_returns_404_not_500(monkeypatch):
    # A request context with no tenant: clear_context() leaves context_org_uuid at its default (None), never re-set. The
    # fix must short-circuit on falsy org_uuid BEFORE calling Organization.get_by_uuid
    # -- proven here by making that call raise instead of returning None.
    token = cm.context_org_uuid.set(None)
    try:
        monkeypatch.setattr(cm.Organization, "get_by_uuid", _raise_invalid_uuid_dataerror)
        resp = cm.get_aom_occupied_state(ConvChannel(channel="chat"))
    finally:
        cm.context_org_uuid.reset(token)
    assert resp.status_code == 404
    assert resp.error == cm.ERROR_ORG_NOT_FOUND
