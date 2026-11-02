"""CoreDBBase.get_by_uuid: a malformed identifier (a non-UUID string, e.g. a placeholder sender_id like "app-x")
previously reached Postgres' uuid cast -> InvalidTextRepresentation -> 500. It must short-circuit to a clean None
(no DB hit) for invalid input."""

from hs.infra.db import CoreDBBase


class _Dummy(CoreDBBase):
    pass


def test_get_by_uuid_malformed_returns_none_no_db():
    # Invalid identifier short-circuits to None BEFORE any DB session is opened.
    assert _Dummy.get_by_uuid("app-x") is None
    assert _Dummy.get_by_uuid("") is None
    assert _Dummy.get_by_uuid(None) is None
