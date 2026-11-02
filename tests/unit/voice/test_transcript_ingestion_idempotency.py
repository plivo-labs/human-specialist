"""Idempotent transcript persistence for overlapping callback and job delivery."""

from types import SimpleNamespace

import hs.voice.transcription_store as store
import tests.conftest  # noqa: F401


class _Query:
    def __init__(self, rows):
        self.rows = rows

    def filter(self, *args):
        return self

    def all(self):
        return self.rows


class _DB:
    def __init__(self, rows=(), dialect="postgresql"):
        self.rows = list(rows)
        self.dialect = SimpleNamespace(name=dialect)
        self.executed = []
        self.added = []
        self.flushed = 0

    def get_bind(self):
        return SimpleNamespace(dialect=self.dialect)

    def execute(self, statement, params):
        self.executed.append((str(statement), params))

    def query(self, *columns):
        return _Query(self.rows)

    def add_all(self, rows):
        self.added.extend(rows)

    def flush(self):
        self.flushed += 1


def _segment(start, end, text, absolute):
    return SimpleNamespace(start_time=start, end_time=end, segment=text, start_ts=absolute)


def test_recording_identity_uses_transaction_scoped_postgres_lock(monkeypatch):
    db = _DB()
    monkeypatch.setattr(store, "get_db_session", lambda: db)

    store.Transcription.acquire_ingestion_lock("rec-1")

    assert len(db.executed) == 1
    statement, params = db.executed[0]
    assert "pg_advisory_xact_lock" in statement
    assert params == {"recording_uuid": "rec-1"}


def test_non_postgres_unit_store_skips_advisory_statement(monkeypatch):
    db = _DB(dialect="sqlite")
    monkeypatch.setattr(store, "get_db_session", lambda: db)

    store.Transcription.acquire_ingestion_lock("rec-1")

    assert db.executed == []


def test_retry_inserts_only_missing_segments(monkeypatch):
    existing = [_segment(1.0, 2.0, "hello", 1001.0)]
    duplicate = _segment(1, 2, "hello", 1001)
    new = _segment(2.0, 3.0, "world", 1002.0)
    db = _DB(rows=existing)
    monkeypatch.setattr(store, "get_db_session", lambda: db)

    inserted = store.TranscriptionSegment.create_missing_transcript_segments(7, [duplicate, new])

    assert inserted == 1
    assert db.added == [new]
    assert db.flushed == 1


def test_duplicate_segments_inside_one_provider_batch_insert_once(monkeypatch):
    first = _segment(1.0, 2.0, "hello", 1001.0)
    repeated = _segment(1.0, 2.0, "hello", 1001.0)
    db = _DB()
    monkeypatch.setattr(store, "get_db_session", lambda: db)

    inserted = store.TranscriptionSegment.create_missing_transcript_segments(7, [first, repeated])

    assert inserted == 1
    assert db.added == [first]
    assert db.flushed == 1


def test_fully_replayed_transcript_is_a_noop(monkeypatch):
    existing = [_segment(1.0, 2.0, "hello", 1001.0)]
    db = _DB(rows=existing)
    monkeypatch.setattr(store, "get_db_session", lambda: db)

    inserted = store.TranscriptionSegment.create_missing_transcript_segments(7, [_segment(1.0, 2.0, "hello", 1001.0)])

    assert inserted == 0
    assert db.added == []
    assert db.flushed == 0
