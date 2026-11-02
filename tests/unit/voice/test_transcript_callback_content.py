"""Transcript callback content and retry-stash tests."""

import json
from types import SimpleNamespace

import pytest

import hs.jobs
import hs.voice.recording as vrec
import hs.voice.transcripts as tm
import tests.conftest  # noqa: F401
from hs.conversations import timers
from hs.infra.models import UseCaseResponse

CT = "11111111-2222-3333-4444-555555555555"
SEGMENTS = [{"start": 0.5, "end": 2.25, "text": "hello there"}, {"start": 2.5, "end": 4.0, "text": "goodbye"}]


class _Row:
    def __init__(self, **kw):
        self.__dict__.update(kw)


def _quiet_plumbing(monkeypatch):
    import hs.voice.hangup as vh
    import hs.voice.monitor as vm

    monkeypatch.setattr(vm, "_seed_context", lambda meta: None)
    monkeypatch.setattr(vh, "_commit_session", lambda: None)


@pytest.mark.parametrize(
    "value", [json.dumps({"transcription": SEGMENTS}), {"transcription": SEGMENTS}, json.dumps(SEGMENTS), SEGMENTS]
)
def test_parser_accepts_every_shape_the_callback_can_deliver(value):
    """Callback content may arrive in several JSON shapes."""
    segments = tm.parse_callback_transcription(value)
    assert [(s.start, s.end, s.text) for s in segments] == [(0.5, 2.25, "hello there"), (2.5, 4.0, "goodbye")]


def test_parser_coerces_string_bounds():
    """Segment bounds are coerced to floats."""
    (seg,) = tm.parse_callback_transcription([{"start": "1", "end": "2", "text": "hi"}])
    assert (seg.start, seg.end) == (1.0, 2.0)
    assert isinstance(seg.start, float)


@pytest.mark.parametrize("value", [None, "", "   ", "not json", 42, {"no_transcription_key": 1}, [{"start": 0}]])
def test_parser_returns_none_for_anything_unusable(value):
    """Unusable content falls back to provider fetch."""
    assert tm.parse_callback_transcription(value) is None


@pytest.mark.parametrize("value", [[], {"transcription": []}, {"transcription": None}, "[]"])
def test_parser_distinguishes_zero_segments_from_absent(value):
    """An empty segment list is a terminal result."""
    assert tm.parse_callback_transcription(value) == []


def _tm_env(monkeypatch, recording=None, fetch=None):
    from hs.admin.account import Organization
    from hs.conversations.store import Recording
    from hs.voice.call_store import CallLeg
    from hs.voice.transcription_store import Transcription, TranscriptionSegment

    monkeypatch.setattr(Organization, "get_org_by_auth_id", staticmethod(lambda a: _Row(auth_id="A", uuid="t-1", id=1)))
    monkeypatch.setattr("hs.routing.account_read.tenant_plivo", lambda tenant_id: ("A", "T"))
    monkeypatch.setattr(Recording, "get_by_recording_uuid", classmethod(lambda cls, u, with_call_leg=False: recording))
    monkeypatch.setattr(tm, "get_db_session", lambda: _Row(execute=lambda *a: _Row(first=lambda: (1,))))
    monkeypatch.setattr(CallLeg, "get_by_call_uuid", classmethod(lambda cls, u, **kw: _Row(id=7)))
    monkeypatch.setattr(Transcription, "get_or_create_transcript", classmethod(lambda cls, i: _Row(id=3)))
    monkeypatch.setattr(Transcription, "acquire_ingestion_lock", classmethod(lambda cls, u: None))
    monkeypatch.setattr(
        TranscriptionSegment, "create_missing_transcript_segments", classmethod(lambda cls, tid, segs: len(segs))
    )
    fetched = []

    def _fetch(auth_id, auth_token, recording_uuid):
        fetched.append(recording_uuid)
        if fetch is None:
            raise tm._PlivoNotFound("transcription_id not found")
        return fetch

    monkeypatch.setattr(tm, "_fetch_plivo_transcription", _fetch)
    return fetched


def _req():
    return tm.TranscriptCreateRequestModel(auth_id="A", recording_uuid="rec-1", call_leg_uuid="leg-1")


def _recording():
    return _Row(call_leg=_Row(parent_call_leg=3), recording_start_time="2026-08-23 10:00:00")


def test_content_attaches_without_touching_the_provider_api(monkeypatch):
    """Supplied content avoids a provider fetch."""
    fetched = _tm_env(monkeypatch, recording=_recording())
    resp = tm.create_transcripts(_req(), transcription_segments=tm.parse_callback_transcription(SEGMENTS))
    assert resp.status_code == 201
    assert fetched == []


def test_absent_content_still_fetches(monkeypatch):
    """Absent content falls back to provider fetch."""
    fetched = _tm_env(monkeypatch, recording=_recording(), fetch=tm._SDKResponseObject({"transcription": []}))
    resp = tm.create_transcripts(_req(), transcription_segments=None)
    assert fetched == ["rec-1"]
    assert resp.error == tm.MSG_CREATE_TRANSCRIPT_EMPTY


def test_zero_segment_content_is_terminal_and_never_fetches(monkeypatch):
    fetched = _tm_env(monkeypatch, recording=_recording())
    resp = tm.create_transcripts(_req(), transcription_segments=[])
    assert (resp.status_code, resp.error) == (404, tm.MSG_CREATE_TRANSCRIPT_EMPTY)
    assert fetched == []


def test_content_path_still_reports_the_recording_race(monkeypatch):
    """Missing recording remains retryable even when content is supplied."""
    _tm_env(monkeypatch, recording=None)
    resp = tm.create_transcripts(_req(), transcription_segments=[_Row(start=0, end=1, text="x")])
    assert resp.error == tm.MSG_CREATE_TRANSCRIPT_ERROR_NO_RECORDING


def _capture_create(monkeypatch, status_code=201, error=None):
    seen = {}

    def _create(req, transcription_segments=None, voicemail=False):
        seen["segments"] = transcription_segments
        return UseCaseResponse(status_code=status_code, error=error)

    monkeypatch.setattr(tm, "create_transcripts", _create)
    return seen


def test_callback_content_reaches_create_transcripts(monkeypatch):
    _quiet_plumbing(monkeypatch)
    seen = _capture_create(monkeypatch)
    _s, err = vrec.handle_transcription_event(
        CT, "AUTH1", {"recording_id": "rec-1", "call_uuid": "leg-1", "transcription": json.dumps(SEGMENTS)}
    )
    assert err is None
    assert [(s.start, s.text) for s in seen["segments"]] == [(0.5, "hello there"), (2.5, "goodbye")]


def test_callback_without_content_passes_none(monkeypatch):
    _quiet_plumbing(monkeypatch)
    seen = _capture_create(monkeypatch)
    vrec.handle_transcription_event(CT, "AUTH1", {"recording_id": "rec-1", "call_uuid": "leg-1"})
    assert seen["segments"] is None


def test_unparseable_content_falls_back_to_the_fetch(monkeypatch):
    _quiet_plumbing(monkeypatch)
    seen = _capture_create(monkeypatch)
    vrec.handle_transcription_event(
        CT, "AUTH1", {"recording_id": "rec-1", "call_uuid": "leg-1", "transcription": "<<garbage>>"}
    )
    assert seen["segments"] is None


class _FakeCache:
    def __init__(self):
        self.store = {}
        self.deleted = []
        self.ttls = {}

    def set(self, key, value, expiry_in_seconds=None, nx=False):
        self.store[key] = (value, expiry_in_seconds)
        return True

    def get(self, key):
        got = self.store.get(key)
        return got[0] if got else None

    def delete(self, key):
        self.deleted.append(key)
        self.store.pop(key, None)

    def ttl(self, key):
        return self.ttls.get(key)


@pytest.fixture()
def cache(monkeypatch):
    from hs.infra.redis import Cache

    fake = _FakeCache()
    monkeypatch.setattr(Cache, "get_instance", staticmethod(lambda: fake))
    return fake


def test_race_stashes_content_before_enqueueing(monkeypatch, cache):
    """Callback content must be stashed before the worker is enqueued."""
    _quiet_plumbing(monkeypatch)
    _capture_create(monkeypatch, 404, tm.MSG_CREATE_TRANSCRIPT_ERROR_NO_RECORDING)
    order = []
    monkeypatch.setattr(cache, "set", lambda k, v, **kw: order.append("stash") or cache.store.__setitem__(k, (v, kw)))
    monkeypatch.setattr(hs.jobs, "schedule", lambda kind, kw, **_: order.append(kw) or "job-1")

    _s, err = vrec.handle_transcription_event(
        CT, "AUTH1", {"recording_id": "rec-1", "call_uuid": "leg-1", "transcription": json.dumps(SEGMENTS)}
    )

    assert err is None
    assert order[0] == "stash"
    assert "transcription" not in order[1]
    assert order[1]["recording_uuid"] == "rec-1"


def test_async_retry_picks_the_content_up_from_the_stash(monkeypatch, cache):
    from hs.infra.context import context_api_id

    tm.stash_callback_transcription("rec-1", SEGMENTS)
    seen = _capture_create(monkeypatch)
    context_api_id.set("t-transcript")

    resp = _attach({"auth_id": "AUTH1", "call_uuid": CT, "recording_uuid": "rec-1", "call_leg_uuid": "leg-1"})

    assert resp.status_code == 200
    assert [s.text for s in seen["segments"]] == ["hello there", "goodbye"]


def test_stash_survives_a_retry_instead_of_being_consumed(monkeypatch, cache):
    """Reading stashed content must not consume it."""
    tm.stash_callback_transcription("rec-1", SEGMENTS)
    first = tm.read_stashed_callback_transcription("rec-1")
    second = tm.read_stashed_callback_transcription("rec-1")
    assert [s.text for s in first] == [s.text for s in second] == ["hello there", "goodbye"]
    assert cache.deleted == []


def test_stash_carries_a_ttl_outlasting_the_retry_ladder(monkeypatch, cache):
    tm.stash_callback_transcription("rec-1", SEGMENTS)
    _value, ttl = cache.store[tm._callback_transcription_key("rec-1")]
    assert ttl == tm.CALLBACK_TRANSCRIPTION_STASH_TTL >= 3600


def test_stash_miss_falls_back_to_the_fetch(monkeypatch, cache):
    assert tm.read_stashed_callback_transcription("never-stashed") is None


def test_redis_failure_degrades_to_the_fetch_instead_of_raising(monkeypatch, cache):
    """A Redis blip must never break the provider acknowledgement or the retry."""
    monkeypatch.setattr(cache, "set", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("redis down")))
    monkeypatch.setattr(cache, "get", lambda k: (_ for _ in ()).throw(RuntimeError("redis down")))
    tm.stash_callback_transcription("rec-1", SEGMENTS)
    assert tm.read_stashed_callback_transcription("rec-1") is None


def test_content_sourced_attach_is_distinguishable_in_logs():
    import inspect

    src = inspect.getsource(tm.create_transcripts)
    assert "create_transcripts: attached from callback content " in src
    assert "create_transcripts: provider transcription fetched " in src


PLAIN_TEXT = "hello there goodbye"


def test_plain_text_callback_is_stashed_for_the_retry(monkeypatch, cache):
    """Plain-text callback content must be stashed for retry."""
    _quiet_plumbing(monkeypatch)
    _capture_create(monkeypatch, 404, tm.MSG_CREATE_TRANSCRIPT_ERROR_NO_RECORDING)
    monkeypatch.setattr(hs.jobs, "schedule", lambda kind, kw, **_: "job-1")

    _s, err = vrec.handle_transcription_event(
        CT, "AUTH1", {"recording_id": "rec-1", "call_uuid": "leg-1", "transcription": PLAIN_TEXT}
    )

    assert err is None
    assert tm.read_stashed_callback_text("rec-1") == PLAIN_TEXT


@pytest.mark.parametrize(
    "stashed, expected",
    [(PLAIN_TEXT, PLAIN_TEXT), ({"transcription": PLAIN_TEXT}, PLAIN_TEXT), (SEGMENTS, None), ("   ", None)],
)
def test_read_stashed_callback_text_shapes(cache, stashed, expected):
    tm.stash_callback_transcription("rec-1", stashed)
    assert tm.read_stashed_callback_text("rec-1") == expected


def test_readiness_window_is_measured_from_the_stash_not_the_recording_row(monkeypatch, cache):
    """Readiness timing is based on the callback-content stash."""
    import datetime as dt

    from hs.conversations.store import Recording

    old = dt.datetime.now(dt.UTC) - dt.timedelta(seconds=3600)
    monkeypatch.setattr(Recording, "get_by_recording_uuid", classmethod(lambda cls, u, **kw: _Row(created_at=old)))
    tm.stash_callback_transcription("rec-1", PLAIN_TEXT)
    key = tm._callback_transcription_key("rec-1")
    for stash_age, expected in ((30, False), (300, True)):
        cache.ttls[key] = tm.CALLBACK_TRANSCRIPTION_STASH_TTL - stash_age
        assert tm.provider_readiness_elapsed("rec-1") is expected
    cache.ttls[key] = -2
    assert tm.provider_readiness_elapsed("rec-1") is True
    monkeypatch.setattr(Recording, "get_by_recording_uuid", classmethod(lambda cls, u, **kw: None))
    assert tm.provider_readiness_elapsed("rec-1") is False


def _run_attach_with_provider_404(monkeypatch, cache, elapsed):
    """Run the transcript task with stashed text and a 404 provider."""
    from hs.infra.context import context_api_id

    monkeypatch.setattr(tm, "provider_readiness_elapsed", lambda recording_uuid: elapsed)
    tm.stash_callback_transcription("rec-1", PLAIN_TEXT)
    context_api_id.set("t-transcript-text")

    calls = []

    def _create(req, transcription_segments=None, voicemail=False):
        calls.append(transcription_segments)
        if transcription_segments is None or transcription_segments == []:
            return UseCaseResponse(status_code=404, error=tm.MSG_CREATE_TRANSCRIPT_ERROR_NO_TRANSCRIPT)
        return UseCaseResponse(status_code=201)

    monkeypatch.setattr(tm, "create_transcripts", _create)
    resp = _attach({"auth_id": "AUTH1", "call_uuid": CT, "recording_uuid": "rec-1", "call_leg_uuid": "leg-1"})
    return resp, calls


def test_text_fallback_attaches_after_the_readiness_window(monkeypatch, cache):
    resp, calls = _run_attach_with_provider_404(monkeypatch, cache, elapsed=True)
    assert resp.status_code == 200
    (segment,) = calls[1]
    assert (segment.start, segment.end, segment.text) == (0.0, 0.0, PLAIN_TEXT)


def test_text_fallback_waits_out_the_readiness_window(monkeypatch, cache):
    """A provider 404 remains retryable inside the readiness window."""
    resp, calls = _run_attach_with_provider_404(monkeypatch, cache, elapsed=False)
    assert resp.status_code == 503
    assert len(calls) == 1


def test_dropped_recording_transcript_is_acknowledged_not_retried(monkeypatch, cache):
    """A dropped recording never gets a row, so its transcription callback must not ride the ladder."""
    _quiet_plumbing(monkeypatch)
    px = []
    monkeypatch.setattr(vrec, "px_create_recording", lambda req: px.append(req) or (201, "", None))
    vrec.handle_participant_recording_event(CT, {"RecordingDuration": "0", "RecordingUUID": "rec-1"})
    assert px == []
    fetched = _tm_env(monkeypatch, recording=None)
    resp = tm.create_transcripts(_req())
    assert (resp.status_code, resp.error) == (404, tm.MSG_CREATE_TRANSCRIPT_DROPPED)
    assert fetched == []
    enqueued = []
    monkeypatch.setattr(hs.jobs, "schedule", lambda kind, kw, **_: enqueued.append(kw) or "job-1")
    assert vrec.handle_transcription_event(CT, "AUTH1", {"recording_id": "rec-1", "call_uuid": "leg-1"}) == ("", None)
    assert enqueued == []


def test_missing_recording_without_a_drop_marker_still_retries(monkeypatch, cache):
    _tm_env(monkeypatch, recording=None)
    resp = tm.create_transcripts(_req())
    assert resp.error == tm.MSG_CREATE_TRANSCRIPT_ERROR_NO_RECORDING


def test_rail_acks_a_dropped_recording(monkeypatch, cache):
    from hs.infra.context import context_api_id

    tm.mark_recording_skipped("rec-1")
    _tm_env(monkeypatch, recording=None)
    context_api_id.set("t-transcript")

    resp = _attach({"auth_id": "AUTH1", "call_uuid": CT, "recording_uuid": "rec-1", "call_leg_uuid": "leg-1"})

    assert resp.status_code == 200


def _attach(payload):
    """The transcript job's outcome as the old dispatcher's status: 200 done, 503 retried."""
    try:
        timers.transcript_attach(payload)
    except RuntimeError:
        return SimpleNamespace(status_code=503)
    return SimpleNamespace(status_code=200)


def test_a_record_callback_is_one_plain_text_segment():
    (seg,), voicemail = tm.callback_segments(
        {"transcription": " call me back ", "transcription_charge": "0", "duration": "7"}
    )
    assert (voicemail, seg.start, seg.end, seg.text) == (True, 0.0, 7.0, "call me back")
    assert tm.callback_segments({"transcription": PLAIN_TEXT})[1] is False
