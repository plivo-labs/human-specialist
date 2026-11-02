"""Transcript readiness — durable retry handoff at the provider callback.
A recording-persistence race or a provider-not-ready transcript fetch must be handed to a
durable retry before Plivo gets a 200. These tests pin the contract:
* both transient states are synchronously handed to the transcript job;
* Plivo receives success only after the rail acknowledges the message;
* enqueue/commit failures surface the 503 retry error;
* intentional MPC-level callbacks are terminal success; and
* malformed/permanent failures are never retried.
"""

import pytest

import hs.jobs
import hs.voice.recording as vrec
import hs.voice.transcripts as tm
import tests.conftest  # noqa: F401
from hs.infra.models import UseCaseResponse

CT = "11111111-2222-3333-4444-555555555555"
NO_RECORDING = tm.MSG_CREATE_TRANSCRIPT_ERROR_NO_RECORDING
NO_TRANSCRIPT = tm.MSG_CREATE_TRANSCRIPT_ERROR_NO_TRANSCRIPT
MPC_IGNORED = tm.MSG_CREATE_TRANSCRIPT_ERROR_DUPLICATE_TRANSCRIPT


def _quiet_plumbing(monkeypatch, commit_error=None):
    import hs.voice.hangup as vh
    import hs.voice.monitor as vm

    monkeypatch.setattr(vm, "_seed_context", lambda meta: None)
    monkeypatch.setattr(vh, "_commit_session", lambda: commit_error)


def _response(monkeypatch, status_code, error=None):
    monkeypatch.setattr(
        tm, "create_transcripts", lambda req, **kw: UseCaseResponse(status_code=status_code, error=error)
    )


@pytest.mark.parametrize("transient", [NO_RECORDING, NO_TRANSCRIPT])
def test_transient_failure_is_durably_enqueued_before_ack(monkeypatch, transient):
    _quiet_plumbing(monkeypatch)
    _response(monkeypatch, 404, transient)
    enqueued = []
    monkeypatch.setattr(hs.jobs, "schedule", lambda kind, kwargs, **_: enqueued.append(kwargs) or "job-1")

    _s, err = vrec.handle_transcription_event(CT, "AUTH1", {"recording_id": "rec-1", "call_uuid": "leg-1"})

    assert err is None
    assert enqueued == [{"auth_id": "AUTH1", "call_uuid": CT, "recording_uuid": "rec-1", "call_leg_uuid": "leg-1"}]


def test_enqueue_failure_surfaces_provider_retry_error(monkeypatch):
    _quiet_plumbing(monkeypatch)
    _response(monkeypatch, 404, NO_TRANSCRIPT)
    monkeypatch.setattr(hs.jobs, "schedule", lambda kind, kwargs, **_: (_ for _ in ()).throw(RuntimeError("jobs down")))

    _s, err = vrec.handle_transcription_event(CT, "AUTH1", {"recording_id": "rec-2", "call_uuid": "leg-2"})

    assert err == vrec.ERR_TRANSCRIPTION_RETRY_UNAVAILABLE


def test_commit_failure_is_never_acknowledged(monkeypatch):
    _quiet_plumbing(monkeypatch, commit_error="writer unavailable")
    _response(monkeypatch, 201)
    enqueued = []
    monkeypatch.setattr(hs.jobs, "schedule", lambda kind, kwargs, **_: enqueued.append(kwargs))

    _s, err = vrec.handle_transcription_event(CT, "AUTH1", {"recording_id": "rec-3", "call_uuid": "leg-3"})

    assert err == vrec.ERR_TRANSCRIPTION_RETRY_UNAVAILABLE
    assert enqueued == []


def test_processing_exception_rolls_back_before_provider_retry(monkeypatch):
    import hs.infra.context as context
    import hs.voice.hangup as vh
    import hs.voice.monitor as vm

    monkeypatch.setattr(vm, "_seed_context", lambda meta: None)
    boundaries = []
    monkeypatch.setattr(tm, "create_transcripts", lambda req, **kw: (_ for _ in ()).throw(RuntimeError("flush failed")))
    monkeypatch.setattr(context, "set_db_session_rollback", lambda: boundaries.append("rollback"))
    monkeypatch.setattr(vh, "_commit_session", lambda: boundaries.append("boundary") or None)

    _s, err = vrec.handle_transcription_event(CT, "AUTH1", {"recording_id": "rec-partial", "call_uuid": "leg-partial"})

    assert err == vrec.ERR_TRANSCRIPTION_RETRY_UNAVAILABLE
    assert boundaries == ["rollback", "boundary"]


def test_success_is_acknowledged_without_enqueue(monkeypatch):
    _quiet_plumbing(monkeypatch)
    _response(monkeypatch, 201)
    enqueued = []
    monkeypatch.setattr(hs.jobs, "schedule", lambda kind, kwargs, **_: enqueued.append(kwargs))

    assert vrec.handle_transcription_event(CT, "AUTH1", {"recording_id": "rec-4", "call_uuid": "leg-4"}) == ("", None)
    assert enqueued == []


def test_empty_provider_transcription_is_terminal_ack_without_enqueue(monkeypatch):
    """Zero-segment final transcriptions must not enter the retry ladder at all."""
    _quiet_plumbing(monkeypatch)
    _response(monkeypatch, 404, tm.MSG_CREATE_TRANSCRIPT_EMPTY)
    enqueued, ready = [], []
    monkeypatch.setattr(hs.jobs, "schedule", lambda kind, kwargs, **_: enqueued.append(kwargs))
    monkeypatch.setattr(vrec, "_transcript_ready", ready.append)

    assert vrec.handle_transcription_event(CT, "AUTH1", {"recording_id": "rec-empty", "call_uuid": "leg-empty"}) == (
        "",
        None,
    )
    assert enqueued == []
    assert ready == ["rec-empty"]  # a silent leg may be the last one its recording waited for


def test_usecase_classifies_zero_segments_as_terminal_empty(monkeypatch):
    """Provider 200 + empty transcription array => terminal EMPTY; provider 404
    stays NO_TRANSCRIPT (retryable). The split keeps the retry ladder for genuine
    not-ready races only."""
    from types import SimpleNamespace as NS

    monkeypatch.setattr(
        tm.Organization, "get_org_by_auth_id", staticmethod(lambda auth_id: NS(auth_id="A", uuid="t-1"))
    )
    monkeypatch.setattr("hs.routing.account_read.tenant_plivo", lambda tenant_id: ("A", "T"))
    monkeypatch.setattr(
        tm.Recording,
        "get_by_recording_uuid",
        staticmethod(lambda uuid, with_call_leg=False: NS(call_leg=NS(parent_call_leg=7))),
    )
    monkeypatch.setattr(tm, "get_db_session", lambda: NS(execute=lambda *a: NS(first=lambda: (1,))))

    monkeypatch.setattr(tm, "_fetch_plivo_transcription", lambda a, t, r: NS(transcription=[]))
    resp = tm.create_transcripts(
        tm.TranscriptCreateRequestModel(auth_id="A", recording_uuid="rec-e", call_leg_uuid="leg-e")
    )
    assert (resp.status_code, resp.error) == (404, tm.MSG_CREATE_TRANSCRIPT_EMPTY)

    def _not_found(a, t, r):
        raise tm._PlivoNotFound("transcription_id: rec-e not found")

    monkeypatch.setattr(tm, "_fetch_plivo_transcription", _not_found)
    resp = tm.create_transcripts(
        tm.TranscriptCreateRequestModel(auth_id="A", recording_uuid="rec-e", call_leg_uuid="leg-e")
    )
    assert (resp.status_code, resp.error) == (404, NO_TRANSCRIPT)


def test_mpc_level_transcript_is_intentional_terminal_success(monkeypatch):
    _quiet_plumbing(monkeypatch)
    _response(monkeypatch, 422, MPC_IGNORED)
    enqueued = []
    monkeypatch.setattr(hs.jobs, "schedule", lambda kind, kwargs, **_: enqueued.append(kwargs))

    assert vrec.handle_transcription_event(CT, "AUTH1", {"recording_id": "rec-main", "call_uuid": "leg-main"}) == (
        "",
        None,
    )
    assert enqueued == []


def test_permanent_failure_is_not_retried_or_enqueued(monkeypatch):
    _quiet_plumbing(monkeypatch)
    _response(monkeypatch, 422, tm.MSG_CREATE_TRANSCRIPT_ERROR_NO_CALL_LEG)
    enqueued = []
    monkeypatch.setattr(hs.jobs, "schedule", lambda kind, kwargs, **_: enqueued.append(kwargs))

    _s, err = vrec.handle_transcription_event(CT, "AUTH1", {"recording_id": "rec-5", "call_uuid": "leg-5"})

    assert err == vrec.ERR_TRANSCRIPTION_INTEGRATION
    assert enqueued == []


def test_missing_provider_identifiers_are_rejected_before_usecase(monkeypatch):
    _quiet_plumbing(monkeypatch)
    called = []
    monkeypatch.setattr(tm, "create_transcripts", lambda req, **kw: called.append(req))

    assert vrec.handle_transcription_event(CT, "AUTH1", {}) == ("", vrec.ERR_TRANSCRIPTION_INTEGRATION)
    assert called == []
