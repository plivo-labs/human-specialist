"""Recording create status codes: 422 for both the not-found and duplicate/finalized cases,
400 for a missing call_uuid, 200 on success.
"""


# ---------------------------------------------------------------------------
# Minimal stubs — no DB, no Redis, no external deps
# ---------------------------------------------------------------------------


class _FakeCallLeg:
    """Minimal CallLeg row stub."""

    def __init__(self, id_=42):
        self.id = id_


class _FakeRecording:
    """Minimal Recording row stub (truthy — means a duplicate exists)."""


def _patch_create_recording(monkeypatch, *, call_leg=None, existing_recording=None, create_raises=None):
    """Wire create_recording data-layer stubs."""

    monkeypatch.setattr(
        "hs.voice.call_store.CallLeg.get_by_call_uuid", lambda call_uuid, participant_leg=False: call_leg
    )

    def _get_by_call_leg_id(call_leg_id):
        if create_raises:
            raise create_raises
        return existing_recording

    monkeypatch.setattr("hs.conversations.store.Recording.get_by_call_leg_id", _get_by_call_leg_id)

    def _create_recording(recording):
        if create_raises:
            raise create_raises

    monkeypatch.setattr("hs.conversations.store.Recording.create_recording", _create_recording)


def _make_request(**overrides):
    from hs.voice.call_models import CallRecordingRequestModel

    base = {
        "call_uuid": "cu-1",
        "call_leg_uuid": "clu-1",
        "recording_url": "https://example.com/rec.mp3",
        "recording_duration": "30",
        "recording_uuid": "ru-1",
        "recording_format": "mp3",
        "recording_start_time": "2026-01-01T00:00:00",
        "recording_end_time": "2026-01-01T00:00:30",
        "recording_type": "mpc",
    }
    base.update(overrides)
    return CallRecordingRequestModel.model_validate(base)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_call_leg_not_found_returns_422(monkeypatch):
    """When CallLeg.get_by_call_uuid returns None → 422."""
    _patch_create_recording(monkeypatch, call_leg=None)
    from hs.conversations import management as cm

    resp = cm.create_recording(_make_request())

    assert resp.status_code == 422
    assert resp.error == cm.ERROR_CALL_LEG_NOT_FOUND
    assert resp.msg is None


def test_duplicate_recording_returns_422(monkeypatch):
    """When a recording already exists for the call-leg → 422."""
    _patch_create_recording(monkeypatch, call_leg=_FakeCallLeg(), existing_recording=_FakeRecording())
    from hs.conversations import management as cm

    resp = cm.create_recording(_make_request())

    assert resp.status_code == 422
    assert resp.error == cm.ERROR_RECORDING_DUPLICATE
    assert resp.msg is None


def test_missing_call_uuid_returns_400(monkeypatch):
    """When both call_uuid and call_leg_uuid are absent-like → 400 unchanged."""
    # The guard checks `not call_uuid and not call_leg_uuid` — supply empty strings.
    # We patch nothing: the 400 branch returns before any DB call.
    from hs.voice.call_models import CallRecordingRequestModel

    req = CallRecordingRequestModel.model_validate(
        {
            "call_uuid": "",
            "call_leg_uuid": "",
            "recording_url": "https://example.com/rec.mp3",
            "recording_duration": "30",
            "recording_uuid": "ru-1",
            "recording_format": "mp3",
            "recording_start_time": "2026-01-01T00:00:00",
            "recording_end_time": "2026-01-01T00:00:30",
            "recording_type": "mpc",
        }
    )
    from hs.conversations import management as cm

    resp = cm.create_recording(req)

    assert resp.status_code == 400
    assert resp.error == cm.ERROR_CALL_UUID_OR_CALL_LEG_UUID_REQUIRED


def test_successful_create_returns_200(monkeypatch):
    """Happy path: new recording created → 200 with success message."""
    _patch_create_recording(monkeypatch, call_leg=_FakeCallLeg(), existing_recording=None)
    from hs.conversations import management as cm

    resp = cm.create_recording(_make_request())

    assert resp.status_code == 200
    assert resp.error is None
    assert resp.msg == cm.SUCCESS_RECORDING_CREATION


def test_creation_exception_returns_400(monkeypatch):
    """If Recording.create_recording raises → 400 (existing behavior, unchanged)."""
    _patch_create_recording(
        monkeypatch, call_leg=_FakeCallLeg(), existing_recording=None, create_raises=RuntimeError("db write failed")
    )
    from hs.conversations import management as cm

    resp = cm.create_recording(_make_request())

    assert resp.status_code == 400
    assert resp.error == cm.ERROR_RECORDING_CREATION_FAILED
