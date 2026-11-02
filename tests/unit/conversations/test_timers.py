"""The timer and async-task jobs: scheduling, the handler context, and the retry contract of each
handler (raise to retry, return when done).
"""

from types import SimpleNamespace as Row

import pytest

import hs.voice.callbacks as vcb
import tests.conftest  # noqa: F401
from hs.conversations import timers
from hs.infra.context import context_aom_uuid, context_org_uuid
from hs.infra.models import UseCaseResponse
from hs.voice import transcripts as tm

MISSED = {"callUUID": "c-1", "qnrRunUUID": "q-1", "eventName": "MPCInitialized"}


def test_a_handler_runs_with_the_payloads_actor(monkeypatch):
    seen = []
    monkeypatch.setattr(
        "hs.conversations.management.handle_conversation_wrapup_timeout",
        lambda number: seen.append((number, context_aom_uuid.get(), context_org_uuid.get())),
    )
    timers.wrap_up_timeout({"conversation_number": "CN1", "aom_uuid": "aom-1", "org_uuid": "org-1", "org_name": "o"})
    assert seen == [("CN1", "aom-1", "org-1")]


def test_the_session_timeout_is_keyed_per_conversation_channel_and_agent(monkeypatch, scheduled):
    monkeypatch.setattr(timers.OrgSetting, "get_conversation_session_timeout_from_aom", classmethod(lambda c, **k: 60))
    timers.schedule_session_timeout("CN1", "call", "aom-1", "org-1", "o", extended=True)
    [(kind, payload, kw)] = scheduled
    assert (kind, kw) == ("session_timeout", {"delay": 60, "key": "CN1:call:aom-1"})
    assert payload["extended"] is True and payload["aom_uuid"] == "aom-1"
    monkeypatch.setattr(
        timers.OrgSetting, "get_conversation_session_timeout_from_aom", classmethod(lambda c, **k: None)
    )
    timers.schedule_session_timeout("CN1", "call", "aom-1", "org-1", "o")
    assert scheduled[-1][2]["delay"] == timers.DEFAULT_CALL_SESSION_TIMEOUT


@pytest.mark.parametrize(("mode", "buffer"), [("auto", 0), ("manual", timers.BUFFER_TIME)])
def test_the_wrap_up_closes_at_its_deadline_in_auto_mode_and_a_safety_buffer_later_in_manual(
    monkeypatch, scheduled, emitted, mode, buffer
):
    settings = Row(wrap_up_configuration={"mode": mode, "seconds": 30, "max_extend_sec": 20})
    monkeypatch.setattr(timers.RoutingSettings, "get_by_org_id", classmethod(lambda cls, org: settings))
    conversation = Row(parent_conversation=Row(organization=1, uuid="p-1", conversation_number="CN1"))
    timers.start_wrap_up(conversation, "aom-1", "org-1", "o")
    [(kind, payload, kw)] = scheduled
    [(_, data, _)] = emitted
    assert (kind, kw["key"], data["mode"]) == ("wrap_up_timeout", "CN1:aom-1", mode)
    assert (kw["run_at"] - data["deadline_at"]).total_seconds() == buffer
    assert (payload["mode"], payload["max_extend_sec"], payload["extended"]) == (mode, 20, 0)


def test_capacity_clearance_is_a_pure_release(monkeypatch):
    runs = []
    monkeypatch.setattr(
        "hs.conversations.management.PostConversationStatusChange.run_wrap_up_conversation_inprocess",
        staticmethod(runs.append),
    )
    timers.capacity_clearance({"conversation_number": "CN1", "send_conversation_closed_event": True})
    assert runs == [
        {"conversation_number": "CN1", "send_conversation_closed_event": False, "release_agent_capacity": True}
    ]


class _Session:
    def __init__(self, calls, commit_error=None):
        self.info, self.calls, self.commit_error = {}, calls, commit_error

    def commit(self):
        if self.commit_error:
            raise self.commit_error
        self.calls.append("commit")

    def rollback(self):
        self.calls.append("rollback")

    def close(self):
        self.calls.append("close")


def _missed(monkeypatch, usecase, commit_error=None):
    calls = []
    monkeypatch.setattr(timers, "SessionLocal", lambda: _Session(calls, commit_error))
    monkeypatch.setattr(vcb, "handle_missed_mpc_event", usecase)
    monkeypatch.setattr(vcb, "release_voice_event_claim", lambda c, q, d: calls.append(("release", c, q, d.timestamp)))
    return calls


def test_missed_mpc_effects_run_only_after_the_commit(monkeypatch):
    def handled(payload, db, claimed_event_out):
        vcb.stage_after_commit_effect(db, lambda: calls.append("publish"))

    calls = _missed(monkeypatch, handled)
    timers.missed_mpc_event(MISSED)
    assert calls == ["commit", "publish", "close"]


def test_a_permanent_missed_mpc_failure_keeps_its_claim(monkeypatch):
    calls = _missed(monkeypatch, lambda payload, db, claimed_event_out: "error in fetching call parameters")
    timers.missed_mpc_event(MISSED)
    assert calls == ["commit", "close"]


def test_a_retryable_missed_mpc_failure_releases_its_claim_and_retries(monkeypatch):
    def failed(payload, db, claimed_event_out):
        claimed_event_out.append(vcb.EventDetails("MPCInitialized", timestamp="claim-1"))
        return vcb.voice_hangup.retryable_voice_callback_error("provider unavailable")

    calls = _missed(monkeypatch, failed)
    with pytest.raises(RuntimeError, match="provider unavailable"):
        timers.missed_mpc_event(MISSED)
    assert calls == ["rollback", ("release", "c-1", "q-1", "claim-1"), "close"]


def test_a_failed_missed_mpc_commit_releases_its_claim_and_retries(monkeypatch):
    def handled(payload, db, claimed_event_out):
        claimed_event_out.append(vcb.EventDetails("MPCInitialized", timestamp="claim-2"))

    calls = _missed(monkeypatch, handled, commit_error=RuntimeError("commit unavailable"))
    with pytest.raises(RuntimeError, match="commit unavailable"):
        timers.missed_mpc_event(MISSED)
    assert calls == ["rollback", ("release", "c-1", "q-1", "claim-2"), "close"]


TRANSCRIPT = {"auth_id": "A", "call_uuid": "c-1", "recording_uuid": "r-1", "call_leg_uuid": "leg-1"}


def _transcript(monkeypatch, status=None, error=None):
    ready = []
    monkeypatch.setattr(timers.vrec, "_transcript_ready", ready.append)
    monkeypatch.setattr(tm, "read_stashed_callback_transcription", lambda r: None)
    monkeypatch.setattr(tm, "read_stashed_callback_text", lambda r: None)
    monkeypatch.setattr(tm, "create_transcripts", lambda req, **kw: UseCaseResponse(status_code=status, error=error))
    return ready


@pytest.mark.parametrize(
    "error", [tm.MSG_CREATE_TRANSCRIPT_ERROR_NO_TRANSCRIPT, tm.MSG_CREATE_TRANSCRIPT_ERROR_NO_RECORDING]
)
def test_a_transcript_not_there_yet_retries(monkeypatch, error):
    _transcript(monkeypatch, 404, error)
    with pytest.raises(RuntimeError):
        timers.transcript_attach(TRANSCRIPT)


@pytest.mark.parametrize(
    ("status", "error"),
    [
        (201, None),
        (422, tm.MSG_CREATE_TRANSCRIPT_EMPTY),
        (422, tm.MSG_CREATE_TRANSCRIPT_ERROR_DUPLICATE_TRANSCRIPT),
        (422, "permanent failure"),
    ],
)
def test_a_final_transcript_outcome_is_done(monkeypatch, status, error):
    ready = _transcript(monkeypatch, status, error)
    timers.transcript_attach(TRANSCRIPT)
    # a stored transcript, an empty one included, may complete its recording
    assert ready == (["r-1"] if status == 201 or error == tm.MSG_CREATE_TRANSCRIPT_EMPTY else [])
