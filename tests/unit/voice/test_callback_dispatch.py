"""MPC status-callback failures: the reply, the log line and whether the dedupe claim is released."""

import json

import pytest

import hs.voice.dispatch as dispatch

IDS = "eventName=MPCInitialized callUUID=call-1"


class _Session:
    def __init__(self):
        self.info = {}

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


def _post(monkeypatch, handler):
    released, logged = [], []
    monkeypatch.setattr(dispatch, "SessionLocal", _Session)
    monkeypatch.setattr(dispatch.voice_callbacks, "update_voice_event_time_in_cache", lambda *a: None)
    monkeypatch.setattr(dispatch.voice_callbacks, "release_voice_event_claim", lambda *a: released.append(a))
    monkeypatch.setattr(dispatch.voice_callbacks, "handle_initialised_callback", handler)
    monkeypatch.setattr(dispatch.logger, "error", lambda msg, *a, **k: logged.append(msg))
    return dispatch.mpc_status_callback("call-1", "qnr-1", {"EventName": "MPCInitialized"}), released, logged


def test_handler_raise_releases_the_claim_and_answers_processing_failed(monkeypatch):
    def handler(*_):
        raise RuntimeError("boom")

    response, released, logged = _post(monkeypatch, handler)

    assert (response.status_code, json.loads(response.body)) == (503, "voice callback processing failed")
    assert len(released) == 1
    assert logged == [f"Voice callback handler failed {IDS}: boom"]


def test_after_commit_effect_failure_keeps_the_claim(monkeypatch):
    def handler(_call, _qnr, _params, db):
        dispatch.voice_callbacks.stage_after_commit_effect(db, lambda: 1 / 0)
        return "ok", None

    response, released, logged = _post(monkeypatch, handler)

    assert (response.status_code, json.loads(response.body)) == (503, "voice callback after-commit effect failed")
    assert released == []
    assert logged[-2:] == [
        f"Voice callback after-commit effect failed; retaining exact claim {IDS}: "
        "1 voice callback after-commit effect(s) failed",
        f"Voice callback reached a non-replay-safe phase; retaining exact claim {IDS} phase=after_commit effects=()",
    ]


def test_a_call_status_callback_logs_ids_not_phone_numbers(monkeypatch):
    lines = []
    for level in ("info", "warning", "error"):
        monkeypatch.setattr(dispatch.logger, level, lambda msg, *a, **k: lines.append(str(msg)))
    body = {"EventName": "Ringing", "CallUUID": "call-1", "From": "+14155550100", "To": "+14155550199"}

    dispatch.mpc_call_status_callback("call-1", "qnr-1", body)

    assert lines and all("4155550100" not in line and "4155550199" not in line for line in lines)


def test_two_legs_of_one_member_ending_in_one_second_are_both_handled(monkeypatch):
    claims, handled = set(), []

    def claim(call_uuid, qnr_run_uuid, details):
        taken = details.event_name in claims
        claims.add(details.event_name)
        return dispatch.vc.VALUE_EXISTS_ERROR if taken else None

    monkeypatch.setattr(dispatch, "SessionLocal", _Session)
    monkeypatch.setattr(dispatch.voice_callbacks, "update_voice_event_time_in_cache", claim)
    monkeypatch.setattr(
        dispatch.voice_hangup,
        "handle_agent_hangup_event",
        lambda c, q, p, db: handled.append(p["call_uuid"]) or ("", None),
    )
    for leg in ("lost-invite", "transfer", "transfer"):  # a lost offer, then a transfer leg Plivo delivers twice
        body = {"EventName": "Hangup", "CallUUID": leg, "To": "sip:a1@x", "HangupTime": "2026-10-31 00:19:42"}
        dispatch.mpc_call_status_callback("call-1", "qnr-1", body)
    assert handled == ["lost-invite", "transfer"]


@pytest.mark.parametrize("failing", ["queue row", "call leg"])
def test_a_waiting_callers_exit_whose_write_fails_is_retried_and_closes_once(monkeypatch, failing):
    from contextlib import contextmanager
    from types import SimpleNamespace

    from sqlalchemy.exc import OperationalError

    from hs.voice import controls

    claims, committed, closed = set(), [], []
    writes = iter([OperationalError("UPDATE active_queue_entry", {}, Exception("db down"))] * (failing == "queue row"))
    legs = iter([dispatch.voice_hangup.CALL_LEG_PERSISTENCE_RETRYABLE] * (failing == "call leg"))

    def claim(call_uuid, qnr_run_uuid, details):
        taken = (call_uuid, details.event_name) in claims
        claims.add((call_uuid, details.event_name))
        return dispatch.vc.VALUE_EXISTS_ERROR if taken else None

    @contextmanager
    def begin():
        tx = []
        yield tx
        if failure := next(writes, None):
            raise failure
        committed.extend(tx)

    def close_row(db, contact_uuid, qnr_run_uuid, status):
        if f"{contact_uuid} closed" in committed:
            return 0
        db.append(f"{contact_uuid} closed")
        return 41

    redis = SimpleNamespace(get_contact_to_agent_mapping=lambda c: None, get_contact_parameters=lambda c, q: {})
    monkeypatch.setattr(dispatch.voice_callbacks, "update_voice_event_time_in_cache", claim)
    monkeypatch.setattr(
        dispatch.voice_callbacks, "release_voice_event_claim", lambda c, q, d: claims.discard((c, d.event_name))
    )
    monkeypatch.setattr(dispatch.voice_hangup, "reconcile_terminal_call_leg", lambda *a, **k: next(legs, None))
    monkeypatch.setattr(controls, "RoutingRedis", lambda: redis)
    monkeypatch.setattr("hs.infra.db.SessionLocal.begin", begin)
    monkeypatch.setattr("hs.routing.active_queue.delete_active_queue_entry", close_row)
    monkeypatch.setattr("hs.conversations.store.ConversationParent.get_by_conversation_number", lambda n: "parent")
    monkeypatch.setattr("hs.conversations.store.emit", lambda t, parent, data, session=None: session.append(t))
    monkeypatch.setattr(controls.voice_callbacks, "_update_unanswered", lambda c, r, q: closed.append((c, r, q)))
    body = {"EventName": "ParticipantExit", "ParticipantRole": "customer", "ParticipantExitCause": "Hangup"}
    replies = [dispatch.mpc_status_callback("call-1", "qnr-1", body).status_code for _ in range(3)]
    assert (replies, committed, closed) == (
        [503, 200, 200],
        ["call-1 closed", "call.ended"],
        [("call-1", "abandoned", 41)],
    )
    timeout = body | {"ParticipantExitCause": "Member Wait Timeout Triggered"}
    assert dispatch.mpc_status_callback("call-2", "qnr-1", timeout).status_code == 200 and len(committed) == 2
