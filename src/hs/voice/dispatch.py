"""Plivo's MPC, call-status, recording and transcription callbacks: dispatch to the voice handlers and pick the reply."""

import functools
import json
import uuid

from fastapi import Response
from fastapi.responses import JSONResponse

from hs.infra.context import context_api_id, context_req_meta
from hs.infra.db import SessionLocal
from hs.infra.log import logger
from hs.voice import agent_leg as voice_agent_leg
from hs.voice import callbacks as voice_callbacks
from hs.voice import constants as vc
from hs.voice import hangup as voice_hangup
from hs.voice import recording as voice_recording


class _BareJSONResponse(JSONResponse):  # compact JSON and a trailing newline
    def render(self, content) -> bytes:
        return (json.dumps(content, ensure_ascii=False, allow_nan=False, separators=(",", ":")) + "\n").encode("utf-8")


_SERVER_ERROR = {"message": "Internal Server Error"}


def seed_context() -> None:
    api_id = str(uuid.uuid4())
    context_api_id.set(api_id)
    context_req_meta.set({"api_id": api_id})


_MPC_CALLBACK_FIELDS = {
    "DigitInput": "digit_input",
    "EventName": "event_name",
    "EventTimestamp": "event_timestamp",
    "MPCBilledAmount": "mpc_billed_amount",
    "MPCBilledDuration": "mpc_billed_duration",
    "MPCCreationTime": "mpc_creation_time",
    "MPCDuration": "mpc_duration",
    "MPCEndTime": "mpc_end_time",
    "MPCName": "mpc_name",
    "MPCStartTime": "mpc_start_time",
    "MPCTerminationCause": "mpc_termination_cause",
    "MPCTerminationCauseCode": "mpc_termination_cause_code",
    "IsMPCTerminationSource": "is_mpc_termination_source",
    "MPCUUID": "mpc_uuid",
    "MemberAddress": "member_address",
    "MemberID": "member_id",
    "ParticipantCallDirection": "participant_call_direction",
    "ParticipantCallFrom": "participant_call_from",
    "ParticipantCallTo": "participant_call_to",
    "ParticipantCallUUID": "participant_call_uuid",
    "ParticipantCoachMode": "participant_coach_mode",
    "ParticipantExitCause": "participant_exit_cause",
    "ParticipantExitTime": "participant_exit_time",
    "ParticipantJoinTime": "participant_join_time",
    "ParticipantRole": "participant_role",
    "SequenceNumber": "sequence_number",
    "AddParticipantAPIRequestOutcome": "add_participant_api_request_outcome",
    "AddParticipantAPIRequestId": "add_participant_api_request_id",
    "AddParticipantAPIRequestType": "add_participant_api_request_type",
    "AddParticipantAPICallUUIDs": "add_participant_api_call_uuids",
    "AddParticipantAPICallCount": "add_participant_api_call_count",
    "AddParticipantAPIRequestOutcomeReason": "add_participant_api_request_outcome_reason",
    "X-PH-Add_Participant": "add_participant_header",
    "X-PH-Participant_Alias": "add_participant_alias_name",
    "X-PH-HsKind": "hs_kind",
    "DigitsSent": "digits_sent",
}

_CS_CALLBACK_FIELDS = {
    "AnswerTime": "answer_time",
    "BilledDuration": "billed_duration",
    "CallUUID": "call_uuid",
    "Duration": "duration",
    "EventName": "event_name",
    "From": "from",
    "HangupTime": "hangup_time",
    "InitiationTime": "initiation_time",
    "MPCName": "mpc_name",
    "MPCUUID": "mpc_uuid",
    "PlivoHangupCause": "plivo_hangup_cause",
    "MPCTerminationCauseCode": "mpc_termination_cause_code",
    "PlivoHangupCauseCode": "plivo_hangup_cause_code",
    "PlivoHangupSource": "plivo_hangup_source",
    "Rate": "rate",
    "RingTime": "ring_time",
    "To": "to",
    "TotalAmount": "total_amount",
    "Role": "role",
    "X-PH-Add_Participant": "add_participant_header",
    "X-PH-Participant_Alias": "add_participant_alias_name",
    "X-PH-HsOffer": "offer_id",
    "DigitsSent": "digits_sent",
}


def _callback_params(body: dict, fields: dict):
    """A Plivo callback's known fields by their snake_case names, "" when absent."""
    lowered = {k.lower(): v for k, v in body.items()}
    return {snake: lowered.get(field.lower()) or "" for field, snake in fields.items()}


_PERSISTENCE_DOWN = "voice callback persistence unavailable"
_EFFECT_FAILED = "voice callback after-commit effect failed"


def _retryable(err) -> bool:
    return voice_hangup.is_retryable_voice_callback_error(err) or voice_callbacks.is_retryable_queue_persistence_error(
        err
    )


def _release_claim(release, raised):
    """release()'s error, or the text of what it raised (logged after `raised`)."""
    try:
        return release()
    except Exception as exc:
        logger.error(f"{raised}: {exc}")
        return str(exc)


def _rollback(db, failed=None):
    """Discard the staged effects and roll back; a failed rollback is logged after `failed`, when given."""
    voice_callbacks.discard_after_commit_effects(db)
    try:
        db.rollback()
    except Exception as exc:
        if failed:
            logger.error(f"{failed}: {exc}")


def _close(db, token, failed):
    voice_callbacks.reset_callback_effect_session(token)
    try:
        db.close()
    except Exception as exc:  # the reply is decided; a pool cleanup failure must not turn it into a 500
        logger.error(f"{failed}: {exc}")


def _retry_after_claim(release, event, call_uuid, content):
    """503 `content` with the claim released, so Plivo's retry runs the event again."""
    err = _release_claim(release, f"Callback exact Redis claim release raised eventName={event} callUUID={call_uuid}")
    if err is not None:
        logger.error(
            f"Callback failed and exact Redis claim could not be released eventName={event} callUUID={call_uuid}: {err}"
        )
    return _BareJSONResponse(content, 503)


def _run_db_callback(handler, release, event, call_uuid):
    """Run a status callback's DB work while it holds the event's dedupe claim, and decide the reply.

    The claim is released (Plivo retries the event) only while nothing irreversible has happened: no
    commit and no unsafe external effect. Otherwise the matching state is committed and the claim kept,
    so the event is never replayed."""
    db = SessionLocal()
    token = voice_callbacks.bind_callback_effect_session(db)

    def kept(content, phase):
        effects = voice_callbacks.unsafe_callback_effects(db)
        logger.error(
            "Voice callback reached a non-replay-safe phase; retaining exact claim "
            f"eventName={event} callUUID={call_uuid} phase={phase} effects={effects}"
        )
        return _BareJSONResponse(content, 503)

    def undone(content, phase, failed):
        unsafe = voice_callbacks.has_unsafe_callback_effect(db)
        _rollback(db)
        logger.error(failed)
        return kept(content, phase) if unsafe else _retry_after_claim(release, event, call_uuid, content)

    try:
        try:
            msg, err = handler(db)
        except Exception as exc:
            failed = f"Voice callback handler failed eventName={event} callUUID={call_uuid}: {exc}"
            return undone("voice callback processing failed", "handler", failed)
        if err is not None:
            retry = _retryable(err)
            if retry and not voice_callbacks.has_unsafe_callback_effect(db):
                _rollback(db, f"Voice callback rollback failed eventName={event} callUUID={call_uuid}")
                return _retry_after_claim(release, event, call_uuid, err)
            try:  # an external mutation or a permanent decision commits its state and keeps the claim
                db.commit()
                voice_callbacks.drain_after_commit_effects(db)
            except Exception as exc:
                logger.error(
                    f"Non-replay-safe voice callback state/effect commit failed eventName={event} "
                    f"callUUID={call_uuid}: {exc}"
                )
                return kept(_PERSISTENCE_DOWN, "retained_claim_commit")
            return kept(err, "handler_result_committed") if retry else _BareJSONResponse(err, 400)
        try:
            db.commit()
        except Exception as exc:
            return undone(
                _PERSISTENCE_DOWN,
                "commit",
                f"Voice callback commit failed eventName={event} callUUID={call_uuid}: {exc}",
            )
        try:
            voice_callbacks.drain_after_commit_effects(db)
        except Exception as exc:  # SQL is committed: a released claim would replay it and the effects already drained
            logger.error(
                f"Voice callback after-commit effect failed; retaining exact claim eventName={event} "
                f"callUUID={call_uuid}: {exc}"
            )
            return kept(_EFFECT_FAILED, "after_commit")
        return _BareJSONResponse(msg)
    finally:
        _close(db, token, f"Voice callback DB close failed eventName={event} callUUID={call_uuid}")


def mpc_status_callback(call_uuid: str, qnr_run_uuid: str, body: dict):
    params = _callback_params(body, _MPC_CALLBACK_FIELDS)
    event = params["event_name"]
    details = voice_callbacks.build_event_details(event, params["member_address"], params["sequence_number"])
    gate_err = voice_callbacks.update_voice_event_time_in_cache(call_uuid, qnr_run_uuid, details)
    if gate_err in (vc.VALUE_EXISTS_ERROR, vc.LATER_EVENT_FOUND_ERROR):
        if event in (vc.MPC_END_EVENT, vc.MPC_PARTICIPANT_EXIT_EVENT):
            is_mpc = event == vc.MPC_END_EVENT
            leg_err = voice_hangup.reconcile_terminal_call_leg(call_uuid, params, is_mpc=is_mpc)
            if leg_err is not None and not voice_hangup.is_retryable_voice_callback_error(leg_err):
                logger.warning(
                    "Duplicate terminal callback is structurally invalid; acknowledging without retry "
                    f"eventName={event} callUUID={call_uuid}"
                )
                return _BareJSONResponse("duplicate invalid terminal callback")
            if leg_err is not None and is_mpc:
                logger.error(
                    "Duplicate terminal callback could not reconcile durable state; requesting provider retry "
                    f"eventName={event} callUUID={call_uuid}"
                )
                return _BareJSONResponse(leg_err, 503)
            if leg_err is not None:
                logger.error(
                    "Duplicate terminal callback could not reconcile call leg; requesting provider retry "
                    f"eventName={event} callUUID={call_uuid}"
                )
                return _BareJSONResponse(leg_err, 503)
            if not is_mpc and (drop_err := voice_hangup.drop_exited_caller(call_uuid, params, qnr_run_uuid)):
                return _BareJSONResponse(drop_err, 503)
        logger.info(
            f"Callback already processed or superseded; ignoring idempotently eventName={event} callUUID={call_uuid}"
        )
        return _BareJSONResponse(gate_err)
    if gate_err is not None:
        logger.error(
            f"Callback ordering gate unavailable; requesting provider retry eventName={event} "
            f"callUUID={call_uuid}: {gate_err}"
        )
        return _BareJSONResponse("voice callback gate unavailable", 503)
    release = functools.partial(voice_callbacks.release_voice_event_claim, call_uuid, qnr_run_uuid, details)
    if event == vc.MPC_ADD_PARTICIPANT_COMPLETED_EVENT:
        logger.info("Add Participant by API action Completed event invoked")
    elif event == vc.MPC_PARTICIPANT_JOIN_EVENT:
        logger.info("Participant join event invoked")
    db_handler = {
        vc.MPC_ADD_PARTICIPANT_COMPLETED_EVENT: voice_hangup.handle_add_participant_action_completed,
        vc.MPC_PARTICIPANT_JOIN_EVENT: voice_agent_leg.handle_participant_join,
        vc.MPC_INITIALISED_CALLBACK_EVENT: voice_callbacks.handle_initialised_callback,
        vc.MPC_PARTICIPANT_SPEAK_STOPPED_EVENT: voice_callbacks.handle_prompt_stopped,
        vc.MPC_PARTICIPANT_AUDIO_STOPPED_EVENT: voice_callbacks.handle_prompt_stopped,
        vc.MPC_PARTICIPANT_DIGIT_INPUT_EVENT: voice_callbacks.handle_dial_input,
    }.get(event)
    if db_handler:
        return _run_db_callback(lambda db: db_handler(call_uuid, qnr_run_uuid, params, db), release, event, call_uuid)
    if event == vc.MPC_START_EVENT:
        metric = voice_callbacks.push_voice_event_metric, call_uuid, qnr_run_uuid, vc.MPC_INITIALISE_EVENT
        voice_callbacks._spawn(*metric, "mpc_start_event_duration")
        msg, err = voice_callbacks.handle_mpc_start(call_uuid, qnr_run_uuid)
    elif event == vc.MPC_END_EVENT:
        logger.info("MPC end event has been invoked")
        msg, err = voice_hangup.handle_mpc_end_event(call_uuid, params, qnr_run_uuid)
    elif event == vc.MPC_PARTICIPANT_EXIT_EVENT:
        logger.info("MPC ParticipantExit event invoked")
        msg, err = voice_hangup.handle_participant_exit_event(call_uuid, params, qnr_run_uuid)
    else:
        return Response(content=b"", status_code=200)
    if err is None:
        return _BareJSONResponse(msg)
    if event != vc.MPC_START_EVENT and voice_hangup.is_retryable_voice_callback_error(err):
        # the claim stays after a failed durable write, so a duplicate's reconciliation can repair the leg
        logger.error(
            "Terminal callback durable state unavailable; retaining exact claim for reconciliation "
            f"eventName={event} callUUID={call_uuid}"
        )
        return _BareJSONResponse(err, 503)
    if _retryable(err):
        return _retry_after_claim(release, event, call_uuid, err)
    logger.warning(f"Permanent voice callback error; retaining exact claim eventName={event} callUUID={call_uuid}")
    return _BareJSONResponse(err, 400)


def mpc_call_status_callback(call_uuid: str, qnr_run_uuid: str, body: dict):
    params = _callback_params(body, _CS_CALLBACK_FIELDS)
    event = params["event_name"]
    logger.info(f"Call status callback from MPC event={event} call_uuid={call_uuid}")
    if event in (vc.CALL_INITIATED_EVENT, vc.CALL_RINGING_EVENT):
        return Response(content=b"", status_code=200)
    if event == vc.CALL_ANSWERED_EVENT:
        err = voice_agent_leg.handle_call_answered(call_uuid, qnr_run_uuid, params)
        if err is not None:
            logger.error(f"Call-answer timestamp cache unavailable; requesting provider retry callUUID={call_uuid}")
            return _BareJSONResponse(err, 503)
        return Response(content=b"", status_code=200)
    if event not in (vc.CALL_HANGUP_EVENT, vc.MPC_PARTICIPANT_EXIT_EVENT):
        logger.error(f"invalid event name event={event} call_uuid={call_uuid}")
        return _BareJSONResponse(_SERVER_ERROR, 500)
    logger.info(f"Hang up event invoked call_uuid={call_uuid} Event name={event}")
    # One claim per leg: HangupTime has whole seconds, and one member's two legs on a call can end inside one.
    details = voice_callbacks.EventDetails(event_name=f"{event}:{params['to']}:{params['call_uuid']}")
    gate_err = voice_callbacks.update_voice_event_time_in_cache(call_uuid, qnr_run_uuid, details)
    if gate_err in (vc.VALUE_EXISTS_ERROR, vc.LATER_EVENT_FOUND_ERROR):
        logger.info(
            f"Call-status callback already processed or superseded; ignoring idempotently eventName={event} "
            f"callUUID={call_uuid}"
        )
        return _BareJSONResponse("duplicate event received")
    if gate_err is not None:
        logger.error(
            f"Call-status callback ordering gate unavailable; requesting provider retry eventName={event} "
            f"callUUID={call_uuid}: {gate_err}"
        )
        return _BareJSONResponse("voice callback gate unavailable", 503)

    def release():
        return _release_claim(
            lambda: voice_callbacks.release_voice_event_claim(call_uuid, qnr_run_uuid, details),
            f"Call-status callback exact claim release raised eventName={event} callUUID={call_uuid}",
        )

    db = SessionLocal()
    token = voice_callbacks.bind_callback_effect_session(db)
    try:
        msg, err = voice_hangup.handle_agent_hangup_event(call_uuid, qnr_run_uuid, params, db)
        if err is not None:
            retry = voice_hangup.is_retryable_voice_callback_error(err)
            if retry and not voice_callbacks.has_unsafe_callback_effect(db):
                _rollback(db, f"Call-status callback rollback failed eventName={event} callUUID={call_uuid}")
                release_err = release()
                if release_err is not None:
                    logger.error(
                        "Call-status callback failed and exact Redis claim could not be released "
                        f"eventName={event} callUUID={call_uuid}: {release_err}"
                    )
                return _BareJSONResponse(err, 503)
            try:  # an external mutation or a permanent cleanup commits its state and keeps the claim
                db.commit()
                voice_callbacks.drain_after_commit_effects(db)
            except Exception as exc:
                logger.error(
                    f"Call-status retained-claim state/effect commit failed eventName={event} "
                    f"callUUID={call_uuid}: {exc}"
                )
                return _BareJSONResponse(_PERSISTENCE_DOWN, 503)
            if retry:
                logger.error(
                    "Call-status callback failed after an external mutation; committed intended state and "
                    f"retained exact claim eventName={event} callUUID={call_uuid} "
                    f"effects={voice_callbacks.unsafe_callback_effects(db)}"
                )
                return _BareJSONResponse(err, 503)
            logger.warning(
                "Permanent call-status callback error; committed intended state and retained exact claim "
                f"eventName={event} callUUID={call_uuid}"
            )
            return _BareJSONResponse(err, 400)
        db.commit()
        try:
            voice_callbacks.drain_after_commit_effects(db)
        except Exception as exc:
            logger.error(
                f"Call-status callback after-commit effect failed; retaining exact claim eventName={event} "
                f"callUUID={call_uuid}: {exc}"
            )
            return _BareJSONResponse(_EFFECT_FAILED, 503)
        return _BareJSONResponse(msg)
    except Exception as exc:
        unsafe = voice_callbacks.has_unsafe_callback_effect(db)
        _rollback(db, f"Call-status callback rollback failed eventName={event} callUUID={call_uuid}")
        release_err = None if unsafe else release()
        logger.error(
            f"Call-status callback DB boundary failed eventName={event} callUUID={call_uuid}: {exc}; "
            f"claim_release={release_err} claim_retained_for_unsafe_effect={unsafe}"
        )
        return _BareJSONResponse(_PERSISTENCE_DOWN, 503)
    finally:
        _close(db, token, f"Call-status callback DB close failed eventName={event} callUUID={call_uuid}")


def _recording_reply(msg, err, retry_err):
    if err is None:
        return _BareJSONResponse(msg)
    return _BareJSONResponse(err, 503 if err == retry_err else 400)


def voice_mpc_recording_callback(call_uuid: str, body: dict):
    event = body.get("EventName")
    if event == vc.MPC_RECORDING_COMPLETED_EVENT:
        msg, err = voice_recording.handle_mpc_recording_event(call_uuid, body)
    elif event == "ParticipantRecordingCompleted":
        msg, err = voice_recording.handle_participant_recording_event(call_uuid, body)
    else:
        logger.info(f"Default case: Recording Callback from MPC Call uuid={call_uuid}")
        return _BareJSONResponse("")
    return _recording_reply(msg, err, voice_recording.ERR_RECORDING_RETRY_UNAVAILABLE)


def voice_mpc_transcription_callback(auth_id: str, call_uuid: str, body: dict):
    msg, err = voice_recording.handle_transcription_event(call_uuid, auth_id, body)
    return _recording_reply(msg, err, voice_recording.ERR_TRANSCRIPTION_RETRY_UNAVAILABLE)
