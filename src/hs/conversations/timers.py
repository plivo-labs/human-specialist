"""Every timer and async task as a `jobs` row: the scheduling helpers and one handler per kind.

A handler runs like the request its timer once called: a fresh context carrying the payload's agent and org,
and its own session, committed when it returns and rolled back (then retried) when it raises."""

import http
from datetime import timedelta

from hs import jobs
from hs.admin.account import OrgSetting, RoutingSettings
from hs.conversations import child_update as ccu
from hs.conversations import management as cm
from hs.conversations.models import ConversationSessionTimeoutPayload, UpdateParentByConversationNumber
from hs.conversations.outcomes import WRAP_UP_TIMEOUT, timer_key
from hs.conversations.store import emit
from hs.infra.constants import BUFFER_TIME, DEFAULT_CALL_SESSION_TIMEOUT, DEFAULT_WRAP_UP_TIME
from hs.infra.context import bg_db_guard, clear_context, context_aom_uuid, context_org_name, context_org_uuid
from hs.infra.db import SessionLocal
from hs.infra.helpers import get_now
from hs.infra.log import logger
from hs.routing.invitation import _ensure_request_context
from hs.voice import callbacks as vcb
from hs.voice import recording as vrec
from hs.voice import transcripts as tm
from hs.voice.hangup import is_retryable_voice_callback_error
from hs.voice.transcription_models import TranscriptCreateRequestModel


def _job(kind: str, backoff: tuple[float, ...] = jobs.DEFAULT_BACKOFF):
    def register(fn):
        def run(payload: dict) -> None:
            clear_context()
            for var, field in (
                (context_aom_uuid, "aom_uuid"),
                (context_org_uuid, "org_uuid"),
                (context_org_name, "org_name"),
            ):
                var.set(payload.get(field))
            _ensure_request_context()
            bg_db_guard(fn)(payload)

        return jobs.handler(kind, backoff)(run)

    return register


def _actor(aom_uuid, org_uuid, org_name) -> dict:
    return {
        "aom_uuid": str(aom_uuid) if aom_uuid else None,
        "org_uuid": str(org_uuid) if org_uuid else None,
        "org_name": org_name,
    }


def schedule_session_timeout(
    conversation_number, channel, aom_uuid, org_uuid, org_name, extended=False, child_conversation_id=None
) -> None:
    seconds = OrgSetting.get_conversation_session_timeout_from_aom(org_uuid=org_uuid, channel=channel)
    payload = {
        "conversation_number": conversation_number,
        "channel": channel,
        "extended": extended,
        "child_conversation_id": child_conversation_id,
        **_actor(aom_uuid, org_uuid, org_name),
    }
    key = f"{conversation_number}:{channel}:{aom_uuid}"
    jobs.schedule("session_timeout", payload, delay=seconds or DEFAULT_CALL_SESSION_TIMEOUT, key=key)


def start_wrap_up(conversation, aom_uuid, org_uuid, org_name) -> None:
    """Arm the agent's wrap-up close and announce its deadline: auto closes then, manual waits for the agent until a
    safety close BUFFER_TIME later. The payload keeps what an extend needs."""
    parent = conversation.parent_conversation
    wrap_up = RoutingSettings.get_by_org_id(str(parent.organization)).wrap_up_configuration or {}
    mode = wrap_up.get("mode", "manual")
    deadline = get_now() + timedelta(seconds=wrap_up.get("seconds", DEFAULT_WRAP_UP_TIME))
    payload = {"conversation_number": parent.conversation_number, "mode": mode, "extended": 0}
    payload |= {"max_extend_sec": wrap_up.get("max_extend_sec", 30), **_actor(aom_uuid, org_uuid, org_name)}
    run_at = deadline if mode == "auto" else deadline + timedelta(seconds=BUFFER_TIME)
    key = timer_key(parent.conversation_number, str(aom_uuid))
    jobs.schedule(WRAP_UP_TIMEOUT, payload, run_at=run_at, key=key)
    emit("conversation.wrap_up_started", parent, {"mode": mode, "deadline_at": deadline}, aom_uuid)


@_job("session_timeout")
def session_timeout(payload: dict) -> None:
    timeout = ConversationSessionTimeoutPayload(
        extended=payload["extended"], child_conversation_id=payload["child_conversation_id"]
    )
    cm.handle_conversation_session_timeout(payload["conversation_number"], payload["channel"], timeout)


@_job(WRAP_UP_TIMEOUT)
def wrap_up_timeout(payload: dict) -> None:
    cm.handle_conversation_wrapup_timeout(payload["conversation_number"])


@_job("parent_close")
def parent_close(payload: dict) -> None:
    ccu.update_parent_conversation_by_conversation_number(UpdateParentByConversationNumber(**payload))


# A pure capacity release that swallows its own errors: no retry.
@_job("capacity_clearance", backoff=())
def capacity_clearance(payload: dict) -> None:
    release = {"send_conversation_closed_event": False, "release_agent_capacity": True}
    cm.PostConversationStatusChange.run_wrap_up_conversation_inprocess({**payload, **release})


@_job("missed_mpc_event")
def missed_mpc_event(payload: dict) -> None:
    """Replay the MPC-initialised callback Plivo never sent. A retryable failure is raised once its claim is
    released; any other outcome commits, because the claim (or an external effect) already stands."""
    claimed, db, committed = [], SessionLocal(), False
    token = vcb.bind_callback_effect_session(db)
    try:
        err = vcb.handle_missed_mpc_event(payload, db, claimed_event_out=claimed)
        retry = err is not None and (
            is_retryable_voice_callback_error(err) or vcb.is_retryable_queue_persistence_error(err)
        )
        if retry and not vcb.has_unsafe_callback_effect(db):
            raise RuntimeError(err)
        db.commit()
        committed = True
        vcb.drain_after_commit_effects(db)
        if err is not None:
            logger.error(f"missed MPC event replay failed callUUID={payload['callUUID']}: {err}")
    except Exception:
        if not committed and not vcb.has_unsafe_callback_effect(db):
            vcb.discard_after_commit_effects(db)
            db.rollback()
            if claimed:
                vcb.release_voice_event_claim(payload["callUUID"], payload["qnrRunUUID"], claimed[0])
        raise
    finally:
        vcb.reset_callback_effect_session(token)
        db.close()


# Plivo's transcript can trail its callback: retry while the recording or transcript is not there yet.
@_job("transcript_attach", backoff=(30, 30, 30, 30, 30))
def transcript_attach(payload: dict) -> None:
    request = TranscriptCreateRequestModel(
        auth_id=payload["auth_id"], recording_uuid=payload["recording_uuid"], call_leg_uuid=payload["call_leg_uuid"]
    )
    recording_uuid, vm = payload["recording_uuid"], payload.get("voicemail")
    segments = tm.parse_callback_transcription(vm) if vm else tm.read_stashed_callback_transcription(recording_uuid)
    response = tm.create_transcripts(request, transcription_segments=segments, voicemail=bool(vm))
    if response.error == tm.MSG_CREATE_TRANSCRIPT_ERROR_NO_TRANSCRIPT:
        # The stashed callback text counts only once the provider's own transcript is final.
        text = tm.read_stashed_callback_text(recording_uuid)
        if text is not None and tm.provider_readiness_elapsed(recording_uuid):
            segment = tm._SDKResponseObject({"start": 0.0, "end": 0.0, "text": text})
            response = tm.create_transcripts(request, transcription_segments=[segment])
    if (response.status_code or 0) == http.HTTPStatus.CREATED or response.error == tm.MSG_CREATE_TRANSCRIPT_EMPTY:
        vrec._transcript_ready(recording_uuid)
    if (response.status_code or 0) == http.HTTPStatus.CREATED:
        return
    if response.error in {tm.MSG_CREATE_TRANSCRIPT_ERROR_NO_TRANSCRIPT, tm.MSG_CREATE_TRANSCRIPT_ERROR_NO_RECORDING}:
        raise jobs.Retry(response.error)
    done = {
        tm.MSG_CREATE_TRANSCRIPT_ERROR_DUPLICATE_TRANSCRIPT,
        tm.MSG_CREATE_TRANSCRIPT_EMPTY,
        tm.MSG_CREATE_TRANSCRIPT_DROPPED,
    }
    if response.error not in done:
        logger.error(f"transcript attach failed call_uuid={payload['call_uuid']}: {response.error}")
