from sqlalchemy import text

from hs import events
from hs.access import redis_client
from hs.infra.db import core_engine
from hs.infra.log import logger
from hs.routing import constants as rc
from hs.voice.call_models import CallRecordingRequestModel
from hs.voice.transcription_models import TranscriptCreateRequestModel

ERR_CREATE_RECORDING_INTEGRATION = "error in creating recording"
ERR_RECORDING_RETRY_UNAVAILABLE = "recording callback retry unavailable"
ERR_TRANSCRIPTION_INTEGRATION = "error in creating transcription"
ERR_TRANSCRIPTION_RETRY_UNAVAILABLE = "transcription retry unavailable"
# The public recording a provider recording belongs to: itself (a voicemail), or the MPC recording of its leg's call.
_PUBLIC = text(
    "SELECT pub.uuid::text, pub.recording_uuid, cp.uuid::text, o.uuid::text FROM recording r"
    " JOIN call_leg l ON l.id = r.call_leg_id JOIN recording pub ON pub.call_leg_id IN (l.id, l.parent_call_leg)"
    " JOIN conversation_chunk ch ON ch.recording = pub.id JOIN conversation c ON c.id = ch.conversation"
    " JOIN conversation_parent cp ON cp.id = c.parent JOIN organization o ON o.id = cp.organization"
    " WHERE r.recording_uuid = :rid LIMIT 1"
)


def _parse_int_error(value) -> str:
    return f'not an integer: "{value}"'


def _recording_event_attributes(req: dict) -> dict:
    return {
        "recording_url": req.get("recording_url", ""),
        "recording_duration": req.get("recording_duration", ""),
        "recording_uuid": req.get("recording_uuid", ""),
        "recording_format": req.get("recording_format", ""),
        "recording_start_time": req.get("recording_start_time", ""),
        "recording_end_time": req.get("recording_end_time", ""),
        "recording_type": req.get("recording_type", ""),
        "recording_resource_url": req.get("recording_resource_url", ""),
    }


def _persist_recording(req: dict) -> tuple[int, str, str | None]:
    """Create the recording/chunk inside the request transaction."""
    from hs.conversations.store import Recording
    from hs.voice.call_store import CallLeg

    if not req.get("call_leg_uuid") and not req.get("call_uuid"):
        logger.error(msg="_persist_recording: call_leg_uuid or call_uuid is required")
        return (400, "call_leg_uuid or call_uuid is required", "call_leg_uuid or call_uuid is required")
    is_participant = req.get("recording_type") == "participant"
    call_leg = CallLeg.get_by_call_uuid(req.get("call_leg_uuid", ""), participant_leg=is_participant)
    if call_leg is None:
        logger.error(msg=f"_persist_recording: Call leg not found call_leg_uuid={req.get('call_leg_uuid', '')}")
        return 404, "Call leg not found", "call leg not found"
    if CallLeg.outside_call(req["call_leg_uuid"], req.get("call_uuid", "")):
        logger.warning(msg=f"_persist_recording: refused a leg outside its call call_uuid={req.get('call_uuid', '')}")
        return 400, "Call leg belongs to another call", "call leg belongs to another call"
    if req.get("recording_uuid"):
        existing = Recording.get_by_recording_uuid(req["recording_uuid"])
        if existing is not None:
            logger.info(
                msg=f"_persist_recording: Recording with same UUID already exists recording_uuid={req['recording_uuid']}"
            )
            if req.get("create_chunk"):
                # A previous attempt committed the DB rows but failed afterwards: the
                # provider retry is idempotent, the recording UUID is the key.
                return 200, "Recording with same UUID already exists", None
            return (409, "Recording with same UUID already exists", "recording with same UUID already exists")

    try:
        recording_row = CallRecordingRequestModel(
            call_uuid=req.get("call_uuid", ""),
            call_leg_uuid=req.get("call_leg_uuid", ""),
            recording_url=req.get("recording_url", ""),
            recording_duration=req.get("recording_duration", ""),
            recording_uuid=req.get("recording_uuid", ""),
            recording_format=req.get("recording_format", ""),
            recording_start_time=req.get("recording_start_time", ""),
            recording_end_time=req.get("recording_end_time", ""),
            recording_type=req.get("recording_type", ""),
        ).build_db_model(call_leg.id)
        Recording.create_recording(recording_row)
    except Exception as error:
        from hs.infra.context import set_db_session_rollback

        set_db_session_rollback()
        logger.error(msg=f"_persist_recording: Failed to create recording: {error}")
        return 400, "Failed to create recording", str(error)

    attributes = _recording_event_attributes(req)
    if req.get("create_chunk"):
        from hs.conversations import child_update as ccu
        from hs.conversations.models import NonQNRChunkInsert

        chunk = NonQNRChunkInsert(
            parent_contact_uuid=req.get("call_uuid", ""),
            is_inbound_chunk=False,
            channel=rc.CHANNEL_CALL,
            attributes=[attributes],
            type="recording",
            description="",
            is_main_recording=req.get("recording_type") == "main",
            recording=recording_row.id,
        )
        response = ccu.add_chunk_to_non_qnr_child(non_qnr_chunk=chunk)
        if response.error:
            from hs.infra.context import set_db_session_rollback

            set_db_session_rollback()
            logger.error(msg=f"_persist_recording: Failed to create chunk: {response.error}")
            return 400, "Failed to create chunk", "failed to create chunk"
    return 201, "Recording created successfully", None


def px_create_recording(req: dict) -> tuple[int, str, str | None]:
    from hs.voice.hangup import _commit_session
    from hs.voice.monitor import _seed_context

    _seed_context({})
    try:
        result = _persist_recording(req)
    except Exception as error:
        from hs.infra.context import set_db_session_rollback

        set_db_session_rollback()
        logger.error(msg=f"px_create_recording: Failed to create recording: {error}")
        result = (400, "Failed to create recording", str(error))

    commit_error = _commit_session()
    if commit_error and result[2] is None:
        return (503, "Recording persistence unavailable", ERR_RECORDING_RETRY_UNAVAILABLE)
    return result


def _too_short(params: dict) -> bool:
    try:
        return int(params.get("RecordingDuration", "")) < 1
    except (ValueError, TypeError):
        return False


def _drop_recording(
    event: str, call_uuid: str, recording_uuid: str, reason: str = "recording duration is less than 1 second"
) -> tuple[str, str | None]:
    from hs.voice.transcripts import mark_recording_skipped

    logger.info(msg=f"{event}: Ignoring event as {reason} call uuid={call_uuid}")
    mark_recording_skipped(recording_uuid)
    return "", None


def handle_mpc_recording_event(call_uuid: str, params: dict) -> tuple[str, str | None]:
    if _too_short(params):
        return _drop_recording("MPCRecordingCompletedEvent", call_uuid, params.get("RecordingUUID", ""))
    _s, _m, err = px_create_recording(
        {
            "call_uuid": call_uuid,
            "call_leg_uuid": call_uuid,
            "recording_url": params.get("RecordingURL", ""),
            "recording_duration": params.get("RecordingDuration", ""),
            "recording_start_time": params.get("RecordingStartTime", ""),
            "recording_end_time": params.get("RecordingEndTime", ""),
            "recording_uuid": params.get("RecordingUUID", ""),
            "recording_resource_url": params.get("RecordingResourceURL", ""),
            "recording_format": params.get("RecordingFormat", ""),
            "recording_type": "mpc",
            "create_chunk": True,
        }
    )
    if err is not None:
        return "", (err if err == ERR_RECORDING_RETRY_UNAVAILABLE else ERR_CREATE_RECORDING_INTEGRATION)
    # The legs' transcripts can land first; their own check found no recording to announce yet.
    _transcript_ready(params.get("RecordingUUID", ""))
    return "", None


def handle_participant_recording_event(call_uuid: str, params: dict) -> tuple[str, str | None]:
    recording_uuid = params.get("RecordingUUID", "")
    if params.get("RecordTrackType") == "all":
        return _drop_recording(
            "ParticipantRecordingCompletedEvent", call_uuid, recording_uuid, "record track type is all"
        )
    if _too_short(params):
        return _drop_recording("ParticipantRecordingCompletedEvent", call_uuid, recording_uuid)
    _s, _m, err = px_create_recording(
        {
            "call_uuid": call_uuid,
            "call_leg_uuid": params.get("ParticipantCallUUID", ""),
            "recording_url": params.get("RecordingURL", ""),
            "recording_duration": params.get("RecordingDuration", ""),
            "recording_uuid": params.get("RecordingUUID", ""),
            "recording_format": params.get("RecordingFormat", ""),
            "recording_start_time": params.get("RecordingStartTime", ""),
            "recording_end_time": params.get("RecordingEndTime", ""),
            "recording_type": "participant",
        }
    )
    if err is not None:
        return "", (err if err == ERR_RECORDING_RETRY_UNAVAILABLE else ERR_CREATE_RECORDING_INTEGRATION)
    return "", None


def handle_main_call_recording_event(call_uuid: str, params: dict) -> tuple[str, str | None]:
    import os.path
    import time as _time

    try:
        start_ms = int(params.get("RecordingStartMs", ""))
    except (ValueError, TypeError):
        logger.error(msg=f"Error parsing recording start time recording_start_ms={params.get('RecordingStartMs', '')}")
        return "", _parse_int_error(params.get("RecordingStartMs", ""))
    recording_start = _time.strftime("%Y-%m-%d %H:%M:%S", _time.gmtime(start_ms / 1000))
    try:
        end_ms = int(params.get("RecordingEndMs", ""))
    except (ValueError, TypeError):
        logger.error(msg=f"Error parsing recording end time recording_end_ms={params.get('RecordingEndMs', '')}")
        return "", _parse_int_error(params.get("RecordingEndMs", ""))
    recording_end = _time.strftime("%Y-%m-%d %H:%M:%S", _time.gmtime(end_ms / 1000))
    recording_format = "mp3"
    record_url = params.get("RecordUrl", "") or ""
    if "/Recording/" in record_url:
        ext = os.path.splitext(record_url)[1]
        if ext:
            recording_format = ext[1:]
    if _too_short(params):
        return _drop_recording("MainCallRecordingCompletedEvent", call_uuid, params.get("RecordingID", ""))
    _s, _m, err = px_create_recording(
        {
            "call_uuid": call_uuid,
            "call_leg_uuid": call_uuid,
            "recording_url": record_url,
            "recording_duration": params.get("RecordingDuration", ""),
            "recording_start_time": recording_start,
            "recording_end_time": recording_end,
            "recording_uuid": params.get("RecordingID", ""),
            "recording_format": recording_format,
            "recording_type": "main",
            "create_chunk": True,
        }
    )
    if err is not None:
        return "", (err if err == ERR_RECORDING_RETRY_UNAVAILABLE else ERR_CREATE_RECORDING_INTEGRATION)
    return "", None


def handle_transcription_event(call_uuid: str, auth_id: str, params: dict) -> tuple[str, str | None]:
    from hs.voice import transcripts as tm
    from hs.voice.hangup import _commit_session
    from hs.voice.monitor import _seed_context
    from hs.voice.transcripts import callback_segments, stash_callback_transcription

    _seed_context({})
    # Plivo's transcription callback posts snake_case keys (recording_id / call_uuid), unlike the
    # PascalCase MPC recording events; the PascalCase fallback stays for safety.
    recording_uuid = params.get("recording_id") or params.get("RecordingID") or ""
    call_leg_uuid = params.get("call_uuid") or params.get("CallUUID") or ""
    if not recording_uuid or not call_leg_uuid:
        logger.error(msg="Invalid transcription callback: recording_id and call_uuid are required")
        return "", ERR_TRANSCRIPTION_INTEGRATION
    # Read transcript content from the callback payload.
    raw_transcription = params.get("transcription")
    transcription_segments, voicemail = callback_segments(params)
    resp = None
    processing_error = None
    try:
        resp = tm.create_transcripts(
            TranscriptCreateRequestModel(auth_id=auth_id, recording_uuid=recording_uuid, call_leg_uuid=call_leg_uuid),
            transcription_segments=transcription_segments,
            voicemail=voicemail,
        )
    except Exception as e:
        processing_error = e
        # The usecase can fail after flushing part of the transcript. Force the
        # explicit boundary below to roll that work back; never turn an exception
        # into a commit merely because the provider callback needs a response.
        from hs.infra.context import set_db_session_rollback

        set_db_session_rollback()
    commit_error = _commit_session()
    if commit_error:
        # Never acknowledge a callback whose transcript transaction did not commit.
        # The dispatcher maps this error to 503, which matches the callback URL's
        # existing rp=5xx provider retry policy.
        logger.error(msg=f"Error while committing transcription call_uuid={call_uuid}")
        return "", ERR_TRANSCRIPTION_RETRY_UNAVAILABLE
    if processing_error is not None:
        logger.error(msg=f"Error while creating transcription call_uuid={call_uuid}: {processing_error}")
        return "", ERR_TRANSCRIPTION_RETRY_UNAVAILABLE
    if (resp.status_code or 0) != 201:
        if resp.error == tm.MSG_CREATE_TRANSCRIPT_ERROR_DUPLICATE_TRANSCRIPT:
            # The schema intentionally ignores the MPC-level transcript and
            # stores participant-level transcripts.  This is a completed callback,
            # not a client error.
            logger.info(msg=f"Ignoring MPC-level transcription callback call_uuid={call_uuid}")
            return "", None
        if resp.error == tm.MSG_CREATE_TRANSCRIPT_EMPTY:
            # Completed transcription with zero segments: the callback fires only
            # after the job finishes, so this is final — acknowledge instead of
            # deferring to the retry job (which would re-fetch the same empty
            # result until its attempts exhaust).
            logger.info(msg=f"Empty (0-segment) transcription acknowledged call_uuid={call_uuid}")
            _transcript_ready(recording_uuid)
            return "", None
        if resp.error == tm.MSG_CREATE_TRANSCRIPT_DROPPED:
            logger.info(msg=f"Transcription for a dropped recording acknowledged call_uuid={call_uuid}")
            return "", None
        if resp.error in {tm.MSG_CREATE_TRANSCRIPT_ERROR_NO_RECORDING, tm.MSG_CREATE_TRANSCRIPT_ERROR_NO_TRANSCRIPT}:
            # The recording or the provider's transcript can trail this callback: retry as a job, written
            # before Plivo gets its 200.
            try:
                from hs import jobs

                # Keep large content in Redis, out of the job row; the raw content may be the only source
                # when the provider fetch fails.
                if raw_transcription is not None:
                    stash_callback_transcription(recording_uuid, raw_transcription)
                ids = {"auth_id": auth_id, "call_uuid": call_uuid, "recording_uuid": recording_uuid}
                # A voicemail's text comes only with this callback, and it is short: the job row carries it.
                vm = {"voicemail": list(map(vars, transcription_segments))} if voicemail else {}
                jobs.schedule("transcript_attach", {**ids, "call_leg_uuid": call_leg_uuid, **vm}, delay=5)
            except Exception as e:
                logger.error(msg=f"Transcript retry enqueue failed call_uuid={call_uuid}: {e}")
                return "", ERR_TRANSCRIPTION_RETRY_UNAVAILABLE
            logger.warning(msg=f"Transcript callback durably deferred call_uuid={call_uuid}")
            return "", None
        logger.error(msg=f"Error while creating transcription call_uuid={call_uuid}: {resp.error}")
        return "", ERR_TRANSCRIPTION_INTEGRATION
    _transcript_ready(recording_uuid)
    return "", None


def _transcript_ready(recording_uuid: str) -> None:
    """transcript.ready, once, when every leg of the public recording has its transcript, an empty one included."""
    from hs.voice import transcripts

    with core_engine.connect() as conn:
        found = conn.execute(_PUBLIC, {"rid": recording_uuid}).first()
    if found is None or not transcripts.get_transcriptions(found[1]).data.get("all_fetched"):
        return
    if redis_client().set(f"TREADY:{found[0]}", 1, nx=True, ex=86400):
        events.emit("transcript.ready", tenant_id=found[3], conversation_id=found[2], data={"recording_id": found[0]})
