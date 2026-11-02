class _PlivoNotFound(Exception):
    pass


class _SDKResponseObject:
    def __init__(self, dct):
        self.__dict__.update(dct)

    def __contains__(self, item):
        return item in self.__dict__


def _fetch_plivo_transcription(auth_id, auth_token, recording_uuid):
    import json as _json

    from hs.infra.settings import settings

    url = f"{settings.plivo_api_url}/v1/Account/{auth_id}/Transcription/{recording_uuid}"
    resp = read("GET", url, params={"type": TRANSCRIPT_TYPE_RAW}, auth=(auth_id, auth_token), timeout=30)
    if resp.status_code == 404:
        raise _PlivoNotFound(resp.text[:200])
    if resp.status_code != 200:
        raise RuntimeError(f"plivo transcription GET {resp.status_code}: {resp.text[:200]}")
    return _json.loads(resp.text, object_hook=lambda x: _SDKResponseObject(x) if isinstance(x, dict) else x)


import datetime
import math

from sqlalchemy import text

from hs.admin.account import AccountOrgMapping, Organization
from hs.conversations.store import ConversationParent, Recording
from hs.infra.context import get_db_session
from hs.infra.http_session import read
from hs.infra.log import logger
from hs.infra.models import UseCaseResponse
from hs.routing import account_read as ar
from hs.voice.call_models import CallParticipantRole
from hs.voice.call_store import CallLeg
from hs.voice.transcription_models import (
    TRANSCRIPT_TYPE_RAW,
    TranscriptCreateRequestModel,
    TranscriptionPlivoResponseModel,
    TranscriptionSegmentResponseModel,
    TranscriptSegmentModel,
)
from hs.voice.transcription_store import Transcription, TranscriptionSegment

# Prefer callback content before provider fetches.

CALLBACK_TRANSCRIPTION_STASH_TTL = 3600
# A recording of this leg, where the leg, or the call it is on, is one of the tenant's conversations.
_OWNED = text(
    "SELECT 1 FROM recording r JOIN call_leg l ON l.id = r.call_leg_id LEFT JOIN call_leg m ON m.id = l.parent_call_leg"
    " JOIN conversation_parent cp ON cp.contact_uuid = COALESCE(m.call_uuid, l.call_uuid) JOIN organization o"
    " ON o.id = cp.organization WHERE r.recording_uuid = :rid AND l.call_uuid = :leg AND o.auth_id = :auth"
)


def _callback_transcription_key(recording_uuid: str) -> str:
    return "callback_transcription:" + recording_uuid


def _skipped_recording_key(recording_uuid: str) -> str:
    return "skipped_recording:" + recording_uuid


def mark_recording_skipped(recording_uuid: str) -> None:
    """Best-effort marker for a recording HS dropped; the provider still transcribes it."""
    from hs.infra.redis import Cache

    try:
        Cache.get_instance().set(
            _skipped_recording_key(recording_uuid), "1", expiry_in_seconds=CALLBACK_TRANSCRIPTION_STASH_TTL
        )
    except Exception as e:
        logger.error(f"mark_recording_skipped failed recording_uuid={recording_uuid}: {e}")


def recording_skipped(recording_uuid: str) -> bool:
    from hs.infra.redis import Cache

    try:
        return bool(Cache.get_instance().get(_skipped_recording_key(recording_uuid)))
    except Exception:
        return False


def parse_callback_transcription(value) -> list | None:
    """Normalize callback content; None is unusable and [] is final empty content."""
    import json as _json

    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8", "replace")
    if isinstance(value, str):
        if not value.strip():
            return None
        try:
            value = _json.loads(value)
        except ValueError as e:
            # Retain plain-text callbacks for the text fallback.
            logger.info(f"parse_callback_transcription: not JSON: {e}")
            return None
    if isinstance(value, dict):
        if "transcription" not in value:
            logger.error("parse_callback_transcription: no transcription key")
            return None
        value = value.get("transcription")
        if value is None:
            return []
    if not isinstance(value, list):
        logger.error(f"parse_callback_transcription: unexpected content type {type(value).__name__}")
        return None
    segments = []
    for raw in value:
        item = raw if isinstance(raw, dict) else getattr(raw, "__dict__", None)
        if not isinstance(item, dict) or "text" not in item:
            logger.error("parse_callback_transcription: malformed segment")
            return None
        try:
            segments.append(
                _SDKResponseObject(
                    {
                        "start": float(item.get("start") or 0),
                        "end": float(item.get("end") or 0),
                        "text": str(item.get("text") or ""),
                    }
                )
            )
        except (TypeError, ValueError) as e:
            logger.error(f"parse_callback_transcription: bad segment bounds: {e}")
            return None
    return segments


def stash_callback_transcription(recording_uuid: str, value) -> None:
    """Store raw callback content for durable retry without raising."""
    import json as _json

    from hs.infra.redis import Cache

    try:
        Cache.get_instance().set(
            _callback_transcription_key(recording_uuid),
            _json.dumps(value, separators=(",", ":")),
            expiry_in_seconds=CALLBACK_TRANSCRIPTION_STASH_TTL,
        )
    except Exception as e:
        logger.error(f"stash_callback_transcription: failed for recording_uuid: {recording_uuid}, error: {e}")


def read_stashed_callback_transcription(recording_uuid: str) -> list | None:
    """Read callback content without consuming the stash."""
    import json as _json

    from hs.infra.redis import Cache

    try:
        raw = Cache.get_instance().get(_callback_transcription_key(recording_uuid))
    except Exception as e:
        logger.error(f"read_stashed_callback_transcription: failed for recording_uuid: {recording_uuid}, error: {e}")
        return None
    if not raw:
        return None
    try:
        return parse_callback_transcription(_json.loads(raw))
    except ValueError:
        return None


# Delay text fallback until the provider readiness window has elapsed.
TRANSCRIPT_TEXT_FALLBACK_AFTER_SECONDS = 240


def read_stashed_callback_text(recording_uuid: str) -> str | None:
    """Return raw text callback content, if present."""
    import json as _json

    from hs.infra.redis import Cache

    try:
        raw = Cache.get_instance().get(_callback_transcription_key(recording_uuid))
    except Exception:
        return None
    if not raw:
        return None
    try:
        value = _json.loads(raw)
    except ValueError:
        return None
    if isinstance(value, dict):
        value = value.get("transcription")
    if isinstance(value, str) and value.strip():
        return value
    return None


def provider_readiness_elapsed(recording_uuid: str) -> bool:
    """Return whether the provider readiness window has elapsed."""
    from hs.infra.redis import Cache

    try:
        remaining = Cache.get_instance().ttl(_callback_transcription_key(recording_uuid))
    except Exception:
        remaining = None
    if remaining is not None and remaining >= 0:
        return CALLBACK_TRANSCRIPTION_STASH_TTL - remaining >= TRANSCRIPT_TEXT_FALLBACK_AFTER_SECONDS
    recording = Recording.get_by_recording_uuid(recording_uuid)
    if recording is None or recording.created_at is None:
        return False
    now = datetime.datetime.now(datetime.UTC)
    created = recording.created_at
    if created.tzinfo is None:
        created = created.replace(tzinfo=datetime.UTC)
    return (now - created).total_seconds() >= TRANSCRIPT_TEXT_FALLBACK_AFTER_SECONDS


MSG_CREATE_TRANSCRIPT_SUCCESS = "Transcript created successfully"
MSG_CREATE_TRANSCRIPT_ERROR_NO_TRANSCRIPT = "No transcript provided"
MSG_CREATE_TRANSCRIPT_EMPTY = "Provider transcription has no segments"
MSG_CREATE_TRANSCRIPT_ERROR_NO_RECORDING = "No recording provided"
MSG_CREATE_TRANSCRIPT_DROPPED = "Recording dropped, nothing to attach"
MSG_CREATE_TRANSCRIPT_ERROR_NO_ORG = "Organization not found"
MSG_CREATE_TRANSCRIPT_ERROR_NO_CALL_LEG = "Call leg not found"
MSG_CREATE_TRANSCRIPT_ERROR_DUPLICATE_TRANSCRIPT = "Transcript already exists"
MSG_INVALID_RECORDING_ID = "Invalid recording id"


def callback_segments(params: dict) -> tuple[list | None, bool]:
    """(segments, is_voicemail). A <Record> transcription callback (the only one that names a charge) carries
    the plain text of the whole message: one segment."""
    raw = params.get("transcription")
    if "transcription_charge" not in params:
        return parse_callback_transcription(raw), False
    return [
        _SDKResponseObject({"start": 0.0, "end": float(params.get("duration") or 0), "text": str(raw or "").strip()})
    ], True


def create_transcripts(
    transcript_request: TranscriptCreateRequestModel, transcription_segments: list | None = None, voicemail=False
) -> UseCaseResponse:
    """Attach callback or fetched transcript segments.
    An empty list is final; None permits provider fetching."""
    auth_id = transcript_request.auth_id
    org = Organization.get_org_by_auth_id(auth_id)
    if not org:
        logger.error(f"create_transcripts: No organization found for auth_id: {auth_id}")
        return UseCaseResponse(status_code=404, error=MSG_CREATE_TRANSCRIPT_ERROR_NO_ORG)
    recording_uuid = transcript_request.recording_uuid
    recording = Recording.get_by_recording_uuid(recording_uuid, with_call_leg=True)
    if not recording:
        if recording_skipped(recording_uuid):
            logger.info(f"create_transcripts: dropped recording, transcript discarded recording_uuid: {recording_uuid}")
            return UseCaseResponse(status_code=404, error=MSG_CREATE_TRANSCRIPT_DROPPED)
        # WARNING, not ERROR: this is usually the transcription callback racing the
        # RecordingCompleted INSERT's commit — voice_recording durably defers it to
        # the async-task rail before acknowledging the provider callback.
        logger.warning(f"create_transcripts: No recording found for recording_uuid: {recording_uuid}")
        return UseCaseResponse(status_code=404, error=MSG_CREATE_TRANSCRIPT_ERROR_NO_RECORDING)
    # Before the ownership check: the room's transcription names another leg's call, and it writes nothing.
    if recording.call_leg.parent_call_leg is None and not voicemail:
        logger.info(
            "create_transcripts: Request for MPC level transcript creation is ignored "
            + f"call_leg_uuid: {transcript_request.call_leg_uuid}, recording_uuid: {recording_uuid}"
        )
        return UseCaseResponse(status_code=422, error=MSG_CREATE_TRANSCRIPT_ERROR_DUPLICATE_TRANSCRIPT)
    owned = {"rid": recording_uuid, "leg": transcript_request.call_leg_uuid, "auth": auth_id}
    if get_db_session().execute(_OWNED, owned).first() is None:  # another tenant's recording or leg
        return UseCaseResponse(status_code=404, error=MSG_CREATE_TRANSCRIPT_ERROR_NO_ORG)
    try:
        if transcription_segments is None:
            transcription: TranscriptionPlivoResponseModel = _fetch_plivo_transcription(
                *ar.tenant_plivo(str(org.uuid)), recording_uuid
            )
            segments = transcription.transcription or []
            logger.info(
                "create_transcripts: provider transcription fetched "
                + f"recording_uuid: {recording_uuid}, segment_count: {len(segments)}"
            )
        else:
            segments = transcription_segments
            logger.info(
                "create_transcripts: attached from callback content "
                + f"recording_uuid: {recording_uuid}, segment_count: {len(segments)}"
            )
        if not segments:
            # Plivo fires the transcription callback only after the job finishes,
            # so a 200 with zero segments is the job's FINAL result (no
            # recognizable speech) — retrying can never fill it. The genuine
            # not-ready race is the _PlivoNotFound 404 branch below.
            logger.warning(
                f"create_transcripts: provider transcription has 0 segments, terminal — nothing to attach "
                f"recording_uuid: {recording_uuid}"
            )
            # An empty transcript is still this leg's final transcript: it counts toward the recording's readiness.
            if leg := CallLeg.get_by_call_uuid(transcript_request.call_leg_uuid, participant_leg=True):
                Transcription.get_or_create_transcript(leg.id)
            return UseCaseResponse(status_code=404, error=MSG_CREATE_TRANSCRIPT_EMPTY)
        call_leg = CallLeg.get_by_call_uuid(transcript_request.call_leg_uuid, participant_leg=True)
        if not call_leg:
            logger.error(
                "create_transcripts: No call leg found for "
                + f"call_leg_uuid: {transcript_request.call_leg_uuid} recording_uuid: {recording_uuid}"
            )
            return UseCaseResponse(status_code=422, error=MSG_CREATE_TRANSCRIPT_ERROR_NO_CALL_LEG)
        transcript = Transcription.get_or_create_transcript(call_leg.id)
        # Provider callbacks and durable retries can overlap. Serialize the
        # recording identity after resolving the shared transcript row, because
        # get_or_create_transcript may roll back its unique-key race and a rollback
        # releases transaction-scoped advisory locks.
        Transcription.acquire_ingestion_lock(recording_uuid)
        transcript_segments_to_be_created = []
        recording_start_time = datetime.datetime.strptime(recording.recording_start_time, "%Y-%m-%d %H:%M:%S")
        for segment in segments:
            start_ts = (recording_start_time + datetime.timedelta(seconds=segment.start)).timestamp()
            transcript_segment_model = TranscriptSegmentModel(
                transcription_id=transcript.id,
                start_time=segment.start,
                end_time=segment.end,
                segment=segment.text,
                start_ts=start_ts,
            )
            transcript_segments_to_be_created.append(transcript_segment_model.build_db_model())
        inserted = TranscriptionSegment.create_missing_transcript_segments(
            transcript.id, transcript_segments_to_be_created
        )
        logger.info(
            "create_transcripts: Transcripts created successfully for "
            + f"call_leg_uuid: {transcript_request.call_leg_uuid}, inserted_segments: {inserted}"
        )
    except _PlivoNotFound as e:
        logger.warning(f"create_transcripts: No transcription found for recording_uuid: {recording_uuid}, error: {e}")
        return UseCaseResponse(status_code=404, error=MSG_CREATE_TRANSCRIPT_ERROR_NO_TRANSCRIPT)
    except Exception as e:
        logger.error(
            f"create_transcripts: Error getting transcription for recording_uuid: {recording_uuid}, error: {e}"
        )
        return UseCaseResponse(status_code=500, error=MSG_CREATE_TRANSCRIPT_ERROR_NO_TRANSCRIPT)
    return UseCaseResponse(status_code=201, msg=MSG_CREATE_TRANSCRIPT_SUCCESS)


def get_transcriptions(recording_uuid: str) -> UseCaseResponse:
    all_fetched = True
    if not recording_uuid:
        logger.error("get_transcriptions: No recording_uuid provided")
        return UseCaseResponse(status_code=400, error=MSG_CREATE_TRANSCRIPT_ERROR_NO_RECORDING)
    recording = Recording.get_by_recording_uuid(recording_uuid, with_call_leg=True)
    if not recording:
        logger.error(f"get_transcriptions: No recording found for recording_uuid: {recording_uuid}")
        return UseCaseResponse(status_code=404, error=MSG_CREATE_TRANSCRIPT_ERROR_NO_RECORDING)
    if recording.call_leg.parent_call_leg is not None:
        logger.error(f"get_transcriptions: Recording type is not MPC for recording_uuid: {recording_uuid}")
        return UseCaseResponse(status_code=400, error=MSG_INVALID_RECORDING_ID)
    main_call_leg = CallLeg.get_by_id(recording.call_leg_id)
    if not main_call_leg:
        logger.error(f"get_transcriptions: No main call leg found for recording_uuid: {recording_uuid}")
        return UseCaseResponse(status_code=404, error=MSG_CREATE_TRANSCRIPT_ERROR_NO_CALL_LEG)
    mpc_recording_start_timestamp = datetime.datetime.strptime(recording.recording_start_time, "%Y-%m-%d %H:%M:%S")
    call_legs = CallLeg.get_all_call_legs_by_parent_call_leg(main_call_leg.id)
    recordings, transcripts = Transcription.get_participant_bundle([leg.id for leg in call_legs])
    transcriptions: list[TranscriptionSegmentResponseModel] = []
    for call_leg in call_legs:
        speaker: str = call_leg.role.title()
        if call_leg.role == CallParticipantRole.CUSTOMER.value:
            parent_conversation = ConversationParent.get_by_contact_uuid(call_leg.call_uuid)
            if parent_conversation and parent_conversation.contact_name:
                speaker = parent_conversation.contact_name
        elif call_leg.role in [CallParticipantRole.AGENT.value, CallParticipantRole.SUPERVISOR.value]:
            aom = AccountOrgMapping.get_by_id(call_leg.participant)
            if aom:
                speaker = AccountOrgMapping.get_agent_name_by_aom_uuid(str(aom.uuid)) or call_leg.participant
            else:
                speaker = call_leg.participant
        p_rec_start_timestamp = mpc_recording_start_timestamp
        participant_recording = recordings.get(str(call_leg.id))
        if not participant_recording:
            logger.error(f"get_transcriptions: No participant recording found for call_leg_id: {call_leg.id}")
        else:
            p_rec_start_timestamp = datetime.datetime.strptime(
                participant_recording.recording_start_time, "%Y-%m-%d %H:%M:%S"
            )
        transcript = transcripts.get(str(call_leg.id))
        if transcript:
            for segment in transcript.transcription_segments:
                if segment.start_ts is None:
                    segment_start_timestamp = p_rec_start_timestamp + datetime.timedelta(seconds=segment.start_time)
                    start_time = math.ceil((segment_start_timestamp - mpc_recording_start_timestamp).total_seconds())
                else:
                    if int(segment.start_ts) < int(mpc_recording_start_timestamp.timestamp()) or int(
                        segment.start_ts
                    ) > int(mpc_recording_start_timestamp.timestamp() + int(recording.recording_duration)):
                        continue
                    start_time = math.ceil(segment.start_ts - mpc_recording_start_timestamp.timestamp())
                transcriptions.append(
                    TranscriptionSegmentResponseModel(speaker=speaker, start_time=start_time, text=segment.segment)
                )
        else:
            all_fetched = False
            logger.info(f"get_transcriptions: No transcript found for call_leg_id: {call_leg.id}")
    transcriptions.sort(key=lambda x: x.start_time)
    # A partial list is useful to polling callers, but it is not a completed
    # result. Returning 200 made status-only consumers silently accept missing
    # participant legs. Preserve the existing list payload while making the
    # lifecycle explicit at the HTTP boundary.
    status_code = 200 if all_fetched else 202
    return UseCaseResponse(status_code=status_code, data={"transcriptions": transcriptions, "all_fetched": all_fetched})
