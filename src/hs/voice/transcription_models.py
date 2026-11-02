from pydantic import ConfigDict

from hs.infra.base_model import BaseModel
from hs.infra.models import CoreDBBase

TRANSCRIPT_TYPE_RAW = "raw"
TRANSCRIPT_LANGUAGE_EN = "en"


class TranscriptSegmentModel(CoreDBBase):
    transcription_id: int
    start_time: float
    end_time: float
    segment: str
    start_ts: float | None = None
    model_config = ConfigDict(from_attributes=True)

    def build_db_model(self):
        from hs.voice.transcription_store import TranscriptionSegment

        res_dict = {
            TranscriptionSegment.transcription_id.name: self.transcription_id,
            TranscriptionSegment.start_time.name: self.start_time,
            TranscriptionSegment.end_time.name: self.end_time,
            TranscriptionSegment.segment.name: self.segment,
            TranscriptionSegment.start_ts.name: self.start_ts,
        }
        return TranscriptionSegment(**res_dict)


class TranscriptModel(CoreDBBase):
    call_leg_id: int
    language: str | None = TRANSCRIPT_LANGUAGE_EN
    transcription_segments: list[TranscriptSegmentModel] | None = None
    model_config = ConfigDict(from_attributes=True)

    def build_db_model(self):
        from hs.voice.transcription_store import Transcription

        res_dict = {
            Transcription.call_leg_id.name: self.call_leg_id,
            Transcription.language.name: self.language if self.language else TRANSCRIPT_LANGUAGE_EN,
        }
        return Transcription(**res_dict)


class TranscriptCreateRequestModel(CoreDBBase):
    auth_id: str
    recording_uuid: str
    call_leg_uuid: str


class TranscriptionPlivoResponseModel(CoreDBBase):
    api_id: str
    cost: float
    rate: float
    recording_duration_ms: int
    recording_start_ms: int
    status: str
    transcription: list[TranscriptSegmentModel]


class TranscriptionSegmentResponseModel(BaseModel):
    speaker: str
    start_time: int
    text: str
