from enum import Enum

from pydantic import ConfigDict

from hs.infra.models import CoreDBBase


class CallParticipantRole(str, Enum):
    AGENT = "agent"
    CUSTOMER = "customer"
    SUPERVISOR = "supervisor"


class CallRecordingType(str, Enum):
    MPC = "mpc"
    PARTICIPANT = "participant"


INTERNAL_PARTICIPANTS = [CallParticipantRole.AGENT.value, CallParticipantRole.SUPERVISOR.value]


class CallLegModel(CoreDBBase):
    call_uuid: str
    parent_call_leg: int | None = None
    participant: str | None = None
    start_time: str
    end_time: str | None = None
    role: str | None = None
    model_config = ConfigDict(from_attributes=True)


class CallLegInsertModel(CoreDBBase):
    call_uuid: str
    call_leg_uuid: str | None = None
    member_address: str | None = None
    start_time: str
    end_time: str | None = None
    role: str | None = None
    participant: str | None = None

    def build_db_model(self, parent_call_id: int | None = None):
        from hs.voice.call_store import CallLeg

        res_dict = {
            CallLeg.call_uuid.name: self.call_leg_uuid,
            CallLeg.parent_call_leg.name: parent_call_id if parent_call_id else None,
            CallLeg.start_time.name: self.start_time,
            CallLeg.participant.name: self.participant if self.participant else None,
            CallLeg.end_time.name: self.end_time if self.end_time else None,
            CallLeg.role.name: self.role if self.role else None,
        }
        return CallLeg(**res_dict)

    def build_parent_call_leg_db_model(self):
        from hs.voice.call_store import CallLeg

        res_dict = {
            CallLeg.call_uuid.name: self.call_leg_uuid,
            CallLeg.start_time.name: self.start_time,
            CallLeg.end_time.name: self.end_time if self.end_time else None,
        }
        return CallLeg(**res_dict)


class CallRecordingRequestModel(CoreDBBase):
    call_uuid: str
    call_leg_uuid: str
    recording_url: str
    recording_duration: str
    recording_uuid: str
    recording_format: str
    recording_start_time: str
    recording_end_time: str
    recording_type: str

    def build_db_model(self, call_leg_id: int):
        from hs.conversations.store import Recording

        res_dict = {
            Recording.recording_url.name: self.recording_url,
            Recording.recording_duration.name: self.recording_duration,
            Recording.recording_uuid.name: self.recording_uuid,
            Recording.recording_format.name: self.recording_format,
            Recording.recording_start_time.name: self.recording_start_time,
            Recording.recording_end_time.name: self.recording_end_time,
            Recording.call_leg_id.name: call_leg_id,
        }
        return Recording(**res_dict)
