"""The event catalog: the `data` model of every event type. Additive changes only inside v1."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class Data(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class Status(Data):
    id: str
    name: str


class ConversationStarted(Data):
    number: str | None
    entry_point_id: str | None
    hours: Literal["open", "closed", "none"]


class MenuInput(Data):
    menu_id: str
    input: str  # a digit, no_input or invalid
    attempt: int
    action: dict | None


class Redirected(Data):
    url: str


class Queued(Data):
    queue_id: str
    from_: str | None = Field(alias="from")
    to: str | None


class AfterHours(Data):
    number: str | None
    action: dict | None


class Overflowed(Data):
    queue_id: str
    reason: Literal["wait_timeout", "dtmf", "assignment_failed", "enqueue_failed"]
    action: dict | None


class OfferCreated(Data):
    offer_id: str
    queue_id: str | None
    from_: str | None = Field(alias="from")
    ring_timeout_sec: int
    auto_accept: bool


class Offer(Data):
    offer_id: str


class OfferMissed(Offer):
    strike: int


class OfferWithdrawn(Offer):
    reason: Literal["taken", "caller_left"]


class CallStarted(Data):
    from_: str | None = Field(alias="from")
    to: str | None


class CallAnswered(Data):
    call_uuid: str


class CallFailed(Data):
    result: str
    hangup_cause_code: int | None


class ByAgent(Data):
    by_agent_id: str | None


class Transferred(Data):
    transfer_id: str
    mode: str
    from_agent_id: str | None
    target: dict
    status: str


class Participant(Data):
    participant_id: str
    participant: dict
    role: str


class Supervisor(Data):
    supervisor_id: str
    mode: Literal["listen", "whisper", "barge"]


class CallEnded(Data):
    from_: str | None = Field(alias="from")
    to: str | None
    result: str
    queue_id: str | None
    hangup_cause: str | None
    hangup_cause_code: int | None
    duration_sec: int | None
    talk_sec: int | None
    ended_by: Literal["customer", "agent", "system"]


class WrapUpStarted(Data):
    mode: Literal["manual", "auto"]
    deadline_at: datetime | None


class WrappedUp(Data):
    disposition_id: str | None
    disposition: str | None
    sub_disposition: str | None
    note: str | None
    system: bool
    skipped: bool


class ClosedDisposition(Data):
    agent_id: str | None
    disposition_id: str | None
    sub_disposition_id: str | None
    system: bool
    skipped: bool


class Closed(Data):
    result: str
    dispositions: list[ClosedDisposition]
    recording_ids: list[str]
    duration_sec: int | None


class RecordingReady(Data):
    recording_id: str
    duration_sec: int | None


class TranscriptReady(Data):
    recording_id: str


class Mailbox(Data):
    queue_id: str | None = None
    agent_id: str | None = None


class VoicemailReceived(Data):
    recording_id: str
    mailbox: Mailbox
    from_: str | None = Field(alias="from")
    transcript: str | None = None


class StatusChanged(Data):
    from_status: Status | None
    to_status: Status
    reason: Literal["manual", "admin", "heartbeat", "rona"]
    reason_for_change: str | None = None


class WebhookTest(Data):
    webhook_id: str


CATALOG: dict[str, type[Data]] = {
    "conversation.started": ConversationStarted,
    "conversation.menu_input": MenuInput,
    "conversation.redirected": Redirected,
    "conversation.queued": Queued,
    "conversation.after_hours": AfterHours,
    "conversation.overflowed": Overflowed,
    "offer.created": OfferCreated,
    "offer.accepted": Offer,
    "offer.missed": OfferMissed,
    "offer.rejected": Offer,
    "offer.withdrawn": OfferWithdrawn,
    "call.started": CallStarted,
    "call.answered": CallAnswered,
    "call.failed": CallFailed,
    "call.held": ByAgent,
    "call.resumed": ByAgent,
    "call.recording_paused": ByAgent,
    "call.recording_resumed": ByAgent,
    "call.transferred": Transferred,
    "call.participant_added": Participant,
    "call.participant_removed": Participant,
    "supervisor.joined": Supervisor,
    "supervisor.mode_changed": Supervisor,
    "supervisor.left": Supervisor,
    "call.ended": CallEnded,
    "conversation.wrap_up_started": WrapUpStarted,
    "conversation.wrapped_up": WrappedUp,
    "conversation.closed": Closed,
    "recording.ready": RecordingReady,
    "transcript.ready": TranscriptReady,
    "voicemail.received": VoicemailReceived,
    "agent.status_changed": StatusChanged,
    "webhook.test": WebhookTest,
}
