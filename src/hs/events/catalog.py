"""The event catalog: the `data` model of every event type. Additive changes only inside v1."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

Channel = Literal["call", "chat", "sms", "whatsapp", "email", "task", "callback"]


class Data(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class Status(Data):
    id: str
    name: str


class ConversationStarted(Data):
    channel: Channel
    number: str | None = None
    entry_point_id: str | None = None
    hours: Literal["open", "closed", "none"] = "none"


class MenuInput(Data):
    menu_id: str
    input: str  # a digit, no_input or invalid
    attempt: int
    action: dict | None


class Redirected(Data):
    url: str


class Parties(Data):
    from_: str | None = Field(alias="from")
    to: str | None


class Queued(Parties):
    queue_id: str


class AfterHours(Data):
    number: str | None
    action: dict | None


class Overflowed(Data):
    queue_id: str
    reason: Literal["wait_timeout", "dtmf", "assignment_failed", "enqueue_failed"]
    action: dict | None


Via = Literal["queue", "transfer", "participant"]


class OfferCreated(Data):
    offer_id: str
    conversation_number: str
    via: Via
    channel: Channel
    queue_id: str | None
    from_: str | None = Field(alias="from")
    to: str | None
    ring_timeout_sec: int
    auto_accept: bool


class Offer(Data):
    offer_id: str


class OfferMissed(Offer):
    strike: int | None = None  # RONA strikes count on calls only
    reason: str | None = None


class OfferWithdrawn(Offer):
    reason: Literal["taken", "caller_left", "cancelled", "agent_offline"]


class CallAnswered(Data):
    call_uuid: str


class CallFailed(Data):
    result: str
    hangup_cause_code: int | None


class ByAgent(Data):
    by_agent_id: str | None


class Held(ByAgent):
    participant_id: str | None = None


class Transferred(Data):
    transfer_id: str
    mode: str
    from_agent_id: str | None
    target: dict
    status: Literal["initiated", "accepted", "completed", "failed", "cancelled"]


class TextTransferred(Transferred):
    to_agent_id: str | None = None


class Participant(Data):
    participant_id: str
    participant: dict
    role: str


class ParticipantRemoved(Participant):
    reason: Literal["no_answer", "left", "removed"]


class Supervisor(Data):
    supervisor_id: str
    mode: Literal["listen", "whisper", "barge"]
    visible: bool


class CallEnded(Parties):
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
    duration_sec: int


class ClosedDisposition(Data):
    agent_id: str | None
    disposition_id: str | None
    sub_disposition_id: str | None
    disposition: str | None = Field(None, description="Its name, which a deleted disposition keeps.")
    sub_disposition: str | None = None
    system: bool
    skipped: bool


class Closed(Data):
    result: str
    dispositions: list[ClosedDisposition]
    recording_ids: list[str]
    duration_sec: int | None


class CsatRecorded(Data):
    score: str


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
    available: bool | None = None
    reason: Literal["manual", "admin", "heartbeat", "rona", "idle"]
    reason_for_change: str | None = None


CARRIED = Field(None, exclude_if=lambda v: v is None)


class Attachment(Data):
    url: str | None  # None for a Plivo-hosted file: events outlive a signed URL, so read the message for one
    name: str | None = CARRIED
    size: int | None = CARRIED
    content_type: str | None = CARRIED
    id: str | None = CARRIED


class Email(Data):
    subject: str | None = None
    message_id: str | None = None
    in_reply_to: str | None = None
    references: list[str] = []
    to: list[str] = []
    cc: list[str] = []
    bcc: list[str] = []
    reply_to: list[str] = []


class MessageReceived(Parties):
    message_id: str
    channel: Channel
    text: str | None = None
    attachments: list[Attachment] = []
    email: Email | None = None
    whatsapp: dict | None = None
    reply_to: str | None = None


class MessageSent(MessageReceived):
    template: dict | None = None
    delivery: Literal["plivo", "external"]
    by_agent_id: str | None
    provider_id: str | None = None


class MessageStatusChanged(Data):
    message_id: str
    status: Literal["pending", "queued", "sent", "delivered", "read", "deleted", "undelivered", "failed"]
    error_code: str | None = None
    error_title: str | None = None
    error_details: str | None = None
    provider_id: str | None = None
    reported_at: datetime | None = None


class NoteChanged(Data):
    note_id: str
    text: str
    attachments: list[dict]
    by_agent_id: str | None


class TaskCreated(Data):
    task_id: str
    type: str
    title: str
    priority: Literal["low", "medium", "high", "critical"]
    due_at: datetime | None
    scheduled_at: datetime | None


class TaskUpdated(Data):
    task_id: str
    changes: dict[str, tuple[object, object]]  # field: [from, to]
    by_agent_id: str | None


class WebhookTest(Data):
    webhook_id: str


SUPERVISOR = ("joined", "mode_changed", "left", "revealed", "assistance_requested", "assistance_rejected", "failed")
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
    "call.started": Parties,
    "call.answered": CallAnswered,
    "call.failed": CallFailed,
    **dict.fromkeys(("call.held", "call.resumed"), Held),
    **dict.fromkeys(("call.recording_paused", "call.recording_resumed"), ByAgent),
    "call.transferred": Transferred,
    "conversation.transferred": TextTransferred,
    **dict.fromkeys(("call.participant_added", "conversation.participant_removed"), Participant),
    "call.participant_removed": ParticipantRemoved,
    **{f"supervisor.{k}": Supervisor for k in SUPERVISOR},
    "call.ended": CallEnded,
    "conversation.wrap_up_started": WrapUpStarted,
    "conversation.wrapped_up": WrappedUp,
    "conversation.closed": Closed,
    "conversation.csat_recorded": CsatRecorded,
    "recording.ready": RecordingReady,
    "transcript.ready": TranscriptReady,
    "voicemail.received": VoicemailReceived,
    "agent.status_changed": StatusChanged,
    "webhook.test": WebhookTest,
    "message.received": MessageReceived,
    "message.sent": MessageSent,
    "message.status_changed": MessageStatusChanged,
    "note.added": NoteChanged,
    "note.deleted": NoteChanged,
    "note.updated": NoteChanged,
    "task.created": TaskCreated,
    "task.updated": TaskUpdated,
}
