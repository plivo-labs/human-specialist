"""The configuration shapes: what the API takes and returns, and what a config document stores."""

import re
import time
from datetime import date as Date
from datetime import datetime
from email.utils import parseaddr
from typing import Annotated, Literal
from uuid import UUID
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, create_model, model_validator

from hs import access

E164_PATTERN = r"^\+[1-9]\d{6,14}$"
E164 = Annotated[str, Field(pattern=E164_PATTERN)]
EMAIL_PATTERN = r"^\S+@[^@\s.][^@\s]*\.[^@\s]*[^@\s.]$"
HttpUrl = Annotated[str, Field(pattern=r"^https?://\S+$", max_length=2000)]
DAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
MenuKey = Literal["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "*"]
Digit = Literal[MenuKey, "#"]
HHMM = Annotated[str, Field(pattern=r"^(([01]\d|2[0-3]):[0-5]\d|24:00)$")]
STRATEGIES = ("availability", "simultaneous", "fewest_handled", "least_handle_time")
Name = Annotated[str, Field(min_length=1, max_length=200)]
ROLES = ("admin", "agent", "supervisor")


STORED = object()


def address(channel: str, value: str) -> str:
    if channel == "email":
        value, pattern = parseaddr(value)[1], EMAIL_PATTERN
    elif channel in ("sms", "whatsapp"):
        value, pattern = "+" + "".join(value.split()).removeprefix("+"), E164_PATTERN
    else:
        return value
    if not re.fullmatch(pattern, value):
        raise ValueError(f"not an address of {channel}")
    return value


class Body(BaseModel):
    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="wrap")
    @classmethod
    def _drop_removed_keys(cls, data, handler, info):
        if info.context is STORED and isinstance(data, dict):
            data = {k: v for k, v in data.items() if k in cls.model_fields}
        return handler(data)


class Say(Body):
    """Text to speak (optionally with a voice and language), or an audio file to play."""

    text: str | None = Field(None, min_length=1, max_length=2000)
    voice: str | None = Field(None, max_length=100)
    language: str | None = Field(None, max_length=20)
    audio_url: HttpUrl | None = None

    @model_validator(mode="after")
    def _one(self):
        if (self.text is None) == (self.audio_url is None):
            raise ValueError("give either text or audio_url")
        return self


def ssml(words: str) -> tuple[str, str]:
    """An SSML prompt's Polly voice (Polly.Joey unless its <speak> names one) and its content."""
    m = re.fullmatch(
        r"\s*<speak(\s[^>]*)?>(.*)</speak>\s*",
        words.replace("Speak", "speak").replace("Voice", "voice").replace('"', "'"),
        re.S,
    )
    if m is None:
        raise ValueError("ssml text must be one <speak> element")
    voice = re.search(r"voice\s*=\s*'([^']+)'", m[1] or "")
    return (voice[1] if voice else "Polly.Joey"), m[2].replace("\n", "").replace("\t", "")


class WaitPrompt(Say):
    """Played in order while the caller waits, then only the `loop` prompts, over and over. The text may hold
    {position}, {wait_time}, {min_wait_time}, {max_wait_time} and {wait_minutes}."""

    repeat: int = Field(0, ge=0, le=10, description="Times it plays again before the next prompt.")
    loop: bool = False
    ssml: bool = Field(False, description="The text is SSML, for a Polly voice.")

    @model_validator(mode="after")
    def _ssml(self):
        if self.ssml:
            ssml(self.text or "")
        return self


class QueueAction(Body):
    type: Literal["queue"]
    queue_id: UUID
    agents: list[UUID] = []
    hold_position: bool = Field(False, description="A caller no agent has answered keeps its place in line.")


class AgentTarget(Body):
    type: Literal["agent"]
    agent_id: UUID


DialNumber = Annotated[
    str,
    Field(
        pattern=r"^\+[1-9]\d{6,14}(#\d{1,20})?$",
        description="E.164, then optionally `#` and digits sent once it answers",
    ),
]


class NameIn(Body):
    id: UUID | None = Field(
        None, exclude=True, description="Keep an id from another system; omit for a new one. 409 id_taken if in use."
    )
    name: Name


class DestinationTarget(Body):
    type: Literal["destination"]
    destination_id: UUID


class DestinationIn(NameIn):
    """A transfer target kept by name: a number to dial, or a URL to redirect the caller to."""

    model_config = ConfigDict(json_schema_extra={"oneOf": [{"required": ["number"]}, {"required": ["url"]}]})
    number: DialNumber | None = None
    url: HttpUrl | None = None

    @model_validator(mode="after")
    def _one(self):
        if (self.number is None) == (self.url is None):
            raise ValueError("give either number or url")
        return self


class Destination(DestinationIn):
    model_config = ConfigDict(json_schema_extra=None)
    id: str


class TeamTarget(Body):
    type: Literal["team"]
    team_id: UUID


TextTarget = Annotated[AgentTarget | TeamTarget | QueueAction, Field(discriminator="type")]


class TransferOut(BaseModel):
    transfer_id: str


class ParticipantOut(BaseModel):
    participant_id: str


class MenuAction(Body):
    type: Literal["menu"]
    menu_id: UUID


class VoicemailBox(Body):
    queue_id: UUID | None = None
    agent_id: UUID | None = None

    @model_validator(mode="after")
    def _one(self):
        if (self.queue_id is None) == (self.agent_id is None):
            raise ValueError("give either queue_id or agent_id")
        return self


class VoicemailAction(Body):
    type: Literal["voicemail"]
    greeting: Say | None = None
    max_length_sec: int = Field(60, ge=1, le=3600)
    transcribe: bool = False
    mailbox: VoicemailBox


class ForwardAction(Body):
    type: Literal["forward"]
    number: E164


class RedirectAction(Body):
    type: Literal["redirect"]
    url: HttpUrl


class HangupAction(Body):
    type: Literal["hangup"]
    greeting: Say | None = None


Action = Annotated[
    QueueAction | MenuAction | VoicemailAction | ForwardAction | RedirectAction | HangupAction,
    Field(discriminator="type"),
]


class Skill(Body):
    skill_id: UUID
    operator: Literal["", "=", "!=", "<>", "<", "<=", ">", ">="] = ">="
    level: int = Field(ge=0, le=100)


class Sticky(Body):
    mode: Literal["loose", "strict"]
    expiry_hours: int | None = Field(None, ge=1, le=8760)  # None: never expires


class AutoAccept(Body):
    enabled: bool = False
    delay: int = Field(0, ge=0, le=100, description="Seconds: at most 60 on a queue, 100 on an enqueue's own route.")


class HoldMusic(Body):
    customer: HttpUrl | None = None
    agent: HttpUrl | None = None


Line = Annotated[str, Field(min_length=1, max_length=2000)]


class TextLines(Body):
    waiting: Line | None = None
    assigned: Line | None = None
    closed: Line | None = None


class Named(BaseModel):
    id: str
    name: str


class NamePatch(Body):
    name: Name | None = None


TextChannel = Literal["chat", "sms", "whatsapp", "email"]
Lined = Literal["chat", "sms", "whatsapp"]
Wait = Annotated[int, Field(ge=0, le=604800)]


class SkillIn(NameIn):
    is_level_required: bool = True
    min_level: int = Field(1, ge=0, le=100)
    max_level: int = Field(10, ge=0, le=100)

    @model_validator(mode="after")
    def _range(self):
        if self.is_level_required and self.min_level >= self.max_level:
            raise ValueError("max_level is below min_level")
        return self


class SkillOut(Named):
    is_level_required: bool = True
    min_level: int = 1
    max_level: int = 10


class QueueText(Body):
    """What a queue does for every channel but a call."""

    wait_timeout_sec: Wait | dict[Literal["chat", "sms", "whatsapp", "email", "task", "callback"], Wait] = Field(
        0, description="0: wait for an agent. An object sets it per channel; a channel it leaves out waits."
    )
    messages: dict[Lined, TextLines] = Field({}, description="The lines the customer gets on each channel.")


class Route(Body):
    """Whom a queue offers a conversation to, and how: the queue's own, or the one its enqueue brought instead. A
    conversation keeps the route it was queued with."""

    strategy: Literal[STRATEGIES] = "availability"
    teams: list[UUID] = []
    agents: list[UUID] = []
    skills: list[Skill] = []
    agents_at_once: int = Field(3, ge=0, le=10, description="0: offered only to its sticky agent or a freed one.")
    retries: int = Field(3, ge=0, le=10, description="Extra offer rounds; always 0 for simultaneous.")
    sticky: Sticky | None = None
    auto_accept: AutoAccept = AutoAccept()

    @model_validator(mode="after")
    def _simultaneous(self):
        if self.strategy == "simultaneous":
            self.retries = 0
        return self


class OwnRoute(Route):
    """The route an enqueue brings in place of its queue's. Its defaults are not a queue's: one round, loosely sticky."""

    retries: int = Field(0, ge=0, le=10, description="Extra offer rounds; always 0 for simultaneous.")
    sticky: Sticky | None = Sticky(mode="loose")

    @model_validator(mode="after")
    def _someone(self):
        if not (self.teams or self.agents):
            raise ValueError("a route names agents or teams")
        return self


class _Inline:
    """Its schema in place of a reference: no operation names OwnRoute, so it is no OpenAPI component."""

    def __get_pydantic_json_schema__(self, schema, handler):
        return handler.resolve_ref_schema(handler(schema))


EnqueueRoute = Annotated[OwnRoute, _Inline()]


class QueueIn(Route, NameIn):
    enabled: bool = Field(True, description="A disabled queue refuses new contacts and keeps those it holds.")
    priority: int = Field(5, ge=1, le=10)
    dequeue_order: Literal["fifo", "lifo"] = "fifo"
    capacity: int = Field(50, ge=1, le=10_000)
    wait_timeout_sec: int = Field(20, ge=0, le=86_400)
    hold_music: HoldMusic = HoldMusic()
    overflow_action: Action | None = None
    dtmf_actions: dict[Digit, Action] = {}
    wait_prompts: list[WaitPrompt] = Field([], max_length=10)
    after_agent_action: Action | None = None
    text: QueueText = QueueText()

    @model_validator(mode="after")
    def _delay(self):
        if self.auto_accept.delay > 60:
            raise ValueError("a queue auto-accepts within 60 s")
        return self


class Queue(QueueIn):
    id: UUID
    enqueue_url: str


class Slot(Body):
    start: HHMM
    end: HHMM

    @model_validator(mode="after")
    def _ordered(self):
        if self.start >= self.end:
            raise ValueError("start must be before end")
        return self


class WeeklySlot(Slot):
    day: Literal[DAYS]


class Holiday(Body):
    date: Date
    slots: list[Slot] = Field([], description="Open only inside these slots; none: closed all day.")


def _zone(value: str) -> str:
    try:
        ZoneInfo(value)
    except (ZoneInfoNotFoundError, ValueError) as e:
        raise ValueError("not an IANA time zone") from e
    return value


Zone = Annotated[str, AfterValidator(_zone)]


class File(Body):
    """A file as the uploader that holds it describes it."""

    url: str | None = None
    name: str | None = None
    size: int | None = None
    content_type: str | None = None
    id: str | None = None


class GroupHoliday(Holiday):
    name: Name


class HolidayGroupIn(NameIn):
    holidays: list[GroupHoliday] = []

    @model_validator(mode="after")
    def _unique(self):
        for field in ("name", "date"):
            if len({getattr(h, field) for h in self.holidays}) < len(self.holidays):
                raise ValueError(f"holiday {field}s must be unique")
        return self


class HolidayGroup(HolidayGroupIn):
    id: UUID


class HoursIn(NameIn):
    time_zone: Zone
    slots: list[WeeklySlot] = []
    holidays: list[Holiday] = []
    holiday_group_id: UUID | None = None
    selected_holidays: list[Date] = Field([], description="The group's holidays, by date, that close these hours.")

    @model_validator(mode="after")
    def _grouped(self):
        if self.selected_holidays and not self.holiday_group_id:
            raise ValueError("selected_holidays need a holiday_group_id")
        return self


class Hours(HoursIn):
    id: UUID


class HoursStatus(Body):
    open: bool


class MenuIn(NameIn):
    greeting: Say
    options: dict[MenuKey, Action] = Field({}, max_length=9)
    input_timeout_sec: int = Field(5, ge=1, le=60)
    retries: int = Field(3, ge=0, le=10)
    invalid_action: Action | None = None


class StoredMenu(MenuIn):
    """`#` ends Plivo's digit input, so a new option cannot take it; one saved before that rule still reads, and a
    PATCH that leaves `options` out keeps it."""

    options: dict[Digit, Action] = Field({}, max_length=9)


class Menu(StoredMenu):
    id: UUID
    menu_url: str


class EntryPointIn(Body):
    open_action: Action
    business_hours_id: UUID | None = None
    closed_action: Action | None = None


class EntryPoint(EntryPointIn):
    id: UUID
    number: str


Color = Annotated[str, Field(pattern=r"^#[0-9A-Fa-f]{6}$")]


class PresenceStatus(Named):
    availability: bool
    color: Color = "#9B9FAA"
    sequence: int = Field(description="List order, lowest first; a new status takes the first place.")
    default: bool = False


class PresenceStatusIn(NameIn):
    availability: bool = False
    color: Color = "#9B9FAA"
    sequence: int = Field(default_factory=lambda: -int(time.time() * 1000), description="Omit to list first.")


class StoredStatus(PresenceStatusIn):
    default: bool = False


class PresenceStatusPatch(NamePatch):
    availability: bool | None = None
    color: Color | None = None


class Disposition(Named):
    parent_disposition_id: str | None
    system: bool
    note_required: bool
    answered: bool = True
    disabled: bool = False
    sequence: int


class DispositionIn(NameIn):
    """`answered: false` files a conversation that ends in this disposition under it as its state in search, with no
    disposition, as the system ones of calls no agent answered do."""

    parent_disposition_id: str | None = None
    note_required: bool = False
    system: bool = False
    answered: bool = True


class DispositionPatch(NamePatch):
    parent_disposition_id: str | None = None
    note_required: bool | None = None
    disabled: bool | None = None


class Reorder(Body):
    ids: list[str] = Field(min_length=1)


STATUS_MAP_DEFAULTS = {
    "inbound": {
        "answered": "Auto-closed",
        "queue_timeout": "Queue timeout",
        "voicemail": "Voicemail",
        "abandoned": "Dropped",
        "after_hours": "Out of business hour",
        "rejected": "Dropped",
        "forwarded": "Forwarded",
        "enqueue_failed": "Enqueue failed",
        "assignment_failed": "Agent assignment failed",
        "queue_exit": "Queue exit",
    }
    | dict.fromkeys(("session_timeout", "agent_closed", "failed", "resolved", "cancelled"), "Auto-closed"),
    "outbound": {"answered": "Auto-closed"}
    | dict.fromkeys(
        ("no_answer", "busy", "rejected", "cancelled", "failed_invalid", "failed_carrier"), "Customer didn't respond"
    ),
}
# The system dispositions a tenant starts with: the status map's defaults, and one HS writes by name.
SYSTEM_DISPOSITIONS = (*dict.fromkeys(n for names in STATUS_MAP_DEFAULTS.values() for n in names.values()),)
SYSTEM_DISPOSITIONS += ("Transferred back to same agent",)
ANSWERED = ("Auto-closed", "Transferred back to same agent")
InboundMap, OutboundMap = (
    create_model(f"{d.title()}StatusMap", __base__=Body, **dict.fromkeys(STATUS_MAP_DEFAULTS[d], (str, ...)))
    for d in ("inbound", "outbound")
)


class StatusMap(Body):
    """Result -> system disposition id, per direction. A result its direction does not map takes the inbound one."""

    inbound: InboundMap
    outbound: OutboundMap


class Recording(Body):
    inbound: bool = False
    outbound: bool = False
    allow_pause: bool = True


class WrapUp(Body):
    mode: Literal["manual", "auto"] = "manual"
    seconds: int = Field(900, ge=1, le=3600)
    max_extend_sec: int = Field(30, ge=0, le=3600)


class Consent(Body):
    inbound: Say | None = None
    outbound: Say | None = None


def _canonical(v: str) -> str:
    try:
        return str(UUID(v))
    except ValueError:
        return v


StatusId = Annotated[str, AfterValidator(_canonical)]


class Rona(Body):
    strikes: int = Field(2, ge=0, le=100, description="Missed calls in a row that move the agent; 0: never.")
    status_id: StatusId


Slots = Annotated[int, Field(ge=0, le=100, description="0 holds one, as 1 does."), AfterValidator(lambda n: max(n, 1))]


class Capacity(Body):
    call: int = Field(1, ge=1, le=1)
    chat: Slots = 1
    sms: Slots = 1
    whatsapp: Slots = 1
    email: Slots = 1
    task: Slots = 1
    callback: Slots = 1


class Monitor(Body):
    barge_notice: str | None = Field("A supervisor has joined the call", min_length=1, max_length=500)


SessionIdle = Annotated[int, Field(ge=60, le=2592000)]


class SessionTimeouts(Body):
    chat: SessionIdle = 1800
    sms: SessionIdle = 259200
    whatsapp: SessionIdle = 1800
    email: SessionIdle = 86400


Delivery = Literal["plivo", "external"]


class TextDelivery(Body):
    sms: Delivery = "plivo"
    whatsapp: Delivery = "plivo"


OfferTimeout = Annotated[int, Field(ge=5, le=3600)]


class TextSettings(Body):
    offer_timeout_sec: OfferTimeout | dict[TextChannel, OfferTimeout] = Field(
        30, description="An object sets it per channel; a channel it leaves out takes 30."
    )
    session_timeout_sec: SessionTimeouts = SessionTimeouts()
    delivery: TextDelivery = TextDelivery()


class Transfer(Body):
    modes: Literal["both", "cold", "warm"] = Field(
        "both", description="Calls and texts. A warm transfer to a number needs both; a cold one always may."
    )


class OutboundSticky(Body):
    enabled: bool = False
    hours: int = Field(24, ge=0, le=8760, description="0: never expires.")


class WhatsAppAccount(Body):
    id: str = Field(pattern=r"^\d{1,30}$", description="The WhatsApp Business Account id at Plivo.")
    senders: tuple[E164, ...] = Field(min_length=1, max_length=100)


class WhatsApp(Body):
    accounts: list[WhatsAppAccount] = Field([], max_length=20, description="Unset: HS checks no sender or template.")


class Grants(Body):
    """What a role sees, by `view`: `org` the whole tenant; `team` its own work, the work of every agent sharing one
    of its teams and what no agent took in a queue routing to one; `self` its own work. Then the downloads,
    monitoring and barging it may use."""

    view: Literal[access.LEVELS]
    download_list: bool
    download_conversation: bool
    download_recording: bool
    monitor: bool
    barge: bool


class AdminGrants(Grants):
    view: Literal["org"]
    download_list: Literal[True]
    download_conversation: Literal[True]
    download_recording: Literal[True]


class AgentGrants(Grants):
    monitor: Literal[False]
    barge: Literal[False]


class Permissions(Body):
    admin: AdminGrants
    supervisor: Grants
    agent: AgentGrants


class StatusRef(Body):
    status_id: StatusId | None = Field(None, description="null: the sign-in or sign-out changes no status.")


class Idle(StatusRef):
    enabled: bool = False
    after_min: int = Field(30, ge=5, le=500, description="Minutes without desk activity before the idle status.")

    @model_validator(mode="after")
    def _needs_status(self):
        if self.enabled and not self.status_id:
            raise ValueError("idle tracking needs a status_id")
        return self


class Settings(Body):
    recording: Recording = Recording()
    wrap_up: WrapUp = WrapUp()
    consent: Consent = Consent()
    ring_timeout_sec: int = Field(30, ge=5, le=120)
    rona: Rona
    capacity: Capacity = Capacity()
    simultaneous_agent_limit: int = Field(
        3, ge=1, le=100, description="The most members a team task or a team transfer is offered to."
    )
    agent_direct_dial: bool = False
    monitor: Monitor = Monitor()
    text: TextSettings = TextSettings()
    outbound_sticky: OutboundSticky = OutboundSticky()
    transfer: Transfer = Transfer()
    default_from: dict[Literal["call", "sms", "whatsapp"], E164] = {}
    acw_blocks_routing: bool = Field(True, description="false: a wrap-up frees the agent's slot at once.")
    call_session_timeout_sec: int = Field(
        86400, ge=60, le=86400, description="A call HS never heard end closes this long after it opened."
    )
    sign_in: StatusRef | None = Field(None, description="Set on an unavailable agent at sign-in; unset: Available.")
    sign_out: StatusRef | None = Field(None, description="Set on an available agent at sign-out; unset: Offline.")
    whatsapp: WhatsApp = WhatsApp()
    idle: Idle = Idle()
    permissions: Permissions = Permissions.model_validate(access.PERMISSIONS)

    @model_validator(mode="after")
    def _whatsapp_from(self):
        senders = [s for a in self.whatsapp.accounts for s in a.senders]
        if senders and self.default_from.get("whatsapp", senders[0]) not in senders:
            raise ValueError("default_from.whatsapp is a sender of a whatsapp account")
        return self


class Stored(Settings):
    """The tenant's settings document: the public settings plus what other resources and HS itself keep there."""

    status_map: dict[str, dict[str, str]] = {"inbound": {}, "outbound": {}}


class TenantIn(BaseModel):
    name: Name
    plivo_auth_id: str = Field(min_length=1)
    plivo_auth_token: str = Field(min_length=1)
    fallback_answer_url: str | None = None
    plivo_app: bool = Field(
        True,
        description="false: HS makes and repoints no Plivo application; the tenant's own application hands calls to"
        " a queue's enqueue_url.",
    )


class TenantPatch(BaseModel):
    name: Name | None = None
    plivo_auth_token: str | None = Field(None, min_length=1)


class Tenant(Named):
    plivo_auth_id: str
    plivo_app_id: str | None
    created_at: datetime


class Registered(BaseModel):
    tenant_id: str
    plivo_app_id: str | None
    api_key: str
