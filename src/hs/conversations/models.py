import enum
import re
from typing import Annotated, Optional, Union
from uuid import UUID, uuid4

from pydantic import ConfigDict, Field, SerializeAsAny, StringConstraints, field_validator, model_validator

from hs.infra.base_model import BaseModel, Datetime
from hs.infra.models import CoreDBBase

try:
    from hs.admin.account_models import AccountOrgMappingModel, AOMFullModel, OrganizationModel
except Exception:

    class OrganizationModel(CoreDBBase):
        name: str
        auth_id: str
        auth_token: str
        skip_contact_creation: bool
        skip_case_creation: bool
        allow_acw_post_transfer: bool
        model_config = ConfigDict(from_attributes=True)

    class AccountOrgMappingModel(CoreDBBase):
        organization: str
        sip_endpoint: str | None = None
        model_config = ConfigDict(from_attributes=True)

    class AOMFullModel(AccountOrgMappingModel):
        org: OrganizationModel


from hs.infra.helpers import get_now

# Stored contact identifiers, customer contacts and sender ids are in this format rather than hs.infra.helpers'
# E.164 one: switching needs a data migration.
PHONE_NUMBER_REGEX = r"^\+?[1-9]\d{1,14}$"


def get_number_in_e164_format(number) -> str:
    return (number or "").strip().lower()


def validate_phone_number(number):
    if number and not re.search(PHONE_NUMBER_REGEX, number):
        raise ValueError("Invalid number")
    return number


def is_valid_phone_number(number):
    try:
        if number:
            validate_phone_number(get_number_in_e164_format(number))
            return True
    except Exception:
        pass
    return False


class ConversationChannels(enum.Enum):
    call = 1
    message = 2
    chat = 3
    task = 5
    callback = 6
    email = 7


class ConversationParentChannels(enum.Enum):
    call = 1
    message = 2
    chat = 3
    task = 6
    callback = 7
    email = 8


class ConversationChunkTypes(enum.Enum):
    message = 1
    chat = 2
    multi = 3
    transcription = 4
    recording = 5
    notes = 6
    file = 7
    signal = 8
    text = 10
    audio = 11
    video = 12
    document = 13
    location = 14
    contacts = 15
    image = 16
    sticker = 17
    voice = 18
    template = 19
    button = 20
    sms = 21
    mms = 22
    voicemail = 23
    task = 24
    single_product = 25
    multi_product = 26
    list = 27
    list_reply = 28
    button_reply = 29
    order = 30
    transfer = 31
    chat_menu_button = 34
    customer_feedback = 36
    call_feedback = 37
    hangup = 38
    email = 40
    flow_execution = 41
    multi_product_reply = 42


CUSTOMER_COMMUNICATION_CHUNK_TYPES = [
    ConversationChunkTypes.message.name,
    ConversationChunkTypes.chat.name,
    ConversationChunkTypes.text.name,
    ConversationChunkTypes.audio.name,
    ConversationChunkTypes.video.name,
    ConversationChunkTypes.document.name,
    ConversationChunkTypes.location.name,
    ConversationChunkTypes.multi.name,
    ConversationChunkTypes.contacts.name,
    ConversationChunkTypes.image.name,
    ConversationChunkTypes.sticker.name,
    ConversationChunkTypes.voice.name,
    ConversationChunkTypes.template.name,
    ConversationChunkTypes.button.name,
    ConversationChunkTypes.sms.name,
    ConversationChunkTypes.mms.name,
    ConversationChunkTypes.single_product.name,
    ConversationChunkTypes.multi_product.name,
    ConversationChunkTypes.list.name,
    ConversationChunkTypes.list_reply.name,
    ConversationChunkTypes.button_reply.name,
    ConversationChunkTypes.order.name,
    ConversationChunkTypes.email.name,
    ConversationChunkTypes.file.name,
    ConversationChunkTypes.chat_menu_button.name,
    ConversationChunkTypes.multi_product_reply.name,
]
AGENT_SPECIFIC_CHUNK_TYPES = [
    ConversationChunkTypes.notes.name,
    ConversationChunkTypes.signal.name,
    ConversationChunkTypes.recording.name,
    ConversationChunkTypes.transfer.name,
]


class ConversationStatus(enum.Enum):
    open = 1
    on_hold = 2
    contact_creation_pending = 3
    case_creation_pending = 4
    in_wrap_up = 5
    closed = 6
    waiting = 7
    session_resolved = 8


RESOLVE_STATUSES = [
    ConversationStatus.contact_creation_pending.name,
    ConversationStatus.case_creation_pending.name,
    ConversationStatus.in_wrap_up.name,
]
OPEN_STATUSES = [ConversationStatus.open.name, ConversationStatus.waiting.name]


class ConversationStates(enum.Enum):
    agent_answered = 1
    agent_unanswered = 2
    outbound_contact_answered = 3
    outbound_contact_unanswered = 4
    bot_answered = 5
    transferred = 6
    outbound_contact_failed = 7
    outbound_contact_agent_joined = 8
    external_transfer_initiated = 9
    async_outbound_agent_joined = 10
    async_outbound_contact_unanswered = 11
    async_outbound_contact_answered = 12
    async_outbound_failed = 13
    async_contact_or_channel_switch_failed = 14
    transferring = 15
    transfer_failed = 16


CONVERSATION_ANSWERED_STATES = [
    ConversationStates.agent_answered.name,
    ConversationStates.outbound_contact_answered.name,
    ConversationStates.async_outbound_contact_answered.name,
    ConversationStates.external_transfer_initiated.name,
    ConversationStates.async_outbound_agent_joined.name,
    ConversationStates.transferred.name,
    ConversationStates.transferring.name,
    ConversationStates.transfer_failed.name,
]


class ConversationGenerationTypes(enum.Enum):
    qnr_generated = 1
    non_qnr_generated = 2
    transfer_generated = 3
    outbound_generated = 4
    channel_switch = 5
    transfer_non_qnr_generated = 6
    transfer_qnr_generated = 7
    contact_id_switch = 8
    task_generated = 9


class ConversationClientTypes(str, enum.Enum):
    sdk_web = "web-sdk"
    sdk_ios = "ios-sdk"
    sdk_android = "android-sdk"


class ConversationParentStates(str, enum.Enum):
    open = "open"
    closed = "closed"
    queue_transferred = "queue_transferred"


class ConversationCustomStatusMappingStates(enum.Enum):
    new = "new"
    open = "open"
    in_progress_ai = "in_progress_ai"
    in_progress_human = "in_progress_human"
    waiting_on_customer = "waiting_on_customer"
    transferred = "transferred"
    inactive = "inactive"
    resolved = "resolved"
    in_wrap_up = "in_wrap_up"
    case_creation_pending = "case_creation_pending"
    contact_creation_pending = "contact_creation_pending"


class ConversationCustomStatusMappingModel(BaseModel):
    id: int
    state: str


class SecondaryAgentStateEnum(str, enum.Enum):
    monitor = "monitor"
    monitor_visible = "monitor_visible"
    monitor_paused = "monitor_paused"
    coach = "coach"
    barge = "barge"
    transferred = "transferred"
    add_participant_success = "add_participant_success"
    participant_did_not_answered = "participant_did_not_answered"
    add_participant_merge_success = "add_participant_merge_success"
    add_participant_merge_failed = "add_participant_merge_failed"
    participant_removed = "participant_removed"
    participant_removed_conference = "participant_removed_conference"
    participant_left = "participant_left"
    participant_left_conference = "participant_left_conference"

    @staticmethod
    def get_conversation_message(agent_state):
        messages = {
            "add_participant_success": "{participant_name} joined the conversation",
            "participant_did_not_answered": "Call declined or participant failed to pick up",
            "add_participant_merge_success": "Call merged successfully",
            "add_participant_merge_failed": "Call merging unsuccessful",
            "participant_removed": "{participant_name} removed from the conversation",
            "participant_left": "{participant_name} left the conversation",
            "participant_left_conference": "{participant_name} left the conference call",
            "participant_removed_conference": "{participant_name} removed from the conference call",
        }
        return messages.get(agent_state, "")


NON_QNR_CHILDREN = [
    ConversationGenerationTypes.non_qnr_generated.name,
    ConversationGenerationTypes.transfer_non_qnr_generated.name,
]
QNR_CHILDREN = [
    ConversationGenerationTypes.qnr_generated.name,
    ConversationGenerationTypes.transfer_qnr_generated.name,
    ConversationGenerationTypes.task_generated.name,
]
TRANSER_CHILDREN = [
    ConversationGenerationTypes.transfer_generated.name,
    ConversationGenerationTypes.transfer_qnr_generated.name,
]


class ConversationTransferTypes(enum.Enum):
    agent = 1
    team = 2
    external = 3
    flow = 4


class SecAgentTypes(str, enum.Enum):
    add_participant = "add_participant"
    agent_transfer = "agent_transfer"
    task_creation = "task_creation"
    supervisor_participant = "supervisor_participant"
    external_transfer = "external_transfer"


class TimeoutTypes(str, enum.Enum):
    session_timeout = "session_timeout"
    wrap_up_timeout = "wrap_up_timeout"


DEFAULT_GEN_TYPE_STATE_MAPPING = {
    ConversationGenerationTypes.qnr_generated.name: ConversationStates.agent_unanswered.name,
    ConversationGenerationTypes.transfer_qnr_generated.name: ConversationStates.agent_unanswered.name,
    ConversationGenerationTypes.non_qnr_generated.name: ConversationStates.bot_answered.name,
    ConversationGenerationTypes.transfer_non_qnr_generated.name: ConversationStates.bot_answered.name,
    ConversationGenerationTypes.transfer_generated.name: ConversationStates.agent_answered.name,
    ConversationGenerationTypes.outbound_generated.name: ConversationStates.outbound_contact_unanswered.name,
    ConversationGenerationTypes.channel_switch.name: ConversationStates.agent_answered.name,
}


class ConvChannel(BaseModel):
    channel: str | None = None

    @field_validator("channel")
    @classmethod
    def channel_must_be_in_enum(cls, v):
        allowed_values = ConversationChannels.__dict__["_member_names_"]
        if v and v not in allowed_values:
            raise ValueError(f"channel : {v} must be in {allowed_values}")
        return v


class ConvParentBase(BaseModel):
    parent_channel: str | None = None
    flow_closure_flag: bool | None = False

    @field_validator("parent_channel")
    @classmethod
    def channel_must_be_in_enum(cls, v):
        allowed_values = ConversationParentChannels.__dict__["_member_names_"]
        if v not in allowed_values:
            raise ValueError(f"parent_channel : {v} must be in {allowed_values}")
        return v


class ConvStatus(BaseModel):
    status: str | None = None

    @field_validator("status")
    @classmethod
    def status_must_be_in_enum(cls, v):
        allowed_values = ConversationStatus.__dict__["_member_names_"]
        if v not in allowed_values:
            raise ValueError(f"status : {v} must be in {allowed_values}")
        return v


class ConvState(BaseModel):
    state: str | None = None

    @field_validator("state")
    @classmethod
    def status_must_be_in_enum(cls, v):
        allowed_values = ConversationStates.__dict__["_member_names_"]
        if v not in allowed_values:
            raise ValueError(f"state : {v} must be in {allowed_values}")
        return v


class ConvGenerationType(BaseModel):
    generation_type: str | None = None

    @field_validator("generation_type")
    @classmethod
    def type_must_be_in_enum(cls, v):
        allowed_values = ConversationGenerationTypes.__dict__["_member_names_"]
        if v not in allowed_values:
            raise ValueError(f"type : {v} must be in {allowed_values}")
        return v


class ConversationUpdateByContactUUID(ConvState, ConvStatus, ConvChannel):
    contact_uuid: Annotated[str, StringConstraints(min_length=1)]
    agent_sip_or_uuid: Annotated[str, StringConstraints(min_length=1)] | None = None
    reason_for_unanswered: Annotated[str, StringConstraints(min_length=1)] | None = None
    active_queue_id: int | None = 0
    child_conversation_id: int | None = 0
    # state=outbound_contact_failed only: the cause call.failed reports
    failure_reason: Annotated[str, StringConstraints(min_length=1)] | None = None
    hangup_cause_code: Annotated[str, StringConstraints(min_length=1)] | None = None
    hangup_source: Annotated[str, StringConstraints(min_length=1)] | None = None
    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="after")
    def validate_payload(self):
        if self.state and (self.agent_sip_or_uuid or self.reason_for_unanswered or self.status):
            raise ValueError(
                "agent_sip_or_uuid and reason_for_unanswered and status are not required when state is not null"
            )
        if self.agent_sip_or_uuid and (self.state or self.reason_for_unanswered or self.status):
            raise ValueError(
                "state and reason_for_unanswered and status is not required when agent_sip_or_uuid  is not null"
            )
        if self.reason_for_unanswered and (self.agent_sip_or_uuid or self.state or self.status):
            raise ValueError(
                "agent_sip_or_uuid and state and status is not required when reason_for_unanswered  is not null"
            )
        if self.status and (self.agent_sip_or_uuid or self.reason_for_unanswered or self.state):
            raise ValueError(
                "agent_sip_or_uuid and reason_for_unanswered and state are not required when status is not null"
            )
        return self


class UpdateParentByConversationNumber(BaseModel):
    conversation_number: (
        Annotated[str, StringConstraints(min_length=1, max_length=18, strip_whitespace=True)] | None
    ) = None
    parent_contact_uuid: Annotated[str, StringConstraints(min_length=1, strip_whitespace=True)] | None = None
    state: ConversationParentStates
    is_flow_ended: bool | None = False


class SignalAttributes(BaseModel):
    signal_message: str
    is_actionable: bool = True
    identifier: str | None = None
    identifier_type: str | None = None
    # The customer-facing lifecycle signal's stable event name (AGENT_ASSIGNED / AGENT_LEFT /
    # CONVERSATION_CLOSED / ASSIGNMENT_FAILED / …), so a reader can classify the history.
    # Optional: other signal attributes omit it.
    event_name: str | None = None
    model_config = ConfigDict(extra="forbid")


class MonitorSignalAttributes(BaseModel):
    supervisor_message: str | None = None
    agent_message: str | None = None
    supervisor_name: str | None = None
    agent_name: str | None = None
    model_config = ConfigDict(extra="forbid")


class RecordChunkAttributes(BaseModel):
    recording_duration: str | None = Field(None, alias="RecordingDuration")
    recording_end_time: str | None = Field(None, alias="RecordingEndTime")
    recording_format: str | None = Field(None, alias="RecordingFormat")
    recording_resource_url: str | None = Field(None, alias="RecordingResourceURL")
    recording_start_time: str | None = Field(None, alias="RecordingStartTime")
    recording_url: str | None = Field(None, alias="RecordingURL")
    recording_uuid: str = Field(alias="RecordingUUID")
    model_config = ConfigDict(extra="allow", populate_by_name=True)


class MainCallRecordingAttributes(BaseModel):
    record_url: str | None = Field(None, alias="record_url")
    recording_duration: str | None = Field(None, alias="recording_duration")
    recording_start_ms: str | None = Field(None, alias="recording_start_ms")
    recording_end_ms: str | None = Field(None, alias="recording_end_ms")
    recording_id: str | None = Field(None, alias="recording_id")
    model_config = ConfigDict(extra="allow", populate_by_name=True)


class CallFeedBackAttributes(BaseModel):
    call_feedback_issue_type: str
    call_feedback_comment: str | None = None


class FeedbackChunkAttributes(BaseModel):
    min_scale: int
    max_scale: int
    feedback_type: str
    component_name: str
    flow_name: str


class HangupSourceAttributes(BaseModel):
    hangup_source: str


class FlowExecutionAttributes(BaseModel):
    flow_name: str
    flow_uuid: str
    type: str
    name: str
    component_id: str
    node_uuid: str
    is_error: bool
    output_state: str | None = None
    create_task_type: str | None = None
    selected_integration: dict | None = None
    selected_api: dict | None = None
    url: str | None = None
    method: str | None = None
    input_value: str | None = None
    assigned_agent_name: str | None = None
    assigned_agent_email: str | None = None
    to_user_name: str | None = None
    sender_id: str | None = None
    customer_contact: str | None = None
    customer_dropped: bool | None = None


VALID_ATTRIBUTES = (
    SignalAttributes,
    MonitorSignalAttributes,
    FeedbackChunkAttributes,
    CallFeedBackAttributes,
    HangupSourceAttributes,
    FlowExecutionAttributes,
    RecordChunkAttributes,
    MainCallRecordingAttributes,
)
ALL_CHUNK_ATTRIBUTES = list[Union[VALID_ATTRIBUTES]]  # noqa: UP007 - a Union over the tuple, which `X | Y` cannot spell


class ConversationDataDict(BaseModel):
    external_identifier: Annotated[str, StringConstraints(strip_whitespace=True)] | None = ""
    conversation_custom_data: dict | None = None

    def json(self, **kwargs):
        return self.conversation_custom_data


class ConversationClientData(BaseModel):
    client_type: ConversationClientTypes | None = Field(alias="clientType", default=None)

    def json(self, **kwargs):
        res_dict = super().model_dump(**kwargs, by_alias=True)
        if all(not value for value in res_dict.values()):
            return None
        return res_dict


class ConversationCustomerData(BaseModel):
    customer_first_name: Annotated[str, StringConstraints(strip_whitespace=True)] | None = ""
    customer_last_name: Annotated[str, StringConstraints(strip_whitespace=True)] | None = ""
    customer_email: Annotated[str, StringConstraints(strip_whitespace=True)] | None = ""
    customer_phone: Annotated[str, StringConstraints(strip_whitespace=True)] | None = ""
    customer_custom_data: dict | None = None

    def json(self, **kwargs):
        res_dict = super().model_dump(**kwargs)
        if all(not value for value in res_dict.values()):
            return None
        res_dict.pop("customer_custom_data")
        return res_dict


class ConvModelRes(ConvChannel, ConvStatus, ConvGenerationType, ConvState):
    channel: str
    status: str
    state: str
    generation_type: str
    updated_at: Datetime
    start_at: Datetime | None = None
    end_at: Datetime | None = None
    primary_agent: UUID | None = None
    wrap_up_start_at: Datetime | None = None
    contact_uuid: str | None = None
    child_direction_inbound: bool = True
    sender_id: str | None = None


class ConvParentRes(ConvParentBase):
    conversation_number: Annotated[str, StringConstraints(min_length=1, max_length=18)]
    crm_contact_id: str | None = None
    contact_identifier: str | None = None
    case_id: str | None = None
    parent_contact_uuid: str | None = None
    parent_end_at: Datetime | None = None
    parent_start_at: Datetime | None = None
    source_direction_inbound: bool = True
    last_activity_at: Datetime | None = None
    flow_run_uuid: UUID | None = None
    client_data: ConversationClientData | None = None
    contact_name: str | None = None
    is_hidden: bool | None = False


class ConversationParentChildModel(ConvParentRes):
    child_conversations: list | None = []
    closed_conversations: list | None = []
    contact_uuid: str | None = None
    channel: str | None = None
    status: str | None = None
    updated_at: Datetime | None = None
    start_at: Datetime | None = None
    end_at: Datetime | None = None
    meta: str | None = None
    agent_id: str | None = None
    unviewed_count: int = 0
    last_message: dict | None = None
    templates_data: dict | None = None
    linked_conversation: Optional["ConversationParentChildModel"] = None
    agent_secondary_conversation_state: SecondaryAgentStateEnum | None = None
    conversation_other_participant_state: SecondaryAgentStateEnum | None = None
    secondary_participants: list | None = []
    secondary_participants_details: list | None = []


class AgentDetailsRes(BaseModel):
    agent_name: str | None = None


class ConversationParentModel(CoreDBBase, ConvParentRes):
    organization: str

    def build_res_model(self) -> ConvParentRes:
        res_dict = self.model_dump()
        return ConvParentRes.model_validate(res_dict)

    model_config = ConfigDict(from_attributes=True)


class ConversationRes(ConvModelRes, ConvParentRes):
    run_reverse_routing: bool | None = False
    update_capacity_on_resolve: bool | None = False


class ActiveConvModelPartial(ConversationRes):
    meta: str | None = None

    def build_full_model(self, contact_name: str | None = None):
        res_dict = self.model_dump()
        if not contact_name:
            contact_name = res_dict.get("contact_identifier", None)
        if not res_dict.get("contact_name"):
            res_dict["contact_name"] = contact_name
        return ActiveConvModel.model_validate(res_dict)


class ActiveConvModel(ActiveConvModelPartial):
    contact_name: str | None = None
    agent_id: str | None = None


class ConversationParentRes(ConvParentRes):
    child_conversations: list[SerializeAsAny[ConversationRes]] | None = None


class ConvGetByContactUUID(BaseModel):
    contact_uuid: Annotated[str, StringConstraints(min_length=1, strip_whitespace=True)] = None


class ConversationModel(CoreDBBase, ConvStatus, ConvGenerationType, ConvChannel, ConvState):
    primary_aom: int | None = None
    parent: int
    channel: str
    status: str
    state: str
    generation_type: str
    start_at: Datetime | None = None
    end_at: Datetime | None = None
    primary_agent: SerializeAsAny[AccountOrgMappingModel] | None = None
    wrap_up_start_at: Datetime | None = None
    contact_uuid: str | None = None
    parent_conversation: SerializeAsAny[ConversationParentModel]
    active_queue_entry_id: int | None = None
    child_direction_inbound: bool | None = True
    sender_id: str | None = None
    customer_contact: str | None = None
    resolution_category: str | None = None
    custom_status_mapping: int | None = None

    def build_res_model(
        self, primary_aom_uuid=None, reverse_routing=False, update_capacity_on_resolve=False
    ) -> ConversationRes:
        res_dict = self.model_dump()
        res_dict["primary_agent"] = (
            self.primary_agent.model_dump().get("uuid") if self.primary_agent else primary_aom_uuid
        )
        parent = res_dict.pop("parent_conversation", {})
        res_dict.update(parent)
        if reverse_routing:
            res_dict["run_reverse_routing"] = True
        res_dict["update_capacity_on_resolve"] = update_capacity_on_resolve
        return ConversationRes.model_validate(res_dict)

    def build_agent_details_response(self) -> AgentDetailsRes:
        from hs.admin.account import AccountOrgMapping

        res_dict = {}
        res_dict["agent_name"] = (
            AccountOrgMapping.get_name_by_aom_uuid(str(self.primary_agent.uuid)) if self.primary_agent else None
        )
        return AgentDetailsRes.model_validate(res_dict)

    def is_primary_agent(self, agent_uuid: UUID) -> bool:
        return str(self.primary_agent.uuid) == str(agent_uuid)

    model_config = ConfigDict(from_attributes=True, arbitrary_types_allowed=True)


class ConvSecAgentMappingModel(BaseModel):
    aom: int | None = None
    conversation: int
    secondary_conversation: int | None = None
    agent: SerializeAsAny[AccountOrgMappingModel] | None = None
    parent_conversation: SerializeAsAny[ConversationParentModel] | None = None
    secondary_parent_conversation: SerializeAsAny[ConversationParentModel] | None = None
    secondary_agent_state: SecondaryAgentStateEnum | None = None
    participant_identifier: str | None = None
    model_config = ConfigDict(from_attributes=True, arbitrary_types_allowed=True)

    def build_res_model(self):
        res_data = self.model_dump().get("parent_conversation")
        if not res_data:
            return None
        return ConversationParentRes.model_validate(res_data)


class ConvParentInsertModel(ConvParentBase):
    parent_channel: Annotated[str, StringConstraints(min_length=1, strip_whitespace=True)]
    phone_or_email: Annotated[str, StringConstraints(min_length=1, strip_whitespace=True)]
    parent_contact_uuid: Annotated[str, StringConstraints(min_length=1, strip_whitespace=True)]
    org_auth_id: Annotated[str, StringConstraints(min_length=1, strip_whitespace=True)]
    agent_sip_or_uuid: Annotated[str, StringConstraints(min_length=1, strip_whitespace=True)] | None = None
    direction_inbound: bool = True
    crm_contact_id: Annotated[str, StringConstraints(min_length=1, strip_whitespace=True)] | None = None
    transfer_conversation: bool = False
    customer_data: ConversationCustomerData | None = None
    conversation_data: ConversationDataDict | None = None
    client_data: ConversationClientData | None = None
    sender_id: str | None = None
    http_request_uuid: str | None = None
    transfer_outbound: bool | None = False
    child_contact_uuid: str | None = None
    flow_run_uuid: Annotated[str, StringConstraints(min_length=1, strip_whitespace=True)] | None = None
    flow_data: dict | None = None
    is_hidden: bool | None = False

    @field_validator("phone_or_email")
    @classmethod
    def validate_contact_phone(cls, value):
        from hs.infra.helpers import is_valid_phone_number

        return get_number_in_e164_format(value) if is_valid_phone_number(value) else value

    model_config = ConfigDict(extra="allow", arbitrary_types_allowed=True)

    def build_db_model(self, org, contact, contact_identifier: str):
        from hs.conversations.store import ConversationParent

        res_dict = {
            ConversationParent.conversation_number.name: int(str(uuid4().int)[:10]),
            ConversationParent.organization.name: org.id,
            ConversationParent.crm_contact_id.name: contact.contact_id if contact else None,
            ConversationParent.contact_name.name: contact.name if contact else None,
            ConversationParent.contact_identifier.name: contact_identifier,
            ConversationParent.channel.name: self.parent_channel,
            ConversationParent.contact_uuid.name: self.parent_contact_uuid,
            ConversationParent.direction_inbound.name: self.direction_inbound,
            ConversationParent.flow_closure_flag.name: self.flow_closure_flag,
            ConversationParent.is_hidden.name: bool(self.is_hidden),
        }
        return ConversationParent(**res_dict)

    def build_customer_conversation_db_entity(self, conversation_parent_id):
        from hs.conversations.store import CustomerConversationData

        if self.customer_data:
            self.customer_data.customer_phone = (
                get_number_in_e164_format(self.customer_data.customer_phone)
                if self.customer_data.customer_phone
                else self.customer_data.customer_phone
            )
        res_dict = {
            CustomerConversationData.conversation_parent_id.name: conversation_parent_id,
            CustomerConversationData.customer_data.name: self.customer_data.json() if self.customer_data else None,
            CustomerConversationData.customer_custom_data.name: self.customer_data.customer_custom_data
            if self.customer_data
            else None,
            CustomerConversationData.conversation_custom_data.name: self.conversation_data.json()
            if self.conversation_data
            else None,
            CustomerConversationData.external_identifier.name: self.conversation_data.external_identifier
            if self.conversation_data
            else None,
            CustomerConversationData.client_data.name: self.client_data.json() if self.client_data else None,
        }
        return CustomerConversationData(**res_dict)


class CustomerConversationDataModel(CoreDBBase):
    customer_data: ConversationCustomerData | None = None
    conversation_data: ConversationDataDict | None = None
    conversation_parent_id: str | None = None


class ConvParentAndCustomerConvDataRes(ConvParentRes):
    customer_data: ConversationCustomerData | None = None
    conversation_data: ConversationDataDict | None = None

    @classmethod
    def build_response(cls, conversation_parent, customer_conversation_data):
        conversation_parent_dict = conversation_parent.model_dump()
        customer_conversation_data_dict = customer_conversation_data.model_dump()
        resp_dict = {**conversation_parent_dict, **customer_conversation_data_dict}
        return cls.model_validate(resp_dict)


class ConvTransferType(BaseModel):
    transfer_type: str | None = ConversationTransferTypes.agent.name

    @field_validator("transfer_type")
    @classmethod
    def type_must_be_in_enum(cls, v):
        allowed_values = ConversationTransferTypes.__dict__["_member_names_"]
        if v not in allowed_values:
            raise ValueError(f"transfer_type : {v} must be in {allowed_values}")
        return v


class ConvTransferModel(ConvTransferType, ConvChannel):
    contact_uuid: Annotated[str, StringConstraints(min_length=1, strip_whitespace=True)]
    agent_sip_or_uuid: Annotated[str, StringConstraints(min_length=1, strip_whitespace=True)] | None = None
    direction_inbound: bool | None = True
    parent_contact_uuid: str | None = None


class UpdateConvTransferModel(ConvChannel):
    parent_contact_uuid: str | None = None
    contact_uuid: Annotated[str, StringConstraints(min_length=1, strip_whitespace=True)]
    agent_sip_or_uuid: Annotated[str, StringConstraints(min_length=1, strip_whitespace=True)] | None = None
    state: str | None = None


class ConvSecAgentMappingInsert(ConvChannel):
    agent_sip_or_uuid: Annotated[str, StringConstraints(min_length=1, strip_whitespace=True)] | None = None
    conversation_number: str
    type: SecAgentTypes = SecAgentTypes.agent_transfer
    secondary_agent: AOMFullModel | None = None
    parent_conversation: SerializeAsAny[ConversationParentModel] | None = None
    secondary_conversation: SerializeAsAny[ConversationParentModel] | None = None
    secondary_agent_state: SecondaryAgentStateEnum | None = None
    active_child_conversation: ConversationModel | None = None
    agent_requested_coach: bool = False
    participant_identifier: Annotated[str, StringConstraints(min_length=1, strip_whitespace=True)] | None = None
    participant_alias: Annotated[str, StringConstraints(strip_whitespace=True)] | None = None

    @model_validator(mode="after")
    def validate_secondary_mapping_type(self):
        if self.type in (SecAgentTypes.agent_transfer.value, SecAgentTypes.supervisor_participant.value):
            if not self.agent_sip_or_uuid:
                raise ValueError(f"agent_sip_or_uuid is required for {self.type.replace('_', ' ')} type")
        if self.type in [SecAgentTypes.add_participant.value, SecAgentTypes.external_transfer.value]:
            if not self.participant_identifier:
                raise ValueError("participant_identifier is required for add_participant type")
        return self

    @field_validator("secondary_agent_state")
    @classmethod
    def validate_secondary_agent_state(cls, value):
        allowed_values = SecondaryAgentStateEnum.__dict__["_member_names_"]
        if value and value not in allowed_values:
            raise ValueError("Invalid secondary agent state")
        return value

    def build_db_model(
        self,
        secondary_aom: "AccountOrgMappingModel" = None,
        parent_conversation: "ConversationParentModel" = None,
        secondary_conversation: "ConversationParentModel" = None,
        secondary_agent_state: str | None = None,
    ):
        from hs.conversations.store import ConvSecondaryAgentMapping

        aom_id = secondary_aom.id if secondary_aom else (self.secondary_agent.id if self.secondary_agent else None)
        parent_conv_id = parent_conversation.id if parent_conversation else self.parent_conversation.id
        secondary_conv_id = (
            secondary_conversation.id
            if secondary_conversation
            else (self.secondary_conversation.id if self.secondary_conversation else None)
        )
        res_dict = {
            ConvSecondaryAgentMapping.aom.name: aom_id,
            ConvSecondaryAgentMapping.conversation.name: parent_conv_id,
            ConvSecondaryAgentMapping.secondary_conversation.name: secondary_conv_id,
            ConvSecondaryAgentMapping.secondary_agent_state.name: self.secondary_agent_state,
            ConvSecondaryAgentMapping.participant_identifier.name: self.participant_identifier,
        }
        return ConvSecondaryAgentMapping(**res_dict)


class ConvSecAgentMappingRemove(ConvSecAgentMappingInsert, ConvChannel):
    ignore_event: bool | None = False


class ChunkType(BaseModel):
    type: str

    @field_validator("type")
    @classmethod
    def type_must_be_in_enum(cls, v):
        allowed_values = ConversationChunkTypes.__dict__["_member_names_"]
        if v not in allowed_values:
            raise ValueError(f"chunk : {v} type must be in {allowed_values}")
        return v


class ConvMessageIdModel(ChunkType):
    message_id: str | None = None


class ConvChunkChannelMetaModel(BaseModel):
    status: str | None = None
    replied_to: str | None = None
    error: str | None = None
    timestamp: str | None = None
    error_data: dict | None = None
    transfer_type: str | None = None


class ChunkAttributes(BaseModel):
    attributes: ALL_CHUNK_ATTRIBUTES | None = None

    def __init__(self, **data):
        super().__init__(**data)
        if data.get("attributes") and isinstance(data["attributes"], list):
            self.attributes = []
            valid_attr_to_parse = None
            for attr in VALID_ATTRIBUTES:
                try:
                    self.attributes.append(attr.model_validate(data["attributes"][0]))
                    valid_attr_to_parse = attr
                    break
                except Exception:
                    pass
            if valid_attr_to_parse:
                for data_to_parse in data["attributes"][1:]:
                    try:
                        self.attributes.append(valid_attr_to_parse.model_validate(data_to_parse))
                    except Exception:
                        raise ValueError(f"Data is not homogenous in attributes {data['attributes']}")
            else:
                raise ValueError(f"Invalid attributes {data['attributes']}")
        else:
            self.attributes = data.get("attributes")


class ConvChunkInsertModel(ConvMessageIdModel, ChunkAttributes):
    sender_id: str | None = None
    customer_contact: str | None = None
    from_user: str | None = Field(None, alias="from")
    to_user: str | None = Field(None, alias="to")
    disposition: str | None = None
    disposition_id: int | None = None
    sub_disposition: str | None = None
    sub_disposition_id: int | None = None
    description: str | None = None
    channel: str | None = None
    timestamp: str | Datetime | None = None
    channel_meta: SerializeAsAny[ConvChunkChannelMetaModel] | None = {}
    retry_chunk_uuid: Annotated[str, StringConstraints(to_lower=True, strip_whitespace=True)] | None = None
    reply_to: str | None = None
    is_first_chunk_for_conv: bool | None = Field(default=False)
    is_inbound_chunk: bool | None = Field(default=False)
    # transient pass-through; exclude=True keeps it out of ConversationChunkRes's wire format
    source_service: str | None = Field(None, exclude=True)
    tmp_id: str | None = Field(None, exclude=True)

    def __init__(self, **data):
        if data.get("from_user"):
            data["from"] = data.pop("from_user")
        if data.get("to_user"):
            data["to"] = data.pop("to_user")
        if data.get("disposition") and data.get("channel"):
            data.pop("channel")
        super().__init__(**data)

    def build_db_model(self, conversation: "ConversationModel"):
        from hs.admin.dispositions import Dispositions
        from hs.conversations.store import ConversationChunk
        from hs.infra.context import context_org_uuid

        res_dict = self.model_dump()
        _tmp_id = self.tmp_id
        if _tmp_id:
            try:
                res_dict["uuid"] = str(UUID(str(_tmp_id)))
            except (ValueError, TypeError):
                pass
        res_dict[ConversationChunk.conversation.name] = conversation.id
        timestamp_val = res_dict.pop("timestamp")
        if not timestamp_val:
            timestamp_val = get_now()
        # A caller that resolved the disposition rows itself passes their ids; names need not be unique.
        if self.disposition and not self.disposition_id:
            dispo = Dispositions.get_by_name(org_uuid=str(context_org_uuid.get()), disposition_name=self.disposition)
            if dispo:
                self.disposition_id = dispo.id
                res_dict["disposition_id"] = dispo.id
            if dispo and self.sub_disposition:
                sub_dispo = Dispositions.get_by_name(
                    org_uuid=str(context_org_uuid.get()),
                    disposition_name=self.sub_disposition,
                    parent_disposition_id=dispo.id,
                )
                if sub_dispo:
                    self.sub_disposition_id = sub_dispo.id
                    res_dict["sub_disposition_id"] = sub_dispo.id
        res_dict[ConversationChunk.timestamp.name] = timestamp_val
        res_dict.pop("sender_id")
        res_dict.pop("customer_contact")
        res_dict.pop("reply_to")
        res_dict.pop("is_first_chunk_for_conv", None)
        res_dict.pop("is_inbound_chunk", None)
        res_dict.pop("contact_uuid", None)
        res_dict.pop("retry_chunk_uuid", None)
        return ConversationChunk(**res_dict)

    model_config = ConfigDict(extra="forbid")


class ConvChunkInsertByContactUUID(ConvChunkInsertModel):
    contact_uuid: Annotated[str, StringConstraints(min_length=1, strip_whitespace=True)]


class ConvChunkChannelMetaRepliedMessageModel(ConvChunkChannelMetaModel):
    replied_message: dict | None = None


class ConversationChunkRes(ConvChunkInsertModel):
    uuid: UUID | None = None
    timestamp: Datetime
    from_user_name: str | None = None
    to_user_name: str | None = None
    channel_meta: ConvChunkChannelMetaRepliedMessageModel | None = None
    contact_uuid: str | None = None

    @field_validator("from_user")
    @classmethod
    def validate_from_user(cls, v):
        return v

    @field_validator("to_user")
    @classmethod
    def validate_to_user(cls, v):
        return v

    model_config = ConfigDict(extra="ignore")


class ConversationChunkModel(CoreDBBase, ConvChunkInsertModel):
    conversation: int
    timestamp: Datetime
    channel_meta: SerializeAsAny[ConvChunkChannelMetaModel] | None = None
    message_id: str | None = None

    def build_res_model(self, conversation: ConversationModel | None = None, alias=False) -> ConversationChunkRes:
        res_dict = self.model_dump(by_alias=alias)
        if conversation:
            res_dict["sender_id"] = conversation.sender_id
            res_dict["customer_contact"] = conversation.customer_contact
            res_dict["is_inbound_chunk"] = conversation.child_direction_inbound
            res_dict["contact_uuid"] = conversation.contact_uuid
        conv_chunk_res = ConversationChunkRes.model_validate(res_dict)
        return self.add_reply_message(conv_chunk_res)

    @classmethod
    def add_reply_message(cls, conv_chunk_res):
        return conv_chunk_res


class ConversationVariablesRes(BaseModel):
    conversation_number: str
    contact_identifier: str
    conv_agents_email: list[str] = []
    conv_created_date: str | None = None
    conv_channels_list: list[str] = []
    conv_duration: str | None = None
    conv_url: str | None = None
    conv_disposition: str | None = None
    conv_wrapup_note: str | None = None
    conv_contact_id: str | None = None
    conv_case_id: str | None = None
    conv_source: str | None = None
    conv_destination: str | None = None
    conv_contact_name: str | None = None
    conv_channel_to_contact_uuid_map: dict[str, str] = {}
    conv_parent_contact_uuid: str
    has_supervisor_or_admin: bool | None = False
    conv_current_agent_name: str | None = None
    conv_participants_aom_uuids: list[str] = []
    direction_inbound: bool | None = True


ConversationParentChildModel.model_rebuild()
ASYNC_CHANNELS = [ConversationChannels.message.name, ConversationChannels.email.name]


class ConvInsertModel(ConvStatus, ConvGenerationType, ConvChannel, ConvState):
    channel: str | None = None
    state: str | None = None
    generation_type: Annotated[str, StringConstraints(min_length=1, strip_whitespace=True)]
    status: str | None = ConversationStatus.open.name
    parent_contact_uuid: Annotated[str, StringConstraints(min_length=1, strip_whitespace=True)] | None = None
    parent_conversation_number: str | None = None
    contact_uuid: str | None = None
    parent: SerializeAsAny[ConversationParentModel] | None = None
    agent_sip_or_uuid: str | None = None
    active_queue_entry_id: int | None = None
    start_at: Datetime | None = None
    direction_inbound: bool | None = True
    sender_id: str | None = None
    customer_contact: str | None = None
    flow_run_uuid: str | None = None

    @model_validator(mode="after")
    def format_customer_contact_sender_id(self):
        from hs.infra.helpers import is_valid_phone_number
        from hs.setup.number import Number

        if self.customer_contact:
            self.customer_contact = get_number_in_e164_format(self.customer_contact)
        if self.sender_id:
            sender_id_db_instance = Number.get_by_number(self.sender_id)
            if self.channel and self.channel == ConversationChannels.email.name:
                pass
            elif (not sender_id_db_instance) or (not sender_id_db_instance.is_sender_id):
                if (
                    self.channel
                    and self.channel != ConversationChannels.chat.name
                    and not is_valid_phone_number(self.sender_id)
                ):
                    self.sender_id = None
                else:
                    self.sender_id = get_number_in_e164_format(self.sender_id)
            elif sender_id_db_instance and sender_id_db_instance.is_sender_id:
                self.sender_id = sender_id_db_instance.number
        return self

    model_config = ConfigDict(extra="forbid")

    def build_db_model(
        self,
        primary_aom: "AccountOrgMappingModel" = None,
        active_queue_id: int | None = None,
        conv_priority: str | None = None,
        custom_conv_state: str | None = None,
    ):
        from hs.conversations.store import Conversation
        from hs.infra.log import logger

        state = self.state if self.state else DEFAULT_GEN_TYPE_STATE_MAPPING.get(self.generation_type)
        self.channel = self.channel if self.channel else self.parent.parent_channel
        if (
            not self.direction_inbound
            and self.generation_type == ConversationGenerationTypes.outbound_generated.name
            and self.channel in ASYNC_CHANNELS
        ):
            state = ConversationStates.async_outbound_contact_unanswered.name
        now = get_now()
        org_id = primary_aom.organization if primary_aom else None
        res_dict = {
            Conversation.parent.name: self.parent.id,
            Conversation.channel.name: self.channel,
            Conversation.generation_type.name: self.generation_type,
            Conversation.state.name: state,
            Conversation.start_at.name: now
            if (
                self.generation_type in NON_QNR_CHILDREN
                or self.generation_type == ConversationGenerationTypes.channel_switch.name
            )
            else self.start_at,
            Conversation.status.name: self.status if self.status else ConversationStatus.open.name,
            Conversation.contact_uuid.name: self.contact_uuid if self.contact_uuid else self.parent.parent_contact_uuid,
            Conversation.primary_aom.name: primary_aom.id if primary_aom else None,
            Conversation.active_queue_entry_id.name: active_queue_id,
            Conversation.direction_inbound.name: self.direction_inbound,
            Conversation.sender_id.name: self.sender_id,
            Conversation.customer_contact.name: self.customer_contact
            if self.customer_contact
            else self.parent.contact_identifier,
        }
        if not org_id:
            org_id = self.parent.organization
        if not org_id:
            logger.error("org_id not found while building conversation insert model")
        if custom_conv_state:
            from hs.conversations.store import ConversationCustomStatusMapping

            conv_custom_state_db_instance = ConversationCustomStatusMapping.get_by_state_and_org(
                state=custom_conv_state, org_id=org_id
            )
            res_dict[Conversation.custom_status_mapping.name] = conv_custom_state_db_instance.id
            if custom_conv_state != ConversationCustomStatusMappingStates.new.value:
                res_dict[Conversation.status_change_timestamps.name] = [
                    {
                        "state": ConversationCustomStatusMappingStates.new.value,
                        "state_ended_at": str(now),
                        "state_started_at": str(now),
                    },
                    {
                        "state": ConversationCustomStatusMappingStates.open.value,
                        "state_ended_at": str(now),
                        "state_started_at": str(now),
                    },
                ]
            else:
                res_dict[Conversation.status_change_timestamps.name] = []
            res_dict[Conversation.status_change_timestamps.name].append(
                {"state": custom_conv_state, "state_ended_at": None, "state_started_at": str(now)}
            )
        if conv_priority:
            from hs.conversations.store import ConversationPriority

            conv_priority_db_instance = ConversationPriority.get_by_name_and_org(
                org_id=self.parent.organization, name=conv_priority
            )
            if conv_priority_db_instance:
                res_dict[Conversation.priority.name] = conv_priority_db_instance.id
        return Conversation(**res_dict)


class NonQNRChunkInsert(ConvMessageIdModel, ConvChannel, ChunkAttributes):
    disposition: Annotated[str, StringConstraints(min_length=1, strip_whitespace=True)] | None = None
    disposition_id: int | None = None
    description: str | None = None
    parent_contact_uuid: Annotated[str, StringConstraints(min_length=1, strip_whitespace=True)]
    to_user: str | None = Field(None, alias="to")
    sender_id: str | None = None
    channel_meta: SerializeAsAny[ConvChunkChannelMetaModel] | None = {}
    flow_run_id: str | None = None
    node_run_id: str | None = None
    remove_cache: bool | None = True
    reply_timeout: int | None = 0
    is_inbound_chunk: bool | None = False
    create_chunk: bool | None = True
    node_type: str | None = None
    timestamp: str | Datetime | None = None
    is_main_recording: bool | None = False
    chunk_for_hangup_node: bool | None = False
    skip_for_active_child: bool | None = False
    message_id: str | None = None
    recording: int | None = None

    def build_db_model(self, conversation: "ConversationModel"):
        from hs.admin.dispositions import Dispositions
        from hs.conversations.store import ConversationChunk
        from hs.infra.constants import SYSTEM_USER_UUID
        from hs.infra.context import context_org_uuid

        res_dict = self.model_dump()
        if self.disposition:
            dispo = Dispositions.get_by_name(org_uuid=str(context_org_uuid.get()), disposition_name=self.disposition)
            if dispo:
                self.disposition_id = dispo.id
                res_dict["disposition_id"] = dispo.id
        del res_dict["parent_contact_uuid"]
        del res_dict["remove_cache"]
        res_dict.pop("chunk_for_hangup_node", None)
        bot_uuid = SYSTEM_USER_UUID
        if not self.channel:
            res_dict["channel"] = conversation.channel
        res_dict["from_user"] = bot_uuid
        res_dict["to_user"] = conversation.parent_conversation.contact_identifier
        if self.is_inbound_chunk:
            res_dict["from_user"] = conversation.parent_conversation.contact_identifier
            res_dict["to_user"] = bot_uuid
        res_dict[ConversationChunk.conversation.name] = conversation.id
        res_dict[ConversationChunk.timestamp.name] = get_now()
        res_dict.pop("sender_id", None)
        res_dict.pop("flow_run_id", None)
        res_dict.pop("node_run_id", None)
        res_dict.pop("reply_timeout", None)
        res_dict.pop("is_inbound_chunk", None)
        res_dict.pop("create_chunk", None)
        res_dict.pop("node_type", None)
        res_dict.pop("is_main_recording", None)
        res_dict.pop("skip_for_active_child", None)
        if res_dict.get("recording") is None:
            res_dict.pop("recording", None)
        return ConversationChunk(**res_dict)

    model_config = ConfigDict(extra="forbid")


CHANNELS_FOR_SESSION_TIMEOUT = [
    ConversationChannels.call.name,
    ConversationChannels.chat.name,
    ConversationChannels.message.name,
    ConversationChannels.task.name,
    ConversationChannels.callback.name,
    ConversationChannels.email.name,
]


class ConversationSessionTimeoutPayload(BaseModel):
    extended: bool = False
    child_conversation_id: str | None = None
