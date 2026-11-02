import json

from pydantic import Field

from hs.infra.base_model import LenientModel


class _LenientModel(LenientModel):
    def to_compact_json(self) -> str:
        return json.dumps(self.model_dump(by_alias=True), separators=(",", ":"), ensure_ascii=False)


class SkillFilterModel(_LenientModel):
    skill: str = Field("", alias="skill")
    operator: str = Field("", alias="operator")
    value: int = Field(0, alias="value")


class AutoAcceptCallConfig(_LenientModel):
    allow_auto_accept: bool = Field(False, alias="allow_auto_accept")
    auto_accept_delay: int = Field(0, alias="auto_accept_delay")


class QueueAndRouteConfigModel(_LenientModel):
    queue: str = Field("", alias="queue")
    routing_teams: list[str] = Field(default_factory=list, alias="routing_teams")
    routing_skills_filter: list[SkillFilterModel] = Field(default_factory=list, alias="routing_skills_filter")
    routing: str = Field("", alias="routing")
    agent_limit: int = Field(0, alias="agent_limit")
    retry_limit: int = Field(0, alias="retry_limit")
    routing_agents: list[str] = Field(default_factory=list, alias="routing_agents")
    final_routing_agents: list[str] = Field(default_factory=list, alias="finalRoutingAgents")
    sticky_routing_type: str = Field("", alias="sticky_routing_options")
    sticky_routing_expiry_time: int = Field(0, alias="expiry_time_value")
    sticky_routing_expiry_unit: str = Field("", alias="expiry_time_unit")
    sticky_routing_expiry: bool = Field(False, alias="expiry")
    auto_accept_call: AutoAcceptCallConfig = Field(default_factory=AutoAcceptCallConfig, alias="auto_accept_call")

    @property
    def allow_auto_accept(self) -> bool:
        return self.auto_accept_call.allow_auto_accept

    @property
    def auto_accept_delay(self) -> int:
        return self.auto_accept_call.auto_accept_delay


class Prompt(_LenientModel):
    prompt_name: str = Field("", alias="prompt_name")
    prompt_type: str = Field("", alias="prompt_type")
    text: str = Field("", alias="text")
    language: str = Field("", alias="language")
    voice: str = Field("", alias="voice")
    url: str = Field("", alias="url")
    file: str = Field("", alias="file")
    loop_with_others: bool = Field(False, alias="loop_with_others")
    prompt_speak_type: str = Field("", alias="prompt_speak_type")
    loops: int = Field(0, alias="loops")


class HoldMusic(_LenientModel):
    url: str = Field("", alias="url")


class QueueConfigModel(_LenientModel):
    queue_priority: int = Field(0, alias="queue_priority")
    dequeue_order: str = Field("", alias="dequeue_order")
    queue_capacity: int = Field(0, alias="queue_capacity")
    queue_call_timeout: int = Field(0, alias="queue_call_timeout")
    prompt: list[Prompt] = Field(default_factory=list, alias="prompt")
    customer_hold_music: HoldMusic = Field(default_factory=HoldMusic, alias="customer_hold_music")
    agent_hold_music: HoldMusic = Field(default_factory=HoldMusic, alias="agent_hold_music")
    queue_call_allowed_inputs: list[str] = Field(default_factory=list, alias="queue_call_allowed_inputs")


class IncomingContactParams(_LenientModel):
    conversation_number: str = Field("", alias="conversation_number")
    contact_uuid: str = Field("", alias="contact_uuid")
    auth_id: str = Field("", alias="auth_id")
    from_: str = Field("", alias="from")
    flow_uuid: str = Field("", alias="flow_uuid")
    flow_run_uuid: str = Field("", alias="flow_run_uuid")
    queue_and_route_run_uuid: str = Field("", alias="queue_and_route_run_uuid")
    contact_channel: str = Field("", alias="contact_channel")
    queue_config: QueueConfigModel = Field(default_factory=QueueConfigModel, alias="queue_config")
    queue_and_route_config: QueueAndRouteConfigModel = Field(default_factory=QueueAndRouteConfigModel)
    is_queue_transfer: bool = Field(False, alias="is_queue_transfer")
    to: str = Field("", alias="to")
    parent_contact_uuid: str = Field("", alias="parent_contact_uuid")
    flow_name: str = Field("", alias="flow_name")
    node_uuid: str = Field("", alias="node_uuid")
    inbound: bool = Field(True, alias="inbound")


class ContactParameters(_LenientModel):
    conversation_number: str = Field("", alias="conversation_number")
    flow_uuid: str = Field("", alias="flow_uuid")
    flow_run_uuid: str = Field("", alias="flow_run_uuid")
    queue_uuid: str = Field("", alias="queue_uuid")
    contact_channel: str = Field("", alias="contact_channel")
    from_number: str = Field("", alias="from_number")
    contact_uuid: str = Field("", alias="contact_uuid")
    auth_id: str = Field("", alias="auth_id")
    auth_token: str = Field("", alias="auth_token")
    queue_and_route_run_uuid: str = Field("", alias="queue_and_route_run_uuid")
    to: str = Field("", alias="to")
    flow_name: str = Field("", alias="flow_name")
    node_uuid: str = Field("", alias="node_uuid")
    inbound: bool = Field(True, alias="inbound")

    def to_compact_json(self) -> str:
        return json.dumps(self.model_dump(by_alias=True), separators=(",", ":"), ensure_ascii=False)
