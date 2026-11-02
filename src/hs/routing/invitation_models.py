import json

from pydantic import ConfigDict, Field

from hs.infra.base_model import BaseModel


class AgentAndContactMapping(BaseModel):
    agent_sip_endpoint: str = Field("", alias="AgentSipEndpoint")
    from_number: str = Field("", alias="FromNumber")
    qnr_run_uuid: str = Field("", alias="QnrRunUuid")
    agent_uuid: str = Field("", alias="AgentUUID")
    agent_call_uuid: str = Field("", alias="AgentCallUuid")
    previous_primary_agent: str = Field("", alias="PreviousPrimaryAgent")
    flow_run_uuid: str = Field("", alias="FlowRunUUID")
    to_number: str = Field("", alias="ToNumber")
    is_inbound: bool = Field(False, alias="IsInbound")
    model_config = ConfigDict(populate_by_name=True)

    def to_json(self) -> str:
        return json.dumps(self.model_dump(by_alias=True), separators=(",", ":"))

    @classmethod
    def parse_redis(cls, raw):
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
            return cls(**data) if isinstance(data, dict) else None
        except (ValueError, TypeError):
            return None
