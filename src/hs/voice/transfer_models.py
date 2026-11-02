import json

from hs.infra.base_model import BaseModel, LenientModel


class IncomingTransferReq(LenientModel):
    contact_uuid: str = ""
    transfer_type: str = ""
    identifier: str = ""
    agent_sip: str = ""
    member: str = ""


class AgentDropParams(LenientModel):
    leave_type: str = ""
    contact_uuid: str = ""
    channel: str = ""


class _CallAgentsReq(LenientModel):
    call_uuid: str = ""
    flow_run_uuid: str = ""
    auth_id: str = ""
    conversation_number: str = ""


class GetAgentsParams(_CallAgentsReq):
    transfer_destination_id: str = ""


class GetAgentsByCapacity(_CallAgentsReq):
    aom_uuid: str = ""


class OutboundQueueTransfer(LenientModel):
    auth_id: str = ""
    identifier: str = ""
    contact_uuid: str = ""
    agent_sip_endpoint: str = ""
    parent_contact_uuid: str = ""


class TransferInfo(BaseModel):
    transfer_type: str = ""
    transfer_mode: str = ""
    target: str = ""
    at: float = 0

    def to_json(self) -> str:
        return json.dumps(self.model_dump(exclude_defaults=True), separators=(",", ":"), ensure_ascii=False)
