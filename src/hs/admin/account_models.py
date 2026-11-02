from pydantic import ConfigDict

from hs.infra.base_model import BaseModel
from hs.infra.models import CoreDBBase


class OrganizationModel(CoreDBBase):
    name: str
    auth_id: str
    auth_token: str
    skip_contact_creation: bool
    skip_case_creation: bool
    allow_acw_post_transfer: bool
    model_config = ConfigDict(from_attributes=True)


class AgentAccount(BaseModel):
    name: str
    email: str
    first_name: str | None = ""
    last_name: str | None = ""

    def get_agent_name(self) -> str:
        first = self.first_name.title() if self.first_name else ""
        last = self.last_name.title() if self.last_name else ""
        return f"{first} {last}".strip() or self.name


class AccountOrgMappingModel(CoreDBBase):
    organization: str
    sip_endpoint: str | None = None
    model_config = ConfigDict(from_attributes=True)


class AOMFullModel(AccountOrgMappingModel):
    org: OrganizationModel


class RoutingSettingModel(CoreDBBase):
    organization: int
    release_capacity_on_resolve: bool
    wrap_up_configuration: dict
    model_config = ConfigDict(from_attributes=True)


class AOMOccupiedState(CoreDBBase):
    is_aom_occupied: bool = False
    secondary_agent_state: str | None = None


class AOMRoleProfileModel(CoreDBBase):
    organization: int
    aom: int
    role: int
    model_config = ConfigDict(from_attributes=True)
