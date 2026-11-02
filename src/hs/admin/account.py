from typing import NamedTuple

from sqlalchemy import TIMESTAMP, Boolean, Column, ForeignKey, Index, Integer, String, Text, text
from sqlalchemy.dialects.postgresql import JSON, JSONB, UUID
from sqlalchemy.orm import Session, joinedload, relationship

from hs.admin.account_models import (
    AccountOrgMappingModel,
    AgentAccount,
    AOMFullModel,
    AOMRoleProfileModel,
    OrganizationModel,
    RoutingSettingModel,
)
from hs.infra.context import get_db_session
from hs.infra.db import DB, CoreDBBase
from hs.infra.helpers import get_now, is_valid_uuid


class AccountOrgMapping(DB, CoreDBBase):
    __tablename__ = "account_org_mapping"
    account = Column(Integer)
    organization = Column(Integer, ForeignKey("organization.id"))
    sip_endpoint = Column(String)
    sip_endpoint_id = Column(String)
    caller_id = Column(String)
    status = Column(String)
    agent_status = Column(Integer, ForeignKey("org_statuses.id"))
    org = relationship("Organization")
    role = Column(Integer, ForeignKey("role.id"))
    role_key = relationship("Role")

    def __to_model(self) -> AccountOrgMappingModel:
        return AccountOrgMappingModel.model_validate(self)

    def __to_full_model(self) -> AOMFullModel:
        return AOMFullModel.model_validate(self)

    def get_model(self) -> AccountOrgMappingModel:
        return self.__to_model()

    @classmethod
    def get_by_uuid(cls, aom_uuid, build_full_model=False) -> AccountOrgMappingModel | AOMFullModel:
        query = cls.select_query(get_db_session()).filter(cls.uuid == aom_uuid)
        # eager: a lazy org load (composite pk) skips SQL only while an earlier copy of this row survives GC
        aom = (query.options(joinedload(cls.org)) if build_full_model else query).first()
        if aom:
            if not build_full_model:
                return aom.__to_model()
            return aom.__to_full_model()
        return None

    @classmethod
    def get_by_id(cls, agent_id, build_full_model=False) -> AccountOrgMappingModel | AOMFullModel:
        db: Session = get_db_session()
        aom = cls.select_query(db).filter(cls.id == agent_id).first()
        if aom:
            if not build_full_model:
                return aom.__to_model()
            return aom.__to_full_model()
        return None

    @classmethod
    def get_by_sip_endpoint(cls, sip_endpoint, build_full_model=False) -> AccountOrgMappingModel | AOMFullModel:
        db: Session = get_db_session()
        aom = cls.select_query(db).filter(cls.sip_endpoint == sip_endpoint).first()
        if aom:
            if not build_full_model:
                return aom.__to_model()
            return aom.__to_full_model()
        return None

    @classmethod
    def _accounts(cls, *where) -> dict[str, AgentAccount]:
        """{aom uuid: the agent's account row} for the agents matching `where`."""
        columns = (cls.uuid, Account.name, Account.email, Account.first_name, Account.last_name)
        rows = get_db_session().query(*columns).join(Account, Account.id == cls.account).filter(*where).all()
        return {
            str(u): AgentAccount(name=n or "", email=e or "", first_name=f, last_name=ln) for u, n, e, f, ln in rows
        }

    @classmethod
    def get_account_by_aom_uuid(cls, aom_uuid: str) -> AgentAccount | None:
        return cls._accounts(cls.uuid == aom_uuid).get(str(aom_uuid)) if is_valid_uuid(aom_uuid) else None

    @classmethod
    def get_name_by_aom_uuid(cls, aom_uuid) -> str:
        return getattr(cls.get_account_by_aom_uuid(aom_uuid), "name", None)

    @classmethod
    def get_agent_name_by_aom_uuid(cls, aom_uuid: str) -> str:
        account = cls.get_account_by_aom_uuid(aom_uuid)
        return account.get_agent_name() if account else None

    @classmethod
    def get_user_details_by_aom_uuids(cls, aom_uuids, organization_id: int) -> dict[str, AgentAccount]:
        """One read of the name and email of each of these agents of the organization."""
        return cls._accounts(cls.uuid.in_(aom_uuids), cls.organization == organization_id) if aom_uuids else {}


class Organization(DB, CoreDBBase):
    __tablename__ = "organization"
    # One live tenant per sub-account: the engine looks tenants up by auth id.
    __table_args__ = (
        Index("ux_organization_live_auth_id", "auth_id", unique=True, postgresql_where=text("is_deleted IS NOT TRUE")),
    )
    name = Column(String)
    auth_id = Column(String)
    auth_token = Column(String)
    plivo_token_enc = Column(Text)
    skip_contact_creation = Column(Boolean)
    skip_case_creation = Column(Boolean)
    allow_acw_post_transfer = Column(Boolean)
    is_active = Column(Boolean)
    allow_transfer_type = Column(Boolean, default=True)
    default_transfer_type = Column(String, default="warm")
    call_recording_inbound = Column(Boolean, default=False)
    call_recording_outbound = Column(Boolean, default=False)
    allow_recording_pause_play = Column(Boolean, default=True)

    def __to_model(self):
        return OrganizationModel.model_validate(self)

    @classmethod
    def get_by_uuid(cls, uuid) -> OrganizationModel:
        db: Session = get_db_session()
        org = cls.select_query(db).filter(cls.uuid == uuid).first()
        if org:
            return org.__to_model()
        return None

    @classmethod
    def get_org_by_auth_id(cls, org_auth_id: str) -> OrganizationModel:
        db: Session = get_db_session()
        org = cls.select_query(db).filter(cls.auth_id == org_auth_id).first()
        if not org:
            return None
        return org.__to_model()

    @classmethod
    def get_by_id(cls, org_id):
        org = super().get_by_id(org_id)
        if org:
            return org.__to_model()
        return org

    @classmethod
    def get_by_name(cls, org_name: str) -> "OrganizationModel":
        db: Session = get_db_session()
        org = cls.select_query(db).filter(cls.name == org_name).first()
        if not org:
            return None
        return org._Organization__to_model()


class Account(DB, CoreDBBase):
    __tablename__ = "account"
    email = Column(String)
    name = Column(String)
    first_name = Column(String)
    last_name = Column(String)


class AOMRoutingParams(DB, CoreDBBase):
    __tablename__ = "aom_routing_params"
    available_since = Column(TIMESTAMP(timezone=True))
    contacts_handle_time = Column(Integer)
    contacts_handled = Column(Integer)
    aom = Column(Integer, ForeignKey("account_org_mapping.id"))

    @classmethod
    def update_routing_params(cls, aom_id, contacts_handle_time):
        db: Session = get_db_session()
        update_dict = {}
        update_dict[cls.available_since.name] = get_now()
        update_dict[cls.contacts_handle_time] = cls.contacts_handle_time + contacts_handle_time
        update_dict[cls.contacts_handled] = cls.contacts_handled + 1
        cls.select_query(db).filter(cls.aom == aom_id).update(update_dict)
        db.flush()


class Role(DB, CoreDBBase):
    __tablename__ = "role"
    organization = Column(Integer, ForeignKey("organization.id"))
    name = Column(String)

    def __to_model(self):
        return Role.model_validate(self)

    def get_model(self):
        return self.__to_model()


class OrgRoutingSettings(NamedTuple):
    capacity: dict
    reservation_timeout: dict
    simultaneous_agent_limit: int
    auto_accept: bool
    outbound_sticky_routing: dict
    wrap_up_configuration: dict
    release_capacity_on_resolve: bool


class RoutingSettings(DB, CoreDBBase):
    __tablename__ = "routing_settings"
    organization = Column(Integer, ForeignKey("organization.id"))
    release_capacity_on_resolve = Column(Boolean)
    wrap_up_configuration = Column(JSON)
    capacity = Column(JSON)
    reservation_timeout = Column(JSON)
    simultaneous_agent_limit = Column(Integer)
    auto_accept = Column(Boolean)
    outbound_sticky_routing = Column(JSON)

    def __to_model(self):
        return RoutingSettingModel.model_validate(self)

    @classmethod
    def get_by_org_id(cls, org_id) -> RoutingSettingModel:
        db: Session = get_db_session()
        setting = cls.select_query(db).filter(cls.organization == org_id).first()
        if setting:
            return setting.__to_model()
        return None

    @classmethod
    def get_org_routing_settings(cls, auth_id: str):
        org = Organization.get_org_by_auth_id(auth_id)
        if not org:
            return None
        db: Session = get_db_session()
        row = cls.select_query(db).filter(cls.organization == org.id).first()
        if not row:
            return None
        return OrgRoutingSettings(
            capacity=row.capacity or {},
            reservation_timeout=row.reservation_timeout or {},
            simultaneous_agent_limit=row.simultaneous_agent_limit or 0,
            auto_accept=bool(row.auto_accept),
            outbound_sticky_routing=row.outbound_sticky_routing or {},
            wrap_up_configuration=row.wrap_up_configuration or {},
            release_capacity_on_resolve=bool(row.release_capacity_on_resolve),
        )


class OrgSetting(DB, CoreDBBase):
    __tablename__ = "org_setting"
    organization = Column(Integer, ForeignKey("organization.id"))
    push_contact_details_async_flag = Column(Boolean)
    conversation_timeout = Column(JSON)
    plivo_app_id = Column(Text)
    voice_settings = Column(JSONB, nullable=False, server_default=text("'{}'"))
    org = relationship("Organization")

    @classmethod
    def get_conversation_session_timeout_from_aom(cls, org_uuid: UUID, channel: str) -> int:
        db: Session = get_db_session()
        conversation_session_timeout = (
            cls.select_query(db, columns=[cls.conversation_timeout])
            .join(cls.org)
            .filter(Organization.uuid == org_uuid)
            .first()
        )
        if not conversation_session_timeout or not conversation_session_timeout[0]:
            return None
        return conversation_session_timeout[0].get(channel, {}).get("timeout_in_seconds", None)

    @classmethod
    def get_by_org_id(cls, org_id: int):
        db: Session = get_db_session()
        query = cls.select_query(db).filter(cls.organization == org_id)
        org_setting = query.first()
        if not org_setting:
            return None
        return org_setting


class AOMRoleProfile(DB, CoreDBBase):
    __tablename__ = "aom_role_profile"
    organization = Column(Integer, ForeignKey("organization.id"))
    aom = Column(Integer, ForeignKey("account_org_mapping.id"))
    role = Column(Integer, ForeignKey("role.id"))
    role_rel = relationship("Role")

    def __to_model(self) -> AOMRoleProfileModel:
        return AOMRoleProfileModel.model_validate(self)

    def get_model(self) -> AOMRoleProfileModel:
        return self.__to_model()

    @classmethod
    def get_role_names_by_aom_uuids(cls, aom_uuids: list[str]) -> list[str]:
        db: Session = get_db_session()
        roles = (
            cls.select_query(db, columns=[Role.name])
            .join(cls.role_rel)
            .join(AccountOrgMapping, cls.aom == AccountOrgMapping.id)
            .filter(AccountOrgMapping.uuid.in_(aom_uuids))
            .all()
        )
        if not roles:
            return []
        return [role.name for role in roles]


# Tables the engine reads through raw SQL (hs/routing): mapped so the schema has them.
class OrgStatus(DB, CoreDBBase):
    __tablename__ = "org_statuses"
    organization = Column(Integer, ForeignKey("organization.id"))
    status = Column(String, nullable=False)
    availability = Column(Boolean, nullable=False, default=False)


class OrgEventAction(DB, CoreDBBase):
    __tablename__ = "org_event_actions"
    organization = Column(Integer, ForeignKey("organization.id"))
    event = Column(String)
    agent_status_action = Column(Integer, ForeignKey("org_statuses.id"))
    event_metadata = Column(JSON, default=dict)


class Skill(DB, CoreDBBase):
    __tablename__ = "skills"
    organization = Column(Integer, ForeignKey("organization.id"))
    name = Column(String)


class AomSkill(DB, CoreDBBase):
    __tablename__ = "aom_skills"
    aom = Column(Integer, ForeignKey("account_org_mapping.id"))
    skill = Column(Integer, ForeignKey("skills.id"))
    skill_level = Column(Integer)


class AomTeam(DB, CoreDBBase):
    __tablename__ = "aom_team"
    organization = Column(Integer, ForeignKey("organization.id"))
    aom = Column(Integer, ForeignKey("account_org_mapping.id"))
    team = Column(Integer, ForeignKey("team.id"))
