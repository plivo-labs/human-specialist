from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, func
from sqlalchemy.orm import relationship

from hs.admin.account import Organization
from hs.infra.context import get_db_session
from hs.infra.db import DB, CoreDBBase


class Dispositions(DB, CoreDBBase):
    __tablename__ = "dispositions"
    organization = Column(Integer, ForeignKey(Organization.id))
    name = Column(String)
    sequence = Column(Integer)
    answered = Column(Boolean)
    internal = Column(Boolean)
    state = Column(Boolean)
    parent_disposition_id = Column(Integer, ForeignKey("dispositions.id"))
    note_required = Column(Boolean, nullable=False, server_default="false")
    org = relationship(Organization)

    @classmethod
    def get_by_name(cls, org_uuid: str, disposition_name: str, parent_disposition_id=None):
        return (
            cls.select_query(get_db_session(), columns=[cls.id, cls.parent_disposition_id])
            .join(Organization, cls.organization == Organization.id)
            .filter(
                Organization.uuid == org_uuid,
                func.lower(cls.name) == disposition_name.lower(),
                cls.parent_disposition_id == parent_disposition_id,
                cls.is_deleted.isnot(True),
            )
            # names are not unique: HS writes by name only for its own (system) dispositions
            .order_by(cls.internal.desc().nullslast())
            .first()
        )

    @classmethod
    def get_by_disposition_list(cls, org_uuid: str, disposition_list: list[str]):
        return (
            cls.select_query(get_db_session(), columns=[cls.id, cls.name, cls.uuid])
            .join(Organization, cls.organization == Organization.id)
            .filter(Organization.uuid == org_uuid, cls.name.in_(disposition_list))
            .all()
        )
