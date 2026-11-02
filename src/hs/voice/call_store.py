from sqlalchemy import Column, ForeignKey, Integer, String, func
from sqlalchemy.orm import Session, aliased

from hs.infra.context import get_db_session
from hs.infra.db import DB, CoreDBBase
from hs.infra.helpers import get_now
from hs.voice.call_models import CallLegModel


class CallLeg(DB, CoreDBBase):
    __tablename__ = "call_leg"
    call_uuid = Column(String, index=True)
    participant = Column(String)
    start_time = Column(String)
    end_time = Column(String)
    role = Column(String)
    parent_call_leg = Column(Integer, ForeignKey("call_leg.id"), index=True)

    def __to_model(self) -> CallLegModel:
        return CallLegModel.model_validate(self)

    @classmethod
    def get_by_call_uuid(cls, call_uuid: str, role: str | None = None, participant_leg: bool = False) -> CallLegModel:
        db: Session = get_db_session()
        call_leg = cls.select_query(db).filter(cls.call_uuid == call_uuid)
        if role:
            call_leg = call_leg.filter(cls.role == role)
        elif participant_leg:
            call_leg = call_leg.filter(cls.role.is_not(None))
        else:
            call_leg = call_leg.filter(cls.role.is_(None))
        call_leg = call_leg.first()
        if not call_leg:
            return None
        return call_leg.__to_model()

    @classmethod
    def get_any_by_call_uuid(cls, call_uuid: str) -> CallLegModel:
        """Resolve an exact provider leg UUID regardless of parent/role shape."""
        db: Session = get_db_session()
        call_leg = cls.select_query(db).filter(cls.call_uuid == call_uuid).first()
        if not call_leg:
            return None
        return call_leg.__to_model()

    @classmethod
    def outside_call(cls, leg_uuid: str, call_uuid: str) -> bool:
        parent = aliased(cls)
        owning_call = func.coalesce(parent.call_uuid, cls.call_uuid)
        rows = cls.select_query(get_db_session(), [cls.id]).outerjoin(parent, parent.id == cls.parent_call_leg)
        return rows.filter(cls.call_uuid == leg_uuid, owning_call != call_uuid).first() is not None

    @classmethod
    def create_call_leg(cls, call_leg) -> CallLegModel:
        db: Session = get_db_session()
        # SAVEPOINT-isolate the INSERT: concurrent voice events (e.g. MpcParticipantJoin + a status
        # callback) race to create the same call leg and one loses on the call_uuid unique constraint.
        # Nesting the add+flush means a duplicate-key IntegrityError rolls back only this savepoint and
        # re-raises, leaving the request session usable — so callers (the create_*call_leg functions)
        # can recover by re-fetching the existing row instead of the whole transaction aborting with
        # "This Session's transaction has been rolled back due to a previous exception during flush".
        with db.begin_nested():
            db.add(call_leg)
            db.flush()
        return call_leg.__to_model()

    @classmethod
    def update_call_leg(cls, call_uuid: str, update_dict: dict) -> None:
        db: Session = get_db_session()
        update_dict.update({cls.updated_at.name: get_now()})
        cls.select_query(db).filter(cls.call_uuid == call_uuid).update(update_dict)
        db.flush()

    @classmethod
    def get_all_call_legs_by_parent_call_leg(cls, parent_call_leg_id: int) -> list[CallLegModel]:
        db: Session = get_db_session()
        call_legs = cls.select_query(db).filter(cls.parent_call_leg == parent_call_leg_id).all()
        return [call_leg.__to_model() for call_leg in call_legs]
