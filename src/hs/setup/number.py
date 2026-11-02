from sqlalchemy import Boolean, Column, String
from sqlalchemy.orm import Session

from hs.infra.db import DB, CoreDBBase


class Number(CoreDBBase, DB):
    __tablename__ = "number"
    number = Column(String)
    is_sender_id = Column(Boolean, default=False)

    @classmethod
    def get_by_number(cls, number):
        from hs.infra.context import get_db_session

        db: Session = get_db_session()
        return db.query(cls).filter(cls.number == number, cls.is_deleted.is_(False)).first()
