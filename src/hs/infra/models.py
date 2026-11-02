import json
import sys
import typing
from uuid import UUID

from hs.infra.base_model import BaseModel, Datetime
from hs.infra.log import logger


class CacheModel(BaseModel):
    @classmethod
    def build_from_cache(cls, data_from_cache):
        if not data_from_cache:
            return None
        try:
            if isinstance(data_from_cache, str):
                data_from_cache = json.loads(data_from_cache)
            return cls.model_validate(data_from_cache)
        except Exception as e:
            logger.error(f"error in parsing response from cache : {e}")
            return None


class DBBase(CacheModel):
    id: str | None = None
    created_at: Datetime | None = None
    updated_at: Datetime | None = None
    is_deleted: bool | None = None


class CoreDBBase(DBBase):
    uuid: UUID | None = None


class UseCaseResponse(BaseModel):
    status_code: int | None = None
    error: str | None = None
    msg: str | None = None
    data: typing.Any = None


def fail(error, status_code: int = 400, data=None, log=None, rollback: bool = False) -> "UseCaseResponse":
    """A failed use-case result, logged as `in <calling function> error : <error>` (or `log` when given);
    rollback marks the request session to roll back."""
    logger.error(log if log is not None else f"in {sys._getframe(1).f_code.co_name} error : {error}")
    if rollback:
        from hs.infra.context import set_db_session_rollback

        set_db_session_rollback()
    return UseCaseResponse(status_code=status_code, error=error, data=data)
