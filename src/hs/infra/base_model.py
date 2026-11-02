import datetime
from typing import Annotated

from pydantic import BaseModel as _PydanticBaseModel
from pydantic import ConfigDict, PlainSerializer, model_validator

# pydantic 1.x wire behaviour callers rely on: datetimes as isoformat() ("+00:00", not "Z")
Datetime = Annotated[datetime.datetime, PlainSerializer(datetime.datetime.isoformat, when_used="json")]


class BaseModel(_PydanticBaseModel):
    # and numbers coerced into str fields, timedeltas as float seconds
    model_config = ConfigDict(coerce_numbers_to_str=True, ser_json_timedelta="float")


class LenientModel(BaseModel):
    """A lenient request body: a null field takes its default and unknown fields are ignored."""

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    @model_validator(mode="before")
    @classmethod
    def _drop_nulls(cls, values):
        if isinstance(values, dict):
            return {k: v for k, v in values.items() if v is not None}
        return values
