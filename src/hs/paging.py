"""`?limit=&cursor=` lists answering `{data, next_cursor}`; the cursor is opaque to clients."""

import base64
import binascii
from typing import NamedTuple

from fastapi import Query
from pydantic import BaseModel

from hs.errors import ApiError


class Page[T](BaseModel):
    data: list[T]
    next_cursor: str | None


class Paging(NamedTuple):
    limit: int
    after: str | None  # the sort key of the last row the client saw


def paging(limit: int = Query(50, ge=1, le=200), cursor: str | None = None) -> Paging:
    """`?limit=&cursor=`: fetch `limit + 1` rows after `after` and hand them to `page`."""
    try:
        return Paging(limit, base64.b64decode(cursor, b"-_", validate=True).decode() if cursor else None)
    except (binascii.Error, UnicodeDecodeError) as e:
        raise ApiError(422, "invalid_cursor", "cursor is not one this API returned") from e


def page(rows: list[dict], p: Paging, key: str = "id") -> dict:
    """`{data, next_cursor}` from the `limit + 1` rows fetched after the cursor."""
    more, rows = len(rows) > p.limit, rows[: p.limit]
    return {
        "data": rows,
        "next_cursor": base64.urlsafe_b64encode(str(rows[-1][key]).encode()).decode() if more else None,
    }
