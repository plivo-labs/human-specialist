"""The SSE frames of GET /events/stream: no `event:` field, so EventSource.onmessage receives every event."""

import json
import time
from collections.abc import AsyncIterator, Awaitable, Callable

PING_EVERY = 15.0
PAGE = 200


async def frames(
    latest: Callable[[], Awaitable[int]],
    read: Callable[[int, int], Awaitable[list[dict]]],
    cursor: int,
    alive: Callable[[], Awaitable[bool]],
    wake: Callable[[], Awaitable[object]],
) -> AsyncIterator[str]:
    """Events after `cursor` as they are stamped; `wake` returns on a new stamp or after about a second.

    Each pass reads up to the tenant's latest seq. A short page means everything to there was seen, so the
    cursor moves to it (sent as an id-only frame) and a scoped stream never re-reads what it may not see."""
    yield "retry: 3000\n: ping\n\n"
    pinged = time.monotonic()
    while True:
        head = await latest()
        batch = await read(cursor, head)
        for event in batch:
            cursor = int(event["id"])
            yield f"id: {event['id']}\ndata: {json.dumps(event, separators=(',', ':'))}\n\n"
        if len(batch) < PAGE and head > cursor:
            cursor = head
            yield f"id: {head}\n\n"
        if not await alive():
            yield 'data: {"type":"auth.expired"}\n\n'
            return
        if time.monotonic() - pinged >= PING_EVERY:
            pinged = time.monotonic()
            yield ": ping\n\n"
        if len(batch) < PAGE:
            await wake()
