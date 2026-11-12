"""An agent's reply on another line than its conversation's: another channel, or another address of the customer, in
a linked conversation the agent holds."""

from uuid import UUID

from sqlalchemy import text
from sqlalchemy.engine import Connection, Row

from hs import config
from hs import conversations as cv
from hs.config import models as m
from hs.core import ApiError
from hs.text import CHANNELS, Reply

SWITCH = ("sms", "whatsapp", "email")
PHONE = {"call", "sms", "whatsapp"}
# A family is the call or text a switch started from and every conversation linked from it, each by its direct row
# naming the stint it came from. _ROOT walks up from :id; _FAMILY keeps the live conversations on :channel that the
# agent holds as primary and whose walk reaches :root.
_UP = (
    " s.conversation_id, up.n + 1 FROM up JOIN assignment t ON t.conversation_id = up.id AND t.via = 'direct'"
    " JOIN assignment s ON s.id = t.from_assignment AND s.conversation_id <> up.id) CYCLE id SET looped USING path"
)
_ROOT = text(
    f"WITH RECURSIVE up(id, n) AS (SELECT CAST(:id AS uuid), 0 UNION ALL SELECT{_UP}"  # noqa: S608 (constants only)
    " SELECT id FROM up ORDER BY n DESC LIMIT 1"
)
_FAMILY = text(
    "WITH RECURSIVE mine AS (SELECT c.id, c.customer, c.ours FROM assignment a JOIN conversation c"  # noqa: S608 (constants only)
    " ON c.id = a.conversation_id WHERE a.agent_id = :agent AND a.channel = :channel AND a.state = 'active'"
    " AND a.role = 'primary' AND c.state = ANY(CAST(:live AS text[]))), up(start, id, n) AS (SELECT id, id, 0 FROM mine"
    f" UNION ALL SELECT up.start,{_UP} SELECT DISTINCT m.id, m.customer, m.ours FROM mine m"
    " JOIN up ON up.start = m.id WHERE up.id = :root ORDER BY m.id"
)


def _reply_line(db: Connection, conv: Row, body: Reply) -> tuple[str, str, str]:
    channel = body.channel or conv.channel
    kin = channel == conv.channel or {channel, conv.channel} <= PHONE
    try:
        to = m.address(channel, body.to) if body.to else conv.customer if kin else None
        ours = m.address(channel, body.from_) if body.from_ else conv.ours if kin else None
    except ValueError as e:
        raise ApiError(422, "invalid_address", str(e)) from None
    ours = ours or config.tenant_settings(db, conv.tenant_id).default_from.get(channel)
    own = (channel, to, ours) == (conv.channel, conv.customer, conv.ours)
    if channel not in (CHANNELS if own else SWITCH) or conv.channel not in ("call", *CHANNELS):
        raise ApiError(422, "channel_not_allowed", "a reply goes on its text channel, or sms, whatsapp or email")
    if not to:
        raise ApiError(422, "to_required", "name the customer's address on this channel")
    if not ours or (not own and config.by_key(db, conv.tenant_id, "number", ours) is None):
        raise ApiError(422, "from_not_allowed", "from must be a number or address registered with PUT /numbers")
    return channel, to, ours


def _hold(db: Connection, tenant_id, cid) -> UUID:
    """The root of the family `cid` belongs to, its row locked before any other: replies in one family take turns, and
    a switch locks the root, its source, then a destination in the family, always in that order."""
    root = db.execute(_ROOT, {"id": cid}).scalar()
    cv.lock(db, root, tenant=tenant_id)
    return root


def _switch(
    db: Connection, conv: Row, stint: Row, line: tuple[str, str, str], body: Reply, root: UUID
) -> tuple[Row, bool]:
    channel, to, ours = line
    if stint.state != "active":
        if (to, ours) != (conv.customer, conv.ours):
            raise ApiError(409, "in_wrap_up", "an agent in wrap-up sends files only to the customer's own address")
        return conv, False
    args = {"agent": stint.agent_id, "channel": channel, "live": list(cv.LIVE), "root": root}
    family = db.execute(_FAMILY, args).all()
    kin = [conv, *family] if conv.channel == channel else family
    if held := next((c.ours for c in kin if c.ours != ours), None):
        raise ApiError(422, "sender_mismatch", "this conversation writes here from another sender", {"from": held})
    if mine := next((c for c in family if c.customer == to), None):
        t, a = cv.lock(db, mine.id), cv.primary_agent(db, mine.id)
        if t.state in cv.LIVE and a and a.agent_id == stint.agent_id:
            return t, False
    if channel == "whatsapp" and body.template is None:
        raise ApiError(422, "template_required", "a WhatsApp conversation the agent opens starts with a template")
    parties = {"channel": channel, "direction": "outbound", "customer": to, "ours": ours}
    # One already open on this line is refused without its lock: its holder may be waiting on this family's rows.
    if (thread := cv.open(db, conv.tenant_id, **parties)).id != db.info["tx"].get("opened"):
        raise ApiError(409, "conversation_open", "this customer already has an open conversation on this line")
    cv.offer(db, thread, stint.agent_id, via="direct", came=stint.id)
    cv.arm(db, thread)
    return thread, True
