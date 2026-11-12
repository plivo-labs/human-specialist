from sqlalchemy import func, literal, select, text, update
from sqlalchemy.engine import Connection, Row

from hs.conversations import ON, _str, emit_event, entry
from hs.conversations.store import rows
from hs.core import T

_UNTOLD, _ENDED = "", "ended"
_OPEN = (_UNTOLD, "initiated", "accepted")
_FOLLOWS = {
    "initiated": (_UNTOLD,),
    "accepted": _OPEN,
    "completed": _OPEN,
    "cancelled": _OPEN,
    "failed": ("initiated", "accepted"),
    _ENDED: _OPEN,
}
_ENDS = text(
    "UPDATE assignment SET transfers_told = (SELECT jsonb_object_agg(k, CASE WHEN v = ANY(:follows)"
    " AND k <> ALL(:ringing) THEN :ended ELSE v END) FROM jsonb_each_text(transfers_told) e(k, v))"
    " WHERE conversation_id = :c AND transfers_told IS NOT NULL"
)


def _told(a: Row, status: str):
    A = T.assignment
    return func.coalesce(A.c.transfers_told, literal({}, A.c.transfers_told.type)).concat({str(a.transfer_id): status})


def open_transfer(db: Connection, a: Row) -> None:
    A = T.assignment
    db.execute(update(A).where(A.c.id == a.from_assignment).values(transfers_told=_told(a, _UNTOLD)))


def end_transfers(db: Connection, conv: Row) -> None:
    ringing = [str(a.transfer_id) for a in rows(db, conv.id, ON) if a.transfer_id]
    db.execute(_ENDS, {"c": conv.id, "follows": list(_FOLLOWS[_ENDED]), "ringing": ringing, "ended": _ENDED})


def transferred(db: Connection, a: Row, status: str, to=None, target: dict | None = None) -> None:
    """call.transferred on a call, conversation.transferred on text, where `initiated` marks the conversation
    `transferring` until another status, unless `target` names the action that already moved it."""
    A, tid, moved = T.assignment, str(a.transfer_id), target is not None
    kind = "call.transferred" if a.channel == "call" else "conversation.transferred"
    follows = func.coalesce(A.c.transfers_told[tid].astext, _UNTOLD).in_(_FOLLOWS[status])
    told = update(A).where(A.c.id == a.from_assignment, follows).values(transfers_told=_told(a, status))
    told = told.returning(A.c.agent_id)
    if (row := db.execute(told).first()) is None:
        return
    came = row.agent_id
    if target is None and (team := getattr(a, "team", None)):
        target = {"type": "team", "team_id": str(team)}
    elif target is None and a.agent_id:
        target = {"type": "agent", "agent_id": str(a.agent_id)}
    elif target is None:
        target = {"type": "number", "number": a.address}
    if status == "completed" or (moved and status == "initiated"):
        C = T.conversation
        conv = db.execute(select(C).where(C.c.id == a.conversation_id)).one()
        body = {"transfer_id": tid, "target": target, "mode": a.mode or "cold"}
        entry(db, conv, "transfer", body, agent_id=came, provider_id=f"transfer:{tid}")
    data = {
        "transfer_id": tid,
        "mode": a.mode or "cold",
        "from_agent_id": _str(came),
        "target": target,
        "status": status,
    }
    if a.channel != "call":
        data |= {"to_agent_id": _str(to)}
        C = T.conversation
        mark = {"transfer": tid if status == "initiated" and not moved else None}
        db.execute(update(C).where(C.c.id == a.conversation_id).values(routing=C.c.routing.concat(mark)))
    emit_event(db, kind, a.tenant_id, a.conversation_id, data, came)
