from datetime import datetime
from itertools import chain
from uuid import UUID

from sqlalchemy import Text, case, func, select
from sqlalchemy.dialects.postgresql import aggregate_order_by
from sqlalchemy.engine import Connection, Row

from hs import config
from hs.conversations import LIVE, TEXT, Offer, _str, inbox
from hs.conversations.store import rows
from hs.core import T


def _mapped(db: Connection, conv: Row, result: str) -> str | None:
    """The tenant's system disposition for a result; queue, text and task results live in the inbound map only, and
    one the tenant's stored map predates takes its default, its direction's first."""
    status_map = config.tenant_settings(db, str(conv.tenant_id)).status_map
    if mapped := status_map[conv.direction].get(result) or status_map["inbound"].get(result):
        return mapped
    defaults = config.default_map(db, conv.tenant_id)
    return defaults[conv.direction].get(result) or defaults["inbound"].get(result)


def _dispositions(db: Connection, conv: Row, every: list[Row]) -> list[dict]:
    """Each agent's disposition with its names, which a deleted one keeps; a closed conversation no agent wrapped up
    takes its result's system one."""
    t, out = str(conv.tenant_id), []

    def name(did) -> str | None:
        return did and (config.get(db, t, "disposition", did) or {}).get("name")

    for a in (a for a in every if a.disposition_id):
        d, skipped = config.get(db, t, "disposition", a.disposition_id), a.reason == "skipped"
        ids = {"disposition_id": str(a.disposition_id), "sub_disposition_id": _str(a.sub_disposition_id)}
        ids |= {"disposition": d and d["name"], "sub_disposition": name(a.sub_disposition_id)}
        out.append(
            {"agent_id": _str(a.agent_id), **ids, "system": skipped or bool(d and d["system"]), "skipped": skipped}
        )
    if not out and conv.state == "closed" and (system := _mapped(db, conv, conv.result)):
        ids = {"disposition_id": str(system), "sub_disposition_id": None, "disposition": name(system)}
        out.append({"agent_id": None, **ids, "sub_disposition": None, "system": True, "skipped": False})
    return out


def _entries(db: Connection, cid: UUID, kinds: tuple[str, ...]) -> list[Row]:
    E = T.entry
    kept = (E.c.conversation_id == cid, E.c.kind.in_(kinds), E.c.deleted_at.is_(None))
    return db.execute(select(E).where(*kept).order_by(E.c.at, func.coalesce(E.c.written_at, E.c.at), E.c.id)).all()


def note(e: Row) -> dict:
    shown = {"text": e.body["text"], "attachments": e.body.get("attachments", []), "agent_id": _str(e.agent_id)}
    return shown | {"id": str(e.id), "created_at": e.at, "updated_at": e.body.get("updated_at", e.at)}


def _media(db: Connection, cid: UUID) -> list[str]:
    return [str(e.id) for e in _entries(db, cid, ("recording", "voicemail"))]


def summary(agent_id=None):
    """The list row of a conversation, as GET /conversations returns it: the select every read builds on. With
    `agent_id`, the row says how that agent came to it: its latest stint's role and via."""
    C, A, G = T.conversation, T.assignment, T.config
    took = (A.c.conversation_id == C.c.id, A.c.agent_id.isnot(None), A.c.started_at.isnot(None))
    agents = select(A.c.agent_id.cast(Text)).where(*took, A.c.role.in_(("primary", "secondary"))).distinct()
    theirs, talking = C.c.routing.contains({"turn": "customer"}), C.c.state.in_(LIVE) & C.c.channel.in_(TEXT)
    state = case(
        (C.c.state == "wrap_up", "in_wrap_up"),
        (C.c.state == "closed", "closed"),
        (C.c.routing["transfer"].astext.isnot(None), "transferring"),
        else_="active",
    )
    mine = (*took, A.c.agent_id == agent_id, A.c.role.in_(("primary", "secondary")))

    def latest(col):
        return select(col).where(*mine).order_by(A.c.offered_at.desc(), A.c.id.desc()).limit(1).scalar_subquery()

    shown = {"id": A.c.id.cast(Text), "agent_id": A.c.agent_id.cast(Text), "transfer_id": A.c.transfer_id.cast(Text)}
    shown |= {"number": case((A.c.role == "external", A.c.address))}
    shown |= {c: A.c[c] for c in ("role", "via", "mode", "state", "started_at", "ended_at", "wrap_up_until")}
    each = aggregate_order_by(func.jsonb_build_object(*chain(*shown.items())), A.c.started_at, A.c.id)
    on = select(func.coalesce(func.jsonb_agg(each), func.jsonb_build_array()))
    on = on.where(A.c.conversation_id == C.c.id, A.c.state.in_(("active", "wrap_up")))

    return select(
        C.c.id.cast(Text).label("id"),
        latest(A.c.role).label("role"),
        latest(A.c.via).label("via"),
        C.c.number.cast(Text).label("conversation_number"),
        C.c.channel,
        C.c.direction,
        state.label("state"),
        C.c.customer.label("number"),
        func.array(agents.scalar_subquery()).label("agent_ids"),
        C.c.queue_id.cast(Text).label("queue_id"),
        select(G.c.body["name"].astext).where(G.c.id == C.c.queue_id).scalar_subquery().label("queue_name"),
        C.c.opened_at.label("created_at"),
        C.c.closed_at.label("ended_at"),
        case((talking & theirs, "customer"), (talking, "agent")).label("awaiting"),
        ((C.c.state == "live") & C.c.routing["waiting"].astext.isnot(None)).label("waiting"),
        on.scalar_subquery().label("assignments"),
        *inbox.columns(),
    )


def _iso(at) -> str | None:
    return at and at.isoformat()


def view(db: Connection, conv: Row, agent_id=None) -> dict:
    """The one read model of a conversation: GET /conversations/{id} and the search document. An agent reads the
    handoff context only once it took the conversation as primary, or while it is on it."""
    doc = dict(db.execute(summary(agent_id).where(T.conversation.c.id == conv.id)).one()._mapping)
    inbox.counted(db, conv.tenant_id, [doc])
    every = rows(db, conv.id)
    mine = [a for a in every if agent_id and _str(a.agent_id) == str(agent_id)]
    reads = not agent_id or any(a.state == "active" or (a.role == "primary" and a.started_at) for a in mine)
    left = conv.state in ("wrap_up", "closed") and conv.routing.get("left_at")
    legs = [(conv.call_uuid, "customer", conv.opened_at, (left and datetime.fromisoformat(left)) or conv.closed_at)]
    legs += [(a.call_uuid, a.role, a.started_at, a.ended_at) for a in every if not isinstance(a, Offer)]
    entries = _entries(db, conv.id, ("note", "recording", "voicemail"))
    return doc | {
        "legs": [{"call_uuid": u, "role": r, "started_at": _iso(s), "ended_at": _iso(e)} for u, r, s, e in legs if u],
        "notes": [note(n) for n in entries if n.kind == "note"],
        "recording_ids": [str(e.id) for e in entries if e.kind != "note"],
        "context": conv.context if reads else None,
        "attributes": conv.attributes,
        "dispositions": _dispositions(db, conv, every),
    }
