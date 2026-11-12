"""Conversations, assignments and entries: the two state machines.

A conversation is one contact with a human. An assignment is one stint on it (an agent's, a phone's or a
supervisor's), from accept to wrap-up. An offer lives in Redis (`hot`) and is open only while its
`offer_timeout` row exists too; once over it stays there as a tombstone for the channel's life. An entry is a
message, note, signal, recording or voicemail. Every write holds the conversation's row lock from `lock()`, and each
transition is one guarded change: none means a duplicate or late event, and the caller does nothing."""

import json
from datetime import UTC, datetime, timedelta
from functools import partial
from operator import attrgetter
from types import SimpleNamespace
from uuid import UUID, uuid4

from sqlalchemy import case, func, literal_column, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Connection, Row

from hs import access, config, events, hot, jobs, plivo
from hs.conversations import inbox, numbers
from hs.core import ApiError, T, after_commit, before_commit, capped, tx

LIVE = ("open", "queued", "live")
ON = ("offered", "active")
# Conversation: target state -> the states it may come from. Every close passes through `gone` and `wrap_up`.
CONVERSATION = {
    "queued": ("open", "live"),
    "live": ("open", "queued"),
    "open": ("queued", "live"),
    "wrap_up": LIVE,
    "closed": ("wrap_up",),
}
# A manual wrap-up waits for the agent this long past its announced deadline, then closes as skipped.
SAFETY = timedelta(seconds=300)
# Offers of one queue round or one team transfer compete: the first accept withdraws the rest. Participants do not.
COMPETE = ("queue", "transfer")
# A missed call offer counts a RONA strike only when it rang out: HS's offer timer, or Plivo's ring timeout cause.
RANG_OUT = ("timeout", "6010")
BUSY_OR_UNREGISTERED = ("3010", "2020")
# The longest a call HS never heard end stays open: the tenant's call_session_timeout_sec is at most this.
CALL_SESSION = timedelta(hours=24)
TEXT = ("chat", "sms", "whatsapp", "email")
CHAT_WAITING_WINDOW = timedelta(hours=25)
# An agent may be away from the desk for days: a task or callback offer waits 72 hours less one, however it came.
TASK_OFFER_SEC = 71 * 3600
UNHELD = {
    "hold_sec": literal_column("hold_sec + coalesce(CAST(extract(epoch FROM now() - held_at) AS int), 0)"),
    "held_at": None,
}


class ForeignCall(ApiError):
    """Another tenant's conversation holds this call's uuid. A Plivo answer naming it gets an unknown call's answer,
    so it confirms nothing."""


def _id(row: Row | UUID) -> UUID:
    return getattr(row, "id", row)


def _str(value) -> str | None:
    return None if value is None else str(value)


def emit_event(db: Connection, type_: str, tenant_id, cid, data: dict, agent_id=None) -> None:
    events.emit(db, type_, str(tenant_id), data, conversation_id=str(cid), agent_id=_str(agent_id))


class Offer(SimpleNamespace):
    """Read like an assignment row: a column it does not keep is None."""

    def __getattr__(self, name: str) -> None:
        return None


_UUIDS = ("id", "tenant_id", "conversation_id", "agent_id", "ring", "from_assignment", "transfer_id")
_KEPT = (*_UUIDS, "role", "via", "mode", "channel", "state", "round", "address", "call_uuid", "team", "reason")


def offer_row(o: dict) -> Offer:
    return Offer(**{k: UUID(v) if k in _UUIDS and v else v for k, v in o.items()})


def _writes(db: Connection, a) -> dict:
    memo, key = db.info["tx"], ("offers", a.conversation_id)
    if key not in memo:
        memo[key] = {"creates": {}, "binds": {}}
        before_commit(db, partial(_put, a.tenant_id, a.conversation_id, a.channel, memo[key]))
    return memo[key]


def _put(tenant_id, cid, channel: str, w: dict) -> None:
    def row(a: Offer) -> str:
        return json.dumps({k: _str(getattr(a, k)) if k in _UUIDS else getattr(a, k) for k in _KEPT})

    creates = [(str(a.id), _str(a.agent_id), row(a)) for a in w["creates"].values() if a.state == "offered"]
    binds = [(str(a.id), _str(a.agent_id) if a.state == "offered" else None, a.call_uuid) for a in w["binds"].values()]
    hot.create_and_bind(tenant_id, cid, channel, creates, binds)


def _opened(o: dict, timer: dict) -> dict:
    expires = datetime.fromisoformat(timer["run_at"])
    return o | {"offered_at": expires - timedelta(seconds=timer["timeout"]), "expires_at": expires}


def retire_lost(db: Connection, conv: Row, lost: list[dict]) -> set[str]:
    for t in lost:
        jobs.cancel(db, "offer_timeout", t["assignment_id"])
        _free(db, Offer(id=t["assignment_id"], tenant_id=conv.tenant_id, agent_id=t["agent_id"], channel=conv.channel))
    return {t["assignment_id"] for t in lost}


def desk(
    db: Connection, tenant_id, agent_id, states: tuple[str, ...] | None = ("offered",), ids: list | None = None
) -> list:
    """The agent's open offers, oldest first, with the outbound dials still ringing; or its rows in `states`, in any
    state for None, tombstones included, and only `ids` if given."""
    A, J = T.assignment, T.jobs
    where = [A.c.tenant_id == tenant_id, A.c.agent_id == agent_id] + ([A.c.state.in_(states)] if states else [])
    where += [] if ids is None else [A.c.id.in_(ids)]
    stints = db.execute(select(A).where(*where).order_by(A.c.offered_at, A.c.id)).all()
    if states and "offered" not in states:
        return stints
    mine = {str(x.id) for x in stints}
    homes = hot.desk(tenant_id, agent_id) if ids is None else hot.located(tenant_id, [str(i) for i in ids])
    found = hot.fresh_rows(tenant_id, homes).items()
    kept = {o: r for o, r in found if o not in mine and r["agent_id"] == str(agent_id)}
    opened = [o for o, r in kept.items() if r["state"] == "offered"]
    due = select(J.c.key, J.c.run_at, J.c.payload).where(J.c.kind == "offer_timeout", J.c.key.in_(opened))
    timers = {k: p | {"run_at": at.isoformat()} for k, at, p in db.execute(due)} if opened else {}
    offers = [offer_row(_opened(r, timers[o])) for o, r in kept.items() if o in timers]
    offers.sort(key=lambda a: a.offered_at)
    ended = [offer_row(r) for r in kept.values() if r["state"] != "offered"] if states is None else []
    return stints + offers + ended


# The slots each agent's stints, offer timers and missed legs hold, with the channel's cap (defaults as
# config.models.Stored), for the conversations `ids` names and agent `agent`, each when not null. A wrap-up holds one
# while wrap-up blocks routing, or while the room it left holds it.
_HELD = text(
    "SELECT x.tenant_id::text AS tenant, x.channel, x.agent_id::text AS agent, x.id::text AS holder,"
    " x.conversation_id::text AS conv, coalesce((t.settings->'capacity'->>x.channel)::int, 1) AS cap, false AS timer"
    " FROM assignment x JOIN tenant t"
    " ON t.id = x.tenant_id WHERE x.agent_id IS NOT NULL AND x.state IN ('offered', 'active', 'wrap_up')"
    " AND x.via <> 'self'"
    " AND (x.state <> 'wrap_up' OR x.held_by_room IS NOT NULL"
    "  OR coalesce((t.settings->>'acw_blocks_routing')::boolean, true))"
    " AND (CAST(:ids AS uuid[]) IS NULL OR x.conversation_id = ANY(CAST(:ids AS uuid[])))"
    " AND (CAST(:agent AS uuid) IS NULL OR x.agent_id = CAST(:agent AS uuid))"
    " UNION ALL SELECT j.payload->>'tenant_id', j.payload->>'channel', j.payload->>'agent_id', j.key,"
    " j.payload->>'conversation_id', coalesce((t.settings->'capacity'->>(j.payload->>'channel'))::int, 1),"
    " j.kind = 'offer_timeout'"
    " FROM jobs j JOIN tenant t ON t.id::text = j.payload->>'tenant_id'"
    " WHERE j.kind IN ('offer_timeout', 'missed_leg') AND j.payload->>'agent_id' IS NOT NULL"
    " AND (CAST(:ids AS text[]) IS NULL OR j.payload->>'conversation_id' = ANY(CAST(:ids AS text[])))"
    " AND (CAST(:agent AS text) IS NULL OR j.payload->>'agent_id' = CAST(:agent AS text))"
)


_OPENING = text("SELECT pg_advisory_xact_lock_shared(:k)")
_NOT_OPENING = text(
    "SELECT c FROM unnest(CAST(:ids AS text[]), CAST(:keys AS bigint[])) u(c, k) WHERE pg_try_advisory_xact_lock(k)"
)
_FOUND = text("SELECT id::text FROM conversation WHERE id = ANY(CAST(:ids AS uuid[]))")


def _key(cid) -> int:
    return int.from_bytes(UUID(str(cid)).bytes[8:], "big", signed=True)


def abandoned(db: Connection, ids: list[str]) -> set[str]:
    """Those of conversations `ids` whose opening transaction ended without a commit, as a dead process's does. The
    caller's transaction holds their keys to its end; a key another transaction holds keeps its conversation out."""
    ended = db.execute(_NOT_OPENING, {"ids": ids, "keys": [_key(c) for c in ids]}).scalars().all() if ids else []
    # A statement after the keys were taken, so it sees a commit that came before them.
    return set(ended) - set(db.execute(_FOUND, {"ids": ended}).scalars()) if ended else set()


def held(db: Connection, ids: list | None = None, agent=None, lost: list | None = None) -> list[Row]:
    """`_HELD`'s rows for `ids` and `agent`, an offer's only while Redis holds it open, or accepted after the read,
    since its stint then holds the same slot."""
    found = db.execute(_HELD, {"ids": ids and [str(i) for i in ids], "agent": _str(agent)}).all()
    timers = [r for r in found if r.timer]
    states = hot.standing(db, [(r.tenant, r.conv, r.holder) for r in timers])
    gone = {r.holder for r, s in zip(timers, states, strict=True) if s not in ("offered", "active")}
    if lost is not None:
        lost += [r.conv for r, s in zip(timers, states, strict=True) if s is None]
    return [r for r in found if r.holder not in gone]


def open(
    db: Connection,
    tenant_id,
    *,
    channel,
    direction,
    customer,
    ours,
    context=None,
    call_uuid=None,
    routing=None,
    agent_id=None,
    **started,
):
    """A new conversation, or the one already open for this call or text thread. Only a new one emits
    `conversation.started`, carrying `started` (the caller's number, entry point and hours) and naming the agent
    `agent_id` that starts it, and is the transaction's `opened`."""
    C = T.conversation
    values = {"tenant_id": tenant_id, "channel": channel, "direction": direction, "state": "open", "customer": customer}
    values |= {"ours": ours, "context": capped(context) or None, "call_uuid": call_uuid, "routing": routing or {}}
    if call_uuid:
        same = (C.c.call_uuid == call_uuid,)
    elif channel not in TEXT:
        same = None
    else:
        same = (C.c.channel == channel, C.c.customer == customer, C.c.ours == ours, C.c.state.in_(LIVE))
    for _ in range(2):
        # The conflicting one may close before this read sees it, and an older replica may take this number from the
        # shared sequence meanwhile, so a second attempt reads again and takes a fresh number.
        if same and (row := db.execute(select(C).where(C.c.tenant_id == tenant_id, *same)).first()):
            return row
        values["number"] = numbers.take(tenant_id)
        if row := db.execute(insert(C).values(**values).on_conflict_do_nothing().returning(*C.c)).first():
            db.info["tx"]["opened"] = row.id
            db.execute(_OPENING, {"k": _key(row.id)})
            emit_event(db, "conversation.started", tenant_id, row.id, {"channel": channel, **started}, agent_id)
            if channel == "call":
                job = {"conversation_id": str(row.id)}
                after = config.tenant_settings(db, tenant_id).call_session_timeout_sec
                jobs.schedule(db, "call_session", job, delay=after, key=str(row.id))
            return row
    error = ForeignCall if call_uuid else ApiError
    raise error(409, "conversation_conflict", "that call belongs to another conversation")


# Teams of the primary agents who took the conversation; listeners, unfinished consults and participants add none.
# Stored at the close, so search files it under them from then on.
TEAMS_OF = (
    "(SELECT coalesce(jsonb_agg(DISTINCT t), '[]') FROM hs.assignment a JOIN hs.config g"
    " ON g.id = a.agent_id AND g.kind = 'agent' CROSS JOIN LATERAL"
    " jsonb_array_elements_text(coalesce(g.body->'teams', '[]')) t"
    " WHERE a.conversation_id = {} AND a.role = 'primary' AND a.started_at IS NOT NULL)"
)
TEAMS = text(TEAMS_OF.format("hs.conversation.id"))


def set_state(db: Connection, conv: Row | UUID, to: str, *, took: list[Row] | None = None, **cols) -> Row | None:
    """Move the conversation to `to` if its state allows it; returns the new row, or None. A close reads the rows
    unless the caller passes them as `took`."""
    C = T.conversation
    if to == "live":
        cols["answered_at"] = func.coalesce(C.c.answered_at, func.now())
    else:
        cols |= UNHELD
    if to == "wrap_up":
        default = case((C.c.answered_at.isnot(None), "answered"), else_="abandoned")
        cols["result"] = func.coalesce(cols.get("result"), C.c.result, default)
    if to == "closed":
        cols |= {"closed_at": func.now(), "version": C.c.version + 1, "updated_at": func.now(), "team_ids": TEAMS}
    guarded = update(C).where(C.c.id == _id(conv), C.c.state.in_(CONVERSATION[to])).values(state=to, **cols)
    row = db.execute(guarded.returning(*C.c)).first()
    if row and to == "live":
        jobs.cancel(db, "queue_timeout", str(row.id))
    if row and to == "closed":
        jobs.cancel(db, "call_session", str(row.id))
        after_commit(db, partial(hot.drop_conversation, row.tenant_id, row.id))
        took = rows(db, row.id) if took is None else took
        data = {"result": row.result, "dispositions": _dispositions(db, row, took), "recording_ids": _media(db, row.id)}
        data["duration_sec"] = int((row.closed_at - row.opened_at).total_seconds())
        last = next((a.agent_id for a in reversed(took) if a.role == "primary" and a.started_at), None)
        emit_event(db, "conversation.closed", row.tenant_id, row.id, data, last)
        payload = {"tenant_id": str(row.tenant_id), "conversation_id": str(row.id)}
        jobs.schedule(db, "search_index", payload, key=str(row.id))
    return row


def offer(
    db: Connection,
    conv: Row,
    agent_id,
    *,
    role="primary",
    via="queue",
    mode=None,
    address=None,
    transfer_id=None,
    came=None,
    team=None,
    force: bool = False,
    timeout: int | None = None,
) -> Row | Offer | None:
    """Offer the conversation to an agent (or a phone, by `address`). `via="direct"` skips the offer: the row
    starts active; `via="self"` too, for an agent's own task, which holds none of its slots. An agent transfer target
    is a secondary until `promote`; a phone stays external. A transfer's offers carry its id and the primary stint
    they `came` from, the current one unless named. The agent's presence row is share-locked first, so an agent
    delete waits for the offer and refuses, or the offer finds the agent gone (404). The agent takes a free slot on
    the channel: without one there is no offer (None), and a direct row or a participant is 409. `force` takes one
    past the cap, as a supervisor or an extra participant joins a call whatever its load. `timeout` overrides the
    tenant's offer timeout."""
    P = T.presence
    held = select(P.c.agent_id).where(P.c.agent_id == agent_id).with_for_update(read=True)
    if agent_id and not db.execute(held).first():
        raise ApiError(404, "agent_not_found", "no such agent")
    s, aid = config.tenant_settings(db, str(conv.tenant_id)), uuid4()
    if agent_id and via != "self" and not hot.take(db, conv, agent_id, aid, getattr(s.capacity, conv.channel), force):
        if via in ("direct", "participant"):
            raise ApiError(409, "agent_at_capacity", "the agent has no free slot on this channel")
        return None
    A, direct = T.assignment, via in ("direct", "self")
    role = "secondary" if via == "transfer" and role == "primary" else role
    if via == "transfer" and came is None:
        came = getattr(primary_agent(db, conv.id), "id", None)
    row = {"id": aid, "tenant_id": conv.tenant_id, "conversation_id": conv.id, "agent_id": agent_id, "role": role}
    row |= {"via": via, "mode": mode, "channel": conv.channel, "state": "active" if direct else "offered"}
    row |= {"round": conv.round, "address": address, "from_assignment": came, "transfer_id": transfer_id}
    if direct:
        a = db.execute(insert(A).values(**row, started_at=func.now()).returning(*A.c)).one()
        if role == "primary":
            _sticks(db, a, set_state(db, conv, "live"))
        return a
    a = Offer(**row, team=_str(team), ring=None, call_uuid=None, reason=None)
    _writes(db, a)["creates"][aid] = a
    text = s.text.offer_timeout_sec
    text = text.get(conv.channel, 30) if isinstance(text, dict) else text
    lives = {"call": s.ring_timeout_sec, "task": TASK_OFFER_SEC, "callback": TASK_OFFER_SEC}
    timeout = timeout or lives.get(conv.channel, text)
    queue = queue_of(db, conv)
    if a.agent_id and role != "monitor":
        data = {"offer_id": str(a.id), "channel": conv.channel, "queue_id": _str(conv.queue_id), "from": conv.customer}
        data |= {"to": conv.ours or None}
        data |= {"ring_timeout_sec": timeout, "auto_accept": bool(queue and queue["auto_accept"]["enabled"])}
        data |= {"conversation_number": str(conv.number), "via": shown(via, conv)}
        emit_event(db, "offer.created", conv.tenant_id, conv.id, data, a.agent_id)
    job = {"tenant_id": str(conv.tenant_id), "conversation_id": str(conv.id), "assignment_id": str(a.id)}
    job |= {"agent_id": _str(agent_id), "channel": conv.channel, "timeout": timeout}
    jobs.schedule(db, "offer_timeout", job, delay=timeout, key=str(a.id))
    return a


def shown(via: str, conv: Row) -> str:
    """How an offer says it came: a queue's offer for a conversation an agent already took is a transfer's."""
    return "transfer" if via == "queue" and conv.answered_at else via


def move(db: Connection, a: Row | Offer | UUID, event: str, **cols) -> Row | Offer | None:
    """Apply `event` to the stint or open offer, then its effects; None when its state does not allow it. An accept
    makes the offer a stint of the same id. Any other end leaves its tombstone after COMMIT, and a row too when it
    names the member of a leg that exited before HS heard it join, so its late join finds it."""
    A = T.assignment
    froms, to, stamp, effect = ASSIGNMENT[event]
    if stamp:
        cols.setdefault(stamp, func.now())
    if event == "leave":
        tenant = getattr(a, "tenant_id", None) or db.execute(select(A.c.tenant_id).where(A.c.id == a)).scalar()
        wrap = config.tenant_settings(db, str(tenant)).wrap_up.seconds if tenant else 0
        cols.setdefault("wrap_up_until", func.now() + timedelta(seconds=wrap))
    if isinstance(a, Offer) and a.state == "offered":
        ends = event in ("accept", "decline", "miss", "withdraw") and (to != "active" or _claim(db, a))
        if not ends or not jobs.cancel(db, "offer_timeout", str(a.id)):
            return None
        reason = cols.get("reason")
        if to == "active" or "member_id" in cols:
            kept = {k: v for k, v in vars(a).items() if k in A.c}
            kept |= {"state": to, "offered_at": a.offered_at or func.now()}
            if to == "active" and a.via == "queue":
                C, visit = T.conversation, ("queue_id", "queued_at")
                kept |= {k: select(C.c[k]).where(C.c.id == a.conversation_id).scalar_subquery() for k in visit}
            row = db.execute(insert(A).values(kept | cols).returning(*A.c)).one()
        else:
            row = Offer(**vars(a) | {"state": to, "reason": reason})
        after_commit(db, partial(hot.tombstone, a.tenant_id, a.conversation_id, a.agent_id, a.id, to, reason))
        effect(db, row)
        return row
    guarded = update(A).where(A.c.id == _id(a), A.c.state.in_(froms)).values(state=to, **cols)
    if (row := db.execute(guarded.returning(*A.c)).first()) is not None:
        if froms == ("offered",) and event != "answer":
            jobs.cancel(db, "offer_timeout", str(row.id))
        effect(db, row)
    return row


def _claim(db: Connection, a: Offer) -> bool:
    hot.share_slots(db, str(a.tenant_id))
    made = db.info["tx"].get(("offers", a.conversation_id), {}).get("creates", {})
    return a.id in made or hot.states([(a.tenant_id, a.conversation_id, str(a.id))]) == ["offered"]


def edit(db: Connection, a: Row | Offer | UUID, **cols) -> Row | Offer | None:
    """Keep `cols` on offer or stint `a` in whatever state it is: an offer's ring, address, leg or member. An offer
    made in this transaction takes them into its create; one Redis holds, open or ended, keeps only its leg, bound
    once, and the rest for this transaction only."""
    A = T.assignment
    if isinstance(a, Offer) and a.state != "active":
        for k, v in cols.items():
            setattr(a, k, v)
        if a.id not in (w := _writes(db, a))["creates"] and "call_uuid" in cols:
            w["binds"][a.id] = a
        return a
    row = db.execute(update(A).where(A.c.id == _id(a)).values(**cols).returning(*A.c)).first()
    if row and cols.get("role") == "primary":
        _sticks(db, row)
    return row


def dial(db: Connection, conv: Row, agent_id, address: str, leg: str | None = None) -> Row:
    """The agent's stint on its own outbound call, which takes the agent's one call slot (409 without it): offered
    while its leg rings, with no offer event or timer since the dial's own check ends it, or active from the start on
    `leg`. `answer` takes it once the leg answers. On a text conversation it is a secondary stint, since the agent's
    chat stint is the primary, and it still takes a call slot."""
    A, aid = T.assignment, uuid4()
    if not hot.take(db, conv, agent_id, aid, 1, channel="call"):
        raise ApiError(409, "agent_on_call", "the agent is on a call")
    row = {"id": aid, "tenant_id": conv.tenant_id, "conversation_id": conv.id, "agent_id": agent_id, "channel": "call"}
    row |= {"role": "primary" if conv.channel == "call" else "secondary", "via": "outbound", "address": address}
    row["dialed"] = conv.routing.get("lead_to", conv.customer)
    row |= {"state": "active", "call_uuid": leg, "started_at": func.now()} if leg else {"state": "offered"}
    a = db.execute(insert(A).values(**row).returning(*A.c)).one()
    _sticks(db, a, conv)
    return a


def primary_agent(db: Connection, cid: UUID) -> Row | None:
    A = T.assignment
    return db.execute(select(A).where(A.c.conversation_id == cid, A.c.role == "primary", A.c.state == "active")).first()


def queue_of(db: Connection, conv: Row) -> dict | None:
    """The conversation's queue, with the route its enqueue kept in place of the queue's current one."""
    q = conv.queue_id and config.get(db, str(conv.tenant_id), "queue", conv.queue_id)
    return q and q | conv.routing.get("route", {})


def holds(q: dict, listed: list[str] | None, agent: dict) -> bool:
    """Python twin of routing's `_MEMBER`."""
    mine = agent["id"] in (listed or q["agents"])
    return mine or (not listed and not set(agent.get("teams", ())).isdisjoint(q["teams"]))


# When stint x set its sticky agent: an agent's call out as it was dialed, a transfer target once the stint f it came
# from ended (unknown without f), any other primary as it started.
BECAME = (
    "CASE WHEN x.via = 'outbound' THEN x.offered_at WHEN x.via <> 'transfer' THEN x.started_at"
    " WHEN f.ended_at IS NOT NULL THEN greatest(x.started_at, f.ended_at) END"
)
_BECAME = text(
    "SELECT " + BECAME + " FROM assignment x LEFT JOIN assignment f ON f.id = x.from_assignment WHERE x.id = :a"  # noqa: S608
)


def _sticks(db: Connection, a: Row, conv: Row | None = None) -> None:
    """Sent after COMMIT, so a rollback leaves nothing. It carries `BECAME`, the order the rebuild keeps too, so a
    write that lands late never replaces a newer stint's agent."""
    C, tenant = T.conversation, str(a.tenant_id)
    conv = conv or db.execute(select(C).where(C.c.id == a.conversation_id)).one()
    if not a.agent_id or not conv.customer or (conv.channel == "email" and a.via != "outbound"):
        return
    out = config.tenant_settings(db, tenant).outbound_sticky
    out, sticks = out.hours if out.enabled else None, None
    if conv.channel not in ("task", "callback") and (
        a.via == "outbound" or (not conv.queue_id and conv.direction == "outbound")
    ):
        sticks = None if out is None else (a.dialed if a.via == "outbound" else conv.customer, out, None)
    elif conv.queue_id:
        q = queue_of(db, conv)
        doc = q and q.get("sticky") and config.get(db, tenant, "agent", a.agent_id)
        if doc and holds(q, conv.routing.get("agents"), doc):
            sticks = (conv.customer, q["sticky"]["expiry_hours"], q["id"])
    if sticks:
        who, hours, queue_id = sticks
        at = db.execute(_BECAME, {"a": a.id}).scalar()
        after_commit(db, partial(hot.stick, tenant, who, a.agent_id, at, a.id, hours, queue_id, out))


def _misses(db: Connection, a: Row, misses) -> int | None:
    P = T.presence
    return db.execute(update(P).where(P.c.agent_id == a.agent_id).values(misses=misses).returning(P.c.misses)).scalar()


def _offer(db: Connection, a: Row, type_: str, **data) -> None:
    """The agent's desk hears an offer's outcome (a phone or a monitor has none)."""
    if a.agent_id and a.role != "monitor":
        emit_event(db, type_, a.tenant_id, a.conversation_id, {"offer_id": str(a.id), **data}, a.agent_id)


def _accept(db: Connection, a: Row) -> None:
    if a.agent_id:
        _misses(db, a, 0)
    _offer(db, a, "offer.accepted")
    same = (a.via, a.role, a.round)
    rivals = [r for r in rows(db, a.conversation_id, ("offered",)) if (r.via, r.role, r.round) == same]
    for rival in by_agent(rivals if a.via in COMPETE else []):
        move(db, rival, "withdraw", reason="taken")
    if a.role == "primary":
        _sticks(db, a, set_state(db, a.conversation_id, "live"))
    timeouts = config.tenant_settings(db, str(a.tenant_id)).text.session_timeout_sec
    if (a.role == "primary" or a.via == "transfer") and (idle := getattr(timeouts, a.channel, None)):
        job = {"conversation_id": str(a.conversation_id)}
        jobs.schedule(db, "session_timeout", job, delay=idle, key=str(a.conversation_id))
    if a.via == "transfer" and a.channel in TEXT:
        set_marks(db, a.conversation_id, turn=None, waiting=None)


def _miss(db: Connection, a: Row) -> None:
    """A call offer's miss carries its agent's strikes, one more if it rang out. HS's own timer runs ahead of Plivo's
    ring, so the leg of an offer it misses may still ring, or be on the call unheard: its slot waits for `leg_ended`,
    or a minute, past the room check that kicks a leg on the call unheard."""
    rings = a.agent_id and a.channel == "call" and a.role != "monitor"  # a supervisor's own ring is not work it missed
    strike = _misses(db, a, T.presence.c.misses + int(a.reason in RANG_OUT)) if rings else None
    _offer(db, a, "offer.missed", reason=a.reason, **({} if strike is None else {"strike": strike}))
    failed(db, a)
    if a.agent_id and a.call_uuid and a.reason == "timeout":
        job = {k: str(getattr(a, k)) for k in ("tenant_id", "conversation_id", "agent_id")} | {"channel": a.channel}
        jobs.schedule(db, "missed_leg", job, delay=60, key=str(a.id))
    else:
        _free(db, a)


def leg_ended(db: Connection, a: Row) -> None:
    """The leg of missed offer `a` ended: the slot it held is free."""
    if jobs.cancel(db, "missed_leg", str(a.id)):
        _free(db, a)


def _decline(db: Connection, a: Row) -> None:
    _offer(db, a, "offer.rejected")
    failed(db, a)
    _free(db, a)


def _withdraw(db: Connection, a: Row) -> None:
    """The offer is off: its leg is hung up, and its agent's slot is free for other work. A transfer target that went
    offline while it rang failed the transfer; a taken, cancelled or left one did not."""
    _offer(db, a, "offer.withdrawn", reason=a.reason)
    if a.call_uuid:
        payload = {"tenant_id": str(a.tenant_id), "conversation_id": str(a.conversation_id), "call_uuid": a.call_uuid}
        plivo.command(db, "hangup", a.call_uuid, payload)
    if a.reason == "agent_offline":
        failed(db, a)
    _free(db, a)


def failed(db: Connection, a: Row) -> None:
    if (a.via, a.channel) == ("participant", "call"):
        participant(db, a, "call.participant_removed", reason="no_answer")
    stopped = a.state == "withdrawn" and a.reason != "agent_offline"
    if a.transfer_id is None or a.reason == "ring_failed" or stopped:
        return
    legs = (r for r in rows(db, a.conversation_id) if r.transfer_id == a.transfer_id and r.id != a.id)
    if any(r.state == "offered" or r.started_at for r in legs):
        return
    if a.call_uuid:
        transferred(db, a, "initiated")
    transferred(db, a, "failed")


def participant(db: Connection, a: Row, type_: str, alias: str | None = None, **reason) -> None:
    who = {"identifier": _str(a.agent_id) or a.address, "alias": alias}
    data = {"participant_id": str(a.transfer_id or a.id), "participant": who, "role": a.role, **reason}
    emit_event(db, type_, a.tenant_id, a.conversation_id, data, a.agent_id)


def _leave(db: Connection, a: Row) -> None:
    """After a cold transfer to a queue the wrap-up is manual and its slot waits for the room it left to end
    (`room_ended`)."""
    s, held = config.tenant_settings(db, str(a.tenant_id)), a.held_by_room is not None
    mode = "manual" if held else s.wrap_up.mode
    wrap_up_timer(db, a, a.wrap_up_until, {"mode": mode, "extended": 0, "max_extend_sec": s.wrap_up.max_extend_sec})
    if not (s.acw_blocks_routing or held):
        _free(db, a)


def wrap_up_timer(db: Connection, a: Row, deadline, timer: dict) -> None:
    """Announce the agent's wrap-up deadline and arm its close: a call's auto wrap-up submits at the deadline, and any
    other waits for the agent until the safety close. `timer` keeps what an extend needs: mode, extended and
    max_extend_sec."""
    at = deadline if timer["mode"] == "auto" and a.channel == "call" else deadline + SAFETY
    payload = timer | {"conversation_id": str(a.conversation_id), "assignment_id": str(a.id)}
    jobs.schedule(db, "wrap_up_timeout", payload, at=at, key=str(a.id))
    data = {"mode": timer["mode"], "deadline_at": deadline.isoformat()}
    emit_event(db, "conversation.wrap_up_started", a.tenant_id, a.conversation_id, data, a.agent_id)


def _submit(db: Connection, a: Row) -> None:
    tenant, skipped = str(a.tenant_id), a.reason == "skipped"
    d = a.disposition_id and config.get(db, tenant, "disposition", a.disposition_id)
    sub = a.sub_disposition_id and config.get(db, tenant, "disposition", a.sub_disposition_id)
    data = {"disposition_id": _str(a.disposition_id), "disposition": d and d["name"]}
    data |= {"sub_disposition": sub and sub["name"], "note": a.note, "system": skipped or bool(d and d["system"])}
    C = T.conversation
    opened = db.execute(select(C.c.opened_at).where(C.c.id == a.conversation_id)).scalar()
    data |= {"skipped": skipped, "duration_sec": int((a.submitted_at - opened).total_seconds())}
    emit_event(db, "conversation.wrapped_up", a.tenant_id, a.conversation_id, data, a.agent_id)
    _done(db, a)


def _done(db: Connection, a: Row) -> None:
    jobs.cancel(db, "wrap_up_timeout", str(a.id))
    if config.tenant_settings(db, str(a.tenant_id)).acw_blocks_routing or a.held_by_room is not None:
        _free(db, a)
    every = rows(db, a.conversation_id)
    if not any(r.state in ("active", "wrap_up") for r in every):
        set_state(db, a.conversation_id, "closed", took=every)


def _free(db: Connection, a: Row) -> None:
    """The stint gives its agent's slot back once the transaction commits: let routing offer the agent the next
    queued conversation it can take. A ring that ended busy or unregistered marks the agent unreachable instead, under
    the presence lock `_ended` took, and its job offers nothing. A run of that job before the give settles the slot
    itself."""
    if a.agent_id:
        payload = {"tenant_id": str(a.tenant_id), "agent_id": str(a.agent_id)}
        if a.reason in BUSY_OR_UNREGISTERED:
            db.execute(update(T.presence).where(T.presence.c.agent_id == a.agent_id).values(unreachable=True))
            payload["unreachable"] = True
        jobs.schedule(db, "route_agent", payload, key=str(a.agent_id))
        hot.give(db, a)


# Assignment: event -> (from states, to state, the time column it stamps, its effects).
ASSIGNMENT = {
    "accept": (("offered",), "active", "started_at", _accept),
    "answer": (("offered",), "active", "started_at", lambda db, a: None),
    "decline": (("offered",), "declined", "ended_at", _decline),
    "miss": (("offered",), "missed", "ended_at", _miss),
    "withdraw": (("offered",), "withdrawn", "ended_at", _withdraw),
    "leave": (("active",), "wrap_up", "ended_at", _leave),
    "end": (("active",), "done", "ended_at", _free),
    "submit": (("wrap_up",), "done", "submitted_at", _submit),
    "fold": (("wrap_up",), "done", None, _done),
    "extend": (("wrap_up",), "wrap_up", None, lambda db, a: None),
}


_SWITCHED = text(
    "SELECT EXISTS (SELECT 1 FROM conversation c, assignment t JOIN conversation s ON s.id = t.conversation_id"
    " WHERE c.id = :c AND c.direction = 'inbound' AND t.from_assignment = :a AND s.channel <> 'call'"
    " AND s.state = ANY(CAST(:live AS text[])))"
)


def move_out(db: Connection, a: Row, reason: str) -> Row | None:
    """Take a row off the conversation: an offer is withdrawn; a primary or a participant agent leaves to wrap-up;
    a consult, a phone or a monitor ends, and so does a primary that hangs up an inbound call it moved to a text
    conversation still live, since the conversation goes on there."""
    if a.state == "offered":
        return move(db, a, "withdraw", reason=reason)
    wraps = a.agent_id and (a.role == "primary" or (a.role == "secondary" and a.via == "participant"))
    hung_up = wraps and (a.role, a.channel, reason) == ("primary", "call", "left")
    if hung_up and db.execute(_SWITCHED, {"c": a.conversation_id, "a": a.id, "live": list(LIVE)}).scalar():
        wraps, reason = False, "switched"
    return move(db, a, "leave" if wraps else "end", reason=reason)


def by_agent(rows, agent=attrgetter("agent_id")) -> list:
    """Rows in agent-id order, `agent` reading each one's agent. Each frees its agent's shared `route_agent` job row,
    so two conversations closing at once lock those rows in one order and cannot deadlock."""
    return sorted(rows, key=lambda r: str(agent(r)))


def kick_agent(db: Connection, conv: Row, a: Row) -> None:
    if a.member_id:
        payload = {"tenant_id": str(conv.tenant_id), "conversation_id": str(conv.id), "room": conv.room}
        plivo.command(db, "kick", str(a.id), payload | {"member_id": a.member_id})


def promote(db: Connection, conv: Row, t: Row | UUID) -> Row | None:
    """Make transfer target `t` the primary: an active consult, or an offered cold or team target. It must still
    come from the current primary, so a duplicate or late complete is a no-op. The old primary leaves first, as the
    one-active-primary index requires, and its member is kicked."""
    t, primary = next((a for a in rows(db, conv.id) if a.id == _id(t)), None), primary_agent(db, conv.id)
    consult = t and ((t.state, t.role) == ("active", "secondary") or (t.state, t.via) == ("offered", "transfer"))
    if not (consult and primary and t.from_assignment == primary.id):
        return None
    if t.state == "offered":
        transferred(db, t, "accepted", t.agent_id)
    if t.state == "offered" and t.mode == "warm":
        return move(db, t, "accept")
    transferred(db, t, "completed", t.agent_id)
    if t.state == "offered":
        t = move(db, t, "accept")
    move_out(db, primary, "transferred")
    kick_agent(db, conv, primary)
    return edit(db, t, role="primary")


def room_ended(db: Connection, conv: Row, room: int) -> None:
    """Room `room`, which the caller left, ended: the wrap-ups it held give their slots back, unless wrap-up blocks
    routing."""
    A = T.assignment
    held = (A.c.conversation_id == conv.id, A.c.state == "wrap_up", A.c.held_by_room == room)
    rows = db.execute(update(A).where(*held).values(held_by_room=None).returning(*A.c)).all()
    if rows and not config.tenant_settings(db, str(conv.tenant_id)).acw_blocks_routing:
        for a in by_agent(rows):
            _free(db, a)


def on_call(conv: Row, every: list[Row]) -> Row | None:
    """The stint among the conversation's rows `every` a call's room end is told to: its newest primary agent stint
    still on the call, or in wrap-up from the caller's current room or gone on from it to a text conversation, unless
    a room's end was told to it already. A phone's, a participant's or a supervisor's stint is never it."""
    seat, told = conv.queued_at or conv.opened_at, conv.routing.get("told", ())

    def here(a: Row) -> bool:
        since = a.ended_at is not None and a.ended_at > seat
        return a.state == "active" or (since and (a.state == "wrap_up" or a.reason == "switched"))

    mine = [a for a in every if a.role == "primary" and a.agent_id and str(a.id) not in told and here(a)]
    return max(mine, key=lambda a: (a.offered_at, str(a.id)), default=None)


def call_ended(db: Connection, conv: Row, hangup: dict, agent_id, result, parties=None, written=False) -> None:
    """call.ended, and the conversation's ended_by unless the caller's own update `written` it."""
    ended = hangup.pop("ended_at", None)
    end = datetime.fromisoformat(ended) if ended else datetime.now(UTC)
    talk = conv.answered_at and max(0, int((end - conv.answered_at).total_seconds()))
    talk = min(talk, hangup["duration_sec"]) if talk and hangup["duration_sec"] is not None else talk
    parties = parties or ((conv.customer, conv.ours) if conv.direction == "inbound" else (conv.ours, conv.customer))
    data = {"from": parties[0], "to": parties[1], "queue_id": _str(conv.queue_id), "talk_sec": talk}
    data["result"] = result or ("answered" if conv.answered_at else "abandoned")
    if not written:
        db.execute(update(T.conversation).where(T.conversation.c.id == conv.id).values(ended_by=hangup["ended_by"]))
    emit_event(db, "call.ended", conv.tenant_id, conv.id, data | hangup, agent_id)


def depart(db: Connection, conv: Row, action: dict, by: str = "system", transfer: dict | None = None) -> Row | None:
    """The customer leaves the room for `action` (a menu, voicemail, a redirect, a number): the room generation
    moves on, so the old room's callbacks are no-ops but its end, its rows come off and the customer is redirected.
    The room's end is told as call.ended, ended `by` whoever moved it, to the agent on the call if there is one."""
    C, every = T.conversation, rows(db, conv.id)
    on, by = on_call(conv, every), "agent" if access.hung_up(conv) else by
    told = {"routing": C.c.routing.concat({"told": [str(on.id)]}), "ended_by": by} if on else {}
    moved = set_state(db, conv, "open", room=C.c.room + 1, **told)
    if moved is None:
        return None
    if on:
        hangup = {"hangup_cause": None, "hangup_cause_code": None, "duration_sec": None, "ended_by": by}
        call_ended(db, conv, hangup, on.agent_id, conv.result, written=True)
    for a in by_agent([a for a in every if a.state in ON]):
        move_out(db, a, "caller_left")
        if a.state == "active":
            kick_agent(db, conv, a)
    payload = {"tenant_id": str(conv.tenant_id), "conversation_id": str(conv.id), "room": moved.room}
    payload |= {"call_uuid": conv.call_uuid, "action": action, "transfer": transfer}
    plivo.command(db, "redirect", f"{conv.id}:{moved.room}", payload)
    return moved


def gone(db: Connection, conv: Row, result=None, every: list[Row] | None = None, why="caller_left", **cols):
    """The customer is gone, or `why` says otherwise: offers are withdrawn and stints end; the conversation waits in
    wrap-up for its agents, or closes now if none is left. Without `result` it is `answered` once an agent took it,
    else `abandoned`. `every` is its rows if the caller read them; `cols` go with the move to wrap-up."""
    every = rows(db, conv.id) if every is None else every
    out = {a.id: move_out(db, a, why) or a for a in by_agent([a for a in every if a.state in ON])}
    after = [out.get(a.id, a) for a in every]
    busy = any(a.state in ("active", "wrap_up") for a in after)
    if set_state(db, conv, "wrap_up", result=result, **cols) and not busy:
        set_state(db, conv, "closed", took=after)


def entry(db: Connection, conv: Row, kind: str, body: dict, *, agent_id=None, provider_id=None, status=None, at=None):
    """Add an entry, at `at` when that is past; None when `provider_id` was seen before in the conversation."""
    E = T.entry
    values = {"tenant_id": conv.tenant_id, "conversation_id": conv.id, "kind": kind, "agent_id": agent_id}
    values |= {"body": body, "provider_id": provider_id, "status": status, "at": func.least(at, func.now())}
    row = db.execute(insert(E).values(**values).on_conflict_do_nothing().returning(*E.c)).first()
    if row:
        touched(db, conv)
        inbox.wrote(db, conv, row)
        if kind == "message" and conv.channel in TEXT:
            _said(db, conv, body.get("direction") == "inbound")
    return row


def set_marks(db: Connection, cid, *where, **marks) -> None:
    C = T.conversation
    db.execute(update(C).where(C.c.id == cid, *where).values(routing=C.c.routing.concat(marks)))


def _said(db: Connection, conv: Row, customer: bool) -> None:
    """Other channels hand the customer the turn on delivery (`reached`), not on send."""
    marks = {"turn": None} if customer else {"turn": "customer"} if conv.channel == "chat" else {}
    if marked := conv.routing.get("waiting"):
        returned = customer and conv.channel == "chat"
        if not returned or db.execute(select(func.now())).scalar() < datetime.fromisoformat(marked):
            marks["waiting"] = None
            if returned:
                arm(db, conv)
    if changed := {k: v for k, v in marks.items() if conv.routing.get(k) != v}:
        set_marks(db, conv.id, **changed)


def reached(db: Connection, cid) -> None:
    C = T.conversation
    theirs = C.c.routing.contains({"turn": "customer"})
    set_marks(db, cid, C.c.state == "live", C.c.channel.in_(TEXT), ~theirs, turn="customer")


def waiting(conv: Row) -> bool:
    """`waiting` as a read shows it."""
    return conv.state == "live" and conv.routing.get("waiting") is not None


def wait(db: Connection, conv: Row, on: bool) -> None:
    if conv.channel not in TEXT:
        raise ApiError(409, "not_text", "only a chat, SMS, WhatsApp or email conversation waits")
    if conv.state != "live":
        raise ApiError(409, "not_live", "only a live conversation waits")
    if not on:
        arm(db, conv)
    until = db.execute(select(func.now() + CHAT_WAITING_WINDOW)).scalar().isoformat() if on else None
    set_marks(db, conv.id, waiting=until)


def arm(db: Connection, conv: Row, **payload) -> None:
    delay = getattr(config.tenant_settings(db, conv.tenant_id).text.session_timeout_sec, conv.channel)
    jobs.schedule(db, "session_timeout", {"conversation_id": str(conv.id)} | payload, delay=delay, key=str(conv.id))


def touched(db: Connection, conv: Row) -> None:
    """A closed conversation changed: move its search version and index it again."""
    if conv.state == "closed":
        C = T.conversation
        db.execute(update(C).where(C.c.id == conv.id).values(version=C.c.version + 1, updated_at=func.now()))
        payload = {"tenant_id": str(conv.tenant_id), "conversation_id": str(conv.id)}
        jobs.schedule(db, "search_index", payload, key=str(conv.id))


@jobs.handler("wrap_up_timeout")
def _wrap_up_timeout(job: Row) -> None:
    """The wrap-up deadline passed: submit the call result's system disposition, as skipped. An extend that
    committed after this claim rescheduled the row with a new rev, so this run is stale and does nothing."""
    J = T.jobs
    with tx() as db:
        conv = lock(db, UUID(job.payload["conversation_id"]))
        if conv and db.execute(select(J.c.rev).where(J.c.kind == job.kind, J.c.key == job.key)).scalar() == job.rev:
            mapped = _mapped(db, conv, conv.result or "answered")
            move(db, UUID(job.payload["assignment_id"]), "submit", disposition_id=mapped, reason="skipped")


@jobs.handler("missed_leg")
def _missed_leg(job: Row) -> None:
    with tx() as db:
        _free(db, Offer(id=job.key, **job.payload))


@jobs.handler("call_session")
def _call_session(job: Row) -> None:
    """The tenant's call session timeout after the call opened, HS has still not heard it end: its caller is gone."""
    with tx() as db:
        if (conv := lock(db, UUID(job.payload["conversation_id"]))) and conv.state in LIVE:
            gone(db, conv)


# These modules import this one's names as they load, so they load once it is complete.
from hs.conversations.api import router as router  # noqa: E402
from hs.conversations.read_model import _dispositions, _mapped, _media  # noqa: E402
from hs.conversations.read_model import note as note  # noqa: E402
from hs.conversations.read_model import summary as summary  # noqa: E402
from hs.conversations.read_model import view as view  # noqa: E402
from hs.conversations.store import lock as lock  # noqa: E402
from hs.conversations.store import rows as rows  # noqa: E402
from hs.conversations.transfers import end_transfers as end_transfers  # noqa: E402
from hs.conversations.transfers import open_transfer as open_transfer  # noqa: E402
from hs.conversations.transfers import transferred as transferred  # noqa: E402
