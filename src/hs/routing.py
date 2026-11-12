"""Queues, offer rounds, capacity and reverse routing.

A queued conversation is offered in rounds: rank the agents it may go to, skipping those `hot` counts full,
share-lock their presence rows in agent order to recheck their status, and offer in rank order while each takes a
slot. The rounds, the agents they tried, each agent's slots and each customer's sticky agents are kept in `hot`; the
strategies' counters are read from assignment rows. An agent whose status or load changes pulls the next queued
conversation it can take (`route_agent`).

What the channels call: `enqueue` to queue a conversation (it raises `Full`), `reoffer` after every decline or miss,
`overflowed` when a conversation leaves its queue unserved, and `CHANNELS[channel] = Channel(open, ring)` to
register."""

from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from itertools import islice
from operator import itemgetter
from typing import Any
from uuid import UUID, uuid4

from fastapi import APIRouter, Response
from sqlalchemy import func, select, text, update
from sqlalchemy.engine import Connection, Row

from hs import access, agents, config, conversations, events, hot, jobs
from hs.access import Principal
from hs.config.models import Route
from hs.core import ApiError, T, read, tx

router = APIRouter()
AGENT = access.require("agent")
RESULTS = {"wait_timeout": "queue_timeout", "dtmf": "queue_exit"}  # overflow reason -> result, where they differ
_WAITS = ("call", "email")  # channels a wait for nobody never spends

_FROM = (
    " FROM conversation c JOIN config q ON q.id = c.queue_id AND NOT q.body ? 'deleted'"
    " JOIN config a ON a.tenant_id = c.tenant_id AND a.kind = 'agent'"
    " JOIN presence p ON p.agent_id = a.id JOIN config s ON s.id = p.status_id"
)
# A field of the route conversation `c` is offered by, kept at its enqueue; one queued before routes were kept reads
# its queue's.
_ROUTE = "coalesce(c.routing->'route'->'{0}', q.body->'{0}')"
# Whom it is offered to: an enqueue's own agent list replaces the route's agents and teams.
_MEMBER = (
    " CASE WHEN c.routing ? 'agents' THEN c.routing->'agents' ? a.id::text ELSE " + _ROUTE.format("agents") + " ?"
    " a.id::text OR " + _ROUTE.format("teams") + " ?| ARRAY(SELECT jsonb_array_elements_text(a.body->'teams')) END"
)
# Who could ever take conversation `c`, whatever their status and load; shared by forward and reverse routing.
_CAN = (
    _MEMBER + " AND (NOT c.routing ? 'strict' OR c.routing->>'sticky' = a.id::text)"  # noqa: S608
    " AND (c.routing->>'sticky' = a.id::text OR " + _ROUTE.format("skills") + " = '[]' OR EXISTS (SELECT 1"
    "  FROM jsonb_array_elements(" + _ROUTE.format("skills") + ") n"
    "  JOIN jsonb_array_elements(a.body->'skills') h ON h->>'skill_id' = n->>'skill_id'"
    "  CROSS JOIN LATERAL (SELECT (h->>'level')::int AS has, (n->>'level')::int AS needs) v"
    "  WHERE CASE n->>'operator' WHEN '' THEN true WHEN '=' THEN has = needs WHEN '!=' THEN has <> needs"
    "   WHEN '<>' THEN has <> needs WHEN '<' THEN has < needs WHEN '<=' THEN has <= needs WHEN '>' THEN has > needs"
    "   WHEN '>=' THEN has >= needs END))"
)
# Who may take it now: available, and not full on its channel (`hot.full`).
_ELIGIBLE = (
    " (s.body->>'availability')::boolean AND" + _CAN + " AND c.channel || ':' || a.id <> ALL(CAST(:full AS text[]))"
)
# A text or task conversation counts, since the desk signed in, for the agent that wrapped it up as its last primary.
# Ties, and availability, go to the agent available longest: from its first available time or last counted wrap-up.
_DONE = (
    "x.role = 'primary' AND x.channel <> 'call' AND x.wrap_up_until IS NOT NULL"
    " AND x.reason IS DISTINCT FROM 'transferred'"
)
_COUNTED = " FILTER (WHERE " + _DONE + " AND x.ended_at > coalesce(p.signed_in_at, p.since))"
_RANK = text(
    "SELECT a.id" + _FROM + " CROSS JOIN LATERAL (SELECT max(x.ended_at) AS last,"  # noqa: S608
    " max(x.ended_at) FILTER (WHERE " + _DONE + ") AS done, count(*)" + _COUNTED + " AS handled,"
    # A transferred turn is timed from the transfer.
    " coalesce(extract(epoch FROM sum(x.ended_at - CASE WHEN x.transfer_id IS NULL THEN x.started_at ELSE x.offered_at END)"
    + _COUNTED
    + "), 0) AS secs FROM assignment x WHERE x.tenant_id = p.tenant_id AND x.agent_id = a.id"
    "  AND x.ended_at > least(p.signed_in_at, p.since, p.first_available_at) AND x.started_at IS NOT NULL) h"
    " WHERE c.id = :c AND" + _ELIGIBLE + " AND a.id <> ALL(CAST(:skip AS uuid[]))"
    " AND (CAST(:ids AS uuid[]) IS NULL OR a.id = ANY(CAST(:ids AS uuid[])))"
    " ORDER BY CASE coalesce(c.routing->'route'->>'strategy', q.body->>'strategy')"
    " WHEN 'fewest_handled' THEN h.handled"
    " WHEN 'least_handle_time' THEN h.secs ELSE 0 END, CASE WHEN p.first_available_at IS NULL"
    " THEN greatest(p.since, h.last) ELSE greatest(p.first_available_at, h.done) END, a.id LIMIT :n"
)
# These agents' presence rows and whether each is available once locked: a status change in flight is waited for,
# and a delete held off.
_LOCK = text(
    "SELECT p.agent_id, (s.body->>'availability')::boolean AS free FROM presence p JOIN config s ON s.id = p.status_id"
    " WHERE p.agent_id = ANY(CAST(:ids AS uuid[])) ORDER BY p.agent_id FOR SHARE OF p"
)
# True when every agent who could ever take `c`, whatever their status and load, was offered it in this stay and no
# queue_timeout job will end its wait.
_SPENT = text(
    "SELECT NOT EXISTS (SELECT 1" + _FROM + " WHERE c.id = :c AND" + _CAN + " AND a.id <> ALL(CAST(:skip AS uuid[])))"  # noqa: S608
    " AND NOT EXISTS (SELECT 1 FROM jobs WHERE kind = 'queue_timeout' AND key = CAST(:c AS text))"
)
# The queued conversations the agent may take now, but none with an open offer. An open offer has neither started_at
# nor ended_at: testing the stamps instead of the state keeps the planner off the partial capacity index, which it
# misreads after an ANALYZE of a near-empty table.
_WAITING = (
    _FROM + " WHERE a.id = :agent AND c.state = 'queued' AND" + _ELIGIBLE + " AND c.id <> ALL(CAST(:seen AS uuid[]))"  # noqa: S608
    " AND NOT EXISTS (SELECT 1 FROM jobs o WHERE o.kind = 'offer_timeout' AND o.payload->>'conversation_id' = c.id::text)"
    " AND NOT p.unreachable"
)
_PICK = text(
    "SELECT c.id" + _WAITING + " ORDER BY q.body->>'dequeue_order' = 'lifo', c.priority,"
    " CASE WHEN q.body->>'dequeue_order' = 'lifo' THEN c.queued_at END DESC, c.queued_at LIMIT 10"
)
# The first of them in that order that no other transaction holds, as a callback does.
_TAKE = text(
    "SELECT id FROM conversation WHERE id = ANY(CAST(:ids AS uuid[])) AND state = 'queued'"
    " ORDER BY array_position(CAST(:ids AS uuid[]), id) LIMIT 1 FOR NO KEY UPDATE SKIP LOCKED"
)
_AWAY = text(
    "SELECT 1 FROM presence p JOIN config s ON s.id = p.status_id"
    " WHERE p.agent_id = :agent AND NOT (s.body->>'availability')::boolean"
)
# A rebuilt tenant's conversations with an offer timer, each due a round that retires its lost offers' timers, and
# those whose round the unrebuilt keys refused, due again at once instead of after its backoff; the rest of its
# queued ones wait for the route_agent runs of its signed-in agents.
_STRANDED = text(
    "SELECT payload->>'conversation_id' FROM jobs WHERE kind = 'offer_timeout' AND payload->>'tenant_id' = ANY(:t)"
    " UNION SELECT j.key FROM jobs j JOIN conversation c ON c.id = CAST(j.key AS uuid)"
    " WHERE j.kind = 'next_round' AND c.tenant_id = ANY(CAST(:t AS uuid[]))"
)
_SPENDABLE = text(
    "SELECT c.id::text FROM conversation c JOIN tenant t ON t.id = c.tenant_id AND t.deleted_at IS NULL"
    " WHERE c.tenant_id = :t AND c.state = 'queued' AND c.channel <> ALL(CAST(:waits AS text[]))"
    " AND EXISTS (SELECT 1 FROM events e WHERE e.conversation_id = c.id::text AND e.type = 'offer.created')"
    " AND NOT EXISTS (SELECT 1 FROM jobs j WHERE j.kind = 'queue_timeout' AND j.key = c.id::text)"
)
# A waiting call's tried agents lapse `ms` after its last offer, so its run starts at the last offer that came more than
# `ms` after the one before it, and the rebuild keeps only that run's declines and misses.
_TRIED = text(
    "SELECT c.tenant_id::text AS tenant, c.id::text AS id, c.channel, c.routing->>'stint' AS stint, s.tried,"
    " CASE WHEN c.channel = 'call' THEN CAST(ceil(:ms - extract(epoch FROM now() - s.last) * 1000) AS bigint)"
    " END AS ms FROM conversation c CROSS JOIN LATERAL (SELECT"
    "  max(e.created_at) FILTER (WHERE e.type = 'offer.created') AS last,"
    "  jsonb_object_agg(e.agent_id, e.data->>'offer_id')"
    "   FILTER (WHERE e.type <> 'offer.created' AND e.id > coalesce(e.run, 0)) AS tried"
    "  FROM (SELECT e.*, max(e.id) FILTER (WHERE e.type = 'conversation.queued') OVER () AS stay,"
    "   max(e.id) FILTER (WHERE c.channel = 'call' AND e.type = 'offer.created'"
    "    AND e.created_at - e.before > :ms * interval '1 millisecond') OVER () AS run"
    "   FROM (SELECT e.*, lag(e.created_at) OVER (PARTITION BY e.type ORDER BY e.id) AS before FROM events e"
    "    WHERE e.conversation_id = c.id::text"
    "    AND e.type IN ('conversation.queued', 'offer.created', 'offer.rejected', 'offer.missed')) e) e"
    "  WHERE e.id > coalesce(e.stay, 0)) s"
    " WHERE c.tenant_id = ANY(CAST(:t AS uuid[])) AND c.state = 'queued' AND s.tried IS NOT NULL"
    " AND (c.channel <> 'call' OR s.last > now() - :ms * interval '1 millisecond')"
).execution_options(stream_results=True)
# The conversations in `ids` no writer holds, share-locked until the transaction commits.
_SHARE = text("SELECT id::text FROM conversation WHERE id = ANY(CAST(:ids AS uuid[])) FOR SHARE SKIP LOCKED")
PAGE = 500
_CUSTOMERS = text(
    "SELECT DISTINCT customer FROM conversation WHERE tenant_id = :t AND customer > :after ORDER BY customer LIMIT :n"
)
# One row per sticky key: its newest stint, kept only if it ran after the last enqueue for a queue's key. An agent's
# outbound, or a primary on an outbound contact before any enqueue, keys the outbound agent (`k` null) of the number it
# dialed. It names its agent only if it also ran after the agent and queue edits (a queue edit leaves an enqueue's own
# route alone); without one it still ends an older outbound agent. A transfer target with no source stint has an
# unknown time, sorts at its own end, and sets nothing.
_EDITS = " v.became > a.updated_at AND (c.routing ? 'route' OR v.became > q.updated_at)"
_OUT = (
    "c.channel NOT IN ('task', 'callback') AND (x.via = 'outbound'"
    " OR c.direction = 'outbound' AND x.role = 'primary' AND coalesce(v.became < c.queued_at, true))"
)
_STUCK = text(
    "SELECT k, who, agent_id, became, id, hours FROM (SELECT DISTINCT ON (o.who, o.k) o.who, o.k,"  # noqa: S608
    " CASE WHEN o.k IS NULL OR" + _EDITS + " THEN x.agent_id END AS agent_id, x.id, v.became,"
    " o.k IS NULL OR v.became >= c.queued_at AS wrote,"
    " (" + _ROUTE.format("sticky") + "->>'expiry_hours')::int AS hours"
    " FROM conversation c JOIN assignment x ON x.conversation_id = c.id"
    " LEFT JOIN assignment f ON f.id = x.from_assignment"
    " CROSS JOIN LATERAL (SELECT " + conversations.BECAME + " AS became) v"
    " CROSS JOIN LATERAL (SELECT CASE WHEN NOT (" + _OUT + ") THEN c.queue_id END AS k, CASE WHEN x.via = 'outbound'"
    "  AND c.channel NOT IN ('task', 'callback') THEN coalesce(x.dialed, c.routing->>'lead_to', c.customer)"
    "  ELSE c.customer END"
    "  AS who) o"
    " LEFT JOIN config q ON q.id = c.queue_id LEFT JOIN config a ON a.id = x.agent_id"
    " WHERE c.tenant_id = :t AND c.customer = ANY(CAST(:page AS text[]))"
    " AND (c.channel <> 'email' OR x.via = 'outbound') AND x.agent_id IS NOT NULL AND ("
    + _OUT
    + " OR c.queue_id IS NOT NULL"
    " AND (x.role = 'primary' AND x.started_at IS NOT NULL OR x.via = 'outbound')"
    " AND (coalesce(v.became < c.queued_at, true) OR jsonb_typeof(" + _ROUTE.format("sticky") + ") = 'object'"
    " AND NOT coalesce(" + _EDITS + " AND NOT" + _MEMBER + ", false)))"
    " ORDER BY o.who, o.k, coalesce(v.became, x.ended_at) DESC NULLS FIRST, x.id DESC) s"
    " WHERE wrote AND became IS NOT NULL"
)


@dataclass(frozen=True)
class Channel:
    """`open(db, p, body)` takes POST /queues/{id}/enqueue's raw JSON body for this channel, validates it with the
    channel's own model (a RequestValidationError is the route's 422), and returns the locked conversation the body
    joins or opens with the validated body, whose `agents` and `route` the route enqueues it by while it is still
    `open`. Each channel the route takes appends its body's JSON schema to `BODIES`. `ring(db, conv, offers)` gets
    each round's new offers. `ended(db, conv, a)` hears how offer `a` ended, when routing hears it: a text or task
    accept (`a` active), every decline or miss, and a withdrawal from an agent gone unavailable; `conv` is the
    conversation as it was before. `unserved(db, conv)` takes a conversation its queue gave up on, closing it unless
    the channel says otherwise. `queued(db, conv)` hears each enqueue of the conversation."""

    open: Callable[[Connection, Principal, dict], tuple[Row, Any]]
    ring: Callable[[Connection, Row, list[Row]], None]
    ended: Callable[[Connection, Row, Row], None] = lambda db, conv, a: None
    unserved: Callable[[Connection, Row], None] = conversations.gone
    queued: Callable[[Connection, Row], None] = lambda db, conv: None


CHANNELS: dict[str, Channel] = {}
# The enqueue route documents the union of these schemas; a list, so a channel module loaded after the route still
# shows.
BODIES: list[dict] = []


class Full(ApiError):
    """409 enqueue_failed: the queue is disabled, or holds as many conversations as its capacity. `enqueue` raises it
    after emitting conversation.overflowed; a caller that keeps the contact catches it inside its transaction so the
    event commits, then renders `action`, the queue's overflow action, closes the contact, or answers the 409."""

    def __init__(self, action: dict | None, message: str):
        super().__init__(409, "enqueue_failed", message)
        self.action = action


def _sticky(db: Connection, conv: Row, q: dict, listed: list[str]) -> dict:
    """None for an agent the queue no longer routes to; a deleted one is forgotten."""
    tenant = str(conv.tenant_id)
    if not (sticky := q["sticky"]) or conv.channel == "email":
        return {}
    outbound = config.tenant_settings(db, tenant).outbound_sticky.enabled
    if (found := hot.stuck(tenant, q["id"], conv.customer, outbound)) is None:
        return {}
    key, value, agent_id = found
    if (agent := config.get(db, tenant, "agent", agent_id)) is None:
        hot.unstick(key, value)
    if not (agent and conversations.holds(q, listed, agent)):
        return {}
    return {"sticky": agent_id} | ({"strict": True} if sticky["mode"] == "strict" else {})


KEPT = ("told", "caller_id", "left_at")  # facts about the call that a queue's fresh routing does not forget


def enqueue(db: Connection, conv: Row, queue_id: UUID, *, agents=(), own=None, priority=None, hold=False) -> None:
    """Queue the conversation and run its first round; the caller holds its lock. The queue's row lock admits one of
    two enqueues racing for its last place: a disabled or full queue tells of the overflow and raises Full, but
    waiting emails and callbacks never fill it. It keeps its route, `own` or else the queue's as it is now, so a queue
    edit reaches only later contacts. `agents`, if any, are the only agents it is offered. `hold` keeps the place in
    line of a contact no agent answered yet."""
    C, tenant = T.conversation, str(conv.tenant_id)
    if (q := config.get(db, tenant, "queue", queue_id, lock=True)) is None:
        raise ApiError(404, "queue_not_found", "no such queue")
    waiting = (C.c.tenant_id == conv.tenant_id, C.c.queue_id == q["id"], C.c.channel == conv.channel)
    counted = conv.channel not in ("email", "callback")
    full = counted and db.execute(select(func.count()).where(*waiting, C.c.state == "queued")).scalar() >= q["capacity"]
    if full or not q.get("enabled", True):
        overflowed(db, conv, q["id"], "enqueue_failed", q["overflow_action"])
        raise Full(q["overflow_action"], "the queue is full" if full else "the queue is disabled")
    listed = [str(a) for a in agents]
    kept = own.model_dump(mode="json") if own else {k: q[k] for k in Route.model_fields}
    routing = {"stint": uuid4().hex, "route": kept} | ({"agents": listed} if listed else {})
    routing |= _sticky(db, conv, q | kept, listed) | {"queue_teams": [] if agents else kept["teams"]}
    routing |= {k: conv.routing[k] for k in KEPT if k in conv.routing}
    since = C.c.queued_at if hold and conv.queued_at and conv.answered_at is None else func.now()
    stint = {"queue_id": q["id"], "queued_at": since, "priority": priority or q["priority"]}
    if (queued := conversations.set_state(db, conv, "queued", **stint, routing=routing, result=None)) is None:
        raise ApiError(409, "enqueue_failed", "the conversation cannot join a queue now")
    conversations.end_transfers(db, conv)
    data = {"queue_id": q["id"], "from": conv.customer, "to": conv.ours or None}
    events.emit(db, "conversation.queued", tenant, data, conversation_id=str(conv.id))
    CHANNELS[conv.channel].queued(db, queued)
    jobs.cancel(db, "queue_timeout", str(conv.id))
    wait = q["wait_timeout_sec"] if conv.channel == "call" else q["text"]["wait_timeout_sec"]
    if wait := wait.get(conv.channel, 0) if isinstance(wait, dict) else wait:
        timer = {"tenant_id": tenant, "conversation_id": str(conv.id)}
        jobs.schedule(db, "queue_timeout", timer, delay=wait, key=str(conv.id))
    route(db, queued, fresh=True)


def route(db: Connection, conv: Row, *, only: str | None = None, fresh: bool = False) -> list[Row]:
    """One offer round for a queued conversation; returns its offers. Nothing while an offer is still out. The first
    round offers the sticky agent alone, and a strict contact fails once that agent cannot take it or lets the offer
    end. Its rounds are spent after `1 + retries`, and a text's or task's also once it waits for nobody: every agent who
    could take it was offered it, and it has no wait timeout. Spent, it overflows as assignment_failed, an email
    excepted. No round offers it to an agent it was offered in this queue stay, or for a call in the 300 s since its
    last offer; with no agent left to offer, it waits for one, or for its wait timeout. `only` is route_agent's agent:
    one offer to it unless it was tried, which its pick may not know yet. `fresh`, at an enqueue, starts the rounds
    over."""
    if conv.state != "queued" or (q := conversations.queue_of(db, conv)) is None:
        return []
    lost: list[dict] = []
    stints = conversations.rows(db, conv.id, lost=lost)
    retired = conversations.retire_lost(db, conv, lost)
    was = hot.rounds(conv)
    r = hot.Rounds(seen=was.seen) if fresh else was
    r = r._replace(tried={a: o for a, o in r.tried.items() if o not in retired})
    r, offers = (r, []) if any(x.state == "offered" for x in stints) else _round(db, conv, q, r, only)
    if fresh or r != was:
        hot.keep_rounds(db, conv, r)
    return offers


def _round(db: Connection, conv: Row, q: dict, r: hot.Rounds, only: str | None) -> tuple[hot.Rounds, list[Row]]:
    """The round's offers, and the rounds once it is made."""
    strict = conv.routing.get("strict")
    if conv.routing.get("failed") or (strict and r.round) or (r.round >= 1 + q["retries"] and conv.channel != "email"):
        _overflow(db, conv, q, "assignment_failed")
        return r, []
    params = {"c": conv.id, "skip": list(r.tried), "full": hot.full(conv.tenant_id, conv.channel)}
    first, n = only or (not r.round and conv.routing.get("sticky")), 0 if only or strict else q["agents_at_once"]
    made = (conversations.offer(db, conv, agent) for agent in _select(db, params, first, n))
    if not (offers := list(islice(filter(None, made), n or 1))):
        waits = only or conv.channel in _WAITS or not r.tried
        if strict and not only:
            _assignment_failed(db, conv)
        elif not waits and db.execute(_SPENT, params).scalar():
            _overflow(db, conv, q, "assignment_failed")
        elif conv.channel == "call" and r.tried and not only:
            lapse = (r.ttl or hot.CALL_TRIED) / 1000
            jobs.schedule(db, "next_round", {"conversation_id": str(conv.id)}, delay=lapse, key=str(conv.id))
        return r, []
    CHANNELS[conv.channel].ring(db, conv, offers)
    tried = r.tried | {str(o.agent_id): str(o.id) for o in offers}
    return r._replace(round=r.round + 1, tried=tried, ttl=hot.CALL_TRIED if conv.channel == "call" else None), offers


# It ranks twice the round's size so a round that loses agents to a concurrent one still fills; one that loses more
# waits for the next route_agent. Rank again after the takes if that is ever seen.
def _select(db: Connection, params: dict, first: str | None, n: int) -> list[UUID]:
    """Agent `first` if it may take the conversation and is still available once the presence rows are
    share-locked, else the first `2n` ranked agents still available, in rank order. All the rows are locked in one
    statement, in agent order."""

    def rank(ids: list | None, k: int) -> list[UUID]:
        return [r.id for r in db.execute(_RANK, params | {"ids": ids, "n": k})] if k else []

    firsts = rank([UUID(first)], 1) if first else []
    if not (ranked := firsts + rank(None, 2 * n)):
        return []
    free = {r.agent_id for r in db.execute(_LOCK, {"ids": ranked}) if r.free}
    return [a for a in firsts if a in free] or [a for a in ranked[len(firsts) :] if a in free]


def overflowed(db: Connection, conv: Row, queue_id, reason: str, action: dict | None) -> None:
    """The conversation leaves queue `queue_id` unserved for `reason`, to `action`: set its result and tell of it."""
    C = T.conversation
    db.execute(update(C).where(C.c.id == conv.id).values(result=RESULTS.get(reason, reason)))
    data = {"queue_id": str(queue_id), "reason": reason, "action": action}
    events.emit(db, "conversation.overflowed", str(conv.tenant_id), data, conversation_id=str(conv.id))


def _assignment_failed(db: Connection, conv: Row) -> None:
    """Overflows in a job of its own: an enqueue still seats the call, and this transaction may hold presence rows."""
    failed = {"tenant_id": str(conv.tenant_id), "conversation_id": str(conv.id), "reason": "assignment_failed"}
    jobs.schedule(db, "queue_timeout", failed, key=str(conv.id))


def _overflow(db: Connection, conv: Row, q: dict, reason: str) -> None:
    """The conversation leaves its queue unserved: a call takes the overflow action (hang up by default), any other
    channel runs its `unserved`."""
    overflowed(db, conv, q["id"], reason, q["overflow_action"])
    if conv.channel == "call":
        conversations.depart(db, conv, q["overflow_action"] or {"type": "hangup"})
    else:
        CHANNELS[conv.channel].unserved(db, conv)


def reoffer(db: Connection, conv: Row, a: Row) -> None:
    """After offer `a` was declined or missed: a ring Plivo refused fails the conversation's assignment, and a missed
    call that brings its agent to the tenant's strike limit (0: none) moves the agent to the RONA status. The next
    round runs in a job of its own, since this transaction holds the agent's presence row and a round takes others."""
    CHANNELS[conv.channel].ended(db, conv, a)
    if a.reason == "ring_failed":
        _assignment_failed(db, conv)
        return
    if a.state == "missed" and a.channel == "call" and a.agent_id:
        P, tenant = T.presence, str(a.tenant_id)
        rona = config.tenant_settings(db, tenant).rona
        if rona.strikes and db.execute(select(P.c.misses).where(P.c.agent_id == a.agent_id)).scalar() >= rona.strikes:
            agents.set_status(db, tenant, a.agent_id, rona.status_id, "rona")
            db.execute(update(P).where(P.c.agent_id == a.agent_id).values(misses=0))
    jobs.schedule(db, "next_round", {"conversation_id": str(conv.id)}, key=str(conv.id))


@jobs.handler("next_round")
def _next_round(job: Row) -> None:
    with tx() as db:
        if conv := conversations.lock(db, UUID(job.payload["conversation_id"])):
            route(db, conv)


@jobs.handler("config_rounds")
def _config_rounds(job: Row) -> None:
    with tx() as db:
        for cid in sorted(db.execute(_SPENDABLE, {"t": job.payload["tenant_id"], "waits": list(_WAITS)}).scalars()):
            jobs.schedule(db, "next_round", {"conversation_id": cid}, key=cid)


jobs.every("sweep", 15)


@jobs.handler("sweep")
def _sweep(job: Row) -> None:
    with read() as db:
        desks = db.execute(agents._DESKS).all()
        live = db.execute(select(T.tenant.c.id).where(T.tenant.c.deleted_at.is_(None))).scalars().all()
    lost = sorted(hot.lost(set(live)), key=str)
    # A transaction rebuilds at most PAGE tenants, as it holds a lock on each to its commit, so a rebuild of any number
    # fits Postgres's shared lock table.
    for batch in [lost[i : i + PAGE] for i in range(0, len(lost) or 1, PAGE)]:
        ts = [str(t) for t in batch]
        with tx() as db:
            if ts:
                db.execute(hot.REBUILDING, {"ts": ts})
            strays: list[str] = []
            held = conversations.held(db, lost=strays)
            for cid in sorted(db.execute(_STRANDED, {"t": ts}).scalars() if ts else ()):
                jobs.schedule(db, "next_round", {"conversation_id": cid}, key=cid)
            for t in ts:
                jobs.schedule(db, "sticky_rebuild", {"tenant_id": t, "after": ""}, key=t)
            tried = db.execute(_TRIED, {"t": ts, "ms": hot.CALL_TRIED}).partitions(PAGE) if ts else ()
            silent, rebuilt = hot.sweep(db, desks, held, set(batch), tried)
            _wake(db, rebuilt)
    odd, room = hot.audit(desks, held, list(CHANNELS))
    with tx() as db:
        for cid in sorted(strays):
            jobs.schedule(db, "next_round", {"conversation_id": cid}, key=cid)
        for tenant_id, agent_id, _ in silent:
            payload = {"tenant_id": str(tenant_id), "agent_id": str(agent_id)}
            jobs.schedule(db, "agent_offline", payload, key=str(agent_id))
        _wake(db, hot.freed(odd, held, room))
    with tx() as db:
        _settle(db, odd, room)


def _wake(db: Connection, agents) -> None:
    for tenant_id, agent_id in conversations.by_agent(agents, itemgetter(1)):
        jobs.schedule(db, "route_agent", {"tenant_id": tenant_id, "agent_id": agent_id}, key=agent_id)


@jobs.handler("sticky_rebuild", lane="slow")
def _sticky_rebuild(job: Row) -> None:
    """A start since the loss wins over the rebuilt agent."""
    t = job.payload["tenant_id"]
    with read() as db:
        page = db.execute(_CUSTOMERS, {"t": t, "after": job.payload["after"], "n": PAGE}).scalars().all()
        found = db.execute(_STUCK, {"t": t, "page": page}).all()
        out = config.tenant_settings(db, t).outbound_sticky
    hot.fill(t, found, out.hours if out.enabled else None)
    if len(page) == PAGE:
        with tx() as db:
            jobs.schedule(db, "sticky_rebuild", {"tenant_id": t, "after": page[-1]}, key=t)


def _settle(db: Connection, odd: list[tuple], room: list[tuple]) -> bool:
    """Settle each slot of `hot.audit`'s `odd` under its conversation's share lock, which skips an offer still being
    made, or once its conversation was `abandoned`, and take `room` out of `full`. Returns whether it skipped a slot."""
    convs = list({k[-1] for k in odd})
    locked = set(db.execute(_SHARE, {"ids": convs}).scalars()) if convs else set()
    done = locked | conversations.abandoned(db, [c for c in convs if c not in locked])
    hot.settle([k for k in odd if k[-1] in done], conversations.held(db, list(locked)) if locked else [], room)
    return len(done) < len(convs)


def _reach(job: Row) -> None:
    """Any change after a ring ended busy or unregistered makes its agent reachable again: each route job clears the
    mark, but the one that ring end scheduled, or one a newer schedule replaced, as that end may have come after.
    The presence row is locked before the job row is read, the order `_ended` takes them in."""
    P, J, agent = T.presence, T.jobs, job.payload["agent_id"]
    if job.payload.get("unreachable"):
        return
    with tx() as db:
        marked = select(P.c.agent_id).where(P.c.agent_id == agent, P.c.unreachable).with_for_update()
        if db.execute(marked).first() and db.execute(select(J.c.rev).where(J.c.id == job.id)).scalar() == job.rev:
            db.execute(update(P).where(P.c.agent_id == agent).values(unreachable=False))


@jobs.handler("offer_timeout")
def _offer_timeout(job: Row) -> None:
    with tx() as db:
        if (conv := conversations.lock(db, UUID(job.payload["conversation_id"]))) is None:
            return
        lost: list[dict] = []
        out = conversations.rows(db, conv.id, ("offered",), lost=lost)
        if a := next((a for a in out if str(a.id) == job.payload["assignment_id"]), None):
            reason = "ring_unconfirmed" if a.channel == "call" and a.call_uuid is None else "timeout"
            if a := conversations.move(db, a, "miss", reason=reason):
                reoffer(db, conv, a)
        elif conversations.retire_lost(db, conv, lost):
            jobs.schedule(db, "next_round", {"conversation_id": str(conv.id)}, key=str(conv.id))


@jobs.handler("queue_timeout")
def _queue_timeout(job: Row) -> None:
    """The conversation waited out its queue's timeout, or failed its assignment (`reason`): its strict sticky round
    or a ring Plivo refused. An enqueue or an answer that committed after this claim replaced or removed the row, so
    this run is stale and does nothing."""
    J = T.jobs
    with tx() as db:
        conv = conversations.lock(db, UUID(job.payload["conversation_id"]))
        current = db.execute(select(J.c.rev).where(J.c.id == job.id)).scalar() == job.rev
        if not (conv and conv.state == "queued" and current):
            return
        if "reason" in job.payload and conversations.rows(db, conv.id, ("offered",)):
            conversations.set_marks(db, conv.id, failed=True)
            return
        deleted = {"id": str(conv.queue_id), "overflow_action": None}
        q = config.get(db, str(conv.tenant_id), "queue", conv.queue_id) or deleted
        _overflow(db, conv, q, job.payload.get("reason", "wait_timeout"))


@jobs.handler("route_agent")
def _route_agent(job: Row) -> None:
    """The agent's status or load changed. Its offers made while it is unavailable are withdrawn and their
    conversations routed on. Then it is offered the queued conversations it can take, one per transaction, until
    none is left or it is full. One whose cycle tried the agent, or that `route` gives it nothing from, as when
    another agent was offered it meanwhile, is passed over; one another transaction holds brings this job back in a
    second. A tried offer that never committed is a dead process's leftover, which `route` clears. `hot` counting the
    agent full is not trusted: with nothing picked, the job ends only while Postgres holds the agent at its cap on each
    such channel. Otherwise it settles the agent's slots against Postgres, as the sweep does, and picks again; a slot
    whose conversation another transaction holds brings this job back in a second. An unreachable agent is offered
    nothing until a later change's job clears its mark (`_reach`)."""
    agent, seen, stale, swept = job.payload["agent_id"], [], [], False
    _reach(job)
    while True:
        for x in stale:
            with tx() as db:
                conv = conversations.lock(db, x.conversation_id)
                away = (r := db.execute(_LOCK, {"ids": [agent]}).first()) and not r.free
                x = next((a for a in conversations.rows(db, conv.id, ("offered",)) if a.id == x.id), None)
                if away and x and (x := conversations.move(db, x, "withdraw", reason="agent_offline")):
                    CHANNELS[conv.channel].ended(db, conv, x)
                    jobs.schedule(db, "next_round", {"conversation_id": str(conv.id)}, key=str(conv.id))
        with tx() as db:
            caps = config.tenant_settings(db, job.payload["tenant_id"], fresh=True).capacity
            full = hot.full(job.payload["tenant_id"], *CHANNELS)
            waiting = db.execute(_PICK, {"agent": agent, "seen": seen, "full": full}).scalars().all()
            # The pick takes only an available agent, so only one it passes over can be away and hold stale offers.
            away = not (waiting or swept) and db.execute(_AWAY, {"agent": agent}).first()
            stale, swept = (conversations.desk(db, job.payload["tenant_id"], agent) if away else []), True
            if stale:
                continue
            if not waiting:
                if not (out := [ch for ch in CHANNELS if f"{ch}:{agent}" in full]):
                    return
                held = conversations.held(db, None, agent)
                load = Counter(ch for ch, _ in {(r.channel, r.conv) for r in held})
                if all(load[ch] >= getattr(caps, ch) for ch in out):
                    return
                if _settle(db, *hot.audit([(job.payload["tenant_id"], agent, True)], held, list(CHANNELS))):
                    jobs.schedule(db, "route_agent", job.payload, delay=1, key=agent, rev=job.rev)
                    return
                seen = []
                continue
            seen += [str(c) for c in hot.tried_by(job.payload["tenant_id"], agent, waiting)]
            if not (untried := [c for c in waiting if str(c) not in seen]):
                continue
            if (cid := db.execute(_TAKE, {"ids": untried}).scalar()) is None:
                jobs.schedule(db, "route_agent", job.payload, delay=1, key=agent, rev=job.rev)
                return
            if not any(str(o.agent_id) == agent for o in route(db, conversations.lock(db, cid), only=agent)):
                seen.append(str(cid))


def _offer(db: Connection, p: Principal, offer_id: UUID) -> tuple[Row, Row | None, Row]:
    """The locked conversation of the agent's own text or task offer, the offer while open, and the agent's row of
    it, open or ended. 404 for an offer the agent never had."""
    mine = conversations.desk(db, p.tenant_id, p.agent_id, states=None, ids=[offer_id])
    if (had := next((a for a in mine if a.channel != "call"), None)) is None:
        raise ApiError(404, "offer_not_found", "no such offer")
    conv = conversations.lock(db, had.conversation_id)
    return conv, next((a for a in conversations.rows(db, conv.id, ("offered",)) if a.id == offer_id), None), had


NOT_OPEN = ApiError(422, "offer_not_open", "the offer is no longer open")


@router.post("/offers/{offer_id}/accept", status_code=204)
def accept_offer(offer_id: UUID, p: Principal = AGENT) -> Response:
    """Take a text or task offer; a transfer's target becomes the conversation's primary. 422 once it is not open."""
    with tx() as db:
        conv, a, had = _offer(db, p, offer_id)
        took = a and (
            conversations.promote(db, conv, a) if had.via == "transfer" else conversations.move(db, a, "accept")
        )
        if took is None:
            raise NOT_OPEN
        CHANNELS[conv.channel].ended(db, conv, took)
    return Response(status_code=204)


@router.post("/offers/{offer_id}/decline", status_code=204)
def decline_offer(offer_id: UUID, p: Principal = AGENT) -> Response:
    """Turn down a text or task offer; the conversation's next round runs. An offer already over is 204 while a
    transfer still offers the conversation, or while its queue stint has made a round, the next one running once no
    offer is out; otherwise 422."""
    with tx() as db:
        conv, a, _ = _offer(db, p, offer_id)
        if a:
            reoffer(db, conv, conversations.move(db, a, "decline"))
            return Response(status_code=204)
        out = conversations.rows(db, conv.id, ("offered",))
        if any(o.via == "transfer" for o in out):
            return Response(status_code=204)
        if conv.state != "queued" or not hot.rounds(conv).round:
            raise NOT_OPEN
        if not out:
            jobs.schedule(db, "next_round", {"conversation_id": str(conv.id)}, key=str(conv.id))
    return Response(status_code=204)
