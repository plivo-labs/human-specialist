"""Search over closed human conversations. The `search_index` job writes each closed conversation, at its version,
to a search index (OpenSearch, or an engine with its API) when SEARCH_URL is set, else to the `search` table. A chat
no one wrote in stays out. The route reads only that sink: an index that does not answer, or answers part of a page,
is 503 search_unavailable, never a fallback to Postgres. The `search_reconcile` pass runs again every index write that
gave up and indexes again what an index lost, and `search_backfill`, which an operator queues, indexes every closed
conversation again."""

import functools
import hashlib
import json
import logging
import operator
import re
import textwrap
from collections import defaultdict
from collections.abc import Iterable, Iterator
from datetime import date, datetime, time, timedelta
from typing import Literal
from uuid import UUID
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx
from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from pydantic import Field, PrivateAttr, model_validator
from sqlalchemy import Text, Time, and_, cast, extract, false, func, or_, select, text, tuple_, type_coerce
from sqlalchemy.dialects.postgresql import ARRAY, insert
from sqlalchemy.engine import Connection, Row

from hs import access, config, jobs
from hs import conversations as cv
from hs.conversations import api as conversations
from hs.core import ApiError, Page, Paging, T, page, paging, read, settings, tx

log = logging.getLogger(__name__)
router = APIRouter()
URL = settings.search_url
HTTP = httpx.Client(timeout=5, trust_env=False)
# A search outage of a few hours delays an index write; it does not park it.
BACKOFF = (5, 30, 120, 600, 1800, 3600, 3600, 3600)
NUMBERS = ("duration_sec", "talk_sec", "wrap_sec", "response_sec", "message_count", "hold_sec", "hold_count")
FIELDS = {
    **{name: ("id", name) for name in ("channel", "direction", "queue_id", "result", "ended_by")},
    **{name: ("id", f"{name}s") for name in ("agent_id", "team_id", "disposition_id", "type")},
    **{name: ("text", name) for name in ("number", "disposition", "wrap_up_note", "csat")},
    "message": ("text", "messages"),
    "state": ("text", "conversation_state"),
    "our_number": ("text", "ours"),
    **{name: ("bool", name) for name in ("answered", "voicemail")},
    "created_date": ("date", "created_at"),
    "closed_date": ("date", "ended_at"),
    "created_time": ("time", "created_at"),
    **{name: ("number", name) for name in NUMBERS},
}
TEXTS = tuple(field for kind, field in FIELDS.values() if kind == "text")
SORTS = {"created_at": "created_at"} | {n: FIELDS[n][1] for n in ("number", "disposition", "state", "csat", *NUMBERS)}
STORED = ("team_ids", "disposition_ids", "disposition", "conversation_state", "wrap_up_note", "ended_by", "voicemail")
STORED += ("messages", "types", "csat", "ours", *NUMBERS)
KINDS = ("message", "note", "voicemail", "recording", "feedback", "transfer")
OPS = {
    "id": ("is", "is_not"),
    "bool": ("is", "is_not"),
    "text": ("is", "is_not", "contains", "does_not_contain", "empty", "not_empty"),
    "number": ("is", "is_not", "gt", "gte", "lt", "lte", "between"),
    "date": ("is", "after", "before", "on_and_after", "on_and_before", "between"),
    "time": ("is", "gt", "gte", "lt", "lte", "between"),
}
RAW = 8191  # at most 4 bytes a character: an index takes a keyword of up to 32,766 bytes
KEYWORD = {"type": "keyword", "ignore_above": RAW}
LOWER = KEYWORD | {"normalizer": "lowercase"}
TYPES = {"id": "keyword", "bool": "boolean", "number": "integer", "date": "date", "time": "date"}
# `words` matches by its words, and as whole texts up to RAW characters; `pieces` holds a longer text cut between
# words, so one word with punctuation inside matches it too. The rest of a document, context and attributes included,
# is stored but never mapped. An index keeps each field it was given, so MAPPING only gains fields, with no analysis
# settings. No migration queues `search_backfill`, so closed conversations gain a new field only once an operator
# queues it (docs/configuration.md, Fill the search index).
MAPPING = {
    "dynamic": False,
    "properties": {
        "conversation_number": {"type": "long"},
        "took": {"type": "keyword"},
        "routed": {"type": "boolean"},
        "route_teams": {"type": "keyword"},
        "words": {"type": "text", "fields": {"raw": KEYWORD}},
        "pieces": KEYWORD,
        "message_keys": {"type": "keyword"},
        **{field: {"type": TYPES[kind]} for kind, field in FIELDS.values() if kind != "text"},
        **{field: KEYWORD | {"fields": {"text": {"type": "text"}, "lower": LOWER}} for field in TEXTS},
    },
}
WORDS = ("words", "words.raw", "pieces")
# The backfill queues BATCH index jobs every PAUSE seconds once the last batch ran: at most 20 a second, about one
# slow-lane worker (an index job takes 40 to 60 ms). That is 72,000 an hour, a million in 14 hours; raise BATCH if a
# large install must refill faster.
BATCH, PAUSE = 200, 10
RECONCILE_EVERY_S, RECONCILE_WINDOW = 900, timedelta(days=7)
RANGE = {"gt": operator.gt, "gte": operator.ge, "lt": operator.lt, "lte": operator.le}
# The one bound of a date operator open on one side: (bound, 0 for the day's start or 1 for the next day's).
DATES = {"after": ("gte", 1), "on_and_after": ("gte", 0), "before": ("lt", 0), "on_and_before": ("lt", 1)}
MONTHS = {"month": 1, "quarter": 3, "year": 12}
CLOCK = (
    "if (doc['created_at'].size() == 0) return false; int s = doc['created_at'].value"
    ".withZoneSameInstant(java.time.ZoneId.of(params.tz)).toLocalTime().toSecondOfDay();"
    " return params.wraps ? s >= params.lo || s <= params.hi : s >= params.lo && s <= params.hi;"
)
# The numbers of closed conversation :c. Talk runs from the first agent's start to the last wrap-up's start, none
# for an outbound call or text the customer never got. The first response is the first message the other way, or an
# agent's start on a call that rang through.
FACTS = text(
    "SELECT CAST(extract(epoch FROM c.closed_at - c.opened_at) AS int) AS duration_sec,"
    " CASE WHEN c.direction = 'outbound' AND (c.channel = 'call' AND c.answered_at IS NULL OR c.channel IN"
    " ('sms', 'whatsapp') AND NOT EXISTS (SELECT FROM entry WHERE conversation_id = c.id AND kind = 'message'"
    " AND status IN ('sent', 'delivered', 'read'))) THEN 0"
    " ELSE CAST(coalesce(extract(epoch FROM a.wrapped - a.took), 0) AS int) END AS talk_sec,"
    " CAST(coalesce(a.wrap, 0) AS int) AS wrap_sec,"
    " CAST(coalesce(extract(epoch FROM least(m.reply, CASE WHEN c.channel = 'call' AND (c.direction = 'inbound'"
    " OR c.answered_at IS NOT NULL) THEN a.took END) - c.opened_at), 0) AS int) AS response_sec,"
    " CAST(m.n AS int) AS message_count, c.hold_sec, c.hold_count, m.voicemail, c.ended_by"
    " FROM conversation c CROSS JOIN LATERAL (SELECT min(started_at) AS took,"
    " max(ended_at) FILTER (WHERE wrap_up_until IS NOT NULL) AS wrapped,"
    " sum(extract(epoch FROM coalesce(submitted_at, least(wrap_up_until, c.closed_at)) - ended_at))"
    " FILTER (WHERE wrap_up_until IS NOT NULL) AS wrap"
    " FROM assignment WHERE conversation_id = c.id AND agent_id IS NOT NULL AND started_at IS NOT NULL"
    " AND role IN ('primary', 'secondary')) a"
    " CROSS JOIN LATERAL (SELECT count(*) AS n, min(at) FILTER (WHERE body->>'direction' <> c.direction) AS reply,"
    " count(*) FILTER (WHERE kind = 'voicemail') > 0 AS voicemail"
    " FROM entry WHERE conversation_id = c.id AND kind IN ('message', 'voicemail')"
    " AND status IS DISTINCT FROM 'failed') m WHERE c.id = :c"
)
# What a conversation closed before ended_by and team_ids existed gains as it is indexed: who ended its call, from the
# events retention left, and the teams its primary agents are in now.
FILL = text(
    "UPDATE conversation c SET ended_by = coalesce(c.ended_by, f.ended_by), team_ids = coalesce(c.team_ids, f.teams)"  # noqa: S608
    " FROM (SELECT (SELECT data->>'ended_by' FROM events WHERE conversation_id = CAST(:c AS text)"
    " AND type = 'call.ended' ORDER BY id DESC LIMIT 1) AS ended_by, "
    + cv.TEAMS_OF.format(":c")
    + " AS teams) f WHERE c.id = :c AND (c.ended_by IS NULL AND f.ended_by IS NOT NULL OR c.team_ids IS NULL)"
)
SUMMARY = list(conversations.ConversationSummary.model_fields)
# A hit's `fields` names what its summary lacks; queue_id joined the summary later, and a hit still names it there.
SHOWN = tuple(n for n, (_, f) in FIELDS.items() if f not in (*SUMMARY, "messages") or n == "queue_id")


Bounds = dict[Literal["gt", "gte", "lt", "lte"], int]
Value = str | int | bool | None
Joins = list[Literal["and", "or"]]


class Atom(conversations.Strict):
    field: Literal[tuple(FIELDS)]
    op: Literal[tuple(dict.fromkeys(op for ops in OPS.values() for op in ops))]
    value: Value | list[Value] = None


class Group(conversations.Strict):
    atoms: list[Atom] = Field(min_length=1, max_length=50)
    conditions: Joins = []


class Filter(conversations.Strict):
    groups: list[Group] = Field([], max_length=50)
    conditions: Joins = []


class SearchIn(conversations.Strict):
    agent_id: UUID | None = None
    team_id: UUID | None = None
    queue_id: UUID | None = None
    channel: conversations.Channel | None = None
    disposition_id: UUID | None = None
    direction: Literal["inbound", "outbound"] | None = None
    number: str | None = None
    text: str | None = Field(None, max_length=1000)
    created_after: datetime | None = None
    created_before: datetime | None = None
    answered: bool | None = None
    result: str | None = Field(None, max_length=100)
    ended_by: Literal["customer", "agent", "system"] | None = None
    duration_sec: Bounds | None = None
    talk_sec: Bounds | None = None
    wrap_sec: Bounds | None = None
    response_sec: Bounds | None = None
    message_count: Bounds | None = None
    hold_sec: Bounds | None = None
    hold_count: Bounds | None = None
    voicemail: bool | None = None
    type: str | None = Field(None, max_length=100)
    filter: Filter | None = None
    time_zone: str = Field("UTC", max_length=64)
    sort: Literal[tuple(SORTS)] = "created_at"
    order: Literal["asc", "desc"] = "desc"
    _conds: list[tuple] = PrivateAttr([])

    @model_validator(mode="after")
    def _parse(self):
        self._conds = _conditions(self)
        return self


class SearchBody(SearchIn):
    fields: list[Literal[SHOWN]] = Field([], description="Filter fields whose values each hit carries in `fields`.")


class Hit(conversations.ConversationSummary):
    fields: dict[str, Value | list[str]] = Field({}, description="The value of each field the search named, by name.")


def _request(method: str, path: str, ok: tuple[int, ...] = (), **kw) -> httpx.Response:
    """One request to the search index; 503 search_unavailable unless it answers 2xx or one of `ok`."""
    try:
        r = HTTP.request(method, URL + path, **kw)
    except httpx.TransportError as e:
        log.warning("search %s %s failed: %s", method, path, type(e).__name__)
        raise ApiError(503, "search_unavailable", "the search index did not answer") from e
    if not (r.is_success or r.status_code in ok):
        log.warning("search %s %s answered %s", method, path, r.status_code)
        raise ApiError(503, "search_unavailable", "the search index did not answer")
    return r


def _index(tenant_id) -> str:
    return f"hs-conversations-{tenant_id}"


@functools.cache
def _mapped(tenant_id) -> str:
    """The tenant's index, once per process, made with MAPPING or given the fields MAPPING added: a replica still
    running an older MAPPING puts the same template, so a write may not make it with this one."""
    body = {"index_patterns": [_index("*")], "priority": 1, "template": {"mappings": MAPPING}}
    _request("PUT", "/_index_template/hs-conversations-fields", json=body)
    _request("PUT", f"/{_index(tenant_id)}", (400,), json={"mappings": MAPPING})
    _request("PUT", f"/{_index(tenant_id)}/_mapping", (404,), json=MAPPING)
    return _index(tenant_id)


def document(db: Connection, conv: Row) -> dict | None:
    """view() plus what search reads: every agent who took part, the teams they were in when it closed, the teams of the route
    its enqueue kept, the final sub-disposition or else the disposition if answered, the last wrap-up's disposition
    name and note, the FACTS, and `words`, the texts `text` matches (the state and every CSAT answer among them), the
    names of the handlers, their skills and their teams. The final disposition is the state when it is not marked
    answered, and then names no disposition. None for a chat no one wrote in."""
    A, E, G = T.assignment, T.entry, T.config
    bodies = db.execute(select(E.c.body).where(*_texts(conv.id))).scalars().all()
    if conv.channel == "chat" and not bodies:
        return None
    doc, tenant = cv.view(db, conv), str(conv.tenant_id)
    took = (A.c.conversation_id == conv.id, A.c.agent_id.isnot(None), A.c.started_at.isnot(None))
    handlers = select(G.c.body).where(G.c.kind == "agent", G.c.id.in_([UUID(a) for a in doc["agent_ids"]]))
    handlers = db.execute(handlers).scalars().all()
    teams = conv.team_ids or []
    named = [*doc["agent_ids"], *teams, *(s["skill_id"] for h in handlers for s in h.get("skills", []))]
    names = select(G.c.id.cast(Text), G.c.body["name"].astext).where(G.c.tenant_id == conv.tenant_id)
    names = dict(db.execute(names.where(G.c.id.in_([UUID(i) for i in named])).order_by(G.c.id)).all())
    wraps = select(A.c.note).where(A.c.conversation_id == conv.id, A.c.disposition_id.isnot(None))
    notes = db.execute(wraps.order_by(A.c.offered_at, A.c.id)).scalars().all()
    last = doc["dispositions"][-1] if doc["dispositions"] else {}
    top, sub = (
        (last.get(k) and config.get(db, tenant, "disposition", last[k])) or {}
        for k in ("disposition_id", "sub_disposition_id")
    )
    answered = top.get("answered", True)
    reached = "Customer answered" if conv.answered_at else "Customer didn't respond"
    state = (reached if answered else top["name"]) if top else None
    facts = db.execute(FACTS, {"c": conv.id}).one()._asdict()
    words = [w for b in bodies for w in (b.get("text"), (b.get("email") or {}).get("subject"))]
    said = [w for w in (*words, *notes) if w]
    kinds = select(E.c.kind).where(E.c.conversation_id == conv.id, E.c.kind.in_(KINDS), E.c.deleted_at.is_(None))
    kinds = db.execute(kinds.where(E.c.status.is_distinct_from("failed")).distinct()).scalars().all()
    kinds = {"notes" if k == "note" else k for k in (*kinds, *(["note"] if any(notes) else []))}
    scores = text(
        "SELECT DISTINCT data->>'score' FROM events WHERE conversation_id = :c AND type = 'conversation.csat_recorded'"
    )
    words += [*notes, top.get("name"), sub.get("name"), conv.customer, facts["ended_by"], conv.csat, *names.values()]
    words += db.execute(scores, {"c": str(conv.id)}).scalars()
    words += [state, conv.ours if re.fullmatch(r"\+?\d{7,15}", conv.ours) else None]
    leaf = last.get("sub_disposition_id") or (answered and last.get("disposition_id"))
    found = {
        "took": list(db.execute(select(A.c.agent_id.cast(Text)).where(*took).distinct()).scalars()),
        "team_ids": [t for t in teams if t in names],
        "routed": "route" in conv.routing,
        "route_teams": conv.routing.get("route", {}).get("teams", []),
        "disposition_ids": [leaf] if leaf else [],
        "words": [w for w in words if w],
        "pieces": [p for w in words if w and len(w) > RAW for p in textwrap.wrap(w, RAW, break_on_hyphens=False)],
        "answered": conv.answered_at is not None,
        "result": conv.result,
        "disposition": top.get("name") if answered else None,
        "conversation_state": state,
        "wrap_up_note": notes[-1] if notes else None,
        "csat": conv.csat,
        "ours": conv.ours,
        "messages": said or None,
        "message_keys": [_whole(m) for m in said],
        "types": sorted(kinds),
    }
    return doc | facts | found


def _texts(cid) -> tuple:
    """The entries whose texts a document holds: a chat with none is never written."""
    E = T.entry
    return E.c.conversation_id == cid, E.c.kind.in_(("message", "note")), E.c.deleted_at.is_(None)


def _pending() -> tuple:
    J = T.jobs
    return J.c.kind == "search_index", J.c.key.isnot(None), or_(J.c.locked_until.is_(None), ~jobs.parked())


def _waiting(db: Connection) -> int:
    return db.execute(select(func.count()).where(*_pending())).scalar()


def write(conv: Row, doc: dict) -> None:
    """Store `doc` as `conv` at its version: an older version changes nothing, and the stored one is replaced, as a
    backfill does."""
    if URL:
        version = {"version": conv.version, "version_type": "external_gte"}
        at = f"/{_mapped(conv.tenant_id)}/_doc/{conv.id}"
        _request("PUT", at, (409,), params=version, json=jsonable_encoder(doc))
        return
    S = T.search
    cols = {k: doc[k] for k in STORED}
    cols |= {"version": conv.version, "content": "\n".join(doc["words"]).lower()}
    cols["messages"] = doc["messages"] and [m.lower() for m in doc["messages"]]
    row = insert(S).values(conversation_id=conv.id, words=func.to_tsvector("simple", " ".join(doc["words"])), **cols)
    newer = {c: row.excluded[c] for c in (*cols, "words")}
    row = row.on_conflict_do_update(
        index_elements=[S.c.conversation_id], set_=newer, where=S.c.version <= row.excluded.version
    )
    with read() as db:
        db.execute(row)


@jobs.handler("search_index", BACKOFF, lane="slow")
def _index_job(job: Row) -> None:
    C, cid = T.conversation, job.payload["conversation_id"]
    with read() as db:
        db.execute(FILL, {"c": cid})
        conv = db.execute(select(C).where(C.c.id == cid)).first()
        doc = conv and conv.state == "closed" and document(db, conv)
    if doc:
        write(conv, doc)


@jobs.handler("search_backfill", lane="slow", parks=False)
def _backfill(job: Row) -> None:
    """Index every closed conversation again, BATCH at a time in id order, each batch once fewer than BATCH index jobs
    wait."""
    C, after = T.conversation, job.payload["after"]
    with tx() as db:
        if _waiting(db) < BATCH:
            batch = select(C.c.id, C.c.tenant_id).where(C.c.state == "closed", C.c.id > after)
            batch = db.execute(batch.order_by(C.c.id).limit(BATCH)).all()
            for cid, tenant in batch:
                payload = {"tenant_id": str(tenant), "conversation_id": str(cid)}
                jobs.schedule(db, "search_index", payload, key=str(cid))
            if len(batch) < BATCH:
                return
            after = str(batch[-1].id)
        jobs.schedule(db, "search_backfill", {"after": after}, key="search_backfill", delay=PAUSE, rev=job.rev)


jobs.every("search_reconcile", RECONCILE_EVERY_S)


@jobs.handler("search_reconcile", lane="slow")
def _reconcile(job: Row) -> None:
    C, J, p = T.conversation, T.jobs, job.payload if URL else {}
    with tx() as db:
        if "since" not in p:
            parked = select(J.c.key, J.c.payload).where(J.c.kind == "search_index", J.c.key.isnot(None), jobs.parked())
            parked = parked.limit(max(BATCH - _waiting(db), 0))
            for key, payload in db.execute(parked).all():
                jobs.schedule(db, "search_index", payload, key=key)
            if not URL:
                return
            p = {"since": db.execute(select(func.now() - RECONCILE_WINDOW)).scalar().isoformat()}
            jobs.schedule(db, "search_reconcile", p, key="search_reconcile", delay=RECONCILE_EVERY_S)
        if _waiting(db) >= BATCH:
            jobs.schedule(db, "search_reconcile", p, key="search_reconcile", delay=PAUSE)
            return
        queued = select(J.c.id).where(*_pending(), J.c.key == C.c.id.cast(Text))
        written = or_(C.c.channel != "chat", select(T.entry.c.id).where(*_texts(C.c.id)).exists())
        recent = [C.c.state == "closed", C.c.closed_at >= datetime.fromisoformat(p["since"]), written, ~queued.exists()]
        if after := p.get("after"):
            recent.append(tuple_(C.c.closed_at, C.c.id) > tuple_(datetime.fromisoformat(after[0]), UUID(after[1])))
        batch = select(C.c.id, C.c.tenant_id, C.c.closed_at).where(*recent).order_by(C.c.closed_at, C.c.id)
        batch = db.execute(batch.limit(BATCH)).all()
    docs = [{"_index": _index(tenant), "_id": str(cid)} for cid, tenant, _ in batch]
    got = _request("POST", "/_mget", params={"_source": "false"}, json={"docs": docs}).json()["docs"] if docs else []
    if any("found" not in d and d.get("error", {}).get("type") != "index_not_found_exception" for d in got):
        raise ApiError(503, "search_unavailable", "the search index could not tell whether it holds a document")
    held = {d["_id"] for d in got if d.get("found")}
    with tx() as db:
        for cid, tenant, _ in batch:
            if str(cid) not in held:
                jobs.schedule(db, "search_index", {"tenant_id": str(tenant), "conversation_id": str(cid)}, key=str(cid))
        more = p | {"after": [batch[-1].closed_at.isoformat(), str(batch[-1].id)]} if len(batch) == BATCH else {}
        jobs.schedule(db, "search_reconcile", more, key="search_reconcile", delay=0 if more else RECONCILE_EVERY_S)


def _span(day: str, zone: ZoneInfo) -> tuple[datetime, datetime]:
    """A day or period in `zone`, as its start and the start after it: today, yesterday, this_ or last_ week, month,
    quarter or year, or an ISO date."""
    now = datetime.now(zone)
    today, (period, _, unit) = datetime.combine(now.date(), time(), zone), day.partition("_")
    if day in ("today", "yesterday"):
        start = today - timedelta(days=day == "yesterday")
        return start, start + timedelta(days=1)
    if period in ("this", "last") and unit == "week":
        start = today - timedelta(days=today.weekday() + 7 * (period == "last"))
        return start, start + timedelta(days=7)
    if period in ("this", "last") and (months := MONTHS.get(unit)):
        first = today.replace(month=(now.month - 1) // months * months + 1, day=1)
        start = _months(first, -months * (period == "last"))
        return start, _months(start, months)
    start = datetime.combine(date.fromisoformat(day), time(), zone)
    return start, start + timedelta(days=1)


def _months(at: datetime, n: int) -> datetime:
    years, month = divmod(at.month - 1 + n, 12)
    return at.replace(year=at.year + years, month=month + 1)


def _atom(a: Atom, zone: str) -> tuple:
    kind, field = FIELDS[a.field]
    ops = ("is",) if a.field == "agent_id" else OPS[kind]
    if a.op not in ops:
        raise ValueError(f"{a.field} takes {', '.join(ops)}, not {a.op}")
    v, negated = a.value, a.op in ("is_not", "does_not_contain", "empty")
    if a.op in ("empty", "not_empty"):
        return ("exists", field, None, negated)
    if a.op in ("contains", "does_not_contain"):
        return ("match", field, str(v).lower(), negated)
    if kind == "date":
        spans = [_span(t.strip(), ZoneInfo(zone)) for t in str(v).split(",")]
        days, (bound, end) = (spans[0][0], spans[-1][1]), DATES.get(a.op, (None, None))
        return ("range", field, {bound: days[end]} if bound else {"gte": days[0], "lt": days[1]}, False)
    if kind == "time":
        at = [datetime.strptime(t.strip(), "%H:%M:%S") for t in str(v).split(",")]
        lo, hi = (t.hour * 3600 + t.minute * 60 + t.second for t in (at[0], at[-1]))
        lo, hi = {"gt": (lo + 1, 86399), "gte": (lo, 86399), "lt": (0, lo - 1), "lte": (0, lo)}.get(a.op, (lo, hi))
        return ("clock", field, {"lo": lo, "hi": hi, "wraps": a.op == "between" and lo > hi, "tz": zone}, False)
    if kind == "number" and a.op not in ("is", "is_not"):
        bounds = dict(zip(("gte", "lte"), map(int, str(v).split(",")), strict=True)) if a.op == "between" else None
        return ("range", field, bounds or {a.op: int(v)}, False)
    parse = {"id": str, "text": lambda x: str(x).lower(), "number": int, "bool": lambda x: str(x).lower() == "true"}
    return ("in", field, [parse[kind](x) for x in (v if isinstance(v, list) else [v])], negated)


def _fold(conds: list[tuple], joins: list[str]) -> tuple:
    """Conditions joined left to right, each to the ones before it by its join, `and` unless given."""
    out = conds[0]
    for cond, join in zip(conds[1:], [*joins, *["and"] * len(conds)], strict=False):
        out = (join, None, [out, cond], False)
    return out


def _conditions(body: SearchIn) -> list[tuple]:
    """Every filter of the body as a condition, (op, index field, argument, negated): all must hold."""
    try:
        ZoneInfo(body.time_zone)
    except ZoneInfoNotFoundError as e:
        raise ValueError(f"unknown time zone {body.time_zone}") from e
    flat = [(field, getattr(body, k)) for k, (kind, field) in FIELDS.items() if kind in ("id", "bool")]
    out = [("in", field, [v if isinstance(v, bool) else str(v)], False) for field, v in flat if v is not None]
    if body.number:
        digits = body.number.lstrip("+").lower()
        out.append(("in", "number", [digits, f"+{digits}"], False))
    if dates := {k: v for k, v in (("gt", body.created_after), ("lt", body.created_before)) if v}:
        out.append(("range", "created_at", dates, False))
    out += [("range", n, bounds, False) for n in NUMBERS if (bounds := getattr(body, n))]
    if words := (body.text or "").lower().split():
        out.append(("word", "words", words[0], False) if len(words) == 1 else ("phrase", "words", words, False))
    if body.filter and body.filter.groups:
        groups = [_fold([_atom(a, body.time_zone) for a in g.atoms], g.conditions) for g in body.filter.groups]
        out.append(_fold(groups, body.filter.conditions))
    return out


def _lexemes(value):
    value = func.array_to_string(value, " ") if isinstance(getattr(value, "type", None), ARRAY) else value
    return func.tsvector_to_array(func.to_tsvector("simple", value), type_=ARRAY(Text))


def _pg(cond: tuple, cols: dict):
    op, field, arg, negated = cond
    col = cols.get(field)
    c = {
        "and": lambda: and_(*(_pg(x, cols) for x in arg)),
        "or": lambda: or_(*(_pg(x, cols) for x in arg)),
        "in": lambda: (
            col.overlap(arg) if isinstance(col.type, ARRAY) else (func.lower(col) if field in TEXTS else col).in_(arg)
        ),
        "match": lambda: _lexemes(col).overlap(_lexemes(arg)),
        "exists": lambda: col.isnot(None),
        "range": lambda: and_(*(RANGE[o](col, v) for o, v in arg.items())),
        "clock": lambda: _clock(col, **arg),
        # A row written before `content` existed matches by its whole words until the backfill reaches it.
        "word": lambda: func.coalesce(
            cols["content"].contains(arg, autoescape=True), col.op("@@")(func.plainto_tsquery("simple", arg))
        ),
        "phrase": lambda: col.op("@@")(func.to_tsquery("simple", " <-> ".join(map(_lexeme, arg)) + ":*")),
    }[op]()
    return ~func.coalesce(c, false()) if negated else c


def _lexeme(word: str) -> str:
    return "'" + word.replace("\\", "\\\\").replace("'", "''") + "'"


def _clock(col, lo: int, hi: int, wraps: bool, tz: str):
    second = func.floor(extract("epoch", cast(func.timezone(tz, col), Time)))
    return (or_ if wraps else and_)(second >= lo, second <= hi)


def _os(cond: tuple) -> dict:
    """A condition as an index query. A bool query of only `should` clauses needs one of them."""
    op, field, arg, negated = cond
    q = {
        "and": lambda: {"bool": {"filter": [_os(x) for x in arg]}},
        "or": lambda: {"bool": {"should": [_os(x) for x in arg]}},
        "in": lambda: _terms(field, arg),
        "match": lambda: {"match": {f"{field}.text": arg}},
        "exists": lambda: {"exists": {"field": "message_keys" if field == "messages" else field}},
        "range": lambda: {"range": {field: arg}},
        "clock": lambda: {"script": {"script": {"source": CLOCK, "params": arg}}},
        "word": lambda: {"bool": {"should": [{"wildcard": {f: _wildcard(arg)}} for f in WORDS]}},
        "phrase": lambda: {"match_phrase_prefix": {field: " ".join(arg)}},
    }[op]()
    return {"bool": {"must_not": [q]}} if negated else q


def _terms(field: str, values: list) -> dict:
    if field == "messages":
        return {"terms": {"message_keys": [_whole(v) for v in values]}}
    if field not in TEXTS:
        return {"terms": {field: values}}
    exact = [{"term": {field: {"value": v, "case_insensitive": True}}} for v in values]
    return {"bool": {"should": exact}} if exact else {"match_none": {}}


def _whole(text: str) -> str:
    """Both backends match a message whole by Python's str.lower of it; the index matches this hash of that, since a
    keyword holds RAW chars and a case-insensitive term 1,000."""
    return hashlib.sha256(text.lower().encode()).hexdigest()


def _wildcard(word: str) -> dict:
    return {"value": "*" + re.sub(r"([*?\\])", r"\\\1", word) + "*", "case_insensitive": True}


def _scoped(cond: tuple, seen: set[str]) -> tuple:
    """`cond` with the agents it asks for narrowed to the ones `seen`: a viewer asking only for others finds none."""
    op, field, arg, negated = cond
    if op in ("and", "or"):
        return (op, field, [_scoped(c, seen) for c in arg], negated)
    return (op, field, [a for a in arg if a in seen], negated) if field == "agent_ids" else cond


def _keys(body: SearchIn) -> list[str]:
    return [SORTS[body.sort], "conversation_number"]


def _after(body: SearchIn, paged: Paging) -> list | None:
    """The sort values a page starts after: the cursor, a JSON list of them. The index sorts a time as epoch ms."""
    if paged.after is None:
        return None
    parse = dict.fromkeys(TEXTS, str) | {"created_at": int if URL else datetime.fromisoformat}
    try:
        return [parse.get(k, int)(v) for k, v in zip(_keys(body), json.loads(paged.after), strict=True)]
    except (ValueError, TypeError) as e:
        raise ApiError(422, "invalid_cursor", "cursor is not one this API returned") from e


def _rows(
    db: Connection, p: access.Principal, body: SearchIn, conds: list[tuple], paged: Paging, fields=()
) -> list[dict]:
    C, S = T.conversation, T.search
    cols = dict(C.c.items()) | {"queue_id": C.c.queue_id.cast(Text), "number": C.c.customer}
    cols |= {"created_at": C.c.opened_at, "answered": C.c.answered_at.isnot(None), "conversation_number": C.c.number}
    cols |= {"ended_at": C.c.closed_at}
    cols |= dict(S.c.items()) | {"agent_ids": type_coerce(cv.summary().selected_columns.agent_ids, ARRAY(Text))}
    keys = [func.lower(func.coalesce(cols[k], "")).collate("C") if k in TEXTS else cols[k] for k in _keys(body)]
    after, asc = _after(body, paged), body.order == "asc"
    shared = conversations.where(db, p, conversations.Filters(), paged._replace(after=None))
    extra = (cols[f].label(f) for f in fields if f not in SUMMARY)
    q = cv.summary().add_columns(*(k.label(f"k{i}") for i, k in enumerate(keys)), *extra)
    q = q.join_from(C, S, S.c.conversation_id == C.c.id).where(*shared, *(_pg(c, cols) for c in conds))
    if after:
        q = q.where(tuple_(*keys) > tuple_(*after) if asc else tuple_(*keys) < tuple_(*after))
    q = q.order_by(*(k.asc() if asc else k.desc() for k in keys)).limit(paged.limit + 1)
    rows = [dict(r._mapping) for r in db.execute(q)]
    return [r | {"cursor": json.dumps(jsonable_encoder([r[f"k{i}"] for i in range(len(keys))]))} for r in rows]


def _hits(
    p: access.Principal, body: SearchIn, conds: list[tuple], paged: Paging, s: access.Seen, fields=()
) -> list[dict]:
    if s.agents is not None:
        queued = ("and", None, [("in", "queue_id", s.queues, False), ("in", "routed", [True], True)], False)
        routed = ("or", None, [queued, ("in", "route_teams", sorted(s.teams), False)], False)
        untaken = ("and", None, [routed, ("exists", "took", None, True)], False)
        conds = [*conds, ("or", None, [("in", "took", sorted(s.agents), False), untaken], False)]
    worded = {"order": body.order, "missing": ""}
    sort = [{f"{k}.lower": worded} if k in TEXTS else {k: body.order} for k in _keys(body)]
    q = {"query": {"bool": {"filter": [_os(c) for c in conds]}}, "sort": sort, "size": paged.limit + 1}
    q |= {"_source": [*SUMMARY, *fields]} | ({"search_after": after} if (after := _after(body, paged)) else {})
    index = _mapped(p.tenant_id)
    r = _request("POST", f"/{index}/_search", (404,), json=jsonable_encoder(q))
    if not r.is_success:
        return []
    try:
        found = r.json()
        hits = found["hits"]["hits"]
        whole = found["timed_out"] is False and found["_shards"]["failed"] == 0 and isinstance(hits, list)
        if not whole or any(h["_index"] != index or h["_source"]["id"] != h["_id"] for h in hits):
            raise ValueError("a partial page, or another index's conversation")
        return [h["_source"] | {"cursor": json.dumps(h["sort"])} for h in hits]
    except (ValueError, KeyError, TypeError) as e:
        log.warning("search %s answered %r", index, e)
        raise ApiError(503, "search_unavailable", "the search index did not answer") from e


ID_FIELDS = {"agent_id": "agent", "team_id": "team", "queue_id": "queue", "disposition_id": "disposition"}


def as_list(v) -> list:
    return v if isinstance(v, list) else [v]


def named_ids(f: dict) -> Iterator[tuple[str, dict, str]]:
    for k, kind in ID_FIELDS.items():
        if f.get(k):
            yield kind, f, k
    for g in (f.get("filter") or {}).get("groups", []):
        yield from ((ID_FIELDS[a["field"]], a, "value") for a in g["atoms"] if a["field"] in ID_FIELDS)


def owned(db: Connection, p: access.Principal, body: SearchIn, **more: Iterable) -> None:
    wanted = defaultdict(set, {kind: set(ids) for kind, ids in more.items()})
    for kind, h, k in named_ids(body.model_dump(mode="json", exclude_unset=True)):
        wanted[kind].update(as_list(h.get(k)))
    for kind, ids in wanted.items():
        config.owned(db, p, kind, ids)


@router.post("/conversations/search", response_model=Page[Hit])
def search_conversations(body: SearchBody, p: access.Principal = conversations.READ, paged: Paging = Depends(paging)):
    """Closed conversations matching every filter given, with the visibility of GET /conversations, newest first unless
    `sort` and `order` say otherwise. One word of `text` matches inside any word of the messages, notes, email subjects,
    wrap-up notes, disposition names, the state, the customer's address, our number, who ended the call and the names of
    the handling agents, their skills and their teams, in any case; several words match
    as a phrase whose last word may be cut short. `team_id` finds those teams too. A range takes `gt`, `gte`, `lt`
    and `lte`, in seconds, messages or holds. `filter` holds groups of atoms, each joined to the one before it by its entry in
    `conditions` (`and` unless given); dates and times of day are read in `time_zone`. Each hit carries the values of
    the filter fields `fields` names in its own `fields`."""
    return run(p, body, paged, shown=body.fields)


def run(
    p: access.Principal, body: SearchIn, paged: Paging, fields: tuple[str, ...] = (), shown=(), skip_owned=False
) -> Page:
    named = {name: FIELDS[name][1] for name in shown}
    fields = (*fields, *named.values())
    with read() as db:
        if not skip_owned:
            owned(db, p, body)
        seen = access.visible_agents(db, p)
        conds = body._conds if seen is None else [_scoped(c, seen) for c in body._conds]
        if URL:
            s = access.seen(db, p)
        else:
            rows = _rows(db, p, body, conds, paged, fields)
    if URL:
        rows = _hits(p, body, conds, paged, s, fields)
    return page([r | {"fields": {n: r.get(f) for n, f in named.items()}} for r in rows], paged, key="cursor")
