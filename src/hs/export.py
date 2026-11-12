"""CSV exports of closed conversations, each search page read on a connection of its own."""

import csv
import io
import itertools
import json
from collections.abc import Iterator
from datetime import datetime
from typing import Literal
from zoneinfo import ZoneInfo

from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from sqlalchemy import text

from hs import access, search
from hs import conversations as cv
from hs import text as texts
from hs.conversations.api import READ
from hs.core import ApiError, paging, read

router = APIRouter()
CAP, PAGE, BATCH = 10_000, 200, 1_000
CSV = {200: {"content": {"text/csv": {}}, "description": "A CSV file, streamed."}}
SUMMARY = ("conversation_number", "channel", "number", "agents", "agent_count", "teams", "our_address", "created_at")
SUMMARY += ("direction", "conversation_state", "wrap_up_note", "disposition", "message_count", "duration_sec")
SUMMARY += ("wrap_sec", "talk_sec", "answered", "types", "csat", "ended_by")
CONTENT = ("conversation_number", "channel", "id", "from", "agent_name", "to", "meta", "type", "text", "number", "at")
FACTS = text(
    "SELECT c.id::text AS id, ARRAY(SELECT coalesce(g.body->>'name', a.agent_id::text) FROM assignment a"  # noqa: S608
    " LEFT JOIN config g ON g.id = a.agent_id WHERE a.conversation_id = c.id AND a.agent_id IS NOT NULL"
    " AND a.started_at IS NOT NULL AND a.role IN ('primary', 'secondary') GROUP BY a.agent_id, g.body->>'name'"
    " ORDER BY min(a.started_at), a.agent_id) AS agents, ARRAY(SELECT t.body->>'name'"
    " FROM jsonb_array_elements_text(coalesce(c.team_ids, "
    + cv.TEAMS_OF.format("c.id")
    + ")) m (id) JOIN config t ON t.id = CAST(m.id AS uuid) AND t.kind = 'team' ORDER BY t.body->>'name', t.id)"
    " AS teams FROM conversation c WHERE c.id = ANY(CAST(:ids AS uuid[]))"
)
_IDS = "conversation_id = ANY(CAST(:ids AS uuid[]))"
_TARGET = (
    "CAST(coalesce(e.body#>>'{target,agent_id}', e.body#>>'{target,team_id}', e.body#>>'{target,queue_id}',"
    " e.body#>>'{target,menu_id}', e.body#>>'{target,mailbox,queue_id}') AS uuid)"
)
ITEMS = text(
    "SELECT x.conversation_id::text AS conversation_id, c.number::text AS conversation_number, c.channel,"  # noqa: S608
    " c.customer AS number, x.at, x.type, x.id::text AS id, x.agent_id::text AS agent_id,"
    " coalesce(g.body->>'name', x.agent_id::text, s.name) AS agent_name,"
    " coalesce(x.src, CASE x.direction WHEN 'inbound' THEN c.customer WHEN 'outbound' THEN c.ours END) AS from,"
    " coalesce(x.dst, CASE x.direction WHEN 'inbound' THEN c.ours WHEN 'outbound' THEN c.customer END) AS to,"
    " x.text, x.meta, x.body, x.provider_id, x.status FROM ("
    " SELECT conversation_id, at, kind AS type, id, agent_id, body->>'direction' AS direction, NULL AS src,"
    " NULL AS dst, CASE kind WHEN 'message' THEN coalesce(body->>'text', body#>>'{email,subject}')"
    " WHEN 'note' THEN body->>'text' WHEN 'voicemail' THEN id::text WHEN 'recording' THEN id::text"
    " ELSE CAST(body AS text) END AS text, body->>'reply_to' AS meta, body, provider_id, status"
    f" FROM entry WHERE {_IDS} AND deleted_at IS NULL"
    " AND status IS DISTINCT FROM 'failed' AND kind IN ('message', 'note', 'voicemail', 'recording', 'feedback', 'signal')"
    " UNION ALL SELECT conversation_id, started_at, 'joined', id, agent_id, NULL, NULL, NULL, NULL, via, NULL, NULL,"
    f" NULL FROM assignment WHERE {_IDS} AND agent_id IS NOT NULL AND started_at IS NOT NULL AND role <> 'monitor'"
    " UNION ALL SELECT e.conversation_id, e.at, 'transfer', e.id, e.agent_id, NULL,"
    " coalesce(n.body->>'name', e.agent_id::text), coalesce(k.name, e.body#>>'{target,number}'),"
    " coalesce(o.body->>'name', e.body#>>'{target,number}', e.body#>>'{target,url}'),"
    " e.body#>>'{target,type}', NULL, NULL, NULL FROM entry e"
    f" LEFT JOIN config n ON n.id = e.agent_id LEFT JOIN config o ON o.id = {_TARGET} LEFT JOIN LATERAL"
    " (SELECT coalesce(g.body->>'name', a.agent_id::text) AS name FROM assignment a LEFT JOIN config g"
    " ON g.id = a.agent_id WHERE a.conversation_id = e.conversation_id AND a.transfer_id = CAST(e.body->>'transfer_id'"
    " AS uuid) AND a.agent_id IS NOT NULL AND a.started_at IS NOT NULL ORDER BY a.started_at LIMIT 1) k"
    f" ON e.body#>>'{{target,type}}' IN ('agent', 'team') WHERE e.{_IDS} AND e.kind = 'transfer' AND e.deleted_at IS NULL"
    " UNION ALL SELECT a.conversation_id, a.ended_at, w.type, a.id, a.agent_id, NULL, NULL, NULL, w.text, w.meta, NULL,"
    " NULL, NULL FROM assignment a LEFT JOIN config d ON d.id = a.disposition_id"
    " LEFT JOIN config s ON s.id = a.sub_disposition_id CROSS JOIN LATERAL (VALUES ('disposition', d.body->>'name',"
    " s.body->>'name'), ('wrap_up_note', a.note, NULL)) w (type, text, meta)"
    f" WHERE a.{_IDS} AND a.disposition_id IS NOT NULL AND w.text IS NOT NULL"
    ") x JOIN conversation c ON c.id = x.conversation_id LEFT JOIN config g ON g.id = x.agent_id"
    " LEFT JOIN LATERAL (SELECT coalesce(n.body->>'name', a.agent_id::text) AS name FROM assignment a"
    " LEFT JOIN config n ON n.id = a.agent_id WHERE x.agent_id IS NULL AND a.conversation_id = x.conversation_id"
    " AND a.agent_id IS NOT NULL AND a.started_at <= x.at AND a.role IN ('primary', 'secondary')"
    " ORDER BY a.started_at DESC LIMIT 1) s ON true WHERE (array_position(CAST(:ids AS uuid[]), x.conversation_id),"
    " x.at, x.type, x.id) > (:pos, CAST(:at AS timestamptz), CAST(:type AS text), CAST(:item AS uuid))"
    " ORDER BY array_position(CAST(:ids AS uuid[]), x.conversation_id), x.at, x.type, x.id LIMIT :n"
)


class ExportIn(search.SearchIn):
    columns: list[Literal[SUMMARY]] = []


def _allowed(p: access.Principal, grant: str) -> frozenset[str] | None:
    with read() as db:
        if not access.permits(db, p, grant):
            raise ApiError(403, "download_not_allowed", f"this caller's role does not hold {grant}")
        return access.visible_agents(db, p)


def _pages(p: access.Principal, body: ExportIn, fields: tuple[str, ...] = ()) -> Iterator[list[dict]]:
    paged, left = paging(PAGE), CAP
    while True:
        got = search.run(p, body, paged._replace(limit=min(PAGE, left)), fields)
        yield got["data"]
        left -= len(got["data"])
        if not (got["next_cursor"] and left):
            return
        paged = paging(PAGE, got["next_cursor"])


def _items(ids: list[str]) -> Iterator[list]:
    """Each batch is read on a connection of its own, so a slow download holds no connection."""
    # Each batch rebuilds all of the page's items and keeps those past the last one, so a page of n items
    # costs n*n/BATCH row builds. Push the keyset into each branch of ITEMS if a page ever holds millions.
    after = {"ids": ids, "n": BATCH, "pos": 0, "at": None, "type": None, "item": None}
    while True:
        with read() as db:
            batch = db.execute(ITEMS, after).all()
        yield batch
        if len(batch) < BATCH:
            return
        last = batch[-1]
        after |= {"pos": ids.index(last.conversation_id) + 1, "at": last.at, "type": last.type, "item": last.id}


def _local(at, zone: ZoneInfo) -> str:
    return (at if isinstance(at, datetime) else datetime.fromisoformat(at)).astimezone(zone).isoformat()


def _cell(v):
    if isinstance(v, list):
        return ",".join(v)
    return str(v).lower() if isinstance(v, bool) else v


def _chunk(i) -> str | None:
    if not (i.type in ("message", "note") and (i.body or {}).get("attachments")):
        return i.text
    files = texts.message(i)["attachments"] if i.type == "message" else i.body["attachments"]
    described = [{k: v for k, v in (("file_url", f.get("url")), ("description", i.text)) if v} for f in files]
    return json.dumps([d for d in described if d], indent=4)


def _stream(name: str, head: list[str], pages: Iterator[list[list]]) -> StreamingResponse:
    """The first page is read before the response starts, so a search outage answers 503."""
    first = [head, *next(pages)]

    def chunks() -> Iterator[str]:
        for rows in itertools.chain([first], pages):
            out = io.StringIO()
            csv.writer(out).writerows(rows)
            yield out.getvalue()

    disposition = {"Content-Disposition": f'attachment; filename="{name}"'}
    return StreamingResponse(chunks(), media_type="text/csv", headers=disposition)


@router.post("/conversations/export", response_class=StreamingResponse, responses=CSV)
def export_conversations(body: ExportIn, p: access.Principal = READ):
    """The closed conversations `/conversations/search` finds for this body, one CSV row each in its order, at most
    10,000. `columns` names the search document's fields to write in that order, `conversation_number` last unless
    named, and defaults to every field; `created_at` is ISO 8601 in `time_zone`. Needs the role's `download_list`."""
    _allowed(p, "download_list")
    heads, zone = list(dict.fromkeys([*(body.columns or SUMMARY), "conversation_number"])), ZoneInfo(body.time_zone)
    fields = tuple(sorted(set(heads) - {*search.SUMMARY, "agents", "agent_count", "teams"}))

    def rows() -> Iterator[list[list]]:
        for got in _pages(p, body, fields):
            with read() as db:
                facts = {f.id: f._asdict() for f in db.execute(FACTS, {"ids": [r["id"] for r in got]})}
            for r in got:
                r |= facts[r["id"]] | {"created_at": _local(r["created_at"], zone)}
                r["agent_count"] = len(r["agents"])
            yield [[_cell(r.get(h)) for h in heads] for r in got]

    return _stream("conversations.csv", heads, rows())


@router.post("/conversations/export/content", response_class=StreamingResponse, responses=CSV)
def export_content(body: ExportIn, p: access.Principal = READ):
    """Each item of the closed conversations `/conversations/search` finds for this body, in its order and then in
    time order: messages, notes, voicemails, recordings, call feedback, task activity, each stint an agent took,
    transfers, dispositions and wrap-up notes. A row by an agent the caller does not see is left out, except a
    transfer or a note. Times are in `time_zone`. Needs the role's `download_conversation`."""
    seen, zone = _allowed(p, "download_conversation"), ZoneInfo(body.time_zone)

    def rows() -> Iterator[list[list]]:
        for got in _pages(p, body):
            for batch in _items([r["id"] for r in got]):
                out = []
                for i in batch:
                    if seen is None or i.agent_id is None or i.type in ("transfer", "note") or i.agent_id in seen:
                        item = i._asdict() | {"at": _local(i.at, zone), "text": _chunk(i)}
                        out.append([item[c] for c in CONTENT])
                yield out

    return _stream("conversation-content.csv", list(CONTENT), rows())
