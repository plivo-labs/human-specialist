"""Saved conversation views: a named search filter and column set, private to its owner, shared with agents and
teams, or public to the tenant. Favorites and the order of each list belong to one agent (a `view_prefs` document
keyed by the agent).

Who sees a view is decided when it is read, so an agent created later sees the public ones. A tenant key reads every
view; the views it makes have no owner, and only keys edit them."""

import json
from contextlib import contextmanager
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Response
from pydantic import Field, create_model
from sqlalchemy import or_, select, text
from sqlalchemy.engine import Connection, Row

from hs import access, config, search
from hs.access import Principal
from hs.config import models as m
from hs.conversations.api import ConversationSummary
from hs.core import ApiError, Page, Paging, T, page, paging, read, tx

router = APIRouter()
ADMIN_AGENT, AGENT = access.require("key:admin", "agent"), access.require("agent")
COLUMNS = Literal[tuple(sorted({*ConversationSummary.model_fields, *search.FIELDS} - {"message"}))]
_LOCK = text("SELECT pg_advisory_xact_lock(hashtext(:key))")
TYPES = ("private", "shared", "public")


class ViewIn(m.Body):
    name: m.Name
    type: Literal[TYPES] = "private"
    filter: search.SearchIn = Field(default_factory=search.SearchIn)
    columns: list[COLUMNS] = []
    agents: list[UUID] = []
    teams: list[UUID] = []


class View(ViewIn):
    id: str
    owner_id: str | None
    favorite: bool


class ViewOrder(m.Body):
    ids: list[UUID] = Field(min_length=1)
    type: Literal[*TYPES, "favorite"]


ViewPatch = create_model(
    "ViewPatch", __base__=m.Body, **{n: (f.annotation | None, None) for n, f in ViewIn.model_fields.items()}
)


def _shown(db: Connection, tenant_id, rows: list[Row], prefs: dict) -> list[dict]:
    """The views with their filters read against the tenant's agents and teams: a selection that was deleted is
    dropped from what is returned, and the stored filter stays as it was."""
    filters = [json.loads(r.body["filter"]) for r in rows]
    picks = [(h, k) for f in filters for kind, h, k in search.named_ids(f) if kind in ("agent", "team")]
    ids = {config.as_uuid(x) for h, k in picks for x in search.as_list(h[k])} - {None}
    C = T.config
    q = select(C.c.id).where(C.c.tenant_id == tenant_id, C.c.kind.in_(("agent", "team")), C.c.id.in_(ids))
    alive = {str(i) for i in db.execute(q).scalars()} if ids else set()
    for h, k in picks:
        live = [x for x in search.as_list(h[k]) if str(config.as_uuid(x)) in alive]
        if k == "value":
            h[k] = live
        elif not live:
            del h[k]
    return [
        {**r.body, "id": str(r.id), "filter": f, "favorite": str(r.id) in prefs["favorites"]}
        for r, f in zip(rows, filters, strict=True)
    ]


def _stored(v: ViewIn, owner: str | None) -> dict:
    body = v.model_dump(mode="json", exclude={"filter"})
    if v.type != "shared":
        body |= {"agents": [], "teams": []}
    # The filter is a string, so its queue_id is not a reference that keeps the queue from being deleted.
    return body | {"filter": json.dumps(v.filter.model_dump(mode="json", exclude_unset=True)), "owner_id": owner}


def _allowed(p: Principal, v: ViewIn) -> None:
    if p.role == "agent" and v.type != "private":
        raise ApiError(403, "forbidden", "an agent makes only private views")
    if v.type == "shared" and not (v.agents or v.teams):
        raise ApiError(422, "shared_with_nobody", "a shared view names an agent or a team")
    if v.type == "shared" and p.agent_id in map(str, v.agents):
        raise ApiError(422, "owner_in_shares", "a shared view does not name its owner among its agents")


def _check(db: Connection, p: Principal, body: ViewIn) -> None:
    search.owned(db, p, body.filter or search.SearchIn(), agent=body.agents or [], team=body.teams or [])


def _seen(db: Connection, p: Principal):
    C = T.config
    q = select(C.c.id, C.c.key, C.c.body, C.c.created_at).where(C.c.tenant_id == p.tenant_id, C.c.kind == "view")
    if p.kind != "agent":
        return q
    teams = (config.get(db, p.tenant_id, "agent", p.agent_id) or {}).get("teams", [])
    b = C.c.body
    mine = [b["owner_id"].astext == p.agent_id, b["type"].astext == "public", b["agents"].has_key(p.agent_id)]
    return q.where(or_(*mine, *(b["teams"].has_key(t) for t in teams)))


def _view(db: Connection, p: Principal, rid: UUID, *, owner: bool = False, lock: bool = False) -> Row:
    row = config.found(db, p.tenant_id, "view", rid, lock=lock)
    if db.execute(_seen(db, p).where(T.config.c.id == row.id)).first() is None:
        raise ApiError(403, "view_not_visible", "this view is not shared with the caller")
    if owner and row.body["owner_id"] != p.agent_id:
        raise ApiError(403, "view_not_owned", "only the owner of a view changes it")
    return row


def _prefs(db: Connection, p: Principal) -> tuple[Row | None, dict]:
    q = config.docs(p.tenant_id, "view_prefs").where(T.config.c.key == p.agent_id)
    row = db.execute(q).first() if p.agent_id else None
    return row, {"order": {}, "favorites": [], **(row.body if row else {})}


def _lock(db: Connection, tenant_id, agent_id) -> None:
    db.execute(_LOCK, {"key": f"{tenant_id}:view_prefs:{agent_id}"})


@contextmanager
def _editing(p: Principal, *view_ids: UUID):
    with tx() as db:
        views = config.docs(p.tenant_id, "view").where(T.config.c.id.in_(view_ids)).order_by(T.config.c.id)
        db.execute(views.with_only_columns(T.config.c.id).with_for_update(read=True))
        _lock(db, p.tenant_id, p.agent_id)
        row, prefs = _prefs(db, p)
        yield db, prefs
        if row:
            config.save(db, row.id, prefs)
        else:
            config.add(db, p.tenant_id, "view_prefs", prefs, key=p.agent_id)


def _forget(db: Connection, p: Principal, view_id, before: str, after: ViewIn) -> None:
    sid, C = str(view_id), T.config
    held = or_(C.c.body["favorites"].has_key(sid), *(C.c.body["order"][t].has_key(sid) for t in TYPES))
    docs = config.docs(p.tenant_id, "view_prefs").where(held).order_by(C.c.key)
    # Agents are locked in key order, after the view row, as every writer of preferences takes them.
    for key in db.execute(docs.with_only_columns(C.c.key)).scalars().all():
        _lock(db, p.tenant_id, key)
    named = {str(a) for a in after.agents}
    private = before != "private" and after.type == "private"
    for row in db.execute(docs).all():
        favorite = private or (after.type == "shared" and row.key not in named)
        order = favorite or before != after.type
        body = {
            "favorites": [i for i in row.body["favorites"] if not (favorite and i == sid)],
            "order": {t: [i for i in ids if not (order and i == sid)] for t, ids in row.body["order"].items()},
        }
        if body != {k: row.body[k] for k in body}:
            config.save(db, row.id, row.body | body)


@router.get("/views", response_model=Page[View])
def list_views(favorite: bool = False, p: Principal = ADMIN_AGENT, paged: Paging = Depends(paging)):
    """The views the caller sees, by type: new ones first, then the caller's order of that type. `favorite` lists the
    caller's favorites in theirs."""
    with read() as db:
        rows = db.execute(_seen(db, p)).all()
        _, prefs = _prefs(db, p)
        if favorite:
            rank = {i: n for n, i in enumerate(prefs["favorites"])}
            rows = sorted((r for r in rows if str(r.id) in rank), key=lambda r: rank[str(r.id)])
        else:
            rank = {t: {i: n for n, i in enumerate(prefs["order"].get(t, []))} for t in TYPES}

            def new_first(r: Row) -> tuple:
                n = rank[r.body["type"]].get(str(r.id))
                return TYPES.index(r.body["type"]), n is not None, -r.created_at.timestamp() if n is None else n

            rows.sort(key=new_first)
        out = _shown(db, p.tenant_id, rows, prefs)
    ids = [v["id"] for v in out]
    start = ids.index(paged.after) + 1 if paged.after in ids else 0
    return page(out[start : start + paged.limit + 1], paged)


@router.post("/views", status_code=201, response_model=View)
def create_view(body: ViewIn, p: Principal = ADMIN_AGENT):
    _allowed(p, body)
    with tx() as db:
        config.hold(db, p.tenant_id)
        _check(db, p, body)
        row = config.add(db, p.tenant_id, "view", _stored(body, p.agent_id))
        return _shown(db, p.tenant_id, [row], _prefs(db, p)[1])[0]


@router.put("/views/order", status_code=204)
def order_views(body: ViewOrder, p: Principal = AGENT) -> Response:
    """Replaces the caller's order of one type of view, or its favorites, with these views."""
    ids = list(dict.fromkeys(map(str, body.ids)))
    C = T.config
    with _editing(p, *body.ids) as (db, prefs):
        q = _seen(db, p).where(C.c.id.in_(body.ids)).with_only_columns(C.c.id, C.c.body["type"].astext)
        types = {str(i): t for i, t in db.execute(q)}
        if missing := set(ids) - types.keys():
            raise ApiError(
                422, "unknown_view", "a view in the list is not one the caller sees", {"ids": sorted(missing)}
            )
        if body.type == "favorite":
            prefs["favorites"] = ids
        elif wrong := sorted(i for i in ids if types[i] != body.type):
            raise ApiError(422, "wrong_view_type", f"a view in the list is not {body.type}", {"ids": wrong})
        else:
            prefs["order"][body.type] = ids
    return Response(status_code=204)


@router.get("/views/{rid}", response_model=View)
def get_view(rid: UUID, p: Principal = ADMIN_AGENT):
    with read() as db:
        return _shown(db, p.tenant_id, [_view(db, p, rid)], _prefs(db, p)[1])[0]


@router.get("/views/{rid}/conversations", response_model=Page[search.Hit])
def run_view(rid: UUID, p: Principal = ADMIN_AGENT, paged: Paging = Depends(paging)):
    """Closed conversations matching the view's stored filter, as `/conversations/search` returns them with its
    columns as `fields`, so an agent or team deleted since still matches."""
    with read() as db:
        row = _view(db, p, rid)
    shown = [c for c in row.body["columns"] if c in search.SHOWN]
    filt = search.SearchIn.model_validate(json.loads(row.body["filter"]), context=m.STORED)
    return search.run(p, filt, paged, shown=shown, skip_owned=True)


@router.patch("/views/{rid}", response_model=View)
def patch_view(rid: UUID, body: ViewPatch, p: Principal = ADMIN_AGENT):
    """Merges the fields sent; a `filter` replaces the stored one."""
    with tx() as db:
        config.hold(db, p.tenant_id)
        row = _view(db, p, rid, owner=True, lock=True)
        _check(db, p, body)
        stored = {**row.body, "filter": json.loads(row.body["filter"])} | body.model_dump(
            mode="json", exclude_unset=True
        )
        merged = config.validate_model(ViewIn, {k: stored[k] for k in ViewIn.model_fields})
        _allowed(p, merged)
        saved = config.save(db, row.id, _stored(merged, p.agent_id))
        _forget(db, p, saved.id, row.body["type"], merged)
        return _shown(db, p.tenant_id, [saved], _prefs(db, p)[1])[0]


@router.delete("/views/{rid}", status_code=204)
def delete_view(rid: UUID, p: Principal = ADMIN_AGENT) -> Response:
    with tx() as db:
        config.hold(db, p.tenant_id)
        db.execute(T.config.delete().where(T.config.c.id == _view(db, p, rid, owner=True, lock=True).id))
    return Response(status_code=204)


@router.put("/views/{rid}/favorite", response_model=View)
def favorite_view(rid: UUID, p: Principal = AGENT):
    """Puts the view first among the caller's favorites; 400 if it already is one."""
    with _editing(p, rid) as (db, prefs):
        row = _view(db, p, rid)
        if str(row.id) in prefs["favorites"]:
            raise ApiError(400, "already_favorite", "the view is already a favorite")
        prefs["favorites"].insert(0, str(row.id))
        return _shown(db, p.tenant_id, [row], prefs)[0]


@router.delete("/views/{rid}/favorite", status_code=204)
def unfavorite_view(rid: UUID, p: Principal = AGENT) -> Response:
    with _editing(p, rid) as (db, prefs):
        row = _view(db, p, rid)
        if str(row.id) not in prefs["favorites"]:
            raise ApiError(400, "not_favorite", "the view is not a favorite")
        prefs["favorites"].remove(str(row.id))
    return Response(status_code=204)
