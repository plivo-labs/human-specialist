"""Tenants and their configuration documents: queues, hours, menus, numbers, teams, skills, statuses,
dispositions and settings.

Every document is one `config` row of a kind. A body names other documents by id; writes check those ids, reads
tolerate a missing one. `key` is the lookup a kind has besides its id: a number's E.164, an agent's SIP username,
a status's system role (`online` is the sign-in status, `offline` the sign-out one)."""

import json
import uuid
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from functools import partial
from types import SimpleNamespace
from typing import Annotated
from uuid import UUID
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, Path, Response
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, ValidationError, create_model
from redis import RedisError
from sqlalchemy import delete, func, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Connection, Row

from hs import access, events, hot, jobs, plivo
from hs.access import Principal, live_tenant
from hs.config import models as m
from hs.config.provision import intend, kept, torn_down, unmade
from hs.core import PUBLIC, ApiError, Page, Paging, T, after_commit, page, paging, read, settings, tx

router = APIRouter()
OP, ADMIN, ADMIN_AGENT = (access.require(*g) for g in (("op",), ("key:admin", "admin"), ("key:admin", "agent")))
# name -> availability; the first two are the sign-in and sign-out statuses.
STATUSES = {"Available": True, "Offline": False, "Idle": False, "Busy": False, "After Contact Work": False}
STATUSES |= {"Away": False, "Break": False}
# Body fields that name another document, and its kind.
REFS = {"agents": "agent", "agent_id": "agent", "teams": "team", "skill_id": "skill", "queue_id": "queue"}
REFS |= {"menu_id": "menu", "business_hours_id": "hours", "parent_disposition_id": "disposition", "status_id": "status"}
REFS |= {"holiday_group_id": "holiday_group"}
SOFT_DELETED_KINDS = ("queue",)
_SERIAL = text("SELECT pg_advisory_xact_lock(hashtext(:key))")
_SHARED = text("SELECT pg_advisory_xact_lock_shared(hashtext(:key))")
# Takes a deleted team or skill off every agent and queue that names it, locking those rows in id order.
_STRIP = text(
    "UPDATE config c SET updated_at = now(), body = jsonb_set(c.body, ARRAY[:field], coalesce((SELECT jsonb_agg(e)"
    " FROM jsonb_array_elements(c.body->:field) e WHERE coalesce(e->>'skill_id', e #>> '{}') <> :id), '[]'))"
    " FROM (SELECT id FROM config WHERE tenant_id = :tenant AND kind IN ('agent', 'queue') AND EXISTS (SELECT 1"
    "  FROM jsonb_array_elements(body->:field) e WHERE coalesce(e->>'skill_id', e #>> '{}') = :id)"
    "  ORDER BY id FOR UPDATE) hit WHERE c.id = hit.id"
)
_OUTSIDE = text(
    "SELECT 1 FROM config c, jsonb_array_elements(c.body->'skills') e WHERE c.tenant_id = :tenant AND c.kind = 'agent'"
    " AND e->>'skill_id' = :id AND (e->>'level')::int NOT BETWEEN :lo AND :hi LIMIT 1"
)
_LEVELS = text(
    "UPDATE config c SET updated_at = now(), body = jsonb_set(c.body, '{skills}', (SELECT jsonb_agg(CASE WHEN"
    " e->>'skill_id' = :id AND (e->>'level' IS NULL OR NOT :fill) THEN e || jsonb_build_object('level', CAST(:level AS"
    " int)) ELSE e END) FROM jsonb_array_elements(c.body->'skills') e)) WHERE c.tenant_id = :tenant AND c.kind = 'agent'"
    " AND c.body->'skills' @> jsonb_build_array(jsonb_build_object('skill_id', CAST(:id AS text)))"
)
# The other documents of the tenant that name :id as a queue or menu, at any depth.
_USED = text(
    "SELECT kind, id, key FROM config WHERE tenant_id = :tenant AND id <> :id AND NOT body ? 'deleted'"
    " AND (jsonb_path_query_array(body, '$.**.queue_id') || jsonb_path_query_array(body, '$.**.menu_id'))"
    " @> jsonb_build_array(cast(:id AS text)) ORDER BY kind, id LIMIT 50"
)
# What still needs queue :q: the conversations queued in it, and the open tasks a task_route job will enqueue in it.
_HELD = text(
    "SELECT (SELECT count(*) FROM conversation WHERE tenant_id = :t AND queue_id = :q AND state = 'queued') AS waiting,"
    " (SELECT count(*) FROM jobs j JOIN task k ON k.id = CAST(j.payload->>'task_id' AS uuid)"
    "  JOIN conversation c ON c.id = k.conversation_id AND c.state = 'open' WHERE j.kind = 'task_route'"
    "  AND k.tenant_id = :t AND k.destination->>'queue_id' = CAST(:q AS text)) AS scheduled"
)
_USERS = {
    "queue": (None, "a number, queue or menu names this"),
    "menu": (None, "a number, queue or menu names this"),
    "hours": (("number", "business_hours_id"), "a number's entry point uses these business hours"),
    "holiday_group": (("hours", "holiday_group_id"), "business hours select holidays from this group"),
}


def as_uuid(value) -> UUID | None:
    try:
        return value if isinstance(value, UUID) else UUID(str(value))
    except ValueError:
        return None


def docs(tenant_id, kind: str):
    C = T.config
    q = select(C.c.id, C.c.key, C.c.body).where(C.c.tenant_id == tenant_id, C.c.kind == kind)
    return q.where(C.c.body["deleted"].as_boolean().isnot(True)) if kind in SOFT_DELETED_KINDS else q


def _row(db: Connection, tenant_id, kind: str, id, *, lock: bool = False) -> Row | None:
    q = docs(tenant_id, kind).where(T.config.c.id == as_uuid(id))
    return db.execute(q.with_for_update() if lock else q).first()


def found(db: Connection, tenant_id, kind: str, id, *, lock: bool = False) -> Row:
    if (row := _row(db, tenant_id, kind, id, lock=lock)) is None:
        raise ApiError(404, f"{kind}_not_found", f"no such {kind}")
    return row


class _Held(dict):
    """A hold's memo: what it read from Postgres, which its hold keeps steady until its own write to config."""


def _cached(db: Connection, tenant_id, key: str, load: Callable[[], object], *, fresh: bool = False):
    """`load()`, read once per transaction through the tenant's cache (`hot.cached`), or from Postgres under a hold.
    `fresh` reads Postgres, and the rest of the transaction reads what it read."""
    if (memo := db.info.get("memo")) is None:
        return load()
    if (k := f"{tenant_id}:{key}") not in memo or fresh:
        memo[k] = json.dumps(load()) if fresh or isinstance(memo, _Held) else hot.cached(tenant_id, key, load)
    return json.loads(memo[k])


def get(db: Connection, tenant_id, kind: str, id, *, lock: bool = False) -> dict | None:
    """The tenant's `kind` document as stored, with its `id`, and for business hours the holidays they select from
    their group; None for an unknown or malformed id. `lock` reads Postgres and holds its row to the end of the
    transaction (queue admission)."""

    def load() -> dict | None:
        row = _row(db, tenant_id, kind, id, lock=lock)
        return None if row is None else {"id": str(row.id), **row.body}

    doc = _cached(db, tenant_id, f"{kind}:{as_uuid(id)}", load, fresh=lock)
    if doc and doc.get("holiday_group_id"):
        group = get(db, tenant_id, "holiday_group", doc["holiday_group_id"])
        chosen = [h for h in group["holidays"] if h["date"] in doc["selected_holidays"]]
        doc = doc | {"holidays": doc["holidays"] + chosen}
    return doc


def by_key(db: Connection, tenant_id, kind: str, key: str) -> tuple[str, dict] | None:
    def load() -> list | None:
        row = db.execute(docs(tenant_id, kind).where(T.config.c.key == key)).first()
        return None if row is None else [str(row.id), row.body]

    return (found := _cached(db, tenant_id, f"{kind}={key}", load)) and tuple(found)


def tenant_settings(db: Connection, tenant_id, *, fresh: bool = False) -> m.Stored:
    q = select(T.tenant.c.settings).where(T.tenant.c.id == tenant_id)
    stored = _cached(db, tenant_id, "settings", lambda: db.execute(q).scalar(), fresh=fresh)
    return m.Stored.model_validate(stored, context=m.STORED)


def is_open(hours: dict) -> bool:
    """Open when a slot of the local day has start <= now < end; on a holiday only its own slots count."""
    local = datetime.now(UTC).astimezone(ZoneInfo(hours["time_zone"]))
    t, today = local.strftime("%H:%M"), local.date().isoformat()
    holiday = next((h for h in hours["holidays"] if h["date"] == today), None)
    slots = holiday["slots"] if holiday else [s for s in hours["slots"] if s["day"] == m.DAYS[local.weekday()]]
    return any(s["start"] <= t < s["end"] for s in slots)


def _refs(value, at: tuple = ()) -> Iterator[tuple[tuple, str, object]]:
    if isinstance(value, BaseModel):
        for name in type(value).model_fields:
            v, here = getattr(value, name), (*at, name)
            if name in REFS and v is not None:
                yield from ((here, REFS[name], i) for i in (v if isinstance(v, list) else [v]))
            else:
                yield from _refs(v, here)
    elif isinstance(value, list | dict):
        for k, v in value.items() if isinstance(value, dict) else enumerate(value):
            yield from _refs(v, (*at, k))


def owned(db: Connection, p: Principal, kind: str, ids: Iterable) -> None:
    """422 foreign_id unless every id names a `kind` document of the caller's tenant."""
    if None in (parsed := {as_uuid(i) for i in ids}):
        raise ApiError(422, "foreign_id", "an id in the body names nothing of this tenant", {"kind": kind})
    access.owned(db, p, kind, parsed)


def check(db: Connection, p: Principal, body: BaseModel, stored: BaseModel | None = None) -> None:
    """422 foreign_id unless every id the body names, at any depth, is a document of the caller's tenant."""
    wanted: dict[str, set] = {}
    kept = set(_refs(stored))
    for path, kind, value in _refs(body):
        if (path, kind, value) not in kept:
            wanted.setdefault(kind, set()).add(value)
    for kind, ids in wanted.items():
        owned(db, p, kind, ids)
        if kind in SOFT_DELETED_KINDS and (
            gone := sorted(str(i) for i in ids if get(db, p.tenant_id, kind, i) is None)
        ):
            raise ApiError(422, "foreign_id", "an id in the body names a deleted document", {"kind": kind, "ids": gone})
    idle = body.idle.status_id if isinstance(body, m.Settings) else None
    if idle and (get(db, p.tenant_id, "status", idle) or {}).get("availability"):
        raise ApiError(422, "idle_status_available", "the idle status must be an unavailable status")
    if isinstance(body, m.HoursIn) and body.holiday_group_id:
        dates = {h["date"] for h in get(db, p.tenant_id, "holiday_group", body.holiday_group_id)["holidays"]}
        if {d.isoformat() for d in body.selected_holidays} - dates:
            raise ApiError(422, "invalid_holiday", "a selected holiday is not in the holiday group")


# One config writer per tenant at a time; take per-document row locks if admin write rates ever matter.
@contextmanager
def write(p: Principal, tenant_id=None) -> Iterator[Connection]:
    """The transaction of `p`'s write to the tenant's config, settings or row (`tenant_id`, else `p`'s). Writes take
    turns per tenant, so the ids a body names stay valid until it commits, a delete sees every write that named its
    row, and an admin's session is checked again on its turn (`access.still_admin`). The lock key is hashed: a
    collision only waits."""
    tenant_id = tenant_id or p.tenant_id
    with tx() as db:
        db.execute(_SERIAL, {"key": f"{tenant_id}:config"})
        access.still_admin(db, p)
        # The rest of this transaction reads config from Postgres: it sees its own writes, and nothing it read
        # outlives a rollback.
        db.info["memo"] = None
        after_commit(db, lambda: hot.changed(tenant_id))
        yield db
        for kind in ("config_changed", "config_rounds"):  # job rows come last, after any presence or config row
            jobs.schedule(db, kind, {"tenant_id": str(tenant_id)}, key=str(tenant_id))


@jobs.handler("config_changed", backoff=(1,), parks=False)
def _changed(job: Row) -> None:
    try:
        hot.changed(job.payload["tenant_id"])
    except RedisError as e:
        raise jobs.NotYet(f"Redis refused the new generation: {e}") from e


def hold(db: Connection, tenant_id) -> None:
    """For a write outside config that names config ids: as its first lock, hold the tenant's config steady to the
    end of the transaction. `write()` waits for it; holders do not wait for each other."""
    db.execute(_SHARED, {"key": f"{tenant_id}:config"})
    db.info["memo"] = _Held()


def validate_model(model: type[BaseModel], data: dict) -> BaseModel:
    try:
        return model.model_validate(data)
    except ValidationError as e:
        raise RequestValidationError(e.errors(include_context=False)) from None


def page_rows(db: Connection, q, paged: Paging, key, parse: Callable = UUID) -> list[Row]:
    """`paged.limit + 1` rows of `q` in `key` order after the cursor, which holds the last `key` seen."""
    try:
        q = q if paged.after is None else q.where(key > parse(paged.after))
    except ValueError as e:
        raise ApiError(422, "invalid_cursor", "cursor is not one this API returned") from e
    return db.execute(q.order_by(key).limit(paged.limit + 1)).all()


def add(db: Connection, tenant_id, kind: str, body: dict, key: str | None = None, id=None) -> Row:
    C = T.config
    row = insert(C).values(id=id or uuid.uuid4(), tenant_id=tenant_id, kind=kind, key=key, body=body)
    made = db.execute(row.on_conflict_do_nothing(index_elements=[C.c.id]).returning(C.c.id, C.c.key, C.c.body)).first()
    if made is None:
        raise ApiError(409, "id_taken", "this id is in use", {"id": str(id)})
    (db.info.get("memo") or {}).clear()  # a hold's memo stops at its own write
    return made


def save(db: Connection, id, body: dict) -> Row:
    C = T.config
    saved = update(C).where(C.c.id == id).values(body=body, updated_at=func.now())
    (db.info.get("memo") or {}).clear()  # a hold's memo stops at its own write
    return db.execute(saved.returning(C.c.id, C.c.key, C.c.body)).one()


def _drop(db: Connection, kind: str, row: Row) -> None:
    if kind in SOFT_DELETED_KINDS:
        save(db, row.id, row.body | {"deleted": True})
    else:
        db.execute(delete(T.config).where(T.config.c.id == row.id))


def crud(
    kind,
    path,
    model,
    out,
    *,
    stored=None,
    patch=None,
    param="rid",
    typ=str,
    reader=ADMIN,
    view=None,
    on_create=None,
    on_patch=None,
    on_delete=None,
    named=False,
):
    """List, create, read, merge-patch and delete for one kind of document; `view(tenant_id, id)` adds fields. A
    `named` kind's name is unique in its tenant."""
    if named:
        on_create = on_patch = partial(_name_free, kind)
    Id = Annotated[typ, Path(alias=param)]
    one_name = {"hours": "Business Hours", "status": "Agent Status"}.get(kind, kind.replace("_", " ").title())
    many = path[1:].replace("-", " ").title()
    fields = {name: (f.annotation | None, None) for name, f in model.model_fields.items() if name != "id"}
    patch = patch or create_model(f"{model.__name__.removesuffix('In')}Patch", __base__=m.Body, **fields)

    def show(p: Principal, row: Row) -> dict:
        return {**row.body, "id": str(row.id), **(view(p.tenant_id, row.id) if view else {})}

    @router.get(path, response_model=Page[out], summary=f"List {many}")
    def listed(p: Principal = reader, paged: Paging = Depends(paging)):
        with read() as db:
            return page([show(p, r) for r in page_rows(db, docs(p.tenant_id, kind), paged, T.config.c.id)], paged)

    @router.post(path, status_code=201, response_model=out, summary=f"Create {one_name}")
    def create(body: model, p: Principal = ADMIN):
        with write(p) as db:
            check(db, p, body)
            if on_create:
                on_create(db, p, body)
            row = add(db, p.tenant_id, kind, body.model_dump(mode="json"), id=body.id)
            access.created(db, str(row.id))
            return show(p, row)

    access.VIEWS[f"POST {PUBLIC}{path}"] = lambda db, p, rid: show(p, found(db, p.tenant_id, kind, rid))

    @router.get(f"{path}/{{{param}}}", response_model=out, summary=f"Get {one_name}")
    def one(rid: Id, p: Principal = reader):
        with read() as db:
            return show(p, found(db, p.tenant_id, kind, rid))

    @router.patch(f"{path}/{{{param}}}", response_model=out, summary=f"Patch {one_name}")
    def change(rid: Id, body: patch, p: Principal = ADMIN):
        with write(p) as db:
            row = found(db, p.tenant_id, kind, rid, lock=True)
            merged = validate_model(stored or model, row.body | body.model_dump(mode="json", exclude_unset=True))
            check(db, p, merged)
            if on_patch:
                on_patch(db, p, merged, row)
            return show(p, save(db, row.id, merged.model_dump(mode="json")))

    @router.delete(f"{path}/{{{param}}}", status_code=204, summary=f"Delete {one_name}")
    def remove(rid: Id, p: Principal = ADMIN) -> Response:
        with write(p) as db:
            # A status's agents' presence rows come before its own row (_status_deleted); the delete then locks it.
            row = found(db, p.tenant_id, kind, rid, lock=kind != "status")
            if kind in _USERS:
                _unused(db, p.tenant_id, kind, row.id)
            if on_delete:
                on_delete(db, p, row)
            _drop(db, kind, row)
        return Response(status_code=204)


def _strip(field: str, db: Connection, p: Principal, row: Row) -> None:
    db.execute(_STRIP, {"tenant": p.tenant_id, "field": field, "id": str(row.id)})


def _skill_changed(db: Connection, p: Principal, now: m.SkillIn, row: Row) -> None:
    """A required level range may not leave an agent outside it. Switching levels on gives every agent without one
    the minimum; switching them off clears them."""
    q = {"tenant": p.tenant_id, "id": str(row.id), "lo": now.min_level, "hi": now.max_level}
    if now.is_level_required and db.execute(_OUTSIDE, q).first():
        raise ApiError(409, "skill_level_outside_range", "an agent has this skill at a level outside the range")
    if now.is_level_required != row.body.get("is_level_required", True):
        db.execute(
            _LEVELS, q | {"level": now.min_level if now.is_level_required else None, "fill": now.is_level_required}
        )


def _status_changed(db: Connection, p: Principal, now: m.PresenceStatusIn, row: Row) -> None:
    if row.body.get("default") and now.availability != row.body["availability"]:
        raise ApiError(409, "status_default", "a default status keeps its availability")
    turns_available = now.availability and not row.body["availability"]
    if turns_available and tenant_settings(db, p.tenant_id).idle.status_id == str(row.id):
        raise ApiError(409, "status_in_use", "the idle status must stay unavailable")


def _unused(db: Connection, tenant_id, kind: str, id) -> None:
    named, why = _USERS[kind]
    if named is None:
        users = db.execute(_USED, {"tenant": tenant_id, "id": str(id)}).all()
    else:
        C, (by, field) = T.config, named
        q = select(C.c.kind, C.c.id, C.c.key).where(C.c.tenant_id == tenant_id, C.c.kind == by)
        users = db.execute(q.where(C.c.body[field].astext == str(id)).order_by(C.c.id).limit(50)).all()
    if users:
        used_by = [{"kind": u.kind, "id": str(u.id)} | ({"number": u.key} if u.kind == "number" else {}) for u in users]
        raise ApiError(409, f"{kind}_in_use", why, {"used_by": used_by})


def _empty(db: Connection, p: Principal, row: Row) -> None:
    counts = db.execute(_HELD, {"t": p.tenant_id, "q": str(row.id)}).one()._mapping
    if held := {k: n for k, n in counts.items() if n}:
        raise ApiError(409, "queue_in_use", "conversations are waiting in this queue or scheduled for it", held)


def _fallback(db: Connection, tenant_id, available: bool) -> Row:
    """The default status that takes over from a deleted one: Available, or for an unavailable one Break."""
    C = T.config
    is_default = docs(tenant_id, "status").where(C.c.body["default"].as_boolean())
    seeded = C.c.key == "online" if available else C.c.key.is_(None)
    q = is_default.where(seeded, C.c.body["availability"].as_boolean() == available)
    return db.execute(q.order_by(C.c.body["name"].astext != "Break", _sequence())).first()


def _status_deleted(db: Connection, p: Principal, row: Row) -> None:
    """A default status stays. Any other goes, and the agents in it, the sign-in and sign-out settings and the RONA
    status that name it move to a default one. Every presence row of the tenant is locked first, in agent order, so
    no agent moves into the status meanwhile; the status row is locked after them, by its delete, as a status change
    takes presence before status."""
    if row.body.get("default"):
        raise ApiError(409, "status_default", "a default status cannot be deleted")
    P = T.presence
    db.execute(select(P.c.agent_id).where(P.c.tenant_id == p.tenant_id).order_by(P.c.agent_id).with_for_update())
    sid, s = str(row.id), tenant_settings(db, p.tenant_id)
    to = _fallback(db, p.tenant_id, row.body["availability"])
    moved = {k: None for k in ("sign_in", "sign_out") if getattr(s, k) and getattr(s, k).status_id == sid}
    if s.idle.status_id == sid:
        moved["idle"] = s.idle.model_dump(mode="json") | {"status_id": str(to.id)}
    if s.rona.status_id == sid:
        moved["rona"] = s.rona.model_dump(mode="json") | {"status_id": str(to.id)}
    if moved:
        _store(db, p.tenant_id, **moved)
    moving = update(P).where(P.c.status_id == row.id).values(status_id=to.id, since=func.now())
    for agent_id in sorted(db.execute(moving.returning(P.c.agent_id)).scalars()):
        data = {
            "from_status": {"id": sid, "name": row.body["name"]},
            "to_status": {"id": str(to.id), "name": to.body["name"]},
            "available": to.body["availability"],
        }
        events.emit(db, "agent.status_changed", str(p.tenant_id), data | {"reason": "admin"}, agent_id=str(agent_id))
        jobs.schedule(db, "route_agent", {"tenant_id": str(p.tenant_id), "agent_id": str(agent_id)}, key=str(agent_id))


def _name_free(kind: str, db: Connection, p: Principal, body: m.NameIn, row: Row | None = None) -> None:
    taken = docs(p.tenant_id, kind).where(T.config.c.body["name"].astext == body.name)
    if row is not None:
        taken = taken.where(T.config.c.id != row.id)
    if db.execute(taken.limit(1)).first():
        raise ApiError(409, f"{kind}_name_taken", f"a {kind} has this name")


@router.post("/agent-statuses/reorder", status_code=204)
def reorder_statuses(body: m.Reorder, p: Principal = ADMIN) -> Response:
    """The listed statuses take the places they already hold, in the order given; the rest stay put."""
    C = T.config
    with write(p) as db:
        owned(db, p, "status", body.ids)
        ids = list(dict.fromkeys(map(as_uuid, body.ids)))
        rows = db.execute(docs(p.tenant_id, "status").where(C.c.id.in_(ids)).order_by(C.c.id).with_for_update())
        for sid, seq in zip(ids, sorted(r.body["sequence"] for r in rows), strict=True):
            db.execute(update(C).where(C.c.id == sid).values(body=C.c.body.concat({"sequence": seq})))
    return Response(status_code=204)


def _enqueue_url(tenant_id, queue_id) -> dict:
    return {"enqueue_url": f"{settings.public_base_url}{PUBLIC}/queues/{queue_id}/enqueue"}


def _menu_url(tenant_id, menu_id) -> dict:
    return {"menu_url": f"{settings.public_base_url}/plivo/{tenant_id}/actions/menu?menu_id={menu_id}"}


crud("queue", "/queues", m.QueueIn, m.Queue, param="row_id", typ=UUID, view=_enqueue_url, named=True, on_delete=_empty)
crud("hours", "/business-hours", m.HoursIn, m.Hours, param="row_id", typ=UUID)
crud("holiday_group", "/holiday-groups", m.HolidayGroupIn, m.HolidayGroup, param="row_id", typ=UUID)
crud("menu", "/menus", m.MenuIn, m.Menu, stored=m.StoredMenu, param="row_id", typ=UUID, view=_menu_url)
crud("team", "/teams", m.NameIn, m.Named, patch=m.NamePatch, reader=ADMIN_AGENT, on_delete=partial(_strip, "teams"))
crud(
    "skill",
    "/skills",
    m.SkillIn,
    m.SkillOut,
    reader=ADMIN_AGENT,
    on_delete=partial(_strip, "skills"),
    on_patch=_skill_changed,
)
crud("destination", "/transfer-destinations", m.DestinationIn, m.Destination, reader=ADMIN_AGENT, named=True)
STATUS = (m.PresenceStatusIn, m.PresenceStatus)
crud(
    "status",
    "/agent-statuses",
    *STATUS,
    patch=m.PresenceStatusPatch,
    stored=m.StoredStatus,
    reader=ADMIN_AGENT,
    on_create=partial(_name_free, "status"),
    on_delete=_status_deleted,
    on_patch=_status_changed,
)


@router.get("/business-hours/{row_id}/status", response_model=m.HoursStatus)
def hours_status(row_id: UUID, p: Principal = ADMIN):
    with read() as db:
        found(db, p.tenant_id, "hours", row_id)
        return {"open": is_open(get(db, p.tenant_id, "hours", row_id))}


@router.get("/roles", response_model=Page[m.Named])
def list_roles(_: Principal = ADMIN, paged: Paging = Depends(paging)):
    """The three fixed roles; a role's id is its name."""
    rows = [{"id": r, "name": r} for r in m.ROLES if paged.after is None or r > paged.after]
    return page(rows[: paged.limit + 1], paged)


def _sequence():
    return T.config.c.body["sequence"].as_integer()


def merge_patch(target, patch):
    """RFC 7396: objects merge key by key, null removes a key, any other value replaces."""
    if not isinstance(patch, dict):
        return patch
    merged = dict(target) if isinstance(target, dict) else {}
    for key, value in patch.items():
        if value is None:
            merged.pop(key, None)
        else:
            merged[key] = merge_patch(merged.get(key), value)
    return merged


def _public(s: m.Stored) -> dict:
    return s.model_dump(mode="json", include=set(m.Settings.model_fields))


def _store(db: Connection, tenant_id, **fields) -> None:
    db.execute(update(T.tenant).where(T.tenant.c.id == tenant_id).values(settings=T.tenant.c.settings.concat(fields)))


@router.get("/settings", response_model=m.Settings)
def get_settings(p: Principal = ADMIN):
    with read() as db:
        return _public(tenant_settings(db, p.tenant_id))


@router.patch("/settings", response_model=m.Settings)
def patch_settings(body: dict, p: Principal = ADMIN):
    """A JSON merge patch (RFC 7396) over the settings body; the whole result is validated before anything is saved."""
    with write(p) as db:
        held = select(T.tenant.c.settings).where(T.tenant.c.id == p.tenant_id).with_for_update(key_share=True)
        stored = _public(m.Stored.model_validate(db.execute(held).scalar(), context=m.STORED))
        new = validate_model(m.Settings, merge_patch(stored, body))
        check(db, p, new, m.Settings.model_validate(stored))
        _store(db, p.tenant_id, **new.model_dump(mode="json"))
        if "capacity" in body:  # the sweep checks each agent's `full` against the new caps
            jobs.schedule(db, "sweep", {}, key="sweep")
    return new


def _seed(db: Connection, tenant_id: UUID, app_id: str | None) -> None:
    statuses = {name: uuid.uuid4() for name in STATUSES}
    for (name, available), key in zip(STATUSES.items(), ("online", "offline", *[None] * 5), strict=True):
        doc = {"name": name, "availability": available, "sequence": list(STATUSES).index(name), "default": True}
        add(db, tenant_id, "status", doc, key, statuses[name])
    system = {name: uuid.uuid4() for name in m.SYSTEM_DISPOSITIONS}
    for n, name in enumerate(m.SYSTEM_DISPOSITIONS, 1):
        doc = {"name": name, "parent_id": None, "note_required": False, "system": True, "sequence": n}
        doc |= {"answered": name in m.ANSWERED}
        add(db, tenant_id, "disposition", doc, id=system[name])
    status_map = {d: {r: str(system[n]) for r, n in names.items()} for d, names in m.STATUS_MAP_DEFAULTS.items()}
    doc = m.Stored(
        rona={"status_id": str(statuses["Break"])}, idle={"status_id": str(statuses["Idle"])}, status_map=status_map
    ).model_dump(mode="json")
    db.execute(update(T.tenant).where(T.tenant.c.id == tenant_id).values(settings=doc, plivo_app_id=app_id))


def _tenant(row: Row) -> dict:
    return {"id": str(row.id)} | {k: getattr(row, k) for k in ("name", "plivo_auth_id", "plivo_app_id", "created_at")}


def _exists(tenant_id) -> ApiError:
    return ApiError(409, "tenant_exists", "this Plivo account is already a tenant", {"tenant_id": str(tenant_id)})


@router.post("/tenants", status_code=201, response_model=m.Registered)
def register(body: m.TenantIn, _: Principal = OP):
    """Registers a Plivo account, or a sub-account, by its Auth ID and Auth Token. Checks the credentials with Plivo,
    points a new Plivo application at HS unless `plivo_app` is false, and seeds the tenant."""
    t = T.tenant
    held = select(t.c.id).where(t.c.plivo_auth_id == body.plivo_auth_id)
    with read() as db:
        if existing := db.execute(held).scalar():
            raise _exists(existing)
    account = {"plivo_auth_id": body.plivo_auth_id, "plivo_token_enc": plivo.encrypt(body.plivo_auth_token)}
    plivo.rest(SimpleNamespace(**account), "GET", "")
    fallback = {"fallback_answer_url": url, "fallback_method": "POST"} if (url := body.fallback_answer_url) else {}
    tenant_id, app_id = uuid.uuid4(), None
    intent = body.plivo_app and intend(tenant_id, "Application", f"hs-{tenant_id}", **account)
    try:
        with tx() as db:
            row = insert(t).values(id=tenant_id, name=body.name, **account)
            if (tenant := db.execute(row.on_conflict_do_nothing().returning(*t.c)).first()) is None:
                raise _exists(db.execute(held).scalar())
            app_id = plivo.register_app(tenant, **fallback) if intent else None
            _seed(db, tenant.id, app_id)
            key = access.new_key(db, tenant.id)
            kept(db, intent)
            after_commit(db, lambda: hot.rebuilt(tenant.id))
    except Exception:
        unmade(intent, app_id and f"Application/{app_id}/")
        raise
    return {"tenant_id": str(tenant.id), "plivo_app_id": app_id, "api_key": key["api_key"]}


@router.get("/tenants", response_model=Page[m.Tenant])
def list_tenants(_: Principal = OP, paged: Paging = Depends(paging)):
    t = T.tenant
    with read() as db:
        rows = page_rows(db, select(t).where(t.c.deleted_at.is_(None)), paged, t.c.id)
    return page([_tenant(r) for r in rows], paged)


@router.get("/tenants/{tenant_id}", response_model=m.Tenant)
def get_tenant(tenant_id: UUID, _: Principal = OP):
    with read() as db:
        return _tenant(live_tenant(db, tenant_id))


@router.patch("/tenants/{tenant_id}", response_model=m.Tenant)
def patch_tenant(tenant_id: UUID, body: m.TenantPatch, op: Principal = OP):
    """Rename, replace the Plivo token (checked with Plivo first) and re-apply the app URLs from PUBLIC_BASE_URL to a
    tenant's application."""
    t = T.tenant
    with read() as db:
        tenant = SimpleNamespace(**live_tenant(db, tenant_id)._mapping)
    changes = {"name": body.name or t.c.name}
    if body.plivo_auth_token:
        changes["plivo_token_enc"] = tenant.plivo_token_enc = plivo.encrypt(body.plivo_auth_token)
        plivo.rest(tenant, "GET", "")
    if tenant.plivo_app_id:
        plivo.register_app(tenant)
    with write(op, tenant_id) as db:
        return _tenant(db.execute(update(t).where(t.c.id == tenant_id).values(**changes).returning(*t.c)).one())


@router.delete("/tenants/{tenant_id}", status_code=204)
def delete_tenant(tenant_id: UUID, op: Principal = OP) -> Response:
    """Refuse every credential of the tenant and cancel its pending Plivo commands, then remove the endpoints and
    application HS made at Plivo."""
    with write(op, tenant_id) as db:
        tenant = live_tenant(db, tenant_id)
        db.execute(update(T.tenant).where(T.tenant.c.id == tenant_id).values(deleted_at=func.now()))
        access.revoke(db, tenant_id)
        torn_down(db, tenant)
    return Response(status_code=204)


NumberPath = Annotated[str, Path(pattern=f"{m.E164_PATTERN}|{m.EMAIL_PATTERN}")]


def _number(row: Row) -> dict:
    return {**row.body, "id": str(row.id), "number": row.key}


def _one_number(db: Connection, p: Principal, number: str) -> Row:
    if (row := db.execute(docs(p.tenant_id, "number").where(T.config.c.key == number)).first()) is None:
        raise ApiError(404, "number_not_found", "no entry point for this number")
    return row


@router.put("/numbers/{number}", response_model=m.EntryPoint)
def put_number(number: NumberPath, body: m.EntryPointIn, repoint: bool = True, p: Principal = ADMIN):
    """Checks at Plivo that the number is the Plivo account's, stores it, then points it at the tenant's application
    unless `repoint` is false or the tenant has none. A write that is refused changes nothing at Plivo; if Plivo
    refuses the repoint, the number stays stored, and putting it again retries. An email address, a sender of the
    tenant's agent emails, is stored as it is."""
    digits, phone = number.removeprefix("+"), "@" not in number
    with read() as db:
        check(db, p, body)
        tenant = live_tenant(db, p.tenant_id)
    if phone:
        plivo.held(tenant, f"Number/{digits}/", "number")
    C = T.config
    row = insert(C).values(tenant_id=p.tenant_id, kind="number", key=number, body=body.model_dump(mode="json"))
    keyed = {"index_elements": [C.c.tenant_id, C.c.kind, C.c.key], "index_where": C.c.key.isnot(None)}
    row = row.on_conflict_do_update(**keyed, set_={"body": row.excluded.body, "updated_at": func.now()})
    with write(p) as db:
        check(db, p, body)
        stored = _number(db.execute(row.returning(C.c.id, C.c.key, C.c.body)).one())
    if phone and repoint and tenant.plivo_app_id:
        plivo.rest(tenant, "POST", f"Number/{digits}/", app_id=tenant.plivo_app_id)
    return stored


@router.get("/numbers", response_model=Page[m.EntryPoint])
def list_numbers(p: Principal = ADMIN, paged: Paging = Depends(paging)):
    with read() as db:
        rows = page_rows(db, docs(p.tenant_id, "number"), paged, T.config.c.key, str)
    return page([_number(r) for r in rows], paged, "number")


@router.get("/numbers/{number}", response_model=m.EntryPoint)
def get_number(number: NumberPath, p: Principal = ADMIN):
    with read() as db:
        return _number(_one_number(db, p, number))


@router.delete("/numbers/{number}", status_code=204)
def delete_number(number: NumberPath, p: Principal = ADMIN) -> Response:
    with write(p) as db:
        db.execute(delete(T.config).where(T.config.c.id == _one_number(db, p, number).id))
    return Response(status_code=204)


from hs.config.dispositions import default_map as default_map  # noqa: E402
from hs.config.dispositions import system_dispositions as system_dispositions  # noqa: E402
