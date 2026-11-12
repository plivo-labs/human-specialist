"""Dispositions, two levels deep, soft-deleted so the history filed under them stays whole, and the status map that
files a result no agent wrapped up under a system disposition."""

from fastapi import Depends, Response
from sqlalchemy import func, or_, select, update
from sqlalchemy.engine import Connection, Row

from hs import access
from hs.access import Principal
from hs.config import (
    ADMIN,
    ADMIN_AGENT,
    _sequence,
    _store,
    add,
    as_uuid,
    check,
    docs,
    get,
    owned,
    page_rows,
    router,
    save,
    tenant_settings,
    write,
)
from hs.config import models as m
from hs.core import PUBLIC, ApiError, Page, Paging, T, page, paging, read


def _disposition(row: Row) -> dict:
    return {**row.body, "id": str(row.id), "parent_disposition_id": row.body["parent_id"]}


def _dispositions(p: Principal):
    """Deleted ones stay in the table for the history that names them. A desk below admin sees what it may pick."""
    body = T.config.c.body
    q = docs(p.tenant_id, "disposition").where(body["deleted"].as_boolean().isnot(True))
    hidden = body["system"].as_boolean().is_(False), body["disabled"].as_boolean().isnot(True)
    return q.where(*hidden) if p.kind == "agent" and p.role != "admin" else q


def _locked(row: Row) -> ApiError:
    msg = "system dispositions cannot be renamed, moved or deleted"
    return ApiError(409, "system_disposition", msg, {"id": str(row.id)})


def _one_disposition(db: Connection, p: Principal, did: str, *, lock: bool = False) -> Row:
    q = _dispositions(p).where(T.config.c.id == as_uuid(did))
    if (row := db.execute(q.with_for_update() if lock else q).first()) is None:
        raise ApiError(404, "disposition_not_found", "no such disposition")
    return row


def _parent(db: Connection, p: Principal, body: m.DispositionIn | m.DispositionPatch, row: Row | None = None):
    """Dispositions stay two levels deep, as wrap-up takes them."""
    if not body.parent_disposition_id:
        return None
    C, parent = T.config, get(db, p.tenant_id, "disposition", body.parent_disposition_id)
    subs = row and _dispositions(p).where(C.c.body["parent_id"].astext == str(row.id)).limit(1)
    top = parent and not parent.get("deleted") and not parent["parent_id"]
    if not top or (row and (parent["id"] == str(row.id) or db.execute(subs).first())):
        msg = "a sub-disposition goes under a top-level disposition and has none of its own"
        raise ApiError(422, "invalid_parent", msg)
    return parent["id"]


@router.get("/dispositions", response_model=Page[m.Disposition])
def list_dispositions(p: Principal = ADMIN_AGENT, paged: Paging = Depends(paging)):
    with read() as db:
        rows = page_rows(db, _dispositions(p), paged, _sequence(), int)
    return page([_disposition(r) for r in rows], paged, "sequence")


@router.post("/dispositions", status_code=201, response_model=m.Disposition)
def create_disposition(body: m.DispositionIn, p: Principal = ADMIN):
    """`system: true` makes a locked row that the status map can point at."""
    C = T.config
    with write(p) as db:
        check(db, p, body)
        last = db.execute(select(func.max(_sequence())).where(C.c.tenant_id == p.tenant_id, C.c.kind == "disposition"))
        parent = (pid := _parent(db, p, body)) and get(db, p.tenant_id, "disposition", pid)
        doc = body.model_dump(include={"name", "note_required", "system", "answered"}) | {"parent_id": pid}
        doc |= {"disabled": bool(parent and parent.get("disabled")), "sequence": (last.scalar() or 0) + 1}
        row = add(db, p.tenant_id, "disposition", doc, id=body.id)
        access.created(db, str(row.id))
        return _disposition(row)


access.VIEWS[f"POST {PUBLIC}/dispositions"] = lambda db, p, rid: _disposition(_one_disposition(db, p, rid))


@router.post("/dispositions/reorder", status_code=204)
def reorder_dispositions(body: m.Reorder, p: Principal = ADMIN) -> Response:
    """The listed dispositions take the places they already hold, in the order given; the rest stay put."""
    C = T.config
    with write(p) as db:
        owned(db, p, "disposition", body.ids)
        ids = list(dict.fromkeys(map(as_uuid, body.ids)))
        rows = db.execute(docs(p.tenant_id, "disposition").where(C.c.id.in_(ids)).order_by(C.c.id).with_for_update())
        places = sorted(r.body["sequence"] for r in rows)
        for did, seq in zip(ids, places, strict=True):
            db.execute(update(C).where(C.c.id == did).values(body=C.c.body.concat({"sequence": seq})))
    return Response(status_code=204)


@router.get("/dispositions/{disposition_id}", response_model=m.Disposition)
def get_disposition(disposition_id: str, p: Principal = ADMIN_AGENT):
    with read() as db:
        return _disposition(_one_disposition(db, p, disposition_id))


@router.patch("/dispositions/{disposition_id}", response_model=m.Disposition)
def patch_disposition(disposition_id: str, body: m.DispositionPatch, p: Principal = ADMIN):
    """`disabled` sets its sub-dispositions too, and a sub-disposition is disabled while its parent is."""
    C = T.config
    with write(p) as db:
        row = _one_disposition(db, p, disposition_id, lock=True)
        moved = (
            "parent_disposition_id" in body.model_fields_set
            and (body.parent_disposition_id or None) != row.body["parent_id"]
        )
        toggled = body.disabled is not None and body.disabled != row.body.get("disabled", False)
        if row.body["system"] and (moved or toggled or body.name not in (None, row.body["name"])):
            raise _locked(row)
        check(db, p, body)
        parent_id = _parent(db, p, body, row) if moved else row.body["parent_id"]
        off = bool(parent_id and (get(db, p.tenant_id, "disposition", parent_id) or {}).get("disabled"))
        if off and toggled:
            raise ApiError(409, "parent_disabled", "enable its parent disposition first", {"id": parent_id})
        doc = row.body | body.model_dump(include={"name", "note_required", "disabled"}, exclude_none=True)
        if toggled:
            subs = C.c.tenant_id == p.tenant_id, C.c.kind == "disposition", C.c.body["parent_id"].astext == str(row.id)
            db.execute(update(C).where(*subs).values(body=C.c.body.concat({"disabled": body.disabled})))
        doc |= {"parent_id": parent_id, "disabled": off or doc.get("disabled", False)}
        return _disposition(save(db, row.id, doc))


@router.delete("/dispositions/{disposition_id}", status_code=204)
def delete_disposition(disposition_id: str, p: Principal = ADMIN) -> Response:
    """Also deletes its sub-dispositions. Each stays as a deleted row, so the conversations filed under it keep their
    disposition in reports, search and exports."""
    C = T.config
    with write(p) as db:
        if (row := _one_disposition(db, p, disposition_id, lock=True)).body["system"]:
            raise _locked(row)
        family = or_(C.c.id == row.id, C.c.body["parent_id"].astext == str(row.id))
        gone = update(C).where(C.c.tenant_id == p.tenant_id, C.c.kind == "disposition", family)
        db.execute(gone.values(body=C.c.body.concat({"deleted": True}), updated_at=func.now()))
    return Response(status_code=204)


def system_dispositions(db: Connection, tenant_id) -> list[Row]:
    q = docs(tenant_id, "disposition").where(T.config.c.body["system"].as_boolean())
    return db.execute(q.order_by(_sequence())).all()


def default_map(db: Connection, tenant_id) -> dict:
    by_name = {r.body["name"]: str(r.id) for r in reversed(system_dispositions(db, tenant_id))}
    return {d: {r: by_name[n] for r, n in names.items() if n in by_name} for d, names in m.STATUS_MAP_DEFAULTS.items()}


@router.get("/status-map", response_model=m.StatusMap)
def get_status_map(defaults: bool = False, p: Principal = ADMIN):
    with read() as db:
        base, stored = default_map(db, p.tenant_id), {} if defaults else tenant_settings(db, p.tenant_id).status_map
    return {d: base[d] | stored.get(d, {}) for d in base}


@router.put("/status-map", response_model=m.StatusMap)
def put_status_map(body: m.StatusMap, p: Principal = ADMIN):
    mapping = body.model_dump()
    with write(p) as db:
        system = {str(r.id) for r in system_dispositions(db, p.tenant_id)}
        if missing := sorted({i for ids in mapping.values() for i in ids.values()} - system):
            msg = "map results to system dispositions only"
            raise ApiError(422, "not_system_disposition", msg, {"ids": missing})
        _store(db, p.tenant_id, status_map=mapping)
    return mapping
