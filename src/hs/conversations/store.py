from datetime import UTC, datetime
from functools import cache
from uuid import UUID

from sqlalchemy import bindparam, func, null, select
from sqlalchemy.engine import Connection, Row

from hs import hot
from hs.conversations import _id, _opened, offer_row
from hs.core import T


def rows(
    db: Connection, cid, states: tuple[str, ...] | None = None, *, ended: bool = False, lost: list | None = None
) -> list:
    """The conversation's stints, oldest first, then its open offers, in `states` if given; with `ended`, also its
    offers' tombstones, which needs the conversation row for `cid`. Timers are read with the stints in one statement
    after the caller's lock, and Redis only when there is a timer or `ended`. Each timer whose offer Redis lost is
    added to list `lost`, if given, for `retire_lost`."""
    found = db.execute(_rows(), {"r_cid": _id(cid), "r_key": str(_id(cid))}).all()
    stints = [r for r in found if r.timer is None]
    timers = {r.timer["assignment_id"]: r.timer for r in found if r.timer is not None}
    memo = db.info.get("tx", {})
    w = memo.get(("offers", _id(cid)), {"creates": {}, "binds": {}})
    offers = [a for a in w["creates"].values() if a.state == "offered"]
    tenant = getattr(cid, "tenant_id", None) or next(iter(timers.values()), {}).get("tenant_id")
    if (timers or ended) and tenant:
        mine = {str(x.id) for x in stints} | {str(a.id) for a in offers}
        if (held := ("kept", _id(cid))) not in memo:
            memo[held] = hot.offers(tenant, _id(cid))
        kept = {o: r for o, r in memo[held].items() if o not in mine}
        for o, r in kept.items():
            if r["state"] == "offered" and o in timers:
                offers.append(w["binds"].get(UUID(o)) or offer_row(_opened(r, timers[o])))
            elif r["state"] != "offered" and ended:
                offers.append(offer_row(r))
        if lost is not None:
            lost += [t for o, t in timers.items() if o not in kept and o not in mine]
    offers.sort(key=lambda a: (a.offered_at is None, a.offered_at or datetime.min.replace(tzinfo=UTC), str(a.id)))
    return [a for a in stints + offers if not states or a.state in states]


# The statements every callback runs are built once: SQLAlchemy keeps a built statement's cache key.
@cache
def _rows():
    A, J = T.assignment, T.jobs
    timer = J.c.payload.op("||")(func.jsonb_build_object("run_at", J.c.run_at))
    stints = select(*A.c, null().cast(J.c.payload.type).label("timer")).where(A.c.conversation_id == bindparam("r_cid"))
    timers = select(*(null().cast(c.type).label(c.name) for c in A.c), timer.label("timer"))
    timers = timers.where(J.c.kind == "offer_timeout", J.c.payload["conversation_id"].astext == bindparam("r_key"))
    both = stints.union_all(timers)
    return both.order_by(both.selected_columns.offered_at, both.selected_columns.id)


def lock(db: Connection, cid: UUID | None = None, *, tenant=None, call_uuid: str | None = None) -> Row | None:
    """Take the conversation's row lock (FOR NO KEY UPDATE) by id or by its call, only `tenant`'s if given; None when
    no row matches. One statement: id, tenant and a bound call_uuid never change, so the recheck after a lock wait
    keeps the row a read by id would have found."""
    keys = {"id": cid, "tenant_id": tenant, "call_uuid": call_uuid}
    return db.execute(_locking(*(k for k, v in keys.items() if v is not None)), keys).first()


@cache
def _locking(*cols: str):
    C = T.conversation
    return select(C).where(*(C.c[k] == bindparam(k) for k in cols)).with_for_update(key_share=True)
