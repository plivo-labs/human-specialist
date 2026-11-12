"""What HS makes at Plivo for a tenant, and how it goes: a durable delete, and the intent an attempt commits before it
makes an endpoint or an application, so nothing it made outlives a crash or a takeover unrecorded."""

import uuid
from datetime import timedelta
from functools import partial
from types import SimpleNamespace

from sqlalchemy import delete, func, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Connection

from hs import access, jobs, plivo
from hs.core import ApiError, T, after_commit, read, tx

plivo.OPS["delete"] = plivo.Op(False, lambda t, job: plivo.rest(t, "DELETE", job["path"]), teardown=True)


def drop(db: Connection, tenant_id, path: str) -> None:
    """Delete `path` in the tenant's Plivo account: the job pool tries it once `db` commits and retries on its backoff
    until Plivo answers; the caller never waits."""
    plivo.command(db, "delete", path, {"tenant_id": str(tenant_id), "path": path})


MADE = {"Endpoint": ("alias", "endpoint_id"), "Application": ("app_name", "app_id")}
# Seconds an intent waits before it deletes: far past an attempt's life between `intend` and `kept`, one Plivo POST
# (at most `plivo.TIMEOUT`, never retried) and one config transaction.
INTENT_WAIT = 300
_KEPT = text(
    "SELECT EXISTS (SELECT 1 FROM config WHERE tenant_id = :tenant AND kind = 'agent' AND body->>'endpoint_id' = :id)"
    " OR EXISTS (SELECT 1 FROM tenant WHERE id = :tenant AND plivo_app_id = :id)"
)


def _unmade(tenant, job: dict) -> dict:
    """Delete what an attempt that never committed made: at `path` once Plivo answered with it, else each of the
    account's `kind` named `name`; HS keeps no id of either. A registration's tenant may never have committed, so its
    intent carries the account."""
    tenant = tenant or SimpleNamespace(plivo_auth_id=job["plivo_auth_id"], plivo_token_enc=job["plivo_token_enc"])
    kind, (name_field, id_field) = job["kind"], MADE[job["kind"]]
    named = (f"{kind}/{o[id_field]}/" for o in plivo.listed(tenant, f"{kind}/") if o.get(name_field) == job["name"])
    for path in [job["path"]] if "path" in job else list(named):
        with read() as db:
            if not db.execute(_KEPT, {"tenant": job["tenant_id"], "id": path.split("/")[1]}).scalar():
                plivo.rest(tenant, "DELETE", path)
    return {}


plivo.OPS["unmade"] = plivo.Op(False, _unmade, teardown=True)


def intend(tenant_id, kind: str, name: str, **account) -> tuple[str, dict]:
    """Before an attempt makes `kind` named `name` at Plivo: commit a job, due once the attempt is surely over, that
    deletes what it made unless its commit calls `kept`, so a crash leaves no orphan. 409 while an earlier attempt of
    this Idempotency-Key has an unsettled intent. Returns the intent's key and payload."""
    J, claim = T.jobs, access.CLAIM.get()
    request = claim and ":".join(claim[k] for k in ("tenant_id", "principal", "key_sha256"))
    key = f"unmade:{request or uuid.uuid4().hex}"
    job = {"tenant_id": str(tenant_id), "op": "unmade", "kind": kind, "name": name} | account
    at = func.now() + timedelta(seconds=INTENT_WAIT)
    row = insert(J).values(kind="plivo", lane="plivo", key=key, run_at=at, payload=job)
    with tx() as db:
        if not db.execute(row.on_conflict_do_nothing()).rowcount:
            raise ApiError(
                409, "idempotency_in_progress", "an earlier attempt with this Idempotency-Key is still settling"
            )
    return key, job


def kept(db: Connection, intent: tuple[str, dict] | None) -> None:
    """In the attempt's commit: what it made is HS's. 409 once a later attempt of its request ran the intent. An
    attempt that made nothing has no intent."""
    if not intent:
        return
    J = T.jobs
    mine = (J.c.kind == "plivo", J.c.key == intent[0], J.c.locked_until.is_(None))
    if not db.execute(delete(J).where(*mine)).rowcount:
        raise ApiError(409, "idempotency_in_progress", "a later request with this Idempotency-Key took it over")


def unmade(intent: tuple[str, dict] | None, path: str | None) -> None:
    """A failed attempt: its intent runs now, on `path` when Plivo answered with one."""
    if not intent:
        return
    key, job = intent
    with tx() as db:
        jobs.schedule(db, "plivo", job | ({"path": path} if path else {}), key=key)
        after_commit(db, partial(jobs.run_now, "plivo", key, wait=True))


def torn_down(db: Connection, tenant) -> None:
    """A deleted tenant's Plivo side: its pending commands are cancelled, but cleanup ones, and the endpoints and
    application HS made are deleted."""
    C, J, cleanup = T.config, T.jobs, [op for op, run in plivo.OPS.items() if run.teardown]
    mine = J.c.payload["tenant_id"].astext == str(tenant.id)
    db.execute(delete(J).where(J.c.kind == "plivo", mine, J.c.payload["op"].astext.not_in(cleanup)))
    made = select(C.c.body["endpoint_id"].astext).where(C.c.tenant_id == tenant.id, C.c.kind == "agent")
    made = made.where(C.c.body["endpoint_adopted"].as_boolean().isnot(True))
    app = [f"Application/{tenant.plivo_app_id}/"] if tenant.plivo_app_id else []
    for path in [f"Endpoint/{e}/" for e in db.execute(made).scalars()] + app:
        drop(db, tenant.id, path)
