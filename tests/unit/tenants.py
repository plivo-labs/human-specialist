"""A tenant whose conversations reach their end through the conversation machines: what search and reports read."""

import uuid

from sqlalchemy import func, insert

from hs import access, core
from hs import conversations as cv
from hs.core import T
from hs.hot import rebuilt


class Tenant:
    def __init__(self):
        with core.tx() as db:
            row = insert(T.tenant).values(name="t", plivo_auth_id=uuid.uuid4().hex, plivo_token_enc="x", settings={})
            self.id = db.execute(row.returning(T.tenant.c.id)).scalar()
            rebuilt(self.id)
            self.queue = self.doc(db, "queue", name="Q", auto_accept={"enabled": False})
            self.sale, self.lost, self.refund, self.dropped = (
                self.doc(db, "disposition", name=n, system=n == "Dropped", parent_id=None, note_required=False)
                for n in ("Sale", "Lost", "Refund", "Dropped")
            )
            settings = {"rona": {"status_id": str(uuid.uuid4())}, "wrap_up": {"mode": "manual", "seconds": 60}}
            settings["status_map"] = {"inbound": {"abandoned": str(self.dropped)}, "outbound": {}}
            db.execute(T.tenant.update().where(T.tenant.c.id == self.id).values(settings=settings))
        self.key = access.Principal(tenant_id=str(self.id), kind="key", scopes=frozenset({"calls", "reports"}))

    def doc(self, db, kind: str, id=None, **body) -> uuid.UUID:
        row = insert(T.config).values(id=id or uuid.uuid4(), tenant_id=self.id, kind=kind, body=body)
        return db.execute(row.returning(T.config.c.id)).scalar()

    def agent(self, *teams, role: str = "agent") -> uuid.UUID:
        a = uuid.uuid4()
        with core.tx() as db:
            db.execute(insert(T.presence).values(agent_id=a, tenant_id=self.id, status_id=uuid.uuid4()))
            self.doc(db, "agent", a, name="A", email=f"{a}@example.test", role=role, teams=[str(t) for t in teams])
        return a

    def open(self, *, channel="call", queued=True, **fields):
        with core.tx() as db:
            fields = {"direction": "inbound", "customer": "+15550001", "ours": "+15550002"} | fields
            ids = {"call_uuid": uuid.uuid4().hex} if channel == "call" else {}
            conv = cv.open(db, self.id, channel=channel, **ids, **fields)
            return (queued and cv.set_state(db, conv, "queued", queue_id=self.queue, queued_at=func.now())) or conv

    def closed(self, agent, *, words=(), disposition=None, sub=None, skipped=False, **fields) -> uuid.UUID:
        """A conversation the agent took and wrapped up, with `words` as customer messages."""
        conv = self.open(**fields)
        with core.tx() as db:
            a = cv.move(db, cv.offer(db, cv.lock(db, conv.id), agent), "accept")
            for text in words:
                cv.entry(db, conv, "message", {"direction": "inbound", "text": text})
            cv.gone(db, cv.lock(db, conv.id))
        ids = {"disposition_id": disposition or self.sale, "sub_disposition_id": sub}
        with core.tx() as db:
            cv.lock(db, conv.id)
            cv.move(db, a.id, "submit", **ids, reason="skipped" if skipped else None)
        return conv.id

    def abandoned(self, **fields) -> uuid.UUID:
        """A conversation the customer left before any agent took it."""
        conv = self.open(**fields)
        with core.tx() as db:
            cv.gone(db, cv.lock(db, conv.id))
        return conv.id
