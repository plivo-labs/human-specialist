"""RONA strikes against the migrated database: what counts, when the agent moves, and that the count starts over."""

import contextlib
import sys
import uuid
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from hs.admin import seeds
from hs.routing import rona

_ORG = text(
    "INSERT INTO organization (uuid, created_at, updated_at, is_deleted, name, auth_id)"
    " VALUES (gen_random_uuid(), now(), now(), false, 'o', :auth_id) RETURNING id"
)
_AGENT = text(
    "INSERT INTO account_org_mapping (uuid, created_at, updated_at, is_deleted, organization, sip_endpoint)"
    " VALUES (gen_random_uuid(), now(), now(), false, :org, :sip) RETURNING uuid::text"
)
_BREAK = text("SELECT uuid::text FROM org_statuses WHERE organization = :org AND status = 'Break'")


class _Redis(dict):
    def incr(self, key):
        self[key] = self.get(key, 0) + 1
        return self[key]

    def delete(self, key):
        self.pop(key, None)


@pytest.fixture()
def world(db, monkeypatch):
    org = db.execute(_ORG, {"auth_id": f"MA{uuid.uuid4().hex[:10]}"}).scalar_one()
    seeds.seed(db.connection(), org, "app")
    sip = f"sip:a{uuid.uuid4().hex[:8]}@phone.plivo.com"
    agent = db.execute(_AGENT, {"org": org, "sip": sip}).scalar_one()
    events, moves = [], []
    monkeypatch.setattr(rona, "core_engine", SimpleNamespace(connect=lambda: contextlib.nullcontext(db.connection())))
    cache = SimpleNamespace(client=_Redis())
    monkeypatch.setattr(rona.Cache, "get_instance", classmethod(lambda cls: cache))
    monkeypatch.setattr(rona, "emit", lambda type, **kw: events.append((type, kw["data"], kw["agent_id"])))
    status = SimpleNamespace(change_status=lambda *args: moves.append(args) or True)
    monkeypatch.setitem(sys.modules, "hs.admin.status", status)
    return SimpleNamespace(
        sip=sip, agent=agent, brk=db.execute(_BREAK, {"org": org}).scalar_one(), events=events, moves=moves
    )


def test_two_ring_outs_move_the_agent_to_break_and_the_count_starts_over(world):
    for leg in ("leg-1", "leg-2", "leg-3"):
        rona.missed(world.sip, leg, "CN1")
    rona.missed("sip:nobody@phone.plivo.com", "leg-4", "CN1")
    strikes = [(data["offer_id"], data["strike"]) for _, data, _ in world.events]
    assert strikes == [("leg-1", 1), ("leg-2", 2), ("leg-3", 1)]
    assert world.moves == [(world.agent, world.brk, "rona")]
