"""The status map applied: a call result becomes the tenant's system disposition and the conversation's result."""

import contextvars
import uuid

import pytest
from sqlalchemy import text

from hs.admin import seeds
from hs.conversations import outcomes
from hs.errors import ApiError
from hs.infra.db import core_engine
from hs.infra.models import UseCaseResponse
from tests.unit._rows import add, agent, principal, tenant, uuid_of

_ORG = text(
    "INSERT INTO organization (uuid, created_at, updated_at, is_deleted, name, auth_id)"
    " VALUES (gen_random_uuid(), now(), now(), false, 'o', :auth_id) RETURNING id"
)
_PARENT = text(
    "INSERT INTO conversation_parent (uuid, created_at, updated_at, organization, channel, direction_inbound)"
    " VALUES (gen_random_uuid(), now(), now(), :org, 'call', :inbound) RETURNING id"
)
_SYSTEM = text(
    "INSERT INTO dispositions (uuid, created_at, updated_at, is_deleted, organization, name, internal)"
    " VALUES (gen_random_uuid(), now(), now(), false, :org, :name, true) RETURNING uuid::text"
)
_MAP = text(
    "UPDATE org_setting SET voice_settings = jsonb_set(voice_settings, CAST(:path AS text[]), to_jsonb(CAST(:id AS"
    " text))) WHERE organization = :org"
)


@pytest.fixture()
def org(db, monkeypatch):
    monkeypatch.setattr(outcomes, "get_db_session", lambda: db)
    org_id = db.execute(_ORG, {"auth_id": f"MA{uuid.uuid4().hex[:10]}"}).scalar_one()
    seeds.seed(db.connection(), org_id, "app")
    return org_id


def test_a_result_takes_the_tenants_mapping_else_the_default_and_becomes_the_conversations(db, org):
    outbound = db.execute(_PARENT, {"org": org, "inbound": False}).scalar_one()
    assert outcomes.system_disposition(outbound, "busy") == "Customer didn't respond"
    assert db.execute(text("SELECT result FROM conversation_parent WHERE id = :p"), {"p": outbound}).scalar() == "busy"
    busy = db.execute(_SYSTEM, {"org": org, "name": "Line busy"}).scalar_one()
    db.execute(_MAP, {"org": org, "path": "{status_map,outbound,busy}", "id": busy})
    assert outcomes.system_disposition(outbound, "busy") == "Line busy"
    db.execute(text("UPDATE org_setting SET voice_settings = '{}' WHERE organization = :org"), {"org": org})
    assert outcomes.system_disposition(outbound, "busy") == "Customer didn't respond"
    inbound = db.execute(_PARENT, {"org": org, "inbound": True}).scalar_one()
    assert outcomes.system_disposition(inbound, "rejected") == "Dropped"


def test_a_queue_result_on_an_outbound_call_reads_the_inbound_map(db, org):
    outbound = db.execute(_PARENT, {"org": org, "inbound": False}).scalar_one()
    assert outcomes.system_disposition(outbound, "abandoned") == "Dropped"
    hung_up = db.execute(_SYSTEM, {"org": org, "name": "Hung up waiting"}).scalar_one()
    db.execute(_MAP, {"org": org, "path": "{status_map,inbound,abandoned}", "id": hung_up})
    assert outcomes.system_disposition(outbound, "abandoned") == "Hung up waiting"


def test_a_wrap_up_the_engine_refuses_answers_a_fixed_409(monkeypatch):
    with core_engine.begin() as conn:
        org, tenant_id = tenant(conn)
        aom, agent_id = agent(conn, org)
        parent = add(
            conn, "conversation_parent", organization=org, channel="call", direction_inbound=True,
            conversation_number=uuid.uuid4().hex[:12], contact_uuid=uuid.uuid4().hex,
        )  # fmt: skip
        add(
            conn, "conversation", parent=parent, primary_aom=aom, channel="call", status="in_wrap_up",
            state="agent_answered", generation_type="qnr_generated", direction_inbound=True,
        )  # fmt: skip
        done = uuid_of(conn, "dispositions", add(conn, "dispositions", organization=org, name="Done", internal=False))
        cid = uuid_of(conn, "conversation_parent", parent)
    refusal = UseCaseResponse(status_code=404, error="Conversation not found")
    monkeypatch.setattr(outcomes.management, "create_conversation_chunk", lambda *a: refusal)
    body = outcomes.WrapUpIn(disposition_id=done, agent_id=agent_id)
    with pytest.raises(ApiError) as raised:
        contextvars.copy_context().run(outcomes.wrap_up, uuid.UUID(cid), body, principal(org, tenant_id))
    assert (raised.value.status, raised.value.code) == (409, "wrap_up_failed")
    assert "Conversation" not in raised.value.message
