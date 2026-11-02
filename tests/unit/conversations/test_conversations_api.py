"""The public conversation reads and notes over seeded rows, and the close-time search row's local team read."""

import uuid

import pytest
from sqlalchemy import text

from hs.conversations import api, search
from hs.infra.context import bg_db_guard
from hs.infra.db import core_engine
from tests.unit._rows import add, agent, principal, tenant, uuid_of


@pytest.fixture()
def seeded():
    """A live inbound call answered by A (with a note and a recording) and a closed outbound call of X."""
    with core_engine.begin() as conn:
        org, tenant_id = tenant(conn)
        team = add(conn, "team", organization=org, name="Red")
        (a, a_id), (_, sup), (x, x_id) = agent(conn, org, team), agent(conn, org, team), agent(conn, org)
        call = uuid.uuid4().hex
        live = add(
            conn, "conversation_parent", organization=org, channel="call", direction_inbound=True,
            conversation_number=uuid.uuid4().hex[:12], contact_identifier="+14155550100", contact_uuid=call,
        )  # fmt: skip
        child = add(
            conn, "conversation", parent=live, primary_aom=a, channel="call", status="open",
            state="agent_answered", generation_type="qnr_generated", direction_inbound=True,
        )  # fmt: skip
        root = add(conn, "call_leg", call_uuid=call, start_time="1")
        add(conn, "call_leg", call_uuid=call, role="Customer", parent_call_leg=root)
        agent_leg = add(conn, "call_leg", call_uuid=uuid.uuid4().hex, role="Agent", parent_call_leg=root)
        recording = add(conn, "recording", call_leg_id=agent_leg, recording_url="https://media.example/r.mp3")
        add(
            conn,
            "conversation_chunk",
            conversation=child,
            type="recording",
            recording=recording,
            **{"from": a_id, "to": "+1"},
        )
        add(
            conn,
            "conversation_chunk",
            conversation=child,
            type="notes",
            description="first",
            **{"from": a_id, "to": "+1"},
        )
        done = add(
            conn, "conversation_parent", organization=org, channel="call", direction_inbound=False,
            conversation_number=uuid.uuid4().hex[:12], contact_identifier="+14155550199", contact_uuid=uuid.uuid4().hex,
            end_at=text("now()"),
        )  # fmt: skip
        add(
            conn, "conversation", parent=done, primary_aom=x, channel="call", status="closed", state="x",
            generation_type="outbound_generated", direction_inbound=False, end_at=text("now()"),
        )  # fmt: skip
        ids = {k: uuid_of(conn, "conversation_parent", v) for k, v in (("live", live), ("done", done))}
        leg = conn.execute(text("SELECT call_uuid FROM call_leg WHERE id = :id"), {"id": agent_leg}).scalar()
        rec = uuid_of(conn, "recording", recording)
    return {
        "org": org,
        "tenant": tenant_id,
        "a": a_id,
        "sup": sup,
        "x": x_id,
        "call": call,
        "leg": leg,
        "rec": rec,
        **ids,
    }


def as_(client, s, agent_id=None, role=None):
    client.app.dependency_overrides[api.CALLS.dependency] = lambda: principal(s["org"], s["tenant"], agent_id, role)
    return client


@pytest.fixture(autouse=True)
def _clear(client):
    yield
    client.app.dependency_overrides.clear()


def listed(client, **query):
    r = client.get("/api/v1/conversations", params=query)
    assert r.status_code == 200, r.text
    return [c["id"] for c in r.json()["data"]]


def test_the_list_filters_by_state_agent_direction_number_and_call_uuid(client, seeded):
    s, c = seeded, as_(client, seeded)
    assert listed(c) == [s["done"], s["live"]]
    assert listed(c, state="active") == [s["live"]] and listed(c, state="closed") == [s["done"]]
    assert listed(c, call_uuid=s["call"]) == listed(c, call_uuid=s["leg"]) == [s["live"]]
    assert listed(c, call_uuid="nope") == []
    assert listed(c, agent_id=s["x"]) == listed(c, direction="outbound") == [s["done"]]
    assert listed(c, number="14155550100") == [s["live"]]
    first = c.get("/api/v1/conversations", params={"limit": 1}).json()
    after = c.get("/api/v1/conversations", params={"limit": 1, "cursor": first["next_cursor"]}).json()
    assert [r["id"] for r in first["data"] + after["data"]] == [s["done"], s["live"]] and after["next_cursor"] is None


def test_agents_see_their_own_and_supervisors_their_teams(client, seeded):
    s = seeded
    assert listed(as_(client, s, s["a"], "agent")) == [s["live"]]
    assert listed(as_(client, s, s["sup"], "supervisor")) == [s["live"]]
    assert listed(as_(client, s, s["x"], "agent"), state="active") == []


def test_one_conversation_has_its_legs_notes_and_recordings(client, seeded):
    s = seeded
    r = as_(client, s, s["a"], "agent").get(f"/api/v1/conversations/{s['live']}")
    assert r.status_code == 200, r.text
    c = r.json()
    assert {(leg["call_uuid"], leg["role"]) for leg in c["legs"]} == {(s["call"], "customer"), (s["leg"], "agent")}
    assert [n["text"] for n in c["notes"]] == ["first"] and c["recording_ids"] == [s["rec"]]
    assert (c["state"], c["agent_ids"], c["number"]) == ("active", [s["a"]], "+14155550100")
    assert "media.example" not in r.text
    assert as_(client, s, s["x"], "agent").get(f"/api/v1/conversations/{s['live']}").status_code == 404


def test_a_note_goes_through_the_chunk_path(client, seeded):
    s = seeded
    c = as_(client, s, s["a"], "agent")
    r = c.post(f"/api/v1/conversations/{s['live']}/notes", json={"text": " Asked for a refund "})
    assert r.status_code == 201, r.text
    assert (r.json()["text"], r.json()["agent_id"]) == ("Asked for a refund", s["a"])
    notes = c.get(f"/api/v1/conversations/{s['live']}").json()["notes"]
    assert [n["text"] for n in notes] == ["first", "Asked for a refund"]
    late = as_(client, s, s["x"], "agent").post(f"/api/v1/conversations/{s['done']}/notes", json={"text": "late"})
    assert (late.status_code, late.json()["error"]["code"]) == (409, "conversation_not_active")


def test_the_closed_search_row_reads_the_agents_teams_locally(seeded):
    with core_engine.begin() as conn:
        number, red = conn.execute(
            text(
                "SELECT cp.conversation_number, t.uuid::text FROM conversation_parent cp, team t"
                " WHERE cp.uuid::text = :id AND t.organization = cp.organization"
            ),
            {"id": seeded["live"]},
        ).one()
        conn.execute(
            text("UPDATE conversation_parent SET end_at = now() WHERE conversation_number = :n"), {"n": number}
        )
    assert bg_db_guard(search.upsert_for_conversation)(number)
    with core_engine.connect() as conn:
        row = conn.execute(
            text("SELECT agent_ids, team_ids FROM closed_conversation_search WHERE conv_number = :n"), {"n": number}
        ).one()
    assert (row.agent_ids, row.team_ids) == ([seeded["a"]], [red])
