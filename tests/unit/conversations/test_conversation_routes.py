"""The conversation routes through the app, with real credentials: the list's filters, paging and visibility, the
read, attributes and notes, and each agent's wrap-up and extend."""

import threading
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime

from fastapi.testclient import TestClient
from sqlalchemy import insert, select, update

from hs import access, core
from hs import conversations as cv
from hs.app import app
from hs.core import T
from tests.unit.conversations import test_conversations
from tests.unit.conversations.test_conversations import World, row
from tests.unit.test_config import held, waiter

world = test_conversations.world

HTTP = TestClient(app, raise_server_exceptions=False)


def key(world: World) -> dict:
    with core.tx() as db:
        return {"Authorization": "Bearer " + access.new_key(db, str(world.tenant), ["calls"])["api_key"]}


def login(world: World, agent_id, role: str = "agent", teams: tuple = ()) -> dict:
    """An agent token for `agent_id`; access reads its role and teams from the agent's config document."""
    body = {"name": "Agent", "email": f"{agent_id}@example.com", "role": role, "teams": list(teams)}
    with core.tx() as db:
        db.execute(insert(T.config).values(id=agent_id, tenant_id=world.tenant, kind="agent", body=body))
        return {"Authorization": "Bearer " + access.mint(db, str(world.tenant), str(agent_id)).agent_token}


def ids(headers: dict, **query) -> list[str]:
    r = HTTP.get("/api/v1/conversations", params=query, headers=headers)
    assert r.status_code == 200, r.text
    return [c["id"] for c in r.json()["data"]]


def code(r) -> str:
    return r.json()["error"]["code"]


def test_the_list_filters_pages_and_shows_each_caller_what_it_may_see(world):
    conv, a = world.live()
    other, leg = world.open("sms", customer="+15559999"), uuid.uuid4().hex
    with core.tx() as db:
        db.execute(update(T.assignment).where(T.assignment.c.id == a.id).values(call_uuid=leg))
    k = key(world)
    assert ids(k) == ids(k, state="active") == [str(other.id), str(conv.id)] and ids(k, state="closed") == []
    assert (
        ids(k, agent_id=str(a.agent_id)) == ids(k, call_uuid=conv.call_uuid) == ids(k, call_uuid=leg) == [str(conv.id)]
    )
    assert ids(k, number="15559999") == ids(k, channel="sms") == [str(other.id)] and ids(k, direction="outbound") == []
    assert ids(k, conversation_number=conv.number) == [str(conv.id)]
    first = HTTP.get("/api/v1/conversations", params={"limit": 1}, headers=k).json()
    second = HTTP.get("/api/v1/conversations", params={"limit": 1, "cursor": first["next_cursor"]}, headers=k).json()
    assert [c["id"] for c in first["data"] + second["data"]] == [str(other.id), str(conv.id)] and not second[
        "next_cursor"
    ]
    assert HTTP.get("/api/v1/conversations", params={"cursor": "eHl6"}, headers=k).status_code == 422
    assert ids(login(world, a.agent_id, teams=("floor",))) == [str(conv.id)]
    assert ids(login(world, world.agent(), role="supervisor", teams=("floor",))) == [str(conv.id)]
    assert ids(login(world, world.agent(), teams=("floor",))) == [], "an agent sees only what it handled"
    monitor = world.agent()
    world.offer(conv, monitor, role="monitor", via="direct")
    listener = login(world, monitor)
    assert ids(listener) == [str(conv.id)] and ids(k, agent_id=str(monitor)) == []
    assert HTTP.get(f"/api/v1/conversations/{conv.id}", headers=listener).status_code == 200


def test_the_read_and_every_write_are_404_for_an_agent_not_on_the_conversation(world):
    conv, a = world.live()
    url, k, agent = f"/api/v1/conversations/{conv.id}", key(world), login(world, a.agent_id)
    assert HTTP.patch(url, json={"attributes": {"csat": 5, "crm": "x"}}, headers=k).status_code == 200
    merged = HTTP.patch(url, json={"attributes": {"csat": 4, "crm": None, "p": "high"}}, headers=agent)
    assert merged.status_code == 200 and merged.json()["attributes"] == {"csat": 4, "p": "high"}, merged.text
    big = HTTP.patch(url, json={"attributes": {"note": "x" * 2100}}, headers=k)
    assert (big.status_code, code(big)) == (422, "attributes_too_large")
    assert HTTP.post(f"{url}/notes", json={"text": "x", "pinned": True}, headers=agent).status_code == 422
    note = HTTP.post(f"{url}/notes", json={"text": "  refund  "}, headers=agent)
    assert note.status_code == 201 and note.json()["text"] == "refund" and note.json()["agent_id"] == str(a.agent_id)
    read = HTTP.get(url, headers=k).json()
    assert (read["attributes"], [n["text"] for n in read["notes"]]) == ({"csat": 4, "p": "high"}, ["refund"])
    assert (read["channel"], read["state"], read["legs"][0]["role"], read["context"]) == (
        "call",
        "active",
        "customer",
        None,
    )
    stranger = login(world, world.agent())
    for method, path, body in (
        ("GET", url, None),
        ("PATCH", url, {"attributes": {}}),
        ("POST", f"{url}/notes", {"text": "x"}),
        ("POST", f"{url}/wrap-up", {"disposition_id": str(world.sale)}),
        ("POST", f"{url}/wrap-up/extend", {"seconds": 5}),
    ):
        r = HTTP.request(method, path, json=body, headers=stranger)
        assert r.status_code == 404 and code(r) == "conversation_not_found", (method, r.text)
    assert HTTP.get(f"/api/v1/conversations/{uuid.uuid4()}", headers=k).status_code == 404


def test_each_agent_wraps_up_its_own_and_extends_only_an_auto_wrap_up(world):
    conv, a = world.live()
    with core.tx() as db:
        cv.gone(db, cv.lock(db, conv.id))
        callback = world.doc(db, "disposition", name="Callback", system=False, note_required=True, parent_id=None)
        upsell = world.doc(
            db, "disposition", name="Upsell", system=False, note_required=False, parent_id=str(world.sale)
        )
    url, k, agent = f"/api/v1/conversations/{conv.id}/wrap-up", key(world), login(world, a.agent_id)
    assert code(HTTP.post(url, json={"disposition_id": str(world.sale)}, headers=k)) == "not_in_wrap_up"
    assert code(HTTP.post(f"{url}/extend", json={"seconds": 5}, headers=agent)) == "not_auto_wrap_up"
    assert code(HTTP.post(url, json={"disposition_id": str(callback)}, headers=agent)) == "note_required"
    wrong = {"disposition_id": str(callback), "sub_disposition_id": str(upsell), "note": "n"}
    assert code(HTTP.post(url, json=wrong, headers=agent)) == "foreign_id"
    sold = {
        "disposition_id": str(world.sale),
        "sub_disposition_id": str(upsell),
        "note": " ok ",
        "agent_id": str(a.agent_id),
    }
    assert HTTP.post(url, json=sold, headers=k).status_code == 204
    assert world.conv(conv.id).state == "closed" and world.events("conversation.wrapped_up")[0].data["note"] == "ok"
    world.settings(wrap_up={"mode": "auto", "seconds": 10, "max_extend_sec": 30})
    conv, a = world.live()
    with core.tx() as db:
        cv.gone(db, cv.lock(db, conv.id))
    extend, agent = f"/api/v1/conversations/{conv.id}/wrap-up/extend", login(world, a.agent_id)
    assert code(HTTP.post(extend, json={"seconds": 5}, headers=k)) == "agent_only"
    assert [HTTP.post(extend, json={"seconds": 20}, headers=agent).status_code for _ in range(2)] == [204, 204]
    assert code(HTTP.post(extend, json={"seconds": 1}, headers=agent)) == "extend_limit_reached"
    starts = [
        datetime.fromisoformat(e.data["deadline_at"]) for e in world.events("conversation.wrap_up_started", conv.id)
    ]
    assert len(starts) == 3 and (starts[-1] - starts[0]).total_seconds() == 30
    assert row(T.assignment, a.id).wrap_up_until == starts[-1]


def taken_back(world: World):
    conv, first = world.live()
    world.move(first, "leave", reason="transferred")
    return conv, first, world.move(world.offer(conv, first.agent_id), "accept")


def test_an_agent_with_two_wrap_ups_on_a_conversation_submits_once_for_both(world):
    conv, first, second = taken_back(world)
    with core.tx() as db:
        cv.gone(db, cv.lock(db, conv.id))
    url = f"/api/v1/conversations/{conv.id}/wrap-up"
    r = HTTP.post(url, json={"disposition_id": str(world.sale)}, headers=login(world, first.agent_id))
    assert r.status_code == 204, r.text
    assert [row(T.assignment, a.id).state for a in (first, second)] == ["done", "done"]
    assert world.conv(conv.id).state == "closed" and len(world.events("conversation.wrapped_up", conv.id)) == 1
    [closed] = world.events("conversation.closed", conv.id)
    assert [d["agent_id"] for d in closed.data["dispositions"]] == [str(first.agent_id)]


def test_a_wrap_up_is_refused_while_the_agents_newest_stint_on_the_conversation_is_live(world):
    conv, first, _ = taken_back(world)
    url = f"/api/v1/conversations/{conv.id}/wrap-up"
    r = HTTP.post(url, json={"disposition_id": str(world.sale)}, headers=login(world, first.agent_id))
    assert r.status_code == 422 and code(r) == "not_in_wrap_up", r.status_code
    assert row(T.assignment, first.id).state == "wrap_up"


def test_a_wrap_up_naming_a_disposition_that_is_being_deleted_waits_for_the_delete_and_is_refused(world):
    conv, a = world.live()
    with core.tx() as db:
        cv.gone(db, cv.lock(db, conv.id))
        doomed = world.doc(db, "disposition", name="Doomed", system=False, note_required=False, parent_id=None)
        child = world.doc(db, "disposition", name="Child", system=False, note_required=False, parent_id=str(doomed))
        admin = {"Authorization": "Bearer " + access.new_key(db, str(world.tenant), ["admin"])["api_key"]}
    url, agent, ready, release = (
        f"/api/v1/conversations/{conv.id}/wrap-up",
        login(world, a.agent_id),
        Future(),
        threading.Event(),
    )
    with ThreadPoolExecutor(3) as pool:
        try:
            # The delete takes its sub-disposition with it: holding that row pauses it inside its transaction.
            pool.submit(held, select(T.config.c.id).where(T.config.c.id == child).with_for_update(), ready, release)
            holder = ready.result(5)
            deleting = pool.submit(HTTP.delete, f"/api/v1/dispositions/{doomed}", headers=admin)
            deleter = waiter(holder)
            wrapping = pool.submit(HTTP.post, url, json={"disposition_id": str(doomed)}, headers=agent)
            assert deleter and waiter(deleter), "the wrap-up waits for the delete"
        finally:
            release.set()
    assert (deleting.result().status_code, wrapping.result().status_code) == (204, 422)
    assert row(T.assignment, a.id).disposition_id is None
