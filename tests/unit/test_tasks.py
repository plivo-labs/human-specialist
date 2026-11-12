"""Tasks through the app on the migrated database: a new target takes the task from its agent and routes it again,
who sees and changes a task before it routes, a scheduled route that runs twice, a reassignment racing a status
change, a route claimed before its time, and a create retried after a crash past its commit."""

import threading
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient
from sqlalchemy import func, select, text, update

from hs import access, config, core, jobs, tasks
from hs import app as service
from hs.config import models as m
from hs.core import T
from tests.unit import test_routing
from tests.unit.test_config import waiter
from tests.unit.test_routing import bearer, job

client = TestClient(service.app, raise_server_exceptions=False)
floor = test_routing.floor


def key(floor) -> dict:
    with core.tx() as db:
        return {"Authorization": "Bearer " + access.new_key(db, floor.tenant, ["calls"])["api_key"]}


def later(hours: float) -> str:
    return (datetime.now(UTC) + timedelta(hours=hours)).isoformat()


def task(headers: dict, target: dict, **body) -> dict:
    r = client.post(
        "/api/v1/tasks", json={"type": "task", "title": "Follow up", "target": target, **body}, headers=headers
    )
    assert r.status_code == 201, r.text
    return r.json()


def team(floor) -> uuid.UUID:
    with core.tx() as db:
        return floor.doc(db, "team", {"name": "Back office"})


def test_a_new_target_takes_the_task_from_its_agent_and_routes_it_again(floor):
    first, second = floor.agent(), floor.agent()
    queue = floor.queue(agents=[first])
    k = key(floor)
    made = task(k, {"type": "agent", "agent_id": str(first)})
    url, cid = f"/api/v1/tasks/{made['id']}", made["conversation_id"]
    assert (made["status"], made["agent_id"]) == ("todo", str(first))
    moved = client.patch(url, json={"target": {"type": "agent", "agent_id": str(second)}}, headers=k).json()
    assert (moved["status"], moved["agent_id"]) == ("todo", str(second))
    assert floor.offer_of(cid, first).state == "done"
    queued = client.patch(url, json={"target": {"type": "queue", "queue_id": str(queue)}}, headers=k).json()
    assert (queued["status"], queued["agent_id"]) == ("todo", None)
    assert floor.offers(cid) == [first] and floor.offer_of(cid, second).state == "done"
    activity = client.get(url, headers=k).json()["activity"]
    assert [(a["from"]["type"], a["to"]["type"]) for a in activity if a["field"] == "target"] == [
        ("agent", "agent"),
        ("agent", "queue"),
    ]
    listed = client.get("/api/v1/tasks", params={"status": "todo"}, headers=bearer(floor, second)).json()["data"]
    assert [t["id"] for t in listed] == [made["id"]]
    assert client.get("/api/v1/tasks", headers=bearer(floor, floor.agent())).json()["data"] == []


def test_an_agent_sees_and_reschedules_its_own_scheduled_task_and_others_see_theirs(floor):
    maker, stranger, aimed = floor.agent(), floor.agent(), floor.agent()
    room = team(floor)
    lead = floor.agent(teams=[room])
    with config.write(access.OPERATOR, floor.tenant) as db:
        body = T.config.c.body.concat({"role": "supervisor"})
        db.execute(update(T.config).where(T.config.c.id == lead).values(body=body))
    queue = floor.queue(agents=[maker])
    mine = task(bearer(floor, maker), {"type": "queue", "queue_id": str(queue)}, scheduled_at=later(1))
    url = f"/api/v1/tasks/{mine['id']}"
    assert client.get(url, headers=bearer(floor, maker)).status_code == 200
    assert client.patch(url, json={"scheduled_at": later(2)}, headers=bearer(floor, maker)).status_code == 200
    assert job("task_route", mine["id"]).wait > timedelta(minutes=90)
    outsider = bearer(floor, stranger)
    codes = {client.get(url, headers=outsider).status_code, client.patch(url, json={}, headers=outsider).status_code}
    assert codes == {404}
    k = key(floor)
    targets = [({"type": "agent", "agent_id": str(aimed)}, aimed), ({"type": "team", "team_id": str(room)}, lead)]
    for target, who in targets:
        theirs = task(k, target, scheduled_at=later(1))
        assert client.get(f"/api/v1/tasks/{theirs['id']}", headers=bearer(floor, who)).status_code == 200
    away = {"target": {"type": "agent", "agent_id": str(stranger)}}
    assert client.patch(f"/api/v1/tasks/{theirs['id']}", json=away, headers=bearer(floor, lead)).status_code == 404


def test_a_task_in_its_agents_only_slot_routes_again_or_is_postponed_and_a_full_agent_waits_for_its_time(floor):
    agent, k = floor.agent(), key(floor)
    made = task(k, {"type": "agent", "agent_id": str(agent)})
    url = f"/api/v1/tasks/{made['id']}"
    again = client.patch(url, json={"reroute": True}, headers=k)
    body = again.json()
    assert again.status_code == 200 and (body["status"], body["agent_id"]) == ("todo", str(agent))
    postponed = client.patch(url, json={"scheduled_at": later(1)}, headers=k)
    assert postponed.status_code == 200 and postponed.json()["status"] == "todo"
    task(k, {"type": "agent", "agent_id": str(agent)}, title="Fills the slot")
    assert client.patch(url, json={"scheduled_at": later(2)}, headers=k).status_code == 200
    assert job("task_route", made["id"]).wait > timedelta(minutes=90)


def test_a_route_claimed_early_after_a_later_postponement_waits_for_its_time(floor, monkeypatch):
    queue, k = floor.queue(agents=[floor.agent()]), key(floor)
    made = task(k, {"type": "queue", "queue_id": str(queue)}, scheduled_at=later(1))
    url = f"/api/v1/tasks/{made['id']}"
    with monkeypatch.context() as patched:
        patched.setattr(jobs, "run_now", lambda *args, **kwargs: None)
        assert client.patch(url, json={"scheduled_at": None}, headers=k).status_code == 200
    assert client.patch(url, json={"scheduled_at": later(1)}, headers=k).status_code == 200
    jobs.run_now("task_route", made["id"], wait=True)
    assert floor.offers(made["conversation_id"]) == [] and job("task_route", made["id"]).wait > timedelta(minutes=50)


def test_a_scheduled_team_route_that_runs_again_after_a_crash_offers_no_one_twice(floor):
    floor.settings(capacity=m.Capacity(task=2).model_dump())
    room = team(floor)
    members = sorted(floor.agent(teams=[room]) for _ in range(2))
    made = task(key(floor), {"type": "team", "team_id": str(room)}, scheduled_at=later(1))
    with core.tx() as db:
        due = (T.jobs.c.kind == "task_route", T.jobs.c.key == made["id"])
        db.execute(update(T.jobs).where(*due).values(run_at=func.now()))
    claimed = job("task_route", made["id"])
    tasks._due(claimed)
    tasks._due(claimed)
    assert sorted(floor.offers(made["conversation_id"])) == members


def test_a_reassignment_waits_for_a_status_change_of_its_new_agent_and_neither_deadlocks(floor):
    room = team(floor)
    agent = floor.agent(teams=[room])
    k = key(floor)
    made = task(k, {"type": "agent", "agent_id": str(agent)})
    ready, go = Future(), threading.Event()

    def status_change():
        """The order a status change takes its locks in: the agent's presence row, then its route_agent job."""
        with core.tx() as db:
            db.execute(select(T.presence).where(T.presence.c.agent_id == agent).with_for_update())
            ready.set_result(db.exec_driver_sql("SELECT pg_backend_pid()").scalar())
            go.wait(10)
            jobs.schedule(db, "route_agent", {"tenant_id": str(floor.tenant), "agent_id": str(agent)}, key=str(agent))

    with ThreadPoolExecutor(2) as pool:
        holder = pool.submit(status_change)
        pid = ready.result(10)
        target = {"target": {"type": "team", "team_id": str(room)}}
        moved = pool.submit(client.patch, f"/api/v1/tasks/{made['id']}", json=target, headers=k)
        assert waiter(pid), "the reassignment did not wait on the agent's presence row"
        go.set()
        holder.result(10)
        assert moved.result(10).status_code == 200
    assert floor.offers(made["conversation_id"]) == [agent]


def test_two_creates_racing_for_an_agents_last_task_slot_both_make_their_task_and_one_reads_failed(floor, monkeypatch):
    floor.settings(capacity=m.Capacity(task=1).model_dump())
    body = {"type": "task", "title": "Follow up", "target": {"type": "agent", "agent_id": str(floor.agent())}}
    both, takers, k = threading.Barrier(2, timeout=10), tasks._takers, key(floor)

    def read_free_together(*args):
        free = takers(*args)
        both.wait()
        return free

    monkeypatch.setattr(tasks, "_takers", read_free_together)
    with ThreadPoolExecutor(2) as pool:
        made = list(pool.map(lambda _: client.post("/api/v1/tasks", json=body, headers=k), range(2)))
    assert [r.status_code for r in made] == [201, 201], [r.text for r in made]
    assert sorted(r.json()["routing_status"] for r in made) == ["accepted", "failed"]


def test_a_create_retried_after_a_crash_past_its_commit_answers_the_first_task(floor, monkeypatch):
    idem = uuid.uuid4().hex
    headers = key(floor) | {"Idempotency-Key": idem}
    body = {"type": "task", "title": "Once", "target": {"type": "agent", "agent_id": str(floor.agent())}}

    def died(*_):
        raise RuntimeError("the process died here")

    with monkeypatch.context() as patched:
        patched.setattr(access, "_store", died)
        assert client.post("/api/v1/tasks", json=body, headers=headers).status_code == 500
    with core.tx() as db:
        stale = "UPDATE idempotency SET created_at = now() - interval '3 minutes' WHERE key_sha256 = :sha"
        db.execute(text(stale), {"sha": access.digest(idem)})
    retried = client.post("/api/v1/tasks", json=body, headers=headers)
    with core.read() as db:
        made = db.execute(select(T.task.c.id).where(T.task.c.tenant_id == floor.tenant)).scalars().all()
    assert retried.status_code == 201 and [retried.json()["id"]] == [str(i) for i in made]


def test_a_task_due_to_route_to_a_queue_holds_its_delete_until_it_routes_or_is_cancelled(floor, monkeypatch):
    agent, admin, k = floor.agent(available=False), test_routing.admin(floor), key(floor)
    first, second = floor.queue(agents=[agent]), floor.queue(agents=[agent])
    soon = task(k, {"type": "queue", "queue_id": str(first)}, scheduled_at=later(1))
    refused = admin.delete(f"/api/v1/queues/{first}")
    assert refused.status_code == 409 and refused.json()["error"]["details"] == {"scheduled": 1}
    client.patch(f"/api/v1/tasks/{soon['id']}", json={"status": "cancelled"}, headers=k).raise_for_status()
    assert admin.delete(f"/api/v1/queues/{first}").status_code == 204
    now = task(k, {"type": "agent", "agent_id": str(floor.agent())})
    with monkeypatch.context() as patched:
        patched.setattr(jobs, "run_now", lambda *_, **__: None)
        moved = {"target": {"type": "queue", "queue_id": str(second)}}
        client.patch(f"/api/v1/tasks/{now['id']}", json=moved, headers=k).raise_for_status()
    refused = admin.delete(f"/api/v1/queues/{second}")
    assert refused.status_code == 409 and refused.json()["error"]["details"] == {"scheduled": 1}
    jobs.run_now("task_route", now["id"], wait=True)
    assert admin.delete(f"/api/v1/queues/{second}").json()["error"]["details"] == {"waiting": 1}


def test_a_task_rerouted_to_its_queue_deleted_since_it_routed_closes_as_enqueue_failed(floor):
    agent, admin, k = floor.agent(), test_routing.admin(floor), key(floor)
    queue = floor.queue(agents=[agent])
    made = task(k, {"type": "queue", "queue_id": str(queue)})
    offer = floor.offer_of(made["conversation_id"], agent).id
    assert client.post(f"/api/v1/offers/{offer}/accept", headers=bearer(floor, agent)).status_code == 204
    assert admin.delete(f"/api/v1/queues/{queue}").status_code == 204
    moved = client.patch(f"/api/v1/tasks/{made['id']}", json={"reroute": True}, headers=k).json()
    over = [e.data["reason"] for e in floor.events("conversation.overflowed", made["conversation_id"])]
    assert (moved["status"], over, job("task_route", made["id"])) == ("cancelled", ["enqueue_failed"], None)
