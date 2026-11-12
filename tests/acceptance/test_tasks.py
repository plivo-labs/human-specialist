"""Agent tasks and callbacks: created over the API for a queue, a team or an agent, offered like text, held
against task capacity unless an agent made its own, scheduled, and resolved with their activity kept as entries."""

from datetime import UTC, datetime, timedelta

from tests.acceptance.channels import decline, offered, take
from tests.acceptance.conftest import (
    dial,
    disposition,
    error,
    fence,
    in_order,
    lead,
    ok,
    replayed,
    seen,
    settings,
    status_changes,
    team,
    wrap_up,
)


def create(client, **body):
    return client.post("/api/v1/tasks", json={"type": "task", "title": "Refund follow-up", **body})


def task_of(tenant, task_id: str) -> dict:
    return ok(tenant.api.get(f"/api/v1/tasks/{task_id}"))


def resolve(agent, task_id: str):
    return agent.api().patch(f"/api/v1/tasks/{task_id}", json={"status": "resolved"})


def later(hours: float) -> str:
    return (datetime.now(UTC) + timedelta(hours=hours)).isoformat()


def offered_one_of_two(tenant):
    """A task on a queue of agents A and B that offers one agent per round. Returns (stream, first offer, the agent
    offered first, the other)."""
    a, b = tenant.agent("A").login(), tenant.agent("B").login()
    stream = tenant.stream(after=0)
    queue = tenant.queue(agents=[a.id, b.id], agents_at_once=1, retries=1)
    ok(create(tenant.api, target={"type": "queue", "queue_id": queue["id"]}))
    first = stream.wait_for("offer.created")
    return (stream, first, *((a, b) if first["agent_id"] == a.id else (b, a)))


def test_unmarking_waiting_on_a_task_conversation_is_a_no_op(tenant):
    task = ok(create(tenant.api, target={"type": "queue", "queue_id": tenant.queue()["id"]}))
    url = f"/api/v1/conversations/{task['conversation_id']}"
    ok(tenant.api.patch(url, json={"waiting": False}))
    error(tenant.api.patch(url, json={"waiting": True}), 409, "not_text")


def test_a_task_for_a_queue_is_offered_and_accepted(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    queue = tenant.queue(agents=[agent.id])
    task = ok(create(tenant.api, priority="high", target={"type": "queue", "queue_id": queue["id"]}))
    created = stream.wait_for("task.created", task_id=task["id"])["data"]
    assert (created["type"], created["title"], created["priority"]) == ("task", "Refund follow-up", "high")
    assert take(stream, agent, channel="task") == task["conversation_id"]
    taken = task_of(tenant, task["id"])
    assert (taken["status"], taken["agent_id"], taken["routing_status"]) == ("todo", agent.id, "accepted")


def test_a_declined_task_offer_moves_to_the_other_agent(tenant):
    stream, first, declining, other = offered_one_of_two(tenant)
    ok(decline(declining, first))
    stream.wait_for("offer.rejected", offer_id=first["data"]["offer_id"])
    offered(stream, other, conversation_id=first["conversation_id"])


def test_an_unanswered_task_offer_moves_to_the_other_agent_without_a_strike(world, tenant):
    settings(tenant, rona={"strikes": 1}, text={"offer_timeout_sec": 30})
    stream, first, missed, other = offered_one_of_two(tenant)
    world.advance(71 * 3600 - 60)
    assert not replayed(tenant, "offer.missed", offer_id=first["data"]["offer_id"])
    world.advance(71 * 3600 + 1)
    assert stream.wait_for("offer.missed", offer_id=first["data"]["offer_id"])["data"].get("strike") is None
    offered(stream, other, conversation_id=first["conversation_id"])
    assert ok(tenant.api.get(f"/api/v1/agents/{missed.id}/status"))["status"]["name"] == "Available"


def test_a_task_of_any_type_its_queue_gives_up_on_reads_failed_and_waits_to_route_again(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    queue = tenant.queue(agents=[agent.id], retries=1, text={"wait_timeout_sec": 60})
    target = {"type": "queue", "queue_id": queue["id"]}
    kinds = {"task": {}, "callback": {"customer": "+14155550801"}, "internal": {}}
    kinds |= {"follow_up": {"channel": "sms"}, "others": {}}
    made = [ok(create(tenant.api, type=k, target=target, **more)) for k, more in kinds.items()]
    first = made[0]["conversation_id"]
    ok(decline(agent, offered(stream, agent, conversation_id=first)))
    world.advance(61)
    for task in made:
        cid = task["conversation_id"]
        assert stream.wait_for("conversation.overflowed", conversation_id=cid)["data"]["reason"] == "wait_timeout"
        stream.wait_for("task.updated", task_id=task["id"], changes={"routing_status": ["pending", "failed"]})
        read = task_of(tenant, task["id"])
        assert (read["status"], read["routing_status"]) == ("todo", "failed"), task["type"]
        assert ok(tenant.api.get(f"/api/v1/conversations/{cid}"))["state"] == "active"
    assert not replayed(tenant, "conversation.closed")
    ok(tenant.api.patch(f"/api/v1/tasks/{made[0]['id']}", json={"reroute": True}))
    world.wait(lambda: len(seen(stream, "conversation.queued", conversation_id=first)) == 2, what="queued again")


def test_a_task_holds_a_task_slot_until_it_is_resolved(world, tenant):
    settings(tenant, capacity={"task": 1})
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    queue = tenant.queue(agents=[agent.id])
    first = ok(create(tenant.api, target={"type": "queue", "queue_id": queue["id"]}))
    take(stream, agent, conversation_id=first["conversation_id"])
    second = ok(create(tenant.api, title="Second", target={"type": "queue", "queue_id": queue["id"]}))
    stream.wait_for("conversation.queued", conversation_id=second["conversation_id"])
    assert not replayed(tenant, "offer.created", conversation_id=second["conversation_id"])
    ok(resolve(agent, first["id"]))
    stream.wait_for("conversation.wrap_up_started", conversation_id=first["conversation_id"])
    ok(wrap_up(agent, first["conversation_id"], disposition(tenant)["id"]))
    stream.wait_for("conversation.closed", conversation_id=first["conversation_id"])
    assert task_of(tenant, first["id"])["status"] == "resolved"
    offered(stream, agent, conversation_id=second["conversation_id"])


def test_a_tasks_activity_lists_its_note_and_each_change_in_order(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    task = ok(create(tenant.api, priority="low", target={"type": "agent", "agent_id": agent.id}))
    ok(agent.api().post(f"/api/v1/conversations/{task['conversation_id']}/notes", json={"text": "Prefers mornings"}))
    ok(agent.api().patch(f"/api/v1/tasks/{task['id']}", json={"priority": "critical"}))
    ok(resolve(agent, task["id"]))
    stream.wait_for("task.updated", task_id=task["id"], by_agent_id=agent.id, changes={"priority": ["low", "critical"]})
    activity = task_of(tenant, task["id"])["activity"]
    steps = [a["text"] if a["type"] == "note" else (a["field"], a["from"], a["to"]) for a in activity]
    assert in_order(steps, "Prefers mornings", ("priority", "low", "critical"), ("status", "todo", "resolved"))


def test_task_events_reach_the_assigned_agent_and_its_supervisor_and_no_other_agent(tenant):
    t = team(tenant, "Back office")
    sup = tenant.agent("Sup", role="supervisor", teams=[t]).login(available=False)
    agent, outsider = tenant.agent("Owner", teams=[t]).login(), tenant.agent("Outsider").login()
    desk, watch, elsewhere = (tenant.stream(token=a.stream_token, after=0) for a in (agent, sup, outsider))
    task = ok(create(tenant.api, target={"type": "agent", "agent_id": agent.id}))
    ok(agent.api().patch(f"/api/v1/tasks/{task['id']}", json={"priority": "high"}))
    for stream in (desk, watch):
        stream.wait_for("task.created", task_id=task["id"])
        stream.wait_for("task.updated", task_id=task["id"])
    [last] = status_changes(tenant, outsider, "Away")
    elsewhere.wait_for("agent.status_changed", id=last["id"])
    assert not [e for e in elsewhere.events if e["type"].startswith("task.")]


def test_a_scheduled_task_routes_at_its_time_and_moving_the_time_moves_it(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    queue = tenant.queue(agents=[agent.id])
    task = ok(create(tenant.api, target={"type": "queue", "queue_id": queue["id"]}, scheduled_at=later(1)))
    cid = task["conversation_id"]
    ok(tenant.api.patch(f"/api/v1/tasks/{task['id']}", json={"scheduled_at": later(2)}))
    world.advance(3600 + 60)
    assert (
        not replayed(tenant, "offer.created", conversation_id=cid) and task_of(tenant, task["id"])["status"] == "todo"
    )
    world.advance(7200 + 60)
    offered(stream, agent, conversation_id=cid)


def test_a_task_for_an_agent_at_task_capacity_is_made_failed_and_tells_its_maker(world, tenant):
    settings(tenant, capacity={"task": 1})
    agent, maker = tenant.agent().login(), tenant.agent("Maker").login()
    stream = tenant.stream(after=0)
    target = {"type": "agent", "agent_id": agent.id}
    ok(create(tenant.api, target=target))
    made = create(maker.api(), title="One too many", target=target)
    assert made.status_code == 201
    task = made.json()
    told = stream.wait_for("task.updated", task_id=task["id"])
    assert (told["agent_id"], told["data"]["changes"]) == (maker.id, {"routing_status": ["pending", "failed"]})
    read = task_of(tenant, task["id"])
    assert (read["status"], read["agent_id"], read["routing_status"]) == ("todo", None, "failed")


def test_a_task_for_a_team_offers_every_available_member(world, tenant):
    t = team(tenant, "Back office")
    stream = tenant.stream(after=0)
    a, b = tenant.agent("A", teams=[t]).login(), tenant.agent("B", teams=[t]).login()
    away = tenant.agent("Away", teams=[t]).login()
    away.set_status("Away")
    tenant.agent("Outsider").login()
    cid = ok(create(tenant.api, target={"type": "team", "team_id": t}))["conversation_id"]
    offered(stream, a, conversation_id=cid)
    offered(stream, b, conversation_id=cid)
    fence(tenant)
    assert {e["agent_id"] for e in seen(stream, "offer.created", conversation_id=cid)} == {a.id, b.id}


def test_a_team_task_whose_last_offered_member_goes_away_reads_declined(world, tenant):
    t = team(tenant, "Back office")
    member = tenant.agent("Member", teams=[t]).login()
    stream = tenant.stream(after=0)
    task = ok(create(tenant.api, target={"type": "team", "team_id": t}))
    offered(stream, member, conversation_id=task["conversation_id"])
    member.set_status("Away")
    stream.wait_for("task.updated", task_id=task["id"], changes={"routing_status": ["pending", "declined"]})


def test_an_agents_own_task_holds_none_of_its_task_slots(world, tenant):
    settings(tenant, capacity={"task": 1})
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    ok(create(agent.api(), target={"type": "agent", "agent_id": agent.id}))
    queued = ok(create(tenant.api, target={"type": "queue", "queue_id": tenant.queue(agents=[agent.id])["id"]}))
    offered(stream, agent, conversation_id=queued["conversation_id"])


def test_a_callback_task_is_worked_by_calling_the_customer_then_resolved(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    queue = tenant.queue(agents=[agent.id])
    number, _ = lead(world)
    target = {"type": "queue", "queue_id": queue["id"]}
    task = ok(create(tenant.api, type="callback", title="Call me back", customer=number, target=target))
    take(stream, agent, conversation_id=task["conversation_id"], channel="callback")
    _, call = dial(tenant, agent, to=number, context={"task_id": task["id"]})
    assert stream.wait_for("conversation.started", conversation_id=call)["context"] == {"task_id": task["id"]}
    ok(resolve(agent, task["id"]))
    assert task_of(tenant, task["id"])["status"] == "resolved"


def test_a_task_naming_another_tenants_queue_is_422(world, tenant):
    foreign = world.new_tenant("Other").queue()
    error(create(tenant.api, target={"type": "queue", "queue_id": foreign["id"]}), 422, "foreign_id")


def test_a_callback_scheduled_for_a_queue_holds_its_delete_until_it_names_another_queue(world, tenant):
    agent = tenant.agent().login()
    first, second = tenant.queue(agents=[agent.id]), tenant.queue(agents=[agent.id])
    target = {"type": "queue", "queue_id": first["id"]}
    callback = ok(create(tenant.api, type="callback", customer="+14155550802", target=target, scheduled_at=later(1)))
    assert error(tenant.api.delete(f"/api/v1/queues/{first['id']}"), 409, "queue_in_use")["details"] == {"scheduled": 1}
    moved = {"target": {"type": "queue", "queue_id": second["id"]}}
    ok(tenant.api.patch(f"/api/v1/tasks/{callback['id']}", json=moved))
    assert tenant.api.delete(f"/api/v1/queues/{first['id']}").status_code == 204


def test_a_callback_rescheduled_after_its_queue_was_deleted_closes_as_enqueue_failed_at_its_time(world, tenant):
    agent = tenant.agent().login()
    stream, q = tenant.stream(after=0), tenant.queue(agents=[agent.id])
    target = {"type": "queue", "queue_id": q["id"]}
    callback = ok(create(tenant.api, type="callback", customer="+14155550803", target=target))
    cid = callback["conversation_id"]
    take(stream, agent, conversation_id=cid, channel="callback")
    ok(tenant.api.delete(f"/api/v1/queues/{q['id']}"))
    ok(tenant.api.patch(f"/api/v1/tasks/{callback['id']}", json={"scheduled_at": later(1)}))
    world.advance(3600 + 60)
    assert stream.wait_for("conversation.overflowed", conversation_id=cid)["data"]["reason"] == "enqueue_failed"
    assert stream.wait_for("conversation.closed", conversation_id=cid)["data"]["result"] == "enqueue_failed"
    assert task_of(tenant, callback["id"])["status"] == "cancelled"
