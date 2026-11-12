"""Tasks as agents work them: how long a team offer lasts, what a task reads when nobody takes it and who is told,
the activity every routing step and edit leaves, comments with files, list filters, a task for oneself, a team task
skipping its creator, a reroute to the same team, a task raised from a conversation, what it takes from it and who
hears it, what an offered agent reads, the idle backstop, the team offer cap, a type change and a schedule's time."""

import pytest

from tests.acceptance.channels import decline, offered, take, text_line
from tests.acceptance.conftest import (
    connect,
    disposition,
    error,
    ok,
    replayed,
    seen,
    settings,
    status_changes,
    team,
    wrap_up,
)

FILE = {"url": "https://files.example/receipt.pdf"}
ACCEPTED = {"routing_status": ["pending", "accepted"]}


def create(client, **body):
    return client.post("/api/v1/tasks", json={"type": "task", "title": "Refund follow-up", **body})


def task_of(tenant, task_id: str) -> dict:
    return ok(tenant.api.get(f"/api/v1/tasks/{task_id}"))


def changes(task: dict) -> list[tuple]:
    return [(a.get("field"), a.get("from"), a.get("to")) for a in task["activity"] if a["type"] == "change"]


def team_of(tenant, *names: str, name: str = "Back office") -> tuple[str, list]:
    t = team(tenant, name)
    return t, [tenant.agent(n, teams=[t]).login() for n in names]


def test_a_team_task_offer_stays_open_past_the_text_offer_timeout_then_reads_declined(world, tenant):
    t, [agent] = team_of(tenant, "A")
    stream = tenant.stream(after=0)
    task = ok(create(tenant.api, target={"type": "team", "team_id": t}))
    cid = task["conversation_id"]
    offer = offered(stream, agent, conversation_id=cid)
    world.advance(71 * 3600 - 60)
    assert not replayed(tenant, "offer.missed", conversation_id=cid)
    world.advance(71 * 3600 + 1)
    stream.wait_for("offer.missed", offer_id=offer["data"]["offer_id"])
    stream.wait_for("task.updated", task_id=task["id"], changes={"routing_status": ["pending", "declined"]})
    assert task_of(tenant, task["id"])["routing_status"] == "declined"


def test_a_team_task_every_member_declines_reads_declined_and_tells_its_creator(world, tenant):
    t, [a, b] = team_of(tenant, "A", "B")
    stream = tenant.stream(after=0)
    task = ok(create(tenant.api, target={"type": "team", "team_id": t}))
    for agent in (a, b):
        ok(decline(agent, offered(stream, agent, conversation_id=task["conversation_id"])))
    updated = stream.wait_for("task.updated", task_id=task["id"])["data"]
    assert (updated["changes"], updated["by_agent_id"]) == ({"routing_status": ["pending", "declined"]}, None)
    declined = task_of(tenant, task["id"])
    assert (declined["status"], declined["routing_status"]) == ("todo", "declined")
    assert ("routing_status", "pending", "declined") in changes(declined)
    ok(tenant.api.patch(f"/api/v1/tasks/{task['id']}", json={"target": {"type": "agent", "agent_id": a.id}}))
    assert routed(task_of(tenant, task["id"])) == ("todo", a.id, "accepted")


def test_a_title_or_description_change_is_task_activity(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    task = ok(create(tenant.api, target={"type": "agent", "agent_id": agent.id}))
    edits = {"title": "Refund, part two", "description": "The second item"}
    ok(agent.api().patch(f"/api/v1/tasks/{task['id']}", json=edits))
    told = {"title": ["Refund follow-up", "Refund, part two"], "description": [None, "The second item"]}
    stream.wait_for("task.updated", task_id=task["id"], changes=told)
    edited = [a for a in task_of(tenant, task["id"])["activity"] if a.get("field") in ("title", "description")]
    assert sorted((a["field"], a["from"], a["to"], a["agent_id"]) for a in edited) == [
        ("description", None, "The second item", agent.id),
        ("title", "Refund follow-up", "Refund, part two", agent.id),
    ]


def test_an_agent_edits_and_deletes_a_task_comment_with_a_file(world, tenant):
    agent = tenant.agent().login()
    task = ok(create(tenant.api, target={"type": "agent", "agent_id": agent.id}))
    notes = f"/api/v1/conversations/{task['conversation_id']}/notes"
    note = ok(agent.api().post(notes, json={"text": "Call at 9", "attachments": [FILE]}))
    assert note["attachments"] == [FILE]
    ok(agent.api().patch(f"{notes}/{note['id']}", json={"text": "Call at 10"}))

    def comments() -> list:
        return [(a["text"], a["attachments"]) for a in task_of(tenant, task["id"])["activity"] if a["type"] == "note"]

    assert comments() == [("Call at 10", [FILE])]
    assert agent.api().delete(f"{notes}/{note['id']}").status_code == 204
    assert comments() == []
    error(agent.api().patch(f"{notes}/{note['id']}", json={"text": "Too late"}), 404, "note_not_found")


def test_a_creator_comments_on_its_task_before_anyone_took_it(world, tenant):
    t, [away] = team_of(tenant, "Away")
    away.set_status("Away")
    creator = tenant.agent("Creator").login()
    task = ok(create(creator.api(), target={"type": "team", "team_id": t}))
    assert task["status"] == "todo"
    ok(creator.api().post(f"/api/v1/conversations/{task['conversation_id']}/notes", json={"text": "Call at 9"}))
    assert [a["text"] for a in task_of(tenant, task["id"])["activity"] if a["type"] == "note"] == ["Call at 9"]


def test_a_comment_keeps_any_number_of_files_as_described_and_an_edit_may_change_its_files_alone(world, tenant):
    agent = tenant.agent().login()
    task = ok(create(tenant.api, target={"type": "agent", "agent_id": agent.id}))
    notes = f"/api/v1/conversations/{task['conversation_id']}/notes"
    files = [
        {"url": f"https://files.example/{n}.pdf", "name": f"{n}.pdf", "size": 2048, "content_type": "application/pdf"}
        | {"id": f"file-{n}"}
        for n in range(11)
    ]
    note = ok(agent.api().post(notes, json={"text": "Receipts", "attachments": files}))
    assert note["attachments"] == files
    ok(agent.api().patch(f"{notes}/{note['id']}", json={"attachments": files[:1]}))
    comments = [(a["text"], a["attachments"]) for a in task_of(tenant, task["id"])["activity"] if a["type"] == "note"]
    assert comments == [("Receipts", files[:1])]
    plain = {"attachments": [{"url": "http://files.example/receipt.pdf"}]}
    error(agent.api().patch(f"{notes}/{note['id']}", json=plain), 422)


def test_whoever_reads_a_task_adds_edits_and_deletes_its_comments(world, tenant):
    t, [away] = team_of(tenant, "Away")
    away.set_status("Away")
    sup = tenant.agent("Sup", role="supervisor", teams=[t]).login(available=False)
    creator, outsider = tenant.agent("Creator").login(), tenant.agent("Outsider").login()
    task = ok(create(creator.api(), target={"type": "team", "team_id": t}))
    notes = f"/api/v1/conversations/{task['conversation_id']}/notes"
    theirs = ok(creator.api().post(notes, json={"text": "Call at 9"}))
    error(sup.api().get(f"/api/v1/conversations/{task['conversation_id']}"), 404, "conversation_not_found")
    ok(sup.api().patch(f"{notes}/{theirs['id']}", json={"text": "Call at 10"}))
    ok(sup.api().post(notes, json={"text": "Ask for the receipt"}))
    assert sup.api().delete(f"{notes}/{theirs['id']}").status_code == 204
    activity = ok(sup.api().get(f"/api/v1/tasks/{task['id']}"))["activity"]
    assert [a["text"] for a in activity if a["type"] == "note"] == ["Ask for the receipt"]
    error(outsider.api().get(f"/api/v1/tasks/{task['id']}"), 404, "task_not_found")
    error(outsider.api().post(notes, json={"text": "Mine now"}), 404, "conversation_not_found")


def test_a_queue_supervisor_who_reads_a_queued_tasks_conversation_but_not_the_task_comments_on_it(world, tenant):
    t = team(tenant, "Queue team")
    sup = tenant.agent("Sup", role="supervisor", teams=[t]).login(available=False)
    task = ok(create(tenant.api, target={"type": "queue", "queue_id": tenant.queue(teams=[t])["id"]}))
    error(sup.api().get(f"/api/v1/tasks/{task['id']}"), 404, "task_not_found")
    ok(sup.api().post(f"/api/v1/conversations/{task['conversation_id']}/notes", json={"text": "Urgent"}))
    assert [a["text"] for a in task_of(tenant, task["id"])["activity"] if a["type"] == "note"] == ["Urgent"]


def test_a_team_task_with_no_free_member_reads_failed_and_says_so(world, tenant):
    t, [away] = team_of(tenant, "Away")
    away.set_status("Away")
    stream = tenant.stream(after=0)
    task = ok(create(tenant.api, target={"type": "team", "team_id": t}))
    updated = stream.wait_for("task.updated", task_id=task["id"])["data"]
    assert (updated["changes"], updated["by_agent_id"]) == ({"routing_status": ["pending", "failed"]}, None)
    failed = task_of(tenant, task["id"])
    assert (failed["status"], failed["routing_status"]) == ("todo", "failed")
    assert ("routing_status", "pending", "failed") in changes(failed)


def test_accepting_a_task_is_task_activity_by_the_agent_who_took_it(world, tenant):
    t, [agent] = team_of(tenant, "Maya")
    stream = tenant.stream(after=0)
    task = ok(create(tenant.api, target={"type": "team", "team_id": t}))
    take(stream, agent, conversation_id=task["conversation_id"])
    updated = stream.wait_for("task.updated", task_id=task["id"])["data"]
    assert (updated["changes"], updated["by_agent_id"]) == (ACCEPTED, agent.id)
    accepted = [a for a in task_of(tenant, task["id"])["activity"] if a.get("to") == "accepted"]
    assert [(a["field"], a["agent_id"]) for a in accepted] == [("routing_status", agent.id)]
    assert task_of(tenant, task["id"])["status"] == "todo"


def test_a_task_keeps_a_title_and_description_of_any_length(world, tenant):
    title, description = "R" * 5_000, "D" * 100_000
    task = ok(create(tenant.api, target={"type": "queue", "queue_id": tenant.queue()["id"]}, title=title))
    ok(tenant.api.patch(f"/api/v1/tasks/{task['id']}", json={"description": description}))
    kept = task_of(tenant, task["id"])
    assert (kept["title"], kept["description"]) == (title, description)


def test_creating_a_task_is_its_first_activity(world, tenant):
    creator, target = tenant.agent("Creator").login(), tenant.agent("Target").login()
    task = ok(create(creator.api(), target={"type": "agent", "agent_id": target.id}))
    activity = task_of(tenant, task["id"])["activity"]
    assert [(a["type"], a.get("field"), a["agent_id"]) for a in activity][:1] == [("change", "created", creator.id)]


def test_a_tasks_attachments_change_is_task_activity(world, tenant):
    agent = tenant.agent().login()
    task = ok(create(tenant.api, target={"type": "agent", "agent_id": agent.id}, attachments=[FILE]))
    assert task_of(tenant, task["id"])["attachments"] == [FILE]
    second = {"url": "https://files.example/invoice.pdf", "name": "invoice.pdf", "size": 48_213}
    second |= {"content_type": "application/pdf", "id": "file-7"}
    bare = {"name": "scan.png"}
    ok(agent.api().patch(f"/api/v1/tasks/{task['id']}", json={"attachments": [FILE, second, bare]}))
    assert ("attachments", [FILE], [FILE, second, bare]) in changes(task_of(tenant, task["id"]))


def test_tasks_filter_by_title_type_priority_status_agent_and_conversation(world, tenant):
    queue = tenant.queue()
    target = {"type": "queue", "queue_id": queue["id"]}
    a, b = tenant.agent("A").login(), tenant.agent("B").login()
    refund = ok(create(tenant.api, target=target, title="Refund the order", priority="high"))["id"]
    call = ok(create(tenant.api, target=target, type="callback", title="Call back", customer="+14155550831"))
    address = ok(create(tenant.api, target=target, title="Update the address", priority="low"))["id"]
    held = ({"title": "Owned", "target": {"type": "agent", "agent_id": x.id}} for x in (a, b))
    mine, theirs = (ok(create(tenant.api, **body))["id"] for body in held)

    def listed(**params) -> list[str]:
        return [t["id"] for t in ok(tenant.api.get("/api/v1/tasks", params=params))["data"]]

    assert listed(title="REFUND") == [refund]
    assert listed(type="callback") == [call["id"]]
    assert listed(priority="high") == [refund]
    assert listed(conversation_id=call["conversation_id"]) == [call["id"]]
    assert listed(priority=["high", "low"]) == [address, refund]
    assert listed(type=["callback", "task"], priority=["high", "medium"]) == [theirs, mine, call["id"], refund]
    assert listed(status=["todo", "resolved"]) == [theirs, mine, address, call["id"], refund]
    assert listed(routing_status="accepted") == listed(agent_id=[a.id, b.id]) == [theirs, mine]
    assert listed(agent_id=[a.id]) == [mine]


def test_an_agents_task_for_itself_needs_no_free_slot(world, tenant):
    settings(tenant, capacity={"task": 1})
    agent = tenant.agent().login()
    mine = {"type": "agent", "agent_id": agent.id}
    ok(create(tenant.api, target=mine))
    second = ok(create(agent.api(), target=mine, title="Note the address"))
    assert routed(second) == ("todo", agent.id, "accepted")


def test_a_team_task_is_not_offered_to_the_member_who_made_it(world, tenant):
    t, [maker, other] = team_of(tenant, "Maker", "Other")
    stream = tenant.stream(after=0)
    task = ok(create(maker.api(), target={"type": "team", "team_id": t}))
    offered(stream, other, conversation_id=task["conversation_id"])
    assert not replayed(tenant, "offer.created", conversation_id=task["conversation_id"], agent_id=maker.id)


def test_a_task_routes_again_when_reassigned_to_the_same_team(world, tenant):
    t, [agent] = team_of(tenant, "A")
    stream = tenant.stream(after=0)
    task = ok(create(tenant.api, target={"type": "team", "team_id": t}))
    ok(decline(agent, offered(stream, agent, conversation_id=task["conversation_id"])))
    body = {"target": {"type": "team", "team_id": t}, "reroute": True}
    ok(tenant.api.patch(f"/api/v1/tasks/{task['id']}", json=body))
    world.wait(
        lambda: len(seen(stream, "offer.created", conversation_id=task["conversation_id"], agent_id=agent.id)) == 2,
        what="a second offer to the team",
    )


def test_a_raised_tasks_events_reach_whoever_worked_its_conversation_and_no_one_it_is_hidden_from(world, tenant):
    watched = team(tenant, "Watched")
    sup = tenant.agent("Sup", role="supervisor", teams=[watched]).login(available=False)
    raiser, worker = tenant.agent("Raiser").login(), tenant.agent("Worker").login()
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, raiser, teams=[watched])
    world.plivo.text(number, "+14155550871", "Please refund me")
    origin = take(stream, raiser)
    desk, watch = (tenant.stream(token=x.stream_token, after=0) for x in (raiser, sup))
    task = ok(create(tenant.api, target={"type": "agent", "agent_id": worker.id}, conversation_id=origin))
    ok(worker.api().patch(f"/api/v1/tasks/{task['id']}", json={"description": "Refund the shoes"}))
    desk.wait_for("task.updated", task_id=task["id"], changes={"description": [None, "Refund the shoes"]})
    assert ok(raiser.api().get(f"/api/v1/tasks/{task['id']}"))["description"] == "Refund the shoes"
    error(sup.api().get(f"/api/v1/tasks/{task['id']}"), 404, "task_not_found")
    [last] = status_changes(tenant, sup, "Away")
    watch.wait_for("agent.status_changed", id=last["id"])
    assert not [e for e in watch.events if e["type"].startswith("task.")]


def test_a_raised_tasks_events_reach_the_agents_of_the_tasks_raised_beside_it_and_its_conversations_past_agent(
    world, tenant
):
    raiser, first, second = (tenant.agent(n).login() for n in ("Raiser", "First", "Second"))
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, raiser)
    world.plivo.text(number, "+14155550891", "Please refund me")
    origin = take(stream, raiser)
    raised = ({"type": "agent", "agent_id": x.id} for x in (first, second))
    _, task = (ok(create(tenant.api, target=to, conversation_id=origin))["id"] for to in raised)
    ok(raiser.api().post(f"/api/v1/conversations/{origin}/close"))
    stream.wait_for("conversation.wrap_up_started", conversation_id=origin)
    ok(wrap_up(raiser, origin, disposition(tenant)["id"]))
    stream.wait_for("conversation.closed", conversation_id=origin)
    ok(second.api().patch(f"/api/v1/tasks/{task}", json={"description": "Refund the shoes"}))
    assert ok(first.api().get(f"/api/v1/tasks/{task}"))["description"] == "Refund the shoes"
    for x in (first, raiser):
        tenant.stream(token=x.stream_token, after=0).wait_for(
            "task.updated", task_id=task, changes={"description": [None, "Refund the shoes"]}
        )


def test_a_participant_removed_from_a_conversation_no_longer_hears_the_tasks_raised_from_it(world, tenant):
    third, worker = tenant.agent("Third").login(), tenant.agent("Worker").login()
    live = connect(tenant)
    url = f"/api/v1/calls/{live.id}/participants"
    pid = ok(live.agent.api().post(url, json={"target": {"type": "agent", "agent_id": third.id}}))["participant_id"]
    live.stream.wait_for("offer.accepted", offer_id=pid)
    ok(live.agent.api().delete(f"{url}/{pid}"))
    live.stream.wait_for("call.participant_removed", conversation_id=live.id)
    task = ok(create(tenant.api, target={"type": "agent", "agent_id": worker.id}, conversation_id=live.id))["id"]
    ok(worker.api().patch(f"/api/v1/tasks/{task}", json={"description": "Refund the shoes"}))
    desk, gone = (tenant.stream(token=x.stream_token, after=0) for x in (live.agent, third))
    desk.wait_for("task.updated", task_id=task, changes={"description": [None, "Refund the shoes"]})
    [last] = status_changes(tenant, third, "Away")
    gone.wait_for("agent.status_changed", id=last["id"])
    assert not [e for e in gone.events if e["type"].startswith("task.")]


def test_a_task_is_raised_from_a_conversation_in_its_wrap_up_but_not_once_it_closed(world, tenant):
    raiser = tenant.agent("Raiser").login()
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, raiser)
    world.plivo.text(number, "+14155550881", "Please call me")
    origin = take(stream, raiser)
    ok(raiser.api().post(f"/api/v1/conversations/{origin}/close"))
    stream.wait_for("conversation.wrap_up_started", conversation_id=origin)
    body = {"target": {"type": "agent", "agent_id": raiser.id}, "conversation_id": origin}
    ok(create(raiser.api(), **body))
    ok(wrap_up(raiser, origin, disposition(tenant)["id"]))
    stream.wait_for("conversation.closed", conversation_id=origin)
    error(create(raiser.api(), **body), 422, "conversation_closed")


def test_a_task_raised_from_a_conversation_is_listed_with_it_and_heard_by_its_agent(world, tenant):
    raiser = tenant.agent("Raiser").login()
    t, [worker] = team_of(tenant, "Worker")
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, raiser)
    world.plivo.text(number, "+14155550841", "Please refund me")
    origin = take(stream, raiser)
    watch = tenant.stream(token=raiser.stream_token, after=0)
    body = {"target": {"type": "team", "team_id": t}, "conversation_id": origin}
    task = ok(create(raiser.api(), **body))
    listed = ok(tenant.api.get("/api/v1/tasks", params={"conversation_id": origin}))["data"]
    assert [x["id"] for x in listed] == [task["id"]]
    take(stream, worker, conversation_id=task["conversation_id"])
    heard = watch.wait_for("task.updated", task_id=task["id"])
    assert heard["data"]["changes"] == ACCEPTED
    timeline = ok(tenant.api.get(f"/api/v1/conversations/{origin}/timeline"))["data"]
    assert {"task.created", "task.updated"} <= {e["type"] for e in timeline}


@pytest.mark.parametrize("kind", ["task", "callback"])
def test_an_accepted_task_or_callback_left_idle_fifteen_days_resolves_and_frees_the_slot(world, tenant, kind):
    settings(tenant, capacity={kind: 1})
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    body = {"type": kind, "target": {"type": "agent", "agent_id": agent.id}}
    body |= {"customer": "+14155550851"} if kind == "callback" else {}
    task = ok(create(tenant.api, **body))
    world.advance(15 * 24 * 3600 - 60)
    assert not replayed(tenant, "conversation.closed", conversation_id=task["conversation_id"])
    assert routed(task_of(tenant, task["id"])) == ("todo", agent.id, "accepted")
    assert ok(create(tenant.api, **body))["routing_status"] == "failed"
    world.advance(15 * 24 * 3600 + 1)
    closed = stream.wait_for("conversation.closed", conversation_id=task["conversation_id"])
    assert closed["data"]["result"] == "session_timeout"
    assert task_of(tenant, task["id"])["status"] == "resolved"
    assert ok(create(tenant.api, **body))["routing_status"] == "accepted"


def test_a_team_task_is_offered_to_at_most_the_tenants_simultaneous_limit(world, tenant):
    settings(tenant, simultaneous_agent_limit=2)
    t, _ = team_of(tenant, "A", "B", "C")
    stream = tenant.stream(after=0)
    task = ok(create(tenant.api, target={"type": "team", "team_id": t}))
    world.wait(lambda: seen(stream, "offer.created", conversation_id=task["conversation_id"]), what="an offer")
    assert len(replayed(tenant, "offer.created", conversation_id=task["conversation_id"])) == 2


def test_a_tasks_type_change_is_task_activity_keeps_its_agents_slot_and_waits_out_its_offers(world, tenant):
    settings(tenant, capacity={"task": 1, "callback": 1})
    agent = tenant.agent().login()
    mine = {"type": "agent", "agent_id": agent.id}
    task = ok(create(tenant.api, target=mine))
    ok(agent.api().patch(f"/api/v1/tasks/{task['id']}", json={"type": "callback"}))
    changed = task_of(tenant, task["id"])
    assert (changed["type"], ("type", "task", "callback") in changes(changed)) == ("callback", True)
    assert ok(tenant.api.get(f"/api/v1/conversations/{task['conversation_id']}"))["channel"] == "callback"
    error(agent.api().patch(f"/api/v1/tasks/{task['id']}", json={"type": "bogus"}), 422)
    assert ok(create(tenant.api, title="No task slot is free", target=mine))["routing_status"] == "failed"
    assert ok(create(tenant.api, type="callback", customer="+14155550861", target=mine))["routing_status"] == "accepted"
    error(tenant.api.patch(f"/api/v1/tasks/{task['id']}", json={"reroute": True}), 409, "agent_at_capacity")
    assert routed(task_of(tenant, task["id"])) == ("todo", agent.id, "accepted")
    t, [member] = team_of(tenant, "Member")
    stream = tenant.stream(after=0)
    pending = ok(create(tenant.api, target={"type": "team", "team_id": t}))
    offered(stream, member, conversation_id=pending["conversation_id"])
    for change in ({"type": "callback"}, {"target": mine}):
        error(tenant.api.patch(f"/api/v1/tasks/{pending['id']}", json=change), 409, "task_routing")
    same = {k: pending[k] for k in ("type", "target")}
    ok(tenant.api.patch(f"/api/v1/tasks/{pending['id']}", json=same))


def test_a_schedule_is_a_local_time_with_its_offset_kept_with_its_zone_and_type(world, tenant):
    queue = tenant.queue()
    target = {"type": "queue", "queue_id": queue["id"]}
    kolkata = {"local_time": "2031-01-01T09:00:00+05:30", "timezone": "Asia/Kolkata", "type": "once"}
    task = ok(create(tenant.api, target=target, schedule=kolkata))
    assert (task["schedule"], task["scheduled_at"]) == (kolkata, "2031-01-01T03:30:00Z")
    repeated = {"local_time": "2031-11-02T01:30:00-05:00", "timezone": "America/New_York", "type": "once"}
    moved = ok(tenant.api.patch(f"/api/v1/tasks/{task['id']}", json={"schedule": repeated}))
    assert (moved["schedule"], moved["scheduled_at"]) == (repeated, "2031-11-02T06:30:00Z")
    assert [c for c in changes(task_of(tenant, task["id"])) if c[0] == "schedule"] == [("schedule", kolkata, repeated)]
    past, naive = (kolkata | {"local_time": t} for t in ("2021-01-01T09:00:00+05:30", "2031-01-01T09:00:00"))
    untyped = {k: v for k, v in kolkata.items() if k != "type"}
    for refused in (past, naive, untyped, kolkata | {"timezone": "Mars/Base"}):
        error(create(tenant.api, target=target, schedule=refused), 422)
    error(create(tenant.api, target=target, schedule=kolkata, scheduled_at="2031-01-01T03:30:00Z"), 422)


def test_a_scheduled_at_in_the_past_is_refused_as_a_past_schedule_is(world, tenant):
    target = {"type": "queue", "queue_id": tenant.queue()["id"]}
    error(create(tenant.api, target=target, scheduled_at="2021-01-01T00:00:00Z"), 422)
    task = ok(create(tenant.api, target=target, scheduled_at="2031-01-01T00:00:00Z"))
    error(tenant.api.patch(f"/api/v1/tasks/{task['id']}", json={"scheduled_at": "2021-01-01T00:00:00Z"}), 422)
    assert task_of(tenant, task["id"])["scheduled_at"] == "2031-01-01T00:00:00Z"


def test_a_task_raised_from_a_conversation_takes_its_customer_and_direction(world, tenant):
    raiser = tenant.agent("Raiser").login()
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, raiser)
    world.plivo.text(number, "+14155550811", "Please call me back")
    origin = take(stream, raiser)
    raised = {"target": {"type": "agent", "agent_id": raiser.id}, "conversation_id": origin}
    task, callback = ok(create(raiser.api(), **raised)), ok(create(raiser.api(), type="callback", **raised))
    assert (task["customer"], callback["customer"]) == ("+14155550811", "+14155550811")
    opened = ok(tenant.api.get(f"/api/v1/conversations/{task['conversation_id']}"))
    assert (opened["direction"], opened["number"]) == ("inbound", "+14155550811")


def test_a_tasks_routing_outcome_is_told_to_whoever_changed_it_last(world, tenant):
    t = team(tenant, "Back office")
    lead = tenant.agent("Lead", role="supervisor", teams=[t]).login(available=False)
    member = tenant.agent("Member", teams=[t]).login()
    stream = tenant.stream(after=0)
    task = ok(create(tenant.api, target={"type": "team", "team_id": t}))
    offer = offered(stream, member, conversation_id=task["conversation_id"])
    edited = ok(lead.api().patch(f"/api/v1/tasks/{task['id']}", json={"priority": "high"}))
    assert (edited["created_by"], edited["updated_by"]) == (None, lead.id)
    ok(decline(member, offer))
    told = stream.wait_for("task.updated", task_id=task["id"], changes={"routing_status": ["pending", "declined"]})
    assert told["agent_id"] == lead.id


def test_a_reassigned_tasks_new_agent_is_told_to_the_agent_it_was_taken_from(world, tenant):
    t, [member] = team_of(tenant, "Member")
    first = tenant.agent("First").login()
    stream = tenant.stream(after=0)
    task = ok(create(tenant.api, target={"type": "agent", "agent_id": first.id}))
    ok(tenant.api.patch(f"/api/v1/tasks/{task['id']}", json={"target": {"type": "team", "team_id": t}}))
    take(stream, member, conversation_id=task["conversation_id"])
    told = stream.wait_for("task.updated", task_id=task["id"], by_agent_id=member.id)
    assert (told["agent_id"], told["data"]["changes"]) == (first.id, ACCEPTED)


def test_routing_a_taken_task_again_is_task_activity(world, tenant):
    t, [agent] = team_of(tenant, "A")
    stream = tenant.stream(after=0)
    task = ok(create(tenant.api, target={"type": "team", "team_id": t}))
    take(stream, agent, conversation_id=task["conversation_id"])
    ok(tenant.api.patch(f"/api/v1/tasks/{task['id']}", json={"reroute": True}))
    stream.wait_for("task.updated", task_id=task["id"], changes={"routing_status": ["accepted", "pending"]})
    again = task_of(tenant, task["id"])
    assert (again["status"], ("routing_status", "accepted", "pending") in changes(again)) == ("todo", True)


def test_an_agent_offered_a_task_reads_it_and_comments_before_taking_it(world, tenant):
    t, [member] = team_of(tenant, "Member")
    stream = tenant.stream(after=0)
    task = ok(create(tenant.api, target={"type": "team", "team_id": t}, description="Refund the shoes"))
    offered(stream, member, conversation_id=task["conversation_id"])
    read = ok(member.api().get(f"/api/v1/tasks/{task['id']}"))
    assert (read["description"], read["routing_status"]) == ("Refund the shoes", "pending")
    note = {"text": "On it after lunch"}
    ok(member.api().post(f"/api/v1/conversations/{task['conversation_id']}/notes", json=note))
    assert [a["text"] for a in task_of(tenant, task["id"])["activity"] if a["type"] == "note"] == [note["text"]]


def test_a_tasks_type_sets_what_it_takes_an_internal_task_no_time_and_a_follow_up_its_channel(world, tenant):
    agent = tenant.agent().login()
    mine, later = {"type": "agent", "agent_id": agent.id}, "2031-01-01T00:00:00Z"
    for refused in ({"scheduled_at": later}, {"channel": "sms"}):
        error(create(agent.api(), type="internal", target=mine, **refused), 422)
    assert ok(create(agent.api(), type="internal", target=mine))["type"] == "internal"
    error(create(tenant.api, type="follow_up", target=mine, scheduled_at=later), 422)
    error(create(agent.api(), type="follow_up", target=mine, channel="sms"), 422)
    keyed = ok(create(tenant.api, type="follow_up", target=mine, channel="whatsapp"))
    assert (keyed["type"], keyed["channel"]) == ("follow_up", "whatsapp")
    assert ok(create(agent.api(), type="follow_up", target=mine, channel="sms", scheduled_at=later))["channel"] == "sms"
    queue = {"type": "queue", "queue_id": tenant.queue()["id"]}
    assert ok(create(tenant.api, type="others", target=queue, channel="call"))["type"] == "others"


def test_a_follow_ups_new_target_does_not_take_it_from_its_agent(world, tenant):
    first, second = tenant.agent("First").login(), tenant.agent("Second").login()
    task = ok(create(tenant.api, type="follow_up", channel="sms", target={"type": "agent", "agent_id": first.id}))
    moved = {"target": {"type": "agent", "agent_id": second.id}}
    kept = ok(tenant.api.patch(f"/api/v1/tasks/{task['id']}", json=moved))
    assert (*routed(kept), kept["target"]) == ("todo", first.id, "accepted", moved["target"])


def test_an_agent_marks_its_task_waiting_on_the_customer_and_back_and_its_conversation_stays_open(world, tenant):
    agent = tenant.agent().login()
    task = ok(create(tenant.api, target={"type": "agent", "agent_id": agent.id}))
    url = f"/api/v1/tasks/{task['id']}"
    waiting = ok(agent.api().patch(url, json={"status": "waiting_on_customer"}))
    assert (waiting["status"], waiting["agent_id"]) == ("waiting_on_customer", agent.id)
    listed = ok(tenant.api.get("/api/v1/tasks", params={"status": "waiting_on_customer"}))["data"]
    assert [x["id"] for x in listed] == [task["id"]]
    assert ok(tenant.api.get(f"/api/v1/conversations/{task['conversation_id']}"))["state"] == "active"
    assert ok(agent.api().patch(url, json={"status": "in_progress"}))["status"] == "in_progress"
    steps = [c for c in changes(task_of(tenant, task["id"])) if c[0] == "status"]
    assert steps == [("status", "todo", "waiting_on_customer"), ("status", "waiting_on_customer", "in_progress")]


def test_a_tasks_routing_status_reads_how_its_routing_stands_and_filters_the_list(world, tenant):
    agent = tenant.agent("Owner").login()
    t, [member] = team_of(tenant, "Member")
    gone, [away] = team_of(tenant, "Away", name="Gone")
    away.set_status("Away")
    queue = {"type": "queue", "queue_id": tenant.queue()["id"]}
    stream = tenant.stream(after=0)
    made = {
        "pending": ok(create(tenant.api, target=queue)),
        "scheduled": ok(create(tenant.api, target=queue, scheduled_at="2031-01-01T00:00:00Z")),
        "accepted": ok(create(tenant.api, target={"type": "agent", "agent_id": agent.id})),
        "failed": ok(create(tenant.api, target={"type": "team", "team_id": gone})),
        "declined": ok(create(tenant.api, target={"type": "team", "team_id": t})),
    }
    ok(decline(member, offered(stream, member, conversation_id=made["declined"]["conversation_id"])))
    for outcome in ("failed", "declined"):
        stream.wait_for("task.updated", task_id=made[outcome]["id"], changes={"routing_status": ["pending", outcome]})
    reads = {x["id"]: x["routing_status"] for x in ok(tenant.api.get("/api/v1/tasks"))["data"]}
    assert reads == {task["id"]: routing for routing, task in made.items()}
    listed = ok(tenant.api.get("/api/v1/tasks", params={"routing_status": ["scheduled", "failed"]}))["data"]
    assert [x["id"] for x in listed] == [made["failed"]["id"], made["scheduled"]["id"]]


def routed(task: dict) -> tuple:
    return task["status"], task["agent_id"], task["routing_status"]


def test_an_agent_sets_the_task_it_holds_back_to_todo_and_keeps_it(world, tenant):
    agent = tenant.agent().login()
    task = ok(create(tenant.api, target={"type": "agent", "agent_id": agent.id}))
    url = f"/api/v1/tasks/{task['id']}"
    ok(agent.api().patch(url, json={"status": "waiting_on_customer"}))
    assert routed(ok(agent.api().patch(url, json={"status": "todo"}))) == ("todo", agent.id, "accepted")
    assert [x["id"] for x in ok(tenant.api.get("/api/v1/tasks", params={"status": "todo"}))["data"]] == [task["id"]]


def test_a_status_sent_with_a_new_target_or_schedule_routes_the_task_too(world, tenant):
    first, second = tenant.agent("First").login(), tenant.agent("Second").login()
    task = ok(create(tenant.api, target={"type": "agent", "agent_id": first.id}))
    url = f"/api/v1/tasks/{task['id']}"
    moved = {"status": "waiting_on_customer", "target": {"type": "agent", "agent_id": second.id}}
    assert routed(ok(tenant.api.patch(url, json=moved))) == ("waiting_on_customer", second.id, "accepted")
    postponed = {"status": "in_progress", "scheduled_at": "2031-01-01T00:00:00Z"}
    assert routed(ok(tenant.api.patch(url, json=postponed))) == ("in_progress", None, "scheduled")


def test_a_tasks_routing_outcome_and_its_work_status_change_apart(world, tenant):
    maker = tenant.agent("Maker").login()
    t, [member] = team_of(tenant, "Member")
    gone, [away] = team_of(tenant, "Away", name="Gone")
    away.set_status("Away")
    stream = tenant.stream(after=0)
    failed = ok(create(maker.api(), target={"type": "team", "team_id": gone}))
    stream.wait_for("task.updated", task_id=failed["id"], changes={"routing_status": ["pending", "failed"]})
    marked = ok(maker.api().patch(f"/api/v1/tasks/{failed['id']}", json={"status": "waiting_on_customer"}))
    assert routed(marked) == ("waiting_on_customer", None, "failed")
    pending = ok(create(maker.api(), target={"type": "team", "team_id": t}))
    offer = offered(stream, member, conversation_id=pending["conversation_id"])
    ok(maker.api().patch(f"/api/v1/tasks/{pending['id']}", json={"status": "waiting_on_customer"}))
    ok(decline(member, offer))
    stream.wait_for("task.updated", task_id=pending["id"], changes={"routing_status": ["pending", "declined"]})
    assert routed(task_of(tenant, pending["id"])) == ("waiting_on_customer", None, "declined")


def test_clearing_an_agent_tasks_schedule_assigns_it_now_and_it_no_longer_reads_scheduled(world, tenant):
    agent = tenant.agent().login()
    task = ok(create(tenant.api, target={"type": "agent", "agent_id": agent.id}, scheduled_at="2031-01-01T00:00:00Z"))
    assert routed(task) == ("todo", None, "scheduled")
    now = ok(tenant.api.patch(f"/api/v1/tasks/{task['id']}", json={"scheduled_at": None}))
    assert routed(now) == ("todo", agent.id, "accepted")

    def listed(routing: str) -> list[str]:
        return [x["id"] for x in ok(tenant.api.get("/api/v1/tasks", params={"routing_status": routing}))["data"]]

    assert (listed("scheduled"), listed("accepted")) == ([], [task["id"]])


def test_a_direct_assignment_is_an_acceptance_told_as_a_team_ones_is(world, tenant):
    maker, first, second = (tenant.agent(n).login() for n in ("Maker", "First", "Second"))
    stream = tenant.stream(after=0)
    task = ok(create(maker.api(), target={"type": "agent", "agent_id": first.id}))
    told = stream.wait_for("task.updated", task_id=task["id"], by_agent_id=first.id)
    assert (told["agent_id"], told["data"]["changes"]) == (maker.id, ACCEPTED)
    ok(maker.api().patch(f"/api/v1/tasks/{task['id']}", json={"target": {"type": "agent", "agent_id": second.id}}))
    moved = stream.wait_for("task.updated", task_id=task["id"], by_agent_id=second.id)
    assert (moved["agent_id"], moved["data"]["changes"]) == (first.id, ACCEPTED)
    took = ("routing_status", "pending", "accepted")
    steps = [c for c in changes(task_of(tenant, task["id"])) if c[0] in ("status", "routing_status")]
    assert steps == [took, ("routing_status", "accepted", "pending"), took]
