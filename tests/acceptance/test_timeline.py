"""The per-call timeline: every hop of a conversation, in order, from the event outbox, for a key, a supervisor, and
an agent who took the conversation as its stream gives it."""

import json

from tests.acceptance.channels import enqueue_text, take
from tests.acceptance.conftest import (
    connect,
    disposition,
    error,
    hangup_action,
    hours,
    hs_header,
    in_order,
    menu,
    ok,
    queue_action,
    redirecting_line,
    run_jobs_until,
    seen,
    team,
    types,
    unavailable,
    voicemail_action,
    with_query,
    wrap_up,
)

HOPS = [
    "conversation.started",
    "conversation.menu_input",
    "conversation.queued",
    "offer.created",
    "offer.rejected",
    "offer.created",
    "offer.accepted",
    "call.ended",
    "conversation.wrap_up_started",
    "conversation.wrapped_up",
    "conversation.closed",
]


def timeline(client, cid: str) -> list[dict]:
    return ok(client.get(f"/api/v1/conversations/{cid}/timeline"))["data"]


def test_the_timeline_lists_every_hop_in_order(world, tenant):
    first, second = tenant.agent("First").login(), tenant.agent("Second").login()
    first.phone.on_ring("decline")
    q = tenant.queue(name="Sales", agents=[first.id, second.id], agents_at_once=1, retries=1)
    number = tenant.number()
    m = menu(tenant, {"1": queue_action(q)})
    open_hours = hours(tenant)["id"]
    tenant.route_number(
        number, {"type": "menu", "menu_id": m["id"]}, business_hours_id=open_hours, closed_action=hangup_action()
    )
    stream = tenant.stream(after=0)
    caller = world.plivo.call(number, digits="1")
    cid = hs_header(second.phone.wait_ring(), "HsConversation")
    stream.wait_for("offer.accepted", conversation_id=cid)
    caller.hangup()
    stream.wait_for("conversation.wrap_up_started", conversation_id=cid)
    ok(wrap_up(second, cid, disposition(tenant)["id"]))
    stream.wait_for("conversation.closed", conversation_id=cid)
    kinds = types(timeline(tenant.api, cid))
    assert in_order(kinds, *HOPS) and in_order(kinds, "offer.accepted", "call.ended")


def test_the_timeline_shows_overflow_and_voicemail(world, tenant):
    q = tenant.queue(name="Q", agents=[unavailable(tenant).id], wait_timeout_sec=1)
    ok(tenant.api.patch(f"/api/v1/queues/{q['id']}", json={"overflow_action": voicemail_action(q)}))
    number = tenant.number()
    tenant.route_number(number, queue_action(q))
    stream = tenant.stream(after=0)
    world.plivo.call(number)
    cid = run_jobs_until(world, lambda: seen(stream, "voicemail.received"), "voicemail.received")[0]["conversation_id"]
    run_jobs_until(world, lambda: seen(stream, "conversation.closed", conversation_id=cid), "closed")
    kinds = types(timeline(tenant.api, cid))
    assert in_order(
        kinds,
        "conversation.started",
        "conversation.queued",
        "conversation.overflowed",
        "voicemail.received",
        "call.ended",
    )


def test_an_after_hours_call_shows_hours_and_the_closed_action(world, tenant):
    number = tenant.number()
    tenant.route_number(
        number,
        {"type": "hangup"},
        business_hours_id=hours(tenant, weekly=False)["id"],
        closed_action=hangup_action("Closed"),
    )
    stream = tenant.stream(after=0)
    world.plivo.call(number).wait_state("completed")
    cid = stream.wait_for("conversation.after_hours")["conversation_id"]
    run_jobs_until(world, lambda: seen(stream, "call.ended", conversation_id=cid), "call.ended")
    entries = timeline(tenant.api, cid)
    assert entries[0]["type"] == "conversation.started" and entries[0]["data"]["hours"] == "closed"
    assert in_order(types(entries), "conversation.started", "conversation.after_hours", "call.ended")


def test_holds_appear_on_the_timeline_as_envelopes_in_id_order(tenant):
    live = connect(tenant)
    for on in (True, False):
        ok(live.agent.api().post(f"/api/v1/calls/{live.id}/hold", json={"on": on}))
    live.stream.wait_for("call.resumed", conversation_id=live.id)
    entries = timeline(tenant.api, live.id)
    assert in_order(types(entries), "offer.accepted", "call.held", "call.resumed")
    assert all(e["conversation_id"] == live.id and e["created_at"] for e in entries)
    assert [int(e["id"]) for e in entries] == sorted(int(e["id"]) for e in entries)


def ours(stream, cid: str) -> list[dict]:
    return [e for e in stream.events if e["conversation_id"] == cid]


def test_the_timeline_is_for_calls_keys_supervisors_and_the_agent_on_the_call(tenant):
    t = team(tenant, "Floor")
    sup = tenant.agent("Sup", role="supervisor", teams=[t]).login(available=False)
    live = connect(tenant, tenant.agent("Agent", teams=[t]).login())
    ok(tenant.key_with("calls").get(f"/api/v1/conversations/{live.id}/timeline"))
    ok(sup.api().get(f"/api/v1/conversations/{live.id}/timeline"))
    ok(live.agent.api().get(f"/api/v1/conversations/{live.id}/timeline"))
    error(tenant.key_with("reports").get(f"/api/v1/conversations/{live.id}/timeline"), 403)


def as_its_stream_gives_it(tenant, agent, cid: str, read: list[dict]) -> None:
    """The agent's timeline is, envelope for envelope, what its stream replays of the conversation."""
    replay = tenant.stream(token=agent.stream_token, after=0)
    replay.wait_for(read[-1]["type"], id=read[-1]["id"])
    assert [e for e in ours(replay, cid) if int(e["id"]) <= int(read[-1]["id"])] == read


def hidden(tenant, cid: str, read: list[dict]) -> list[str]:
    shown = {e["id"] for e in read}
    return [e["type"] for e in timeline(tenant.api, cid) if e["id"] not in shown]


def test_the_agent_on_a_call_reads_the_events_its_stream_delivered_in_order_and_no_other(tenant):
    t = team(tenant, "Floor")
    agent = tenant.agent("Agent", teams=[t]).login()
    sup = tenant.agent("Sup", role="supervisor", teams=[t]).login(available=False)
    desk = tenant.stream(token=agent.stream_token, after=0)
    live = connect(tenant, agent)
    ok(sup.api().post(f"/api/v1/calls/{live.id}/monitor", json={"mode": "listen"}))
    live.stream.wait_for("supervisor.joined", conversation_id=live.id)
    for on in (True, False):
        ok(agent.api().post(f"/api/v1/calls/{live.id}/hold", json={"on": on}))
    desk.wait_for("call.resumed", conversation_id=live.id)
    read = timeline(agent.api(), live.id)
    delivered = [e["id"] for e in ours(desk, live.id)]
    assert "call.resumed" in types(read) and [e["id"] for e in read if e["id"] in delivered] == delivered
    assert "supervisor.joined" in hidden(tenant, live.id, read), "a silent listener stays out of the agent's timeline"
    as_its_stream_gives_it(tenant, agent, live.id, read)


def left_call(world, tenant, holds: int):
    """A call with handoff context that the agent took, handed to another agent by a cold transfer and wrapped up;
    the other agent then holds and resumes it `holds` times, and the first agent adds a note. Returns them and the
    conversation id."""
    agent, target = tenant.agent("Agent").login(), tenant.agent("Target").login()
    first, billing = tenant.queue(agents=[agent.id]), tenant.queue(name="Billing", agents=[target.id])
    stream = tenant.stream(after=0)
    world.plivo.call(
        redirecting_line(tenant, with_query(first["enqueue_url"], context=json.dumps({"topic": "refund"})))
    )
    cid = stream.wait_for("offer.accepted", agent_id=agent.id)["conversation_id"]
    moved = {"mode": "cold", "target": {"type": "queue", "queue_id": billing["id"]}}
    ok(agent.api().post(f"/api/v1/calls/{cid}/transfers", json=moved))
    stream.wait_for("offer.accepted", conversation_id=cid, agent_id=target.id)
    stream.wait_for("conversation.wrap_up_started", conversation_id=cid, agent_id=agent.id)
    ok(wrap_up(agent, cid, disposition(tenant)["id"]))
    stream.wait_for("conversation.wrapped_up", conversation_id=cid, agent_id=agent.id)
    for _ in range(holds):
        for on in (True, False):
            ok(target.api().post(f"/api/v1/calls/{cid}/hold", json={"on": on}))
    stream.wait_for("call.resumed", conversation_id=cid)
    ok(agent.api().post(f"/api/v1/conversations/{cid}/notes", json={"text": "Checked the order."}))
    stream.wait_for("note.added", conversation_id=cid)
    return agent, cid


def test_an_agent_who_left_a_call_reads_no_other_agents_events_and_no_context_as_its_stream(world, tenant):
    agent, cid = left_call(world, tenant, holds=1)
    read = timeline(agent.api(), cid)
    assert read[-1]["type"] == "note.added" and "call.held" in hidden(tenant, cid, read)
    assert not [e for e in read if e["context"]] and [e for e in timeline(tenant.api, cid) if e["context"]]
    as_its_stream_gives_it(tenant, agent, cid, read)


def test_an_agent_pages_its_timeline_past_a_run_of_events_its_stream_hides(world, tenant):
    agent, cid = left_call(world, tenant, holds=10)
    path, read, after = f"/api/v1/conversations/{cid}/timeline", [], 0
    while after is not None:
        got = ok(agent.api().get(path, params={"after": after, "limit": 1}))
        read, after = read + got["data"], got["next_cursor"]
    assert read == timeline(agent.api(), cid) and read[-1]["type"] == "note.added"


def test_the_agent_who_took_a_chat_reads_its_timeline(tenant):
    agent = tenant.agent().login()
    stream, queue = tenant.stream(after=0), tenant.queue(agents=[agent.id])
    enqueue_text(tenant.api, queue, "chat", "visitor-21", "inbox", text="Hi")
    cid = take(stream, agent)
    assert in_order(types(timeline(agent.api(), cid)), "conversation.started", "offer.created", "offer.accepted")


def test_an_agent_the_call_only_rang_one_never_on_it_and_another_tenant_read_no_timeline(world, tenant):
    rang = tenant.agent("Rang").login()
    rang.phone.on_ring("ignore")
    live = connect(tenant, also=(rang,), agents_at_once=2)
    live.stream.wait_for("offer.created", conversation_id=live.id, agent_id=rang.id)
    path = f"/api/v1/conversations/{live.id}/timeline"
    for client in (rang.api(), tenant.agent("Away").login().api(), world.new_tenant("Other").key_with("calls")):
        error(client.get(path), 404, "conversation_not_found")


def test_an_agent_pages_its_timeline_with_after_and_limit(tenant):
    live = connect(tenant)
    for on in (True, False):
        ok(live.agent.api().post(f"/api/v1/calls/{live.id}/hold", json={"on": on}))
    live.stream.wait_for("call.resumed", conversation_id=live.id)
    path, read, after = f"/api/v1/conversations/{live.id}/timeline", [], 0
    while after is not None:
        got = ok(live.agent.api().get(path, params={"after": after, "limit": 2}))
        read, after = read + got["data"], got["next_cursor"]
    assert len(read) > 2 and read == timeline(live.agent.api(), live.id) == timeline(tenant.api, live.id)
