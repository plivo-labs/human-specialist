"""Single gaps the other spec files do not cover: the handoff context on the conversation read, wait prompts with
position and estimate, transfer to a menu or the tenant's app, the after-agent action, conversation attributes and
the conversation-number filter."""

import json
import re
from urllib.parse import urlsplit

from tests.acceptance.channels import accept, enqueue_text, offered, texts
from tests.acceptance.conftest import (
    connect,
    dial,
    disposition,
    error,
    hangup_action,
    menu,
    ok,
    redirecting_line,
    say,
    settings,
    team,
    unavailable,
    with_query,
    wrap_up,
)
from tests.acceptance.test_in_call_controls import emitted

CTX = {"lead_id": "L-7", "summary": "asked about pricing"}


def prompt_heard(world, call) -> str:
    return world.wait(lambda: next((s for s in call.said if "in line" in s), None), what="a wait prompt")


def transfer(live, target: dict):
    return live.agent.api().post(f"/api/v1/calls/{live.id}/transfers", json={"mode": "cold", "target": target})


def test_the_conversation_read_returns_its_handoff_context(world, tenant):
    queue = tenant.queue(agents=[unavailable(tenant).id])
    stream = tenant.stream(after=0)
    world.plivo.call(redirecting_line(tenant, with_query(queue["enqueue_url"], context=json.dumps(CTX))))
    cid = stream.wait_for("conversation.queued", queue_id=queue["id"])["conversation_id"]
    assert ok(tenant.api.get(f"/api/v1/conversations/{cid}")).get("context") == CTX


def handed_over(world, tenant, *names: str):
    """A team with a supervisor and agents `names`, and a call handed over with CTX that the first agent answers.
    Returns (conversation id, caller, tenant stream, supervisor, agents)."""
    floor = team(tenant, "Floor")
    sup = tenant.agent("Sup", role="supervisor", teams=[floor]).login(available=False)
    agents = [tenant.agent(name, teams=[floor]).login() for name in names]
    stream, queue = tenant.stream(after=0), tenant.queue(agents=[agents[0].id])
    caller = world.plivo.call(redirecting_line(tenant, with_query(queue["enqueue_url"], context=json.dumps(CTX))))
    return stream.wait_for("offer.accepted", agent_id=agents[0].id)["conversation_id"], caller, stream, sup, agents


def context(client, cid: str):
    return ok(client.get(f"/api/v1/conversations/{cid}"))["context"]


def test_only_an_agent_on_the_conversation_gets_its_handoff_context(world, tenant):
    cid, _, _, sup, [mate] = handed_over(world, tenant, "Mate")
    desk, watch = (tenant.stream(token=a.stream_token, after=0) for a in (mate, sup))
    assert context(mate.api(), cid) == CTX == context(tenant.api, cid) and context(sup.api(), cid) is None
    assert desk.wait_for("offer.accepted", conversation_id=cid)["context"] == CTX
    assert watch.wait_for("offer.accepted", conversation_id=cid)["context"] is None
    timeline = f"/api/v1/conversations/{cid}/timeline"
    assert all(e["context"] is None for e in ok(sup.api().get(timeline))["data"])
    assert [e["context"] for e in ok(tenant.api.get(timeline))["data"] if e["type"] == "offer.accepted"] == [CTX]


def test_a_ringing_agent_only_reads_the_conversation_and_its_messages_without_the_context_until_its_offer_ends(
    world, tenant
):
    one, two = tenant.agent("One").login(), tenant.agent("Two").login()
    stream, queue = tenant.stream(after=0), tenant.queue(agents=[one.id, two.id], agents_at_once=2)
    cid = ok(enqueue_text(tenant.api, queue, "chat", "visitor-7", "inbox", text="Hi", context=CTX))["conversation_id"]
    mine, theirs = offered(stream, one, conversation_id=cid), offered(stream, two, conversation_id=cid)
    read = ok(one.api().get(f"/api/v1/conversations/{cid}"))
    assert (read["number"], read["created_at"] is not None, read["context"]) == ("visitor-7", True, None)
    assert texts(one.api(), cid) == ["Hi"]
    path, notes = f"/api/v1/conversations/{cid}", f"/api/v1/conversations/{cid}/notes"
    note = ok(tenant.api.post(notes, json={"text": "Repeat caller"}))["id"]
    for write in (
        one.api().patch(path, json={"attributes": {"tier": "gold"}}),
        one.api().patch(path, json={"customer": "visitor-8"}),
        one.api().post(notes, json={"text": "Mine"}),
        one.api().patch(f"{notes}/{note}", json={"text": "Edited"}),
        one.api().delete(f"{notes}/{note}"),
    ):
        error(write, 404, "conversation_not_found")
    ok(accept(two, theirs))
    stream.wait_for("offer.withdrawn", offer_id=mine["data"]["offer_id"])
    error(one.api().get(f"/api/v1/conversations/{cid}"), 404, "conversation_not_found")


def test_a_ringing_agent_reads_nothing_once_its_offer_timer_ran_past_the_ring_timeout(world, tenant):
    one = tenant.agent("One").login()
    stream, queue = tenant.stream(after=0), tenant.queue(agents=[one.id])
    cid = ok(enqueue_text(tenant.api, queue, "chat", "visitor-9", "inbox", text="Hi"))["conversation_id"]
    offered(stream, one, conversation_id=cid)
    ok(one.api().get(f"/api/v1/conversations/{cid}"))
    with world.passes_held():
        with world.redis_down():
            world.run_jobs("offer_timeout")
        error(one.api().get(f"/api/v1/conversations/{cid}"), 404, "conversation_not_found")


def test_a_monitoring_supervisor_gets_the_handoff_context_and_a_former_primary_keeps_reading_it(world, tenant):
    cid, caller, stream, sup, [mate] = handed_over(world, tenant, "Mate")
    ok(sup.api().post(f"/api/v1/calls/{cid}/monitor", json={"mode": "listen"}))
    stream.wait_for("supervisor.joined", conversation_id=cid)
    assert context(sup.api(), cid) == CTX
    assert tenant.stream(token=sup.stream_token, after=0).wait_for("supervisor.joined")["context"] == CTX
    caller.hangup()
    stream.wait_for("conversation.wrap_up_started", conversation_id=cid, agent_id=mate.id)
    ok(wrap_up(mate, cid, disposition(tenant)["id"]))
    stream.wait_for("conversation.closed", conversation_id=cid)
    assert context(mate.api(), cid) == CTX and context(sup.api(), cid) is None


def test_an_agent_that_handed_the_call_on_gets_its_later_notes_without_the_handoff_context(world, tenant):
    cid, _, stream, _, [mate, target] = handed_over(world, tenant, "Mate", "Target")
    cold = {"mode": "cold", "target": {"type": "agent", "agent_id": target.id}}
    ok(mate.api().post(f"/api/v1/calls/{cid}/transfers", json=cold))
    stream.wait_for("call.transferred", conversation_id=cid, status="completed")
    stream.wait_for("conversation.wrap_up_started", conversation_id=cid, agent_id=mate.id)
    ok(wrap_up(mate, cid, disposition(tenant)["id"]))
    stream.wait_for("conversation.wrapped_up", conversation_id=cid, agent_id=mate.id)
    ok(target.api().post(f"/api/v1/conversations/{cid}/notes", json={"text": "VIP"}))
    assert (
        tenant.stream(token=mate.stream_token, after=0).wait_for("note.added", conversation_id=cid)["context"] is None
    )
    assert tenant.stream(token=target.stream_token, after=0).wait_for("note.added")["context"] == CTX


def test_the_wait_prompt_speaks_queue_position_and_estimated_wait(world, tenant):
    prompt = say("You are number {position} in line. Your wait is about {wait_minutes} minutes.")
    number, _ = tenant.inbound_line(unavailable(tenant), wait_prompts=[prompt])
    first = world.plivo.call(number, from_="+14155550701")
    second = world.plivo.call(number, from_="+14155550702")
    first_heard, second_heard = prompt_heard(world, first), prompt_heard(world, second)
    assert "number 1 in line" in first_heard and "number 2 in line" in second_heard
    minutes = [re.search(r"about (\d+) minutes\.$", heard) for heard in (first_heard, second_heard)]
    assert all(m and int(m[1]) >= 1 for m in minutes), (first_heard, second_heard)


def test_a_transfer_to_a_menu_plays_it_to_the_caller(world, tenant):
    m = menu(tenant, {"1": hangup_action("Thanks for the feedback")}, greeting="Press 1 to rate us")
    live = connect(tenant)
    ok(transfer(live, {"type": "menu", "menu_id": m["id"]}))
    live.leg.wait_state("completed")
    world.wait(lambda: "Press 1 to rate us" in live.caller.said, what="the menu greeting")
    live.caller.press("1")
    world.wait(lambda: "Thanks for the feedback" in live.caller.said, what="the menu's action")
    assert [e["data"]["status"] for e in emitted(tenant, "call.transferred", live.id)] == ["initiated"]


def test_a_transfer_to_a_redirect_hands_the_caller_to_the_tenant_app(world, tenant):
    settings(tenant, agent_direct_dial=True)
    app = world.plivo.app("<Response><Speak>Welcome to self service</Speak></Response>")
    live = connect(tenant)
    ok(transfer(live, {"type": "redirect", "url": app}))
    live.leg.wait_state("completed")
    world.wait(lambda: "Welcome to self service" in live.caller.said, what="the tenant app")
    assert world.plivo.requests_to(urlsplit(app).path)[-1].params["conversation_id"] == live.id
    assert [e["data"]["status"] for e in emitted(tenant, "call.transferred", live.id)] == ["initiated"]


def test_the_after_agent_action_runs_when_the_agent_hangs_up_first(world, tenant):
    app = world.plivo.app("<Response><Speak>Please rate your call</Speak></Response>")
    live = connect(tenant, after_agent_action={"type": "redirect", "url": app})
    live.leg.hangup()
    world.wait(lambda: "Please rate your call" in live.caller.said, what="the after-agent action")
    assert world.plivo.requests_to(urlsplit(app).path)[-1].params["conversation_id"] == live.id


def test_conversation_attributes_merge_and_cap_at_2_kb(world, tenant):
    _, cid = dial(tenant)
    url = f"/api/v1/conversations/{cid}"
    ok(tenant.api.patch(url, json={"attributes": {"csat": 5, "crm_id": "acct-9"}}))
    ok(tenant.api.patch(url, json={"attributes": {"csat": 4, "priority": "high"}}))
    merged = {"csat": 4, "crm_id": "acct-9", "priority": "high"}
    assert ok(tenant.api.get(url))["attributes"] == merged
    error(tenant.api.patch(url, json={"attributes": {"note": "x" * 2100}}), 422, "attributes_too_large")
    assert ok(tenant.api.get(url))["attributes"] == merged


def test_conversations_filter_by_conversation_number(world, tenant):
    _, first = dial(tenant)
    dial(tenant)
    number = ok(tenant.api.get(f"/api/v1/conversations/{first}"))["conversation_number"]
    found = ok(tenant.api.get("/api/v1/conversations", params={"conversation_number": number}))["data"]
    assert [c["id"] for c in found] == [first]
