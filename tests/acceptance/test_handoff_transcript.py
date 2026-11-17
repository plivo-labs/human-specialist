"""The transcript a bot's handoff brings with a call or a text contact: kept once with the conversation, read on the
conversation by whoever may read its handoff context, and in no event, webhook, list row, search document or export."""

import json

import httpx

from tests.acceptance.channels import accept, close, enqueue_text, offered, reply, take
from tests.acceptance.conftest import disposition, error, ok, webhook
from tests.acceptance.test_closed_search import indexed, search
from tests.acceptance.test_export import export

TURNS = [
    {"role": "bot", "text": "Hi, this is Acme Support. What can I help with?", "at": "2026-11-17T10:00:00Z"},
    {"role": "customer", "text": "My blender jar arrived cracked.", "at": None},
    {"role": "bot", "text": "Sorry about that. Let me find someone who can help.", "at": "2026-11-17T10:00:09Z"},
]
SENT = [{k: v for k, v in t.items() if v is not None} for t in TURNS]
WORD = "blender"
HOLD = '<Response><GetDigits timeout="60"><Speak>One moment</Speak></GetDigits></Response>'


def transcript(client, cid: str):
    return ok(client.get(f"/api/v1/conversations/{cid}"))["transcript"]


def chat(tenant, queue: dict, visitor: str, **fields) -> str:
    return ok(enqueue_text(tenant.api, queue, "chat", visitor, "inbox", text="Hi", **fields))["conversation_id"]


def held_call(world, tenant):
    """A call the bot holds on the tenant's app, ready to hand over."""
    number = tenant.number()
    tenant.route_number(number, {"type": "redirect", "url": world.plivo.app(HOLD)})
    caller = world.plivo.call(number)
    world.wait(lambda: "One moment" in caller.said, what="the bot holds the call")
    return number, caller


def hand_over(tenant, queue: dict, number: str, caller, **fields):
    body = {"call_uuid": caller.call_uuid, "from": caller.from_, "to": number, **fields}
    return tenant.key_with("calls").post(queue["enqueue_url"], json=body)


def test_a_call_handed_over_with_a_transcript_shows_it_to_the_agent_who_took_it_not_to_one_it_rings(world, tenant):
    agent = tenant.agent().login()
    agent.phone.on_ring("ignore")
    queue, desk = tenant.queue(agents=[agent.id]), tenant.stream(token=agent.stream_token, after=0)
    number, caller = held_call(world, tenant)
    cid = ok(hand_over(tenant, queue, number, caller, transcript=SENT))["conversation_id"]
    desk.wait_for("offer.created", conversation_id=cid)
    assert transcript(agent.api(), cid) is None and transcript(tenant.api, cid) == TURNS
    agent.phone.wait_ring().answer()
    desk.wait_for("offer.accepted", conversation_id=cid)
    assert transcript(agent.api(), cid) == TURNS


def test_a_text_contact_enqueued_with_a_transcript_shows_it_to_the_agent_who_took_it_not_to_one_it_rang(world, tenant):
    one, two = tenant.agent("One").login(), tenant.agent("Two").login()
    stream, queue = tenant.stream(after=0), tenant.queue(agents=[one.id, two.id], agents_at_once=2)
    cid = chat(tenant, queue, "visitor-7", transcript=SENT)
    offered(stream, one, conversation_id=cid)
    theirs = offered(stream, two, conversation_id=cid)
    assert transcript(one.api(), cid) is None
    ok(accept(two, theirs))
    stream.wait_for("offer.accepted", conversation_id=cid, agent_id=two.id)
    assert transcript(two.api(), cid) == TURNS == transcript(tenant.api, cid)


def test_the_first_enqueue_that_brings_a_transcript_keeps_it_and_a_later_one_leaves_it(world, tenant):
    queue = tenant.queue(agents=[tenant.agent().login().id])
    first = chat(tenant, queue, "visitor-12")
    assert transcript(tenant.api, first) is None
    again = enqueue_text(tenant.api, queue, "chat", "visitor-12", "inbox", text="Still there?", transcript=SENT)
    later = [{"role": "bot", "text": "A later bot's summary."}]
    last = enqueue_text(tenant.api, queue, "chat", "visitor-12", "inbox", text="Hello?", transcript=later)
    assert ok(again)["conversation_id"] == ok(last)["conversation_id"] == first
    assert transcript(tenant.api, first) == TURNS
    assert transcript(tenant.api, chat(tenant, queue, "visitor-8", transcript=[])) is None


def test_a_replayed_message_that_first_brings_a_transcript_keeps_it_and_leaves_the_last_activity(world, tenant):
    queue = tenant.queue(agents=[tenant.agent().id])
    cid = chat(tenant, queue, "visitor-13", provider_id="gateway-msg-13")
    before = ok(tenant.api.get(f"/api/v1/conversations/{cid}"))["last_activity_at"]
    assert chat(tenant, queue, "visitor-13", provider_id="gateway-msg-13", transcript=SENT) == cid
    shown = ok(tenant.api.get(f"/api/v1/conversations/{cid}"))
    assert (shown["transcript"], shown["last_activity_at"]) == (TURNS, before)


def test_a_transcript_with_an_unknown_speaker_an_empty_or_long_turn_or_over_200_turns_or_32_kb_is_422(tenant):
    queue = tenant.queue(agents=[tenant.agent().login().id])

    def sent(turns: list[dict]):
        return enqueue_text(tenant.api, queue, "chat", "visitor-9", "inbox", text="Hi", transcript=turns)

    error(sent([{"role": "agent", "text": "Hello"}]), 422, "validation_failed")
    error(sent([{"role": "bot", "text": "   "}]), 422, "validation_failed")
    error(sent([{"role": "bot", "text": "x" * 4001}]), 422, "validation_failed")
    error(sent([{"role": "bot", "text": "Hi"}] * 201), 422, "transcript_too_large")
    error(sent([{"role": "customer", "text": "x" * 3500}] * 10), 422, "transcript_too_large")
    call = {"call_uuid": "f6d1c0de-0000-4000-8000-000000000001", "from": "+15550100142", "to": "+15550100100"}
    too_long = call | {"transcript": [{"role": "bot", "text": "Hi"}] * 201}
    error(tenant.key_with("calls").post(queue["enqueue_url"], json=too_long), 422, "transcript_too_large")
    assert ok(tenant.api.get("/api/v1/conversations"))["data"] == []


def test_a_transcript_is_in_no_event_webhook_list_row_search_document_or_export(world, tenant):
    agent, sink = tenant.agent().login(), world.webhook_sink()
    webhook(tenant, sink.url)
    stream, queue = tenant.stream(after=0), tenant.queue(agents=[agent.id])
    cid = chat(tenant, queue, "visitor-10", transcript=SENT)
    take(stream, agent, conversation_id=cid)
    reply(agent, cid, "Hello, I can help with that.")
    closed = close(agent, cid, disposition(tenant)["id"], stream)
    assert transcript(tenant.api, cid) == TURNS
    sink.wait_for("conversation.closed", conversation_id=cid)
    timeline = ok(tenant.api.get(f"/api/v1/conversations/{cid}/timeline"))
    listed = ok(tenant.api.get("/api/v1/conversations"))
    indexed(tenant, [cid])
    found = ok(tenant.api.post("/api/v1/conversations/search", json={}))
    told = [stream.events, closed, timeline, listed, found, [d.json for d in sink.deliveries]]
    assert WORD not in json.dumps(told) and "transcript" not in json.dumps(told)
    assert search(tenant, text=WORD) == []
    assert WORD not in json.dumps([export(tenant.api), export(tenant.api, "/content")])
    if world.search_url:
        stored = httpx.get(f"{world.search_url}/hs-conversations-{tenant.id}/_doc/{cid}").json()["_source"]
        assert "transcript" not in stored and WORD not in json.dumps(stored)


def test_another_tenants_key_cannot_read_a_transcript(world, tenant):
    queue = tenant.queue(agents=[tenant.agent().login().id])
    cid = chat(tenant, queue, "visitor-11", transcript=SENT)
    error(world.new_tenant("Other").api.get(f"/api/v1/conversations/{cid}"), 404, "conversation_not_found")
