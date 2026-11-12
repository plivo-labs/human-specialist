"""Text a tenant's own gateway carries: chat and email start through the enqueue route and continue through
POST /messages; agent replies leave as signed `message.sent` events with `delivery: external`; delivery
statuses come back through the API."""

from tests.acceptance.channels import close, enqueue_text, plivo_sent, reply, take, text_line, texts
from tests.acceptance.conftest import disposition, error, ok, settings, signed_with, webhook

VISITOR, INBOX = "visitor-81", "support-inbox"


def chat(tenant, agent, stream, **fields) -> str:
    """A chat from VISITOR enqueued with the tenant key on a queue holding `agent`, who accepts it."""
    queue = tenant.queue(agents=[agent.id])
    body = {"text": "Hi", **fields}
    cid = ok(enqueue_text(tenant.api, queue, "chat", VISITOR, INBOX, **body))["conversation_id"]
    assert take(stream, agent, conversation_id=cid) == cid
    return cid


def gateway_message(client, text: str, provider_id: str):
    body = {"channel": "chat", "from": VISITOR, "to": INBOX, "text": text, "provider_id": provider_id}
    return client.post("/api/v1/messages", json=body)


def test_a_chat_enqueued_by_a_tenant_key_reaches_the_agent_and_a_second_enqueue_joins_it(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    queue = tenant.queue(agents=[agent.id])
    cid = ok(enqueue_text(tenant.api, queue, "chat", VISITOR, INBOX, text="Hi"))["conversation_id"]
    take(stream, agent, conversation_id=cid)
    again = ok(enqueue_text(tenant.api, queue, "chat", VISITOR, INBOX, text="Are you there?"))
    assert again["conversation_id"] == cid
    assert texts(agent.api(), cid) == ["Hi", "Are you there?"]


def test_a_chat_reply_is_a_signed_external_message_sent_webhook_with_the_context(world, tenant):
    sink = world.webhook_sink()
    hook = webhook(tenant, sink.url, event_types=["message.sent"])
    agent = tenant.agent().login()
    ctx = {"page": "/pricing"}
    cid = chat(tenant, agent, tenant.stream(after=0), context=ctx)
    sent = reply(agent, cid, "How can I help?")
    d = sink.wait_for("message.sent", conversation_id=cid)
    assert signed_with(d, hook["secret"]) and d.json["context"] == ctx
    data = d.json["data"]
    assert (data["message_id"], data["delivery"]) == (sent["id"], "external")
    assert (data["text"], data["to"]) == ("How can I help?", VISITOR)


def test_a_gateway_delivery_status_reaches_the_agents_stream(world, tenant):
    gateway = tenant.key_with("messages")
    agent = tenant.agent().login()
    desk = tenant.stream(token=agent.stream_token, after=0)
    cid = chat(tenant, agent, desk)
    sent = reply(agent, cid, "How can I help?")
    ok(gateway.post(f"/api/v1/messages/{sent['id']}/status", json={"status": "delivered"}))
    desk.wait_for("message.status_changed", conversation_id=cid, message_id=sent["id"], status="delivered")


def test_a_gateway_message_joins_the_open_chat_and_is_404_once_it_closed(world, tenant):
    gateway = tenant.key_with("messages")
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    cid = chat(tenant, agent, stream)
    ok(gateway_message(gateway, "Second", "m-2"))
    stream.wait_for("message.received", conversation_id=cid, text="Second")
    close(agent, cid, disposition(tenant)["id"], stream)
    error(gateway_message(gateway, "Late", "m-3"), 404, "no_open_conversation")


def test_an_email_reply_threads_into_its_conversation_by_in_reply_to(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    queue = tenant.queue(agents=[agent.id])
    inbox = "billing@acme.example"
    first = {"text": "My invoice is wrong", "email": {"subject": "Invoice", "message_id": "<c1@customer.example>"}}
    cid = ok(enqueue_text(tenant.api, queue, "email", "ana@customer.example", inbox, **first))["conversation_id"]
    take(stream, agent, conversation_id=cid)
    reply(agent, cid, "Looking into it", email={"subject": "Re: Invoice"})
    ours = stream.wait_for("message.sent", conversation_id=cid)["data"]["email"]
    assert ours["in_reply_to"] == "<c1@customer.example>" and ours["message_id"]
    answer = {"text": "Thanks", "email": {"subject": "Re: Invoice", "message_id": "<c2@other.example>"}}
    answer["email"]["in_reply_to"] = ours["message_id"]
    again = ok(enqueue_text(tenant.api, queue, "email", "ana@other.example", inbox, **answer))
    assert again["conversation_id"] == cid


def test_an_email_reply_threads_into_its_conversation_by_references_alone(tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    queue = tenant.queue(agents=[agent.id])
    inbox = "billing@acme.example"
    first = {"text": "My invoice is wrong", "email": {"subject": "Invoice", "message_id": "<r1@customer.example>"}}
    cid = ok(enqueue_text(tenant.api, queue, "email", "ana@customer.example", inbox, **first))["conversation_id"]
    take(stream, agent, conversation_id=cid)
    reply(agent, cid, "Looking into it", email={"subject": "Re: Invoice"})
    ours = stream.wait_for("message.sent", conversation_id=cid)["data"]["email"]["message_id"]
    chain = {
        "subject": "Re: Invoice",
        "message_id": "<r3@other.example>",
        "references": ["<r1@customer.example>", ours],
    }
    again = ok(enqueue_text(tenant.api, queue, "email", "ana@other.example", inbox, text="Thanks", email=chain))
    assert again["conversation_id"] == cid


def test_with_external_sms_delivery_a_reply_is_an_event_and_no_plivo_request(world, tenant):
    settings(tenant, text={"delivery": {"sms": "external"}})
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, agent)
    customer = "+14155550401"
    world.plivo.text(number, customer, "Hi")
    cid = take(stream, agent)
    sent = reply(agent, cid, "Hello")
    event = stream.wait_for("message.sent", conversation_id=cid)["data"]
    assert (event["message_id"], event["delivery"]) == (sent["id"], "external")
    world.run_jobs()
    assert plivo_sent(world, number, customer) == []


def test_another_tenants_gateway_key_cannot_reach_this_tenants_conversation(world, tenant):
    agent = tenant.agent().login()
    cid = chat(tenant, agent, tenant.stream(after=0))
    sent = reply(agent, cid, "How can I help?")
    other = world.new_tenant("Other").key_with("messages")
    error(other.post(f"/api/v1/messages/{sent['id']}/status", json={"status": "delivered"}), 404)
    error(gateway_message(other, "Not yours", "m-9"), 404, "no_open_conversation")


def test_a_gateway_key_cannot_read_conversations(world, tenant):
    gateway = tenant.key_with("messages")
    queue = tenant.queue()
    cid = ok(enqueue_text(tenant.api, queue, "chat", VISITOR, INBOX, text="Hi"))["conversation_id"]
    error(gateway.get(f"/api/v1/conversations/{cid}"), 403)
    error(gateway.get(f"/api/v1/conversations/{cid}/messages"), 403)
