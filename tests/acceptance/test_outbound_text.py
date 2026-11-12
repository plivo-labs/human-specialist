"""Agent-initiated text: the server starts an SMS, WhatsApp or email conversation for an agent, under the same
dial-from-the-server rule as POST /calls; an agent on a call can message the caller on WhatsApp
in a conversation it holds."""

from tests.acceptance.channels import messages, plivo_sent, text_line
from tests.acceptance.conftest import connect, error, ok, registered_number, replayed, settings

CUSTOMER = "+14155550501"


def start(client, agent, channel: str, from_: str, to: str, **body):
    payload = {"channel": channel, "agent_id": agent.id, "from": from_, "to": to}
    return client.post("/api/v1/conversations", json={**payload, **body})


def test_a_tenant_key_starts_an_sms_that_holds_the_agents_slot_and_the_reply_joins_it(world, tenant):
    settings(tenant, capacity={"sms": 1})
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, agent)
    cid = ok(start(tenant.api, agent, "sms", number, CUSTOMER, text="Your order shipped"))["conversation_id"]
    [out] = world.wait(lambda: plivo_sent(world, number, CUSTOMER), what="the first text at Plivo")
    assert (out.type, out.text) == ("sms", "Your order shipped")
    world.plivo.text(number, "+14155550502", "New customer here")
    waiting = stream.wait_for("conversation.queued", **{"from": "+14155550502"})["conversation_id"]
    assert not replayed(tenant, "offer.created", conversation_id=waiting)
    world.plivo.text(number, CUSTOMER, "Thanks!")
    stream.wait_for("message.received", conversation_id=cid, text="Thanks!")


def test_an_agent_cannot_start_a_text_conversation_without_direct_dial(world, tenant):
    agent = tenant.agent().login()
    number = registered_number(tenant)
    r = start(agent.api(), agent, "sms", number, CUSTOMER, text="Hi")
    error(r, 403, "dial_requires_server")
    world.wait(world.plivo.idle, what="the simulator to go idle")
    assert plivo_sent(world, number, CUSTOMER) == []


def test_an_agent_initiated_email_goes_out_as_an_external_message(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    to = "ana@customer.example"
    body = {"text": "Your refund is done", "email": {"subject": "Refund"}}
    tenant.route_number("billing@acme.example", {"type": "hangup"})
    r = start(tenant.api, agent, "email", "billing@acme.example", to, **body)
    sent = stream.wait_for("message.sent", conversation_id=ok(r)["conversation_id"])["data"]
    assert (sent["delivery"], sent["to"], sent["email"]["subject"]) == ("external", to, "Refund")
    assert sent["email"]["message_id"]


def test_an_agent_on_a_call_sends_the_caller_a_whatsapp_in_a_conversation_it_holds(world, tenant):
    template = {"name": "follow_up", "language": "en_US", "components": []}
    world.plivo.templates.add(("follow_up", "en_US"))
    live = connect(tenant)
    caller = live.caller.from_
    body = {"channel": "whatsapp", "template": template}
    mine = ok(live.agent.api().post(f"/api/v1/conversations/{live.id}/messages", json=body))
    out = world.wait(lambda: plivo_sent(world, live.number, caller), what="the WhatsApp")
    assert [(m.type, m.template) for m in out] == [("whatsapp", template)]
    thread = ok(tenant.api.get(f"/api/v1/conversations/{mine['conversation_id']}"))
    assert (thread["channel"], thread["state"], thread["agent_ids"]) == ("whatsapp", "active", [live.agent.id])
    sent = live.stream.wait_for("message.sent", message_id=mine["id"])
    assert (sent["conversation_id"], sent["data"]["delivery"]) == (thread["id"], "plivo")
    listed = messages(live.agent.api(), thread["id"])
    assert [(m["channel"], m["direction"]) for m in listed] == [("whatsapp", "outbound")]
