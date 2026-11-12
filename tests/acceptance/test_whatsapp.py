"""WhatsApp over the simulator: inbound text and media, free-text replies inside the window, agent-initiated
templates passed to Plivo as given, and Plivo's refusal reported back."""

import json

from tests.acceptance.channels import messages, plivo_sent, reply, take, text_line, texts
from tests.acceptance.conftest import error, fence, ok, registered_number

CUSTOMER = "+14155550301"
TEMPLATE = {
    "name": "order_update",
    "language": "en_US",
    "components": [{"type": "body", "parameters": [{"type": "text", "text": "1234"}]}],
}


def start_whatsapp(tenant, agent, number: str, **body):
    """POST /conversations with the tenant key: the agent's WhatsApp to CUSTOMER from the tenant's `number`."""
    payload = {"channel": "whatsapp", "agent_id": agent.id, "from": number, "to": CUSTOMER}
    return tenant.api.post("/api/v1/conversations", json={**payload, **body})


def test_a_whatsapp_reaches_the_agent_and_the_reply_goes_out_as_whatsapp(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, agent)
    world.plivo.text(number, CUSTOMER, "Hola", type_="whatsapp")
    cid = take(stream, agent, channel="whatsapp")
    assert texts(agent.api(), cid) == ["Hola"]
    reply(agent, cid, "Hi there")
    [out] = world.wait(lambda: plivo_sent(world, number, CUSTOMER), what="the reply at Plivo")
    assert (out.type, out.text) == ("whatsapp", "Hi there")
    stream.wait_for("message.status_changed", conversation_id=cid, status="delivered")


def test_an_agent_initiated_whatsapp_sends_the_template_as_given(world, tenant):
    world.plivo.templates.add((TEMPLATE["name"], TEMPLATE["language"]))
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    number = registered_number(tenant)
    cid = ok(start_whatsapp(tenant, agent, number, template=TEMPLATE))["conversation_id"]
    [out] = world.wait(lambda: plivo_sent(world, number, CUSTOMER), what="the template at Plivo")
    assert (out.type, out.template) == ("whatsapp", TEMPLATE)
    stream.wait_for("message.status_changed", conversation_id=cid, status="delivered")


def test_a_whatsapp_send_plivo_refuses_is_422_and_reported_failed(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    world.plivo.reject_next("/Message", 400, account=tenant.auth_id)
    error(start_whatsapp(tenant, agent, registered_number(tenant), template=TEMPLATE), 422, "message_refused")
    stream.wait_for("message.status_changed", status="failed")
    stream.wait_for("conversation.closed", result="failed")


def test_inbound_media_is_served_by_a_signed_url_and_the_plivo_url_never_shows(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, agent)
    inbound = world.plivo.text(number, CUSTOMER, "", type_="whatsapp", media=(b"picture-bytes",))
    cid = take(stream, agent)
    [message] = messages(agent.api(), cid)
    [attachment] = message["attachments"]
    url = attachment["url"]
    assert world.anon().get(url).content == b"picture-bytes"
    altered = url[:-1] + ("1" if url.endswith("0") else "0")
    assert world.anon().get(altered).status_code in (401, 403)
    assert world.anon().get(url.partition("?")[0]).status_code in (401, 403, 422)
    fence(tenant)
    shown = [tenant.events(), messages(tenant.api, cid), ok(tenant.api.get(f"/api/v1/conversations/{cid}"))]
    assert inbound.media[0] not in json.dumps(shown)
