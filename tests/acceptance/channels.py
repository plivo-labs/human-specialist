"""Builders for the text and task specs: SMS and WhatsApp through the simulator, chat and email through a
tenant's gateway, and offers an agent accepts or declines over the API."""

from tests.acceptance.conftest import ok, wrap_up


def plivo_app(tenant, number: str) -> dict:
    """The Plivo application the number points at, read from the simulator's REST API as the tenant would."""
    world, auth = tenant.world, (tenant.auth_id, tenant.auth_token)
    base = f"{world.plivo.url}/v1/Account/{tenant.auth_id}"
    found = world.anon().get(f"{base}/Number/{number.removeprefix('+')}/", auth=auth).json()
    return world.anon().get(f"{world.plivo.url}{found['application']}", auth=auth).json()


def text_line(tenant, *agents, **queue_fields) -> tuple[str, dict]:
    """A number whose queue holds `agents`, with HS's message URL on its Plivo application, so the simulator
    posts customer texts to HS. Returns (number, queue)."""
    number, queue = tenant.inbound_line(*agents, **queue_fields)
    assert plivo_app(tenant, number).get("message_url"), "HS put no message URL on the number's Plivo application"
    return number, queue


def offered(stream, agent, **match) -> dict:
    return stream.wait_for("offer.created", agent_id=agent.id, **match)


def accept(agent, offer: dict):
    return agent.api().post(f"/api/v1/offers/{offer['data']['offer_id']}/accept")


def decline(agent, offer: dict):
    return agent.api().post(f"/api/v1/offers/{offer['data']['offer_id']}/decline")


def take(stream, agent, **match) -> str:
    """The agent accepts its next offer. Returns the conversation id."""
    offer = offered(stream, agent, **match)
    ok(accept(agent, offer))
    stream.wait_for("offer.accepted", agent_id=agent.id, conversation_id=offer["conversation_id"])
    return offer["conversation_id"]


def messages(client, cid: str) -> list[dict]:
    return ok(client.get(f"/api/v1/conversations/{cid}/messages", params={"limit": 200}))["data"]


def texts(client, cid: str) -> list[str]:
    return [m["text"] for m in messages(client, cid)]


def reply(agent, cid: str, text: str, **body) -> dict:
    return ok(agent.api().post(f"/api/v1/conversations/{cid}/messages", json={"text": text, **body}))


def close(agent, cid: str, disposition_id: str, stream) -> dict:
    """The agent ends the text session and submits a disposition. Returns `conversation.closed`."""
    ok(agent.api().post(f"/api/v1/conversations/{cid}/close"))
    stream.wait_for("conversation.wrap_up_started", conversation_id=cid, agent_id=agent.id)
    ok(wrap_up(agent, cid, disposition_id))
    return stream.wait_for("conversation.closed", conversation_id=cid)


def enqueue_text(client, queue: dict, channel: str, from_: str, to: str, **fields):
    """POST /queues/{id}/enqueue for a text contact: starts its conversation, or joins the open one."""
    body = {"channel": channel, "from": from_, "to": to, **fields}
    return client.post(f"/api/v1/queues/{queue['id']}/enqueue", json=body)


def plivo_sent(world, from_: str, to: str) -> list:
    """Messages HS sent through Plivo from the tenant's number `from_` to `to`."""
    return [m for m in world.plivo.messages if (m.direction, m.from_, m.to) == ("outbound", from_, to)]
