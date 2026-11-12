"""Changing hands on a text conversation: cold transfer to an agent, team or queue, a second agent, and a
supervisor reading a live conversation."""

from tests.acceptance.channels import accept, decline, offered, plivo_sent, reply, take, text_line, texts
from tests.acceptance.conftest import error, ok, replayed, settings, team, unavailable

CUSTOMER = "+14155550601"


def live_sms(world, tenant, agent):
    """An SMS from CUSTOMER that `agent` accepted. Returns (conversation id, stream, the tenant's number)."""
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, agent)
    world.plivo.text(number, CUSTOMER, "Hi")
    return take(stream, agent), stream, number


def add_participant(agent, cid: str, other) -> dict:
    body = {"target": {"type": "agent", "agent_id": other.id}}
    return ok(agent.api().post(f"/api/v1/conversations/{cid}/participants", json=body))


def transfer(agent, cid: str, target: dict):
    return agent.api().post(f"/api/v1/conversations/{cid}/transfers", json={"target": target})


def test_a_text_transfer_to_an_agent_reoffers_it_and_the_new_agent_sees_the_history(world, tenant):
    first, target = tenant.agent("First").login(), tenant.agent("Target").login()
    cid, stream, _ = live_sms(world, tenant, first)
    reply(first, cid, "Let me get a colleague")
    ok(transfer(first, cid, {"type": "agent", "agent_id": target.id}))
    ok(accept(target, offered(stream, target, conversation_id=cid)))
    stream.wait_for("conversation.wrap_up_started", conversation_id=cid, agent_id=first.id)
    assert texts(target.api(), cid) == ["Hi", "Let me get a colleague"]


def test_a_text_transfer_to_a_queue_offers_its_agents(world, tenant):
    first, target = tenant.agent("First").login(), tenant.agent("Billing").login()
    billing = tenant.queue(name="Billing", agents=[target.id])
    cid, stream, _ = live_sms(world, tenant, first)
    ok(transfer(first, cid, {"type": "queue", "queue_id": billing["id"]}))
    stream.wait_for("conversation.queued", conversation_id=cid, queue_id=billing["id"])
    offered(stream, target, conversation_id=cid)


def test_a_text_transfer_to_a_queue_is_told_initiated_once_with_its_target_and_never_reads_transferring(world, tenant):
    first = tenant.agent("First").login()
    billing = tenant.queue(name="Billing", agents=[unavailable(tenant).id])
    cid, stream, _ = live_sms(world, tenant, first)
    tid = ok(transfer(first, cid, {"type": "queue", "queue_id": billing["id"]}))["transfer_id"]
    stream.wait_for("conversation.queued", conversation_id=cid, queue_id=billing["id"])
    [told] = replayed(tenant, "conversation.transferred", conversation_id=cid)
    assert (told["agent_id"], told["data"]["transfer_id"], told["data"]["status"]) == (first.id, tid, "initiated")
    assert (told["data"]["target"]["type"], told["data"]["target"]["queue_id"]) == ("queue", billing["id"])
    assert ok(tenant.api.get(f"/api/v1/conversations/{cid}"))["state"] != "transferring"


def test_a_text_transfer_to_a_team_offers_its_available_members(world, tenant):
    t = team(tenant, "Escalations")
    first, member, outsider = (
        tenant.agent("First").login(),
        tenant.agent("Member", teams=[t]).login(),
        tenant.agent("Outsider").login(),
    )
    cid, stream, _ = live_sms(world, tenant, first)
    ok(transfer(first, cid, {"type": "team", "team_id": t}))
    offered(stream, member, conversation_id=cid)
    assert not replayed(tenant, "offer.created", conversation_id=cid, agent_id=outsider.id)


def test_a_text_transfer_to_a_team_offers_at_most_the_tenants_simultaneous_agent_limit(world, tenant):
    settings(tenant, simultaneous_agent_limit=1)
    t = team(tenant, "Escalations")
    first = tenant.agent("First").login()
    tenant.agent("Member-0", teams=[t]).login()
    tenant.agent("Member-1", teams=[t]).login()
    cid, _, _ = live_sms(world, tenant, first)
    ok(transfer(first, cid, {"type": "team", "team_id": t}))
    world.wait(lambda: replayed(tenant, "offer.created", conversation_id=cid, via="transfer"), what="an offer")
    assert len(replayed(tenant, "offer.created", conversation_id=cid, via="transfer")) == 1


def test_a_second_agent_joins_a_text_conversation_and_replies(world, tenant):
    first, helper = tenant.agent("First").login(), tenant.agent("Helper").login()
    cid, stream, number = live_sms(world, tenant, first)
    desks = [tenant.stream(token=a.stream_token, after=0) for a in (first, helper)]
    add_participant(first, cid, helper)
    ok(accept(helper, offered(stream, helper, conversation_id=cid)))
    reply(helper, cid, "I can help with billing")
    [out] = world.wait(lambda: plivo_sent(world, number, CUSTOMER), what="the helper's reply at Plivo")
    assert out.text == "I can help with billing"
    world.plivo.text(number, CUSTOMER, "Thanks both")
    for desk in desks:
        desk.wait_for("message.received", conversation_id=cid, text="Thanks both")
    assert not replayed(tenant, "conversation.wrap_up_started", conversation_id=cid)


def test_a_second_agent_leaves_and_the_first_keeps_the_conversation(world, tenant):
    first, helper = tenant.agent("First").login(), tenant.agent("Helper").login()
    cid, stream, number = live_sms(world, tenant, first)
    joined = add_participant(first, cid, helper)
    ok(accept(helper, offered(stream, helper, conversation_id=cid)))
    ok(first.api().delete(f"/api/v1/conversations/{cid}/participants/{joined['participant_id']}"))
    stream.wait_for("conversation.wrap_up_started", conversation_id=cid, agent_id=helper.id)
    reply(first, cid, "Still with you")
    [out] = world.wait(lambda: plivo_sent(world, number, CUSTOMER), what="the first agent's reply at Plivo")
    assert out.text == "Still with you"
    assert not replayed(tenant, "conversation.wrap_up_started", conversation_id=cid, agent_id=first.id)


def test_closing_a_chat_tells_its_second_agent_it_was_removed(world, tenant):
    first, helper = tenant.agent("First").login(), tenant.agent("Helper").login()
    cid, stream, _ = live_sms(world, tenant, first)
    joined = add_participant(first, cid, helper)
    ok(accept(helper, offered(stream, helper, conversation_id=cid)))
    ok(first.api().post(f"/api/v1/conversations/{cid}/close"))
    gone = stream.wait_for("conversation.participant_removed", conversation_id=cid)
    assert (gone["data"]["participant_id"], gone["agent_id"]) == (joined["participant_id"], helper.id)


def test_a_supervisor_reads_a_live_text_conversation_of_their_team(world, tenant):
    t = team(tenant, "Floor")
    sup = tenant.agent("Sup", role="supervisor", teams=[t]).login(available=False)
    agent = tenant.agent("Agent", teams=[t]).login()
    cid, _, _ = live_sms(world, tenant, agent)
    assert texts(sup.api(), cid) == ["Hi"]


def test_a_transfer_targets_decline_sent_again_while_another_target_holds_its_offer_is_204(world, tenant):
    t = team(tenant, "Escalations")
    first, one, two = (
        tenant.agent(name, teams=[t] if name != "First" else []).login() for name in ("First", "One", "Two")
    )
    cid, stream, _ = live_sms(world, tenant, first)
    ok(transfer(first, cid, {"type": "team", "team_id": t}))
    mine, theirs = offered(stream, one, conversation_id=cid), offered(stream, two, conversation_id=cid)
    ok(decline(one, mine))
    ok(decline(one, mine))
    ok(accept(two, theirs))
    stream.wait_for("offer.accepted", conversation_id=cid, agent_id=two.id)


def test_a_decline_once_its_conversation_waits_in_a_new_queue_before_any_round_is_422(world, tenant):
    first = tenant.agent("First").login()
    billing = tenant.queue(name="Billing", agents=[unavailable(tenant).id])
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, first)
    world.plivo.text(number, CUSTOMER, "Hi")
    mine = offered(stream, first)
    ok(accept(first, mine))
    cid = mine["conversation_id"]
    ok(transfer(first, cid, {"type": "queue", "queue_id": billing["id"]}))
    stream.wait_for("conversation.queued", conversation_id=cid, queue_id=billing["id"])
    error(decline(first, mine), 422, "offer_not_open")
