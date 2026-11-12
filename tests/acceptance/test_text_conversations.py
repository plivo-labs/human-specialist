"""SMS conversations over the simulator: a customer text enters a queue, is offered, accepted and answered
through Plivo. Offers, capacity, wrap-up, timeouts and sticky routing work as they do for calls, without RONA
strikes."""

import contextlib
import uuid
from concurrent.futures import ThreadPoolExecutor

from tests.acceptance.channels import (
    accept,
    close,
    decline,
    messages,
    offered,
    plivo_app,
    plivo_sent,
    reply,
    take,
    text_line,
    texts,
)
from tests.acceptance.conftest import connect, disposition, error, fence, ok, replayed, seen, settings, unavailable
from tests.harness import World

CUSTOMER = "+14155550201"


def status_name(tenant, agent) -> str:
    return ok(tenant.api.get(f"/api/v1/agents/{agent.id}/status"))["status"]["name"]


def two_agents_one_at_a_time(world, tenant, **queue_fields):
    """Agents A and B on a text queue that offers one agent per round. Returns (number, stream, first offer,
    the agent offered first, the other)."""
    a, b = tenant.agent("A").login(), tenant.agent("B").login()
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, a, b, agents_at_once=1, retries=1, **queue_fields)
    world.plivo.text(number, CUSTOMER, "Hi")
    first = stream.wait_for("offer.created")
    first_agent, other = (a, b) if first["agent_id"] == a.id else (b, a)
    return number, stream, first, first_agent, other


def test_an_sms_to_a_queue_number_is_offered_to_its_agent(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    number, queue = text_line(tenant, agent)
    world.plivo.text(number, CUSTOMER, "Is my order shipped?")
    cid = offered(stream, agent, channel="sms")["conversation_id"]
    assert stream.wait_for("conversation.started", conversation_id=cid)["data"]["channel"] == "sms"
    queued = stream.wait_for("conversation.queued", conversation_id=cid)["data"]
    assert (queued["queue_id"], queued["from"], queued["to"]) == (queue["id"], CUSTOMER, number)
    assert agent.phone.legs == []


def test_messages_sent_while_queued_reach_the_agent_on_accept(world, tenant):
    agent = unavailable(tenant)
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, agent)
    world.plivo.text(number, CUSTOMER, "Hello")
    world.plivo.text(number, CUSTOMER, "Anyone there?")
    cid = stream.wait_for("message.received", text="Anyone there?")["conversation_id"]
    agent.set_status("Available")
    assert take(stream, agent) == cid
    assert texts(agent.api(), cid) == ["Hello", "Anyone there?"]


def test_a_customer_message_reaches_the_agents_stream(world, tenant):
    agent = tenant.agent().login()
    desk = tenant.stream(token=agent.stream_token, after=0)
    number, _ = text_line(tenant, agent)
    world.plivo.text(number, CUSTOMER, "First")
    cid = take(desk, agent)
    world.plivo.text(number, CUSTOMER, "Second")
    got = desk.wait_for("message.received", conversation_id=cid, text="Second")["data"]
    assert (got["channel"], got["from"], got["to"]) == ("sms", CUSTOMER, number)


def test_an_agent_sms_reply_reaches_plivo_from_the_tenant_number(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, agent)
    world.plivo.text(number, CUSTOMER, "Hi")
    cid = take(stream, agent)
    sent = reply(agent, cid, "On its way")
    [out] = world.wait(lambda: plivo_sent(world, number, CUSTOMER), what="the reply at Plivo")
    assert (out.type, out.text) == ("sms", "On its way")
    event = stream.wait_for("message.sent", conversation_id=cid)["data"]
    assert (event["message_id"], event["delivery"], event["by_agent_id"]) == (sent["id"], "plivo", agent.id)


def test_a_delivery_report_updates_the_message(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, agent)
    world.plivo.text(number, CUSTOMER, "Hi")
    cid = take(stream, agent)
    sent = reply(agent, cid, "On its way")
    stream.wait_for("message.status_changed", conversation_id=cid, message_id=sent["id"], status="delivered")
    assert [m["status"] for m in messages(agent.api(), cid) if m["id"] == sent["id"]] == ["delivered"]


def test_a_second_sms_from_the_same_customer_joins_the_open_conversation(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, agent)
    world.plivo.text(number, CUSTOMER, "Hi")
    cid = take(stream, agent)
    world.plivo.text(number, CUSTOMER, "Also, my address changed")
    stream.wait_for("message.received", conversation_id=cid, text="Also, my address changed")
    fence(tenant)
    assert len(seen(stream, "conversation.started")) == 1


def test_a_plivo_retry_of_an_inbound_sms_stores_it_once(world, tenant):
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant)
    once = world.plivo.text(number, CUSTOMER, "Once")
    cid = stream.wait_for("message.received", text="Once")["conversation_id"]
    [sent] = [cb for cb in world.plivo.callbacks if cb.params.get("MessageUUID") == once.message_uuid]
    retry = world.plivo.sign(sent.url, sent.params, tenant.auth_token, version="V2")
    assert world.anon().post(sent.url, data=sent.params, headers=retry).status_code == 200
    fence(tenant)
    assert len(seen(stream, "message.received")) == 1 and texts(tenant.api, cid) == ["Once"]


def test_an_inbound_sms_needs_a_valid_v2_signature_and_a_fresh_nonce(world, tenant):
    number, _ = text_line(tenant)
    url = plivo_app(tenant, number)["message_url"]

    def form(text: str) -> dict:
        return {
            "From": "14155550202",
            "To": number.removeprefix("+"),
            "Type": "sms",
            "Text": text,
            "MessageUUID": str(uuid.uuid4()),
        }

    bad = form("Forged")
    forged = world.plivo.sign(url, bad, "not-the-tenants-token", version="V2")
    assert world.anon().post(url, data=bad).status_code == 401
    assert world.anon().post(url, data=bad, headers=forged).status_code == 401
    first = form("Hi")
    signed = world.plivo.sign(url, first, tenant.auth_token, version="V2")
    assert world.anon().post(url, data=first, headers=signed).status_code == 200
    assert world.anon().post(url, data=form("Replayed"), headers=signed).status_code == 401
    fence(tenant)
    assert [e["data"]["text"] for e in tenant.events(type="message.received")] == ["Hi"]
    [conversation] = ok(tenant.api.get("/api/v1/conversations"))["data"]
    assert texts(tenant.api, conversation["id"]) == ["Hi"]


def test_an_unanswered_text_offer_moves_to_the_next_agent_without_a_strike(world, tenant):
    settings(tenant, rona={"strikes": 1}, text={"offer_timeout_sec": 30})
    _, stream, first, missed, other = two_agents_one_at_a_time(world, tenant)
    world.advance(31)
    assert stream.wait_for("offer.missed", offer_id=first["data"]["offer_id"])["data"].get("strike") is None
    offered(stream, other)
    assert status_name(tenant, missed) == "Available"


def test_a_declined_text_offer_moves_to_the_next_agent_without_a_strike(world, tenant):
    settings(tenant, rona={"strikes": 1})
    _, stream, first, declining, other = two_agents_one_at_a_time(world, tenant)
    ok(decline(declining, first))
    stream.wait_for("offer.rejected", offer_id=first["data"]["offer_id"])
    offered(stream, other)
    assert status_name(tenant, declining) == "Available"


def test_an_unanswered_sms_overflows_and_closes_at_the_text_wait_timeout(world, tenant):
    agent = unavailable(tenant)
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, agent, text={"wait_timeout_sec": 60})
    world.plivo.text(number, CUSTOMER, "Hello?")
    cid = stream.wait_for("conversation.queued")["conversation_id"]
    world.advance(61)
    assert stream.wait_for("conversation.overflowed", conversation_id=cid)["data"]["reason"] == "wait_timeout"
    assert stream.wait_for("conversation.closed", conversation_id=cid)["data"]["result"] == "queue_timeout"


def test_sms_capacity_holds_two_conversations_and_a_close_offers_the_third(world, tenant):
    settings(tenant, capacity={"sms": 2})
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, agent)
    held = []
    for customer in ("+14155550211", "+14155550212"):
        world.plivo.text(number, customer, "Hi")
        held.append(take(stream, agent, **{"from": customer}))
    world.plivo.text(number, "+14155550213", "Me too")
    third = stream.wait_for("conversation.queued", **{"from": "+14155550213"})["conversation_id"]
    assert not replayed(tenant, "offer.created", conversation_id=third)
    close(agent, held[0], disposition(tenant)["id"], stream)
    offered(stream, agent, conversation_id=third)


def test_raising_the_sms_capacity_offers_the_agent_the_conversation_waiting_for_its_slot(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, agent)
    world.plivo.text(number, "+14155550214", "Hi")
    take(stream, agent, **{"from": "+14155550214"})
    world.plivo.text(number, "+14155550215", "Me too")
    waiting = stream.wait_for("conversation.queued", **{"from": "+14155550215"})["conversation_id"]
    settings(tenant, capacity={"sms": 2})
    offered(stream, agent, conversation_id=waiting)


def test_an_agent_on_a_call_is_still_offered_an_sms(world, tenant):
    agent = tenant.agent().login()
    live = connect(tenant, agent)
    number, _ = text_line(tenant, agent)
    world.plivo.text(number, CUSTOMER, "Hi")
    offered(live.stream, agent, channel="sms")


def test_an_agent_at_sms_capacity_still_gets_a_call(world, tenant):
    settings(tenant, capacity={"sms": 1})
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, agent)
    world.plivo.text(number, CUSTOMER, "Hi")
    take(stream, agent)
    connect(tenant, agent)


def test_closing_an_sms_starts_wrap_up_and_the_disposition_closes_it(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, agent)
    world.plivo.text(number, CUSTOMER, "Hi")
    cid = take(stream, agent)
    assert close(agent, cid, disposition(tenant)["id"], stream)["data"]["result"] == "agent_closed"


def test_a_tenant_key_closing_a_waiting_sms_withdraws_its_offer_and_closes_it_as_cancelled(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, agent)
    world.plivo.text(number, CUSTOMER, "Hi")
    cid = offered(stream, agent)["conversation_id"]
    ok(tenant.api.post(f"/api/v1/conversations/{cid}/close"))
    assert stream.wait_for("offer.withdrawn", conversation_id=cid, agent_id=agent.id)["data"]["reason"] == "cancelled"
    assert stream.wait_for("conversation.closed", conversation_id=cid)["data"]["result"] == "cancelled"


def test_an_idle_sms_closes_at_the_session_timeout(world, tenant):
    settings(tenant, text={"session_timeout_sec": {"sms": 600}})
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, agent)
    world.plivo.text(number, CUSTOMER, "Hi")
    cid = take(stream, agent)
    world.advance(601)
    assert stream.wait_for("conversation.closed", conversation_id=cid)["data"]["result"] == "session_timeout"
    assert not replayed(tenant, "conversation.wrap_up_started", conversation_id=cid)


def test_an_sms_after_close_opens_a_new_conversation_offered_to_the_same_agent_first(world, tenant):
    number, stream, first, served, _ = two_agents_one_at_a_time(world, tenant, sticky={"mode": "loose"})
    ok(accept(served, first))
    old = first["conversation_id"]
    close(served, old, disposition(tenant)["id"], stream)
    world.plivo.text(number, CUSTOMER, "One more thing")
    [new] = world.wait(lambda: seen(stream, "conversation.started")[1:], what="a second conversation")
    assert new["conversation_id"] != old
    assert stream.wait_for("offer.created", conversation_id=new["conversation_id"])["agent_id"] == served.id


def test_a_sticky_write_another_replica_sends_late_leaves_the_later_agent_sticky(world, tenant):
    number, stream, first, early, later = two_agents_one_at_a_time(world, tenant, sticky={"mode": "loose"})
    done, replica = disposition(tenant)["id"], world.replica(gated=True)
    with ThreadPoolExecutor(1) as pool:
        with replica.redis_held(lambda sent: b":sticky:" in sent and b"MGET" not in sent) as held:
            took = pool.submit(
                replica.client(early.agent_token).post, f"/api/v1/offers/{first['data']['offer_id']}/accept"
            )
            world.wait(held, what="the replica's sticky write to wait")
            close(early, first["conversation_id"], done, stream)
            world.plivo.text(number, CUSTOMER, "Me again")
            second = world.wait(lambda: seen(stream, "conversation.started")[1:], what="a second conversation")[0]
            offer = stream.wait_for("offer.created", conversation_id=second["conversation_id"])
            if offer["agent_id"] == early.id:
                ok(decline(early, offer))
                offer = offered(stream, later, conversation_id=second["conversation_id"])
            ok(accept(later, offer))
        assert took.result().status_code == 204
    close(later, second["conversation_id"], done, stream)
    world.plivo.text(number, CUSTOMER, "And again")
    third = world.wait(lambda: seen(stream, "conversation.started")[2:], what="a third conversation")[0]
    assert stream.wait_for("offer.created", conversation_id=third["conversation_id"])["agent_id"] == later.id


def test_the_agents_active_list_filters_by_channel(world, tenant):
    agent = tenant.agent().login()
    live = connect(tenant, agent)
    number, _ = text_line(tenant, agent)
    world.plivo.text(number, CUSTOMER, "Hi")
    sms = take(live.stream, agent, channel="sms")
    active = ok(agent.api().get("/api/v1/conversations", params={"state": "active", "channel": "sms"}))["data"]
    assert [(c["id"], c["channel"]) for c in active] == [(sms, "sms")]


def test_an_accept_on_one_replica_and_a_decline_on_the_other_in_one_round_leave_one_primary(world, tenant):
    first, second = tenant.agent("First").login(), tenant.agent("Second").login()
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, first, second, agents_at_once=2)
    world.plivo.text(number, CUSTOMER, "Hi")
    mine, theirs = offered(stream, first), offered(stream, second)
    replica = world.replica()
    with ThreadPoolExecutor(2) as pool:
        took = pool.submit(replica.client(first.agent_token).post, f"/api/v1/offers/{mine['data']['offer_id']}/accept")
        turned = pool.submit(decline, second, theirs)
        assert took.result().status_code == 204 and turned.result().status_code in (204, 422)
    stream.wait_for("offer.accepted", agent_id=first.id)
    fence(tenant)
    cid = mine["conversation_id"]
    assert ok(tenant.api.get(f"/api/v1/conversations/{cid}"))["agent_ids"] == [first.id]
    assert [e["agent_id"] for e in replayed(tenant, "offer.accepted", conversation_id=cid)] == [first.id]
    assert ok(second.api().get("/api/v1/me"))["pending_offers"] == []


def test_a_late_accept_is_422_and_a_late_decline_is_204_while_the_text_still_waits_and_422_once_it_is_taken(
    world, tenant
):
    _, stream, first, first_agent, other = two_agents_one_at_a_time(world, tenant)
    world.run_jobs("offer_timeout")
    offered(stream, other)
    error(accept(first_agent, first), 422, "offer_not_open")
    ok(decline(first_agent, first))
    take(stream, other)
    error(decline(first_agent, first), 422, "offer_not_open")


def test_a_late_accept_or_decline_once_the_conversation_closed_is_422(world, tenant):
    _, stream, first, first_agent, other = two_agents_one_at_a_time(world, tenant)
    ok(decline(first_agent, first))
    close(other, take(stream, other), disposition(tenant)["id"], stream)
    error(accept(first_agent, first), 422, "offer_not_open")
    error(decline(first_agent, first), 422, "offer_not_open")


def test_a_late_accept_or_decline_is_422_past_its_offers_life_while_a_later_offer_keeps_its_tombstone():
    # A World of its own: a 26 h jump in the shared one runs every timer the earlier tests left.
    with contextlib.closing(World()) as world:
        tenant = world.new_tenant()
        first_agent, other = tenant.agent("A").login(), tenant.agent("B")
        stream = tenant.stream(after=0)
        number, _ = text_line(tenant, first_agent, other, agents_at_once=1, retries=1)
        world.plivo.text(number, CUSTOMER, "Hi")
        first = offered(stream, first_agent)
        ok(decline(first_agent, first))
        world.travel(20 * 3600)
        other.login()
        close(other, take(stream, other), disposition(tenant)["id"], stream)
        world.travel(6 * 3600)
        first_agent.login(available=False)
        error(accept(first_agent, first), 422, "offer_not_open")
        error(decline(first_agent, first), 422, "offer_not_open")


def test_a_decline_sent_again_while_its_rounds_other_offer_is_open_is_204_and_starts_no_round(world, tenant):
    first, second = tenant.agent("First").login(), tenant.agent("Second").login()
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, first, second, agents_at_once=2, retries=1)
    world.plivo.text(number, CUSTOMER, "Hi")
    mine, theirs = offered(stream, first), offered(stream, second)
    ok(decline(first, mine))
    ok(decline(first, mine))
    world.run_jobs("next_round")
    fence(tenant)
    assert len(replayed(tenant, "offer.created", conversation_id=mine["conversation_id"])) == 2
    ok(accept(second, theirs))
