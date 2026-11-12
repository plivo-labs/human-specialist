"""What an agent desk reads about a conversation: the unread count and the viewed mark, the last message, the last
activity and the tenant's own number on every row, whose turn it is, the waiting mark,
dispositions on the read, wrap-up facts on events, notes and who hears them, a corrected customer identifier, the
conversation number, and a call that joins a text conversation."""

import contextlib
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from tests.acceptance.channels import accept, close, enqueue_text, messages, offered, plivo_sent, reply, take, text_line
from tests.acceptance.conftest import connect, disposition, error, finish, ok, refused, status_changes, team, wrap_up
from tests.acceptance.test_text_channels import CUSTOMER, live_text
from tests.harness import World

VISITOR, INBOX = "visitor-81b", "support-inbox"


def row(agent, cid: str) -> dict:
    """The conversation's row in the agent's active list."""
    active = ok(agent.api().get("/api/v1/conversations", params={"state": "active"}))["data"]
    return next(c for c in active if c["id"] == cid)


def read(client, cid: str) -> dict:
    return ok(client.get(f"/api/v1/conversations/{cid}"))


def viewed(agent, cid: str):
    return agent.api().post(f"/api/v1/conversations/{cid}/viewed")


def texted(world, stream, number: str, cid: str, text: str, customer: str = CUSTOMER) -> None:
    world.plivo.text(number, customer, text)
    stream.wait_for("message.received", conversation_id=cid, text=text)


def live_chat(tenant, agent, stream, visitor: str = VISITOR) -> str:
    queue = tenant.queue(agents=[agent.id])
    cid = ok(enqueue_text(tenant.api, queue, "chat", visitor, INBOX, text="Hi"))["conversation_id"]
    return take(stream, agent, conversation_id=cid)


def chat_message(tenant, text: str, visitor: str = VISITOR):
    return ok(tenant.api.post("/api/v1/messages", json={"channel": "chat", "from": visitor, "to": INBOX, "text": text}))


def test_unread_counts_what_arrived_until_someone_views_the_conversation(world, tenant):
    agent = tenant.agent().login()
    cid, number, stream = live_text(world, tenant, agent)
    assert row(agent, cid)["unread_count"] == 1
    texted(world, stream, number, cid, "Are you there?")
    assert row(agent, cid)["unread_count"] == 2
    assert viewed(agent, cid).status_code == 204
    assert row(agent, cid)["unread_count"] == 0
    texted(world, stream, number, cid, "Hello?")
    assert row(agent, cid)["unread_count"] == 1


def test_reading_the_messages_clears_the_unread_count_for_every_viewer(world, tenant):
    t = team(tenant, "Desk")
    agent = tenant.agent("Agent", teams=[t]).login()
    sup = tenant.agent("Sup", role="supervisor", teams=[t]).login(available=False)
    cid, number, stream = live_text(world, tenant, agent)
    texted(world, stream, number, cid, "Still there?")
    assert read(sup.api(), cid)["unread_count"] == 2
    messages(agent.api(), cid)
    assert row(agent, cid)["unread_count"] == 0
    assert read(sup.api(), cid)["unread_count"] == 0


def test_after_a_redis_loss_a_list_read_shows_each_open_conversations_count_with_no_sweep(world, tenant):
    first, second = tenant.agent("First").login(), tenant.agent("Second").login()
    one, number, stream = live_text(world, tenant, first)
    reply(first, one, "Hello")
    for text in ("One", "Two"):
        texted(world, stream, number, one, text)
    other = "+14155550902"
    two, number_two, stream_two = live_text(world, tenant, second, customer=other)
    with world.redis_down():
        pass
    texted(world, stream_two, number_two, two, "Anyone?", customer=other)
    active = ok(tenant.api.get("/api/v1/conversations", params={"state": "active"}))["data"]
    assert {c["id"]: c["unread_count"] for c in active} == {one: 2, two: 2}


def test_two_replicas_reading_and_writing_at_once_end_with_the_count_the_messages_imply(world, tenant):
    agent = tenant.agent().login()
    cid = live_chat(tenant, agent, tenant.stream(after=0))
    replica = world.replica()
    writers, readers = (tenant.api, replica.client(tenant.key)), (agent.api(), replica.client(agent.agent_token))

    def counts() -> list[int]:
        pages = (ok(r.get("/api/v1/conversations", params={"state": "active"}))["data"] for r in readers)
        return [next(c["unread_count"] for c in data if c["id"] == cid) for data in pages]

    def burst(n: int) -> None:
        sent = {"channel": "chat", "from": VISITOR, "to": INBOX}
        with ThreadPoolExecutor(8) as pool:
            writes = [
                pool.submit(w.post, "/api/v1/messages", json=sent | {"text": f"m{i}"})
                for i, w in enumerate(writers * n)
            ]
            reads = [pool.submit(counts) for _ in range(n)]
        for done in writes:
            ok(done.result())
        for done in reads:
            done.result()

    burst(4)
    assert counts() == [9, 9], "no agent read it: each replica counts what the customer wrote"
    assert readers[1].post(f"/api/v1/conversations/{cid}/viewed").status_code == 204
    burst(4)
    assert counts() == [8, 8], "each message after the view, on either replica"


def test_a_view_on_one_replica_clears_the_unread_count_each_replica_reads(world, tenant):
    agent = tenant.agent().login()
    cid, _, _ = live_text(world, tenant, agent)
    other = world.replica().client(agent.agent_token)
    assert ok(other.get(f"/api/v1/conversations/{cid}"))["unread_count"] == 1
    assert other.post(f"/api/v1/conversations/{cid}/viewed").status_code == 204
    assert row(agent, cid)["unread_count"] == 0


def test_the_inbox_row_shows_the_last_message_the_last_activity_and_our_number(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, agent)
    world.plivo.text(number, CUSTOMER, "Where is my order?")
    cid = take(stream, agent)
    first = row(agent, cid)
    assert first["our_address"] == number
    assert (first["last_message"]["text"], first["last_message"]["direction"]) == ("Where is my order?", "inbound")
    sent = reply(agent, cid, "It ships today")
    last = row(agent, cid)
    shown = (last["last_message"]["id"], last["last_message"]["direction"], last["last_message"]["agent_id"])
    assert shown == (sent["id"], "outbound", agent.id)
    assert datetime.fromisoformat(last["last_activity_at"]) > datetime.fromisoformat(first["last_activity_at"])
    ok(agent.api().post(f"/api/v1/conversations/{cid}/notes", json={"text": "Gold customer"}))
    assert row(agent, cid)["last_message"]["id"] == sent["id"]
    latest = ok(tenant.api.get("/api/v1/conversations", params={"number": CUSTOMER, "channel": "sms", "limit": 1}))
    assert latest["data"][0]["our_address"] == number


def test_the_active_row_says_how_the_agent_came_to_the_conversation(world, tenant):
    first, target = tenant.agent("First").login(), tenant.agent("Target").login()
    cid, _, stream = live_text(world, tenant, first)
    assert (row(first, cid)["role"], row(first, cid)["via"]) == ("primary", "queue")
    ok(
        first.api().post(
            f"/api/v1/conversations/{cid}/transfers", json={"target": {"type": "agent", "agent_id": target.id}}
        )
    )
    take(stream, target, conversation_id=cid)
    assert (row(target, cid)["role"], row(target, cid)["via"]) == ("primary", "transfer")


def test_the_last_number_used_with_any_of_a_customers_numbers_reads_from_the_list(world, tenant):
    agent = tenant.agent().login()
    work, home = "+14155550811", "+14155550812"
    older, number_one, stream = live_text(world, tenant, agent, customer=work)
    close(agent, older, disposition(tenant)["id"], stream)
    _, number_two, _ = live_text(world, tenant, agent, customer=home)

    def last(**params) -> list[str]:
        found = ok(tenant.api.get("/api/v1/conversations", params={"channel": "sms", "limit": 1, **params}))
        return [c["our_address"] for c in found["data"]]

    assert last(number=f"{work},{home}") == [number_two]
    assert last(number=work) == [number_one]


def test_a_wrap_up_event_carries_the_seconds_from_the_conversations_start_to_that_wrap_up(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, agent)
    world.plivo.text(number, CUSTOMER, "Hi")
    offer = offered(stream, agent)
    started = time.time()
    world.wait(lambda: time.time() > started + 1.1, timeout=3, what="a second in the queue")
    ok(accept(agent, offer))
    cid = offer["conversation_id"]
    closed = close(agent, cid, disposition(tenant)["id"], stream)
    wrapped = stream.wait_for("conversation.wrapped_up", conversation_id=cid)["data"]
    assert 1 <= wrapped["duration_sec"] <= closed["data"]["duration_sec"]


def heard_notes(tenant, agent) -> list[str]:
    watch = tenant.stream(token=agent.stream_token, after=0)
    [last] = status_changes(tenant, agent, "Away")
    watch.wait_for("agent.status_changed", id=last["id"])
    return [e["data"]["note_id"] for e in watch.events if e["type"].startswith("note.")]


def test_a_note_its_edit_and_its_delete_reach_the_supervisors_stream(world, tenant):
    t = team(tenant, "Floor")
    sup = tenant.agent("Sup", role="supervisor", teams=[t]).login(available=False)
    live = connect(tenant, tenant.agent("Agent", teams=[t]).login())
    watch = tenant.stream(token=sup.stream_token, after=0)
    api, notes = live.agent.api(), f"/api/v1/conversations/{live.id}/notes"
    note = ok(api.post(notes, json={"text": "VIP caller"}))
    added = watch.wait_for("note.added", conversation_id=live.id)["data"]
    assert (added["note_id"], added["by_agent_id"], added["text"]) == (note["id"], live.agent.id, "VIP caller")
    assert note["updated_at"] == note["created_at"]
    edited = ok(api.patch(f"{notes}/{note['id']}", json={"text": "VIP caller, French"}))
    assert watch.wait_for("note.updated", note_id=note["id"])["data"]["text"] == "VIP caller, French"
    assert read(sup.api(), live.id)["notes"] == [edited]
    assert datetime.fromisoformat(edited["updated_at"]) > datetime.fromisoformat(edited["created_at"])
    assert api.delete(f"{notes}/{note['id']}").status_code == 204
    watch.wait_for("note.deleted", note_id=note["id"])
    assert read(sup.api(), live.id)["notes"] == []


def test_a_note_reaches_whoever_reads_its_conversation_and_no_one_else(world, tenant):
    watched = team(tenant, "Watched")
    sup = tenant.agent("Sup", role="supervisor", teams=[watched]).login(available=False)
    raiser, worker = tenant.agent("Raiser").login(), tenant.agent("Worker").login()
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, raiser, teams=[watched])
    world.plivo.text(number, CUSTOMER, "Please refund me")
    origin = take(stream, raiser)
    task = {"type": "task", "title": "Refund", "target": {"type": "agent", "agent_id": worker.id}}
    ok(tenant.api.post("/api/v1/tasks", json=task | {"conversation_id": origin}))
    note = ok(raiser.api().post(f"/api/v1/conversations/{origin}/notes", json={"text": "Wants a refund"}))
    assert heard_notes(tenant, raiser) == [note["id"]]
    for hidden in (worker, sup):
        error(hidden.api().get(f"/api/v1/conversations/{origin}"), 404, "conversation_not_found")
        assert heard_notes(tenant, hidden) == []


def test_a_queue_supervisor_hears_a_note_on_a_conversation_nobody_took_and_its_offered_agent_does_not(world, tenant):
    t = team(tenant, "Queue team")
    sup = tenant.agent("Sup", role="supervisor", teams=[t]).login(available=False)
    offeree = tenant.agent("Offeree").login()
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, offeree, teams=[t])
    world.plivo.text(number, CUSTOMER, "Hello?")
    cid = offered(stream, offeree)["conversation_id"]
    note = ok(tenant.api.post(f"/api/v1/conversations/{cid}/notes", json={"text": "Repeat caller"}))
    assert heard_notes(tenant, sup) == [note["id"]]
    assert [n["text"] for n in read(sup.api(), cid)["notes"]] == ["Repeat caller"]
    assert read(offeree.api(), cid)["context"] is None
    assert heard_notes(tenant, offeree) == []


def test_a_participant_whose_stint_ended_hears_and_reads_the_conversations_next_note(world, tenant):
    third = tenant.agent("Third").login()
    live = connect(tenant)
    url = f"/api/v1/calls/{live.id}/participants"
    pid = ok(live.agent.api().post(url, json={"target": {"type": "agent", "agent_id": third.id}}))["participant_id"]
    live.stream.wait_for("offer.accepted", offer_id=pid)
    ok(live.agent.api().delete(f"{url}/{pid}"))
    live.stream.wait_for("call.participant_removed", conversation_id=live.id)
    ok(wrap_up(third, live.id, disposition(tenant)["id"]))
    note = ok(live.agent.api().post(f"/api/v1/conversations/{live.id}/notes", json={"text": "Escalate"}))
    assert heard_notes(tenant, third) == [note["id"]]
    assert [n["text"] for n in read(third.api(), live.id)["notes"]] == ["Escalate"]


def test_the_conversation_read_lists_each_agents_disposition_and_queue(tenant):
    live = connect(tenant)
    sale = disposition(tenant, "Sale")
    finish(live, sale["id"])
    conv = read(tenant.api, live.id)
    assert conv["queue_id"] == live.queue["id"]
    assert [(d["agent_id"], d["disposition_id"], d["system"]) for d in conv["dispositions"]] == [
        (live.agent.id, sale["id"], False)
    ]


def test_an_sms_awaits_the_customer_after_the_agents_reply_and_the_agent_after_the_customers(world, tenant):
    agent = tenant.agent().login()
    cid, number, stream = live_text(world, tenant, agent)

    def awaiting() -> str | None:
        return read(agent.api(), cid)["awaiting"]

    assert awaiting() == "agent"
    sent = reply(agent, cid, "How can I help?")
    stream.wait_for("message.status_changed", message_id=sent["id"], status="delivered")
    assert awaiting() == "customer"
    active = ok(agent.api().get("/api/v1/conversations", params={"state": "active"}))["data"]
    assert [c["awaiting"] for c in active if c["id"] == cid] == ["customer"]
    texted(world, stream, number, cid, "My order is late")
    assert awaiting() == "agent"


def test_a_conversation_in_wrap_up_awaits_nobody_even_after_a_delivery_report(world, tenant):
    agent = tenant.agent().login()
    cid, _, stream = live_text(world, tenant, agent)
    late = world.plivo.fault(
        "delay", lambda r: "message-status" in r.path and r.params.get("Status") == "delivered", account=tenant.auth_id
    )
    sent = reply(agent, cid, "Goodbye")
    ok(agent.api().post(f"/api/v1/conversations/{cid}/close"))
    stream.wait_for("conversation.wrap_up_started", conversation_id=cid, agent_id=agent.id)
    late.release()
    stream.wait_for("message.status_changed", message_id=sent["id"], status="delivered")
    assert read(agent.api(), cid)["awaiting"] is None


def test_a_late_delivery_report_of_an_agent_reply_turns_the_conversation_back_to_the_customer(world, tenant):
    agent = tenant.agent().login()
    cid, number, stream = live_text(world, tenant, agent)
    late = world.plivo.fault(
        "delay", lambda r: "message-status" in r.path and r.params.get("Status") == "delivered", account=tenant.auth_id
    )
    sent = reply(agent, cid, "Is it fixed?")
    texted(world, stream, number, cid, "Not yet")
    assert read(agent.api(), cid)["awaiting"] == "agent"
    late.release()
    stream.wait_for("message.status_changed", message_id=sent["id"], status="delivered")
    assert read(agent.api(), cid)["awaiting"] == "customer"


def test_an_agent_marks_a_chat_waiting_until_the_customer_or_an_agent_writes_again(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    cid = live_chat(tenant, agent, stream)

    def waiting() -> bool:
        return read(agent.api(), cid)["waiting"]

    ok(agent.api().patch(f"/api/v1/conversations/{cid}", json={"waiting": True}))
    assert waiting() is True
    chat_message(tenant, "I'm back")
    assert waiting() is False
    ok(agent.api().patch(f"/api/v1/conversations/{cid}", json={"waiting": True}))
    reply(agent, cid, "Any news on your side?")
    assert waiting() is False


def test_only_a_live_text_conversation_can_be_marked_waiting(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    cid = live_chat(tenant, agent, stream)
    ok(agent.api().patch(f"/api/v1/conversations/{cid}", json={"waiting": True}))
    ok(agent.api().post(f"/api/v1/conversations/{cid}/close"))
    stream.wait_for("conversation.wrap_up_started", conversation_id=cid, agent_id=agent.id)
    assert ok(agent.api().patch(f"/api/v1/conversations/{cid}", json={"waiting": False}))["waiting"] is False
    refused(agent.api().patch(f"/api/v1/conversations/{cid}", json={"waiting": True}))
    live = connect(tenant)
    refused(live.agent.api().patch(f"/api/v1/conversations/{live.id}", json={"waiting": True}))


def test_an_agent_who_finished_a_call_sends_its_waiting_back_unchanged(tenant):
    live = connect(tenant)
    finish(live)
    url = f"/api/v1/conversations/{live.id}"
    ok(live.agent.api().patch(url, json={"waiting": read(live.agent.api(), live.id)["waiting"]}))
    error(live.agent.api().patch(url, json={"waiting": True}), 404, "conversation_not_found")


def test_a_corrected_customer_identifier_takes_the_replies_and_the_next_messages(world, tenant):
    agent = tenant.agent().login()
    cid, number, stream = live_text(world, tenant, agent, customer="+14155550821")
    right = "+14155550822"
    ok(agent.api().patch(f"/api/v1/conversations/{cid}", json={"customer": right}))
    assert read(agent.api(), cid)["number"] == right
    reply(agent, cid, "Is this the right number?")
    world.wait(lambda: plivo_sent(world, number, right), what="the reply to the corrected number")
    texted(world, stream, number, cid, "Yes it is", customer=right)


def test_conversation_numbers_are_ten_digits_in_every_tenant(world, tenant):
    other = world.new_tenant("Other")
    numbers = []
    for t in (tenant, other, tenant):
        r = enqueue_text(t.api, t.queue(), "chat", f"visitor-{len(numbers)}", INBOX, text="Hi")
        numbers.append(read(t.api, ok(r)["conversation_id"])["conversation_number"])
    assert all(len(n) == 10 and n.isdigit() for n in numbers), numbers


def test_conversations_started_at_once_on_two_replicas_take_distinct_ten_digit_numbers():
    with contextlib.closing(World()) as world:
        tenant, replica = world.new_tenant(), world.replica()
        clients, queue = (tenant.api, replica.client(tenant.key)), tenant.queue()

        def start(n: int) -> str:
            r = enqueue_text(clients[n % 2], queue, "chat", f"visitor-{n}", INBOX, text="Hi")
            return read(tenant.api, ok(r)["conversation_id"])["conversation_number"]

        with ThreadPoolExecutor(8) as pool:
            numbers = list(pool.map(start, range(40)))
    assert len(set(numbers)) == 40 and all(len(n) == 10 and n.isdigit() for n in numbers), numbers
