from tests.acceptance.channels import decline, enqueue_text, messages, offered, plivo_sent, take
from tests.acceptance.conftest import connect, error, ok, registered_number, replayed, seen, settings, unavailable
from tests.acceptance.test_outbound_text import start
from tests.acceptance.test_text_channels import live_text

CUSTOMER, OTHER = "+14155550931", "+14155550932"
TEMPLATE = {"name": "follow_up", "language": "en_US", "components": []}


def send(agent, cid: str, **body):
    return agent.api().post(f"/api/v1/conversations/{cid}/messages", json=body)


def unread(client, cid: str) -> int:
    return ok(client.get(f"/api/v1/conversations/{cid}"))["unread_count"]


def test_an_agents_text_in_wrap_up_is_refused(world, tenant):
    agent = tenant.agent().login()
    cid, number, stream = live_text(world, tenant, agent, customer=CUSTOMER)
    ok(agent.api().post(f"/api/v1/conversations/{cid}/close"))
    stream.wait_for("conversation.wrap_up_started", conversation_id=cid, agent_id=agent.id)
    error(send(agent, cid, text="One more thing"), 409, "in_wrap_up")
    live = connect(tenant)
    live.caller.hangup()
    live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id)
    error(send(live.agent, live.id, channel="sms", text="Here is the tracking link"), 409, "in_wrap_up")
    assert not plivo_sent(world, number, CUSTOMER) and not plivo_sent(world, live.number, live.caller.from_)


def test_a_message_on_a_call_names_sms_or_whatsapp(world, tenant):
    live = connect(tenant)
    error(send(live.agent, live.id, text="Hello"), 422, "channel_not_allowed")
    assert not replayed(tenant, "message.sent", conversation_id=live.id)


def test_an_agents_whatsapp_start_needs_a_template(world, tenant):
    agent = tenant.agent().login()
    number = registered_number(tenant)
    error(start(tenant.api, agent, "whatsapp", number, CUSTOMER, text="Hi there"), 422, "validation_failed")
    assert not plivo_sent(world, number, CUSTOMER)


def test_numbers_are_taken_with_spaces_and_without_their_plus(world, tenant):
    agent = tenant.agent().login()
    number = registered_number(tenant)
    r = start(tenant.api, agent, "sms", number.removeprefix("+"), "1 415 555 0931", text="Your order shipped")
    cid = ok(r)["conversation_id"]
    world.wait(lambda: plivo_sent(world, number, CUSTOMER), what="the text at Plivo")
    assert ok(tenant.api.get(f"/api/v1/conversations/{cid}"))["number"] == CUSTOMER
    queue = tenant.queue(agents=[agent.id])
    joined = ok(enqueue_text(tenant.api, queue, "sms", "+1 415 555 0932", number, text="Hi"))["conversation_id"]
    assert ok(tenant.api.get(f"/api/v1/conversations/{joined}"))["number"] == OTHER


def test_a_corrected_customer_is_an_address_of_the_conversations_channel(world, tenant):
    agent = tenant.agent().login()
    cid, number, _ = live_text(world, tenant, agent, customer=CUSTOMER)
    path = f"/api/v1/conversations/{cid}"
    error(tenant.api.patch(path, json={"customer": "ana@customer.example"}), 422, "invalid_customer")
    assert ok(tenant.api.patch(path, json={"customer": "1 415 555 0932"}))["number"] == OTHER
    ok(send(agent, cid, text="Is this the right number?"))
    world.wait(lambda: plivo_sent(world, number, OTHER), what="the reply at the corrected number")


def test_a_send_plivo_answers_with_no_message_id_is_refused(world, tenant):
    agent = tenant.agent().login()
    cid = live_text(world, tenant, agent, customer=CUSTOMER)[0]
    sent = lambda r: r.method == "POST" and r.path.endswith("/Message/")  # noqa: E731
    world.plivo.fault("lose", sent, account=tenant.auth_id, status=202)
    error(send(agent, cid, text="Your refund is on its way"), 422, "message_refused")


def test_reading_one_page_of_messages_leaves_the_later_ones_unread(world, tenant):
    agent = tenant.agent().login()
    cid, number, stream = live_text(world, tenant, agent, customer=CUSTOMER)
    for said in ("Still there?", "Hello?"):
        world.plivo.text(number, CUSTOMER, said)
        stream.wait_for("message.received", conversation_id=cid, text=said)
    path = f"/api/v1/conversations/{cid}/messages"
    first = ok(agent.api().get(path, params={"limit": 1}))
    assert unread(agent.api(), cid) == 3
    assert ok(agent.api().get(path, params={"limit": 2, "cursor": first["next_cursor"]}))["next_cursor"] is None
    assert unread(agent.api(), cid) == 0


def test_a_message_cursor_of_another_conversation_is_refused(world, tenant):
    agent = tenant.agent().login()
    one, number, stream = live_text(world, tenant, agent, customer=CUSTOMER)
    world.plivo.text(number, CUSTOMER, "Still there?")
    stream.wait_for("message.received", conversation_id=one, text="Still there?")
    cursor = ok(tenant.api.get(f"/api/v1/conversations/{one}/messages", params={"limit": 1}))["next_cursor"]
    world.plivo.text(number, OTHER, "Hi")
    two = stream.wait_for("message.received", **{"from": OTHER})["conversation_id"]
    path = f"/api/v1/conversations/{two}/messages"
    error(tenant.api.get(path, params={"cursor": cursor}), 422, "invalid_cursor")


def test_a_chat_queues_assigned_line_names_the_agent_title_cased(world, tenant):
    agent = tenant.agent("maya lopez").login()
    stream = tenant.stream(after=0)
    queue = tenant.queue(agents=[agent.id], text={"messages": {"chat": {"assigned": "{agent_name} has joined"}}})
    cid = ok(enqueue_text(tenant.api, queue, "chat", "visitor-93", "support-inbox", text="Hi"))["conversation_id"]
    take(stream, agent, conversation_id=cid)
    stream.wait_for("message.sent", conversation_id=cid, text="Maya Lopez has joined")


def test_a_file_keeps_its_name_type_and_size(world, tenant):
    agent = tenant.agent().login()
    cid, number, stream = live_text(world, tenant, agent, type_="whatsapp", customer=CUSTOMER)
    invoice = {"url": "https://files.example/invoice.pdf", "name": "invoice.pdf", "size": 120_000}
    invoice["content_type"] = "application/pdf"
    ok(send(agent, cid, attachments=[invoice]))
    world.plivo.text(number, CUSTOMER, "Here it is", type_="whatsapp", media=(b"picture-bytes",))
    stream.wait_for("message.received", conversation_id=cid, text="Here it is")
    sent, got = (m["attachments"][0] for m in messages(agent.api(), cid)[1:])
    assert {k: sent[k] for k in invoice} == invoice and got["content_type"] == "image/jpeg"


def test_a_customers_whatsapp_file_with_no_caption_reads_no_text(world, tenant):
    agent = tenant.agent().login()
    cid, number, stream = live_text(world, tenant, agent, type_="whatsapp", customer=CUSTOMER)
    world.plivo.text(number, CUSTOMER, "", type_="whatsapp", media=(b"picture-bytes",))
    world.wait(lambda: seen(stream, "message.received", conversation_id=cid)[1:], what="the customer's file")
    assert seen(stream, "message.received", conversation_id=cid)[1]["data"]["text"] is None
    assert [m["text"] for m in messages(agent.api(), cid)] == ["Hi", None]


def test_an_sms_with_its_files_is_at_most_five_megabytes(world, tenant):
    agent = tenant.agent().login()
    cid = live_text(world, tenant, agent, customer=CUSTOMER)[0]
    video = [{"url": "https://files.example/unboxing.mp4", "size": 5_000_000}]
    error(send(agent, cid, text="Here", attachments=video), 422, "message_too_large")
    ok(send(agent, cid, attachments=video))


def test_an_email_whose_offer_rounds_run_out_waits_for_an_agent_not_yet_offered(world, tenant):
    first, later = tenant.agent("First").login(), unavailable(tenant, "Later")
    stream = tenant.stream(after=0)
    queue = tenant.queue(agents=[first.id, later.id], agents_at_once=1, retries=0)
    r = enqueue_text(tenant.api, queue, "email", "ana@customer.example", "help@acme.example", text="Refund?")
    cid = ok(r)["conversation_id"]
    ok(decline(first, offered(stream, first, conversation_id=cid)))
    later.set_status("Available")
    offered(stream, later, conversation_id=cid)
    assert not replayed(tenant, "conversation.overflowed", conversation_id=cid)


def test_an_agent_on_an_sms_writes_on_whatsapp_and_to_another_number_in_conversations_it_holds(world, tenant):
    world.plivo.templates.add((TEMPLATE["name"], TEMPLATE["language"]))
    settings(tenant, capacity={"sms": 2})
    agent = tenant.agent().login()
    cid, number, _ = live_text(world, tenant, agent, customer=CUSTOMER)
    error(send(agent, cid, channel="whatsapp", text="Hi"), 422, "template_required")
    wa = ok(send(agent, cid, channel="whatsapp", template=TEMPLATE))
    world.wait(lambda: [m for m in plivo_sent(world, number, CUSTOMER) if m.type == "whatsapp"], what="the WhatsApp")
    thread = ok(tenant.api.get(f"/api/v1/conversations/{wa['conversation_id']}"))
    assert (thread["channel"], thread["state"], thread["agent_ids"]) == ("whatsapp", "active", [agent.id])
    assert ok(send(agent, cid, channel="whatsapp", text="Any questions?"))["conversation_id"] == thread["id"]
    other = ok(send(agent, cid, to="1 415 555 0932", text="As promised"))
    assert other["conversation_id"] not in (cid, thread["id"])
    world.wait(lambda: plivo_sent(world, number, OTHER), what="the SMS to the other number")
    assert ok(tenant.api.get(f"/api/v1/conversations/{cid}"))["state"] == "active"


def test_an_agent_on_a_call_emails_the_caller_from_a_registered_address_and_keeps_one_sender_per_channel(tenant):
    live = connect(tenant)
    tenant.route_number("billing@acme.example", {"type": "hangup"})
    mail = {"channel": "email", "text": "Your receipt", "email": {"subject": "Receipt"}}
    error(send(live.agent, live.id, **mail), 422, "to_required")
    to = {"to": "Ana <ana@customer.example>"}
    error(send(live.agent, live.id, **mail, **to, **{"from": "sales@acme.example"}), 422, "from_not_allowed")
    sent = ok(send(live.agent, live.id, **mail, **to, **{"from": "billing@acme.example"}))
    told = live.stream.wait_for("message.sent", message_id=sent["id"])
    assert told["conversation_id"] == sent["conversation_id"] != live.id
    assert (told["data"]["from"], told["data"]["to"]) == ("billing@acme.example", "ana@customer.example")
    ok(send(live.agent, live.id, channel="sms", text="A link"))
    second = {"from": registered_number(tenant)}
    refused = error(send(live.agent, live.id, channel="sms", text="Another link", **second), 422, "sender_mismatch")
    assert refused["details"]["from"] == live.number


def test_a_calls_first_free_text_whatsapp_is_refused_and_opens_nothing(world, tenant):
    live = connect(tenant)
    error(send(live.agent, live.id, channel="whatsapp", text="Hi"), 422, "template_required")
    world.wait(world.plivo.idle, what="the simulator to go idle")
    assert not plivo_sent(world, live.number, live.caller.from_)
    assert ok(tenant.api.get("/api/v1/conversations", params={"channel": "whatsapp"}))["data"] == []


def test_a_switch_from_an_enqueued_conversation_needs_its_sender_registered(world, tenant):
    settings(tenant, capacity={"sms": 2})
    agent = tenant.agent().login()
    stream, queue, unregistered = tenant.stream(after=0), tenant.queue(agents=[agent.id]), tenant.number()
    cid = ok(enqueue_text(tenant.api, queue, "sms", CUSTOMER, unregistered, text="Hi"))["conversation_id"]
    take(stream, agent, conversation_id=cid)
    error(send(agent, cid, to=OTHER, text="As promised"), 422, "from_not_allowed")
    world.wait(world.plivo.idle, what="the simulator to go idle")
    assert not plivo_sent(world, unregistered, OTHER)


def test_the_conversations_a_call_opened_share_one_sender_per_channel_and_each_others_threads(tenant):
    live = connect(tenant)
    tenant.route_number("billing@acme.example", {"type": "hangup"})
    sms = ok(send(live.agent, live.id, channel="sms", text="A link"))["conversation_id"]
    mail = {"channel": "email", "to": "ana@customer.example", "from": "billing@acme.example", "text": "Your receipt"}
    email = ok(send(live.agent, live.id, **mail))["conversation_id"]
    assert ok(send(live.agent, sms, **mail))["conversation_id"] == email
    other = {"channel": "sms", "to": live.caller.from_, "from": registered_number(tenant), "text": "Another link"}
    refused = error(send(live.agent, email, **other), 422, "sender_mismatch")
    assert refused["details"]["from"] == live.number


def test_an_agent_in_wrap_up_cannot_send_files_to_another_address_through_a_thread_it_opened(world, tenant):
    settings(tenant, capacity={"sms": 2})
    agent = tenant.agent().login()
    cid, number, stream = live_text(world, tenant, agent, customer=CUSTOMER)
    ok(send(agent, cid, to=OTHER, text="As promised"))
    world.wait(lambda: plivo_sent(world, number, OTHER), what="the SMS to the other number")
    ok(agent.api().post(f"/api/v1/conversations/{cid}/close"))
    stream.wait_for("conversation.wrap_up_started", conversation_id=cid, agent_id=agent.id)
    label = [{"url": "https://files.example/label.png"}]
    error(send(agent, cid, to=OTHER, attachments=label), 409, "in_wrap_up")
    world.wait(world.plivo.idle, what="the simulator to go idle")
    assert len(plivo_sent(world, number, OTHER)) == 1


def test_a_family_nine_switches_deep_still_shares_its_threads(world, tenant):
    settings(tenant, capacity={"sms": 10})
    agent = tenant.agent().login()
    root = cid = live_text(world, tenant, agent, customer=CUSTOMER)[0]
    customers = [f"+1415555094{i}" for i in range(9)]
    for to in customers:
        cid = ok(send(agent, cid, to=to, text="Passing you on"))["conversation_id"]
    assert ok(send(agent, root, to=customers[-1], text="And back"))["conversation_id"] == cid


def test_a_participant_on_an_sms_writes_to_its_customer_from_the_same_sender(world, tenant):
    settings(tenant, capacity={"sms": 2})
    agent, helper = tenant.agent().login(), tenant.agent("Helper").login()
    cid, number, stream = live_text(world, tenant, agent, customer=CUSTOMER)
    target = {"target": {"type": "agent", "agent_id": helper.id}}
    ok(tenant.api.post(f"/api/v1/conversations/{cid}/participants", json=target))
    take(stream, helper, conversation_id=cid)
    other = {"from": registered_number(tenant), "text": "Billing here"}
    assert error(send(helper, cid, **other), 422, "sender_mismatch")["details"]["from"] == number
    world.wait(world.plivo.idle, what="the simulator to go idle")
    assert not plivo_sent(world, other["from"], CUSTOMER)
