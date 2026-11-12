"""Text channels as a contact center runs them: chat queue lines, email recipients and checks, delivery statuses,
WhatsApp quotes, interactive messages and templates, SMS sending rules, the idle close and text transfers."""

import json
import math
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from tests.acceptance.channels import (
    accept,
    close,
    enqueue_text,
    messages,
    offered,
    plivo_app,
    plivo_sent,
    reply,
    take,
    text_line,
    texts,
)
from tests.acceptance.conftest import (
    connect,
    disposition,
    disposition_named,
    error,
    finish,
    hs_header,
    ok,
    plivo_post,
    refused,
    registered_number,
    replayed,
    run_jobs_until,
    seen,
    settings,
    team,
    unavailable,
)
from tests.acceptance.test_outbound_text import start
from tests.acceptance.test_recordings import transcript

CUSTOMER = "+14155550901"
WABA, OTHER_WABA = "104561283945", "104561283946"
VISITOR, INBOX = "visitor-91", "support-inbox"
LABEL = [{"url": "https://files.example/label.png"}]


def plivo_callback(tenant, number: str, path: str, params: dict):
    """A messaging callback as Plivo posts it to HS: a form, V2-signed with the tenant's token."""
    base = plivo_app(tenant, number)["message_url"].removesuffix("/message")
    url = f"{base}/{path}"
    signed = tenant.world.plivo.sign(url, params, tenant.auth_token, version="V2")
    return tenant.world.anon().post(url, data=params, headers=signed)


def whatsapp_in(tenant, number: str, content_type: str, **fields):
    """A customer's WhatsApp of `content_type` to `number`; Plivo sends nested objects JSON-encoded."""
    params = {"From": CUSTOMER.removeprefix("+"), "To": number.removeprefix("+"), "Type": "whatsapp"}
    params |= {"MessageUUID": str(uuid.uuid4()), "ContentType": content_type}
    params |= {k: json.dumps(v) if isinstance(v, dict) else v for k, v in fields.items()}
    return plivo_callback(tenant, number, "message", params)


def live_text(world, tenant, agent, type_="sms", text="Hi", customer=CUSTOMER) -> tuple[str, str, object]:
    """A text from `customer` that `agent` accepted. Returns (conversation id, the tenant's number, stream)."""
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, agent)
    world.plivo.text(number, customer, text, type_=type_)
    return take(stream, agent, **{"from": customer}), number, stream


def text_transfer(agent, cid: str, target: dict, **body):
    return agent.api().post(f"/api/v1/conversations/{cid}/transfers", json={"target": target, **body})


def email_sender(tenant, agent) -> str:
    """An email address registered with HS as the tenant's own, routed to a queue holding `agent`."""
    queue = tenant.queue(agents=[agent.id])
    return tenant.route_number("billing@acme.example", {"type": "queue", "queue_id": queue["id"]})["number"]


def test_a_chat_queue_sends_its_assigned_and_closed_lines(world, tenant):
    agent = tenant.agent("Maya").login()
    stream = tenant.stream(after=0)
    lines = {"assigned": "{agent_name} has joined", "closed": "Thanks for chatting with us"}
    queue = tenant.queue(agents=[agent.id], text={"messages": {"chat": lines}})
    cid = ok(enqueue_text(tenant.api, queue, "chat", VISITOR, INBOX, text="Hi"))["conversation_id"]
    take(stream, agent, conversation_id=cid)
    joined = stream.wait_for("message.sent", conversation_id=cid, text="Maya has joined")["data"]
    assert (joined["delivery"], joined["to"], joined["by_agent_id"]) == ("external", VISITOR, None)
    close(agent, cid, disposition(tenant)["id"], stream)
    assert texts(agent.api(), cid) == ["Hi", "Maya has joined", "Thanks for chatting with us"]


def sent_lines(world, number: str, to: str) -> list[tuple[str, str]]:
    return [(m.type, m.text) for m in plivo_sent(world, number, to)]


def test_a_queue_sends_each_channels_own_lines_on_that_channel(world, tenant):
    agent = tenant.agent("Maya").login()
    stream, done, texter = tenant.stream(after=0), disposition(tenant)["id"], "+14155550902"
    lines = {"chat": {"assigned": "{agent_name} joined the chat"}, "sms": {"closed": "Bye from SMS"}}
    lines["whatsapp"] = {"assigned": "{agent_name} is on WhatsApp", "closed": "Bye from WhatsApp"}
    number, queue = text_line(tenant, agent, text={"messages": lines})
    chat = ok(enqueue_text(tenant.api, queue, "chat", VISITOR, INBOX, text="Hi"))["conversation_id"]
    take(stream, agent, conversation_id=chat)
    close(agent, chat, done, stream)
    for customer, type_ in ((CUSTOMER, "whatsapp"), (texter, "sms")):
        world.plivo.text(number, customer, "Hello", type_=type_)
        close(agent, take(stream, agent, **{"from": customer}), done, stream)
    world.wait(lambda: len(sent_lines(world, number, CUSTOMER)) == 2 and sent_lines(world, number, texter), what="sent")
    assert sent_lines(world, number, CUSTOMER) == [
        ("whatsapp", "Maya is on WhatsApp"),
        ("whatsapp", "Bye from WhatsApp"),
    ]
    assert sent_lines(world, number, texter) == [("sms", "Bye from SMS")]
    assert texts(agent.api(), chat) == ["Hi", "Maya joined the chat"]


def test_a_queues_lines_are_keyed_by_channel_and_a_single_set_is_refused(tenant):
    flat = {"name": "Flat", "text": {"messages": {"assigned": "{agent_name} has joined"}}}
    error(tenant.api.post("/api/v1/queues", json=flat), 422, "validation_failed")
    error(tenant.api.post("/api/v1/queues", json=flat | {"text": {"messages": {"email": {}}}}), 422)


def test_a_queue_sends_each_channels_waiting_line_once_when_a_text_queues(world, tenant):
    texter, stream = "+14155550903", tenant.stream(after=0)
    lines = {"sms": {"waiting": "An agent will text you soon"}, "whatsapp": {"waiting": "An agent will answer soon"}}
    lines["chat"] = {"waiting": "An agent will chat with you soon"}
    number, queue = text_line(tenant, unavailable(tenant), text={"messages": lines})
    for customer, type_ in ((CUSTOMER, "whatsapp"), (texter, "sms")):
        world.plivo.text(number, customer, "Hello", type_=type_)
    chat = ok(enqueue_text(tenant.api, queue, "chat", VISITOR, INBOX, text="Hi"))["conversation_id"]
    world.wait(lambda: sent_lines(world, number, CUSTOMER) and sent_lines(world, number, texter), what="waiting lines")
    world.plivo.text(number, texter, "Still there?")
    stream.wait_for("message.received", text="Still there?")
    world.run_jobs("plivo")
    assert sent_lines(world, number, CUSTOMER) == [("whatsapp", "An agent will answer soon")]
    assert sent_lines(world, number, texter) == [("sms", "An agent will text you soon")]
    world.wait(lambda: len(texts(tenant.api, chat)) == 2, what="the chat's waiting line")
    assert texts(tenant.api, chat) == ["Hi", "An agent will chat with you soon"]


def test_a_call_to_a_queue_with_text_lines_gets_no_text(world, tenant):
    number, _ = text_line(tenant, unavailable(tenant), text={"messages": {"sms": {"waiting": "Hold tight"}}})
    stream = tenant.stream(after=0)
    caller = world.plivo.call(number)
    stream.wait_for("conversation.queued")
    world.run_jobs("plivo")
    assert not plivo_sent(world, number, caller.from_)


def test_a_text_moved_to_another_queue_gets_that_queues_waiting_line(world, tenant):
    first, stream = tenant.agent("First").login(), tenant.stream(after=0)
    waiting = {"messages": {"sms": {"waiting": "Billing will text you soon"}}}
    billing = tenant.queue(name="Billing", agents=[unavailable(tenant).id], text=waiting)
    number, _ = text_line(tenant, first)
    world.plivo.text(number, CUSTOMER, "Hi")
    cid = take(stream, first)
    moved = {"target": {"type": "queue", "queue_id": billing["id"]}}
    ok(first.api().post(f"/api/v1/conversations/{cid}/transfers", json=moved))
    world.wait(lambda: sent_lines(world, number, CUSTOMER), what="the waiting line")
    assert sent_lines(world, number, CUSTOMER) == [("sms", "Billing will text you soon")]


def test_an_agent_email_carries_bcc_more_to_recipients_and_a_reply_to(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    mail = {"subject": "Refund", "cc": ["boss@customer.example"], "bcc": ["audit@acme.example"]}
    mail |= {"to": ["ana.work@customer.example"], "reply_to": ["billing@acme.example"]}
    r = start(tenant.api, agent, "email", email_sender(tenant, agent), "ana@customer.example", text="Done", email=mail)
    sent = stream.wait_for("message.sent", conversation_id=ok(r)["conversation_id"])["data"]["email"]
    assert {k: sent[k] for k in mail} == mail


def test_a_callers_sms_reply_after_the_call_reaches_the_agent_who_texted_them(world, tenant):
    live = connect(tenant)
    body = {"channel": "sms", "text": "Here is the tracking link"}
    ok(live.agent.api().post(f"/api/v1/conversations/{live.id}/messages", json=body))
    finish(live)
    world.plivo.text(live.number, live.caller.from_, "Got it, thanks")
    cid = live.stream.wait_for("message.received", text="Got it, thanks")["conversation_id"]
    conv = ok(tenant.api.get(f"/api/v1/conversations/{cid}"))
    assert (conv["channel"], conv["state"], conv["agent_ids"]) == ("sms", "active", [live.agent.id])
    assert not replayed(tenant, "conversation.queued", conversation_id=cid)


def test_an_idle_sms_closes_with_the_auto_closed_disposition(world, tenant):
    settings(tenant, text={"session_timeout_sec": {"sms": 600}}, wrap_up={"mode": "auto", "seconds": 1})
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, agent)
    world.plivo.text(number, CUSTOMER, "Hi")
    cid = take(stream, agent)
    world.advance(601)
    closed = run_jobs_until(world, lambda: seen(stream, "conversation.closed", conversation_id=cid), "closed")[0]
    assert closed["data"]["result"] == "session_timeout"
    auto = disposition_named(tenant, "Auto-closed")["id"]
    assert [d["disposition_id"] for d in closed["data"]["dispositions"]] == [auto]


def test_an_undelivered_report_reads_undelivered_with_plivos_code(world, tenant):
    agent = tenant.agent().login()
    unreachable = "+14155559901"
    cid, _, stream = live_text(world, tenant, agent, customer=unreachable)
    world.plivo.message_outcome(unreachable, "undelivered", "30")
    sent = reply(agent, cid, "Your code is 1234")
    changed = stream.wait_for("message.status_changed", message_id=sent["id"], status="undelivered")["data"]
    assert changed["error_code"] == "30"
    [mine] = [m for m in messages(agent.api(), cid) if m["id"] == sent["id"]]
    assert mine["status"] == "undelivered"


def test_a_message_and_its_status_events_carry_plivos_message_id(world, tenant):
    agent = tenant.agent().login()
    cid, number, stream = live_text(world, tenant, agent)
    sent = reply(agent, cid, "On its way")
    [out] = world.wait(lambda: plivo_sent(world, number, CUSTOMER), what="the reply at Plivo")
    delivered = stream.wait_for("message.status_changed", message_id=sent["id"], status="delivered")["data"]
    assert delivered["provider_id"] == out.message_uuid
    [mine] = [m for m in messages(agent.api(), cid) if m["id"] == sent["id"]]
    assert mine["provider_id"] == out.message_uuid


def test_a_whatsapp_status_carries_the_time_plivo_reported_it(world, tenant):
    agent = tenant.agent().login()
    cid, number, stream = live_text(world, tenant, agent, "whatsapp")
    sent = reply(agent, cid, "Hello")
    stream.wait_for("message.status_changed", message_id=sent["id"], status="sent")
    at = (datetime.now(UTC) - timedelta(seconds=40)).replace(microsecond=0)
    report = {"MessageUUID": plivo_sent(world, number, CUSTOMER)[0].message_uuid, "Status": "read"}
    report |= {"From": number.removeprefix("+"), "To": CUSTOMER.removeprefix("+")}
    report["ReadReceiptTime"] = at.strftime("%Y-%m-%d %H:%M:%S")
    ok(plivo_callback(tenant, number, f"message-status/{sent['id']}", report))
    read = stream.wait_for("message.status_changed", message_id=sent["id"], status="read")["data"]
    assert datetime.fromisoformat(read["reported_at"]) == at


def test_a_whatsapp_button_tap_list_pick_or_button_reply_reaches_the_agent(world, tenant):
    agent = tenant.agent().login()
    cid, number, stream = live_text(world, tenant, agent, "whatsapp")
    ok(whatsapp_in(tenant, number, "button", Button={"Text": "Talk to sales", "Payload": "sales"}))
    stream.wait_for("message.received", conversation_id=cid, text="Talk to sales")
    pick = {"Type": "list_reply", "ListReply": {"Id": "r-2", "Title": "Refund", "Description": "Money back"}}
    ok(whatsapp_in(tenant, number, "interactive", Interactive=pick))
    tap = {"Type": "button_reply", "ButtonReply": {"Id": "b-yes", "Title": "Yes"}}
    ok(whatsapp_in(tenant, number, "interactive", Interactive=tap))
    stream.wait_for("message.received", conversation_id=cid, text="Yes")
    shown = [(m["text"], m.get("whatsapp")) for m in messages(agent.api(), cid)][1:]
    assert shown == [
        ("Talk to sales", {"type": "button"}),
        ("Refund", {"type": "list_reply", "id": "r-2", "title": "Refund", "description": "Money back"}),
        ("Yes", {"type": "button_reply", "id": "b-yes", "title": "Yes"}),
    ]


def test_a_whatsapp_location_reaches_the_agent(world, tenant):
    agent = tenant.agent().login()
    cid, number, stream = live_text(world, tenant, agent, "whatsapp")
    where = {"Latitude": "37.7749", "Longitude": "-122.4194", "Name": "Head office", "Address": "1 Main St"}
    ok(whatsapp_in(tenant, number, "location", Location=where))
    got = stream.wait_for("message.received", conversation_id=cid, channel="whatsapp", text=None)["data"]
    [m] = [m for m in messages(agent.api(), cid) if m["id"] == got["message_id"]]
    assert m["whatsapp"] == {"type": "location", "location": where}


def test_a_whatsapp_of_an_unknown_content_type_is_refused_and_opens_nothing(world, tenant):
    agent = tenant.agent().login()
    number, _ = text_line(tenant, agent)
    assert whatsapp_in(tenant, number, "reaction", Body="").status_code == 422
    assert not replayed(tenant, "conversation.started")


def test_a_customer_whatsapp_quoting_an_agent_message_names_it(world, tenant):
    agent = tenant.agent().login()
    cid, number, stream = live_text(world, tenant, agent, "whatsapp")
    sent = reply(agent, cid, "Which size?")
    [out] = world.wait(lambda: plivo_sent(world, number, CUSTOMER), what="the reply at Plivo")
    ok(whatsapp_in(tenant, number, "text", Body="Medium", Context={"MessageUUID": out.message_uuid}))
    got = stream.wait_for("message.received", conversation_id=cid, text="Medium")["data"]
    assert got["reply_to"] == sent["id"]
    assert [m["reply_to"] for m in messages(agent.api(), cid) if m["text"] == "Medium"] == [sent["id"]]


def test_an_agent_whatsapp_reply_quotes_a_customer_message(world, tenant):
    agent = tenant.agent().login()
    cid, _, stream = live_text(world, tenant, agent, "whatsapp", "Is it in stock?")
    [question] = messages(agent.api(), cid)
    sent = reply(agent, cid, "Yes, two left", reply_to=question["id"])
    assert sent["reply_to"] == question["id"]
    assert stream.wait_for("message.sent", message_id=sent["id"])["data"]["reply_to"] == question["id"]


def test_an_agent_sends_whatsapp_buttons_a_list_and_a_location(world, tenant):
    agent = tenant.agent().login()
    cid, number, _ = live_text(world, tenant, agent, "whatsapp")
    buttons = {"type": "button", "body": "Pick one", "buttons": [{"title": "Yes"}, {"title": "No"}]}
    rows = [{"title": "Refund", "description": "Money back"}]
    menu = {
        "type": "list",
        "body": "How can we help?",
        "button": "Options",
        "sections": [{"title": "Billing", "rows": rows}],
    }
    for interactive in (buttons, menu):
        ok(agent.api().post(f"/api/v1/conversations/{cid}/messages", json={"interactive": interactive}))
    where = {"latitude": 37.7749, "longitude": -122.4194}
    ok(agent.api().post(f"/api/v1/conversations/{cid}/messages", json={"location": where}))
    sent = world.wait(lambda: len(out := plivo_sent(world, number, CUSTOMER)) == 3 and out, what="three at Plivo")
    shown_buttons, shown_list, shown_location = (m.interactive or m.location for m in sent)
    assert (shown_buttons["type"], shown_buttons["body"]["text"]) == ("button", "Pick one")
    assert [b["title"] for b in shown_buttons["action"]["buttons"]] == ["Yes", "No"]
    assert all(b["id"] for b in shown_buttons["action"]["buttons"])
    assert [r["title"] for r in shown_list["action"]["sections"][0]["rows"]] == ["Refund"]
    assert shown_location == {"latitude": "37.774900", "longitude": "-122.419400"}


def test_the_tenants_whatsapp_templates_are_listed_by_name(world, tenant):
    settings(tenant, whatsapp={"accounts": [{"id": WABA, "senders": [registered_number(tenant)]}]})
    for name in ("order_update", "delivery_eta"):
        world.plivo.template(tenant.auth_id, WABA, name, "en_US")
    listed = ok(tenant.api.get("/api/v1/whatsapp/templates", params={"status": "approved"}))["data"]
    names = [t["name"] for t in listed if t["name"] in ("order_update", "delivery_eta")]
    assert names == ["delivery_eta", "order_update"]
    assert all(t["language"] == "en_US" for t in listed if t["name"] in names)


def test_the_template_list_shows_components_and_takes_a_sender_a_status_and_a_cursor(world, tenant):
    ours, theirs = registered_number(tenant), registered_number(tenant)
    accounts = [{"id": WABA, "senders": [ours]}, {"id": OTHER_WABA, "senders": [theirs]}]
    settings(tenant, whatsapp={"accounts": accounts})
    body = [{"type": "BODY", "text": "Hi {{1}}, order {{2}} shipped"}]
    shipped = world.plivo.template(tenant.auth_id, WABA, "shipped", components=body)
    world.plivo.template(tenant.auth_id, WABA, "spring_sale", status="PENDING")
    world.plivo.template(tenant.auth_id, OTHER_WABA, "other_brand")

    def listed(**params) -> dict:
        return ok(tenant.api.get("/api/v1/whatsapp/templates", params=params))

    [one] = listed(sender=ours, status="approved")["data"]
    assert one == {
        "id": shipped["template_id"],
        "whatsapp_account_id": WABA,
        "name": "shipped",
        "namespace": f"{WABA}-shipped-en_US",
        "language": "en_US",
        "category": "UTILITY",
        "status": "approved",
        "components": body,
    }
    assert [(t["name"], t["status"]) for t in listed(sender=ours)["data"]] == [
        ("shipped", "approved"),
        ("spring_sale", "pending"),
    ]
    first = listed(limit=2)
    assert [t["name"] for t in first["data"]] == ["other_brand", "shipped"]
    assert [t["name"] for t in listed(limit=2, cursor=first["next_cursor"])["data"]] == ["spring_sale"]
    error(tenant.api.get("/api/v1/whatsapp/templates", params={"sender": CUSTOMER}), 404, "sender_not_found")


def test_with_whatsapp_accounts_a_whatsapp_goes_only_from_one_of_their_senders(world, tenant):
    agent = tenant.agent().login()
    sender, plain = registered_number(tenant), registered_number(tenant)
    settings(tenant, whatsapp={"accounts": [{"id": WABA, "senders": [sender]}]})
    error(tenant.api.patch("/api/v1/settings", json={"default_from": {"whatsapp": plain}}), 422, "validation_failed")
    world.plivo.template(tenant.auth_id, WABA, "order_update")
    template = {"name": "order_update", "language": "en_US", "namespace": f"{WABA}-order_update-en_US"}
    error(start(tenant.api, agent, "whatsapp", plain, CUSTOMER, template=template), 422, "from_not_allowed")
    settings(tenant, default_from={"whatsapp": sender})
    ok(start(tenant.api, agent, "whatsapp", None, CUSTOMER, template=template))
    [out] = world.wait(lambda: plivo_sent(world, sender, CUSTOMER), what="the template at Plivo")
    assert (out.from_, out.template) == (sender, template)


def test_a_whatsapp_template_must_be_approved_in_the_senders_account_and_filled_as_it_reads(world, tenant):
    agent = tenant.agent().login()
    sender = registered_number(tenant)
    accounts = [{"id": WABA, "senders": [sender]}, {"id": OTHER_WABA, "senders": [registered_number(tenant)]}]
    settings(tenant, whatsapp={"accounts": accounts})
    parts = [{"type": "HEADER", "format": "IMAGE"}, {"type": "BODY", "text": "Hi {{1}}, order {{2}} shipped"}]
    parts.append({"type": "BUTTONS", "buttons": [{"type": "URL", "text": "Track", "url": "https://t.test/{{1}}"}]})
    world.plivo.template(tenant.auth_id, WABA, "shipped", components=parts)
    world.plivo.template(tenant.auth_id, WABA, "spring_sale", status="PENDING")
    world.plivo.template(tenant.auth_id, OTHER_WABA, "other_brand")
    image = {"type": "header", "parameters": [{"type": "media", "media": "https://m.test/box.png"}]}
    names = {"type": "body", "parameters": [{"type": "text", "text": "Ana"}, {"type": "text", "text": "A-1"}]}
    track = {"type": "button", "sub_type": "url", "index": "0", "parameters": [{"type": "text", "text": "A-1"}]}

    def template(name="shipped", language="en_US", components=(image, names, track)) -> dict:
        own = f"{WABA}-{name}-{language}"
        return {"name": name, "language": language, "namespace": own, "components": list(components)}

    for wrong in (
        template("spring_sale", components=[]),
        template("other_brand", components=[]),
        template(language="es_MX"),
        template(components=[names]),
        template(components=[{"type": "header", "parameters": [{"type": "text", "text": "Box"}]}, names]),
        template(components=[image, names | {"parameters": names["parameters"][:1]}]),
        template(components=[image, names | {"parameters": [{"type": "text", "text": ""}] * 2}]),
        {"language": "en_US"},
    ):
        error(start(tenant.api, agent, "whatsapp", sender, CUSTOMER, template=wrong), 422, "invalid_template")
    world.wait(world.plivo.idle, what="the simulator to go idle")
    assert plivo_sent(world, sender, CUSTOMER) == []
    cid = ok(start(tenant.api, agent, "whatsapp", sender, CUSTOMER, template=template()))["conversation_id"]
    [out] = world.wait(lambda: plivo_sent(world, sender, CUSTOMER), what="the template at Plivo")
    assert out.template == template()
    late = {"template": template("spring_sale", components=[])}
    error(agent.api().post(f"/api/v1/conversations/{cid}/messages", json=late), 422, "invalid_template")


def test_a_whatsapp_templates_namespace_names_the_senders_account_its_name_and_language(world, tenant):
    agent = tenant.agent().login()
    sender = registered_number(tenant)
    settings(tenant, whatsapp={"accounts": [{"id": WABA, "senders": [sender]}]})
    world.plivo.template(tenant.auth_id, WABA, "order_update")
    named = {"name": "order_update", "language": "en_US"}
    for wrong in (f"{OTHER_WABA}-order_update-en_US", f"{WABA}-order_update-es_MX", ""):
        mismatched = named | {"namespace": wrong}
        error(start(tenant.api, agent, "whatsapp", sender, CUSTOMER, template=mismatched), 422, "invalid_template")
    own = named | {"namespace": f"{WABA}-order_update-en_US"}
    ok(start(tenant.api, agent, "whatsapp", sender, CUSTOMER, template=own))


def test_a_whatsapp_template_needs_the_namespace_its_list_entry_names(world, tenant):
    agent = tenant.agent().login()
    sender = registered_number(tenant)
    settings(tenant, whatsapp={"accounts": [{"id": WABA, "senders": [sender]}]})
    world.plivo.template(tenant.auth_id, WABA, "order_update")
    named = {"name": "order_update", "language": "en_US"}
    for missing in (named, named | {"namespace": None}):
        error(start(tenant.api, agent, "whatsapp", sender, CUSTOMER, template=missing), 422, "invalid_template")
    [listed] = ok(tenant.api.get("/api/v1/whatsapp/templates"))["data"]
    ok(start(tenant.api, agent, "whatsapp", sender, CUSTOMER, template=named | {"namespace": listed["namespace"]}))


def test_one_whatsapp_account_failing_at_plivo_leaves_the_others_templates_listed(world, tenant):
    settings(tenant, whatsapp={"accounts": [{"id": OTHER_WABA, "senders": [registered_number(tenant)]}]})
    error(tenant.api.get("/api/v1/whatsapp/templates"), 422)
    accounts = [
        {"id": WABA, "senders": [registered_number(tenant)]},
        {"id": OTHER_WABA, "senders": [registered_number(tenant)]},
    ]
    settings(tenant, whatsapp={"accounts": accounts})
    world.plivo.template(tenant.auth_id, WABA, "order_update")
    assert [t["name"] for t in ok(tenant.api.get("/api/v1/whatsapp/templates"))["data"]] == ["order_update"]


def test_an_agent_without_direct_dial_is_refused_before_the_template_is_checked(world, tenant):
    agent = tenant.agent().login()
    sender = registered_number(tenant)
    settings(tenant, whatsapp={"accounts": [{"id": WABA, "senders": [sender]}]})
    world.plivo.template(tenant.auth_id, WABA, "order_update")
    wrong = {"name": "missing", "language": "en_US", "components": []}
    error(start(agent.api(), agent, "whatsapp", sender, CUSTOMER, template=wrong), 403, "dial_requires_server")


def test_an_email_start_needs_email_addresses(world, tenant):
    agent = tenant.agent().login()
    for from_, to in (
        ("billing@acme.example", "not-an-address"),
        ("billing", "ana@customer.example"),
        ("billing@acme.example", "ana@customer"),
    ):
        error(start(tenant.api, agent, "email", from_, to, text="Hello"), 422, "validation_failed")
    sender = email_sender(tenant, agent)
    error(start(tenant.api, agent, "email", sender, "ana@customer", text="Hello"), 422, "validation_failed")
    ok(start(tenant.api, agent, "email", sender, "ana@customer.example", text="Hello"))


def test_an_email_start_needs_a_registered_sender(world, tenant):
    agent = tenant.agent().login()
    unknown = start(tenant.api, agent, "email", "billing@acme.example", "ana@customer.example", text="Hi")
    error(unknown, 422, "from_not_allowed")
    ok(start(tenant.api, agent, "email", email_sender(tenant, agent), "ana@customer.example", text="Hi"))


def test_an_inbound_email_keeps_its_recipients_bcc_and_reply_to(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    queue = tenant.queue(agents=[agent.id])
    mail = {"subject": "Invoice", "message_id": "<i1@customer.example>", "to": ["billing@acme.example"]}
    mail |= {"bcc": ["audit@acme.example"], "reply_to": ["ana.work@customer.example"]}
    body = {"text": "Invoice wrong", "email": mail}
    cid = ok(enqueue_text(tenant.api, queue, "email", "ana@customer.example", "support@acme.example", **body))
    take(stream, agent, conversation_id=cid["conversation_id"])
    [m] = messages(agent.api(), cid["conversation_id"])
    assert {k: m["email"][k] for k in ("to", "bcc", "reply_to")} == {k: mail[k] for k in ("to", "bcc", "reply_to")}


def test_an_email_reply_threads_only_into_a_conversation_on_the_same_address(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    queue = tenant.queue(agents=[agent.id])
    settings(tenant, capacity={"email": 2})
    first = {"text": "Invoice wrong", "email": {"subject": "Invoice", "message_id": "<s1@customer.example>"}}
    support = ok(enqueue_text(tenant.api, queue, "email", "ana@customer.example", "support@acme.example", **first))
    take(stream, agent, conversation_id=support["conversation_id"])
    other = {"text": "Upgrade please", "email": {"subject": "Upgrade", "message_id": "<s2@customer.example>"}}
    sales = ok(enqueue_text(tenant.api, queue, "email", "ana@customer.example", "sales@acme.example", **other))
    take(stream, agent, conversation_id=sales["conversation_id"])
    answer = {"text": "Any news?", "email": {"subject": "Re: Upgrade", "in_reply_to": "<s1@customer.example>"}}
    landed = ok(enqueue_text(tenant.api, queue, "email", "ana@customer.example", "sales@acme.example", **answer))
    assert landed["conversation_id"] == sales["conversation_id"]


def test_an_agent_sms_to_any_number_is_sent_and_plivos_own_refusal_is_message_refused(world, tenant):
    sender, other = tenant.agent().login(), tenant.agent().login()
    number = registered_number(tenant)
    ok(start(tenant.api, sender, "sms", number, "+919812345678", text="Your order shipped"))
    world.wait(lambda: plivo_sent(world, number, "+919812345678"), what="the text at Plivo")
    world.plivo.reject_next("/Message", 400, account=tenant.auth_id)
    error(start(tenant.api, other, "sms", number, "+442071234567", text="Not deliverable"), 422, "message_refused")


def test_a_start_without_from_uses_the_tenants_default_number_for_the_channel(world, tenant):
    agent = tenant.agent().login()
    number = registered_number(tenant)
    settings(tenant, default_from={"sms": number})
    body = {"channel": "sms", "agent_id": agent.id, "to": CUSTOMER, "text": "Your order shipped"}
    ok(tenant.api.post("/api/v1/conversations", json=body))
    [out] = world.wait(lambda: plivo_sent(world, number, CUSTOMER), what="the text at Plivo")
    assert out.text == "Your order shipped"


def test_a_send_waits_for_a_slow_plivo_answer(world, tenant):
    agent = tenant.agent().login()
    cid, _, _ = live_text(world, tenant, agent)
    world.plivo.reject_next("/Message", 400, account=tenant.auth_id, after_sec=8)
    error(agent.api().post(f"/api/v1/conversations/{cid}/messages", json={"text": "Hi"}), 422, "message_refused")


def test_a_text_transfer_start_is_announced_with_its_target(world, tenant):
    t = team(tenant, "Billing")
    first = tenant.agent("First").login()
    tenant.agent("Target", teams=[t]).login()
    cid, _, stream = live_text(world, tenant, first)
    tid = ok(text_transfer(first, cid, {"type": "team", "team_id": t}))["transfer_id"]
    started = stream.wait_for("conversation.transferred", conversation_id=cid, status="initiated")
    assert (started["data"]["transfer_id"], started["agent_id"]) == (tid, first.id)
    assert started["data"]["target"] == {"type": "team", "team_id": t}
    timeline = ok(tenant.api.get(f"/api/v1/conversations/{cid}/timeline"))["data"]
    assert started["id"] in [e["id"] for e in timeline]


def test_a_cancelled_text_transfer_tells_the_agent_it_was_cancelled(world, tenant):
    first, target = tenant.agent("First").login(), tenant.agent("Target").login()
    cid, _, stream = live_text(world, tenant, first)
    tid = ok(text_transfer(first, cid, {"type": "agent", "agent_id": target.id}))["transfer_id"]
    offered(stream, target, conversation_id=cid)
    first.api().post(f"/api/v1/conversations/{cid}/transfers/{tid}/cancel")
    cancelled = stream.wait_for("conversation.transferred", conversation_id=cid, transfer_id=tid, status="cancelled")
    assert cancelled["agent_id"] == first.id


def test_a_completed_text_transfer_says_who_handed_it_to_whom(world, tenant):
    t = team(tenant, "Billing")
    first, target = tenant.agent("First").login(), tenant.agent("Target", teams=[t]).login()
    cid, _, stream = live_text(world, tenant, first)
    tid = ok(text_transfer(first, cid, {"type": "team", "team_id": t}))["transfer_id"]
    ok(accept(target, offered(stream, target, conversation_id=cid)))
    done = stream.wait_for("conversation.transferred", conversation_id=cid, transfer_id=tid, status="completed")["data"]
    assert (done["from_agent_id"], done["to_agent_id"]) == (first.id, target.id)
    assert done["target"]["type"] == "team"


def test_a_cold_text_accept_is_announced_before_it_completes(world, tenant):
    first, target = tenant.agent("First").login(), tenant.agent("Target").login()
    cid, _, stream = live_text(world, tenant, first)
    tid = ok(text_transfer(first, cid, {"type": "agent", "agent_id": target.id}))["transfer_id"]
    ok(accept(target, offered(stream, target, conversation_id=cid)))
    stream.wait_for("conversation.transferred", conversation_id=cid, transfer_id=tid, status="completed")
    timeline = ok(tenant.api.get(f"/api/v1/conversations/{cid}/timeline"))["data"]
    seen = [e["data"]["status"] for e in timeline if e["type"] == "conversation.transferred"]
    assert seen == ["initiated", "accepted", "completed"]


def test_a_warm_text_accept_is_announced_and_its_complete_is_not_an_accept(world, tenant):
    first, target = tenant.agent("First").login(), tenant.agent("Target").login()
    cid, _, stream = live_text(world, tenant, first)
    tid = ok(text_transfer(first, cid, {"type": "agent", "agent_id": target.id}, mode="warm"))["transfer_id"]
    ok(accept(target, offered(stream, target, conversation_id=cid)))
    took = stream.wait_for("conversation.transferred", conversation_id=cid, transfer_id=tid, status="accepted")
    assert (took["agent_id"], took["data"]["to_agent_id"]) == (first.id, target.id)
    ok(first.api().post(f"/api/v1/conversations/{cid}/transfers/{tid}/complete"))
    stream.wait_for("conversation.transferred", conversation_id=cid, transfer_id=tid, status="completed")
    timeline = ok(tenant.api.get(f"/api/v1/conversations/{cid}/timeline"))["data"]
    seen = [e["data"]["status"] for e in timeline if e["type"] == "conversation.transferred"]
    assert seen == ["initiated", "accepted", "completed"]


@pytest.mark.parametrize("who", ["primary", "target"])
def test_a_joined_warm_text_target_drops_out_and_the_primary_keeps_the_conversation(world, tenant, who):
    first, target = tenant.agent("First").login(), tenant.agent("Target").login()
    cid, _, stream = live_text(world, tenant, first)
    tid = ok(text_transfer(first, cid, {"type": "agent", "agent_id": target.id}, mode="warm"))["transfer_id"]
    ok(accept(target, offered(stream, target, conversation_id=cid)))
    stream.wait_for("conversation.transferred", conversation_id=cid, transfer_id=tid, status="accepted")
    ok((first if who == "primary" else target).api().post(f"/api/v1/conversations/{cid}/transfers/{tid}/cancel"))
    stream.wait_for("conversation.transferred", conversation_id=cid, transfer_id=tid, status="cancelled")
    reply(first, cid, "Still here")
    refused(target.api().post(f"/api/v1/conversations/{cid}/messages", json={"text": "Gone"}))


def test_only_the_texts_agent_changes_its_transfer_and_its_joined_warm_target_may_only_leave_it(world, tenant):
    t = team(tenant, "Floor")
    sup = tenant.agent("Sup", role="supervisor", teams=[t]).login()
    first, target = tenant.agent("First", teams=[t]).login(), tenant.agent("Target").login()
    cid, _, stream = live_text(world, tenant, first)
    tid = ok(text_transfer(first, cid, {"type": "agent", "agent_id": target.id}, mode="warm"))["transfer_id"]
    ok(accept(target, offered(stream, target, conversation_id=cid)))
    stream.wait_for("conversation.transferred", conversation_id=cid, transfer_id=tid, status="accepted")
    url = f"/api/v1/conversations/{cid}/transfers/{tid}"
    error(target.api().post(f"{url}/complete"), 403)
    for step in ("complete", "cancel"):
        error(sup.api().post(f"{url}/{step}"), (403, 404))
    ok(target.api().post(f"{url}/cancel"))
    stream.wait_for("conversation.transferred", conversation_id=cid, transfer_id=tid, status="cancelled")


def test_a_second_text_transfer_while_the_first_still_rings_offers_its_target_too(world, tenant):
    first = tenant.agent("First").login()
    one, two = tenant.agent("One").login(), tenant.agent("Two").login()
    cid, _, stream = live_text(world, tenant, first)
    ok(text_transfer(first, cid, {"type": "agent", "agent_id": one.id}))
    offered(stream, one, conversation_id=cid)
    ok(text_transfer(first, cid, {"type": "agent", "agent_id": two.id}))
    offered(stream, two, conversation_id=cid)


def call_sms(live, agent=None, text: str | None = "Here is the tracking link", **body):
    """`agent` (default: the call's) texts the caller by SMS from the call."""
    body = {"channel": "sms", "text": text, **body}
    return (agent or live.agent).api().post(f"/api/v1/conversations/{live.id}/messages", json=body)


def test_a_callers_text_after_their_sms_thread_closed_queues_as_a_new_contact(world, tenant):
    live = connect(tenant)
    ok(call_sms(live))
    finish(live)
    world.plivo.text(live.number, live.caller.from_, "Got it, thanks")
    thread = live.stream.wait_for("message.received", text="Got it, thanks")["conversation_id"]
    close(live.agent, thread, disposition(tenant)["id"], live.stream)
    world.plivo.text(live.number, live.caller.from_, "One more question")
    again = live.stream.wait_for("message.received", text="One more question")["conversation_id"]
    assert again != thread
    assert replayed(tenant, "conversation.queued", conversation_id=again)


def test_a_second_agents_sms_to_a_caller_whose_sms_thread_is_open_is_refused(world, tenant):
    target = tenant.agent("Target").login()
    live = connect(tenant)
    ok(call_sms(live))
    cold = {"mode": "cold", "target": {"type": "agent", "agent_id": target.id}}
    ok(live.agent.api().post(f"/api/v1/calls/{live.id}/transfers", json=cold))
    target.phone.wait_ring().wait_state("in_progress")
    live.stream.wait_for("call.transferred", conversation_id=live.id, status="completed")
    error(call_sms(live, target, "Me too"), 409, "conversation_open")


def test_an_agents_texts_from_a_call_and_the_callers_answers_share_one_conversation(world, tenant):
    live = connect(tenant)
    first = ok(call_sms(live))
    assert first["conversation_id"] != live.id
    world.plivo.text(live.number, live.caller.from_, "Got it, thanks")
    live.stream.wait_for("message.received", text="Got it, thanks", conversation_id=first["conversation_id"])
    assert ok(call_sms(live, text="And the invoice"))["conversation_id"] == first["conversation_id"]
    said = ["Here is the tracking link", "Got it, thanks", "And the invoice"]
    assert texts(live.agent.api(), first["conversation_id"]) == said


def test_a_callers_text_after_their_thread_closed_during_the_call_queues_as_a_new_contact(world, tenant):
    live = connect(tenant)
    ok(call_sms(live))
    world.plivo.text(live.number, live.caller.from_, "Got it, thanks")
    thread = live.stream.wait_for("message.received", text="Got it, thanks")["conversation_id"]
    ok(call_sms(live, text="And the invoice"))
    close(live.agent, thread, disposition(tenant)["id"], live.stream)
    world.plivo.text(live.number, live.caller.from_, "One more question")
    again = live.stream.wait_for("message.received", text="One more question")["conversation_id"]
    assert again != thread
    assert replayed(tenant, "conversation.queued", conversation_id=again)


def test_an_agent_whose_one_sms_slot_is_taken_is_refused_a_text_to_a_second_caller_from_a_call(world, tenant):
    agent = tenant.agent().login()
    first = connect(tenant, agent)
    ok(call_sms(first))
    finish(first)
    second = connect(tenant, agent)
    error(call_sms(second), 409, "agent_at_capacity")
    assert not plivo_sent(world, second.number, second.caller.from_)


def test_a_text_from_a_call_closes_at_its_idle_time_only_after_a_second_while_the_call_is_on(world, tenant):
    settings(tenant, text={"session_timeout_sec": {"sms": 600}})
    live = connect(tenant)
    ok(call_sms(live))
    world.plivo.text(live.number, live.caller.from_, "Got it, thanks")
    thread = live.stream.wait_for("message.received", text="Got it, thanks")["conversation_id"]
    world.advance(601)
    assert ok(tenant.api.get(f"/api/v1/conversations/{thread}"))["state"] == "active"
    world.advance(601)
    closed = run_jobs_until(world, lambda: seen(live.stream, "conversation.closed", conversation_id=thread), "closed")
    assert closed[0]["data"]["result"] == "session_timeout"
    assert ok(tenant.api.get(f"/api/v1/conversations/{live.id}"))["state"] == "active"


def test_a_text_from_a_call_in_its_wrap_up_still_gets_a_second_idle_time(world, tenant):
    settings(tenant, text={"session_timeout_sec": {"sms": 600}})
    live = connect(tenant)
    ok(call_sms(live))
    world.plivo.text(live.number, live.caller.from_, "Got it, thanks")
    thread = live.stream.wait_for("message.received", text="Got it, thanks")["conversation_id"]
    live.caller.hangup()
    live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id)
    world.advance(601)
    assert ok(tenant.api.get(f"/api/v1/conversations/{thread}"))["state"] == "active"


def test_a_text_from_a_call_that_closed_closes_at_its_first_idle_time(world, tenant):
    settings(tenant, text={"session_timeout_sec": {"sms": 600}})
    live = connect(tenant)
    ok(call_sms(live))
    finish(live)
    world.plivo.text(live.number, live.caller.from_, "Got it, thanks")
    thread = live.stream.wait_for("message.received", text="Got it, thanks")["conversation_id"]
    world.advance(601)
    closed = run_jobs_until(world, lambda: seen(live.stream, "conversation.closed", conversation_id=thread), "closed")
    assert closed[0]["data"]["result"] == "session_timeout"


def test_a_file_from_a_call_in_wrap_up_opens_no_conversation(world, tenant):
    live = connect(tenant)
    live.caller.hangup()
    live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id)
    assert ok(call_sms(live, text=None, attachments=LABEL))["conversation_id"] == live.id
    world.plivo.text(live.number, live.caller.from_, "Got it, thanks")
    cid = live.stream.wait_for("message.received", text="Got it, thanks")["conversation_id"]
    assert replayed(tenant, "conversation.queued", conversation_id=cid)


def test_a_message_between_a_text_from_a_calls_two_idle_times_leaves_its_close_at_the_second(world, tenant):
    settings(tenant, text={"session_timeout_sec": {"sms": 600}})
    live = connect(tenant)
    ok(call_sms(live))
    world.plivo.text(live.number, live.caller.from_, "Got it, thanks")
    thread = live.stream.wait_for("message.received", text="Got it, thanks")["conversation_id"]
    world.advance(601)
    world.plivo.text(live.number, live.caller.from_, "Still there?")
    live.stream.wait_for("message.received", text="Still there?", conversation_id=thread)
    world.advance(601)
    assert live.stream.wait_for("conversation.closed", conversation_id=thread)["data"]["result"] == "session_timeout"


def test_a_message_before_a_text_from_a_calls_session_timeout_leaves_its_close_at_twice_the_timeout(world, tenant):
    settings(tenant, text={"session_timeout_sec": {"sms": 600}})
    live = connect(tenant)
    thread = ok(call_sms(live))["conversation_id"]
    world.wait(world.plivo.idle, what="the call settled")
    world.travel(500)
    world.plivo.text(live.number, live.caller.from_, "Still there?")
    live.stream.wait_for("message.received", text="Still there?", conversation_id=thread)
    world.advance(101)
    assert ok(tenant.api.get(f"/api/v1/conversations/{thread}"))["state"] == "active"
    world.advance(601)
    assert live.stream.wait_for("conversation.closed", conversation_id=thread)["data"]["result"] == "session_timeout"


def test_a_second_agents_accept_in_a_text_from_a_calls_extension_leaves_its_close_there(world, tenant):
    settings(tenant, text={"session_timeout_sec": {"sms": 600}})
    live = connect(tenant)
    helper = tenant.agent("Helper").login()
    thread = ok(call_sms(live))["conversation_id"]
    world.advance(601)
    assert ok(tenant.api.get(f"/api/v1/conversations/{thread}"))["state"] == "active"
    body = {"target": {"type": "agent", "agent_id": helper.id}}
    ok(live.agent.api().post(f"/api/v1/conversations/{thread}/participants", json=body))
    ok(accept(helper, offered(live.stream, helper, conversation_id=thread)))
    world.advance(601)
    assert live.stream.wait_for("conversation.closed", conversation_id=thread)["data"]["result"] == "session_timeout"


def test_an_sms_from_a_call_is_refused_while_its_agent_holds_the_callers_own_sms_conversation(world, tenant):
    live = connect(tenant)
    world.plivo.text(live.number, live.caller.from_, "A separate question")
    inbound = take(live.stream, live.agent, channel="sms")
    error(call_sms(live), 409, "conversation_open")
    assert texts(live.agent.api(), inbound) == ["A separate question"]
    assert not plivo_sent(world, live.number, live.caller.from_)


def test_an_mms_from_a_call_in_wrap_up_stays_on_it_beside_the_callers_own_sms_conversation(world, tenant):
    live = connect(tenant)
    world.plivo.text(live.number, live.caller.from_, "A separate question")
    inbound = take(live.stream, live.agent, channel="sms")
    live.caller.hangup()
    live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id)
    assert ok(call_sms(live, text=None, attachments=LABEL))["conversation_id"] == live.id
    assert texts(live.agent.api(), inbound) == ["A separate question"]


def test_a_later_mms_from_a_call_outside_the_us_and_canada_is_sent(world, tenant):
    agent, caller = tenant.agent().login(), "+442071234567"
    number, queue = tenant.number("44207"), tenant.queue(agents=[agent.id])
    tenant.route_number(number, {"type": "queue", "queue_id": queue["id"]})
    stream = tenant.stream(after=0)
    agent.phone.on_ring("answer")
    world.plivo.call(number, caller)
    cid = hs_header(agent.phone.wait_ring(), "HsConversation")
    stream.wait_for("offer.accepted", conversation_id=cid)
    path = f"/api/v1/conversations/{cid}/messages"
    ok(agent.api().post(path, json={"channel": "sms", "text": "Here is the tracking link"}))
    label = {"channel": "sms", "attachments": [{"url": "https://files.example/label.png"}]}
    ok(agent.api().post(path, json=label))
    world.wait(lambda: len(plivo_sent(world, number, caller)) == 2, what="both messages at Plivo")


def test_an_incomplete_whatsapp_interactive_is_refused(world, tenant):
    agent = tenant.agent().login()
    cid, number, _ = live_text(world, tenant, agent, "whatsapp")
    section = {"rows": [{"title": "Refund"}]}
    image = {"url": "https://cdn.acme.example/menu.png"}
    for interactive in (
        {"type": "button", "body": "Pick one"},
        {"type": "list", "body": "Menu", "sections": [section]},
        {"type": "list", "body": "Menu", "button": "Options"},
        {"type": "list", "body": "Menu", "button": "Options", "sections": [section], "header": image},
    ):
        r = agent.api().post(f"/api/v1/conversations/{cid}/messages", json={"interactive": interactive})
        error(r, 422, "validation_failed")
    world.wait(world.plivo.idle, what="the simulator to go idle")
    assert plivo_sent(world, number, CUSTOMER) == []


def test_an_agent_sends_whatsapp_reply_buttons_under_a_media_header(world, tenant):
    agent = tenant.agent().login()
    cid, number, _ = live_text(world, tenant, agent, "whatsapp")
    image = "https://cdn.acme.example/menu.png"
    buttons = {"type": "button", "body": "Pick one", "header": {"url": image}, "buttons": [{"title": "Yes"}]}
    ok(agent.api().post(f"/api/v1/conversations/{cid}/messages", json={"interactive": buttons}))
    [out] = world.wait(lambda: plivo_sent(world, number, CUSTOMER), what="the buttons at Plivo")
    assert out.interactive["header"] == {"type": "media", "media": image}


def test_an_email_start_takes_display_name_addresses(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    sender = email_sender(tenant, agent)
    r = start(tenant.api, agent, "email", f"Billing <{sender}>", "Ana Diaz <ana@customer.example>", text="Hi")
    sent = stream.wait_for("message.sent", conversation_id=ok(r)["conversation_id"])["data"]
    assert (sent["from"], sent["to"]) == (sender, "ana@customer.example")


def test_an_inbound_email_is_taken_whatever_its_addresses_look_like(world, tenant):
    agent = tenant.agent().login()
    queue = tenant.queue(agents=[agent.id])
    body = {"text": "Hello", "email": {"subject": "Hi"}}
    ok(enqueue_text(tenant.api, queue, "email", "Ana Diaz <ana@customer.example>", "support", **body))


def test_a_whatsapp_status_in_epoch_milliseconds_carries_that_time(world, tenant):
    agent = tenant.agent().login()
    cid, number, stream = live_text(world, tenant, agent, "whatsapp")
    sent = reply(agent, cid, "Hello")
    stream.wait_for("message.status_changed", message_id=sent["id"], status="delivered")
    at = (datetime.now(UTC) - timedelta(seconds=40)).replace(microsecond=0)
    report = {"MessageUUID": plivo_sent(world, number, CUSTOMER)[0].message_uuid, "Status": "read"}
    report |= {"From": number.removeprefix("+"), "To": CUSTOMER.removeprefix("+")}
    report["ReadReceiptTime"] = str(int(at.timestamp() * 1000))
    ok(plivo_callback(tenant, number, f"message-status/{sent['id']}", report))
    read = stream.wait_for("message.status_changed", message_id=sent["id"], status="read")["data"]
    assert datetime.fromisoformat(read["reported_at"]) == at


def test_plivos_queued_and_whatsapps_deleted_reports_are_kept_and_told(world, tenant):
    agent = tenant.agent().login()
    cid, number, stream = live_text(world, tenant, agent, "whatsapp")
    sent = reply(agent, cid, "Hello")
    stream.wait_for("message.status_changed", message_id=sent["id"], status="queued")
    stream.wait_for("message.status_changed", message_id=sent["id"], status="delivered")
    report = {"MessageUUID": plivo_sent(world, number, CUSTOMER)[0].message_uuid, "Status": "deleted"}
    report |= {"From": number.removeprefix("+"), "To": CUSTOMER.removeprefix("+")}
    ok(plivo_callback(tenant, number, f"message-status/{sent['id']}", report))
    stream.wait_for("message.status_changed", message_id=sent["id"], status="deleted")
    assert [m["status"] for m in messages(agent.api(), cid) if m["id"] == sent["id"]] == ["deleted"]


def test_an_mms_caption_is_its_body(world, tenant):
    agent = tenant.agent().login()
    cid, number, stream = live_text(world, tenant, agent)
    mms = {"From": CUSTOMER.removeprefix("+"), "To": number.removeprefix("+"), "Type": "mms"}
    mms |= {"MessageUUID": str(uuid.uuid4()), "Text": "Sent you a picture", "Body": "My receipt"}
    mms |= {"MediaCount": "1", "Media0": "https://media.plivo.example/receipt.jpg"}
    ok(plivo_callback(tenant, number, "message", mms))
    stream.wait_for("message.received", conversation_id=cid, text="My receipt")


def test_a_late_gateway_message_keeps_the_time_the_customer_sent_it(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    cid = ok(enqueue_text(tenant.api, tenant.queue(agents=[agent.id]), "chat", VISITOR, INBOX, text="Hi"))
    take(stream, agent, conversation_id=cid["conversation_id"])
    earlier = (datetime.now(UTC) - timedelta(minutes=1)).replace(microsecond=0)
    late = {"channel": "chat", "from": VISITOR, "to": INBOX, "text": "Are you there?", "sent_at": earlier.isoformat()}
    ok(tenant.api.post("/api/v1/messages", json=late))
    shown = [(m["text"], m["created_at"]) for m in messages(agent.api(), cid["conversation_id"])]
    assert shown[0][0] == "Are you there?" and datetime.fromisoformat(shown[0][1]) == earlier


def test_a_tenant_keys_reply_is_the_primary_agents_and_needs_one(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    cid = ok(enqueue_text(tenant.api, tenant.queue(agents=[agent.id]), "chat", VISITOR, INBOX, text="Hi"))
    url = f"/api/v1/conversations/{cid['conversation_id']}/messages"
    error(tenant.api.post(url, json={"text": "Hold on"}), 409, "not_live")
    take(stream, agent, conversation_id=cid["conversation_id"])
    sent = ok(tenant.api.post(url, json={"text": "Hello from us"}))
    assert sent["agent_id"] == agent.id
    assert stream.wait_for("message.sent", message_id=sent["id"])["data"]["by_agent_id"] == agent.id


def test_message_text_is_trimmed_and_blank_text_is_refused(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    cid = ok(enqueue_text(tenant.api, tenant.queue(agents=[agent.id]), "chat", VISITOR, INBOX, text=" hello \n"))
    take(stream, agent, conversation_id=cid["conversation_id"])
    blank = agent.api().post(f"/api/v1/conversations/{cid['conversation_id']}/messages", json={"text": " \n "})
    error(blank, 422, "empty_message")
    assert texts(agent.api(), cid["conversation_id"]) == ["hello"]


def test_a_send_plivo_refuses_is_never_told_as_sent(world, tenant):
    agent = tenant.agent().login()
    cid, _, stream = live_text(world, tenant, agent)
    world.plivo.reject_next("/Message", 400, account=tenant.auth_id)
    error(agent.api().post(f"/api/v1/conversations/{cid}/messages", json={"text": "Hi"}), 422, "message_refused")
    stream.wait_for("message.status_changed", conversation_id=cid, status="failed")
    assert not replayed(tenant, "message.sent", conversation_id=cid)


def test_one_gateway_message_id_at_two_of_our_addresses_reaches_both(world, tenant):
    queue = tenant.queue(agents=[tenant.agent().login().id])
    body = {"text": "Hi", "provider_id": "gateway-msg-1"}
    one = ok(enqueue_text(tenant.api, queue, "chat", VISITOR, "sales-inbox", **body))["conversation_id"]
    two = ok(enqueue_text(tenant.api, queue, "chat", VISITOR, INBOX, **body))["conversation_id"]
    assert one != two and texts(tenant.api, two) == ["Hi"]


def test_a_gateway_failure_report_tells_its_time_title_and_details(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    cid = ok(enqueue_text(tenant.api, tenant.queue(agents=[agent.id]), "chat", VISITOR, INBOX, text="Hi"))
    take(stream, agent, conversation_id=cid["conversation_id"])
    sent = reply(agent, cid["conversation_id"], "Hello")
    at = (datetime.now(UTC) - timedelta(seconds=20)).replace(microsecond=0)
    report = {"status": "failed", "error_code": "131026", "error_title": "Message undeliverable"}
    report |= {"error_details": "The receiver cannot take this message", "reported_at": at.isoformat()}
    ok(tenant.api.post(f"/api/v1/messages/{sent['id']}/status", json=report))
    told = stream.wait_for("message.status_changed", message_id=sent["id"], status="failed")["data"]
    assert (told["error_title"], told["error_details"]) == (report["error_title"], report["error_details"])
    assert datetime.fromisoformat(told["reported_at"]) == at


def test_an_idle_sms_closes_at_once_with_no_wrap_up(world, tenant):
    settings(tenant, text={"session_timeout_sec": {"sms": 600}}, wrap_up={"mode": "manual", "seconds": 60})
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, agent)
    world.plivo.text(number, CUSTOMER, "Hi")
    cid = take(stream, agent)
    world.advance(601)
    run_jobs_until(world, lambda: seen(stream, "conversation.closed", conversation_id=cid), "closed")
    assert not replayed(tenant, "conversation.wrap_up_started", conversation_id=cid)


def test_an_accepted_text_gets_a_full_idle_period_from_its_acceptance(world, tenant):
    settings(tenant, text={"session_timeout_sec": {"sms": 600}})
    agent = unavailable(tenant)
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, agent)
    world.plivo.text(number, CUSTOMER, "Hi")
    stream.wait_for("conversation.queued")
    world.travel(500)
    agent.set_status("Available")
    cid = take(stream, agent)
    world.advance(300)
    assert ok(tenant.api.get(f"/api/v1/conversations/{cid}"))["state"] == "active"


def test_a_message_either_way_leaves_a_texts_session_timeout_where_its_accept_set_it(world, tenant):
    settings(tenant, text={"session_timeout_sec": {"sms": 600}})
    agent = tenant.agent().login()
    cid, number, stream = live_text(world, tenant, agent)
    world.travel(500)
    world.plivo.text(number, CUSTOMER, "Still there?")
    stream.wait_for("message.received", text="Still there?", conversation_id=cid)
    reply(agent, cid, "Yes, one moment")
    world.advance(101)
    assert stream.wait_for("conversation.closed", conversation_id=cid)["data"]["result"] == "session_timeout"


def test_a_text_transfers_accept_gives_its_new_agent_a_full_session_timeout(world, tenant):
    settings(tenant, text={"session_timeout_sec": {"sms": 600}})
    first, target = tenant.agent("First").login(), tenant.agent("Target").login()
    cid, _, stream = live_text(world, tenant, first)
    world.travel(500)
    ok(text_transfer(first, cid, {"type": "agent", "agent_id": target.id}))
    ok(accept(target, offered(stream, target, conversation_id=cid)))
    world.advance(300)
    assert ok(tenant.api.get(f"/api/v1/conversations/{cid}"))["state"] == "active"


def test_a_text_transferred_to_a_queue_still_reaches_an_agent_after_its_session_timeout(world, tenant):
    settings(tenant, text={"session_timeout_sec": {"sms": 600}})
    first, away = tenant.agent("First").login(), unavailable(tenant)
    billing = tenant.queue(name="Billing", agents=[away.id])
    cid, _, stream = live_text(world, tenant, first)
    ok(text_transfer(first, cid, {"type": "queue", "queue_id": billing["id"]}))
    stream.wait_for("conversation.queued", conversation_id=cid, queue_id=billing["id"])
    world.advance(601)
    away.set_status("Available")
    take(stream, away, conversation_id=cid)


def test_a_transcript_plivo_serves_slowly_is_fetched_on_the_first_try(world, tenant):
    settings(tenant, recording={"inbound": True})
    live = connect(tenant)
    held = world.plivo.fault(
        "delay",
        lambda r: "/transcription" in r.path and r.params.get("call_uuid") == live.leg.call_uuid,
        account=tenant.auth_id,
    )
    live.caller.hangup()
    rid = live.stream.wait_for("recording.ready", conversation_id=live.id)["data"]["recording_id"]
    track = world.wait(lambda: held.hits, what="the agent track's transcription held back")[0].params["recording_id"]
    slow = {"account": tenant.auth_id, "times": math.inf, "after_sec": 6}
    fetches = world.plivo.fault("slow", lambda r: f"/Transcription/{track}" in r.path, **slow)
    again = {"recording_id": track, "call_uuid": live.leg.call_uuid, "transcription": ""}
    ok(plivo_post(world, tenant.id, f"transcription?c={live.id}", again, tenant.auth_token))
    got = run_jobs_until(world, lambda: (t := transcript(tenant.api, rid)) and t["complete"] and t, "both texts", 20)
    assert len(got["segments"]) == 2 and len(fetches.hits) == 1


def test_a_customer_file_plivo_serves_slowly_still_reaches_the_agent(world, tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, agent)
    world.plivo.text(number, CUSTOMER, "Receipt", type_="mms", media=(b"receipt-bytes",))
    [m] = messages(agent.api(), take(stream, agent))
    world.plivo.fault("slow", lambda r: "/Media/" in r.path, account=tenant.auth_id, after_sec=6)
    assert world.anon().get(m["attachments"][0]["url"]).content == b"receipt-bytes"
