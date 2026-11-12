"""DTMF menus (options, re-prompts, timeouts, nesting) and the two redirect targets for a tenant's own app."""

import json
import threading
import uuid
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from urllib.parse import urlsplit

from tests.acceptance.conftest import (
    answer_params,
    app_call,
    enqueue_as_plivo,
    error,
    fence,
    hangup_action,
    hs_header,
    in_order,
    lead,
    menu,
    menu_line,
    ok,
    queue_action,
    redirecting_line,
    registered_number,
    seen,
    unavailable,
    voicemail_action,
    with_query,
)

RACES = 5


def test_each_of_three_options_runs_its_action(world, tenant):
    number, m = menu_line(tenant, {d: hangup_action(f"Option {d}") for d in "123"})
    stream = tenant.stream(after=0)
    for digit in "123":
        caller = world.plivo.call(number, digits=digit)
        caller.wait_state("completed")
        assert in_order(caller.said, "Press a key", f"Option {digit}"), digit
    picks = world.wait(
        lambda: (
            len(seen(stream, "conversation.menu_input", menu_id=m["id"])) == 3
            and seen(stream, "conversation.menu_input")
        )
    )
    assert [(e["data"]["input"], e["data"]["attempt"]) for e in picks] == [("1", 1), ("2", 1), ("3", 1)]
    assert all(e["data"]["action"] for e in picks)


def test_no_input_reprompts_three_times_then_takes_the_invalid_action(world, tenant):
    number, _ = menu_line(tenant, {"1": hangup_action("One")}, retries=3, invalid_action=hangup_action("No choice"))
    stream = tenant.stream(after=0)
    caller = world.plivo.call(number, digits="wwww")
    caller.wait_state("completed", timeout=15)
    assert caller.said.count("Press a key") == 4 and caller.said[-1] == "No choice"
    inputs = world.wait(
        lambda: len(seen(stream, "conversation.menu_input")) == 4 and seen(stream, "conversation.menu_input")
    )
    assert [(e["data"]["input"], e["data"]["attempt"]) for e in inputs] == [("no_input", n) for n in (1, 2, 3, 4)]


def test_an_invalid_digit_reprompts_then_routes_the_next_one(world, tenant):
    number, _ = menu_line(tenant, {"1": hangup_action("One")})
    stream = tenant.stream(after=0)
    caller = world.plivo.call(number, digits="91")
    caller.wait_state("completed")
    assert caller.said.count("Press a key") == 2 and caller.said[-1] == "One"
    inputs = world.wait(
        lambda: len(seen(stream, "conversation.menu_input")) == 2 and seen(stream, "conversation.menu_input")
    )
    assert [(e["data"]["input"], e["data"]["attempt"]) for e in inputs] == [("invalid", 1), ("1", 2)]


def test_a_replayed_answer_and_menu_digit_get_the_same_xml_and_one_event_each(world, tenant):
    number, _ = menu_line(tenant, {"1": hangup_action("One")})
    call = answer_params(number)

    def twice(url: str, params: dict) -> str:
        signed = world.plivo.sign(url, params, tenant.auth_token)
        first, again = [world.anon().post(url, data=params, headers=signed) for _ in range(2)]
        assert first.status_code == again.status_code == 200 and first.text == again.text
        return first.text

    answered = twice(f"{world.base_url}/plivo/{tenant.id}/answer", call)
    action = ET.fromstring(answered).find("GetDigits").get("action").partition("#")[0]  # noqa: S314 (HS's own reply)
    assert "One" in twice(action, {**call, "Digits": "1"})
    fence(tenant)
    assert len(tenant.events(type="conversation.started")) == len(tenant.events(type="conversation.menu_input")) == 1


def test_a_menu_option_can_open_a_nested_menu(world, tenant):
    billing = menu(tenant, {"2": hangup_action("Billing desk")}, greeting="Billing menu")
    number, _ = menu_line(tenant, {"1": {"type": "menu", "menu_id": billing["id"]}})
    caller = world.plivo.call(number, digits="12")
    caller.wait_state("completed")
    assert in_order(caller.said, "Press a key", "Billing menu", "Billing desk")


def test_a_menu_option_can_route_to_a_queue(world, tenant):
    agent = tenant.agent().login()
    q = tenant.queue(name="Sales", agents=[agent.id])
    number, _ = menu_line(tenant, {"1": queue_action(q)})
    stream = tenant.stream(after=0)
    world.plivo.call(number, digits="1")
    agent.phone.wait_ring()
    stream.wait_for("conversation.queued")
    assert in_order(
        [e["type"] for e in stream.events], "conversation.started", "conversation.menu_input", "conversation.queued"
    )


def test_a_menu_holds_at_most_nine_options(tenant):
    error(
        tenant.api.post(
            "/api/v1/menus",
            json={"name": "Big", "greeting": {"text": "Hi"}, "options": {str(d): hangup_action() for d in range(10)}},
        ),
        422,
    )


def test_a_redirect_hands_the_call_to_the_tenant_app_with_the_conversation_id(world, tenant):
    app = world.plivo.app("<Response><Hangup/></Response>")
    number = tenant.number()
    tenant.route_number(number, {"type": "redirect", "url": app})
    stream = tenant.stream(after=0)
    world.plivo.call(number).wait_state("completed")
    redirected = stream.wait_for("conversation.redirected")
    assert redirected["data"]["url"].startswith(app)
    assert world.plivo.requests_to(urlsplit(app).path)[-1].params["conversation_id"] == redirected["conversation_id"]


def test_the_tenant_app_can_redirect_into_a_menu_url(world, tenant):
    m = menu(tenant, {"1": hangup_action("Thanks")}, greeting="Owner menu")
    assert m["menu_url"].startswith(f"{world.base_url}/plivo/{tenant.id}/actions/menu")
    caller = world.plivo.call(redirecting_line(tenant, m["menu_url"]), digits="1")
    caller.wait_state("completed")
    assert in_order(caller.said, "Owner menu", "Thanks")


def test_the_tenant_app_can_redirect_into_an_enqueue_url_with_agents_and_context(world, tenant):
    owner, teammate = tenant.agent("Owner").login(), tenant.agent("Teammate").login()
    q = tenant.queue(name="Team", agents=[owner.id, teammate.id])
    ctx = {"lead_id": "L-42"}
    stream = tenant.stream(after=0)
    world.plivo.call(redirecting_line(tenant, with_query(q["enqueue_url"], agents=owner.id, context=json.dumps(ctx))))
    owner.phone.wait_ring()
    offer = stream.wait_for("offer.created", agent_id=owner.id)
    assert offer["context"] == ctx and teammate.phone.legs == []
    cid = offer["conversation_id"]
    queued = stream.wait_for("conversation.queued", conversation_id=cid)
    after = [e for e in stream.events if e.get("conversation_id") == cid and int(e["id"]) >= int(queued["id"])]
    assert all(e["context"] == ctx for e in after)


def test_an_enqueue_url_naming_a_foreign_agent_is_422(world, tenant):
    q = tenant.queue(name="Q")
    foreign = world.new_tenant("Other").agent().id
    error(enqueue_as_plivo(world, tenant, with_query(q["enqueue_url"], agents=foreign)), 422)


def test_an_enqueue_context_over_2_kb_is_422(world, tenant):
    q = tenant.queue(name="Q")
    big = json.dumps({"blob": "x" * 2100})
    error(enqueue_as_plivo(world, tenant, with_query(q["enqueue_url"], context=big)), 422, "context_too_large")


def test_a_calls_key_enqueues_a_live_call_by_json_into_the_queues_room(world, tenant):
    agent = tenant.agent().login()
    q = tenant.queue(name="Q", agents=[agent.id])
    number = tenant.number()
    hold = '<Response><GetDigits timeout="60"><Speak>One moment</Speak></GetDigits></Response>'
    tenant.route_number(number, {"type": "redirect", "url": world.plivo.app(hold)})
    stream = tenant.stream(after=0)
    caller = world.plivo.call(number)
    world.wait(lambda: "One moment" in caller.said, what="the tenant app holds the call")
    body = {"call_uuid": caller.call_uuid, "from": "+14155550111", "to": number}
    r = tenant.key_with("calls").post(q["enqueue_url"], json=body)
    assert r.is_success and r.headers["content-type"].startswith("application/json")
    assert list(ok(r)) == ["conversation_id"]
    leg = agent.phone.wait_ring()
    stream.wait_for("offer.accepted", conversation_id=ok(r)["conversation_id"])
    world.wait(lambda: caller.mpc and caller.mpc == leg.mpc, what="the caller in the queue's room with the agent")
    error(tenant.key_with("reports").post(q["enqueue_url"], json=body), 403)


def test_a_json_enqueue_of_a_call_that_is_not_live_is_409_and_leaves_the_queue_empty(tenant):
    q = tenant.queue(name="Q", agents=[unavailable(tenant).id])
    body = {"call_uuid": str(uuid.uuid4()), "from": "+14155550123", "to": tenant.number()}
    error(tenant.key_with("calls").post(q["enqueue_url"], json=body), 409, "call_not_live")
    assert ok(tenant.api.get(f"/api/v1/queues/{q['id']}/stats"))["waiting"] == 0


def test_an_enqueue_url_answers_plivo_with_xml(world, tenant):
    q = tenant.queue(name="Q", agents=[tenant.agent().login().id])
    r = enqueue_as_plivo(world, tenant, q["enqueue_url"])
    assert r.status_code == 200 and r.headers["content-type"].startswith("text/xml")
    assert "<MultiPartyCall" in r.text


def test_an_outbound_call_handed_to_an_enqueue_url_stays_outbound(world, tenant):
    q, customer = tenant.queue(name="Q", agents=[unavailable(tenant).id]), "+16465550123"
    stream = tenant.stream(after=0)
    params = answer_params(customer, from_=registered_number(tenant), Direction="outbound")
    url = q["enqueue_url"]
    assert world.anon().post(url, data=params, headers=world.plivo.sign(url, params, tenant.auth_token)).is_success
    queued = stream.wait_for("conversation.queued")
    conv = ok(tenant.api.get(f"/api/v1/conversations/{queued['conversation_id']}"))
    assert (queued["direction"], conv["direction"], conv["number"]) == ("outbound", "outbound", customer)


def test_a_hangup_posted_to_an_enqueue_url_does_not_enqueue_again(world, tenant):
    _, q = tenant.inbound_line(unavailable(tenant))
    dialled, phone = lead(world)
    stream = tenant.stream(after=0)
    app_call(tenant, dialled, q["enqueue_url"], hangup_url=q["enqueue_url"])
    phone.wait_ring().hangup()
    cid = stream.wait_for("conversation.queued")["conversation_id"]
    stream.wait_for("conversation.closed", conversation_id=cid)
    world.wait(world.plivo.idle)
    fence(tenant)
    assert len([e for e in tenant.events(type="conversation.queued") if e["conversation_id"] == cid]) == 1


def test_a_menu_change_applies_to_the_next_call(world, tenant):
    number, m = menu_line(tenant, {"1": hangup_action("Old option")}, greeting="Old greeting")
    change = {"greeting": {"text": "New greeting"}, "options": {"1": hangup_action("New option")}}
    ok(tenant.api.patch(f"/api/v1/menus/{m['id']}", json=change))
    caller = world.plivo.call(number, digits="1")
    caller.wait_state("completed")
    assert in_order(caller.said, "New greeting", "New option") and "Old greeting" not in caller.said


def test_a_late_exit_from_the_queue_a_caller_left_for_a_menu_keeps_the_conversation(world, tenant):
    agent = tenant.agent().login()
    sales = tenant.queue(name="Sales", agents=[agent.id])
    m = menu(tenant, {"1": queue_action(sales)}, greeting="Press 1 for sales")
    to_menu = {"9": {"type": "menu", "menu_id": m["id"]}}
    waiting = tenant.queue(name="Waiting", agents=[unavailable(tenant).id], dtmf_actions=to_menu, wait_timeout_sec=600)
    number = tenant.number()
    tenant.route_number(number, queue_action(waiting))
    stream = tenant.stream(after=0)
    caller = world.plivo.call(number)
    cid = stream.wait_for("conversation.queued", queue_id=waiting["id"])["conversation_id"]
    room = world.wait(lambda: caller.mpc, what="the caller in the queue's room")
    left = ("ParticipantExit", "MPCEnd")
    late = world.plivo.fault(
        "delay",
        lambda r: r.params.get("MPCName") == room and r.params.get("EventName") in left,
        account=tenant.auth_id,
        times=2,
    )
    caller.press("9")
    world.wait(lambda: "Press 1 for sales" in caller.said, what="the menu greeting")
    caller.press("1")
    leg = agent.phone.wait_ring()
    assert hs_header(leg, "HsConversation") == cid
    stream.wait_for("offer.accepted", conversation_id=cid)
    late.release()
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    fence(tenant)
    assert len(late.hits) == 2 and caller.state == leg.state == "in_progress" and leg.mpc == caller.mpc
    for ended in ("call.ended", "conversation.closed"):
        assert not [e for e in tenant.events(type=ended) if e["conversation_id"] == cid], ended


def test_a_menu_refuses_an_option_on_the_hash_key(tenant):
    error(
        tenant.api.post(
            "/api/v1/menus", json={"name": "Hash", "greeting": {"text": "Hi"}, "options": {"#": hangup_action()}}
        ),
        422,
        "validation_failed",
    )


def test_a_menu_or_queue_a_number_routes_to_cannot_be_deleted(tenant):
    q = tenant.queue(name="Routed")
    m = menu(tenant, {"1": hangup_action()})
    for kind, target, action in (("queues", q, queue_action(q)), ("menus", m, {"type": "menu", "menu_id": m["id"]})):
        number = tenant.number()
        tenant.route_number(number, action)
        refused = error(tenant.api.delete(f"/api/v1/{kind}/{target['id']}"), 409, f"{kind[:-1]}_in_use")
        assert refused["message"] == "a number, queue or menu names this"
        tenant.route_number(number, hangup_action())
        assert tenant.api.delete(f"/api/v1/{kind}/{target['id']}").status_code == 204


def test_a_queue_or_menu_another_row_names_cannot_be_deleted(tenant):
    inner = menu(tenant, {"1": hangup_action()})
    outer = menu(tenant, {"1": {"type": "menu", "menu_id": inner["id"]}})
    tenant.route_number(tenant.number(), {"type": "menu", "menu_id": outer["id"]})
    error(tenant.api.delete(f"/api/v1/menus/{inner['id']}"), 409, "menu_in_use")
    fallback = tenant.queue(name="Fallback")
    busy = tenant.queue(name="Busy", overflow_action=queue_action(fallback))
    error(tenant.api.delete(f"/api/v1/queues/{fallback['id']}"), 409, "queue_in_use")
    ok(tenant.api.patch(f"/api/v1/queues/{busy['id']}", json={"overflow_action": hangup_action()}))
    assert tenant.api.delete(f"/api/v1/queues/{fallback['id']}").status_code == 204


def test_free_text_naming_a_queue_does_not_block_its_delete(tenant):
    q = tenant.queue(name="Mentioned")
    menu(tenant, {"1": hangup_action()}, greeting=f"queue {q['id']}")
    tenant.queue(name=f"copy of {q['id']}")
    assert tenant.api.delete(f"/api/v1/queues/{q['id']}").status_code == 204


def test_a_voicemail_mailbox_queue_blocks_its_delete(tenant):
    q = tenant.queue(name="Box")
    number = tenant.number()
    tenant.route_number(number, hangup_action(), closed_action=voicemail_action(q))
    error(tenant.api.delete(f"/api/v1/queues/{q['id']}"), 409, "queue_in_use")


def test_a_menu_or_queue_only_its_own_action_names_can_be_deleted(tenant):
    m, q = menu(tenant, {"1": hangup_action()}), tenant.queue(name="Loop")
    ok(tenant.api.patch(f"/api/v1/menus/{m['id']}", json={"options": {"1": {"type": "menu", "menu_id": m["id"]}}}))
    ok(tenant.api.patch(f"/api/v1/queues/{q['id']}", json={"overflow_action": queue_action(q)}))
    assert tenant.api.delete(f"/api/v1/menus/{m['id']}").status_code == 204
    assert tenant.api.delete(f"/api/v1/queues/{q['id']}").status_code == 204


def raced(write, delete, landed: int, in_use: str):
    """A write and the delete of a document it names, sent through the API at the same moment. One lands and the
    other is refused: the write's response when it landed with `landed` and the delete got 409 `in_use`, else None
    when the delete landed and the write got 422 foreign_id."""
    start = threading.Barrier(2)

    def send(call):
        start.wait(5)
        return call()

    with ThreadPoolExecutor(2) as pool:
        wrote, deleted = pool.map(send, (write, delete))
    if deleted.status_code == 204:
        error(wrote, 422, "foreign_id")
        return None
    assert wrote.status_code == landed, f"{wrote.status_code} {wrote.text}"
    error(deleted, 409, in_use)
    return wrote


def test_a_route_cannot_name_a_voicemail_queue_deleted_meanwhile(world, tenant):
    number = tenant.number()
    url = f"/api/v1/numbers/{number}"

    def route(q):
        return {"open_action": hangup_action(), "closed_action": voicemail_action(q)}

    gone = tenant.queue(name="Box")
    pointing, since = f"/Number/{number.removeprefix('+')}/", len(world.plivo.requests)
    held = world.plivo.fault("stall", lambda r: r.method == "GET" and pointing in r.path, account=tenant.auth_id)
    with ThreadPoolExecutor(1) as pool:
        put = pool.submit(tenant.api.put, url, json=route(gone))
        world.wait(lambda: held.hits, what="the route's Plivo check")
        assert tenant.api.delete(f"/api/v1/queues/{gone['id']}").status_code == 204
        held.release()
        error(put.result(), 422, "foreign_id")
    assert [r.method for r in world.plivo.requests[since:] if pointing in r.path] == ["GET"], "nothing repointed"
    error(tenant.api.get(url), 404, "number_not_found")
    box = tenant.queue(name="Box")
    ok(tenant.api.put(url, json=route(box)))
    error(tenant.api.delete(f"/api/v1/queues/{box['id']}"), 409, "queue_in_use")
    assert ok(tenant.api.get(url))["closed_action"]["mailbox"]["queue_id"] == box["id"]
    assert ok(tenant.api.get(f"/api/v1/queues/{box['id']}"))["id"] == box["id"]


def test_a_queue_cannot_overflow_to_a_queue_deleted_meanwhile(tenant):
    for i in range(RACES):
        fallback = tenant.queue(name=f"Fallback {i}")
        post = partial(
            tenant.api.post, "/api/v1/queues", json={"name": f"Busy {i}", "overflow_action": queue_action(fallback)}
        )
        wrote = raced(post, partial(tenant.api.delete, f"/api/v1/queues/{fallback['id']}"), 201, "queue_in_use")
        assert tenant.api.get(f"/api/v1/queues/{fallback['id']}").status_code == (200 if wrote else 404)
        queues = ok(tenant.api.get("/api/v1/queues", params={"limit": 100}))["data"]
        named = [q["id"] for q in queues if (q["overflow_action"] or {}).get("queue_id") == fallback["id"]]
        assert named == ([wrote.json()["id"]] if wrote else [])


def test_a_menu_option_cannot_name_a_menu_deleted_meanwhile_and_may_name_its_own_menu(tenant):
    outer = menu(tenant, {"1": hangup_action()})
    url, own = f"/api/v1/menus/{outer['id']}", {"2": {"type": "menu", "menu_id": outer["id"]}}
    for _ in range(RACES):
        inner = menu(tenant, {"1": hangup_action()})
        before = ok(tenant.api.get(url))["options"]
        options = {"1": {"type": "menu", "menu_id": inner["id"]}} | own
        patch = partial(tenant.api.patch, url, json={"options": options})
        wrote = raced(patch, partial(tenant.api.delete, f"/api/v1/menus/{inner['id']}"), 200, "menu_in_use")
        assert ok(tenant.api.get(url))["options"] == (options if wrote else before)
        assert tenant.api.get(f"/api/v1/menus/{inner['id']}").status_code == (200 if wrote else 404)
        assert ok(tenant.api.patch(url, json={"options": own}))["options"] == own


def test_a_menu_option_that_names_its_own_menu_is_accepted_and_reads_back(world, tenant):
    m = menu(tenant, {"1": hangup_action("One")}, greeting="Loop menu")
    own = {"1": {"type": "menu", "menu_id": m["id"]}}
    saved = ok(tenant.api.patch(f"/api/v1/menus/{m['id']}", json={"options": own}))
    assert saved["options"] == own
    assert ok(tenant.api.get(f"/api/v1/menus/{m['id']}"))["options"] == own
    number = tenant.number()
    tenant.route_number(number, {"type": "menu", "menu_id": m["id"]})
    caller = world.plivo.call(number, digits="1")
    world.wait(lambda: caller.said.count("Loop menu") >= 2, what="the menu to play again")
