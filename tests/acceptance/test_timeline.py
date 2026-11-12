"""The per-call timeline: every hop of a conversation, in order, from the event outbox."""

from tests.acceptance.conftest import (
    connect,
    disposition,
    error,
    hangup_action,
    hours,
    hs_header,
    in_order,
    menu,
    ok,
    queue_action,
    run_jobs_until,
    seen,
    team,
    types,
    unavailable,
    voicemail_action,
    wrap_up,
)

HOPS = [
    "conversation.started",
    "conversation.menu_input",
    "conversation.queued",
    "offer.created",
    "offer.rejected",
    "offer.created",
    "offer.accepted",
    "call.ended",
    "conversation.wrap_up_started",
    "conversation.wrapped_up",
    "conversation.closed",
]


def timeline(client, cid: str) -> list[dict]:
    return ok(client.get(f"/api/v1/conversations/{cid}/timeline"))["data"]


def test_the_timeline_lists_every_hop_in_order(world, tenant):
    first, second = tenant.agent("First").login(), tenant.agent("Second").login()
    first.phone.on_ring("decline")
    q = tenant.queue(name="Sales", agents=[first.id, second.id], agents_at_once=1, retries=1)
    number = tenant.number()
    m = menu(tenant, {"1": queue_action(q)})
    open_hours = hours(tenant)["id"]
    tenant.route_number(
        number, {"type": "menu", "menu_id": m["id"]}, business_hours_id=open_hours, closed_action=hangup_action()
    )
    stream = tenant.stream(after=0)
    caller = world.plivo.call(number, digits="1")
    cid = hs_header(second.phone.wait_ring(), "HsConversation")
    stream.wait_for("offer.accepted", conversation_id=cid)
    caller.hangup()
    stream.wait_for("conversation.wrap_up_started", conversation_id=cid)
    ok(wrap_up(second, cid, disposition(tenant)["id"]))
    stream.wait_for("conversation.closed", conversation_id=cid)
    kinds = types(timeline(tenant.api, cid))
    assert in_order(kinds, *HOPS) and in_order(kinds, "offer.accepted", "call.ended")


def test_the_timeline_shows_overflow_and_voicemail(world, tenant):
    q = tenant.queue(name="Q", agents=[unavailable(tenant).id], wait_timeout_sec=1)
    ok(tenant.api.patch(f"/api/v1/queues/{q['id']}", json={"overflow_action": voicemail_action(q)}))
    number = tenant.number()
    tenant.route_number(number, queue_action(q))
    stream = tenant.stream(after=0)
    world.plivo.call(number)
    cid = run_jobs_until(world, lambda: seen(stream, "voicemail.received"), "voicemail.received")[0]["conversation_id"]
    run_jobs_until(world, lambda: seen(stream, "conversation.closed", conversation_id=cid), "closed")
    kinds = types(timeline(tenant.api, cid))
    assert in_order(
        kinds,
        "conversation.started",
        "conversation.queued",
        "conversation.overflowed",
        "voicemail.received",
        "call.ended",
    )


def test_an_after_hours_call_shows_hours_and_the_closed_action(world, tenant):
    number = tenant.number()
    tenant.route_number(
        number,
        {"type": "hangup"},
        business_hours_id=hours(tenant, weekly=False)["id"],
        closed_action=hangup_action("Closed"),
    )
    stream = tenant.stream(after=0)
    world.plivo.call(number).wait_state("completed")
    cid = stream.wait_for("conversation.after_hours")["conversation_id"]
    run_jobs_until(world, lambda: seen(stream, "call.ended", conversation_id=cid), "call.ended")
    entries = timeline(tenant.api, cid)
    assert entries[0]["type"] == "conversation.started" and entries[0]["data"]["hours"] == "closed"
    assert in_order(types(entries), "conversation.started", "conversation.after_hours", "call.ended")


def test_holds_appear_on_the_timeline_as_envelopes_in_id_order(tenant):
    live = connect(tenant)
    for on in (True, False):
        ok(live.agent.api().post(f"/api/v1/calls/{live.id}/hold", json={"on": on}))
    live.stream.wait_for("call.resumed", conversation_id=live.id)
    entries = timeline(tenant.api, live.id)
    assert in_order(types(entries), "offer.accepted", "call.held", "call.resumed")
    assert all(e["conversation_id"] == live.id and e["created_at"] for e in entries)
    assert [int(e["id"]) for e in entries] == sorted(int(e["id"]) for e in entries)


def test_the_timeline_is_for_calls_keys_and_supervisors(tenant):
    t = team(tenant, "Floor")
    sup = tenant.agent("Sup", role="supervisor", teams=[t]).login(available=False)
    live = connect(tenant, tenant.agent("Agent", teams=[t]).login())
    ok(tenant.key_with("calls").get(f"/api/v1/conversations/{live.id}/timeline"))
    ok(sup.api().get(f"/api/v1/conversations/{live.id}/timeline"))
    error(live.agent.api().get(f"/api/v1/conversations/{live.id}/timeline"), (401, 403))
    error(tenant.key_with("reports").get(f"/api/v1/conversations/{live.id}/timeline"), 403)
