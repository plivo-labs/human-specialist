"""The status map: call result -> system disposition per direction, and where HS applies it."""

import pytest

from tests.acceptance.conftest import (
    app_call,
    connect,
    dial,
    disposition,
    disposition_named,
    error,
    hangup_action,
    hours,
    in_order,
    lead,
    menu_line,
    ok,
    run_jobs_until,
    seen,
    settings,
    types,
    unavailable,
)

M = "/api/v1/status-map"
OUTBOUND = ("no_answer", "busy", "rejected", "cancelled", "failed_invalid", "failed_carrier")
DEFAULTS = {
    "inbound": {
        "answered": "Auto-closed",
        "queue_timeout": "Queue timeout",
        "voicemail": "Voicemail",
        "abandoned": "Dropped",
        "after_hours": "Out of business hour",
        "rejected": "Dropped",
        "forwarded": "Forwarded",
        "enqueue_failed": "Enqueue failed",
        "assignment_failed": "Agent assignment failed",
        "queue_exit": "Queue exit",
    },
    "outbound": {"answered": "Auto-closed"} | dict.fromkeys(OUTBOUND, "Customer didn't respond"),
}


def remap(tenant, direction: str, result: str) -> str:
    """Map one result to a new system disposition; return its id."""
    mapped = disposition(tenant, f"Mapped {direction} {result}", system=True)["id"]
    current = ok(tenant.api.get(M))
    current[direction][result] = mapped
    ok(tenant.api.put(M, json=current))
    return mapped


def closed_with(tenant, stream, cid: str) -> dict:
    return run_jobs_until(tenant.world, lambda: seen(stream, "conversation.closed", conversation_id=cid), "closed")[0]


def mapped_ids(closed: dict) -> list[str]:
    return [d["disposition_id"] for d in closed["data"]["dispositions"]]


def test_the_map_covers_every_result_with_todays_names_by_default(tenant):
    rows = {d["id"]: d for d in ok(tenant.api.get("/api/v1/dispositions", params={"limit": 200}))["data"]}
    current = ok(tenant.api.get(M))
    assert current == ok(tenant.api.get(M, params={"defaults": "true"}))
    for direction, expected in DEFAULTS.items():
        assert {r: rows[current[direction][r]]["name"] for r in expected} == expected, direction
        assert all(rows[current[direction][r]]["system"] for r in expected), direction


def test_only_system_dispositions_map_and_restoring_is_a_put_of_the_defaults(tenant):
    current = ok(tenant.api.get(M))
    current["outbound"]["busy"] = disposition(tenant, "Sale")["id"]
    error(tenant.api.put(M, json=current), 422, "not_system_disposition")
    remap(tenant, "inbound", "abandoned")
    defaults = ok(tenant.api.get(M, params={"defaults": "true"}))
    assert ok(tenant.api.get(M)) != defaults
    ok(tenant.api.put(M, json=defaults))
    assert ok(tenant.api.get(M)) == defaults


def test_an_unreached_lead_is_call_failed_and_closes_with_the_mapped_disposition(tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    for behaviour, result in (("no_answer", "no_answer"), ("busy", "busy"), ("reject", "rejected")):
        mapped = remap(tenant, "outbound", result)
        _, cid = dial(tenant, agent, to=lead(tenant.world, behaviour)[0])
        failed = stream.wait_for("call.failed", conversation_id=cid)
        assert failed["data"]["result"] == result and failed["data"]["hangup_cause_code"], behaviour
        assert stream.wait_for("call.ended", conversation_id=cid)["data"]["result"] == result, behaviour
        assert mapped in mapped_ids(closed_with(tenant, stream, cid)), behaviour


def test_an_unanswered_agent_leg_is_call_failed_cancelled_with_the_mapped_disposition(tenant):
    mapped = remap(tenant, "outbound", "cancelled")
    agent = tenant.agent().login()
    agent.phone.on_ring("timeout")
    number, phone = lead(tenant.world)
    stream = tenant.stream(after=0)
    _, cid = dial(tenant, agent, to=number)
    assert stream.wait_for("call.failed", conversation_id=cid)["data"]["result"] == "cancelled"
    closed = closed_with(tenant, stream, cid)
    failed, ended = (stream.wait_for(t, conversation_id=cid) for t in ("call.failed", "call.ended"))
    assert int(failed["id"]) < int(ended["id"]) and phone.legs == []
    assert closed["data"]["result"] == "cancelled" and mapped in mapped_ids(closed)


def test_a_caller_who_hangs_up_in_a_menu_ends_with_call_ended_and_closes_abandoned(tenant):
    mapped = remap(tenant, "inbound", "abandoned")
    number, _ = menu_line(tenant, {"1": hangup_action()}, input_timeout_sec=30)
    stream = tenant.stream(after=0)
    caller = tenant.world.plivo.call(number)
    cid = stream.wait_for("conversation.started")["conversation_id"]
    tenant.world.wait(lambda: "Press a key" in caller.said, what="menu greeting")
    caller.hangup()
    closed = closed_with(tenant, stream, cid)
    assert closed["data"]["result"] == "abandoned" and mapped in mapped_ids(closed)
    kinds = types(ok(tenant.api.get(f"/api/v1/conversations/{cid}/timeline"))["data"])
    assert in_order(kinds, "conversation.started", "call.ended") and "conversation.queued" not in kinds


@pytest.mark.parametrize("direction", ["inbound", "outbound"])
def test_a_caller_who_hangs_up_while_queued_closes_abandoned(world, tenant, direction):
    number, q = tenant.inbound_line(unavailable(tenant))
    stream = tenant.stream(after=0)
    if direction == "inbound":
        caller = world.plivo.call(number)
    else:
        dialled, phone = lead(world)
        app_call(tenant, dialled, q["enqueue_url"], hangup_url=world.plivo.app("<Response/>"))
        caller = phone.wait_ring()
    cid = stream.wait_for("conversation.queued")["conversation_id"]
    caller.hangup()
    closed = stream.wait_for("conversation.closed", conversation_id=cid)
    assert (closed["direction"], closed["data"]["result"]) == (direction, "abandoned")
    assert disposition_named(tenant, "Dropped")["id"] in mapped_ids(closed)


def test_an_after_hours_call_closes_with_the_mapped_disposition(tenant):
    mapped = remap(tenant, "inbound", "after_hours")
    number = tenant.number()
    closed_hours = hours(tenant, weekly=False)["id"]
    tenant.route_number(
        number, {"type": "hangup"}, business_hours_id=closed_hours, closed_action=hangup_action("Closed")
    )
    stream = tenant.stream(after=0)
    tenant.world.plivo.call(number).wait_state("completed")
    closed = closed_with(tenant, stream, stream.wait_for("conversation.after_hours")["conversation_id"])
    assert closed["data"]["result"] == "after_hours" and mapped in mapped_ids(closed)


def test_a_skipped_wrap_up_takes_the_answered_mapping(tenant):
    mapped = remap(tenant, "inbound", "answered")
    settings(tenant, wrap_up={"mode": "auto", "seconds": 1})
    live = connect(tenant)
    live.caller.hangup()
    live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id)
    done = run_jobs_until(
        tenant.world, lambda: seen(live.stream, "conversation.wrapped_up", conversation_id=live.id), "wrapped up"
    )[0]
    assert (done["data"]["disposition_id"], done["data"]["system"], done["data"]["skipped"]) == (mapped, True, True)


def test_a_queue_timeout_closes_with_its_default_disposition(tenant):
    q = tenant.queue(name="Nobody free", agents=[unavailable(tenant).id], wait_timeout_sec=1)
    number = tenant.number()
    tenant.route_number(number, {"type": "queue", "queue_id": q["id"]})
    stream = tenant.stream(after=0)
    tenant.world.plivo.call(number).wait_state("completed", timeout=10)
    closed = closed_with(tenant, stream, stream.wait_for("conversation.queued")["conversation_id"])
    assert closed["data"]["result"] == "queue_timeout"
    assert disposition_named(tenant, "Queue timeout")["id"] in mapped_ids(closed)
