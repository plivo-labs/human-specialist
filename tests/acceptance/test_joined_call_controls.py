"""A call an agent places to work a callback or a task joins that conversation, and takes the controls of any live
call: hold, recording and hangup. Its end leaves the callback or task with its agent until it is resolved."""

import math
from concurrent.futures import ThreadPoolExecutor

import pytest

from tests.acceptance.conftest import (
    disposition,
    error,
    fence,
    lead,
    ok,
    registered_number,
    replayed,
    run_jobs_until,
    settings,
    team,
    wrap_up,
)
from tests.acceptance.test_conversation_reports import one, rows
from tests.acceptance.test_recordings import transcript

KINDS = ["callback", "task"]
# A leg's hangup cause: its ring cancelled by HS, its answer refused, or HS hung it up once answered.
CANCELLED, REJECTED, HUNG_UP = 1000, 3020, 4000


def act(client, cid: str, action: str, body=None):
    return client.post(f"/api/v1/calls/{cid}/{action}", json=body)


def worked(tenant, kind: str, agent=None):
    """A callback or a task naming a lead's number, assigned to `agent`. Returns (agent, task, the lead's phone,
    stream)."""
    agent = agent or tenant.agent().login()
    number, phone = lead(tenant.world)
    stream = tenant.stream(after=0)
    body = {"type": kind, "title": "Call back", "customer": number, "target": {"type": "agent", "agent_id": agent.id}}
    task = ok(tenant.api.post("/api/v1/tasks", json=body))
    assert task["routing_status"] == "accepted"
    return agent, task, phone, stream


def placed(tenant, agent, task) -> dict:
    cid = task["conversation_id"]
    return {"agent_id": agent.id, "to": task["customer"], "from": registered_number(tenant), "conversation_id": cid}


def call(tenant, agent, task, phone, stream):
    """The agent calls the task's customer from its conversation, and the customer answers. Returns the lead's leg."""
    cid = task["conversation_id"]
    assert ok(tenant.api.post("/api/v1/calls", json=placed(tenant, agent, task)))["conversation_id"] == cid
    leg = phone.wait_ring()
    stream.wait_for("call.answered", conversation_id=cid, call_uuid=leg.call_uuid)
    return leg


def state(tenant, cid: str) -> str:
    return ok(tenant.api.get(f"/api/v1/conversations/{cid}"))["state"]


@pytest.mark.parametrize("kind", KINDS)
def test_a_call_from_a_callback_or_task_holds_and_resumes_its_customer(world, tenant, kind):
    agent, task, phone, stream = worked(tenant, kind)
    cid = task["conversation_id"]
    customer = call(tenant, agent, task, phone, stream)
    ok(act(agent.api(), cid, "hold", {"on": True}))
    held = stream.wait_for("call.held", conversation_id=cid)
    assert (held["agent_id"], held["data"]["by_agent_id"], held["data"]["participant_id"]) == (agent.id, agent.id, None)
    assert customer.held and not agent.phone.legs[-1].held
    ok(act(tenant.api, cid, "hold", {"on": False}))
    stream.wait_for("call.resumed", conversation_id=cid)
    assert not customer.held
    ok(act(agent.api(), cid, "switch", {}))
    ok(act(agent.api(), cid, "merge"))
    [dial] = [a for a in ok(tenant.api.get(f"/api/v1/conversations/{cid}"))["assignments"] if a["via"] == "outbound"]
    error(act(agent.api(), cid, "hold", {"on": True, "participant_id": dial["id"]}), 404, "participant_not_found")


@pytest.mark.parametrize("kind", KINDS)
def test_a_call_from_a_callback_or_task_pauses_and_resumes_its_recording(world, tenant, kind):
    settings(tenant, recording={"outbound": True, "allow_pause": True})
    agent, task, phone, stream = worked(tenant, kind)
    cid = task["conversation_id"]
    call(tenant, agent, task, phone, stream)
    ok(act(agent.api(), cid, "recording", {"action": "pause"}))
    paused = stream.wait_for("call.recording_paused", conversation_id=cid)
    assert (paused["agent_id"], paused["data"]["by_agent_id"]) == (agent.id, agent.id)
    ok(act(tenant.api, cid, "recording", {"action": "resume"}))
    stream.wait_for("call.recording_resumed", conversation_id=cid)


@pytest.mark.parametrize("kind", KINDS)
def test_a_resume_starts_the_recording_of_a_call_from_a_callback_or_task_that_records_nothing(world, tenant, kind):
    settings(tenant, recording={"outbound": False, "allow_pause": True})
    agent, task, phone, stream = worked(tenant, kind)
    cid = task["conversation_id"]
    customer = call(tenant, agent, task, phone, stream)
    ok(act(agent.api(), cid, "recording", {"action": "resume"}))
    stream.wait_for("call.recording_resumed", conversation_id=cid)
    customer.hangup()
    assert stream.wait_for("recording.ready", conversation_id=cid)["data"]["recording_id"]


@pytest.mark.parametrize("by", ["agent", "key"])
@pytest.mark.parametrize("kind", KINDS)
def test_a_hangup_ends_the_call_and_leaves_the_callback_or_task_with_its_agent_until_resolved(world, tenant, kind, by):
    settings(tenant, recording={"allow_pause": True})
    agent, task, phone, stream = worked(tenant, kind)
    cid = task["conversation_id"]
    customer = call(tenant, agent, task, phone, stream)
    ok(act(agent.api() if by == "agent" else tenant.api, cid, "hangup"))
    customer.wait_state("completed")
    agent.phone.legs[-1].wait_state("completed")
    assert stream.wait_for("call.ended", conversation_id=cid)["data"]["ended_by"] == "agent"
    for action, body in (("hangup", None), ("hold", {"on": True}), ("recording", {"action": "pause"})):
        error(act(agent.api(), cid, action, body), 409, "call_not_live")
    assert (state(tenant, cid), ok(tenant.api.get(f"/api/v1/tasks/{task['id']}"))["status"]) == ("active", "todo")
    fence(tenant)
    assert not of(tenant, "conversation.wrap_up_started", cid)
    ok(agent.api().patch(f"/api/v1/tasks/{task['id']}", json={"status": "resolved"}))
    stream.wait_for("conversation.wrap_up_started", conversation_id=cid, agent_id=agent.id)
    ok(wrap_up(agent, cid, disposition(tenant, "Called back")["id"]))
    stream.wait_for("conversation.closed", conversation_id=cid)


@pytest.mark.parametrize("kind", KINDS)
def test_a_hangup_while_the_agents_phone_rings_cancels_the_call_from_a_callback_or_task(world, tenant, kind):
    settings(tenant, ring_timeout_sec=120)  # the agent's ring outlasts the test unless HS cancels it
    agent, task, _, stream = worked(tenant, kind)
    cid = task["conversation_id"]
    agent.phone.on_ring("ignore")
    ok(tenant.api.post("/api/v1/calls", json=placed(tenant, agent, task)))
    ring = agent.phone.wait_ring()
    world.wait(world.plivo.idle, what="Plivo's answer to the agent's dial to reach HS")
    ok(act(tenant.api, cid, "hangup"))
    assert ring.wait_state("completed").hangup_cause_code == CANCELLED
    assert stream.wait_for("call.failed", conversation_id=cid)["data"]["result"] == "cancelled"
    stream.wait_for("call.ended", conversation_id=cid)
    assert state(tenant, cid) == "active"


@pytest.mark.parametrize("kind", KINDS)
def test_each_call_from_a_callback_or_task_that_ends_on_hold_counts_its_hold_until_its_end(world, tenant, kind):
    agent, task, phone, stream = worked(tenant, kind)
    cid = task["conversation_id"]
    for n in (1, 2):
        customer = call(tenant, agent, task, phone, stream)
        ok(act(agent.api(), cid, "hold", {"on": True}))
        assert customer.held
        world.travel(20)
        customer.hangup()
        world.wait(lambda n=n: len(of(tenant, "call.ended", cid)) == n, what="the call's end")
        world.travel(30)
    ok(agent.api().patch(f"/api/v1/tasks/{task['id']}", json={"status": "resolved"}))
    ok(wrap_up(agent, cid, disposition(tenant, "Called back")["id"]))
    stream.wait_for("conversation.closed", conversation_id=cid)
    row = one(rows(tenant.api)["data"], id=cid)
    assert row["hold_count"] == 2 and 40 <= row["hold_sec"] <= 46


def of(tenant, type_: str, cid: str) -> list[dict]:
    return [e for e in tenant.events(type=type_) if e["conversation_id"] == cid]


@pytest.mark.parametrize("kind", KINDS)
def test_a_call_from_a_callback_or_task_takes_no_transfer_participant_or_supervisor(world, tenant, kind):
    t = team(tenant, "Floor")
    sup = tenant.agent("Sup", role="supervisor", teams=[t]).login()
    agent, task, phone, stream = worked(tenant, kind, tenant.agent("Agent", teams=[t]).login())
    other = tenant.agent("Other").login()
    cid = task["conversation_id"]
    call(tenant, agent, task, phone, stream)
    target = {"type": "agent", "agent_id": other.id}
    error(act(agent.api(), cid, "transfers", {"mode": "cold", "target": target}), 409, "call_joined")
    error(act(agent.api(), cid, "participants", {"target": target}), 409, "call_joined")
    error(act(sup.api(), cid, "monitor", {"mode": "listen"}), 409, "call_joined")
    assert not other.phone.legs and not sup.phone.legs


@pytest.mark.parametrize("kind", KINDS)
def test_resolving_a_callback_or_task_while_its_call_is_on_ends_the_call(world, tenant, kind):
    agent, task, phone, stream = worked(tenant, kind)
    cid = task["conversation_id"]
    world.travel(600)
    customer = call(tenant, agent, task, phone, stream)
    ok(agent.api().patch(f"/api/v1/tasks/{task['id']}", json={"status": "resolved"}))
    ended = stream.wait_for("call.ended", conversation_id=cid)
    assert (ended["agent_id"], ended["data"]["result"], ended["data"]["ended_by"]) == (agent.id, "answered", "system")
    assert ended["data"]["talk_sec"] < 60, "talk counts from the call, not from the task's accept"
    customer.wait_state("completed")
    agent.phone.legs[-1].wait_state("completed")
    stream.wait_for("conversation.wrap_up_started", conversation_id=cid, agent_id=agent.id)
    ok(wrap_up(agent, cid, disposition(tenant, "Called back")["id"]))
    stream.wait_for("conversation.closed", conversation_id=cid)
    assert len(of(tenant, "call.ended", cid)) == 1


LATE = {
    "hold": ({"on": True}, "call.held", lambda r: r.params.get("hold") is True, 1),
    "recording": ({"action": "pause"}, "call.recording_paused", lambda r: "/Participant/" in r.path, 2),  # per track
}


@pytest.mark.parametrize("action", LATE)
def test_a_hold_or_pause_plivo_confirms_after_the_customer_hung_up_tells_and_counts_nothing(world, tenant, action):
    body, told, member, last = LATE[action]
    settings(tenant, recording={"outbound": True, "allow_pause": True})
    agent, task, phone, stream = worked(tenant, "callback")
    cid = task["conversation_id"]
    customer = call(tenant, agent, task, phone, stream)
    sent = []

    def the_last(r) -> bool:
        if r.method == "POST" and member(r):
            sent.append(r)
            return len(sent) == last
        return False

    stall = world.plivo.fault("stall", the_last, account=tenant.auth_id)
    with ThreadPoolExecutor(1) as pool:
        asking = pool.submit(act, agent.api(), cid, action, body)
        world.wait(lambda: stall.hits, what=f"the {action}'s last request stalled at Plivo")
        customer.hangup()
        stream.wait_for("call.ended", conversation_id=cid)
        stall.release()
        assert asking.result(15).status_code == 204
    world.travel(20)
    assert not replayed(tenant, told, conversation_id=cid)
    ok(agent.api().patch(f"/api/v1/tasks/{task['id']}", json={"status": "resolved"}))
    ok(wrap_up(agent, cid, disposition(tenant, "Called back")["id"]))
    stream.wait_for("conversation.closed", conversation_id=cid)
    row = one(rows(tenant.api)["data"], id=cid)
    assert (row["hold_count"], row["hold_sec"]) == (0, 0)


def ended_once(tenant, phone, stream, cid: str) -> None:
    assert stream.wait_for("call.failed", conversation_id=cid)["data"]["result"] == "cancelled"
    assert len(replayed(tenant, "call.ended", conversation_id=cid)) == 1
    assert not phone.legs


@pytest.mark.parametrize("status", ["resolved", "cancelled"])
def test_ending_a_task_while_its_agents_phone_rings_cancels_the_ring_and_tells_the_calls_end_once(
    world, tenant, status
):
    settings(tenant, ring_timeout_sec=120)
    agent, task, phone, stream = worked(tenant, "task")
    agent.phone.on_ring("ignore")
    ok(tenant.api.post("/api/v1/calls", json=placed(tenant, agent, task)))
    ring = agent.phone.wait_ring()
    world.wait(world.plivo.idle, what="Plivo's answer to the agent's dial to reach HS")
    ok(agent.api().patch(f"/api/v1/tasks/{task['id']}", json={"status": status}))
    assert ring.wait_state("completed").hangup_cause_code == CANCELLED
    ended_once(tenant, phone, stream, task["conversation_id"])


@pytest.mark.parametrize(("end", "then"), [("hangup", "rings"), ("hangup", "answers"), ("resolve", "rings")])
def test_a_call_from_a_task_ended_while_plivo_still_places_it_never_reaches_its_customer(world, tenant, end, then):
    settings(tenant, ring_timeout_sec=120)
    agent, task, phone, stream = worked(tenant, "task")
    cid = task["conversation_id"]
    agent.phone.on_ring("ignore")
    placing = world.plivo.fault(
        "stall", lambda r: r.method == "POST" and r.path.endswith("/Call/"), account=tenant.auth_id
    )
    with ThreadPoolExecutor(1) as pool:
        dialing = pool.submit(tenant.api.post, "/api/v1/calls", json=placed(tenant, agent, task))
        ring = agent.phone.wait_ring()
        if end == "hangup":
            ok(act(tenant.api, cid, "hangup"))
        else:
            ok(agent.api().patch(f"/api/v1/tasks/{task['id']}", json={"status": "resolved"}))
        if then == "answers":
            ring.answer()
            ring.wait_state("completed")
        placing.release()
        assert dialing.result(15).status_code == 201
    assert ring.wait_state("completed").hangup_cause_code == (CANCELLED if then == "rings" else REJECTED)
    ended_once(tenant, phone, stream, cid)


@pytest.mark.parametrize("then", ["rings", "answers"])
def test_a_redial_while_plivo_still_places_the_ended_call_goes_through_and_the_ended_call_never_touches_it(
    world, tenant, then
):
    settings(tenant, ring_timeout_sec=120)
    agent, task, phone, stream = worked(tenant, "task")
    cid = task["conversation_id"]
    agent.phone.on_ring("ignore")
    placing = world.plivo.fault(
        "stall", lambda r: r.method == "POST" and r.path.endswith("/Call/"), account=tenant.auth_id
    )
    with ThreadPoolExecutor(1) as pool:
        first = pool.submit(tenant.api.post, "/api/v1/calls", json=placed(tenant, agent, task))
        ring = agent.phone.wait_ring()
        ok(act(tenant.api, cid, "hangup"))
        ok(tenant.api.post("/api/v1/calls", json=placed(tenant, agent, task)))
        second = agent.phone.wait_ring()
        if then == "answers":
            ring.answer()
            assert ring.wait_state("completed").hangup_cause_code == REJECTED
        placing.release()
        assert first.result(15).status_code == 201
    assert ring.wait_state("completed").hangup_cause_code == (CANCELLED if then == "rings" else REJECTED)
    second.answer()
    customer = phone.wait_ring()
    assert stream.wait_for("call.answered", conversation_id=cid)["data"]["call_uuid"] == customer.call_uuid
    told = [len(replayed(tenant, kind, conversation_id=cid)) for kind in ("call.failed", "call.ended", "call.answered")]
    assert told == [1, 1, 1] and second.state == customer.state == "in_progress"


def added(r) -> bool:
    return r.method == "POST" and r.path.endswith("/Participant/") and r.params.get("role") == "Customer"


def test_a_customer_leg_plivo_tells_of_only_after_its_call_ended_and_a_redial_went_live_is_hung_up(world, tenant):
    agent, task, phone, stream = worked(tenant, "task")
    cid = task["conversation_id"]
    phone.on_ring("ignore")
    world.plivo.fault("lose", added, account=tenant.auth_id)
    late = world.plivo.fault(
        "delay", lambda r: r.params.get("Role") == "Customer", account=tenant.auth_id, times=math.inf
    )
    ok(tenant.api.post("/api/v1/calls", json=placed(tenant, agent, task)))
    agent.phone.wait_ring()
    stray = phone.wait_ring()
    ok(act(tenant.api, cid, "hangup"))
    stream.wait_for("call.ended", conversation_id=cid)
    phone.on_ring("answer")
    ok(tenant.api.post("/api/v1/calls", json=placed(tenant, agent, task)))
    agent.phone.wait_ring()
    customer = phone.wait_ring()
    assert stream.wait_for("call.answered", conversation_id=cid)["data"]["call_uuid"] == customer.call_uuid
    late.release()
    assert stray.wait_state("completed").hangup_cause_code == CANCELLED
    told = [len(replayed(tenant, kind, conversation_id=cid)) for kind in ("call.ended", "call.answered")]
    assert told == [1, 1] and customer.state == "in_progress"


@pytest.mark.parametrize("redial", [False, True], ids=["alone", "after_a_redial"])
def test_a_customer_who_joins_an_ended_calls_room_late_is_hung_up_and_never_joins_a_later_call(world, tenant, redial):
    agent, task, phone, stream = worked(tenant, "task")
    cid = task["conversation_id"]
    phone.on_ring("ignore")
    world.plivo.fault("lose", added, account=tenant.auth_id)
    world.plivo.fault("drop", lambda r: r.params.get("Role") == "Customer", account=tenant.auth_id, times=math.inf)
    ok(tenant.api.post("/api/v1/calls", json=placed(tenant, agent, task)))
    agent.phone.wait_ring()
    stray = phone.wait_ring()
    ok(act(tenant.api, cid, "hangup"))
    stream.wait_for("call.ended", conversation_id=cid)
    if redial:
        agent.phone.on_ring("ignore")
        ok(tenant.api.post("/api/v1/calls", json=placed(tenant, agent, task)))
        agent.phone.wait_ring()
    stray.answer()
    assert stray.wait_state("completed").hangup_cause_code == HUNG_UP
    assert not replayed(tenant, "call.answered", conversation_id=cid)
    assert len(replayed(tenant, "call.ended", conversation_id=cid)) == 1


def test_a_redial_answered_while_plivo_holds_the_ended_calls_customer_add_rings_its_own_customer(world, tenant):
    agent, task, phone, stream = worked(tenant, "task")
    cid = task["conversation_id"]
    phone.on_ring("ignore")
    adding = world.plivo.fault("stall", added, account=tenant.auth_id)
    world.plivo.fault("drop", lambda r: r.params.get("Role") == "Customer", account=tenant.auth_id, times=math.inf)
    ok(tenant.api.post("/api/v1/calls", json=placed(tenant, agent, task)))
    agent.phone.wait_ring()
    stray = phone.wait_ring()
    ok(act(tenant.api, cid, "hangup"))
    stream.wait_for("call.ended", conversation_id=cid)
    phone.on_ring("answer")
    ok(tenant.api.post("/api/v1/calls", json=placed(tenant, agent, task)))
    agent.phone.wait_ring()
    customer = phone.wait_ring()
    assert stream.wait_for("call.answered", conversation_id=cid)["data"]["call_uuid"] == customer.call_uuid
    adding.release()
    assert stray.wait_state("completed").hangup_cause_code == CANCELLED
    told = [len(replayed(tenant, kind, conversation_id=cid)) for kind in ("call.ended", "call.answered")]
    assert told == [1, 1] and customer.state == "in_progress"
    adds = [r.path for r in world.plivo.requests_to(f"/Account/{tenant.auth_id}/") if added(r)]
    assert len(adds) == len(set(adds)) == 2


def test_a_redial_from_a_task_leaves_the_ended_calls_transcript_naming_its_customer(world, tenant):
    settings(tenant, recording={"outbound": True})
    agent, task, phone, stream = worked(tenant, "task")
    cid = task["conversation_id"]
    call(tenant, agent, task, phone, stream).hangup()
    rid = stream.wait_for("recording.ready", conversation_id=cid)["data"]["recording_id"]
    call(tenant, agent, task, phone, stream)
    speakers = {
        s["speaker"] for s in run_jobs_until(world, lambda: transcript(tenant.api, rid), "the transcript")["segments"]
    }
    assert "Customer" in speakers and len(speakers) == 2
