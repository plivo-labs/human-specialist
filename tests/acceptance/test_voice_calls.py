"""Voice calls: what a caller, an agent, a supervisor or an integrator can observe on a call. The caller held through
a transfer's consult, a participant's consult, transfer and participant outcomes, the tenant's transfer policy, the
call's owner, the numbers and names a ring shows, supervisor capacity, moves, mute, assistance and silence,
recording tracks and transcripts, wait prompts, call feedback and the text transfers."""

import json
import math
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from uuid import uuid4

import httpx
import pytest

from tests.acceptance.channels import accept, enqueue_text, offered, reply, take, text_line
from tests.acceptance.conftest import (
    activity,
    connect,
    error,
    fence,
    hangup_action,
    hs_header,
    lead,
    menu,
    ok,
    plivo_post,
    registered_number,
    replayed,
    run_jobs_until,
    say,
    seen,
    settings,
    team,
    unavailable,
)
from tests.acceptance.test_export import closed, content

CUSTOMER = "+14155550601"
TRACK_GRACE_SEC = 30


def act(client, cid: str, action: str, body=None):
    return client.post(f"/api/v1/calls/{cid}/{action}", json=body)


def to_agent(agent) -> dict:
    return {"type": "agent", "agent_id": agent.id}


def to_number(number: str) -> dict:
    return {"type": "number", "number": number}


def member(tenant, leg) -> dict:
    """The leg's live member in its room as Plivo's participant list reports it (hold, mute, coach_mode); {} when
    the leg is in no room."""
    if leg.mpc is None:
        return {}
    url = f"{tenant.world.plivo.url}/v1/Account/{tenant.auth_id}/MultiPartyCall/name_{leg.mpc}/Participant/"
    listed = httpx.get(url, auth=(tenant.auth_id, tenant.auth_token), timeout=10).json().get("objects", [])
    return next((m for m in listed if m["call_uuid"] == leg.call_uuid and not m["exit_cause"]), {})


def on_hold(tenant, leg, held: bool = True) -> None:
    tenant.world.wait(lambda: (m := member(tenant, leg)) and m["hold"] is held, what=f"{leg.to} hold {held}")


def rings_to(world, address: str) -> list[dict]:
    """The add-member requests HS made to Plivo that dialed `address` (a number, or an endpoint's username)."""
    adds = [r.params for r in world.plivo.requests if r.method == "POST" and r.path.endswith("/Participant/")]
    return [p for p in adds if any(address in to for to in str(p.get("to", "")).split("<"))]


def speaks(world, tenant) -> list[dict]:
    """The member Speak requests HS made for the tenant, in order."""
    return [
        r.params
        for r in world.plivo.requests
        if r.method == "POST" and tenant.auth_id in r.path and "/Member/" in r.path and r.path.endswith("/Speak/")
    ]


def heard(world, call, words: str) -> str:
    return world.wait(lambda: next((s for s in call.said if words in s), None), what=f"a prompt with {words!r}")


def floor(tenant):
    """A team with a supervisor and an agent who is on a live inbound call. Returns (live, supervisor)."""
    t = team(tenant, "Floor")
    sup = tenant.agent("Sup", role="supervisor", teams=[t]).login()
    return connect(tenant, tenant.agent("Agent", teams=[t]).login()), sup


def monitor(client, live, method: str = "POST", **body):
    return client.request(method, f"/api/v1/calls/{live.id}/monitor", json=body or None)


def joined_leg(sup, live):
    leg = sup.phone.wait_ring().wait_state("in_progress")
    live.stream.wait_for("supervisor.joined", conversation_id=live.id)
    return leg


def live_sms(world, tenant, agent):
    """An SMS from CUSTOMER that `agent` accepted. Returns (conversation id, stream)."""
    stream = tenant.stream(after=0)
    number, _ = text_line(tenant, agent)
    world.plivo.text(number, CUSTOMER, "Hi")
    return take(stream, agent), stream


def text_transfer(agent, cid: str, target: dict, **body):
    return agent.api().post(f"/api/v1/conversations/{cid}/transfers", json={"target": target, **body})


def test_the_caller_holds_while_a_warm_transfer_consults_and_comes_back_with_the_target(tenant):
    target = tenant.agent("Target").login()
    live = connect(tenant)
    tid = ok(act(live.agent.api(), live.id, "transfers", {"mode": "warm", "target": to_agent(target)}))["transfer_id"]
    target.phone.wait_ring().wait_state("in_progress")
    live.stream.wait_for("offer.accepted", conversation_id=live.id, agent_id=target.id)
    on_hold(tenant, live.caller)
    ok(act(live.agent.api(), live.id, f"transfers/{tid}/complete"))
    live.leg.wait_state("completed")
    on_hold(tenant, live.caller, False)


def test_cancelling_a_transfer_leaves_the_caller_held_until_the_agent_resumes_whoever_held_them(world, tenant):
    first, second = tenant.agent("First").login(), tenant.agent("Second").login()
    live = connect(tenant)
    for target, by_hand in ((first, False), (second, True)):
        if by_hand:
            ok(act(live.agent.api(), live.id, "hold", {"on": True}))
        body = {"mode": "warm", "target": to_agent(target)}
        tid = ok(act(live.agent.api(), live.id, "transfers", body))["transfer_id"]
        leg = target.phone.wait_ring().wait_state("in_progress")
        on_hold(tenant, live.caller)
        ok(act(live.agent.api(), live.id, f"transfers/{tid}/cancel"))
        leg.wait_state("completed")
        world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
        assert member(tenant, live.caller)["hold"] is True
        ok(act(live.agent.api(), live.id, "hold", {"on": False}))
        on_hold(tenant, live.caller, False)


def test_an_unanswered_number_transfer_reports_failed_and_keeps_the_caller_held_until_resumed(world, tenant):
    number, _ = lead(world, "no_answer")
    live = connect(tenant)
    tid = ok(act(tenant.api, live.id, "transfers", {"mode": "warm", "target": to_number(number)}))["transfer_id"]
    failed = live.stream.wait_for("call.transferred", conversation_id=live.id, status="failed")
    assert (failed["data"]["transfer_id"], failed["agent_id"]) == (tid, live.agent.id)
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    on_hold(tenant, live.caller)
    assert live.leg.state == "in_progress"
    ok(act(live.agent.api(), live.id, "hold", {"on": False}))
    on_hold(tenant, live.caller, False)


def test_an_agent_transfer_nobody_answers_reports_failed_to_the_transferring_agent(world, tenant):
    target = tenant.agent("Target").login()
    target.phone.on_ring("ignore")
    live = connect(tenant)
    tid = ok(act(live.agent.api(), live.id, "transfers", {"mode": "cold", "target": to_agent(target)}))["transfer_id"]
    target.phone.wait_ring()
    failed = run_jobs_until(
        world,
        lambda: seen(live.stream, "call.transferred", conversation_id=live.id, status="failed"),
        "the transfer to fail at its ring timeout",
    )[0]
    assert (failed["data"]["transfer_id"], failed["agent_id"]) == (tid, live.agent.id)
    assert live.leg.state == "in_progress"


def test_a_warm_target_that_hangs_up_during_the_consult_reports_failed(tenant):
    target = tenant.agent("Target").login()
    live = connect(tenant)
    tid = ok(act(live.agent.api(), live.id, "transfers", {"mode": "warm", "target": to_agent(target)}))["transfer_id"]
    target.phone.wait_ring().wait_state("in_progress").hangup()
    failed = live.stream.wait_for("call.transferred", conversation_id=live.id, status="failed")
    assert (failed["data"]["transfer_id"], failed["agent_id"]) == (tid, live.agent.id)
    assert live.leg.state == live.caller.state == "in_progress"


def test_a_warm_target_answering_tells_the_transferring_agent(tenant):
    target = tenant.agent("Target").login()
    live = connect(tenant)
    tid = ok(act(live.agent.api(), live.id, "transfers", {"mode": "warm", "target": to_agent(target)}))["transfer_id"]
    target.phone.wait_ring().wait_state("in_progress")
    took = live.stream.wait_for("call.transferred", conversation_id=live.id, status="accepted")
    assert (took["data"]["transfer_id"], took["agent_id"]) == (tid, live.agent.id)
    assert took["data"]["target"] == {"type": "agent", "agent_id": target.id}


def test_a_transfer_start_is_announced_with_its_target(tenant):
    target = tenant.agent("Target").login()
    target.phone.on_ring("ignore")
    live = connect(tenant)
    tid = ok(act(live.agent.api(), live.id, "transfers", {"mode": "cold", "target": to_agent(target)}))["transfer_id"]
    started = live.stream.wait_for("call.transferred", conversation_id=live.id, status="initiated")
    assert (started["data"]["transfer_id"], started["agent_id"]) == (tid, live.agent.id)
    assert started["data"]["target"] == {"type": "agent", "agent_id": target.id}


def test_a_cancelled_transfer_is_announced(tenant):
    target = tenant.agent("Target").login()
    target.phone.on_ring("ignore")
    live = connect(tenant)
    tid = ok(act(live.agent.api(), live.id, "transfers", {"mode": "warm", "target": to_agent(target)}))["transfer_id"]
    target.phone.wait_ring()
    ok(act(live.agent.api(), live.id, f"transfers/{tid}/cancel"))
    cancelled = live.stream.wait_for("call.transferred", conversation_id=live.id, status="cancelled")
    assert (cancelled["data"]["transfer_id"], cancelled["agent_id"]) == (tid, live.agent.id)


def test_a_joined_warm_target_the_agent_drops_is_told_it_was_removed(tenant):
    target = tenant.agent("Target").login()
    live = connect(tenant)
    tid = ok(act(live.agent.api(), live.id, "transfers", {"mode": "warm", "target": to_agent(target)}))["transfer_id"]
    target.phone.wait_ring().wait_state("in_progress")
    live.stream.wait_for("call.transferred", conversation_id=live.id, status="accepted")
    ok(act(live.agent.api(), live.id, f"transfers/{tid}/cancel"))
    live.stream.wait_for("call.transferred", conversation_id=live.id, status="cancelled")
    gone = [
        (e["data"]["participant_id"], e["data"]["reason"], e["agent_id"])
        for e in seen(live.stream, "call.participant_removed")
    ]
    assert gone == [(tid, "removed", target.id)]


@pytest.mark.parametrize("mode", ["cold", "warm"])
def test_a_team_transfer_completes_as_a_team_transfer(tenant, mode):
    billing = team(tenant, "Billing")
    taker = tenant.agent("Member", teams=[billing]).login()
    live = connect(tenant)
    body = {"mode": mode, "target": {"type": "team", "team_id": billing}}
    tid = ok(act(live.agent.api(), live.id, "transfers", body))["transfer_id"]
    taker.phone.wait_ring().wait_state("in_progress")
    if mode == "warm":
        live.stream.wait_for("offer.accepted", conversation_id=live.id, agent_id=taker.id)
        ok(act(live.agent.api(), live.id, f"transfers/{tid}/complete"))
    done = live.stream.wait_for("call.transferred", conversation_id=live.id, status="completed")
    assert (done["data"]["transfer_id"], done["data"]["target"]) == (tid, {"type": "team", "team_id": billing})


def test_a_transfer_whose_ring_plivo_refuses_fails_the_request_and_clears_the_transfer(world, tenant):
    target = tenant.agent("Target").login()
    live = connect(tenant)
    world.plivo.fault(
        "reject", lambda r: r.method == "POST" and r.path.endswith("/Participant/"), account=tenant.auth_id, status=400
    )
    cold = {"mode": "cold", "target": to_agent(target)}
    error(act(live.agent.api(), live.id, "transfers", cold), 422, "plivo_refused")
    ok(act(live.agent.api(), live.id, "transfers", cold))
    target.phone.wait_ring()


def test_a_transfer_whose_ring_plivo_refuses_is_never_announced(world, tenant):
    target = tenant.agent("Target").login()
    target.phone.on_ring("ignore")
    live = connect(tenant)
    world.plivo.fault(
        "reject", lambda r: r.method == "POST" and r.path.endswith("/Participant/"), account=tenant.auth_id, status=400
    )
    cold = {"mode": "cold", "target": to_agent(target)}
    error(act(live.agent.api(), live.id, "transfers", cold), 422, "plivo_refused")
    tid = ok(act(live.agent.api(), live.id, "transfers", cold))["transfer_id"]
    live.stream.wait_for("call.transferred", conversation_id=live.id, status="initiated", transfer_id=tid)
    told = seen(live.stream, "call.transferred", conversation_id=live.id)
    assert [(e["data"]["transfer_id"], e["data"]["status"]) for e in told] == [(tid, "initiated")]


def test_a_transfer_out_of_the_room_is_announced_as_initiated_with_its_target(tenant):
    billing = tenant.queue(name="Billing")
    live = connect(tenant)
    target = {"type": "queue", "queue_id": billing["id"]}
    tid = ok(act(live.agent.api(), live.id, "transfers", {"mode": "cold", "target": target}))["transfer_id"]
    started = live.stream.wait_for("call.transferred", conversation_id=live.id, status="initiated")
    assert (started["data"]["transfer_id"], started["agent_id"]) == (tid, live.agent.id)
    assert started["data"]["target"].items() >= target.items()


def test_a_warm_transfer_switches_between_the_caller_and_the_target_and_merges(tenant):
    target = tenant.agent("Target").login()
    live = connect(tenant)
    tid = ok(act(live.agent.api(), live.id, "transfers", {"mode": "warm", "target": to_agent(target)}))["transfer_id"]
    leg = target.phone.wait_ring().wait_state("in_progress")
    ok(act(live.agent.api(), live.id, "switch", {}))
    on_hold(tenant, leg)
    on_hold(tenant, live.caller, False)
    ok(act(live.agent.api(), live.id, "switch", {"participant_id": tid}))
    on_hold(tenant, live.caller)
    on_hold(tenant, leg, False)
    ok(act(live.agent.api(), live.id, "merge"))
    on_hold(tenant, live.caller, False)
    on_hold(tenant, leg, False)
    assert live.leg.state == "in_progress"


def test_a_transfer_mode_the_tenant_does_not_allow_is_refused_except_a_cold_one_to_a_number(world, tenant):
    settings(tenant, transfer={"modes": "warm"})
    target = tenant.agent("Target").login()
    live = connect(tenant)
    cold = {"mode": "cold", "target": to_agent(target)}
    error(act(live.agent.api(), live.id, "transfers", cold), 409, "transfer_mode_not_allowed")
    number, phone = lead(world)
    ok(act(tenant.api, live.id, "transfers", {"mode": "cold", "target": to_number(number)}))
    phone.wait_ring()
    assert target.phone.legs == []


def test_a_warm_transfer_to_a_number_needs_a_tenant_that_allows_both_modes(world, tenant):
    settings(tenant, transfer={"modes": "warm"})
    live = connect(tenant)
    number, _ = lead(world)
    warm = {"mode": "warm", "target": to_number(number)}
    error(act(tenant.api, live.id, "transfers", warm), 409, "transfer_mode_not_allowed")
    assert rings_to(world, number) == []


def test_a_team_transfer_rings_at_most_the_tenants_simultaneous_agent_limit(world, tenant):
    settings(tenant, simultaneous_agent_limit=1)
    billing = team(tenant, "Billing")
    members = [tenant.agent(f"Member-{i}", teams=[billing]).login() for i in range(2)]
    live = connect(tenant)
    ok(act(live.agent.api(), live.id, "transfers", {"mode": "cold", "target": {"type": "team", "team_id": billing}}))
    world.wait(lambda: any(m.phone.legs for m in members), what="a team member to ring")
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    assert sum(len(m.phone.legs) for m in members) == 1
    assert len(replayed(tenant, "offer.created", conversation_id=live.id, via="transfer")) == 1


def test_a_number_transfer_dials_the_extension_after_the_number(world, tenant):
    number, phone = lead(world)
    live = connect(tenant)
    ok(act(tenant.api, live.id, "transfers", {"mode": "cold", "target": to_number(f"{number}#567")}))
    assert phone.wait_ring().to == number
    assert [p.get("send_digits") for p in rings_to(world, number)] == ["567"]


def test_a_number_transfer_and_a_phone_participant_dial_from_the_business_number(world, tenant):
    (to_add, added), (to_take, taker) = lead(world), lead(world)
    live = connect(tenant)
    ok(act(tenant.api, live.id, "participants", {"target": to_number(to_add)}))
    assert added.wait_ring().from_ == live.number
    ok(act(tenant.api, live.id, "transfers", {"mode": "cold", "target": to_number(to_take)}))
    assert taker.wait_ring().from_ == live.number


def test_only_the_calls_primary_agent_transfers_it_or_changes_its_participants(tenant):
    t = team(tenant, "Floor")
    sup = tenant.agent("Sup", role="supervisor", teams=[t]).login()
    helper, target = tenant.agent("Helper").login(), tenant.agent("Target").login()
    live = connect(tenant, tenant.agent("Owner", teams=[t]).login())
    pid = ok(act(live.agent.api(), live.id, "participants", {"target": to_agent(helper)}))["participant_id"]
    helper.phone.wait_ring().wait_state("in_progress")
    cold = {"mode": "cold", "target": to_agent(target)}
    for who in (helper, sup):
        error(act(who.api(), live.id, "transfers", cold), (403, 404))
    error(act(helper.api(), live.id, "participants", {"target": to_agent(target)}), (403, 404))
    error(helper.api().delete(f"/api/v1/calls/{live.id}/participants/{pid}"), (403, 404))
    assert target.phone.legs == [] and live.leg.state == "in_progress"


def test_only_the_calls_agent_cancels_or_completes_its_transfer(tenant):
    t = team(tenant, "Floor")
    sup = tenant.agent("Sup", role="supervisor", teams=[t]).login()
    target = tenant.agent("Target").login()
    live = connect(tenant, tenant.agent("Owner", teams=[t]).login())
    tid = ok(act(live.agent.api(), live.id, "transfers", {"mode": "warm", "target": to_agent(target)}))["transfer_id"]
    target.phone.wait_ring().wait_state("in_progress")
    live.stream.wait_for("call.transferred", conversation_id=live.id, status="accepted")
    for who in (target, sup):
        for step in ("complete", "cancel"):
            error(act(who.api(), live.id, f"transfers/{tid}/{step}"), (403, 404))
    ok(act(live.agent.api(), live.id, f"transfers/{tid}/complete"))
    live.stream.wait_for("call.transferred", conversation_id=live.id, status="completed")


def test_the_calls_own_agent_cannot_be_added_as_a_participant(tenant):
    live = connect(tenant)
    error(act(live.agent.api(), live.id, "participants", {"target": to_agent(live.agent)}), 422)
    assert len(live.agent.phone.legs) == 1


@pytest.mark.parametrize("via", ["agent", "queue"])
def test_an_agent_who_took_the_call_by_transfer_cannot_add_a_participant(tenant, via):
    target, helper = tenant.agent("Target").login(), tenant.agent("Helper").login()
    live = connect(tenant)
    to = to_agent(target)
    if via == "queue":
        to = {"type": "queue", "queue_id": tenant.queue(name="Billing", agents=[target.id])["id"]}
    ok(act(live.agent.api(), live.id, "transfers", {"mode": "cold", "target": to}))
    target.phone.wait_ring().wait_state("in_progress")
    live.stream.wait_for("offer.accepted", conversation_id=live.id, agent_id=target.id)
    error(act(target.api(), live.id, "participants", {"target": to_agent(helper)}), 409, "participant_not_allowed")
    assert helper.phone.legs == []


def test_a_transfer_back_to_an_agent_still_in_wrap_up_closes_that_wrap_up_with_the_system_disposition(tenant):
    settings(tenant, wrap_up={"mode": "manual", "seconds": 600})
    target = tenant.agent("Target").login()
    live = connect(tenant)
    ok(act(live.agent.api(), live.id, "transfers", {"mode": "cold", "target": to_agent(target)}))
    target.phone.wait_ring().wait_state("in_progress")
    live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id, agent_id=live.agent.id)
    ok(act(target.api(), live.id, "transfers", {"mode": "cold", "target": to_agent(live.agent)}))
    live.agent.phone.wait_ring().wait_state("in_progress")
    done = live.stream.wait_for("conversation.wrapped_up", conversation_id=live.id, agent_id=live.agent.id)
    assert (done["data"]["disposition"], done["data"]["system"]) == ("Transferred back to same agent", True)


def test_a_transfer_to_a_menu_tells_a_barging_supervisor_it_was_removed(tenant):
    live, sup = floor(tenant)
    ok(monitor(sup.api(), live, mode="barge"))
    leg = joined_leg(sup, live)
    m = menu(tenant, {"1": hangup_action("Thanks")}, greeting="Press 1 to rate us")
    ok(act(live.agent.api(), live.id, "transfers", {"mode": "cold", "target": {"type": "menu", "menu_id": m["id"]}}))
    leg.wait_state("completed")
    left = live.stream.wait_for("supervisor.left", conversation_id=live.id)
    assert (left["data"]["supervisor_id"], left["agent_id"]) == (sup.id, sup.id)


def test_a_transfer_to_a_saved_destination_dials_its_number(world, tenant):
    number, phone = lead(world)
    dest = ok(tenant.api.post("/api/v1/transfer-destinations", json={"name": "Billing desk", "number": number}))
    live = connect(tenant)
    target = {"type": "destination", "destination_id": dest["id"]}
    ok(act(tenant.api, live.id, "transfers", {"mode": "cold", "target": target}))
    assert phone.wait_ring().wait_state("in_progress").mpc == live.caller.mpc


def test_saved_destinations_are_unique_by_name_and_an_unknown_one_is_refused(tenant):
    dest = {"name": "Billing desk", "number": "+14155550777"}
    ok(tenant.api.post("/api/v1/transfer-destinations", json=dest))
    assert tenant.api.post("/api/v1/transfer-destinations", json=dest).status_code == 409
    live = connect(tenant)
    target = {"type": "destination", "destination_id": str(uuid4())}
    error(act(tenant.api, live.id, "transfers", {"mode": "cold", "target": target}), 422)


def test_transfer_targets_list_saved_destinations_for_calls_only(tenant):
    dest = ok(tenant.api.post("/api/v1/transfer-destinations", json={"name": "Desk", "number": "+14155550778"}))
    agent = tenant.agent("Lister").login().api()
    calls = ok(agent.get("/api/v1/transfer-targets", params={"channel": "call"}))
    assert [d["id"] for d in calls["destinations"]] == [dest["id"]]
    assert ok(agent.get("/api/v1/transfer-targets", params={"channel": "chat"}))["destinations"] == []
    error(agent.get("/api/v1/transfer-targets", params={"channel": "fax"}), 422)


def test_a_destination_cannot_be_renamed_onto_another_ones_name(tenant):
    for name in ("Sales", "Support"):
        dest = ok(tenant.api.post("/api/v1/transfer-destinations", json={"name": name, "number": "+14155550777"}))
    path = f"/api/v1/transfer-destinations/{dest['id']}"
    error(tenant.api.patch(path, json={"name": "Sales"}), 409, "destination_name_taken")
    assert ok(tenant.api.patch(path, json={"name": "Support", "number": "+14155550778"}))["number"] == "+14155550778"


def test_a_saved_destination_holds_a_redirect_that_a_cold_transfer_takes_and_a_warm_one_refuses(world, tenant):
    app = world.plivo.app("<Response><Speak>Welcome to self service</Speak></Response>")
    both = {"name": "Both", "number": "+14155550777", "url": app}
    error(tenant.api.post("/api/v1/transfer-destinations", json=both), 422)
    dest = ok(tenant.api.post("/api/v1/transfer-destinations", json={"name": "Self service", "url": app}))
    live = connect(tenant)
    target = {"type": "destination", "destination_id": dest["id"]}
    error(act(tenant.api, live.id, "transfers", {"mode": "warm", "target": target}), 422, "warm_not_supported")
    ok(act(tenant.api, live.id, "transfers", {"mode": "cold", "target": target}))
    world.wait(lambda: "Welcome to self service" in live.caller.said, what="the saved redirect")


def test_the_transfer_picker_lists_every_agent_and_team_with_the_unavailable_last(tenant):
    alpha, beta = team(tenant, "Alpha"), team(tenant, "Beta")
    away = tenant.agent("Aaron", teams=[alpha]).login()
    away.set_status("Away")
    free = tenant.agent("Zoe", teams=[beta]).login()
    picker = ok(tenant.agent("Lister").login().api().get("/api/v1/transfer-targets"))
    assert [a["id"] for a in picker["agents"]] == [free.id, away.id]
    assert [t["id"] for t in picker["teams"]] == [beta, alpha]
    assert [a["availability"] for a in picker["agents"]] == ["available", "unavailable"]
    assert [t["available_agents"] for t in picker["teams"]] == [1, 0]


def test_an_agent_full_on_chat_is_busy_in_the_chat_picker_and_free_in_the_call_one(tenant):
    settings(tenant, capacity={"chat": 1})
    desk = team(tenant, "Desk")
    full = tenant.agent("Full", teams=[desk]).login()
    stream = tenant.stream(after=0)
    queue = tenant.queue(agents=[full.id])
    chat = ok(enqueue_text(tenant.api, queue, "chat", "visitor-71", "support-inbox", text="Hi"))["conversation_id"]
    take(stream, full, conversation_id=chat)
    lister = tenant.agent("Lister").login().api()

    def seen(channel: str) -> tuple[str, int]:
        picker = ok(lister.get("/api/v1/transfer-targets", params={"channel": channel}))
        agent = next(a for a in picker["agents"] if a["id"] == full.id)
        return agent["availability"], next(t for t in picker["teams"] if t["id"] == desk)["available_agents"]

    assert seen("chat") == ("busy", 0)
    assert seen("call") == ("available", 1)


def test_an_agent_given_one_team_twice_is_in_it_once_and_the_picker_counts_it_once(tenant):
    desk = team(tenant, "Desk")
    twice = tenant.agent("Twice").login()
    ok(tenant.api.patch(f"/api/v1/agents/{twice.id}", json={"teams": [desk, desk]}))
    picker = ok(tenant.agent("Lister").login().api().get("/api/v1/transfer-targets"))
    assert [t["available_agents"] for t in picker["teams"]] == [1]
    assert ok(tenant.api.get(f"/api/v1/agents/{twice.id}"))["teams"] == [desk]


def test_a_transfer_hand_over_restores_the_tenants_recording_after_a_pause(world, tenant):
    settings(tenant, recording={"inbound": True, "allow_pause": True})
    target = tenant.agent("Target").login()
    live = connect(tenant)
    ok(act(live.agent.api(), live.id, "recording", {"action": "pause"}))
    live.stream.wait_for("call.recording_paused", conversation_id=live.id)

    def resumed() -> int:
        return len([r for r in world.plivo.requests if tenant.auth_id in r.path and r.path.endswith("/Record/Resume/")])

    before = resumed()
    ok(act(live.agent.api(), live.id, "transfers", {"mode": "cold", "target": to_agent(target)}))
    target.phone.wait_ring().wait_state("in_progress")
    live.stream.wait_for("call.transferred", conversation_id=live.id, status="completed")
    world.wait(lambda: resumed() > before, what="the recording resumed for the new agent")


def test_adding_a_participant_holds_the_others_until_the_agent_merges_holds_or_switches(world, tenant):
    number, phone = lead(world)
    live = connect(tenant)
    pid = ok(act(tenant.api, live.id, "participants", {"target": to_number(number)}))["participant_id"]
    extra = phone.wait_ring().wait_state("in_progress")
    on_hold(tenant, live.caller)
    ok(act(live.agent.api(), live.id, "merge"))
    on_hold(tenant, live.caller, False)
    on_hold(tenant, extra, False)
    ok(act(live.agent.api(), live.id, "hold", {"on": True, "participant_id": pid}))
    on_hold(tenant, extra)
    held = live.stream.wait_for("call.held", conversation_id=live.id, participant_id=pid)
    assert held["data"]["by_agent_id"] == live.agent.id and member(tenant, live.caller)["hold"] is False
    ok(act(live.agent.api(), live.id, "switch", {"participant_id": pid}))
    on_hold(tenant, live.caller)
    on_hold(tenant, extra, False)
    ok(act(live.agent.api(), live.id, "hold", {"on": True, "participant_id": "all"}))
    on_hold(tenant, extra)
    on_hold(tenant, live.caller)


def test_a_participant_is_announced_when_it_joins_and_never_when_it_does_not_answer(world, tenant):
    (silent, quiet), (number, phone) = lead(world, "no_answer"), lead(world)
    live = connect(tenant)
    unanswered = ok(act(tenant.api, live.id, "participants", {"target": to_number(silent)}))["participant_id"]
    quiet.wait_ring().wait_state("completed")
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    assert not replayed(tenant, "call.participant_added", participant_id=unanswered)
    pid = ok(act(tenant.api, live.id, "participants", {"target": to_number(number)}))["participant_id"]
    phone.wait_ring().wait_state("in_progress")
    added = live.stream.wait_for("call.participant_added", conversation_id=live.id, participant_id=pid)
    assert added["data"]["participant"]["identifier"] == number


def test_a_participant_outcome_says_whether_it_never_answered_left_or_was_removed(world, tenant):
    live = connect(tenant)
    reasons = []
    for behaviour, end in (("no_answer", None), ("answer", "hangup"), ("answer", "api")):
        number, phone = lead(world, behaviour)
        pid = ok(act(tenant.api, live.id, "participants", {"target": to_number(number)}))["participant_id"]
        leg = phone.wait_ring()
        if end:
            leg.wait_state("in_progress")
        if end == "hangup":
            leg.hangup()
        elif end == "api":
            ok(tenant.api.delete(f"/api/v1/calls/{live.id}/participants/{pid}"))
        reasons.append(live.stream.wait_for("call.participant_removed", participant_id=pid)["data"]["reason"])
    assert reasons == ["no_answer", "left", "removed"]


def test_an_added_leg_tells_the_softphone_it_is_a_participant_with_its_alias(tenant):
    third = tenant.agent("Third").login()
    live = connect(tenant)
    alias = "Billing specialist on duty"
    ok(act(live.agent.api(), live.id, "participants", {"target": to_agent(third), "alias": alias}))
    leg = third.phone.wait_ring()
    assert hs_header(leg, "HsKind") == "participant"
    assert hs_header(leg, "HsAlias") == alias


def test_a_participants_alias_reaches_the_softphone_as_its_first_64_encoded_characters(tenant):
    third = tenant.agent("Third").login()
    live = connect(tenant)
    alias = " ".join("abcdefghijklmnopqrstuvw")
    ok(act(live.agent.api(), live.id, "participants", {"target": to_agent(third), "alias": alias}))
    assert hs_header(third.phone.wait_ring(), "HsAlias") == "a b c d e f g h i j k l m n o p "


def test_a_withheld_caller_that_is_not_restricted_rings_with_the_from_it_sent(world, tenant):
    agent = tenant.agent("Agent").login()
    number, _ = tenant.inbound_line(agent)
    world.plivo.call(number, from_="anonymous")
    leg = agent.phone.wait_ring()
    assert (leg.from_, rings_to(world, agent.endpoint_username)[0].get("caller_name")) == ("anonymous", "anonymous")


def test_an_agent_hangup_whose_participant_exit_never_comes_ends_the_stint(world, tenant):
    number, phone = lead(world)
    live = connect(tenant)
    ok(act(tenant.api, live.id, "participants", {"target": to_number(number)}))
    phone.wait_ring().wait_state("in_progress")

    def exit_of(r) -> bool:
        return (
            r.params.get("EventName") == "ParticipantExit" and r.params.get("ParticipantCallUUID") == live.leg.call_uuid
        )

    lost = world.plivo.fault("drop", exit_of, account=tenant.auth_id)
    live.leg.hangup()
    live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id, agent_id=live.agent.id)
    assert lost.hits and live.caller.state == "in_progress"


def test_a_withheld_caller_rings_the_agent_and_the_supervisor_from_the_business_number_as_no_caller_id(world, tenant):
    t = team(tenant, "Floor")
    sup = tenant.agent("Sup", role="supervisor", teams=[t]).login()
    agent = tenant.agent("Agent", teams=[t]).login()
    number, _ = tenant.inbound_line(agent)
    stream = tenant.stream(after=0)
    world.plivo.call(number, from_="restricted")
    leg = agent.phone.wait_ring()
    cid = hs_header(leg, "HsConversation")
    stream.wait_for("offer.accepted", conversation_id=cid)
    ok(sup.api().post(f"/api/v1/calls/{cid}/monitor", json={"mode": "listen"}))
    assert (leg.from_, sup.phone.wait_ring().from_) == (number, number)
    names = [rings_to(world, who.endpoint_username)[0].get("caller_name") for who in (agent, sup)]
    assert names == ["No Caller ID", "No Caller ID"]


@pytest.mark.parametrize("withheld", ["anonymous", "restricted", "1000000000"])
def test_a_withheld_caller_reads_as_anonymous(world, tenant, withheld):
    number, _ = tenant.inbound_line(unavailable(tenant))
    stream = tenant.stream(after=0)
    world.plivo.call(number, from_=withheld)
    cid = stream.wait_for("conversation.queued")["conversation_id"]
    assert ok(tenant.api.get(f"/api/v1/conversations/{cid}"))["number"] == "Anonymous"


def test_the_agents_ring_shows_the_caller(world, tenant):
    live = connect(tenant)
    ring = rings_to(world, live.agent.endpoint_username)[0]
    assert str(ring.get("caller_name", "")).lstrip("+") == live.caller.from_.lstrip("+")


def test_a_ring_tells_the_softphone_the_team_the_numbers_alias_and_who_transferred(world, tenant):
    billing, escalations = team(tenant, "Billing"), team(tenant, "Escalations")
    first = tenant.agent("Agent-One", teams=[billing]).login()
    second = tenant.agent("Agent-Two", teams=[escalations]).login()
    number, _ = tenant.inbound_line(first, teams=[billing])
    url = f"{world.plivo.url}/v1/Account/{tenant.auth_id}/Number/{number.removeprefix('+')}/"
    assert httpx.post(url, data={"alias": "Support line"}, auth=(tenant.auth_id, tenant.auth_token)).is_success
    stream = tenant.stream(after=0)
    world.plivo.call(number)
    leg = first.phone.wait_ring()
    cid = hs_header(leg, "HsConversation")
    assert (hs_header(leg, "HsTeam"), hs_header(leg, "HsAlias")) == ("Billing", "Support line")
    stream.wait_for("offer.accepted", conversation_id=cid)
    to_team = {"mode": "cold", "target": {"type": "team", "team_id": escalations}}
    ok(act(first.api(), cid, "transfers", to_team))
    ring = second.phone.wait_ring()
    conversation_number = str(ok(tenant.api.get(f"/api/v1/conversations/{cid}"))["conversation_number"])
    got = [hs_header(ring, h) for h in ("HsFromAgent", "HsTeam", "HsConversationNumber")]
    assert got == ["Agent-One", "Escalations", conversation_number]


@pytest.mark.parametrize("exit_after_events", [False, True])
def test_a_call_the_agent_ends_reads_ended_by_agent(world, tenant, monkeypatch, exit_after_events):
    monkeypatch.setattr(world.plivo, "exit_after_events", exit_after_events)
    live = connect(tenant)
    live.leg.hangup()
    assert live.stream.wait_for("call.ended", timeout=3, conversation_id=live.id)["data"]["ended_by"] == "agent"


def test_the_customer_leg_ends_at_the_callers_hangup(world, tenant):
    settings(tenant, wrap_up={"mode": "manual", "seconds": 600})
    live = connect(tenant)

    def customer() -> tuple[str, str | None]:
        found = ok(tenant.api.get(f"/api/v1/conversations/{live.id}"))
        return found["state"], next(leg for leg in found["legs"] if leg["role"] == "customer")["ended_at"]

    assert customer() == ("active", None)
    late = world.plivo.fault("delay", lambda r: "/plivo/" in r.path, account=tenant.auth_id, times=math.inf)

    def exit_time() -> str | None:
        mine = {"EventName": "ParticipantExit", "ParticipantCallUUID": live.caller.call_uuid}
        return next((h.params["ParticipantExitTime"] for h in late.hits if mine.items() <= h.params.items()), None)

    live.caller.hangup()
    left = datetime.fromisoformat(world.wait(exit_time, what="Plivo's exit of the caller") + "+00:00")
    world.wait(lambda: time.time() >= left.timestamp() + 1, what="a second after the caller left")
    late.release()
    live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id)
    state, ended = world.wait(lambda: (c := customer())[1] and c, what="the customer leg's end")
    assert state == "in_wrap_up" and datetime.fromisoformat(ended).replace(microsecond=0) == left


def test_a_call_whose_end_hs_never_hears_ends_at_the_call_session_timeout(world, tenant):
    live = connect(tenant)
    world.plivo.fault("drop", lambda r: "/plivo/" in r.path, account=tenant.auth_id, times=math.inf)
    world.plivo.fault(
        "reject", lambda r: r.method == "GET" and "/MultiPartyCall/" in r.path, account=tenant.auth_id, times=math.inf
    )
    live.caller.hangup()
    live.caller.wait_state("completed")
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    world.advance(86400 - 60)
    assert ok(tenant.api.get(f"/api/v1/conversations/{live.id}"))["state"] == "active"
    world.advance(86400 + 60)
    live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id, agent_id=live.agent.id)


def test_an_outbound_call_with_no_from_dials_from_the_tenants_default_number(world, tenant):
    number = registered_number(tenant)
    settings(tenant, default_from={"call": number})
    agent = tenant.agent().login()
    to, phone = lead(world)
    r = tenant.api.post("/api/v1/calls", json={"agent_id": agent.id, "to": to})
    assert r.status_code == 201, r.text
    assert phone.wait_ring().from_ == number


def test_an_agent_calls_the_customer_from_its_text_conversation_and_the_call_joins_it(world, tenant):
    agent = tenant.agent().login()
    cid, _ = live_sms(world, tenant, agent)
    phone = world.plivo.lead(CUSTOMER)
    body = {"agent_id": agent.id, "to": CUSTOMER, "from": registered_number(tenant), "conversation_id": cid}
    r = tenant.api.post("/api/v1/calls", json=body)
    assert r.status_code == 201, r.text
    assert r.json()["conversation_id"] == cid
    phone.wait_ring()


def test_an_agent_calls_a_chat_visitor_at_a_phone_number_and_the_call_joins_the_chat(world, tenant):
    agent, stream = tenant.agent().login(), tenant.stream(after=0)
    queue = tenant.queue(agents=[agent.id])
    cid = ok(enqueue_text(tenant.api, queue, "chat", "visitor-7", "support-inbox", text="Hi"))["conversation_id"]
    take(stream, agent, conversation_id=cid)
    phone = world.plivo.lead(CUSTOMER)
    body = {"agent_id": agent.id, "to": CUSTOMER, "from": registered_number(tenant), "conversation_id": cid}
    error(tenant.api.post("/api/v1/calls", json=body | {"to": "visitor-7"}), 422, "invalid_number")
    assert ok(tenant.api.post("/api/v1/calls", json=body))["conversation_id"] == cid
    phone.wait_ring()
    assert stream.wait_for("call.started", conversation_id=cid)["data"]["to"] == CUSTOMER


def test_a_call_joined_to_a_text_conversation_ends_without_closing_it(world, tenant):
    agent = tenant.agent().login()
    cid, stream = live_sms(world, tenant, agent)
    phone = world.plivo.lead(CUSTOMER)
    body = {"agent_id": agent.id, "to": CUSTOMER, "from": registered_number(tenant), "conversation_id": cid}
    ok(tenant.api.post("/api/v1/calls", json=body))
    leg = phone.wait_ring()
    stream.wait_for("call.answered", conversation_id=cid)
    leg.hangup()
    stream.wait_for("call.ended", conversation_id=cid)
    assert ok(tenant.api.get(f"/api/v1/conversations/{cid}"))["state"] == "active"
    other = tenant.agent("Other").login()
    error(tenant.api.post("/api/v1/calls", json=body | {"agent_id": other.id}), 409, "conversation_not_joinable")


def test_closing_a_text_conversation_while_its_call_is_on_ends_the_call(world, tenant):
    agent = tenant.agent().login()
    cid, stream = live_sms(world, tenant, agent)
    phone = world.plivo.lead(CUSTOMER)
    body = {"agent_id": agent.id, "to": CUSTOMER, "from": registered_number(tenant), "conversation_id": cid}
    ok(tenant.api.post("/api/v1/calls", json=body))
    leg = phone.wait_ring()
    stream.wait_for("call.answered", conversation_id=cid)
    ok(agent.api().post(f"/api/v1/conversations/{cid}/close"))
    assert stream.wait_for("call.ended", conversation_id=cid)["data"]["ended_by"] == "system"
    leg.wait_state("completed")
    agent.phone.legs[-1].wait_state("completed")


def test_closing_a_text_conversation_while_its_agents_phone_rings_cancels_the_call(world, tenant):
    settings(tenant, ring_timeout_sec=120)
    agent = tenant.agent().login()
    cid, stream = live_sms(world, tenant, agent)
    agent.phone.on_ring("ignore")
    body = {"agent_id": agent.id, "to": CUSTOMER, "from": registered_number(tenant), "conversation_id": cid}
    ok(tenant.api.post("/api/v1/calls", json=body))
    ring = agent.phone.wait_ring()
    world.wait(world.plivo.idle, what="Plivo's answer to the agent's dial to reach HS")
    ok(agent.api().post(f"/api/v1/conversations/{cid}/close"))
    assert ring.wait_state("completed").hangup_cause_code == 1000
    assert stream.wait_for("call.failed", conversation_id=cid)["data"]["result"] == "cancelled"
    assert stream.wait_for("call.ended", conversation_id=cid)["data"]["ended_by"] == "system"


def test_an_agent_already_dialing_cannot_call_from_its_text_conversation(world, tenant):
    agent = tenant.agent().login()
    cid, _ = live_sms(world, tenant, agent)
    sender, (to, _) = registered_number(tenant), lead(world)
    ok(tenant.api.post("/api/v1/calls", json={"agent_id": agent.id, "to": to, "from": sender}))
    body = {"agent_id": agent.id, "to": CUSTOMER, "from": sender, "conversation_id": cid}
    error(tenant.api.post("/api/v1/calls", json=body), 409, "agent_on_call")


def test_two_calls_one_after_the_other_from_a_text_conversation_both_answer(world, tenant):
    agent = tenant.agent().login()
    cid, stream = live_sms(world, tenant, agent)
    phone = world.plivo.lead(CUSTOMER)
    body = {"agent_id": agent.id, "to": CUSTOMER, "from": registered_number(tenant), "conversation_id": cid}
    for _ in range(2):
        ok(tenant.api.post("/api/v1/calls", json=body))
        leg = phone.wait_ring()
        stream.wait_for("call.answered", conversation_id=cid, call_uuid=leg.call_uuid)
        leg.hangup()
        stream.wait_for("call.ended", conversation_id=cid)


def test_a_call_from_an_sms_conversation_dials_the_customer_from_the_number_it_asked_for(world, tenant):
    agent = tenant.agent().login()
    cid, stream = live_sms(world, tenant, agent)
    phone, sender = world.plivo.lead(CUSTOMER), registered_number(tenant)
    body = {"agent_id": agent.id, "to": CUSTOMER, "from": sender, "conversation_id": cid}
    ok(tenant.api.post("/api/v1/calls", json=body))
    assert phone.wait_ring().from_ == sender
    assert stream.wait_for("call.started", conversation_id=cid)["data"]["from"] == sender


def test_call_feedback_posts_the_rating_to_plivo_with_its_issue_codes(world, tenant):
    live = connect(tenant)
    live.caller.hangup()
    live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id)
    body = {"rating": 3, "issue_type": "no_audio", "comment": "The caller could not hear me"}
    ok(live.agent.api().post(f"/api/v1/calls/{live.id}/feedback", json=body))
    path = f"/Call/{live.caller.call_uuid}/Feedback"
    sent = world.wait(
        lambda: [r.params for r in world.plivo.requests if r.method == "POST" and path in r.path],
        what="the feedback at Plivo",
    )
    assert (sent[0]["rating"], sent[0]["issues"], sent[0]["notes"]) == (3, ["ONE_WAY_AUDIO"], body["comment"])


def test_call_feedback_needs_an_issue_type_at_four_or_below_and_a_call(world, tenant):
    live = connect(tenant)
    feedback = f"/api/v1/calls/{live.id}/feedback"
    for body in ({"rating": 4}, {"rating": 2, "issue_type": "bad_line"}, {"rating": 6, "issue_type": "others"}):
        error(live.agent.api().post(feedback, json=body), 422)
    ok(live.agent.api().post(feedback, json={"rating": 5}))
    texter = tenant.agent("Texter").login()
    cid, _ = live_sms(world, tenant, texter)
    error(texter.api().post(f"/api/v1/calls/{cid}/feedback", json={"rating": 5}), 404)


def test_call_feedback_is_refused_for_a_call_no_agent_took(world, tenant):
    number, _ = tenant.inbound_line(unavailable(tenant))
    stream = tenant.stream(after=0)
    world.plivo.call(number)
    cid = stream.wait_for("conversation.queued")["conversation_id"]
    error(tenant.api.post(f"/api/v1/calls/{cid}/feedback", json={"rating": 5}), 422, "call_not_answered")


@pytest.mark.parametrize("by", ["agent", "key"])
@pytest.mark.parametrize("step", ["cancel", "complete", "cold"])
def test_feedback_after_a_transfer_names_the_call_s_primary_agent_whoever_posts_it(tenant, by, step):
    target = tenant.agent("Agent-Two").login()
    live = connect(tenant, tenant.agent("Agent-One").login())
    mode = "cold" if step == "cold" else "warm"
    tid = ok(act(live.agent.api(), live.id, "transfers", {"mode": mode, "target": to_agent(target)}))["transfer_id"]
    target.phone.wait_ring().wait_state("in_progress")
    if mode == "warm":
        live.stream.wait_for("offer.accepted", conversation_id=live.id, agent_id=target.id)
        ok(act(live.agent.api(), live.id, f"transfers/{tid}/{step}"))
    status = "cancelled" if step == "cancel" else "completed"
    live.stream.wait_for("call.transferred", conversation_id=live.id, status=status)
    poster = live.agent.api() if by == "agent" else tenant.api
    ok(poster.post(f"/api/v1/calls/{live.id}/feedback", json={"rating": 5}))
    live.caller.hangup()
    closed(tenant, live.stream, live.id, *([live.agent] if step == "cancel" else [live.agent, target]))
    primary = "Agent-One" if step == "cancel" else "Agent-Two"
    assert [r["agent_name"] for r in content(tenant.api) if r["type"] == "feedback"] == [primary]


def test_a_supervisor_on_a_call_cannot_monitor_another(tenant):
    live, sup = floor(tenant)
    connect(tenant, sup)
    error(monitor(sup.api(), live, mode="listen"), 409, "supervisor_busy")
    assert len(sup.phone.legs) == 1


def test_a_supervisor_moves_to_another_call_on_the_same_leg(tenant):
    t = team(tenant, "Floor")
    sup = tenant.agent("Sup", role="supervisor", teams=[t]).login()
    first, second = (connect(tenant, tenant.agent(name, teams=[t]).login()) for name in ("First", "Second"))
    ok(monitor(sup.api(), first, mode="listen"))
    leg = joined_leg(sup, first)
    ok(monitor(sup.api(), second, mode="whisper"))
    tenant.world.wait(lambda: leg.mpc == second.caller.mpc, what="the supervisor's leg in the second call")
    left = first.stream.wait_for("supervisor.left", conversation_id=first.id)
    joined = second.stream.wait_for("supervisor.joined", conversation_id=second.id)
    assert (left["data"]["supervisor_id"], joined["data"]["mode"]) == (sup.id, "whisper")
    assert len(sup.phone.legs) == 1 and leg.state == "in_progress"
    assert (member(tenant, leg)["mute"], member(tenant, leg)["coach_mode"]) == (False, True)


def test_a_moved_supervisor_who_hangs_up_leaves_the_second_call_and_the_first_call_answers_the_status(world, tenant):
    """A moved leg keeps the call-status callback of the ring that dialed it, so its hangup reaches the call it left."""
    t = team(tenant, "Floor")
    sup = tenant.agent("Sup", role="supervisor", teams=[t]).login()
    first, second = (connect(tenant, tenant.agent(name, teams=[t]).login()) for name in ("First", "Second"))
    ok(monitor(sup.api(), first, mode="listen"))
    leg = joined_leg(sup, first)
    ok(monitor(sup.api(), second, mode="listen"))
    second.stream.wait_for("supervisor.joined", conversation_id=second.id)
    leg.hangup()
    second.stream.wait_for("supervisor.left", conversation_id=second.id)
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    hangup = {"CallUUID": leg.call_uuid, "EventName": "Hangup"}
    status = [c.status for c in world.plivo.callbacks if hangup.items() <= c.params.items() and "call-status" in c.url]
    assert status == [200], status
    assert [len(seen(c.stream, "supervisor.left", conversation_id=c.id)) for c in (first, second)] == [1, 1]


def test_a_status_for_a_moved_supervisor_leg_that_beats_its_join_is_answered_and_binds_it_once(world, tenant):
    """Plivo may send a moved leg's call-status to the URL its move gave, and that can beat the room's join."""
    t = team(tenant, "Floor")
    sup = tenant.agent("Sup", role="supervisor", teams=[t]).login()
    first, second = (connect(tenant, tenant.agent(name, teams=[t]).login()) for name in ("First", "Second"))
    ok(monitor(sup.api(), first, mode="listen"))
    leg = joined_leg(sup, first)
    room = f"/mpc-status?c={second.id}"
    joins = world.plivo.fault("delay", lambda r: room in r.path, account=tenant.auth_id, times=math.inf)
    ok(monitor(sup.api(), second, mode="listen"))
    move = next(r for r in world.plivo.requests if r.params.get("call_uuid") == leg.call_uuid)
    url = move.params["call_status_callback_url"].partition("#")[0].split(f"/plivo/{tenant.id}/", 1)[1]
    answered = {"CallUUID": leg.call_uuid, "EventName": "Answered"}
    params = next(c.params for c in world.plivo.callbacks if answered.items() <= c.params.items())
    assert url.startswith(f"call-status?c={second.id}&")
    join = {"EventName": "ParticipantJoin", "ParticipantCallUUID": leg.call_uuid}
    world.wait(lambda: any(join.items() <= r.params.items() for r in joins.hits), what="the second call's join held")
    assert plivo_post(world, tenant.id, url, params, tenant.auth_token).status_code == 200
    joins.release()
    second.stream.wait_for("supervisor.joined", conversation_id=second.id)
    leg.hangup()
    second.stream.wait_for("supervisor.left", conversation_id=second.id)
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    assert not [c for c in world.plivo.callbacks if leg.call_uuid in c.params.values() and c.status >= 500]
    assert [len(seen(c.stream, "supervisor.left", conversation_id=c.id)) for c in (first, second)] == [1, 1]


def test_a_supervisor_mutes_and_unmutes_its_own_leg_and_keeps_its_mode(tenant):
    live, sup = floor(tenant)
    ok(monitor(sup.api(), live, mode="barge"))
    leg = joined_leg(sup, live)
    mute = f"/api/v1/calls/{live.id}/monitor/mute"
    for on in (True, False):
        ok(sup.api().post(mute, json={"on": on}))
        tenant.world.wait(lambda on=on: member(tenant, leg).get("mute") is on, what=f"the supervisor's mute {on}")
        assert member(tenant, leg)["coach_mode"] is False


def test_a_silent_listener_is_hidden_from_the_agent_until_it_reveals_itself(tenant):
    live, sup = floor(tenant)
    ok(monitor(sup.api(), live, mode="listen"))
    leg = joined_leg(sup, live)
    assert seen(live.stream, "supervisor.joined", conversation_id=live.id)[0]["data"]["visible"] is False
    ok(monitor(sup.api(), live, "DELETE"))
    leg.wait_state("completed")
    assert live.stream.wait_for("supervisor.left", conversation_id=live.id)["data"]["visible"] is False
    ok(monitor(sup.api(), live, mode="listen"))
    leg = sup.phone.wait_ring().wait_state("in_progress")
    tenant.world.wait(lambda: len(seen(live.stream, "supervisor.joined")) == 2, what="the second join")
    ok(sup.api().post(f"/api/v1/calls/{live.id}/monitor/reveal"))
    shown = live.stream.wait_for("supervisor.revealed", conversation_id=live.id)
    assert (shown["data"]["supervisor_id"], shown["agent_id"]) == (sup.id, live.agent.id)
    ok(monitor(sup.api(), live, "DELETE"))
    leg.wait_state("completed")
    tenant.world.wait(lambda: len(seen(live.stream, "supervisor.left")) == 2, what="the second leave")
    left = [e["data"]["visible"] for e in seen(live.stream, "supervisor.left", conversation_id=live.id)]
    assert left == [False, True]


def test_the_agents_own_stream_hears_nothing_of_a_silent_listener_until_it_reveals_itself(tenant):
    live, sup = floor(tenant)
    desk, watch = (tenant.stream(token=who.stream_token, after=0) for who in (live.agent, sup))
    ok(monitor(sup.api(), live, mode="listen"))
    joined_leg(sup, live)
    watch.wait_for("supervisor.joined", conversation_id=live.id)
    ok(sup.api().post(f"/api/v1/calls/{live.id}/monitor/reveal"))
    revealed = desk.wait_for("supervisor.revealed", conversation_id=live.id)
    told = [
        e["type"] for e in desk.events if e["type"].startswith("supervisor.") and int(e["id"]) <= int(revealed["id"])
    ]
    assert told == ["supervisor.revealed"]


def test_an_agent_asks_the_supervisor_for_help_and_the_supervisor_declines(tenant):
    live, sup = floor(tenant)
    assistance = f"/api/v1/calls/{live.id}/monitor/assistance"
    error(live.agent.api().post(assistance), 422, "not_monitored")
    ok(monitor(sup.api(), live, mode="listen"))
    joined_leg(sup, live)
    ok(live.agent.api().post(assistance))
    asked = live.stream.wait_for("supervisor.assistance_requested", conversation_id=live.id)
    assert (asked["data"]["supervisor_id"], asked["agent_id"]) == (sup.id, live.agent.id)
    ok(sup.api().delete(assistance))
    live.stream.wait_for("supervisor.assistance_rejected", conversation_id=live.id, supervisor_id=sup.id)


def test_a_cold_transfer_to_a_number_drops_the_supervisor(tenant):
    live, sup = floor(tenant)
    ok(monitor(sup.api(), live, mode="listen"))
    leg = joined_leg(sup, live)
    number, phone = lead(tenant.world)
    ok(act(tenant.api, live.id, "transfers", {"mode": "cold", "target": to_number(number)}))
    phone.wait_ring().wait_state("in_progress")
    leg.wait_state("completed")
    assert live.stream.wait_for("supervisor.left", conversation_id=live.id)["data"]["supervisor_id"] == sup.id


def test_a_supervisor_whose_phone_never_answers_is_told(tenant):
    live, sup = floor(tenant)
    sup.phone.on_ring("timeout")
    ok(monitor(sup.api(), live, mode="listen"))
    failed = live.stream.wait_for("supervisor.failed", conversation_id=live.id)
    assert (failed["data"]["supervisor_id"], failed["agent_id"]) == (sup.id, sup.id)


def test_a_supervisor_ring_plivo_refuses_is_the_monitor_requests_error(tenant):
    live, sup = floor(tenant)
    tenant.world.plivo.fault(
        "reject", lambda r: r.method == "POST" and r.path.endswith("/Participant/"), account=tenant.auth_id, status=400
    )
    assert not monitor(sup.api(), live, mode="listen").is_success
    ok(monitor(sup.api(), live, mode="listen"))
    sup.phone.wait_ring()


def test_a_supervisor_joins_and_leaves_without_a_sound(world, tenant):
    live, sup = floor(tenant)
    ok(monitor(sup.api(), live, mode="listen"))
    sup.phone.wait_ring()
    ring = rings_to(world, sup.endpoint_username)[0]
    assert (ring.get("enter_sound"), ring.get("exit_sound")) == ("none", "none")


def test_a_supervisors_phone_rings_for_45_seconds_whatever_the_agents_ring_timeout(world, tenant):
    settings(tenant, ring_timeout_sec=20)
    live, sup = floor(tenant)
    ok(monitor(sup.api(), live, mode="listen"))
    sup.phone.wait_ring()
    rings = [int(rings_to(world, who.endpoint_username)[0]["ring_timeout"]) for who in (live.agent, sup)]
    assert rings == [20, 45]


def test_a_participants_phone_rings_for_45_seconds_whatever_the_agents_ring_timeout(world, tenant):
    settings(tenant, ring_timeout_sec=20)
    helper = tenant.agent("Helper").login()
    helper.phone.on_ring("ignore")
    live = connect(tenant)
    number, phone = lead(world)
    ok(act(live.agent.api(), live.id, "participants", {"target": to_agent(helper)}))
    ok(act(tenant.api, live.id, "participants", {"target": to_number(number)}))
    helper.phone.wait_ring()
    phone.wait_ring()
    who = (live.agent.endpoint_username, helper.endpoint_username, number)
    assert [int(rings_to(world, w)[0]["ring_timeout"]) for w in who] == [20, 45, 45]
    offer = live.stream.wait_for("offer.created", conversation_id=live.id, agent_id=helper.id)
    assert offer["data"]["ring_timeout_sec"] == 45


def test_a_supervisor_cannot_join_a_room_that_ended_unheard(world, tenant):
    live, sup = floor(tenant)
    world.plivo.fault("drop", lambda r: "/plivo/" in r.path, account=tenant.auth_id, times=math.inf)
    live.caller.hangup()
    live.caller.wait_state("completed")
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    error(monitor(sup.api(), live, mode="listen"), 409, "call_not_live")
    assert sup.phone.legs == []


def test_a_call_that_also_texts_its_caller_cannot_be_monitored(tenant):
    live, sup = floor(tenant)
    ok(live.agent.api().post(f"/api/v1/conversations/{live.id}/messages", json={"channel": "sms", "text": "A link"}))
    error(monitor(sup.api(), live, mode="listen"), 409, "omnichannel_not_supported")
    assert sup.phone.legs == []


def test_of_two_supervisors_asking_at_once_one_rings_and_the_other_is_told(tenant):
    live, sup = floor(tenant)
    teams = ok(tenant.api.get(f"/api/v1/agents/{live.agent.id}"))["teams"]
    other = tenant.agent("Other", role="supervisor", teams=teams).login()
    with ThreadPoolExecutor(2) as pool:
        answers = list(pool.map(lambda who: monitor(who.api(), live, mode="listen"), (sup, other)))
    assert sorted(r.status_code for r in answers) == [204, 409]
    assert next(r for r in answers if r.status_code == 409).json()["error"]["code"] == "supervisor_present"
    tenant.world.wait(tenant.world.plivo.idle, what="the Plivo simulator to go idle")
    assert len(sup.phone.legs) + len(other.phone.legs) == 1


def test_two_moves_of_one_supervisor_at_once_on_two_replicas_move_its_leg_once(world, tenant):
    """Two simultaneous moves of one listening supervisor to different calls: one succeeds, the other is busy."""
    t = team(tenant, "Floor")
    sup = tenant.agent("Sup", role="supervisor", teams=[t]).login()
    calls = [connect(tenant, tenant.agent(name, teams=[t]).login()) for name in ("First", "Second", "Third")]
    ok(monitor(sup.api(), calls[0], mode="listen"))
    leg, on = joined_leg(sup, calls[0]), calls[0]
    clients = (sup.api(), world.replica().client(sup.agent_token))

    def is_add(r) -> bool:
        return r.method == "POST" and r.path.endswith("/Participant/")

    def moves() -> int:
        return sum(r.params.get("call_uuid") == leg.call_uuid for r in filter(is_add, world.plivo.requests))

    for trial in range(5):
        before, targets = moves(), [c for c in calls if c is not on]
        hold = world.plivo.fault("stall", is_add, account=tenant.auth_id)
        with ThreadPoolExecutor(2) as pool:
            asked = [pool.submit(monitor, c, live, mode="listen") for c, live in zip(clients, targets, strict=True)]
            next(as_completed(asked))
            hold.release()
            answers = [f.result() for f in asked]
        assert sorted(r.status_code for r in answers) == [204, 409], f"trial {trial}: {[r.text for r in answers]}"
        error(next(r for r in answers if r.status_code == 409), 409, "supervisor_busy")
        on = targets[[r.status_code for r in answers].index(204)]
        world.wait(lambda n=trial + 2: len(seen(calls[0].stream, "supervisor.joined")) == n, 20, f"join {trial}")
        assert (leg.mpc, leg.state, moves() - before) == (on.caller.mpc, "in_progress", 1), f"trial {trial}"


def test_a_leave_plivo_refuses_keeps_the_supervisor_on_the_call(world, tenant):
    live, sup = floor(tenant)
    ok(monitor(sup.api(), live, mode="listen"))
    leg = joined_leg(sup, live)
    world.plivo.fault(
        "reject", lambda r: r.method == "DELETE" and leg.call_uuid in r.path, account=tenant.auth_id, status=400
    )
    error(monitor(sup.api(), live, "DELETE"), 422, "plivo_refused")
    fence(tenant)
    assert (leg.state, tenant.events(type="supervisor.left"), activity(tenant, sup)) == ("in_progress", [], "on_call")
    ok(monitor(sup.api(), live, "DELETE"))
    leg.wait_state("completed")
    assert live.stream.wait_for("supervisor.left", conversation_id=live.id)["data"]["supervisor_id"] == sup.id


def test_pausing_a_recording_pauses_every_members_track_and_resuming_resumes_them(world, tenant):
    settings(tenant, recording={"inbound": True, "allow_pause": True})
    live = connect(tenant)

    def tracks(action: str) -> set[str]:
        return {
            r.path
            for r in world.plivo.requests
            if r.method == "POST"
            and tenant.auth_id in r.path
            and "/Participant/" in r.path
            and f"/Record/{action}" in r.path
        }

    for action, done in (("pause", "Pause"), ("resume", "Resume")):
        ok(act(live.agent.api(), live.id, "recording", {"action": action}))
        live.stream.wait_for(f"call.recording_{action}d", conversation_id=live.id)
        world.wait(lambda done=done: len(tracks(done)) >= 2, what=f"the caller's and the agent's tracks {action}d")


def test_a_zero_second_recording_and_a_mixed_track_are_not_kept(world, tenant):
    settings(tenant, recording={"inbound": True})
    live = connect(tenant)
    room = next(c.params["MPCUUID"] for c in world.plivo.callbacks if c.params.get("MPCName") == live.caller.mpc)
    mixed = {"RecordTrackType": "all", "RecordingDuration": "9", "ParticipantCallUUID": live.caller.call_uuid}
    negative = {"RecordingDuration": "-1"}
    for event, mpc, extra in (
        ("MPCRecordingCompleted", "empty", {}),
        ("MPCRecordingCompleted", "negative", negative),
        ("ParticipantRecordingCompleted", room, mixed),
    ):
        form = {"EventName": event, "MPCUUID": mpc, "RecordingUUID": str(uuid4()), "RecordingDuration": "0"}
        form |= {"RecordingURL": "https://media.example.test/r.mp3", **extra}
        assert plivo_post(world, tenant.id, f"recording?c={live.id}", form, tenant.auth_token).status_code == 200
    live.caller.hangup()

    def kept():
        return next(
            (e for e in seen(live.stream, "recording.ready", conversation_id=live.id) if e["data"]["duration_sec"]),
            None,
        )

    rid = world.wait(kept, what="the room's recording")["data"]["recording_id"]
    assert all(e["data"]["duration_sec"] for e in seen(live.stream, "recording.ready", conversation_id=live.id))
    assert not [
        e for e in seen(live.stream, "recording.ready", conversation_id=live.id) if e["data"]["duration_sec"] < 1
    ]
    run_jobs_until(world, lambda: seen(live.stream, "transcript.ready", recording_id=rid), "transcript.ready")
    assert [r["id"] for r in ok(tenant.api.get("/api/v1/recordings", params={"conversation_id": live.id}))["data"]] == [
        rid
    ]
    assert not replayed(tenant, "recording.ready", duration_sec=0)


def test_a_recording_duration_is_read_as_int_does_and_one_that_is_not_a_number_is_kept_without(world, tenant):
    settings(tenant, recording={"inbound": True})
    live = connect(tenant)
    for duration in ("²", " 5"):
        form = {"EventName": "MPCRecordingCompleted", "MPCUUID": "m", "RecordingUUID": str(uuid4())}
        form |= {"RecordingURL": "https://media.example.test/r.mp3", "RecordingDuration": duration}
        assert plivo_post(world, tenant.id, f"recording?c={live.id}", form, tenant.auth_token).status_code == 200
    got = ok(tenant.api.get("/api/v1/recordings", params={"conversation_id": live.id}))["data"]
    assert sorted(r["duration_sec"] or 0 for r in got) == [0, 5]


def test_a_transcript_that_comes_before_its_recording_survives_a_transcript_read_that_fails(world, tenant):
    settings(tenant, recording={"inbound": True})
    live = connect(tenant)
    late = world.plivo.fault(
        "delay",
        lambda r: r.params.get("EventName", "").endswith("RecordingCompleted"),
        account=tenant.auth_id,
        times=math.inf,
    )
    world.plivo.fault(
        "reject", lambda r: "/Transcription/" in r.path, account=tenant.auth_id, times=math.inf, status=404
    )
    live.caller.hangup()
    world.wait(world.plivo.idle, what="the transcripts delivered ahead of their recordings")
    late.release()
    rid = live.stream.wait_for("recording.ready", conversation_id=live.id)["data"]["recording_id"]
    run_jobs_until(world, lambda: seen(live.stream, "transcript.ready", recording_id=rid), "transcript.ready")
    got = ok(tenant.api.get(f"/api/v1/recordings/{rid}/transcript"))
    assert got["complete"] is True and world.plivo.transcript in [s["text"] for s in got["segments"]]


def test_a_plain_text_transcript_gives_way_to_plivos_timed_one(world, tenant):
    settings(tenant, recording={"inbound": True})
    live = connect(tenant)
    world.plivo.fault("drop", lambda r: "/transcription" in r.path, account=tenant.auth_id, times=math.inf)
    live.caller.hangup()
    rid = live.stream.wait_for("recording.ready", conversation_id=live.id)["data"]["recording_id"]
    world.wait(world.plivo.idle, what="every recording of the call delivered")
    done = ("MPCRecordingCompleted", "ParticipantRecordingCompleted")
    recorded = {c.params["RecordingUUID"] for c in world.plivo.callbacks if c.params.get("EventName") in done}
    for prid in recorded:
        form = {"recording_id": prid, "transcription": "one plain blob"}
        assert plivo_post(world, tenant.id, f"transcription?c={live.id}", form, tenant.auth_token).status_code == 200
    run_jobs_until(world, lambda: seen(live.stream, "transcript.ready", recording_id=rid), "transcript.ready")
    got = ok(tenant.api.get(f"/api/v1/recordings/{rid}/transcript"))
    assert {s["text"] for s in got["segments"]} == {world.plivo.transcript}


def test_a_transferred_calls_transcript_names_each_agent_on_the_recordings_clock(world, tenant):
    settings(tenant, recording={"inbound": True})
    target = tenant.agent("Agent-Two").login()
    live = connect(tenant, tenant.agent("Agent-One").login())
    began = int(time.time())
    world.wait(lambda: int(time.time()) > began, what="a later second")
    ok(act(live.agent.api(), live.id, "transfers", {"mode": "cold", "target": to_agent(target)}))
    target.phone.wait_ring().wait_state("in_progress")
    live.stream.wait_for("call.transferred", conversation_id=live.id, status="completed")
    live.caller.hangup()
    rid = live.stream.wait_for("recording.ready", conversation_id=live.id)["data"]["recording_id"]

    def complete():
        r = tenant.api.get(f"/api/v1/recordings/{rid}/transcript")
        return r.status_code == 200 and r.json()["complete"] and r.json()

    said = [(s["speaker"], s["start_time"]) for s in run_jobs_until(world, complete, "the transcript")["segments"]]
    agents = [(who, at) for who, at in said if who != "Customer"]
    assert "Customer" in {who for who, _ in said}, said
    assert [who for who, _ in agents] == ["Agent-One", "Agent-Two"] and agents[0][1] < agents[1][1], said


def test_a_transcript_waits_for_the_tracks_plivo_sends_beside_its_room_recording(world, tenant):
    settings(tenant, recording={"inbound": True})
    target = tenant.agent("Agent-Two").login()
    live = connect(tenant, tenant.agent("Agent-One").login())
    ok(act(live.agent.api(), live.id, "transfers", {"mode": "cold", "target": to_agent(target)}))
    target.phone.wait_ring().wait_state("in_progress")
    live.stream.wait_for("call.transferred", conversation_id=live.id, status="completed")
    first = next(r for r in world.plivo.recordings.values() if r.call_uuid == live.leg.call_uuid)
    world.wait(
        lambda: any(c.params.get("recording_id") == first.id and c.status == 200 for c in world.plivo.callbacks),
        what="the first agent's track transcribed",
    )
    world.plivo.fault(
        "delay",
        lambda r: r.params.get("EventName") == "ParticipantRecordingCompleted",
        account=tenant.auth_id,
        times=math.inf,
        after=lambda r: r.params.get("EventName") == "MPCRecordingCompleted",
    )
    live.caller.hangup()
    rid = live.stream.wait_for("recording.ready", conversation_id=live.id)["data"]["recording_id"]
    assert not replayed(tenant, "transcript.ready", recording_id=rid)
    world.wait(world.plivo.idle, what="the tracks held behind the room's recording delivered")
    run_jobs_until(world, lambda: seen(live.stream, "transcript.ready", recording_id=rid), "transcript.ready")
    got = ok(tenant.api.get(f"/api/v1/recordings/{rid}/transcript"))
    assert got["complete"] and {s["speaker"] for s in got["segments"]} == {"Customer", "Agent-One", "Agent-Two"}


def room_kept(world, tenant) -> bool:
    rooms = {r.id for r in world.plivo.recordings.values() if r.account.auth_id == tenant.auth_id and r.kind == "mpc"}
    done = (c.params for c in world.plivo.callbacks if c.status == 200)
    return any(c.get("EventName") == "MPCRecordingCompleted" and c.get("RecordingUUID") in rooms for c in done)


def test_a_transcript_is_announced_once_with_every_track_that_comes_inside_its_grace(world, tenant):
    settings(tenant, recording={"inbound": True})
    live = connect(tenant, tenant.agent("Agent-One").login())
    tracks = world.plivo.recordings.values()
    mine = world.wait(
        lambda: next((r for r in tracks if r.kind == "participant" and r.call_uuid == live.leg.call_uuid), None),
        what="the agent's track",
    )
    late = world.plivo.fault(
        "delay",
        lambda r: mine.id in (r.params.get("RecordingUUID"), r.params.get("recording_id")),
        account=tenant.auth_id,
        times=math.inf,
    )
    world.plivo.fault(
        "delay",
        lambda r: r.params.get("EventName") == "MPCRecordingCompleted",
        account=tenant.auth_id,
        after=lambda r: r.params.get("EventName") == "ParticipantRecordingCompleted",
    )
    live.caller.hangup()
    rid = live.stream.wait_for("recording.ready", conversation_id=live.id)["data"]["recording_id"]
    # Wall time counts toward the grace, so release before any travel.
    late.release()
    world.wait(world.plivo.idle, what="the agent's track and its transcript delivered")
    assert not replayed(tenant, "transcript.ready", recording_id=rid)
    world.travel(TRACK_GRACE_SEC)
    assert len(replayed(tenant, "transcript.ready", recording_id=rid)) == 1
    got = ok(tenant.api.get(f"/api/v1/recordings/{rid}/transcript"))
    assert got["complete"] and {s["speaker"] for s in got["segments"]} == {"Customer", "Agent-One"}


def test_a_room_recording_kept_behind_a_slow_transaction_still_waits_its_whole_grace(world, tenant):
    settings(tenant, recording={"inbound": True})
    live = connect(tenant, tenant.agent("Agent-One").login())
    room = world.plivo.fault(
        "delay", lambda r: r.params.get("EventName") == "MPCRecordingCompleted", account=tenant.auth_id
    )
    live.caller.hangup()
    world.wait(world.plivo.idle, what="the tracks and their transcripts delivered")
    with world.passes_held(), world.stall("entry") as waiting:
        room.release()
        world.wait(waiting, what="the room's recording waiting on a slow transaction")
    world.wait(lambda: room_kept(world, tenant), what="the room's recording kept")
    world.advance(TRACK_GRACE_SEC - 1)
    rid = live.stream.wait_for("recording.ready", conversation_id=live.id)["data"]["recording_id"]
    assert not replayed(tenant, "transcript.ready", recording_id=rid)
    world.advance(TRACK_GRACE_SEC)
    assert len(replayed(tenant, "transcript.ready", recording_id=rid)) == 1


def test_a_transcript_keeps_only_the_words_a_track_says_while_its_room_records(world, tenant):
    settings(tenant, recording={"inbound": True})
    agent = tenant.agent().login()
    agent.phone.on_ring("ignore")
    stream = tenant.stream(after=0)
    number, _ = tenant.inbound_line(agent)
    caller = world.plivo.call(number)
    leg = agent.phone.wait_ring()
    began = int(time.time())
    world.wait(lambda: int(time.time()) > began, what="the caller waiting into a later second")
    leg.answer()
    cid = hs_header(leg, "HsConversation")
    stream.wait_for("offer.accepted", conversation_id=cid)
    caller.hangup()
    ready = stream.wait_for("recording.ready", conversation_id=cid)["data"]
    world.wait(world.plivo.idle, what="every recording of the call delivered")
    mine = [r for r in world.plivo.recordings.values() if r.account.auth_id == tenant.auth_id]
    room = next(r for r in mine if r.kind == "mpc")
    track = next(r for r in mine if r.kind == "participant" and r.call_uuid == caller.call_uuid)
    waited, length = int(room.start) - int(track.start), ready["duration_sec"]
    assert waited > 0, "the caller's track began in the second the room's recording did"
    said = [(0.0, "While I waited"), (float(waited), "Hello agent"), (float(waited + length + 60), "After it ended")]
    text = json.dumps([{"start": at, "text": words} for at, words in said])
    form = {"recording_id": track.id, "call_uuid": caller.call_uuid, "transcription": text}
    assert plivo_post(world, tenant.id, f"transcription?c={cid}", form, tenant.auth_token).status_code == 200
    got = ok(tenant.api.get(f"/api/v1/recordings/{ready['recording_id']}/transcript"))["segments"]
    assert [(s["start_time"], s["text"]) for s in got if s["speaker"] == "Customer"] == [(0, "Hello agent")], got


def test_a_recording_read_carries_its_start_end_and_format(tenant):
    settings(tenant, recording={"inbound": True})
    live = connect(tenant)
    live.caller.hangup()
    rid = live.stream.wait_for("recording.ready", conversation_id=live.id)["data"]["recording_id"]
    got = ok(tenant.api.get(f"/api/v1/recordings/{rid}"))
    assert got["format"] == "mp3" and got["started_at"] <= got["ended_at"]


def test_wait_prompts_play_with_their_repeats_then_the_loop_prompts_cycle_reading_repeats_by_position(world, tenant):
    prompts = [
        say("Welcome"),
        {"text": "Thanks for waiting", "loop": True},
        {"text": "Hold on", "loop": True, "repeat": 1},
    ]
    number, _ = tenant.inbound_line(unavailable(tenant), wait_prompts=prompts)
    caller = world.plivo.call(number)
    order = [
        "Welcome",
        "Thanks for waiting",
        "Hold on",
        "Hold on",
        "Thanks for waiting",
        "Hold on",
        "Thanks for waiting",
    ]

    def played() -> list[str]:
        return [s for s in caller.said if s in order]

    for n in range(1, len(order) + 1):
        world.wait(lambda n=n: len(played()) >= n, what=f"wait prompt {n}")
        if n < len(order):
            caller.media_done()
    assert played() == order


def test_an_ssml_wait_prompt_is_spoken_as_ssml_and_no_prompt_mixes_with_the_hold_music(world, tenant):
    prompts = [
        {"text": "<speak voice='Polly.Joanna'>Please <break time='1s'/> hold</speak>", "ssml": True},
        say("Still in line"),
        {"text": "<speak>Thanks</speak>", "ssml": True},
    ]
    number, _ = tenant.inbound_line(unavailable(tenant), wait_prompts=prompts)
    caller = world.plivo.call(number)
    for n in (1, 2, 3):
        world.wait(lambda n=n: len(speaks(world, tenant)) >= n, what=f"wait prompt {n}")
        if n < 3:
            caller.media_done()
    first, second, third = speaks(world, tenant)
    assert (first["type"], first["voice"], first["text"]) == ("ssml", "Polly.Joanna", "Please <break time='1s'/> hold")
    assert (third["type"], third["voice"], third["text"]) == ("ssml", "Polly.Joey", "Thanks")
    assert "type" not in second and [p["mix"] for p in (first, second, third)] == [False] * 3


def test_a_callers_place_counts_only_calls_and_follows_a_lifo_queue(world, tenant):
    prompt = say("You are number {position} in line.")
    queue = tenant.queue(agents=[unavailable(tenant).id], wait_prompts=[prompt], dequeue_order="lifo")
    number = tenant.number()
    tenant.route_number(number, {"type": "queue", "queue_id": queue["id"]})
    ok(enqueue_text(tenant.api, queue, "chat", "visitor-1", "web"))
    first = world.plivo.call(number, from_="+14155550701")
    assert "number 1 in line" in heard(world, first, "in line")
    second = world.plivo.call(number, from_="+14155550702")
    assert "number 1 in line" in heard(world, second, "in line")


def test_the_estimated_wait_is_the_callers_place_times_the_recent_wait(world, tenant):
    agent = tenant.agent().login()
    agent.phone.on_ring("answer", after=1.5)
    number, _ = tenant.inbound_line(agent, wait_prompts=[say("Your wait is about {wait_time}.")])
    stream = tenant.stream(after=0)
    first = world.plivo.call(number, from_="+14155550701")
    assert heard(world, first, "Your wait") == "Your wait is about 1 minutes."
    stream.wait_for("offer.accepted", agent_id=agent.id)
    second = world.plivo.call(number, from_="+14155550702")
    estimate = re.fullmatch(r"Your wait is about (\d+) seconds\.", heard(world, second, "Your wait"))
    assert estimate and 1 <= int(estimate[1]) <= 10, estimate


def test_the_estimated_wait_counts_only_calls_queued_before_this_caller(world, tenant):
    later = tenant.agent("Later").login()
    later.phone.on_ring("answer", after=1.5)
    prompt = {"text": "Your wait is at most {max_wait_time}.", "loop": True}
    number, queue = tenant.inbound_line(unavailable(tenant), wait_prompts=[prompt])
    other = tenant.number()
    tenant.route_number(other, {"type": "queue", "queue_id": queue["id"], "agents": [later.id]})
    stream = tenant.stream(after=0)
    first = world.plivo.call(number, from_="+14155550701")
    heard(world, first, "at most")
    world.plivo.call(other, from_="+14155550702")
    stream.wait_for("offer.accepted", agent_id=later.id)
    first.media_done()

    def estimates() -> list[str]:
        return [s for s in first.said if "at most" in s]

    world.wait(lambda: len(estimates()) == 2, what="the caller's next prompt")
    assert estimates() == ["Your wait is at most 1 minutes."] * 2


def test_a_call_answered_in_two_queues_is_a_sample_of_each_queues_estimated_wait(world, tenant):
    first, billing = tenant.agent("First").login(), tenant.agent("Billing").login()
    for agent in (first, billing):
        agent.phone.on_ring("answer", after=1.5)
    prompts = [say("Your wait is about {wait_time}.")]
    number, _ = tenant.inbound_line(first, wait_prompts=prompts)
    billing_line, queue = tenant.inbound_line(billing, wait_prompts=prompts)
    stream = tenant.stream(after=0)
    world.plivo.call(number, from_="+14155550701")
    cid = stream.wait_for("offer.accepted", agent_id=first.id)["conversation_id"]
    ok(act(first.api(), cid, "transfers", {"mode": "cold", "target": {"type": "queue", "queue_id": queue["id"]}}))
    stream.wait_for("offer.accepted", agent_id=billing.id, conversation_id=cid)
    for line, caller in ((number, "+14155550702"), (billing_line, "+14155550703")):
        said = heard(world, world.plivo.call(line, from_=caller), "Your wait")
        estimate = re.fullmatch(r"Your wait is about (\d+) seconds\.", said)
        assert estimate and 1 <= int(estimate[1]) <= 10, said


def test_the_agents_join_stops_a_wait_prompt_still_playing(world, tenant):
    agent = tenant.agent().login()
    agent.set_status("Away")
    number, _ = tenant.inbound_line(agent, wait_prompts=[say("Please hold, all our agents are busy")])
    caller = world.plivo.call(number)
    heard(world, caller, "Please hold")
    agent.set_status("Available")
    agent.phone.wait_ring().wait_state("in_progress")

    def stopped() -> set[str]:
        stops = [r.path for r in world.plivo.requests if r.method == "DELETE" and tenant.auth_id in r.path]
        return {kind for kind in ("Speak", "Play") if any(f"/{kind}" in p for p in stops)}

    world.wait(lambda: stopped() == {"Speak", "Play"}, what="the caller's prompt stopped")


def room_event(name: str, agent: bool = False):
    def match(r) -> bool:
        return r.params.get("EventName") == name and (
            not agent or r.params.get("ParticipantRole", "").lower() == "agent"
        )

    return match


def prompted_while_held(world, tenant, *held) -> list[str]:
    """An away agent's caller hears "One" of two wait prompts; the agent comes back and answers while the room events
    `held` match are delayed, the caller's prompt ends, and the prompts it heard are returned."""
    agent = tenant.agent().login()
    agent.set_status("Away")
    number, _ = tenant.inbound_line(agent, wait_prompts=[say("One"), {"text": "Two", "loop": True}])
    caller = world.plivo.call(number)
    heard(world, caller, "One")
    faults = [world.plivo.fault("delay", match, account=tenant.auth_id) for match in held]
    agent.set_status("Available")
    agent.phone.wait_ring().wait_state("in_progress")
    world.wait(world.plivo.idle, what="the room events not held delivered")
    caller.media_done()
    world.wait(world.plivo.idle, what="the end of the prompt delivered")
    for fault in faults:
        fault.release()
    return [s for s in caller.said if s in ("One", "Two")]


def test_the_first_wait_prompt_waits_for_mpc_initialized_not_the_callers_join(world, tenant):
    number, _ = tenant.inbound_line(unavailable(tenant), wait_prompts=[say("Welcome")])
    held = world.plivo.fault("delay", room_event("MPCInitialized"), account=tenant.auth_id)
    caller = world.plivo.call(number)
    world.wait(world.plivo.idle, what="the caller's join delivered")
    assert speaks(world, tenant) == []
    held.release()
    heard(world, caller, "Welcome")


def test_a_room_whose_initialization_and_callers_join_never_reach_hs_gets_its_first_wait_prompt_after_ten_seconds(
    world, tenant
):
    number, _ = tenant.inbound_line(unavailable(tenant), wait_prompts=[say("Welcome")])
    for name in ("MPCInitialized", "ParticipantJoin"):
        world.plivo.fault("drop", room_event(name), account=tenant.auth_id)
    caller = world.plivo.call(number)
    world.wait(world.plivo.idle, what="the caller's join delivered")
    world.advance(5)
    assert speaks(world, tenant) == []
    world.advance(10)
    heard(world, caller, "Welcome")


def test_a_wait_prompt_plivo_failed_is_not_sent_again_once_mpc_start_ended_the_prompts(world, tenant):
    agent = tenant.agent().login()
    agent.set_status("Away")
    number, _ = tenant.inbound_line(agent, wait_prompts=[say("One"), {"text": "Two", "loop": True}])
    caller = world.plivo.call(number)
    heard(world, caller, "One")
    joins = (
        room_event("ParticipantJoin", agent=True),
        room_event("MPCStart"),
        room_event("AddParticipantByAPIActionCompleted"),
    )
    held = [world.plivo.fault("delay", match, account=tenant.auth_id) for match in joins]
    failed = world.plivo.fault("refuse", lambda r: r.path.endswith("/Speak/"), account=tenant.auth_id)
    caller.media_done()
    world.wait(lambda: failed.hits, what="the second prompt sent")
    agent.set_status("Available")
    agent.phone.wait_ring().wait_state("in_progress")
    world.wait(lambda: held[1].delayed, what="MPCStart held")
    held[1].release()
    world.wait(world.plivo.idle, what="MPCStart delivered")
    failed.release()
    world.run_jobs("plivo")
    assert "Two" not in caller.said
    held[0].release()


def test_mpc_start_stops_further_wait_prompts(world, tenant):
    held = (room_event("ParticipantJoin", agent=True), room_event("AddParticipantByAPIActionCompleted"))
    assert prompted_while_held(world, tenant, *held) == ["One"]


def test_a_successful_add_member_stops_further_wait_prompts(world, tenant):
    held = (room_event("ParticipantJoin", agent=True), room_event("MPCStart"))
    assert prompted_while_held(world, tenant, *held) == ["One"]


def test_a_caller_whose_wait_prompt_progress_redis_lost_hears_no_prompt_again_and_a_new_caller_hears_its_own(
    world, tenant
):
    number, _ = tenant.inbound_line(unavailable(tenant), wait_prompts=[say("One"), {"text": "Two", "loop": True}])
    caller = world.plivo.call(number)
    heard(world, caller, "One")
    with world.redis_down():
        pass
    world.travel_until_rebuilt()
    caller.media_done()
    later = world.plivo.call(number, from_="+14155550703")
    heard(world, later, "One")
    world.wait(world.plivo.idle, what="the end of the first caller's prompt delivered")
    world.run_jobs("plivo")
    assert [s for s in caller.said if s in ("One", "Two")] == ["One"]


def test_a_warm_text_transfer_keeps_the_first_agent_until_it_completes(world, tenant):
    first, target = tenant.agent("First").login(), tenant.agent("Target").login()
    cid, stream = live_sms(world, tenant, first)
    tid = ok(text_transfer(first, cid, to_agent(target), mode="warm"))["transfer_id"]
    ok(accept(target, offered(stream, target, conversation_id=cid)))
    reply(target, cid, "Hi, I can help with that")
    assert not replayed(tenant, "conversation.wrap_up_started", conversation_id=cid, agent_id=first.id)
    ok(first.api().post(f"/api/v1/conversations/{cid}/transfers/{tid}/complete"))
    stream.wait_for("conversation.wrap_up_started", conversation_id=cid, agent_id=first.id)


@pytest.mark.parametrize("end", ["cancel", "close"])
def test_a_joined_warm_text_target_is_told_it_was_removed_when_the_agent_drops_it_or_closes(world, tenant, end):
    first, target = tenant.agent("First").login(), tenant.agent("Target").login()
    cid, stream = live_sms(world, tenant, first)
    tid = ok(text_transfer(first, cid, to_agent(target), mode="warm"))["transfer_id"]
    ok(accept(target, offered(stream, target, conversation_id=cid)))
    path = f"transfers/{tid}/cancel" if end == "cancel" else "close"
    ok(first.api().post(f"/api/v1/conversations/{cid}/{path}"))
    gone = stream.wait_for("conversation.participant_removed", conversation_id=cid)
    assert (gone["data"]["participant_id"], gone["agent_id"]) == (tid, target.id)


def test_a_text_transfer_in_a_mode_the_tenant_does_not_allow_is_refused(world, tenant):
    settings(tenant, transfer={"modes": "cold"})
    first, target = tenant.agent("First").login(), tenant.agent("Target").login()
    cid, _ = live_sms(world, tenant, first)
    error(text_transfer(first, cid, to_agent(target), mode="warm"), 409, "transfer_mode_not_allowed")
    ok(text_transfer(first, cid, to_agent(target), mode="cold"))


def test_a_pending_text_transfer_can_be_cancelled_and_an_accepted_one_cannot(world, tenant):
    first, target, second = (tenant.agent(n).login() for n in ("First", "Target", "Second"))
    cid, stream = live_sms(world, tenant, first)
    tid = ok(text_transfer(first, cid, to_agent(target)))["transfer_id"]
    offer = offered(stream, target, conversation_id=cid)
    ok(first.api().post(f"/api/v1/conversations/{cid}/transfers/{tid}/cancel"))
    assert stream.wait_for("offer.withdrawn", offer_id=offer["data"]["offer_id"])["data"]["reason"] == "cancelled"
    taken = ok(text_transfer(first, cid, to_agent(second)))["transfer_id"]
    ok(accept(second, offered(stream, second, conversation_id=cid)))
    error(first.api().post(f"/api/v1/conversations/{cid}/transfers/{taken}/cancel"), 409)


def test_a_text_transfer_reads_transferring_until_nobody_accepts_and_then_reports_failed(world, tenant):
    first, target = tenant.agent("First").login(), tenant.agent("Target").login()
    cid, stream = live_sms(world, tenant, first)

    def state() -> str:
        return ok(tenant.api.get(f"/api/v1/conversations/{cid}"))["state"]

    tid = ok(text_transfer(first, cid, to_agent(target)))["transfer_id"]
    offered(stream, target, conversation_id=cid)
    assert state() == "transferring"
    world.advance(31)
    failed = stream.wait_for("conversation.transferred", conversation_id=cid, status="failed")
    assert (failed["data"]["transfer_id"], failed["agent_id"]) == (tid, first.id)
    assert state() == "active"
