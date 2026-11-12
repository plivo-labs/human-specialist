"""Supervisors: listen, whisper, barge with a spoken notice, switch and leave; access by team."""

import threading

import pytest

from tests.acceptance.conftest import (
    activity,
    connect,
    error,
    fence,
    fresh_second,
    hang_up_with_the_room_alive,
    hs_header,
    lead,
    ok,
    settings,
    team,
)
from tests.acceptance.test_voice_calls import member

NOTICE = "A supervisor has joined the call"
FLAGS = {
    "listen": {"coach_mode": True, "mute": True},
    "whisper": {"coach_mode": True, "mute": False},
    "barge": {"coach_mode": False, "mute": False},
}
REFUSED = (422, "plivo_refused")


def floor(tenant):
    """A team with a supervisor and an agent who is on a live inbound call. Returns (live, supervisor)."""
    t = team(tenant, "Floor")
    sup = tenant.agent("Sup", role="supervisor", teams=[t]).login()
    agent = tenant.agent("Agent", teams=[t]).login()
    return connect(tenant, agent), sup


def monitor(client, live, method: str = "POST", **body):
    return client.request(method, f"/api/v1/calls/{live.id}/monitor", json=body or None)


def joined_leg(sup, live):
    leg = sup.phone.wait_ring()
    leg.wait_state("in_progress")
    live.stream.wait_for("supervisor.joined", conversation_id=live.id)
    return leg


def teammate_of(tenant, agent):
    return tenant.agent("Teammate", teams=ok(tenant.api.get(f"/api/v1/agents/{agent.id}"))["teams"]).login()


def test_listen_joins_the_supervisor_to_the_call(tenant):
    live, sup = floor(tenant)
    ok(monitor(sup.api(), live, mode="listen"))
    leg = sup.phone.wait_ring()
    assert (hs_header(leg, "HsKind"), hs_header(leg, "HsConversation")) == ("monitor", live.id)
    leg.wait_state("in_progress")
    assert leg.mpc == live.caller.mpc
    joined = live.stream.wait_for("supervisor.joined", conversation_id=live.id)
    assert (joined["data"]["supervisor_id"], joined["data"]["mode"], joined["agent_id"]) == (
        sup.id,
        "listen",
        live.agent.id,
    )


def test_switching_to_whisper_then_leaving(tenant):
    live, sup = floor(tenant)
    ok(monitor(sup.api(), live, mode="listen"))
    leg = joined_leg(sup, live)
    ok(monitor(sup.api(), live, "PATCH", mode="whisper"))
    changed = live.stream.wait_for("supervisor.mode_changed", conversation_id=live.id)
    assert (changed["data"]["mode"], changed["data"]["supervisor_id"]) == ("whisper", sup.id)
    ok(monitor(sup.api(), live, "DELETE"))
    leg.wait_state("completed")
    assert live.stream.wait_for("supervisor.left", conversation_id=live.id)["data"]["supervisor_id"] == sup.id
    assert live.caller.state == live.leg.state == "in_progress"


def sets(mode: str):
    """Matches HS's request that sets the supervisor's member to `mode`."""
    return lambda r: r.method == "POST" and all(r.params.get(k) is v for k, v in FLAGS[mode].items())


def answer(r):
    return r.status_code if r.status_code == 204 else (r.status_code, r.json()["error"]["code"])


def switching(sup, live, mode: str, answers: dict) -> threading.Thread:
    thread = threading.Thread(target=lambda: answers.update({mode: monitor(sup.api(), live, "PATCH", mode=mode)}))
    thread.start()
    return thread


def modes(tenant) -> list[str]:
    """The modes supervisor.mode_changed has told of, in order."""
    fence(tenant)
    return [e["data"]["mode"] for e in tenant.events(type="supervisor.mode_changed")]


@pytest.mark.parametrize(
    ("first", "later", "answers", "muted", "changed"),
    [
        pytest.param("stall", None, (204, 204), True, ["whisper", "listen"], id="applied-then-accepted"),
        pytest.param("stall", "reject", (204, REFUSED), False, ["whisper"], id="applied-then-refused"),
        pytest.param("refuse", None, (REFUSED, 204), True, [], id="refused-then-accepted"),
    ],
)
def test_a_switch_that_plivo_answers_after_a_later_switch_replaced_it_answers_for_its_own_command(
    world, tenant, first, later, answers, muted, changed
):
    live, sup = floor(tenant)
    ok(monitor(sup.api(), live, mode="listen"))
    leg = joined_leg(sup, live)
    held = world.plivo.fault(first, sets("whisper"), account=tenant.auth_id, status=400)
    if later:
        world.plivo.fault(later, sets("listen"), account=tenant.auth_id, status=400)
    got = {}
    threads = [switching(sup, live, "whisper", got)]
    world.wait(lambda: held.hits, what="Plivo holding its answer to the whisper")
    threads.append(switching(sup, live, "listen", got))
    sent = len(world.plivo.requests)
    held.release()
    for thread in threads:
        thread.join(15)
    assert (answer(got["whisper"]), answer(got["listen"])) == answers
    listens = [r for r in world.plivo.requests[sent:] if tenant.auth_id in r.path and sets("listen")(r)]
    assert len(listens) == 1, "the listen did not reach Plivo exactly once"
    assert member(tenant, leg)["mute"] is muted
    assert modes(tenant) == changed


def test_a_switch_plivo_refuses_emits_no_mode_change_and_its_retry_switches(world, tenant):
    live, sup = floor(tenant)
    ok(monitor(sup.api(), live, mode="listen"))
    leg = joined_leg(sup, live)
    world.plivo.fault("reject", sets("whisper"), account=tenant.auth_id, status=400)
    assert answer(monitor(sup.api(), live, "PATCH", mode="whisper")) == REFUSED
    assert modes(tenant) == []
    ok(monitor(sup.api(), live, "PATCH", mode="whisper"))
    assert modes(tenant) == ["whisper"]
    assert member(tenant, leg)["mute"] is False


def test_a_supervisor_who_rejoins_after_a_cold_transfer_and_leaves_again_is_off_the_call(tenant):
    live, sup = floor(tenant)
    target = teammate_of(tenant, live.agent)
    ok(monitor(sup.api(), live, mode="listen"))
    joined_leg(sup, live)
    cold = {"mode": "cold", "target": {"type": "agent", "agent_id": target.id}}
    ok(live.agent.api().post(f"/api/v1/calls/{live.id}/transfers", json=cold))
    target.phone.wait_ring().wait_state("in_progress")
    live.stream.wait_for("call.transferred", conversation_id=live.id, status="completed")
    fresh_second(tenant.world)
    ok(monitor(sup.api(), live, "DELETE"))
    ok(monitor(sup.api(), live, mode="listen"))
    leg = sup.phone.wait_ring().wait_state("in_progress")
    tenant.world.wait(lambda: len(tenant.events(type="supervisor.joined")) == 2, what="the rejoin")
    ok(monitor(sup.api(), live, "DELETE"))
    leg.wait_state("completed")
    tenant.world.wait(lambda: activity(tenant, sup) == "idle", what="the supervisor to read idle")
    fence(tenant)
    moves = [e["type"] for e in tenant.events() if e["type"] in ("supervisor.joined", "supervisor.left")]
    assert moves == ["supervisor.joined", "supervisor.left"] * 2


def test_a_warm_transfer_completed_to_a_number_takes_the_supervisor_off_the_call(world, tenant):
    live, sup = floor(tenant)
    ok(monitor(sup.api(), live, mode="listen"))
    leg = joined_leg(sup, live)
    number, phone = lead(world)
    body = {"mode": "warm", "target": {"type": "number", "number": number}}
    tid = ok(tenant.api.post(f"/api/v1/calls/{live.id}/transfers", json=body))["transfer_id"]
    target = phone.wait_ring().wait_state("in_progress")
    world.wait(lambda: target.mpc == live.caller.mpc, what="the consult in the caller's room")
    world.wait(world.plivo.idle, what="the consult's join answered")
    ok(tenant.api.post(f"/api/v1/calls/{live.id}/transfers/{tid}/complete"))
    leg.wait_state("completed")
    assert live.stream.wait_for("supervisor.left", conversation_id=live.id)["agent_id"] == sup.id
    world.wait(lambda: activity(tenant, sup) == "idle", what="the supervisor to read idle")
    assert live.caller.state == target.state == "in_progress"


def test_a_supervisor_who_leaves_while_their_phone_rings_never_joins(tenant):
    live, sup = floor(tenant)
    sup.phone.on_ring("answer", after=2.0)
    ok(monitor(sup.api(), live, mode="listen"))
    leg = sup.phone.wait_ring()
    ok(monitor(sup.api(), live, "DELETE"))
    leg.wait_state("completed")
    tenant.world.wait(lambda: activity(tenant, sup) == "idle", what="the supervisor to read idle")
    fence(tenant)
    assert leg.mpc is None and not tenant.events(type="supervisor.joined")
    assert live.caller.state == live.leg.state == "in_progress"


def test_a_supervisor_who_hangs_up_their_own_phone_can_listen_again(tenant):
    live, sup = floor(tenant)
    ok(monitor(sup.api(), live, mode="listen"))
    joined_leg(sup, live).hangup()
    tenant.world.wait(lambda: activity(tenant, sup) == "idle", what="the supervisor to read idle")
    # their own hangup took them off the call: the desk's leave has no supervisor to remove
    error(monitor(sup.api(), live, "DELETE"), 422, "not_monitored")
    ok(monitor(sup.api(), live, mode="listen"))
    sup.phone.wait_ring().wait_state("in_progress")
    tenant.world.wait(lambda: len(tenant.events(type="supervisor.joined")) == 2, what="the rejoin")


def test_switching_to_barge_plays_the_notice_to_everyone(tenant):
    settings(tenant, monitor={"barge_notice": NOTICE})
    live, sup = floor(tenant)
    ok(monitor(sup.api(), live, mode="listen"))
    leg = joined_leg(sup, live)
    assert NOTICE not in live.caller.said
    ok(monitor(sup.api(), live, "PATCH", mode="barge"))
    tenant.world.wait(lambda: all(NOTICE in c.said for c in (live.caller, live.leg, leg)), what="barge notice")
    assert live.stream.wait_for("supervisor.mode_changed", conversation_id=live.id)["data"]["mode"] == "barge"


def test_joining_in_barge_mode_is_announced(tenant):
    settings(tenant, monitor={"barge_notice": NOTICE})
    live, sup = floor(tenant)
    ok(monitor(sup.api(), live, mode="barge"))
    joined_leg(sup, live)
    tenant.world.wait(lambda: NOTICE in live.caller.said and NOTICE in live.leg.said, what="barge notice")


def test_a_barge_notice_whose_reply_was_lost_is_spoken_once(world, tenant):
    settings(tenant, monitor={"barge_notice": NOTICE})
    live, sup = floor(tenant)
    lost = world.plivo.fault(
        "lose",
        lambda r: r.path.rstrip("/").endswith("/Speak") and r.params.get("text") == NOTICE,
        account=tenant.auth_id,
    )
    ok(monitor(sup.api(), live, mode="barge"))
    everyone = (live.caller, live.leg, joined_leg(sup, live))
    world.wait(lambda: all(NOTICE in c.said for c in everyone), what="barge notice")
    world.advance(60)
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    assert lost.hits and [c.said.count(NOTICE) for c in everyone] == [1, 1, 1]


def test_a_tenant_key_monitors_for_a_named_supervisor(tenant):
    live, sup = floor(tenant)
    error(monitor(tenant.api, live, mode="listen"), 422)
    ok(monitor(tenant.api, live, mode="listen", supervisor_id=sup.id))
    assert hs_header(sup.phone.wait_ring(), "HsKind") == "monitor"


def test_a_supervisor_of_another_team_gets_404(tenant):
    live, _ = floor(tenant)
    outsider = tenant.agent("Outsider", role="supervisor", teams=[team(tenant, "Elsewhere")]).login()
    error(monitor(outsider.api(), live, mode="listen"), 404)
    assert outsider.phone.legs == []


def test_an_agent_cannot_monitor_or_control_a_teammates_call(tenant):
    live, _ = floor(tenant)
    teammate = teammate_of(tenant, live.agent)
    error(monitor(teammate.api(), live, mode="listen"), (403, 404))
    for path, body in [("hold", {"on": True}), ("recording", {"action": "pause"}), ("hangup", None)]:
        error(teammate.api().post(f"/api/v1/calls/{live.id}/{path}", json=body), 404)
    assert teammate.phone.legs == [] and live.caller.state == "in_progress"


def test_a_supervisor_lists_only_team_agents_and_an_admin_lists_all(tenant):
    t = team(tenant, "Mine")
    sup = tenant.agent("Sup", role="supervisor", teams=[t]).login(available=False)
    admin = tenant.agent("Admin", role="admin").login(available=False)
    mine, theirs = tenant.agent("Mine", teams=[t]), tenant.agent("Theirs", teams=[team(tenant, "Theirs")])

    def listed(who):
        return {a["id"] for a in ok(who.api().get("/api/v1/agents", params={"limit": 200}))["data"]}

    assert mine.id in listed(sup) and theirs.id not in listed(sup)
    assert {mine.id, theirs.id} <= listed(admin)


def test_a_supervisor_reads_and_sets_only_team_agent_status(tenant):
    t = team(tenant, "Mine")
    sup = tenant.agent("Sup", role="supervisor", teams=[t]).login(available=False)
    mine, theirs = tenant.agent("Mine", teams=[t]), tenant.agent("Theirs", teams=[team(tenant, "Theirs")])
    body = {"status_id": tenant.status_id("Away")}
    ok(sup.api().put(f"/api/v1/agents/{mine.id}/status", json=body))
    error(sup.api().put(f"/api/v1/agents/{theirs.id}/status", json=body), 404)
    error(sup.api().get(f"/api/v1/agents/{theirs.id}/status"), 404)


def test_the_monitored_agent_sees_the_events_of_a_supervisor_it_can_hear(tenant):
    t = team(tenant, "Floor")
    sup = tenant.agent("Sup", role="supervisor", teams=[t]).login()
    agent = tenant.agent("Agent", teams=[t]).login()
    desk = tenant.stream(token=agent.stream_token, after=0)
    live = connect(tenant, agent)
    ok(monitor(sup.api(), live, mode="whisper"))
    assert desk.wait_for("supervisor.joined", conversation_id=live.id)["agent_id"] == agent.id


def test_a_supervisor_still_listening_when_the_agent_hangs_up_leaves_and_the_after_agent_action_runs(world, tenant):
    app = world.plivo.app("<Response><Speak>Please rate your call</Speak></Response>")
    t = team(tenant, "Floor")
    sup = tenant.agent("Sup", role="supervisor", teams=[t]).login()
    live = connect(
        tenant, tenant.agent("Agent", teams=[t]).login(), after_agent_action={"type": "redirect", "url": app}
    )
    ok(monitor(sup.api(), live, mode="listen"))
    leg = joined_leg(sup, live)
    live.leg.hangup()
    leg.wait_state("completed")
    world.wait(lambda: "Please rate your call" in live.caller.said, what="the after-agent action")
    assert live.stream.wait_for("supervisor.left", conversation_id=live.id)["agent_id"] == sup.id


def test_the_agents_api_hangup_takes_a_listening_supervisor_off_and_the_after_agent_action_runs(world, tenant):
    app = world.plivo.app("<Response><Speak>Please rate your call</Speak></Response>")
    t = team(tenant, "Floor")
    sup = tenant.agent("Sup", role="supervisor", teams=[t]).login()
    live = connect(
        tenant, tenant.agent("Agent", teams=[t]).login(), after_agent_action={"type": "redirect", "url": app}
    )
    ok(monitor(sup.api(), live, mode="listen"))
    leg = joined_leg(sup, live)
    ok(live.agent.api().post(f"/api/v1/calls/{live.id}/hangup"))
    for x in (live.leg, leg):
        x.wait_state("completed")
    world.wait(lambda: "Please rate your call" in live.caller.said, what="the after-agent action")
    assert live.stream.wait_for("supervisor.left", conversation_id=live.id)["agent_id"] == sup.id


def test_the_agents_api_hangup_hands_the_caller_back_when_the_rooms_end_reaches_hs_before_its_exit(
    world, tenant, monkeypatch
):
    monkeypatch.setattr(world.plivo, "exit_after_events", True)
    app = world.plivo.app("<Response><Speak>Please rate your call</Speak></Response>")
    t = team(tenant, "Floor")
    sup = tenant.agent("Sup", role="supervisor", teams=[t]).login()
    live = connect(
        tenant, tenant.agent("Agent", teams=[t]).login(), after_agent_action={"type": "redirect", "url": app}
    )
    ok(monitor(sup.api(), live, mode="listen"))
    leg = joined_leg(sup, live)
    ok(live.agent.api().post(f"/api/v1/calls/{live.id}/hangup"))
    for x in (live.leg, leg):
        x.wait_state("completed")
    world.wait(lambda: "Please rate your call" in live.caller.said, what="the after-agent action")
    assert live.stream.wait_for("supervisor.left", conversation_id=live.id)["agent_id"] == sup.id


def test_a_monitor_join_sent_after_the_hangup_is_refused(world, tenant):
    live, sup = floor(tenant)
    hang_up_with_the_room_alive(world, tenant, live)
    error(monitor(sup.api(), live, mode="listen"), 409, "call_not_live")
    fence(tenant)
    assert sup.phone.legs == [] and not tenant.events(type="supervisor.joined")


def test_a_monitor_switch_sent_after_the_hangup_is_refused(world, tenant):
    live, sup = floor(tenant)
    ok(monitor(sup.api(), live, mode="listen"))
    joined_leg(sup, live)
    hang_up_with_the_room_alive(world, tenant, live)
    error(monitor(sup.api(), live, "PATCH", mode="whisper"), 409, "call_not_live")
    fence(tenant)
    assert not tenant.events(type="supervisor.mode_changed")


def test_a_second_monitor_while_a_supervisor_is_on_the_call_is_refused(tenant):
    live, sup = floor(tenant)
    teams = ok(tenant.api.get(f"/api/v1/agents/{live.agent.id}"))["teams"]
    other = tenant.agent("Other", role="supervisor", teams=teams).login()
    ok(monitor(sup.api(), live, mode="listen"))
    joined_leg(sup, live)
    for who in (sup, other):
        error(monitor(who.api(), live, mode="listen"), 409, "supervisor_present")
    tenant.world.wait(tenant.world.plivo.idle, what="the Plivo simulator to go idle")
    fence(tenant)
    assert other.phone.legs == [] and len(sup.phone.legs) == 1 and len(tenant.events(type="supervisor.joined")) == 1


def test_a_supervisor_who_declines_never_joins_and_can_try_again(tenant):
    live, sup = floor(tenant)
    sup.phone.on_ring("decline")
    ok(monitor(sup.api(), live, mode="listen"))
    sup.phone.wait_ring().wait_state("completed")
    tenant.world.wait(lambda: activity(tenant, sup) == "idle", what="the supervisor to read idle")
    fence(tenant)
    assert not tenant.events(type="supervisor.joined") and live.caller.state == live.leg.state == "in_progress"
    sup.phone.on_ring("answer")
    ok(monitor(sup.api(), live, mode="listen"))
    sup.phone.wait_ring().wait_state("in_progress")
    live.stream.wait_for("supervisor.joined", conversation_id=live.id)


def test_a_supervisor_whose_phone_rings_out_never_joins_and_can_try_again(tenant):
    settings(tenant, ring_timeout_sec=5)
    live, sup = floor(tenant)
    sup.phone.on_ring("ignore")
    ok(monitor(sup.api(), live, mode="listen"))
    sup.phone.wait_ring().wait_state("completed", timeout=10)
    tenant.world.wait(lambda: activity(tenant, sup) == "idle", what="the supervisor to read idle")
    fence(tenant)
    assert not tenant.events(type="supervisor.joined") and live.caller.state == live.leg.state == "in_progress"
    sup.phone.on_ring("answer")
    ok(monitor(sup.api(), live, mode="listen"))
    sup.phone.wait_ring().wait_state("in_progress")
    live.stream.wait_for("supervisor.joined", conversation_id=live.id)


def test_a_supervisor_without_the_barge_grant_listens_and_whispers_but_cannot_barge_or_switch_to_it(tenant):
    live, sup = floor(tenant)
    settings(tenant, permissions={"supervisor": {"barge": False}})
    error(monitor(sup.api(), live, mode="barge"), 403, "barge_not_allowed")
    error(monitor(tenant.api, live, mode="barge", supervisor_id=sup.id), 403, "barge_not_allowed")
    assert sup.phone.legs == []
    ok(monitor(sup.api(), live, mode="whisper"))
    joined_leg(sup, live)
    error(monitor(sup.api(), live, "PATCH", mode="barge"), 403, "barge_not_allowed")
    error(monitor(tenant.api, live, "PATCH", mode="barge"), 403, "barge_not_allowed")
    ok(monitor(sup.api(), live, "PATCH", mode="listen"))


def test_a_supervisor_token_naming_another_tenants_supervisor_is_422_and_one_of_its_own_tenant_403(world, tenant):
    live, sup = floor(tenant)
    theirs = world.new_tenant("Other").agent("Theirs", role="supervisor").id
    error(monitor(sup.api(), live, mode="listen", supervisor_id=theirs), 422, "foreign_id")
    error(monitor(sup.api(), live, mode="listen", supervisor_id=tenant.agent("Peer", role="supervisor").id), 403)
    assert sup.phone.legs == []


def test_a_supervisor_without_the_monitor_grant_cannot_listen_nor_a_tenant_key_acting_for_it(tenant):
    live, sup = floor(tenant)
    settings(tenant, permissions={"supervisor": {"monitor": False}})
    error(monitor(sup.api(), live, mode="listen"), 403, "monitor_not_allowed")
    error(monitor(tenant.api, live, mode="listen", supervisor_id=sup.id), 403, "monitor_not_allowed")
    assert sup.phone.legs == []
