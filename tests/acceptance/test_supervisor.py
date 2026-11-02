"""Supervisors: listen, whisper, barge with a spoken notice, switch and leave; access by team."""

from tests.acceptance.conftest import (
    activity,
    connect,
    error,
    fence,
    fresh_second,
    hang_up_with_the_room_alive,
    hs_header,
    ok,
    settings,
    team,
)

NOTICE = "A supervisor has joined the call"


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


def test_a_supervisor_who_rejoins_after_a_cold_transfer_and_leaves_again_is_off_the_call(tenant):
    live, sup = floor(tenant)
    target = teammate_of(tenant, live.agent)
    ok(monitor(sup.api(), live, mode="listen"))
    joined_leg(sup, live)
    cold = {"mode": "cold", "target": {"type": "agent", "agent_id": target.id}}
    ok(live.agent.api().post(f"/api/v1/calls/{live.id}/transfers", json=cold))
    target.phone.wait_ring().wait_state("in_progress")
    live.stream.wait_for("call.transferred", conversation_id=live.id)
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


def test_a_monitor_join_sent_after_the_hangup_is_refused(world, tenant):
    live, sup = floor(tenant)
    hang_up_with_the_room_alive(world, tenant, live)
    error(monitor(sup.api(), live, mode="listen"), 409, "call_not_connected")
    fence(tenant)
    assert sup.phone.legs == [] and not tenant.events(type="supervisor.joined")


def test_a_monitor_switch_sent_after_the_hangup_is_refused(world, tenant):
    live, sup = floor(tenant)
    ok(monitor(sup.api(), live, mode="listen"))
    joined_leg(sup, live)
    hang_up_with_the_room_alive(world, tenant, live)
    error(monitor(sup.api(), live, "PATCH", mode="whisper"), 409, "call_not_connected")
    fence(tenant)
    assert not tenant.events(type="supervisor.mode_changed")


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
    error(monitor(sup.api(), live, "DELETE"), 404, "not_monitored")
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


def test_the_monitored_agent_sees_supervisor_events(tenant):
    t = team(tenant, "Floor")
    sup = tenant.agent("Sup", role="supervisor", teams=[t]).login()
    agent = tenant.agent("Agent", teams=[t]).login()
    desk = tenant.stream(token=agent.stream_token, after=0)
    live = connect(tenant, agent)
    ok(monitor(sup.api(), live, mode="listen"))
    assert desk.wait_for("supervisor.joined", conversation_id=live.id)["agent_id"] == agent.id


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
