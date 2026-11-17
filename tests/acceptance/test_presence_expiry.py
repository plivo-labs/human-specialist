"""Presence after the heartbeats stop: the sweeper marks the agent Offline, two pings bring it back, and an
expired session does not touch a call in progress. Liveness holds across a Redis loss and on two replicas."""

import contextlib

from tests.acceptance.conftest import activity, connect, error, ok, replayed, set_status, settings
from tests.harness import World


def status_name(agent) -> str:
    return agent._status()


def wait_offline(agent) -> None:
    """Stop the agent's pings and wait for the sweeper (every 15 s) to set it Offline."""
    agent.stop_pinging()
    agent.tenant.world.wait(lambda: status_name(agent) == "Offline", timeout=25, what="the sweeper to set Offline")


def test_a_swept_agent_needs_two_quick_pings_to_come_back(tenant):
    agent = tenant.agent().login()
    wait_offline(agent)
    agent.heartbeat()
    assert status_name(agent) == "Offline"
    agent.heartbeat()
    tenant.world.wait(lambda: status_name(agent) == "Available", what="Available after the second ping")


def changes(tenant, agent) -> list[str]:
    return [e["data"]["to_status"]["name"] for e in replayed(tenant, "agent.status_changed", agent_id=agent.id)]


def test_an_agent_on_break_keeps_it_when_its_desk_comes_back_after_a_silence(tenant):
    agent = tenant.agent().login()
    agent.set_status("Break")
    before = changes(tenant, agent)
    agent.stop_pinging()
    for _ in range(3):
        agent.heartbeat()
    assert status_name(agent) == "Break"
    assert changes(tenant, agent) == before, "neither the silence nor the return changes the status"


def test_a_silent_desk_on_break_that_is_set_available_is_signed_out_by_the_next_sweep(world, tenant):
    agent = tenant.agent().login()
    agent.set_status("Break")
    agent.stop_pinging()
    ok(set_status(tenant, agent, "Available"))
    world.travel(15)
    world.wait(lambda: status_name(agent) == "Offline", 20, "Offline once a sweep finds the silent desk available")


def test_with_another_sign_out_status_only_break_survives_a_silence_and_offline_signs_in_as_before(world):
    tenant = world.new_tenant("Elsewhere")
    out = ok(tenant.api.post("/api/v1/agent-statuses", json={"name": "Signed out", "availability": False}))
    settings(tenant, sign_out={"status_id": out["id"]})
    desks = {name: tenant.agent(name=name).login() for name in ("Break", "Signed out", "Offline")}
    for name, agent in desks.items():
        agent.set_status(name)
        agent.stop_pinging()
        agent.heartbeat()
        agent.heartbeat()
    assert {name: status_name(agent) for name, agent in desks.items()} == {
        "Break": "Break",
        "Signed out": "Available",
        "Offline": "Available",
    }


def test_a_new_session_signs_in_an_agent_on_break_whose_desk_fell_silent(tenant):
    agent = tenant.agent().login()
    agent.set_status("Break")
    agent.stop_pinging()
    agent.login(available=False)
    agent.heartbeat()
    assert status_name(agent) == "Break"
    agent.heartbeat()
    assert status_name(agent) == "Available"


def test_a_desk_that_beats_too_slowly_is_reset_before_each_second_ping_and_never_recovers(tenant):
    agent = tenant.agent().login()
    wait_offline(agent)
    for _ in range(2):
        agent.heartbeat()
        assert status_name(agent) == "Offline"
        agent.stop_pinging()
    assert status_name(agent) == "Offline"


def test_an_expired_session_gets_401_and_the_agent_goes_offline(tenant):
    agent = tenant.agent().login()
    agent.expire_tokens()
    error(agent.api().post("/api/v1/me/heartbeat"), 401)
    wait_offline(agent)


def test_a_session_that_expires_during_a_call_leaves_the_call_running(tenant):
    live = connect(tenant)
    live.agent.expire_tokens()
    error(live.agent.api().post("/api/v1/me/heartbeat"), 401)
    wait_offline(live.agent)
    assert live.caller.state == live.leg.state == "in_progress"
    assert activity(tenant, live.agent) == "on_call"
    live.caller.hangup()
    live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id)


def test_across_a_redis_loss_a_beating_desk_stays_signed_in_and_on_break(world, tenant):
    agent = tenant.agent().login()
    agent.set_status("Break")
    with world.redis_down():
        error(agent.api().post("/api/v1/me/heartbeat"), 503, "heartbeat_failed")
    agent.heartbeat()
    agent.heartbeat()
    assert status_name(agent) == "Break", "a count lost with Redis never signs a desk in again"
    world.travel(60)
    assert status_name(agent) == "Break"


def test_a_desk_stopped_before_a_redis_loss_goes_offline_40_s_after_redis_returns(world, tenant):
    agent = tenant.agent().login(available=False)
    for _ in range(2):
        ok(agent.api().post("/api/v1/me/heartbeat"))
    assert status_name(agent) == "Available"
    with world.redis_down():
        world.travel(30)
    world.travel_until_rebuilt()
    assert status_name(agent) == "Available", "Redis came back empty, so every desk signed in gets 40 s to beat"
    world.travel(41)
    # The sweep runs every 15 s, so Offline can lag the jump by up to 15 s.
    world.wait(lambda: status_name(agent) == "Offline", 20, "Offline once the 40 s after Redis returned ran out")


def test_a_desk_beating_only_on_the_replica_stays_online_while_the_first_sweeps():
    with contextlib.closing(World()) as world:
        agent = world.replica().new_tenant().agent().login()
        world.travel(60)
        assert status_name(agent) == "Available"
