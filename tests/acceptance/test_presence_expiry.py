"""Presence after the heartbeats stop: the sweeper marks the agent Offline, two pings bring it back, and an
expired session does not touch a call in progress."""

from tests.acceptance.conftest import activity, connect, error


def stop_pinging(agent) -> None:
    """The agent's last ping is older than the 40 s limit: Redis expires its ping record (in 50 ms rather than 40 s)."""
    world = agent.tenant.world
    assert world.redis.pexpire(f"LST:{agent.id}", 50)
    world.wait(lambda: not world.redis.exists(f"LST:{agent.id}"), what="the ping record to expire")


def status_name(agent) -> str:
    return agent._status()


def wait_offline(agent) -> None:
    """Stop the agent's pings and wait for the sweeper (every 15 s) to set it Offline."""
    stop_pinging(agent)
    agent.tenant.world.wait(lambda: status_name(agent) == "Offline", timeout=25, what="the sweeper to set Offline")


def test_a_swept_agent_needs_two_quick_pings_to_come_back(tenant):
    agent = tenant.agent().login()
    wait_offline(agent)
    agent.heartbeat()
    assert status_name(agent) == "Offline"
    agent.heartbeat()
    tenant.world.wait(lambda: status_name(agent) == "Available", what="Available after the second ping")


def test_a_desk_that_beats_too_slowly_is_reset_before_each_second_ping_and_never_recovers(tenant):
    agent = tenant.agent().login()
    wait_offline(agent)
    for _ in range(2):
        agent.heartbeat()
        assert status_name(agent) == "Offline"
        stop_pinging(agent)
        tenant.world.wait(
            lambda: int(tenant.world.redis.get(f"AOMPC:{agent.id}") or 0) == 0, timeout=25, what="the count reset"
        )
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
