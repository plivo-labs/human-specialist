"""Ring timeout, ring-no-answer strikes and the RONA status move; only available agents are offered."""

from tests.acceptance.conftest import connect, fence, finish, hs_header, ok, seen, settings


def ringing_out(tenant, agent, behaviour: str = "timeout") -> str:
    """A number whose queue holds only `agent`, who lets every offer end with `behaviour`."""
    agent.phone.on_ring(behaviour)
    number, _ = tenant.inbound_line(agent, retries=0)
    return number


def status_name(tenant, agent) -> str:
    return ok(tenant.api.get(f"/api/v1/agents/{agent.id}/status"))["status"]["name"]


def test_the_ring_timeout_reaches_the_offer_and_a_change_applies_to_the_next_call(tenant):
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    number = ringing_out(tenant, agent, "decline")
    tenant.world.plivo.call(number)
    assert stream.wait_for("offer.created", agent_id=agent.id)["data"]["ring_timeout_sec"] == 30
    stream.wait_for("offer.rejected", agent_id=agent.id)
    settings(tenant, ring_timeout_sec=9)
    tenant.world.plivo.call(number)
    stream.wait_for("offer.created", agent_id=agent.id, ring_timeout_sec=9)


def test_two_missed_rings_move_the_agent_to_break(tenant):
    settings(tenant, ring_timeout_sec=5, rona={"strikes": 2})
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    number = ringing_out(tenant, agent)
    for strike in (1, 2):
        tenant.world.plivo.call(number)
        stream.wait_for("offer.missed", agent_id=agent.id, strike=strike)
    changed = stream.wait_for("agent.status_changed", agent_id=agent.id, reason="rona")
    assert changed["data"]["to_status"]["name"] == "Break"
    assert status_name(tenant, agent) == "Break"


def test_the_rona_target_status_is_configurable(tenant):
    missed = ok(tenant.api.post("/api/v1/agent-statuses", json={"name": "Missed calls", "availability": False}))
    settings(tenant, rona={"strikes": 1, "status_id": missed["id"]})
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    tenant.world.plivo.call(ringing_out(tenant, agent))
    changed = stream.wait_for("agent.status_changed", agent_id=agent.id, reason="rona")
    to = changed["data"]["to_status"]
    assert (to["id"], to["name"]) == (missed["id"], "Missed calls")


def test_an_agent_back_from_break_starts_at_zero_strikes(tenant):
    settings(tenant, rona={"strikes": 2})
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    number = ringing_out(tenant, agent)
    for strike in (1, 2):
        tenant.world.plivo.call(number)
        stream.wait_for("offer.missed", agent_id=agent.id, strike=strike)
    stream.wait_for("agent.status_changed", agent_id=agent.id, reason="rona")
    agent.set_status("Available")
    tenant.world.plivo.call(number)
    third = tenant.world.wait(lambda: seen(stream, "offer.missed", agent_id=agent.id)[2:], what="third miss")
    assert third[0]["data"]["strike"] == 1 and status_name(tenant, agent) == "Available"


def test_declines_are_not_strikes(tenant):
    settings(tenant, rona={"strikes": 1})
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    number = ringing_out(tenant, agent, "decline")
    for _ in range(2):
        tenant.world.plivo.call(number)
    tenant.world.wait(lambda: len(seen(stream, "offer.rejected")) == 2, what="two rejects")
    for leg in agent.phone.legs:
        leg.wait_state("completed")
    fence(tenant)
    assert not tenant.events(type="offer.missed") and status_name(tenant, agent) == "Available"


def test_a_losing_simultaneous_invite_is_not_a_strike(tenant):
    settings(tenant, rona={"strikes": 1})
    winner, loser = tenant.agent("Winner").login(), tenant.agent("Loser").login()
    loser.phone.on_ring("ignore")
    live = connect(tenant, winner, also=[loser], strategy="Simultaneous")
    live.stream.wait_for("offer.withdrawn", agent_id=loser.id, reason="taken")
    loser.phone.legs[0].wait_state("completed")
    fence(tenant)
    assert not tenant.events(type="offer.missed") and status_name(tenant, loser) == "Available"


def test_an_accepted_offer_resets_the_strikes(tenant):
    settings(tenant, rona={"strikes": 2}, wrap_up={"mode": "manual", "seconds": 1})
    agent = tenant.agent().login()
    stream = tenant.stream(after=0)
    tenant.world.plivo.call(ringing_out(tenant, agent))
    stream.wait_for("offer.missed", agent_id=agent.id, strike=1)
    agent.phone.wait_ring()  # the missed leg, so connect() takes the next one
    finish(connect(tenant, agent))
    tenant.world.plivo.call(ringing_out(tenant, agent))
    missed = tenant.world.wait(lambda: seen(stream, "offer.missed")[1:], what="second miss")
    assert missed[0]["data"]["strike"] == 1 and status_name(tenant, agent) == "Available"


def test_only_available_agents_are_offered(tenant):
    away, available = tenant.agent("Away").login(), tenant.agent("Ready").login()
    away.set_status("Away")
    number, _ = tenant.inbound_line(away, available)
    tenant.world.plivo.call(number)
    assert hs_header(available.phone.wait_ring(), "HsKind") == "offer"
    assert away.phone.legs == []
