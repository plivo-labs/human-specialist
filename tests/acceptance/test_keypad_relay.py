"""The digits an agent presses on a call reach the other party: HS seats each of its agents as a talking party with
Plivo's DTMF relay on, and the customer's leg, a dialed-in number and a supervisor without it."""

from tests.acceptance.conftest import dial, lead, ok
from tests.acceptance.test_inbound_routing import room_xml
from tests.acceptance.test_supervisor import floor, monitor
from tests.acceptance.test_voice_calls import rings_to


def relays(params: dict) -> bool:
    return str(params.get("relay_dtmf_inputs", "")).lower() == "true"


def test_an_agent_who_takes_a_call_relays_its_digits_and_the_caller_and_a_supervisor_do_not(world, tenant):
    live, sup = floor(tenant)
    assert [relays(r) for r in rings_to(world, live.agent.endpoint_username)] == [True]
    assert room_xml(world, live.caller).get("relayDTMFInputs") in (None, "false")
    for mode in ("listen", "whisper", "barge"):
        ok(monitor(sup.api(), live, mode=mode))
        sup.phone.wait_ring()
        ok(monitor(sup.api(), live, method="DELETE"))
    assert [relays(r) for r in rings_to(world, sup.endpoint_username)] == [False, False, False]


def test_an_agent_who_dials_out_relays_its_digits_and_the_lead_does_not(world, tenant):
    number, phone = lead(world)
    stream = tenant.stream(after=0)
    agent, cid = dial(tenant, to=number)
    leg = agent.phone.wait_ring()
    phone.wait_ring()
    stream.wait_for("call.answered", conversation_id=cid)
    assert room_xml(world, leg).get("relayDTMFInputs") == "true"
    assert [relays(r) for r in rings_to(world, number)] == [False]
