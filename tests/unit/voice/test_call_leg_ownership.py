"""A callback signed for its own call names whatever leg UUID it likes; only that call's own legs are acted on."""

import uuid

import hs.voice.call_legs as cm
from hs.infra.context import get_db_session
from hs.voice.call_models import CallLegInsertModel
from hs.voice.call_store import CallLeg

START, END, FORGED = "2026-11-02 10:00:00", "2026-11-02 10:05:00", "2000-01-01 00:00:00"


def _id() -> str:
    return str(uuid.uuid4())


def leg(call_uuid: str, leg_uuid: str, role: str = "customer", start: str = START, end: str | None = None):
    member = "+14155550100"
    return CallLegInsertModel(
        call_uuid=call_uuid, call_leg_uuid=leg_uuid, role=role, member_address=member, start_time=start, end_time=end
    )


def call(call_uuid: str, *legs: tuple[str, str]) -> None:
    """A call's parent row and its participant legs, as its MPC and join callbacks store them."""
    cm.create_parent_call_leg(CallLegInsertModel(call_uuid=call_uuid, call_leg_uuid=call_uuid, start_time=START))
    for leg_uuid, role in legs:
        assert cm.create_call_leg(leg(call_uuid, leg_uuid, role)).status_code == 201


def stored(leg_uuid: str) -> list[tuple]:
    found = CallLeg.select_query(get_db_session()).filter(CallLeg.call_uuid == leg_uuid)
    return sorted((r.role or "", r.start_time, r.end_time) for r in found)


def test_the_inbound_callers_exit_ends_its_leg_whose_uuid_is_the_calls():
    c = _id()
    call(c, (c, "customer"))
    assert cm.finalize_call_leg(leg(c, c, end=END)).status_code == 200
    assert stored(c) == [("", START, END), ("customer", START, END)]


def test_on_an_outbound_call_the_agents_leg_is_the_call_and_the_leads_leg_is_its_child():
    agent, lead = _id(), _id()
    call(agent, (agent, "Agent"), (lead, "customer"))
    assert cm.finalize_call_leg(leg(agent, lead, end=END)).status_code == 200
    assert cm.finalize_call_leg(leg(agent, agent, "agent", end=END)).status_code == 200
    assert stored(lead) == [("customer", START, END)]
    assert stored(agent) == [("", START, END), ("Agent", START, END)]


def test_an_exit_that_beats_its_call_stores_the_call_and_the_leg_under_it():
    c, caller = _id(), _id()
    assert cm.finalize_call_leg(leg(c, caller, end=END)).status_code == 200
    assert CallLeg.get_by_call_uuid(caller, role="customer").parent_call_leg == int(CallLeg.get_by_call_uuid(c).id)
    assert stored(caller) == [("customer", START, END)]


def test_a_leg_of_another_call_is_refused_and_left_as_it_was():
    mine, theirs, their_agent = _id(), _id(), _id()
    call(mine)
    call(theirs, (theirs, "customer"), (their_agent, "Agent"))
    for named in (their_agent, theirs):
        assert cm.finalize_call_leg(leg(mine, named, "agent", end=FORGED)).error == cm.MSG_CALL_LEG_FOREIGN
        for role in ("Agent", "customer", "supervisor"):
            assert cm.create_call_leg(leg(mine, named, role, start=FORGED)).error == cm.MSG_CALL_LEG_FOREIGN
    assert stored(their_agent) == [("Agent", START, None)]
    assert stored(theirs) == [("", START, None), ("customer", START, None)]
