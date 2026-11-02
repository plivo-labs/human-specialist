"""Voicemail cut short or left running, the hangup action on a number, and the * key in nested menus."""

import xml.etree.ElementTree as ET

from tests.acceptance.conftest import (
    answer_params,
    hangup_action,
    hours,
    in_order,
    menu,
    menu_line,
    plivo_post,
    queue_action,
    seen,
    voicemail_action,
)


def voicemail_line(tenant, **action) -> str:
    q = tenant.queue()
    number = tenant.number()
    tenant.route_number(
        number,
        queue_action(q),
        business_hours_id=hours(tenant, weekly=False)["id"],
        closed_action=voicemail_action(q, **action),
    )
    return number


def test_the_voicemail_record_is_capped_at_the_configured_length_after_the_greeting(world, tenant):
    number = voicemail_line(tenant, max_length_sec=7)
    answered = plivo_post(world, tenant.id, "answer", answer_params(number), tenant.auth_token)
    doc = ET.fromstring(answered.text)  # noqa: S314 (HS's own reply)
    assert [el.tag for el in doc if el.tag in ("Speak", "Record")] == ["Speak", "Record"]
    assert doc.find("Record").get("maxLength") == "7"


def test_a_caller_who_hangs_up_during_the_greeting_leaves_no_voicemail(world, tenant):
    number = voicemail_line(tenant)
    stream = tenant.stream(after=0)
    params = answer_params(number)
    assert plivo_post(world, tenant.id, "answer", params, tenant.auth_token).status_code == 200
    hangup = {**params, "CallStatus": "completed", "HangupCauseCode": "4000", "HangupCauseName": "Normal Hangup"}
    plivo_post(world, tenant.id, "hangup", {**hangup, "HangupSource": "Caller", "Duration": "1"}, tenant.auth_token)
    world.run_jobs("close_call")
    closed = stream.wait_for("conversation.closed")
    assert closed["data"]["result"] == "after_hours" and not seen(stream, "voicemail.received")


def test_a_closed_number_with_no_closed_action_hangs_up(world, tenant):
    q = tenant.queue()
    number = tenant.number()
    tenant.route_number(number, queue_action(q), business_hours_id=hours(tenant, weekly=False)["id"])
    stream = tenant.stream(after=0)
    caller = world.plivo.call(number)
    caller.wait_state("completed")
    assert stream.wait_for("conversation.after_hours")["data"]["number"] == number
    assert not seen(stream, "conversation.queued")


def test_a_number_that_hangs_up_plays_its_greeting_first(world, tenant):
    number = tenant.number()
    tenant.route_number(number, hangup_action("This number is not in service"))
    caller = world.plivo.call(number)
    caller.wait_state("completed")
    assert caller.said == ["This number is not in service"]


def test_a_number_that_hangs_up_without_a_greeting_says_nothing(world, tenant):
    number = tenant.number()
    tenant.route_number(number, {"type": "hangup"})
    caller = world.plivo.call(number)
    caller.wait_state("completed")
    assert caller.said == []


def test_the_star_key_picks_an_option_in_a_nested_menu(world, tenant):
    billing = menu(tenant, {"*": hangup_action("Back to the operator")}, greeting="Billing menu")
    number, _ = menu_line(tenant, {"1": {"type": "menu", "menu_id": billing["id"]}})
    caller = world.plivo.call(number, digits="1*")
    caller.wait_state("completed")
    assert in_order(caller.said, "Press a key", "Billing menu", "Back to the operator")
