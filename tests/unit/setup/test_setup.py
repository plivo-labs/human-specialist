"""Routing config: is-open, queue rules, and the XML each action renders."""

from datetime import UTC, datetime

import pytest
from pydantic import TypeAdapter, ValidationError

from hs.setup import actions, api

NOON_MONDAY = datetime(2026, 10, 19, 12, 0, tzinfo=UTC)
ACTION = TypeAdapter(actions.Action)


def hours(slots=(), holidays=()) -> dict:
    return {"time_zone": "UTC", "slots": list(slots), "holidays": list(holidays)}


@pytest.mark.parametrize(
    ("h", "is_open"),
    [
        (hours([{"day": "mon", "start": "09:00", "end": "17:00"}]), True),
        (hours([{"day": "mon", "start": "12:00", "end": "17:00"}]), True),
        (hours([{"day": "mon", "start": "09:00", "end": "12:00"}]), False),
        (hours([{"day": "tue", "start": "00:00", "end": "24:00"}]), False),
        (hours([{"day": "mon", "start": "00:00", "end": "24:00"}], [{"date": "2026-10-19", "slots": []}]), False),
        (hours([], [{"date": "2026-10-19", "slots": [{"start": "11:00", "end": "13:00"}]}]), True),
    ],
)
def test_is_open(h, is_open):
    assert api.is_open(h, NOON_MONDAY) is is_open


def test_is_open_reads_the_local_clock():
    new_york = {**hours([{"day": "mon", "start": "08:00", "end": "09:00"}]), "time_zone": "America/New_York"}
    assert api.is_open(new_york, NOON_MONDAY)


def test_queue_defaults_and_simultaneous_forces_no_retries():
    q = api.QueueIn(name="Q")
    assert (q.strategy, q.agents_at_once, q.retries, q.priority, q.capacity, q.wait_timeout_sec) == (
        "Availability",
        3,
        3,
        5,
        50,
        20,
    )
    assert api.QueueIn(name="Q", strategy="Simultaneous", retries=3).retries == 0


def test_a_menu_holds_at_most_nine_options():
    options = {str(d): {"type": "hangup"} for d in range(10)}
    with pytest.raises(ValidationError):
        api.MenuIn(name="M", greeting={"text": "Hi"}, options=options)


def test_patch_bodies_take_any_subset_but_no_read_only_field():
    patch = api._partial(api.QueueIn)
    assert patch(overflow_action={"type": "hangup"}).model_dump(exclude_unset=True) == {
        "overflow_action": {"type": "hangup"}
    }
    with pytest.raises(ValidationError):
        patch(enqueue_url="https://evil.example/x")


def render(action: dict) -> str:
    return actions.xml(ACTION.validate_python(action), "t1", "c1")


def test_hangup_speaks_its_greeting_escaped():
    assert render({"type": "hangup", "greeting": {"text": "A & B"}}) == "<Speak>A &amp; B</Speak><Hangup/>"


def test_audio_greetings_play():
    assert render({"type": "hangup", "greeting": {"audio_url": "https://x/a.mp3"}}) == (
        "<Play>https://x/a.mp3</Play><Hangup/>"
    )


def test_forward_dials_the_number():
    assert render({"type": "forward", "number": "+14155550100"}) == "<Dial><Number>14155550100</Number></Dial>"


def test_redirect_carries_the_conversation_id():
    assert render({"type": "redirect", "url": "https://app.example/x?a=1"}) == (
        "<Redirect>https://app.example/x?a=1&amp;conversation_id=c1</Redirect>"
    )


def test_voicemail_records_to_the_voicemail_action():
    mailbox = {"queue_id": "6f1c1a3e-6f0e-4b43-8a32-8f6c3b1f0c11"}
    got = render({"type": "voicemail", "mailbox": mailbox, "transcribe": True, "max_length_sec": 30})
    assert "/plivo/t1/actions/voicemail?queue_id=6f1c1a3e" in got and 'maxLength="30"' in got
    assert 'transcriptionUrl="http://testserver/plivo/t1/transcription"' in got


def test_a_menu_attempt_reports_no_input_through_its_redirect():
    menu = {"greeting": {"text": "Press 1"}, "input_timeout_sec": 4}
    got = actions.menu_xml("t1", "m1", menu, 2)
    url = "http://testserver/plivo/t1/actions/menu?menu_id=m1&amp;attempt=2"
    assert got == (
        f'<GetDigits action="{url}" method="POST" numDigits="1" retries="1" timeout="4">'
        f"<Speak>Press 1</Speak></GetDigits><Redirect>{url}</Redirect>"
    )
