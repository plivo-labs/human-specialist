"""The catalog events the conversation lifecycle emits: call.ended, conversation.closed, conversation.wrapped_up."""

from datetime import timedelta
from types import SimpleNamespace as Row

import hs.conversations.child_update as ccu
import hs.conversations.management as cm
import tests.conftest  # noqa: F401
from hs.infra.helpers import get_now

PARENT = Row(
    organization=1,
    uuid="00000000-0000-4000-a000-000000000001",
    conversation_number="CN1",
    contact_identifier="+14155550100",
    source_direction_inbound=True,
)


def test_call_ended_takes_who_hung_up_from_the_mpc_termination_chunk(monkeypatch, emitted):
    now = get_now()
    monkeypatch.setattr(ccu, "get_now", lambda: now)
    hangups = [Row(attributes=[Row(hangup_source="Agent")]), Row(attributes=[Row(hangup_source="Customer")])]
    monkeypatch.setattr(
        ccu.ConversationChunk, "get_conversation_chunks_by_conv_number", classmethod(lambda cls, n, **k: hangups)
    )
    monkeypatch.setattr(ccu.ActiveQueueEntry, "get_by_id", classmethod(lambda cls, i: Row(queue_uuid="q-1")))
    parent = Row(**vars(PARENT), parent_start_at=now - timedelta(seconds=90))
    conv = Row(
        parent_conversation=parent,
        sender_id="+14155550199",
        active_queue_entry_id=3,
        start_at=now - timedelta(seconds=60),
        primary_agent=Row(uuid="aom-1"),
    )
    ccu._emit_call_ended(conv)
    [(kind, data, kw)] = emitted
    assert (kind, kw["agent_id"]) == ("call.ended", "aom-1")
    assert data == {
        "from": "+14155550100",
        "to": "+14155550199",
        "result": "answered",
        "queue_id": "q-1",
        "hangup_cause": None,
        "hangup_cause_code": None,
        "duration_sec": 90,
        "talk_sec": 60,
        "ended_by": "customer",
    }


def _closed(monkeypatch, *children, result=None):
    now = get_now()
    parent = Row(id=7, child_convs=list(children), created_at=now - timedelta(seconds=30))
    monkeypatch.setattr(
        ccu.ConversationParent, "get_parent_conversation_preloaded", classmethod(lambda cls, n: [parent])
    )
    monkeypatch.setattr(ccu, "get_db_session", lambda: Row(scalar=lambda query: result))
    return ccu._closed_event(PARENT, now)


def _note(disposition, dispo=None, by="aom-1"):
    return Row(type="notes", disposition=disposition, dispo=dispo, sub_dispo=None, recording=None, from_user=by)


def test_closed_names_each_agents_disposition_and_marks_the_timers_own_as_skipped(monkeypatch):
    sale, auto = _note("Sale", Row(uuid="d-1", internal=False)), _note("Auto-closed", Row(uuid="d-2", internal=True))
    auto.from_user = "hs"
    text = Row(type="text", disposition=None, recording=None)
    first = Row(id=1, primary_agent=Row(uuid="aom-1"), wrap_up_start_at=get_now(), chunks=[sale, text])
    second = Row(id=2, primary_agent=Row(uuid="aom-2"), wrap_up_start_at=get_now(), chunks=[auto])
    data, last_agent = _closed(monkeypatch, second, first)  # newest first, as the preloaded query orders them
    assert last_agent == "aom-2" and data["result"] == "answered" and data["duration_sec"] == 30
    assert [(d["agent_id"], d["disposition_id"], d["system"], d["skipped"]) for d in data["dispositions"]] == [
        ("aom-1", "d-1", False, False),
        ("aom-2", "d-2", True, True),
    ]


def test_closed_takes_the_recorded_result_and_lists_an_unanswered_calls_system_disposition(monkeypatch):
    timeout = _note("Queue timeout", Row(uuid="d-3", internal=True), by="hs")
    unanswered = Row(id=1, primary_agent=None, wrap_up_start_at=None, chunks=[timeout])
    data, last_agent = _closed(monkeypatch, unanswered, result="queue_timeout")
    assert (data["result"], last_agent) == ("queue_timeout", None)
    assert data["dispositions"] == [
        {"agent_id": None, "disposition_id": "d-3", "sub_disposition_id": None, "system": True, "skipped": False}
    ]
    data, _ = _closed(monkeypatch, Row(id=1, primary_agent=None, wrap_up_start_at=None, chunks=[]))
    assert data["result"] == "abandoned"


def test_an_auto_closed_wrap_up_is_skipped(monkeypatch, emitted):
    monkeypatch.setattr("hs.infra.context.get_db_session", lambda: "session")
    chunk = Row(disposition_id=None, disposition="Auto-closed", sub_disposition=None, description=None)
    cm._emit_wrapped_up(Row(parent_conversation=PARENT), chunk, "aom-1", skipped=True)
    [(kind, data, kw)] = emitted
    assert (kind, kw["session"]) == ("conversation.wrapped_up", "session")
    assert data == {
        "disposition_id": None,
        "disposition": "Auto-closed",
        "sub_disposition": None,
        "note": None,
        "system": True,
        "skipped": True,
    }
