"""build_row derives the denormalized closed-conversation row.

Synthetic conversation graphs; the agents' teams and skills and the Dispositions lookup
are supplied explicitly so the test exercises the derivation logic in isolation.
"""

import datetime
from types import SimpleNamespace

import tests.conftest  # noqa: F401
from hs.conversations import search as ccsm
from hs.conversations.models import CONVERSATION_ANSWERED_STATES, ConversationChunkTypes

ANSWERED_STATE = sorted(CONVERSATION_ANSWERED_STATES)[0]
AGENT_UUID = "11111111-1111-4111-a111-111111111111"


def _dt(h=0, m=0, s=0):
    return datetime.datetime(2026, 6, 28, h, m, s, tzinfo=datetime.UTC)


def _chunk(**kw):
    base = {
        "type": "chat",
        "disposition": None,
        "sub_disposition": None,
        "description": None,
        "attributes": None,
        "timestamp": _dt(10, 0, 0),
        "channel": "chat",
        "message_id": None,
        "from_user": "+15551230000",
        "to_user": "+15551230000",
        "uuid": "c0000000-0000-4000-a000-000000000000",
        "created_at": _dt(10, 0, 0),
        "channel_meta": None,
        "disposition_id": None,
        "sub_disposition_id": None,
        "dispo": None,
        "sub_dispo": None,
    }
    base.update(kw)
    return SimpleNamespace(**base)


def _child(chunks, **kw):
    base = {
        "created_at": _dt(9, 0, 0),
        "primary_aom": "aom-1",
        "state": ANSWERED_STATE,
        "channel": "chat",
        "sender_id": None,
        "customer_contact": "+15551230000",
        "start_at": _dt(9, 0, 0),
        "end_at": _dt(9, 5, 0),
        "wrap_up_start_at": _dt(9, 4, 0),
        "contact_creation_start_at": None,
        "case_creation_start_at": None,
        "direction_inbound": True,
        "primary_agent": SimpleNamespace(uuid=AGENT_UUID),
        "generation_type": "qnr_generated",
        "active_queue_entry_id": 7,
        "chunks": chunks,
    }
    base.update(kw)
    return SimpleNamespace(**base)


def _parent(children, **kw):
    base = {
        "conversation_number": "001-9990001112",
        "organization": 1,
        "org": SimpleNamespace(uuid="org-uuid"),
        "contact_identifier": "+15551230000",
        "case_id": "C-1",
        "crm_contact_id": "CRM1",
        "channel": "chat",
        "direction_inbound": True,
        "contact_name": "Jane Doe",
        "created_at": _dt(9, 0, 0),
        "end_at": _dt(9, 6, 0),
        "updated_at": _dt(9, 6, 30),
        "customer_conversation_data": None,
        "child_convs": children,
    }
    base.update(kw)
    return SimpleNamespace(**base)


def _agent_skills(aom="aom-1"):
    return ({"data": {aom: {"agent_id": AGENT_UUID, "agent_name": "Agent A", "teams": [{"team_id": "t1"}]}}}, 200)


def _dispos():
    answered = SimpleNamespace(name=ccsm.DISPOSITION_FOR_CLOSED_STATE, uuid="ans-uuid", answered=True)
    auto = SimpleNamespace(name=ccsm.DISPOSITION_FOR_AUTO_CLOSED, uuid="auto-uuid", answered=True)
    unans = SimpleNamespace(name=ccsm.DISPOSITION_FOR_UNANSWERED, uuid="unans-uuid", answered=False)
    return [answered, auto, unans]


def test_search_text_flattens_lists_and_skips_none():
    row = SimpleNamespace(
        customer_name="Jane Doe",
        wrap_up_note=None,
        disposition="Resolved",
        conv_state="Customer answered",
        contact_identifier="+15551230000",
        conv_chunk_descriptions=["hi there", "bye"],
        case_id=None,
        crm_contact_id="CRM1",
        csat_feedback="Happy",
        sub_disposition=None,
        conv_child_customer_contacts=["+15551230000"],
        conv_child_sender_ids=[],
        hangup_source=None,
    )
    txt = ccsm._search_text(row)
    assert "Jane Doe" in txt and "hi there" in txt and "Resolved" in txt and "CRM1" in txt
    assert "None" not in txt  # None-valued fields are skipped, not stringified


def test_build_row_core_fields_and_agent_ids(monkeypatch):
    monkeypatch.setattr(
        ccsm.AccountOrgMapping,
        "get_account_by_aom_uuid",
        lambda *a, **k: SimpleNamespace(name="Agent A", email="a@example.com"),
    )
    notes = _chunk(
        type=ConversationChunkTypes.notes.name,
        disposition="Resolved",
        disposition_id=5,
        dispo=SimpleNamespace(answered=True, name="Resolved", uuid="d-uuid"),
        description="wrapped up",
        timestamp=_dt(9, 5, 0),
        created_at=_dt(9, 5, 0),
    )
    msg = _chunk(
        type="chat",
        description="hello world",
        message_id="m1",
        channel_meta={"status": "delivered"},
        to_user="+15551230000",
        created_at=_dt(9, 1, 0),
    )
    parent = _parent([_child([msg, notes])])

    row = ccsm.build_row(
        parent, org_uuid="org-uuid", agent_team_skills=_agent_skills()[0], internal_dispositions=_dispos()
    )

    assert row is not None
    assert row.conv_number == "001-9990001112"
    assert row.organization == 1
    assert row.customer_name == "Jane Doe"
    assert row.parent_channel == "chat"
    assert row.conv_channels == ["chat"]
    assert row.agent_ids == [AGENT_UUID]  # RBAC source — from child.primary_agent.uuid
    assert row.is_conv_answered is True  # state is an answered state
    assert row.disposition == "Resolved"
    assert row.conv_state == ccsm.DISPOSITION_FOR_CLOSED_STATE
    assert row.wrap_up_note == "wrapped up"
    assert row.conv_total_duration == 360  # 9:00:00 -> 9:06:00 = 360s
    assert row.conv_message_count == 1  # the chat msg; notes is agent-specific
    assert "t1" in row.team_ids
    assert row.search_vector is not None  # tsvector func expression set


def test_build_row_csat_and_unanswered(monkeypatch):
    monkeypatch.setattr(ccsm.AccountOrgMapping, "get_account_by_aom_uuid", lambda *a, **k: None)
    feedback = _chunk(
        type=ConversationChunkTypes.customer_feedback.name,
        attributes=[
            {"feedback_type": "CSAT", "min_scale": 1, "max_scale": 5, "component_name": "csat", "flow_name": "flow"}
        ],
        description="Very Satisfied",
        created_at=_dt(9, 2, 0),
    )
    # unanswered: state not in answered states
    child = _child([feedback], state="missed", primary_agent=None, primary_aom=None)
    parent = _parent([child])
    row = ccsm.build_row(parent, org_uuid="org-uuid", agent_team_skills={"data": {}}, internal_dispositions=_dispos())
    assert row is not None
    assert row.csat_feedback == "Very Satisfied"
    assert row.is_conv_answered is False
    assert row.agent_ids == []  # no agent handled it


def test_build_row_indexes_all_channel_sender_ids(monkeypatch):
    """Regression: non-phone senders (chat ids, email) must be indexed for search.

    The builder previously gated conv_child_sender_ids on validate_phone_number(),
    silently dropping every non-E.164 sender (and logging an error per child). The
    row stores sender_id verbatim, so any truthy value belongs in the index.
    """
    monkeypatch.setattr(
        ccsm.AccountOrgMapping,
        "get_account_by_aom_uuid",
        lambda *a, **k: SimpleNamespace(name="Agent A", email="a@example.com"),
    )
    phone_child = _child([_chunk()], sender_id="+15551230000", channel="call")
    chat_child = _child([_chunk()], sender_id="U08ABCDEFG", channel="chat")
    email_child = _child([_chunk()], sender_id="ops@example.com", channel="email")
    none_child = _child([_chunk()], sender_id=None, channel="chat")
    parent = _parent([phone_child, chat_child, email_child, none_child])

    row = ccsm.build_row(
        parent, org_uuid="org-uuid", agent_team_skills=_agent_skills()[0], internal_dispositions=_dispos()
    )

    assert row is not None
    # phone kept (regression), non-phone now included, falsy sender_id still skipped
    assert set(row.conv_child_sender_ids) == {"+15551230000", "U08ABCDEFG", "ops@example.com"}
