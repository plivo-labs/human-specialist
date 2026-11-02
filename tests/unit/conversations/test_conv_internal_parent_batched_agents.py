"""The parent-conversation read takes every open child's agent name and email in
one get_user_details_by_aom_uuids call (its org-scoped read is pinned in test_routing_account_read_db).

These tests FAIL if the usecase reverts to per-child get_name_by_aom_uuid lookups (patched to raise), calls the
batch more than once per request, or changes the response contract (conv_agents_email / conv_current_agent_name /
conv_participants_aom_uuids).
"""

import datetime
import uuid as uuid_lib


def test_get_user_details_by_aom_uuids_empty_input():
    import hs.admin.account as da

    assert da.AccountOrgMapping.get_user_details_by_aom_uuids([], organization_id=1) == {}


# ---------------------------------------------------------------------------
# usecase level: get_parent_conversation_internal uses the batch ONCE, never
# falls back to per-child lookups, and preserves the response contract.
# ---------------------------------------------------------------------------
def _child(primary_aom, agent_uuid, status, start_at, channel="chat"):
    return type(
        "Child",
        (),
        {
            "channel": channel,
            "contact_uuid": "child-contact",
            "direction_inbound": True,
            "status": status,
            "primary_aom": primary_aom,
            "primary_agent": type("Agent", (), {"uuid": agent_uuid})(),
            "chunks": [],
            "customer_contact": "cust",
            "sender_id": "sndr",
            "start_at": start_at,
        },
    )()


def test_get_parent_conversation_internal_batches_agent_lookups(monkeypatch):
    import hs.conversations.management as cm

    aom_a = uuid_lib.uuid4()
    aom_b = uuid_lib.uuid4()
    # B starts later -> B is the last open child -> drives conv_current_agent_name
    child_a = _child(101, aom_a, "open", datetime.datetime(2024, 1, 1, 10, 0, 0))
    child_b = _child(102, aom_b, "open", datetime.datetime(2024, 1, 1, 11, 0, 0))

    fake_cp = type(
        "CP",
        (),
        {
            "conversation_number": "1140463",
            "contact_identifier": "ci",
            "contact_uuid": "parent-contact",
            "case_id": None,
            "crm_contact_id": None,
            "created_at": datetime.datetime(2024, 1, 1, 9, 0, 0, tzinfo=datetime.UTC),
            "end_at": None,
            "contact_name": "Cust Name",
            "secondary_participants": [],
            "child_convs": [child_a, child_b],
            "organization": 1,
        },
    )()

    monkeypatch.setattr(
        cm.ConversationParent, "get_parent_conversation_preloaded", classmethod(lambda cls, cn, active=False: [fake_cp])
    )
    monkeypatch.setattr(
        cm.Conversation, "get_first_non_bot_by_contact_uuid", classmethod(lambda cls, contact_uuid: None)
    )

    batch_calls = {"n": 0, "arg": None}

    def _batch(cls, aom_uuids, organization_id=None):
        assert organization_id == 1, f"call site passed organization_id={organization_id!r}"
        batch_calls["n"] += 1
        batch_calls["arg"] = list(aom_uuids)
        return {
            str(aom_a): type("U", (), {"name": "alice", "email": "alice@example.com"})(),
            str(aom_b): type("U", (), {"name": "bob", "email": "bob@example.com"})(),
        }

    monkeypatch.setattr(cm.AccountOrgMapping, "get_user_details_by_aom_uuids", classmethod(_batch))

    # Regression guard: the per-child N+1 methods must NOT be called.
    def _boom(*a, **k):
        raise AssertionError("per-child N+1 agent lookup reintroduced")

    monkeypatch.setattr(cm.AccountOrgMapping, "get_name_by_aom_uuid", classmethod(_boom))

    resp = cm.get_parent_conversation_internal("1140463", False, False)

    assert resp.status_code == 200
    assert not resp.error
    data = resp.data
    # ONE batched call covering BOTH child AOM uuids
    assert batch_calls["n"] == 1
    assert set(batch_calls["arg"]) == {str(aom_a), str(aom_b)}
    # response contract preserved
    assert set(data["conv_agents_email"]) == {"alice@example.com", "bob@example.com"}
    assert set(data["conv_participants_aom_uuids"]) == {str(aom_a), str(aom_b)}
    # last open child (B, later start_at) drives current-agent name, title-cased
    assert data["conv_current_agent_name"] == "Bob"
