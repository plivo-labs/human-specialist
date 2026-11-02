"""A supervisor's actions on a call touch only their supervisor row (rolled-back test database)."""

import uuid
from types import SimpleNamespace

import tests.conftest  # noqa: F401 -- eager data_adapter imports / registry finalize
from tests.unit.conversations.test_aom_occupied_exists_rewrite import (  # noqa: F401 -- ctx_db is a fixture
    _seed_aom,
    _seed_org,
    _seed_parent,
    ctx_db,
)


def test_supervisor_actions_leave_a_transfer_the_same_agent_took_on_that_call(ctx_db, monkeypatch):  # noqa: F811
    from hs.conversations import models as mc
    from hs.conversations import transfer as cxfer
    from hs.conversations.store import ConversationParent, ConvSecondaryAgentMapping

    org = _seed_org(ctx_db)
    aom_id, aom_uuid = _seed_aom(ctx_db, org)
    parent_id = _seed_parent(ctx_db, org)

    def seat(state):
        row = ConvSecondaryAgentMapping(uuid=uuid.uuid4(), aom=aom_id, conversation=parent_id, is_deleted=False)
        ctx_db.add(row)
        row.secondary_agent_state = state
        ctx_db.flush()

    def states():
        rows = ConvSecondaryAgentMapping.select_query(ctx_db).filter_by(conversation=parent_id).all()
        return sorted(r.secondary_agent_state for r in rows)

    seat("monitor")
    seat("transferred")
    parent = ctx_db.query(ConversationParent).filter_by(id=parent_id).one().get_model()
    agent = SimpleNamespace(id=aom_id, uuid=aom_uuid)
    supervisor = {"conversation_number": parent.conversation_number, "agent_sip_or_uuid": "sip-agent-x"}
    supervisor |= {"type": mc.SecAgentTypes.supervisor_participant, "parent_conversation": parent}
    child = SimpleNamespace(parent_conversation=parent)
    monkeypatch.setattr(cxfer.Conversation, "get_primary_active_conversation", classmethod(lambda cls, **kw: child))
    monkeypatch.setattr(cxfer, "create_chunks_on_secondary_mapping_update", lambda *a: None)

    switch = mc.ConvSecAgentMappingInsert(**supervisor, secondary_agent_state="coach")
    switch.secondary_agent = agent
    assert cxfer.update_secondary_participant(switch).status_code == 200
    assert states() == ["coach", "transferred"]

    cxfer._evict_barging_supervisor(parent, SimpleNamespace(uuid=uuid.uuid4()))
    assert states() == ["transferred"]

    seat("monitor")
    removal = mc.ConvSecAgentMappingRemove(**supervisor)
    removal.secondary_agent = agent
    assert cxfer.remove_secondary_participant(removal, bypass_all_events=True).status_code == 200
    assert states() == ["transferred"]
