from types import SimpleNamespace

import pytest

from hs.conversations import child_update as ccu
from hs.conversations import management as cm


@pytest.mark.parametrize(
    ("others", "ends_flow"), [([], True), ([SimpleNamespace(id=2, generation_type="transfer_qnr_generated")], False)]
)
def test_the_call_ends_with_its_last_agent_child_unless_a_transfer_destination_waits(monkeypatch, others, ends_flow):
    mine = SimpleNamespace(id=1, parent_conversation=SimpleNamespace(parent_contact_uuid="c", organization=1))
    monkeypatch.setattr(cm.Conversation, "get_active_by_conv_number_for_aom", classmethod(lambda cls, *a: []))
    monkeypatch.setattr(cm.AgentConversationSetting, "set_or_get_setting", staticmethod(lambda *a: False))
    monkeypatch.setattr(
        cm.Conversation, "get_all_active_conversations_for_parent", classmethod(lambda cls, c: [mine, *others])
    )
    closed = []
    monkeypatch.setattr(ccu, "close_conversation", lambda conv, **kw: closed.append(kw))
    cm._close_wrapped_up(mine, "n")
    assert closed == [{"resolve_for_agent": True, "flow_ended": ends_flow}]
