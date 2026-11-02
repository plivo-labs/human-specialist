"""A secondary participation that is a barge or not a supervisor listen (transfer target, added participant)
occupies the agent; a monitor- or coach-only supervisor stays free; a barge always wins the reported state."""

from types import SimpleNamespace

import pytest

from hs.conversations import management as cm
from hs.conversations.models import ConvChannel


@pytest.fixture
def occupancy(monkeypatch):
    t_org = cm.context_org_uuid.set("00000000-0000-4000-a000-000000000000")
    t_aom = cm.context_aom_uuid.set("11111111-1111-4111-a111-111111111111")
    monkeypatch.setattr(cm.Organization, "get_by_uuid", lambda *a, **k: SimpleNamespace(id=7))
    monkeypatch.setattr(cm.RoutingSettings, "get_by_org_id", lambda *a, **k: None)
    monkeypatch.setattr(cm.Conversation, "get_active_conversations_for_primary_aom", lambda *a, **k: None)

    def read(states):
        lookup = cm.ConversationParent
        monkeypatch.setattr(lookup, "get_active_secondary_conversation_mappings_with_state", lambda *a, **k: states)
        data = cm.get_aom_occupied_state(ConvChannel(channel="call")).data
        return data["is_aom_occupied"], data["secondary_agent_state"]

    yield read
    cm.context_org_uuid.reset(t_org)
    cm.context_aom_uuid.reset(t_aom)


@pytest.mark.parametrize("state", ["monitor", "monitor_visible", "monitor_paused", "coach"])
def test_supervisor_only_listening_is_not_occupied(occupancy, state):
    assert occupancy({"CN1": state}) == (False, state)


@pytest.mark.parametrize("state", ["barge", "transferred", "add_participant_success"])
def test_barge_or_participant_is_occupied(occupancy, state):
    assert occupancy({"CN1": state}) == (True, state)


def test_barge_wins_whatever_the_order(occupancy):
    assert occupancy({"CN1": "barge", "CN2": "transferred"}) == (True, "barge")
    assert occupancy({"CN1": "transferred", "CN2": "barge"}) == (True, "barge")
    assert occupancy({"CN1": "coach", "CN2": "transferred"}) == (True, "transferred")
