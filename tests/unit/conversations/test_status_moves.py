"""The status table a child may move through, and what entering open does."""

from types import SimpleNamespace
from unittest.mock import patch

import pytest

import tests.conftest  # noqa: F401
from hs.conversations.management import PostConversationStatusChange, check_status_move


@pytest.mark.parametrize(
    "now,to,expected",
    [
        ("open", "closed", (True, "")),
        ("open", "in_wrap_up", (True, "")),
        ("in_wrap_up", "closed", (True, "")),
        ("in_wrap_up", "open", (False, "closed")),
        ("session_resolved", "open", (False, "in_wrap_up")),
        ("closed", "closed", (False, None)),
    ],
)
def test_check_status_move(now, to, expected):
    assert check_status_move(SimpleNamespace(status=now), to) == expected


def _post(channel):
    conv = SimpleNamespace(channel=channel, parent_conversation=SimpleNamespace(conversation_number="CN1"))
    return PostConversationStatusChange(
        aom_uuid="aom-1", org_name="org", org_uuid="org-1", conversation=conv, changed_status="open", aom=object()
    )


def test_open_commits_then_arms_the_session_timeout():
    order = []
    with (
        patch("hs.conversations.management.get_db_session") as gds,
        patch("hs.conversations.timers.schedule_session_timeout") as sched,
    ):
        gds.return_value.commit.side_effect = lambda: order.append("commit")
        sched.side_effect = lambda *a, **kw: order.append("schedule")
        _post("call").invoke()
    assert order == ["commit", "schedule"]
    assert sched.call_args.args == ("CN1", "call", "aom-1", "org-1", "org")


def test_open_arms_no_timeout_for_a_channel_without_one():
    with (
        patch("hs.conversations.management.get_db_session"),
        patch("hs.conversations.timers.schedule_session_timeout") as sched,
    ):
        _post("fax").invoke()
    assert not sched.called
