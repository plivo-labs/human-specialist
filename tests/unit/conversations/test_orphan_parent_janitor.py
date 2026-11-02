"""Orphan-parent janitor: a flow that dies without sending its closure PATCH must not
strand its parent conversation forever (non-qnr leg open, agent legs closed, flow_closure_flag
never stamped, no retry scheduled → the pair is blocked with "parent conversation already exists").

Two cooperating pieces, both in update_parent_conversation_by_conversation_number:
  * Close condition: when every open child is flow-owned (non-qnr) and the parent has been
    inactive past ORPHANED_FLOW_LEG_CLOSE_AFTER_SEC (24h), the close proceeds —
    close_parent_and_non_qnr already force-closes non-qnr legs. A live flow keeps
    refreshing last_activity_at, so an active non-qnr conversation can never be reclaimed.
  * Retry scheduling: a refusal with NO agent children open now schedules the parent-close
    retry even when is_flow_ended is False (nothing else re-triggers the evaluation — no
    agent left to resolve, dead flow never signals). Hourly cadence; the fast 60s cadence
    stays reserved for flow-ended closes on the message channel. Refusals with agent
    children still open schedule nothing — the next resolve re-triggers naturally.
"""

import datetime as dt
import types

import pytest

import tests.conftest  # noqa: F401
from hs.conversations import child_update as ccu


@pytest.fixture(autouse=True)
def _ctx():
    from hs.infra.context import context_api_id, context_req_meta

    tokens = [context_req_meta.set({"api_id": "janitor-unit"}), context_api_id.set("janitor-unit")]
    yield
    from hs.infra.context import context_api_id, context_req_meta

    for var, tok in zip((context_req_meta, context_api_id), tokens):
        var.reset(tok)


class _Parent(types.SimpleNamespace):
    def model_copy(self, update=None):
        d = dict(self.__dict__)
        d.update(update or {})
        return _Parent(**d)


def _now():
    return dt.datetime.now(dt.UTC)


def _parent(channel="message", inactive_for_sec=0):
    return _Parent(
        id=1,
        conversation_number="000-0000000000",
        parent_contact_uuid="pcu-1",
        organization=1,
        flow_closure_flag=False,
        is_hidden=False,
        end_at=None,
        parent_channel=channel,
        parent_start_at=None,
        last_activity_at=_now() - dt.timedelta(seconds=inactive_for_sec),
    )


def _bot_child():
    return types.SimpleNamespace(
        id=353261,
        generation_type="non_qnr_generated",
        primary_aom=None,
        status="open",
        state="bot_answered",
        end_at=None,
    )


def _agent_child():
    return types.SimpleNamespace(
        id=353263,
        generation_type="qnr_generated",
        primary_aom="aom-1",
        status="open",
        state="agent_answered",
        end_at=None,
    )


def _update(is_flow_ended=False):
    from hs.conversations.models import UpdateParentByConversationNumber

    return UpdateParentByConversationNumber(
        conversation_number="000-0000000000", state="closed", is_flow_ended=is_flow_ended
    )


def _run(monkeypatch, parent, children, is_flow_ended=False):
    """Drive update_parent_conversation_by_conversation_number with everything stubbed;
    returns (response, scheduled_timeouts, closed_flag)."""
    scheduled = []
    closed = {}
    from hs.infra.models import UseCaseResponse

    monkeypatch.setattr(ccu.jobs, "schedule", lambda kind, payload, delay, key: scheduled.append(delay))
    monkeypatch.setattr(
        ccu.ConversationParent,
        "get_by_conversation_number",
        staticmethod(lambda n, org_uuid=None, active=False, for_update=False: parent),
    )
    monkeypatch.setattr(ccu.ConversationParent, "update_conversation", staticmethod(lambda *a, **k: None))
    monkeypatch.setattr(ccu.Conversation, "get_all_active_conversations_for_parent", staticmethod(lambda pcu: children))
    monkeypatch.setattr(
        ccu,
        "close_parent_and_non_qnr",
        lambda p, all_convs, agent_convs, now: (
            closed.update(done=True) or UseCaseResponse(status_code=200, msg="closed")
        ),
    )
    from hs.admin.account import Organization

    monkeypatch.setattr(Organization, "get_by_id", classmethod(lambda cls, i: types.SimpleNamespace(id=i)))
    resp = ccu.update_parent_conversation_by_conversation_number(_update(is_flow_ended))
    return resp, scheduled, closed.get("done", False)


def test_stale_orphan_is_reclaimed(monkeypatch):
    parent = _parent(inactive_for_sec=ccu.ORPHANED_FLOW_LEG_CLOSE_AFTER_SEC + 60)
    resp, scheduled, closed = _run(monkeypatch, parent, [_bot_child()])
    assert closed and resp.status_code == 200
    assert scheduled == []  # closed — no further polling


def test_fresh_orphan_waits_and_schedules_hourly_poll(monkeypatch):
    parent = _parent(inactive_for_sec=ccu.ORPHANED_FLOW_LEG_CLOSE_AFTER_SEC - 3600)
    resp, scheduled, closed = _run(monkeypatch, parent, [_bot_child()])
    assert not closed and resp.status_code == 422
    assert scheduled == [3600]  # the poll that eventually reclaims it


def test_agent_children_open_schedules_nothing(monkeypatch):
    # An agent is still on the conversation: their resolve re-triggers the evaluation
    # naturally, so no timer volume is added for the normal multi-leg case.
    parent = _parent(inactive_for_sec=ccu.ORPHANED_FLOW_LEG_CLOSE_AFTER_SEC + 60)
    resp, scheduled, closed = _run(monkeypatch, parent, [_agent_child(), _bot_child()])
    assert not closed and resp.status_code == 422
    assert scheduled == []


def test_missing_activity_timestamps_never_reclaim(monkeypatch):
    parent = _parent(inactive_for_sec=0)
    parent.last_activity_at = None
    parent.parent_start_at = None
    resp, scheduled, closed = _run(monkeypatch, parent, [_bot_child()])
    assert not closed and resp.status_code == 422  # fail-safe: no timestamp → no reclaim
    assert scheduled == [3600]


def test_staleness_helper_boundary():
    now = _now()
    fresh = _Parent(
        conversation_number="x",
        parent_start_at=None,
        last_activity_at=now - dt.timedelta(seconds=ccu.ORPHANED_FLOW_LEG_CLOSE_AFTER_SEC - 1),
    )
    stale = _Parent(
        conversation_number="x",
        parent_start_at=None,
        last_activity_at=now - dt.timedelta(seconds=ccu.ORPHANED_FLOW_LEG_CLOSE_AFTER_SEC + 1),
    )
    assert ccu._orphaned_flow_legs_expired(fresh, now) is False
    assert ccu._orphaned_flow_legs_expired(stale, now) is True
    naive = _Parent(
        conversation_number="x", parent_start_at=None, last_activity_at=dt.datetime(2020, 1, 1)
    )  # tz-naive vs aware → TypeError
    assert ccu._orphaned_flow_legs_expired(naive, now) is False  # swallowed, fail-safe
