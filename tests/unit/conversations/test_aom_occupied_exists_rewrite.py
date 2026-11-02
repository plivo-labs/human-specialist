"""DB-backed regression for the is-aom-occupied hot-path rewrite.

`Conversation.get_active_conversations_for_primary_aom` was converted from a
materialise-every-row query (`.order_by(parent.last_activity_at).all()` plus a
per-row `__to_model()`/`build_res_model()` lazy-load N+1 over parent_conversation
and primary_agent) into a single EXISTS round-trip returning a bool, because its
only caller, get_aom_occupied_state in hs.conversations.management, consumes the result purely as a boolean.

These tests run against the rolled-back `_test` Postgres DB and lock the existence semantics the
rewrite must preserve:
  * the helper returns a *bool* (not a list of models),
  * True iff >=1 matching active conversation exists for the AOM as primary_aom,
  * the channel filter still narrows the match,
  * the `active` filter still excludes ended/closed conversations,
  * soft-deleted conversations and other AOMs never match,
  * conv_channel=None applies no channel filter.
Skips cleanly when the `_test` Postgres DB is unreachable (the `db` fixture skips).
"""

import uuid
from datetime import timedelta

import pytest

import tests.conftest  # noqa: F401 -- eager data_adapter imports / registry finalize
from hs.infra.db import time_now


@pytest.fixture()
def ctx_db(db):
    """Bind the rolled-back test session into the context var the adapter reads
    via hs.infra.context.get_db_session."""
    import hs.infra.context as ctx

    token = ctx.context_db_session.set(db)
    yield db
    ctx.context_db_session.reset(token)


def _seed_org(db):
    from hs.admin.account import Organization

    org = Organization(uuid=uuid.uuid4(), name="aomocc-org", auth_id="AID", auth_token="ATOK", is_deleted=False)
    db.add(org)
    db.flush()
    return org.id


def _seed_aom(db, org_id):
    from hs.admin.account import AccountOrgMapping

    aom_uuid = uuid.uuid4()
    aom = AccountOrgMapping(uuid=aom_uuid, organization=org_id, sip_endpoint="sip-agent-x", is_deleted=False)
    db.add(aom)
    db.flush()
    return aom.id, aom_uuid


def _seed_parent(db, org_id, *, channel="call"):
    from hs.conversations.store import ConversationParent

    p = ConversationParent(
        uuid=uuid.uuid4(),
        organization=org_id,
        contact_identifier="c-1",
        channel=channel,
        conversation_number=str(uuid.uuid4().int)[:12],
        direction_inbound=True,
        is_deleted=False,
        last_activity_at=time_now(),
    )
    db.add(p)
    db.flush()
    return p.id


def _seed_conv(db, parent_id, aom_id, *, channel="call", status="open", end_at=None, is_deleted=False):
    from hs.conversations.store import Conversation

    c = Conversation(
        uuid=uuid.uuid4(),
        parent=parent_id,
        primary_aom=aom_id,
        channel=channel,
        status=status,
        state="agent_unanswered",
        generation_type="qnr_generated",
        direction_inbound=True,
        end_at=end_at,
        is_deleted=is_deleted,
    )
    db.add(c)
    db.flush()
    return c


def test_returns_bool_true_for_active_primary_aom_conv(ctx_db):
    from hs.conversations.models import ConvChannel
    from hs.conversations.store import Conversation

    db = ctx_db
    org_id = _seed_org(db)
    aom_id, aom_uuid = _seed_aom(db, org_id)
    parent_id = _seed_parent(db, org_id, channel="call")
    _seed_conv(db, parent_id, aom_id, channel="call", status="open")

    result = Conversation.get_active_conversations_for_primary_aom(aom_uuid, ConvChannel(channel="call"), True)
    # Must be a plain boolean -- reverting to `.all()` would return a list and fail `is True`.
    assert result is True


def test_channel_filter_excludes_other_channels(ctx_db):
    from hs.conversations.models import ConvChannel
    from hs.conversations.store import Conversation

    db = ctx_db
    org_id = _seed_org(db)
    aom_id, aom_uuid = _seed_aom(db, org_id)
    parent_id = _seed_parent(db, org_id, channel="call")
    _seed_conv(db, parent_id, aom_id, channel="call", status="open")

    # No active *chat* conversation for this AOM -> False.
    assert Conversation.get_active_conversations_for_primary_aom(aom_uuid, ConvChannel(channel="chat"), True) is False


def test_inactive_deleted_and_other_aom_excluded(ctx_db):
    from hs.conversations.models import ConvChannel
    from hs.conversations.store import Conversation

    db = ctx_db
    org_id = _seed_org(db)
    aom_id, aom_uuid = _seed_aom(db, org_id)
    other_aom_id, other_aom_uuid = _seed_aom(db, org_id)
    parent_id = _seed_parent(db, org_id, channel="call")

    # Closed + ended (not active) for our AOM.
    _seed_conv(db, parent_id, aom_id, channel="call", status="closed", end_at=time_now() - timedelta(days=1))
    # Soft-deleted but otherwise-active conv for our AOM.
    _seed_conv(db, parent_id, aom_id, channel="call", status="open", is_deleted=True)
    # Active conv but for a DIFFERENT AOM.
    _seed_conv(db, parent_id, other_aom_id, channel="call", status="open")

    # Our AOM has no live, non-deleted, active call conv -> False.
    assert Conversation.get_active_conversations_for_primary_aom(aom_uuid, ConvChannel(channel="call"), True) is False
    # Sanity: the other AOM IS occupied -> True.
    assert (
        Conversation.get_active_conversations_for_primary_aom(other_aom_uuid, ConvChannel(channel="call"), True) is True
    )


def test_none_channel_applies_no_channel_filter(ctx_db):
    from hs.conversations.store import Conversation

    db = ctx_db
    org_id = _seed_org(db)
    aom_id, aom_uuid = _seed_aom(db, org_id)
    parent_id = _seed_parent(db, org_id, channel="chat")
    _seed_conv(db, parent_id, aom_id, channel="chat", status="open")

    # conv_channel=None -> no channel filter -> matches the chat conv.
    assert Conversation.get_active_conversations_for_primary_aom(aom_uuid, None, True) is True
