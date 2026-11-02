"""The unanswered QNR child a contact's accept or close takes is the newest one, not whichever row the heap returns first."""

import uuid
from datetime import timedelta

import pytest

import tests.conftest  # noqa: F401
from hs.admin.account import AccountOrgMapping, Organization
from hs.conversations.store import Conversation, ConversationParent
from hs.infra.db import time_now


@pytest.fixture()
def ctx_db(db):
    import hs.infra.context as ctx

    token = ctx.context_db_session.set(db)
    yield db
    ctx.context_db_session.reset(token)


def test_newest_unanswered_child_wins(ctx_db):
    db = ctx_db
    org = Organization(uuid=uuid.uuid4(), name="order-org", auth_id="AID", auth_token="ATOK", is_deleted=False)
    db.add(org)
    db.flush()
    aom = AccountOrgMapping(uuid=uuid.uuid4(), organization=org.id, sip_endpoint="sip-order", is_deleted=False)
    parent = ConversationParent(
        uuid=uuid.uuid4(),
        organization=org.id,
        contact_identifier="c-1",
        channel="call",
        conversation_number=str(uuid.uuid4().int)[:12],
        direction_inbound=True,
        is_deleted=False,
        last_activity_at=time_now(),
    )
    db.add_all([aom, parent])
    db.flush()
    contact, now = str(uuid.uuid4()), time_now()
    # the older row is inserted first, so a heap-order read would return it
    for age in (1, 2):
        db.add(
            Conversation(
                uuid=uuid.uuid4(),
                parent=parent.id,
                channel="call",
                status="open",
                state="agent_unanswered",
                generation_type="qnr_generated",
                direction_inbound=True,
                contact_uuid=contact,
                created_at=now - timedelta(minutes=3 - age),
                is_deleted=False,
            )
        )
    db.flush()
    newest = Conversation.get_active_unanswered_qnr_child_by_contact_uuid(contact)
    created = db.query(Conversation.created_at).filter(Conversation.id == newest.id).scalar()
    assert created == now - timedelta(minutes=1)
