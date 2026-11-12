"""The voice flow against the migrated database: call.ended once per call, whoever said the caller was gone first,
and a key's hand-over only into its own tenant's queues."""

import uuid
from types import SimpleNamespace

import pytest
from sqlalchemy import func, insert, select

from hs import conversations as cv
from hs import core, plivo
from hs.access import Principal
from hs.core import ApiError, T
from hs.hot import rebuilt
from hs.voice import flow


def count(cid, type_: str) -> int:
    E = T.events
    with core.read() as db:
        return db.execute(select(func.count()).where(E.c.conversation_id == str(cid), E.c.type == type_)).scalar()


def queued_call(*, gone: bool = False):
    """A new tenant's call waiting in a queue (or gone already): (tenant id, conversation)."""
    call_uuid = uuid.uuid4().hex
    with core.tx() as db:
        settings = {"rona": {"status_id": str(uuid.uuid4())}}
        tenant = insert(T.tenant).values(name="t", plivo_auth_id=call_uuid, plivo_token_enc="sealed", settings=settings)
        tenant_id = db.execute(tenant.returning(T.tenant.c.id)).scalar()
    rebuilt(tenant_id)
    with core.tx() as db:
        parties = {"direction": "inbound", "customer": "+15550001", "ours": "+15550002", "number": "+15550002"}
        conv = cv.set_state(db, cv.open(db, tenant_id, channel="call", call_uuid=call_uuid, **parties), "queued")
        if gone:
            cv.gone(db, conv)
    return tenant_id, conv


@pytest.mark.parametrize("gone_first", [True, False])
def test_the_hangup_emits_one_call_ended_and_the_caller_goes_once(gone_first):
    tenant_id, conv = queued_call(gone=gone_first)
    form = {"CallUUID": conv.call_uuid, "HangupCauseName": "Normal Hangup", "HangupCauseCode": "4000", "Duration": "7"}
    for _ in range(2):
        assert flow.hangup(SimpleNamespace(id=tenant_id), form).status_code == 200
    assert (count(conv.id, "call.ended"), count(conv.id, "conversation.closed")) == (1, 1)


def test_a_key_cannot_hand_its_call_to_another_tenants_queue():
    tenant_id, conv = queued_call()
    other, _ = queued_call()
    with core.tx() as db:
        theirs = insert(T.config).values(tenant_id=other, kind="queue", body={"name": "Theirs"})
        queue_id = db.execute(theirs.returning(T.config.c.id)).scalar()
    body = flow.EnqueueIn.model_validate({"call_uuid": conv.call_uuid, "from": conv.customer, "to": conv.ours})
    with pytest.raises(ApiError) as refused:
        flow._hand_over(Principal(str(tenant_id), "key"), queue_id, body)
    assert (refused.value.status, refused.value.code) == (404, "queue_not_found")


def test_a_hand_over_refused_where_the_job_pool_ran_it_is_409_and_the_caller_leaves_the_queue(pool_first, monkeypatch):
    tenant_id, conv = queued_call()
    with core.tx() as db:
        queue = insert(T.config).values(tenant_id=tenant_id, kind="queue", body={"name": "Q"})
        queue_id = db.execute(queue.returning(T.config.c.id)).scalar()

    def ended(tenant, method, path, **params):
        raise plivo.Refused(422, "plivo_refused", "Plivo refused the request", {"status": 404})

    monkeypatch.setattr(plivo, "rest", ended)
    body = flow.EnqueueIn.model_validate({"call_uuid": conv.call_uuid, "from": conv.customer, "to": conv.ours})
    with pytest.raises(ApiError) as refused:
        flow._hand_over(Principal(str(tenant_id), "key"), queue_id, body)
    with core.read() as db:
        state = db.execute(select(T.conversation.c.state).where(T.conversation.c.id == conv.id)).scalar()
    assert (refused.value.status, refused.value.code, state) == (409, "call_not_live", "closed")


@pytest.mark.parametrize(
    ("sent", "stored"),
    [
        ("anonymous", "Anonymous"),
        ("restricted", "Anonymous"),
        ("1000000000", "Anonymous"),
        ("+anonymous", "+anonymous"),
        ("+restricted", "+restricted"),
        ("+1000000000", "+1000000000"),
    ],
)
def test_only_the_bare_withheld_words_are_stored_as_anonymous(sent, stored):
    from tests.unit.tenants import Tenant

    t = Tenant()
    form = {"From": sent, "To": "14155550100", "CallUUID": uuid.uuid4().hex}
    with core.tx() as db:
        conv, _ = flow._call(db, t.id, form)
    assert conv.customer == stored


def test_a_hangup_whose_session_start_does_not_parse_still_records_the_end():
    facts = flow.hangup_facts(
        {"Duration": "12", "SessionStart": "yesterday", "HangupCauseName": "NORMAL_CLEARING"}, "customer"
    )
    assert facts["ended_at"] is None and facts["duration_sec"] == 12
    good = flow.hangup_facts({"Duration": "81", "SessionStart": "2026-11-10 09:04:26.545000"}, "customer")
    assert good["ended_at"] == "2026-11-10T09:05:47.545000+00:00"
