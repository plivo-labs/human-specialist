"""Text on the migrated database: a Plivo callback's work commits in its nonce's transaction, a message status
only moves forward, a desk that reloads still finds its open text offers, racing transfers never overfill an
agent, and a reply's switch neither waits on another family's conversation nor sends in one its agent lost."""

import base64
import hashlib
import threading
import time
import uuid
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import insert, select, text, update

from hs import access, core, plivo
from hs import app as service
from hs import conversations as cv
from hs import text as messages
from hs.access import Principal
from hs.config import models as m
from hs.core import ApiError, T
from hs.hot import rebuilt
from tests.unit.test_agents import Desk
from tests.unit.test_routing import Floor, while_held

TOKEN = "text-auth-token-0123456789"
client = TestClient(service.app)


def outbound() -> tuple[uuid.UUID, uuid.UUID]:
    """A tenant and one pending agent SMS of it."""
    with core.tx() as db:
        tenant = insert(T.tenant).values(name="t", plivo_auth_id=uuid.uuid4().hex, plivo_token_enc=plivo.encrypt(TOKEN))
        tenant = db.execute(tenant.returning(T.tenant.c.id)).scalar()
    rebuilt(tenant)
    with core.tx() as db:
        conv = cv.open(db, tenant, channel="sms", direction="outbound", customer="+14155550101", ours="+14155550102")
        body = {"direction": "outbound", "channel": "sms", "text": "Hi", "attachments": []}
        return tenant, cv.entry(db, conv, "message", body, status="pending").id


def status_of(eid: uuid.UUID) -> str:
    with core.read() as db:
        return db.execute(select(T.entry.c.status).where(T.entry.c.id == eid)).scalar()


def test_a_delivery_report_commits_in_the_transaction_that_records_its_nonce():
    tenant, eid = outbound()
    path, nonce = f"/plivo/{tenant}/message-status/{eid}", uuid.uuid4().hex
    signed = {"X-Plivo-Signature-V2": plivo.v2(TOKEN, f"{core.settings.public_base_url}{path}", nonce)}
    report = {"Status": "delivered", "MessageUUID": str(uuid.uuid4())}
    assert client.post(path, data=report, headers=signed | {"X-Plivo-Signature-V2-Nonce": nonce}).status_code == 200
    assert status_of(eid) == "delivered"
    with core.read() as db:
        work = db.execute(text("SELECT xmin::text FROM entry WHERE id = :e"), {"e": eid}).scalar()
        seen = text("SELECT xmin::text FROM idempotency WHERE principal = 'plivo-v2' AND key_sha256 = :k")
        assert db.execute(seen, {"k": hashlib.sha256(nonce.encode()).hexdigest()}).scalar() == work


def test_a_status_moves_only_forward_and_fails_only_before_delivery():
    runs = {outbound(): ("delivered", "sent", "failed"), outbound(): ("failed", "delivered")}
    runs |= {outbound(): ("delivered", "undelivered")}
    for (tenant, eid), steps in runs.items():
        for to in steps:
            with core.tx() as db:
                messages.status(db, tenant, eid, to)
    assert [status_of(eid) for _, eid in runs] == ["delivered", "failed", "delivered"]
    E, tenants = T.events, [str(t) for t, _ in runs]
    told = select(E.c.data["status"].astext).where(E.c.type == "message.status_changed", E.c.tenant_id.in_(tenants))
    with core.read() as db:
        assert sorted(db.execute(told).scalars()) == ["delivered", "delivered", "failed"]


def test_a_status_time_out_of_the_window_reads_as_the_time_hs_heard_it():
    early, stale = datetime.now(UTC) - timedelta(minutes=2), datetime(2020, 1, 1, tzinfo=UTC)
    runs = {outbound(): early, outbound(): stale}
    for (tenant, eid), reported in runs.items():
        with core.tx() as db:
            messages.status(db, tenant, eid, "sent", reported=reported)
    E = T.events
    with core.read() as db:
        told = [
            select(E.c.data["reported_at"].astext).where(E.c.tenant_id == str(t), E.c.type.like("message.%"))
            for t, _ in runs
        ]
        kept, heard = (datetime.fromisoformat(db.execute(q).scalar()) for q in told)
    assert kept == early and heard > datetime.now(UTC) - timedelta(minutes=1)


def test_me_lists_the_agents_open_text_offers_but_not_its_calls():
    desk, doc = Desk(), T.config
    with core.tx() as db:
        whole = doc.c.body.concat({"external_id": None, "skills": [], "caller_id": None})
        db.execute(update(doc).where(doc.c.id == desk.agent).values(body=whole))
        parties = {"direction": "inbound", "customer": "+14155550103", "ours": "+14155550104"}
        sms = cv.offer(db, cv.open(db, desk.tenant, channel="sms", **parties), desk.agent)
        cv.offer(db, cv.open(db, desk.tenant, channel="call", call_uuid=uuid.uuid4().hex, **parties), desk.agent)
    me = client.get("/api/v1/me", headers={"Authorization": f"Bearer {desk.tokens.agent_token}"}).json()
    assert [(o["offer_id"], o["channel"]) for o in me["pending_offers"]] == [(str(sms.id), "sms")]


def test_two_transfers_racing_for_an_agents_last_slot_offer_it_once(monkeypatch):
    floor = Floor(monkeypatch)
    target, holders = floor.agent(), [floor.agent(), floor.agent()]
    first, second = [floor.open("sms") for _ in holders]
    with core.tx() as db:
        for conv, agent in zip((first, second), holders, strict=True):
            cv.offer(db, conv, agent, via="direct")

    def hold(db):
        conv = cv.lock(db, first.id)
        for agent in messages._takers(db, conv, [target], direct=True):
            cv.offer(db, conv, agent, via="transfer", mode="cold")

    def then():
        to = messages.TextTransferIn(target={"type": "agent", "agent_id": str(target)})
        with pytest.raises(ApiError) as refused:
            messages.transfer(second.id, to, Principal(str(floor.tenant), "key"))
        return refused.value.code

    assert while_held(hold, then, waits=False) == "agent_at_capacity", "the first transfer's offer took the slot"
    assert (floor.offers(first.id), floor.offers(second.id)) == ([target], [])


def replied(floor: Floor, agent, cid, **body) -> str:
    """The agent's reply's error code, or "sent"."""
    p = Principal(str(floor.tenant), "agent", agent_id=str(agent), role="agent")
    try:
        messages.reply(cid, messages.Reply.model_validate(body | {"text": "As promised"}), p)
    except ApiError as e:
        return e.code
    return "sent"


def test_a_reply_into_an_unrelated_sms_another_reply_holds_is_refused_without_waiting(monkeypatch):
    floor = Floor(monkeypatch)
    floor.settings(capacity=m.Capacity(sms=2).model_dump())
    agent = floor.agent()
    mine, theirs = floor.open("sms", "+14155550931"), floor.open("sms", "+14155550932")
    with core.tx() as db:
        floor.doc(db, "number", {"number": mine.ours}, mine.ours)
        for conv in (mine, theirs):
            cv.offer(db, conv, agent, via="direct")
    crossing = lambda: replied(floor, agent, mine.id, to=theirs.customer)  # noqa: E731
    assert while_held(lambda db: cv.lock(db, theirs.id), crossing, waits=False) == "conversation_open"


def test_a_reply_into_a_linked_sms_transferred_meanwhile_is_refused_not_sent_as_its_old_agent(monkeypatch):
    floor = Floor(monkeypatch)
    external = m.TextSettings(delivery=m.TextDelivery(sms="external")).model_dump(mode="json")
    floor.settings(capacity=m.Capacity(sms=2).model_dump(), text=external)
    old, new = floor.agent(), floor.agent()
    root, linked = floor.open("sms", "+14155550931"), floor.open("sms", "+14155550932")
    with core.tx() as db:
        floor.doc(db, "number", {"number": root.ours}, root.ours)
        cv.offer(db, linked, old, via="direct", came=cv.offer(db, root, old, via="direct").id)
        cold = cv.offer(db, linked, new, via="transfer", mode="cold")
    moved = lambda db: cv.promote(db, cv.lock(db, linked.id), cold.id)  # noqa: E731
    assert while_held(moved, lambda: replied(floor, old, root.id, to=linked.customer)) == "conversation_open"


def test_a_second_delivery_of_a_message_id_waits_for_the_first_and_finds_it_though_its_conversation_closed(monkeypatch):
    floor = Floor(monkeypatch)
    conv = floor.open("sms")
    msg = {"channel": "sms", "from": conv.customer, "to": conv.ours, "text": "Hi", "provider_id": "dup-1"}
    first = {}

    def hold(db):
        assert messages.seen(db, floor.tenant, conv.ours, "dup-1") is None
        first["id"] = messages.received(db, messages.find(db, floor.tenant, msg), msg).id
        messages.ended(db, cv.lock(db, conv.id), "session_timeout")

    def then():
        with core.tx() as db:
            return messages.seen(db, floor.tenant, conv.ours, "dup-1")

    assert while_held(hold, then).id == first["id"]


def test_a_message_id_a_recording_of_the_line_holds_is_no_repeat(monkeypatch):
    floor = Floor(monkeypatch)
    call = floor.open("call")
    with core.tx() as db:
        cv.entry(db, call, "recording", {"url": None}, provider_id="rec-1")
        assert messages.seen(db, floor.tenant, call.ours, "rec-1") is None


def test_a_customer_file_reaches_its_reader_as_plivo_sends_it(monkeypatch):
    tenant, _ = outbound()
    files = [{"url": f"{core.settings.plivo_media_url}/v1/Account/a/Message/m/Media/0", "hosted": True}]
    with core.tx() as db:
        conv = db.execute(select(T.conversation).where(T.conversation.c.tenant_id == tenant)).one()
        body = {"direction": "inbound", "channel": "sms", "text": None, "attachments": files}
        eid = cv.entry(db, conv, "message", body, status="received").id
    read, waited = threading.Event(), []

    def file():
        yield b"first part"
        waited.append(read.wait(5))
        yield b"last part"

    upstream = httpx.MockTransport(lambda r: httpx.Response(200, content=file()))
    monkeypatch.setattr(plivo, "HTTP", httpx.Client(transport=upstream))
    exp = int(time.time()) + 60
    with client.stream("GET", f"/api/v1/messages/{eid}/media/0?exp={exp}&sig={messages.signature(eid, '0', exp)}") as r:
        chunks = r.iter_bytes()
        first = next(chunks)
        read.set()
        rest = b"".join(chunks)
    assert (first + rest, waited) == (b"first partlast part", [True])


def _written(floor, channel, kind, body, n=30):
    cid = floor.open(channel).id
    with core.tx() as db:
        conv = cv.lock(db, cid)
        ids = [str(cv.entry(db, conv, kind, body, at=datetime.now(UTC)).id) for _ in range(n)]
    return cid, ids


def test_messages_written_in_one_transaction_read_back_in_the_order_they_were_written(monkeypatch):
    floor = Floor(monkeypatch)
    cid, ids = _written(floor, "sms", "message", {"direction": "inbound", "text": "Hi", "attachments": []})
    key = access.Principal(tenant_id=str(floor.tenant), kind="key", scopes=frozenset({"calls"}))
    first = messages.list_messages(cid, key, core.Paging(10, None))
    after = base64.urlsafe_b64decode(first["next_cursor"]).decode()
    rest = messages.list_messages(cid, key, core.Paging(100, after))
    assert [m["id"] for m in first["data"] + rest["data"]] == ids


def test_notes_and_media_written_in_one_transaction_read_back_in_the_order_they_were_written(monkeypatch):
    floor = Floor(monkeypatch)
    cid, notes = _written(floor, "chat", "note", {"text": "n"})
    with core.tx() as db:
        conv = cv.lock(db, cid)
        media = [str(cv.entry(db, conv, "recording", {"url": None}).id) for _ in range(30)]
    with core.read() as db:
        shown = cv.view(db, floor.conv(cid))
    assert ([n["id"] for n in shown["notes"]], shown["recording_ids"]) == (notes, media)


def _upstream(monkeypatch, chunks: int, served: list, headers=None) -> str:
    tenant, _ = outbound()
    files = [{"url": f"{core.settings.plivo_media_url}/v1/Account/a/Message/m/Media/0", "hosted": True}]
    with core.tx() as db:
        conv = db.execute(select(T.conversation).where(T.conversation.c.tenant_id == tenant)).one()
        body = {"direction": "inbound", "channel": "sms", "text": None, "attachments": files}
        eid = cv.entry(db, conv, "message", body, status="received").id

    def file():
        for _ in range(chunks):
            served.append(1)
            yield b"x" * 2**20

    upstream = httpx.MockTransport(lambda r: httpx.Response(200, headers=headers, content=file()))
    monkeypatch.setattr(plivo, "HTTP", httpx.Client(transport=upstream))
    exp = int(time.time()) + 60
    return f"/api/v1/messages/{eid}/media/0?exp={exp}&sig={messages.signature(eid, '0', exp)}"


def test_a_file_declared_over_the_media_ceiling_is_refused_before_any_byte(monkeypatch):
    pulled: list = []
    url = _upstream(monkeypatch, 130, pulled, {"content-length": str(101 * 2**20)})
    r = client.get(url)
    assert (r.status_code, r.json()["error"]["code"], pulled) == (413, "media_too_large", [])


def test_a_file_of_no_declared_length_that_passes_the_ceiling_fails_its_read(monkeypatch):
    pulled: list = []
    url = _upstream(monkeypatch, 130, pulled)
    got = 0
    with pytest.raises(ValueError), client.stream("GET", url) as r:
        for chunk in r.iter_bytes():
            got += len(chunk)
    assert got <= 100 * 2**20
    assert len(pulled) < 130


def test_a_file_under_the_media_ceiling_streams_whole(monkeypatch):
    url = _upstream(monkeypatch, 3, [])
    assert len(client.get(url).content) == 3 * 2**20
