"""hs.events and hs.jobs: the relay, scoped reads, job leasing and backoff, the SSRF guard, signing, SSE frames."""

import asyncio
import dataclasses
import random
import socket
import threading
import time
from uuid import uuid4

import pytest
from pydantic import ValidationError
from sqlalchemy import text

from hs.events import emit, read, relay_once, webhooks
from hs.events.sse import frames
from hs.events.webhooks import RETRIES, destination, pinned, post, signature
from hs.jobs import DEFAULT_BACKOFF, cancel, claim, handler, schedule, work

STATUS = {"from_status": None, "to_status": {"id": "s1", "name": "Away"}, "reason": "admin"}
ran: list[dict] = []


@handler("test_ok")
def _ok(payload: dict) -> None:
    ran.append(payload)


@handler("test_fail", backoff=RETRIES)
def _fail(payload: dict) -> None:
    raise RuntimeError("receiver down")


@pytest.fixture
def db(db_engine):
    return db_engine


@pytest.fixture
def jobs(db):
    with db.begin() as conn:
        conn.execute(text("DELETE FROM jobs"))
    return db


def conversation(conn, inbound: bool, context: str) -> tuple[int, str]:
    """A conversation row and its context; returns (a free id, the conversation id)."""
    n, cid = random.randrange(10**9, 2**31 - 10), str(uuid4())
    conn.execute(
        text(
            "INSERT INTO conversation_parent (id, uuid, created_at, updated_at, channel, direction_inbound)"
            " VALUES (:n, :c, now(), now(), 'voice', :inbound)"
        ),
        {"n": n, "c": cid, "inbound": inbound},
    )
    conn.execute(
        text(
            "INSERT INTO customer_conversation_data (id, uuid, created_at, updated_at, conversation_parent_id,"
            " conversation_custom_data) VALUES (:n, gen_random_uuid(), now(), now(), :n, :ctx)"
        ),
        {"n": n, "ctx": context},
    )
    return n, cid


def test_relay_stamps_seq_in_id_order_and_queues_one_delivery_per_matching_hook(db):
    tenant = str(uuid4())
    with db.begin() as conn:
        n, cid = conversation(conn, False, '{"lead": "L-1"}')
        for i in (n + 3, n + 2, n + 1):  # stored against id order
            kind, on = ("call.ended", cid) if i == n + 2 else ("agent.status_changed", None)
            conn.execute(
                text("INSERT INTO events (id, tenant_id, type, conversation_id, data) VALUES (:i, :t, :k, :c, '{}')"),
                {"i": i, "t": tenant, "k": kind, "c": on},
            )
        hooks = {}
        for name, types, enabled in (("all", "{}", True), ("ended", "{call.ended}", True), ("off", "{}", False)):
            hooks[name] = conn.execute(
                text(
                    "INSERT INTO webhooks (tenant_id, url, secret, event_types, enabled) VALUES (:t, 'u', 's', :e, :on) RETURNING id::text"
                ),
                {"t": tenant, "e": types, "on": enabled},
            ).scalar()
    relay_once()
    with db.connect() as conn:
        rows = conn.execute(
            text("SELECT seq, direction, context FROM events WHERE tenant_id = :t ORDER BY id"), {"t": tenant}
        ).all()
        sent = conn.execute(
            text("SELECT payload FROM jobs WHERE payload->>'webhook_id' = ANY(:h)"), {"h": list(hooks.values())}
        ).scalars()
        deliveries = sorted((p["event_id"], p["webhook_id"]) for p in sent)
    assert [r.seq for r in rows] == sorted(r.seq for r in rows)
    assert (rows[1].direction, rows[1].context, rows[0].direction) == ("outbound", {"lead": "L-1"}, None)
    expected = [(n + 1, hooks["all"]), (n + 2, hooks["all"]), (n + 2, hooks["ended"]), (n + 3, hooks["all"])]
    assert deliveries == sorted(expected)


def test_a_context_jsonb_cannot_hold_is_dropped_and_the_batch_still_stamps(db):
    tenant = str(uuid4())
    with db.begin() as conn:
        _, cid = conversation(conn, True, r'{"note": "a\u0000b"}')
    emit("call.held", tenant_id=tenant, conversation_id=cid, data={"by_agent_id": None})
    emit("agent.status_changed", tenant_id=tenant, data=STATUS)
    relay_once()
    stamped = [(e["type"], e["direction"], e["context"]) for e in read(tenant, 0, 50)]
    assert stamped == [("call.held", "inbound", None), ("agent.status_changed", None, None)]


def test_emit_refuses_data_off_the_catalog(db):
    with pytest.raises(ValidationError):
        emit("agent.status_changed", tenant_id=str(uuid4()), data={**STATUS, "reason": "bored"})


def test_reads_page_after_the_cursor_and_keep_an_agent_to_its_own_events(db):
    tenant, me, other = str(uuid4()), str(uuid4()), str(uuid4())
    for agent in (me, other, me):
        emit("agent.status_changed", tenant_id=tenant, agent_id=agent, data=STATUS)
    emit("agent.status_changed", tenant_id=str(uuid4()), agent_id=me, data=STATUS)
    relay_once()
    everything = read(tenant, 0, 50)
    assert [e["agent_id"] for e in everything] == [me, other, me]
    assert read(tenant, 0, 50, scope=([me], None)) == [everything[0], everything[2]]
    assert read(tenant, 0, 50, head=int(everything[1]["id"])) == everything[:2]
    assert read(tenant, int(everything[0]["id"]), 1) == [everything[1]]
    assert read(tenant, 0, 50, type="call.ended") == []


def test_a_leased_job_is_claimed_once_and_only_its_lease_completes_it(jobs):
    schedule("test_ok", {"n": 1})
    with jobs.connect() as a, jobs.connect() as b:
        held = a.begin()
        first = claim(a)
        with b.begin():
            assert first and claim(b) is None  # skipped, not waited on (lock_timeout would raise)
        held.commit()
    with jobs.begin() as conn:
        assert claim(conn) is None
        conn.execute(text("UPDATE jobs SET locked_until = now() - interval '1 second'"))
    with jobs.begin() as conn:
        second = claim(conn)
    work(first)
    with jobs.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM jobs")).scalar() == 1
    work(second)
    with jobs.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM jobs")).scalar() == 0
    assert ran[-2:] == [{"n": 1}, {"n": 1}]


def test_failures_follow_the_webhook_retry_schedule_then_park(jobs):
    schedule("test_fail", {})
    waits = []
    for _ in range(len(RETRIES) + 1):
        with jobs.begin() as conn:
            conn.execute(text("UPDATE jobs SET run_at = now()"))
            job = claim(conn)
        work(job)
        with jobs.connect() as conn:
            waits.append(conn.execute(text("SELECT round(extract(epoch FROM run_at - now())) FROM jobs")).scalar())
            parked = conn.execute(text("SELECT attempts, locked_until = 'infinity', last_error FROM jobs")).one()
    assert waits[:-1] == list(RETRIES)
    assert tuple(parked) == (len(RETRIES) + 1, True, "RuntimeError: receiver down")


def test_a_job_whose_runs_never_finish_parks_once_its_attempts_are_spent(jobs):
    schedule("test_ok", {"n": "lost"})
    for _ in range(len(DEFAULT_BACKOFF) + 1):  # each run dies: its lease just lapses
        with jobs.begin() as conn:
            assert claim(conn)
            conn.execute(text("UPDATE jobs SET locked_until = now() - interval '1 second'"))
    with jobs.begin() as conn:
        last = claim(conn)
    work(last)
    with jobs.connect() as conn:
        assert conn.execute(text("SELECT locked_until = 'infinity' FROM jobs")).scalar()
    assert {"n": "lost"} not in ran


def test_rescheduling_a_key_moves_the_job_and_cancel_removes_it(jobs):
    schedule("test_ok", {"n": 1}, key="k", delay=60)
    schedule("test_ok", {"n": 2}, key="k", delay=120)
    with jobs.connect() as conn:
        rows = conn.execute(text("SELECT payload, extract(epoch FROM run_at - now()) AS wait FROM jobs")).all()
    assert len(rows) == 1 and rows[0].payload == {"n": 2} and 110 < rows[0].wait <= 120
    assert cancel("test_ok", "k") and not cancel("test_ok", "k")


def private(monkeypatch) -> None:
    monkeypatch.setattr(
        webhooks, "settings", dataclasses.replace(webhooks.settings, webhook_private_hosts=("localhost", "127.0.0.1"))
    )


@pytest.mark.parametrize(
    "url",
    [
        "http://hooks.example.com/hook",
        "https://10.0.0.7/hook",
        "https://169.254.169.254/latest/meta-data",
        "https://100.64.0.1/hook",
        "https://[::1]/hook",
        "https://[fd00::1]/hook",
        "https://[::ffff:10.0.0.1]/hook",
        "https://[64:ff9b::a00:1]/hook",
        "https://user:pw@93.184.216.34/hook",
        "ftp://localhost/hook",
    ],
)
def test_webhook_urls_off_the_public_internet_are_refused(url, monkeypatch):
    private(monkeypatch)
    with pytest.raises(ValueError):
        destination(url)


def test_a_webhook_host_is_pinned_to_the_public_address_it_resolved_to(monkeypatch):
    answers = {"good.test": "93.184.216.34", "rebind.test": "10.0.0.1"}
    monkeypatch.setattr(socket, "getaddrinfo", lambda host, *_, **__: [(2, 1, 6, "", (answers[host], 443))])
    private(monkeypatch)
    with pytest.raises(ValueError):
        destination("https://rebind.test/hook")
    assert pinned("http://localhost:8080/hook") == ("http://localhost:8080/hook", {}, {})
    assert pinned("https://good.test:8443/hook") == (
        "https://93.184.216.34:8443/hook",
        {"Host": "good.test:8443"},
        {"sni_hostname": "good.test"},
    )


@pytest.fixture
def receiver(monkeypatch):
    """A local webhook receiver that answers with `reply` (optionally a byte at a time) and never closes."""
    private(monkeypatch)
    server, done = socket.create_server(("127.0.0.1", 0)), threading.Event()

    def start(reply: bytes, drip: bool = False) -> str:
        def answer() -> None:
            conn, _ = server.accept()
            conn.recv(65536)
            for chunk in [reply[i : i + 1] for i in range(len(reply))] if drip else [reply]:
                if done.wait(0.05 if drip else 0):
                    break
                conn.sendall(chunk)
            done.wait()
            conn.close()

        threading.Thread(target=answer, daemon=True).start()
        return f"http://127.0.0.1:{server.getsockname()[1]}/hook"

    yield start
    done.set()
    server.close()


def test_a_delivery_takes_the_status_without_reading_the_body(receiver):
    url = receiver(b"HTTP/1.1 200 OK\r\nContent-Length: 1000000000\r\n\r\n" + b"x" * 1024)
    assert asyncio.run(post(url, b"{}", {})) == 200


def test_a_trickled_answer_is_cut_at_the_total_deadline(receiver, monkeypatch):
    monkeypatch.setattr(webhooks, "DEADLINE", 0.5)
    url = receiver(b"HTTP/1.1 200 OK\r\nX-Pad: " + b"a" * 200 + b"\r\n\r\n", drip=True)
    started = time.monotonic()
    with pytest.raises(TimeoutError):
        asyncio.run(post(url, b"{}", {}))
    assert time.monotonic() - started < 2


def test_a_signature_carries_one_hmac_of_timestamp_and_body_per_secret():
    assert signature(1767225600, b'{"id":"1"}', "whsec_new", "whsec_old").split(",") == [
        "v1=5cc14c1ea78fa0e37b3bca2a01e3b846d69f22a8fe6c5c3d2d17773e0f6b80b8",
        "v1=a15e78703ca9aaa68190b4467b7e1ed82f0a70f723a8d82f2bde96ff9dfda6a3",
    ]


def test_the_stream_resumes_after_the_last_id_it_saw_then_expires():
    heads, batches, reads = iter([2, 5, 9, 9]), [[{"id": "1"}, {"id": "2"}], [], [{"id": "7"}], []], []
    answers = iter([True, True, True, False])

    async def latest():
        return next(heads)

    async def read(cursor, head):
        reads.append((cursor, head))
        return batches.pop(0)

    async def alive():
        return next(answers)

    async def wake():
        return None

    async def collect():
        return [chunk async for chunk in frames(latest, read, 0, alive, wake)]

    out = asyncio.run(collect())
    assert out[:3] == ["retry: 3000\n: ping\n\n", 'id: 1\ndata: {"id":"1"}\n\n', 'id: 2\ndata: {"id":"2"}\n\n']
    assert out[3:6] == ["id: 5\n\n", 'id: 7\ndata: {"id":"7"}\n\n', "id: 9\n\n"]  # scoped-out seqs skipped
    assert reads == [(0, 2), (2, 5), (5, 9), (9, 9)]
    assert out[-1] == 'data: {"type":"auth.expired"}\n\n'
