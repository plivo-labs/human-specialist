"""Self-tests for World against a real HS and Postgres."""

import os
import socket
import subprocess
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

import httpx
import psycopg2
import pytest

import tests.harness as harness
from tests.conftest import REPO
from tests.harness import SEARCH_URL, EventStream, World, _Redis, require_search, slow

pytestmark = pytest.mark.xdist_group("harness")


@pytest.fixture(scope="module")
def world():
    w = World()
    yield w
    w.close()


def test_a_world_never_shares_a_database_with_a_live_one(world):
    tenant = world.new_tenant()
    other = World()
    try:
        assert world.operator.get(f"/api/v1/tenants/{tenant.id}").status_code == 200
        assert other.operator.get(f"/api/v1/tenants/{tenant.id}").status_code == 404
    finally:
        other.close()


def test_a_world_whose_database_name_is_taken_fails_and_leaves_the_live_one_alone(world, monkeypatch):
    taken = uuid.UUID(world._db.info.dbname.removeprefix("hs_accept_"))
    monkeypatch.setattr(harness, "uuid4", lambda: taken)
    with pytest.raises(psycopg2.errors.DuplicateDatabase):
        World()
    assert world.new_tenant().id


def test_run_jobs_never_runs_or_waits_on_an_earlier_tests_job(world):
    with world._db.cursor() as cur:
        cur.execute(
            "INSERT INTO jobs (kind, run_at, payload) VALUES ('earlier', now() + interval '1 day', '{}') RETURNING id, run_at"
        )
        earlier, run_at = cur.fetchone()
    world.begin_test()
    world.run_jobs()
    world.advance(2 * 86400)
    with world._db.cursor() as cur:
        cur.execute("SELECT run_at, attempts FROM jobs WHERE id = %s", (earlier,))
        assert cur.fetchone() == (run_at, 0)


def test_a_flush_never_fires_the_call_session_backstop(world):
    world.begin_test()
    with world._db.cursor() as cur:
        cur.execute(
            "INSERT INTO jobs (kind, run_at, payload) VALUES ('call_session', now() + interval '1 day', '{}')"
            " RETURNING id, run_at"
        )
        backstop, run_at = cur.fetchone()
    world.run_jobs()
    with world._db.cursor() as cur:
        cur.execute("SELECT run_at, attempts FROM jobs WHERE id = %s", (backstop,))
        assert cur.fetchone() == (run_at, 0)


def test_a_world_deletes_its_tenants_search_indices_and_no_other_when_it_ends():
    require_search()
    w = World(search_url=SEARCH_URL)
    mine, other = (f"{SEARCH_URL}/hs-conversations-{t}" for t in (w.new_tenant().id, uuid.uuid4()))
    try:
        for index in (mine, other):
            httpx.put(index, timeout=30).raise_for_status()
    finally:
        w.close()
    try:
        assert (httpx.head(mine, timeout=30).status_code, httpx.head(other, timeout=30).status_code) == (404, 200)
    finally:
        httpx.delete(other, timeout=30)


def test_travel_runs_a_job_due_in_300_s_and_expires_a_300_s_key(world):
    world.begin_test()
    tenant = world.new_tenant()
    hangup = {"type": "hangup", "greeting": {"text": "Nobody here"}}
    number, _ = tenant.inbound_line(tenant.agent(), wait_timeout_sec=300, overflow_action=hangup)
    stream = tenant.stream(after=0)
    caller = world.plivo.call(number)
    stream.wait_for("conversation.queued")
    world.redis("SET", "travel", "300 s", "EX", "300")
    world.travel(290)
    assert (caller.state, world.redis("EXISTS", "travel")) == ("in_progress", "1")
    world.travel(11)
    assert stream.wait_for("conversation.overflowed")["data"]["reason"] == "wait_timeout"
    assert (caller.wait_state("completed").said[-1], world.redis("EXISTS", "travel")) == ("Nobody here", "0")


def test_a_later_tests_travel_never_runs_an_earlier_tests_timer(world):
    world.begin_test()
    tenant = world.new_tenant()
    hangup = {"type": "hangup", "greeting": {"text": "Nobody here"}}
    number, _ = tenant.inbound_line(tenant.agent(), wait_timeout_sec=300, overflow_action=hangup)
    stream = tenant.stream(after=0)
    caller = world.plivo.call(number)
    stream.wait_for("conversation.queued")
    world.end_test()
    world.begin_test()
    world.travel(301)
    world.end_test()
    assert (caller.state, tenant.events(type="conversation.overflowed")) == ("in_progress", [])


def test_a_timer_a_transaction_holds_as_its_test_ends_is_parked_once_let_go(world):
    world.begin_test()
    tenant = world.new_tenant()
    number, _ = tenant.inbound_line(tenant.agent(), wait_timeout_sec=300, overflow_action={"type": "hangup"})
    for _ in range(2):
        world.plivo.call(number)
    world.wait(lambda: len(tenant.events(type="conversation.queued")) == 2, what="both callers queued")
    parked = "SELECT count(*) FROM hs.jobs WHERE id > %s AND locked_until = 'infinity'"
    other = psycopg2.connect(world._env["DATABASE_URL"])
    try:
        cur = other.cursor()
        cur.execute(parked, (world._jobs_after,))
        before = cur.fetchone()
        cur.execute("SELECT max(id) FROM hs.jobs WHERE id > %s AND kind = 'queue_timeout'", (world._jobs_after,))
        cur.execute("SELECT id FROM hs.jobs WHERE id = %s FOR UPDATE", cur.fetchone())
        (held,) = cur.fetchone()

        def release_once_a_pass_skipped_it() -> None:
            try:
                world.wait(lambda: cur.execute(parked, (world._jobs_after,)) or cur.fetchone() > before, timeout=10)
            finally:
                other.commit()

        with ThreadPoolExecutor(1) as pool:
            release = pool.submit(release_once_a_pass_skipped_it)
            world.end_test()
            release.result()
        cur.execute("SELECT locked_until = 'infinity' FROM hs.jobs WHERE id = %s", (held,))
        assert cur.fetchone() == (True,)
    finally:
        other.close()


def test_two_timers_inside_one_jump_run_in_order_each_at_its_own_time(world):
    world.begin_test()
    tenant = world.new_tenant()
    agent, hangup, waits = tenant.agent(), {"type": "hangup"}, (100, 200)
    lines = [tenant.inbound_line(agent, wait_timeout_sec=s, overflow_action=hangup) for s in waits]
    stream = tenant.stream(after=0)
    for number, queue in lines:
        world.plivo.call(number)
        stream.wait_for("conversation.queued", queue_id=queue["id"])
    world.travel(201)
    overflowed = tenant.events(type="conversation.overflowed")
    assert [e["data"]["queue_id"] for e in overflowed] == [queue["id"] for _, queue in lines]
    queued = {e["data"]["queue_id"]: e["created_at"] for e in tenant.events(type="conversation.queued")}
    due = [
        datetime.fromisoformat(queued[q["id"]]) + timedelta(seconds=s) for (_, q), s in zip(lines, waits, strict=True)
    ]
    ran = [datetime.fromisoformat(e["created_at"]) for e in overflowed]
    # A late first run narrows the gap to the second, so each run is held to its own due time instead.
    assert due[0] <= ran[0] < due[1] <= ran[1]


def test_two_timers_due_a_moment_apart_run_in_order_after_the_sweep_of_a_desk_gone_quiet(world):
    world.begin_test()
    tenant = world.new_tenant()
    quiet = tenant.agent("Quiet").login()
    for name in ("Busy 1", "Busy 2", "Busy 3"):
        tenant.agent(name).login()
    world._desks.discard(quiet)
    lines = [tenant.inbound_line(wait_timeout_sec=100, overflow_action={"type": "hangup"}) for _ in range(2)]
    stream = tenant.stream(after=0)
    for number, queue in lines:
        world.plivo.call(number)
        stream.wait_for("conversation.queued", queue_id=queue["id"])
    world.travel(101)
    seen = [
        e["agent_id"] if e["type"] == "agent.status_changed" else e["data"]["queue_id"]
        for e in tenant.events()
        if e["type"] == "conversation.overflowed"
        or (e["type"] == "agent.status_changed" and e["data"]["to_status"]["name"] == "Offline")
    ]
    assert seen == [quiet.id, *(queue["id"] for _, queue in lines)]


def test_the_time_travel_itself_takes_counts_toward_the_jump(world):
    world.begin_test()
    tenant = world.new_tenant()
    sink = world.webhook_sink()
    tenant.api.post("/api/v1/webhooks", json={"url": sink.url}).raise_for_status()
    number, _ = tenant.inbound_line(tenant.agent(), wait_timeout_sec=300, overflow_action={"type": "hangup"})
    sink.stall_next()
    caller = world.plivo.call(number)
    world.wait(lambda: sink.deliveries, what="a delivery HS waits 10 s on")
    world.travel(290)
    assert caller.state == "in_progress"


def test_travel_a_held_row_blocks_gives_up_naming_it_and_leaves_redis_alone(world):
    world.new_tenant()
    world.redis("SET", "held", "1", "EX", "100")
    other = psycopg2.connect(world._env["DATABASE_URL"])
    try:
        other.cursor().execute("SELECT 1 FROM hs.tenant LIMIT 1 FOR NO KEY UPDATE")
        with pytest.raises(RuntimeError, match=r"hs\.tenant"):
            world.travel(50)
    finally:
        other.close()
    assert 60 < int(world.redis("TTL", "held")) <= 100


def test_a_stop_whose_beat_fails_leaves_none_of_its_timers_parked(world, monkeypatch):
    world.begin_test()
    tenant = world.new_tenant()
    desk = tenant.agent().login()
    lines = [tenant.inbound_line(wait_timeout_sec=s, overflow_action={"type": "hangup"}) for s in (300, 600)]
    stream = tenant.stream(after=0)
    for number, queue in lines:
        world.plivo.call(number)
        stream.wait_for("conversation.queued", queue_id=queue["id"])
    down = httpx.Client(base_url=world.base_url, transport=httpx.MockTransport(lambda _: httpx.Response(503)))
    monkeypatch.setattr(desk, "api", lambda: down)
    with pytest.raises(AssertionError, match="503"):
        world.travel(301)
    world._desks.discard(desk)
    world.travel(600)
    overflowed = tenant.events(type="conversation.overflowed")
    assert [e["data"]["queue_id"] for e in overflowed] == [queue["id"] for _, queue in lines]


def test_travel_never_frees_a_job_hs_rescheduled_and_parked_during_a_stop(world, monkeypatch):
    world.begin_test()
    desk = world.new_tenant().agent().login()
    with world._db.cursor() as cur:
        cur.execute(
            "INSERT INTO jobs (kind, key, run_at, payload) VALUES ('refused', 'k', now() + interval '1 day', '{}')"
            " RETURNING id"
        )
        (job,) = cur.fetchone()

    def reschedule_then_refuse(_: httpx.Request) -> httpx.Response:
        with world._db.cursor() as cur:
            cur.execute("UPDATE jobs SET rev = rev + 1, locked_until = 'infinity' WHERE id = %s", (job,))
        return httpx.Response(204)

    hs = httpx.Client(base_url=world.base_url, transport=httpx.MockTransport(reschedule_then_refuse))
    monkeypatch.setattr(desk, "api", lambda: hs)
    world.travel(10)
    with world._db.cursor() as cur:
        cur.execute("SELECT locked_until = 'infinity' FROM jobs WHERE id = %s", (job,))
        assert cur.fetchone() == (True,)


def test_travel_beats_the_desks_still_pinging_and_not_the_one_that_stopped(world):
    world.begin_test()
    tenant = world.new_tenant()
    quiet, busy = tenant.agent("Quiet").login(), tenant.agent("Busy").login()
    quiet.stop_pinging()
    assert (quiet._status(), busy._status()) == ("Offline", "Available")


def test_a_desk_that_stopped_pinging_is_swept_even_when_a_stop_late_in_the_jump_ran_the_sweep_first(world):
    world.begin_test()
    tenant = world.new_tenant()
    agent = tenant.agent().login()
    agent.stop_pinging()
    agent.heartbeat()
    number, _ = tenant.inbound_line(wait_timeout_sec=30, overflow_action={"type": "hangup"})
    stream = tenant.stream(after=0)
    world.plivo.call(number)
    stream.wait_for("conversation.queued")
    agent.stop_pinging()
    agent.heartbeat()
    assert agent._status() == "Offline", "the sweep reset the desk, so one beat does not sign it in"


def test_a_desk_gone_quiet_is_swept_before_a_ring_timeout_later_in_the_jump_so_the_next_offer_skips_it(
    world, monkeypatch
):
    world.begin_test()
    monkeypatch.setattr(world.plivo, "time_factor", 1000.0)  # Plivo rings 60 s as 16 h, so only travel times it out
    tenant = world.new_tenant()
    tenant.api.patch("/api/v1/settings", json={"ring_timeout_sec": 60}).raise_for_status()
    ringing, quiet = tenant.agent("Ringing").login(), tenant.agent("Quiet")
    ringing.phone.on_ring("ignore")
    number, _ = tenant.inbound_line(ringing, quiet, wait_timeout_sec=1800)
    stream = tenant.stream(after=0)
    world.plivo.call(number)
    stream.wait_for("offer.created", agent_id=ringing.id)
    quiet.login()
    world._desks.discard(quiet)
    world.travel(61)
    stream.wait_for("offer.missed", agent_id=ringing.id)
    assert [e["agent_id"] for e in tenant.events(type="offer.created")] == [ringing.id]
    assert quiet._status() == "Offline"


def test_a_desk_whose_token_expires_in_the_jump_stops_beating(world):
    world.begin_test()
    agent = world.new_tenant().agent().login()
    world.travel(3601)
    world.wait(lambda: agent._status() == "Offline", what="the sweep to sign out the desk whose token expired")


def test_a_desk_that_signs_in_after_the_jump_rings_for_its_whole_ring_timeout(world):
    world.begin_test()
    tenant = world.new_tenant()
    tenant.api.patch("/api/v1/settings", json={"ring_timeout_sec": 120}).raise_for_status()
    agent = tenant.agent().login(available=False)
    agent.phone.on_ring("ignore")
    number, _ = tenant.inbound_line(agent, wait_timeout_sec=1800)
    stream = tenant.stream(after=0)
    world.plivo.call(number)
    stream.wait_for("conversation.queued")
    agent.heartbeat()
    world.travel(121)
    floor = tenant.api.get("/api/v1/floor").raise_for_status().json()["agents"]
    assert [a["activity"] for a in floor if a["agent_id"] == agent.id] == ["ringing"]


def test_travel_refuses_to_move_time_back(world):
    world.redis("SET", "back", "1", "EX", "100")
    with pytest.raises(ValueError):
        world.travel(-50)
    assert 0 < int(world.redis("TTL", "back")) <= 100


def test_after_redis_down_the_server_comes_back_empty(world):
    world.redis("SET", "lost", "1")
    with world.redis_down(), pytest.raises(subprocess.CalledProcessError):
        world.redis("PING")
    assert world.redis("DBSIZE") == "0"


def test_travel_until_rebuilt_travels_on_past_a_sweep_that_failed_after_a_redis_loss(world, monkeypatch):
    world.begin_test()
    tenant = world.new_tenant()
    with world.redis_down():
        pass
    lock = psycopg2.connect(world._env["DATABASE_URL"])
    lock.autocommit = True
    lock.cursor().execute("SELECT pg_advisory_lock(5107, hashtext(%s))", (tenant.id,))
    travel, travels = world.travel, []

    def travel_then_unlock(seconds: float) -> None:
        travel(seconds)
        travels.append(world.redis("EXISTS", f"hs:{{{tenant.id}}}:epoch"))
        lock.close()

    monkeypatch.setattr(world, "travel", travel_then_unlock)
    world.travel_until_rebuilt()
    assert travels == ["0", "1"]


def test_a_job_a_request_to_the_replica_scheduled_runs(world):
    world.begin_test()
    tenant = world.replica().new_tenant()
    agent = tenant.agent()
    number, _ = tenant.inbound_line(agent)
    stream = tenant.stream(after=0)
    world.plivo.call(number)
    stream.wait_for("conversation.queued")
    agent.login()
    stream.wait_for("offer.created", agent_id=agent.id)


def test_a_world_restores_the_environment_it_changed_when_it_ends():
    before = dict(os.environ)
    World().close()
    assert dict(os.environ) == before


def test_closing_an_event_stream_ends_its_connection_at_once():
    server, conns = socket.create_server(("127.0.0.1", 0)), []

    def answer() -> None:
        conns.append(conn := server.accept()[0])
        conn.recv(65536)
        conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n")

    threading.Thread(target=answer, daemon=True).start()
    with server:
        EventStream(f"http://127.0.0.1:{server.getsockname()[1]}/", "token").close()
        with conns[0] as conn:
            conn.settimeout(10 * slow())
            assert conn.recv(1) == b""


def test_each_xdist_worker_draws_its_redis_ports_from_a_slice_of_its_own(monkeypatch):
    for count in (1, 2, 3, 4, 8):
        monkeypatch.setenv("PYTEST_XDIST_WORKER_COUNT", str(count))
        edges = []
        for worker in range(count):
            monkeypatch.setenv("PYTEST_XDIST_WORKER", f"gw{worker}")
            for pick in (min, max):
                monkeypatch.setattr(harness.secrets, "randbelow", lambda n, pick=pick: pick(0, n - 1))
                edges.append(harness._redis_port())
        assert edges[0] >= 20000 and edges[-1] <= 32767 and edges == sorted(set(edges)), (count, edges)


def test_a_worlds_redis_on_a_port_another_server_holds_moves_to_a_fresh_one(monkeypatch):
    holder = _Redis()
    ports = iter([holder.port])
    monkeypatch.setattr(harness, "_redis_port", lambda draw=harness._redis_port: next(ports, None) or draw())
    mine = _Redis()
    try:
        assert mine.port != holder.port
        assert f"process_id:{mine._proc.pid}" in mine.cli("INFO", "server").split()
    finally:
        mine.stop()
        holder.stop()


def test_a_child_of_a_killed_test_process_ends_with_it():
    code = (
        "import subprocess, sys, time\n"
        "from tests.harness import _outlive_never\n"
        "child = subprocess.Popen(['sleep', '600'])\n"
        "_outlive_never(child)\n"
        "print(child.pid, flush=True)\n"
        "time.sleep(600)\n"
    )
    parent = subprocess.Popen([sys.executable, "-c", code], stdout=subprocess.PIPE, text=True, cwd=REPO)
    child = int(parent.stdout.readline())
    parent.kill()
    parent.wait(10)

    def gone() -> bool:
        try:
            os.kill(child, 0)
        except ProcessLookupError:
            return True
        return False

    deadline = time.monotonic() + 15 * slow()
    while not gone() and time.monotonic() < deadline:
        time.sleep(0.1)
    assert gone(), "the killed process's child still runs"
