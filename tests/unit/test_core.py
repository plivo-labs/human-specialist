import asyncio
import logging
import os
import subprocess
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from types import SimpleNamespace

import psycopg2
import pytest
from redis import ConnectionError as RedisDown
from redis import Redis, RedisError, ResponseError
from redis import TimeoutError as RedisTimeout
from sqlalchemy import insert, select, text
from sqlalchemy.exc import DBAPIError, OperationalError

from hs import app as service
from hs import core, plivo


def cursor(db, name: str) -> None:
    db.execute(insert(core.T.hook_cursor).values(name=name))


def stored(name: str) -> bool:
    with core.read() as db:
        return (
            db.execute(select(core.T.hook_cursor.c.name).where(core.T.hook_cursor.c.name == name)).first() is not None
        )


def test_after_commit_runs_in_order_once_the_rows_are_visible():
    name, seen = uuid.uuid4().hex, []
    with core.tx() as db:
        cursor(db, name)
        core.after_commit(db, lambda: seen.append(("first", stored(name))))
        core.after_commit(db, lambda: seen.append(("second", stored(name))))
        assert seen == []
    assert seen == [("first", True), ("second", True)]


def test_a_failing_after_commit_is_logged_and_the_next_one_still_runs(caplog):
    seen = []
    with core.tx() as db:
        core.after_commit(db, lambda: 1 / 0)
        core.after_commit(db, lambda: seen.append("ran"))
    assert seen == ["ran"]
    assert "after-commit" in caplog.text and "ZeroDivisionError" in caplog.text


def test_a_rollback_runs_nothing_after_commit():
    name, seen = uuid.uuid4().hex, []
    with pytest.raises(RuntimeError), core.tx() as db:
        cursor(db, name)
        core.after_commit(db, lambda: seen.append("ran"))
        raise RuntimeError("abort")
    assert seen == [] and not stored(name)


def test_capped_refuses_an_object_over_its_limit_or_with_nul():
    assert core.capped(None) == {}
    assert core.capped({"a": "b" * 2000}) == {"a": "b" * 2000}
    with pytest.raises(core.ApiError) as big:
        core.capped({"a": "b" * 2048})
    with pytest.raises(core.ApiError) as nul:
        core.capped({"a": "\x00"}, field="attributes")
    assert (big.value.code, nul.value.code) == ("context_too_large", "attributes_invalid")


@pytest.mark.parametrize(("query", "expected"), [("", "3"), ("?connect_timeout=15", "15")])
def test_the_engine_connects_with_3s_unless_the_url_sets_a_connect_timeout(query, expected):
    env = {**os.environ, "DATABASE_URL": "postgresql://u:p@127.0.0.1:1/d" + query}
    show = "from hs.core import engine as e; print(e.url.query['connect_timeout'])"
    out = subprocess.run([sys.executable, "-c", show], env=env, capture_output=True, text=True, check=True)
    assert out.stdout.strip() == expected


def boot(public_base_url: str) -> subprocess.CompletedProcess:
    show = "from hs.core import settings; print(settings.public_base_url)"
    env = {**os.environ, "PUBLIC_BASE_URL": public_base_url}
    return subprocess.run([sys.executable, "-c", show], env=env, capture_output=True, text=True)


@pytest.mark.parametrize(
    "url",
    [
        "hs.example",
        "ftp://hs.example",
        "https://",
        "https://hs.example:bad",
        "https://bad host",
        "https://%zz",
        "https://hs..example/hs",
        "https://-hs.example",
        "https://hs.example/h\ns",
    ],
)
def test_a_public_base_url_that_is_not_an_absolute_http_url_stops_the_start(url):
    started = boot(url)
    assert started.returncode != 0 and "PUBLIC_BASE_URL" in started.stderr, started


@pytest.mark.parametrize(
    "url", ["https://hs.example.com", "http://10.0.0.5:8080/hs", "https://[::1]:8443", "https://bücher.example/hs"]
)
def test_a_public_base_url_with_a_port_or_a_path_prefix_starts(url):
    started = boot(url + "/")
    assert started.returncode == 0 and started.stdout.strip() == url, started


@pytest.mark.parametrize(("name", "value"), [("DB_MAX_OVERFLOW", "-1"), ("DB_POOL_SIZE", "0")])
def test_a_connection_budget_that_is_unbounded_or_empty_stops_the_start(name, value):
    started = subprocess.run(
        [sys.executable, "-c", "import hs.core"], env={**os.environ, name: value}, capture_output=True, text=True
    )
    assert started.returncode != 0 and name in started.stderr, started


def test_a_connection_budget_with_no_room_beside_the_timers_reserve_stops_the_start():
    small = {"DB_POOL_SIZE": "8", "DB_MAX_OVERFLOW": "0"}
    started = subprocess.run(
        [sys.executable, "-c", "import hs.core"], env={**os.environ, **small}, capture_output=True, text=True
    )
    assert started.returncode != 0 and "DB_POOL_SIZE + DB_MAX_OVERFLOW" in started.stderr, started


def test_a_failed_statement_keeps_its_bound_values_out_of_the_error():
    with pytest.raises(DBAPIError) as raised, core.read() as db:
        db.execute(text("SELECT 1 / 0 WHERE CAST(:number AS text) <> ''"), {"number": "+14155550123"})
    assert "+14155550123" not in str(raised.value), raised.value


def test_after_a_postgres_restart_each_held_connection_fails_once_and_the_next_request_reconnects():
    held = [core.engine.connect() for _ in range(2)]
    for db in held:
        db.exec_driver_sql("SELECT 1")
    restart = psycopg2.connect(core.settings.database_url.partition("?")[0])
    restart.autocommit = True
    with restart.cursor() as cur:
        cur.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity"
            " WHERE datname = current_database() AND pid <> pg_backend_pid()"
        )
    restart.close()
    for db in held:
        with pytest.raises(OperationalError):
            db.exec_driver_sql("SELECT 1")
        db.close()
    for _ in range(3):
        with core.read() as db:
            assert db.exec_driver_sql("SELECT 1").scalar() == 1


def test_a_write_refused_by_a_demoted_primary_discards_the_pool_so_the_next_checkout_is_fresh():
    def backend(db) -> int:
        return db.exec_driver_sql("SELECT pg_backend_pid()").scalar()

    first, second = core.engine.connect(), core.engine.connect()
    old = {backend(first), backend(second)}
    first.close()
    second.close()
    try:
        demoted = core.engine.connect()
        demoted.exec_driver_sql("SET default_transaction_read_only = on")
        demoted.commit()
        with pytest.raises(DBAPIError):
            demoted.exec_driver_sql("INSERT INTO hook_cursor (name) VALUES ('refused')")
        demoted.close()
        with core.tx() as db:
            assert db.exec_driver_sql("SHOW default_transaction_read_only").scalar() == "off"
            assert backend(db) not in old
    finally:
        core.engine.dispose()


def test_a_postgres_older_than_14_stops_the_start(monkeypatch):
    @contextmanager
    def read():
        yield SimpleNamespace(exec_driver_sql=lambda sql: SimpleNamespace(scalar=lambda: "130012"))

    monkeypatch.setattr(core, "read", read)
    with pytest.raises(RuntimeError, match="Postgres 14 or later"):
        core.check_versions()


def probe_answers(monkeypatch, error: Exception) -> None:
    run = core.redis.execute_command

    def refuse(*args, **kw):
        if args[0] == "EXPIRE":
            raise error
        return run(*args, **kw)

    monkeypatch.setattr(core.redis, "execute_command", refuse)


def test_a_redis_older_than_7_stops_the_start(monkeypatch):
    probe_answers(monkeypatch, ResponseError("wrong number of arguments for 'expire' command"))
    with pytest.raises(RuntimeError, match="needs Redis 7 or later"):
        core.check_versions()


def test_a_redis_that_refuses_info_but_accepts_the_probe_starts(monkeypatch):
    def refuse(section):
        raise ResponseError("NOPERM this user has no permissions to run the 'info' command")

    monkeypatch.setattr(core.redis, "info", refuse)
    core.check_versions()


@pytest.mark.parametrize(
    "error",
    [
        ResponseError("NOPERM this user has no permissions to run the 'expire' command"),
        ResponseError("unknown command 'EXPIRE', with args beginning with: 'hs:{probe}' '1' 'GT' "),
        ResponseError("unknown command `HELLO`, with args beginning with: `3`, "),
        RedisDown("down"),
    ],
    ids=["acl", "renamed", "hello-disabled", "down"],
)
def test_a_redis_that_cannot_answer_the_probe_warns_and_starts(monkeypatch, caplog, error):
    probe_answers(monkeypatch, error)
    core.check_versions()
    assert [r.levelname for r in caplog.records if r.name == "hs.core"] == ["WARNING"]


def test_the_servers_the_tests_run_on_pass_the_version_check():
    core.check_versions()


def test_a_redis_that_may_evict_keys_logs_an_error_naming_the_configuration_page(caplog):
    core.check_eviction()
    core.redis.config_set("maxmemory-policy", "allkeys-lru")
    try:
        core.check_eviction()
    finally:
        core.redis.config_set("maxmemory-policy", "noeviction")
    assert [(r.levelname, r.getMessage()) for r in caplog.records if r.name == "hs.core"] == [
        ("ERROR", "Redis maxmemory-policy is allkeys-lru, not noeviction: see docs/configuration.md#run-redis")
    ]


def test_a_5xx_api_error_logs_its_cause(caplog):
    request = SimpleNamespace(method="POST", url=SimpleNamespace(path="/x"), state=SimpleNamespace())
    with caplog.at_level("WARNING", logger="hs.core"):
        core._api_error(request, plivo.Uncertain("POST /Call/: ReadTimeout"))
        core._api_error(request, core.ApiError(404, "not_found"))
    assert [r.getMessage() for r in caplog.records] == [
        "POST /x answered 503 plivo_unavailable: POST /Call/: ReadTimeout"
    ]


def test_a_caller_gone_while_its_body_is_read_is_a_499():
    sent, path = [], "/api/v1/tenants"
    scope = {"type": "http", "method": "POST", "path": path, "raw_path": path.encode(), "query_string": b""}
    scope |= {"headers": [(b"content-type", b"application/json"), (b"content-length", b"40")], "root_path": ""}

    async def receive() -> dict:
        return {"type": "http.disconnect"}

    async def send(message: dict) -> None:
        sent.append(message)

    asyncio.run(service.app(scope, receive, send))
    assert sent[0]["status"] == 499


def test_a_redis_that_stops_answering_is_waited_on_once_logged_once_and_probed_until_it_answers(monkeypatch, caplog):
    monkeypatch.setattr(core, "REDIS_SHUT", 0.3, raising=False)
    core.redis.execute_command("CLIENT", "PAUSE", "600", "ALL")
    waits = []
    with caplog.at_level(logging.WARNING, logger="hs.core"):
        for _ in range(4):
            start = time.monotonic()
            with pytest.raises(RedisError):
                core.redis.get("k")
            waits.append(time.monotonic() - start)
        deadline = time.monotonic() + 5
        while True:
            try:
                core.redis.get("k")
                break
            except RedisError:
                assert time.monotonic() < deadline, "a probe answers once the pause is over"
                time.sleep(0.02)
    assert waits[0] < 1 and max(waits[1:]) < 0.05, "one wait, then every call fails fast"
    assert len([r for r in caplog.records if r.name == "hs.core"]) == 2, "logged once as it stops, once as it answers"


@pytest.fixture
def gets(monkeypatch):
    """A GET of a key the test maps to a function runs that function in place of Redis's reply; the breaker is
    closed again after the test."""
    fakes, send = {}, Redis.execute_command

    def execute(self, *args, **options):
        if args[0] == "GET" and args[1] in fakes:
            return fakes[args[1]]()
        return send(self, *args, **options)

    monkeypatch.setattr(Redis, "execute_command", execute)
    monkeypatch.setattr(core, "_probe_at", None)
    return fakes


def stalls():
    raise RedisTimeout("Timeout reading from socket")


def held_until(release: threading.Event, then=lambda: None):
    held = threading.Event()

    def hold():
        held.set()
        assert release.wait(5)
        return then()

    return held, hold


def test_a_call_that_reaches_the_breaker_while_a_probe_closes_it_is_sent_to_redis(gets, monkeypatch):
    monkeypatch.setattr(core, "REDIS_SHUT", 0)
    gets["stalled"] = stalls
    with pytest.raises(RedisTimeout):
        core.redis.get("stalled")
    here, go, lock = threading.Event(), threading.Event(), core._breaker

    class Arrived:
        def __enter__(self):
            if threading.current_thread() is not threading.main_thread() and not here.is_set():
                here.set()
                assert go.wait(5)
            return lock.__enter__()

        def __exit__(self, *exc):
            return lock.__exit__(*exc)

    monkeypatch.setattr(core, "_breaker", Arrived())
    late = ThreadPoolExecutor(1).submit(core.redis.get, "k")
    assert here.wait(5)
    assert core.redis.get("k") is None, "the probe answers"
    go.set()
    assert late.result(5) is None, "the late call is sent once the probe closed the breaker"


def test_while_the_probe_is_out_every_other_call_fails_fast(gets, monkeypatch):
    monkeypatch.setattr(core, "REDIS_SHUT", 0)
    release = threading.Event()
    held, gets["held"] = held_until(release)
    gets["stalled"] = stalls
    with pytest.raises(RedisTimeout):
        core.redis.get("stalled")
    probe = ThreadPoolExecutor(1).submit(core.redis.get, "held")
    assert held.wait(5)
    with pytest.raises(RedisError):
        core.redis.get("k")
    release.set()
    assert probe.result(5) is None and core.redis.get("k") is None, "the probe closed the breaker"


def test_a_call_sent_before_redis_stopped_answering_does_not_close_the_breaker_by_answering_late(gets, monkeypatch):
    monkeypatch.setattr(core, "REDIS_SHUT", 60)
    release = threading.Event()
    held, gets["held"] = held_until(release)
    gets["stalled"] = stalls
    sent = ThreadPoolExecutor(1).submit(core.redis.get, "held")
    assert held.wait(5)
    with pytest.raises(RedisTimeout):
        core.redis.get("stalled")
    release.set()
    assert sent.result(5) is None
    with pytest.raises(RedisError):
        core.redis.get("k")


def test_a_call_sent_before_redis_stopped_answering_does_not_reopen_the_breaker_a_probe_closed(gets, monkeypatch):
    monkeypatch.setattr(core, "REDIS_SHUT", 0)
    release = threading.Event()
    held, gets["held"] = held_until(release, stalls)
    gets["stalled"] = stalls
    sent = ThreadPoolExecutor(1).submit(core.redis.get, "held")
    assert held.wait(5)
    with pytest.raises(RedisTimeout):
        core.redis.get("stalled")
    assert core.redis.get("k") is None, "a probe closes the breaker"
    monkeypatch.setattr(core, "REDIS_SHUT", 60)
    release.set()
    assert isinstance(sent.exception(5), RedisTimeout)
    assert core.redis.get("k") is None


def test_a_write_the_breaker_refused_has_nothing_to_undo(gets, monkeypatch):
    monkeypatch.setattr(core, "REDIS_SHUT", 60)
    gets["stalled"] = stalls
    with pytest.raises(RedisTimeout):
        core.redis.get("stalled")
    undone = []
    with pytest.raises(RedisError), core.tx() as db:
        core.undoable(db, lambda: core.redis.set("k", 1), lambda: undone.append("k"))
    assert undone == []
