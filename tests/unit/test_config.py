"""Configuration documents on the migrated database, through the API."""

import json
import logging
import threading
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient
from redis import RedisError
from sqlalchemy import event, func, insert, select, text, update

from hs import access, config, core, hot, jobs, plivo
from hs import app as service
from hs.core import T


def admin() -> tuple[uuid.UUID, TestClient]:
    with core.tx() as db:
        tenant = insert(T.tenant).values(name="t", plivo_auth_id=uuid.uuid4().hex, plivo_token_enc="sealed")
        tenant_id = db.execute(tenant.returning(T.tenant.c.id)).scalar()
        key = access.new_key(db, tenant_id, ["admin"])["api_key"]
    return tenant_id, TestClient(service.app, headers={"Authorization": f"Bearer {key}"})


def test_deleting_a_team_or_skill_takes_it_off_every_queue_and_agent_that_names_it():
    tenant_id, api = admin()
    sales, french, spanish = (
        api.post(f"/api/v1/{k}", json={"name": n}).json()["id"]
        for k, n in (("teams", "Sales"), ("skills", "French"), ("skills", "Spanish"))
    )
    skills = [{"skill_id": s, "level": 3} for s in (french, spanish)]
    queue = api.post("/api/v1/queues", json={"name": "Q", "teams": [sales], "skills": skills}).json()["id"]
    with core.tx() as db:
        body = {"name": "A", "role": "agent", "teams": [sales], "skills": skills}
        row = insert(T.config).values(tenant_id=tenant_id, kind="agent", body=body).returning(T.config.c.id)
        agent = db.execute(row).scalar()
    assert api.delete(f"/api/v1/teams/{sales}").status_code == api.delete(f"/api/v1/skills/{french}").status_code == 204
    q = api.get(f"/api/v1/queues/{queue}").json()
    with core.read() as db:
        a = db.execute(select(T.config.c.body).where(T.config.c.id == agent)).scalar()
    assert (q["teams"], [s["skill_id"] for s in q["skills"]]) == ([], [spanish])
    assert (a["teams"], [s["skill_id"] for s in a["skills"]]) == ([], [spanish])


HANGUP = {"type": "hangup", "greeting": {"text": "Goodbye"}}


def stored_menu_with_a_hash_option() -> tuple[TestClient, str]:
    _, api = admin()
    made = api.post("/api/v1/menus", json={"name": "Menu", "greeting": {"text": "Press"}, "options": {"1": HANGUP}})
    menu = made.json()["id"]
    with core.tx() as db:
        db.execute(
            text("UPDATE hs.config SET body = jsonb_set(body, '{options,#}', CAST(:opt AS jsonb)) WHERE id = :id"),
            {"opt": json.dumps(HANGUP), "id": menu},
        )
    return api, menu


def test_a_menu_saved_with_a_hash_option_still_reads_and_patches():
    api, menu = stored_menu_with_a_hash_option()
    assert "#" in api.get(f"/api/v1/menus/{menu}").json()["options"]
    assert any(row["id"] == menu for row in api.get("/api/v1/menus").json()["data"])
    renamed = api.patch(f"/api/v1/menus/{menu}", json={"name": "Renamed"}).json()
    assert renamed["name"] == "Renamed" and set(renamed["options"]) == {"1", "#"}


def test_a_menu_that_replaces_options_drops_the_hash_option():
    api, menu = stored_menu_with_a_hash_option()
    api.patch(f"/api/v1/menus/{menu}", json={"options": {"2": HANGUP}}).raise_for_status()
    assert set(api.get(f"/api/v1/menus/{menu}").json()["options"]) == {"2"}


def held(lock, ready: Future, release: threading.Event) -> None:
    """Hold `lock` in a transaction until `release`; `ready` gets the backend's pid."""
    with core.tx() as db:
        db.execute(lock)
        ready.set_result(db.exec_driver_sql("SELECT pg_backend_pid()").scalar())
        release.wait(10)


def waiter(*pids: int) -> int | None:
    """The backend that waits on a lock one of `pids` holds, once there is one; None after 5 s."""
    blocked = text("SELECT pid FROM pg_stat_activity WHERE pg_blocking_pids(pid) && CAST(:pids AS int[])")
    deadline = time.monotonic() + 5
    with core.read() as db:
        while time.monotonic() < deadline:
            if found := db.execute(blocked, {"pids": list(pids)}).scalar():
                return found
            time.sleep(0.02)
    return None


@pytest.mark.parametrize("kind", ["queues", "agents"])
def test_a_write_naming_a_team_that_is_being_deleted_waits_for_the_delete_and_is_refused(kind, monkeypatch):
    calls = []
    monkeypatch.setattr(
        plivo, "rest", lambda t, *call, **_: calls.append(call) or {"endpoint_id": "e1", "username": "u"}
    )
    tenant_id, api = admin()
    with core.tx() as db:
        config.add(db, tenant_id, "status", {"name": "Offline", "availability": False}, "offline")
    team = api.post("/api/v1/teams", json={"name": "Sales"}).json()["id"]
    named = api.post("/api/v1/queues", json={"name": "Q", "teams": [team]}).json()["id"]
    body = {"name": "W", "teams": [team]} | ({"role": "agent"} if kind == "agents" else {})
    ready, release = Future(), threading.Event()
    with ThreadPoolExecutor(3) as pool:
        # The delete strips the team off `named`: holding that row pauses it inside its transaction.
        pool.submit(held, select(T.config.c.id).where(T.config.c.id == named).with_for_update(), ready, release)
        try:
            holder = ready.result(5)
            deleting = pool.submit(api.delete, f"/api/v1/teams/{team}")
            deleter = waiter(holder)
            writing = pool.submit(api.post, f"/api/v1/{kind}", json=body)
            assert deleter and waiter(deleter)
        finally:
            release.set()
    reply = writing.result()
    assert (deleting.result().status_code, reply.status_code) == (204, 422) and reply.json()["error"][
        "code"
    ] == "foreign_id"
    with core.read() as db:
        naming = text("SELECT count(*) FROM config WHERE tenant_id = :t AND body->'teams' ? :team")
        assert db.execute(naming, {"t": tenant_id, "team": team}).scalar() == 0
    assert calls == ([] if kind == "queues" else [("POST", "Endpoint/"), ("DELETE", "Endpoint/e1/")])


def test_holders_of_a_tenants_config_share_it_and_a_config_write_waits_for_them():
    tenant_id, _ = admin()

    def write() -> None:
        with config.write(access.OPERATOR, tenant_id):
            pass

    with ThreadPoolExecutor(1) as pool:
        with core.tx() as one, core.tx() as two:
            config.hold(one, tenant_id)
            config.hold(two, tenant_id)
            writing = pool.submit(write)
            assert waiter(one.exec_driver_sql("SELECT pg_backend_pid()").scalar()), "the write waits for the holders"
        writing.result(5)


def tenant_with_queue_q() -> tuple[uuid.UUID, TestClient, str]:
    tenant_id, api = admin()
    with config.write(access.OPERATOR, tenant_id) as db:
        status = config.add(db, tenant_id, "status", {"name": "Break", "availability": False})
        config._store(db, tenant_id, rona={"status_id": str(status.id)})
    return tenant_id, api, api.post("/api/v1/queues", json={"name": "Q"}).json()["id"]


def test_a_config_read_another_transaction_made_sends_no_sql_and_a_config_write_is_seen_at_once():
    tenant_id, api, queue = tenant_with_queue_q()
    with core.read() as db:
        assert config.get(db, tenant_id, "queue", queue)["name"] == "Q"
        assert config.tenant_settings(db, tenant_id).ring_timeout_sec == 30
    sent = []

    def count(conn, cursor, statement, *_):
        sent.append(statement)

    event.listen(core.engine, "before_cursor_execute", count)
    try:
        with core.tx() as db:
            assert config.get(db, tenant_id, "queue", queue)["name"] == "Q"
            assert config.tenant_settings(db, tenant_id).ring_timeout_sec == 30
    finally:
        event.remove(core.engine, "before_cursor_execute", count)
    assert sent == []
    assert api.patch(f"/api/v1/queues/{queue}", json={"name": "R"}).status_code == 200
    assert api.patch("/api/v1/settings", json={"ring_timeout_sec": 45}).status_code == 200
    with core.read() as db:
        assert config.get(db, tenant_id, "queue", queue)["name"] == "R"
        assert config.tenant_settings(db, tenant_id).ring_timeout_sec == 45


def test_a_locked_read_is_what_the_rest_of_its_transaction_reads():
    tenant_id, api, queue = tenant_with_queue_q()
    with core.tx() as db:
        assert config.get(db, tenant_id, "queue", queue)["name"] == "Q"
        assert api.patch(f"/api/v1/queues/{queue}", json={"name": "R"}).status_code == 200
        assert config.get(db, tenant_id, "queue", queue, lock=True)["name"] == "R"
        assert config.get(db, tenant_id, "queue", queue)["name"] == "R"


def test_a_config_write_that_read_its_own_change_and_rolled_back_is_never_served():
    tenant_id, _, queue = tenant_with_queue_q()
    with core.read() as db:
        assert config.tenant_settings(db, tenant_id).ring_timeout_sec == 30
    with pytest.raises(RuntimeError), config.write(access.OPERATOR, tenant_id) as db:
        config._store(db, tenant_id, ring_timeout_sec=45)
        config.save(db, queue, config.found(db, tenant_id, "queue", queue).body | {"name": "R"})
        assert config.tenant_settings(db, tenant_id).ring_timeout_sec == 45, "a write reads its own change"
        assert config.get(db, tenant_id, "queue", queue)["name"] == "R"
        raise RuntimeError("rolled back")
    for _ in range(2):
        with core.read() as db:
            assert config.tenant_settings(db, tenant_id).ring_timeout_sec == 30
            assert config.get(db, tenant_id, "queue", queue)["name"] == "Q"


def rename_without_a_new_generation(queue: str, name: str) -> None:
    with core.tx() as db:
        db.execute(update(T.config).where(T.config.c.id == queue).values(body=T.config.c.body.concat({"name": name})))


def test_a_transaction_holding_the_config_reads_postgres_and_not_the_cache():
    tenant_id, _, queue = tenant_with_queue_q()
    with core.read() as db:
        assert config.get(db, tenant_id, "queue", queue)["name"] == "Q"
    rename_without_a_new_generation(queue, "R")
    with core.tx() as db:
        config.hold(db, tenant_id)
        assert config.get(db, tenant_id, "queue", queue)["name"] == "R"


def test_a_hold_reads_a_document_from_postgres_once_until_it_writes_config_itself():
    tenant_id, _, queue = tenant_with_queue_q()
    sent = []

    def count(conn, cursor, statement, *_):
        sent.append(statement)

    with core.tx() as db:
        config.hold(db, tenant_id)
        assert config.get(db, tenant_id, "queue", queue)["name"] == "Q"
        event.listen(core.engine, "before_cursor_execute", count)
        try:
            assert config.get(db, tenant_id, "queue", queue)["name"] == "Q"
        finally:
            event.remove(core.engine, "before_cursor_execute", count)
        config.save(db, queue, config.found(db, tenant_id, "queue", queue).body | {"name": "R"})
        assert config.get(db, tenant_id, "queue", queue)["name"] == "R"
    assert sent == []


def refused(*_, **__):
    raise RedisError("refused")


@pytest.mark.parametrize("job_first", [False, True], ids=["fill_first", "job_first"])
def test_a_fill_of_the_row_before_a_write_whose_bump_failed_is_ignored_once_its_job_runs(monkeypatch, job_first):
    tenant_id, api, queue = tenant_with_queue_q()

    def bump():
        jobs.run_now("config_changed", str(tenant_id), wait=True)

    def load_then_write() -> dict:
        with core.read() as db:
            before = config.found(db, tenant_id, "queue", queue)
        with monkeypatch.context() as m:
            m.setattr(hot, "changed", refused)
            assert api.patch(f"/api/v1/queues/{queue}", json={"name": "R"}).status_code == 200
        if job_first:
            bump()
        return {"id": str(before.id), **before.body}

    bump()
    hot.cached(tenant_id, f"queue:{queue}", load_then_write)
    bump()
    with core.read() as db:
        assert config.get(db, tenant_id, "queue", queue)["name"] == "R"


def test_the_bump_job_runs_again_within_a_second_however_often_redis_refuses_it(monkeypatch):
    tenant_id, _, _ = tenant_with_queue_q()
    monkeypatch.setattr(hot, "changed", refused)
    for _ in range(len(jobs.DEFAULT_BACKOFF) + 2):
        jobs.run_now("config_changed", str(tenant_id), wait=True)
    J = T.jobs
    mine = (J.c.kind == "config_changed", J.c.key == str(tenant_id))
    with core.read() as db:
        job = db.execute(select(J.c.locked_until, (J.c.run_at - func.now()).label("due_in")).where(*mine)).one()
    assert job.locked_until is None and timedelta(0) < job.due_in <= timedelta(seconds=1)


def test_a_bump_job_redis_keeps_refusing_logs_its_first_refusal_and_the_run_that_ends_them(monkeypatch, caplog):
    tenant_id, _, _ = tenant_with_queue_q()
    monkeypatch.setattr(hot, "changed", refused)
    with caplog.at_level(logging.INFO, logger="hs.jobs"):
        for _ in range(3):
            jobs.run_now("config_changed", str(tenant_id), wait=True)
        monkeypatch.setattr(hot, "changed", lambda _: None)
        jobs.run_now("config_changed", str(tenant_id), wait=True)
    logged = [r.getMessage() for r in caplog.records if str(tenant_id) in r.getMessage()]
    assert len(logged) == 2 and "refused" in logged[0], logged


def test_a_fill_redis_refuses_returns_what_it_loaded_after_one_load(monkeypatch):
    loads = []
    monkeypatch.setattr(core.redis, "set", refused)
    assert hot.cached(uuid.uuid4(), "settings", lambda: loads.append(1) or {"ring_timeout_sec": 9}) == (
        '{"ring_timeout_sec": 9}'
    )
    assert loads == [1]


def died(*_):
    raise RuntimeError("the process died before storing the response")


def settle_plivo(tenant_id) -> None:
    J = T.jobs
    mine = (J.c.kind == "plivo", J.c.payload["tenant_id"].astext == str(tenant_id))
    for _ in range(5):
        with core.tx() as db:
            db.execute(update(J).where(*mine).values(run_at=func.now()))
        while job := jobs._claim(*mine, J.c.run_at <= func.now()):
            jobs.work(job)


def first_held(fn):
    """`fn`, whose first call sets `reached` and waits for `release` before it runs."""
    reached, release = Future(), threading.Event()

    def held_first(*args, **kwargs):
        if not reached.done():
            reached.set_result(None)
            release.wait(5)
        return fn(*args, **kwargs)

    return held_first, reached, release


def lost(path):
    raise plivo.Uncertain(f"POST {path}: read timeout")


class Account:
    """A Plivo sub-account's endpoints and applications: made, listed a page at a time, deleted. `made(path)` runs
    once Plivo holds what a POST made, before its answer."""

    def __init__(self, made=lambda path: None):
        self.objects, self.made, self.ids = {}, made, iter(range(1, 100))

    def rest(self, t, method, path, **params):
        kind, rid = path.split("/")[0], path.split("/")[1] if path.count("/") > 1 else ""
        field, key = {"Endpoint": ("alias", "endpoint_id"), "Application": ("app_name", "app_id")}.get(kind, ("", ""))
        if method == "POST" and field and not rid:
            rid = str(next(self.ids))
            made = self.objects[rid] = {key: rid, field: params[field], "username": f"u{rid}"}
            self.made(path)
            return made
        if method == "GET" and field and not rid:
            mine = [o for o in self.objects.values() if key in o]
            return {"objects": mine[params["offset"] : params["offset"] + params["limit"]]}
        if method == "DELETE":
            self.objects.pop(rid, None)
        return {"sip_registered": "true"}


@pytest.mark.parametrize(
    ("path", "body"),
    [
        ("/queues", {"name": "Q"}),
        ("/teams", {"name": "T"}),
        ("/dispositions", {"name": "D"}),
        ("/agents", {"name": "A", "role": "agent"}),
    ],
)
def test_a_create_that_died_before_its_answer_was_stored_answers_that_document_and_never_makes_another(
    path, body, monkeypatch
):
    tenant_id, api = admin()
    with core.tx() as db:
        config.add(db, tenant_id, "status", {"name": "Offline", "availability": False}, "offline")
    monkeypatch.setattr(plivo, "rest", lambda *_, **__: {"endpoint_id": "1", "username": uuid.uuid4().hex})
    headers = {"Authorization": api.headers["Authorization"], "Idempotency-Key": uuid.uuid4().hex}
    api = TestClient(service.app, headers=headers, raise_server_exceptions=False)
    store = access._store
    monkeypatch.setattr(access, "_store", died)
    assert api.post(f"/api/v1{path}", json=body).status_code == 500
    monkeypatch.setattr(access, "_store", store)
    monkeypatch.setattr(access, "TAKEOVER", 0)
    again = api.post(f"/api/v1{path}", json=body)
    assert again.status_code == 201 and api.get(f"/api/v1{path}").json()["data"] == [again.json()]


def operator() -> TestClient:
    headers = {"Authorization": f"Bearer {core.settings.operator_key}"}
    return TestClient(service.app, headers=headers, raise_server_exceptions=False)


def registered() -> uuid.UUID:
    with core.tx() as db:
        values = {"name": "t", "plivo_auth_id": uuid.uuid4().hex, "plivo_token_enc": plivo.encrypt("old")}
        tenant = insert(T.tenant).values(**values, plivo_app_id="a1")
        return db.execute(tenant.returning(T.tenant.c.id)).scalar()


def test_two_registrations_of_one_subaccount_make_one_plivo_application(monkeypatch):
    apps, reached, release = [], Future(), threading.Event()

    def rest(t, method, path, **_):
        if (method, path) != ("POST", "Application/"):
            return {}
        apps.append(t.id)
        if not reached.done():
            reached.set_result(None)
            release.wait(5)
        return {"app_id": str(len(apps))}

    monkeypatch.setattr(plivo, "rest", rest)
    body = {"name": "T", "plivo_auth_id": uuid.uuid4().hex, "plivo_auth_token": "token"}
    waiting = text(
        "SELECT 1 FROM pg_stat_activity WHERE datname = current_database() AND pg_blocking_pids(pid) <> '{}'"
    )
    with ThreadPoolExecutor(2) as pool:
        first = pool.submit(operator().post, "/api/v1/tenants", json=body)
        reached.result(5)
        second = pool.submit(operator().post, "/api/v1/tenants", json=body)
        deadline = time.monotonic() + 5
        while not second.done() and time.monotonic() < deadline:
            with core.read() as db:
                if db.execute(waiting).first():
                    break
            time.sleep(0.02)
        release.set()
        codes = sorted(f.result(10).status_code for f in (first, second))
    assert codes == [201, 409] and len(apps) == 1


def test_a_tenant_delete_whose_plivo_cleanup_fails_retries_it_until_plivo_answers(monkeypatch):
    calls = []

    def rest(t, method, path, **_):
        calls.append((method, path))
        if calls.count((method, path)) == 1:
            raise plivo.Uncertain(f"{method} {path}: 503")
        return {}

    monkeypatch.setattr(plivo, "rest", rest)
    tenant_id = registered()
    with core.tx() as db:
        config.add(db, tenant_id, "agent", {"name": "A", "endpoint_id": "e1", "endpoint_username": "u1"}, "u1")
    assert operator().delete(f"/api/v1/tenants/{tenant_id}").status_code == 204
    settle_plivo(tenant_id)
    assert sorted(calls) == [("DELETE", "Application/a1/")] * 2 + [("DELETE", "Endpoint/e1/")] * 2


def test_a_rename_never_restores_the_token_a_rotation_committed_meanwhile(monkeypatch):
    tenant_id = registered()

    def rotated(tenant, **_):
        with core.tx() as db:
            db.execute(update(T.tenant).where(T.tenant.c.id == tenant_id).values(plivo_token_enc=plivo.encrypt("new")))
        return tenant.plivo_app_id

    monkeypatch.setattr(plivo, "register_app", rotated)
    assert operator().patch(f"/api/v1/tenants/{tenant_id}", json={"name": "Renamed"}).status_code == 200
    with core.read() as db:
        row = db.execute(select(T.tenant).where(T.tenant.c.id == tenant_id)).one()
    assert (row.name, plivo.decrypt(row.plivo_token_enc)) == ("Renamed", "new")


def test_a_registration_whose_application_answer_was_lost_leaves_no_application(monkeypatch):
    acct = Account(lost)
    monkeypatch.setattr(plivo, "rest", acct.rest)
    body = {"name": "T", "plivo_auth_id": uuid.uuid4().hex, "plivo_auth_token": "token"}
    assert operator().post("/api/v1/tenants", json=body).status_code == 503
    assert acct.objects == {}


def test_a_send_queued_before_or_after_its_tenant_is_deleted_sends_nothing_and_the_cleanup_still_runs(monkeypatch):
    from tests.unit.test_access import replying

    calls = []
    api, path, tenant_id = replying(monkeypatch, lambda t, m, p, **_: calls.append((m, p)) or {"message_uuid": ["m"]})
    with core.tx() as db:
        db.execute(update(T.tenant).where(T.tenant.c.id == tenant_id).values(plivo_app_id="a1"))
    monkeypatch.setattr("hs.text.verdict", lambda key: None)
    assert api.post(path, json={"text": "queued"}).status_code == 201
    monkeypatch.undo()
    monkeypatch.setattr(plivo, "rest", lambda t, m, p, **_: calls.append((m, p)) or {"message_uuid": ["m"]})
    J = T.jobs
    with core.read() as db:
        queued = db.execute(
            select(J.c.payload).where(J.c.kind == "plivo", J.c.key.startswith(f"send:{tenant_id}:"))
        ).scalar()
    assert operator().delete(f"/api/v1/tenants/{tenant_id}").status_code == 204
    with core.tx() as db:
        jobs.schedule(db, "plivo", queued, key=f"send:{tenant_id}:late", delay=10)
    settle_plivo(tenant_id)
    assert ("POST", "Message/") not in calls and ("DELETE", "Application/a1/") in calls


def test_a_send_after_a_delete_whose_cache_bump_was_lost_sends_nothing(monkeypatch):
    from tests.unit.test_access import replying

    calls = []
    api, path, tenant_id = replying(monkeypatch, lambda t, m, p, **_: calls.append((m, p)) or {"message_uuid": ["m"]})
    monkeypatch.setattr("hs.text.verdict", lambda key: None)
    assert api.post(path, json={"text": "queued"}).status_code == 201
    J = T.jobs
    with core.read() as db:
        queued = db.execute(
            select(J.c.payload).where(J.c.kind == "plivo", J.c.key.startswith(f"send:{tenant_id}:"))
        ).scalar()
    plivo.account(tenant_id)
    monkeypatch.setattr(hot, "changed", lambda tenant_id: None)
    assert operator().delete(f"/api/v1/tenants/{tenant_id}").status_code == 204
    with core.tx() as db:
        jobs.schedule(db, "plivo", queued, key=f"send:{tenant_id}:late", delay=10)
    settle_plivo(tenant_id)
    assert ("POST", "Message/") not in calls


def test_every_zone_a_browser_lists_is_accepted_and_a_bad_range_names_the_larger_bound():
    from pydantic import TypeAdapter, ValidationError

    from hs.config.models import SkillIn, Zone

    for name in ("Asia/Calcutta", "Asia/Kolkata", "Europe/Kiev", "America/Buenos_Aires", "Asia/Saigon"):
        assert TypeAdapter(Zone).validate_python(name) == name
    with pytest.raises(ValidationError, match="max_level is below min_level"):
        SkillIn(name="x", min_level=8, max_level=3)
