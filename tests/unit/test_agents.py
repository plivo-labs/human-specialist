"""Presence on the migrated database: the beat authenticates itself, a session's second beat signs the agent in,
and a silent desk is swept Offline while a beating one stays on."""

import hashlib
import threading
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor, wait
from datetime import timedelta
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import event, func, insert, select, text, update

from hs import access, agents, core, hot, jobs, plivo
from hs import app as service
from hs import conversations as cv
from hs.config import models as m
from hs.config import provision
from hs.core import ApiError, T
from hs.voice import outbound
from tests.unit.test_access import claimed_at, dying
from tests.unit.test_config import Account, died, first_held, held, lost, operator, settle_plivo, waiter

client = TestClient(service.app)


class Desk:
    """A tenant with its sign-in and sign-out statuses, and one Offline agent signed in to a session."""

    def __init__(self):
        with core.tx() as db:
            sealed = plivo.encrypt("token")
            tenant = insert(T.tenant).values(name="t", plivo_auth_id=uuid.uuid4().hex, plivo_token_enc=sealed)
            self.tenant = db.execute(tenant.returning(T.tenant.c.id)).scalar()
            hot.rebuilt(self.tenant)
            self.online = self.doc(db, "status", {"name": "Available", "availability": True}, "online")
            self.offline = self.doc(db, "status", {"name": "Offline", "availability": False}, "offline")
            self.agent = self.doc(
                db, "agent", {"name": "A", "role": "agent", "teams": [], "endpoint_id": "e1", "endpoint_username": "u1"}
            )
            settings = m.Stored(rona={"status_id": str(self.offline)}).model_dump(mode="json")
            db.execute(update(T.tenant).where(T.tenant.c.id == self.tenant).values(settings=settings))
            db.execute(insert(T.presence).values(agent_id=self.agent, tenant_id=self.tenant, status_id=self.offline))
            self.tokens = access.mint(db, self.tenant, self.agent)

    def doc(self, db, kind: str, body: dict, key: str | None = None) -> uuid.UUID:
        row = insert(T.config).values(tenant_id=self.tenant, kind=kind, key=key, body=body)
        return db.execute(row.returning(T.config.c.id)).scalar()

    def presence(self):
        with core.read() as db:
            return db.execute(select(T.presence).where(T.presence.c.agent_id == self.agent)).one()

    def token(self, *, revoked: bool = False, expired: bool = False) -> str:
        with core.tx() as db:
            token = access.mint(db, self.tenant, self.agent).agent_token
            dead = {"revoked_at": func.now()} if revoked else {"expires_at": func.now()} if expired else {}
            if dead:
                db.execute(update(T.credential).where(T.credential.c.sha256 == access.digest(token)).values(**dead))
        return token


def beat(token: str) -> int:
    return client.post("/api/v1/me/heartbeat", headers={"Authorization": f"Bearer {token}"}).status_code


def test_only_a_live_agent_token_beats_and_only_for_its_own_agent():
    desk, other = Desk(), Desk()
    with core.tx() as db:
        key = access.new_key(db, desk.tenant)["api_key"]
    refused = [desk.tokens.stream_token, key, desk.token(revoked=True), desk.token(expired=True), "", "hs_at_unknown"]
    assert [beat(t) for t in refused] == [401] * len(refused)
    assert beat(other.tokens.agent_token) == 204
    assert beat(desk.tokens.agent_token) == 204 and desk.presence().status_id == desk.offline
    assert beat(desk.tokens.agent_token) == 204 and desk.presence().status_id == desk.online
    assert other.presence().status_id == other.offline


def test_a_beat_resolves_any_credential_but_an_agent_token_with_no_connection_held(monkeypatch):
    desk, resolving, checkouts = Desk(), [], []
    with core.tx() as db:
        key = access.new_key(db, desk.tenant)["api_key"]
    before = core.engine.pool.checkedout()

    def held(*_) -> None:
        checkouts.append(core.engine.pool.checkedout() - before)

    monkeypatch.setattr(access, "RESOLVERS", [lambda request: resolving.append(core.engine.pool.checkedout() - before)])
    event.listen(core.engine.pool, "checkout", held)
    try:
        assert [beat(t) for t in ("a-plugin-token", key, desk.tokens.stream_token)] == [401] * 3
    finally:
        event.remove(core.engine.pool, "checkout", held)
    assert resolving == [0] and max(checkouts) == 1


def test_the_second_beat_of_a_session_signs_the_agent_in():
    desk = Desk()
    assert beat(desk.tokens.agent_token) == 204 and desk.presence().status_id == desk.offline
    assert beat(desk.tokens.agent_token) == 204 and desk.presence().status_id == desk.online
    with core.read() as db:
        changed = db.execute(select(T.events.c.data).where(T.events.c.agent_id == str(desk.agent))).scalar()
    assert (changed["reason"], changed["to_status"]["name"]) == ("heartbeat", "Available")


def test_a_silent_desk_is_swept_offline_and_a_beating_one_stays_on():
    silent, beating = Desk(), Desk()
    for desk in (silent, beating):
        hot.rebuilt(desk.tenant)
        assert [beat(desk.tokens.agent_token) for _ in range(2)] == [204, 204]
    core.redis.delete(hot.HB % (silent.tenant, silent.agent))
    with core.tx() as db:
        jobs.schedule(db, "sweep", {}, key="sweep")
    jobs.run_now("sweep", "sweep", wait=True)
    J = T.jobs
    with core.read() as db:
        swept = set(db.execute(select(J.c.key).where(J.c.kind == "agent_offline")).scalars())
    assert str(silent.agent) in swept and str(beating.agent) not in swept
    jobs.run_now("agent_offline", str(silent.agent), wait=True)
    assert (silent.presence().status_id, beating.presence().status_id) == (silent.offline, beating.online)
    assert beat(silent.tokens.agent_token) == 204 and silent.presence().status_id == silent.offline


def test_a_process_killed_between_the_sign_in_beat_and_its_commit_leaves_the_next_beat_to_sign_in():
    desk = Desk()
    mine = {"Authorization": f"Bearer {desk.tokens.agent_token}"}
    hot.rebuilt(desk.tenant)
    assert beat(desk.tokens.agent_token) == 204
    hot.beat(desk.tenant, desk.agent)  # the second beat's process dies before the sign-in commits
    assert desk.presence().status_id == desk.offline
    assert beat(desk.tokens.agent_token) == 204 and desk.presence().status_id == desk.online
    assert client.put("/api/v1/me/status", json={"status_id": str(desk.offline)}, headers=mine).status_code == 200
    assert [beat(desk.tokens.agent_token) for _ in range(3)] == [204] * 3
    assert desk.presence().status_id == desk.offline, "an agent who signs out by hand stays out while the desk beats"


def test_a_session_closed_while_its_sign_in_rereads_the_credential_ends_offline():
    """Closing the sessions starts right after the sign-in has read the beat's credential again: it waits for the
    sign-in on the presence lock, then signs the agent out. Without either side's lock it would revoke first and the
    sign-in would still make the agent Available."""
    desk, reads, closing, pool = Desk(), [], [], ThreadPoolExecutor(1)
    api, sha = sessions_admin(desk), access.digest(desk.tokens.agent_token)
    assert beat(desk.tokens.agent_token) == 204

    def reread(conn, cursor, statement, parameters, *_):
        if "FROM credential WHERE sha256" in statement and sha in str(parameters):
            reads.append(statement)
        if len(reads) != 2 or closing:
            return
        closing.append(pool.submit(api.delete, f"/api/v1/agents/{desk.agent}/sessions"))
        waiter(cursor.connection.get_backend_pid())

    event.listen(core.engine, "after_cursor_execute", reread)
    try:
        assert beat(desk.tokens.agent_token) == 204
    finally:
        event.remove(core.engine, "after_cursor_execute", reread)
    assert closing and closing[0].result(10).status_code == 204
    assert desk.presence().status_id == desk.offline


def test_the_rebuild_after_a_redis_loss_gives_each_signed_in_desk_40_s_to_beat():
    desk = Desk()
    assert [beat(desk.tokens.agent_token) for _ in range(2)] == [204, 204]
    core.redis.delete(*(k % (desk.tenant, desk.agent) for k in (hot.HB, hot.BEATS)), hot.EPOCH % desk.tenant)
    with core.tx() as db:
        jobs.schedule(db, "sweep", {}, key="sweep")
    began = time.monotonic()
    jobs.run_now("sweep", "sweep", wait=True)
    left = core.redis.pttl(hot.HB % (desk.tenant, desk.agent))
    # The rebuild set it after `began`, so it lost at most the time since then, plus a few ms of rounding.
    assert 40_000 - (time.monotonic() - began) * 1000 - 5 < left <= 40_000


def test_a_beat_between_the_rebuilds_read_and_its_write_leaves_a_break_agent_on_break(monkeypatch):
    desk, beats, pipeline = Desk(), [], core.redis.pipeline
    with core.tx() as db:
        brk = desk.doc(db, "status", {"name": "Break", "availability": False})
    assert [beat(desk.tokens.agent_token) for _ in range(2)] == [204, 204]
    with core.tx() as db:
        agents.set_status(db, desk.tenant, desk.agent, brk, "manual")
    core.redis.delete(*(k % (desk.tenant, desk.agent) for k in (hot.HB, hot.BEATS)), hot.EPOCH % desk.tenant)

    def read_then_beat(*args, **kwargs):
        p = pipeline(*args, **kwargs)
        execute = p.execute

        def then():
            found = execute()
            if not beats:
                beats.append(None)
                beats[0] = beat(desk.tokens.agent_token)
            return found

        p.execute = then
        return p

    monkeypatch.setattr(core.redis, "pipeline", read_then_beat)
    with core.tx() as db:
        jobs.schedule(db, "sweep", {}, key="sweep")
    jobs.run_now("sweep", "sweep", wait=True)
    assert beats == [204], "a beat landed between the sweep's read and its rebuild"
    assert beat(desk.tokens.agent_token) == 204
    assert desk.presence().status_id == brk, "a count a beat began during the rebuild never signs a Break agent in"


def offered(desk: Desk, ready: Future | None = None, release: threading.Event | None = None) -> None:
    """An offer of a new call to the desk's agent; with `ready`, its transaction stays open until `release`."""
    with core.tx() as db:
        call = {"channel": "call", "direction": "inbound", "customer": "+15550001", "ours": "+15550002"}
        conv = cv.open(db, desk.tenant, **call, call_uuid=uuid.uuid4().hex)
        cv.offer(db, cv.lock(db, conv.id), desk.agent)
        if ready:
            ready.set_result(db.exec_driver_sql("SELECT pg_backend_pid()").scalar())
            release.wait(10)


def test_an_agent_delete_and_an_offer_to_that_agent_never_both_commit(monkeypatch):
    calls = []
    monkeypatch.setattr(plivo, "rest", lambda t, *call, **_: calls.append(call) or {})
    first, then = Desk(), Desk()
    with core.tx() as db:
        keys = [access.new_key(db, d.tenant, ["admin"])["api_key"] for d in (first, then)]
    api = [TestClient(service.app, headers={"Authorization": f"Bearer {k}"}) for k in keys]
    with ThreadPoolExecutor(3) as pool:
        ready, release = Future(), threading.Event()
        try:
            offering = pool.submit(offered, first, ready, release)
            holder = ready.result(5)
            deleting = pool.submit(api[0].delete, f"/api/v1/agents/{first.agent}")
            assert waiter(holder), "the delete waits for the open offer"
        finally:
            release.set()
        offering.result()
        assert deleting.result().status_code == 409

        ready, release = Future(), threading.Event()
        # The delete revokes the agent's tokens last: holding one pauses it with the presence row taken.
        tokens = select(T.credential.c.id).where(T.credential.c.agent_id == then.agent).with_for_update()
        try:
            pool.submit(held, tokens, ready, release)
            holder = ready.result(5)
            deleting = pool.submit(api[1].delete, f"/api/v1/agents/{then.agent}")
            deleter = waiter(holder)
            offering = pool.submit(offered, then)
            assert deleter and waiter(deleter), "the offer waits for the delete"
        finally:
            release.set()
        assert deleting.result().status_code == 204
        with pytest.raises(ApiError) as refused:
            offering.result()
        assert refused.value.code == "agent_not_found"
    settle_plivo(then.tenant)
    with core.read() as db:
        left = [(a.agent_id, a.state) for d in (first, then) for a in cv.desk(db, d.tenant, d.agent, states=None)]
    assert left == [(first.agent, "offered")] and calls == [("DELETE", "Endpoint/e1/")]


def sessions_admin(desk: Desk) -> TestClient:
    with core.tx() as db:
        key = access.new_key(db, desk.tenant, ["admin", "sessions"])["api_key"]
    return TestClient(service.app, headers={"Authorization": f"Bearer {key}"}, raise_server_exceptions=False)


def test_closing_sessions_and_deleting_the_agent_both_take_presence_before_tokens_so_neither_deadlocks(monkeypatch):
    monkeypatch.setattr(plivo, "rest", lambda *_, **__: {})
    desk = Desk()
    api, ready, release = sessions_admin(desk), Future(), threading.Event()
    with core.tx() as db:
        db.execute(update(T.presence).where(T.presence.c.agent_id == desk.agent).values(status_id=desk.online))
    offline = select(T.config.c.id).where(T.config.c.id == desk.offline).with_for_update()
    with ThreadPoolExecutor(3) as pool:
        try:
            # Closing sets Offline under a share lock on that status: holding the row pauses it there.
            pool.submit(held, offline, ready, release)
            holder = ready.result(5)
            closing = pool.submit(api.delete, f"/api/v1/agents/{desk.agent}/sessions")
            assert waiter(holder)
            deleting = pool.submit(api.delete, f"/api/v1/agents/{desk.agent}")
            wait([deleting], timeout=2)
        finally:
            release.set()
    assert (closing.result().status_code, deleting.result().status_code) == (204, 204)


def test_a_sign_out_into_the_status_being_deleted_and_the_delete_both_commit():
    """The agent is in the sign-out status. Its delete holds the tenant's presence rows while it waits to repoint the
    sign-out setting; the sign-out waits for it and then commits, and the agent is on a status that exists."""
    desk, C, P = Desk(), T.config, T.presence
    api = sessions_admin(desk)
    with core.tx() as db:
        db.execute(update(C).where(C.c.id == desk.online).values(body=C.c.body.concat({"default": True})))
        floor = desk.doc(db, "status", {"name": "Floor", "availability": True})
        db.execute(update(P).where(P.c.agent_id == desk.agent).values(status_id=floor))
        sign_out = T.tenant.c.settings.concat({"sign_out": {"status_id": str(floor)}})
        db.execute(update(T.tenant).where(T.tenant.c.id == desk.tenant).values(settings=sign_out))
    tenant = select(T.tenant.c.id).where(T.tenant.c.id == desk.tenant).with_for_update(key_share=True)
    ready, release = Future(), threading.Event()
    blocked = text("SELECT 1 FROM pg_stat_activity WHERE :pid = ANY(pg_blocking_pids(pid))")
    with ThreadPoolExecutor(3) as pool:
        try:
            pool.submit(held, tenant, ready, release)
            deleting = pool.submit(api.delete, f"/api/v1/agent-statuses/{floor}")
            deleter = waiter(ready.result(5))
            assert deleter, "the delete holds the presence rows and waits for the settings"
            closing = pool.submit(api.delete, f"/api/v1/agents/{desk.agent}/sessions")
            deadline = time.monotonic() + 5
            with core.read() as db:
                while not (closing.done() or db.execute(blocked, {"pid": deleter}).first()):
                    assert time.monotonic() < deadline, "the sign-out neither finished nor waited for the delete"
                    time.sleep(0.02)
        finally:
            release.set()
    assert (closing.result().status_code, deleting.result().status_code) == (204, 204)
    assert desk.presence().status_id in (desk.offline, desk.online)


def test_a_session_opened_while_its_agent_is_deleted_gets_no_tokens(monkeypatch):
    monkeypatch.setattr(plivo, "rest", lambda *_, **__: {"token": "sdk"})
    desk = Desk()
    api, ready, release = sessions_admin(desk), Future(), threading.Event()
    tokens = select(T.credential.c.id).where(T.credential.c.agent_id == desk.agent).with_for_update()
    with ThreadPoolExecutor(3) as pool:
        try:
            pool.submit(held, tokens, ready, release)
            holder = ready.result(5)
            deleting = pool.submit(api.delete, f"/api/v1/agents/{desk.agent}")
            deleter = waiter(holder)
            opening = pool.submit(api.post, f"/api/v1/agents/{desk.agent}/sessions")
            assert deleter and waiter(deleter), "the session waits for the delete"
        finally:
            release.set()
    assert (deleting.result().status_code, opening.result().status_code) == (204, 404)


def test_only_a_live_session_holding_a_plivo_token_rings_the_agent_at_its_sdk_login(monkeypatch):
    monkeypatch.setattr(plivo, "rest", lambda *_, **__: {"token": "sdk"})
    desk = Desk()
    api, agent = sessions_admin(desk), {"id": str(desk.agent), "endpoint_username": "u1"}
    with core.read() as db:
        auth_id = db.execute(select(T.tenant.c.plivo_auth_id).where(T.tenant.c.id == desk.tenant)).scalar()

    def rings() -> str:
        with core.read() as db:
            return agents.ring_address(db, desk.tenant, agent)

    assert rings() == "sip:u1@phone.plivo.com", "a session a plugin mints through access.mint rings the endpoint"
    assert api.post(f"/api/v1/agents/{desk.agent}/sessions").status_code == 201
    assert rings() == f"sip:u1_{auth_id}@phone.plivo.com"
    assert api.delete(f"/api/v1/agents/{desk.agent}/sessions").status_code == 204
    assert rings() == "sip:u1@phone.plivo.com", "a closed session rings the endpoint again"


def test_a_dial_rings_the_sdk_login_it_chose_though_the_session_ends_before_the_dial_is_placed(monkeypatch):
    desk, dialed, login = Desk(), [], agents.sdk_login
    monkeypatch.setattr(plivo, "rest", lambda *_, **__: {"token": "sdk", "sip_registered": "false"})
    monkeypatch.setattr(plivo, "command", lambda db, op, key, payload, **_: dialed.append(payload["params"]["to"]))
    with core.tx() as db:
        desk.doc(db, "number", {"number": "+14155550100"}, "+14155550100")
        db.execute(update(T.presence).where(T.presence.c.agent_id == desk.agent).values(status_id=desk.online))
        key = access.new_key(db, desk.tenant, ["sessions", "calls"])["api_key"]
        auth_id = db.execute(select(T.tenant.c.plivo_auth_id).where(T.tenant.c.id == desk.tenant)).scalar()
    api = TestClient(service.app, headers={"Authorization": f"Bearer {key}"}, raise_server_exceptions=False)
    assert api.post(f"/api/v1/agents/{desk.agent}/sessions").status_code == 201

    def ending(db, tenant_id, agent_id):
        found = login(db, tenant_id, agent_id)
        with core.tx() as other:
            other.execute(update(T.credential).where(T.credential.c.agent_id == agent_id).values(expires_at=func.now()))
        return found

    monkeypatch.setattr(agents, "sdk_login", ending)
    body = {"agent_id": str(desk.agent), "to": "+14155550111", "from": "+14155550100"}
    assert api.post("/api/v1/calls", json=body).status_code == 201
    assert dialed == [f"sip:u1_{auth_id}@phone.plivo.com"], "an endpoint Plivo shows unregistered is never dialed"
    with core.read() as db:
        assert login(db, desk.tenant, desk.agent) is None, "the session ended while the dial was placed"


def test_going_idle_waits_for_an_offer_in_flight_and_then_keeps_the_agent_available():
    desk = Desk()
    with core.tx() as db:
        idle = desk.doc(db, "status", {"name": "Idle", "availability": False})
        stored = m.Stored(rona={"status_id": str(desk.offline)}, idle={"enabled": True, "status_id": str(idle)})
        db.execute(update(T.tenant).where(T.tenant.c.id == desk.tenant).values(settings=stored.model_dump(mode="json")))
        db.execute(update(T.presence).where(T.presence.c.agent_id == desk.agent).values(status_id=desk.online))
        token = access.mint(db, desk.tenant, desk.agent).agent_token
    with ThreadPoolExecutor(2) as pool:
        ready, release = Future(), threading.Event()
        try:
            offering = pool.submit(offered, desk, ready, release)
            holder = ready.result(5)
            going = pool.submit(client.post, "/api/v1/me/idle", headers={"Authorization": f"Bearer {token}"})
            assert waiter(holder), "going idle waits for the offer before it reads the desk"
        finally:
            release.set()
        offering.result()
        assert going.result().status_code == 200
    assert desk.presence().status_id == desk.online


def test_an_available_status_restarts_the_first_available_time_only_12_hours_past_it():
    desk, P = Desk(), T.presence
    first = select(P.c.first_available_at, func.now()).where(P.c.agent_id == desk.agent)
    with core.tx() as db:
        brk = desk.doc(db, "status", {"name": "Break", "availability": False})
        agents.set_status(db, desk.tenant, desk.agent, brk, "manual")
        assert db.execute(first).first()[0] is None
        for hours, moved in ((11, False), (13, True)):
            earlier = func.now() - timedelta(hours=hours)
            db.execute(update(P).where(P.c.agent_id == desk.agent).values(first_available_at=earlier))
            agents.set_status(db, desk.tenant, desk.agent, desk.online, "manual")
            at, now = db.execute(first).first()
            assert (at == now) is moved
            agents.set_status(db, desk.tenant, desk.agent, brk, "manual")


def test_an_agent_create_refused_after_its_endpoint_was_made_keeps_its_error_and_retries_the_endpoint_delete(
    monkeypatch,
):
    calls = []

    def rest(t, method, path, **_):
        calls.append((method, path))
        if method == "POST":
            return {"endpoint_id": f"e{len(calls)}", "username": f"u{len(calls)}"}
        if calls.count((method, path)) == 1:
            raise plivo.Uncertain(f"{method} {path}: 503")
        return {}

    monkeypatch.setattr(plivo, "rest", rest)
    desk = Desk()
    with core.tx() as db:
        key = access.new_key(db, desk.tenant, ["admin"])["api_key"]
    api = TestClient(service.app, headers={"Authorization": f"Bearer {key}"}, raise_server_exceptions=False)
    body = {"name": "B", "role": "agent", "external_id": "x1"}
    assert api.post("/api/v1/agents", json=body).status_code == 201
    again = api.post("/api/v1/agents", json=body)
    assert (again.status_code, again.json()["error"]["code"]) == (409, "agent_exists")
    settle_plivo(desk.tenant)
    assert calls.count(("DELETE", "Endpoint/e2/")) == 2


def dialer(monkeypatch, refusal: plivo.Refused | None = None, own: bool = False) -> tuple[TestClient, dict, list]:
    """A key's client with an Idempotency-Key, a body dialing an Available agent's registered endpoint, and the Make
    Call requests Plivo got, each refused with `refusal` when there is one. With `own`, the agent dials itself."""
    desk, dials = Desk(), []

    def rest(t, method, path, **_):
        if path != "Call/":
            return {"sip_registered": "true"}
        dials.append(path)
        if refusal:
            raise refusal
        return {"request_uuid": uuid.uuid4().hex}

    monkeypatch.setattr(plivo, "rest", rest)
    with core.tx() as db:
        desk.doc(db, "number", {"number": "+14155550100"}, "+14155550100")
        db.execute(update(T.presence).where(T.presence.c.agent_id == desk.agent).values(status_id=desk.online))
        key = access.new_key(db, desk.tenant, ["calls"])["api_key"]
        if own:
            key, direct = desk.tokens.agent_token, T.tenant.c.settings.concat({"agent_direct_dial": True})
            db.execute(update(T.tenant).where(T.tenant.c.id == desk.tenant).values(settings=direct))
    headers = {"Authorization": f"Bearer {key}", "Idempotency-Key": uuid.uuid4().hex}
    body = {"agent_id": str(desk.agent), "to": "+14155550111", "from": "+14155550100"}
    return TestClient(service.app, headers=headers, raise_server_exceptions=False), body, dials


def test_a_dial_whose_claim_was_taken_over_dials_once(monkeypatch):
    api, body, dials = dialer(monkeypatch)
    held_first, reached, release = first_held(outbound._check)
    monkeypatch.setattr(outbound, "_check", held_first)
    with ThreadPoolExecutor(1) as pool:
        first = pool.submit(api.post, "/api/v1/calls", json=body)
        reached.result(5)
        token = claimed_at(api.headers["Idempotency-Key"])
        monkeypatch.setattr(access, "TAKEOVER", 0)
        taken = api.post("/api/v1/calls", json=body)
        assert claimed_at(api.headers["Idempotency-Key"]) > token, "the takeover renewed the claim"
        with core.tx() as db:
            won = cv.rows(db, cv.lock(db, uuid.UUID(taken.json()["conversation_id"])).id)[0]
            cv.move(db, won, "withdraw", reason="cancelled")
        release.set()
        late = first.result(10)
    assert (taken.status_code, late.status_code, dials) == (201, 409, ["Call/"])
    assert late.json()["error"]["code"] == "idempotency_in_progress", "the fence, not the agent's call slot, stops it"
    assert api.post("/api/v1/calls", json=body).json() == taken.json() and dials == ["Call/"]


def test_an_agents_own_dial_whose_answer_was_lost_replays_it_before_its_leg_answers(monkeypatch):
    api, body, dials = dialer(monkeypatch, own=True)
    monkeypatch.setattr(access, "_store", died)
    assert api.post("/api/v1/calls", json=body).status_code == 500
    monkeypatch.setattr(access, "TAKEOVER", 0)
    again = api.post("/api/v1/calls", json=body)
    assert (again.status_code, again.json()["status"], dials) == (201, "dialing_agent", ["Call/"]), again.text


REFUSALS = [plivo.Refused(503, "plivo_unavailable", "Plivo did not answer"), plivo.Refused(422, "plivo_refused", "no")]


@pytest.mark.parametrize("refusal", REFUSALS, ids=["unreached", "refused"])
def test_a_refused_dial_retried_with_its_key_replays_the_refusal_and_never_dials_again(monkeypatch, refusal):
    api, body, dials = dialer(monkeypatch, refusal)
    first, again = (api.post("/api/v1/calls", json=body) for _ in range(2))
    assert (first.status_code, dials) == (refusal.status, ["Call/"])
    assert (again.status_code, again.json(), dials) == (first.status_code, first.json(), ["Call/"])


@pytest.mark.parametrize("refusal", REFUSALS, ids=["unreached", "refused"])
def test_a_refused_dial_whose_answer_was_lost_replays_the_refusal(monkeypatch, refusal):
    api, body, dials = dialer(monkeypatch, refusal)
    monkeypatch.setattr(access, "_store", died)
    assert api.post("/api/v1/calls", json=body).status_code == 500
    monkeypatch.setattr(access, "TAKEOVER", 0)
    again = api.post("/api/v1/calls", json=body)
    assert (again.status_code, dials) == (refusal.status, ["Call/"])
    assert again.json()["error"]["code"] == refusal.code


@pytest.mark.parametrize("refusal", REFUSALS, ids=["unreached", "refused"])
def test_a_refused_dial_a_worker_ran_after_its_request_died_replays_the_refusal(monkeypatch, refusal):
    api, body, dials = dialer(monkeypatch, refusal)
    keys = []
    monkeypatch.setattr(plivo, "verdict", dying(keys))
    assert api.post("/api/v1/calls", json=body).status_code == 500
    jobs.run_now("plivo", keys[0], wait=True)
    monkeypatch.setattr(access, "TAKEOVER", 0)
    again = api.post("/api/v1/calls", json=body)
    assert (again.status_code, dials) == (refusal.status, ["Call/"])
    assert again.json()["error"]["code"] == refusal.code


def creating(monkeypatch, acct: Account) -> tuple[TestClient, uuid.UUID]:
    """An admin key's client with an Idempotency-Key, and its tenant, whose sub-account is `acct`."""
    monkeypatch.setattr(plivo, "rest", acct.rest)
    desk = Desk()
    with core.tx() as db:
        key = access.new_key(db, desk.tenant, ["admin"])["api_key"]
    headers = {"Authorization": f"Bearer {key}", "Idempotency-Key": uuid.uuid4().hex}
    return TestClient(service.app, headers=headers, raise_server_exceptions=False), desk.tenant


AGENT = {"name": "B", "role": "agent"}


@pytest.mark.parametrize("made", [True, False], ids=["made", "making"])
def test_a_takeover_while_the_first_attempt_may_still_make_its_endpoint_makes_none(monkeypatch, made):
    """The first attempt stalls once Plivo made its endpoint, or before Plivo makes it, and dies when it resumes,
    before any cleanup of its own: its intent alone deletes what it made."""
    stall, reached, release = first_held(lambda *_: None)
    acct = Account(stall if made else lambda path: None)
    api, tenant = creating(monkeypatch, acct)

    def stalling(t, method, path, **params):
        if method == "POST":
            stall(path)
        return acct.rest(t, method, path, **params)

    monkeypatch.setattr(plivo, "rest", acct.rest if made else stalling)
    monkeypatch.setattr(provision, "unmade", lambda *_: None)
    with ThreadPoolExecutor(1) as pool:
        first = pool.submit(api.post, "/api/v1/agents", json=AGENT)
        reached.result(5)
        monkeypatch.setattr(access, "TAKEOVER", 0)
        taken = api.post("/api/v1/agents", json=AGENT)
        release.set()
        first.result(10)
    assert taken.status_code == 409 and taken.json()["error"]["code"] == "idempotency_in_progress"
    settle_plivo(tenant)
    assert acct.objects == {}
    again = api.post("/api/v1/agents", json=AGENT)
    assert again.status_code == 201 and list(acct.objects) == [again.json()["endpoint_id"]]


def test_two_attempts_that_overlap_before_either_commits_its_intent_post_one_endpoint(monkeypatch):
    acct, posts = Account(), []
    api, _ = creating(monkeypatch, acct)
    intent, writing, written = first_held(lambda: None)
    post, posting, posted = first_held(lambda: None)

    def stalled(conn, cursor, statement, parameters, *_):
        if statement.startswith("INSERT INTO hs.jobs") and "unmade" in str(parameters):
            intent()

    def rest(t, method, path, **params):
        if method == "POST":
            posts.append(path)
            post()
        return acct.rest(t, method, path, **params)

    monkeypatch.setattr(plivo, "rest", rest)
    event.listen(core.engine, "before_cursor_execute", stalled)
    try:
        with ThreadPoolExecutor(2) as pool:
            first = pool.submit(api.post, "/api/v1/agents", json=AGENT)
            writing.result(5)
            monkeypatch.setattr(access, "TAKEOVER", 0)
            taken = pool.submit(api.post, "/api/v1/agents", json=AGENT)
            posting.result(5)
            written.set()
            late = first.result(10)
            posted.set()
            taken = taken.result(10)
    finally:
        event.remove(core.engine, "before_cursor_execute", stalled)
    assert (late.status_code, taken.status_code, posts) == (409, 201, ["Endpoint/"])
    assert late.json()["error"]["code"] == "idempotency_in_progress"
    assert list(acct.objects) == [taken.json()["endpoint_id"]]


def test_a_retry_while_an_earlier_attempts_intent_cannot_run_answers_409_and_makes_nothing(monkeypatch):
    acct = Account(lost)
    api, _ = creating(monkeypatch, acct)
    monkeypatch.setattr(provision, "unmade", lambda *_: None)
    assert api.post("/api/v1/agents", json=AGENT).status_code == 503, "Plivo made it, the answer was lost, it died"
    acct.made = lambda path: None

    def listing_down(t, method, path, **params):
        if method == "GET":
            raise plivo.Refused(503, "plivo_unavailable", "Plivo did not answer")
        return acct.rest(t, method, path, **params)

    monkeypatch.setattr(plivo, "rest", listing_down)
    again = api.post("/api/v1/agents", json=AGENT)
    assert (again.status_code, len(acct.objects)) == (409, 1)
    assert again.json()["error"]["code"] == "idempotency_in_progress"


def test_an_agent_create_that_resumes_after_its_tenant_was_deleted_commits_nothing_and_leaves_no_endpoint(monkeypatch):
    stall, reached, release = first_held(lambda *_: None)
    acct = Account(stall)
    api, tenant = creating(monkeypatch, acct)
    with ThreadPoolExecutor(1) as pool:
        first = pool.submit(api.post, "/api/v1/agents", json=AGENT)
        reached.result(5)
        assert operator().delete(f"/api/v1/tenants/{tenant}").status_code == 204
        release.set()
        late = first.result(10)
    assert (late.status_code, acct.objects) == (404, {})


def test_an_agent_create_whose_endpoint_answer_was_lost_leaves_no_endpoint(monkeypatch):
    acct = Account(lost)
    assert creating(monkeypatch, acct)[0].post("/api/v1/agents", json=AGENT).status_code == 503
    assert acct.objects == {}


def test_sign_in_tries_count_before_the_check_and_a_right_password_gives_the_address_its_try_back():
    email, ip, gone = (f"{kind}:{uuid.uuid4().hex}" for kind in ("email", "ip", "ip"))
    limits = {email: 2, ip: 3}
    assert [hot.sign_in_tried(limits, 60) for _ in range(2)] == [0, 0]
    assert 0 < hot.sign_in_tried(limits, 60) <= 60, "the email's third try is over its limit"
    hot.signed_in(email, ip)
    assert hot.sign_in_tried(limits, 60) == 0, "the email starts over and the address got a try back"
    assert hot.sign_in_tried(limits, 60) > 0, "the address's fourth try counted"
    hot.signed_in(email, gone)
    assert not core.redis.exists(hot.SIGNIN % gone), "a try given back to an ended window leaves no count"


def test_a_sign_in_count_left_without_its_window_takes_one_again_and_never_locks_for_good():
    tag = f"email:{uuid.uuid4().hex}"
    core.redis.set(hot.SIGNIN % tag, 10)
    assert 30 < hot.sign_in_tried({tag: 10}, 60) <= 60, "the count over its limit waits out a new window"
    assert 30 < core.redis.ttl(hot.SIGNIN % tag) <= 60


def password(admin: TestClient, agent, value: str) -> None:
    assert admin.post(f"/api/v1/agents/{agent}/password", json={"password": value}).status_code == 200


def whole(desk: Desk, **fields) -> None:
    """The desk's agent with every field GET /agents/{id} shows."""
    body = T.config.c.body.concat({"external_id": None, "skills": [], "caller_id": None, **fields})
    with core.tx() as db:
        db.execute(update(T.config).where(T.config.c.id == desk.agent).values(body=body))


def stored(agent) -> str | None:
    with core.read() as db:
        return db.execute(select(T.password.c.hash).where(T.password.c.agent_id == agent)).scalar()


def pause_first_revoke(monkeypatch) -> tuple[Future, threading.Event]:
    """The first `access.revoke` holds its transaction open until the event is set; the future gets its backend."""
    revoke, paused_at, release = access.revoke, Future(), threading.Event()

    def paused(db, *args, **kwargs):
        if not paused_at.done():
            paused_at.set_result(db.exec_driver_sql("SELECT pg_backend_pid()").scalar())
            release.wait(10)
        return revoke(db, *args, **kwargs)

    monkeypatch.setattr(access, "revoke", paused)
    return paused_at, release


def test_a_password_change_racing_an_admin_reset_keeps_the_reset(monkeypatch):
    desk = Desk()
    admin = sessions_admin(desk)
    password(admin, desk.agent, "stolen-pass-1")
    thief = TestClient(service.app, headers={"Authorization": f"Bearer {desk.token()}"})
    hashed = access.hashed

    def reset_meanwhile(value: str) -> str:
        if value == "thief-pass-2":
            password(admin, desk.agent, "reset-pass-3")
        return hashed(value)

    monkeypatch.setattr(access, "hashed", reset_meanwhile)
    change = {"current_password": "stolen-pass-1", "password": "thief-pass-2"}
    assert thief.put("/api/v1/me/password", json=change).status_code == 401
    assert access.matches("reset-pass-3", stored(desk.agent))


def test_a_password_set_while_its_agent_is_deleted_is_refused_and_signs_in_no_agent_of_another_tenant(monkeypatch):
    monkeypatch.setattr(plivo, "rest", lambda *_, **__: {})
    desk, other = Desk(), Desk()
    admin, (deleting, release) = sessions_admin(desk), pause_first_revoke(monkeypatch)
    with ThreadPoolExecutor(2) as pool:
        try:
            deleted = pool.submit(admin.delete, f"/api/v1/agents/{desk.agent}")
            pid = deleting.result(5)
            setting = pool.submit(admin.post, f"/api/v1/agents/{desk.agent}/password", json={"password": "old-pass-1"})
            assert waiter(pid), "the password set waits for the delete"
        finally:
            release.set()
    assert (deleted.result().status_code, setting.result().status_code) == (204, 404)
    assert stored(desk.agent) is None, "no password outlives its agent"
    email = f"{uuid.uuid4().hex[:8]}@example.test"
    with core.tx() as db:
        body = {"name": "B", "role": "agent", "teams": [], "email": email}
        db.execute(insert(T.config).values(id=desk.agent, tenant_id=other.tenant, kind="agent", body=body))
        db.execute(insert(T.presence).values(agent_id=desk.agent, tenant_id=other.tenant, status_id=other.offline))
        left = {"agent_id": desk.agent, "tenant_id": desk.tenant, "hash": access.hashed("old-pass-1")}
        db.execute(insert(T.password).values(**left))
    signing = {"email": email, "password": "old-pass-1", "tenant_id": str(other.tenant)}
    assert client.post("/api/v1/sessions", json=signing).status_code == 401


def test_a_role_change_racing_a_renewal_ends_the_renewed_session_too(monkeypatch):
    monkeypatch.setattr(plivo, "rest", lambda *_, **__: {"token": "sdk"})
    desk = Desk()
    whole(desk)
    admin, mint, renewer, release = sessions_admin(desk), access.mint, Future(), threading.Event()
    renewing_desk = TestClient(service.app, headers={"Authorization": f"Bearer {desk.tokens.agent_token}"})

    def paused(db, *args):
        if not renewer.done():
            renewer.set_result(db.exec_driver_sql("SELECT pg_backend_pid()").scalar())
            release.wait(10)
        return mint(db, *args)

    monkeypatch.setattr(access, "mint", paused)
    with ThreadPoolExecutor(2) as pool:
        try:
            renewing = pool.submit(renewing_desk.post, "/api/v1/me/sessions")
            pid = renewer.result(5)
            patching = pool.submit(admin.patch, f"/api/v1/agents/{desk.agent}", json={"role": "supervisor"})
            waiter(pid)
        finally:
            release.set()
    assert (renewing.result().status_code, patching.result().status_code) == (201, 200)
    assert beat(renewing.result().json()["agent_token"]) == 401


def test_an_admin_whose_own_promotion_waited_for_its_demotion_is_refused_and_stays_demoted(monkeypatch):
    monkeypatch.setattr(plivo, "rest", lambda *_, **__: {})
    desk = Desk()
    whole(desk, role="admin")
    other_admin, (demoting, release) = sessions_admin(desk), pause_first_revoke(monkeypatch)
    me = TestClient(service.app, headers={"Authorization": f"Bearer {desk.tokens.agent_token}"})
    with ThreadPoolExecutor(2) as pool:
        try:
            demoted = pool.submit(other_admin.patch, f"/api/v1/agents/{desk.agent}", json={"role": "agent"})
            pid = demoting.result(5)
            promoted = pool.submit(me.patch, f"/api/v1/agents/{desk.agent}", json={"role": "admin"})
            assert waiter(pid), "the admin's own write waits for the demotion's config lock"
        finally:
            release.set()
    assert (demoted.result().status_code, promoted.result().status_code) == (200, 401)
    with core.read() as db:
        assert db.execute(select(T.config.c.body["role"].astext).where(T.config.c.id == desk.agent)).scalar() == "agent"


def test_an_admin_demoted_while_its_reset_of_another_admin_waits_sets_no_password_for_that_admin(monkeypatch):
    monkeypatch.setattr(plivo, "rest", lambda *_, **__: {})
    desk, P = Desk(), T.presence
    whole(desk, role="admin")
    with core.tx() as db:
        target = desk.doc(db, "agent", {"name": "T", "role": "admin", "teams": []})
        db.execute(insert(P).values(agent_id=target, tenant_id=desk.tenant, status_id=desk.offline))
    other_admin, (demoting, release) = sessions_admin(desk), pause_first_revoke(monkeypatch)
    me = TestClient(service.app, headers={"Authorization": f"Bearer {desk.tokens.agent_token}"})
    with ThreadPoolExecutor(2) as pool, core.tx() as busy:
        holder = busy.exec_driver_sql("SELECT pg_backend_pid()").scalar()
        busy.execute(select(P.c.agent_id).where(P.c.agent_id == target).with_for_update())
        try:
            demoted = pool.submit(other_admin.patch, f"/api/v1/agents/{desk.agent}", json={"role": "agent"})
            pid = demoting.result(5)
            reset = pool.submit(me.post, f"/api/v1/agents/{target}/password", json={"password": "taken-over-1"})
            assert waiter(pid, holder), "the reset waits for the demotion or for the target's presence"
        finally:
            release.set()
        assert demoted.result().status_code == 200
    assert reset.result().status_code == 401
    assert stored(target) is None, "the demoted admin set no password for the other admin"


def test_a_sip_rotation_whose_admin_is_demoted_meanwhile_shows_no_password(monkeypatch):
    desk = Desk()
    whole(desk, role="admin")
    other_admin = sessions_admin(desk)
    me = TestClient(service.app, headers={"Authorization": f"Bearer {desk.tokens.agent_token}"})

    def demoted_meanwhile(*_, **__) -> dict:
        assert other_admin.patch(f"/api/v1/agents/{desk.agent}", json={"role": "agent"}).status_code == 200
        return {}

    monkeypatch.setattr(plivo, "rest", demoted_meanwhile)
    rotated = me.post(f"/api/v1/agents/{desk.agent}/sip-credentials")
    assert (rotated.status_code, "password" in rotated.text) == (401, False)


def test_a_number_put_whose_admin_is_demoted_meanwhile_repoints_nothing_at_plivo(monkeypatch):
    desk, calls = Desk(), []
    whole(desk, role="admin")
    with core.tx() as db:
        db.execute(update(T.tenant).where(T.tenant.c.id == desk.tenant).values(plivo_app_id="a1"))
    other_admin = sessions_admin(desk)
    me = TestClient(service.app, headers={"Authorization": f"Bearer {desk.tokens.agent_token}"})

    def rest(t, method, path, **params) -> dict:
        calls.append((method, path, params))
        if len(calls) == 1:
            assert other_admin.patch(f"/api/v1/agents/{desk.agent}", json={"role": "agent"}).status_code == 200
        return {}

    monkeypatch.setattr(plivo, "rest", rest)
    hangup = {"open_action": {"type": "hangup"}}
    assert me.put("/api/v1/numbers/+15550003", json=hangup).status_code == 401
    assert other_admin.put("/api/v1/numbers/+15550003", json=hangup).status_code == 200
    assert calls == [("GET", "Number/15550003/", {})] * 2 + [("POST", "Number/15550003/", {"app_id": "a1"})]


def test_every_sign_in_runs_one_scrypt_and_one_without_a_tenant_on_a_shared_install_runs_none(monkeypatch):
    monkeypatch.setattr(plivo, "rest", lambda *_, **__: {"token": "sdk"})
    desks, email, runs, scrypt = (Desk(), Desk()), f"{uuid.uuid4().hex[:8]}@example.test", [], hashlib.scrypt
    for d in desks:
        whole(d, email=email)
        password(sessions_admin(d), d.agent, "right-pass-1")
    monkeypatch.setattr(hashlib, "scrypt", lambda *a, **k: runs.append(1) or scrypt(*a, **k))

    def tried(address: str = email, guess: str = "wrong-pass-1", **body) -> tuple[int, int]:
        runs.clear()
        status = client.post("/api/v1/sessions", json={"email": address, "password": guess, **body}).status_code
        return status, len(runs)

    mine = {"tenant_id": str(desks[0].tenant)}
    assert tried() == (409, 0)
    assert tried(**mine) == (401, 1)
    assert tried(f"nobody-{email}", **mine) == (401, 1)
    assert tried(guess="right-pass-1", **mine) == (201, 1)


def test_the_client_budget_counts_an_ipv6_client_by_its_64_and_an_ipv4_mapped_one_as_its_ipv4():
    def tried(host: str):
        return agents._try(SimpleNamespace(client=SimpleNamespace(host=host)), f"{uuid.uuid4()}:x")

    h = uuid.uuid4().hex
    for i in range(100):
        tried(f"2001:db8:{h[:4]}:{h[4:8]}::{i + 1:x}")
    with pytest.raises(ApiError, match="too many failed sign-ins"):
        tried(f"2001:db8:{h[:4]}:{h[4:8]}:ffff:ffff:ffff:ffff")
    tried(f"2001:db8:{h[:4]}:{h[8:12]}::1")
    v4 = f"10.{int(h[12:14], 16)}.{int(h[14:16], 16)}.{int(h[16:18], 16)}"
    for _ in range(50):
        tried(f"::ffff:{v4}")
        tried(v4)
    with pytest.raises(ApiError):
        tried(v4)
