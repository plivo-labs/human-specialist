import functools
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from types import SimpleNamespace

import pytest
from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import func, insert, text

from hs import access, core, jobs, plivo
from hs import app as service
from hs import conversations as cv
from hs.access import Principal
from hs.conversations import api as cvapi
from hs.core import ApiError, T
from tests.unit.tenants import Tenant
from tests.unit.test_config import died, first_held

RAN: list[str] = []
REQUEST = SimpleNamespace(headers={})


def new_tenant() -> str:
    with core.tx() as db:
        row = insert(T.tenant).values(name="T", plivo_auth_id=uuid.uuid4().hex, plivo_token_enc="sealed")
        return str(db.execute(row.returning(T.tenant.c.id)).scalar_one())


def new_doc(tenant_id: str, kind: str, body: dict | None = None) -> uuid.UUID:
    with core.tx() as db:
        row = insert(T.config).values(tenant_id=tenant_id, kind=kind, body=body or {})
        return db.execute(row.returning(T.config.c.id)).scalar_one()


def new_agent(tenant_id: str, role: str = "agent", teams: tuple = ()) -> str:
    body = {"name": "A", "email": f"{uuid.uuid4().hex}@x.test", "role": role, "teams": [str(t) for t in teams]}
    return str(new_doc(tenant_id, "agent", body))


def new_key(tenant_id: str, *scopes: str) -> dict:
    with core.tx() as db:
        return access.new_key(db, tenant_id, scopes or access.SCOPES)


def login(tenant_id: str, agent_id: str) -> access.Tokens:
    with core.tx() as db:
        return access.mint(db, tenant_id, agent_id)


def sql(statement: str, **values) -> None:
    with core.tx() as db:
        db.execute(text(statement), values)


def test_resolve_takes_the_operator_key_then_hs_credentials_then_resolvers_in_order(monkeypatch):
    t = new_tenant()
    sup = new_agent(t, "supervisor")
    key, tokens, calls = new_key(t, "calls"), login(t, sup), []
    sso = Principal(t, "key", scopes=frozenset({"reports"}))

    def declines(request):
        calls.append("declines")

    def accepts(request):
        calls.append("accepts")
        return sso

    def never(request):
        calls.append("never")

    monkeypatch.setattr(access, "RESOLVERS", [declines, accepts, never])
    assert access.resolve(core.settings.operator_key, REQUEST) == access.OPERATOR
    assert access.resolve(key["api_key"], REQUEST) == Principal(t, "key", key["id"], scopes=frozenset({"calls"}))
    assert access.resolve(tokens.agent_token, REQUEST) == Principal(t, "agent", None, sup, "supervisor")
    assert access.resolve(tokens.stream_token, REQUEST).kind == "stream"
    assert calls == []
    assert access.resolve("a-foreign-token", REQUEST) is sso and calls == ["declines", "accepts"]
    assert access.resolve("", REQUEST) is sso and access.resolve("a-foreign-token") is None


def dead_key(t: str) -> str:
    key = new_key(t)
    with core.tx() as db:
        access.revoke(db, t, key_id=key["id"])
    return key["api_key"]


def dead_token(t: str) -> str:
    agent = new_agent(t)
    token = login(t, agent).agent_token
    with core.tx() as db:
        access.revoke(db, t, agent_id=agent)
    return token


def expired_token(t: str) -> str:
    token = login(t, new_agent(t)).stream_token
    sql("UPDATE credential SET expires_at = now() WHERE sha256 = :sha", sha=access.digest(token))
    return token


def deleted_agent(t: str) -> str:
    agent = new_agent(t)
    token = login(t, agent).agent_token
    sql("DELETE FROM config WHERE id = :id", id=agent)
    return token


def deleted_tenant(t: str) -> str:
    key = new_key(t)["api_key"]
    sql("UPDATE tenant SET deleted_at = now() WHERE id = :id", id=t)
    return key


@pytest.mark.parametrize("kill", [dead_key, dead_token, expired_token, deleted_agent, deleted_tenant])
def test_a_dead_hs_credential_is_refused_and_never_reaches_a_resolver(monkeypatch, kill):
    calls = []
    monkeypatch.setattr(access, "RESOLVERS", [lambda request: calls.append(request) or access.OPERATOR])
    assert access.resolve(kill(new_tenant()), REQUEST) is None
    assert access.resolve(access.AGENT_PREFIX + uuid.uuid4().hex, REQUEST) is None
    assert calls == []


def test_a_resolver_that_raises_or_returns_junk_rejects_and_no_later_resolver_is_asked(monkeypatch, caplog):
    calls = []

    def down(request):
        raise RuntimeError("identity service refused sso-c2VjcmV0")

    def junk(request):
        return {"tenant_id": "t"}

    def bound(secret, request):
        raise RuntimeError("identity service down")

    for first in (down, junk, functools.partial(bound, "sso-c2VjcmV0")):
        monkeypatch.setattr(access, "RESOLVERS", [first, lambda request: calls.append(1) or access.OPERATOR])
        assert access.resolve("a-foreign-token", REQUEST) is None
    assert calls == [] and caplog.text.count("rejected the caller") == 3
    assert "c2VjcmV0" not in caplog.text and "RuntimeError" in caplog.text
    assert [r.exc_info for r in caplog.records] == [None, None, None]


grants = APIRouter(prefix=core.PUBLIC)
for path, needs in {
    "key-calls": ("key:calls",),
    "sup": ("sup",),
    "admin": ("admin",),
    "agent": ("agent",),
    "op": ("op",),
    "stream": ("key:events", "agent", "stream"),
}.items():
    needed = access.require(*needs)
    grants.get(f"/{path}")(lambda p=needed: {"kind": p.kind})
ADMIN, AGENT = access.require("key:admin"), access.require("agent")


@grants.post("/desk", status_code=201)
def desk(p: Principal = AGENT):
    return {"id": str(uuid.uuid4())}


@grants.post("/things", status_code=201)
def things(body: dict, p: Principal = ADMIN):
    name, rid = body["name"], str(uuid.uuid4())
    RAN.append(name)
    if name in ("crash", "refused", "unavailable"):
        with core.tx() as db:
            access.created(db, rid)
    if name.startswith("crash"):
        raise RuntimeError("the process died here")
    if name in ("fail", "refused"):
        raise ApiError(422, "refused")
    if name == "unavailable":
        raise ApiError(503, "dependency_unavailable", "Plivo did not answer; try again")
    return {"id": rid, "api_key": access.KEY_PREFIX + "x"} if name == "secret" else {"id": rid}


access.VIEWS["POST /api/v1/things"] = lambda db, p, rid: {"id": rid, "rebuilt": True}
probe = FastAPI()
probe.middleware("http")(access.idempotency)
core.install(probe)
probe.include_router(grants)


@pytest.fixture(scope="module")
def client():
    return TestClient(probe, raise_server_exceptions=False)


@pytest.fixture(scope="module")
def creds():
    t = new_tenant()
    agent, sup, admin = new_agent(t), new_agent(t, "supervisor"), new_agent(t, "admin")
    tokens = login(t, agent)
    return {
        "operator": core.settings.operator_key,
        "key:calls": new_key(t, "calls")["api_key"],
        "key:reports": new_key(t, "reports")["api_key"],
        "key:admin": new_key(t, "admin")["api_key"],
        "key:other-admin": new_key(t, "admin")["api_key"],
        "agent": tokens.agent_token,
        "stream": tokens.stream_token,
        "supervisor": login(t, sup).agent_token,
        "admin": login(t, admin).agent_token,
        "unknown": access.STREAM_PREFIX + uuid.uuid4().hex,
    }


@pytest.mark.parametrize(
    ("path", "cred", "status"),
    [
        ("key-calls", None, 401),
        ("key-calls", "key:calls", 200),
        ("key-calls", "key:reports", 403),
        ("key-calls", "agent", 403),
        ("key-calls", "operator", 403),
        ("sup", "agent", 403),
        ("sup", "supervisor", 200),
        ("sup", "admin", 200),
        ("sup", "key:calls", 403),
        ("admin", "admin", 200),
        ("admin", "supervisor", 403),
        ("admin", "agent", 403),
        ("admin", "key:admin", 403),
        ("agent", "agent", 200),
        ("agent", "stream", 403),
        ("op", "operator", 200),
        ("op", "key:admin", 403),
        ("stream", "?stream", 200),
        ("stream", "?agent", 401),
        ("stream", "?key:calls", 401),
        ("stream", "?unknown", 401),
        ("agent", "?stream", 401),
    ],
)
def test_a_route_takes_only_the_credentials_its_grants_name(client, creds, path, cred, status):
    query = cred and cred.startswith("?")
    token = creds[cred.removeprefix("?")] if cred else None
    headers = {"Authorization": f"Bearer {token}"} if token and not query else {}
    r = client.get(f"/api/v1/{path}", headers=headers, params={"access_token": token} if query else None)
    assert r.status_code == status, r.text


def test_a_password_hash_is_salted_scrypt_and_matches_only_its_password():
    one, two = access.hashed("correct horse"), access.hashed("correct horse")
    assert one != two and one.startswith("scrypt$16384$8$1$") and "correct horse" not in one
    assert access.matches("correct horse", one) and access.matches("correct horse", two)
    assert not access.matches("correct hors", one)


def test_an_unknown_grant_fails_at_import():
    with pytest.raises(ValueError, match="unknown grants"):
        access.require("plivo")


def test_owned_is_422_for_another_tenants_or_another_kinds_id():
    a, b = new_tenant(), new_tenant()
    mine, theirs, skill = new_doc(a, "team"), new_doc(b, "team"), new_doc(a, "skill")
    with core.read() as db:
        access.owned(db, Principal(a, "key"), "team", [mine])
        access.owned(db, Principal(a, "key"), "team", [])
        for bad in ([theirs], [mine, skill]):
            with pytest.raises(ApiError) as e:
                access.owned(db, Principal(a, "key"), "team", bad)
            assert (e.value.status, e.value.code, e.value.details["ids"]) == (422, "foreign_id", [str(bad[-1])])


@pytest.fixture(scope="module")
def floor():
    """One live call in tenant `a`: `on` holds the active assignment, `gone` a finished one, `rang` an offer it never
    took; `mate` and `sup` share `on`'s team, `far` supervises another team."""
    a, b = new_tenant(), new_tenant()
    team, elsewhere = new_doc(a, "team"), new_doc(a, "team")
    who = {
        "on": new_agent(a, teams=(team,)),
        "gone": new_agent(a),
        "rang": new_agent(a),
        "mate": new_agent(a, teams=(team,)),
    }
    who |= {"sup": new_agent(a, "supervisor", (team,)), "far": new_agent(a, "supervisor", (elsewhere,))}
    who |= {"admin": new_agent(a, "admin")}
    with core.tx() as db:
        row = insert(T.conversation).values(
            tenant_id=a, channel="call", direction="inbound", state="live", customer="+1"
        )
        cid = db.execute(row.returning(T.conversation.c.id)).scalar_one()
        for name, state, started in (
            ("on", "active", func.now()),
            ("gone", "done", func.now()),
            ("rang", "missed", None),
        ):
            values = {"tenant_id": a, "conversation_id": cid, "agent_id": who[name], "channel": "call"}
            db.execute(
                insert(T.assignment).values(**values, role="primary", via="queue", state=state, started_at=started)
            )
    roles = {"sup": "supervisor", "far": "supervisor", "admin": "admin"}
    principals = {n: Principal(a, "agent", agent_id=i, role=roles.get(n, "agent")) for n, i in who.items()}
    return cid, principals | {"key": Principal(a, "key"), "other tenant": Principal(b, "key")}


def seen(p: Principal, cid: uuid.UUID, control: bool, offer: bool = False) -> bool:
    with core.read() as db:
        try:
            return access.conversation(db, p, cid, control=control, offer=offer).id == cid
        except ApiError as e:
            assert (e.status, e.code) == (404, "conversation_not_found")
            return False


@pytest.mark.parametrize(
    ("who", "sees", "controls"),
    [
        ("key", True, True),
        ("other tenant", False, False),
        ("on", True, True),
        ("gone", True, False),
        ("rang", False, False),
        ("mate", False, False),
        ("sup", True, True),
        ("far", False, False),
        ("admin", True, True),
    ],
)
def test_a_conversation_is_404_unless_the_caller_may_see_it(floor, who, sees, controls):
    cid, principals = floor
    assert (seen(principals[who], cid, False), seen(principals[who], cid, True)) == (sees, controls)


@pytest.mark.parametrize(("due_in", "attempts", "reads"), [(30, 0, True), (-1, 0, False), (30, 1, False)])
def test_an_offered_agent_reads_where_asked_until_its_timer_is_due_or_ran(floor, due_in, attempts, reads):
    cid, principals = floor
    tenant = principals["key"].tenant_id
    agent, offer = new_agent(tenant), str(uuid.uuid4())
    timer = {"tenant_id": tenant, "conversation_id": str(cid), "assignment_id": offer, "agent_id": agent}
    due = func.now() + timedelta(seconds=due_in)
    with core.tx() as db:
        db.execute(insert(T.jobs).values(kind="offer_timeout", key=offer, payload=timer, run_at=due, attempts=attempts))
    p = Principal(tenant, "agent", agent_id=agent, role="agent")
    assert (seen(p, cid, False), seen(p, cid, False, offer=True)) == (False, reads)


def test_a_supervisor_sees_its_teammates_and_a_key_or_an_admin_sees_everyone(floor):
    _, principals = floor
    agent = {n: p.agent_id for n, p in principals.items()}
    with core.read() as db:
        assert access.visible_agents(db, principals["sup"]) == {agent["sup"], agent["on"], agent["mate"]}
        assert access.visible_agents(db, principals["far"]) == {agent["far"]}
        assert access.visible_agents(db, principals["on"]) == {agent["on"]}
        assert access.visible_agents(db, principals["admin"]) is access.visible_agents(db, principals["key"]) is None


def post(client, key: str, idem: str, name: str):
    headers = {"Authorization": f"Bearer {key}", "Idempotency-Key": idem}
    return client.post("/api/v1/things", json={"name": name}, headers=headers)


def test_a_replay_answers_the_stored_response_and_another_body_is_409(client, creds):
    idem, RAN[:] = uuid.uuid4().hex, []
    first, again = (post(client, creds["key:admin"], idem, "a") for _ in range(2))
    assert first.status_code == again.status_code == 201 and again.json() == first.json() and RAN == ["a"]
    other = post(client, creds["key:admin"], idem, "b")
    assert (other.status_code, other.json()["error"]["code"]) == (409, "idempotency_conflict")
    assert post(client, creds["key:other-admin"], idem, "a").status_code == 201 and RAN == ["a", "a"]


@pytest.mark.parametrize(("name", "runs"), [("fail", 2), ("refused", 1)])
def test_an_error_answer_frees_the_key_only_when_the_route_recorded_nothing(client, creds, name, runs):
    idem, RAN[:] = uuid.uuid4().hex, []
    first, again = (post(client, creds["key:admin"], idem, name) for _ in range(2))
    assert (first.status_code, again.status_code, RAN) == (422, 422, [name] * runs)
    assert runs == 2 or again.json() == first.json()


def test_a_secret_is_never_stored_and_its_replay_is_409_with_the_id(client, creds):
    idem = uuid.uuid4().hex
    first, again = (post(client, creds["key:admin"], idem, "secret") for _ in range(2))
    assert (again.status_code, again.json()["error"]["code"]) == (409, "idempotency_replay_secret")
    assert again.json()["error"]["details"] == {"id": first.json()["id"]} and first.json()["api_key"] not in again.text
    with core.read() as db:
        stored = text("SELECT status, body FROM idempotency WHERE key_sha256 = :sha")
        assert db.execute(stored, {"sha": access.digest(idem)}).one() == (201, None)


def test_a_stream_token_never_replays_its_agents_write(client, creds):
    idem = uuid.uuid4().hex
    as_agent, as_stream = (
        {"Authorization": f"Bearer {creds[t]}", "Idempotency-Key": idem} for t in ("agent", "stream")
    )
    assert client.post("/api/v1/desk", headers=as_agent).status_code == 201
    r = client.post("/api/v1/desk", headers=as_stream)
    assert (r.status_code, r.json()["error"]["code"]) == (403, "forbidden")


def test_plugin_callers_never_share_a_key_and_one_without_an_identity_cannot_hold_one(client, monkeypatch):
    t, idem, RAN[:] = new_tenant(), uuid.uuid4().hex, []
    admin = frozenset({"admin"})
    callers = {
        "ann": Principal(t, "key", "ann", scopes=admin),
        "bob": Principal(t, "key", "bob", scopes=admin),
        "none": Principal(t, "key", scopes=admin),
        "empty key": Principal(t, "key", "", scopes=admin),
        "empty agent": Principal(t, "agent", agent_id="", role="admin"),
    }
    monkeypatch.setattr(access, "RESOLVERS", [lambda request: callers[request.headers["x-sso"]]])

    def send(who: str):
        return client.post("/api/v1/things", json={"name": "a"}, headers={"X-SSO": who, "Idempotency-Key": idem})

    assert [send(who).status_code for who in ("ann", "bob", "ann")] == [201, 201, 201] and RAN == ["a", "a"]
    for who in ("none", "empty key", "empty agent"):
        r = send(who)
        assert (r.status_code, r.json()["error"]["code"]) == (422, "idempotency_unsupported"), who
    callers["ann"] = Principal(t, "key", "ann", scopes=frozenset({"reports"}))
    shrunk = send("ann")
    assert (shrunk.status_code, shrunk.json()["error"]["code"]) == (409, "idempotency_conflict") and RAN == ["a", "a"]


def age(idem: str) -> None:
    sql(
        "UPDATE idempotency SET created_at = now() - interval '3 minutes' WHERE key_sha256 = :sha",
        sha=access.digest(idem),
    )


def test_a_takeover_after_the_route_committed_its_create_answers_from_the_view_and_never_reruns(client, creds):
    idem, RAN[:] = uuid.uuid4().hex, []
    assert post(client, creds["key:admin"], idem, "crash").status_code == 500
    stalled = post(client, creds["key:admin"], idem, "crash")
    assert (stalled.status_code, stalled.json()["error"]["code"]) == (409, "idempotency_in_progress")
    age(idem)
    taken = post(client, creds["key:admin"], idem, "crash")
    assert taken.status_code == 201 and taken.json()["rebuilt"] and RAN == ["crash"]


def test_a_5xx_after_the_route_recorded_its_resource_replays_as_it_was_and_never_reruns(client, creds):
    idem, RAN[:] = uuid.uuid4().hex, []
    first, again = (post(client, creds["key:admin"], idem, "unavailable") for _ in range(2))
    assert (first.status_code, again.status_code, again.json(), RAN) == (503, 503, first.json(), ["unavailable"])


def claimed_at(idem: str):
    with core.read() as db:
        return db.execute(
            text("SELECT created_at FROM idempotency WHERE key_sha256 = :sha"), {"sha": access.digest(idem)}
        ).scalar()


def replying(monkeypatch, rest) -> tuple[TestClient, str, uuid.UUID]:
    """A calls key's client with an Idempotency-Key, the messages path of an SMS conversation an agent holds, and its
    tenant; Plivo answers through `rest`."""
    floor = Tenant()
    monkeypatch.setattr(plivo, "rest", rest)
    agent, conv = floor.agent(), floor.open(channel="sms", customer="+14155550101", ours="+14155550102")
    with core.tx() as db:
        cv.move(db, cv.offer(db, cv.lock(db, conv.id), agent), "accept")
        key = access.new_key(db, floor.id, ["calls"])["api_key"]
    headers = {"Authorization": f"Bearer {key}", "Idempotency-Key": uuid.uuid4().hex}
    api = TestClient(service.app, headers=headers, raise_server_exceptions=False)
    return api, f"/api/v1/conversations/{conv.id}/messages", floor.id


def test_a_reply_whose_claim_was_taken_over_never_sends_its_message(monkeypatch):
    sent = []
    api, path, _ = replying(monkeypatch, lambda t, m, p, **_: sent.append(p) or {"message_uuid": [uuid.uuid4().hex]})
    held_first, reached, release = first_held(access.conversation)
    monkeypatch.setattr(access, "conversation", held_first)
    with ThreadPoolExecutor(1) as pool:
        first = pool.submit(api.post, path, json={"text": "hi"})
        reached.result(5)
        token = claimed_at(api.headers["Idempotency-Key"])
        monkeypatch.setattr(access, "TAKEOVER", 0)
        taken = api.post(path, json={"text": "hi"})
        assert claimed_at(api.headers["Idempotency-Key"]) > token, "the takeover renewed the claim"
        release.set()
        late = first.result(10)
    assert (taken.status_code, late.status_code, sent) == (201, 409, ["Message/"])
    assert api.post(path, json={"text": "hi"}).json() == taken.json() and sent == ["Message/"]


def refusing(monkeypatch) -> tuple[TestClient, str, list]:
    """Like `replying`, but Plivo refuses every request; the paths it was sent."""
    sent = []

    def refused(t, m, path, **_):
        sent.append(path)
        raise plivo.Refused(422, "plivo_refused", "Plivo refused the request")

    api, path, _ = replying(monkeypatch, refused)
    return api, path, sent


def test_a_refused_reply_retried_with_its_key_replays_the_refusal_and_makes_one_message(monkeypatch):
    api, path, sent = refusing(monkeypatch)
    first, again = (api.post(path, json={"text": "hi"}) for _ in range(2))
    assert (first.status_code, again.status_code, again.json(), sent) == (422, 422, first.json(), ["Message/"])
    assert len(api.get(path).json()["data"]) == 1


def test_a_refused_reply_whose_answer_was_lost_replays_the_refusal_and_makes_one_message(monkeypatch):
    api, path, sent = refusing(monkeypatch)
    monkeypatch.setattr(access, "_store", died)
    assert api.post(path, json={"text": "hi"}).status_code == 500
    monkeypatch.setattr(access, "TAKEOVER", 0)
    again = api.post(path, json={"text": "hi"})
    assert (again.status_code, sent) == (422, ["Message/"]) and again.json()["error"]["code"] == "message_refused"
    assert len(api.get(path).json()["data"]) == 1


def dying(keys: list):
    def verdict(*waited, **_):
        keys.extend(waited)
        raise RuntimeError("the process died before it ran its command")

    return verdict


def test_a_refused_reply_a_worker_sent_after_its_request_died_replays_the_refusal(monkeypatch):
    api, path, sent = refusing(monkeypatch)
    keys = []
    monkeypatch.setattr("hs.text.verdict", dying(keys))
    assert api.post(path, json={"text": "hi"}).status_code == 500
    jobs.run_now("plivo", keys[0], wait=True)
    monkeypatch.setattr(access, "TAKEOVER", 0)
    again = api.post(path, json={"text": "hi"})
    assert (again.status_code, sent) == (422, ["Message/"]) and again.json()["error"]["code"] == "message_refused"


def noting(monkeypatch) -> tuple[TestClient, str, Callable[[], list]]:
    """A client with an Idempotency-Key, a conversation's notes path, and the notes its read model shows."""
    api, path, _ = replying(monkeypatch, lambda *_, **__: {})
    conversation = path.removesuffix("/messages")
    return api, f"{conversation}/notes", lambda: api.get(conversation).json()["notes"]


def test_a_note_whose_answer_was_lost_replays_that_note_and_adds_no_other(monkeypatch):
    api, path, notes = noting(monkeypatch)
    store = access._store
    monkeypatch.setattr(access, "_store", died)
    assert api.post(path, json={"text": "n"}).status_code == 500
    monkeypatch.setattr(access, "_store", store)
    monkeypatch.setattr(access, "TAKEOVER", 0)
    again = api.post(path, json={"text": "n"})
    assert again.status_code == 201 and notes() == [again.json()]


def test_a_note_whose_claim_was_taken_over_adds_one_note(monkeypatch):
    api, path, notes = noting(monkeypatch)
    held_first, reached, release = first_held(cvapi._noted)
    monkeypatch.setattr(cvapi, "_noted", held_first)
    with ThreadPoolExecutor(1) as pool:
        first = pool.submit(api.post, path, json={"text": "n"})
        reached.result(5)
        monkeypatch.setattr(access, "TAKEOVER", 0)
        taken = api.post(path, json={"text": "n"})
        release.set()
        late = first.result(10)
    assert (taken.status_code, late.status_code, notes()) == (201, 409, [taken.json()])


def test_a_takeover_never_answers_a_secret_its_view_holds(client, creds, monkeypatch):
    monkeypatch.setitem(access.VIEWS, "POST /api/v1/things", lambda db, p, rid: {"id": rid, "password": "pw-c2VjcmV0"})
    idem = uuid.uuid4().hex
    assert post(client, creds["key:admin"], idem, "crash").status_code == 500
    age(idem)
    r = post(client, creds["key:admin"], idem, "crash")
    assert (r.status_code, r.json()["error"]["code"]) == (409, "idempotency_replay_secret")
    assert r.json()["error"]["details"]["id"] and "c2VjcmV0" not in r.text


def test_a_crash_before_any_create_frees_the_key_and_a_dead_claim_is_taken_over(client, creds, monkeypatch):
    idem, RAN[:] = uuid.uuid4().hex, []
    assert [post(client, creds["key:admin"], idem, "crash early").status_code for _ in range(2)] == [500, 500]
    monkeypatch.setattr(access, "_free", lambda claim: None)
    idem = uuid.uuid4().hex
    assert post(client, creds["key:admin"], idem, "crash early").status_code == 500
    assert post(client, creds["key:admin"], idem, "crash early").status_code == 409
    age(idem)
    assert post(client, creds["key:admin"], idem, "crash early").status_code == 500
    assert RAN == ["crash early"] * 4


def test_the_operator_mints_lists_and_revokes_scoped_keys():
    t, op = new_tenant(), TestClient(service.app, headers={"Authorization": f"Bearer {core.settings.operator_key}"})
    keys = f"/api/v1/tenants/{t}/api-keys"
    made = [op.post(keys, json={"scopes": ["reports"]}).json() for _ in range(3)]
    assert access.resolve(made[0]["api_key"]).scopes == {"reports"}
    first = op.get(keys, params={"limit": 2}).json()
    rest = op.get(keys, params={"limit": 2, "cursor": first["next_cursor"]}).json()
    listed = [k["id"] for k in first["data"] + rest["data"]]
    assert sorted(listed) == sorted(k["id"] for k in made) and not rest["next_cursor"]
    assert not [k for k in made if k["api_key"] in str(first) + str(rest)]
    assert op.delete(f"{keys}/{made[0]['id']}").status_code == 204 and access.resolve(made[0]["api_key"]) is None
    assert op.delete(f"{keys}/{made[0]['id']}").status_code == 404
    assert op.post(f"/api/v1/tenants/{uuid.uuid4()}/api-keys", json={"scopes": ["admin"]}).status_code == 404
    replay = {"Idempotency-Key": uuid.uuid4().hex}
    minted, again = (op.post(keys, json={"scopes": ["admin"]}, headers=replay) for _ in range(2))
    assert again.status_code == 409 and minted.json()["api_key"] not in again.text


def test_a_key_mint_that_died_before_its_response_was_stored_never_mints_twice(monkeypatch):
    t, idem = new_tenant(), uuid.uuid4().hex
    keys = f"/api/v1/tenants/{t}/api-keys"
    op = TestClient(service.app, raise_server_exceptions=False)
    op.headers.update({"Authorization": f"Bearer {core.settings.operator_key}", "Idempotency-Key": idem})
    store = access._store

    def died(*args):
        raise RuntimeError("the process died before storing the response")

    monkeypatch.setattr(access, "_store", died)
    assert op.post(keys, json={"scopes": ["admin"]}).status_code == 500
    monkeypatch.setattr(access, "_store", store)
    age(idem)
    again = op.post(keys, json={"scopes": ["admin"]})
    assert (again.status_code, again.json()["error"]["code"]) == (409, "idempotency_replay_secret")
    assert [k["id"] for k in op.get(keys).json()["data"]] == [again.json()["error"]["details"]["id"]]
