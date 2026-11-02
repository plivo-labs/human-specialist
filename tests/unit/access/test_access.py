"""hs.access, hs.errors and hs.paging without a database: credentials, the Plivo signature, idempotency,
the error envelope, CORS and log redaction."""

import dataclasses
import logging
from datetime import datetime
from urllib.parse import parse_qsl, urlsplit

import fakeredis
import pytest
from cryptography.fernet import InvalidToken
from fastapi import Depends, FastAPI, Query
from fastapi.testclient import TestClient
from plivo.utils.signature_v3 import validate_v3_signature
from pydantic import BaseModel

from hs import access, api, errors, paging
from hs.access.idempotency import idempotency
from hs.api import _ApiCors

OPERATOR = "hs_op_test"
RING = ("sip:alice123@phone.plivo.com", "sip:alice123_SATESTTESTTESTTEST12@phone.plivo.com")
ADMIN, STREAM, SUP = (
    Depends(access.require(*a)) for a in (("key:admin",), ("key:events", "agent", "stream"), ("sup",))
)
OP, OP_SECRET, PLIVO = (
    Depends(access.require("op")),
    Depends(access.require("op", secret=True)),
    Depends(access.require("plivo")),
)


def configure(monkeypatch, **values):
    monkeypatch.setattr(access, "settings", dataclasses.replace(access.settings, **values))
    access._fernet.cache_clear()


@pytest.fixture(autouse=True)
def redis(monkeypatch):
    client = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(access, "redis_client", lambda: client)
    configure(monkeypatch, operator_key=OPERATOR)
    return client


class Body(BaseModel):
    name: str


@pytest.fixture()
def app():
    app = FastAPI()
    errors.install(app)
    app.middleware("http")(idempotency)
    calls = app.state.calls = []

    @app.get("/api/v1/admin", dependencies=[ADMIN])
    def admin():
        return {}

    @app.get("/api/v1/stream")
    def stream(p: access.Principal = STREAM):
        return {"kind": p.kind}

    @app.get("/api/v1/sup", dependencies=[SUP])
    def sup():
        return {}

    @app.post("/api/v1/things", status_code=201)
    def create(body: Body, p: access.Principal = OP):
        calls.append(body.name)
        return {"id": f"t{len(calls)}", "name": body.name}

    @app.post("/api/v1/secrets", status_code=201)
    def secret(p: access.Principal = OP_SECRET):
        calls.append("secret")
        return {"id": "s1", "api_key": "hs_tk_secret"}

    @app.get("/api/v1/items")
    def items(p: paging.Paging = Depends(paging.paging), n: int = Query(3)):
        rows = [{"id": i} for i in range(n) if p.after is None or i > int(p.after)]
        return paging.page(rows[: p.limit + 1], p)

    @app.post("/plivo/{tenant_id}/answer", dependencies=[PLIVO])
    def answer(tenant_id: str):
        return {}

    return app


def bearer(token):
    return {"Authorization": f"Bearer {token}"}


def test_v3_signature_matches_the_sdk():
    cases = [
        ("https://hs.example/plivo/t/answer", {"CallUUID": "c1", "From": "14155550111", "To": "1415"}),
        ("https://hs.example/plivo/t/menu?step=2&dial=a&dial=b", {"Digits": "1", "CallUUID": "c1"}),
        ("https://hs.example/plivo/t/menu?step=2", {}),
        ("https://hs.example/plivo/t/hangup", {}),
    ]
    for url, form in cases:
        parts = urlsplit(url)
        base = f"{parts.scheme}://{parts.netloc}{parts.path}"
        query = parse_qsl(parts.query, keep_blank_values=True)
        sig = access.v3_signature("token-1", base, query, list(form.items()), "4242")
        assert validate_v3_signature("POST", url, "4242", "token-1", sig, dict(form)), url
        assert not validate_v3_signature("POST", url, "4242", "token-2", sig, dict(form)), url


def test_plivo_routes_need_the_tenants_signature_over_the_public_url(app, monkeypatch):
    configure(monkeypatch, public_base_url="https://hs.example", secret_key="k" * 32)
    monkeypatch.setattr(access, "_tenant", lambda t: (7, "MA1", access.encrypt("tok")))
    form = {"CallUUID": "c1"}
    good = access.v3_signature("tok", "https://hs.example/plivo/t1/answer", [], list(form.items()), "9")
    client = TestClient(app, base_url="http://evil.example")
    signed = {"X-Plivo-Signature-V3": f"other,{good}", "X-Plivo-Signature-V3-Nonce": "9"}
    assert client.post("/plivo/t1/answer", data=form, headers=signed).status_code == 200
    assert client.post("/plivo/t1/answer", data={"CallUUID": "c2"}, headers=signed).status_code == 401
    unsigned = client.post("/plivo/t1/answer", data=form)
    assert (unsigned.status_code, unsigned.content) == (401, b"")
    # A Host header can rewrite request.url's path; the signed URL is built from the raw path only.
    for_hangup = access.v3_signature("tok", "https://hs.example/plivo/t1/hangup", [], list(form.items()), "9")
    hostile = {"X-Plivo-Signature-V3": for_hangup, "X-Plivo-Signature-V3-Nonce": "9", "Host": "x/plivo/t1/hangup?"}
    assert client.post("/plivo/t1/answer", data=form, headers=hostile).status_code == 401


def test_the_plivo_token_is_sealed_with_a_key_from_the_secret(monkeypatch):
    configure(monkeypatch, secret_key="a" * 32)
    sealed = access.encrypt("plivo-token")
    assert "plivo-token" not in sealed and access.decrypt(sealed) == "plivo-token"
    configure(monkeypatch, secret_key="b" * 32)
    with pytest.raises(InvalidToken):
        access.decrypt(sealed)


def test_credentials_open_only_their_routes(app):
    client = TestClient(app)
    agent, stream, _ = access.mint_tokens("t1", 7, "a1", "agent", RING)
    boss, _, _ = access.mint_tokens("t1", 7, "a2", "supervisor", RING)
    body = client.get("/api/v1/admin").json()
    assert body["error"]["code"] == "unauthorized" and body["request_id"]
    assert client.get("/api/v1/admin", headers=bearer(OPERATOR)).status_code == 403
    assert client.get("/api/v1/stream", headers=bearer(agent)).json() == {"kind": "agent"}
    assert client.get("/api/v1/stream", params={"access_token": stream}).json() == {"kind": "stream"}
    assert client.get("/api/v1/stream", params={"access_token": agent}).status_code == 401
    assert client.get("/api/v1/admin", headers=bearer(stream)).status_code == 403
    assert client.get("/api/v1/sup", headers=bearer(agent)).status_code == 403
    assert client.get("/api/v1/sup", headers=bearer(boss)).status_code == 200
    access.revoke_sessions("a1")
    assert client.get("/api/v1/stream", headers=bearer(agent)).status_code == 401
    assert access.resolve(stream) is None and access.resolve(boss).role == "supervisor"


def test_tokens_are_kept_under_their_sha256(redis):
    agent, stream, _ = access.mint_tokens("t1", 7, "a1", "agent", RING)
    assert redis.ttl(f"AGT:{access.digest(agent)}") > 3500 and redis.exists(f"STR:{access.digest(stream)}")
    assert not [k for k in redis.scan_iter() if agent in k or stream in k]


def test_errors_use_the_envelope_on_the_public_api_only(app):
    client = TestClient(app)
    invalid = client.post("/api/v1/things", json={"name": 5}, headers=bearer(OPERATOR)).json()["error"]
    assert invalid["code"] == "validation_failed" and invalid["details"]["fields"][0]["loc"] == ["body", "name"]
    bad_json = client.post(
        "/api/v1/things", content=b"{no", headers={**bearer(OPERATOR), "Content-Type": "application/json"}
    )
    assert (bad_json.status_code, bad_json.json()["error"]["code"]) == (400, "invalid_json")
    assert client.get("/api/v1/nowhere").json()["error"]["code"] == "not_found"
    assert client.get("/plivo/t1/nowhere").content == b""


def test_an_idempotent_replay_is_the_stored_response(app):
    client = TestClient(app)
    key = {**bearer(OPERATOR), "Idempotency-Key": "k1"}
    first = client.post("/api/v1/things", json={"name": "a"}, headers=key)
    again = client.post("/api/v1/things", json={"name": "a"}, headers=key)
    assert (again.status_code, again.json()) == (first.status_code, first.json()) == (201, {"id": "t1", "name": "a"})
    conflict = client.post("/api/v1/things", json={"name": "b"}, headers=key)
    assert (conflict.status_code, conflict.json()["error"]["code"]) == (409, "idempotency_conflict")
    assert client.post("/api/v1/things?x=1", json={"name": "a"}, headers=key).status_code == 409
    assert app.state.calls == ["a"]
    retry = {**key, "Idempotency-Key": "k3"}
    assert client.post("/api/v1/things", json={"name": 5}, headers=retry).status_code == 422
    assert client.post("/api/v1/things", json={"name": "c"}, headers=retry).status_code == 201  # errors are not kept


def test_a_secret_route_replays_only_its_id(app):
    client = TestClient(app)
    key = {**bearer(OPERATOR), "Idempotency-Key": "k2"}
    assert client.post("/api/v1/secrets", headers=key).json()["api_key"]
    again = client.post("/api/v1/secrets", headers=key)
    assert again.status_code == 409 and again.json()["error"]["details"] == {"id": "s1"}
    assert "hs_tk_secret" not in again.text and app.state.calls == ["secret"]


def test_lists_page_with_an_opaque_cursor(app):
    client = TestClient(app)
    first = client.get("/api/v1/items", params={"limit": 2}).json()
    rest = client.get("/api/v1/items", params={"limit": 2, "cursor": first["next_cursor"]}).json()
    assert [r["id"] for r in first["data"]] == [0, 1] and rest == {"data": [{"id": 2}], "next_cursor": None}
    assert client.get("/api/v1/items", params={"limit": 201}).status_code == 422
    assert client.get("/api/v1/items", params={"cursor": "%%%"}).json()["error"]["code"] == "invalid_cursor"


def test_cors_covers_the_public_api_only(monkeypatch):
    monkeypatch.setattr(
        api, "settings", dataclasses.replace(api.settings, cors_allowed_origins=("https://desk.example",))
    )
    app = FastAPI()
    app.add_middleware(_ApiCors)
    app.post("/plivo/{t}/answer")(lambda t: {})
    client = TestClient(app)
    ask = {"Origin": "https://desk.example", "Access-Control-Request-Method": "GET"}
    assert client.options("/api/v1/x", headers=ask).headers["access-control-allow-origin"] == "https://desk.example"
    evil = client.options("/api/v1/x", headers={**ask, "Origin": "https://evil.example"})
    assert "access-control-allow-origin" not in evil.headers
    assert "access-control-allow-origin" not in client.options("/plivo/t/answer", headers=ask).headers


def test_the_access_token_never_reaches_the_access_log():
    from uvicorn.logging import AccessFormatter

    args = ("1.2.3.4:5", "GET", "/api/v1/events/stream?access_token=hs_st_x&after=1", "1.1", 200)
    record = logging.LogRecord("uvicorn.access", 20, "", 0, '%s - "%s %s HTTP/%s" %d', args, None)
    access.redact_access_token(record)
    line = AccessFormatter("%(client_addr)s %(request_line)s %(status_code)s", use_colors=False).format(record)
    assert line == "1.2.3.4:5 GET /api/v1/events/stream?access_token=[REDACTED]&after=1 HTTP/1.1 200 OK"


@pytest.mark.parametrize("name", ["sig", "%73ig", "%73%69%67"])
def test_a_recording_url_signature_never_reaches_the_access_log(name):
    path = f"/api/v1/recordings/r1/audio?exp=99&v=k1&{name}=abc%26123&download=1&x=%0d%0aFAKE"
    record = logging.LogRecord("uvicorn.access", 20, "", 0, '"GET %s"', (path,), None)
    access.redact_access_token(record)
    assert record.args == (f"/api/v1/recordings/r1/audio?exp=99&v=k1&{name}=[REDACTED]&download=1&x=%0d%0aFAKE",)


def test_a_deleted_tenants_tokens_and_unknown_auth_codes_are_refused():
    agent, stream, _ = access.mint_tokens("t1", 7, "a1", "agent", RING)
    access.close_tenant("t1")
    assert access.resolve(agent) is None and access.resolve(stream) is None
    with pytest.raises(ValueError):
        access.require("key:everything")


def test_a_session_asks_plivo_for_the_sdk_token_and_rings_the_sdk_user_until_revoked(monkeypatch):
    from hs.access import api

    sent = []
    monkeypatch.setattr(api, "_agent", lambda agent_id, p: (RING[0], "supervisor", True, "app9"))
    monkeypatch.setattr(api, "tenant_plivo", lambda tenant_id: ("SATESTTESTTESTTEST12", "tok"))
    monkeypatch.setattr(api.rest, "ok", lambda *a, **claims: sent.append((a, claims)) or {"token": "plivo-minted"})
    app = FastAPI()
    errors.install(app)
    app.include_router(api.sessions_router)
    key = access.Principal("t1", 7, "key", frozenset({"sessions"}), key_id="k1")
    for dep in (api.SESSIONS, api.SESSIONS_SECRET):
        app.dependency_overrides[dep.dependency] = lambda: key
    client = TestClient(app)
    agent_id = "6f1c2a3e-0000-4000-8000-000000000001"
    s = client.post(f"/agents/{agent_id}/sessions").json()
    agent = access.resolve(s["agent_token"])
    assert (agent.kind, agent.agent_id, agent.role, agent.tenant_id) == ("agent", agent_id, "supervisor", "t1")
    [(args, claims)] = sent
    assert (
        args == (("SATESTTESTTESTTEST12", "tok"), "POST", "JWT/Token/") and s["plivo"]["access_token"] == "plivo-minted"
    )
    per = {"voice": {"incoming_allow": True, "outgoing_allow": True}}
    assert claims == {
        "iss": args[0][0],
        "sub": "alice123",
        "nbf": claims["nbf"],
        "exp": claims["exp"],
        "per": per,
        "app": "app9",
    }
    expiry = (claims["exp"] - claims["nbf"], datetime.fromisoformat(s["plivo"]["expires_at"]).timestamp())
    assert (
        expiry == (access.TOKEN_TTL, claims["exp"])
        and claims["exp"] <= datetime.fromisoformat(s["expires_at"]).timestamp()
    )
    assert access.ring_address(RING[0]) == RING[1]
    assert client.delete(f"/agents/{agent_id}/sessions").status_code == 204
    assert access.resolve(s["agent_token"]) is None and access.resolve(s["stream_token"]) is None
    assert access.ring_address(RING[0]) == RING[0]


def test_a_browser_sdk_user_reads_as_its_endpoint_and_nothing_else_changes():
    assert access.agent_address(RING[1]) == RING[0]
    assert access.agent_address(RING[1].removeprefix("sip:")) == RING[0].removeprefix("sip:")
    for kept in (RING[0], "sip:alice_bob@example.com", "14155550100", "+14155550100"):
        assert access.agent_address(kept) == kept


def test_plivo_failures_answer_the_api_caller(monkeypatch):
    import httpx

    from hs.errors import ApiError
    from hs.plivo import rest

    for status, code in ((401, "plivo_credentials_rejected"), (404, "plivo_error")):
        monkeypatch.setattr(rest, "call", lambda *a, s=status, **k: httpx.Response(s))
        with pytest.raises(ApiError) as e:
            rest.ok(("MA1", "tok"), "DELETE", "Endpoint/1/")
        assert e.value.code == code
    assert e.value.details == {"status": 404, "errors": {}}


def test_a_plivo_refusal_carries_its_field_errors_and_is_logged_once(monkeypatch, caplog):
    import httpx

    from hs.errors import ApiError
    from hs.plivo import rest

    body = {"api_id": "x", "answer_url": ["Must be a valid url"], "error": "bad " + "word " * 100, "n": 1}
    monkeypatch.setattr(httpx, "request", lambda *a, **k: httpx.Response(400, json=body))
    with caplog.at_level("WARNING", logger="human-specialist"), pytest.raises(ApiError) as e:
        rest.ok(("MA1", "tok"), "POST", "Application/", app_name="a")
    errors = e.value.details["errors"]
    assert errors["answer_url"] == ["Must be a valid url"] and len(errors["error"]) <= 200 and "api_id" not in errors
    logged = [r.getMessage() for r in caplog.records]
    assert len(logged) == 1 and "POST Application/" in logged[0] and "400" in logged[0]
    assert "Must be a valid url" in logged[0] and "tok" not in logged[0] and "MA1" not in logged[0]


def test_a_plivo_403_is_a_refusal_with_its_reason_not_bad_credentials(monkeypatch):
    import httpx

    from hs.errors import ApiError
    from hs.plivo import rest

    msg = (
        "Access denied. The request originated from IP 203.0.113.9, which is not in the IP whitelist for this account."
    )
    monkeypatch.setattr(httpx, "request", lambda *a, **k: httpx.Response(403, json={"error": msg}))
    with pytest.raises(ApiError) as e:
        rest.ok(("MA1", "tok"), "POST", "Endpoint/", username="a")
    assert (e.value.status, e.value.code, e.value.details["errors"]["error"]) == (502, "plivo_error", msg)


TOKEN = "FakeTokenForTestsOnly0123456789abcdefABC"


@pytest.mark.parametrize(
    "body",
    [
        {"error": f"bad token {TOKEN}"},
        {"error": "bad token short-1"},
        {TOKEN: "bad token"},
        {"error": [{"api_key": "private", "signature": "private"}]},
        {"api_key": "private", "signature": "private"},
    ],
)
def test_a_plivo_reason_never_shows_a_credential(monkeypatch, caplog, body):
    import httpx

    from hs.errors import ApiError
    from hs.plivo import rest

    monkeypatch.setattr(httpx, "request", lambda *a, **k: httpx.Response(400, json=body))
    with caplog.at_level("WARNING", logger="human-specialist"), pytest.raises(ApiError) as e:
        rest.ok(("MA1", TOKEN if "short-1" not in str(body) else "short-1"), "GET")
    shown = str(e.value.details) + caplog.text
    assert e.value.details["errors"] == {} and all(
        s not in shown for s in (TOKEN[:20], TOKEN[20:], "short-1", "private")
    )


def test_a_plivo_refusal_that_names_the_app_or_a_url_is_relayed(monkeypatch):
    import httpx

    from hs.errors import ApiError
    from hs.plivo import rest

    body = {
        "app_name": ["hs-3f2a9c1e-7b4d-4e8a-9c0f-1a2b3c4d5e6f already exists"],
        "answer_url": "https://humancontactcenterproduction.example.com/plivo/3f2a9c1e-7b4d-4e8a-9c0f-1a2b3c4d5e6f/answer is down",
    }
    monkeypatch.setattr(httpx, "request", lambda *a, **k: httpx.Response(400, json=body))
    with pytest.raises(ApiError) as e:
        rest.ok(("MA1", TOKEN), "POST", "Application/")
    assert e.value.details["errors"] == body


def test_a_plivo_refusal_of_any_json_shape_is_still_logged_once(monkeypatch, caplog):
    import httpx

    from hs.errors import ApiError
    from hs.plivo import rest

    monkeypatch.setattr(httpx, "request", lambda *a, **k: httpx.Response(400, json=[[1, "refused"]]))
    with caplog.at_level("WARNING", logger="human-specialist"), pytest.raises(ApiError) as e:
        rest.ok(("MA1", "tok"), "GET")
    assert e.value.details["errors"] == {} and len(caplog.records) == 1


def test_a_refused_number_lookup_logs_no_credential(monkeypatch, caplog):
    import httpx

    from hs.plivo import numbers

    body = {"error": f"not found for {TOKEN}", "signature": "private", "message": "Number not found"}
    monkeypatch.setattr(numbers, "read", lambda *a, **k: httpx.Response(404, json=body))
    with caplog.at_level("ERROR", logger="human-specialist"), pytest.raises(numbers.NumberServiceError):
        numbers.get_plivo_number("MA1", TOKEN, "+14155550100")
    assert "Number not found" in caplog.text and TOKEN not in caplog.text and "private" not in caplog.text
