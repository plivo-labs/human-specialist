import base64
import secrets
import threading
import uuid
from types import SimpleNamespace
from urllib.parse import quote

import httpx
import pytest
from fastapi import Depends, FastAPI, Request
from fastapi.testclient import TestClient
from plivo.utils import validate_signature
from plivo.utils.signature_v3 import validate_v3_signature
from sqlalchemy import insert, select, text, update

from hs import core, jobs, plivo
from hs import text as messages
from hs.text import plivo as text_plivo

TOKEN = "sim-auth-token-0123456789"
HEX, PW = "ab0123456789abcdef0123456789abcd", "abcdefghijklmnop"
REASON = {"error": "Voice is disabled on this sub-account.", "to": ["+14155550100 is not a valid number"]}


@pytest.fixture
def tenant():
    row = {
        "name": "Acme",
        "plivo_auth_id": f"SA{secrets.token_hex(9).upper()}",
        "plivo_token_enc": plivo.encrypt(TOKEN),
    }
    with core.tx() as db:
        return db.execute(insert(core.T.tenant).values(**row).returning(*core.T.tenant.c)).one()


@pytest.fixture
def ops(monkeypatch):
    """Register a test op: `register(name, **op fields)`; returns the calls each callback received."""
    calls = {"send": [], "done": [], "failed": [], "uncertain": []}

    def register(name: str, create: bool, send) -> None:
        def record(kind):
            return lambda *args: calls[kind].append(args)

        def sending(tenant, payload):
            calls["send"].append(payload)
            return send(tenant, payload)

        op = plivo.Op(create, sending, record("done"), record("failed"), record("uncertain"))
        monkeypatch.setitem(plivo.OPS, name, op)

    register.calls = calls
    return register


def command(tenant, op: str, subject: str, **kw) -> str:
    with core.tx() as db:
        return plivo.command(db, op, subject, {"tenant_id": str(tenant.id)}, **kw)


def job(key: str):
    J = core.T.jobs
    with core.read() as db:
        return db.execute(
            select(J, J.c.locked_until.is_not_distinct_from(jobs.INFINITY).label("parked")).where(
                J.c.kind == "plivo", J.c.key == key
            )
        ).first()


def answering(monkeypatch, respond) -> None:
    monkeypatch.setattr(plivo, "HTTP", httpx.Client(transport=httpx.MockTransport(respond)))


def raising(error: type[httpx.TransportError]):
    def respond(request):
        raise error("simulated", request=request)

    return respond


@pytest.mark.parametrize(
    ("method", "respond", "expected"),
    [
        ("POST", lambda r: httpx.Response(201, json={"ok": 1}), {"ok": 1}),
        ("DELETE", lambda r: httpx.Response(404), {"gone": True}),
        ("POST", lambda r: httpx.Response(400), (plivo.Refused, 422, "plivo_refused")),
        ("GET", lambda r: httpx.Response(401), (plivo.Refused, 422, "plivo_credentials_rejected")),
        (
            "POST",
            lambda r: httpx.Response(429, headers={"Retry-After": "7"}),
            (plivo.Refused, 429, "carrier_rate_limited"),
        ),
        ("POST", raising(httpx.ConnectError), (plivo.Refused, 503, "plivo_unavailable")),
        ("POST", lambda r: httpx.Response(502), (plivo.Uncertain,)),
        ("POST", raising(httpx.ReadTimeout), (plivo.Uncertain,)),
        ("POST", raising(httpx.RemoteProtocolError), (plivo.Uncertain,)),
    ],
    ids=["2xx", "delete-404", "4xx", "401", "429", "connect", "5xx", "read-timeout", "reset"],
)
def test_each_answer_class_is_a_reply_a_refusal_or_uncertain(monkeypatch, tenant, method, respond, expected):
    answering(monkeypatch, respond)
    if isinstance(expected, dict):
        assert plivo.rest(tenant, method, "Call/") == expected
        return
    with pytest.raises(expected[0]) as raised:
        plivo.rest(tenant, method, "Call/")
    if expected[0] is plivo.Refused:
        assert (raised.value.status, raised.value.code) == expected[1:]
        assert raised.value.details.get("retry_after", 7) == 7


def test_a_plivo_request_is_bounded_at_ten_seconds(monkeypatch, tenant):
    bounds = []
    answering(monkeypatch, lambda r: bounds.append(r.extensions["timeout"]) or httpx.Response(200, json={}))
    plivo.rest(tenant, "GET", "Call/")
    assert bounds == [dict.fromkeys(("connect", "read", "write", "pool"), 10)]


def test_a_refusal_relays_plivos_reason_minus_every_credential_the_request_sent(monkeypatch, tenant, caplog):
    password, basic = "pw-0123456789abcdef", base64.b64encode(f"{tenant.plivo_auth_id}:{TOKEN}".encode()).decode()
    reason = {"api_id": "a1", "error": "Voice is disabled on this sub-account.", "alias": ["Invalid alias"]}
    echoed = {"auth_token": "x", "message": f"bad token {TOKEN}", "nested": {"error": "deep"}}
    echoed |= {"username": f"{password} is taken", "hint": "retry with signature=abc"}

    def echo(request):
        header = request.headers["authorization"]
        return httpx.Response(403, json=reason | echoed | {"header": f"sent {header}", "basic": header.split()[1]})

    answering(monkeypatch, echo)
    with pytest.raises(plivo.Refused) as raised:
        plivo.rest(tenant, "POST", "Endpoint/", username="hsagent", password=password)
    assert (raised.value.status, raised.value.code) == (422, "plivo_refused")
    errors = {"error": "Voice is disabled on this sub-account.", "alias": ["Invalid alias"], "basic": "[REDACTED]"}
    errors |= {"message": "[REDACTED]", "username": "[REDACTED]"}
    errors |= {"hint": "retry with signature=[REDACTED]", "header": "[REDACTED]"}
    assert raised.value.details == {"status": 403, "errors": errors}
    assert "Voice is disabled" in caplog.text and not any(s in caplog.text for s in (TOKEN, password, basic))


@pytest.mark.parametrize(
    ("params", "echo"),
    [
        ({}, {"error": f"bad token {TOKEN.upper()}"}),
        ({}, {"error": "bad token " + "".join(f"%{b:02X}" for b in TOKEN.encode())}),
        ({"password": "pw 0123456789"}, {"error": "bad password pw+0123456789"}),
        ({}, {"error": "bad token " + TOKEN.replace("a", "%2541")}),
        ({}, {"error": "bad token " + TOKEN.replace("a", "%" + "25" * 9 + "41")}),
        ({"PASSWORD": "pw-0123456789"}, {"error": "bad password pw-0123456789"}),
        ({"pin_token": 1234567890123}, {"error": "bad pin 1234567890123"}),
        ({"password": HEX}, {"error": f"bad password %{HEX}"}),
        ({"password": HEX[:18]}, {"error": f"bad password %25%{ord(HEX[0]):X}{HEX[1:18]}"}),
    ],
    ids=[
        "case-changed",
        "percent-encoded",
        "plus-encoded",
        "nested-percent-escape",
        "encoded-past-three-decodes",
        "uppercase-param-name",
        "non-string-value",
        "after-a-percent-sign",
        "merged-mid-decode",
    ],
)
def test_a_credential_echoed_in_another_form_is_neither_relayed_nor_logged(monkeypatch, tenant, caplog, params, echo):
    answering(monkeypatch, lambda r: httpx.Response(403, json=echo))
    with pytest.raises(plivo.Refused) as raised:
        plivo.rest(tenant, "POST", "Call/", **params)
    assert raised.value.details == {"status": 403, "errors": dict.fromkeys(echo, "[REDACTED]")}
    assert "answered 403" in caplog.text and not any(message in caplog.text for message in echo.values())


def test_a_field_whose_name_holds_a_credential_is_dropped(monkeypatch, tenant, caplog):
    echo = {PW: "invalid value", f"x_{PW}": "invalid value", "error": "Voice is disabled on this sub-account."}
    answering(monkeypatch, lambda r: httpx.Response(403, json=echo))
    with pytest.raises(plivo.Refused) as raised:
        plivo.rest(tenant, "POST", "Endpoint/", password=PW)
    assert raised.value.details == {"status": 403, "errors": {"error": echo["error"]}}
    assert "answered 403: {'error': 'Voice is disabled" in caplog.text and PW not in caplog.text


URL = "https://alice:example-password@callbacks.example/answer"


@pytest.mark.parametrize(
    ("echo", "relayed"),
    [
        (lambda auth_id: f"Account {auth_id} is not enabled", "[REDACTED]"),
        (lambda auth_id: f"Account {auth_id.lower()} is not enabled", "[REDACTED]"),
        (lambda _: f"Invalid callback URL {URL}", "Invalid callback URL https://[REDACTED]@callbacks.example/answer"),
        (lambda _: f"Invalid {quote(URL, safe='')}", "[REDACTED]"),
        (lambda _: "Rejected access_token%3Dother-secret-0123456789", "[REDACTED]"),
    ],
    ids=["auth-id", "auth-id-case-changed", "url-user-and-password", "url-encoded-user-and-password", "encoded-token"],
)
def test_a_reason_relays_neither_the_auth_id_nor_a_urls_user_and_password(monkeypatch, tenant, caplog, echo, relayed):
    message = echo(tenant.plivo_auth_id)
    answering(monkeypatch, lambda r: httpx.Response(403, json={"error": message}))
    with pytest.raises(plivo.Refused) as raised:
        plivo.rest(tenant, "POST", "Call/")
    assert raised.value.details == {"status": 403, "errors": {"error": relayed}}
    assert "answered 403" in caplog.text and not any(s in caplog.text for s in (message, tenant.plivo_auth_id, "alice"))


def test_a_signed_audio_url_echoed_in_a_refusal_keeps_none_of_its_credentials(monkeypatch, tenant, caplog):
    signed = "https://cdn.example/hold.mp3?exp=9&token={}&key={}&X-Amz-Credential={}&X-Amz-Security-Token={}"
    sent = [f"c2VjcmV0{i}" for i in range(4)]
    answering(monkeypatch, lambda r: httpx.Response(400, json={"error": f"Invalid url {signed.format(*sent)}"}))
    with pytest.raises(plivo.Refused) as raised:
        plivo.rest(tenant, "POST", "Call/", answer_url=signed.format(*sent))
    assert raised.value.details["errors"] == {"error": f"Invalid url {signed.format(*['[REDACTED]'] * 4)}"}
    lines = [core._Json().format(r) for r in caplog.records]
    assert any("exp=9" in line for line in lines) and not any(s in line for line in lines for s in sent), lines


@pytest.mark.parametrize(
    ("method", "params", "echo"),
    [
        ("POST", {}, {"error": f"bad token {TOKEN[:12]}", "message": f"{TOKEN[12:]} expired"}),
        ("POST", {"password": PW}, {PW[:8]: f"{PW[8:]} is invalid"}),
        ("POST", {"password": PW}, {PW[:8]: "taken", PW[8:]: "taken"}),
        ("POST", {"password": "pw-123"}, REASON),
        ("POST", {"password": "ßßßß"}, REASON),
        ("POST", {"password": ["abcdefgh1234"]}, {"error": 'bad password ["abcdefgh1234"]'}),
        ("POST", {"password": {"pin": "abcdefgh1234"}}, {"error": 'bad password {"pin":"abcdefgh1234"}'}),
        ("POST", {"password": True}, REASON),
        ("POST", {"password": None}, REASON),
        ("GET", {"password": b"abcdefgh1234"}, {"error": "bad password abcdefgh1234"}),
    ],
    ids=[
        "split-across-messages",
        "split-across-a-name-and-its-message",
        "split-across-names",
        "short",
        "short-before-casefolding",
        "list-value",
        "dict-value",
        "bool-value",
        "none-value",
        "bytes-value",
    ],
)
def test_no_reason_is_relayed_when_it_could_spell_a_credential_or_one_cannot_be_looked_for(
    monkeypatch, tenant, caplog, method, params, echo
):
    answering(monkeypatch, lambda r: httpx.Response(403, json=echo))
    with pytest.raises(plivo.Refused) as raised:
        plivo.rest(tenant, method, "Endpoint/", **params)
    assert raised.value.details == {"status": 403, "errors": {}}
    assert f"plivo {method} Endpoint/ answered 403: {{}}" in caplog.text


@pytest.mark.parametrize("password", ["", 0.123456789], ids=["empty", "float-value"])
def test_an_empty_credential_or_a_long_number_hides_no_reason(monkeypatch, tenant, caplog, password):
    answering(monkeypatch, lambda r: httpx.Response(403, json=REASON))
    with pytest.raises(plivo.Refused) as raised:
        plivo.rest(tenant, "POST", "Endpoint/", password=password)
    assert raised.value.details == {"status": 403, "errors": REASON}
    assert "answered 403: {'error': 'Voice is disabled" in caplog.text


@pytest.mark.parametrize(("status", "code"), [(401, "plivo_credentials_rejected"), (429, "carrier_rate_limited")])
def test_a_401_and_a_429_keep_plivos_reason_under_their_own_code(monkeypatch, tenant, status, code):
    answering(monkeypatch, lambda r: httpx.Response(status, headers={"Retry-After": "7"}, json={"error": "why"}))
    with pytest.raises(plivo.Refused) as raised:
        plivo.rest(tenant, "POST", "Call/")
    retry = {"retry_after": 7} if status == 429 else {}
    assert (raised.value.code, raised.value.details) == (code, {"status": status, "errors": {"error": "why"}} | retry)


def test_a_create_sends_once_and_its_verdict_reaches_the_caller(ops, tenant):
    ops("t_dial", create=True, send=lambda t, p: {"request_uuid": "r1"})
    key = command(tenant, "t_dial", "a1", wait=True)
    assert jobs.run_now("plivo", key, wait=True) is None
    assert len(ops.calls["send"]) == 1 and ops.calls["done"][0][1] == {"request_uuid": "r1"} and job(key) is None

    def refuse(t, p):
        raise plivo.Refused(422, "plivo_refused", "Plivo refused the request")

    ops("t_dial", create=True, send=refuse)
    key = command(tenant, "t_dial", "a2", wait=True)
    verdict = jobs.run_now("plivo", key, wait=True)
    assert isinstance(verdict, plivo.Refused) and ops.calls["failed"][0][1] is verdict and job(key).parked
    ops("t_dial", create=True, send=lambda t, p: {"request_uuid": "r2"})
    assert jobs.run_now("plivo", command(tenant, "t_dial", "a2", wait=True), wait=True) is None
    assert ops.calls["done"][-1][1] == {"request_uuid": "r2"}, "a refused create commanded again is sent"


def test_a_text_send_another_worker_holds_hands_its_refusal_to_the_waiting_agent(ops, tenant):
    started, release = threading.Event(), threading.Event()

    def refuse(t, p):
        started.set()
        release.wait(5)
        raise plivo.Refused(422, "plivo_refused", "Plivo refused the request")

    ops("t_text", create=True, send=refuse)
    key = command(tenant, "t_text", "e1", wait=True)
    holder = threading.Thread(target=jobs.run_now, args=("plivo", key), kwargs={"wait": True})
    holder.start()
    assert started.wait(5)
    threading.Timer(0.2, release.set).start()
    with pytest.raises(core.ApiError) as refused:
        messages.verdict(key)
    holder.join(5)
    assert refused.value.code == "message_refused"


def test_a_create_claimed_twice_runs_uncertain_and_is_never_sent_again(ops, tenant):
    ops("t_ring", create=True, send=lambda t, p: {})
    key = command(tenant, "t_ring", "ring-1", wait=True)
    with core.tx() as db:
        J = core.T.jobs
        died = {"attempts": 1, "locked_until": text("now() - interval '1 second'")}
        db.execute(update(J).where(J.c.kind == "plivo", J.c.key == key).values(**died))
    jobs.run_now("plivo", key, wait=True)
    assert ops.calls["send"] == [] and len(ops.calls["uncertain"]) == 1 and job(key) is None


def test_a_create_scheduled_again_while_in_flight_is_never_sent_again(ops, tenant):
    started, release = threading.Event(), threading.Event()

    def slow(t, p):
        started.set()
        release.wait(5)
        return {}

    ops("t_record", create=True, send=slow)
    key = command(tenant, "t_record", "c1:1", wait=True)
    stale = threading.Thread(target=jobs.run_now, args=("plivo", key), kwargs={"wait": True})
    stale.start()
    assert started.wait(5)
    command(tenant, "t_record", "c1:1", wait=True)
    release.set()
    stale.join(5)
    jobs.run_now("plivo", key, wait=True)
    assert len(ops.calls["send"]) == 1, "the create was sent again"
    assert len(ops.calls["uncertain"]) == 1 and job(key) is None


def test_an_uncertain_create_is_reconciled_not_retried(ops, tenant):
    def lost(t, p):
        raise plivo.Uncertain("reply lost")

    ops("t_send", create=True, send=lost)
    key = command(tenant, "t_send", "e1", wait=True)
    assert jobs.run_now("plivo", key, wait=True) is None
    assert len(ops.calls["send"]) == 1 and len(ops.calls["uncertain"]) == 1 and job(key) is None


def test_a_convergent_op_retries_then_parks(ops, tenant):
    def lost(t, p):
        raise plivo.Uncertain("reply lost")

    ops("t_hold", create=False, send=lost)
    key = command(tenant, "t_hold", "c1", wait=True)
    for attempt in range(1, 5):
        jobs.run_now("plivo", key, wait=True)
        assert job(key).attempts == attempt and job(key).parked == (attempt == 4)
    assert len(ops.calls["send"]) == 4 and ops.calls["failed"] == ops.calls["uncertain"] == []


def test_a_convergent_op_refused_fails_once_without_retry(ops, tenant):
    def refuse(t, p):
        raise plivo.Refused(422, "plivo_refused", "Plivo refused the request")

    ops("t_kick", create=False, send=refuse)
    key = command(tenant, "t_kick", "m1", wait=True)
    jobs.run_now("plivo", key, wait=True)
    assert jobs.run_now("plivo", key, wait=True) is None
    assert len(ops.calls["failed"]) == len(ops.calls["send"]) == 1 and job(key).parked


def test_a_request_whose_command_was_replaced_before_it_was_sent_succeeds_without_running_the_newer_one(ops, tenant):
    def refuse(t, p):
        raise plivo.Refused(422, "plivo_refused", "Plivo refused the request")

    ops("t_coach", create=False, send=refuse)
    for request in ("a", "b"):
        with core.tx() as db:
            key = plivo.command(db, "t_coach", "m1", {"tenant_id": str(tenant.id), "request": request}, wait=True)
    plivo.verdict(key, request="a")
    assert ops.calls["send"] == [], "the replaced request ran the command that replaced it"
    with pytest.raises(plivo.Refused):
        plivo.verdict(key, request="b")
    assert [p["request"] for p in ops.calls["send"]] == ["b"]


def test_one_provider_id_commanded_by_two_tenants_keeps_each_tenants_command(tenant):
    other, leg = SimpleNamespace(id=uuid.uuid4()), uuid.uuid4().hex
    keys = [command(t, "hangup", leg, wait=True) for t in (tenant, other)]
    assert [job(k).payload["tenant_id"] for k in keys] == [str(tenant.id), str(other.id)]
    with core.tx() as db:
        for key in keys:
            jobs.cancel(db, "plivo", key)


def test_v3_matches_plivos_reference():
    url, nonce = "https://hs.example/plivo/t/mpc-status", "12345678901234567890"
    query, form = [("c", "conv"), ("r", "2")], [("Event", "ParticipantJoin"), ("CallUUID", "cu-1")]
    assert validate_v3_signature(
        "POST", url + "?c=conv&r=2", nonce, TOKEN, plivo.v3(TOKEN, url, query, form, nonce), dict(form)
    )
    assert validate_v3_signature("GET", url, nonce, TOKEN, plivo.v3(TOKEN, url, query, [], nonce), dict(query))
    assert not validate_v3_signature("POST", url, nonce, TOKEN, plivo.v3(TOKEN, url, query, form, nonce), dict(form))


def test_v2_matches_plivos_reference():
    url, nonce = "https://hs.example/plivo/t/message?e=1", "98765432109876543210"
    assert validate_signature(url, nonce, plivo.v2(TOKEN, url, nonce), TOKEN)
    assert not validate_signature(url, nonce, plivo.v2("another-token", url, nonce), TOKEN)


def test_a_voice_callback_needs_plivos_v3_signature_over_its_url_and_form(tenant):
    app = FastAPI()
    core.install(app)
    app.post("/plivo/{tenant_id}/answer")(lambda t=Depends(plivo.voice_signed): {"tenant": str(t.id)})
    client, base = TestClient(app), f"/plivo/{tenant.id}"
    form, nonce = {"CallUUID": "cu-1"}, uuid.uuid4().hex
    signature = plivo.v3(
        TOKEN, f"{core.settings.public_base_url}{base}/answer", [("c", "x")], list(form.items()), nonce
    )
    v3 = {"X-Plivo-Signature-V3": signature, "X-Plivo-Signature-V3-Nonce": nonce}
    assert client.post(f"{base}/answer?c=x", data=form, headers=v3).json() == {"tenant": str(tenant.id)}
    assert client.post(f"{base}/answer?c=y", data=form, headers=v3).status_code == 401


@pytest.fixture
def text_callback():
    """A route on the production text callback router, mounted as the app mounts it. Its work is one event row in a
    transaction of its own, so only the router's dependency can record the nonce."""

    @text_plivo.callbacks.post("/test-message")
    def message(request: Request, fail: bool = False):
        with core.tx() as db:
            db.execute(insert(core.T.events).values(tenant_id=request.path_params["tenant_id"], type="t.msg", data={}))
            if fail:
                raise RuntimeError("the callback failed")
        return {}

    app = FastAPI()
    core.install(app)
    app.include_router(text_plivo.callbacks, prefix="/plivo/{tenant_id}")
    yield TestClient(app, raise_server_exceptions=False)
    text_plivo.callbacks.routes.pop()


def test_a_text_callback_commits_its_nonce_with_its_work_so_a_failed_one_is_retried_and_a_replay_refused(
    tenant, text_callback
):
    url, nonce = f"/plivo/{tenant.id}/test-message", uuid.uuid4().hex
    signed = {"X-Plivo-Signature-V2": plivo.v2(TOKEN, f"{core.settings.public_base_url}{url}", nonce)}
    signed["X-Plivo-Signature-V2-Nonce"] = nonce
    forged = signed | {
        "X-Plivo-Signature-V2": plivo.v2("another-token", f"{core.settings.public_base_url}{url}", nonce)
    }
    assert text_callback.post(url, headers=forged).status_code == 401
    assert text_callback.post(f"{url}?fail=true", headers=signed).status_code == 500
    assert text_callback.post(url, headers=signed).status_code == 200, "Plivo's retry of a failed callback"
    assert text_callback.post(url, headers=signed).status_code == 401, "a replay of one that committed"
    with core.read() as db:
        done = text("SELECT count(*) FROM events WHERE tenant_id = :t AND type = 't.msg'")
        assert db.execute(done, {"t": str(tenant.id)}).scalar() == 1


def test_xml_escapes_text_and_attributes():
    url = plivo.url("t", "actions/menu", menu_id="m", attempt=None)
    body = plivo.xml(("GetDigits", {"action": url, "retries": 1, "redirect": False}, ("Speak", {}, "a < b & c")))
    assert body.body.decode() == (
        '<Response><GetDigits action="http://testserver/plivo/t/actions/menu?menu_id=m#rc=5&amp;ct=2000&amp;rd=1000&amp;rp=5xx,ct"'
        ' retries="1" redirect="false"><Speak>a &lt; b &amp; c</Speak></GetDigits></Response>'
    )
