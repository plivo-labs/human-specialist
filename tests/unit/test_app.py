import json
import logging
import re
import time
import timeit
import uuid
from logging.handlers import QueueHandler
from types import SimpleNamespace

import httpx
import pytest
from fastapi import APIRouter
from fastapi.testclient import TestClient
from prometheus_client import REGISTRY
from sqlalchemy import create_engine

from hs import access, core, events
from hs import app as service
from tests.harness.sim import serve


@pytest.fixture
def plugins(monkeypatch):
    """Install plugins as `{name: setup}`; the app's routes and the hook registries are restored afterwards."""
    monkeypatch.setattr(service.app.router, "routes", list(service.app.router.routes))
    monkeypatch.setattr(events, "HOOKS", {})
    monkeypatch.setattr(access, "RESOLVERS", [])

    def install(setups: dict) -> None:
        found = [SimpleNamespace(name=name, load=lambda s=setup: s) for name, setup in setups.items()]
        monkeypatch.setattr(service, "entry_points", lambda group: found if group == "hs.plugins" else [])

    return install


@pytest.fixture
def written(monkeypatch) -> list[str]:
    """The lines the service's log handler writes, captured instead of queued for stdout."""
    handler = next(h for h in logging.getLogger().handlers if isinstance(h, QueueHandler))
    lines = []
    monkeypatch.setattr(handler, "enqueue", lambda record: lines.append(record.msg))
    return lines


def test_a_logged_exception_writes_no_token(written):
    try:
        raise RuntimeError("GET /api/v1/events?access_token=s3cr3t-a failed")
    except RuntimeError:
        logging.getLogger("hs.test").exception("request %s", {"authorization": "Bearer s3cr3t-b"})
    line = json.loads(written[-1])
    assert "s3cr3t" not in written[-1], written[-1]
    assert "access_token=[REDACTED]" in line["error"] and "Bearer [REDACTED]" in line["message"]


@pytest.mark.parametrize(
    "logged",
    [
        "GET /api/v1/events?access%5Ftoken=hs_at_c2VjcmV0-_x HTTP/1.1",
        "context {'note': 'hs_tk_c2VjcmV0-_x'}",
        '{"Password": "c2VjcmV0 two words"}',
        "X-Plivo-Signature-V3: c2VjcmV0",
        "Authorization: Basic c2VjcmV0",
        "Authorization: Bearer " + "eyJ" + "x" * 1489 + "c2VjcmV0",
        'password="' + "y" * 300 + 'c2VjcmV0"',
        "password     = c2VjcmV0",
        "callback https://alice:c2VjcmV0@hs.example/plivo/answer refused",
        "https://bucket.example/a.mp3?X-Amz-Credential=c2VjcmV0%2F20261112&X-Amz-Security-Token=c2VjcmV0",
    ],
    ids=[
        "encoded-name",
        "dict-repr",
        "password",
        "header",
        "basic",
        "long-jwt",
        "long-quoted",
        "spaced",
        "url-user-and-password",
        "aws-signed-url",
    ],
)
def test_a_credential_is_redacted_wherever_it_appears(written, logged):
    logging.getLogger("hs.test").warning("seen %s", logged)
    assert "c2VjcmV0" not in written[-1] and "[REDACTED]" in written[-1], written[-1]


CREDENTIAL_NAMES = ["signature", "password", "secret", "api_key", "api-key", "access_token", "sig", "token"]
CREDENTIAL_NAMES += ["credential", "key", "X-Api-Key", "X-Amz-Credential", "X-Amz-Security-Token"]


def spellings(name: str) -> list[str]:
    """`name` as written and in capitals, wholly percent-encoded, and with each one of its characters encoded."""
    codes = [f"%{ord(c):02x}" for c in name]
    whole = [name, name.upper(), "".join(codes).upper()]
    return whole + [name[:i] + code + name[i + 1 :] for i, code in enumerate(codes)]


def test_every_credential_name_hides_its_value_however_it_is_spelled():
    forms = ("{}: c2VjcmV0", "GET /a?exp=9&{}=c2VjcmV0&v=1", "{{'{}': 'c2VjcmV0'}}")
    lines = [form.format(s) for name in CREDENTIAL_NAMES for s in spellings(name) for form in forms]
    assert [line for line in lines if "c2VjcmV0" in core.redact(line)] == []


@pytest.mark.parametrize("line", ["Idempotency-Key: retry-123", "idempotency_key=retry-123", "key_id=k1", "signal=go"])
def test_a_name_that_holds_no_credential_keeps_its_value(line):
    assert core.redact(line) == line


def test_an_outbound_request_line_is_not_logged(written):
    logging.getLogger("httpx").info("HTTP Request: GET %s", "https://media.plivo.com/v1/Account/MA1/Recording/r1.mp3")
    assert not any("Recording" in line for line in written), written


def test_a_request_to_a_signed_url_logs_no_signature(written):
    base, stop = serve(service.app)
    try:
        httpx.get(f"{base}/api/v1/recordings/{uuid.uuid4()}/audio", params={"exp": 1, "v": "a", "sig": "c2VjcmV0"})
        httpx.get(f"{base}/api/v1/events/stream", params={"access_token": "c2VjcmV0", "agent": "a1"})
    finally:
        stop()
    assert any("/audio" in line for line in written) and not any("c2VjcmV0" in line for line in written), written
    assert any("access_token=[REDACTED]&agent=a1" in line for line in written), written


def test_an_exception_note_carrying_a_token_is_redacted(written):
    error = RuntimeError("callback failed")
    error.add_note("retry with hs_st_c2VjcmV0-_x")
    try:
        raise error
    except RuntimeError:
        logging.getLogger("hs.test").exception("failed")
    assert "c2VjcmV0" not in written[-1] and "hs_st_[REDACTED]" in json.loads(written[-1])["error"], written[-1]


def test_redacting_hostile_lines_takes_linear_time(written):
    """Twice the input takes about twice as long; a pattern that backtracks across the line takes four times. The runs
    count CPU time, not wall time, so other processes on a loaded machine cannot stretch one size's runs."""
    log = logging.getLogger("hs.test")

    def took(n: int) -> float:
        lines = ["signature" * n, "signature_" * n, "signature='" * n, "bearer " * n, "hs_tk_" * n]
        lines += ["token" * n, "key-" * n, "api-key" * n, "idempotency-key" * n]
        lines += ["Bearer " + "x" * 9 * n, 'password="' + "A" * 9 * n, "password=" + "B" * 9 * n, "://" + "C" * 9 * n]
        return timeit.timeit(lambda: [log.warning("%s", text) for text in lines], number=1, timer=time.process_time)

    small, large = map(min, zip(*[(took(5000), took(10000)) for _ in range(7)], strict=True))
    assert large < 3 * small and len(written) == 13 * 14, (small, large)


def test_a_database_that_does_not_answer_is_a_503(monkeypatch):
    monkeypatch.setattr(core, "engine", create_engine("postgresql://hs:hs@127.0.0.1:1/hs"))
    operator = {"Authorization": f"Bearer {core.settings.operator_key}"}
    answer = TestClient(service.app, raise_server_exceptions=False).get("/api/v1/tenants", headers=operator)
    assert (answer.status_code, answer.json()["error"]["code"]) == (503, "dependency_unavailable"), answer.text


def test_a_request_that_finds_every_connection_busy_answers_503_once_the_pool_timeout_passes(monkeypatch):
    spent = create_engine(core.engine.url, pool_size=1, max_overflow=0, pool_timeout=0.2)
    monkeypatch.setattr(core, "engine", spent)
    operator = {"Authorization": f"Bearer {core.settings.operator_key}"}
    waited_before = REGISTRY.get_sample_value("hs_db_checkout_seconds_sum") or 0.0
    with spent.connect():
        start = time.monotonic()
        answer = TestClient(service.app, raise_server_exceptions=False).get("/api/v1/tenants", headers=operator)
        took = time.monotonic() - start
    spent.dispose()
    assert (answer.status_code, answer.json()["error"]["code"]) == (503, "dependency_unavailable"), answer.text
    assert took < 2, took
    assert REGISTRY.get_sample_value("hs_db_checkout_seconds_sum") - waited_before >= 0.2


@pytest.mark.parametrize(
    ("sent", "kept"),
    [("req-1.A_b", True), ("access_token=s3cr3t", False), ("a\nforged line", False), ("x" * 65, False)],
    ids=["safe", "token", "newline", "too-long"],
)
def test_a_request_id_is_echoed_only_when_short_and_safe(sent, kept):
    rid = TestClient(service.app).get("/status", headers={"X-Request-Id": sent}).headers["X-Request-Id"]
    assert (rid == sent) == kept and re.fullmatch(r"[A-Za-z0-9._-]{1,64}", rid)


def test_the_app_boots_and_status_answers():
    with TestClient(service.app) as client:
        assert client.get("/status").json() == {"db": True}


def test_a_plugin_gets_its_three_hooks(plugins):
    router = APIRouter()
    router.get("/extra/ping")(lambda: {"pong": True})

    def on_event(event: dict) -> None:
        return None

    def resolve(request) -> None:
        return None

    def setup(hs) -> None:
        hs.on_event(on_event)
        hs.add_resolver(resolve)
        hs.app.include_router(router)

    plugins({"acme": setup})
    with TestClient(service.app) as client:
        assert client.get("/extra/ping").json() == {"pong": True}
    assert {f"acme:{__name__}.{on_event.__qualname__}": on_event} == events.HOOKS
    assert [resolve] == access.RESOLVERS


def test_a_plugin_whose_setup_raises_stops_startup(plugins, caplog):
    def setup(hs) -> None:
        raise RuntimeError("bad plugin config")

    plugins({"broken": setup})
    with pytest.raises(RuntimeError, match="bad plugin config"), TestClient(service.app):
        pass
    assert "plugin broken failed to set up" in caplog.text
