import inspect
from dataclasses import fields
from pathlib import Path

from hs import access, app, core, events, plivo


def test_host_offers_the_three_hooks():
    assert isinstance(app.Host.app, property)
    assert list(inspect.signature(app.Host.on_event).parameters) == ["self", "fn"]
    assert list(inspect.signature(app.Host.add_resolver).parameters) == ["self", "fn"]


def test_principal_fields():
    names = ["tenant_id", "kind", "key_id", "agent_id", "role", "scopes", "credential_id"]
    assert [f.name for f in fields(access.Principal)] == names


def test_require_takes_grants_and_returns_a_dependency():
    assert list(inspect.signature(access.require).parameters) == ["grants"]
    assert access.require("op").dependency is not None


def test_api_error_takes_status_code_message_details():
    assert list(inspect.signature(core.ApiError).parameters) == ["status", "code", "message", "details"]


def test_voice_signed_is_an_async_dependency_of_a_request():
    assert inspect.iscoroutinefunction(plivo.voice_signed)
    assert list(inspect.signature(plivo.voice_signed).parameters) == ["request"]


def test_event_envelope_keeps_its_documented_fields():
    selected = events.ENVELOPE.split(" AS created_at, ")[1].split(", ")
    assert ["id", "type", "created_at", *selected] == [
        "id",
        "type",
        "created_at",
        "tenant_id",
        "conversation_id",
        "agent_id",
        "direction",
        "context",
        "data",
    ]


def _snippet(marker: str) -> str:
    doc = Path(__file__).parents[2].joinpath("docs/plugins.md").read_text()
    blocks = (b.split("```", 1)[0] for b in doc.split("```python\n")[1:])
    return next(b for b in blocks if marker in b)


def test_the_documented_resolver_and_protected_route_run():
    from types import SimpleNamespace

    from fastapi import APIRouter, FastAPI
    from fastapi.testclient import TestClient

    resolvers = []
    ns = {"hs": SimpleNamespace(add_resolver=resolvers.append)}
    exec(_snippet("def sso"), ns)  # noqa: S102
    request = lambda auth: SimpleNamespace(headers={"authorization": auth})  # noqa: E731
    assert resolvers[0](request("Bearer my-sso-token")).role == "agent"
    assert resolvers[0](request("nope")) is None

    ns = {"router": APIRouter()}
    exec(_snippet("access.require"), ns)  # noqa: S102
    probe = FastAPI()
    core.install(probe)
    probe.include_router(ns["router"])
    assert TestClient(probe).get("/mine").status_code == 401
