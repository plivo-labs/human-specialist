"""What a plugin may rely on from the three hooks, through the plugin loader as an adapter uses it."""

import json
import threading

from fastapi import APIRouter
from fastapi.testclient import TestClient
from sqlalchemy import insert

from hs import access, core, events, jobs, plivo
from hs import app as service
from hs.access import Principal
from hs.core import T
from hs.events import webhooks
from tests.unit.test_app import plugins  # noqa: F401
from tests.unit.test_events import agent, call, doc, emit, key, stamped, tenant, tokens, until

CALLS = access.require("key:calls")


def webhook(tenant_id: str) -> None:
    with core.tx() as db:
        db.execute(
            insert(T.webhooks).values(
                tenant_id=tenant_id, url="https://x.test", secret=plivo.encrypt("s"), event_types=[]
            )
        )


def test_on_event_reaches_the_plugin_in_order_and_a_failing_call_delays_neither_webhook_nor_stream(
    plugins,  # noqa: F811
    monkeypatch,
):
    monkeypatch.setattr(events, "HOOK_RETRY", (0, 0))
    t, tries, seen, hooked, release = tenant(), [], [], [], threading.Event()
    asked = []

    def on_event(e: dict) -> None:
        if e["tenant_id"] != t:
            return
        if e["id"] not in asked:
            asked.append(e["id"])
        if len(asked) > 1 and e["id"] == asked[1] and not release.is_set():
            tries.append(e["id"])
            raise RuntimeError("platform down")
        seen.append(e["id"])

    async def post(url: str, body: bytes, headers: dict) -> int:
        hooked.append(json.loads(body)["id"])
        return 200

    monkeypatch.setattr(webhooks, "post", post)
    plugins({"acme": lambda hs: hs.on_event(on_event)})
    webhook(t)
    with TestClient(service.app):
        for _ in range(5):
            emit(t)
        assert until(lambda: len(stamped(t)) == 5, 10), "the relay stamps all five"
        ids = [e["id"] for e in stamped(t)]
        assert until(lambda: len(seen) == 4 and len(hooked) == 5, 10), (seen, hooked)
        assert len(tries) >= 1 + len(events.HOOK_RETRY), "a raising call is retried in place"
        assert seen == [i for i in ids if i != asked[1]], "the rest follow in seq order, once each"
        assert sorted(hooked) == sorted(ids) and [e["id"] for e in events.read(t, 0, 50)[0]] == ids
        release.set()
        name = next(iter(events.HOOKS))
        jobs.run_now("hook", f"{name}:{asked[1]}", wait=True)
        assert until(lambda: asked[1] in seen, events.RETRIES[0] + 20)
    assert seen[-1] == asked[1] and sorted(seen) == sorted(ids), "the parked event is replayed once"


def principals(t: str, role: str, agent_id: str):
    return lambda request: (
        Principal(t, "agent", agent_id=agent_id, role=role) if request.headers.get("x-sso") == "sso" else None
    )


def listed(client: TestClient, **headers) -> list[str]:
    r = client.get("/api/v1/conversations", headers=headers)
    assert r.status_code == 200, r.text
    return [c["id"] for c in r.json()["data"]]


def test_a_resolver_principal_sees_what_an_hs_token_of_its_role_sees_over_http_and_on_the_stream(plugins):  # noqa: F811
    t = tenant()
    floor = doc(t, "team", name="Floor")
    sup, me, mate, away = (
        agent(t, "supervisor", teams=(floor,)),
        agent(t, teams=(floor,)),
        agent(t, teams=(floor,)),
        agent(t),
    )
    for who in (me, mate, away, sup):
        call(t, on=who)
    plugins({})
    client = TestClient(service.app)
    for who, role in ((me, "agent"), (sup, "supervisor")):
        access.RESOLVERS[:] = []
        service.Host("sso").add_resolver(principals(t, role, who))
        hs_token = tokens(t, who)
        expected = listed(client, Authorization=f"Bearer {hs_token.agent_token}")
        assert listed(client, Authorization="Bearer an-sso-token", **{"X-SSO": "sso"}) == expected
        stream = events.authorize("an-sso-token", type("R", (), {"headers": {"x-sso": "sso"}})())
        assert stream.scope == events.authorize(hs_token.stream_token, None).scope
    assert len(listed(client, Authorization=f"Bearer {tokens(t, me).agent_token}")) == 1 < len(expected)


def test_a_resolver_that_declines_leaves_the_caller_to_hs_auth(plugins):  # noqa: F811
    t, asked = tenant(), []
    plugins({})
    access.RESOLVERS.append(lambda request: asked.append(1))
    client = TestClient(service.app)
    own = key(t, ("calls",))["api_key"]
    assert client.get("/api/v1/conversations", headers={"Authorization": f"Bearer {own}"}).status_code == 200
    assert asked == [], "an HS credential never reaches a resolver"
    r = client.get("/api/v1/conversations", headers={"Authorization": "Bearer unknown"})
    assert (r.status_code, r.json()["error"]["code"], asked) == (401, "unauthorized", [1])


def test_a_plugin_route_is_refused_without_a_credential_and_cannot_shadow_an_hs_route(plugins):  # noqa: F811
    t = tenant()
    router = APIRouter()
    router.get("/acme/whoami", dependencies=[CALLS])(lambda: {"ok": True})
    router.get("/api/v1/conversations")(lambda: {"shadow": True})
    router.get("/status")(lambda: {"shadow": True})
    plugins({"acme": lambda hs: hs.app.include_router(router)})
    with TestClient(service.app) as client:
        r = client.get("/acme/whoami")
        assert (r.status_code, r.json()["error"]["code"]) == (401, "unauthorized")
        mine = {"Authorization": f"Bearer {key(t, ('calls',))['api_key']}"}
        assert client.get("/acme/whoami", headers=mine).json() == {"ok": True}
        assert client.get("/api/v1/conversations", headers=mine).json() == {"data": [], "next_cursor": None}
        assert client.get("/status").json() == {"db": True}
