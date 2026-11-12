import re
from urllib.parse import quote

import pytest

from tests.acceptance.conftest import DAYS, ok

PASSWORD = "round-trip-password"
ADMIN = ("key", "admin")
STAMPS = {"updated_at", "updated_by"}


def _status(row: dict) -> dict:
    return {"status_id": row["status"]["id"]}


def _listed(got: dict) -> list[dict]:
    return got["data"] if "next_cursor" in got else [got]


def _seeded(got: dict) -> list[dict]:
    return [row for row in got["data"] if row.get("default") or row.get("system")]


def _added(got: dict) -> list[dict]:
    return [row for row in got["data"] if not (row.get("default") or row.get("system"))]


def _notes(got: dict) -> list[dict]:
    return got["notes"]


ROUTES = [
    ("/agent-statuses", _seeded, "PATCH", "/agent-statuses/{id}", ADMIN, None, "built-in statuses"),
    ("/agent-statuses", _added, "PATCH", "/agent-statuses/{id}", ADMIN, None, "added statuses"),
    ("/dispositions", _seeded, "PATCH", "/dispositions/{id}", ADMIN, None, "system dispositions"),
    ("/dispositions", _added, "PATCH", "/dispositions/{id}", ADMIN, None, "dispositions"),
    ("/agents", _listed, "PATCH", "/agents/{id}", ADMIN, None, "agents"),
    ("/agents", _listed, "PUT", "/agents/{id}/status", ADMIN, _status, "agent status"),
    ("/me", _listed, "PUT", "/me/status", ("agent",), _status, "my status"),
    ("/business-hours", _listed, "PATCH", "/business-hours/{id}", ADMIN, None, "business hours"),
    ("/holiday-groups", _listed, "PATCH", "/holiday-groups/{id}", ADMIN, None, "holiday groups"),
    ("/menus", _listed, "PATCH", "/menus/{id}", ADMIN, None, "menus"),
    ("/numbers", _listed, "PUT", "/numbers/{number}", ADMIN, None, "numbers"),
    ("/queues", _listed, "PATCH", "/queues/{id}", ADMIN, None, "queues"),
    ("/skills", _listed, "PATCH", "/skills/{id}", ADMIN, None, "skills"),
    ("/teams", _listed, "PATCH", "/teams/{id}", ADMIN, None, "teams"),
    ("/transfer-destinations", _listed, "PATCH", "/transfer-destinations/{id}", ADMIN, None, "contacts"),
    ("/views", _listed, "PATCH", "/views/{id}", ("admin",), None, "views"),
    ("/webhooks", _listed, "PATCH", "/webhooks/{id}", ("key",), None, "webhooks"),
    ("/tasks", _listed, "PATCH", "/tasks/{id}", ("key",), None, "tasks"),
    ("/conversations", _listed, "PATCH", "/conversations/{id}", ("key",), None, "conversations"),
    ("/conversations/{note_on}", _notes, "PATCH", "/conversations/{note_on}/notes/{id}", ("key",), None, "notes"),
    ("/settings", _listed, "PATCH", "/settings", ADMIN, None, "settings"),
    ("/status-map", _listed, "PUT", "/status-map", ADMIN, None, "status map"),
    ("/tenants/{tenant}", _listed, "PATCH", "/tenants/{tenant}", ("op",), None, "tenant"),
]
BUGS = {}
PARAMS = [
    pytest.param(*r[:-1], id=r[-1], marks=[pytest.mark.xfail(reason=BUGS[r[-1]], strict=True)] if r[-1] in BUGS else [])
    for r in ROUTES
]


@pytest.fixture(scope="module")
def org(world):
    t = world.new_tenant("Round Trip Co")
    api, number, after_hours = t.api, t.number(), t.number()
    post = lambda path, body: ok(api.post(f"/api/v1{path}", json=body))  # noqa: E731
    me = post("/agents", {"name": "Ada Admin", "role": "admin", "email": "ada@example.com"})
    post(f"/agents/{me['id']}/password", {"password": PASSWORD})
    general = post("/queues", {"name": "General", "agents": [me["id"]]})
    resolved, *_ = [post("/dispositions", {"name": n}) for n in ("Resolved", "Follow-up needed", "Not resolved")]
    to_general = {"type": "queue", "queue_id": general["id"]}
    entry = {"open_action": to_general, "business_hours_id": None, "closed_action": None}
    ok(api.put(f"/api/v1/numbers/{quote(number)}", json=entry))
    ok(api.patch("/api/v1/settings", json={"agent_direct_dial": True, "default_from": {"call": number}}))

    team, skill = post("/teams", {"name": "Support"})["id"], post("/skills", {"name": "Spanish"})["id"]
    bo = {"name": "Bo Agent", "role": "agent", "email": "bo@example.com", "teams": [team], "caller_id": number}
    bo = post("/agents", bo | {"skills": [{"skill_id": skill, "level": 5}]})
    post(f"/agents/{bo['id']}/password", {"password": PASSWORD})
    post("/agents", {"name": "Sue Super", "role": "supervisor", "teams": [team]})
    post(
        "/queues",
        {"name": "Support", "teams": [team], "skills": [{"skill_id": skill, "level": 3}], "agents": [bo["id"]]},
    )
    post("/dispositions", {"name": "Refund", "parent_disposition_id": resolved["id"]})
    post("/agent-statuses", {"name": "Lunch", "availability": False})
    group = post("/holiday-groups", {"name": "US 2027", "holidays": [{"name": "New Year", "date": "2027-01-01"}]})
    week = [{"day": d, "start": "09:00", "end": "17:00"} for d in DAYS[:5]]
    hours = {"name": "Office", "time_zone": "America/New_York", "slots": week, "holiday_group_id": group["id"]}
    hours = post("/business-hours", hours | {"selected_holidays": ["2027-01-01"]})
    closed = {"type": "hangup", "greeting": {"text": "We are closed"}}
    entry = {"open_action": to_general, "business_hours_id": hours["id"], "closed_action": closed}
    ok(api.put(f"/api/v1/numbers/{quote(after_hours)}", json=entry))
    post("/menus", {"name": "Main", "greeting": {"text": "Press 1"}, "options": {"1": to_general}})
    post("/transfer-destinations", {"name": "Front desk", "number": "+14155550100"})
    post("/webhooks", {"url": world.webhook_sink().url})
    task = post("/tasks", {"type": "task", "title": "Refund follow-up", "target": to_general})
    post(f"/conversations/{task['conversation_id']}/notes", {"text": "Call at 9"})

    def signed_in(email: str):
        body = {"email": email, "password": PASSWORD, "tenant_id": t.id}
        return world.client(ok(world.anon().post("/api/v1/sessions", json=body))["agent_token"])

    clients = {
        "key": api,
        "admin": signed_in("ada@example.com"),
        "agent": signed_in("bo@example.com"),
        "op": world.operator,
    }
    ok(
        clients["admin"].post(
            "/api/v1/views", json={"name": "Missed", "filter": {"answered": False}, "columns": ["number"]}
        )
    )
    return {"clients": clients, "keys": {"tenant": t.id, "note_on": task["conversation_id"]}}


@pytest.fixture(scope="module")
def accepted(world):
    spec = ok(world.anon().get("/openapi.json"))
    shapes = spec["components"]["schemas"]
    out = {}
    for path, ops in spec["paths"].items():
        for method, op in ops.items():
            if ref := op.get("requestBody", {}).get("content", {}).get("application/json", {}).get("schema"):
                shape = shapes[ref["$ref"].rsplit("/", 1)[1]] if "$ref" in ref else ref
                out[method.upper(), re.sub(r"{[^}]+}", "{}", path)] = shape.get("properties") or None
    return out


def _unstamped(row: dict) -> dict:
    return {k: v for k, v in row.items() if k not in STAMPS}


def _same(row: dict):
    key = next((k for k in ("id", "number") if k in row), None)
    return lambda other: key is None or other[key] == row[key]


@pytest.mark.parametrize(("read", "rows", "method", "item", "who", "body"), PARAMS)
def test_a_row_sent_back_as_it_reads_saves_unchanged(org, accepted, read, rows, method, item, who, body):
    read = f"/api/v1{read.format(**org['keys'])}"
    reread = lambda client: rows(ok(client.get(read, params={"limit": 200})))  # noqa: E731
    first = reread(org["clients"][who[0]])
    assert first, "the tenant has a row of this kind"
    fields = accepted[method, re.sub(r"{[^}]+}", "{}", f"/api/v1{item}")]
    for name in who:
        client = org["clients"][name]
        for row in first:
            path = "/api/v1" + item.format(**{k: quote(str(v), safe="") for k, v in row.items()} | org["keys"])
            sent = body(row) if body else {k: v for k, v in row.items() if fields is None or k in fields}
            answer = client.request(method, path, json=sent)
            assert answer.is_success, f"{name} {method} {path} {sent}: {answer.status_code} {answer.text}"
            again = next(filter(_same(row), reread(client)))
            assert _unstamped(again) == _unstamped(row), f"{name} {method} {path} changed the row"
