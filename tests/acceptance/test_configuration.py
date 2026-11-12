"""Configuration resources share one contract: listed, read, patched by merge, deleted for good."""

import pytest

from tests.acceptance.conftest import DAYS, disposition, error, hangup_action, hours, ok, say, skill, team, webhook

RESOURCES = {
    "queues": ({"name": "Sales", "priority": 2}, {"name": "Renamed", "wait_timeout_sec": 90}),
    "business-hours": (
        {
            "name": "Hours",
            "time_zone": "Europe/Paris",
            "slots": [{"day": d, "start": "09:00", "end": "17:00"} for d in DAYS],
        },
        {"name": "Renamed", "time_zone": "Asia/Kolkata"},
    ),
    "menus": ({"name": "Menu", "greeting": say("Hello")}, {"name": "Renamed", "retries": 1}),
    "teams": ({"name": "Sales"}, {"name": "Renamed"}),
    "skills": ({"name": "French"}, {"name": "Renamed"}),
    "agent-statuses": ({"name": "Lunch", "availability": False}, {"name": "Renamed", "availability": True}),
}


@pytest.mark.parametrize("kind", RESOURCES)
def test_a_resource_is_listed_read_patched_and_deleted(world, tenant, kind):
    body, change = RESOURCES[kind]
    path = f"/api/v1/{kind}"

    def listed() -> list[str]:
        return [r["id"] for r in ok(tenant.api.get(path, params={"limit": 200}))["data"]]

    created = ok(tenant.api.post(path, json=body))
    url = f"{path}/{created['id']}"
    assert created["id"] in listed() and ok(tenant.api.get(url)) == created
    patched = ok(tenant.api.patch(url, json=change))
    assert patched == {**created, **change} and ok(tenant.api.get(url)) == patched
    error(world.new_tenant("Other").api.get(url), 404)
    ok(tenant.api.delete(url))
    error(tenant.api.get(url), 404)
    assert created["id"] not in listed()


def _post(t, kind: str, body: dict) -> str:
    return ok(t.api.post(f"/api/v1/{kind}", json=body))["id"]


KEY = {"scopes": ["calls"]}
# Every list and a builder of one more row, returning its id.
LISTS = {
    "queues": lambda w, t, n: t.queue()["id"],
    "business-hours": lambda w, t, n: hours(t)["id"],
    "menus": lambda w, t, n: _post(t, "menus", RESOURCES["menus"][0]),
    "teams": lambda w, t, n: team(t, f"T{n}"),
    "skills": lambda w, t, n: skill(t, f"S{n}"),
    "agent-statuses": lambda w, t, n: _post(t, "agent-statuses", {"name": f"S{n}", "availability": False}),
    "dispositions": lambda w, t, n: disposition(t, f"D{n}")["id"],
    "agents": lambda w, t, n: t.agent(f"A{n}").id,
    "numbers": lambda w, t, n: t.route_number(t.number(), hangup_action())["id"],
    "webhooks": lambda w, t, n: webhook(t, w.webhook_sink().url)["id"],
    "api-keys": lambda w, t, n: ok(w.operator.post(f"/api/v1/tenants/{t.id}/api-keys", json=KEY))["id"],
    "tenants": lambda w, t, n: w.new_tenant(f"T{n}").id,
}


def walk(client, path: str) -> list[list[str]]:
    """The ids on each 2-row page of a list, following next_cursor until it is null; a repeated row fails."""
    pages, cursor = [], None
    while True:
        body = ok(client.get(path, params={"limit": 2} | ({"cursor": cursor} if cursor else {})))
        ids = [row["id"] for row in body["data"]]
        assert not set(ids) & {i for page in pages for i in page}, f"page {len(pages) + 1} repeats a row"
        pages.append(ids)
        if (cursor := body["next_cursor"]) is None:
            return pages


@pytest.mark.parametrize("kind", LISTS)
def test_a_list_pages_through_every_row_exactly_once(world, tenant, kind):
    made = {LISTS[kind](world, tenant, n) for n in range(3)}
    client, path = tenant.api, f"/api/v1/{kind}"
    if kind in ("api-keys", "tenants"):
        client, path = world.operator, path.replace("/api-keys", f"/tenants/{tenant.id}/api-keys")
    pages = walk(client, path)
    assert len(pages) >= 2 and all(len(page) == 2 for page in pages[:-1])
    assert made <= {i for page in pages for i in page}


def test_the_seeded_roles_page_through_exactly_once(world):
    # Three random ids hide a sort-key mismatch one time in six, so six tenants make a miss 1 in 46,656.
    for tenant in (world.new_tenant() for _ in range(6)):
        everything = [r["id"] for r in ok(tenant.api.get("/api/v1/roles", params={"limit": 200}))["data"]]
        assert sorted(i for page in walk(tenant.api, "/api/v1/roles") for i in page) == sorted(everything)
