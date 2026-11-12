"""Every guarded route that hs.app serves refuses a missing credential (401), the wrong credential
(403 for a tenant key without the route's scope) and another tenant's resource id (404).

The other tenant owns the resources the harness can make (agents, queues, numbers, statuses);
other kinds use an unknown id, which must be a 404 as well.
"""

import re
import uuid

import pytest

from tests.contract import routes
from tests.harness import ALL_SCOPES, fixtures

app = routes.import_app()
world = fixtures.world  # registers the session fixture in this module

pytestmark = pytest.mark.xdist_group("harness")

SERVED = {(method, routes.norm(path)) for method, path, _ in routes.served(app)}
GUARDED = [r for r in routes.ROUTES if r[2] != routes.NONE and (r[0], routes.norm(r[1])) in SERVED]
FOREIGN = [r for r in GUARDED if "{" in r[1] and any(a.startswith("key:") for a in r[2])]
FACTORIES = {
    "agents": lambda t: t.agent().id,
    "queues": lambda t: t.inbound_line()[1]["id"],
    "numbers": lambda t: t.inbound_line()[0],
    "agent-statuses": lambda t: t.status_id("Available"),
}


def ids(route) -> str:
    return f"{route[0]} {route[1]}"


def url(path: str, tenant_id: str, first: str | None = None) -> str:
    """A concrete path: the caller's tenant, `first` (or a fresh id) for the first resource id."""
    path = path.replace("{tenant_id}", tenant_id)
    for i, name in enumerate(re.findall(r"\{[^}]*\}", path)):
        value = first if i == 0 and first else "+14155550100" if name == "{number}" else str(uuid.uuid4())
        path = path.replace(name, value, 1)
    return path


def send(client, method: str, path: str):
    return client.request(method, path, json={} if method in ("POST", "PUT", "PATCH") else None)


@pytest.fixture(scope="module")
def env(world):
    return world, world.new_tenant("Home"), world.new_tenant("Other")


@pytest.fixture(scope="module")
def keys(env):
    home, made = env[1], {}

    def key(scope: str):
        if scope not in made:
            made[scope] = home.key_with(scope)
        return made[scope]

    return key


@pytest.fixture(scope="module")
def foreign(env):
    other, made = env[2], {}

    def resource_id(path: str) -> str | None:
        kind = path.split("/")[3]
        if kind not in made:
            made[kind] = FACTORIES[kind](other) if kind in FACTORIES else None
        return made[kind]

    return resource_id


@pytest.mark.parametrize("route", GUARDED, ids=ids)
def test_no_credential_is_401(env, route):
    world, home, _ = env
    r = send(world.anon(), route[0], url(route[1], home.id))
    assert r.status_code == 401, r.text


@pytest.mark.parametrize("route", GUARDED, ids=ids)
def test_wrong_credential_is_refused(env, keys, route):
    method, path, auth = route
    home = env[1]
    need = {a.removeprefix("key:") for a in auth if a.startswith("key:")}
    if need:
        r = send(keys(next(s for s in ALL_SCOPES if s not in need)), method, url(path, home.id))
        assert r.status_code == 403, r.text
    else:
        r = send(home.api, method, url(path, home.id))
        assert r.status_code in (401, 403), r.text


@pytest.mark.parametrize("route", FOREIGN, ids=ids)
def test_other_tenants_id_is_404(env, foreign, route):
    method, path, _ = route
    home = env[1]
    r = send(home.api, method, url(path, home.id, foreign(path)))
    # A body route may reject the empty body first; it must never act on the other tenant's resource.
    assert r.status_code in ((404,) if method in ("GET", "DELETE") else (404, 422)), r.text
