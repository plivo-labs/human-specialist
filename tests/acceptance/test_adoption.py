import uuid
from urllib.parse import quote
from xml.sax.saxutils import escape

from tests.acceptance.channels import plivo_app
from tests.acceptance.conftest import error, ok

A = "/api/v1/agents"
HANGUP = {"open_action": {"type": "hangup"}}


def plivo(tenant, method: str, path: str, **body) -> dict:
    """The tenant's own request to Plivo, outside HS."""
    url = f"{tenant.world.plivo.url}/v1/Account/{tenant.auth_id}/{path}"
    r = tenant.world.anon().request(method, url, json=body or None, auth=(tenant.auth_id, tenant.auth_token))
    assert r.is_success, r.text
    return r.json() if r.content else {}


def own_endpoint(tenant) -> dict:
    return plivo(tenant, "POST", "Endpoint/", username="desk", password="desk-secret-1", alias="desk")


def own_app(tenant, number: str, xml: str) -> str:
    """The number points at a Plivo application of the tenant's own that answers with `xml`; its app id."""
    app_id = plivo(tenant, "POST", "Application/", app_name="own", answer_url=tenant.world.plivo.app(xml))["app_id"]
    plivo(tenant, "POST", f"Number/{number.removeprefix('+')}/", app_id=app_id)
    return app_id


def writes(world, fragment: str, since: int) -> list[str]:
    return [f"{r.method} {r.path}" for r in world.plivo.requests[since:] if fragment in r.path and r.method != "GET"]


def test_documents_keep_the_ids_they_are_created_with(world, tenant):
    other = world.new_tenant("Other")
    kinds = {"queues": {}, "teams": {}, "skills": {}, "dispositions": {}, "agents": {"role": "agent"}}
    for path, extra in kinds.items():
        rid, url = str(uuid.uuid4()), f"/api/v1/{path}"
        assert ok(tenant.api.post(url, json={"id": rid, "name": "Kept", **extra}))["id"] == rid
        assert ok(tenant.api.get(f"{url}/{rid}"))["name"] == "Kept"
        error(tenant.api.post(url, json={"id": rid, "name": "Again", **extra}), 409, "id_taken")
        error(other.api.post(url, json={"id": rid, "name": "Theirs", **extra}), 409, "id_taken")
    queue = ok(tenant.api.post("/api/v1/queues", json={"id": str(uuid.uuid4()), "name": "Sales"}))
    error(tenant.api.patch(f"/api/v1/queues/{queue['id']}", json={"id": str(uuid.uuid4())}), 422)


def test_an_agent_created_on_an_existing_endpoint_rings_there(world, tenant):
    endpoint = own_endpoint(tenant)
    since = len(world.plivo.requests)
    agent = tenant.agent("Ann", endpoint_id=endpoint["endpoint_id"])
    assert agent.endpoint_username == endpoint["username"] and writes(world, f"{tenant.auth_id}/Endpoint", since) == []
    number, _ = tenant.inbound_line(agent.login())
    world.plivo.call(number)
    agent.phone.wait_ring()


def test_an_existing_endpoint_stays_at_plivo_through_its_agents_rotate_and_delete(world, tenant):
    endpoint = own_endpoint(tenant)
    on = {"role": "agent", "endpoint_id": endpoint["endpoint_id"]}
    since = len(world.plivo.requests)
    ann = ok(tenant.api.post(A, json={"name": "Ann", **on}))
    assert error(tenant.api.post(A, json={"name": "Bob", **on}), 409, "agent_exists")["details"]["id"] == ann["id"]
    error(tenant.api.post(f"{A}/{ann['id']}/sip-credentials"), 409, "endpoint_adopted")
    ok(tenant.api.delete(f"{A}/{ann['id']}"))
    ok(tenant.api.post(A, json={"name": "Cat", **on}))
    theirs = own_endpoint(world.new_tenant("Other"))["endpoint_id"]
    for foreign in ("12345678901234", theirs):
        body = {"name": "Dan", "role": "agent", "endpoint_id": foreign}
        error(tenant.api.post(A, json=body), 422, "endpoint_not_in_account")
    ok(world.operator.delete(f"/api/v1/tenants/{tenant.id}"))
    assert writes(world, f"/Endpoint/{endpoint['endpoint_id']}", since) == []
    assert plivo(tenant, "GET", f"Endpoint/{endpoint['endpoint_id']}/")["username"] == endpoint["username"]


def test_a_tenant_registered_without_a_plivo_app_never_makes_or_touches_one(world):
    since = len(world.plivo.requests)
    tenant = world.new_tenant("Own", plivo_app=False)
    assert ok(world.operator.get(f"/api/v1/tenants/{tenant.id}"))["plivo_app_id"] is None
    ok(world.operator.patch(f"/api/v1/tenants/{tenant.id}", json={"name": "Own telephony"}))
    tenant.agent("Ann").login(available=False)
    ok(world.operator.delete(f"/api/v1/tenants/{tenant.id}"))
    assert writes(world, f"{tenant.auth_id}/Application", since) == []


def test_a_tenant_without_a_plivo_app_takes_the_calls_its_own_app_hands_over(world):
    tenant = world.new_tenant("Own", plivo_app=False)
    agent = tenant.agent("Ann").login()
    queue, number = tenant.queue(agents=[agent.id]), tenant.number()
    app_id = own_app(tenant, number, f"<Response><Redirect>{escape(queue['enqueue_url'])}</Redirect></Response>")
    ok(tenant.api.put(f"/api/v1/numbers/{quote(number)}", json=HANGUP))
    assert plivo_app(tenant, number)["app_id"] == app_id
    world.plivo.call(number)
    agent.phone.wait_ring()


def test_a_number_put_without_repoint_stays_on_its_own_application(tenant):
    number = tenant.number()
    app_id = own_app(tenant, number, "<Response><Speak>Our own menu</Speak></Response>")
    put = tenant.api.put(f"/api/v1/numbers/{quote(number)}", params={"repoint": "false"}, json=HANGUP)
    assert ok(put)["number"] == number and plivo_app(tenant, number)["app_id"] == app_id
    ok(tenant.api.post(A, json={"name": "Ann", "role": "agent", "caller_id": number}))
