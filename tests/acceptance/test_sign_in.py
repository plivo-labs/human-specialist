"""An agent signs in to their own desk with an email and password, renews the session through a 12-hour shift without
a new link, and an admin who signed in runs the tenant's settings without the tenant key."""

import uuid
from datetime import UTC, datetime, timedelta

from tests.acceptance.conftest import connect, error, ok
from tests.harness import World

A, PASSWORD = "/api/v1/agents", "first-pass-1"


def person(tenant, role: str = "agent", email: str | None = None, password: str = PASSWORD) -> tuple:
    email = email or f"{uuid.uuid4().hex[:10]}@example.test"
    agent = tenant.agent(role=role, email=email)
    assert ok(tenant.api.post(f"{A}/{agent.id}/password", json={"password": password})) == {"password": None}
    return agent, email


def sign_in(tenant, email: str, password: str = PASSWORD, **body):
    """POST /sessions to the tenant from an IPv6 /64 of its own, so failed tries count against no other test's."""
    h, body = uuid.uuid4().hex, {"email": email, "password": password, "tenant_id": tenant.id} | body
    headers = {"X-Forwarded-For": f"2001:db8:{h[:4]}:{h[4:8]}::1"}
    return tenant.world.anon().post("/api/v1/sessions", json=body, headers=headers)


def renew(world, session: dict):
    return world.client(session["agent_token"]).post("/api/v1/me/sessions")


def me(world, session: dict):
    return world.client(session["agent_token"]).get("/api/v1/me")


def test_an_agent_signs_in_with_its_email_and_a_password_hs_made(world, tenant):
    agent = tenant.agent(email="Ann.Lee@Example.test")
    made = ok(tenant.api.post(f"{A}/{agent.id}/password"))["password"]
    assert made and made not in tenant.api.get(f"{A}/{agent.id}").text
    s = ok(sign_in(tenant, "ann.lee@example.TEST", made))
    assert ok(me(world, s))["id"] == agent.id and ok(me(world, s))["email"] == "Ann.Lee@Example.test"
    assert s["plivo"]["endpoint_username"] == agent.endpoint_username and s["plivo"]["access_token"]
    chain = datetime.fromisoformat(s["renewable_until"]) - datetime.now(UTC)
    assert timedelta(hours=11, minutes=59) < chain <= timedelta(hours=12)
    ok(world.client(s["agent_token"]).post("/api/v1/me/heartbeat"))


def test_a_wrong_password_and_an_unknown_email_get_one_answer(world, tenant):
    _, email = person(tenant)
    wrong = error(sign_in(tenant, email, "not-the-password"), 401, "invalid_credentials")
    assert error(sign_in(tenant, f"nobody-{email}", PASSWORD), 401) == wrong


def test_ten_failed_tries_lock_the_email_until_the_window_ends_and_a_sign_in_clears_the_count(world, tenant):
    _, email = person(tenant)
    for _ in range(10):
        error(sign_in(tenant, email, "not-the-password"), 401, "invalid_credentials")
    locked = sign_in(tenant, email)
    error(locked, 429, "too_many_attempts")
    assert 0 < int(locked.headers["Retry-After"]) <= 900
    error(sign_in(tenant, email), 429, "too_many_attempts")
    world.travel(900)
    ok(sign_in(tenant, email))
    for _ in range(10):
        error(sign_in(tenant, email, "not-the-password"), 401, "invalid_credentials")
    error(sign_in(tenant, email), 429, "too_many_attempts")


def test_a_renewal_mints_the_next_session_and_the_old_tokens_live_to_their_own_expiry(world, tenant):
    _, email = person(tenant)
    first = ok(sign_in(tenant, email))
    second = ok(renew(world, first))
    assert second["agent_token"] != first["agent_token"] and second["renewable_until"] == first["renewable_until"]
    assert ok(me(world, first))["id"] == ok(me(world, second))["id"]
    error(world.client(first["stream_token"]).post("/api/v1/me/sessions"), 403)


def test_a_renewal_keeps_the_call(tenant):
    live = connect(tenant)
    fresh = ok(live.agent.api().post("/api/v1/me/sessions"))
    assert live.caller.state == "in_progress" and live.leg.state == "in_progress"
    ok(tenant.world.client(fresh["agent_token"]).post(f"/api/v1/calls/{live.id}/hold", json={"on": True}))
    live.stream.wait_for("call.held", conversation_id=live.id)


def test_renewals_end_twelve_hours_after_the_first_session(world, tenant):
    _, email = person(tenant)
    s = ok(sign_in(tenant, email))
    for _ in range(13):
        world.travel(55 * 60)
        s = ok(renew(world, s))
    world.travel(10 * 60)
    error(renew(world, s), 403, "sign_in_required")
    ok(me(world, s))
    ok(renew(world, ok(sign_in(tenant, email))))


def test_signing_out_revoking_a_role_change_or_a_new_password_ends_the_whole_chain(world, tenant):
    agent, email = person(tenant)
    ends = {
        "sign-out": lambda s: ok(world.client(s["agent_token"]).delete("/api/v1/me/sessions")),
        "revoke": lambda s: ok(tenant.api.delete(f"{A}/{agent.id}/sessions")),
        "role": lambda s: ok(tenant.api.patch(f"{A}/{agent.id}", json={"role": "supervisor"})),
        "password": lambda s: ok(tenant.api.post(f"{A}/{agent.id}/password", json={"password": "second-pass-2"})),
    }
    for name, end in ends.items():
        first = ok(sign_in(tenant, email))
        second = ok(renew(world, first))
        end(second)
        assert [me(world, s).status_code for s in (first, second)] == [401, 401], name
        error(renew(world, second), 401)
    error(sign_in(tenant, email), 401, "invalid_credentials")
    ok(sign_in(tenant, email, "second-pass-2"))


def test_an_agent_changes_its_own_password_with_the_current_one(world, tenant):
    _, email = person(tenant)
    s = ok(sign_in(tenant, email))
    desk = world.client(s["agent_token"])
    error(desk.put("/api/v1/me/password", json={"current_password": "wrong-guess", "password": "mine-now-9"}), 403)
    ok(desk.put("/api/v1/me/password", json={"current_password": PASSWORD, "password": "mine-now-9"}))
    error(me(world, s), 401)
    error(sign_in(tenant, email), 401, "invalid_credentials")
    ok(sign_in(tenant, email, "mine-now-9"))


def test_an_admin_session_runs_the_settings_and_a_supervisor_or_agent_session_cannot(world, tenant):
    sessions = {role: ok(sign_in(tenant, person(tenant, role)[1])) for role in ("admin", "supervisor", "agent")}
    admin = world.client(sessions["admin"]["agent_token"])
    assert ok(admin.patch("/api/v1/settings", json={"agent_direct_dial": True}))["agent_direct_dial"] is True
    ok(admin.post("/api/v1/teams", json={"name": "Night shift"}))
    made = ok(admin.post(A, json={"name": "New hire", "role": "agent", "email": "new.hire@example.test"}))
    assert ok(admin.post(f"{A}/{made['id']}/password"))["password"]
    error(admin.post(f"/api/v1/tenants/{tenant.id}/api-keys", json={"scopes": ["admin"]}), 403)
    for role in ("supervisor", "agent"):
        desk = world.client(sessions[role]["agent_token"])
        error(desk.post("/api/v1/teams", json={"name": "Mine"}), 403)
        error(desk.patch("/api/v1/settings", json={"agent_direct_dial": False}), 403)


def test_one_email_in_two_tenants_signs_in_to_the_tenant_the_sign_in_names(world, tenant):
    other, email = world.new_tenant("Other"), f"pat-{uuid.uuid4().hex[:6]}@example.test"
    mine, _ = person(tenant, email=email)
    theirs, _ = person(other, email=email.upper())
    error(tenant.api.post(A, json={"name": "Twin", "role": "agent", "email": email.upper()}), 409, "agent_exists")
    assert ok(me(world, ok(sign_in(tenant, email))))["id"] == mine.id
    assert ok(me(world, ok(sign_in(other, email))))["id"] == theirs.id
    for unknown in (email, f"nobody-{email}"):
        assert not error(sign_in(tenant, unknown, tenant_id=None), 409, "tenant_required")["details"]


def test_a_sign_in_in_one_tenant_leaves_the_failed_tries_of_the_same_email_in_another(world, tenant):
    other, email = world.new_tenant("Other"), f"pat-{uuid.uuid4().hex[:6]}@example.test"
    person(tenant, email=email)
    person(other, email=email, password="their-pass-3")
    for _ in range(10):
        error(sign_in(tenant, email, "not-the-password"), 401, "invalid_credentials")
        ok(sign_in(other, email, "their-pass-3"))
    error(sign_in(tenant, email), 429, "too_many_attempts")


def test_an_install_of_one_organization_signs_in_without_a_tenant_id_until_it_has_two():
    solo = World()
    try:
        only = solo.new_tenant("Solo")
        _, email = person(only)
        assert ok(me(solo, ok(sign_in(only, email, tenant_id=None))))["email"] == email
        solo.new_tenant("Second")
        error(sign_in(only, email, tenant_id=None), 409, "tenant_required")
        ok(sign_in(only, email))
    finally:
        solo.close()
