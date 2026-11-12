"""Plivo refusing a registration for a reason that is not bad credentials: the reason reaches the caller."""

from tests.acceptance.conftest import error, ok

T = "/api/v1/tenants"
WHITELIST = "Access denied. The request originated from IP 203.0.113.9, which is not in the IP whitelist."
VOICE_OFF = "Voice is disabled on this sub-account."


def register(world, sub):
    return world.operator.post(
        T, json={"name": "Acme", "plivo_auth_id": sub.auth_id, "plivo_auth_token": sub.auth_token}
    )


def test_a_plivo_403_is_a_refusal_with_its_reason_and_not_bad_credentials(world, caplog):
    caplog.set_level("WARNING", logger="human-specialist")
    for reason in (WHITELIST, VOICE_OFF):
        sub = world.plivo.subaccount("Acme")
        world.plivo.reject_next("/v1/Account/", 403, reason, account=sub.auth_id)
        err = error(register(world, sub), 422, "plivo_refused")
        assert err["details"] == {"status": 403, "errors": {"error": reason}}
        assert sub.auth_token not in str(err)
        world.wait(lambda reason=reason: reason in caplog.text, what="the refusal reason in the log")
        assert sub.auth_token not in caplog.text


def test_a_refused_registration_leaves_nothing_and_another_subaccount_registers(world):
    first, second = world.plivo.subaccount("First"), world.plivo.subaccount("Second")
    world.plivo.reject_next("/v1/Account/", 403, WHITELIST, account=first.auth_id)
    error(register(world, first), 422)
    ok(register(world, second))
    again = ok(register(world, first))
    assert again["tenant_id"] and again["api_key"]
