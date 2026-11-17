"""The first run of an install whose deploy named its owner's Plivo account in HS_SETUP_PLIVO_AUTH_ID: that account
registers with no operator key, once, and nothing changes while the setting is empty."""

import contextlib
import threading

from tests.acceptance.conftest import error, ok
from tests.harness import World

T = "/api/v1/tenants"
OWNER = "SATESTOWNER000000000"


def body(account) -> dict:
    return {"plivo_auth_id": account.auth_id, "plivo_auth_token": account.auth_token}


def test_the_named_account_claims_an_empty_install_once_without_the_operator_key():
    with contextlib.closing(World(env={"HS_SETUP_PLIVO_AUTH_ID": OWNER})) as world:
        owner, stranger = world.plivo.subaccount("Owner", OWNER), world.plivo.subaccount("Stranger")
        anon = world.anon()
        assert ok(anon.get("/status")) == {"db": True, "setup": True}
        error(anon.post(T, json={}), 422)
        unauthorized = error(anon.get(T), 401, "unauthorized")
        assert error(anon.post(T, json=body(stranger)), 401) == unauthorized
        assert not world.plivo.requests_to(stranger.auth_id), "refused before HS asks Plivo"
        claimed = ok(anon.post(T, json=body(owner)))
        assert ok(world.operator.get(f"{T}/{claimed['tenant_id']}"))["name"] == "Owner"
        assert ok(anon.get("/status")) == {"db": True}
        ok(world.client(claimed["api_key"]).get("/api/v1/settings"))
        assert error(anon.post(T, json=body(owner)), 401) == unauthorized
        ok(world.operator.delete(f"{T}/{claimed['tenant_id']}"))
        assert error(anon.post(T, json=body(owner)), 401) == unauthorized, "a deleted tenant still ends setup"
        assert world.new_tenant("Second").id


def test_two_claims_at_once_make_one_tenant_and_one_409():
    with contextlib.closing(World(env={"HS_SETUP_PLIVO_AUTH_ID": OWNER})) as world:
        owner, answers = world.plivo.subaccount("Owner", OWNER), []

        def claim() -> None:
            answers.append(world.anon().post(T, json=body(owner)))

        threads = [threading.Thread(target=claim) for _ in range(2)]
        with world.stall("tenant"):
            for t in threads:
                t.start()
            world.wait(lambda: len(world.plivo.requests_to(OWNER)) >= 2, what="both claims past the auth check")
        for t in threads:
            t.join()
        assert sorted(a.status_code for a in answers) == [201, 409], [a.text for a in answers]
        assert len(ok(world.operator.get(T))["data"]) == 1


def test_with_the_setting_empty_an_empty_install_takes_only_the_operator_key():
    with contextlib.closing(World(env={"HS_SETUP_PLIVO_AUTH_ID": ""})) as world:
        owner = world.plivo.subaccount("Owner", OWNER)
        assert ok(world.anon().get("/status")) == {"db": True}
        error(world.anon().post(T, json=body(owner)), 401, "unauthorized")
        error(world.anon().post(T, json={}), 401, "unauthorized")
        assert not world.plivo.requests_to(OWNER)
        assert world.new_tenant("Owner").id


def test_keyless_tries_stop_reaching_plivo_after_ten_from_one_client_and_the_operator_key_is_not_limited():
    with contextlib.closing(World(env={"HS_SETUP_PLIVO_AUTH_ID": OWNER})) as world:
        owner = world.plivo.subaccount("Owner", OWNER)
        for _ in range(10):
            error(world.anon().post(T, json=body(owner) | {"plivo_auth_token": "wrong"}), 422)
        asked = len(world.plivo.requests_to(OWNER))
        assert error(world.anon().post(T, json=body(owner)), 429, "too_many_attempts")["details"]["retry_after"] > 0
        assert len(world.plivo.requests_to(OWNER)) == asked, "refused before HS asks Plivo"
        assert ok(world.operator.post(T, json=body(owner)))["api_key"]
