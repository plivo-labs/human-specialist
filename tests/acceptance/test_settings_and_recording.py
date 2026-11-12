"""The one settings resource: defaults, JSON-merge PATCH, validation, and the recording toggles."""

import math
import time
from concurrent.futures import ThreadPoolExecutor

from tests.acceptance.conftest import connect, dial, fence, ok, refused, replayed, run_jobs_until, say, seen, settings
from tests.harness.sim import SLOW

S = "/api/v1/settings"


def test_settings_defaults(tenant):
    s = ok(tenant.api.get(S))
    assert s["agent_direct_dial"] is False
    assert s["ring_timeout_sec"] == 30 and s["capacity"]["call"] == 1
    assert (s["rona"]["strikes"], s["rona"]["status_id"]) == (2, tenant.status_id("Break"))
    assert s["wrap_up"]["max_extend_sec"] == 30
    assert set(s) >= {"recording", "wrap_up", "consent", "ring_timeout_sec", "rona", "capacity", "monitor"}


def test_patch_is_a_json_merge(tenant):
    before = ok(tenant.api.get(S))
    settings(tenant, wrap_up={"seconds": 7})
    after = ok(tenant.api.get(S))
    assert after["wrap_up"] == {**before["wrap_up"], "seconds": 7}
    assert {k: v for k, v in after.items() if k != "wrap_up"} == {k: v for k, v in before.items() if k != "wrap_up"}
    settings(tenant, consent={"inbound": say("This call is recorded")})
    assert ok(tenant.api.get(S))["consent"]["inbound"]["text"] == "This call is recorded"
    settings(tenant, consent={"inbound": None})
    assert not ok(tenant.api.get(S))["consent"].get("inbound")


def test_with_redis_down_config_reads_still_work(world, tenant):
    settings(tenant, ring_timeout_sec=20)
    before = ok(tenant.api.get(S)), ok(tenant.api.get("/api/v1/status-map"))
    with world.redis_down():
        assert (ok(tenant.api.get(S)), ok(tenant.api.get("/api/v1/status-map"))) == before
    assert ok(tenant.api.get(S))["ring_timeout_sec"] == 20


def test_with_redis_not_answering_config_reads_and_routes_without_redis_answer_well_inside_the_pause(world, tenant):
    agent = tenant.agent().login()
    world.redis("CLIENT", "PAUSE", "4000", "ALL")
    try:
        start = time.monotonic()
        with ThreadPoolExecutor(41) as pool:
            reads = [pool.submit(tenant.api.get, S) for _ in range(40)]
            status = pool.submit(world.anon().get, "/status")
            assert [r.result().status_code for r in reads] == [200] * 40 and status.result().status_code == 200
        assert time.monotonic() - start < 1.5 * SLOW, "no read waits out the pause, and none holds the pool meanwhile"
    finally:
        world.redis("CLIENT", "UNPAUSE")
        world.wait(
            lambda: agent.api().post("/api/v1/me/heartbeat").status_code == 204, timeout=10, what="Redis is back"
        )


def test_invalid_settings_are_422_and_change_nothing(world, tenant):
    before = ok(tenant.api.get(S))
    attempts = [
        {"ring_timeout_sec": 4},
        {"ring_timeout_sec": 121},
        {"capacity": {"call": 2}},
        {"wrap_up": {"mode": "never"}},
        {"rona": {"status_id": world.new_tenant("Other").status_id("Break")}},
    ]
    assert [b for b in attempts if tenant.api.patch(S, json=b).status_code != 422] == []
    assert ok(tenant.api.get(S)) == before


def test_inbound_recording_on_records_inbound_calls(tenant):
    settings(tenant, recording={"inbound": True})
    live = connect(tenant)
    live.caller.hangup()
    ready = live.stream.wait_for("recording.ready", conversation_id=live.id)
    closed = run_jobs_until(
        tenant.world, lambda: seen(live.stream, "conversation.closed", conversation_id=live.id), "conversation.closed"
    )[0]
    assert ready["data"]["recording_id"] in closed["data"]["recording_ids"]


def test_inbound_recording_off_records_nothing(tenant):
    settings(tenant, recording={"inbound": False, "outbound": True})
    live = connect(tenant)
    live.caller.hangup()
    closed = run_jobs_until(
        tenant.world, lambda: seen(live.stream, "conversation.closed", conversation_id=live.id), "conversation.closed"
    )[0]
    assert closed["data"]["recording_ids"] == [] and not replayed(tenant, "recording.ready")


def test_outbound_recording_follows_its_own_toggle(tenant):
    settings(tenant, recording={"inbound": False, "outbound": True})
    stream = tenant.stream(after=0)
    _, cid = dial(tenant)
    stream.wait_for("call.answered", conversation_id=cid)
    ok(tenant.api.post(f"/api/v1/calls/{cid}/hangup"))
    assert stream.wait_for("recording.ready", conversation_id=cid)["data"]["recording_id"]


def test_pause_is_refused_when_pausing_is_off(tenant):
    settings(tenant, recording={"inbound": True, "allow_pause": False})
    live = connect(tenant)
    refused(live.agent.api().post(f"/api/v1/calls/{live.id}/recording", json={"action": "pause"}))
    fence(tenant)
    assert not tenant.events(type="call.recording_paused")


def test_a_null_in_a_patch_removes_only_its_own_key(tenant):
    settings(tenant, consent={"inbound": say("In"), "outbound": say("Out")}, ring_timeout_sec=20)
    after = settings(tenant, consent={"inbound": None}, ring_timeout_sec=None)
    assert ok(tenant.api.get(S)) == after
    assert not after["consent"].get("inbound") and after["consent"]["outbound"]["text"] == "Out"
    assert after["ring_timeout_sec"] == 30


def test_a_call_whose_end_hs_never_hears_ends_at_the_tenants_call_session_timeout(world, tenant):
    settings(tenant, call_session_timeout_sec=3600)
    live = connect(tenant)
    world.plivo.fault("drop", lambda r: "/plivo/" in r.path, account=tenant.auth_id, times=math.inf)
    world.plivo.fault(
        "reject", lambda r: r.method == "GET" and "/MultiPartyCall/" in r.path, account=tenant.auth_id, times=math.inf
    )
    live.caller.hangup()
    live.caller.wait_state("completed")
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    world.advance(3600 - 60)
    assert ok(tenant.api.get(f"/api/v1/conversations/{live.id}"))["state"] == "active"
    world.advance(3600 + 60)
    live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id, agent_id=live.agent.id)
