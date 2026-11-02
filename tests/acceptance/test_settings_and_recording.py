"""The one settings resource: defaults, JSON-merge PATCH, validation, and the recording toggles."""

from tests.acceptance.conftest import connect, dial, fence, ok, refused, run_jobs_until, say, seen, settings

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
    assert closed["data"]["recording_ids"] == [] and not seen(live.stream, "recording.ready")


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
