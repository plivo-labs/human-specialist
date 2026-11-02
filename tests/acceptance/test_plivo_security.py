"""Plivo-facing routes: V3 signature against the tenant's own token over PUBLIC_BASE_URL; refused dial-outs; a
callback any tenant can sign acts only on that tenant's own calls."""

import json
import time
import uuid

import pytest

from tests.acceptance.conftest import (
    answer_params,
    connect,
    dial,
    fence,
    hangup_action,
    menu_line,
    ok,
    plivo_post,
    run_jobs_until,
    seen,
    settings,
)

PLIVO_PATHS = [
    "answer",
    "hangup",
    "mpc-status",
    "call-status",
    "recording",
    "transcription",
    "actions/menu",
    "actions/voicemail",
    "actions/queue-exit",
]


def test_unsigned_callbacks_are_refused_on_every_plivo_route(world, tenant):
    number, _ = tenant.inbound_line(tenant.agent())
    statuses = {
        path: plivo_post(world, tenant.id, path, answer_params(number), None).status_code for path in PLIVO_PATHS
    }
    assert {p: s for p, s in statuses.items() if s != 401} == {}


def test_a_forged_or_misdirected_signature_is_refused(world, tenant):
    number, _ = tenant.inbound_line(tenant.agent())
    forged = {"X-Plivo-Signature-V3": "bm90LWEtc2lnbmF0dXJl", "X-Plivo-Signature-V3-Nonce": "12345"}
    assert plivo_post(world, tenant.id, "answer", answer_params(number), None, **forged).status_code == 401
    params = answer_params(number)
    signed_for_hangup = world.plivo.sign(f"{world.base_url}/plivo/{tenant.id}/hangup", params, tenant.auth_token)
    assert (
        world.anon()
        .post(f"{world.base_url}/plivo/{tenant.id}/answer", data=params, headers=signed_for_hangup)
        .status_code
        == 401
    )
    fence(tenant)
    assert not tenant.events(type="conversation.started")


def test_a_callback_signed_with_another_tenants_token_is_refused(world, tenant):
    other = world.new_tenant("Other")
    number, _ = tenant.inbound_line(tenant.agent())
    assert plivo_post(world, tenant.id, "answer", answer_params(number), other.auth_token).status_code == 401
    fence(tenant)
    assert not tenant.events(type="conversation.started")


def test_the_signed_url_is_rebuilt_from_the_public_base_url_not_the_host_header(world, tenant):
    number, _ = tenant.inbound_line(tenant.agent().login())
    params = answer_params(number)
    url = f"{world.base_url}/plivo/{tenant.id}/answer"
    forged_host = world.plivo.sign(f"http://evil.example/plivo/{tenant.id}/answer", params, tenant.auth_token)
    assert world.anon().post(url, data=params, headers={**forged_host, "Host": "evil.example"}).status_code == 401
    honest = world.anon().post(
        url, data=params, headers={**world.plivo.sign(url, params, tenant.auth_token), "Host": "evil.example"}
    )
    assert honest.status_code == 200 and "xml" in honest.headers["content-type"]
    world.wait(lambda: tenant.events(type="conversation.started"), what="conversation.started")


def test_a_plivo_form_enqueue_must_be_signed_by_the_tenant(world, tenant):
    q = tenant.queue(name="Q")
    params = answer_params(tenant.number())
    url = q["enqueue_url"]
    assert world.anon().post(url, data=params).status_code == 401
    other = world.new_tenant("Other")
    assert (
        world.anon().post(url, data=params, headers=world.plivo.sign(url, params, other.auth_token)).status_code == 401
    )


def test_an_endpoint_dial_out_without_a_pending_dial_is_hung_up_rejected(world, tenant):
    agent = tenant.agent()
    params = answer_params("+14155550123", from_=f"sip:{agent.endpoint_username}@phone.plivo.com")
    r = plivo_post(world, tenant.id, "answer", params, tenant.auth_token)
    assert r.status_code == 200 and "<Hangup" in r.text and 'reason="rejected"' in r.text
    fence(tenant)
    assert not tenant.events(type="conversation.started")


def test_a_sip_uri_destination_is_refused_even_with_direct_dial(world, tenant):
    settings(tenant, agent_direct_dial=True)
    caller, other = tenant.agent("Caller").login(), tenant.agent("Other").login()
    caller.phone.dial(f"sip:{other.endpoint_username}@phone.plivo.com").wait_state("completed")
    fence(tenant)
    assert other.phone.legs == [] and not tenant.events(type="conversation.started")


def signed_by(other, path: str, params: dict):
    """A callback `other` signs with its own token, naming whatever ids it likes."""
    return plivo_post(other.world, other.id, path, params, other.auth_token)


def naming(tenant, cid: str) -> list[str]:
    """The types of the tenant's events about this conversation, once all committed ones are visible."""
    tenant.world.wait(tenant.world.plivo.idle, what="the Plivo simulator to go idle")
    fence(tenant)
    return [e["type"] for e in tenant.events() if e.get("conversation_id") == cid]


def in_menu(tenant):
    """A caller waiting in a menu. Returns (caller, conversation_id, stream)."""
    number, _ = menu_line(tenant, {"1": hangup_action()}, input_timeout_sec=30)
    stream = tenant.stream(after=0)
    caller = tenant.world.plivo.call(number)
    cid = stream.wait_for("conversation.started")["conversation_id"]
    tenant.world.wait(lambda: "Press a key" in caller.said, what="the menu greeting")
    return caller, cid, stream


def test_another_tenants_hangup_naming_a_live_call_leaves_it_live_and_its_close_alone(world, tenant):
    caller, cid, stream = in_menu(tenant)
    other = world.new_tenant("Other")
    hangup = {"CallUUID": caller.call_uuid, "HangupCauseCode": "3020", "HangupCauseName": "Forged"}
    hangup |= {"HangupSource": "Callee", "Duration": "999"}
    assert signed_by(other, "hangup", hangup).status_code == 200
    world.advance(30)
    assert caller.state == "in_progress" and "conversation.closed" not in naming(tenant, cid)
    caller.hangup()
    world.wait(world.plivo.idle, what="the caller's hangup callback")
    assert signed_by(other, "hangup", hangup).status_code == 200
    ended = run_jobs_until(world, lambda: seen(stream, "call.ended", conversation_id=cid), "call.ended")[0]["data"]
    assert (ended["result"], ended["ended_by"]) == ("abandoned", "customer") and ended["hangup_cause"] != "Forged"
    assert naming(other, cid) == []


@pytest.mark.parametrize("path", ["answer", "actions/menu", "actions/voicemail"])
def test_another_tenants_callback_naming_a_call_in_a_menu_changes_nothing(world, tenant, path):
    caller, cid, stream = in_menu(tenant)
    other = world.new_tenant("Other")
    number, m = menu_line(other, {"1": hangup_action()})
    now = int(time.time() * 1000)
    params = {**answer_params(number), "CallUUID": caller.call_uuid, "Digits": "1", "RecordingID": str(uuid.uuid4())}
    params |= {"RecordUrl": "https://example.test/Recording/forged.mp3", "RecordingDuration": "5"}
    params |= {"RecordingStartMs": str(now - 5000), "RecordingEndMs": str(now)}
    query = f"?menu_id={m['id']}&attempt=1" if path == "actions/menu" else ""
    assert tenant.id not in signed_by(other, path + query, params).text
    assert naming(other, cid) == []
    caller.hangup()
    closed = run_jobs_until(world, lambda: seen(stream, "conversation.closed", conversation_id=cid), "closed")[0]
    assert closed["data"]["result"] == "abandoned"


@pytest.mark.parametrize("path", ["mpc-status", "call-status", "recording"])
def test_another_tenants_engine_callback_naming_an_answered_call_changes_nothing(world, tenant, path):
    live, other = connect(tenant), world.new_tenant("Other")
    now = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    params = {
        "mpc-status": {"EventName": "MPCEnd", "MPCName": live.caller.mpc, "MPCEndTime": now, "SequenceNumber": "99"},
        "call-status": {"EventName": "Hangup", "CallUUID": live.leg.call_uuid, "To": live.leg.to, "Role": "agent"},
        "recording": {
            "EventName": "MPCRecordingCompleted",
            "RecordingUUID": str(uuid.uuid4()),
            "RecordingURL": "https://example.test/Recording/forged.mp3",
            "RecordingDuration": "5",
            "RecordingStartTime": now,
            "RecordingEndTime": now,
        },
    }[path]
    params |= {"HangupTime": now, "PlivoHangupCauseCode": "4000", "PlivoHangupSource": "Callee"}
    before = naming(tenant, live.id)
    run = live.caller.mpc.removeprefix("mpc_")
    signed_by(other, f"{path}?call_uuid={live.caller.call_uuid}&qnr_run_uuid={run}", params)
    assert naming(tenant, live.id) == before and live.caller.state == live.leg.state == "in_progress"
    assert ok(tenant.api.get("/api/v1/recordings", params={"conversation_id": live.id}))["data"] == []


def leg_ends(world, leg_uuid: str) -> list:
    with world._db.cursor() as cur:
        cur.execute("SELECT end_time FROM call_leg WHERE call_uuid = %s", (leg_uuid,))
        return [row[0] for row in cur.fetchall()]


def test_a_participant_exit_for_a_tenants_own_call_cannot_end_another_tenants_leg(world, tenant):
    live, other = connect(tenant), world.new_tenant("Other")
    own, leg, forged = connect(other), live.leg.call_uuid, "2000-01-01 00:00:00"
    world.wait(lambda: leg_ends(world, leg) == [None], what="the agent's leg row")
    params = {"EventName": "ParticipantExit", "MPCName": own.caller.mpc, "SequenceNumber": "99"}
    params |= {"ParticipantCallUUID": leg, "ParticipantRole": "agent", "MemberAddress": "sip:forged@phone.plivo.com"}
    params |= {"ParticipantJoinTime": forged, "ParticipantExitTime": forged, "ParticipantExitCause": "Forged"}
    run = own.caller.mpc.removeprefix("mpc_")
    signed_by(other, f"mpc-status?call_uuid={own.caller.call_uuid}&qnr_run_uuid={run}", params)
    assert live.caller.state == live.leg.state == "in_progress" and leg_ends(world, leg) == [None]
    live.caller.hangup()
    live.stream.wait_for("call.ended", conversation_id=live.id)
    world.wait(lambda: leg_ends(world, leg) != [None], what="the agent's leg end")
    assert leg_ends(world, leg) != [forged]


def leg_recordings(world, leg_uuid: str) -> int:
    with world._db.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM recording r JOIN call_leg l ON l.id = r.call_leg_id WHERE l.call_uuid = %s",
            (leg_uuid,),
        )
        return cur.fetchone()[0]


def test_a_participant_recording_for_a_tenants_own_call_cannot_attach_to_another_tenants_leg(world, tenant):
    live, other = connect(tenant), world.new_tenant("Other")
    own, leg, now = connect(other), live.leg.call_uuid, time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    world.wait(lambda: leg_ends(world, leg) == [None], what="the agent's leg row")
    params = {
        "EventName": "ParticipantRecordingCompleted",
        "ParticipantCallUUID": leg,
        "RecordTrackType": "participant",
    }
    params |= {"RecordingUUID": str(uuid.uuid4()), "RecordingURL": "https://example.test/Recording/forged.mp3"}
    params |= {"RecordingDuration": "5", "RecordingStartTime": now, "RecordingEndTime": now}
    run = own.caller.mpc.removeprefix("mpc_")
    signed_by(other, f"recording?call_uuid={own.caller.call_uuid}&qnr_run_uuid={run}", params)
    assert leg_recordings(world, leg) == 0


def test_another_tenants_transcription_naming_a_recording_leaves_its_transcript_alone(world, tenant):
    settings(tenant, recording={"inbound": True})
    live, other = connect(tenant), world.new_tenant("Other")
    live.caller.hangup()
    rid = live.stream.wait_for("recording.ready", conversation_id=live.id)["data"]["recording_id"]
    run_jobs_until(world, lambda: seen(live.stream, "transcript.ready", recording_id=rid), "transcript.ready")
    track = next(
        r for r in world.plivo.recordings.values() if r.account.auth_id == tenant.auth_id and r.kind == "participant"
    )
    forged = json.dumps([{"start": 0.0, "end": 1.0, "text": "Forged words"}])
    params = {"recording_id": track.id, "call_uuid": track.call_uuid, "transcription": forged}
    signed_by(other, f"transcription?call_uuid={live.caller.call_uuid}", params)
    world.run_jobs()
    assert "Forged words" not in json.dumps(ok(tenant.api.get(f"/api/v1/recordings/{rid}/transcript")))


@pytest.mark.parametrize("path", ["answer", "hangup"])
def test_another_tenants_dial_callback_naming_a_ringing_click_to_call_changes_nothing(world, tenant, path):
    agent = tenant.agent().login()
    agent.phone.on_ring("ignore")
    _, cid = dial(tenant, agent)
    leg = agent.phone.wait_ring()
    other, before = world.new_tenant("Other"), naming(tenant, cid)
    params = {**answer_params("+14155550123"), "Direction": "outbound", "HangupCauseCode": "3010"}
    signed_by(other, f"{path}?dial={cid}", params)
    assert naming(tenant, cid) == before and leg.state == "ringing"
