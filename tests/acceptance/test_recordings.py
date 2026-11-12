"""Recordings: never a raw URL, only a short-lived signed audio_url with Range; download and delete rights."""

import json
import logging
import time
from urllib.parse import parse_qsl, urlencode, urlsplit

from tests.acceptance.conftest import (
    connect,
    error,
    ok,
    plivo_post,
    replayed,
    run_jobs_until,
    seen,
    settings,
    team,
    webhook,
)

R = "/api/v1/recordings"


def recorded(tenant, agent=None):
    """A finished inbound call with inbound recording on. Returns (live, recording_id)."""
    settings(tenant, recording={"inbound": True})
    live = connect(tenant, agent)
    live.caller.hangup()
    return live, live.stream.wait_for("recording.ready", conversation_id=live.id)["data"]["recording_id"]


def audio_url(client, rid: str, **params) -> str:
    return ok(client.get(f"{R}/{rid}", params=params))["audio_url"]


def tampered(url: str, **changes) -> str:
    parts = urlsplit(url)
    query = {**dict(parse_qsl(parts.query)), **changes}
    return parts._replace(query=urlencode(query)).geturl()


def test_no_response_or_event_carries_the_raw_recording_url(world, tenant):
    sink = world.webhook_sink()
    webhook(tenant, sink.url)
    live, rid = recorded(tenant)
    run_jobs_until(
        world, lambda: seen(live.stream, "conversation.closed", conversation_id=live.id), "conversation.closed"
    )[0]
    raw = {
        str(v)
        for cb in world.plivo.callbacks
        if f"/plivo/{tenant.id}/" in cb.url
        for k, v in cb.params.items()
        if "record" in k.lower() and "url" in k.lower()
    }
    assert raw, "the simulator handed HS no recording URL"
    paths = (
        f"{R}/{rid}",
        R,
        f"/api/v1/conversations/{live.id}",
        f"/api/v1/conversations/{live.id}/timeline",
        "/api/v1/events",
    )
    texts = [tenant.api.get(p).text for p in paths] + [d.body.decode() for d in sink.deliveries]
    texts += [json.dumps(e) for e in live.stream.events]
    assert not [u for u in raw for text in texts if u in text]


def test_the_audio_url_is_absolute_signed_and_lives_about_5_minutes(world, tenant):
    _, rid = recorded(tenant)
    url = audio_url(tenant.api, rid)
    query = dict(parse_qsl(urlsplit(url).query))
    assert url.startswith(f"{world.base_url}/api/v1/recordings/{rid}/audio?") and query["sig"]
    assert 240 <= int(query["exp"]) - time.time() <= 330


def test_the_audio_plays_and_seeks_with_http_range(world, tenant):
    _, rid = recorded(tenant)
    url = audio_url(tenant.api, rid)
    part = world.anon().get(url, headers={"Range": "bytes=0-99"})
    assert part.status_code == 206 and part.headers["content-range"].startswith("bytes 0-99/")
    assert len(part.content) == 100
    full = world.anon().get(url)
    assert full.status_code == 200 and full.headers["accept-ranges"] == "bytes"


def test_an_unsigned_or_tampered_audio_url_is_refused(world, tenant):
    live, rid = recorded(tenant)
    url = audio_url(live.agent.api(), rid)
    changes = [{"sig": "0" * 64}, {"exp": str(int(time.time()) + 3600)}, {"download": "1"}]
    assert [c for c in changes if world.anon().get(tampered(url, **c)).status_code not in (401, 403)] == []
    assert world.anon().get(f"{world.base_url}{R}/{rid}/audio").status_code in (401, 403, 422)
    assert tenant.api.get(f"{R}/{rid}/audio").status_code in (401, 403, 422)


def test_download_is_signed_in_for_recordings_keys_and_each_role_that_holds_the_grant(tenant):
    t = team(tenant, "Floor")
    agent = tenant.agent("Agent", teams=[t]).login()
    other = tenant.agent("Other", teams=[t]).login(available=False)
    sup = tenant.agent("Sup", role="supervisor", teams=[t]).login(available=False)
    _, rid = recorded(tenant, agent)
    for client in (tenant.key_with("recordings"), sup.api(), agent.api()):
        assert "download=1" in audio_url(client, rid, download=1)
    error(other.api().get(f"{R}/{rid}", params={"download": 1}), 404)
    settings(tenant, permissions={"agent": {"download_recording": False}, "supervisor": {"download_recording": False}})
    for client in (agent.api(), sup.api()):
        error(client.get(f"{R}/{rid}", params={"download": 1}), 403, "download_not_allowed")
    assert "download=1" not in audio_url(agent.api(), rid)
    assert "download=1" in audio_url(tenant.key_with("recordings"), rid, download=1)


def test_recordings_are_visible_only_to_their_agent_and_tenant(world, tenant):
    live, rid = recorded(tenant)
    ok(live.agent.api().get(f"{R}/{rid}"))
    other_agent = tenant.agent("Other").login(available=False)
    other_tenant = world.new_tenant("Other")
    for client in (other_agent.api(), other_tenant.api):
        error(client.get(f"{R}/{rid}"), 404)
        error(client.get(f"{R}/{rid}/transcript"), 404)
        assert rid not in [r["id"] for r in ok(client.get(R))["data"]]
    error(other_tenant.api.delete(f"{R}/{rid}"), 404)
    error(live.agent.api().delete(f"{R}/{rid}"), (401, 403))


def test_recordings_list_by_conversation(tenant):
    live, rid = recorded(tenant)
    rows = ok(tenant.api.get(R, params={"conversation_id": live.id}))["data"]
    assert [r["id"] for r in rows] == [rid] and rows[0]["audio_url"]
    assert rows[0]["conversation_id"] == live.id


def test_deleting_a_recording_deletes_it_at_plivo_too(world, tenant):
    _, rid = recorded(tenant)
    url = audio_url(tenant.api, rid)
    ok(tenant.api.delete(f"{R}/{rid}"))
    assert any(r.method == "DELETE" and "/Recording/" in r.path for r in world.plivo.requests)
    error(tenant.api.get(f"{R}/{rid}"), 404)
    assert world.anon().get(url).status_code in (403, 404, 410)


def test_deleting_a_recording_deletes_its_participant_tracks_at_plivo_too(world, tenant):
    _, rid = recorded(tenant)
    mine = [r for r in world.plivo.recordings.values() if r.account.auth_id == tenant.auth_id]
    tracks = {r.id for r in mine if r.kind == "participant"}
    world.wait(lambda: tracks <= {c.params.get("RecordingUUID") for c in world.plivo.callbacks if c.status == 200})
    ok(tenant.api.delete(f"{R}/{rid}"))
    assert len(tracks) == 2 and all(r.deleted for r in mine)
    error(tenant.api.get(f"{R}/{rid}/transcript"), 404)


def test_a_plivo_outage_on_deleting_a_recording_is_a_503_and_a_retry_deletes_it(world, tenant):
    _, rid = recorded(tenant)
    world.plivo.reject_next("/Recording/", 503, account=tenant.auth_id)
    error(tenant.api.delete(f"{R}/{rid}"), 503, "dependency_unavailable")
    ok(tenant.api.delete(f"{R}/{rid}"))
    mine = [r for r in world.plivo.recordings.values() if r.account.auth_id == tenant.auth_id]
    assert mine and all(r.deleted for r in mine)
    error(tenant.api.get(f"{R}/{rid}"), 404)


def test_a_calls_tracks_go_with_its_last_recording(world, tenant):
    settings(tenant, recording={"inbound": True})
    target = tenant.agent("Target").login()
    billing = tenant.queue(name="Billing", agents=[target.id])
    live = connect(tenant)
    transfer = {"mode": "cold", "target": {"type": "queue", "queue_id": billing["id"]}}
    ok(live.agent.api().post(f"/api/v1/calls/{live.id}/transfers", json=transfer))
    leg = target.phone.wait_ring()
    world.wait(lambda: leg.mpc, what="the target in the second room")
    live.caller.hangup()
    rows = world.wait(lambda: (r := ok(tenant.api.get(R, params={"conversation_id": live.id}))["data"])[1:] and r)
    tracks = [
        r for r in world.plivo.recordings.values() if r.account.auth_id == tenant.auth_id and r.kind == "participant"
    ]
    world.wait(
        lambda: (
            {r.id for r in tracks} <= {c.params.get("RecordingUUID") for c in world.plivo.callbacks if c.status == 200}
        )
    )
    ok(tenant.api.delete(f"{R}/{rows[0]['id']}"))
    assert len(rows) == 2 and not any(r.deleted for r in tracks)
    ok(tenant.api.delete(f"{R}/{rows[1]['id']}"))
    assert len(tracks) == 4 and all(r.deleted for r in tracks)


def transcript(client, rid: str):
    r = client.get(f"{R}/{rid}/transcript")
    return r.json() if r.status_code == 200 else None


def test_a_recorded_calls_transcript_is_readable(world, tenant):
    live, rid = recorded(tenant)
    got = run_jobs_until(world, lambda: transcript(tenant.api, rid), "the transcript")
    assert got["recording_id"] == rid and got["complete"] is True
    assert world.plivo.transcript in [s["text"] for s in got["segments"]]
    assert transcript(live.agent.api(), rid) == got


def test_a_recorded_calls_transcript_is_announced(world, tenant):
    live, rid = recorded(tenant)
    run_jobs_until(world, lambda: seen(live.stream, "transcript.ready", recording_id=rid), "transcript.ready")


def provider_track(world, tenant, kind: str):
    return next(r for r in world.plivo.recordings.values() if r.account.auth_id == tenant.auth_id and r.kind == kind)


def test_the_rooms_transcription_callback_is_taken_without_an_error(world, tenant, caplog):
    _live, _rid = recorded(tenant)
    room = provider_track(world, tenant, "mpc")

    def taken():
        return [c for c in world.plivo.callbacks if c.params.get("recording_id") == room.id]

    world.wait(taken, what="the room recording's transcription callback")
    world.advance(10)
    assert [c.status for c in taken()] == [200]
    assert not [r.getMessage() for r in caplog.records if "Organization not found" in r.getMessage()]


def test_a_transcript_plivo_has_not_published_yet_is_fetched_again_without_an_error(world, tenant, caplog):
    live, rid = recorded(tenant)
    run_jobs_until(world, lambda: seen(live.stream, "transcript.ready", recording_id=rid), "transcript.ready")
    track = provider_track(world, tenant, "participant")
    for _ in range(2):
        world.plivo.reject_next(f"/Transcription/{track.id}", 404)
    caplog.clear()
    fetched = len(world.plivo.requests_to(f"/Transcription/{track.id}"))
    again = {"recording_id": track.id, "call_uuid": track.call_uuid, "transcription": ""}
    ok(plivo_post(world, tenant.id, f"transcription?c={live.id}", again, tenant.auth_token))
    world.advance(10)
    world.advance(40)
    assert len(world.plivo.requests_to(f"/Transcription/{track.id}")) >= fetched + 2, "fetched again after a 404"
    assert not [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR and track.id in r.getMessage()]


def test_a_track_plivo_never_publishes_reads_incomplete_without_its_text_and_never_announces_the_transcript(
    world, tenant
):
    settings(tenant, recording={"inbound": True})
    live = connect(tenant)
    held = world.plivo.fault(
        "delay",
        lambda r: "/transcription" in r.path and r.params.get("call_uuid") == live.leg.call_uuid,
        account=tenant.auth_id,
    )
    live.caller.hangup()
    rid = live.stream.wait_for("recording.ready", conversation_id=live.id)["data"]["recording_id"]
    world.wait(lambda: held.hits, what="the agent track's transcription callback held back")
    track = held.hits[0].params["recording_id"]
    run_jobs_until(
        world, lambda: len(ok(tenant.api.get(f"{R}/{rid}/transcript"))["segments"]) == 1, "the caller's text"
    )
    for _ in range(60):
        world.plivo.reject_next(f"/Transcription/{track}", 404)
    again = {"recording_id": track, "call_uuid": live.leg.call_uuid, "transcription": ""}
    ok(plivo_post(world, tenant.id, f"transcription?c={live.id}", again, tenant.auth_token))
    for _ in range(12):
        world.advance(30)
    for _ in range(3):
        world.advance(3600)
    world.wait(world.plivo.idle, what="the Plivo simulator to go idle")
    asked = len(world.plivo.requests_to(f"/Transcription/{track}"))
    world.advance(3600)
    transcript = ok(tenant.api.get(f"{R}/{rid}/transcript"))
    assert transcript["complete"] is False and len(transcript["segments"]) == 1
    assert len(world.plivo.requests_to(f"/Transcription/{track}")) == asked > 0
    assert not replayed(tenant, "transcript.ready", recording_id=rid)
