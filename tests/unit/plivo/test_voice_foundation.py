"""The voice foundation: key shapes, the three voice Luas, the Plivo-facing callback URL templates
(on PUBLIC_BASE_URL), and the MPC/Calls client URL+verb shapes.
"""

from dataclasses import replace
from types import SimpleNamespace

import hs.infra.http_session as hsess


def test_voice_key_shapes():
    from hs.voice import redis_store as vr

    assert vr.mpc_call_events_key("c1", "q1") == "MPCVE:c1:q1"
    assert vr.mpc_call_events_key("c1", "") == "MPCVE:c1"  # the no-qnr variant
    assert vr.mpc_calls_key("p1", "q1") == "MPCCALLSKEY:p1:q1"
    assert vr.mpc_transfer_calls_key("q1") == "MTCK:q1"
    assert vr.active_conference_key("q1") == "ACTIVECONFERENCE:q1"
    assert vr.prompt_param_key("c1") == "PROMPT:c1"
    assert vr.supervisor_to_call_key("s1") == "SUPCM:s1"
    assert vr.call_to_supervisor_key("c1") == "CSUPM:c1"
    assert vr.current_participant_key("ct1") == "CAP:ct1"
    assert vr.agent_switch_key("q1", "sip:a@x") == "AS:q1:sip:a@x"


def test_voice_luas_on_redis():
    """The claim, ordering-gate, release and guarded-delete Luas, run on fakeredis's Lua engine."""
    import fakeredis

    from hs.voice import redis_store as vr

    class FakeCache:
        client = fakeredis.FakeStrictRedis(decode_responses=True)

    redis, raw = vr.VoiceRedis(cache=FakeCache()), FakeCache.client
    assert redis.set_mpc_callback_event("c1", "q1", "MPCStart", "1") is None
    assert redis.set_mpc_callback_event("c1", "q1", "MPCStart", "2") == "value already exists"
    assert redis.set_mpc_callback_event("c1", "q1", "ParticipantHold", "1") is None
    assert redis.set_mpc_callback_event("c1", "q1", "ParticipantHold", "2") is None  # repeatable: overwrites
    assert raw.hget("MPCVE:c1:q1", "ParticipantHold") == "2" and raw.ttl("MPCVE:c1:q1") > 0

    def join(seq, claim):
        return redis.set_mpc_event_with_dependency(
            "c1", "q1", f"ParticipantJoin:+1:{seq}", seq, "ParticipantExit:+1", claim
        )

    assert join("2", "t2") is None
    assert join("2", "t3") == "value already exists"
    raw.hset("MPCVE:c1:q1", "ParticipantExit:+1:5", "x")  # a later-sequenced exit supersedes earlier joins
    assert join("3", "t4") == "later event found"
    assert join("6", "t6") is None
    assert (
        redis.release_mpc_callback_event("c1", "q1", "ParticipantJoin:+1:6", "other") == "event claim no longer owned"
    )
    assert redis.release_mpc_callback_event("c1", "q1", "ParticipantJoin:+1:6", "t6") is None
    assert raw.hget("MPCVE:c1:q1", "ParticipantJoin:+1:6") is None

    raw.set("SUPCM:s1", '{"call_uuid":"c1"}')
    assert redis.delete_supervisor_to_call_mapping("s1", "c2") == "don't clear the cache"
    assert redis.delete_supervisor_to_call_mapping("s1", "c1") is None and not raw.exists("SUPCM:s1")
    assert redis.delete_call_to_supervisor_mapping("c1", "s1") == "don't clear the cache"  # absent


def test_repeatable_events_set_is_pinned():
    from hs.voice.constants import REPEATABLE_EVENTS

    assert {
        "ParticipantHold",
        "ParticipantUnhold",
        "ParticipantSpeakStopped",
        "ParticipantAudioStopped",
        "ParticipantDigitInput",
    } == REPEATABLE_EVENTS


def test_mpcve_writer_claim_and_ttl_are_atomic(monkeypatch):
    """Repeatability, claim value, and TTL all reach one Lua call."""
    from hs.voice import redis_store as vr

    class FakeCache:
        client = object()

    redis = vr.VoiceRedis(cache=FakeCache())
    calls = []
    redis._eval = lambda src, keys, args: calls.append((src, keys, args)) or "success"

    assert redis.set_mpc_callback_event("c1", "q1", "ParticipantHold", "111") is None
    assert redis.set_mpc_callback_event("c1", "q1", "MPCInitialise", "222") is None
    assert calls == [
        (vr.SET_MPC_EVENT_WITH_TTL, ["MPCVE:c1:q1"], ["ParticipantHold", "111", "1", str(2 * 3600)]),
        (vr.SET_MPC_EVENT_WITH_TTL, ["MPCVE:c1:q1"], ["MPCInitialise", "222", "0", str(2 * 3600)]),
    ]

    redis._eval = lambda *_args: "value already exists"
    assert redis.set_mpc_callback_event("c1", "q1", "MPCInitialise", "333") == "value already exists"


def test_dependency_event_passes_ttl_to_atomic_lua():
    from hs.voice import redis_store as vr

    class FakeCache:
        client = object()

    redis = vr.VoiceRedis(cache=FakeCache())
    calls = []
    redis._eval = lambda src, keys, args: calls.append((src, keys, args)) or "success"

    assert (
        redis.set_mpc_event_with_dependency(
            "c1",
            "q1",
            "ParticipantJoin:+1:2",
            sequence_number="2",
            dependent_event="ParticipantExit:+1",
            timestamp="claim-1",
        )
        is None
    )
    assert calls[0][2][-1] == str(2 * 3600)


def test_mpc_event_claim_release_is_compare_and_delete():
    from hs.voice import redis_store as vr

    class FakeCache:
        client = object()

    redis = vr.VoiceRedis(cache=FakeCache())
    calls = []
    redis._eval = lambda src, keys, args: calls.append((src, keys, args)) or 1

    assert redis.release_mpc_callback_event("c1", "q1", "ParticipantJoin:+1:2", "claim-123") is None
    assert calls == [(vr.RELEASE_MPC_EVENT_CLAIM, ["MPCVE:c1:q1"], ["ParticipantJoin:+1:2", "claim-123"])]

    redis._eval = lambda *_args: 0
    assert redis.release_mpc_callback_event("c1", "q1", "MPCStart", "old-claim") == "event claim no longer owned"


def test_active_conference_miss_is_noop_but_redis_failure_is_not():
    from hs.voice.redis_store import VoiceRedis

    class FakeCache:
        def __init__(self, result):
            self.result = result

        def get_with_error(self, _key):
            return self.result

    assert VoiceRedis(cache=FakeCache((None, None))).is_conference_active("q1") == (False, None)
    assert VoiceRedis(cache=FakeCache(("false", None))).is_conference_active("q1") == (False, None)
    assert VoiceRedis(cache=FakeCache(("true", None))).is_conference_active("q1") == (True, None)
    assert VoiceRedis(cache=FakeCache((None, "redis unavailable"))).is_conference_active("q1") == (
        False,
        "redis unavailable",
    )


def test_callback_urls_are_the_tenants_plivo_routes(monkeypatch):
    """/plivo/{tenant}/<name>?call_uuid&qnr_run_uuid, Plivo's retry policy in the fragment."""
    import hs.plivo.voice as pv
    import hs.routing.account_read as ar

    monkeypatch.setattr(pv, "settings", replace(pv.settings, public_base_url="https://example.test"))
    monkeypatch.setattr(ar, "get_org_for_auth_id", lambda auth: SimpleNamespace(org_uuid=f"T-{auth}"))
    suffix = "#rc=5&ct=2000&rp=5xx,ct"
    run = "call_uuid=ct1&qnr_run_uuid=q1"
    assert (
        pv.mpc_call_status_callback_url("A", "ct1", "q1") == f"https://example.test/plivo/T-A/call-status?{run}{suffix}"
    )
    assert pv.mpc_status_callback_url("A", "ct1", "q1") == f"https://example.test/plivo/T-A/mpc-status?{run}{suffix}"
    assert pv.mpc_recording_callback_url("A", "ct1", "q1") == f"https://example.test/plivo/T-A/recording?{run}{suffix}"
    assert (
        pv.mpc_transcription_callback_url("A", "ct1")
        == f"https://example.test/plivo/T-A/transcription?call_uuid=ct1{suffix}"
    )


def test_make_mpc_id():
    from hs.plivo.voice import make_mpc_id

    assert make_mpc_id(friendly_name="mpc_q1") == "name_mpc_q1"


def test_client_url_and_verb_shapes(monkeypatch):
    """The Plivo resource forms under /v1/Account/{auth}/ with the tenant's basic auth."""
    import hs.plivo.voice as pv

    sent = []

    def fake_request(method, url, json=None, data=None, params=None, auth=None, headers=None, timeout=None, **kw):
        assert kw.get("follow_redirects") is False
        sent.append((method, url, json, params, auth))

        class R:
            status_code = 204
            content = b""
            text = ""

        return R()

    monkeypatch.setattr(hsess.WRITES, "request", fake_request)
    monkeypatch.setattr(pv, "settings", replace(pv.settings, plivo_api_url="http://mock:5020"))
    c = pv.PlivoVoiceClient("AUTH1", "TOK1")
    c.mpc_add_participant("name_mpc_q1", {"role": "agent", "to": "sip:a@x<sip:b@x"})
    c.mpc_update_participant("name_mpc_q1", "10", {"hold": True})
    c.mpc_stop("uuid_u1")
    c.mpc_start_speak("name_mpc_q1", "10", {"text": "hi"})
    c.mpc_start_play_audio("name_mpc_q1", "10", {"urls": "http://a/x.mp3"})
    c.mpc_start_recording("name_mpc_q1", {"file_format": "mp3"})
    c.mpc_pause_participant_recording("name_mpc_q1", "10")
    c.call_update("call-1", {"legs": "aleg", "aleg_url": "http://r/x"})
    c.call_delete("call-1")
    urls = [(m, u) for (m, u, *_rest) in sent]
    base = "http://mock:5020/v1/Account/AUTH1"
    assert urls == [
        ("POST", f"{base}/MultiPartyCall/name_mpc_q1/Participant/"),
        ("POST", f"{base}/MultiPartyCall/name_mpc_q1/Participant/10/"),
        ("DELETE", f"{base}/MultiPartyCall/uuid_u1/"),
        ("POST", f"{base}/MultiPartyCall/name_mpc_q1/Member/10/Speak/"),
        ("POST", f"{base}/MultiPartyCall/name_mpc_q1/Member/10/Play/"),
        ("POST", f"{base}/MultiPartyCall/name_mpc_q1/Record/"),
        ("POST", f"{base}/MultiPartyCall/name_mpc_q1/Participant/10/Record/Pause/"),
        ("POST", f"{base}/Call/call-1/"),
        ("DELETE", f"{base}/Call/call-1/"),
    ]
    assert all(a == ("AUTH1", "TOK1") for (_m, _u, _b, _p, a) in sent)
    # parallel-ring 'to' rides VERBATIM ('<'-joined)
    assert sent[0][2]["to"] == "sip:a@x<sip:b@x"


def test_client_error_postures(monkeypatch):
    import hs.plivo.voice as pv

    def boom(*a, **k):
        raise RuntimeError("net down")

    monkeypatch.setattr(hsess.WRITES, "request", boom)
    body, status, err = pv.PlivoVoiceClient("A", "T").call_delete("c1")
    assert (body, status) == (None, 0) and "net down" in err

    def non2xx(method, url, **k):
        class R:
            status_code = 404
            content = b'{"error":"not found"}'
            text = '{"error":"not found"}'

            @staticmethod
            def json():
                return {"error": "not found"}

        return R()

    monkeypatch.setattr(hsess.READS, "request", non2xx)
    body, status, err = pv.PlivoVoiceClient("A", "T").mpc_get("uuid_x")
    assert status == 404 and body == {"error": "not found"} and "non 2xx" in err
    # The provider primitive cannot distinguish an already-absent call from a
    # wrong account/resource identifier. Terminal-aware callers own any safe
    # idempotent-404 decision.
    monkeypatch.setattr(hsess.WRITES, "request", non2xx)
    body, status, err = pv.PlivoVoiceClient("A", "T").call_delete("c1")
    assert status == 404 and body == {"error": "not found"} and "non 2xx" in err
    body, status, err = pv.PlivoVoiceClient("A", "T").call_delete("c1", quiet_not_found=True)
    assert status == 404 and body == {"error": "not found"} and "non 2xx" in err
    body, status, err = pv.PlivoVoiceClient("A", "T").call_update("c1", {})
    assert status == 404 and "non 2xx" in err


def test_client_non2xx_logs_only_allowlisted_provider_diagnostics(monkeypatch):
    import hs.plivo.voice as pv

    provider_request_id = "03f1655c-6c5e-4f19-9878-66724bf890d0"
    secret = "provider-secret-that-must-not-be-logged"
    phone = "+14155550123"
    payload = {
        "api_id": provider_request_id,
        "error_code": 30001,
        "error": f"invalid destination {phone}",
        "auth_token": secret,
        "destination": phone,
    }

    def non2xx(*_args, **_kwargs):
        class R:
            status_code = 400
            content = b'{"body":"must not be logged"}'
            headers = {"Authorization": secret, "X-Plivo-Request-ID": provider_request_id}

            @staticmethod
            def json():
                return payload

        return R()

    logs = []
    monkeypatch.setattr(hsess.WRITES, "request", non2xx)
    monkeypatch.setattr(pv.logger, "error", lambda *args, **kwargs: logs.append(args[0]))

    body, status, err = pv.PlivoVoiceClient("A", "T").call_update("call-1", {})

    assert body == payload
    assert status == 400
    assert err == "non 2xx response (400)"
    assert len(logs) == 1
    assert f"provider_request_id={provider_request_id}" in logs[0]
    assert "provider_error_code=30001" in logs[0]
    assert "response_body_present=True" in logs[0]
    for forbidden in (secret, phone, payload["error"], "destination", "Authorization", "must not be logged"):
        assert forbidden not in logs[0]


def test_client_quiet_404_uses_safe_header_request_id_and_preserves_result(monkeypatch):
    import hs.plivo.voice as pv

    provider_request_id = "3d2d3ec7-b3b9-4912-b639-2188af82cfd2"
    payload = {"api_id": "not-a-uuid", "code": "404", "error": "resource +14155550123 not found"}

    def not_found(*_args, **_kwargs):
        class R:
            status_code = 404
            content = b'{"error":"sensitive provider text"}'
            headers = {"X-Plivo-Request-UUID": provider_request_id, "X-Private-Diagnostic": "do-not-log-me"}

            @staticmethod
            def json():
                return payload

        return R()

    info_logs = []
    error_logs = []
    monkeypatch.setattr(hsess.READS, "request", not_found)
    monkeypatch.setattr(pv.logger, "info", lambda *args, **kwargs: info_logs.append(args[0]))
    monkeypatch.setattr(pv.logger, "error", lambda *args, **kwargs: error_logs.append(args[0]))

    result = pv.PlivoVoiceClient("A", "T").mpc_get("uuid_x", quiet_not_found=True)

    assert result == (payload, 404, "non 2xx response (404)")
    assert not error_logs
    assert len(info_logs) == 1
    assert f"provider_request_id={provider_request_id}" in info_logs[0]
    assert "provider_error_code=404" in info_logs[0]
    assert "caller supplied exact terminal proof" in info_logs[0]
    assert payload["error"] not in info_logs[0]
    assert "X-Private-Diagnostic" not in info_logs[0]
    assert "do-not-log-me" not in info_logs[0]


def test_provider_diagnostics_reject_arbitrary_ids_and_code_text():
    import hs.plivo.voice as pv

    assert (
        pv._provider_error_diagnostics(
            {
                "api_id": "request-for-+14155550123",
                "request_id": "+14155550123",
                "error_code": "INVALID DESTINATION +14155550123",
                "code": "1234567890",
            },
            {"X-Plivo-Request-ID": "token-not-a-uuid", "X-Request-ID": "+14155550123"},
        )
        == ""
    )


def test_a_read_rides_out_a_502_503_504_and_a_write_is_one_attempt(monkeypatch):
    """Every client instance shares the pooled clients; reads retry a gateway fault twice, writes never."""
    import httpx

    import hs.plivo.voice as pv

    statuses, sent = iter([503, 502, 200]), []

    def flaky(method, url, **kw):
        sent.append(method)
        return httpx.Response(next(statuses, 503), request=httpx.Request(method, url))

    monkeypatch.setattr(hsess.time, "sleep", lambda s: None)
    monkeypatch.setattr(hsess.READS, "request", flaky)
    monkeypatch.setattr(hsess.WRITES, "request", flaky)
    assert pv.PlivoVoiceClient("A1", "T1").mpc_get("uuid_1")[1] == 200 and sent == ["GET"] * 3
    sent.clear()
    assert pv.PlivoVoiceClient("A2", "T2").mpc_stop("uuid_2")[1] == 503 and sent == ["DELETE"]
