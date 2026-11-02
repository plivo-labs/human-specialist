"""A call control answers a fixed message per failure kind; the engine's own error string never reaches the caller."""

import httpx
import pytest
import redis
import sqlalchemy.exc

import hs.infra.http_session as hsess
from hs.access import Call
from hs.errors import ApiError, status_of
from hs.infra.redis import Cache
from hs.plivo import voice as pv
from hs.routing.redis_store import RoutingRedis
from hs.voice import api
from hs.voice import constants as vc
from hs.voice import hangup as voice_hangup

KINDS = ["call_control_failed", "recording_control_failed", "transfer_failed", "participant_failed", "monitor_failed"]


def answer(err: str, kind: str = "transfer_failed") -> tuple[int, str, str]:
    with pytest.raises(ApiError) as raised:
        api._done(err, kind)
    return raised.value.status, raised.value.code, raised.value.message


@pytest.mark.parametrize("kind", KINDS)
def test_every_refusal_of_a_kind_answers_the_same_409_whatever_the_engine_said(kind):
    owner, cache = answer("not the owner of the channel", kind), answer("contact params not found in cache", kind)
    assert owner == cache and owner[:2] == (409, kind)
    assert "owner" not in owner[2] and "cache" not in owner[2]


@pytest.mark.parametrize(
    ("err", "kind", "own"),
    [
        ("maximum secondary participants reached", "participant_failed", "too_many_participants"),
        (vc.MONITOR_EVENT_SUPERVISOR_PRESENT, "monitor_failed", "supervisor_present"),
    ],
)
def test_a_refusal_the_caller_can_act_on_answers_its_own_code(err, kind, own):
    status, code, message = answer(err, kind)
    assert (status, code) == (409, own) and err not in message


def plivo_error(monkeypatch, reply) -> str:
    monkeypatch.setattr(hsess.WRITES, "request", reply)
    return pv.PlivoVoiceClient("MA1", "secret-token").mpc_pause_recording("name_mpc_1")[2]


def refused(method, url, **kw):
    raise httpx.ConnectError("connection refused by 10.0.0.9")


def status(code: int):
    return lambda method, url, **kw: httpx.Response(code, request=httpx.Request(method, url))


@pytest.mark.parametrize("reply", [refused, status(503)])
def test_a_plivo_outage_answers_503_even_inside_an_engine_message(monkeypatch, reply):
    err = plivo_error(monkeypatch, reply)
    got = answer(f"failed to delete MPC call leg-1: {err}")
    assert got[:2] == (503, "dependency_unavailable") and "10.0.0.9" not in got[2] and "503" not in got[2]


def test_a_plivo_refusal_is_a_409_and_a_retryable_store_failure_a_503(monkeypatch):
    assert answer(plivo_error(monkeypatch, status(404)))[:2] == (409, "transfer_failed")
    store = voice_hangup.retryable_voice_callback_error("participant cache persistence unavailable")
    assert answer(store)[:2] == (503, "dependency_unavailable")


def test_a_refusal_read_while_redis_does_not_answer_is_a_503(monkeypatch):
    monkeypatch.setattr(Cache.get_instance(), "_client", redis.Redis(port=1, socket_connect_timeout=0.2))
    for refusal in (lambda: api._mapping(Call("c", "n", "call-1", {})), lambda: api._done("call not found")):
        with pytest.raises(ApiError) as raised:
            refusal()
        assert (raised.value.status, raised.value.code) == (503, "dependency_unavailable")


def test_a_redis_fault_while_reading_whether_the_room_ends_is_a_503(monkeypatch):
    monkeypatch.setattr(RoutingRedis, "get_contact_to_agent_mapping", lambda self, call_uuid: {"QnrRunUuid": "qnr-1"})
    monkeypatch.setattr(Cache.get_instance(), "_client", redis.Redis(port=1, socket_connect_timeout=0.2))
    with pytest.raises(redis.ConnectionError) as raised:
        api._mapping(Call("c", "n", "call-1", {}))
    assert status_of(raised.value) == 503


def test_a_database_that_does_not_answer_is_a_503():
    down = sqlalchemy.exc.OperationalError("SELECT 1", {}, Exception("timeout expired"))
    assert status_of(down) == 503
