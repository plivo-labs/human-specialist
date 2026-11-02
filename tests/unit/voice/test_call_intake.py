"""Call intake / initiate_mpc: the MPC answer XML (attribute order, escaping), the
queue-exit continuation URL, the MPCVE MPCInitialise stamp, the 10s missed-MPCInitialized timer,
and the handle_incoming_call wrapper side effects (the unchecked contact-params read, transfer
request type mapping, stop-recording spawn).
"""

import xml.etree.ElementTree as ET
from dataclasses import replace

import pytest

import hs.voice.intake as vi
from hs.intake.models import IncomingContactParams

_SUFFIX = "#rc=5&ct=2000&rp=5xx,ct"
_ESC_SUFFIX = "#rc=5&amp;ct=2000&amp;rp=5xx,ct"


def _params(**over):
    body = {
        "auth_id": "AUTH1",
        "contact_uuid": "ct-1",
        "flow_run_uuid": "flow-1",
        "queue_and_route_run_uuid": "qnr-1",
        "contact_channel": "call",
        "conversation_number": "9911",
        "flow_name": "voiceflow",
        "queue_config": {"queue_call_timeout": 120},
        "queue_and_route_config": {"queue": "q-uuid-1"},
    }
    body.update(over)
    return IncomingContactParams.model_validate(body)


class FakeVoiceRedis:
    def __init__(self, err=None):
        self.events = []
        self.err = err

    def set_mpc_callback_event(self, call_uuid, qnr_run_uuid, event_name, timestamp=""):
        self.events.append((call_uuid, qnr_run_uuid, event_name, timestamp))
        return self.err


@pytest.fixture()
def wired(monkeypatch):
    """Offline wiring: public base pinned, org reads → (tok, True, None) and tenant T1,
    _spawn runs INLINE (deterministic side-effect assertions)."""
    from types import SimpleNamespace

    import hs.plivo.voice as pv

    monkeypatch.setattr(pv, "settings", replace(pv.settings, public_base_url="https://example.test"))
    monkeypatch.setattr(vi.ar, "get_org_for_auth_id", lambda auth: SimpleNamespace(org_uuid="T1"))
    monkeypatch.setattr(
        vi.ar, "get_organisation_auth_token_and_record_value", lambda auth, inbound: ("tok", True, None)
    )
    monkeypatch.setattr(vi, "_spawn", lambda target, *a: target(*a))
    monkeypatch.setattr(vi, "VoiceRedis", FakeVoiceRedis)
    return {}


# ======================================================================================
# The XML — byte-pinned
# ======================================================================================
EXPECTED_XML = (
    '<Response><MultiPartyCall role="customer" waitTime="120" record="true"'
    f' recordingCallbackUrl="https://example.test/plivo/T1/recording?call_uuid=ct-1&amp;qnr_run_uuid=qnr-1{_ESC_SUFFIX}"'
    ' statusCallbackEvents="mpc-state-changes,participant-state-changes,add-participant-api-events,'
    'participant-audio-events,participant-digit-input-events"'
    f' statusCallbackUrl="https://example.test/plivo/T1/mpc-status?call_uuid=ct-1&amp;qnr_run_uuid=qnr-1{_ESC_SUFFIX}"'
    ' startMpcOnEnter="false" endMpcOnExit="true"'
    ' onExitActionUrl="https://example.test/plivo/T1/actions/queue-exit?call_uuid=ct-1&amp;qnr_run_uuid=qnr-1"'
    ' transcript="true"'
    f' transcriptionUrl="https://example.test/plivo/T1/transcription?call_uuid=ct-1{_ESC_SUFFIX}"'
    ' recordParticipantTrack="true"'
    ">mpc_qnr-1<AgentHoldMusic><Play>https://s3.amazonaws.com/plivocloud/music.mp3</Play></AgentHoldMusic>"
    "<CustomerHoldMusic><Play>https://s3.amazonaws.com/plivocloud/music.mp3</Play></CustomerHoldMusic>"
    "</MultiPartyCall></Response>"
)


def test_initiate_mpc_xml_byte_pin(wired):
    xml = vi.initiate_mpc(_params())
    assert xml == EXPECTED_XML


def test_org_read_error_renders_all_three_record_attrs_false(wired, monkeypatch):
    """:31-34 — a failed org lookup is logged only; record_value stays the zero bool → record/transcript/
    recordParticipantTrack all 'false' (and the XML still returns — Handle never errors)."""
    monkeypatch.setattr(
        vi.ar, "get_organisation_auth_token_and_record_value", lambda auth, inbound: ("", False, "db down")
    )
    xml = vi.initiate_mpc(_params())
    assert ' record="false"' in xml and ' transcript="false"' in xml
    assert ' recordParticipantTrack="false"' in xml


def test_custom_hold_music_used_and_defaults_on_empty(wired):
    p = _params(
        queue_config={
            "queue_call_timeout": 5,
            "agent_hold_music": {"url": "https://h/a.mp3"},
            "customer_hold_music": {"url": "https://h/c.mp3"},
        }
    )
    xml = vi.initiate_mpc(p)
    assert "<AgentHoldMusic><Play>https://h/a.mp3</Play></AgentHoldMusic>" in xml
    assert "<CustomerHoldMusic><Play>https://h/c.mp3</Play></CustomerHoldMusic>" in xml
    assert ' waitTime="5"' in xml


def test_a_hold_music_url_with_markup_characters_parses_back_unchanged(wired):
    url = "https://h/a.mp3?x=1&y=<2>&z=\"q\"&w='s'"
    p = _params(queue_config={"queue_call_timeout": 0, "agent_hold_music": {"url": url}})
    room = ET.fromstring(vi.initiate_mpc(p)).find("MultiPartyCall")  # noqa: S314 (HS's own output)
    assert room.findtext("AgentHoldMusic/Play") == url


def test_queue_exit_url_carries_the_tenant_the_run_and_the_outcome(wired):
    url = vi.queue_exit_url("A1", "C1", "Q1", result="queue_exit", digit="5")
    assert (
        url == "https://example.test/plivo/T1/actions/queue-exit?call_uuid=C1&qnr_run_uuid=Q1&result=queue_exit&digit=5"
    )


# ======================================================================================
# Side effects
# ======================================================================================
def test_initiate_mpc_stamps_mpcinitialise_and_schedules_timer(wired, monkeypatch, scheduled):
    rr = FakeVoiceRedis()
    monkeypatch.setattr(vi, "VoiceRedis", lambda: rr)
    vi.initiate_mpc(_params())
    assert len(rr.events) == 1
    call_uuid, qnr, event, ts = rr.events[0]
    assert (call_uuid, qnr, event) == ("ct-1", "qnr-1", "MPCInitialise")
    assert ts.isdigit() and len(ts) == 19  # nanosecond epoch
    # the MPCVE hash field is 'MPCInitialise' (s); the replayed callback event is 'MPCInitialized' (z)
    payload = {"callUUID": "ct-1", "qnrRunUUID": "qnr-1", "eventName": "MPCInitialized"}
    assert scheduled == [("missed_mpc_event", payload, {"delay": 10})]


def test_mpcve_write_failure_counts_mpc_failure_and_still_returns_xml(wired, monkeypatch):
    counted = []
    monkeypatch.setattr(vi, "increase_counter", lambda m, args: counted.append((m, args)))
    monkeypatch.setattr(vi, "VoiceRedis", lambda: FakeVoiceRedis(err="redis down"))
    xml = vi.initiate_mpc(_params())
    assert xml.startswith("<Response>")  # Handle NEVER errors
    assert counted == [(vi.MetricName.MPC_FAILURE, {"output_state": "add_customer"})]


def test_spawn_carries_the_request_context_into_the_thread():
    """A bare thread reads an EMPTY contextvars context, so code reading metadata["api_id"] crashes
    and the missed-event timer silently never registers. _spawn must copy the caller's context."""
    import threading

    from hs.infra.context import context_req_meta

    token = context_req_meta.set({"api_id": "ctx-pin-1"})
    try:
        seen, done = [], threading.Event()

        def body():
            seen.append(context_req_meta.get())
            done.set()

        vi._spawn(body)
        assert done.wait(2)
        assert seen == [{"api_id": "ctx-pin-1"}]
    finally:
        context_req_meta.reset(token)


def test_stop_call_recording_empty_creds_short_circuits(monkeypatch):
    """the Plivo client errors on empty creds — with the unchecked contact-params read the
    background call dies at client init, no HTTP and no ACR delete."""
    calls = []

    class FakeClient:
        def __init__(self, *a):
            calls.append("client")

    monkeypatch.setattr(vi.pv, "PlivoVoiceClient", FakeClient)
    vi._stop_call_recording("c1", "", "")
    assert calls == []


def test_stop_call_recording_success_deletes_acr(monkeypatch):
    flow = []

    class FakeClient:
        def __init__(self, auth_id, auth_token):
            flow.append(("client", auth_id, auth_token))

        def call_stop_recording(self, call_uuid):
            flow.append(("stop", call_uuid))
            return None, 204, None

    class FakeVR:
        def stop_recording_for_call(self, call_uuid):
            flow.append(("acr_del", call_uuid))
            return

    monkeypatch.setattr(vi.pv, "PlivoVoiceClient", FakeClient)
    monkeypatch.setattr(vi, "VoiceRedis", FakeVR)
    vi._stop_call_recording("c1", "A", "T")
    assert flow == [("client", "A", "T"), ("stop", "c1"), ("acr_del", "c1")]


# ======================================================================================
# handle_incoming_call — the wrapper
# ======================================================================================
class FakeRoutingRedis:
    def __init__(self, contact_params=None, tip=None):
        self._cp = contact_params
        self._tip = tip

    def get_contact_parameters(self, contact_uuid, qnr_run_uuid):
        return self._cp

    def get_transfer_in_progress_info(self, contact_uuid):
        return self._tip


def test_handle_incoming_call_unchecked_contact_params_bug(wired, monkeypatch):
    """The contact-params read is unchecked, so a cache MISS runs the side effects with empty
    params: stopping the recording gets EMPTY creds and the conversation event an empty params dict. None → {}."""
    stop_args, _event_args = [], []
    monkeypatch.setattr(vi, "RoutingRedis", lambda: FakeRoutingRedis(contact_params=None, tip=None))
    monkeypatch.setattr(vi, "_stop_call_recording", lambda c, a, t: stop_args.append((c, a, t)))
    xml = vi.handle_incoming_call(_params())
    assert xml == EXPECTED_XML
    assert stop_args == [("ct-1", "", "")]


def test_handle_incoming_call_passes_creds_and_transfer_type(wired, monkeypatch):
    stop_args, _event_args = [], []
    cp = {"auth_id": "A", "auth_token": "T", "conversation_number": "9911"}
    monkeypatch.setattr(vi, "RoutingRedis", lambda: FakeRoutingRedis(contact_params=cp, tip={"transfer_type": "warm"}))
    monkeypatch.setattr(vi, "_stop_call_recording", lambda c, a, t: stop_args.append((c, a, t)))
    vi.handle_incoming_call(_params())
    assert stop_args == [("ct-1", "A", "T")]
