"""Self-tests for the Plivo simulator and the harness receivers, against a fake HS (no HS process)."""

import json
import math
import threading
import time
import uuid
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from urllib.parse import parse_qsl

import httpx
import pytest
from plivo.utils import validate_signature, validate_v3_signature

from tests.harness import EventStream, PlivoSim, WebhookSink

SSE = (
    "retry: 3000\n\n: ping\n\n"
    'id: 1\ndata: {"id": "1", "type": "call.started", "data": {"from": "+14155550111"}}\n\n'
    'id: 2\ndata: {"id": "2",\ndata: "type": "call.ended", "data": {}}\n\n'
)


@dataclass
class Hit:
    path: str
    url: str
    params: dict
    headers: httpx.Headers


class FakeHS:
    """Plays HS: answers each path with a scripted XML reply, or 500 while `fail_next` counts down (GET /stream
    streams SSE), and records every request. The first request to a path in `meet` has its response withheld for up
    to 2 s; a second request arriving meanwhile puts the path in `met`, proving two were sent together."""

    def __init__(self) -> None:
        self.replies: dict[str, str] = {}
        self.fail_next: dict[str, int] = {}
        self.hits: list[Hit] = []
        self.closed = threading.Event()
        self.meet: set[str] = set()
        self.held: set[str] = set()
        self.met: set[str] = set()
        self.changed = threading.Condition()
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def _hit(self, params: dict) -> str:
                path = self.path.partition("?")[0]
                fake.hits.append(Hit(path, fake.url + self.path, params, httpx.Headers(self.headers.items())))
                return path

            def _answer(self, path: str) -> None:
                with fake.changed:
                    if path in fake.held:
                        fake.met.add(path)
                        fake.changed.notify_all()
                    elif path in fake.meet:
                        fake.meet.discard(path)
                        fake.held.add(path)
                        fake.changed.wait_for(lambda: path in fake.met, 2)
                        fake.held.discard(path)
                failing = fake.fail_next.get(path, 0)
                fake.fail_next[path] = max(failing - 1, 0)
                body = b"" if failing else fake.replies.get(path, "").encode()
                self.send_response(500 if failing else 200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self) -> None:
                raw = self.rfile.read(int(self.headers.get("Content-Length") or 0)).decode()
                self._answer(self._hit(dict(parse_qsl(raw))))

            def do_GET(self) -> None:
                if (path := self._hit(dict(parse_qsl(self.path.partition("?")[2])))) != "/stream":
                    self._answer(path)
                    return
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.end_headers()
                self.wfile.write(SSE.encode())
                fake.closed.wait(5)

            def log_message(self, *args) -> None:
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        threading.Thread(target=self.server.serve_forever, args=(0.05,), daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def wait(self, path: str, **match: str) -> Hit:
        return _until(
            lambda: next((h for h in self.hits if h.path == path and match.items() <= h.params.items()), None)
        )

    def trail(self) -> list[tuple[str, str]]:
        return [(h.path, h.params.get("EventName", "")) for h in self.hits]


def _until(find, timeout: float = 5.0):
    deadline = time.monotonic() + timeout
    while not (found := find()):
        assert time.monotonic() < deadline, "condition not met"
        time.sleep(0.01)
    return found


@pytest.fixture(scope="module")
def sim():
    s = PlivoSim()
    yield s
    s.close()


@pytest.fixture
def fake(sim):
    f = FakeHS()
    yield f
    f.closed.set()
    f.server.shutdown()
    f.server.server_close()
    sim.time_factor, sim.short_takes, sim.member_address = 0.1, False, "dialled"
    sim.reset_faults()
    assert sim.errors == []


@pytest.fixture
def line(sim, fake):
    return _line(sim, fake)


def _line(sim, fake):
    """A sub-account whose number is routed to the fake HS's application."""
    acc = sim.subaccount("Test")
    api = httpx.Client(base_url=f"{sim.url}/v1/Account/{acc.auth_id}/", auth=(acc.auth_id, acc.auth_token))
    app = {"app_name": "hs", "answer_url": f"{fake.url}/answer?tenant=t1", "hangup_url": f"{fake.url}/hangup"}
    app["message_url"] = f"{fake.url}/message?tenant=t1"
    app_id = api.post("Application/", json=app).json()["app_id"]
    number = sim.buy_number(acc.auth_id)
    assert api.post(f"Number/{number[1:]}/", json={"app_id": app_id}).status_code == 202
    return SimpleNamespace(acc=acc, api=api, number=number, app_id=app_id)


def _phone(sim, line, behaviour="answer", after=0.0):
    ep = line.api.post(
        "Endpoint/", json={"username": "agent", "password": "s3cret!", "alias": "Agent", "app_id": line.app_id}
    )
    phone = sim.phone(ep.json()["username"])
    phone.on_ring(behaviour, after)
    return phone


def _sip(phone) -> str:
    return f"sip:{phone.endpoint_username}@phone.plivo.com"


def test_callbacks_carry_v3_signatures_the_sdk_accepts(sim, fake, line):
    fake.replies["/answer"] = "<Response><Hangup/></Response>"
    sim.call(line.number).wait_state("completed")
    hit = fake.wait("/answer")
    nonce, args = hit.headers["X-Plivo-Signature-V3-Nonce"], ("POST", hit.url)
    assert hit.params["From"] == "14155550111" and hit.params["To"] == line.number[1:]
    assert validate_v3_signature(*args, nonce, line.acc.auth_token, hit.headers["X-Plivo-Signature-V3"], hit.params)
    assert validate_v3_signature(*args, nonce, sim.main.auth_token, hit.headers["X-Plivo-Signature-Ma-V3"], hit.params)
    assert not validate_v3_signature(*args, nonce, sim.main.auth_token, hit.headers["X-Plivo-Signature-V3"], hit.params)
    forged = sim.sign(hit.url, {"CallUUID": "c1"}, line.acc.auth_token)
    forged_nonce = forged["X-Plivo-Signature-V3-Nonce"]
    assert validate_v3_signature(
        *args, forged_nonce, line.acc.auth_token, forged["X-Plivo-Signature-V3"], {"CallUUID": "c1"}
    )
    assert httpx.get(f"{sim.url}/v1/Account/{line.acc.auth_id}/", auth=(line.acc.auth_id, "wrong")).status_code == 401


def test_get_digits_posts_digits_and_falls_through_on_no_input(sim, fake, line):
    menu = f'<GetDigits action="{fake.url}/menu" numDigits="1"><Speak>Press 1</Speak></GetDigits><Speak>Bye</Speak>'
    one = '<?xml version="1.0" encoding="UTF-8"?><Response><Speak voice="Polly.Joanna">O<break/>ne</Speak></Response>'
    fake.replies.update({"/answer": f"<Response>{menu}</Response>", "/menu": one})
    pressed = sim.call(line.number, digits="1").wait_state("completed")
    assert pressed.said == ["Press 1", "One"] and pressed.hangup_cause_code == 4010
    assert fake.wait("/menu").params["Digits"] == "1"
    silent = sim.call(line.number, digits="w").wait_state("completed")
    assert silent.said == ["Press 1", "Bye"]
    assert fake.wait("/hangup", CallUUID=silent.call_uuid).params["HangupCauseCode"] == "4010"


def test_get_digits_waits_for_press_hangup_or_its_timeout(sim, fake, line):
    menu = (
        f'<GetDigits action="{fake.url}/menu" numDigits="1" timeout="{{}}" retries="2"><Speak>Menu</Speak></GetDigits>'
    )
    fake.replies.update({"/answer": f"<Response>{menu.format(30)}<Speak>Bye</Speak></Response>"})
    pressing = sim.call(line.number)
    _until(lambda: pressing.said == ["Menu"])
    pressing.press("3")
    assert fake.wait("/menu", CallUUID=pressing.call_uuid).params["Digits"] == "3"
    leaving = sim.call(line.number)
    _until(lambda: leaving.said == ["Menu"])
    leaving.hangup()
    assert leaving.wait_state("completed").hangup_cause_code == 4000
    fake.replies["/answer"] = f"<Response>{menu.format(0.2)}<Speak>Bye</Speak></Response>"
    silent = sim.call(line.number).wait_state("completed")
    assert silent.said == ["Menu", "Menu", "Bye"]
    assert [h.params["CallUUID"] for h in fake.hits if h.path == "/menu"] == [pressing.call_uuid]


def test_customer_app_rejections_token_rotation_and_lead_phones(sim, fake, line):
    owner = sim.app("<Response><Speak>Owner first</Speak></Response>")
    fake.replies["/answer"] = f"<Response><Redirect>{owner}?conversation_id=c1</Redirect></Response>"
    assert sim.call(line.number).wait_state("completed").said == ["Owner first"]
    fetch = sim.requests_to("/apps/")[-1].params
    assert (fetch["conversation_id"], fetch["To"]) == ("c1", line.number[1:])
    sim.reject_next("/Call/", 429, account=line.acc.auth_id)
    dial = {"from": "+14155550111", "to": "+14155550142", "answer_url": f"{fake.url}/out"}
    assert line.api.post("Call/", json=dial).status_code == 429
    sim.reject_next("/Call/", 403, "IP not in the whitelist", account=line.acc.auth_id)
    denied = line.api.post("Call/", json=dial)
    assert (denied.status_code, denied.json()["error"]) == (403, "IP not in the whitelist")
    lead = sim.lead("+14155550142")
    assert line.api.post("Call/", json=dial).status_code == 201
    assert lead.wait_ring().from_ == "+14155550111"
    old = line.acc.auth_token
    new = sim.rotate_token(line.acc.auth_id)
    account = f"{sim.url}/v1/Account/{line.acc.auth_id}/"
    assert httpx.get(account, auth=(line.acc.auth_id, old)).status_code == 401
    assert httpx.get(account, auth=(line.acc.auth_id, new)).status_code == 200


def _queue_xml(fake, wait_time=30) -> str:
    events = "mpc-state-changes,participant-state-changes,add-participant-api-events,participant-digit-input-events"
    attrs = f'role="customer" waitTime="{wait_time}" statusCallbackUrl="{fake.url}/mpc" statusCallbackEvents="{events}"'
    attrs += f' startMpcOnEnter="false" endMpcOnExit="true" onExitActionUrl="{fake.url}/exit"'
    return f"<Response><MultiPartyCall {attrs}>mpc_q1</MultiPartyCall></Response>"


def test_mpc_callbacks_follow_plivo_order(sim, fake, line):
    fake.replies["/answer"] = _queue_xml(fake)
    phone = _phone(sim, line)
    customer = sim.call(line.number)
    fake.wait("/mpc", EventName="ParticipantJoin")
    body = {"role": "Agent", "from": "+14155550111", "to": _sip(phone), "call_status_callback_url": f"{fake.url}/leg"}
    added = line.api.post(
        "MultiPartyCall/name_mpc_q1/Participant/", json={**body, "sip_headers": "HsKind=offer,HsOffer=o1"}
    )
    leg = phone.wait_ring().wait_state("in_progress")
    assert leg.call_uuid == added.json()["calls"][0]["call_uuid"]
    assert leg.headers == {"X-PH-HsKind": "offer", "X-PH-HsOffer": "o1"}
    joined = fake.wait("/mpc", EventName="ParticipantJoin", ParticipantRole="Agent")
    assert joined.params["MemberAddress"] == _sip(phone) and joined.params["X-PH-HsOffer"] == "o1"
    fake.wait("/mpc", EventName="AddParticipantByAPIActionCompleted", AddParticipantAPIRequestOutcome="successful")
    customer.hangup()
    fake.wait("/mpc", EventName="MPCEnd")
    assert fake.trail() == [
        ("/answer", ""),
        ("/mpc", "MPCInitialized"),
        ("/mpc", "ParticipantJoin"),
        ("/mpc", "AddParticipantByAPIActionInitiated"),
        ("/leg", "Initiated"),
        ("/leg", "Ringing"),
        ("/leg", "Answered"),
        ("/mpc", "ParticipantJoin"),
        ("/mpc", "MPCStart"),
        ("/mpc", "AddParticipantByAPIActionCompleted"),
        ("/hangup", ""),
        ("/leg", "Hangup"),
        ("/mpc", "ParticipantExit"),
        ("/mpc", "ParticipantExit"),
        ("/mpc", "MPCEnd"),
    ]
    seqs = [int(h.params["SequenceNumber"]) for h in fake.hits if h.path == "/mpc"]
    assert seqs == sorted(seqs)
    assert (
        fake.wait("/mpc", EventName="ParticipantExit", ParticipantRole="customer").params["IsMPCTerminationSource"]
        == "true"
    )
    assert fake.wait("/leg", EventName="Hangup").params["PlivoHangupCauseCode"] == "4020"
    assert leg.hangup_cause_code == 4020 and customer.hangup_cause_code == 4000


def test_wait_timeout_exits_the_member_to_its_exit_action(sim, fake, line):
    fake.replies.update(
        {"/answer": _queue_xml(fake, wait_time=0.3), "/exit": "<Response><Speak>Sorry</Speak><Hangup/></Response>"}
    )
    customer = sim.call(line.number).wait_state("completed", timeout=4)
    exit_action = fake.wait("/exit").params
    assert (
        exit_action["ParticipantExitCause"] == "Member Wait Timeout Triggered"
        and exit_action["MPCFriendlyName"] == "mpc_q1"
    )
    assert customer.said == ["Sorry"]
    fake.wait("/mpc", EventName="MPCEnd")
    trail = fake.trail()
    order = [("/exit", ""), ("/hangup", ""), ("/mpc", "ParticipantExit"), ("/mpc", "MPCEnd")]
    assert [trail.index(step) for step in order] == sorted(trail.index(step) for step in order)


def test_a_member_waits_out_its_wait_time_on_demand(sim, fake, line):
    fake.replies.update({"/answer": _queue_xml(fake, wait_time=600), "/exit": "<Response><Hangup/></Response>"})
    customer = sim.call(line.number)
    customer.wait_out()
    customer.wait_state("completed")
    assert fake.wait("/exit").params["ParticipantExitCause"] == "Member Wait Timeout Triggered"


def test_dtmf_in_the_mpc_and_a_transfer_out_of_it(sim, fake, line):
    fake.replies.update({"/answer": _queue_xml(fake), "/away": "<Response><Speak>Transferred</Speak></Response>"})
    customer = sim.call(line.number)
    fake.wait("/mpc", EventName="ParticipantJoin")
    customer.press("2")
    assert fake.wait("/mpc", EventName="ParticipantDigitInput").params["DigitInput"] == "2"
    assert (
        line.api.post(f"Call/{customer.call_uuid}/", json={"legs": "aleg", "aleg_url": f"{fake.url}/away"}).status_code
        == 202
    )
    customer.wait_state("completed")
    assert fake.wait("/mpc", EventName="ParticipantExit").params["ParticipantExitCause"] == "sim:transferred"
    assert customer.said == ["Transferred"] and not [h for h in fake.hits if h.path == "/exit"]


def test_a_wait_holds_the_call_then_goes_on_unless_a_transfer_takes_it(sim, fake, line):
    fake.replies.update({"/answer": '<Response><Wait length="5"/><Speak>After</Speak></Response>'})
    fake.replies["/away"] = "<Response><Speak>Transferred</Speak></Response>"
    waited = sim.call(line.number)
    waited.wait_state("completed")
    fake.replies["/answer"] = '<Response><Wait length="50"/><Speak>Never</Speak></Response>'
    taken = sim.call(line.number).wait_state("in_progress")
    moved = line.api.post(f"Call/{taken.call_uuid}/", json={"legs": "aleg", "aleg_url": f"{fake.url}/away"})
    taken.wait_state("completed")
    assert moved.status_code == 202 and waited.said == ["After"] and taken.said == ["Transferred"]


def _agent_in_queue(sim, fake, line, phone, **extra):
    """A caller waiting in mpc_q1 and an agent (phone) the Add Participant API dialled into it."""
    customer = sim.call(line.number)
    fake.wait("/mpc", EventName="ParticipantJoin")
    body = {"role": "Agent", "from": "+14155550111", "to": _sip(phone), "call_status_callback_url": f"{fake.url}/leg"}
    assert line.api.post("MultiPartyCall/name_mpc_q1/Participant/", json={**body, **extra}).status_code == 201
    return customer


def test_stay_alone_off_exits_the_last_member_to_its_exit_action(sim, fake, line):
    fake.replies.update({"/answer": _queue_xml(fake), "/exit": "<Response><Hangup/></Response>"})
    phone = _phone(sim, line)
    customer = _agent_in_queue(sim, fake, line, phone)
    phone.wait_ring().wait_state("in_progress").hangup()
    customer.wait_state("completed")
    assert fake.wait("/exit").params["ParticipantExitCause"] == "sim:alone"
    end = fake.wait("/mpc", EventName="MPCEnd").params
    assert end["MPCTerminationCause"] == "sim:alone"
    mpc = line.api.get("MultiPartyCall/name_mpc_q1/").json()
    assert (mpc["status"], mpc["termination_cause"], bool(mpc["end_time"])) == ("Ended", "sim:alone", True)
    trail = fake.trail()
    assert trail.index(("/exit", "")) < trail.index(("/hangup", "")) < trail.index(("/mpc", "MPCEnd"))


def test_a_leg_answering_after_mpc_end_starts_a_fresh_mpc(sim, fake, line):
    fake.replies["/answer"] = _queue_xml(fake)
    sim.member_address = "bare"
    phone = _phone(sim, line, "answer", after=0.3)
    customer = _agent_in_queue(sim, fake, line, phone, status_callback_url=f"{fake.url}/mpc")
    customer.hangup()
    first = fake.wait("/mpc", EventName="MPCEnd").params["MPCUUID"]
    leg = phone.wait_ring()
    assert leg.state == "ringing"
    leg.wait_state("in_progress")
    joined = fake.wait("/mpc", EventName="ParticipantJoin", ParticipantRole="Agent").params
    assert joined["MPCUUID"] != first and joined["MemberAddress"] == _sip(phone).removeprefix("sip:")
    assert line.api.delete(f"Call/{leg.call_uuid}/").status_code == 204


def test_add_participant_validates_like_plivo_and_moves_a_live_leg(sim, fake, line):
    fake.replies["/answer"] = _queue_xml(fake)
    sim.call(line.number)
    fake.wait("/mpc", EventName="ParticipantJoin")
    url, body = (
        "MultiPartyCall/name_mpc_q1/Participant/",
        {"role": "Agent", "from": "+14155550111", "to": "+14155550177"},
    )
    for bad in ({"from": "sip:a@phone.plivo.com"}, {"caller_name": "x" * 51}, {"sip_headers": "HsConversation=a-b"}):
        assert line.api.post(url, json={**body, **bad}).status_code == 400
    assert line.api.post(url, json={**body, "sip_headers": "HsConversation=a%2Db"}).status_code == 201
    menu = f'<Response><GetDigits action="{fake.url}/menu" timeout="30"><Speak>Wait</Speak></GetDigits></Response>'
    fake.replies["/answer"] = menu
    waiting = sim.call(line.number, from_="+14155550188")
    _until(lambda: waiting.said == ["Wait"])
    assert line.api.post(url, json={"role": "Customer", "call_uuid": waiting.call_uuid}).status_code == 201
    moved = fake.wait("/mpc", EventName="ParticipantJoin", ParticipantCallUUID=waiting.call_uuid).params
    assert moved["MemberAddress"] == "14155550188" and waiting.mpc == "mpc_q1"


def test_recordings_participant_tracks_and_transcriptions(sim, fake, line):
    rec = f'record="true" recordParticipantTrack="true" recordingCallbackUrl="{fake.url}/rec" transcript="true"'
    queue = _queue_xml(fake).replace('role="customer"', f'role="customer" {rec} transcriptionUrl="{fake.url}/text"')
    fake.replies["/answer"] = queue
    phone = _phone(sim, line)
    tracks = {"record_participant_track": True, "recording_callback_url": f"{fake.url}/rec", "transcript": True}
    customer = _agent_in_queue(sim, fake, line, phone, transcription_url=f"{fake.url}/text", **tracks)
    phone.wait_ring().wait_state("in_progress")
    fake.wait("/mpc", EventName="MPCStart")
    assert line.api.post("MultiPartyCall/name_mpc_q1/Record/Pause/").status_code == 400
    assert line.api.post("MultiPartyCall/name_mpc_q1/Record/Pause/", json={}).status_code == 204
    assert line.api.post("MultiPartyCall/name_mpc_q1/Record/Resume/", json={}).status_code == 204
    customer.hangup()
    mpc_rec = fake.wait("/rec", EventName="MPCRecordingCompleted").params
    _until(lambda: len([h for h in fake.hits if h.path == "/text"]) == 3)
    events = [h.params["EventName"] for h in fake.hits if h.path == "/rec"]
    assert events.count("ParticipantRecordingCompleted") == 2 and events[:2] == [
        "MPCRecordingPaused",
        "MPCRecordingResumed",
    ]
    track = fake.wait("/rec", EventName="ParticipantRecordingCompleted").params
    assert track["RecordTrackType"] == "participant" and track["ParticipantCallUUID"] in (customer.call_uuid,)
    text = fake.wait("/text", recording_id=mpc_rec["RecordingUUID"]).params
    assert json.loads(text["transcription"])[0]["text"] == sim.transcript and text["call_uuid"]
    got = line.api.get(f"Transcription/{mpc_rec['RecordingUUID']}", params={"type": "raw"}).json()
    assert {"api_id", "cost", "rate", "recording_duration_ms", "recording_start_ms", "status"} <= got.keys()
    assert got["transcription"][0]["text"] == sim.transcript


def test_sdk_dial_out_carries_extra_headers_and_the_answer_request_rings(sim, fake, line):
    fake.replies["/answer"] = "<Response><Hangup/></Response>"
    phone = _phone(sim, line)
    phone.dial("+14155550199", headers={"X-PH-Lead": "42"}).wait_state("completed")
    answer = fake.wait("/answer").params
    assert (answer["X-PH-Lead"], answer["CallStatus"], answer["From"]) == ("42", "ringing", _sip(phone))


@pytest.mark.parametrize(
    "behaviour, code, status",
    [("decline", 3020, "failed"), ("timeout", 6010, "timeout"), ("unregistered", 2020, "failed")],
)
def test_phone_failures_end_the_leg_with_plivo_causes(sim, fake, line, behaviour, code, status):
    phone = _phone(sim, line, "answer" if behaviour == "unregistered" else behaviour)
    if behaviour == "unregistered":
        phone.unregister()
    dial = {
        "from": "+14155550111",
        "to": _sip(phone),
        "answer_url": f"{fake.url}/out",
        "hangup_url": f"{fake.url}/hangup",
    }
    assert line.api.post("Call/", json=dial).status_code == 201
    hangup = fake.wait("/hangup").params
    assert (hangup["HangupCauseCode"], hangup["CallStatus"]) == (str(code), status)
    assert not [h for h in fake.hits if h.path == "/out"]


def test_a_browser_sdk_login_rings_at_sub_iss_and_a_sip_device_at_its_endpoint(sim, fake, line):
    fake.replies["/out"] = "<Response><Hangup/></Response>"
    browser, desk = _phone(sim, line), _phone(sim, line)
    browser.unregister()
    now, voice = int(time.time()), {"incoming_allow": True, "outgoing_allow": False}
    claims = {"iss": line.acc.auth_id, "sub": browser.endpoint_username, "nbf": now, "exp": now + 60}
    claims |= {"per": {"voice": voice}, "app": line.app_id}
    assert line.api.post("JWT/Token/", json=claims | {"sub": "nobody"}).status_code == 400
    assert line.api.post("JWT/Token/", json=claims | {"app": "1"}).status_code == 400
    assert line.api.post("JWT/Token/", json={k: v for k, v in claims.items() if k != "app"}).status_code == 200
    token = line.api.post("JWT/Token/", json=claims).json()["token"]
    assert not browser.login("a.self.signed") and not desk.login(token) and browser.login(token)
    sdk = f"sip:{browser.endpoint_username}_{line.acc.auth_id}@phone.plivo.com"
    for to in (sdk, _sip(browser), _sip(desk)):
        dial = {"from": "+14155550111", "to": to, "answer_url": f"{fake.url}/out", "hangup_url": f"{fake.url}/hangup"}
        assert line.api.post("Call/", json=dial).status_code == 201
    assert browser.wait_ring().to == sdk and desk.wait_ring().to == _sip(desk)
    fake.wait("/hangup", To=_sip(browser), HangupCauseCode="2020")
    assert len(browser.legs) == len(desk.legs) == 1


def test_an_endpoint_alias_takes_only_the_characters_plivo_allows(line):
    endpoint = {"username": "agent", "password": "s3cret!", "app_id": line.app_id}
    assert line.api.post("Endpoint/", json=endpoint | {"alias": "Agent One"}).status_code == 400
    assert line.api.post("Endpoint/", json=endpoint | {"alias": "agent.one+1@a_b-c"}).status_code == 201


def test_a_ringing_leg_answers_on_demand_and_takes_quality_feedback_once_ended(sim, fake, line):
    fake.replies["/out"] = "<Response><Hangup/></Response>"
    ignoring = _phone(sim, line, "ignore")
    dial = {"from": "+14155550111", "to": _sip(ignoring), "answer_url": f"{fake.url}/out"}
    assert line.api.post("Call/", json=dial).status_code == 201
    leg = ignoring.wait_ring()
    leg.answer()
    leg.wait_state("in_progress").wait_state("completed")
    feedback = f"Call/{leg.call_uuid}/Feedback/"
    assert line.api.post(feedback, json={"rating": 2, "issues": ["ECHO"]}).status_code == 201
    assert sim.feedback == [{"call_uuid": leg.call_uuid, "rating": 2, "issues": ["ECHO"]}]


def test_a_message_outcome_lasts_until_the_test_ends(sim, fake, line):
    sim.message_outcome("+14155550143", "undelivered", "80")
    sim.reset_faults()
    uid = _send(line, fake, dst="+14155550143", text="Hello").json()["message_uuid"][0]
    assert _statuses(fake, uid, "delivered")[-1] == ("delivered", "000")


def test_ignored_ring_waits_for_cancel_and_after_delays_the_answer(sim, fake, line):
    ignoring = _phone(sim, line, "ignore")
    dial = {
        "from": "+14155550111",
        "to": _sip(ignoring),
        "answer_url": f"{fake.url}/out",
        "hangup_url": f"{fake.url}/hangup",
    }
    request_uuid = line.api.post("Call/", json=dial).json()["request_uuid"]
    leg = ignoring.wait_ring()
    time.sleep(0.1)
    assert leg.state == "ringing"
    assert line.api.delete(f"Request/{request_uuid}/").status_code == 204
    assert fake.wait("/hangup").params["HangupCauseCode"] == "1000"
    slow = _phone(sim, line, "answer", after=0.3)
    started = time.monotonic()
    line.api.post("Call/", json={**dial, "to": _sip(slow)})
    slow.wait_ring().wait_state("in_progress")
    assert time.monotonic() - started >= 0.3
    sim.time_factor = 0.01
    unanswered = {**dial, "ring_timeout": 20, "hangup_url": ""}
    line.api.post("Call/", json=unanswered)
    timed_out = ignoring.wait_ring().wait_state("completed")
    assert timed_out.hangup_cause_code == 6010
    hangup = fake.wait("/out", Event="Hangup").params
    assert (hangup["HangupCauseCode"], hangup["HangupCause"]) == ("6010", "NO_ANSWER")


def test_make_call_runs_the_answer_url_and_the_first_answer_wins(sim, fake, line):
    first, second = _phone(sim, line), _phone(sim, line)
    fake.replies["/out"] = "<Response><Speak>Hi</Speak></Response>"
    dial = {"from": "+14155550111", "to": f"{_sip(first)}<{_sip(second)}", "answer_url": f"{fake.url}/out?dial=r1"}
    made = line.api.post("Call/", json={**dial, "hangup_url": f"{fake.url}/hangup", "sip_headers": "HsKind=outbound"})
    winner, loser = first.wait_ring().wait_state("completed"), second.wait_ring().wait_state("completed")
    answer = fake.wait("/out").params
    assert answer["Direction"] == "outbound" and answer["RequestUUID"] == made.json()["request_uuid"]
    assert answer["CallUUID"] == winner.call_uuid and answer["To"] == _sip(first)
    assert winner.headers == {"X-PH-HsKind": "outbound"} and winner.said == ["Hi"]
    assert (winner.hangup_cause_code, loser.hangup_cause_code) == (4010, 9000)


def test_dial_reports_the_b_leg_outcome_to_its_action(sim, fake, line):
    forward = f'<Dial action="{fake.url}/dialled"><Number>+14155550199</Number></Dial><Speak>Unreached</Speak>'
    fake.replies.update(
        {"/answer": f"<Response>{forward}</Response>", "/dialled": "<Response><Speak>Done</Speak></Response>"}
    )
    sim.lead("+14155550199", "busy")
    busy = sim.call(line.number).wait_state("completed")
    assert fake.wait("/dialled").params["DialStatus"] == "busy" and busy.said == ["Done"]
    sim.lead("+14155550199", "answer")
    caller = sim.call(line.number)
    lead = _until(lambda: next((c for c in sim.calls if c.to == "+14155550199" and c.state == "in_progress"), None))
    assert caller.state == "in_progress" and lead.from_ == "+14155550111"
    lead.hangup()
    caller.wait_state("completed")
    assert fake.wait("/dialled", DialStatus="completed").params["DialBLegUUID"] == lead.call_uuid


def test_record_posts_the_recording_and_serves_it_with_ranges(sim, fake, line):
    urls = f'action="{fake.url}/vm" callbackUrl="{fake.url}/vm-ready" transcriptionUrl="{fake.url}/vm-text"'
    record = f'<Record {urls} transcriptionType="auto"/>'
    fake.replies.update({"/answer": f"<Response>{record}</Response>", "/vm": "<Response><Hangup/></Response>"})
    sim.call(line.number).wait_state("completed")
    fake.wait("/hangup")
    assert [h.path for h in fake.hits] == ["/answer", "/vm", "/vm-ready", "/vm-text", "/hangup"]
    rec = fake.wait("/vm").params
    assert rec["RecordingDuration"] == "1" and fake.wait("/vm-ready").params["RecordingID"] == rec["RecordingID"]
    assert rec["RecordUrl"].startswith(sim.media_url)
    assert fake.wait("/vm-text").params["transcription"] == sim.transcript
    creds = (line.acc.auth_id, line.acc.auth_token)
    full = httpx.get(rec["RecordUrl"], auth=creds)
    assert full.status_code == 200 and full.content == sim.audio(rec["RecordingID"])
    part = httpx.get(rec["RecordUrl"], headers={"Range": "bytes=10-19"})
    assert (part.status_code, part.content, part.headers["Content-Range"]) == (
        206,
        full.content[10:20],
        "bytes 10-19/16384",
    )
    assert httpx.get(rec["RecordUrl"], headers={"Range": "bytes=-4"}).content == full.content[-4:]
    assert httpx.get(rec["RecordUrl"], headers={"Range": "bytes=99999-"}).status_code == 416
    assert httpx.get(rec["RecordUrl"], auth=(line.acc.auth_id, "wrong")).status_code == 401
    assert httpx.get(rec["RecordUrl"].replace(sim.media_url, sim.url), auth=creds).status_code == 404
    assert line.api.delete(f"Recording/{rec['RecordingID']}/").status_code == 204
    assert httpx.get(rec["RecordUrl"]).status_code == 404


def test_webhook_sink_records_deliveries_and_replies_with_the_set_status():
    sink = WebhookSink()
    try:
        assert httpx.post(sink.url, json={"type": "call.ended", "data": {"result": "answered"}}).status_code == 200
        sink.respond_with(503)
        sent = httpx.post(
            sink.url, json={"type": "call.ended", "data": {"result": "missed"}}, headers={"X-HS-Signature": "v1=ab"}
        )
        assert sent.status_code == 503
        delivery = sink.wait_for("call.ended", result="missed")
        assert delivery.headers["X-HS-Signature"] == "v1=ab" and len(sink.deliveries) == 2
        with pytest.raises(AssertionError):
            sink.wait_for("call.started", timeout=0.1)
        sink.redirect_to("https://elsewhere.example/hook")
        moved = httpx.post(sink.url, json={"type": "call.started"})
        assert (moved.status_code, moved.headers["Location"]) == (302, "https://elsewhere.example/hook")
        assert sink.wait_for("call.started").json == {"type": "call.started"}
    finally:
        sink.close()


def test_event_stream_parses_sse_frames(fake):
    stream = EventStream(f"{fake.url}/stream", "tok", after=7)
    try:
        assert stream.status == 200
        assert stream.wait_for("call.started", **{"from": "+14155550111"})["id"] == "1"
        assert stream.wait_for("call.ended")["id"] == "2"
        assert [e["type"] for e in stream.events] == ["call.started", "call.ended"]
        assert fake.hits[0].url.endswith("/stream?after=7") and fake.hits[0].headers["Authorization"] == "Bearer tok"
        fake.closed.set()
        stream.wait_closed()
    finally:
        stream.close()
    by_query = EventStream(f"{fake.url}/stream", "tok", query=True)
    by_query.close()
    assert fake.hits[-1].url.endswith("/stream?access_token=tok") and "Authorization" not in fake.hits[-1].headers


def _signed_v2(hit, auth_token: str, header: str = "X-Plivo-Signature-V2") -> bool:
    return validate_signature(hit.url, hit.headers["X-Plivo-Signature-V2-Nonce"], hit.headers[header], auth_token)


def test_inbound_sms_and_mms_reach_the_message_url_v2_signed(sim, fake, line):
    sms = sim.text(line.number, "+14155550199", "Where is my order?")
    hit = fake.wait("/message", MessageUUID=sms.message_uuid)
    assert hit.params == {
        "From": "14155550199",
        "To": line.number[1:],
        "Type": "sms",
        "Text": "Where is my order?",
        "MessageUUID": sms.message_uuid,
    }
    assert _signed_v2(hit, line.acc.auth_token) and _signed_v2(hit, sim.main.auth_token, "X-Plivo-Signature-Ma-V2")
    assert not _signed_v2(hit, sim.main.auth_token) and "X-Plivo-Signature-V3" not in hit.headers
    mms = sim.text(
        line.number, "+14155550199", "Photo", type_="mms", media=("https://m.test/a.jpg", "https://m.test/b.png")
    )
    params = fake.wait("/message", MessageUUID=mms.message_uuid).params
    assert (params["Type"], params["Body"], params["MediaCount"]) == ("mms", "Photo", "2")
    assert (params["Media0"], params["Media1"]) == ("https://m.test/a.jpg", "https://m.test/b.png")


def test_inbound_media_bytes_become_a_url_only_the_owning_account_can_fetch(sim, fake, line):
    msg = sim.text(line.number, "+14155550199", "", type_="mms", media=(b"one", "https://m.test/b.png", b"two"))
    params = fake.wait("/message", MessageUUID=msg.message_uuid).params
    prefix = f"{sim.media_url}/v1/Account/{line.acc.auth_id}/Message/{msg.message_uuid}/Media/"
    assert (params["Media0"], params["Media1"], params["Media2"]) == (
        prefix + "0",
        "https://m.test/b.png",
        prefix + "2",
    )
    creds = (line.acc.auth_id, line.acc.auth_token)
    got = httpx.get(params["Media0"], auth=creds)
    assert (got.status_code, got.content, "image" in got.headers["content-type"]) == (200, b"one", True)
    assert httpx.get(params["Media2"], auth=creds).content == b"two"
    assert httpx.get(params["Media0"]).status_code == 401
    assert httpx.get(params["Media0"], auth=(line.acc.auth_id, "wrong")).status_code == 401
    other = sim.subaccount("Other")
    assert httpx.get(params["Media0"], auth=(other.auth_id, other.auth_token)).status_code == 401
    assert httpx.get(prefix + "9", auth=creds).status_code == 404
    assert httpx.get(params["Media0"], auth=(sim.main.auth_id, sim.main.auth_token)).content == b"one"


def test_inbound_whatsapp_media_carries_its_mime_type_and_is_fetchable(sim, fake, line):
    msg = sim.text(line.number, "+14155550199", "", type_="whatsapp", media=(b"pic",))
    params = fake.wait("/message", MessageUUID=msg.message_uuid).params
    assert params["ContentType"] == "media"
    got = httpx.get(params["Media0"], auth=(line.acc.auth_id, line.acc.auth_token))
    assert (got.content, got.headers["content-type"]) == (b"pic", params["MimeType"])


def _send(line, fake, **body):
    return line.api.post("Message/", json={"src": line.number, "url": f"{fake.url}/status", **body})


def _statuses(fake, message_uuid: str, final: str) -> list[tuple[str, str]]:
    fake.wait("/status", MessageUUID=message_uuid, Status=final)
    return [
        (h.params["Status"], h.params["ErrorCode"]) for h in fake.hits if h.params.get("MessageUUID") == message_uuid
    ]


def test_outbound_messages_post_each_status_and_read_back(sim, fake, line):
    sent = _send(line, fake, dst="+14155550142", text="Your order shipped")
    assert sent.status_code == 202
    uid = sent.json()["message_uuid"][0]
    assert _statuses(fake, uid, "delivered") == [("queued", "000"), ("sent", "000"), ("delivered", "000")]
    hit = fake.wait("/status", MessageUUID=uid, Status="delivered")
    assert hit.params["From"] == line.number[1:] and _signed_v2(hit, line.acc.auth_token)
    found = line.api.get(f"Message/{uid}/").json()
    assert (found["message_state"], found["message_type"], found["to_number"]) == ("delivered", "sms", "14155550142")
    other = sim.subaccount("Other")
    assert (
        httpx.get(
            f"{sim.url}/v1/Account/{other.auth_id}/Message/{uid}/", auth=(other.auth_id, other.auth_token)
        ).status_code
        == 404
    )
    sim.message_outcome("+14155550143", "undelivered", "80")
    gone = _send(line, fake, dst="+14155550143", text="Hello").json()["message_uuid"][0]
    assert _statuses(fake, gone, "undelivered")[-2:] == [("sent", "000"), ("undelivered", "80")]
    foreign = _send(line, fake, dst="+14155550142", text="Hi", src="+14155550000").json()["message_uuid"][0]
    assert _statuses(fake, foreign, "failed") == [("queued", "000"), ("failed", "40")]
    assert _send(line, fake, dst="+14155550142").status_code == 400
    sim.reject_next("/Message/", 429, account=line.acc.auth_id)
    assert _send(line, fake, dst="+14155550142", text="Hi").status_code == 429


def test_whatsapp_free_text_needs_a_customer_reply_in_the_last_24_hours(sim, fake, line):
    customer, template = "+14155550177", {"name": "order_update", "language": "en_US"}
    cold = _send(line, fake, dst=customer, text="Hi", type="whatsapp").json()["message_uuid"][0]
    assert _statuses(fake, cold, "failed") == [("queued", "000"), ("failed", "380")]
    unknown = _send(line, fake, dst=customer, type="whatsapp", template=template).json()["message_uuid"][0]
    assert _statuses(fake, unknown, "failed")[-1] == ("failed", "340")
    sim.templates.add(("order_update", "en_US"))
    opener = _send(line, fake, dst=customer, type="whatsapp", template=template).json()["message_uuid"][0]
    assert _statuses(fake, opener, "delivered")[-1] == ("delivered", "000")
    reply = sim.text(line.number, customer, "Thanks!", type_="whatsapp")
    params = fake.wait("/message", MessageUUID=reply.message_uuid).params
    assert (params["Type"], params["Body"], params["ContentType"]) == ("whatsapp", "Thanks!", "text")
    warm = _send(line, fake, dst=customer, text="Glad to help", type="whatsapp").json()["message_uuid"][0]
    assert _statuses(fake, warm, "delivered")[-1] == ("delivered", "000")
    assert _send(line, fake, dst=customer, template=template).status_code == 400
    reply.created -= 24 * 3600
    stale = _send(line, fake, dst=customer, text="Anything else?", type="whatsapp").json()["message_uuid"][0]
    assert _statuses(fake, stale, "failed")[-1] == ("failed", "380")
    recorded = len(sim.messages)
    for bad in ("order_update", {"name": "order_update"}, {**template, "components": "none"}):
        assert _send(line, fake, dst=customer, type="whatsapp", template=bad).status_code == 400
    assert len(sim.messages) == recorded


def test_whatsapp_templates_list_a_page_at_a_time_and_read_one_with_its_components(sim, line):
    made = [sim.template(line.acc.auth_id, "4001", f"promo_{n}") for n in range(21)]
    first = line.api.get("WhatsApp/Template/4001/").json()
    assert [t["name"] for t in first["objects"]] == [f"promo_{n}" for n in range(20)]
    assert "components" not in first["objects"][0]
    rest = line.api.get("WhatsApp/Template/4001/", params={"limit": 20, "offset": 20}).json()
    assert ([t["name"] for t in rest["objects"]], rest["meta"]["next"]) == (["promo_20"], None)
    one = line.api.get(f"WhatsApp/Template/4001/{made[3]['template_id']}/").json()
    assert (one["name"], one["status"], one["components"]) == ("promo_3", "APPROVED", made[3]["components"])
    other = sim.subaccount("Other")
    theirs = httpx.get(
        f"{sim.url}/v1/Account/{other.auth_id}/WhatsApp/Template/4001/", auth=(other.auth_id, other.auth_token)
    )
    assert theirs.status_code == 404
    assert ("promo_3", "en_US") in sim.templates


def test_outbound_mms_needs_one_to_ten_media_urls(sim, fake, line):
    customer, media = "+14155550142", ["https://m.test/receipt.png"]
    assert _send(line, fake, dst=customer, type="mms", text="hello").status_code == 400
    assert _send(line, fake, dst=customer, type="mms", media_urls=media * 11).status_code == 400
    assert _send(line, fake, dst=customer, media_urls=media).status_code == 400
    for form in (media, media[0], "https://m.test/a.jpg, https://m.test/b.jpg"):
        assert _send(line, fake, dst=customer, type="mms", media_urls=form).status_code == 202
    assert sim.messages[-1].media == ["https://m.test/a.jpg", "https://m.test/b.jpg"]
    assert _send(line, fake, dst=customer, type="mms", media_urls=",".join(media * 11)).status_code == 400
    uid = _send(line, fake, dst=customer, type="mms", text="Receipt", media_urls=media).json()["message_uuid"][0]
    assert _statuses(fake, uid, "delivered") == [("queued", "000"), ("sent", "000"), ("delivered", "000")]
    assert line.api.get(f"Message/{uid}/").json()["message_type"] == "mms"


def test_get_callbacks_carry_their_parameters_in_the_query_string(sim, fake, line):
    fake.replies["/answer"] = "<Response><Hangup/></Response>"
    assert line.api.post(
        f"Application/{line.app_id}/", json={"answer_method": "GET", "message_method": "GET"}
    ).is_success
    call = sim.call(line.number)
    hit = fake.wait("/answer", CallUUID=call.call_uuid)
    assert (hit.params["tenant"], hit.params["To"]) == ("t1", line.number[1:])
    nonce, signature = hit.headers["X-Plivo-Signature-V3-Nonce"], hit.headers["X-Plivo-Signature-V3"]
    assert validate_v3_signature("GET", hit.url, nonce, line.acc.auth_token, signature)
    call.wait_state("completed")
    sms = sim.text(line.number, "+14155550199", "Hi")
    hit = fake.wait("/message", MessageUUID=sms.message_uuid)
    assert (hit.params["tenant"], hit.params["Text"]) == ("t1", "Hi") and _signed_v2(hit, line.acc.auth_token)
    uid = _send(line, fake, dst="+14155550142", text="Hi", method="GET").json()["message_uuid"][0]
    assert _statuses(fake, uid, "delivered") == [("queued", "000"), ("sent", "000"), ("delivered", "000")]


def test_messaging_callbacks_retry_three_times_until_the_receiver_answers_200(sim, fake, line):
    sim.time_factor = 0.0001

    def attempts(msg):
        return [c.status for c in sim.callbacks if c.params.get("MessageUUID") == msg.message_uuid]

    fake.fail_next["/message"] = 2
    flaky = sim.text(line.number, "+14155550199", "Anyone there?")
    _until(lambda: attempts(flaky) == [500, 500, 200])
    fake.fail_next["/message"] = 99
    down = sim.text(line.number, "+14155550199", "Hello?")
    _until(lambda: len(attempts(down)) == 4)
    time.sleep(0.15)
    assert attempts(down) == [500] * 4 and attempts(flaky) == [500, 500, 200]


def test_a_voice_callback_retries_after_plivos_own_delay_however_slow_the_rings(sim, fake, line):
    sim.time_factor = 1000.0
    hangup = {"hangup_url": f"{fake.url}/hangup#rc=1&rd=200&rp=5xx"}
    assert line.api.post(f"Application/{line.app_id}/", json=hangup).status_code == 202
    fake.replies["/answer"], fake.fail_next["/hangup"] = "<Response><Hangup/></Response>", 1
    call = sim.call(line.number)
    call.wait_state("completed")

    def hangups():
        return [c.status for c in sim.callbacks if "/hangup" in c.url and c.params.get("CallUUID") == call.call_uuid]

    _until(lambda: hangups() == [500, 200], timeout=2)


def _event(name: str):
    return lambda r: r.params.get("EventName") == name


LEFT = [("/answer", ""), ("/mpc", "MPCInitialized"), ("/mpc", "ParticipantJoin")]
LEFT += [("/hangup", ""), ("/mpc", "ParticipantExit"), ("/mpc", "MPCEnd")]


def _caller_leaves(sim, fake, line) -> list[tuple[str, str]]:
    """A caller waits in mpc_q1 and hangs up; what HS got (LEFT, with no faults) once the simulator is idle."""
    fake.replies["/answer"] = _queue_xml(fake)
    caller = sim.call(line.number)
    _until(lambda: caller.mpc)
    caller.hangup()
    _until(sim.idle)
    return fake.trail()


def test_a_drop_fault_loses_the_next_matching_callback_only(sim, fake, line):
    dropped = sim.fault("drop", lambda r: "/mpc" in r.path, account=line.acc.auth_id)
    assert _caller_leaves(sim, fake, line) == [step for step in LEFT if step != ("/mpc", "MPCInitialized")]
    assert [r.params["EventName"] for r in dropped.hits] == ["MPCInitialized"]


@pytest.mark.parametrize("action", ["duplicate", "race"])
def test_a_duplicate_or_race_fault_sends_every_matching_callback_twice(sim, fake, line, action):
    sim.fault(action, lambda r: True, times=math.inf, account=line.acc.auth_id)
    assert _caller_leaves(sim, fake, line) == [LEFT[0]] + [step for step in LEFT[1:] for _ in range(2)]


def test_a_delayed_callback_waits_for_release_without_blocking_idle(sim, fake, line):
    held = sim.fault("delay", _event("ParticipantJoin"), account=line.acc.auth_id)
    assert _caller_leaves(sim, fake, line) == [step for step in LEFT if step != ("/mpc", "ParticipantJoin")]
    held.release()
    _until(lambda: fake.trail()[-1] == ("/mpc", "ParticipantJoin"))


def test_a_race_fault_has_both_copies_of_a_callback_in_flight_at_once(sim, fake, line):
    sim.fault("race", _event("MPCInitialized"), account=line.acc.auth_id)
    fake.meet.add("/mpc")
    assert _caller_leaves(sim, fake, line) == [LEFT[0], LEFT[1], *LEFT[1:]]
    assert "/mpc" in fake.met


def test_a_flush_sends_what_a_delay_holds_at_once_and_holds_what_comes_later(sim, fake, line):
    held = sim.fault("delay", lambda r: "/mpc" in r.path, times=math.inf, account=line.acc.auth_id)
    fake.replies["/answer"] = _queue_xml(fake)
    caller = sim.call(line.number)
    _until(lambda: caller.mpc and sim.idle() and len(held.delayed) == 2)
    fake.meet.add("/mpc")
    held.flush()
    caller.hangup()
    _until(lambda: sim.idle() and len(held.delayed) == 2)
    assert "/mpc" in fake.met and sorted(fake.trail()) == sorted(LEFT[:4])


def test_a_flushed_callback_is_still_in_flight_when_the_call_sends_its_next_one(sim, fake, line):
    held = sim.fault("delay", _event("ParticipantJoin"), account=line.acc.auth_id)
    fake.replies["/answer"] = _queue_xml(fake)
    caller = sim.call(line.number)
    _until(lambda: caller.mpc and sim.idle() and held.delayed)
    fake.meet.add("/mpc")
    held.flush()
    caller.hangup()
    _until(lambda: sim.idle() and len(fake.hits) == len(LEFT))
    assert "/mpc" in fake.met and sorted(fake.trail()) == sorted(LEFT)


@pytest.mark.parametrize(("send", "copies"), [("release", 1), ("flush", 2)])
def test_a_released_callback_skips_the_other_faults_and_a_flushed_one_meets_them(sim, fake, line, send, copies):
    held = sim.fault("delay", _event("ParticipantJoin"), account=line.acc.auth_id)
    sim.fault("duplicate", _event("ParticipantJoin"), times=math.inf, account=line.acc.auth_id)
    _caller_leaves(sim, fake, line)
    getattr(held, send)()
    _until(sim.idle)
    assert fake.trail().count(("/mpc", "ParticipantJoin")) == copies


def test_a_delay_released_by_a_later_callback_reorders_the_two(sim, fake, line):
    sim.fault("delay", _event("MPCInitialized"), after=_event("ParticipantExit"), account=line.acc.auth_id)
    trail = _caller_leaves(sim, fake, line)
    assert sorted(trail) == sorted(LEFT)
    assert trail.index(("/mpc", "MPCInitialized")) > trail.index(("/mpc", "ParticipantExit"))


def test_a_stalled_reply_reaches_the_client_after_the_callback_it_waits_for(sim, fake, line):
    fake.replies["/answer"] = _queue_xml(fake)
    phone = _phone(sim, line)
    sim.fault("stall", lambda r: r.path.endswith("/Participant/"), after=_event("Ringing"), account=line.acc.auth_id)
    sim.call(line.number)
    fake.wait("/mpc", EventName="ParticipantJoin")
    body = {"role": "Agent", "from": "+14155550111", "to": _sip(phone), "call_status_callback_url": f"{fake.url}/leg"}
    added = line.api.post("MultiPartyCall/name_mpc_q1/Participant/", json=body)
    ringing = [h.params["CallUUID"] for h in fake.hits if h.params.get("EventName") == "Ringing"]
    assert ringing == [added.json()["calls"][0]["call_uuid"]]


def test_resetting_faults_releases_what_they_hold_and_spares_the_next_request(sim, fake, line):
    sim.fault("reject", lambda r: True, times=math.inf, status=429, account=line.acc.auth_id)
    sim.fault("delay", _event("ParticipantJoin"), account=line.acc.auth_id)
    held = sim.faults[1]
    sim.reset_faults()
    assert sim.faults == [] and held.released.is_set()
    assert line.api.get("Number/").status_code != 429


def test_a_callback_held_at_reset_is_dropped_not_delivered_to_the_next_test(sim, fake, line):
    sim.fault("delay", _event("ParticipantJoin"), account=line.acc.auth_id)
    _caller_leaves(sim, fake, line)
    sim.reset_faults()
    waiting = sim.fault("delay", _event("Never"), after=_event("ParticipantJoin"), account=line.acc.auth_id)
    _until(sim.idle)
    assert not waiting.released.is_set()
    assert ("/mpc", "ParticipantJoin") not in fake.trail()


def test_a_fault_leaves_another_accounts_requests_alone_and_stays_unspent(sim, fake, line):
    other = _line(sim, fake)
    lose = sim.fault("lose", lambda r: True, account=line.acc.auth_id)
    assert other.api.get("Application/").status_code != 500
    assert lose.times == 1 and lose.hits == []
    assert line.api.get("Application/").status_code == 500


def test_a_callback_of_another_account_does_not_release_an_after_fault(sim, fake, line):
    other = _line(sim, fake)
    waiting = sim.fault("delay", _event("Never"), after=_event("ParticipantJoin"), account=line.acc.auth_id)
    _caller_leaves(sim, fake, other)
    assert not waiting.released.is_set()
    _caller_leaves(sim, fake, line)
    assert waiting.released.is_set()


def test_a_slow_fault_answers_as_plivo_would_after_its_wait(sim, fake, line):
    plain = line.api.get("Number/").status_code
    sim.fault("slow", lambda r: r.path.endswith("/Number/"), times=math.inf, after_sec=0.2, account=line.acc.auth_id)
    for _ in range(2):
        began = time.monotonic()
        assert line.api.get("Number/").status_code == plain
        assert time.monotonic() - began >= 0.2


def test_a_fault_needs_an_account_and_a_broad_reject_next_does_too(sim, line):
    with pytest.raises(ValueError):
        sim.fault("lose", lambda r: True, account=None)
    with pytest.raises(ValueError):
        sim.reject_next("/Call/", 429)
    sim.reject_next("/Call/9f1c2d3e-aaaa-bbbb-cccc-000000000000/", 429)
    sim.reject_next("/Call/", 429, account=line.acc.auth_id)
    assert len(sim.faults) == 2


def test_a_broad_reject_next_needs_a_complete_uuid(sim):
    with pytest.raises(ValueError):
        sim.reject_next("/Call/deadbeef-1234-", 503)
    sim.reject_next(f"/Call/{uuid.uuid4()}/", 503)
    assert len(sim.faults) == 1


def test_a_lose_fault_applies_each_create_but_answers_5xx(sim, fake, line):
    creates = ("/Call/", "/Participant/", "/Message/")
    lost = sim.fault(
        "lose", lambda r: r.method == "POST" and r.path.endswith(creates), times=3, status=503, account=line.acc.auth_id
    )
    called, added = sim.lead("+14155550152"), sim.lead("+14155550153")
    dial = {"from": "+14155550111", "to": "+14155550152", "answer_url": f"{fake.url}/out"}
    assert line.api.post("Call/", json=dial).status_code == 503
    called.wait_ring().wait_state("in_progress")
    fake.replies["/answer"] = _queue_xml(fake)
    sim.call(line.number)
    fake.wait("/mpc", EventName="ParticipantJoin")
    member = {"role": "Agent", "from": "+14155550111", "to": "+14155550153"}
    assert line.api.post("MultiPartyCall/name_mpc_q1/Participant/", json=member).status_code == 503
    added.wait_ring().wait_state("in_progress")
    assert _send(line, fake, dst="+14155550154", text="Hi").status_code == 503
    sent = next(m for m in sim.messages if m.to == "+14155550154")
    fake.wait("/status", MessageUUID=sent.message_uuid, Status="delivered")
    assert len(lost.hits) == 3


def test_a_timeout_fault_applies_the_create_and_never_answers(sim, fake, line):
    sim.fault("timeout", lambda r: r.path.endswith("/Message/"), account=line.acc.auth_id)
    body = {"src": line.number, "dst": "+14155550155", "text": "Hi", "url": f"{fake.url}/status"}
    with pytest.raises(httpx.ReadTimeout):
        line.api.post("Message/", json=body, timeout=0.2)
    sent = next(m for m in sim.messages if m.to == "+14155550155")
    fake.wait("/status", MessageUUID=sent.message_uuid, Status="delivered")
    assert _send(line, fake, dst="+14155550156", text="Again").status_code == 202
