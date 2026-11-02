"""A fake Plivo: the REST API HS calls, and a call engine that runs HS's XML and sends V3-signed callbacks.

One engine thread applies every call and MPC change, in order, and fetches XML (answer, action, redirect,
transfer and exit-action URLs) inline. Status callbacks (MPC, call-status, hangup, recording,
transcription) go out through a separate delivery thread, so, as with Plivo, they can reach HS while it is
still inside another request. REST handlers only read state and queue work, so HS can call the API from
inside a callback without deadlocking anything. Real time passes only for a phone's `after`, an MPC
member's `waitTime`, a GetDigits waiting for input, and ring timeouts and retry delays scaled by
`time_factor`.
"""

from __future__ import annotations

import base64
import hashlib
import itertools
import json
import logging
import os
import queue
import random
import re
import secrets
import socket
import string
import threading
import time
import uuid
import xml.etree.ElementTree as ET
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from urllib.parse import parse_qsl

import httpx
import uvicorn
from plivo.utils.signature_v3 import construct_post_url, get_signature_v3
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

log = logging.getLogger(__name__)

SIP_DOMAIN = "phone.plivo.com"
# Plivo hangup cause code -> (HangupCauseName, switch-style hangup_cause, call_status of a never-answered leg)
CAUSES = {
    1000: ("Canceled", "ORIGINATOR_CANCEL", "cancel"),
    2000: ("Invalid Destination Address", "UNALLOCATED_NUMBER", "failed"),
    2020: ("Endpoint Not Registered", "USER_NOT_REGISTERED", "failed"),
    3000: ("No Answer", "NO_ANSWER", "no-answer"),
    3010: ("Busy Line", "USER_BUSY", "busy"),
    3020: ("Rejected", "CALL_REJECTED", "failed"),
    4000: ("Normal Hangup", "NORMAL_CLEARING", "completed"),
    4010: ("End Of XML Instructions", "NORMAL_CLEARING", "completed"),
    4020: ("Multiparty Call Ended", "NORMAL_CLEARING", "completed"),
    4030: ("Kicked Out Of Multiparty Call", "NORMAL_CLEARING", "completed"),
    6010: ("Ring Timeout Reached", "NO_ANSWER", "timeout"),
    7011: ("Error Reaching Answer URL", "NORMAL_CLEARING", "failed"),
    7012: ("Error Reaching Action URL", "NORMAL_CLEARING", "failed"),
    7013: ("Error Reaching Transfer URL", "NORMAL_CLEARING", "failed"),
    7014: ("Error Reaching Redirect URL", "NORMAL_CLEARING", "failed"),
    8011: ("Invalid Answer XML", "NORMAL_CLEARING", "failed"),
    8012: ("Invalid Action XML", "NORMAL_CLEARING", "failed"),
    8013: ("Invalid Transfer XML", "NORMAL_CLEARING", "failed"),
    8014: ("Invalid Redirect XML", "NORMAL_CLEARING", "failed"),
    9000: ("Lost Race", "LOSE_RACE", "cancel"),
}
# Stretches every wait deadline (not simulated time) on a loaded machine: HS_TEST_SLOW=3.
SLOW = float(os.environ.get("HS_TEST_SLOW", "1"))
PHONE_BEHAVIOURS = ("answer", "decline", "timeout", "ignore")
LEAD_BEHAVIOURS = ("answer", "busy", "no_answer", "reject", "fail")
FAILURES = {
    "decline": 3020,
    "timeout": 6010,
    "busy": 3010,
    "no_answer": 3000,
    "reject": 3020,
    "fail": 2000,
    "unregistered": 2020,
}
# Plivo's own wording is known only for the wait timeout; every other cause here is invented.
EXIT_WAIT_TIMEOUT = "Member Wait Timeout Triggered"
EXIT_HANGUP, EXIT_ENDED, EXIT_ALONE = "sim:hangup", "sim:mpc-ended", "sim:alone"
EXIT_KICKED, EXIT_TRANSFERRED = "sim:kicked", "sim:transferred"
END_EXIT, END_ALONE, END_API = ("sim:participant-exited", "2000"), ("sim:alone", "2010"), ("sim:api", "1000")
EVENT_GROUPS = {
    "mpc-state-changes": ("MPCInitialized", "MPCStart", "MPCEnd"),
    "participant-state-changes": (
        "ParticipantJoin",
        "ParticipantExit",
        "ParticipantHold",
        "ParticipantUnhold",
        "ParticipantMute",
        "ParticipantUnmute",
        "ParticipantCoachModeStart",
        "ParticipantCoachModeStop",
    ),
    "add-participant-api-events": ("AddParticipantByAPIActionInitiated", "AddParticipantByAPIActionCompleted"),
    # Member Speak/Play API completions, under the names HS handles (Plivo's docs list ParticipantAudioStart/Stop).
    "participant-audio-events": ("ParticipantSpeakStopped", "ParticipantAudioStopped"),
    "participant-digit-input-events": ("ParticipantDigitInput",),
}
DIAL_STATUS = {3010: "busy", 3000: "no-answer", 6010: "timeout", 1000: "cancel"}
MEMBER_TOGGLES = (
    ("hold", "ParticipantHold", "ParticipantUnhold"),
    ("mute", "ParticipantMute", "ParticipantUnmute"),
    ("coach_mode", "ParticipantCoachModeStart", "ParticipantCoachModeStop"),
)
SUSPEND = object()  # an element parked the call (MPC, Dial, a waiting GetDigits); the engine resumes it later


def free_socket() -> socket.socket:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    return sock


def serve(app, sock: socket.socket | None = None) -> tuple[str, Callable[[], None]]:
    """Run an ASGI app (with its lifespan) under uvicorn in a thread; returns (base URL, stop). uvicorn's
    loggers, access log included, are left unconfigured so their records reach the root logger (caplog)."""
    sock = sock or free_socket()
    server = uvicorn.Server(uvicorn.Config(app, lifespan="on", log_config=None))
    thread = threading.Thread(target=server.run, kwargs={"sockets": [sock]}, daemon=True)
    thread.start()
    while not server.started:
        if not thread.is_alive():
            raise RuntimeError(f"{app!r} failed to start")
        time.sleep(0.01)

    def stop() -> None:
        server.should_exit = True
        thread.join(10)

    return f"http://127.0.0.1:{sock.getsockname()[1]}", stop


def _fmt(t: float) -> str:
    return datetime.fromtimestamp(t, UTC).strftime("%Y-%m-%d %H:%M:%S") if t else ""


def _rand(chars: str, n: int) -> str:
    return "".join(secrets.choice(chars) for _ in range(n))


def _bool(value, default: bool = False) -> bool:
    if value in (None, ""):
        return default
    return value if isinstance(value, bool) else str(value).lower() == "true"


def _snake(attrs: dict) -> dict:
    """XML attribute names as the REST API spells them: onExitActionUrl -> on_exit_action_url."""
    return {re.sub(r"(?<!^)(?=[A-Z])", "_", k).lower(): v for k, v in attrs.items()}


def _plivo(number: str) -> str:
    """Plivo sends PSTN numbers without the plus."""
    return number.removeprefix("+")


def _e164(address: str) -> str:
    return f"+{address}" if address.isdigit() else address


def _new_account(prefix: str, name: str, parent: _Account | None = None) -> _Account:
    upper, mixed = string.ascii_uppercase + string.digits, string.ascii_letters + string.digits
    return _Account(prefix + _rand(upper, 18), _rand(mixed, 40), name, parent)


def _recording_callback(opts: dict) -> tuple[str, str, str]:
    """(recording callback URL, method, transcription URL) from MPC XML attributes or REST params."""
    transcription = opts.get("transcription_url", "") if _bool(opts.get("transcript")) else ""
    return opts.get("recording_callback_url", ""), opts.get("recording_callback_method", "POST"), transcription


def _duration(call: SimCall) -> str:
    return str(int(call._ended - call._answered) if call._answered else 0)


def _sip_headers(spec: str | None) -> dict[str, str]:
    return {f"X-PH-{k}": v for k, _, v in (h.partition("=") for h in (spec or "").split(",") if h)}


def _valid_sip_headers(spec: str | None) -> bool:
    """Plivo refuses a header name outside [A-Za-z0-9_] or a value outside [A-Za-z0-9%] (encode the rest)."""
    pairs = [h.partition("=") for part in str(spec or "").split("<") for h in part.split(",") if h]
    return all(re.fullmatch(r"[A-Za-z0-9_]+", k) and re.fullmatch(r"[A-Za-z0-9%]*", v) for k, _, v in pairs)


@dataclass
class SimRequest:
    method: str
    path: str  # e.g. /v1/Account/SA.../Call/; a status callback's URL when a fault's predicate sees one
    params: dict  # parsed JSON or form body / query


@dataclass(eq=False)
class Fault:
    """One rule from PlivoSim.fault(); `hits` are the REST requests or callbacks it applied to."""

    sim: PlivoSim
    action: str
    account: str
    match: Callable[[SimRequest], bool]
    times: float
    hits: list[SimRequest] = field(default_factory=list)
    delayed: list = field(default_factory=list)
    released: bool = False

    def release(self) -> None:
        """Send the callbacks it delays, behind what was sent meanwhile; from then on it holds nothing."""
        with self.sim._changed:
            self.released, delayed, self.delayed = True, self.delayed, []
        for job in delayed:
            self.sim._out.put(job)


@dataclass
class SimCallback:
    url: str
    params: dict
    status: int  # HS's response status
    body: str  # HS's response body (XML for answer/action URLs)


@dataclass
class SimCall:
    call_uuid: str
    direction: str  # inbound | outbound
    from_: str
    to: str
    headers: dict[str, str] = field(default_factory=dict)  # X-PH-* SIP headers on the leg
    state: str = "ringing"  # ringing | in_progress | completed
    said: list[str] = field(default_factory=list)  # Speak texts and Play URLs, in order
    mpc: str | None = None  # MPC room name when the leg is in one
    hangup_cause_code: int | None = None

    # Engine state, set by PlivoSim._new_call.
    _sim = _account = _ring = _member = _resume = _dialing = _dial_a = _bridge = _playing = _status = _menu = None
    _digits, _request_uuid, _source, _dialled = "", "", "", ""
    _started = _answered = _ended = 0.0

    @property
    def held(self) -> bool:
        """Whether this leg's MPC member is on hold."""
        return bool(self._member and self._member.flag("hold"))

    def press(self, digits: str) -> None:
        """Keypad presses: DTMF into the MPC when the leg is in one, else input for the waiting GetDigits."""
        self._sim._queue(self._sim._press, self, digits)

    def hangup(self) -> None:
        """The person on this leg hangs up (declines it while it rings)."""
        self._sim._queue(self._sim._hangup_by_party, self)

    def media_done(self) -> None:
        """The member Speak/Play the MPC API started on this leg finished playing."""
        self._sim._queue(self._sim._media_done, self)

    def wait_state(self, state: str, timeout: float = 5.0) -> SimCall:
        """Wait until the leg has been in `state` (a call that already ended still counts as answered)."""
        with self._sim._changed:
            if not self._sim._changed.wait_for(lambda: state in self._history, timeout * SLOW):
                raise AssertionError(f"call to {self.to} never reached {state}; it is {self.state}")
        return self


class SimPhone:
    """A softphone registered on one endpoint (Browser SDK or SIP device), or a PSTN lead's number."""

    def __init__(self, sim: PlivoSim, endpoint_username: str):
        self.endpoint_username = endpoint_username
        self.legs: list[SimCall] = []  # every leg that rang this phone
        self.registered = True  # a SIP device on the endpoint; False: calls to the endpoint fail with 2020
        self.sdk: str | None = None  # the `<sub>_<iss>` SIP user a Browser SDK login registers
        self._sim, self._behaviour, self._taken = sim, ("answer", 0.0), 0

    def on_ring(self, behaviour: str = "answer", after: float = 0.0) -> None:
        """answer | decline | timeout (cause 6010 at once) | ignore (rings until the ring timeout); a PSTN
        lead also takes busy | no_answer | reject | fail."""
        assert behaviour in PHONE_BEHAVIOURS + LEAD_BEHAVIOURS, behaviour
        self._behaviour = (behaviour, after)

    def unregister(self) -> None:
        """The device goes offline: the endpoint reads as not registered and calls to it fail (2020)."""
        self.registered, self.sdk = False, None

    def login(self, token: str) -> bool:
        """The Browser SDK's loginWithAccessToken. Only a live token the simulator minted for this endpoint logs in:
        it registers `<sub>_<iss>`, not the endpoint, which stays unregistered. Any other token fails, as
        INVALID_ACCESS_TOKEN does, and changes nothing."""
        claims = self._sim.tokens.get(token)
        if not claims or claims["sub"] != self.endpoint_username or not claims["nbf"] <= time.time() < claims["exp"]:
            return False
        self.sdk = f"{claims['sub']}_{claims['iss']}"
        return True

    def wait_ring(self, timeout: float = 5.0) -> SimCall:
        """The next leg that rang this phone (each leg is returned once, in ring order)."""
        with self._sim._changed:
            if not self._sim._changed.wait_for(lambda: len(self.legs) > self._taken, timeout * SLOW):
                raise AssertionError(f"{self.endpoint_username} did not ring")
        self._taken += 1
        return self.legs[self._taken - 1]

    def dial(self, to: str, headers: dict[str, str] | None = None) -> SimCall:
        """An SDK or SIP dial-out from this endpoint; `headers` are the SDK's extraHeaders (X-PH-*)."""
        return self._sim._dial_out(self, to, headers or {})


@dataclass(eq=False)
class _Account:
    auth_id: str
    auth_token: str
    name: str
    parent: _Account | None = None


@dataclass(eq=False)
class _Recording:
    account: _Account
    id: str
    url: str
    kind: str  # call | mpc | participant
    call_uuid: str
    callback: tuple  # (recording callback URL, method, transcription URL)
    track: str = "participant"  # a participant recording's record_track_type: participant or all
    start: float = field(default_factory=time.time)
    end: float = 0.0
    deleted: bool = False


@dataclass(eq=False)
class _Mpc:
    account: _Account
    name: str
    opts: dict  # MPC-level settings, from the participant that created it
    events: frozenset
    uuid: str = field(default_factory=lambda: str(uuid.uuid4()))
    status: str = "Initialized"
    members: list = field(default_factory=list)
    seq: int = 0
    created: float = field(default_factory=time.time)
    started: float = 0.0
    ended: float = 0.0
    cause: tuple = ("", "")  # termination cause and code
    recording: _Recording | None = None


@dataclass(eq=False)
class _Member:
    mpc: _Mpc
    call: SimCall
    member_id: str
    role: str
    address: str
    api: bool  # added by the API: no XML to go on with after it leaves
    opts: dict
    join_time: float = 0.0
    exit_time: float = 0.0
    exit_cause: str = ""
    timer: threading.Timer | None = None
    recording: _Recording | None = None

    def flag(self, name: str, default: bool = False) -> bool:
        return _bool(self.opts.get(name), default)


@dataclass(eq=False)
class _Ring:
    legs: list
    on_answer: Callable
    on_fail: Callable
    winner: SimCall | None = None


class PlivoSim:
    """Fake Plivo: REST API (calls, MPC, endpoints, applications, numbers, recordings, account) and a
    call engine that executes HS's XML and sends signed (V3) callbacks like Plivo does.

    Knobs: `shuffle_seed` reorders and delays status callbacks (seeded); `deliver_twice` sends each one
    twice; `time_factor` scales ring timeouts, retry delays and recording durations; `short_takes` makes
    every recording 0 s long; `member_address` is "dialled" (as HS dialled it), "plus" or "bare" (no sip:);
    `exit_after_events` delivers a room's ParticipantExit and MPCEnd before the leaving members' exit actions
    are fetched, an order Plivo's concurrent sends allow; `transcribe_first` delivers a <Record> transcription before the element's action URL is fetched.
    """

    def __init__(self, shuffle_seed: int | None = None, deliver_twice: bool = False, time_factor: float = 0.1):
        self.requests: list[SimRequest] = []
        self.callbacks: list[SimCallback] = []
        self.calls: list[SimCall] = []  # every leg, in creation order
        self.errors: list[BaseException] = []  # engine failures; a healthy run has none
        self.transcript = "Hi, please call me back about my order."
        self.shuffle_seed, self.deliver_twice, self.time_factor = shuffle_seed, deliver_twice, time_factor
        self.short_takes, self.member_address, self.exit_after_events, self.transcribe_first = (
            False,
            "dialled",
            False,
            False,
        )
        self.main = _new_account("MA", "Main")
        self.accounts = {self.main.auth_id: self.main}
        self.resources: dict[tuple[str, str], tuple[_Account, dict]] = {}  # (Application|Endpoint|Number, id)
        self.phones: dict[str, SimPhone] = {}
        self.tokens: dict[str, dict] = {}  # every Browser SDK access token POST JWT/Token minted, with its claims
        self.recordings: dict[str, _Recording] = {}
        self._mpcs: list[_Mpc] = []
        self._leads: dict[str, SimPhone] = {}
        self._apps: dict[str, str] = {}
        self._rejects: list[tuple[str, int, str]] = []
        self.faults: list[Fault] = []  # checked in order; the first that matches applies
        self._ids, self._numbers = itertools.count(10001), itertools.count(1000)
        self._changed = threading.Condition()
        self._work: queue.Queue = queue.Queue()
        self._out: queue.Queue = queue.Queue()
        self._http, self._http_out = httpx.Client(timeout=15 * SLOW), httpx.Client(timeout=15 * SLOW)
        threading.Thread(target=self._loop, daemon=True).start()
        threading.Thread(target=self._deliver_loop, daemon=True).start()
        mpc = r"MultiPartyCall/(?P<ref>(?:name|uuid)_[^/]+)"
        member = mpc + r"/Participant/(?P<member>\w+)"
        self._routes = [
            ("GET", r"", self._account_get),
            ("GET", r"Subaccount/(?P<sub>\w+)", self._subaccount_get),
            ("POST", r"Application", self._app_create),
            ("POST", r"Application/(?P<rid>\d+)", self._app_update),
            ("POST", r"Endpoint", self._endpoint_create),
            ("POST", r"Endpoint/(?P<rid>\d+)", self._endpoint_update),
            ("GET", r"(?P<kind>Application|Endpoint)/(?P<rid>\d+)", self._resource_get),
            ("DELETE", r"(?P<kind>Application|Endpoint)/(?P<rid>\d+)", self._resource_delete),
            ("GET", r"Number/(?P<rid>\d+)", self._number_get),
            ("POST", r"Number/(?P<rid>\d+)", self._number_update),
            ("POST", r"Call", self._call_create),
            ("POST", r"Call/(?P<uid>[^/]+)", self._call_transfer),
            ("DELETE", r"Call/(?P<uid>[^/]+)", self._call_hangup),
            ("DELETE", r"Call/(?P<uid>[^/]+)/Record", self._call_record_stop),
            ("DELETE", r"Request/(?P<uid>[^/]+)", self._request_cancel),
            ("GET", mpc, self._mpc_get),
            ("DELETE", mpc, self._mpc_stop),
            ("POST", mpc + "/Participant", self._participant_add),
            ("GET", mpc + "/Participant", self._participant_list),
            ("POST", member, self._participant_update),
            ("DELETE", member, self._participant_kick),
            ("POST", member + "/Record", self._member_record_start),
            ("POST", member + r"/Record/(?:Pause|Resume)", self._member_record_toggle),
            ("POST", mpc + r"/Member/(?P<member>\w+)/(?P<kind>Speak|Play)", self._media_start),
            ("DELETE", mpc + r"/Member/(?P<member>\w+)/(?P<kind>Speak|Play)", self._media_stop),
            ("POST", mpc + "/Record", self._record_start),
            ("POST", mpc + r"/Record/(?P<action>Pause|Resume)", self._record_toggle),
            ("DELETE", mpc + "/Record", self._record_stop),
            ("GET", r"Recording/(?P<rid>[\w-]+)", self._recording_get),
            ("DELETE", r"Recording/(?P<rid>[\w-]+)", self._recording_delete),
            ("GET", r"Transcription/(?P<rid>[\w-]+)", self._transcription_get),
            ("POST", r"JWT/Token", self._jwt_token),
        ]
        api = Route("/v1/Account/{auth_id}/{rest:path}", self._rest, methods=["GET", "POST", "DELETE"])
        apps = Route("/apps/{app_id}", self._customer_app, methods=["GET", "POST"])
        media = Route("/v1/Account/{auth_id}/Recording/{name}", self._media_file, methods=["GET"])
        self.url, stop_api = serve(Starlette(routes=[api, apps]))
        self.media_url, stop_media = serve(Starlette(routes=[media]))  # recording files, on their own origin
        self._stops = (stop_api, stop_media)

    def close(self) -> None:
        for stop in self._stops:
            stop()
        self._work.put(None)
        self._out.put(None)
        self._http.close()
        self._http_out.close()

    # ---- harness API -------------------------------------------------------------------------------

    def subaccount(self, name: str) -> _Account:
        """A new sub-account of the main account (`.auth_id`, `.auth_token`)."""
        acc = _new_account("SA", name, self.main)
        self.accounts[acc.auth_id] = acc
        return acc

    def buy_number(self, auth_id: str) -> str:
        number = f"1415{next(self._numbers):07d}"
        obj = {"number": number, "alias": None, "application": None, "voice_enabled": True}
        self.resources[("Number", number)] = (self.accounts[auth_id], obj)
        return f"+{number}"

    def endpoint(self, auth_id: str, endpoint_id: str | None = None) -> dict:
        """An endpoint of the account: this id, else the one created last."""
        eps = [o for acc, o in self._all("Endpoint") if acc.auth_id == auth_id]
        return next(e for e in eps if e["endpoint_id"] == endpoint_id) if endpoint_id else eps[-1]

    def phone(self, endpoint_username: str) -> SimPhone:
        """Register a softphone on the endpoint (one per endpoint)."""
        return self.phones.setdefault(endpoint_username, SimPhone(self, endpoint_username))

    def audio(self, recording_id: str) -> bytes:
        """The bytes the simulator serves for a recording: deterministic, 16 KiB."""
        return hashlib.sha256(recording_id.encode()).digest() * 512

    def idle(self) -> bool:
        """No engine work queued or running, and no status callback queued or in flight."""
        return self._work.unfinished_tasks == 0 and self._out.unfinished_tasks == 0

    @staticmethod
    def sign(url: str, params: dict, auth_token: str, nonce: str = "") -> dict[str, str]:
        """The signature headers Plivo sends with a form POST of `params` to `url` (the SDK's V3 algorithm)."""
        nonce = nonce or _rand(string.digits, 20)
        base = construct_post_url(url, dict(params)).decode()
        signature = get_signature_v3(auth_token.encode(), base, nonce.encode()).decode()
        return {"X-Plivo-Signature-V3": signature, "X-Plivo-Signature-V3-Nonce": nonce}

    def call(self, to: str, from_: str = "+14155550111", *, digits: str = "") -> SimCall:
        """A PSTN caller dials `to`: the simulator fetches the number's application answer URL and runs
        the XML. `digits` are pressed, in order, whenever a GetDigits is waiting; a `w` in them lets one
        GetDigits attempt time out at once. With the script used up, a GetDigits waits for press() or its
        timeout."""
        if ("Number", _plivo(to)) not in self.resources:
            raise AssertionError(f"{to} is not a simulator number")
        acc, number = self.resources[("Number", _plivo(to))]
        app = self._app_of(number["application"])
        call = self._new_call(acc, "inbound", from_, to, hangup=self._hangup_of(app))
        call._digits = digits
        self._queue(self._inbound, call, app)
        return call

    def lead(self, number: str, behaviour: str = "answer") -> SimPhone:
        """How an outbound PSTN number behaves: answer | busy | no_answer | reject | fail. Returns the
        number as a phone, to see the legs that rang it."""
        assert behaviour in LEAD_BEHAVIOURS, behaviour
        phone = self._leads.setdefault(_plivo(number), SimPhone(self, _e164(_plivo(number))))
        phone.on_ring(behaviour)
        return phone

    def app(self, xml: str) -> str:
        """URL of a customer-owned Plivo app (outside HS) that answers every fetch with `xml`."""
        app_id = str(len(self._apps) + 1)
        self._apps[app_id] = xml
        return f"{self.url}/apps/{app_id}"

    def reject_next(self, fragment: str, status: int, error: str = "rejected by the test") -> None:
        """The next REST request whose path contains `fragment` gets `status` (e.g. 429 on Make Call), with
        `{"error": error}` as its body."""
        self._rejects.append((fragment, status, error))

    def fault(self, action: str, match: Callable[[SimRequest], bool], *, account: str, times: float = 1) -> Fault:
        """Fault the next `times` REST requests or status callbacks of the account `account` that `match` accepts.
        REST: lose applies the request and answers 500. Status callbacks (not XML fetches): delay holds it until
        release, then queues it behind what was sent meanwhile."""
        assert action in ("lose", "delay"), action
        fault = Fault(self, action, account, match, times)
        with self._changed:
            self.faults.append(fault)
        return fault

    def _take(self, seen: SimRequest, auth_id: str, action: str) -> Fault | None:
        """The first unspent, unreleased `action` fault for `auth_id` that matches `seen`; it spends one of its times."""
        with self._changed:
            fault = next(
                (
                    f
                    for f in self.faults
                    if (f.action, f.account) == (action, auth_id) and f.times > 0 and not f.released and f.match(seen)
                ),
                None,
            )
            if fault:
                fault.times -= 1
                fault.hits.append(seen)
        return fault

    def rotate_token(self, auth_id: str) -> str:
        """A new auth token for the account; the old one stops working."""
        self.accounts[auth_id].auth_token = _rand(string.ascii_letters + string.digits, 40)
        return self.accounts[auth_id].auth_token

    def requests_to(self, fragment: str) -> list[SimRequest]:
        return [r for r in self.requests if fragment in r.path]

    # ---- REST --------------------------------------------------------------------------------------

    def _logged(self, request: Request, params: dict) -> Response | None:
        """Log the request; the reply a pending reject_next() asks for, if any."""
        self.requests.append(SimRequest(request.method, request.url.path, params))
        rule = next((r for r in self._rejects if r[0] in request.url.path), None)
        if rule is None:
            return None
        self._rejects.remove(rule)
        return self._reply(rule[1], {"error": rule[2]})

    async def _rest(self, request: Request) -> Response:
        auth_id, rest = request.path_params["auth_id"], request.path_params["rest"].strip("/")
        raw, ctype = await request.body(), request.headers.get("content-type", "")
        try:
            body = json.loads(raw) if raw and "json" in ctype else dict(parse_qsl(raw.decode(), keep_blank_values=True))
        except ValueError:
            return self._reply(400, {"error": "invalid request body"})
        params = {**request.query_params, **(body if isinstance(body, dict) else {})}
        if rejected := self._logged(request, params):
            return rejected
        acc = self._authorized(auth_id, request.headers.get("authorization"))
        if acc is None:
            return self._reply(401, {"error": "authentication failed"})
        for method, pattern, handler in self._routes:
            if method == request.method and (m := re.fullmatch(pattern, rest)):
                reply = self._reply(*handler(acc, params, **m.groupdict()))
                if self._take(SimRequest(request.method, request.url.path, params), auth_id, "lose"):
                    return self._reply(500, {"error": "internal server error"})
                return reply
        return self._reply(404, {"error": "not found"})

    async def _customer_app(self, request: Request) -> Response:
        form = dict(parse_qsl((await request.body()).decode(), keep_blank_values=True))
        self.requests.append(SimRequest(request.method, request.url.path, {**request.query_params, **form}))
        return Response(self._apps[request.path_params["app_id"]], media_type="application/xml")

    async def _media_file(self, request: Request) -> Response:
        """Recording files: public as Plivo serves them by default, but checked when credentials are sent."""
        if rejected := self._logged(request, dict(request.query_params)):
            return rejected
        auth_id, rid = request.path_params["auth_id"], request.path_params["name"].rpartition(".")[0]
        rec = self.recordings.get(rid)
        if rec is None or rec.deleted or rec.account.auth_id != auth_id:
            return Response(status_code=404)
        auth = request.headers.get("authorization")
        if auth and self._authorized(auth_id, auth) is None:
            return Response(status_code=401)
        data, headers = self.audio(rid), {"Accept-Ranges": "bytes"}
        if not (byte_range := request.headers.get("range")):
            return Response(data, 200, headers, "audio/mpeg")
        m, size = re.fullmatch(r"bytes=(\d*)-(\d*)", byte_range.strip()), len(data)
        if m and m[1]:
            start, end = int(m[1]), min(int(m[2] or size - 1), size - 1)
        elif m and m[2]:
            start, end = max(size - int(m[2]), 0), size - 1
        else:
            start, end = size, 0
        if start > end:
            return Response(status_code=416, headers={"Content-Range": f"bytes */{size}"})
        headers["Content-Range"] = f"bytes {start}-{end}/{size}"
        return Response(data[start : end + 1], 206, headers, "audio/mpeg")

    @staticmethod
    def _reply(status: int, body: dict | None = None) -> Response:
        if body is None:
            return Response(status_code=status)
        return JSONResponse({"api_id": str(uuid.uuid4()), **body}, status)

    def _authorized(self, auth_id: str, header: str | None) -> _Account | None:
        """The path's account, when the Basic credentials are its own or its parent's."""
        target = self.accounts.get(auth_id)
        try:
            user, _, token = base64.b64decode((header or "").removeprefix("Basic ")).decode().partition(":")
        except ValueError:
            return None
        cred = self.accounts.get(user)
        if not target or not cred or cred not in (target, target.parent):
            return None
        return target if secrets.compare_digest(cred.auth_token, token) else None

    def _all(self, kind: str) -> list[tuple[_Account, dict]]:
        return [found for (k, _), found in list(self.resources.items()) if k == kind]

    def _owned(self, acc: _Account, kind: str, rid: str) -> dict | None:
        owner, obj = self.resources.get((kind, rid), (None, None))
        return obj if owner is acc else None

    def _app_of(self, uri: str | None) -> dict | None:
        m = re.search(r"Application/(\d+)", uri or "")
        return self.resources.get(("Application", m[1]), (None, None))[1] if m else None

    @staticmethod
    def _hangup_of(app: dict | None) -> tuple[str, str]:
        return (app["hangup_url"], app["hangup_method"]) if app else ("", "")

    def _account_get(self, acc, p):
        return 200, {"auth_id": acc.auth_id, "name": acc.name, "resource_uri": f"/v1/Account/{acc.auth_id}/"}

    def _subaccount_get(self, acc, p, sub):
        s = self.accounts.get(sub)
        if s is None or s.parent is not acc:
            return 404, {"error": "not found"}
        return 200, {"auth_id": s.auth_id, "auth_token": s.auth_token, "enabled": True, "name": s.name}

    def _app_create(self, acc, p):
        if not p.get("app_name") or not p.get("answer_url"):
            return 400, {"error": "app_name and answer_url are required"}
        rid = _rand(string.digits, 17)
        app = {"app_id": rid, "app_name": p["app_name"], "answer_url": p["answer_url"]}
        app.update({k: p.get(k, "POST") for k in ("answer_method", "fallback_method")})
        app.update(
            fallback_answer_url=p.get("fallback_answer_url", ""), hangup_url=p.get("hangup_url") or app["answer_url"]
        )
        app["hangup_method"] = p.get("hangup_method") or app["answer_method"]
        self.resources[("Application", rid)] = (acc, app)
        return 201, {"message": "created", "app_id": rid}

    def _app_update(self, acc, p, rid):
        app = self._owned(acc, "Application", rid)
        if app is None:
            return 404, {"error": "not found"}
        app.update({k: v for k, v in p.items() if k in app and k != "app_id"})
        return 202, {"message": "changed"}

    def _endpoint_create(self, acc, p):
        if not all(p.get(k) for k in ("username", "password", "alias")):
            return 400, {"error": "username, password and alias are required"}
        if not re.fullmatch(r"[A-Za-z0-9.+@_-]+", p["alias"]):
            return 400, {
                "alias": ["Invalid Endpoint Alias name. Allowed characters: Letters, Numbers, . , + , @ , - , _"]
            }
        rid, username = _rand(string.digits, 14), p["username"] + _rand(string.digits, 12)
        ep = {"endpoint_id": rid, "username": username, "alias": p["alias"], "sip_uri": f"sip:{username}@{SIP_DOMAIN}"}
        self.resources[("Endpoint", rid)] = (acc, ep)
        self._endpoint_update(acc, p, rid)
        return 201, {"username": username, "alias": ep["alias"], "message": "created", "endpoint_id": rid}

    def _endpoint_update(self, acc, p, rid):
        ep = self._owned(acc, "Endpoint", rid)
        if ep is None:
            return 404, {"error": "not found"}
        if "password" in p:
            ep["password"] = hashlib.md5(p["password"].encode()).hexdigest()
        if "alias" in p:
            ep["alias"] = p["alias"]
        if p.get("app_id"):
            ep["application"] = f"/v1/Account/{acc.auth_id}/Application/{p['app_id']}/"
        return 202, {"message": "changed"}

    def _resource_get(self, acc, p, kind, rid):
        obj = self._owned(acc, kind, rid)
        if obj is None:
            return 404, {"error": "not found"}
        if kind == "Endpoint":
            phone = self.phones.get(obj["username"])
            return 200, {**obj, "sip_registered": str(bool(phone and phone.registered)).lower()}
        return 200, obj

    def _jwt_token(self, acc, p):
        """A Browser SDK access token: iss the account, sub one of its endpoints, app one of its applications, the
        voice permissions booleans, a validity of at most 24 hours."""
        voice = (p.get("per") or {}).get("voice") or {}
        valid = p.get("iss") == acc.auth_id and self._owned(acc, "Application", str(p.get("app")))
        valid = valid and any(o["username"] == p.get("sub") for a, o in self._all("Endpoint") if a is acc)
        valid = valid and all(isinstance(voice.get(k), bool) for k in ("incoming_allow", "outgoing_allow"))
        nbf, exp = p.get("nbf"), p.get("exp")
        if not (valid and isinstance(nbf, int) and isinstance(exp, int) and 0 < exp - nbf <= 86400):
            return 400, {"error": "invalid token request"}
        token = f"sim.{secrets.token_urlsafe(24)}"
        self.tokens[token] = dict(p)
        return 200, {"token": token}

    def _resource_delete(self, acc, p, kind, rid):
        if self._owned(acc, kind, rid) is None:
            return 404, {"error": "not found"}
        del self.resources[(kind, rid)]
        return 204, None

    def _number_get(self, acc, p, rid):
        obj = self._owned(acc, "Number", rid)
        return (200, obj) if obj else (404, {"error": "not found"})

    def _number_update(self, acc, p, rid):
        obj = self._owned(acc, "Number", rid)
        if obj is None:
            return 404, {"error": "not found"}
        if p.get("app_id"):
            if self._owned(acc, "Application", p["app_id"]) is None:
                return 404, {"error": "application not found"}
            obj["application"] = f"/v1/Account/{acc.auth_id}/Application/{p['app_id']}/"
        if "alias" in p:
            obj["alias"] = p["alias"]
        return 202, {"message": "changed"}

    def _live(self, acc, uid) -> SimCall | None:
        live = (c for c in self.calls if c.call_uuid == uid and c._account is acc and c.state != "completed")
        return next(live, None)

    def _call_create(self, acc, p):
        if not all(p.get(k) for k in ("from", "to", "answer_url")):
            return 400, {"error": "from, to and answer_url are required"}
        if not _valid_sip_headers(p.get("sip_headers")):
            return 400, {"error": "invalid sip_headers"}
        request_uuid = str(uuid.uuid4())
        self._queue(self._make_call, acc, p, request_uuid)
        return 201, {"message": "call fired", "request_uuid": request_uuid}

    def _call_transfer(self, acc, p, uid):
        call = self._live(acc, uid)
        if call is None or call.state != "in_progress":
            return 404, {"error": "call not found"}
        if p.get("legs", "aleg") not in ("aleg", "both") or not p.get("aleg_url"):
            return 400, {"error": "aleg_url is required"}
        self._queue(self._transfer, call, p["aleg_url"], p.get("aleg_method", "POST"))
        return 202, {"message": "call transferred"}

    def _call_hangup(self, acc, p, uid):
        call = self._live(acc, uid)
        if call is None:
            return 404, {"error": "call not found"}
        self._queue(self._hangup_by_api, call)
        return 204, None

    def _call_record_stop(self, acc, p, uid):
        return (204, None) if self._live(acc, uid) else (404, {"error": "call not found"})

    def _request_cancel(self, acc, p, uid):
        legs = [c for c in self.calls if c._request_uuid == uid and c._account is acc and c.state == "ringing"]
        for leg in legs:
            self._queue(self._hangup_by_api, leg)
        return (204, None) if legs else (404, {"error": "request not found"})

    def _find_mpc(self, acc, ref) -> _Mpc | None:
        kind, _, key = ref.partition("_")
        found = (m for m in reversed(self._mpcs) if m.account is acc and key == (m.name if kind == "name" else m.uuid))
        return next(found, None)

    def _members(self, acc, ref, member) -> list[_Member]:
        mpc = self._find_mpc(acc, ref)
        return [m for m in (mpc.members if mpc else []) if not m.exit_cause and member in ("all", m.member_id)]

    def _mpc_get(self, acc, p, ref):
        mpc = self._find_mpc(acc, ref)
        if mpc is None:
            return 404, {"error": "not found"}
        times = {"creation_time": _fmt(mpc.created), "start_time": _fmt(mpc.started), "end_time": _fmt(mpc.ended)}
        found = {"friendly_name": mpc.name, "mpc_uuid": mpc.uuid, "status": mpc.status, **times}
        cause = {"termination_cause": mpc.cause[0], "termination_cause_code": mpc.cause[1]}
        return 200, {**found, **cause, "stay_alone": _bool(mpc.opts.get("stay_alone"))}

    def _mpc_stop(self, acc, p, ref):
        mpc = self._find_mpc(acc, ref)
        if mpc is None or mpc.status == "Ended":
            return 404, {"error": "not found"}
        self._queue(self._stop_mpc, mpc)
        return 204, None

    def _participant_add(self, acc, p, ref):
        mpc = self._find_mpc(acc, ref)
        if ref.startswith("uuid_") and mpc is None:
            return 404, {"error": "not found"}
        if str(p.get("role", "")).lower() not in ("agent", "supervisor", "customer"):
            return 400, {"error": "role must be Agent, Supervisor or Customer"}
        name, request_uuid = mpc.name if mpc else ref.partition("_")[2], str(uuid.uuid4())
        reply = {"message": "add participant action initiated", "request_uuid": request_uuid}
        if p.get("call_uuid"):
            call = self._live(acc, p["call_uuid"])
            if call is None or call.state != "in_progress":
                return 404, {"error": "call not found"}
            self._queue(self._move_in, call, name, p, request_uuid)
            return 201, {"calls": [{"to": call.to, "from": call.from_, "call_uuid": call.call_uuid}], **reply}
        tos = [t for t in str(p.get("to", "")).split("<") if t]
        if not tos or not p.get("from"):
            return 400, {"error": "from and to are required"}
        if str(p["from"]).startswith("sip:") or len(str(p.get("caller_name") or "")) > 50:
            return 400, {"error": "from must be a phone number and caller_name at most 50 characters"}
        if not _valid_sip_headers(p.get("sip_headers")):
            return 400, {"error": "invalid sip_headers"}
        calls = [{"to": to, "from": p["from"], "call_uuid": str(uuid.uuid4())} for to in tos]
        self._queue(self._add_participants, acc, name, p, calls, request_uuid)
        return 201, {"calls": calls, **reply}

    def _participant(self, m: _Member) -> dict:
        member = {"member_id": m.member_id, "member_address": m.address, "role": m.role, "call_uuid": m.call.call_uuid}
        return {**member, "exit_cause": m.exit_cause or None, **{k: m.flag(k) for k in ("coach_mode", "mute", "hold")}}

    def _participant_list(self, acc, p, ref):
        mpc = self._find_mpc(acc, ref)
        if mpc is None:
            return 404, {"error": "not found"}
        objects = [self._participant(m) for m in mpc.members]
        return 200, {"meta": {"count": len(objects), "next": None}, "objects": objects}

    def _participant_update(self, acc, p, ref, member):
        members = self._members(acc, ref, member)
        for m in members:
            self._queue(self._update_member, m, p)
        changed = {k: p[k] for k in ("coach_mode", "mute", "hold") if k in p}
        return (202, {"message": "participant updated", **changed}) if members else (404, {"error": "not found"})

    def _participant_kick(self, acc, p, ref, member):
        members = self._members(acc, ref, member)
        for m in members:
            self._queue(self._exit, m, EXIT_KICKED)
        return (204, None) if members else (404, {"error": "not found"})

    def _member_record_start(self, acc, p, ref, member):
        members = self._members(acc, ref, member)
        if len(members) != 1 or members[0].recording:
            return 400, {"error": "no such member, or it is already recording"}
        rec = self._new_recording(acc, "participant", members[0].call.call_uuid, _recording_callback(p))
        rec.track = p.get("record_track_type", "all")
        self._queue(setattr, members[0], "recording", rec)
        return 200, {"message": "participant record started", "recording_id": rec.id, "recording_url": rec.url}

    def _member_record_toggle(self, acc, p, ref, member):
        """Plivo pauses or resumes only the recording of the record_track_type asked for, `all` by default."""
        track = p.get("record_track_type", "all")
        recording = [m for m in self._members(acc, ref, member) if m.recording and track in (m.recording.track, "both")]
        return (204, None) if recording else (409, {"error": "no recording in progress"})

    def _media_start(self, acc, p, ref, member, kind):
        members = self._members(acc, ref, member)
        if not members:
            return 404, {"error": "member not found"}
        self._queue(self._media, members, kind, member, p.get("text" if kind == "Speak" else "url", ""))
        return 202, {"message": f"{kind.lower()} queued into MPC", "mpcMemberId": [m.member_id for m in members]}

    def _media_stop(self, acc, p, ref, member, kind):
        for m in self._members(acc, ref, member):
            if m.call._playing and m.call._playing[0] == kind:
                self._queue(self._media_done, m.call)
        return 204, None

    def _record_start(self, acc, p, ref):
        mpc = self._find_mpc(acc, ref)
        if mpc is None or mpc.status == "Ended":
            return 404, {"error": "not found"}
        if mpc.recording:
            return 400, {"error": "recording already in progress"}
        rec = self._new_recording(
            acc, "mpc", mpc.members[0].call.call_uuid if mpc.members else "", _recording_callback(p)
        )
        self._queue(self._attach_recording, mpc, rec)
        return 200, {"message": f"MPC: {mpc.name} record started", "recording_id": rec.id, "recording_url": rec.url}

    def _record_toggle(self, acc, p, ref, action):
        mpc = self._find_mpc(acc, ref)
        if mpc is None:
            return 404, {"error": "not found"}
        if mpc.recording is None:
            return 409, {"error": "no recording in progress"}
        self._queue(self._recording_event, mpc, f"MPCRecording{action}d")
        return 204, None

    def _record_stop(self, acc, p, ref):
        mpc = self._find_mpc(acc, ref)
        if mpc is None or mpc.recording is None:
            return 404, {"error": "no recording in progress"}
        self._queue(self._stop_mpc_recording, mpc)
        return 204, None

    def _recording(self, acc, rid) -> _Recording | None:
        rec = self.recordings.get(rid)
        return rec if rec and rec.account is acc and not rec.deleted else None

    def _recording_get(self, acc, p, rid):
        rec = self._recording(acc, rid)
        if rec is None:
            return 404, {"error": "not found"}
        found = {"recording_id": rid, "recording_url": rec.url, "recording_type": rec.kind, "call_uuid": rec.call_uuid}
        ms = self._seconds(rec) * 1000 if rec.end else 0
        return 200, {**found, "recording_duration_ms": str(ms), "add_time": _fmt(rec.start)}

    def _recording_delete(self, acc, p, rid):
        rec = self._recording(acc, rid)
        if rec is None:
            return 404, {"error": "not found"}
        rec.deleted = True
        return 204, None

    def _transcription_get(self, acc, p, rid):
        rec = self._recording(acc, rid)
        if rec is None or not rec.end:
            return 404, {"error": "transcription not found"}
        seconds = self._seconds(rec)
        found = {"cost": 0.005, "rate": 0.05, "status": "completed", "transcription": self._segments(seconds)}
        return 200, {**found, "recording_duration_ms": seconds * 1000, "recording_start_ms": int(rec.start * 1000)}

    # ---- engine ------------------------------------------------------------------------------------

    def _loop(self) -> None:
        while (job := self._work.get()) is not None:
            fn, args = job
            try:
                fn(*args)
            except Exception as exc:
                log.exception("plivo sim: %s failed", fn.__name__)
                self.errors.append(exc)
            finally:
                self._work.task_done()

    def _deliver_loop(self) -> None:
        """Send status callbacks in order, or (shuffle_seed) in a seeded shuffle of each burst, with small
        random delays."""
        rng = random.Random(self.shuffle_seed)
        while True:
            batch = [self._out.get()]
            if self.shuffle_seed is not None:
                time.sleep(0.01)
                while not self._out.empty():
                    batch.append(self._out.get())
                rng.shuffle(batch)
            for job in batch:
                try:
                    if job is None:
                        return
                    if self.shuffle_seed is not None:
                        time.sleep(rng.uniform(0, 0.005))
                    if self._delayed(job):
                        continue
                    for _ in range(2 if self.deliver_twice else 1):
                        self._send(*job, http=self._http_out)
                except Exception as exc:
                    log.exception("plivo sim: callback delivery failed")
                    self.errors.append(exc)
                finally:
                    self._out.task_done()

    def _delayed(self, job: tuple) -> bool:
        """Whether a delay fault holds this callback until its release."""
        acc, url, params, method = job
        with self._changed:
            if fault := self._take(SimRequest(method, url.partition("#")[0], params), acc.auth_id, "delay"):
                fault.delayed.append(job)
            return fault is not None

    def _queue(self, fn: Callable, *args) -> None:
        self._work.put((fn, args))

    def _post(self, acc: _Account, url: str, params: dict, method: str = "POST") -> None:
        """A status callback, sent by the delivery thread."""
        if url:
            self._out.put((acc, url, params, method or "POST"))

    def _later(self, delay: float, fn: Callable, *args) -> threading.Timer:
        timer = threading.Timer(delay, self._queue, (fn, *args))
        timer.daemon = True
        timer.start()
        return timer

    def _notify(self) -> None:
        with self._changed:
            self._changed.notify_all()

    def _set_state(self, call: SimCall, state: str) -> None:
        call.state = state
        call._history.append(state)
        self._notify()

    def _new_call(self, acc, direction, from_, to, *, call_uuid="", headers=None, hangup=("", ""), request_uuid=""):
        call = SimCall(call_uuid or str(uuid.uuid4()), direction, _e164(from_), _e164(to), headers or {})
        call._sim, call._account, call._hangup, call._request_uuid = self, acc, hangup, request_uuid
        call._history, call._timers, call._started = ["ringing"], [], time.time()
        self.calls.append(call)
        return call

    def _send(self, acc: _Account, url: str, params: dict, method: str = "POST", http=None) -> SimCallback:
        """Deliver one signed request, retrying as the URL's #rc/#rp/#rd fragment asks (Plivo's default: once,
        on a connection failure only)."""
        url, _, fragment = url.partition("#")
        opts = dict(parse_qsl(fragment))
        policy, delay = opts.get("rp", "ct,rt").split(","), int(opts.get("rd", 0)) / 1000 * self.time_factor
        for attempt in range(1 + int(opts.get("rc", 1))):
            if attempt:
                time.sleep(delay)
            headers = self.sign(url, params, acc.auth_token)
            main = self.sign(url, params, (acc.parent or acc).auth_token, headers["X-Plivo-Signature-V3-Nonce"])
            headers["X-Plivo-Signature-Ma-V3"] = main["X-Plivo-Signature-V3"]
            try:
                r = (http or self._http).request(method, url, data=params, headers=headers)
                status, body = r.status_code, r.text
            except httpx.TransportError as exc:
                status, body = 0, str(exc)
            callback = SimCallback(url, params, status, body)
            self.callbacks.append(callback)
            retry = "all" in policy or (f"{status // 100}xx" in policy if status else "ct" in policy)
            if 200 <= status < 300 or not retry:
                break
        return callback

    def _std(self, call: SimCall) -> dict:
        """What every request about a call carries, its X-PH-* headers included."""
        p = {"CallUUID": call.call_uuid, "From": _plivo(call.from_), "To": _plivo(call.to), "Direction": call.direction}
        p.update(CallerName=p["From"], CallStatus="ringing" if call.state == "ringing" else "in-progress")
        if call._request_uuid:
            p.update(RequestUUID=call._request_uuid, ALegUUID=call.call_uuid, ALegRequestUUID=call._request_uuid)
        return {**p, **call.headers}

    def _fetch(self, call: SimCall, url: str, method: str, extra: dict | None = None, code=7012, fallback=""):
        """HS's XML document for the call, or None once the call ended as Plivo ends it on a bad reply."""
        cb = self._send(call._account, url, {**self._std(call), **(extra or {})}, method or "POST")
        if not 200 <= cb.status < 300:
            if fallback:
                return self._fetch(call, fallback, "POST", extra, code)
            return self._end(call, code, "Plivo")
        try:
            root = ET.fromstring(cb.body.encode())
        except ET.ParseError:
            root = None
        if root is None or root.tag != "Response":
            return self._end(call, code + 1000, "Plivo")
        return list(root)

    def _execute(self, call: SimCall, elements: list) -> None:
        i = 0
        while call.state != "completed":
            if i == len(elements):
                return self._end(call, 4010, "Plivo")
            el, i = elements[i], i + 1
            handler = getattr(self, f"_x_{el.tag}", None)
            nxt = handler(call, el) if handler else None
            if nxt is SUSPEND:
                call._resume = elements[i:]
                return None
            if nxt is not None:
                elements, i = nxt, 0

    def _action(self, call: SimCall, el: ET.Element, extra: dict):
        """Post an element's result to its action URL; with redirect on (the default) the reply takes over."""
        url, method = el.get("action"), el.get("method", "POST")
        if not url:
            return None
        if not _bool(el.get("redirect"), True):
            self._send(call._account, url, {**self._std(call), **extra}, method)
            return None
        return self._fetch(call, url, method, extra)

    def _say(self, call: SimCall, el: ET.Element) -> None:
        for child in el:
            if child.tag in ("Speak", "Play"):
                self._x_Speak(call, child)

    def _x_Speak(self, call, el):
        call.said.append("".join(el.itertext()).strip())  # SSML: the text without its tags

    _x_Play = _x_Speak

    def _x_GetDigits(self, call, el):
        call._menu = {"el": el, "attempt": 0}
        return self._menu_attempt(call)

    _x_GetInput = _x_GetDigits

    def _menu_attempt(self, call: SimCall):
        call._menu["attempt"] += 1
        self._say(call, call._menu["el"])
        return self._menu_input(call)

    def _menu_input(self, call: SimCall):
        """Read the waiting GetDigits' input from the digit script; with none scripted, wait for press() or
        the element's timeout. A `w` (or the timeout) is one attempt with no input."""
        menu = call._menu
        el, digits, quiet = menu["el"], "", False
        while call._digits and len(digits) < int(el.get("numDigits", 99)):
            key, call._digits = call._digits[0], call._digits[1:]
            if key in "wW" or key == el.get("finishOnKey", "#"):
                quiet = not digits
                break
            digits += key
        if digits:
            call._menu = None
            return self._action(call, el, {"Digits": digits} | ({"InputType": "dtmf"} if el.tag == "GetInput" else {}))
        if not quiet:
            menu["wait"] = object()
            call._timers.append(self._later(float(el.get("timeout", 5)), self._menu_timeout, call, menu["wait"]))
            return SUSPEND
        if menu["attempt"] < int(el.get("retries", 1)):
            return self._menu_attempt(call)
        call._menu = None
        return None

    def _menu_resume(self, call: SimCall) -> None:
        call._menu["wait"] = None
        nxt = self._menu_input(call)
        if nxt is not SUSPEND:
            resume, call._resume = call._resume, None
            self._execute(call, resume if nxt is None else nxt)

    def _menu_timeout(self, call: SimCall, wait: object) -> None:
        if call._menu and call._menu.get("wait") is wait and call.state != "completed":
            call._digits = "w" + call._digits
            self._menu_resume(call)

    def _x_Record(self, call, el):
        rec = self._new_recording(call._account, "call", call.call_uuid, ("", "POST", ""))
        rec.end = rec.start
        seconds = self._seconds(rec)
        start, ms = int(rec.start * 1000), seconds * 1000
        result = {"RecordUrl": rec.url, "RecordingID": rec.id, "RecordingDuration": str(seconds)}
        result.update(RecordingDurationMs=str(ms), RecordingStartMs=str(start), RecordingEndMs=str(start + ms))
        text = {"transcription": self.transcript, "transcription_charge": "0.00500", "transcription_rate": "0.05000"}
        text.update(duration=str(seconds), call_uuid=call.call_uuid, recording_id=rec.id, error="")
        transcribed = (call._account, el.get("transcriptionUrl", ""), text, el.get("transcriptionMethod", "POST"))
        if self.transcribe_first and transcribed[1]:
            self._send(*transcribed)
        doc = self._action(call, el, {**result, "Digits": ""})
        acc, std = call._account, self._std(call)
        self._post(acc, el.get("callbackUrl", ""), {**std, **result}, el.get("callbackMethod", "POST"))
        if not self.transcribe_first:
            self._post(*transcribed)
        return doc

    def _x_Redirect(self, call, el):
        return self._fetch(call, (el.text or "").strip(), el.get("method", "POST"), code=7014)

    def _x_Hangup(self, call, el):
        self._end(call, {"rejected": 3020, "busy": 3010}.get(el.get("reason", ""), 4000), "Answer XML")

    def _x_Dial(self, call, el):
        caller_id, legs = el.get("callerId") or call.from_, []
        for t in (t for t in el if t.tag in ("Number", "User")):
            headers = _sip_headers(t.get("sipHeaders") or el.get("sipHeaders"))
            legs.append(self._new_call(call._account, "outbound", caller_id, (t.text or "").strip(), headers=headers))
        if not legs:
            return None
        call._dialing = (el, legs)
        for leg in legs:
            leg._dial_a = call

        def answered(leg):
            call._bridge, leg._bridge = leg, call

        self._dial(legs, answered, lambda leg: self._dial_done(call, leg), int(el.get("timeout") or 60))
        return SUSPEND

    def _dial_done(self, call: SimCall, bleg: SimCall) -> None:
        """A Dial ended: report it to the action URL and go on with the call."""
        dialing, call._dialing, call._bridge = call._dialing, None, None
        if dialing is None or call.state == "completed":
            return
        answered = bleg._answered > 0
        status = "completed" if answered else DIAL_STATUS.get(bleg.hangup_cause_code, "failed")
        extra = {"DialStatus": status, "DialALegUUID": call.call_uuid, "DialBLegUUID": bleg.call_uuid}
        extra.update(DialRingStatus=str(answered).lower(), DialHangupCause=CAUSES[bleg.hangup_cause_code][1])
        doc = self._action(call, dialing[0], extra)
        resume, call._resume = call._resume, None
        self._execute(call, doc if doc is not None else resume or [])

    def _drop_dial(self, call: SimCall) -> None:
        legs, call._dialing, call._bridge = call._dialing[1] if call._dialing else [], None, None
        for leg in legs:
            self._end(leg, 4000 if leg.state == "in_progress" else 1000, "Plivo")

    def _x_MultiPartyCall(self, call, el):
        opts = _snake(el.attrib)
        mpc = self._mpc_for(call._account, (el.text or "").strip(), opts)
        self._join(self._member(mpc, call, opts, api=False))
        return SUSPEND

    def _inbound(self, call: SimCall, app: dict | None) -> None:
        """Plivo asks the answer URL while the call still rings, and answers when the document runs."""
        if app is None:
            return self._end(call, 7011, "Plivo")
        doc = self._fetch(call, app["answer_url"], app["answer_method"], code=7011, fallback=app["fallback_answer_url"])
        if doc is not None:
            call._answered = time.time()
            self._set_state(call, "in_progress")
            self._execute(call, doc)

    def _dial_out(self, phone: SimPhone, to: str, headers: dict[str, str]) -> SimCall:
        acc, ep = next((a, o) for a, o in self._all("Endpoint") if o["username"] == phone.endpoint_username)
        app = self._app_of(ep.get("application"))
        from_ = f"sip:{phone.sdk}@{SIP_DOMAIN}" if phone.sdk else ep["sip_uri"]
        call = self._new_call(acc, "inbound", from_, to, headers=headers, hangup=self._hangup_of(app))
        self._queue(self._inbound, call, app)
        return call

    def _make_call(self, acc: _Account, p: dict, request_uuid: str) -> None:
        answer = (p["answer_url"], p.get("answer_method", "POST"))
        hangup = (p.get("hangup_url") or answer[0], p.get("hangup_method") or answer[1])
        dial = {"headers": _sip_headers(p.get("sip_headers")), "hangup": hangup, "request_uuid": request_uuid}
        legs = [self._new_call(acc, "outbound", p["from"], to, **dial) for to in str(p["to"]).split("<")]

        def answered(leg):
            doc = self._fetch(leg, *answer, code=7011, fallback=p.get("fallback_url", ""))
            if doc is not None:
                self._execute(leg, doc)

        self._dial(legs, answered, lambda leg: None, int(p.get("ring_timeout") or 60))

    # Ringing: every leg of one request rings at once, the first to answer wins and the rest lose the race.

    def _dial(self, legs: list[SimCall], on_answer: Callable, on_fail: Callable, ring_timeout: int) -> None:
        ring = _Ring(legs, on_answer, on_fail)
        for leg in legs:
            leg._ring = ring
            self._call_status(leg, "Initiated")
            self._call_status(leg, "Ringing")
        for leg, (behaviour, after) in [(leg, self._alert(leg)) for leg in legs]:
            if leg.state != "ringing":
                continue
            leg._timers.append(self._later(ring_timeout * self.time_factor, self._ring_outcome, leg, "timeout"))
            if behaviour == "ignore":
                continue
            if after:
                leg._timers.append(self._later(after, self._ring_outcome, leg, behaviour))
            else:
                self._ring_outcome(leg, behaviour)

    def _alert(self, leg: SimCall) -> tuple[str, float]:
        """Ring the leg's far end and say how it will respond. An endpoint's user rings its SIP device; its
        `<username>_<auth id>` rings its Browser SDK login."""
        user = leg.to.removeprefix("sip:").partition("@")[0]
        username = user.partition("_")[0]
        phone = self.phones.get(username)
        if not leg.to.startswith("sip:"):
            phone = self._leads.get(_plivo(leg.to))
            if phone is None:
                return "answer", 0.0
        elif not any(o["username"] == username for _, o in self._all("Endpoint")):
            return "fail", 0.0
        elif not phone or not (phone.registered if user == username else phone.sdk == user):
            return "unregistered", 0.0
        phone.legs.append(leg)
        self._notify()
        return phone._behaviour

    def _ring_outcome(self, leg: SimCall, outcome: str) -> None:
        if leg.state != "ringing":
            return None
        if outcome != "answer":
            return self._end(leg, FAILURES[outcome], "Callee")
        ring = leg._ring
        ring.winner, leg._answered = leg, time.time()
        self._set_state(leg, "in_progress")
        self._call_status(leg, "Answered")
        for other in ring.legs:
            if other is not leg:
                self._end(other, 9000, "Plivo")
        ring.on_answer(leg)

    def _hangup_by_party(self, call: SimCall) -> None:
        if call.state == "ringing" and call._ring:
            return self._ring_outcome(call, "decline")
        self._end(call, 4000, "Caller" if call.direction == "inbound" else "Callee")

    def _hangup_by_api(self, call: SimCall) -> None:
        self._end(call, 1000 if call.state == "ringing" else 4000, "API Request")

    def _end(self, call: SimCall, code: int, source: str) -> None:
        """The leg hangs up: its call-status and hangup callbacks, then its MPC exit, Dial bridge, ring
        outcome."""
        if call.state == "completed":
            return
        answered = call.state == "in_progress"
        call.hangup_cause_code, call._ended, call._source = code, time.time(), source
        self._set_state(call, "completed")
        for timer in call._timers:
            timer.cancel()
        self._call_status(call, "Hangup")
        if call._hangup[0]:
            name, switch, unanswered = CAUSES[code]
            p = {**self._std(call), "CallStatus": "completed" if answered else unanswered, "Event": "Hangup"}
            p.update(HangupCause=switch, HangupCauseName=name, HangupCauseCode=str(code), HangupSource=source)
            p.update(Duration=_duration(call), BillDuration=_duration(call), StartTime=_fmt(call._started))
            p.update(AnswerTime=_fmt(call._answered), EndTime=_fmt(call._ended))
            self._post(call._account, call._hangup[0], p, call._hangup[1])
        if call._member and not call._member.exit_cause:
            self._exit(call._member, EXIT_HANGUP)
        if call._dialing:
            self._drop_dial(call)
        elif call._dial_a and call._dial_a._bridge is call:
            self._dial_done(call._dial_a, call)
        ring = call._ring
        if ring and ring.winner is None and all(leg.state == "completed" for leg in ring.legs):
            ring.winner = call
            ring.on_fail(call)

    def _call_status(self, call: SimCall, event: str) -> None:
        """The Add Participant API's call_status_callback_url, for each leg it dials."""
        if not call._status or not call._status[0]:
            return
        url, method, role, name = call._status
        mpc = self._find_mpc(call._account, f"name_{name}")
        p = {"EventName": event, "CallUUID": call.call_uuid, "From": _plivo(call.from_), "To": _plivo(call.to)}
        p.update(Role=role, MPCName=name, MPCUUID=mpc.uuid, InitiationTime=_fmt(call._started))
        p.update(AnswerTime=_fmt(call._answered), **call.headers)
        if event == "Hangup":
            p.update(HangupTime=_fmt(call._ended), Duration=_duration(call), BilledDuration=_duration(call))
            code = call.hangup_cause_code
            p.update(PlivoHangupCause=CAUSES[code][1], PlivoHangupCauseCode=str(code), PlivoHangupSource=call._source)
        self._post(call._account, url, p, method)

    def _transfer(self, call: SimCall, url: str, method: str) -> None:
        if call.state == "completed":
            return
        if call._member and not call._member.exit_cause:
            self._exit(call._member, EXIT_TRANSFERRED, follow=False)
        self._drop_dial(call)
        call._resume = call._menu = None
        doc = self._fetch(call, url, method, code=7013)
        if doc is not None:
            self._execute(call, doc)

    def _press(self, call: SimCall, digits: str) -> None:
        m = call._member
        if m and not m.exit_cause:
            for digit in digits:
                self._event(m.mpc, "ParticipantDigitInput", m, DigitInput=digit)
            return
        call._digits += digits
        if call._menu and call.state != "completed":
            self._menu_resume(call)

    # ---- multi-party calls ---------------------------------------------------------------------------

    def _mpc_for(self, acc: _Account, name: str, opts: dict) -> _Mpc:
        """The live MPC by this name, else a new one with this participant's MPC-level settings."""
        live = (m for m in reversed(self._mpcs) if m.account is acc and m.name == name and m.status != "Ended")
        live = next(live, None)
        if live:
            return live
        groups = (opts.get("status_callback_events") or "mpc-state-changes,participant-state-changes").split(",")
        mpc = _Mpc(acc, name, opts, frozenset(e for g in groups for e in EVENT_GROUPS.get(g.strip(), ())))
        self._mpcs.append(mpc)
        self._event(mpc, "MPCInitialized")
        return mpc

    def _member(self, mpc: _Mpc, call: SimCall, opts: dict, api: bool) -> _Member:
        role = opts.get("role", "")
        address = call._dialled or _plivo(call.to if call.direction == "outbound" else call.from_)
        if self.member_address == "plus":
            address = _e164(_plivo(address))
        elif self.member_address == "bare":
            address = address.removeprefix("sip:")
        opts = {"coach_mode": role.lower() == "supervisor", **opts}
        return _Member(mpc, call, str(next(self._ids)), role, address, api, opts)

    def _event(self, mpc: _Mpc, name: str, m: _Member | None = None, **extra) -> None:
        mpc.seq += 1
        if not mpc.opts.get("status_callback_url") or name not in mpc.events:
            return
        p = {"EventName": name, "EventTimestamp": _fmt(time.time()), "MPCUUID": mpc.uuid, "MPCName": mpc.name}
        p.update(SequenceNumber=str(mpc.seq), MPCCreationTime=_fmt(mpc.created), MPCStartTime=_fmt(mpc.started))
        if mpc.ended:
            duration = str(int(mpc.ended - (mpc.started or mpc.created)))
            p.update(MPCEndTime=_fmt(mpc.ended), MPCDuration=duration, MPCBilledDuration=duration)
        if m:
            c = m.call
            p.update(MemberID=m.member_id, MemberAddress=m.address, ParticipantRole=m.role)
            p.update(ParticipantCallUUID=c.call_uuid, ParticipantCallDirection=c.direction)
            p.update(ParticipantCallFrom=_plivo(c.from_), ParticipantCallTo=_plivo(c.to))
            p.update(ParticipantJoinTime=_fmt(m.join_time), ParticipantExitTime=_fmt(m.exit_time))
            p.update(ParticipantExitCause=m.exit_cause, ParticipantCoachMode=str(m.flag("coach_mode")).lower())
            p.update(c.headers)
        p.update(extra)
        self._post(mpc.account, mpc.opts["status_callback_url"], p, mpc.opts.get("status_callback_method", "POST"))

    def _join(self, m: _Member) -> None:
        mpc, call = m.mpc, m.call
        m.join_time, call._member, call.mpc = time.time(), m, mpc.name
        mpc.members.append(m)
        self._notify()
        if m.flag("record_participant_track"):
            m.recording = self._new_recording(mpc.account, "participant", call.call_uuid, _recording_callback(m.opts))
        self._event(mpc, "ParticipantJoin", m)
        if mpc.status == "Initialized" and m.flag("start_mpc_on_enter", True):
            mpc.status, mpc.started = "Active", time.time()
            for x in mpc.members:
                if x.timer:
                    x.timer.cancel()
            self._event(mpc, "MPCStart")
            if _bool(mpc.opts.get("record")):
                mpc.recording = self._new_recording(mpc.account, "mpc", call.call_uuid, _recording_callback(mpc.opts))
        elif mpc.status == "Initialized" and float(m.opts.get("wait_time") or 0):
            m.timer = self._later(float(m.opts["wait_time"]), self._wait_timeout, m)

    def _wait_timeout(self, m: _Member) -> None:
        if not m.exit_cause and m.mpc.status == "Initialized":
            self._exit(m, EXIT_WAIT_TIMEOUT)

    def _mark_exit(self, m: _Member, cause: str) -> None:
        m.exit_cause, m.exit_time, m.call.mpc, m.call._playing = cause, time.time(), None, None
        if m.timer:
            m.timer.cancel()
        self._notify()

    def _exit(self, m: _Member, cause: str, follow: bool = True) -> None:
        """A member leaves. An endMpcOnExit member, the last member, or leaving one member alone (its
        stayAlone off) in an active MPC ends it."""
        if m.exit_cause:
            return
        mpc = m.mpc
        self._mark_exit(m, cause)
        live = [x for x in mpc.members if not x.exit_cause]
        alone = mpc.status == "Active" and len(live) == 1 and not live[0].flag("stay_alone")
        alone = alone and not m.flag("end_mpc_on_exit")
        ends = mpc.status != "Ended" and (m.flag("end_mpc_on_exit") or not live or alone)
        rest = live if ends else []
        for x in rest:
            self._mark_exit(x, EXIT_ALONE if alone else EXIT_ENDED)
        leaving = [(m, str(ends).lower())] + [(x, "false") for x in rest]
        self._finish(mpc, leaving, (END_ALONE if alone else END_EXIT) if ends else None, None if follow else m)

    def _stop_mpc(self, mpc: _Mpc) -> None:
        if mpc.status == "Ended":
            return
        live = [x for x in mpc.members if not x.exit_cause]
        for x in live:
            self._mark_exit(x, EXIT_ENDED)
        self._finish(mpc, [(x, "false") for x in live], END_API)

    def _finish(self, mpc: _Mpc, leaving: list, cause: tuple | None, skip: _Member | None = None) -> None:
        """Members left (and the MPC ended, with `cause`), in Plivo's order: exit actions and leg hangups,
        then ParticipantExit, then MPCEnd."""
        if cause:
            mpc.status, mpc.ended, mpc.cause = "Ended", time.time(), cause
        exits = [x for x, _ in leaving if x is not skip]
        for x in [] if self.exit_after_events else exits:
            self._after_exit(x)
        for x, source in leaving:
            self._event(mpc, "ParticipantExit", x, IsMPCTerminationSource=source)
            if x.recording:
                self._recording_done(x.recording, mpc, x)
        if cause:
            self._event(mpc, "MPCEnd", MPCTerminationCause=cause[0], MPCTerminationCauseCode=cause[1])
            if mpc.recording:
                self._stop_mpc_recording(mpc)
        if self.exit_after_events:
            self._out.join()
            for x in exits:
                self._after_exit(x)

    def _after_exit(self, m: _Member) -> None:
        """Where a member's call goes after it left the MPC: hung up (API legs), the onExitActionUrl
        document, or the rest of the document that joined it."""
        call = m.call
        if call.state == "completed":
            return None
        if m.api:
            return self._end(call, 4030 if m.exit_cause == EXIT_KICKED else 4020, "Plivo")
        doc, call._resume = call._resume, None
        if url := m.opts.get("on_exit_action_url"):
            extra = {"MPCUUID": m.mpc.uuid, "MPCFriendlyName": m.mpc.name, "MemberID": m.member_id}
            extra.update(ParticipantCallUUID=call.call_uuid, ParticipantRole=m.role, ParticipantExitCause=m.exit_cause)
            extra.update(ParticipantJoinTime=_fmt(m.join_time), ParticipantEndTime=_fmt(m.exit_time))
            doc = self._fetch(call, url, m.opts.get("on_exit_action_method", "POST"), extra)
        if doc is not None:
            self._execute(call, doc)

    def _add_participants(self, acc: _Account, name: str, p: dict, calls: list[dict], request_uuid: str) -> None:
        mpc = self._mpc_for(acc, name, p)
        headers = str(p.get("sip_headers") or "").split("<")
        status = (p.get("call_status_callback_url", ""), p.get("call_status_callback_method", "POST"), p["role"], name)
        legs = []
        for i, c in enumerate(calls):
            sip = _sip_headers(headers[min(i, len(headers) - 1)])
            legs.append(self._new_call(acc, "outbound", c["from"], c["to"], call_uuid=c["call_uuid"], headers=sip))
            legs[-1]._status, legs[-1]._dialled = status, c["to"]
        api = {"AddParticipantAPIRequestId": request_uuid, "AddParticipantAPIRequestType": "Dial", **legs[0].headers}
        uuids = ",".join(c["call_uuid"] for c in calls)
        dialled = {"AddParticipantAPICallUUIDs": uuids, "AddParticipantAPICallCount": str(len(calls))}
        who = {"ParticipantRole": p["role"], "MemberAddress": str(p["to"])}
        self._event(mpc, "AddParticipantByAPIActionInitiated", **api, **dialled, **who)

        def answered(leg: SimCall) -> None:
            # Legs ring on after an MPCEnd (HS cancels them); one answering late starts a fresh MPC by that name.
            live = self._mpc_for(acc, name, p)
            self._join(self._member(live, leg, p, api=True))
            outcome = {"AddParticipantAPIRequestOutcome": "successful", "AddParticipantAPICallUUIDs": leg.call_uuid}
            outcome["AddParticipantAPICallCount"] = "1"
            self._event(live, "AddParticipantByAPIActionCompleted", leg._member, **api, **outcome)

        def failed(_leg: SimCall) -> None:
            outcome = {"AddParticipantAPIRequestOutcome": "unsuccessful", "AddParticipantAPIRequestOutcomeReason": ""}
            latest = self._find_mpc(acc, f"name_{name}")
            self._event(latest, "AddParticipantByAPIActionCompleted", **api, **dialled, **who, **outcome)

        self._dial(legs, answered, failed, int(p.get("ring_timeout") or 45))

    def _move_in(self, call: SimCall, name: str, p: dict, request_uuid: str) -> None:
        """Add Participant with a call_uuid: the live leg leaves where it is and joins this MPC."""
        if call.state != "in_progress":
            return
        mpc = self._mpc_for(call._account, name, p)
        api = {"AddParticipantAPIRequestId": request_uuid, "AddParticipantAPICallUUIDs": call.call_uuid}
        api["AddParticipantAPICallCount"] = "1"
        self._event(mpc, "AddParticipantByAPIActionInitiated", **api, ParticipantRole=p["role"])
        if call._member and not call._member.exit_cause:
            self._exit(call._member, EXIT_TRANSFERRED, follow=False)
        self._drop_dial(call)
        call._resume = call._menu = None
        self._join(self._member(mpc, call, p, api=True))
        api["AddParticipantAPIRequestOutcome"] = "successful"
        self._event(mpc, "AddParticipantByAPIActionCompleted", call._member, **api)

    def _update_member(self, m: _Member, p: dict) -> None:
        for key, on, off in MEMBER_TOGGLES:
            if key in p and not m.exit_cause and _bool(p[key]) != m.flag(key):
                m.opts = {**m.opts, key: _bool(p[key])}
                self._event(m.mpc, on if m.flag(key) else off, m)

    def _media(self, members: list[_Member], kind: str, target: str, what: str) -> None:
        for m in members:
            if not m.exit_cause:
                m.call.said.append(what)
                m.call._playing = (kind, target)

    def _media_done(self, call: SimCall) -> None:
        playing, call._playing, m = call._playing, None, call._member
        if playing and m and not m.exit_cause:
            name = "ParticipantSpeakStopped" if playing[0] == "Speak" else "ParticipantAudioStopped"
            self._event(m.mpc, name, m, MemberID=playing[1])

    # ---- recordings ----------------------------------------------------------------------------------

    def _new_recording(self, acc: _Account, kind: str, call_uuid: str, callback: tuple) -> _Recording:
        rid = str(uuid.uuid4())
        url = f"{self.media_url}/v1/Account/{acc.auth_id}/Recording/{rid}.mp3"
        self.recordings[rid] = rec = _Recording(acc, rid, url, kind, call_uuid, callback)
        return rec

    def _seconds(self, rec: _Recording) -> int:
        """The recording's length in sim seconds: real time over `time_factor`, at least 1 (0 with short_takes)."""
        return 0 if self.short_takes else max(1, round((rec.end - rec.start) / self.time_factor))

    def _segments(self, seconds: int) -> list[dict]:
        return [{"start": 0.0, "end": float(seconds), "text": self.transcript}]

    def _attach_recording(self, mpc: _Mpc, rec: _Recording) -> None:
        mpc.recording = rec
        if mpc.status == "Ended":
            self._stop_mpc_recording(mpc)

    def _stop_mpc_recording(self, mpc: _Mpc) -> None:
        rec, mpc.recording = mpc.recording, None
        self._recording_done(rec, mpc)

    def _recording_event(self, mpc: _Mpc, name: str) -> None:
        rec = mpc.recording
        if rec:
            p = {"EventName": name, "MPCUUID": mpc.uuid, "MPCName": mpc.name, "RecordingUUID": rec.id}
            self._post(rec.account, rec.callback[0], p, rec.callback[1])

    def _recording_done(self, rec: _Recording, mpc: _Mpc, member: _Member | None = None) -> None:
        """MPCRecordingCompleted (or ParticipantRecordingCompleted for one leg's track), then its transcript."""
        rec.end = time.time()
        seconds, (url, method, transcription) = self._seconds(rec), rec.callback
        event = "ParticipantRecordingCompleted" if member else "MPCRecordingCompleted"
        p = {"EventName": event, "MPCUUID": mpc.uuid, "MPCName": mpc.name, "RecordingUUID": rec.id}
        p.update(RecordingURL=rec.url, RecordingResourceURL=f"/v1/Account/{rec.account.auth_id}/Recording/{rec.id}/")
        p.update(RecordingDuration=str(seconds), RecordingDurationMs=str(seconds * 1000), RecordingFormat="mp3")
        p.update(RecordingStartTime=_fmt(rec.start), RecordingEndTime=_fmt(rec.start + seconds))
        if member:
            p.update(MemberID=member.member_id, ParticipantCallUUID=member.call.call_uuid, RecordTrackType=rec.track)
        self._post(rec.account, url, p, method)
        text = {
            "transcription": json.dumps(self._segments(seconds)),
            "recording_id": rec.id,
            "call_uuid": rec.call_uuid,
        }
        self._post(rec.account, transcription, text)
