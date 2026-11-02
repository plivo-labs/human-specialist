"""Shared builders. Request shapes the design leaves open are fixed here, once, for the whole suite."""

import hashlib
import hmac
import random
import re
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import unquote, urlencode
from xml.sax.saxutils import escape
from zoneinfo import ZoneInfo

from tests.harness.fixtures import *

DAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
# The default hold music: Plivo's own sample track from its MultiPartyCall docs.
MUSIC = "https://s3.amazonaws.com/plivocloud/music.mp3"


def ok(r) -> dict:
    assert r.is_success, f"{r.status_code} {r.text}"
    return r.json() if r.content else {}


def error(r, status, code: str | None = None) -> dict:
    """The response is the shared error envelope with this status (an int or a tuple of accepted ones)."""
    assert r.status_code in (status if isinstance(status, tuple) else (status,)), f"{r.status_code} {r.text}"
    body = r.json()
    assert body["request_id"] and body["error"]["code"] and "message" in body["error"], body
    if code:
        assert body["error"]["code"] == code, body
    return body["error"]


def refused(r) -> dict:
    """Refused with some 4xx the design leaves open."""
    return error(r, tuple(range(400, 500)))


def say(text: str) -> dict:
    return {"text": text}


def hangup_action(greeting: str = "Goodbye") -> dict:
    return {"type": "hangup", "greeting": say(greeting)}


def queue_action(queue: dict, **extra) -> dict:
    return {"type": "queue", "queue_id": queue["id"], **extra}


def voicemail_action(queue: dict, greeting: str = "Leave a message", **extra) -> dict:
    return {
        "type": "voicemail",
        "greeting": say(greeting),
        "max_length_sec": 60,
        "transcribe": False,
        "mailbox": {"queue_id": queue["id"]},
        **extra,
    }


def settings(tenant, **body) -> dict:
    return ok(tenant.api.patch("/api/v1/settings", json=body))


def team(tenant, name: str = "Team") -> str:
    return ok(tenant.api.post("/api/v1/teams", json={"name": name}))["id"]


def skill(tenant, name: str = "Spanish") -> str:
    return ok(tenant.api.post("/api/v1/skills", json={"name": name}))["id"]


def disposition(tenant, name: str = "Done", **fields) -> dict:
    return ok(tenant.api.post("/api/v1/dispositions", json={"name": name, **fields}))


def dispositions(client) -> list[dict]:
    return ok(client.get("/api/v1/dispositions", params={"limit": 200}))["data"]


def disposition_named(tenant, name: str) -> dict:
    return next(d for d in dispositions(tenant.api) if d["name"] == name)


def noon_zone() -> str:
    """An Etc/GMT zone where the local time is between 12:00 and 13:00 now, so the slots below are
    far from their edges for the length of a test."""
    offset = 12 - datetime.now(UTC).hour
    return f"Etc/GMT{-offset:+d}"


def hours(tenant, *, weekly: bool = True, holiday: list | None = None) -> dict:
    """Business hours open every day (or never), plus an optional holiday today with these open slots."""
    zone = noon_zone()
    body = {
        "name": "Hours",
        "time_zone": zone,
        "slots": [{"day": d, "start": "00:00", "end": "23:59"} for d in DAYS] if weekly else [],
        "holidays": []
        if holiday is None
        else [{"date": datetime.now(ZoneInfo(zone)).date().isoformat(), "slots": holiday}],
    }
    return ok(tenant.api.post("/api/v1/business-hours", json=body))


def menu(tenant, options: dict, greeting: str = "Press a key", **fields) -> dict:
    body = {"name": "Menu", "greeting": say(greeting), "options": options, "input_timeout_sec": 1, "retries": 3}
    return ok(tenant.api.post("/api/v1/menus", json={**body, **fields}))


def menu_line(tenant, options: dict, **fields) -> tuple[str, dict]:
    """A number whose open action is a new menu."""
    m = menu(tenant, options, **fields)
    number = tenant.number()
    tenant.route_number(number, {"type": "menu", "menu_id": m["id"]})
    return number, m


def with_query(url: str, **params) -> str:
    return url + ("&" if "?" in url else "?") + urlencode(params)


def redirecting_line(tenant, target_url: str) -> str:
    """A number whose open action redirects to a tenant app that redirects on to `target_url`."""
    app = tenant.world.plivo.app(f"<Response><Redirect>{escape(target_url)}</Redirect></Response>")
    number = tenant.number()
    tenant.route_number(number, {"type": "redirect", "url": app})
    return number


def unavailable(tenant, name: str = "Away"):
    """A logged-in agent whose status is Away, so a queue holding only them is never answered."""
    agent = tenant.agent(name).login()
    agent.set_status("Away")
    return agent


def registered_number(tenant) -> str:
    """A number registered with HS (outbound-only: its open action hangs up), usable as `from`."""
    number = tenant.number()
    tenant.route_number(number, {"type": "hangup"})
    return number


def app_call(tenant, to: str, answer_url: str, **params) -> None:
    """The tenant's own app places a call from one of its numbers through Plivo's REST API."""
    url = f"{tenant.world.plivo.url}/v1/Account/{tenant.auth_id}/Call/"
    body = {"from": registered_number(tenant), "to": to, "answer_url": answer_url, **params}
    assert tenant.world.anon().post(url, auth=(tenant.auth_id, tenant.auth_token), data=body).status_code == 201


def lead(world, behaviour: str = "answer"):
    """A PSTN number that behaves as told when HS dials it. Returns (number, its SimPhone)."""
    number = f"+1555{random.randint(1_000_000, 9_999_999)}"
    return number, world.plivo.lead(number, behaviour)


def dial(tenant, agent=None, to: str | None = None, **body):
    """POST /calls with the tenant key. Returns (agent, conversation_id)."""
    agent = agent or tenant.agent().login()
    to = to or lead(tenant.world)[0]
    payload = {"agent_id": agent.id, "to": to, "from": registered_number(tenant), **body}
    r = tenant.api.post("/api/v1/calls", json=payload)
    assert r.status_code == 201, r.text
    return agent, r.json()["conversation_id"]


@dataclass
class Live:
    """An inbound call in progress: the caller's leg and the answering agent's leg."""

    caller: object
    leg: object
    agent: object
    queue: dict
    number: str
    stream: object
    id: str


def hs_header(leg, name: str) -> str:
    """An Hs* SIP header as HS sent it: Plivo delivers it as X-PH-<name>, URL-encoded by HS."""
    return unquote(leg.headers[f"X-PH-{name}"])


def connect(tenant, agent=None, *, also=(), **queue_fields) -> Live:
    """A new inbound call answered by `agent` (default: a new logged-in agent) on a new queue that also
    holds the agents in `also`."""
    agent = agent or tenant.agent().login()
    stream = tenant.stream(after=0)
    number, queue = tenant.inbound_line(agent, *also, **queue_fields)
    agent.phone.on_ring("answer")
    caller = tenant.world.plivo.call(number)
    leg = agent.phone.wait_ring()
    cid = hs_header(leg, "HsConversation")
    stream.wait_for("offer.accepted", conversation_id=cid)
    return Live(caller, leg, agent, queue, number, stream, cid)


def wrap_up(agent, cid: str, disposition_id: str, **body):
    return agent.api().post(f"/api/v1/conversations/{cid}/wrap-up", json={"disposition_id": disposition_id, **body})


def finish(live: Live, disposition_id: str | None = None) -> dict:
    """The caller hangs up and the agent submits a disposition. Returns `conversation.closed`."""
    live.caller.hangup()
    live.stream.wait_for("conversation.wrap_up_started", conversation_id=live.id)
    did = disposition_id or disposition(live.agent.tenant, f"Done {uuid.uuid4().hex[:6]}")["id"]
    ok(wrap_up(live.agent, live.id, did))
    return live.stream.wait_for("conversation.closed", conversation_id=live.id)


def seen(stream, type_: str, **match) -> list[dict]:
    """Events of `type_` on the stream so far whose top-level or `data` fields equal `match`."""
    return [
        e
        for e in stream.events
        if e["type"] == type_ and all(e.get(k, e.get("data", {}).get(k)) == v for k, v in match.items())
    ]


def types(events: list[dict]) -> list[str]:
    return [e["type"] for e in events]


def in_order(seq: list, *wanted) -> bool:
    """`wanted` appears in `seq` as a subsequence."""
    it = iter(seq)
    return all(w in it for w in wanted)


def plivo_post(world, tenant_id: str, path: str, params: dict, auth_token: str | None, **headers):
    """POST to /plivo/{tenant_id}/{path} as Plivo would, signed with `auth_token` (None: unsigned)."""
    url = f"{world.base_url}/plivo/{tenant_id}/{path}"
    signed = world.plivo.sign(url, params, auth_token) if auth_token else {}
    return world.anon().post(url, data=params, headers={**signed, **headers})


def enqueue_as_plivo(world, tenant, url: str, token: str | None = None):
    """A Plivo-form request to an enqueue URL for a call HS has not seen, signed with `token` (the tenant's)."""
    params = answer_params(tenant.number())
    return world.anon().post(url, data=params, headers=world.plivo.sign(url, params, token or tenant.auth_token))


def answer_params(to: str, from_: str = "+14155550111", **extra) -> dict:
    """An answer callback as Plivo sends it: PSTN numbers carry no leading "+"."""
    return {
        "CallUUID": str(uuid.uuid4()),
        "From": from_.removeprefix("+"),
        "To": to.removeprefix("+"),
        "Direction": "inbound",
        "CallStatus": "ringing",
        **extra,
    }


def header(delivery, name: str) -> str:
    return {k.lower(): v for k, v in delivery.headers.items()}[name.lower()]


def signatures(delivery) -> list[str]:
    return re.findall(r"v1=([0-9a-f]{64})", header(delivery, "X-HS-Signature"))


def signed_with(delivery, secret: str) -> bool:
    msg = header(delivery, "X-HS-Timestamp").encode() + b"." + delivery.body
    return hmac.new(secret.encode(), msg, hashlib.sha256).hexdigest() in signatures(delivery)


def webhook(tenant, url: str, **fields) -> dict:
    return ok(tenant.api.post("/api/v1/webhooks", json={"url": url, **fields}))


def sse_frames(lines: list[str]) -> list[dict]:
    """Complete SSE frames (field -> value); comment lines are dropped."""
    frames, cur = [], {}
    for line in lines:
        if not line:
            if cur:
                frames.append(cur)
            cur = {}
        elif not line.startswith(":"):
            field, _, value = line.partition(":")
            cur[field] = value.removeprefix(" ")
    return frames


def read_stream(client, until, params=None, headers=None, timeout: float = 5.0) -> list[str]:
    """Raw lines of GET /events/stream until `until(lines)` is true."""
    lines: list[str] = []
    with client.stream("GET", "/api/v1/events/stream", params=params, headers=headers, timeout=timeout) as r:
        assert r.status_code == 200, r.read()
        for line in r.iter_lines():
            lines.append(line)
            if until(lines):
                return lines
    raise AssertionError(f"stream ended early: {lines}")


def data_frames(n: int):
    return lambda lines: len([f for f in sse_frames(lines) if "data" in f]) >= n


def set_status(tenant, agent, name: str):
    return tenant.api.put(f"/api/v1/agents/{agent.id}/status", json={"status_id": tenant.status_id(name)})


def status_changes(tenant, agent, *names: str) -> list[dict]:
    """Set the agent's status (tenant key) to each name in turn; return the events those changes emitted."""

    def mine():
        return [e for e in tenant.events(type="agent.status_changed") if e["agent_id"] == agent.id]

    base = len(mine())
    for name in names:
        ok(set_status(tenant, agent, name))

    def emitted():
        evs = mine()
        return evs[base:] if len(evs) >= base + len(names) else None

    return tenant.world.wait(emitted, what="agent.status_changed events")


def run_jobs_until(world, find, what: str = "", timeout: float = 10.0):
    """Run due jobs until `find()` is truthy: a callback may schedule its job just after the last pass."""
    return world.wait(lambda: (world.run_jobs(), find())[1], timeout=timeout, what=what)


def hang_up_with_the_room_alive(world, tenant, live) -> None:
    """The agent hangs up and their leave asks Plivo to end the room, but HS has not yet heard the room end."""
    room = live.caller.mpc
    world.plivo.fault("delay", lambda r: r.params.get("EventName") == "MPCEnd", account=tenant.auth_id)
    ok(live.agent.api().post(f"/api/v1/calls/{live.id}/hangup"))
    stop = f"/name_{room}/"
    world.wait(
        lambda: any(r.method == "DELETE" and r.path.endswith(stop) for r in world.plivo.requests),
        what="the agent's leave to ask Plivo to end the room",
    )


def fence(tenant) -> None:
    """Emit an event and wait until it is replayable. Events are stamped in commit order, so after this
    every event committed earlier is visible too, and "no such event" checks are deterministic."""
    status_changes(tenant, tenant.agent("Fence"), "Away")


def activity(tenant, agent) -> str:
    """What the floor shows the agent doing: idle, ringing, on_call or wrap_up."""
    return next(a for a in ok(tenant.api.get("/api/v1/floor"))["agents"] if a["agent_id"] == agent.id)["activity"]


def fresh_second(world) -> None:
    """Wait for a new wall-clock second. Plivo stamps a leg's HangupTime to the second, so two legs that end
    soon after this end inside the same one."""
    world.wait(lambda: time.time() % 1 < 0.05, what="a new second")
