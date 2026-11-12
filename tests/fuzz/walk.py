"""A seeded random walk: callers, agents, a supervisor and the clock act on one tenant at random, and after
every step HS must still be somewhere possible.

`make fuzz` walks SEEDS; `HS_FUZZ_SEED=8,1 make fuzz` walks those; `HS_FUZZ_CHAOS=1` delivers every status callback
twice, as a Plivo retry would. A failure names its seed, prints every step's
action with its inputs, and writes that log and the tenant's events to files it names. Every choice comes from the
seeded RNG, but HS and the simulator run on real threads, so a replay makes the same choices only while HS answers
the same way.
"""

import json
import math
import os
import random
import tempfile
import time
from collections import Counter
from collections.abc import Callable
from itertools import pairwise
from urllib.parse import unquote

import httpx
import pytest

from tests.acceptance.conftest import disposition, fence, ok, registered_number, settings, team
from tests.harness import Agent, SimCall, SimPhone, World

SEEDS = [int(n) for n in os.environ.get("HS_FUZZ_SEED", "7,10,14,15,17").split(",")]
CHAOS = os.environ.get("HS_FUZZ_CHAOS") == "1"
STEPS = 50
STRATEGIES = ("availability", "simultaneous", "fewest_handled", "least_handle_time")
CALLERS = ("+14155550111", "+14155550122", "+14155550133")
RINGS = (("answer", 0.0), ("answer", 0.0), ("answer", 0.15), ("decline", 0.0), ("ignore", 0.0))
VOICE = ("offer", "transfer", "outbound")  # agent legs that hold the agent's one call; "monitor" does not
OUTCOMES = ("offer.accepted", "offer.missed", "offer.rejected", "offer.withdrawn")


class Walk:
    """One tenant's floor: three agents on two queues, a supervisor over them, and what the walk started."""

    def __init__(self, world: World, seed: int):
        self.rng, self.world, self.tenant = random.Random(seed), world, world.new_tenant(f"Fuzz {seed}")
        rng, tenant = self.rng, self.tenant
        wrap = (
            {"mode": "auto", "seconds": rng.randint(1, 5)} if rng.random() < 0.5 else {"mode": "manual", "seconds": 30}
        )
        recording = {"inbound": rng.random() < 0.5, "outbound": rng.random() < 0.5}
        settings(tenant, ring_timeout_sec=5, rona={"strikes": rng.randint(1, 3)}, wrap_up=wrap, recording=recording)
        floor = team(tenant, "Floor")
        self.agents = [tenant.agent(f"A{i}", teams=[floor]).login() for i in range(3)]
        self.sup = tenant.agent("S", role="supervisor", teams=[floor]).login()
        self.queues, self.lines = [], []
        for members in (self.agents, self.agents[1:]):
            strategy, wait = rng.choice(STRATEGIES), rng.choice((3, 20))
            self.queues.append(tenant.queue(agents=[a.id for a in members], strategy=strategy, wait_timeout_sec=wait))
            self.lines.append(tenant.number())
            tenant.route_number(self.lines[-1], {"type": "queue", "queue_id": self.queues[-1]["id"]})
        self.caller_id, self.done = registered_number(tenant), disposition(tenant, "Done")["id"]
        self.status = {n: tenant.status_id(n) for n in ("Available", "Break", "Offline")}
        self.stream, self.online = tenant.stream(after=0), {a.id for a in [*self.agents, self.sup]}
        self.callers: list[SimCall] = []
        self.leads: list[SimPhone] = []
        self.warm: list[tuple[Agent, str, str]] = []  # (agent, conversation, transfer) not yet completed or cancelled
        self.submitted: set[str] = set()
        self.beats: dict[str, float] = {}
        self.heard, self.broke = len(world.plivo.callbacks), len(world.plivo.errors)
        self.names = {a.id: f"A{i}" for i, a in enumerate(self.agents)} | {self.sup.id: "S"}
        shapes = [(q["strategy"], q["wait_timeout_sec"]) for q in self.queues]
        self.log = [f"seed {seed}: wrap-up {wrap}, recording {recording}, queues {shapes}, chaos {CHAOS}"]
        if CHAOS:
            world.plivo.fault("duplicate", lambda r: True, account=tenant.auth_id, times=math.inf)

    def name(self, agent_id: str) -> str:
        return self.names.get(agent_id, agent_id)

    def call(self, client: httpx.Client, method: str, path: str, body: dict | None = None) -> httpx.Response:
        r = client.request(method, path, json=body)
        self.log.append(f"    {method} {path} {body or ''} -> {r.status_code}")
        assert r.status_code < 500, f"{method} {path} -> {r.status_code}: {r.text}"
        return r

    def desk(self, agent: Agent) -> httpx.Client:
        """The agent's own token while logged in; the tenant key acts for them once they log out."""
        return agent.api() if agent.id in self.online else self.tenant.api


def kind(leg: SimCall) -> str:
    return unquote(leg.headers.get("X-PH-HsKind", ""))


def conversation(leg: SimCall) -> str:
    return unquote(leg.headers.get("X-PH-HsConversation", ""))


def live(phone: SimPhone) -> list[SimCall]:
    return [leg for leg in phone.legs if leg.state != "completed"]


def on_calls(w: Walk) -> list[tuple[Agent, SimCall]]:
    return [(a, leg) for a in w.agents for leg in live(a.phone) if leg.state == "in_progress" and kind(leg) in VOICE]


def customers(w: Walk) -> list[SimCall]:
    return [c for c in w.callers if c.state != "completed"] + [c for p in w.leads for c in live(p)]


def seen(w: Walk, type_: str) -> list[dict]:
    return [e for e in w.stream.events if e.get("type") == type_]


def pending_wrap_ups(w: Walk) -> list[tuple[str, str, str]]:
    """A conversation can return to the agent who wrapped it up (a transfer to their own queue) and needs a second
    wrap-up, so each start event is tracked, not each (conversation, agent)."""
    last: dict[tuple[str, str], str | None] = {}
    for e in w.stream.events:
        if (t := e.get("type")) in ("conversation.wrap_up_started", "conversation.wrapped_up"):
            last[e["conversation_id"], e["agent_id"]] = e["id"] if t == "conversation.wrap_up_started" else None
    return [(*p, s) for p, s in last.items() if s and s not in w.submitted]


def pick(w: Walk, items: list):
    return w.rng.choice(items) if items else None


# Moves: each acts and returns its log line, or returns None (drawing nothing from the RNG) when nothing fits.


def caller_dials(w: Walk) -> str:
    number, caller = w.rng.choice(w.lines), w.rng.choice(CALLERS)
    w.callers.append(w.world.plivo.call(number, from_=caller))
    return f"{caller} dials Q{w.lines.index(number)} ({number})"


def agent_picks_ring_behaviour(w: Walk) -> str:
    agent, (how, after) = w.rng.choice(w.agents), w.rng.choice(RINGS)
    agent.phone.on_ring(how, after)
    return f"{w.name(agent.id)} will {how} the next ring" + (f" after {after}s" if after else "")


def presence(w: Walk) -> str:
    agent, to = w.rng.choice(w.agents), w.rng.choice(("Available", "Break", "Offline"))
    if to == "Offline" and agent.id not in w.online:
        return f"{w.name(agent.id)} stays logged out"
    if agent.id not in w.online:
        agent.login()
        w.online.add(agent.id)
    w.call(agent.api(), "PUT", "/api/v1/me/status", {"status_id": w.status[to]})
    if to == "Offline":
        w.online.discard(agent.id)
        w.call(w.tenant.api, "DELETE", f"/api/v1/agents/{agent.id}/sessions")
        return f"{w.name(agent.id)} logs out"
    return f"{w.name(agent.id)} goes {to}"


def agent_declines(w: Walk) -> str | None:
    ringing = [(a, leg) for a in w.agents for leg in live(a.phone) if leg.state == "ringing"]
    if not (found := pick(w, ringing)):
        return None
    agent, leg = found
    leg.hangup()
    return f"{w.name(agent.id)} declines a ringing {kind(leg)} leg of {conversation(leg)}"


def hold(w: Walk) -> str | None:
    if not (found := pick(w, on_calls(w))):
        return None
    agent, leg = found
    on = w.rng.random() < 0.6
    w.call(w.desk(agent), "POST", f"/api/v1/calls/{conversation(leg)}/hold", {"on": on})
    return f"{w.name(agent.id)} {'holds' if on else 'resumes'} {conversation(leg)}"


def transfer(w: Walk) -> str | None:
    if not (found := pick(w, on_calls(w))):
        return None
    agent, leg = found
    cid, mode = conversation(leg), w.rng.choice(("cold", "warm"))
    if w.rng.random() < 0.6:
        to = w.rng.choice([a for a in w.agents if a is not agent])
        target, named = {"type": "agent", "agent_id": to.id}, w.name(to.id)
    else:
        queue = w.rng.choice(w.queues)
        target, named = {"type": "queue", "queue_id": queue["id"]}, f"Q{w.queues.index(queue)}"
    r = w.call(w.desk(agent), "POST", f"/api/v1/calls/{cid}/transfers", {"mode": mode, "target": target})
    if mode == "warm" and r.is_success:
        w.warm.append((agent, cid, r.json()["transfer_id"]))
    return f"{w.name(agent.id)} transfers {cid} ({mode}) to {named}"


def finish_warm_transfer(w: Walk) -> str | None:
    if not w.warm:
        return None
    agent, cid, tid = w.warm.pop(w.rng.randrange(len(w.warm)))
    how = w.rng.choice(("complete", "cancel"))
    w.call(w.desk(agent), "POST", f"/api/v1/calls/{cid}/transfers/{tid}/{how}")
    return f"{w.name(agent.id)} {how}s warm transfer {tid} of {cid}"


def supervisor(w: Walk) -> str | None:
    if listening := live(w.sup.phone):
        w.call(w.sup.api(), "DELETE", f"/api/v1/calls/{conversation(listening[0])}/monitor")
        return f"the supervisor leaves {conversation(listening[0])}"
    if not (found := pick(w, on_calls(w))):
        return None
    agent, leg = found
    w.call(w.sup.api(), "POST", f"/api/v1/calls/{conversation(leg)}/monitor", {"mode": "listen"})
    return f"the supervisor listens to {w.name(agent.id)} on {conversation(leg)}"


def customer_hangs_up(w: Walk) -> str | None:
    if not (leg := pick(w, customers(w))):
        return None
    leg.hangup()
    return f"the customer on {leg.to if leg.direction == 'outbound' else leg.from_} hangs up ({leg.state})"


def agent_hangs_up(w: Walk) -> str | None:
    if not (found := pick(w, on_calls(w))):
        return None
    agent, leg = found
    if w.rng.random() < 0.5:
        leg.hangup()
        return f"{w.name(agent.id)} hangs up the softphone on {conversation(leg)}"
    w.call(w.desk(agent), "POST", f"/api/v1/calls/{conversation(leg)}/hangup")
    return f"{w.name(agent.id)} ends {conversation(leg)} from the desk"


def wrap_up(w: Walk) -> str | None:
    if not (found := pick(w, pending_wrap_ups(w))):
        return None
    submit(w, *found)
    return f"{w.name(found[1])} wraps up {found[0]}"


def submit(w: Walk, cid: str, agent_id: str, started: str) -> None:
    w.submitted.add(started)
    agent = next(a for a in [*w.agents, w.sup] if a.id == agent_id)
    body = {"disposition_id": w.done} | ({} if agent_id in w.online else {"agent_id": agent_id})
    w.call(w.desk(agent), "POST", f"/api/v1/conversations/{cid}/wrap-up", body)


def agent_dials_out(w: Walk) -> str:
    agent, number = w.rng.choice(w.agents), f"+1555{w.rng.randint(1_000_000, 9_999_999)}"
    behaviour = w.rng.choice(("answer", "answer", "busy", "no_answer"))
    w.leads.append(w.world.plivo.lead(number, behaviour))
    w.call(w.tenant.api, "POST", "/api/v1/calls", {"agent_id": agent.id, "to": number, "from": w.caller_id})
    return f"{w.name(agent.id)} dials {number} ({behaviour})"


def time_passes(w: Walk) -> str:
    seconds = w.rng.choice((5, 30))
    return f"{seconds}s pass: {w.world.advance(seconds)} timer(s) fire"


MOVES: list[Callable[[Walk], str | None]] = [
    *[caller_dials] * 4,
    *[agent_picks_ring_behaviour] * 2,
    *[presence] * 2,
    agent_declines,
    *[hold] * 2,
    *[transfer] * 2,
    finish_warm_transfer,
    *[supervisor] * 2,
    *[customer_hangs_up] * 3,
    *[agent_hangs_up] * 2,
    *[wrap_up] * 3,
    *[agent_dials_out] * 2,
    time_passes,
]


def eventually(w: Walk, problems: Callable[[Walk], list[str]], timeout: float = 8.0) -> None:
    """Wait for `problems` to come back empty; fail with the last ones seen."""
    last: list[str] = []

    def clear() -> bool:
        last[:] = problems(w)
        return not last

    try:
        w.world.wait(clear, timeout)
    except AssertionError:
        raise AssertionError("; ".join(last)) from None


def increasing(ids: list[int]) -> bool:
    return all(a < b for a, b in pairwise(ids))


def busy_unassigned(w: Walk) -> list[str]:
    """An agent ringing on the floor holds an open offer, whose leg may still be on its way, or a live leg (a
    warm-transfer consult and a listening supervisor read as ringing); one on a call has a live leg."""
    ended = {(e["data"]["offer_id"], e["agent_id"]) for e in w.stream.events if e.get("type") in OUTCOMES}
    offered = {e["agent_id"] for e in seen(w, "offer.created") if (e["data"]["offer_id"], e["agent_id"]) not in ended}
    ours, problems = {a.id: a for a in [*w.agents, w.sup]}, []
    for r in ok(w.tenant.api.get("/api/v1/floor"))["agents"]:
        if (agent := ours.get(r["agent_id"])) is None or live(agent.phone):
            continue
        if r["activity"] == "ringing" and agent.id not in offered:
            problems.append(f"{w.name(agent.id)} is ringing on the floor with no open offer and no live leg")
        elif r["activity"] == "on_call":
            problems.append(f"{w.name(agent.id)} is on_call on the floor with no live leg")
    return problems


def check_step(w: Walk) -> None:
    sim = w.world.plivo
    w.world.wait(sim.idle, timeout=10, what="the Plivo simulator to go idle")
    problems = [
        f"HS answered {c.url} with {c.status}: {c.body[:300]}" for c in sim.callbacks[w.heard :] if c.status >= 500
    ]
    problems += [f"simulator error: {e!r}" for e in sim.errors[w.broke :]]
    w.heard, w.broke = len(sim.callbacks), len(sim.errors)
    for a in w.agents:
        if len(legs := [leg for leg in live(a.phone) if kind(leg) in VOICE]) > 1:
            problems.append(f"{w.name(a.id)} holds {len(legs)} voice legs: {[(kind(g), g.state) for g in legs]}")
    if not increasing([int(e["id"]) for e in w.stream.events if "id" in e]):
        problems.append("event ids on the live stream went backwards")
    assert not problems, "; ".join(problems)
    eventually(w, busy_unassigned)


def heartbeats(w: Walk) -> None:
    """A desk heartbeats every few seconds; HS marks an agent Offline after 40 s of silence."""
    for agent in [*w.agents, w.sup]:
        if agent.id in w.online and time.monotonic() - w.beats.get(agent.id, 0) > 10:
            w.beats[agent.id] = time.monotonic()
            agent.heartbeat()


def still_active(w: Walk) -> list[str]:
    active = ok(w.tenant.api.get("/api/v1/conversations", params={"state": "active", "limit": 200}))["data"]
    return [f"conversation {c['id']} is still active after its legs ended" for c in active]


def unclosed(w: Walk) -> list[str]:
    closed = {e["conversation_id"] for e in seen(w, "conversation.closed")}
    return [
        f"conversation {e['conversation_id']} never closed after its call ended"
        for e in seen(w, "conversation.started")
        if e["conversation_id"] not in closed
    ]


def not_idle(w: Walk) -> list[str]:
    rows = ok(w.tenant.api.get("/api/v1/floor"))["agents"]
    return [f"{w.name(r['agent_id'])} is still {r['activity']}" for r in rows if r["activity"] != "idle"]


def offer_problems(events: list[dict]) -> list[str]:
    """Each agent's share of an offer (one offer id rings several agents) ends once, and no agent holds two
    offers at once."""
    problems, holding = [], {}
    outcomes = Counter((e["data"]["offer_id"], e["agent_id"]) for e in events if e["type"] in OUTCOMES)
    for e in events:
        offer, agent = e["data"].get("offer_id"), e["agent_id"]
        if e["type"] == "offer.created":
            if (n := outcomes[offer, agent]) != 1:
                problems.append(f"offer {offer} to agent {agent} in {e['conversation_id']} ended {n} times")
            if (held := holding.get(agent)) and outcomes[held, agent]:  # one that never ends is reported above
                problems.append(f"agent {agent} got offer {offer} while holding {held}")
            holding[agent] = offer
        elif e["type"] in OUTCOMES and holding.get(agent) == offer:
            del holding[agent]
    return problems


def caught_up(w: Walk) -> list[dict]:
    """GET /events once everything committed so far is replayable and the live stream has it too."""
    fence(w.tenant)
    events = w.tenant.events()
    last = events[-1]["id"]
    w.world.wait(lambda: any(e.get("id") == last for e in w.stream.events), what="the live stream to catch up")
    return events


def finish(w: Walk) -> None:
    """The floor winds down as people would: every leg hangs up, then each agent wraps up once. Each check only
    observes and no clock is advanced, so neither the walk nor a forced timer can mend what it checks; an auto
    wrap-up deadline (at most 5 s) and the 10 s close of a call that left its queue fire on their own while the
    checks wait."""
    w.log.append("finish: every leg hangs up")
    phones = [a.phone for a in [*w.agents, w.sup]]
    for leg in customers(w) + [leg for p in phones for leg in live(p)]:
        leg.hangup()
    w.world.wait(lambda: not customers(w) and not any(map(live, phones)), timeout=10, what="every leg to end")
    w.world.wait(w.world.plivo.idle, what="every hangup callback answered")
    eventually(w, busy_unassigned)
    problems = offer_problems(caught_up(w))
    assert not problems, "; ".join(problems)
    eventually(w, still_active, timeout=15)
    for cid, agent_id, started in pending_wrap_ups(w):
        w.log.append(f"finish: {w.name(agent_id)} wraps up {cid}")
        submit(w, cid, agent_id, started)
    eventually(w, unclosed, timeout=20)
    eventually(w, not_idle)
    events = caught_up(w)
    closes = Counter(e["conversation_id"] for e in events if e["type"] == "conversation.closed")
    problems = [f"conversation {c} closed {n} times" for c, n in closes.items() if n != 1]
    for cid in closes:
        if (state := ok(w.tenant.api.get(f"/api/v1/conversations/{cid}"))["state"]) != "closed":
            problems.append(f"conversation {cid} reads {state} after conversation.closed")
    if not increasing([int(e["id"]) for e in events]):
        problems.append("GET /events ids are not increasing")
    last = events[-1]["id"]
    if [e for e in w.stream.events if "id" in e][: len(events)] != events:
        problems.append("the live stream differs from GET /events")
    replay = w.tenant.stream(after=0)
    w.world.wait(lambda: any(e.get("id") == last for e in replay.events), what="the replay to catch up")
    if replay.events[: len(events)] != events:
        problems.append("SSE replay from 0 differs from GET /events")
    assert not problems, "; ".join(problems)


@pytest.mark.parametrize("seed", SEEDS)
def test_walk(world: World, seed: int) -> None:
    w = Walk(world, seed)
    try:
        for n in range(STEPS):
            heartbeats(w)
            skipped = []
            for move in w.rng.sample(MOVES, len(MOVES)):
                if (line := move(w)) is not None:
                    break
                skipped.append(move.__name__)
            w.log.append(
                f"{n:3} {move.__name__}: {line}"
                + (f" [not eligible: {', '.join(dict.fromkeys(skipped))}]" if skipped else "")
            )
            check_step(w)
        finish(w)
    except Exception as e:
        base = os.path.join(tempfile.gettempdir(), f"hs-fuzz-{seed}")
        with open(f"{base}.jsonl", "w") as f:
            f.writelines(json.dumps(event) + "\n" for event in w.stream.events)
        log = "\n".join(w.log)
        with open(f"{base}.log", "w") as f:
            f.write(log + "\n")
        raise AssertionError(
            f"replay, best effort (HS timing can change which moves are eligible): HS_FUZZ_SEED={seed} make fuzz\n"
            f"{type(e).__name__}: {e}\nevents: {base}.jsonl\nsteps: {base}.log\n{log}"
        ) from e
