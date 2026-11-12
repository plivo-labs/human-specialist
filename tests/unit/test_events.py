"""The outbox on the migrated database: the relay, the stream fan-out and its scopes, the stream credential check,
plugin hooks with their cursor, parking and replay, and retention."""

import asyncio
import json
import logging
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import timedelta
from types import SimpleNamespace

import pytest
from fastapi import Request
from pydantic import ValidationError
from redis import RedisError
from sqlalchemy import func, insert, select, text, update

from hs import access, core, events, hot, jobs, plivo
from hs import conversations as cv
from hs.access import Principal
from hs.core import T
from hs.events import api, webhooks
from hs.hot import rebuilt
from tests.unit import tenants

HELD = {"by_agent_id": None}
ENDED = dict.fromkeys(("from", "to", "queue_id", "hangup_cause", "hangup_cause_code", "duration_sec", "talk_sec"))
ENDED |= {"result": "completed", "ended_by": "customer"}


def tenant() -> str:
    with core.tx() as db:
        settings = {"rona": {"status_id": str(uuid.uuid4())}}
        row = insert(T.tenant).values(
            name="T", plivo_auth_id=uuid.uuid4().hex, plivo_token_enc="sealed", settings=settings
        )
        tenant_id = str(db.execute(row.returning(T.tenant.c.id)).scalar_one())
    rebuilt(tenant_id)
    return tenant_id


def doc(tenant_id: str, kind: str, **body) -> str:
    with core.tx() as db:
        return str(
            db.execute(
                insert(T.config).values(tenant_id=tenant_id, kind=kind, body=body).returning(T.config.c.id)
            ).scalar()
        )


def agent(tenant_id: str, role: str = "agent", teams: tuple = ()) -> str:
    """An agent with its presence row, which an offer to it takes."""
    made = doc(tenant_id, "agent", name="A", email=f"{uuid.uuid4().hex}@x.test", role=role, teams=list(teams))
    with core.tx() as db:
        db.execute(insert(T.presence).values(agent_id=made, tenant_id=tenant_id, status_id=uuid.uuid4()))
    return made


def call(tenant_id: str, on: str | None = None, queue: str | None = None, context: dict | None = None):
    """A call conversation, with agent `on` active on it or queued on `queue`."""
    with core.tx() as db:
        conv = cv.open(
            db, tenant_id, channel="call", direction="inbound", customer="+1555", ours="+1666", context=context
        )
        if on:
            cv.offer(db, conv, on, via="direct")
        if queue:
            cv.set_state(db, conv, "queued", queue_id=queue)
        return conv


def key(tenant_id: str, scopes: tuple = ("events",)) -> dict:
    with core.tx() as db:
        return access.new_key(db, tenant_id, scopes)


def tokens(tenant_id: str, agent_id: str) -> access.Tokens:
    with core.tx() as db:
        return access.mint(db, tenant_id, agent_id)


def until(condition, seconds: float = 2) -> bool:
    deadline = time.monotonic() + seconds
    while not (ok := condition()) and time.monotonic() < deadline:
        time.sleep(0.05)
    return bool(ok)


async def rest(frames) -> list[str]:
    return [f async for f in frames]


def emit(tenant_id: str, type_: str = "call.held", data: dict = HELD, **ids) -> None:
    with core.tx() as db:
        events.emit(db, type_, tenant_id, data, **ids)


def stamped(tenant_id: str) -> list[dict]:
    while events.relay_once():
        pass
    return events.read(tenant_id, 0, 200)[0]


@pytest.fixture
def fan_out(monkeypatch):
    """Streams of a token on a private event loop, fed by `events._poll` from the current head."""
    while events.relay_once():
        pass
    with core.read() as db:
        monkeypatch.setattr(events, "_last", db.execute(events.HEAD).scalar())
    loop, made = asyncio.new_event_loop(), []

    def open_stream(token: str, request: Request | None = None) -> events.Stream:
        s = events.Stream(events.authorize(token, request), token, request, loop)
        events.join(s)
        made.append(s)
        return s

    yield open_stream
    for s in made:
        events.leave(s)
    loop.close()


def got(s: events.Stream) -> list:
    """What the fan-out handed the stream: event ids, and None for its end."""
    s.loop.run_until_complete(asyncio.sleep(0))
    items = []
    while not s.queue.empty():
        item = s.queue.get_nowait()
        items.append(item and item[0])
    return items


def test_emit_checks_the_data_against_the_catalog():
    t = tenant()
    with pytest.raises(ValidationError), core.tx() as db:
        events.emit(db, "call.held", t, {"by_agent_id": None, "extra": 1})
    emit(t, "offer.missed", {"offer_id": "o"})
    assert [e["data"] for e in stamped(t)] == [{"offer_id": "o", "strike": None, "reason": None}]


def test_the_relay_stamps_in_commit_order_copies_the_conversation_and_queues_matching_deliveries():
    t = tenant()
    conv = call(t, context={"lead": "L-1"})
    hooks = {}
    with core.tx() as db:
        for types in ([], ["call.held"], ["call.ended"]):
            row = insert(T.webhooks).values(tenant_id=t, url="https://x.test", secret="s", event_types=types)
            hooks[tuple(types)] = db.execute(row.returning(T.webhooks.c.id)).scalar()
    emit(t, conversation_id=conv.id)
    emit(t)
    started, held, bare = stamped(t)
    assert (started["type"], held["type"]) == ("conversation.started", "call.held")
    assert int(started["id"]) < int(held["id"]) < int(bare["id"])
    assert (held["direction"], held["context"]) == ("inbound", {"lead": "L-1"}) and bare["context"] is None
    J = T.jobs
    with core.read() as db:
        queued = (
            db.execute(
                select(J.c.payload).where(
                    J.c.kind == "webhook", J.c.payload["webhook_id"].astext.in_([str(h) for h in hooks.values()])
                )
            )
            .scalars()
            .all()
        )
    every, wanted = str(hooks[()]), str(hooks[("call.held",)])
    expected = {(every, e["id"]): e for e in (started, held, bare)} | {(wanted, e["id"]): e for e in (held, bare)}
    assert {(p["webhook_id"], p["body"]["id"]): p["body"] for p in queued} == expected
    with core.tx() as db:
        db.execute(J.delete().where(J.c.kind == "webhook", J.c.payload["webhook_id"].astext.in_([every, wanted])))


def test_a_stream_gets_only_its_tenants_events_and_an_agent_only_its_own_and_its_conversations(fan_out):
    a, b = tenant(), tenant()
    floor = doc(a, "team", name="Floor")
    me, mate, other = agent(a, teams=(floor,)), agent(a, teams=(floor,)), agent(a)
    sup = agent(a, "supervisor", teams=(floor,))
    queue = doc(a, "queue", name="Q", teams=[floor])
    streams = {
        "me": fan_out(tokens(a, me).agent_token),
        "other": fan_out(tokens(a, other).stream_token),
        "sup": fan_out(tokens(a, sup).stream_token),
        "key a": fan_out(key(a)["api_key"]),
        "key b": fan_out(key(b)["api_key"]),
    }
    mine, theirs, queued = call(a, on=me), call(a, on=other), call(a, queue=queue)
    emit(a, conversation_id=mine.id)
    emit(a, conversation_id=theirs.id)
    emit(a, "offer.rejected", {"offer_id": "o"}, conversation_id=theirs.id, agent_id=me)
    emit(a, "offer.rejected", {"offer_id": "o"}, conversation_id=theirs.id, agent_id=mate)
    emit(a, "conversation.queued", {"queue_id": queue, "from": None, "to": None}, conversation_id=queued.id)
    emit(b)
    seq = {(e["type"], e["conversation_id"], e["agent_id"]): int(e["id"]) for e in stamped(a)}
    starts = [seq[("conversation.started", str(c.id), None)] for c in (mine, theirs, queued)]
    on_mine, on_theirs = seq[("call.held", str(mine.id), None)], seq[("call.held", str(theirs.id), None)]
    my_offer, mate_offer = seq[("offer.rejected", str(theirs.id), me)], seq[("offer.rejected", str(theirs.id), mate)]
    team_queued = seq[("conversation.queued", str(queued.id), None)]
    while events._poll():
        pass
    assert got(streams["me"]) == [starts[0], on_mine, my_offer]
    assert got(streams["other"]) == [starts[1], on_theirs, my_offer, mate_offer]
    assert got(streams["sup"]) == [starts[0], starts[2], on_mine, my_offer, mate_offer, team_queued]
    assert got(streams["key a"]) == sorted(seq.values())
    assert got(streams["key b"]) == [int(e["id"]) for e in stamped(b)]


def test_an_agent_stream_follows_a_conversation_through_wrap_up_and_no_further(fan_out):
    t = tenant()
    me = agent(t)
    s, conv = fan_out(tokens(t, me).stream_token), call(t, on=me)

    def then(state: str, type_: str, data: dict) -> None:
        with core.tx() as db:
            db.execute(update(T.assignment).where(T.assignment.c.conversation_id == conv.id).values(state=state))
        emit(t, type_, data, conversation_id=conv.id)
        stamped(t)
        events._poll()

    then("wrap_up", "call.ended", ENDED)
    then("done", "call.held", HELD)
    assert [e["type"] for e in stamped(t)] == ["conversation.started", "call.ended", "call.held"]
    assert got(s) == [int(e["id"]) for e in stamped(t)[:2]]


def test_an_offer_accepted_after_its_timer_was_read_shows_its_agent_the_conversation_while_redis_fails(
    fan_out, monkeypatch
):
    t = tenant()
    me = agent(t)
    s, conv = fan_out(tokens(t, me).stream_token), call(t)
    with core.tx() as db:
        cv.offer(db, cv.lock(db, conv.id), me)
    emit(t, conversation_id=conv.id)
    states, offered = hot.states, [int(e["id"]) for e in stamped(t)]

    def accepted_then_lost(offers: list) -> list:
        monkeypatch.setattr(hot, "states", states)
        with core.tx() as db:
            [offer] = cv.rows(db, cv.lock(db, conv.id).id, ("offered",))
            cv.move(db, offer, "accept")
        raise RedisError("Redis is gone")

    monkeypatch.setattr(hot, "states", accepted_then_lost)
    events._poll()
    assert got(s) == offered


def test_a_page_whose_fan_out_failed_is_handed_over_on_the_next_poll_each_event_once(fan_out, monkeypatch):
    t = tenant()
    s = fan_out(key(t)["api_key"])
    emit(t)
    emit(t)
    facts, failed = events.facts, []

    def flaky(db, rows):
        if not failed:
            failed.append(True)
            raise RuntimeError("facts down")
        return facts(db, rows)

    monkeypatch.setattr(events, "facts", flaky)
    ids = [int(e["id"]) for e in stamped(t)]
    with pytest.raises(RuntimeError):
        events._poll()
    while events._poll():
        pass
    assert got(s) == ids


def test_a_removed_scope_or_a_demoted_role_ends_the_stream_at_the_next_check(fan_out):
    t = tenant()
    narrowed, sup = key(t, ("events", "calls")), agent(t, "supervisor")
    kept, scoped, demoted = (fan_out(k) for k in (key(t)["api_key"], narrowed["api_key"], tokens(t, sup).stream_token))
    with core.tx() as db:
        db.execute(update(T.credential).where(T.credential.c.id == narrowed["id"]).values(scopes=["calls"]))
        db.execute(text('UPDATE config SET body = body || \'{"role": "agent"}\' WHERE id = :id'), {"id": sup})
    events._check()
    assert (got(kept), got(scoped), got(demoted)) == ([], [None], [None])
    assert [s.confirmed() for s in (kept, scoped, demoted)] == [True, False, False]


def test_a_supervisor_stream_stays_open_for_a_new_teammate_and_ends_when_one_leaves(fan_out):
    t = tenant()
    floor = doc(t, "team", name="Floor")
    s = fan_out(tokens(t, agent(t, "supervisor", teams=(floor,))).stream_token)
    mate = agent(t, teams=(floor,))
    events._check()
    emit(t, "offer.rejected", {"offer_id": "o"}, agent_id=mate)
    [e] = stamped(t)
    events._poll()
    assert (got(s), s.confirmed()) == ([int(e["id"])], True)
    with core.tx() as db:
        db.execute(text("UPDATE config SET body = body || '{\"teams\": []}' WHERE id = :id"), {"id": mate})
    events._check()
    assert (got(s), s.confirmed()) == ([None], False)


def views(tenant_id: str, **levels: str) -> None:
    downloads = dict.fromkeys(("download_list", "download_conversation", "download_recording"), True)
    grants = {
        "admin": {"view": "org", **downloads, "monitor": True, "barge": True},
        "supervisor": {"view": "team", **downloads, "monitor": True, "barge": True},
        "agent": {"view": "self", **downloads, "monitor": False, "barge": False},
    }
    doc = {role: g | ({"view": levels[role]} if role in levels else {}) for role, g in grants.items()}
    with core.tx() as db:
        db.execute(
            text(
                "UPDATE tenant SET settings = settings || jsonb_build_object('permissions', CAST(:p AS jsonb))"
                " WHERE id = :t"
            ),
            {"p": json.dumps(doc), "t": tenant_id},
        )


def test_a_view_narrowed_from_org_ends_only_that_stream_and_a_widened_one_takes_its_teammates_in_place(fan_out):
    t = tenant()
    floor = doc(t, "team", name="Floor")
    sup, me, mate = agent(t, "supervisor", teams=(floor,)), agent(t, teams=(floor,)), agent(t, teams=(floor,))
    views(t, supervisor="org")
    narrowed, widened = fan_out(tokens(t, sup).stream_token), fan_out(tokens(t, me).stream_token)
    assert (narrowed.auth.scope, widened.auth.scope) == (None, events.Scope(frozenset({me}), frozenset()))
    views(t, supervisor="team", agent="team")
    events._check()
    assert (got(narrowed), narrowed.confirmed(), got(widened), widened.confirmed()) == ([None], False, [], True)
    assert widened.auth.scope == events.Scope(frozenset({me, mate, sup}), frozenset({floor}))


def test_a_plugin_credential_gets_the_scope_of_an_hs_token_of_its_role(monkeypatch):
    t = tenant()
    floor = doc(t, "team", name="Floor")
    sup, desk, mate = agent(t, "supervisor", teams=(floor,)), agent(t, teams=(floor,)), agent(t, teams=(floor,))
    monkeypatch.setattr(access, "RESOLVERS", [lambda request: Principal(t, "agent", agent_id=desk, role="supervisor")])

    def scopes() -> tuple:
        plugin = events.authorize("a-plugin-token", Request({"type": "http", "headers": []}))
        return plugin.scope, events.authorize(tokens(t, sup).stream_token, None).scope

    team_scope = frozenset({sup, desk, mate}), frozenset({floor})
    assert scopes() == (events.Scope(*team_scope), events.Scope(*team_scope))
    views(t, supervisor="org")
    assert scopes() == (None, None)


def test_no_frame_is_yielded_after_a_revoke_whether_replayed_or_buffered(fan_out):
    t = tenant()
    k = key(t)
    emit(t)
    emit(t)
    old = stamped(t)
    events._poll()
    replaying, waiting = fan_out(k["api_key"]), fan_out(k["api_key"])
    run = replaying.loop.run_until_complete
    replay, live = replaying.frames(int(old[0]["id"]) - 1), waiting.frames(None)
    assert [run(anext(f))[:6] for f in (replay, live)] == ["retry:"] * 2 and run(anext(replay)) == events.frame(old[0])
    woken = replaying.loop.create_task(anext(live))
    run(asyncio.sleep(0.05))
    emit(t)
    stamped(t)
    events._poll()
    with core.tx() as db:
        access.revoke(db, t, key_id=k["id"])
    events._check()
    assert (run(woken), run(rest(live)), run(rest(replay))) == (events.EXPIRED, [], [events.EXPIRED])


def test_a_scoped_read_scans_a_budget_of_pages_each_on_its_own_connection_and_returns_where_it_stopped(monkeypatch):
    t = tenant()
    me = agent(t)
    for _ in range(5):
        emit(t, conversation_id=call(t, on=agent(t)).id)
    mine = call(t, on=me)
    emit(t, conversation_id=mine.id)
    scope = events.authorize(tokens(t, me).agent_token, None).scope
    every = stamped(t)
    assert [(e["type"], e["conversation_id"]) for e in every[-2:]] == [
        ("conversation.started", str(mine.id)),
        ("call.held", str(mine.id)),
    ]
    held, real = [], core.read

    @contextmanager
    def page():
        held.append(core.engine.pool.checkedout())
        with real() as db:
            yield db

    monkeypatch.setattr(core, "read", page)
    monkeypatch.setattr(events, "SCAN", 2)
    reads = [events.read(t, 0, 2, scope=scope)]
    while reads[-1][1] is not None:
        reads.append(events.read(t, reads[-1][1], 2, scope=scope))
    seqs = [int(e["id"]) for e in every]
    assert reads == [([], seqs[3]), ([], seqs[7]), (every[-2:], seqs[11]), ([], None)]
    assert held == [0] * 7, "each page checks a connection out with none held"


def test_a_stream_replays_up_to_its_join_then_takes_the_live_feed_each_event_once(fan_out):
    t = tenant()
    token = key(t)["api_key"]
    emit(t)
    old = stamped(t)[-1]
    events._poll()
    emit(t)
    live = stamped(t)[-1]

    async def run() -> list[str]:
        s = events.Stream(events.authorize(token, None), token, None, asyncio.get_running_loop())
        frames = s.frames(int(old["id"]) - 1)
        out = [await anext(frames), await anext(frames)]
        await asyncio.to_thread(events._poll)
        out.append(await anext(frames))
        s.send((int(old["id"]), "a repeat"))
        s.send(None)
        return out + await rest(frames)

    hello, replayed, fed = asyncio.run(run())
    assert hello.startswith("retry: 3000")
    assert (replayed, fed) == (events.frame(old), events.frame(live))
    assert not events._streams.get(t), "a finished stream leaves the fan-out"


def test_a_fifth_replay_waits_for_one_of_four_to_finish_its_slice_before_it_takes_a_connection(monkeypatch):
    t = tenant()
    me = agent(t)
    emit(t, conversation_id=call(t, on=me).id)
    old = stamped(t)
    monkeypatch.setattr(events, "_last", int(old[-1]["id"]))
    token = tokens(t, me).agent_token
    auth, entered, go, real = events.authorize(token, None), [], threading.Event(), core.read

    @contextmanager
    def gated():
        entered.append(threading.current_thread())
        go.wait(5)
        with real() as db:
            yield db

    monkeypatch.setattr(core, "read", gated)

    async def run() -> tuple[int, list[str]]:
        loop = asyncio.get_running_loop()
        replays = [events.Stream(auth, token, None, loop).frames(int(old[0]["id"]) - 1) for _ in range(5)]
        for replay in replays:
            await anext(replay)
        firsts = [asyncio.ensure_future(anext(replay)) for replay in replays]
        async with asyncio.timeout(2):
            while len(entered) < 4:
                await asyncio.sleep(0.01)
        await asyncio.sleep(0.1)
        waiting = len(entered)
        go.set()
        out = await asyncio.gather(*firsts)
        for replay in replays:
            await replay.aclose()
        return waiting, out

    assert asyncio.run(run()) == (4, [events.frame(old[0])] * 5)


def test_a_revoked_keys_stream_gets_auth_expired_within_the_check_interval(monkeypatch):
    t, made = tenant(), []
    revoked, kept = key(t), key(t)
    monkeypatch.setattr(events, "HOOKS", {})

    async def run() -> tuple[list[str], float]:
        made.extend(
            events.Stream(events.authorize(k["api_key"], None), k["api_key"], None, asyncio.get_running_loop())
            for k in (revoked, kept)
        )
        events.join(made[1])
        frames = made[0].frames(None)
        await anext(frames)
        with core.tx() as db:
            access.revoke(db, t, key_id=revoked["id"])
        began = time.monotonic()
        return await rest(frames), time.monotonic() - began

    stop = events.start()
    try:
        out, took = asyncio.run(asyncio.wait_for(run(), 3 * events.AUTH_EVERY))
        assert out == [events.EXPIRED] and took <= events.AUTH_EVERY + 1
        assert events._streams[t] == {made[1]} and made[1].confirmed()
    finally:
        events.leave(made[1])
        stop()


def test_a_check_pass_that_runs_late_leaves_a_valid_stream_its_feed(monkeypatch):
    t = tenant()
    token = key(t)["api_key"]
    auth, late, release = events.authorize(token, None), threading.Event(), threading.Event()
    authorize = events._authorize

    def held(callers: list) -> list:
        late.set()
        release.wait(10)
        return authorize(callers)

    monkeypatch.setattr(events, "HOOKS", {})
    monkeypatch.setattr(events, "AUTH_EVERY", 0.05)
    monkeypatch.setattr(events, "_authorize", held)

    async def run() -> str:
        s = events.Stream(auth, token, None, asyncio.get_running_loop())
        frames = s.frames(None)
        await anext(frames)
        await asyncio.to_thread(late.wait, 5)
        while time.monotonic() - s.checked <= 2 * events.AUTH_EVERY:
            await asyncio.sleep(0.01)
        emit(t)
        return await anext(frames)

    stop = events.start()
    try:
        fed = asyncio.run(asyncio.wait_for(run(), 5))
        assert until(lambda: events.read(t, 0, 10)[0]) and fed == events.frame(events.read(t, 0, 10)[0][-1])
    finally:
        release.set()
        stop()


def test_a_stream_whose_check_cannot_finish_ends_for_its_client_to_resume_and_the_relay_goes_on(monkeypatch, caplog):
    t = tenant()
    token = key(t)["api_key"]
    auth, stuck, release = events.authorize(token, None), threading.Event(), threading.Event()
    monkeypatch.setattr(events, "HOOKS", {})
    monkeypatch.setattr(events, "AUTH_EVERY", 0.05)
    monkeypatch.setattr(events, "STALE", 0.3)
    monkeypatch.setattr(events, "_check", lambda ours: stuck.set() or release.wait(10))

    async def run() -> list[str]:
        return await rest(events.Stream(auth, token, None, asyncio.get_running_loop()).frames(None))

    stop = events.start()
    try:
        began = time.monotonic()
        assert asyncio.run(asyncio.wait_for(run(), 5)) == ["retry: 3000\n: ping\n\n"] and stuck.is_set()
        assert events.STALE <= time.monotonic() - began <= events.STALE + 0.5
        assert "no stream check finished" in caplog.text
        emit(t)
        assert until(lambda: events.read(t, 0, 10)[0]), "the relay waited for the check"
    finally:
        release.set()
        stop()


def test_a_plugin_resolver_that_stalls_holds_up_no_check_of_a_stream_on_an_hs_credential(monkeypatch):
    t = tenant()
    k = key(t)
    auth, stuck, release = events.authorize(k["api_key"], None), threading.Event(), threading.Event()

    def stalled(request: Request) -> None:
        stuck.set()
        release.wait(10)

    monkeypatch.setattr(access, "RESOLVERS", [stalled])
    monkeypatch.setattr(events, "HOOKS", {})
    monkeypatch.setattr(events, "AUTH_EVERY", 0.05)
    plugin = events.Stream(auth, "a-plugin-token", Request({"type": "http", "headers": []}), asyncio.new_event_loop())
    events.join(plugin)

    async def run() -> list[str]:
        frames = events.Stream(auth, k["api_key"], None, asyncio.get_running_loop()).frames(None)
        await anext(frames)
        await asyncio.to_thread(stuck.wait, 5)
        with core.tx() as db:
            access.revoke(db, t, key_id=k["id"])
        return await rest(frames)

    stop = events.start()
    try:
        assert asyncio.run(asyncio.wait_for(run(), 5)) == [events.EXPIRED]
    finally:
        release.set()
        stop()
        events.leave(plugin)
        plugin.loop.close()


@pytest.mark.parametrize("stalled", [False, True], ids=("fast", "stalled"))
def test_plugin_streams_keep_their_feeds_while_their_checks_add_up_past_stale_and_a_revoked_one_ends(
    monkeypatch, stalled
):
    t = tenant()
    desks = {f"desk-{i}": agent(t, "admin") for i in range(40)}
    slow, release = threading.Event(), threading.Event()

    def resolver(request: Request) -> Principal | None:
        token = access.bearer(request)
        if slow.is_set() and stalled and token == "desk-0":
            release.wait(10)
        elif slow.is_set():
            time.sleep(0.1)
        revoked = slow.is_set() and token == "desk-1"
        return None if revoked else Principal(t, "agent", agent_id=desks[token], role="admin")

    monkeypatch.setattr(access, "RESOLVERS", [resolver])
    monkeypatch.setattr(events, "HOOKS", {})
    monkeypatch.setattr(events, "AUTH_EVERY", 0.05)
    # A confirmation trails its ask by a few check passes and their database reads, so STALE leaves room for slow reads.
    monkeypatch.setattr(events, "STALE", 2.0)

    async def run() -> dict[str, str | None]:
        feeds = {}
        for token in desks:
            request = Request({"type": "http", "headers": [(b"authorization", f"Bearer {token}".encode())]})
            s = events.Stream(events.authorize(token, request), token, request, asyncio.get_running_loop())
            feeds[token] = s.frames(None)
            await anext(feeds[token])
        slow.set()
        began = time.monotonic()
        while time.monotonic() - began <= 2 * events.STALE:
            await asyncio.sleep(0.05)
        emit(t)
        return {token: await anext(frames, None) for token, frames in feeds.items()}

    stop = events.start()
    try:
        got = asyncio.run(asyncio.wait_for(run(), 15))
    finally:
        release.set()
        stop()
    fed, revoked, stuck = events.frame(stamped(t)[-1]), got.pop("desk-1"), got.pop("desk-0")
    assert set(got.values()) == {fed}, "a plugin stream whose resolver answered lost its feed"
    assert revoked == events.EXPIRED and stuck == (None if stalled else fed)


def cookie(value: str) -> Request:
    return Request({"type": "http", "headers": [(b"cookie", value.encode())]})


def test_plugin_streams_that_share_a_credential_string_are_each_checked_by_their_own_request(fan_out, monkeypatch):
    t = tenant()
    who, gone = {"desk=a": agent(t), "desk=b": agent(t)}, set()

    def by_cookie(request: Request) -> Principal | None:
        c = request.headers["cookie"]
        return None if c in gone else Principal(t, "agent", agent_id=who[c], role="agent")

    monkeypatch.setattr(access, "RESOLVERS", [by_cookie])
    a, b = (fan_out("", cookie(c)) for c in who)
    began = (a.checked, b.checked)
    events._check(False)
    assert a.checked > began[0] and b.checked > began[1], "both checks completed"
    emit(t, agent_id=who["desk=a"])
    emit(t, agent_id=who["desk=b"])
    seq = {e["agent_id"]: int(e["id"]) for e in stamped(t)}
    while events._poll():
        pass
    assert (got(a), got(b)) == ([seq[who["desk=a"]]], [seq[who["desk=b"]]])
    gone.add("desk=b")
    events._check(False)
    assert (got(a), got(b), a.confirmed(), b.confirmed()) == ([], [None], True, False)


def test_a_saturated_check_pool_queues_nothing_for_a_plugin_stream_that_left(fan_out, monkeypatch):
    t, stall, release, asked = tenant(), threading.Event(), threading.Event(), []
    desk = agent(t)

    def resolver(request: Request) -> Principal:
        asked.append(request.headers["cookie"])
        if stall.is_set() and request.headers["cookie"] == "stuck":
            release.wait(10)
        return Principal(t, "agent", agent_id=desk, role="agent")

    monkeypatch.setattr(access, "RESOLVERS", [resolver])
    monkeypatch.setattr(events, "AUTH_EVERY", 0.05)
    monkeypatch.setattr(events, "_askers", ThreadPoolExecutor(2))
    monkeypatch.setattr(events, "_asking", {})
    for _ in range(2):
        fan_out("", cookie("stuck"))
    stall.set()
    for i in range(20):
        reconnect = fan_out("", cookie(f"reconnect-{i}"))
        events._check(False)
        events.leave(reconnect)
    events._check(False)
    asked.clear()
    release.set()
    events._askers.shutdown(wait=True)
    assert asked == [], "a stream that left was resolved again"


def test_a_stream_checks_the_credential_its_route_accepted(monkeypatch):
    t = tenant()
    me = agent(t)
    caller = [Principal(t, "agent", agent_id=me, role="agent")]
    monkeypatch.setattr(access, "RESOLVERS", [lambda request: caller[0]])
    monkeypatch.setattr(events, "AUTH_EVERY", 0.05)
    query = f"access_token={tokens(t, me).agent_token}".encode()
    request = Request({"type": "http", "method": "GET", "path": "/", "headers": [], "query_string": query})

    async def run() -> tuple[bool, list[str]]:
        frames = (await api.stream(request, api.STREAM.dependency(request), None, None)).body_iterator
        await anext(frames)
        [s] = events._streams[t]
        caller[0] = None
        await asyncio.to_thread(events._check, False)
        return s.confirmed(), await rest(frames)

    assert asyncio.run(run()) == (False, [events.EXPIRED]), "the stream followed the query's agent token"


def test_the_access_log_line_never_carries_a_stream_token(caplog):
    caplog.set_level(logging.INFO, "uvicorn.access")
    path = "/api/v1/events/stream?access_token=hs_st_c2VjcmV0"
    logging.getLogger("uvicorn.access").info('%s - "%s %s HTTP/%s" %d', "127.0.0.1", "GET", path, "1.1", 200)
    assert "/api/v1/events/stream" in caplog.text and "c2VjcmV0" not in caplog.text


@pytest.fixture
def hook(monkeypatch):
    """A fresh hook name whose cursor starts at the current head; in-place retries take no time."""
    monkeypatch.setattr(events, "HOOK_RETRY", (0, 0, 0))
    events.relay_once()
    name = f"test:{uuid.uuid4().hex}"
    with core.tx() as db:
        db.execute(events.HOOK_START, {"name": name})
    return name


def cursor(name: str) -> int:
    with core.read() as db:
        return db.execute(text("SELECT seq FROM hook_cursor WHERE name = :n"), {"n": name}).scalar()


def test_a_hook_gets_every_event_in_seq_order_and_its_cursor_follows(hook):
    t, seen = tenant(), []
    for _ in range(3):
        emit(t)
    ids = [int(e["id"]) for e in stamped(t)]
    while events.hook_pass(hook, seen.append):
        pass
    assert cursor(hook) >= ids[-1]
    assert [e for e in seen if e["tenant_id"] == t] == stamped(t)


def test_two_replicas_never_run_one_hook_at_once(hook):
    t, seen, running, overlaps, lock = tenant(), [], [0], [], threading.Lock()

    def fn(e: dict) -> None:
        with lock:
            running[0] += 1
            overlaps.append(running[0])
        time.sleep(0.005)
        seen.append(int(e["id"]))
        with lock:
            running[0] -= 1

    for _ in range(20):
        emit(t)
    ids = [int(e["id"]) for e in stamped(t)]

    def replica() -> None:
        while cursor(hook) < ids[-1]:
            events.hook_pass(hook, fn)

    replicas = [threading.Thread(target=replica) for _ in range(2)]
    for r in replicas:
        r.start()
    for r in replicas:
        r.join(20)
    assert max(overlaps) == 1 and [i for i in seen if i in set(ids)] == ids


def test_an_event_that_fails_every_retry_is_parked_with_its_envelope_and_replayed(hook, monkeypatch):
    t, tries, seen = tenant(), [], []
    for _ in range(3):
        emit(t)
    first, bad, last = stamped(t)

    def fn(e: dict) -> None:
        if e["id"] == bad["id"]:
            tries.append(e["id"])
            raise RuntimeError("down")
        seen.append(e)

    monkeypatch.setitem(events.HOOKS, hook, fn)
    while events.hook_pass(hook, fn):
        pass
    assert len(tries) == 1 + len(events.HOOK_RETRY) and cursor(hook) >= int(last["id"])
    assert [e for e in seen if e["tenant_id"] == t] == [first, last]
    key = f"{hook}:{bad['id']}"
    with core.read() as db:
        assert db.execute(select(T.jobs.c.payload).where(T.jobs.c.kind == "hook", T.jobs.c.key == key)).scalar() == {
            "hook": hook,
            "envelope": bad,
        }
    monkeypatch.setitem(events.HOOKS, hook, seen.append)
    jobs.run_now("hook", key, wait=True)
    assert seen[-1] == bad


def test_a_replay_waits_while_the_live_loop_holds_the_hook(hook, monkeypatch):
    t, calls, inside, release = tenant(), [], threading.Event(), threading.Event()
    emit(t)
    [parked] = stamped(t)
    while events.hook_pass(hook, lambda e: None):
        pass
    key = f"{hook}:{parked['id']}"
    with core.tx() as db:
        jobs.schedule(db, "hook", {"hook": hook, "envelope": parked}, key=key)
    emit(t)
    live = stamped(t)[-1]

    def fn(e: dict) -> None:
        calls.append(("in", e["id"]))
        if e["id"] == live["id"]:
            inside.set()
            release.wait(5)
        calls.append(("out", e["id"]))

    monkeypatch.setitem(events.HOOKS, hook, fn)
    loop = threading.Thread(target=lambda: [events.hook_pass(hook, fn) for _ in range(3)])
    loop.start()
    assert inside.wait(5)
    jobs.run_now("hook", key, wait=True)
    release.set()
    loop.join(5)
    assert ("in", parked["id"]) not in calls, "the replay ran while the live pass held the cursor"
    with core.read() as db:
        assert db.execute(select(T.jobs.c.attempts).where(T.jobs.c.key == key)).scalar() == 0, "a wait is no attempt"
    jobs.run_now("hook", key, wait=True)
    mine = [c for c in calls if c[1] in (parked["id"], live["id"])]
    assert mine[-2:] == [("in", parked["id"]), ("out", parked["id"])] and ("out", live["id"]) in mine[:-2]


def _now():
    with core.read() as db:
        return db.execute(select(func.now())).scalar()


def parked_job(hook: str) -> tuple[dict, str]:
    t = tenant()
    emit(t)
    [parked] = stamped(t)
    key = f"{hook}:{parked['id']}"
    with core.tx() as db:
        jobs.schedule(db, "hook", {"hook": hook, "envelope": parked}, key=key)
    return parked, key


def test_a_replay_that_outlives_its_lease_runs_once(hook, monkeypatch):
    (parked, key), calls, inside, release = parked_job(hook), [], threading.Event(), threading.Event()

    def fn(e: dict) -> None:
        calls.append(e["id"])
        inside.set()
        release.wait()

    monkeypatch.setitem(events.HOOKS, hook, fn)
    monkeypatch.setattr(jobs, "LEASE", timedelta(seconds=1))
    first = threading.Thread(target=jobs.run_now, args=("hook", key), kwargs={"wait": True})
    first.start()
    try:
        assert inside.wait(5)
        lapsed = _now() + jobs.LEASE
        assert until(lambda: _now() > lapsed, 5), "the claim's own lease never ran out"
        jobs.run_now("hook", key, wait=True)
    finally:
        release.set()
    first.join(5)
    jobs.run_now("hook", key, wait=True)
    with core.read() as db:
        left = db.execute(select(func.count()).where(T.jobs.c.kind == "hook", T.jobs.c.key == key)).scalar()
    assert calls == [parked["id"]] and left == 0


def test_a_replay_that_finishes_past_its_claims_lease_is_not_delivered_again_by_the_next_claim(hook, monkeypatch):
    (parked, key), calls = parked_job(hook), []

    def fn(e: dict) -> None:
        calls.append(e["id"])
        assert until(lambda: _now() > first.locked_until, 5), "the claim's own lease never ran out"

    monkeypatch.setitem(events.HOOKS, hook, fn)
    monkeypatch.setattr(jobs, "LEASE", timedelta(seconds=1))
    first = jobs._claim(T.jobs.c.kind == "hook", T.jobs.c.key == key)
    events._replay(first)
    jobs.run_now("hook", key, wait=True)
    with core.read() as db:
        left = db.execute(select(func.count()).where(T.jobs.c.kind == "hook", T.jobs.c.key == key)).scalar()
    assert calls == [parked["id"]] and left == 0


def test_a_replay_whose_claim_another_worker_took_and_completed_delivers_nothing_and_leaves_the_hook_free(
    hook, monkeypatch
):
    (parked, key), calls = parked_job(hook), []
    monkeypatch.setitem(events.HOOKS, hook, lambda e: calls.append(e["id"]))
    monkeypatch.setattr(jobs, "LEASE", timedelta(seconds=0.5))
    stale = jobs._claim(T.jobs.c.kind == "hook", T.jobs.c.key == key)
    assert until(lambda: _now() > stale.locked_until, 5), "the claim's own lease never ran out"
    jobs.run_now("hook", key, wait=True)
    events._replay(stale)
    assert calls == [parked["id"]] and events.hook_pass(hook, lambda e: None) > 0


def test_a_call_that_outlives_its_lease_keeps_the_hook_from_every_other_holder_so_delivery_stays_in_order(
    hook, monkeypatch
):
    t, seen, running, overlaps, lock = tenant(), [], [0], [], threading.Lock()
    inside, release = threading.Event(), threading.Event()
    for _ in range(2):
        emit(t)
    ids = [int(e["id"]) for e in stamped(t)]

    def fn(e: dict) -> None:
        with lock:
            running[0] += 1
            overlaps.append(running[0])
        if not inside.is_set():
            inside.set()
            release.wait(10)
        seen.append(int(e["id"]))
        with lock:
            running[0] -= 1

    monkeypatch.setattr(events, "HOOK_LEASE", 2)
    monkeypatch.setattr(events, "HOOKS", {})
    stop, first = events.start(), threading.Thread(target=events.hook_pass, args=(hook, fn))
    try:
        first.start()
        assert inside.wait(5)
        lapsed = _now() + timedelta(seconds=events.HOOK_LEASE)
        assert until(lambda: _now() > lapsed, 5), "the first lease never ran out"
        events.hook_pass(hook, fn)
    finally:
        release.set()
        first.join(5)
        stop()
    while events.hook_pass(hook, fn):
        pass
    assert max(overlaps) == 1 and [i for i in seen if i in set(ids)] == ids


def test_a_hook_stuck_in_one_call_past_its_lease_is_named_in_one_warning(hook, monkeypatch, caplog):
    t, inside, release = tenant(), threading.Event(), threading.Event()
    emit(t)
    stamped(t)

    def fn(e: dict) -> None:
        inside.set()
        release.wait(10)

    def warned() -> list:
        return [r for r in caplog.records if r.levelno == logging.WARNING and hook in r.getMessage()]

    monkeypatch.setattr(events, "HOOK_LEASE", 1)
    monkeypatch.setattr(events, "HOOKS", {})
    stop, stuck = events.start(), threading.Thread(target=events.hook_pass, args=(hook, fn))
    try:
        stuck.start()
        assert inside.wait(5) and until(warned, 5), "the stuck hook was never named"
        later = time.monotonic() + 4 * events.HOOK_LEASE
        until(lambda: time.monotonic() > later, 5)
    finally:
        release.set()
        stuck.join(5)
        stop()
    assert len(warned()) == 1


def test_a_failing_call_is_retried_while_renewals_land_and_never_once_its_lease_may_have_passed_to_another_holder(
    hook, monkeypatch
):
    t, seen, renews, ahead, cut_off = tenant(), [], [0], [0.0], threading.Event()
    for _ in range(2):
        emit(t)
    mine = [e["id"] for e in stamped(t)]
    real, renew = time.monotonic, events._renew

    def counted(*args) -> None:
        try:
            if cut_off.is_set():
                raise ConnectionError("Postgres is out of reach")
            renew(*args)
        finally:
            renews[0] += 1

    def tick(leases: float) -> None:
        ahead[0] += leases * events.HOOK_LEASE
        n = renews[0]
        until(lambda: renews[0] >= n + 2, 5)

    def other(e: dict) -> None:
        if e["id"] in mine:
            seen.append(e["id"])

    def fn(e: dict) -> None:
        if e["id"] not in mine:
            return
        seen.append(e["id"])
        if len(seen) == 1:
            tick(0.6)
        elif len(seen) == 2:
            cut_off.set()
            tick(1)
            lapsed = _now() + timedelta(seconds=events.HOOK_LEASE)
            until(lambda: _now() > lapsed, 5)
            events.hook_pass(hook, other)
            cut_off.clear()
            tick(0)
        raise RuntimeError("down")

    monkeypatch.setattr(events, "HOOK_LEASE", 2)
    monkeypatch.setattr(events, "HOOKS", {})
    monkeypatch.setattr(events, "_renew", counted)
    monkeypatch.setattr(time, "monotonic", lambda: real() + ahead[0])
    stop = events.start()
    try:
        events.hook_pass(hook, fn)
    finally:
        stop()
    first, second = mine
    assert seen == [first, first, first, second], "a call started after another holder delivered a later event"


def test_a_plugin_resolves_a_stream_again_with_no_connection_held(monkeypatch):
    held, before = [], core.engine.pool.checkedout()

    def resolver(request) -> None:
        held.append(core.engine.pool.checkedout() - before)

    monkeypatch.setattr(access, "RESOLVERS", [resolver])
    assert events.authorize("a-plugin-token", Request({"type": "http", "headers": []})) is None and held == [0]


def test_a_slow_hook_leaves_the_relay_on_time(monkeypatch):
    t, release, reached = tenant(), threading.Event(), threading.Event()

    def slow(e: dict) -> None:
        reached.set()
        release.wait(10)

    monkeypatch.setattr(events, "HOOKS", {f"slow:{uuid.uuid4().hex}": slow})
    stop = events.start()
    try:
        emit(t)
        assert reached.wait(5), "the hook never ran"
        emit(t)
        assert until(lambda: len(events.read(t, 0, 10)[0]) == 2), "the relay waited for the hook"
    finally:
        release.set()
        stop()


def test_retention_keeps_every_event_of_a_conversation_that_has_not_closed():
    t, E = tenants.Tenant(), T.events
    waiting = t.open().id
    t.closed(t.agent())
    stamped(str(t.id))
    mine = E.c.tenant_id == str(t.id)
    with core.tx() as db:
        db.execute(text("DELETE FROM hook_cursor"))
        db.execute(update(E).where(mine).values(created_at=func.now() - text("interval '100 days'")))
    events._purge(None)
    with core.read() as db:
        assert db.execute(select(E.c.conversation_id).where(mine).distinct()).scalars().all() == [str(waiting)]


def test_retention_waits_for_the_hooks_still_running_anywhere_and_a_parked_delivery_keeps_its_body(monkeypatch):
    t, sent = tenant(), []
    with core.tx() as db:
        made = insert(T.webhooks).values(
            tenant_id=t, url="https://x.test", secret=plivo.encrypt("s"), event_types=["call.resumed"]
        )
        hook = str(db.execute(made.returning(T.webhooks.c.id)).scalar())
    for type_ in ("call.resumed", "call.held", "call.held", "call.held"):
        emit(t, type_)
    parked, purged, kept_by_cursor, fresh = stamped(t)
    E, J = T.events, T.jobs
    mine = (J.c.kind == "webhook", J.c.payload["webhook_id"].astext == hook)
    running, retired, elsewhere = (f"test:{uuid.uuid4().hex}" for _ in range(3))
    monkeypatch.setitem(events.HOOKS, running, lambda e: None)
    C, quiet = T.hook_cursor, func.now() - text("interval '1 day'")
    rows = [(running, int(purged["id"])), (retired, 0), (elsewhere, 0)]
    with core.tx() as db:
        db.execute(text("DELETE FROM hook_cursor"))
        db.execute(insert(C).values([{"name": n, "seq": seq, "updated_at": quiet} for n, seq in rows]))
        old = E.c.seq.in_([int(e["id"]) for e in (parked, purged, kept_by_cursor)])
        db.execute(update(E).where(E.c.tenant_id == t, old).values(created_at=func.now() - text("interval '100 days'")))
        job = db.execute(update(J).where(*mine).values(locked_until=jobs.INFINITY).returning(*J.c)).one()

    async def post(url: str, body: bytes, headers: dict) -> int:
        sent.append(json.loads(body))
        return 200

    monkeypatch.setattr(webhooks, "post", post)

    def left(**cursor) -> list[str]:
        with core.tx() as db:
            db.execute(update(C).where(C.c.name == elsewhere).values(**cursor))
        events._purge(None)
        return [e["id"] for e in events.read(t, 0, 10)[0]]

    try:
        every = [e["id"] for e in (parked, purged, kept_by_cursor, fresh)]
        assert left(updated_at=func.now()) == every, "a cursor another replica advanced a moment ago holds the purge"
        assert left(updated_at=quiet, held_until=func.now()) == every, "so does one another replica holds"
        assert left(held_until=None) == [kept_by_cursor["id"], fresh["id"]], "a quiet one does not"
        webhooks.deliver(job)
        assert sent == [parked]
    finally:
        with core.tx() as db:
            db.execute(J.delete().where(*mine))


def subscribed(t: str, *hosts: str) -> list[str]:
    made = insert(T.webhooks).returning(T.webhooks.c.id)
    with core.tx() as db:
        return [
            str(db.execute(made.values(tenant_id=t, url=f"https://{h}.test", secret=plivo.encrypt("s"))).scalar())
            for h in hosts
        ]


def test_an_endpoint_whose_exchange_failed_is_not_called_until_its_breaker_closes(monkeypatch):
    t, called = tenant(), []
    dead, live = subscribed(t, "dead", "live")

    def job(hook: str) -> SimpleNamespace:
        return SimpleNamespace(payload={"webhook_id": hook, "body": {"id": "1", "type": "call.resumed"}})

    async def post(url: str, body: bytes, headers: dict) -> int:
        called.append(url)
        if "dead" in url:
            raise TimeoutError
        return 200

    def tried() -> bool:
        try:
            webhooks.deliver(job(dead))
        except jobs.NotYet:
            return False
        except TimeoutError:
            return True
        raise AssertionError("a timed-out exchange succeeded")

    monkeypatch.setattr(webhooks, "post", post)
    monkeypatch.setattr(hot, "BREAK_MS", 500)
    assert tried() and not tried(), "the endpoint was called again while its breaker was open"
    webhooks.deliver(job(live))
    assert until(tried, 5) and called == ["https://dead.test", "https://live.test", "https://dead.test"]


def test_hanging_webhook_deliveries_hold_up_no_other_lane(monkeypatch):
    t, release, ran = tenant(), threading.Event(), set()
    [hook] = subscribed(t, "hangs")
    for _ in range(max(jobs.LANES.values()) + 1):
        emit(t, "call.resumed")
    stamped(t)

    async def hang(url: str, body: bytes, headers: dict) -> int:
        release.wait(10)
        return 200

    monkeypatch.setattr(webhooks, "post", hang)
    monkeypatch.setattr(jobs, "_handlers", {"webhook": jobs._handlers["webhook"]})
    monkeypatch.setattr(jobs, "_every", {})
    kinds = {lane: f"t_{lane}_{uuid.uuid4().hex[:8]}" for lane in ("slow", "control")}
    for lane, kind in kinds.items():
        jobs.handler(kind, lane=lane)(lambda job: ran.add(job.kind))
    with core.tx() as db:
        for kind in kinds.values():
            jobs.schedule(db, kind, {})
    stop = jobs.start()
    try:
        assert until(lambda: ran == set(kinds.values()), 5), f"beside hanging deliveries only {ran or 'nothing'} ran"
    finally:
        release.set()
        stop()
        with core.tx() as db:
            db.execute(T.jobs.delete().where(T.jobs.c.payload["webhook_id"].astext == hook))


def test_a_call_does_not_start_when_its_stamp_was_old_at_the_moment_the_clock_was_read(hook, monkeypatch):
    t, calls, real = tenant(), [], time.monotonic
    emit(t)
    stamped(t)
    armed = [False]

    def clock() -> float:
        now = real()
        if armed[0] and events._held:
            armed[0] = False
            [hold] = events._held.values()
            now += 0.6 * events.HOOK_LEASE
            hold.renewed = now
        return now

    monkeypatch.setattr(time, "sleep", lambda s: armed.__setitem__(0, True))
    monkeypatch.setattr(time, "monotonic", clock)
    events.hook_pass(hook, calls.append)
    assert calls == []


def test_a_replay_whose_hold_is_stale_by_the_call_leaves_the_plugin_uncalled_and_its_job_parked(hook, monkeypatch):
    (_, key), calls, ahead, real = parked_job(hook), [], [0.0], time.monotonic
    lease = events._lease

    @contextmanager
    def late(name):
        with lease(name) as token:
            ahead[0] = 0.6 * events.HOOK_LEASE
            yield token

    monkeypatch.setitem(events.HOOKS, hook, calls.append)
    monkeypatch.setattr(events, "_lease", late)
    monkeypatch.setattr(time, "monotonic", lambda: real() + ahead[0])
    jobs.run_now("hook", key, wait=True)
    with core.read() as db:
        left = db.execute(select(func.count()).where(T.jobs.c.kind == "hook", T.jobs.c.key == key)).scalar()
        held = db.execute(
            text("SELECT count(*) FROM hook_cursor WHERE name = :n AND held_by IS NOT NULL"), {"n": hook}
        ).scalar()
    assert calls == [] and left == 1 and held == 0
