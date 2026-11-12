"""The conversation and assignment machines on the migrated database: every transition row, its duplicate and a
late event, promote under the one-primary index, depart, the offline sweep against accepts, and view()."""

import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, insert, select, update

from hs import access, config, core, jobs
from hs import conversations as cv
from hs.conversations import numbers
from hs.core import T
from hs.hot import rebuilt

SETTINGS = {
    "ring_timeout_sec": 30,
    "wrap_up": {"mode": "manual", "seconds": 900, "max_extend_sec": 30},
    "acw_blocks_routing": False,
    "text": {"offer_timeout_sec": 60},
    "rona": {"strikes": 2, "status_id": str(uuid.uuid4())},
}


class World:
    """One tenant with its settings, dispositions and a queue."""

    def __init__(self):
        with core.tx() as db:
            tenant = insert(T.tenant).values(name="t", plivo_auth_id=uuid.uuid4().hex, plivo_token_enc="sealed")
            self.tenant = db.execute(tenant.returning(T.tenant.c.id)).scalar()
            rebuilt(self.tenant)
            self.sale, self.answered, self.dropped, self.unreached = (
                self.doc(db, "disposition", name=name, system=system, parent_id=None, note_required=False)
                for name, system in (("Sale", False), ("Auto-closed", True), ("Dropped", True), ("No answer", True))
            )
            self.queue = self.doc(db, "queue", name="Q", auto_accept={"enabled": True, "delay": 0})
        status_map = {"inbound": {"answered": str(self.answered), "abandoned": str(self.dropped)}}
        self.settings(**SETTINGS, status_map=status_map | {"outbound": {"no_answer": str(self.unreached)}})

    def doc(self, db, kind: str, **body) -> uuid.UUID:
        row = insert(T.config).values(tenant_id=self.tenant, kind=kind, body=body).returning(T.config.c.id)
        return db.execute(row).scalar()

    def settings(self, **changes) -> None:
        with config.write(access.OPERATOR, self.tenant) as db:
            S = T.tenant.c.settings
            db.execute(update(T.tenant).where(T.tenant.c.id == self.tenant).values(settings=S.concat(changes)))

    def agent(self, misses: int = 0) -> uuid.UUID:
        agent_id = uuid.uuid4()
        with core.tx() as db:
            row = {"agent_id": agent_id, "tenant_id": self.tenant, "status_id": uuid.uuid4(), "misses": misses}
            db.execute(insert(T.presence).values(**row))
        return agent_id

    def misses(self, agent_id) -> int:
        with core.read() as db:
            return db.execute(select(T.presence.c.misses).where(T.presence.c.agent_id == agent_id)).scalar()

    def open(self, channel: str = "call", queued: bool = True, **fields):
        with core.tx() as db:
            ids = {"call_uuid": uuid.uuid4().hex} if channel == "call" else {}
            fields = {"direction": "inbound", "customer": "+15550001", "ours": "+15550002"} | ids | fields
            fields = {"number": fields["ours"], "entry_point_id": None, "hours": "none"} | fields
            conv = cv.open(db, self.tenant, channel=channel, **fields)
            return (queued and cv.set_state(db, conv, "queued", queue_id=self.queue)) or conv

    def offer(self, conv, agent_id=None, **kw):
        with core.tx() as db:
            return cv.offer(db, cv.lock(db, conv.id), agent_id, **kw)

    def move(self, a, event: str, **cols):
        """`event` on `a` as the seam reads it under the conversation's lock."""
        with core.tx() as db:
            now = cv.rows(db, cv.lock(db, a.conversation_id).id)
            return cv.move(db, next((x for x in now if x.id == a.id), a), event, **cols)

    def live(self, channel: str = "call"):
        """A conversation whose primary agent accepted: (conversation, assignment)."""
        conv = self.open(channel)
        a = self.move(self.offer(conv, self.agent()), "accept", member_id="m-primary")
        return self.conv(conv.id), a

    def conv(self, cid):
        return row(T.conversation, cid)

    def of(self, a):
        """Stint or offer `a` as the seam reads it now, an ended offer's tombstone included."""
        with core.read() as db:
            return next(x for x in cv.rows(db, row(T.conversation, a.conversation_id), ended=True) if x.id == a.id)

    def events(self, type_: str, cid=None) -> list:
        """The tenant's events of this type, in emit order, as the events table holds them."""
        E = T.events
        mine = (E.c.tenant_id == str(self.tenant), E.c.type == type_)
        mine += () if cid is None else (E.c.conversation_id == str(cid),)
        with core.read() as db:
            return db.execute(select(E).where(*mine).order_by(E.c.id)).all()


def has(data: dict, **fields) -> bool:
    """`data` holds `fields`: the catalog may add optional fields as null."""
    return all(data.get(k) == v for k, v in fields.items())


@pytest.fixture
def world() -> World:
    return World()


def locked_row(db, a):
    """Stint or open offer `a`, read through the seam under its conversation's lock."""
    return next(x for x in cv.rows(db, cv.lock(db, a.conversation_id).id) if x.id == a.id)


def row(table, id):
    with core.read() as db:
        return db.execute(select(table).where(table.c.id == id)).one()


def job(kind: str, key):
    J = T.jobs
    wait = (J.c.run_at - func.now()).label("wait")
    with core.read() as db:
        return db.execute(select(J, wait).where(J.c.kind == kind, J.c.key == str(key))).first()


def race(*fns) -> list:
    """Start every fn at once and return their results; a worker that raised or never finished fails the test."""
    gate = threading.Barrier(len(fns))
    with ThreadPoolExecutor(len(fns)) as pool:
        futures = [pool.submit(lambda fn=fn: (gate.wait(10), fn())[1]) for fn in fns]
        return [f.result(timeout=30) for f in futures]


def states(conv) -> list[tuple[str, str]]:
    with core.read() as db:
        return [(a.role, a.state) for a in cv.rows(db, conv.id)]


def test_open_announces_a_call_once_and_the_retried_answer_finds_it(world):
    call = uuid.uuid4().hex
    first = world.open(queued=False, call_uuid=call, number="+15550002")
    again = world.open(queued=False, call_uuid=call)
    assert again.id == first.id
    [started] = world.events("conversation.started")
    assert has(started.data, channel="call", number="+15550002") and started.conversation_id == str(first.id)


def test_a_text_thread_joins_its_open_conversation_and_opens_a_new_one_after_it_closed(world):
    first = world.open("sms", context={"lead": "L-1"})
    assert world.open("sms").id == first.id and first.context == {"lead": "L-1"}
    with core.tx() as db:
        cv.gone(db, cv.lock(db, first.id), "session_timeout")
    later = world.open("sms")
    assert later.id != first.id and len(world.events("conversation.started")) == 2


def test_a_tenant_holding_ten_digit_numbers_numbers_its_next_conversation_above_them(world):
    copied = {"tenant_id": world.tenant, "channel": "sms", "direction": "inbound", "state": "closed"}
    with core.tx() as db:
        db.execute(insert(T.conversation).values(**copied, customer="+15550031", number=1_234_567_890))
    assert world.open("sms").number == 1_234_567_891


def test_a_number_an_older_replica_took_meanwhile_sends_the_creation_to_the_next(world):
    first = world.open("sms", customer="+15550041")
    older = {"tenant_id": world.tenant, "channel": "sms", "direction": "inbound", "state": "closed"}
    with core.tx() as db:
        db.execute(insert(T.conversation).values(**older, customer="+15550042", number=first.number + 1))
    assert (first.number, world.open("sms", customer="+15550043").number) == (1_000_000_000, 1_000_000_002)


def test_a_conversation_opens_after_postgres_ended_the_number_counts_connection(world):
    world.open("sms", customer="+15550061")
    with numbers._POOL.connect() as db:
        pid = db.exec_driver_sql("SELECT pg_backend_pid()").scalar()
    with core.tx() as db:
        assert db.execute(select(func.pg_terminate_backend(pid, 5000))).scalar()
    assert world.open("sms", customer="+15550062").number


def test_a_creation_in_flight_holds_nothing_another_creation_in_its_tenant_waits_on(world):
    with core.tx() as db:
        cv.open(db, world.tenant, channel="sms", direction="inbound", customer="+15550051", ours="+15550002")
        with ThreadPoolExecutor(1) as pool:
            assert pool.submit(world.open, "sms", customer="+15550052").result(timeout=2).number


def test_the_conversation_moves_only_along_its_table(world):
    conv = world.open(queued=False)
    with core.tx() as db:
        assert cv.set_state(db, conv, "closed") is None, "only a wrap-up closes"
        assert cv.set_state(db, conv, "queued").state == "queued"
        answered = cv.set_state(db, conv, "live").answered_at
    with core.tx() as db:
        assert cv.set_state(db, conv, "queued") and cv.set_state(db, conv, "live").answered_at == answered
        assert cv.set_state(db, conv, "live") is None, "a second answer is a no-op"
        cv.gone(db, conv)
        assert [cv.set_state(db, conv, to) for to in ("queued", "live", "open", "wrap_up")] == [None] * 4


def test_going_live_cancels_the_queue_timeout(world):
    conv = world.open()
    with core.tx() as db:
        jobs.schedule(db, "queue_timeout", {}, delay=20, key=str(conv.id))
    world.move(world.offer(conv, world.agent()), "accept")
    assert world.conv(conv.id).state == "live" and job("queue_timeout", conv.id) is None


def test_an_offer_announces_itself_and_arms_its_timeout(world):
    conv, agent = world.open(), world.agent()
    a = world.offer(conv, agent)
    [created] = world.events("offer.created")
    expected = {"offer_id": str(a.id), "channel": "call", "queue_id": str(world.queue), "from": "+15550001"}
    assert has(created.data, **expected, ring_timeout_sec=30, auto_accept=True)
    assert created.agent_id == str(agent) and a.state == "offered"
    assert timedelta(seconds=28) < job("offer_timeout", a.id).wait <= timedelta(seconds=30)
    text = world.offer(world.open("chat", customer="web-1", ours=""), world.agent())
    assert world.events("offer.created")[-1].data["ring_timeout_sec"] == 60
    assert job("offer_timeout", text.id) is not None


def test_accept_takes_the_round_withdraws_its_rivals_and_resets_misses(world):
    conv, mine, theirs = world.open(), world.agent(misses=1), world.agent()
    a, b = world.offer(conv, mine), world.offer(conv, theirs)
    assert world.move(a, "accept").state == "active"
    assert world.move(a, "accept") is None and world.move(b, "accept") is None, "duplicate and late accepts"
    assert world.of(b).reason == "taken" and world.misses(mine) == 0
    assert [e.data["offer_id"] for e in world.events("offer.accepted")] == [str(a.id)]
    [withdrawn] = world.events("offer.withdrawn")
    assert has(withdrawn.data, offer_id=str(b.id), reason="taken")
    assert (
        world.conv(conv.id).state == "live"
        and job("offer_timeout", a.id) is None
        and job("offer_timeout", b.id) is None
    )


def test_decline_ends_an_offer_once(world):
    conv = world.open()
    a = world.offer(conv, world.agent())
    assert world.move(a, "decline").state == "declined" and world.move(a, "decline") is None
    assert world.move(a, "accept") is None and job("offer_timeout", a.id) is None
    assert [e.data["offer_id"] for e in world.events("offer.rejected")] == [str(a.id)]


def test_a_missed_call_counts_a_strike_only_when_it_rang_out_and_a_missed_text_never_does(world):
    agent = world.agent()
    for cause in ("timeout", "6010", "2020"):
        a = world.offer(world.open(), agent)
        assert world.move(a, "miss", reason=cause).state == "missed" and world.move(a, "miss") is None
    assert [e.data["strike"] for e in world.events("offer.missed")] == [1, 2, 2]
    text = world.offer(world.open("sms"), agent)
    world.move(text, "miss")
    missed = world.events("offer.missed")[-1].data
    assert has(missed, offer_id=str(text.id), strike=None) and world.misses(agent) == 2
    taken = world.offer(world.open(), agent)
    world.move(taken, "accept")
    assert world.move(taken, "miss") is None, "a late timeout after the join"


def test_withdrawing_a_bound_leg_hangs_it_up(world):
    conv = world.open()
    bound, unbound = world.offer(conv, world.agent()), world.offer(conv, world.agent())
    with core.tx() as db:
        cv.edit(db, locked_row(db, bound), call_uuid="leg-1")
    for a in (bound, unbound):
        assert world.move(a, "withdraw", reason="caller_left").state == "withdrawn"
    assert job("plivo", f"hangup:{world.tenant}:leg-1").payload["call_uuid"] == "leg-1"
    mine = T.jobs.c.payload["conversation_id"].astext == str(conv.id)
    with core.read() as db:
        assert db.execute(select(func.count()).where(T.jobs.c.kind == "plivo", mine)).scalar() == 1


def test_direct_starts_active_and_the_conversation_live(world):
    conv = world.open("sms", queued=False)
    a = world.offer(conv, world.agent(), via="direct")
    assert a.state == "active" and a.started_at and job("offer_timeout", a.id) is None
    assert world.conv(conv.id).state == "live" and world.events("offer.created") == []


def test_leave_announces_the_deadline_arms_the_timer_and_frees_the_agent(world):
    _, a = world.live()
    left = world.move(a, "leave")
    [started] = world.events("conversation.wrap_up_started")
    deadline = left.ended_at + timedelta(seconds=900)
    assert started.data["mode"] == "manual" and datetime.fromisoformat(started.data["deadline_at"]) == deadline
    assert started.agent_id == str(a.agent_id)
    assert world.of(a).wrap_up_until == deadline and world.move(a, "leave") is None
    assert job("wrap_up_timeout", a.id).run_at == deadline + cv.SAFETY and job("route_agent", a.agent_id)
    world.settings(wrap_up={"mode": "auto", "seconds": 5, "max_extend_sec": 30}, acw_blocks_routing=True)
    _, b = world.live()
    left = world.move(b, "leave")
    assert job("wrap_up_timeout", b.id).run_at == left.ended_at + timedelta(seconds=5)
    assert job("route_agent", b.agent_id) is None, "after-call work blocks routing until the submit"


def test_end_frees_the_agent_without_a_wrap_up(world):
    _, a = world.live()
    assert world.move(a, "end").state == "done" and world.move(a, "end") is None
    assert job("route_agent", a.agent_id) and job("wrap_up_timeout", a.id) is None
    assert world.events("conversation.wrap_up_started") == []


def test_the_last_submit_after_the_customer_left_closes_once_with_every_disposition(world):
    conv, a = world.live()
    helper = world.move(world.offer(conv, world.agent(), role="secondary", via="participant"), "accept")
    with core.tx() as db:
        cv.gone(db, cv.lock(db, conv.id))
    assert world.conv(conv.id).state == "wrap_up" and world.conv(conv.id).result == "answered"
    assert world.move(a, "submit", disposition_id=world.sale, note="sold").state == "done"
    assert world.conv(conv.id).state == "wrap_up", "the helper is still wrapping up"
    world.move(helper, "submit", disposition_id=world.sale)
    assert world.move(helper, "submit", disposition_id=world.sale) is None
    [done] = [e for e in world.events("conversation.wrapped_up") if e.agent_id == str(a.agent_id)]
    sold = {"disposition_id": str(world.sale), "disposition": "Sale", "sub_disposition": None, "note": "sold"}
    assert has(done.data, **sold, system=False, skipped=False)
    [closed] = world.events("conversation.closed")
    assert closed.data["result"] == "answered" and closed.agent_id == str(a.agent_id)
    assert [d["agent_id"] for d in closed.data["dispositions"]] == [str(a.agent_id), str(helper.agent_id)]
    assert world.conv(conv.id).closed_at and job("search_index", conv.id)


def test_a_secondary_who_wraps_up_while_the_customer_is_on_leaves_the_conversation_live(world):
    conv, _ = world.live()
    helper = world.move(world.offer(conv, world.agent(), role="secondary", via="participant"), "accept")
    world.move(helper, "leave")
    world.move(helper, "submit", disposition_id=world.sale)
    assert world.conv(conv.id).state == "live" and world.events("conversation.closed") == []


def test_a_customer_gone_with_no_agent_closes_now_with_the_mapped_system_disposition(world):
    conv = world.open()
    a = world.offer(conv, world.agent())
    for _ in range(2):
        with core.tx() as db:
            cv.gone(db, cv.lock(db, conv.id))
    [withdrawn] = world.events("offer.withdrawn")
    assert withdrawn.data["offer_id"] == str(a.id) and withdrawn.data["reason"] == "caller_left"
    [closed] = world.events("conversation.closed")
    system = {"agent_id": None, "disposition_id": str(world.dropped), "sub_disposition_id": None, "system": True}
    system |= {"disposition": "Dropped", "sub_disposition": None, "skipped": False}
    assert closed.data["result"] == "abandoned" and closed.data["dispositions"] == [system]
    outbound = world.open(direction="outbound", queued=False)
    with core.tx() as db:
        cv.gone(db, cv.lock(db, outbound.id), "no_answer")
    assert world.events("conversation.closed")[-1].data["dispositions"][0]["disposition_id"] == str(world.unreached)
    dropped = world.open(direction="outbound", queued=False)
    with core.tx() as db:
        cv.gone(db, cv.lock(db, dropped.id))
    assert world.events("conversation.closed")[-1].data["dispositions"][0]["disposition_id"] == str(world.dropped), (
        "an outbound result missing from its map reads the inbound one"
    )


def test_a_result_the_tenants_stored_map_predates_takes_its_default_system_disposition(world):
    idle = world.open(channel="sms", queued=False)
    with core.tx() as db:
        cv.gone(db, cv.lock(db, idle.id), "session_timeout")
    assert world.events("conversation.closed")[-1].data["dispositions"][0]["disposition_id"] == str(world.answered)


def test_the_wrap_up_timeout_submits_the_results_system_disposition_as_skipped(world):
    conv, a = world.live()
    with core.tx() as db:
        cv.gone(db, cv.lock(db, conv.id))
    jobs.run_now("wrap_up_timeout", str(a.id), wait=True)
    [done] = world.events("conversation.wrapped_up")
    assert (done.data["disposition_id"], done.data["system"], done.data["skipped"]) == (str(world.answered), True, True)
    assert world.events("conversation.closed")[0].data["dispositions"][0]["skipped"] is True
    with core.tx() as db:
        late = {"conversation_id": str(conv.id), "assignment_id": str(a.id), "mode": "auto"}
        jobs.schedule(db, "wrap_up_timeout", late, key=str(a.id))
    jobs.run_now("wrap_up_timeout", str(a.id), wait=True)
    assert len(world.events("conversation.wrapped_up")) == 1, "a late timer finds nothing to submit"


def test_an_extend_committed_after_the_timer_was_claimed_wins(world):
    world.settings(wrap_up={"mode": "auto", "seconds": 10, "max_extend_sec": 30})
    conv, a = world.live()
    with core.tx() as db:
        cv.gone(db, cv.lock(db, conv.id))
    J = T.jobs
    claimed = jobs._claim(J.c.kind == "wrap_up_timeout", J.c.key == str(a.id))
    with core.tx() as db:
        cv.lock(db, conv.id)
        later = world.of(a).wrap_up_until + timedelta(seconds=10)
        a = cv.move(db, a.id, "extend", wrap_up_until=later)
        cv.wrap_up_timer(db, a, a.wrap_up_until, claimed.payload | {"extended": 10})
    jobs.work(claimed)
    assert world.of(a).state == "wrap_up" and world.events("conversation.wrapped_up") == []
    again = job("wrap_up_timeout", a.id)
    assert (again.locked_until, again.run_at, again.payload["extended"]) == (None, later, 10)
    first, second = (
        datetime.fromisoformat(e.data["deadline_at"]) for e in world.events("conversation.wrap_up_started")
    )
    assert second - first == timedelta(seconds=10)
    jobs.run_now("wrap_up_timeout", str(a.id), wait=True)
    assert world.of(a).state == "done" and len(world.events("conversation.wrapped_up")) == 1


def transfer(world, conv, **kw):
    return world.offer(conv, world.agent(), role="secondary", via="transfer", **kw)


def promote(conv, t):
    with core.tx() as db:
        return cv.promote(db, cv.lock(db, conv.id), t)


def test_a_transfer_target_offered_with_the_default_role_is_a_secondary_until_promoted(world):
    conv, primary = world.live()
    target = world.offer(conv, world.agent(), via="transfer", mode="cold")
    assert target.role == "secondary"
    assert promote(conv, target).role == "primary" and world.of(primary).state == "wrap_up"


def test_a_transfer_to_a_phone_stays_external_and_ends_without_a_wrap_up(world):
    conv, _ = world.live()
    phone = world.offer(conv, None, role="external", via="transfer", address="+15550003")
    assert phone.role == "external"
    world.move(phone, "accept")
    with core.tx() as db:
        cv.gone(db, cv.lock(db, conv.id))
    assert world.of(phone).state == "done"


def test_participant_invitations_do_not_compete(world):
    conv, _ = world.live()
    first, second = (world.offer(conv, world.agent(), role="secondary", via="participant") for _ in range(2))
    world.move(first, "accept")
    assert world.of(second).state == "offered" and world.events("offer.withdrawn") == []


def test_a_warm_transfer_completes_once(world):
    conv, primary = world.live()
    consult = world.move(transfer(world, conv, mode="warm"), "accept")
    assert consult.from_assignment == primary.id and states(conv)[1] == ("secondary", "active")
    done = promote(conv, consult)
    assert (done.role, done.state) == ("primary", "active") and promote(conv, consult) is None
    old = world.of(primary)
    assert (old.state, old.reason) == ("wrap_up", "transferred") and job("plivo", f"kick:{world.tenant}:{primary.id}")
    assert sorted(states(conv)) == [("primary", "active"), ("primary", "wrap_up")]


def test_promote_refuses_a_cancelled_target_and_one_from_an_older_primary(world):
    conv, primary = world.live()
    cancelled = transfer(world, conv)
    world.move(cancelled, "withdraw", reason="caller_left")
    assert promote(conv, cancelled) is None and world.of(primary).state == "active"
    first, second = (world.move(transfer(world, conv), "accept") for _ in range(2))
    assert promote(conv, first) and promote(conv, second) is None, "second came from the old primary"
    assert [r for r in states(conv) if r == ("primary", "active")] == [("primary", "active")]


def test_a_cold_team_transfer_promotes_the_first_to_join_and_withdraws_the_rest(world):
    conv, _ = world.live()
    first, second = transfer(world, conv, mode="cold"), transfer(world, conv, mode="cold")
    assert promote(conv, first).role == "primary"
    assert world.of(second).state == "withdrawn" and promote(conv, second) is None
    assert world.events("offer.accepted")[-1].data["offer_id"] == str(first.id)


def test_complete_racing_cancel_leaves_exactly_one_active_primary(world):
    for _ in range(5):
        conv, _ = world.live()
        target = world.move(transfer(world, conv), "accept")

        def complete(conv=conv, target=target):
            return promote(conv, target)

        def cancel(conv=conv, target=target):
            """The transfer's cancel: under the lock, a consult that is still one ends."""
            with core.tx() as db:
                cv.lock(db, conv.id)
                if locked_row(db, target).role == "secondary":
                    cv.move(db, target, "end")

        race(complete, cancel)
        active = [r for r in states(conv) if r == ("primary", "active")]
        assert len(active) == 1 and world.of(target).state in ("active", "done")


def test_two_conversations_ending_at_once_free_their_agents_in_one_order(world):
    """Each end frees both agents' shared route_agent rows; offered in opposite orders, they still lock in one, or
    one transaction dies on a deadlock (on unordered loops nearly every round did)."""

    def gone(conv):
        with core.tx() as db:
            cv.gone(db, cv.lock(db, conv.id))

    for _ in range(10):
        a, b, pair = world.agent(), world.agent(), []
        for first, second in ((a, b), (b, a)):
            conv = world.open("chat", customer=f"web-{first}", ours="")
            world.move(world.offer(conv, first, force=True), "accept")
            world.move(world.offer(conv, second, role="secondary", via="participant", force=True), "accept")
            pair.append(conv)
        race(*(lambda c=c: gone(c) for c in pair))
        assert [world.conv(c.id).state for c in pair] == ["wrap_up", "wrap_up"]


def test_depart_moves_the_room_on_and_takes_the_old_rooms_rows_off(world):
    conv, primary = world.live()
    ringing = world.offer(conv, world.agent(), role="secondary", via="participant")
    with core.tx() as db:
        cv.edit(db, locked_row(db, ringing), call_uuid="leg-2")
        moved = cv.depart(db, cv.lock(db, conv.id), {"type": "menu", "menu_id": "m"})
    assert (moved.state, moved.room) == ("open", 1)
    assert world.of(primary).state == "wrap_up" and world.of(ringing).state == "withdrawn"
    assert job("plivo", f"kick:{world.tenant}:{primary.id}").payload["room"] == 0 and job(
        "plivo", f"hangup:{world.tenant}:leg-2"
    )
    redirect = job("plivo", f"redirect:{world.tenant}:{conv.id}:1").payload
    assert (redirect["room"], redirect["call_uuid"], redirect["action"]) == (
        1,
        conv.call_uuid,
        {"type": "menu", "menu_id": "m"},
    )
    with core.tx() as db:
        assert cv.depart(db, cv.lock(db, conv.id), {"type": "hangup"}) is None, "the caller is in a menu, not a room"


def test_entries_dedupe_on_provider_id_within_a_conversation_and_reindex_a_closed_one(world):
    first, second = world.open("sms", customer="+1"), world.open("sms", customer="+2")
    with core.tx() as db:
        assert cv.entry(db, cv.lock(db, first.id), "message", {"text": "hi"}, provider_id="p-1")
        assert cv.entry(db, cv.lock(db, second.id), "message", {"text": "hi"}, provider_id="p-1")
        soon = datetime.now(UTC) + timedelta(hours=1)
        assert cv.entry(db, cv.lock(db, second.id), "note", {"text": "x"}, at=soon).at < soon, "a future time is now"
        later = cv.entry(db, cv.lock(db, first.id), "message", {"text": "yo"}, provider_id="p-2", status="delivered")
        assert cv.entry(db, cv.lock(db, first.id), "message", {"text": "yo"}, provider_id="p-2", status="sent") is None
    assert row(T.entry, later.id).status == "delivered", "an earlier callback arriving late changes nothing"
    with core.tx() as db:
        cv.gone(db, cv.lock(db, first.id))
        jobs.cancel(db, "search_index", str(first.id))
        cv.entry(db, cv.lock(db, first.id), "note", {"text": "late"})
    assert job("search_index", first.id) and world.conv(first.id).version > first.version


def test_a_chat_customer_ends_the_waiting_mark_and_restarts_the_session_only_within_25_hours(world):
    conv, _ = world.live("chat")

    def act(fn, *args):
        with core.tx() as db:
            fn(db, cv.lock(db, conv.id), *args)

    def said(direction: str):
        act(cv.entry, "message", {"direction": direction})

    def marked() -> bool:
        with core.read() as db:
            return cv.view(db, world.conv(conv.id))["waiting"]

    def session_nearly_over():
        J = T.jobs
        with core.tx() as db:
            ours = (J.c.kind == "session_timeout", J.c.key == str(conv.id))
            db.execute(update(J).where(*ours).values(run_at=func.now() + timedelta(seconds=10)))

    act(cv.wait, True)
    session_nearly_over()
    said("inbound")
    assert not marked()
    assert job("session_timeout", conv.id).wait > timedelta(minutes=29), "the customer's return restarts the session"
    act(cv.wait, True)
    with core.tx() as db:
        C, ended = T.conversation, (datetime.now(UTC) - timedelta(minutes=1)).isoformat()
        db.execute(update(C).where(C.c.id == conv.id).values(routing=C.c.routing.concat({"waiting": ended})))
    said("inbound")
    assert marked(), "past 25 hours the customer's message leaves the mark"
    said("outbound")
    assert not marked()
    act(cv.wait, True)
    session_nearly_over()
    act(cv.wait, False)
    assert not marked() and job("session_timeout", conv.id).wait > timedelta(minutes=29), "an unmark restarts it"


def test_the_offline_sweep_never_deadlocks_with_accepts_and_leaves_no_offer_behind(world):
    """ARCHITECTURE 3.5: the sweep sets Offline on the presence row alone, then withdraws per conversation; joins
    race it. Each offer ends exactly one way, and the conversation agrees with it."""
    agent, convs = world.agent(), [world.open() for _ in range(8)]
    offers = [world.offer(conv, agent, force=True) for conv in convs]

    def sweep():
        with core.tx() as db:
            P = T.presence
            db.execute(select(P).where(P.c.agent_id == agent).with_for_update())
            db.execute(update(P).where(P.c.agent_id == agent).values(status_id=uuid.uuid4()))
        with core.read() as db:
            held = cv.desk(db, world.tenant, agent)
        return [(a.id, "withdraw", world.move(a, "withdraw", reason="caller_left")) for a in held]

    def join(a):
        return [(a.id, "accept", world.move(a, "accept"))]

    won = [one for ran in race(sweep, *(lambda a=a: join(a) for a in offers)) for one in ran]
    for conv, a in zip(convs, offers, strict=True):
        [winner] = [event for id_, event, moved in won if id_ == a.id and moved is not None]
        assert (winner, world.conv(conv.id).state) in (("accept", "live"), ("withdraw", "queued"))
        assert world.of(a).state != "offered"


def test_view_is_the_read_model(world):
    conv, primary = world.live()
    world.offer(conv, world.agent())
    with core.tx() as db:
        db.execute(update(T.assignment).where(T.assignment.c.id == primary.id).values(call_uuid="leg-a"))
        db.execute(update(T.conversation).where(T.conversation.c.id == conv.id).values(attributes={"csat": 5}))
        locked = cv.lock(db, conv.id)
        note = cv.entry(db, locked, "note", {"text": "refund"}, agent_id=primary.agent_id)
        kept = cv.entry(db, locked, "recording", {"plivo_id": "r1"})
        gone = cv.entry(db, locked, "recording", {"plivo_id": "r2"})
        db.execute(update(T.entry).where(T.entry.c.id == gone.id).values(deleted_at=func.now()))
        cv.gone(db, locked)
        doc = cv.view(db, cv.lock(db, conv.id))
    assert (doc["id"], doc["conversation_number"], doc["state"]) == (str(conv.id), str(conv.number), "in_wrap_up")
    assert (doc["channel"], doc["direction"], doc["number"]) == ("call", "inbound", "+15550001")
    assert doc["agent_ids"] == [str(primary.agent_id)], "only who took it, not who it rang"
    assert [(leg["call_uuid"], leg["role"]) for leg in doc["legs"]] == [
        (conv.call_uuid, "customer"),
        ("leg-a", "primary"),
    ]
    assert doc["notes"] == [
        {
            "id": str(note.id),
            "text": "refund",
            "attachments": [],
            "agent_id": str(primary.agent_id),
            "created_at": note.at,
            "updated_at": note.at,
        }
    ]
    assert doc["recording_ids"] == [str(kept.id)] and doc["attributes"] == {"csat": 5}
    assert (doc["queue_id"], doc["ended_at"], doc["dispositions"]) == (str(world.queue), None, [])
    with core.read() as db:
        listed = db.execute(cv.summary().where(T.conversation.c.id == conv.id)).one()._mapping
    assert {k: doc[k] for k in listed} == dict(listed)
