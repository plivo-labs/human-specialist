import threading
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import timedelta
from types import SimpleNamespace

import pytest
import sqlalchemy
from fastapi.testclient import TestClient
from sqlalchemy import func, select, text, update

from hs import access, core, jobs, routing
from hs import app as service
from hs import conversations as cv
from hs.config import models as m
from hs.core import T
from hs.voice import flow, mpc
from tests.unit.tenants import Tenant
from tests.unit.test_config import held, waiter


def joined(tenant: Tenant, conv, call_uuid: str, member: str) -> None:
    form = {"EventName": "ParticipantJoin", "ParticipantCallUUID": call_uuid, "MemberID": member}
    mpc.mpc_status(conv.id, conv.room, tenant=SimpleNamespace(id=tenant.id), form=form)


@pytest.mark.parametrize("name", mpc.IDLE)
def test_a_room_event_no_branch_acts_on_sends_no_statement(name):
    t, sent = Tenant(), []
    with core.tx() as db:
        conv = cv.lock(db, t.open().id)

    def count(conn, cursor, statement, *_):
        sent.append(statement)

    sqlalchemy.event.listen(core.engine, "before_cursor_execute", count)
    try:
        form = {"EventName": name, "ParticipantCallUUID": uuid.uuid4().hex}
        answered = mpc.mpc_status(conv.id, conv.room, tenant=SimpleNamespace(id=t.id), form=form)
    finally:
        sqlalchemy.event.remove(core.engine, "before_cursor_execute", count)
    assert (answered.status_code, sent) == (200, [])


def test_a_primary_offer_that_joins_while_another_primary_is_on_the_call_is_withdrawn_and_kicked():
    t = Tenant()
    first, second = t.agent(), t.agent()
    legs = {"on_call": uuid.uuid4().hex, "late": uuid.uuid4().hex}
    with core.tx() as db:
        conv = cv.lock(db, t.open().id)
        on_call = cv.move(db, cv.offer(db, conv, first), "accept")
        late = cv.offer(db, conv, second)
        for a, leg in ((on_call, legs["on_call"]), (late, legs["late"])):
            cv.edit(db, a, call_uuid=leg)
    joined(t, conv, legs["on_call"], "11")
    joined(t, conv, legs["late"], "12")
    rows = seen(conv)
    with core.read() as db:
        kicked = db.execute(select(T.jobs.c.payload).where(T.jobs.c.key == f"kick:{t.id}:{late.id}")).scalar()
    assert (rows[on_call.id].state, rows[on_call.id].member_id) == ("active", "11")
    assert (rows[late.id].state, rows[late.id].reason) == ("withdrawn", "taken") and kicked["member_id"] == "12"


def test_an_add_member_failure_before_the_ring_reply_waits_for_its_legs_then_misses_the_offer():
    t, ring, address, leg = Tenant(), uuid.uuid4(), "sip:agent1@phone.plivo.com", uuid.uuid4().hex
    with core.tx() as db:
        conv = cv.lock(db, t.open().id)
        offer = cv.edit(db, cv.offer(db, conv, t.agent()), ring=ring, address=address)
    form = {"EventName": "AddParticipantByAPIActionCompleted", "AddParticipantAPIRequestOutcome": mpc.FAILED}
    form["AddParticipantAPICallUUIDs"] = leg
    with pytest.raises(core.ApiError) as early:
        mpc.mpc_status(conv.id, conv.room, tenant=SimpleNamespace(id=t.id), form=form)
    mpc._rung({"conversation_id": str(conv.id), "ring": str(ring)}, {"calls": [{"call_uuid": leg, "to": address}]})
    mpc.mpc_status(conv.id, conv.room, tenant=SimpleNamespace(id=t.id), form=form)
    assert early.value.status == 503 and seen(conv)[offer.id].state == "missed"


def test_a_leg_whose_ring_reply_comes_after_its_offer_was_withdrawn_is_known_at_its_join_and_not_kicked():
    """Main's withdrawn row kept the leg's uuid: the leg is hung up again on each call-status, and its join is not a
    stranger's."""
    t, ring, address, leg = Tenant(), uuid.uuid4(), "sip:agent1@phone.plivo.com", uuid.uuid4().hex
    with core.tx() as db:
        conv = cv.lock(db, t.open().id)
        offer = cv.edit(db, cv.offer(db, conv, t.agent()), ring=ring, address=address)
    with core.tx() as db:
        now = cv.rows(db, cv.lock(db, conv.id).id)
        cv.move(db, next(a for a in now if a.id == offer.id), "withdraw", reason="caller_left")
    mpc._rung({"conversation_id": str(conv.id), "ring": str(ring)}, {"calls": [{"call_uuid": leg, "to": address}]})
    joined(t, conv, leg, "12")
    with core.read() as db:
        kicked = db.execute(select(T.jobs.c.id).where(T.jobs.c.key == f"kick:{t.id}:{leg}")).first()
    assert (seen(conv)[offer.id].state, seen(conv)[offer.id].call_uuid, kicked) == ("withdrawn", leg, None)


def seen(conv) -> dict:
    """The conversation's stints and offers, ended ones included, by id, read through the seam."""
    with core.read() as db:
        conv = db.execute(select(T.conversation).where(T.conversation.c.id == conv.id)).one()
        return {r.id: r for r in cv.rows(db, conv, ended=True)}


def queued(t: Tenant, wait: int, *, edited: int, ago: int):
    """A caller its queue took `ago` seconds since with a wait timeout of `wait`, edited to `edited` since then."""
    C, J, D = T.conversation, T.jobs, T.config
    with core.tx() as db:
        q = t.doc(db, "queue", **m.QueueIn(name="Q", wait_timeout_sec=wait).model_dump(mode="json"))
    conv = t.open(queued=False)
    with core.tx() as db:
        routing.enqueue(db, cv.lock(db, conv.id), q)
        db.execute(update(D).where(D.c.id == q).values(body=D.c.body.concat({"wait_timeout_sec": edited})))
        back = timedelta(seconds=ago)
        db.execute(update(J).where(J.c.key == str(conv.id)).values(run_at=J.c.run_at - back))
        return db.execute(
            update(C).where(C.c.id == conv.id).values(queued_at=C.c.queued_at - back).returning(*C.c)
        ).one()


def state(cid) -> tuple:
    with core.read() as db:
        return tuple(
            db.execute(select(T.conversation.c.state, T.conversation.c.result).where(T.conversation.c.id == cid)).one()
        )


def checked(t: Tenant, conv, monkeypatch) -> tuple:
    monkeypatch.setattr(mpc.plivo, "rest", lambda *_, **__: {"status": "Ended", "objects": []})
    mpc._check(SimpleNamespace(payload={"tenant_id": str(t.id), "conversation_id": str(conv.id), "room": conv.room}))
    return state(conv.id)


def test_a_waited_out_callers_late_queue_exit_reads_queue_timeout_after_the_room_check_saw_its_room_end(monkeypatch):
    t = Tenant()
    conv = queued(t, 600, edited=600, ago=601)
    assert checked(t, conv, monkeypatch) == ("queued", None)
    exit = {"CallUUID": conv.call_uuid, "ParticipantExitCause": flow.WAIT_TIMEOUT}
    flow.queue_exit(conv.room, tenant=SimpleNamespace(id=t.id), form=exit)
    assert state(conv.id) == ("open", "queue_timeout")


@pytest.mark.parametrize(
    ("wait", "edited", "after"), [(60, 600, ("queued", None)), (600, 60, ("closed", "abandoned"))], ids=["up", "down"]
)
def test_the_room_check_decides_by_the_wait_timeout_the_callers_stint_began_with_whatever_its_queue_says_now(
    monkeypatch, wait, edited, after
):
    t = Tenant()
    assert checked(t, queued(t, wait, edited=edited, ago=120), monkeypatch) == after


def test_a_caller_requeued_while_its_last_stints_timer_was_claimed_is_gone_by_the_room_check(monkeypatch):
    t = Tenant()
    first = queued(t, 60, edited=60, ago=120)
    assert jobs._claim(T.jobs.c.kind == "queue_timeout", T.jobs.c.key == str(first.id))
    with core.tx() as db:
        q = t.doc(db, "queue", **m.QueueIn(name="Q2", wait_timeout_sec=600).model_dump(mode="json"))
        cv.depart(db, cv.lock(db, first.id), {"type": "queue", "queue_id": str(q)})
        routing.enqueue(db, cv.lock(db, first.id), q)
        again = cv.lock(db, first.id)
    assert checked(t, again, monkeypatch) == ("closed", "abandoned")


CALLER = "m-caller"


def waiting(t: Tenant, monkeypatch, *prompts: dict):
    """A caller seated in a queue with these wait prompts, in a room Plivo lists it in, with every command run as its
    transaction commits, as the job pool runs it. Returns the conversation, the texts Plivo was asked to speak, and
    `cuts`: a (callback, spoken) put there runs inside the next command's after-commit work, just before that command
    reaches Plivo, or just after it if `spoken`."""
    said, cuts, run_now = [], [], jobs.run_now

    def rest(tenant, method, path, **params):
        if path.endswith("/Speak/"):
            said.append(params["text"])
        room = path.startswith("MultiPartyCall/") and int(path.split("/")[1].rsplit("-", 1)[1])
        listed = [{"call_uuid": conv.call_uuid, "member_id": CALLER}] if room == seat.room else []
        return {"objects": listed} if method == "GET" else {}

    def run(kind, key, *where, wait=False):
        cut, spoken = cuts.pop() if cuts else (None, False)
        if cut and not spoken:
            cut()
        done = run_now(kind, key, *where, wait=True)
        if cut and spoken:
            cut()
        return done

    monkeypatch.setattr(mpc.plivo, "rest", rest)
    monkeypatch.setattr(jobs, "run_now", run)
    with core.tx() as db:
        q = t.doc(db, "queue", **m.QueueIn(name="P", wait_prompts=list(prompts)).model_dump(mode="json"))
    conv = t.open(queued=False)
    with core.tx() as db:
        flow.run(db, cv.lock(db, conv.id), {"type": "queue", "queue_id": str(q)}, True)
        seat = conv = cv.lock(db, conv.id)
    return conv, said, cuts


def event(t: Tenant, conv, name: str, seq: int, *, caller: bool = True) -> None:
    form = {"EventName": name, "SequenceNumber": str(seq)}
    if caller:
        form |= {"ParticipantCallUUID": conv.call_uuid, "MemberID": CALLER}
    mpc.mpc_status(conv.id, conv.room, tenant=SimpleNamespace(id=t.id), form=form)


def due(conv, within: float) -> None:
    J = T.jobs
    soon = (J.c.payload["conversation_id"].astext == str(conv.id), J.c.kind != "plivo")
    with core.read() as db:
        found = db.execute(select(J.c.kind, J.c.key).where(*soon, J.c.run_at <= func.now() + timedelta(seconds=within)))
        for kind, key in found.all():
            jobs.run_now(kind, key, wait=True)


@pytest.mark.parametrize("spoken", [False, True], ids=["sent_after", "sent_before"])
def test_a_wait_prompt_chosen_before_mpc_start_never_reaches_plivo_after_it_and_no_prompt_follows(monkeypatch, spoken):
    t = Tenant()
    conv, said, cuts = waiting(t, monkeypatch, {"text": "One"}, {"text": "Two", "loop": True})
    event(t, conv, "MPCInitialized", 1)
    cuts.append((lambda: event(t, conv, "MPCStart", 3, caller=False), spoken))
    event(t, conv, "ParticipantSpeakStopped", 4)
    event(t, conv, "ParticipantSpeakStopped", 6)
    assert said == ["One", "Two"][: 1 + spoken]


def test_mpc_initialized_and_the_recovery_that_overlaps_it_speak_one_first_prompt(monkeypatch):
    t = Tenant()
    conv, said, cuts = waiting(t, monkeypatch, {"text": "Welcome"})
    cuts.append((lambda: (event(t, conv, "ParticipantJoin", 2), due(conv, 10)), True))
    event(t, conv, "MPCInitialized", 1)
    assert said == ["Welcome"]


def test_the_recovery_of_a_room_the_caller_has_left_speaks_to_no_one(monkeypatch):
    t = Tenant()
    conv, said, _ = waiting(t, monkeypatch, {"text": "Welcome"})
    event(t, conv, "ParticipantJoin", 2)
    again = {"type": "queue", "queue_id": str(conv.queue_id)}
    with core.tx() as db:
        cv.depart(db, cv.lock(db, conv.id), again)
        flow.run(db, cv.lock(db, conv.id), again, True)
    due(conv, 10)
    assert said == []


def test_a_first_prompt_sent_after_the_caller_moved_rooms_neither_speaks_nor_restarts_the_new_rooms(monkeypatch):
    t = Tenant()
    conv, said, cuts = waiting(t, monkeypatch, {"text": "One"}, {"text": "Two", "loop": True})
    again, moved = {"type": "queue", "queue_id": str(conv.queue_id)}, []

    def move() -> None:
        with core.tx() as db:
            cv.depart(db, cv.lock(db, conv.id), again)
            flow.run(db, cv.lock(db, conv.id), again, True)
            moved.append(cv.lock(db, conv.id))
        event(t, moved[0], "MPCInitialized", 1)

    cuts.append((move, False))
    event(t, conv, "MPCInitialized", 1)
    event(t, moved[0], "ParticipantSpeakStopped", 3)
    assert said == ["One", "Two"]


@pytest.mark.parametrize("taken", [True, False], ids=["reply_lost", "never_sent"])
def test_a_wait_prompt_plivo_may_have_spoken_is_not_sent_again_and_one_that_never_reached_it_is(monkeypatch, taken):
    t = Tenant()
    conv, said, _ = waiting(t, monkeypatch, {"text": "One"}, {"text": "Two", "loop": True})
    rest, failed = mpc.plivo.rest, []

    def once(tenant, method, path, **params):
        if not path.endswith("/Speak/") or failed:
            return rest(tenant, method, path, **params)
        failed.append(path)
        if not taken:
            raise mpc.plivo.Refused(503, "plivo_unavailable", "Plivo did not answer")
        rest(tenant, method, path, **params)
        raise mpc.plivo.Uncertain(f"{method} {path}: ReadTimeout")

    monkeypatch.setattr(mpc.plivo, "rest", once)
    event(t, conv, "MPCInitialized", 1)
    jobs.run_now("plivo", f"prompt:{t.id}:{conv.id}:{conv.room}", wait=True)
    event(t, conv, "ParticipantSpeakStopped", 3)
    assert said == ["One", "Two"]


@pytest.mark.parametrize("died", [False, True], ids=["after_commit_ran", "died_after_commit"])
def test_a_wait_prompt_queued_as_the_caller_leaves_its_queue_is_not_spoken_before_any_next_room_starts(
    monkeypatch, died
):
    t = Tenant()
    conv, said, cuts = waiting(t, monkeypatch, {"text": "One"}, {"text": "Two", "loop": True})

    def leave() -> None:
        with core.tx() as db:
            cv.depart(db, cv.lock(db, conv.id), {"type": "queue", "queue_id": str(conv.queue_id)})
            if died:
                db.info["after_commit"].clear()

    event(t, conv, "MPCInitialized", 1)
    cuts.append((leave, False))
    event(t, conv, "ParticipantSpeakStopped", 3)
    assert said == ["One"]


def test_an_agent_whose_hangup_lands_after_the_caller_moved_rooms_is_not_on_the_call_in_the_new_room():
    t = Tenant()
    with core.tx() as db:
        conv = cv.lock(db, t.open().id)
        stint = cv.move(db, cv.offer(db, conv, t.agent()), "accept")
        cv.depart(db, conv, {"type": "queue", "queue_id": str(uuid.uuid4())}, "agent")
        cv.move(db, stint, "leave", reason="left")
    with core.tx() as db:
        db.execute(
            update(T.assignment).where(T.assignment.c.id == stint.id).values(ended_at=func.now() + timedelta(seconds=1))
        )
        conv = cv.lock(db, conv.id)
        assert cv.on_call(conv, cv.rows(db, conv.id)) is None


def test_a_told_stint_is_still_told_after_the_row_depart_returns_is_queued_again():
    t = Tenant()
    with core.tx() as db:
        conv = cv.lock(db, t.open().id)
        stint = cv.move(db, cv.offer(db, conv, t.agent()), "accept")
        queue = t.doc(db, "queue", **m.QueueIn(name="Second").model_dump(mode="json"))
        moved = cv.depart(db, conv, {"type": "queue", "queue_id": str(queue)}, "agent")
        routing.enqueue(db, moved, queue)
    with core.tx() as db:
        cv.move(db, stint.id, "leave", reason="left")
        db.execute(
            update(T.assignment).where(T.assignment.c.id == stint.id).values(ended_at=func.now() + timedelta(seconds=1))
        )
        assert cv.on_call(cv.lock(db, conv.id), cv.rows(db, conv.id)) is None


@pytest.mark.parametrize("exits", ["before", "after"])
def test_the_callers_exit_from_a_room_it_left_for_another_queue_still_ends_its_leg(exits):
    t, time = Tenant(), "2026-11-07 10:00:00"
    with core.tx() as db:
        handed_back = m.QueueIn(name="First", after_agent_action={"type": "hangup"}).model_dump(mode="json")
        first = t.doc(db, "queue", **handed_back)
        conv = cv.lock(db, t.open().id)
        db.execute(update(T.conversation).where(T.conversation.c.id == conv.id).values(queue_id=first))
        cv.move(db, cv.offer(db, conv, t.agent()), "accept")
    form = {"EventName": "ParticipantExit", "ParticipantCallUUID": conv.call_uuid, "ParticipantExitTime": time}
    deliver = lambda: mpc.mpc_status(conv.id, conv.room, tenant=SimpleNamespace(id=t.id), form=form)  # noqa: E731
    if exits == "before":
        deliver()
    with core.tx() as db:
        queue = t.doc(db, "queue", **m.QueueIn(name="Second").model_dump(mode="json"))
        moved = cv.depart(db, cv.lock(db, conv.id), {"type": "queue", "queue_id": str(queue)}, "agent")
        routing.enqueue(db, moved, queue)
    if exits == "after":
        deliver()
    with core.tx() as db:
        cv.gone(db, cv.lock(db, conv.id))
        leg = cv.view(db, cv.lock(db, conv.id))["legs"][0]
    assert leg["ended_at"].startswith("2026-11-07T10:00:00")


def test_two_rings_to_one_number_read_its_alias_from_plivo_once(monkeypatch):
    t, reads = Tenant(), []

    def rest(tenant, method, path, **_):
        reads.append(path)
        return {"alias": ""}

    monkeypatch.setattr(mpc.plivo, "rest", rest)
    row = SimpleNamespace(id=t.id)
    assert [mpc._number_alias(row, "+14155550100") for _ in range(2)] == ["", ""]
    assert reads == ["Number/14155550100/"]


def test_a_failed_ring_of_several_legs_and_the_delete_of_its_rona_status_both_commit():
    """Agent b moved to the RONA status while its offer rang; agent a meets the strikes when the ring fails. The delete
    holds the tenant's presence rows while it waits to repoint the RONA setting; the ring's end waits for it."""
    t, P = Tenant(), T.presence
    a, b = t.agent(), t.agent()
    with core.tx() as db:
        brk = t.doc(db, "status", name="Break", availability=False, default=True)
        lunch = t.doc(db, "status", name="Lunch", availability=False)
        free = t.doc(db, "status", name="Ready", availability=True)
        db.execute(update(P).where(P.c.agent_id == a).values(status_id=free, misses=1))
        db.execute(update(P).where(P.c.agent_id == b).values(status_id=lunch))
        rona = T.tenant.c.settings.concat({"rona": {"strikes": 1, "status_id": str(lunch)}})
        db.execute(update(T.tenant).where(T.tenant.c.id == t.id).values(settings=rona))
        key = access.new_key(db, t.id, ["admin"])["api_key"]
    with core.tx() as db:
        conv = cv.lock(db, t.open().id)
        for agent in (a, b):
            cv.offer(db, conv, agent)
    api = TestClient(service.app, headers={"Authorization": f"Bearer {key}"}, raise_server_exceptions=False)

    def ring_failed() -> None:
        with core.tx() as db:
            mpc._ended(db, cv.lock(db, conv.id), cv.rows(db, conv.id, ("offered",)), "miss", reason=mpc.FAILED)

    tenant = select(T.tenant.c.id).where(T.tenant.c.id == t.id).with_for_update(key_share=True)
    ready, release = Future(), threading.Event()
    with ThreadPoolExecutor(3) as pool:
        try:
            pool.submit(held, tenant, ready, release)
            deleting = pool.submit(api.delete, f"/api/v1/agent-statuses/{lunch}")
            deleter = waiter(ready.result(5))
            assert deleter, "the delete holds the presence rows and waits for the settings"
            ending = pool.submit(ring_failed)
            assert waiter(deleter), "the ring's end waits for the delete"
        finally:
            release.set()
    ending.result()
    assert deleting.result().status_code == 204
    with core.read() as db:
        assert db.execute(select(P.c.status_id).where(P.c.agent_id == b)).scalar() == brk


def blocked(pid: int) -> bool:
    """True once backend `pid` waits on a lock; False after 5 s."""
    deadline, waits = time.monotonic() + 5, text("SELECT cardinality(pg_blocking_pids(:pid)) > 0")
    with core.read() as db:
        while time.monotonic() < deadline:
            if db.execute(waits, {"pid": pid}).scalar():
                return True
            time.sleep(0.02)
    return False


def test_a_move_into_a_status_being_deleted_a_failed_ring_and_the_delete_all_commit():
    """Agent b sorts first and sits in status s. Agent a is moved into s while a failed ring of both holds b's presence
    and waits for a's, and the delete of s waits to repoint the RONA setting that names it, then for b's presence."""
    t, P = Tenant(), T.presence
    b, a = sorted((t.agent(), t.agent()), key=str)
    with core.tx() as db:
        brk = t.doc(db, "status", name="Break", availability=False, default=True)
        lunch = t.doc(db, "status", name="Lunch", availability=False)
        free = t.doc(db, "status", name="Ready", availability=True)
        db.execute(update(P).where(P.c.agent_id == a).values(status_id=free))
        db.execute(update(P).where(P.c.agent_id == b).values(status_id=lunch))
        rona = T.tenant.c.settings.concat({"rona": {"strikes": 0, "status_id": str(lunch)}})
        db.execute(update(T.tenant).where(T.tenant.c.id == t.id).values(settings=rona))
        key = access.new_key(db, t.id, ["admin"])["api_key"]
    with core.tx() as db:
        conv = cv.lock(db, t.open().id)
        for agent in (a, b):
            cv.offer(db, conv, agent)
    api = TestClient(service.app, headers={"Authorization": f"Bearer {key}"}, raise_server_exceptions=False)
    ending_pid = Future()

    def ring_failed() -> None:
        with core.tx() as db:
            ending_pid.set_result(db.exec_driver_sql("SELECT pg_backend_pid()").scalar())
            mpc._ended(db, cv.lock(db, conv.id), cv.rows(db, conv.id, ("offered",)), "miss", reason=mpc.FAILED)

    tenant = select(T.tenant.c.id).where(T.tenant.c.id == t.id).with_for_update(key_share=True)
    ready, release = Future(), threading.Event()
    with ThreadPoolExecutor(4) as pool:
        try:
            pool.submit(held, tenant, ready, release)
            deleting = pool.submit(api.delete, f"/api/v1/agent-statuses/{lunch}")
            deleter = waiter(ready.result(5))
            assert deleter, "the delete waits for the settings"
            moving = pool.submit(api.put, f"/api/v1/agents/{a}/status", json={"status_id": str(lunch)})
            assert waiter(deleter), "the move waits for the delete"
            ending = pool.submit(ring_failed)
            assert blocked(ending_pid.result(5)), "the ring's end waits"
        finally:
            release.set()
    ending.result()
    assert (deleting.result().status_code, moving.result().status_code) == (204, 200)
    with core.read() as db:
        now = dict(db.execute(select(P.c.agent_id, P.c.status_id).where(P.c.agent_id.in_([a, b]))).all())
    assert now == {a: free, b: brk}
