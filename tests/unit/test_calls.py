"""Transfers, participants and supervisors on the migrated database: one test per row of the transfers,
participants and monitor table, each with a replay of the callback that moves it. The room's callbacks are called
as Plivo sends them, and Plivo's REST calls are only recorded."""

import copy
import json
import threading
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from functools import partial
from types import SimpleNamespace

import pytest
from sqlalchemy import delete, event, func, insert, select, text, update

from hs import access, agents, config, core, hot, jobs, plivo
from hs import conversations as cv
from hs.access import Principal
from hs.config import models as m
from hs.core import ApiError, T
from hs.hot import rebuilt
from hs.voice import calls, flow, media, mpc, outbound
from tests.unit.tenants import Tenant
from tests.unit.test_config import waiter
from tests.unit.test_routing import CAP, while_held

NUMBER = "+14155550123"
WAITING = text(
    "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND cardinality(pg_blocking_pids(pid)) > 0"
)


class Call:
    """A tenant whose agent took an inbound call from its queue; the agents it adds are available."""

    def __init__(self, monkeypatch):
        self.sent, self.members = [], []
        monkeypatch.setattr(plivo, "rest", self.rest)
        with core.tx() as db:
            row = insert(T.tenant).values(name="t", plivo_auth_id=uuid.uuid4().hex, plivo_token_enc="sealed")
            self.tenant = db.execute(row.returning(T.tenant.c.id)).scalar()
            rebuilt(self.tenant)
            self.available = self.doc(db, "status", {"name": "Available", "availability": True})
            self.queue = self.doc(db, "queue", m.QueueIn(name="Q").model_dump(mode="json"))
            settings = m.Stored(rona={"status_id": str(uuid.uuid4())}).model_dump(mode="json")
            db.execute(update(T.tenant).where(T.tenant.c.id == self.tenant).values(settings=settings))
        self.key = Principal(str(self.tenant), "key", scopes=frozenset({"calls"}))
        self.answer(self.agent())

    def answer(self, agent_id) -> None:
        """A new inbound call from the queue, which `agent_id` takes: this object's call from now on."""
        self.agent_id = agent_id
        with core.tx() as db:
            parties = {"direction": "inbound", "customer": "+15550001", "ours": "+15550002"}
            conv = cv.open(db, self.tenant, channel="call", call_uuid=uuid.uuid4().hex, **parties)
            cv.set_state(db, conv, "queued", queue_id=self.queue, queued_at=func.now())
            self.cid, self.caller = conv.id, conv.call_uuid
            self.primary = cv.move(db, cv.offer(db, cv.lock(db, conv.id), self.agent_id), "accept")
        self.join(self.primary)

    def rest(self, tenant, method: str, path: str, **params) -> dict:
        self.sent.append((method, path, params))
        return {"objects": self.members} if method == "GET" else {}

    def doc(self, db, kind: str, body: dict) -> uuid.UUID:
        return db.execute(
            insert(T.config).values(tenant_id=self.tenant, kind=kind, body=body).returning(T.config.c.id)
        ).scalar()

    def agent(self, *teams, role: str = "agent") -> uuid.UUID:
        with core.tx() as db:
            body = {"name": "A", "role": role, "teams": [str(t) for t in teams], "endpoint_username": uuid.uuid4().hex}
            agent = self.doc(db, "agent", body)
            db.execute(insert(T.presence).values(agent_id=agent, tenant_id=self.tenant, status_id=self.available))
        return agent

    def team(self) -> uuid.UUID:
        with core.tx() as db:
            return self.doc(db, "team", {"name": "T"})

    def conv(self):
        with core.read() as db:
            return db.execute(select(T.conversation).where(T.conversation.c.id == self.cid)).one()

    def rows(self, **match) -> list:
        """The call's stints, open offers and ended offers' tombstones, read through the seam, that `match`."""
        with core.read() as db:
            every = cv.rows(
                db, db.execute(select(T.conversation).where(T.conversation.c.id == self.cid)).one(), ended=True
            )
        return [a for a in every if all(getattr(a, k) == v for k, v in match.items())]

    def row(self, a):
        return self.rows(id=a.id)[0]

    def bind(self, *rows) -> None:
        """Each row's leg gets a call uuid of its own, as the ring's reply gives it."""
        with core.tx() as db:
            for a in cv.rows(db, cv.lock(db, self.cid).id):
                if a.id in {r.id for r in rows} and a.call_uuid is None:
                    cv.edit(db, a, call_uuid=uuid.uuid4().hex)

    def event(self, name: str, a, room: int | None = None) -> None:
        """Plivo's MPC event `name` for the leg of row `a`."""
        self.bind(a)
        leg = self.row(a)
        form = {"EventName": name, "ParticipantCallUUID": leg.call_uuid, "MemberID": f"m-{leg.id}"}
        room = self.conv().room if room is None else room
        mpc.mpc_status(self.cid, room, tenant=SimpleNamespace(id=self.tenant), form=form)

    def join(self, a) -> None:
        self.event("ParticipantJoin", a)

    def exit(self, a) -> None:
        self.event("ParticipantExit", a)

    def events(self, type_: str, **match) -> list[dict]:
        E = T.events
        with core.read() as db:
            found = select(E).where(E.c.conversation_id == str(self.cid), E.c.type == type_).order_by(E.c.id)
            told = [{"agent_id": e.agent_id, **e.data} for e in db.execute(found)]
        return [e for e in told if e.items() >= match.items()]

    def job(self, key: str, kind: str = "plivo"):
        """The job `key`; a Plivo command's `<op>:<subject>` is this tenant's."""
        op, _, subject = key.partition(":")
        key = f"{op}:{self.tenant}:{subject}" if kind == "plivo" else key
        J = T.jobs
        with core.read() as db:
            return db.execute(select(J).where(J.c.kind == kind, J.c.key == key)).first()

    def transfer(self, mode: str, target: dict, p: Principal | None = None) -> str:
        body = calls.TransferIn.model_validate({"mode": mode, "target": target})
        return calls.transfer(self.cid, body, p=p or self.key)["transfer_id"]

    def primaries(self) -> list:
        return self.rows(role="primary", state="active")


@pytest.fixture
def call(monkeypatch) -> Call:
    return Call(monkeypatch)


def test_a_warm_target_consults_then_takes_the_call_on_complete_and_a_repeat_changes_nothing(call):
    target = call.agent()
    tid = call.transfer("warm", {"type": "agent", "agent_id": str(target)})
    [consult] = call.rows(via="transfer")
    call.join(consult)
    assert (call.row(consult).state, call.row(consult).role) == ("active", "secondary")
    assert call.primaries()[0].id == call.primary.id
    for _ in range(2):
        calls.complete_transfer(call.cid, uuid.UUID(tid), p=call.key)
    first = call.row(call.primary)
    assert [a.id for a in call.primaries()] == [consult.id] and (first.state, first.reason) == (
        "wrap_up",
        "transferred",
    )
    assert call.sent.count(("DELETE", f"Call/{first.call_uuid}/", {})) == 1
    assert not call.job(f"hangup:{first.call_uuid}") and not call.job(f"kick:{call.primary.id}")
    [done] = call.events("call.transferred", status="completed")
    assert (done["transfer_id"], done["mode"], done["agent_id"]) == (tid, "warm", str(call.agent_id))
    with pytest.raises(ApiError) as late:
        calls.cancel_transfer(call.cid, uuid.UUID(tid), p=call.key)
    assert late.value.code == "transfer_completed"


def test_a_warm_consult_takes_the_call_when_the_primary_leaves_and_a_repeated_exit_changes_nothing(call):
    call.transfer("warm", {"type": "agent", "agent_id": str(call.agent())})
    [consult] = call.rows(via="transfer")
    call.join(consult)
    for _ in range(2):
        call.exit(call.primary)
    assert [a.id for a in call.primaries()] == [consult.id] and call.row(call.primary).state == "wrap_up"
    assert len(call.events("call.transferred", status="completed")) == 1


def test_a_warm_target_that_joins_after_the_primary_left_takes_the_call_once(call):
    tid = call.transfer("warm", {"type": "agent", "agent_id": str(call.agent())})
    [target] = call.rows(via="transfer")
    call.exit(call.primary)
    for _ in range(2):
        call.join(target)
    assert [a.id for a in call.primaries()] == [target.id] and call.conv().state == "live"
    assert [e["transfer_id"] for e in call.events("call.transferred", status="completed")] == [tid]


def test_a_cold_team_target_takes_the_call_when_it_joins_and_a_repeated_or_losing_join_changes_nothing(call):
    team = call.team()
    first, second = call.agent(team), call.agent(team)
    call.transfer("cold", {"type": "team", "team_id": str(team)})
    rings = {a.agent_id: a for a in call.rows(via="transfer")}
    assert set(rings) == {first, second}
    call.bind(*rings.values())
    for _ in range(2):
        call.join(rings[first])
    call.join(rings[second])
    lost = call.row(rings[second])
    assert [a.agent_id for a in call.primaries()] == [first] and (lost.state, lost.reason) == ("withdrawn", "taken")
    assert call.job(f"hangup:{lost.call_uuid}") and not call.job(f"kick:{lost.id}")
    assert call.row(call.primary).state == "wrap_up" and len(call.events("call.transferred", status="completed")) == 1


def test_cancelling_a_transfer_withdraws_its_ring_hangs_its_leg_up_and_keeps_the_primary(call):
    tid = call.transfer("cold", {"type": "agent", "agent_id": str(call.agent())})
    [ring] = call.rows(via="transfer")
    call.bind(ring)
    calls.cancel_transfer(call.cid, uuid.UUID(tid), p=call.key)
    with pytest.raises(ApiError) as again:
        calls.cancel_transfer(call.cid, uuid.UUID(tid), p=call.key)
    assert (again.value.status, again.value.code) == (422, "not_in_transfer"), "a repeat finds no transfer"
    call.join(ring)
    gone = call.row(ring)
    assert (gone.state, gone.reason) == ("withdrawn", "cancelled") and call.primaries()[0].id == call.primary.id
    assert ("DELETE", f"Call/{gone.call_uuid}/", {}) in call.sent and not call.job(f"kick:{gone.id}")


def test_a_parked_hangup_of_a_leg_whose_offer_is_over_is_sent_again_on_the_legs_next_call_status(call):
    tid = call.transfer("cold", {"type": "agent", "agent_id": str(call.agent())})
    [ring] = call.rows(via="transfer")
    call.bind(ring)
    calls.cancel_transfer(call.cid, uuid.UUID(tid), p=call.key)
    leg = call.row(ring).call_uuid
    key = f"hangup:{call.tenant}:{leg}"
    with core.tx() as db:
        db.execute(T.jobs.delete().where(T.jobs.c.key == key))
        jobs.schedule(db, "plivo", {"tenant_id": str(call.tenant), "op": "hangup", "call_uuid": leg}, key=key)
        db.execute(update(T.jobs).where(T.jobs.c.key == key).values(locked_until=jobs.INFINITY))
    mpc.call_status(call.cid, uuid.uuid4(), SimpleNamespace(id=call.tenant), {"CallUUID": leg, "EventName": "Ringing"})
    assert call.job(f"hangup:{leg}").locked_until is None


def test_a_late_cancel_or_complete_of_a_cancelled_transfer_leaves_the_next_transfer_ringing(call):
    old = call.transfer("warm", {"type": "agent", "agent_id": str(call.agent())})
    calls.cancel_transfer(call.cid, uuid.UUID(old), p=call.key)
    new = call.transfer("warm", {"type": "agent", "agent_id": str(call.agent())})
    with pytest.raises(ApiError) as cancelled:
        calls.cancel_transfer(call.cid, uuid.UUID(old), p=call.key)
    assert cancelled.value.code == "not_in_transfer"
    with pytest.raises(ApiError) as late:
        calls.complete_transfer(call.cid, uuid.UUID(old), p=call.key)
    [ringing] = call.rows(via="transfer", state="offered")
    assert new != old and late.value.code == "transfer_not_open" and str(ringing.transfer_id) == new


@pytest.mark.parametrize("room_end", ["heard", "lost"])
@pytest.mark.parametrize("blocks", [True, False])
def test_a_transfer_to_a_queue_moves_the_caller_and_wraps_up_the_primary_whose_slot_waits_for_the_room_end(
    call, blocks, room_end
):
    with config.write(access.OPERATOR, call.tenant) as db:
        other = call.doc(db, "queue", m.QueueIn(name="Billing").model_dump(mode="json"))
        stored = T.tenant.c.settings.concat({"acw_blocks_routing": blocks, "wrap_up": {"mode": "auto", "seconds": 30}})
        db.execute(update(T.tenant).where(T.tenant.c.id == call.tenant).values(settings=stored))
    action = {"type": "queue", "queue_id": str(other), "agents": [], "hold_position": False}
    call.transfer("cold", action)
    first, slot = call.row(call.primary), CAP % (call.tenant, "call", call.agent_id)
    timer = call.job(str(first.id), "wrap_up_timeout")
    assert (first.state, first.reason) == ("wrap_up", "transferred")
    assert call.job(f"redirect:{call.cid}:1").payload["action"] == action
    assert timer.payload["mode"] == "manual" and timer.run_at == first.wrap_up_until + cv.SAFETY
    assert [e["mode"] for e in call.events("conversation.wrap_up_started")] == ["manual"]
    assert set(core.redis.hvals(slot)) == {str(call.cid)} and call.job(str(call.agent_id), "route_agent") is None
    if room_end == "heard":
        for late in ({"EventName": "ParticipantExit", "ParticipantCallUUID": call.caller}, {"EventName": "MPCEnd"}):
            mpc.mpc_status(call.cid, 0, tenant=SimpleNamespace(id=call.tenant), form=late)
        assert (call.conv().state, call.conv().room) == ("open", 1), "the old room's caller exit is dropped"
        assert set(core.redis.hvals(slot)) == ({str(call.cid)} if blocks else set())
        assert (call.job(str(call.agent_id), "route_agent") is None) == blocks
    jobs.run_now("wrap_up_timeout", str(first.id), wait=True)
    assert call.row(first).state == "done" and set(core.redis.hvals(slot)) == set()
    assert call.job(str(call.agent_id), "route_agent") is not None


@pytest.mark.parametrize("moved", [True, False])
def test_a_transfer_out_of_the_room_is_told_initiated_only_once_plivo_moved_the_caller(call, monkeypatch, moved):
    with config.write(access.OPERATOR, call.tenant) as db:
        other = call.doc(db, "queue", m.QueueIn(name="Billing").model_dump(mode="json"))
    action = {"type": "queue", "queue_id": str(other), "agents": [], "hold_position": False}
    tid = call.transfer("cold", action)
    if not moved:
        monkeypatch.setattr(plivo, "rest", lambda *_, **__: (_ for _ in ()).throw(refusal()))
    jobs.run_now("plivo", f"redirect:{call.tenant}:{call.cid}:1", wait=True)
    told = [(e["transfer_id"], e["status"], e["target"], e["agent_id"]) for e in call.events("call.transferred")]
    assert told == ([(tid, "initiated", action, str(call.agent_id))] if moved else [])


@pytest.mark.parametrize("moved_first", [True, False])
def test_a_transfer_to_a_queue_is_told_initiated_once_whether_its_move_is_done_before_or_after_the_caller_queued(
    call, moved_first
):
    with config.write(access.OPERATOR, call.tenant) as db:
        other = call.doc(db, "queue", m.QueueIn(name="Billing").model_dump(mode="json"))
    action = {"type": "queue", "queue_id": str(other), "agents": []}
    tid = call.transfer("cold", action)
    redirect = call.job(f"redirect:{call.cid}:1")
    if moved_first:
        plivo._run(redirect)
    flow.move(1, json.dumps(action), tenant=SimpleNamespace(id=call.tenant), form={"CallUUID": call.caller})
    assert call.conv().state == "queued"
    jobs.run_now("plivo", redirect.key, wait=True)
    assert [(e["transfer_id"], e["status"]) for e in call.events("call.transferred")] == [(tid, "initiated")]


def test_a_phone_target_stays_external_when_a_warm_transfer_to_it_completes(call):
    tid = call.transfer("warm", {"type": "number", "number": NUMBER})
    [phone] = call.rows(via="transfer")
    call.join(phone)
    calls.complete_transfer(call.cid, uuid.UUID(tid), p=call.key)
    call.join(phone)
    assert (call.row(phone).role, call.row(phone).state) == ("external", "active") and call.primaries() == []
    [done] = call.events("call.transferred", status="completed")
    assert done["target"] == {"type": "number", "number": NUMBER} and call.conv().state == "live"


def test_a_transfer_whose_destination_is_deleted_before_its_read_is_refused_as_foreign(call, monkeypatch):
    with core.tx() as db:
        dest = call.doc(db, "destination", {"name": "Desk", "number": NUMBER})
    stored = config.get

    def gone(db, tenant, kind, id, **kw):
        return None if kind == "destination" else stored(db, tenant, kind, id, **kw)

    monkeypatch.setattr(config, "get", gone)
    with pytest.raises(ApiError) as refused:
        call.transfer("cold", {"type": "destination", "destination_id": str(dest)})
    assert (refused.value.status, refused.value.code) == (422, "foreign_id") and call.rows(via="transfer") == []


def test_participants_join_without_taking_the_call_two_at_most_and_leave_on_removal(call):
    helper = call.agent()
    pid = calls.add_participant(call.cid, calls.ParticipantIn(target={"type": "agent", "agent_id": helper}), p=call.key)
    calls.add_participant(call.cid, calls.ParticipantIn(target={"type": "number", "number": NUMBER}), p=call.key)
    with pytest.raises(ApiError) as third:
        calls.add_participant(call.cid, calls.ParticipantIn(target={"type": "number", "number": NUMBER}), p=call.key)
    [second, phone] = call.rows(via="participant")
    for _ in range(2):
        call.join(second)
    assert third.value.code == "too_many_participants" and call.row(second).state == "active"
    assert call.primaries()[0].id == call.primary.id and phone.role == "external"
    for _ in range(2):
        calls.remove_participant(call.cid, pid["participant_id"], p=call.key)
    left = call.row(second)
    assert left.state == "wrap_up" and ("DELETE", f"Call/{left.call_uuid}/", {}) in call.sent
    assert [e["participant_id"] for e in call.events("call.participant_removed")] == [str(second.id)]


def test_a_participant_leg_that_exits_before_its_join_stays_on_the_record_and_its_late_join_is_kicked(call):
    helper = call.agent()
    calls.add_participant(call.cid, calls.ParticipantIn(target={"type": "agent", "agent_id": helper}), p=call.key)
    [second] = call.rows(via="participant")
    call.exit(second)
    leg = call.row(second)
    with core.read() as db:
        assert leg.call_uuid in [x["call_uuid"] for x in cv.view(db, call.conv())["legs"]]
    call.join(second)
    assert call.job(f"kick:{leg.id}").payload["member_id"] == f"m-{leg.id}", "the late join is kicked"


def test_a_repeated_call_status_or_ring_reply_of_a_bound_leg_binds_nothing_new_and_hangs_nothing_up(call):
    helper = call.agent()
    calls.add_participant(call.cid, calls.ParticipantIn(target={"type": "agent", "agent_id": helper}), p=call.key)
    J = T.jobs
    with core.read() as db:
        ring = db.execute(
            select(J.c.payload).where(J.c.kind == "plivo", J.c.key.like(f"ring:{call.tenant}:%"))
        ).scalar()
    leg, to, tenant = uuid.uuid4().hex, ring["params"]["to"], SimpleNamespace(id=call.tenant)
    for _ in range(2):
        mpc.call_status(call.cid, uuid.UUID(ring["ring"]), tenant=tenant, form={"CallUUID": leg, "To": to})
        mpc._rung(ring, {"calls": [{"call_uuid": leg, "to": to}]})
    with core.tx() as db:
        assert [a.call_uuid for a in cv.rows(db, cv.lock(db, call.cid).id, ("offered",))] == [leg]
    assert call.job(f"hangup:{leg}") is None


class Rollback(Exception):
    pass


def test_a_wait_prompt_whose_transaction_rolled_back_is_spoken_when_plivo_sends_its_event_again(call):
    with core.tx() as db:
        q = call.doc(db, "queue", m.QueueIn(name="P", wait_prompts=[{"text": "Please hold"}]).model_dump(mode="json"))
        parties = {"direction": "inbound", "customer": "+15550003", "ours": "+15550002"}
        conv = cv.open(db, call.tenant, channel="call", call_uuid=uuid.uuid4().hex, **parties)
        conv = cv.set_state(db, conv, "queued", queue_id=q, queued_at=func.now())
    form = {"EventName": "MPCInitialized", "ParticipantCallUUID": conv.call_uuid, "MemberID": "m-caller"}
    form["SequenceNumber"] = "4"
    with pytest.raises(Rollback), core.tx() as db:
        mpc._caller(db, cv.lock(db, conv.id), "MPCInitialized", form)
        raise Rollback
    mpc.mpc_status(conv.id, conv.room, tenant=SimpleNamespace(id=call.tenant), form=form)
    assert call.job(f"prompt:{conv.id}:{conv.room}") is not None


def test_a_participant_who_hangs_up_leaves_once_and_is_told_of(call):
    helper = call.agent()
    calls.add_participant(call.cid, calls.ParticipantIn(target={"type": "agent", "agent_id": helper}), p=call.key)
    [second] = call.rows(via="participant")
    call.join(second)
    for _ in range(2):
        call.exit(second)
    assert (call.row(second).state, call.row(second).reason) == ("wrap_up", "left")
    assert [e["agent_id"] for e in call.events("call.participant_removed")] == [str(helper)]
    assert call.primaries()[0].id == call.primary.id


def test_a_supervisor_joins_switches_and_leaves_never_taking_the_call(call):
    sup = call.agent(role="admin")
    calls.monitor(call.cid, calls.MonitorIn(mode="barge", supervisor_id=sup), p=call.key)
    [watch] = call.rows(role="monitor")
    ring = next(params for method, path, params in call.sent if method == "POST" and path.endswith("/Participant/"))
    assert (ring["role"], ring["coach_mode"], ring["mute"]) == ("Supervisor", False, False)
    for _ in range(2):
        call.join(watch)
    [joined] = call.events("supervisor.joined")
    assert (joined["agent_id"], joined["supervisor_id"], joined["mode"]) == (str(call.agent_id), str(sup), "barge")
    assert call.job(f"speak:{watch.id}").payload["member_id"] == "all"
    calls.switch_monitor(call.cid, calls.MonitorMode(mode="listen"), p=call.key)
    flags = (
        "POST",
        f"MultiPartyCall/name_hs-{call.cid}-0/Participant/m-{watch.id}/",
        {"coach_mode": True, "mute": True},
    )
    assert flags in call.sent and call.row(watch).mode == "listen"
    calls.leave_monitor(call.cid, p=call.key)
    call.exit(watch)
    assert call.row(watch).state == "done" and call.primaries()[0].id == call.primary.id
    assert [e["mode"] for e in call.events("supervisor.left")] == ["listen"]


def watching(call, mode: str = "listen"):
    sup = call.agent(role="admin")
    calls.monitor(call.cid, calls.MonitorIn(mode=mode, supervisor_id=sup), p=call.key)
    [watch] = call.rows(role="monitor")
    return sup, watch


def test_a_supervisor_who_hangs_up_leaves_once_and_is_told_of(call):
    sup, watch = watching(call)
    call.join(watch)
    for _ in range(2):
        call.exit(watch)
    assert (call.row(watch).state, call.row(watch).reason) == ("done", "left")
    assert [e["agent_id"] for e in call.events("supervisor.left")] == [str(sup)]


def test_a_supervisor_still_listening_when_the_primary_leaves_is_taken_off_the_call_with_it(call):
    sup, watch = watching(call)
    call.join(watch)
    for _ in range(2):
        call.exit(call.primary)
    assert (call.row(watch).state, call.row(watch).reason) == ("done", "left")
    assert call.job(f"kick:{watch.id}").payload["member_id"] == f"m-{watch.id}"
    assert [e["agent_id"] for e in call.events("supervisor.left")] == [str(sup)]


def test_a_supervisor_left_with_the_caller_by_the_last_participant_is_taken_off_the_call_with_it(call):
    sup, watch = watching(call)
    call.join(watch)
    calls.add_participant(call.cid, calls.ParticipantIn(target={"type": "number", "number": NUMBER}), p=call.key)
    [phone] = call.rows(via="participant")
    call.join(phone)
    call.exit(call.primary)
    assert call.row(watch).state == "active"
    for _ in range(2):
        call.exit(phone)
    assert (call.row(watch).state, call.row(watch).reason) == ("done", "left")
    assert call.job(f"kick:{watch.id}") and [e["agent_id"] for e in call.events("supervisor.left")] == [str(sup)]


def test_a_supervisor_who_misses_the_ring_gets_no_strike(call):
    sup, watch = watching(call)
    call.exit(watch)
    with core.read() as db:
        misses = db.execute(select(T.presence.c.misses).where(T.presence.c.agent_id == sup)).scalar()
    assert call.row(watch).state == "missed" and misses == 0


def test_a_leave_plivo_does_not_answer_is_a_503_and_its_retry_hangs_up_and_answers_204(call, monkeypatch):
    _, watch = watching(call)
    call.join(watch)
    fine = call.rest

    def outage(tenant, method, path, **params):
        raise plivo.Uncertain("503")

    monkeypatch.setattr(plivo, "rest", outage)
    with pytest.raises(ApiError) as down:
        calls.leave_monitor(call.cid, p=call.key)
    monkeypatch.setattr(plivo, "rest", fine)
    calls.leave_monitor(call.cid, p=call.key)
    left = call.row(watch)
    assert down.value.code == "dependency_unavailable" and (left.state, left.reason) == ("done", "stopped")
    assert call.sent[-1] == ("DELETE", f"Call/{left.call_uuid}/", {}) and not call.job(f"hangup:{left.call_uuid}")
    assert len(call.events("supervisor.left")) == 1


def test_a_caller_left_alone_while_its_transfer_rings_waits_in_a_new_room_where_the_target_rings_again(call):
    target = call.agent()
    call.transfer("warm", {"type": "agent", "agent_id": str(target)})
    [old] = call.rows(via="transfer")
    form = {"CallUUID": call.caller, "ParticipantExitCause": "alone"}
    answers = [flow.queue_exit(0, tenant=SimpleNamespace(id=call.tenant), form=form).body for _ in range(2)]
    conv, rings = call.conv(), call.rows(via="transfer", state="offered")
    assert answers[0] == answers[1] and f"hs-{call.cid}-1" in answers[0].decode()
    assert (conv.state, conv.room, call.row(old).state, call.row(call.primary).state) == (
        "queued",
        1,
        "withdrawn",
        "wrap_up",
    )
    assert [(a.agent_id, a.from_assignment) for a in rings] == [(target, call.primary.id)]
    for _ in range(2):
        call.join(rings[0])
    assert [a.id for a in call.primaries()] == [rings[0].id] and call.conv().state == "live"
    assert [e["from_agent_id"] for e in call.events("call.transferred", status="completed")] == [str(call.agent_id)]


def test_cancelling_a_transfer_while_it_rings_again_withdraws_the_new_ring_and_the_queue_routes_the_caller(call):
    tid = call.transfer("warm", {"type": "agent", "agent_id": str(call.agent())})
    form = {"CallUUID": call.caller, "ParticipantExitCause": "alone"}
    flow.queue_exit(0, tenant=SimpleNamespace(id=call.tenant), form=form)
    [again] = call.rows(via="transfer", state="offered")
    calls.cancel_transfer(call.cid, uuid.UUID(tid), p=call.key)
    assert (call.row(again).state, call.row(again).reason) == ("withdrawn", "cancelled")
    assert call.conv().state == "queued" and call.job(str(call.cid), "next_round")
    with core.tx() as db:
        jobs.cancel(db, "next_round", str(call.cid))
    with pytest.raises(ApiError, match="not in transfer mode"):
        calls.cancel_transfer(call.cid, uuid.UUID(tid), p=call.key)
    assert not call.job(str(call.cid), "next_round")


def test_hold_reads_the_callers_member_from_plivo_and_tells_of_it_once_done(call):
    call.members = [{"member_id": "7", "call_uuid": call.caller, "exit_cause": None}]
    calls.hold(call.cid, calls.Hold(on=True), p=call.key)
    room = f"MultiPartyCall/name_hs-{call.cid}-0/Participant/"
    assert call.sent[-2:] == [("GET", room, {}), ("POST", f"{room}7/", {"hold": True})]
    assert call.events("call.held") == [{"agent_id": str(call.agent_id), "by_agent_id": None, "participant_id": None}]


def test_a_hold_refused_where_the_job_pool_ran_it_answers_the_refusal(call, pool_first):
    with pytest.raises(ApiError) as refused:
        calls.hold(call.cid, calls.Hold(on=True), p=call.key)
    assert (refused.value.status, refused.value.code) == (409, "caller_not_in_room") and not call.events("call.held")


UNHELD = {"member_id": "7", "exit_cause": None, "hold": False}


def holds_sent(call) -> list[dict]:
    """Run the call's hold command if one is due; returns every hold flag Plivo was sent for the caller so far."""
    jobs.run_now("plivo", f"hold:{call.tenant}:{call.cid}", wait=True)
    member = f"MultiPartyCall/name_hs-{call.cid}-0/Participant/7/"
    return [params for method, path, params in call.sent if (method, path) == ("POST", member)]


def answer(control: Future):
    try:
        control.result(10)
    except ApiError as e:
        return e.status, e.code
    return 204


def racing(call, monkeypatch, key: str, stalls, says, first, *later) -> tuple[list, list]:
    """Run `first` until Plivo holds the request `stalls(path, params)` accepts, then each later control until its
    schedule replaced the command `key` in flight; then Plivo answers, raising what `says(path, params)` returns.
    Returns each control's answer, 204 or (status, code), and the requests Plivo got once it answered."""
    held, answered, received, timely, rest = threading.Event(), threading.Event(), [], [], call.rest

    def plivo_rest(tenant, method, path, **params):
        if answered.is_set():
            received.append((method, path, params))
        elif method == "POST" and stalls(path, params):
            held.set()
            timely.append(answered.wait(30))
        if method == "POST" and (failure := says(path, params)):
            raise failure
        for member in call.members if method == "POST" else ():
            if path.endswith(f"/Participant/{member['member_id']}/"):
                member.update(params)
        return rest(tenant, method, path, **params)

    monkeypatch.setattr(plivo, "rest", plivo_rest)
    with ThreadPoolExecutor(1 + len(later)) as pool:
        answers = [pool.submit(first)]
        assert held.wait(5), "Plivo never got the first control's request"
        for control in later:
            rev, deadline = call.job(key).rev, time.monotonic() + 5
            answers.append(pool.submit(control))
            while call.job(key).rev == rev:
                assert time.monotonic() < deadline, "the later control never replaced the command in flight"
                time.sleep(0.02)
        answered.set()
    assert all(timely), "Plivo let the first control's request go before the later controls replaced it"
    return [answer(a) for a in answers], received


refusal = partial(plivo.Refused, 422, "plivo_refused", "Plivo refused the request", {"status": 400})
rate_limited = partial(plivo.Refused, 429, "carrier_rate_limited", "Plivo is rate limiting this account")
unanswered = partial(plivo.Uncertain, "POST: 500")
REFUSED = (422, "plivo_refused")


@pytest.mark.parametrize(
    ("route", "bodies", "key", "stalls", "refuses"),
    [
        pytest.param(
            calls.hold,
            (calls.Hold(on=True), calls.Hold(on=False)),
            "hold:{cid}",
            lambda path, params: params.get("hold") is True,
            lambda path, params: params.get("hold") is False,
            id="hold",
        ),
        pytest.param(
            media.control_recording,
            (media.RecordingAction(action="pause"), media.RecordingAction(action="resume")),
            "recording:{cid}:0",
            lambda path, params: path.endswith("-0/Record/Pause/"),
            lambda path, params: path.endswith("-0/Record/Resume/"),
            id="recording",
        ),
    ],
)
def test_controls_a_later_one_replaced_answer_204_and_only_the_last_answers_plivos_refusal(
    call, monkeypatch, route, bodies, key, stalls, refuses
):
    """The first control's command is replaced while Plivo holds it, the second's before it is sent."""
    call.members = [UNHELD | {"call_uuid": call.caller}]

    def says(path, params):
        return refuses(path, params) and refusal()

    controls = [partial(route, call.cid, body, p=call.key) for body in (bodies[0], *bodies)]
    answers, received = racing(call, monkeypatch, key.format(cid=call.cid), stalls, says, *controls)
    assert answers == [204, 204, REFUSED]
    assert not [path for _, path, params in received if stalls(path, params)], "the replaced command was sent"


@pytest.mark.parametrize(
    ("whisper", "later", "listen", "answers", "changed"),
    [
        pytest.param(None, ["listen"], None, [204, 204], ["whisper", "listen"], id="applied-then-accepted"),
        pytest.param(None, ["listen"], refusal, [204, REFUSED], ["whisper"], id="applied-then-refused"),
        pytest.param(refusal, ["listen"], None, [REFUSED, 204], [], id="refused-then-accepted"),
        pytest.param(rate_limited, ["listen"], None, [(429, "carrier_rate_limited"), 204], [], id="rate-limited"),
        pytest.param(unanswered, ["listen"], None, [(503, "plivo_unavailable"), 204], [], id="unanswered"),
        pytest.param(None, ["barge", "listen"], None, [204] * 3, ["whisper", "listen"], id="barge-unsent-accepted"),
        pytest.param(None, ["barge", "listen"], refusal, [204, 204, REFUSED], ["whisper"], id="barge-unsent-refused"),
    ],
)
def test_a_switch_a_later_one_replaced_answers_its_own_command_and_the_mode_is_the_last_plivo_applied(
    call, monkeypatch, whisper, later, listen, answers, changed
):
    """The whisper's command is replaced while Plivo holds it; a barge between is replaced before it is sent."""
    _, watch = watching(call)
    call.join(watch)
    member, modes = f"MultiPartyCall/name_hs-{call.cid}-0/Participant/m-{watch.id}/", calls.MODES

    def says(path, params):
        fails = whisper if params == modes["whisper"] else listen if params == modes["listen"] else None
        return fails and fails()

    switches = [
        partial(calls.switch_monitor, call.cid, calls.MonitorMode(mode=x), p=call.key) for x in ["whisper", *later]
    ]
    got, received = racing(
        call, monkeypatch, f"coach:{watch.id}", lambda path, params: params == modes["whisper"], says, *switches
    )
    assert got == answers
    assert [e["mode"] for e in call.events("supervisor.mode_changed")] == changed
    sent = [params for method, path, params in received if (method, path) == ("POST", member)]
    assert sent == [modes["listen"]], "the listen did not reach Plivo exactly once, or the barge reached it"


def test_a_transfer_answers_once_plivo_takes_its_ring_which_holds_the_caller(call):
    call.members = [UNHELD | {"call_uuid": call.caller}]
    call.transfer("cold", {"type": "agent", "agent_id": str(call.agent())})
    assert holds_sent(call) == [{"hold": True}]
    assert call.events("call.held") == [{"agent_id": str(call.agent_id), "by_agent_id": None, "participant_id": None}]


HELD = UNHELD | {"hold": True}


def unheld(call, *subjects) -> list[str]:
    for subject in subjects:
        jobs.run_now("plivo", f"hold:{call.tenant}:{subject}", wait=True)
    return [path.split("/")[-2] for method, path, params in call.sent if (method, params) == ("POST", {"hold": False})]


def test_a_consult_target_the_agent_held_comes_off_hold_once_it_takes_the_call(call):
    tid = call.transfer("warm", {"type": "agent", "agent_id": str(call.agent())})
    [consult] = call.rows(via="transfer")
    call.join(consult)
    call.members = [HELD | {"member_id": f"m-{consult.id}", "call_uuid": call.row(consult).call_uuid}]
    calls.complete_transfer(call.cid, uuid.UUID(tid), p=call.key)
    assert unheld(call, consult.id) == [f"m-{consult.id}"]


def test_the_primary_leaving_a_participant_with_the_caller_takes_both_off_hold(call):
    calls.add_participant(call.cid, calls.ParticipantIn(target={"type": "number", "number": NUMBER}), p=call.key)
    [phone] = call.rows(via="participant")
    call.join(phone)
    phoned = HELD | {"member_id": f"m-{phone.id}", "call_uuid": call.row(phone).call_uuid}
    call.members = [HELD | {"call_uuid": call.caller}, phoned]
    call.exit(call.primary)
    assert unheld(call, call.cid, phone.id) == ["7", f"m-{phone.id}"]


@pytest.mark.parametrize("first", ["the_reply", "the_hand_over", "the_hangup"])
def test_a_transfers_hold_never_runs_once_the_call_is_handed_over_or_over_whatever_lands_first(
    call, first, monkeypatch
):
    monkeypatch.setattr(plivo, "verdict", lambda *keys, **_: None)
    call.members = [UNHELD | {"call_uuid": call.caller}]
    call.transfer("cold", {"type": "agent", "agent_id": str(call.agent())})
    [target] = call.rows(via="transfer")
    ring = f"ring:{call.tenant}:{target.ring}"
    if first == "the_reply":
        jobs.run_now("plivo", ring, wait=True)
    if first == "the_hangup":
        form = {"EventName": "ParticipantExit", "ParticipantCallUUID": call.caller}
        mpc.mpc_status(call.cid, 0, tenant=SimpleNamespace(id=call.tenant), form=form)
    else:
        call.join(target)
    jobs.run_now("plivo", ring, wait=True)
    assert holds_sent(call) == [] and not call.events("call.held") and not call.events("call.resumed")


@pytest.mark.parametrize("first", ["the_reply", "a_join"])
def test_a_team_transfer_is_told_initiated_once_and_first_whether_its_reply_or_a_join_lands_first(
    call, first, monkeypatch
):
    monkeypatch.setattr(plivo, "verdict", lambda *keys, **_: None)
    team = call.team()
    call.agent(team), call.agent(team)
    tid = call.transfer("cold", {"type": "team", "team_id": str(team)})
    ring = call.job(f"ring:{call.rows(via='transfer')[0].ring}").payload
    legs = [{"call_uuid": uuid.uuid4().hex, "to": to} for to in ring["params"]["to"].split("<")]
    if first == "a_join":
        form = {"CallUUID": legs[0]["call_uuid"], "To": legs[0]["to"], "EventName": "Ringing"}
        mpc.call_status(call.cid, uuid.UUID(ring["ring"]), tenant=SimpleNamespace(id=call.tenant), form=form)
        call.join(call.rows(call_uuid=legs[0]["call_uuid"])[0])
    for _ in range(2):
        mpc._rung(ring, {"calls": legs})
    told = [(e["transfer_id"], e["status"]) for e in call.events("call.transferred")]
    assert told == [(tid, "initiated")] + ([(tid, "accepted"), (tid, "completed")] if first == "a_join" else [])


def test_a_team_transfer_whose_withdrawn_leg_binds_first_is_still_told_initiated_once_and_first(call, monkeypatch):
    monkeypatch.setattr(plivo, "verdict", lambda *keys, **_: None)
    team = call.team()
    gone, stays = call.agent(team), call.agent(team)
    tid = call.transfer("cold", {"type": "team", "team_id": str(team)})
    ring = call.job(f"ring:{call.rows(via='transfer')[0].ring}").payload
    legs = {a.agent_id: {"call_uuid": uuid.uuid4().hex, "to": a.address} for a in call.rows(via="transfer")}
    with core.tx() as db:
        away = call.doc(db, "status", {"name": "Away", "availability": False})
        agents.set_status(db, str(call.tenant), gone, away, "manual")
    jobs.run_now("route_agent", str(gone), wait=True)
    form = {"CallUUID": legs[gone]["call_uuid"], "To": legs[gone]["to"], "EventName": "Ringing"}
    mpc.call_status(call.cid, uuid.UUID(ring["ring"]), tenant=SimpleNamespace(id=call.tenant), form=form)
    mpc._rung(ring, {"calls": list(legs.values())})
    call.join(call.rows(agent_id=stays, via="transfer")[0])
    told = [(e["transfer_id"], e["status"]) for e in call.events("call.transferred")]
    assert told == [(tid, "initiated"), (tid, "accepted"), (tid, "completed")]


def _team_ring_going_away(call, monkeypatch) -> tuple[str, dict, list[dict], uuid.UUID]:
    monkeypatch.setattr(plivo, "verdict", lambda *keys, **_: None)
    team = call.team()
    first, last = call.agent(team), call.agent(team)
    tid = call.transfer("cold", {"type": "team", "team_id": str(team)})
    ring = call.job(f"ring:{call.rows(via='transfer')[0].ring}").payload
    legs = [
        {"call_uuid": uuid.uuid4().hex, "to": call.rows(agent_id=a, via="transfer")[0].address} for a in (first, last)
    ]
    with core.tx() as db:
        away = call.doc(db, "status", {"name": "Away", "availability": False})
        for agent in (first, last):
            agents.set_status(db, str(call.tenant), agent, away, "manual")
    jobs.run_now("route_agent", str(first), wait=True)
    return tid, ring, legs, last


def _ringing(call, ring: dict, leg: dict) -> None:
    form = {"CallUUID": leg["call_uuid"], "To": leg["to"], "EventName": "Ringing"}
    mpc.call_status(call.cid, uuid.UUID(ring["ring"]), tenant=SimpleNamespace(id=call.tenant), form=form)


@pytest.mark.parametrize("answer", ["the_reply", "each_legs_call_status", "a_refusal"])
def test_a_team_transfer_whose_every_leg_ends_before_plivo_answers_is_told_once_plivo_answers(
    call, monkeypatch, answer
):
    tid, ring, legs, last = _team_ring_going_away(call, monkeypatch)
    jobs.run_now("route_agent", str(last), wait=True)
    assert call.events("call.transferred") == []
    if answer == "the_reply":
        mpc._rung(ring, {"calls": legs})
    elif answer == "each_legs_call_status":
        for leg in legs:
            _ringing(call, ring, leg)
    else:
        mpc._refused(ring, refusal())
    told = [(e["transfer_id"], e["status"]) for e in call.events("call.transferred")]
    assert told == ([] if answer == "a_refusal" else [(tid, "initiated"), (tid, "failed")])


def test_a_transfer_told_initiated_is_not_told_it_again_once_its_events_are_gone(call, monkeypatch):
    monkeypatch.setattr(plivo, "verdict", lambda *keys, **_: None)
    team = call.team()
    call.agent(team), call.agent(team)
    tid = call.transfer("cold", {"type": "team", "team_id": str(team)})
    ring = call.job(f"ring:{call.rows(via='transfer')[0].ring}").payload
    legs = [{"call_uuid": uuid.uuid4().hex, "to": a.address} for a in call.rows(via="transfer")]
    form = {"CallUUID": legs[0]["call_uuid"], "To": legs[0]["to"], "EventName": "Ringing"}
    mpc.call_status(call.cid, uuid.UUID(ring["ring"]), tenant=SimpleNamespace(id=call.tenant), form=form)
    with core.tx() as db:
        db.execute(delete(T.events).where(T.events.c.conversation_id == str(call.cid)))
    mpc._rung(ring, {"calls": legs})
    call.join(call.rows(call_uuid=legs[1]["call_uuid"])[0])
    told = [(e["transfer_id"], e["status"]) for e in call.events("call.transferred")]
    assert told == [(tid, "accepted"), (tid, "completed")]


def test_a_transfer_tells_nothing_after_cancelled_when_a_leg_binds_before_the_cancel_reaches_redis(call, monkeypatch):
    tid, ring, legs, _ = _team_ring_going_away(call, monkeypatch)
    publish, held = hot.tombstone, []
    monkeypatch.setattr(hot, "tombstone", lambda *args: held.append(args))
    calls.cancel_transfer(call.cid, uuid.UUID(tid), p=call.key)
    _ringing(call, ring, legs[0])
    for args in held:
        publish(*args)
    mpc._rung(ring, {"calls": legs})
    assert [(e["transfer_id"], e["status"]) for e in call.events("call.transferred")] == [(tid, "cancelled")]


def test_a_transfer_whose_targets_go_offline_tells_failed_once_when_a_leg_binds_before_the_last_reaches_redis(
    call, monkeypatch
):
    tid, ring, legs, last = _team_ring_going_away(call, monkeypatch)
    publish, held = hot.tombstone, []
    monkeypatch.setattr(hot, "tombstone", lambda *args: held.append(args))
    jobs.run_now("route_agent", str(last), wait=True)
    assert call.events("call.transferred") == []
    _ringing(call, ring, legs[0])
    for args in held:
        publish(*args)
    _ringing(call, ring, legs[1])
    told = [(e["transfer_id"], e["status"]) for e in call.events("call.transferred")]
    assert told == [(tid, "initiated"), (tid, "failed")]


def test_a_transfer_its_agent_hung_up_on_tells_nothing_once_the_queue_takes_the_caller_back_and_a_leg_binds(
    call, monkeypatch
):
    publish, held = hot.tombstone, []
    monkeypatch.setattr(hot, "tombstone", lambda *args: held.append(args))
    tid, ring, legs, _ = _team_ring_going_away(call, monkeypatch)
    back, C = {"type": "queue", "queue_id": str(call.queue), "agents": []}, T.config
    with config.write(access.OPERATOR, call.tenant) as db:
        db.execute(update(C).where(C.c.id == call.queue).values(body=C.c.body.concat({"after_agent_action": back})))
    calls.hangup(call.cid, p=call.key)
    call.exit(call.primary)
    flow.queue_exit(0, tenant=SimpleNamespace(id=call.tenant), form={"CallUUID": call.caller})
    assert (call.conv().state, call.conv().room) == ("queued", 1)
    for args in held:
        publish(*args)
    mpc._rung(ring, {"calls": legs})
    assert call.events("call.transferred", transfer_id=tid) == []


def test_two_calls_handed_to_phones_at_once_by_agents_who_watch_each_other_both_complete(call):
    """Each hand-over frees its agent and its supervisor, the other call's agent. With the first agent's
    route_agent row held, the first call waits for it, then the second; on release both must finish. Each monitor is
    offered directly, because the API refuses a supervisor already on a call."""
    with config.write(access.OPERATOR, call.tenant) as db:
        unblocked = T.tenant.c.settings.concat({"acw_blocks_routing": False})
        db.execute(update(T.tenant).where(T.tenant.c.id == call.tenant).values(settings=unblocked))
    first, second = sorted((call.agent(role="admin"), call.agent(role="admin")), key=str)
    x, y = copy.copy(call), copy.copy(call)
    x.answer(first)
    y.answer(second)
    transfers = []
    for mine, sup in ((x, second), (y, first)):
        with core.tx() as db:
            cv.offer(db, cv.lock(db, mine.cid), sup, role="monitor", via="monitor", mode="listen", force=True)
        mine.join(*mine.rows(role="monitor"))
        transfers.append((mine, uuid.UUID(mine.transfer("warm", {"type": "number", "number": NUMBER}))))
        mine.join(*mine.rows(via="transfer"))
    with core.tx() as db:
        jobs.schedule(db, "route_agent", {"tenant_id": str(call.tenant), "agent_id": str(first)}, key=str(first))
    held, release = Future(), threading.Event()

    def holding():
        with core.tx() as db:
            J = T.jobs
            db.execute(select(J.c.id).where(J.c.kind == "route_agent", J.c.key == str(first)).with_for_update())
            held.set_result(None)
            release.wait(10)

    with ThreadPoolExecutor(3) as pool:
        holder = pool.submit(holding)
        held.result(10)
        done = []
        for n, (mine, tid) in enumerate(transfers, 1):
            done.append(pool.submit(calls.complete_transfer, mine.cid, tid, p=call.key))
            deadline = time.monotonic() + 5
            with core.read() as db:
                while db.execute(WAITING).scalar() < n and not done[-1].done():
                    assert time.monotonic() < deadline, "the hand-over neither waited nor finished"
                    time.sleep(0.02)
        release.set()
        holder.result(10)
        for f in done:
            f.result(10)
    assert [len(c.events("call.transferred", status="completed")) for c in (x, y)] == [1, 1]


def test_a_hangup_plivo_does_not_answer_is_a_503_on_the_agents_leg_alone_and_one_after_the_call_ended_is_409(
    call, monkeypatch
):
    def outage(tenant, method, path, **params):
        call.sent.append((method, path, params))
        raise plivo.Uncertain("503")

    monkeypatch.setattr(plivo, "rest", outage)
    with pytest.raises(ApiError) as down:
        calls.hangup(call.cid, p=call.key)
    leg = call.row(call.primary).call_uuid
    assert down.value.code == "dependency_unavailable" and call.sent == [("DELETE", f"Call/{leg}/", {})]
    assert call.job(f"hangup:{leg}").last_error and not call.job(f"hangup:{call.caller}")
    with core.tx() as db:
        cv.gone(db, cv.lock(db, call.cid))
    with pytest.raises(ApiError) as over:
        calls.hangup(call.cid, p=call.key)
    assert over.value.code == "call_not_live" and len(call.sent) == 1


def test_an_agents_hangup_from_a_stint_not_the_primarys_ends_only_its_own_leg(call):
    helper = call.agent()
    calls.add_participant(call.cid, calls.ParticipantIn(target={"type": "agent", "agent_id": helper}), p=call.key)
    [second] = call.rows(via="participant")
    call.join(second)
    calls.hangup(call.cid, p=Principal(str(call.tenant), "agent", agent_id=str(helper), role="agent"))
    hung = [path for method, path, _ in call.sent if method == "DELETE"]
    assert hung == [f"Call/{call.row(second).call_uuid}/"] and not access.hung_up(call.conv())
    assert call.primaries()[0].id == call.primary.id


def test_the_primarys_hangup_ends_its_leg_then_the_callers_and_the_call_takes_no_more_controls(call):
    calls.hangup(call.cid, p=call.key)
    hung = [path for method, path, _ in call.sent if method == "DELETE"]
    assert hung == [f"Call/{call.row(call.primary).call_uuid}/", f"Call/{call.caller}/"]
    with pytest.raises(ApiError) as held:
        calls.hold(call.cid, calls.Hold(on=True), p=call.key)
    assert held.value.code == "call_not_live"


def test_a_teardown_that_waited_on_its_primarys_leg_leaves_the_stint_the_caller_began_meanwhile_alone(
    call, monkeypatch
):
    nxt, C, plain = call.agent(), T.config, call.rest
    back = {"type": "queue", "queue_id": str(call.queue), "agents": [str(nxt)]}
    with config.write(access.OPERATOR, call.tenant) as db:
        db.execute(update(C).where(C.c.id == call.queue).values(body=C.c.body.concat({"after_agent_action": back})))
    first = call.row(call.primary).call_uuid

    def down(tenant, method, path, **params):
        if method == "DELETE":
            raise plivo.Uncertain(f"{method} {path}: 503")
        return plain(tenant, method, path, **params)

    def meanwhile(tenant, method, path, **params):
        """While the first agent's leg ends: the room ends around the caller, its exit hands it back to the queue,
        the next agent takes it, and that agent's hangup waits on a Plivo that does not answer."""
        assert (method, path) == ("DELETE", f"Call/{first}/")
        monkeypatch.setattr(plivo, "rest", plain)
        call.exit(call.primary)
        flow.queue_exit(0, tenant=SimpleNamespace(id=call.tenant), form={"CallUUID": call.caller})
        [taken] = call.rows(agent_id=nxt)
        call.join(taken)
        monkeypatch.setattr(plivo, "rest", down)
        with pytest.raises(ApiError) as waiting:
            calls.hangup(call.cid, p=Principal(str(call.tenant), "agent", agent_id=str(nxt), role="agent"))
        assert waiting.value.code == "dependency_unavailable"
        monkeypatch.setattr(plivo, "rest", plain)
        return plain(tenant, method, path, **params)

    monkeypatch.setattr(plivo, "rest", meanwhile)
    calls.hangup(call.cid, p=call.key)
    [now] = call.primaries()
    assert now.agent_id == nxt and call.conv().room == 1
    assert [path for method, path, _ in call.sent if method == "DELETE"] == [f"Call/{first}/"]


def test_a_room_check_that_finds_the_room_ended_leaves_a_caller_its_queue_hands_back_to_its_queue_exit(
    call, monkeypatch
):
    C, back = T.config, {"type": "redirect", "url": "https://example.com/rate"}
    with config.write(access.OPERATOR, call.tenant) as db:
        db.execute(update(C).where(C.c.id == call.queue).values(body=C.c.body.concat({"after_agent_action": back})))
    call.exit(call.primary)
    monkeypatch.setattr(plivo, "rest", lambda tenant, method, path, **params: {"status": "Ended", "objects": []})
    mpc._check(SimpleNamespace(payload={"tenant_id": str(call.tenant), "conversation_id": str(call.cid), "room": 0}))
    answer = flow.queue_exit(0, tenant=SimpleNamespace(id=call.tenant), form={"CallUUID": call.caller})
    assert "https://example.com/rate" in answer.body.decode()


@pytest.mark.parametrize("target", ["agent", "number"])
def test_a_cold_target_that_joins_while_the_primarys_leg_ends_keeps_the_caller_through_every_later_pass(
    call, monkeypatch, target
):
    to = {"type": "number", "number": "+15550009"}
    call.transfer("cold", {"type": "agent", "agent_id": str(call.agent())} if target == "agent" else to)
    [cold] = call.rows(via="transfer")
    call.bind(cold)
    first, plain = call.row(call.primary).call_uuid, call.rest

    def meanwhile(tenant, method, path, **params):
        """The cold target joins while the first agent's leg ends, and takes the call."""
        assert (method, path) == ("DELETE", f"Call/{first}/")
        monkeypatch.setattr(plivo, "rest", plain)
        call.join(cold)
        return plain(tenant, method, path, **params)

    monkeypatch.setattr(plivo, "rest", meanwhile)
    calls.hangup(call.cid, p=call.key)
    call.members = [{"call_uuid": call.caller, "member_id": "m-caller"}]
    try:
        calls.hold(call.cid, calls.Hold(on=True), p=call.key)
        answer = "held"
    except ApiError as e:
        answer = e.code
    assert answer == ("held" if target == "agent" else "call_not_connected")
    jobs.run_now("teardown", str(call.cid), wait=True)
    calls.hangup(call.cid, p=Principal(str(call.tenant), "agent", agent_id=str(call.agent_id), role="agent"))
    assert call.row(cold).state == "active"
    assert {path for method, path, _ in call.sent if method == "DELETE"} == {f"Call/{first}/"}


def test_a_transfer_rings_no_agent_freed_while_it_waited_on_another_agents_lock(call):
    team = call.team()
    P, locked, freed = T.presence, call.agent(team), call.agent(team)
    with core.tx() as db:
        away = call.doc(db, "status", {"name": "Away", "availability": False})
        db.execute(update(P).where(P.c.agent_id == freed).values(status_id=away))

    def hold(db):
        db.execute(select(P).where(P.c.agent_id == locked).with_for_update())
        db.execute(update(P).where(P.c.agent_id == freed).values(status_id=call.available))

    def pick():
        with core.tx() as db:
            xfer = {"via": "transfer", "mode": "cold", "transfer_id": uuid.uuid4()}
            target = m.TeamTarget(type="team", team_id=team)
            return [str(a.agent_id) for a in calls._free(db, cv.lock(db, call.cid), call.primary, target, xfer, [])]

    assert while_held(hold, pick) == [str(locked)]


def test_a_transfer_whose_only_target_goes_offline_while_it_rings_fails(call):
    target = call.agent()
    tid = call.transfer("cold", {"type": "agent", "agent_id": str(target)})
    call.bind(*call.rows(via="transfer"))
    with core.tx() as db:
        away = call.doc(db, "status", {"name": "Away", "availability": False})
        agents.set_status(db, str(call.tenant), target, away, "manual")
    jobs.run_now("route_agent", str(target), wait=True)
    [failed] = call.events("call.transferred", status="failed")
    assert (failed["transfer_id"], failed["status"], failed["agent_id"]) == (tid, "failed", str(call.agent_id))
    assert failed["target"] == {"type": "agent", "agent_id": str(target)} and call.primaries()[0].id == call.primary.id


def test_a_dial_into_a_text_conversation_and_the_accept_of_an_offer_there_both_commit():
    """The dial pauses right after it share-locks the agent's presence; the accept of the agent's offer on the same
    text conversation holds that conversation, then resets the agent's misses."""
    t, P = Tenant(), T.presence
    a = t.agent()
    with core.tx() as db:
        free = t.doc(db, "status", name="Ready", availability=True)
        db.execute(update(P).where(P.c.agent_id == a).values(status_id=free))
    with core.tx() as db:
        conv = cv.lock(db, t.open(channel="sms").id)
        offer = cv.offer(db, conv, a)
    paused, release, dialer = Future(), threading.Event(), []

    def pause(conn, cursor, statement, *_) -> None:
        if (
            threading.get_ident() in dialer
            and "hs.presence" in statement
            and "FOR SHARE" in statement
            and not paused.done()
        ):
            paused.set_result(cursor.connection.get_backend_pid())
            release.wait(10)

    def dial() -> None:
        dialer.append(threading.get_ident())
        body = outbound.DialIn(agent_id=a, to="+15550001", conversation_id=conv.id)
        with core.tx() as db:
            outbound._open(db, t.key, body, {"id": str(a)}, "+15550002", "sip:u@phone.plivo.com")

    def accept() -> None:
        with core.tx() as db:
            cv.lock(db, conv.id)
            cv.move(db, offer, "accept")

    event.listen(core.engine, "after_cursor_execute", pause)
    try:
        with ThreadPoolExecutor(2) as pool:
            try:
                dialing = pool.submit(dial)
                pid = paused.result(5)
                accepting = pool.submit(accept)
                assert waiter(pid), "the accept waits for the paused dial"
            finally:
                release.set()
    finally:
        event.remove(core.engine, "after_cursor_execute", pause)
    accepting.result()
    with pytest.raises(ApiError) as refused:
        dialing.result()
    assert refused.value.code == "conversation_not_joinable"
