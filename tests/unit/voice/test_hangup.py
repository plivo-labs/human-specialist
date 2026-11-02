"""The hangup/close FSM: MPCAgentHangupEvent's reason ladder (primary/secondary/supervisor), the
3010/2020 capacity-only release (warm-external skip), the 4020 continue-flow, the cause-code →
routing-event mapping (incl. the 'reverse routing ' trailing space), handle_reverse_routing_and_agent_capacity
(RRLOCK → pick → call capacity swap → add_agents_to_mpc), the MTBPAK promotion + transfer chunks,
mpc_end/ParticipantExit/AddParticipantByAPIActionCompleted, the hangup-chunk sink, and the voice
wrap-up branches.
"""

import json

import fakeredis
import pytest

import hs.voice.hangup as vh
import tests.conftest  # noqa: F401 — eager data_adapter imports (AbstractConcreteBase registry)
from hs.admin.account import RoutingSettings
from hs.infra.redis import Cache
from hs.routing import account_read as ar
from hs.routing import constants as rc
from hs.routing.invitation_models import AgentAndContactMapping
from hs.routing.redis_store import CAPACITY_OLD_CONTACT_NOT_FOUND, RoutingRedis
from hs.voice import constants as vc
from hs.voice.redis_store import VoiceRedis

BEN = "sip:ben@x.sip.plivo.com"
CARA = "sip:cara@x.sip.plivo.com"
CP = {
    "contact_uuid": "c-1",
    "conversation_number": "5550001111",
    "auth_id": "AUTH1",
    "auth_token": "tok",
    "flow_run_uuid": "flow-1",
    "queue_and_route_run_uuid": "qnr-1",
    "from_number": "+14150000001",
    "to": "+18000000001",
    "node_uuid": "n-1",
    "queue_uuid": "q-1",
    "flow_uuid": "pu-1",
    "flow_name": "flow",
}


def _mapping(sip=BEN, qnr="qnr-1", agent_uuid="aom-ben", from_number="+14150000001"):
    return {
        "AgentSipEndpoint": sip,
        "FromNumber": from_number,
        "QnrRunUuid": qnr,
        "AgentUUID": agent_uuid,
        "AgentCallUuid": "leg-1",
        "PreviousPrimaryAgent": "",
        "FlowRunUUID": "flow-1",
        "ToNumber": "+18000000001",
        "IsInbound": True,
    }


class FakeRR:
    def __init__(
        self,
        *,
        mapping=None,
        cp=CP,
        transfer_info=None,
        lock=True,
        to_be_primary=None,
        qnr_cfg=None,
        attempt=(3, None),
        update_result=None,
        inc_result=(0, None),
        offers=None,
        made=(),
    ):
        self.offers, self.made = offers or {}, set(made)
        self.mapping = mapping
        self.cp = cp
        self.transfer_info = transfer_info
        self.lock = lock
        self.to_be_primary = to_be_primary
        self.qnr_cfg = qnr_cfg or json.dumps({"queue": "q-1", "sticky_routing_type": "No sticky routing"})
        self.attempt = attempt
        self.update_result = update_result
        self.inc_result = inc_result
        self.calls = {
            "decrease": [],
            "update": [],
            "set_mapping": [],
            "del_tbp": [],
            "del_tip": [],
            "lock": [],
            "increase": [],
            "lock_del": [],
        }

    def get_contact_to_agent_mapping(self, contact_uuid):
        return self.mapping

    def get_contact_parameters(self, contact_uuid, qnr_run_uuid):
        return self.cp

    def get_offer(self, run_uuid, agent):
        return self.offers.get((run_uuid, agent), "")

    def is_offer_of(self, run_uuid, agent, offer_id):
        return (run_uuid, agent, offer_id) in self.made

    def get_transfer_in_progress_info(self, contact_uuid):
        return self.transfer_info

    def acquire_reverse_routing_lock_bool(self, sip, conv, channel):
        self.calls["lock"].append((sip, conv, channel))
        return self.lock

    def decrease_agents_capacity(self, auth_id, agent, channel, conversation_id):
        self.calls["decrease"].append((auth_id, agent, channel, conversation_id))
        return

    def update_agents_capacity(self, auth_id, agent, channel, old, new, cap):
        self.calls["update"].append((auth_id, agent, channel, old, new, cap))
        return self.update_result

    def increase_agent_capacity(self, auth_id, agent, channel, org_capacity, conversation_id):
        self.calls["increase"].append((auth_id, agent, channel, org_capacity, conversation_id))
        return self.inc_result

    def delete_reverse_routing_lock(self, agent, conversation_id, channel):
        self.calls["lock_del"].append((agent, conversation_id, channel))
        return

    def get_queue_and_route_config(self, flow, qnr):
        return self.qnr_cfg

    def get_to_be_primary_agent(self, qnr):
        return self.to_be_primary

    def set_contact_to_agent_mapping(self, contact_uuid, mapping_json, ttl):
        self.calls["set_mapping"].append((contact_uuid, mapping_json, ttl))
        return

    def delete_to_be_primary_agent(self, qnr):
        self.calls["del_tbp"].append(qnr)

    def delete_transfer_in_progress(self, contact_uuid):
        self.calls["del_tip"].append(contact_uuid)
        return

    def get_invited_agent_attempt(self, flow, qnr, agent):
        return self.attempt


class FakeVR:
    def __init__(self, *, sup=None, sup_err=None, sup_call=None, sup_call_err=None, mpc_end=("", None)):
        self.sup = sup
        self.sup_err = sup_err
        self.sup_call = sup_call
        self.sup_call_err = sup_call_err
        self.mpc_end = dict(
            mpc_end if isinstance(mpc_end, dict) else {vc.MPC_END_INITIATED: mpc_end, vc.MPC_END_EVENT: mpc_end}
        )
        self.calls = {"del_sup": [], "del_csup": [], "as_reset": [], "toggle": [], "del_mtck": [], "remove_cap": []}

    def get_call_to_supervisor_mapping(self, call_uuid):
        return self.sup, self.sup_err

    def get_supervisor_to_call_mapping(self, sup_uuid):
        return self.sup_call, self.sup_call_err

    def delete_supervisor_to_call_mapping(self, sup_uuid, call_uuid):
        self.calls["del_sup"].append((sup_uuid, call_uuid))
        return

    def delete_call_to_supervisor_mapping(self, call_uuid, sup_uuid):
        self.calls["del_csup"].append((call_uuid, sup_uuid))
        return

    def set_agent_switch_role(self, qnr, sip, value):
        self.calls["as_reset"].append((qnr, sip, value))
        return

    def get_mpc_callback_event(self, call_uuid, qnr, event):
        return self.mpc_end.get(event, ("", None))

    def toggle_active_conference(self, qnr, value, ttl):
        self.calls["toggle"].append((qnr, value, ttl))
        return

    def delete_transfer_mpc_call_uuids(self, qnr):
        self.calls["del_mtck"].append(qnr)

    def remove_current_participant(self, call_uuid, member):
        self.calls["remove_cap"].append((call_uuid, member))
        return


def _inline(fn, *args):
    fn(*args)


def _hangup_params(**over):
    p = {
        "to": BEN,
        "role": "agent",
        "plivo_hangup_cause": "NORMAL_HANGUP",
        "plivo_hangup_cause_code": "4000",
        "hangup_time": "2026-06-11 10:00:00",
        "answer_time": "2026-06-11 09:59:00",
        "plivo_hangup_source": "Caller",
        "add_participant_header": "",
        "add_participant_alias_name": "",
        "digits_sent": "",
        "member_address": "",
        "is_mpc_termination_source": "",
        "participant_role": "",
        "participant_call_uuid": "leg-9",
        "participant_exit_time": "2026-06-11 10:00:01",
        "mpc_end_time": "",
        "add_participant_api_request_outcome": "",
        "add_participant_api_request_outcome_reason": "",
    }
    p.update(over)
    return p


# ============================================================================ reverse routing
def test_reverse_routing_lock_fail_no_capacity_change(monkeypatch):
    rr = FakeRR(lock=False)
    monkeypatch.setattr(vh, "RoutingRedis", lambda: rr)
    res, err = vh.handle_reverse_routing_and_agent_capacity(BEN, "call", "555000", db=object())
    assert (res, err) == ("handled reverse routing and agent capacity", "failed to acquire reverse routing lock")
    assert rr.calls["decrease"] == [] and rr.calls["update"] == []


def test_reverse_routing_pick_error_decreases_with_call_channel(monkeypatch):
    rr = FakeRR()
    monkeypatch.setattr(vh.rrv, "get_contact", lambda *a, **k: (None, "agent is not available"))
    monkeypatch.setattr(ar, "get_org_details_for_agent_sip", lambda sip: type("O", (), {"auth_id": "AUTH1"})())
    monkeypatch.setattr(vh, "RoutingRedis", lambda: rr)
    res, err = vh.handle_reverse_routing_and_agent_capacity(BEN, "call", "old-conv", db=object())
    assert (res, err) == ("", None)  # the decrease's None err IS the result err
    assert rr.calls["decrease"] == [("AUTH1", BEN, rc.CHANNEL_CALL, "old-conv")]


def test_reverse_routing_pick_error_org_miss(monkeypatch):
    rr = FakeRR()
    monkeypatch.setattr(vh.rrv, "get_contact", lambda *a, **k: (None, "boom"))
    monkeypatch.setattr(ar, "get_org_details_for_agent_sip", lambda sip: None)
    monkeypatch.setattr(vh, "RoutingRedis", lambda: rr)
    res, err = vh.handle_reverse_routing_and_agent_capacity(BEN, "call", "old", db=object())
    assert (res, err) == ("failed to run reverse routing", "not found")
    assert rr.calls["decrease"] == []


class _Contact:
    contact_uuid = "c-1"
    qnr_run_uuid = "qnr-1"


def _patch_pick(monkeypatch, rr, settings_capacity=None):
    monkeypatch.setattr(vh.rrv, "get_contact", lambda *a, **k: (_Contact(), None))
    monkeypatch.setattr(
        RoutingSettings,
        "get_org_routing_settings",
        staticmethod(
            lambda auth: None if settings_capacity is None else type("S", (), {"capacity": settings_capacity})()
        ),
    )


def test_reverse_routing_happy_swap_then_add(monkeypatch):
    rr = FakeRR(transfer_info=None)
    _patch_pick(monkeypatch, rr, settings_capacity={"call": 1})
    seen = {}

    def fake_add(cp, agents, primary, is_transfer, ttype, dest):
        seen.update(cp=cp, agents=agents, primary=primary, is_transfer=is_transfer, ttype=ttype, dest=dest)

    monkeypatch.setattr(vh, "RoutingRedis", lambda: rr)
    monkeypatch.setattr("hs.voice.agent_leg.add_agents_to_mpc", fake_add)
    res, err = vh.handle_reverse_routing_and_agent_capacity(BEN, "call", "old-conv", db=object())
    assert (res, err) == ("handled reverse routing and agent capacity", None)
    assert rr.calls["update"] == [("AUTH1", BEN, rc.CHANNEL_CALL, "old-conv", "5550001111", 1)]
    assert seen["agents"] == [BEN] and seen["primary"] == "" and seen["dest"] == ""
    assert seen["is_transfer"] is False and seen["ttype"] == "reverse routing"


def test_reverse_routing_flow_transfer_marks_transfer(monkeypatch):
    rr = FakeRR(transfer_info={"transfer_type": "flow"})
    _patch_pick(monkeypatch, rr, settings_capacity=None)  # settings None → capacity 0
    seen = {}
    monkeypatch.setattr(vh, "RoutingRedis", lambda: rr)
    monkeypatch.setattr("hs.voice.agent_leg.add_agents_to_mpc", lambda cp, a, p, t, tt, d: seen.update(t=t, tt=tt))
    _res, err = vh.handle_reverse_routing_and_agent_capacity(BEN, "call", "old", db=object())
    assert err is None and seen == {"t": True, "tt": "reverse routing flow"}
    assert rr.calls["update"][0][5] == 0


def test_reverse_routing_add_failure_decreases_new_conv(monkeypatch):
    rr = FakeRR()
    _patch_pick(monkeypatch, rr, settings_capacity={"call": 1})

    def boom(*a):
        raise vh.voice_agent_leg.AddAgentsToMPCError("mpc fail")

    monkeypatch.setattr(vh, "RoutingRedis", lambda: rr)
    monkeypatch.setattr("hs.voice.agent_leg.add_agents_to_mpc", boom)
    res, err = vh.handle_reverse_routing_and_agent_capacity(BEN, "call", "old-conv", db=object())
    assert (res, err) == ("", "mpc fail")
    assert rr.calls["decrease"] == [("AUTH1", BEN, rc.CHANNEL_CALL, "5550001111")]


def test_reverse_routing_cleanup_error_cannot_mask_primary_add_failure(monkeypatch):
    rr = _DecreaseResultRR(decrease_result="redis cleanup unavailable")
    _patch_pick(monkeypatch, rr, settings_capacity={"call": 1})

    def boom(*_args):
        raise vh.voice_agent_leg.AddAgentsToMPCError("primary mpc failure")

    monkeypatch.setattr(vh, "RoutingRedis", lambda: rr)
    monkeypatch.setattr("hs.voice.agent_leg.add_agents_to_mpc", boom)
    res, err = vh.handle_reverse_routing_and_agent_capacity(BEN, "call", "old-conv", db=object())

    assert res == ""
    assert "primary mpc failure" in err
    assert "capacity rollback failed: redis cleanup unavailable" in err


def test_reverse_routing_releases_lock_on_success(monkeypatch):
    # the RRLOCK (keyed by the OLD conversation) MUST be released once reverse routing completes —
    # previously it was held for its full 1h TTL, blocking every retry on that (agent, conversation).
    rr = FakeRR(transfer_info=None)
    _patch_pick(monkeypatch, rr, settings_capacity={"call": 1})
    monkeypatch.setattr(vh, "RoutingRedis", lambda: rr)
    monkeypatch.setattr("hs.voice.agent_leg.add_agents_to_mpc", lambda *a: None)
    _res, err = vh.handle_reverse_routing_and_agent_capacity(BEN, "call", "old-conv", db=object())
    assert err is None
    assert rr.calls["lock_del"] == [(BEN, "old-conv", "call")]


def test_reverse_routing_lock_release_is_best_effort(monkeypatch):
    # a Redis error while releasing the RRLOCK must not mask the handler's real result nor raise.
    rr = FakeRR(transfer_info=None)
    _patch_pick(monkeypatch, rr, settings_capacity={"call": 1})

    def boom_del(*a):
        raise RuntimeError("redis down")

    rr.delete_reverse_routing_lock = boom_del
    monkeypatch.setattr(vh, "RoutingRedis", lambda: rr)
    monkeypatch.setattr("hs.voice.agent_leg.add_agents_to_mpc", lambda *a: None)
    res, err = vh.handle_reverse_routing_and_agent_capacity(BEN, "call", "old-conv", db=object())
    assert (res, err) == ("handled reverse routing and agent capacity", None)


def test_reverse_routing_old_contact_not_found_recovers_and_adds(monkeypatch):
    # -1 "old contact not found" on the swap → the release was a no-op, so recover with an ADD-ONLY
    # acquire of the NEW contact and still dial the agent (reassignment completes); lock released.
    rr = FakeRR(transfer_info=None, update_result=CAPACITY_OLD_CONTACT_NOT_FOUND, inc_result=(0, None))
    _patch_pick(monkeypatch, rr, settings_capacity={"call": 1})
    added = {}
    monkeypatch.setattr(vh, "RoutingRedis", lambda: rr)
    monkeypatch.setattr(
        "hs.voice.agent_leg.add_agents_to_mpc", lambda cp, agents, *a: added.update(agents=list(agents))
    )
    res, err = vh.handle_reverse_routing_and_agent_capacity(BEN, "call", "old-conv", db=object())
    assert (res, err) == ("handled reverse routing and agent capacity", None)
    assert rr.calls["increase"] == [("AUTH1", BEN, rc.CHANNEL_CALL, 1, "5550001111")]
    assert added == {"agents": [BEN]}
    assert rr.calls["lock_del"] == [(BEN, "old-conv", "call")]


def test_reverse_routing_old_contact_not_found_recovery_capacity_full_stops(monkeypatch):
    # if the recovery acquire finds the agent genuinely occupied (-2) we must NOT dial (no
    # over-allocation) and must still release the lock.
    rr = FakeRR(
        transfer_info=None,
        update_result=CAPACITY_OLD_CONTACT_NOT_FOUND,
        inc_result=(-2, "error in increasing agent capacity"),
    )
    _patch_pick(monkeypatch, rr, settings_capacity={"call": 1})
    added = {}
    monkeypatch.setattr(vh, "RoutingRedis", lambda: rr)
    monkeypatch.setattr("hs.voice.agent_leg.add_agents_to_mpc", lambda *a: added.setdefault("called", True))
    res, err = vh.handle_reverse_routing_and_agent_capacity(BEN, "call", "old-conv", db=object())
    assert (res, err) == ("", "error in increasing agent capacity")
    assert added == {}
    assert rr.calls["lock_del"] == [(BEN, "old-conv", "call")]


def test_reverse_routing_other_update_error_returns_and_releases_lock(monkeypatch):
    # a non-(-1) capacity error still aborts with no recovery/dial, but the lock is released.
    rr = FakeRR(transfer_info=None, update_result="failed to update capacity as capacity already occupied")
    _patch_pick(monkeypatch, rr, settings_capacity={"call": 1})
    added = {}
    monkeypatch.setattr(vh, "RoutingRedis", lambda: rr)
    monkeypatch.setattr("hs.voice.agent_leg.add_agents_to_mpc", lambda *a: added.setdefault("called", True))
    res, err = vh.handle_reverse_routing_and_agent_capacity(BEN, "call", "old-conv", db=object())
    assert (res, err) == ("", "failed to update capacity as capacity already occupied")
    assert rr.calls["increase"] == [] and added == {}
    assert rr.calls["lock_del"] == [(BEN, "old-conv", "call")]


class _DecreaseResultRR(FakeRR):
    """FakeRR whose decrease returns a configurable result (the base fake always succeeds)."""

    def __init__(self, decrease_result=None, **kw):
        super().__init__(**kw)
        self.decrease_result = decrease_result

    def decrease_agents_capacity(self, auth_id, agent, channel, conversation_id):
        super().decrease_agents_capacity(auth_id, agent, channel, conversation_id)
        return self.decrease_result


@pytest.mark.parametrize(
    "already_clear_err",
    [
        "key not found while decreasing capacity",  # Lua -4: whole key gone
        "failed to decrease capacity as contact not found",  # Lua -1: contact absent
    ],
)
def test_reverse_routing_already_clear_release_is_idempotent_noop(monkeypatch, already_clear_err):
    # The +5s async capacity-clearance re-runs the wrap-up after the
    # synchronous path already released — the release finds nothing and must be a quiet
    # success, not an error that bubbles to "error in handling reverse routing" at ERROR.
    rr = _DecreaseResultRR(decrease_result=already_clear_err)
    monkeypatch.setattr(vh.rrv, "get_contact", lambda *a, **k: (None, None))
    monkeypatch.setattr(ar, "get_org_details_for_agent_sip", lambda sip: type("O", (), {"auth_id": "AUTH1"})())
    monkeypatch.setattr(vh, "RoutingRedis", lambda: rr)
    res, err = vh.handle_reverse_routing_and_agent_capacity(BEN, "call", "old-conv", db=object())
    assert (res, err) == ("capacity already clear", None)
    assert rr.calls["decrease"] == [("AUTH1", BEN, rc.CHANNEL_CALL, "old-conv")]


def test_reverse_routing_no_contact_real_decrease_error_still_propagates(monkeypatch):
    # Only the two already-clear sentinels are no-ops; a genuine failure keeps its error contract.
    rr = _DecreaseResultRR(decrease_result="redis: connection refused")
    monkeypatch.setattr(vh.rrv, "get_contact", lambda *a, **k: (None, None))
    monkeypatch.setattr(ar, "get_org_details_for_agent_sip", lambda sip: type("O", (), {"auth_id": "AUTH1"})())
    monkeypatch.setattr(vh, "RoutingRedis", lambda: rr)
    res, err = vh.handle_reverse_routing_and_agent_capacity(BEN, "call", "old-conv", db=object())
    assert (res, err) == ("", "redis: connection refused")


# ============================================================================ the hangup FSM
def _fsm(
    monkeypatch,
    rr,
    vr,
    params,
    *,
    active_child=(None, "x"),
    reverse=("rev-res", None),
    call_uuid="c-1",
    qnr_run_uuid="qnr-1",
):
    import hs.voice.monitor as vmon

    calls = {"primary": [], "secondary": [], "publish": [], "events": [], "reverse": [], "decrease_norr": []}
    calls["supervisor"] = []
    monkeypatch.setattr(vh, "_active_child", lambda c: active_child[0])
    monkeypatch.setattr(vmon, "mark_leave_requested", lambda *a: calls["supervisor"].append(("mark", *a)))
    monkeypatch.setattr(vmon, "drop_supervisor_leg", lambda *a: calls["supervisor"].append(("drop", *a)))
    monkeypatch.setattr(vh, "handle_primary_agent_leave", lambda *a, **k: calls["primary"].append(a))
    monkeypatch.setattr(vh, "handle_secondary_agent_leave", lambda *a, **k: calls["secondary"].append(a))

    def fake_reverse(sip, channel, conv, db=None, routing_redis=None, add_agents_to_mpc=None):
        calls["reverse"].append((sip, channel, conv))
        return reverse

    monkeypatch.setattr(vh, "handle_reverse_routing_and_agent_capacity", fake_reverse)
    monkeypatch.setattr(vh, "RoutingRedis", lambda: rr)
    monkeypatch.setattr(vh, "VoiceRedis", lambda: vr)
    monkeypatch.setattr(vh, "_spawn", _inline)
    msg, err = vh.handle_agent_hangup_event(call_uuid, qnr_run_uuid, params, db=object())
    return msg, err, calls


def test_hangup_primary_normal_no_reverse(monkeypatch):
    # primary leave + active child with a primary agent and no flags → reason stays empty
    rr = FakeRR(mapping=_mapping())
    msg, err, calls = _fsm(
        monkeypatch,
        rr,
        FakeVR(),
        _hangup_params(),
        active_child=({"primary_agent": "aom-ben", "update_capacity_on_resolve": False}, None),
    )
    assert (msg, err) == ("successfully handled hangup", None)
    assert len(calls["primary"]) == 1 and calls["reverse"] == []


def test_hangup_primary_agent_none_is_not_an_unstarted_conversation(monkeypatch):
    # only an empty-string primary_agent means conversationNotStarted; a JSON null does not
    rr = FakeRR(mapping=_mapping())
    msg, err, calls = _fsm(
        monkeypatch,
        rr,
        FakeVR(),
        _hangup_params(),
        active_child=({"primary_agent": None, "update_capacity_on_resolve": False}, None),
    )
    assert (msg, err) == ("successfully handled hangup", None) and calls["reverse"] == []


def test_hangup_conversation_not_started_reverse_routes(monkeypatch):
    rr = FakeRR(mapping=_mapping())
    msg, err, calls = _fsm(monkeypatch, rr, FakeVR(), _hangup_params(), active_child=({"primary_agent": ""}, None))
    assert (msg, err) == ("rev-res", None)
    assert calls["reverse"] == [(BEN, rc.CHANNEL_CALL, "5550001111")]


def test_hangup_update_capacity_on_resolve_suppresses_events(monkeypatch):
    rr = FakeRR(mapping=_mapping())
    msg, err, calls = _fsm(
        monkeypatch,
        rr,
        FakeVR(),
        _hangup_params(),
        active_child=({"primary_agent": "aom-ben", "update_capacity_on_resolve": True}, None),
    )
    assert (msg, err) == ("rev-res", None)
    assert calls["reverse"]


def test_hangup_reverse_routing_error_propagates_and_stops_event_fanout(monkeypatch):
    rr = FakeRR(mapping=_mapping())
    msg, err, _calls = _fsm(
        monkeypatch,
        rr,
        FakeVR(),
        _hangup_params(),
        active_child=(None, "x"),
        reverse=("", "known_error: no valid contact found"),
    )
    assert (msg, err) == (None, "known_error: no valid contact found")


def test_hangup_contact_params_miss_is_redis_nil(monkeypatch):
    rr = FakeRR(mapping=None, cp=None)  # mapping miss → reverse path; cp miss
    msg, err, _calls = _fsm(monkeypatch, rr, FakeVR(), _hangup_params())
    assert (msg, err) == (None, "not found in cache")


def _org_for_sip(monkeypatch, org=ar.OrgDetails(7, "AUTH1", "tok", "org")):
    seen = []
    monkeypatch.setattr(vh.ar, "get_org_details_for_agent_sip", lambda sip: seen.append(sip) or org)
    return seen


def test_hangup_3010_decreases_capacity_only(monkeypatch):
    rr = FakeRR(mapping=None)  # 'agent mapping not found'
    _org_for_sip(monkeypatch)
    msg, err, calls = _fsm(monkeypatch, rr, FakeVR(), _hangup_params(plivo_hangup_cause_code="3010"))
    assert (msg, err) == ("successfully handled hangup", None)
    assert rr.calls["decrease"] == [("AUTH1", BEN, rc.CHANNEL_CALL, "5550001111")] and calls["reverse"] == []


def test_hangup_3010_capacity_already_clear_skips_events(monkeypatch):
    rr = _DecreaseResultRR("key not found while decreasing capacity", mapping=None)
    _org_for_sip(monkeypatch)
    msg, err, _calls = _fsm(monkeypatch, rr, FakeVR(), _hangup_params(plivo_hangup_cause_code="3010"))
    assert (msg, err) == ("successfully handled hangup", None)


def test_hangup_3010_of_a_number_leg_after_its_transfer_closed_is_handled(monkeypatch):
    rr = FakeRR(mapping=_mapping())
    _org_for_sip(monkeypatch, org=None)
    params = _hangup_params(to="+15550001234", plivo_hangup_cause_code="3010", answer_time="")
    msg, err, _calls = _fsm(monkeypatch, rr, FakeVR(), params)
    assert (msg, err) == ("successfully handled hangup", None) and rr.calls["decrease"] == []


def test_hangup_capacity_dependency_failure_is_explicitly_retryable(monkeypatch):
    rr = _DecreaseResultRR("capacity store unavailable", mapping=None)
    _org_for_sip(monkeypatch)

    msg, err, _calls = _fsm(monkeypatch, rr, FakeVR(), _hangup_params(plivo_hangup_cause_code="3010"))

    assert msg is None
    assert err == "retryable voice callback: capacity store unavailable"
    assert vh.is_retryable_voice_callback_error(err) is True


@pytest.mark.parametrize(("org", "cp", "expected"), [(None, CP, "not found"), (True, None, "not found in cache")])
def test_hangup_permanent_lookup_misses_are_not_tagged_retryable(monkeypatch, org, cp, expected):
    rr = FakeRR(mapping=None, cp=cp)
    _org_for_sip(monkeypatch, ar.OrgDetails(7, "AUTH1", "tok", "org") if org else None)

    msg, err, _calls = _fsm(monkeypatch, rr, FakeVR(), _hangup_params(plivo_hangup_cause_code="3010"))

    assert (msg, err) == (None, expected)
    assert vh.is_retryable_voice_callback_error(err) is False
    assert rr.calls["decrease"] == []


def test_hangup_2020_warm_external_skips_decrease(monkeypatch):
    rr = FakeRR(mapping=None, transfer_info={"transfer_type": "warm_external"})
    looked_up = _org_for_sip(monkeypatch)
    msg, err, _calls = _fsm(monkeypatch, rr, FakeVR(), _hangup_params(plivo_hangup_cause_code="2020"))
    assert (msg, err) == ("successfully handled hangup", None)
    assert looked_up == [] and rr.calls["decrease"] == []


def test_hangup_secondary_spawns_secondary_leave(monkeypatch):
    rr = FakeRR(mapping=_mapping(sip=CARA))  # primary is cara, hangup is ben
    monkeypatch.setattr(vh.ar, "get_aom_row_for_sip", lambda _sip: type("Aom", (), {"uuid": "aom-ben"})())
    msg, err, calls = _fsm(monkeypatch, rr, FakeVR(), _hangup_params(), active_child=(None, "unused"))
    assert (msg, err) == ("rev-res", None)  # 'secondary agent hangup' reverse-routes
    assert len(calls["secondary"]) == 1 and calls["secondary"][0][3] == CARA
    assert calls["primary"] == []


@pytest.mark.parametrize(
    ("contact_uuid", "qnr_run_uuid", "winner_sip", "loser_sip"),
    [
        (
            "00000000-0000-4000-8000-0000000000c1",
            "00000000-0000-4000-8000-0000000000e1",
            "sip:agent-ana@phone.plivo.com",
            "sip:agent-ben@phone.plivo.com",
        ),
        (
            "00000000-0000-4000-8000-0000000000c2",
            "00000000-0000-4000-8000-0000000000e2",
            "sip:agent-cara@phone.plivo.com",
            "sip:agent-dev@phone.plivo.com",
        ),
        (
            "00000000-0000-4000-8000-0000000000c2",
            "00000000-0000-4000-8000-0000000000e2",
            "sip:agent-cara@phone.plivo.com",
            "sip:agent-ana@phone.plivo.com",
        ),
    ],
)
def test_unanswered_losing_invites_do_not_run_transfer_cleanup(
    monkeypatch, contact_uuid, qnr_run_uuid, winner_sip, loser_sip
):
    rr = FakeRR(mapping=_mapping(sip=winner_sip))
    loser_uuid = f"aom-{loser_sip.split(':', 1)[1].split('@', 1)[0]}"
    monkeypatch.setattr(
        vh.ar, "get_aom_row_for_sip", lambda sip: type("Aom", (), {"uuid": loser_uuid})() if sip == loser_sip else None
    )

    msg, err, calls = _fsm(
        monkeypatch,
        rr,
        FakeVR(),
        _hangup_params(to=loser_sip, role="Agent", answer_time="", add_participant_header=""),
        active_child=(None, "unused"),
        call_uuid=contact_uuid,
        qnr_run_uuid=qnr_run_uuid,
    )

    assert (msg, err) == ("rev-res", None)
    assert calls["secondary"] == []
    assert calls["reverse"] == [(loser_sip, rc.CHANNEL_CALL, "5550001111")]


@pytest.mark.parametrize(
    ("transfer_info", "add_participant_header"),
    [({"transfer_type": rc.TRANSFER_TYPE_AGENT}, ""), (None, vc.HEADER_PARTICIPANT_EXISTS_VALUE)],
)
def test_unanswered_real_secondary_or_external_still_runs_secondary_cleanup(
    monkeypatch, transfer_info, add_participant_header
):
    rr = FakeRR(mapping=_mapping(sip=CARA), transfer_info=transfer_info)
    monkeypatch.setattr(vh.ar, "get_aom_row_for_sip", lambda _sip: type("Aom", (), {"uuid": "aom-ben"})())

    msg, err, calls = _fsm(
        monkeypatch,
        rr,
        FakeVR(),
        _hangup_params(answer_time="", role="Agent", add_participant_header=add_participant_header),
        active_child=(None, "unused"),
    )

    assert (msg, err) == ("rev-res", None)
    assert len(calls["secondary"]) == 1


def test_hangup_supervisor_vt6056_other_call_clears_reason(monkeypatch):
    sup = {"supervisor_uuid": "sup-1", "supervisor_sip": BEN, "conversation_number": "555"}
    vr = FakeVR(sup=sup, sup_call={"call_uuid": "OTHER"})
    rr = FakeRR(mapping=_mapping(sip=CARA))
    msg, err, calls = _fsm(monkeypatch, rr, vr, _hangup_params(role="supervisor"))
    assert (msg, err) == ("successfully handled hangup", None)  # reason cleared → no reverse
    assert vr.calls["del_sup"] == [("sup-1", "c-1")]  # leave event STILL ran
    assert vr.calls["del_csup"] == [("c-1", "sup-1")]
    assert calls["secondary"] == []


def test_hangup_supervisor_reverse_uses_supervisor_conversation(monkeypatch):
    sup = {"supervisor_uuid": "sup-1", "supervisor_sip": BEN, "conversation_number": "888"}
    vr = FakeVR(sup=sup, sup_call={"call_uuid": "c-1"})  # same call → keep 'supervisor hangup'
    rr = FakeRR(mapping=_mapping(sip=CARA), cp=None)  # cp must NOT be read on this path
    msg, err, calls = _fsm(monkeypatch, rr, vr, _hangup_params(role="supervisor", call_uuid="leg-3"))
    assert (msg, err) == ("rev-res", None)
    assert calls["reverse"] == [(BEN, rc.CHANNEL_CALL, "888")]
    # their own hangup takes them off, marking the leg first so a join still in flight takes itself off too
    assert calls["supervisor"] == [("mark", "c-1", "leg-3"), ("drop", "c-1", "leg-3", "888", BEN, "sup-1")]


# ============================================================================ offer outcome
@pytest.mark.parametrize(
    "code,room_ended,event,extra",
    [
        ("3020", False, "offer.rejected", {}),
        ("9000", False, "offer.withdrawn", {"reason": "taken"}),
        ("4000", True, "offer.withdrawn", {"reason": "caller_left"}),
        ("4000", False, "offer.missed", {"strike": False}),
        ("3010", False, "offer.missed", {"strike": False}),
    ],
)
def test_an_unanswered_agent_leg_reports_its_offer_outcome(monkeypatch, code, room_ended, event, extra):
    sent = []
    monkeypatch.setattr("hs.conversations.store.emit_by_number", lambda *a: sent.append(a))
    monkeypatch.setattr(
        vh.rona,
        "missed",
        lambda a, o, n, strike: sent.append(("offer.missed", n, {"offer_id": o, "strike": strike}, a)),
    )
    monkeypatch.setattr(vh.ar, "get_aom_for_sip_endpoint", lambda sip: "aom-ben" if sip == BEN else None)
    rr = FakeRR(mapping=_mapping(sip=CARA), offers={("qnr-1", BEN): "offer-1"})
    vr = FakeVR(mpc_end={vc.MPC_END_EVENT: ("1", None) if room_ended else ("", "event not found in cache")})
    _fsm(monkeypatch, rr, vr, _hangup_params(plivo_hangup_cause_code=code, answer_time=""))
    agent = BEN if event == "offer.missed" else "aom-ben"
    assert sent == [(event, CP["conversation_number"], {"offer_id": "offer-1", **extra}, agent)]


def test_an_unanswered_leg_reports_the_offer_it_carried(monkeypatch):
    sent = []
    monkeypatch.setattr("hs.conversations.store.emit_by_number", lambda *a: sent.append(a[2]["offer_id"]))
    monkeypatch.setattr(vh.ar, "get_aom_for_sip_endpoint", lambda sip: "aom-ben")
    rr = FakeRR(mapping=_mapping(sip=CARA), offers={("qnr-1", BEN): "offer-2"}, made={("qnr-1", BEN, "offer-1")})
    _fsm(
        monkeypatch, rr, FakeVR(), _hangup_params(plivo_hangup_cause_code="3020", answer_time="", offer_id="offer%2D1")
    )
    assert sent == ["offer-1"]


def test_a_leg_carrying_another_runs_offer_never_claims_it(monkeypatch):
    sent = []
    monkeypatch.setattr("hs.conversations.store.emit_by_number", lambda *a: sent.append(a[2]["offer_id"]))
    monkeypatch.setattr(vh.ar, "get_aom_for_sip_endpoint", lambda sip: "aom-ben")
    cache = Cache()
    cache._client = fakeredis.FakeStrictRedis(decode_responses=True)
    monkeypatch.setattr(Cache, "_instance", cache)
    rr = RoutingRedis(cache=cache)
    rr.set_offer("qnr-1", [BEN], "offer-1", 60)
    rr.set_offer("qnr-2", [BEN], "offer-2", 60)  # another run's, and maybe another tenant's
    monkeypatch.setattr(rr, "get_contact_parameters", lambda *a: CP)
    monkeypatch.setattr(vh, "RoutingRedis", lambda: rr)
    vh.emit_offer_outcome("c-1", "qnr-1", BEN, "3020", "offer-2")
    vh.emit_offer_outcome("c-2", "qnr-2", BEN, "3020", "offer-2")
    assert sent == ["offer-1", "offer-2"]


def test_a_failed_offer_write_is_returned():
    server = fakeredis.FakeServer()
    cache = Cache()
    cache._client = fakeredis.FakeStrictRedis(server=server, decode_responses=True)
    rr = RoutingRedis(cache=cache)
    assert rr.set_offer("qnr-1", [BEN], "offer-1", 60) is None
    assert (rr.get_offer("qnr-1", BEN), rr.is_offer_of("qnr-1", BEN, "offer-1")) == ("offer-1", True)
    server.connected = False
    assert rr.set_offer("qnr-1", [BEN], "offer-2", 60) == "error while storing the offer"


def test_an_offer_ended_by_two_paths_reports_one_outcome(monkeypatch):
    sent = []
    monkeypatch.setattr("hs.conversations.store.emit_by_number", lambda *a: sent.append(a))
    monkeypatch.setattr(vh.ar, "get_aom_for_sip_endpoint", lambda sip: "aom-ben")
    monkeypatch.setattr(vh, "RoutingRedis", lambda: FakeRR(offers={("qnr-1", BEN): "offer-1"}))
    vh.emit_offer_outcome("c-1", "qnr-1", BEN, "")  # the late pickup's failed join
    vh.emit_offer_outcome("c-1", "qnr-1", BEN, "1000")  # the cancelled ring's hangup
    assert sent == [
        ("offer.withdrawn", CP["conversation_number"], {"offer_id": "offer-1", "reason": "caller_left"}, "aom-ben")
    ]


def test_a_ring_timeout_is_a_rona_strike(monkeypatch):
    strikes = []
    monkeypatch.setattr(vh.rona, "missed", lambda *a: strikes.append(a))
    monkeypatch.setattr(vh.ar, "get_aom_for_sip_endpoint", lambda sip: "aom-ben")
    rr = FakeRR(mapping=_mapping(sip=CARA), offers={("qnr-1", BEN): "offer-1"})
    _fsm(monkeypatch, rr, FakeVR(), _hangup_params(plivo_hangup_cause_code="6010", answer_time=""))
    assert strikes == [(BEN, "offer-1", CP["conversation_number"])]


def test_an_answered_leg_or_one_without_an_offer_reports_nothing(monkeypatch):
    sent = []
    monkeypatch.setattr("hs.conversations.store.emit_by_number", lambda *a: sent.append(a))
    monkeypatch.setattr(vh.ar, "get_aom_for_sip_endpoint", lambda sip: "aom-ben")
    rr = FakeRR(mapping=_mapping(sip=CARA), offers={("qnr-1", BEN): "offer-1"})
    _fsm(monkeypatch, rr, FakeVR(), _hangup_params(plivo_hangup_cause_code="6010"))  # answered
    _fsm(monkeypatch, FakeRR(mapping=_mapping(sip=CARA)), FakeVR(), _hangup_params(answer_time=""))  # no offer
    _fsm(monkeypatch, rr, FakeVR(), _hangup_params(role="supervisor", answer_time=""))
    assert sent == []


# ============================================================================ primary leave
def _leave(monkeypatch, rr, vr, params, transfer_info, *, recording=(True, True, None), child=("child-1", None)):
    calls = {"end": [], "rec": [], "child": [], "event": [], "chunks": [], "monitor": [], "unhold": [], "sticky": []}
    monkeypatch.setattr(vh, "end_mpc", lambda *a: calls["end"].append(a) or None)
    monkeypatch.setattr(ar, "get_organisation_record_value", lambda a, inb: recording)
    monkeypatch.setattr(
        vh, "handle_call_recording_event", lambda c, p, routing_redis=None: calls["rec"].append((c, p)) or None
    )
    import hs.conversations.transfer as ct

    def _child(contact_uuid, transfer_type, channel, agent_sip_or_uuid=None, mode="cold"):
        fields = {"contact_uuid": contact_uuid, "transfer_type": transfer_type, "channel": channel, "mode": mode}
        return calls["child"].append(dict(fields, agent_sip_or_uuid=agent_sip_or_uuid)) or child

    monkeypatch.setattr(ct, "transfer_child", _child)
    monkeypatch.setattr(vh, "_commit_session", lambda: None)
    monkeypatch.setattr(vh, "_signal_chunk", lambda *a: calls["chunks"].append(a))
    monkeypatch.setattr(
        vh.voice_agent_leg,
        "_get_participants",
        lambda *a: ([{"role": "Customer", "member_id": "77", "exit_cause": ""}], None),
    )
    monkeypatch.setattr(vh, "hold_or_unhold_call", lambda *a: calls["unhold"].append(a) or None)
    monkeypatch.setattr(vh.routing_sticky, "is_agent_in_routing_assignment", lambda cfg, u: (True, None))
    monkeypatch.setattr(
        vh.routing_sticky,
        "set_inbound_sticky_mapping",
        lambda u, f, cfg, routing_redis=None: calls["sticky"].append((u, f)) or None,
    )
    monkeypatch.setattr(
        vh, "_get_agent_name_by_uuid", lambda u, a: ({"aom-ben": "Ben", "aom-cara": "Cara"}.get(u, ""), None)
    )
    monkeypatch.setattr(vh, "RoutingRedis", lambda: rr)
    monkeypatch.setattr(vh, "VoiceRedis", lambda: vr)
    monkeypatch.setattr("hs.voice.monitor.hangup_monitored_conversation", lambda *a: calls["monitor"].append(a) or None)
    monkeypatch.setattr(vh, "_spawn", _inline)
    vh.handle_primary_agent_leave("c-1", "qnr-1", params, AgentAndContactMapping.parse_redis(_mapping()), transfer_info)
    return calls


def test_primary_leave_4020_leaves_nothing_to_hand_over(monkeypatch):
    calls = _leave(monkeypatch, FakeRR(), FakeVR(), _hangup_params(plivo_hangup_cause_code="4020"), {})
    assert calls["end"] == [] and calls["child"] == []


def test_primary_leave_no_to_be_primary_ends_mpc(monkeypatch):
    vr = FakeVR()
    calls = _leave(monkeypatch, FakeRR(to_be_primary=None), vr, _hangup_params(), {})
    assert calls["end"] == [("mpc_qnr-1", "AUTH1", "tok", "c-1", "qnr-1")] and calls["child"] == []


def test_primary_leave_same_sip_ends_mpc(monkeypatch):
    rr = FakeRR(to_be_primary=_mapping(sip=BEN))
    vr = FakeVR()
    calls = _leave(monkeypatch, rr, vr, _hangup_params(), {})
    assert calls["end"] == [("mpc_qnr-1", "AUTH1", "tok", "c-1", "qnr-1")]


def test_primary_leave_empty_transfer_stops_after_recording(monkeypatch):
    rr = FakeRR(to_be_primary=_mapping(sip=CARA, agent_uuid="aom-cara"))
    calls = _leave(monkeypatch, rr, FakeVR(), _hangup_params(), {})
    assert calls["rec"] == [("c-1", False)]  # pause = !recording_enabled(True) = False
    assert calls["child"] == [] and rr.calls["del_tip"] == []


def test_primary_leave_mpc_already_ended_skips_child(monkeypatch):
    rr = FakeRR(to_be_primary=_mapping(sip=CARA))
    vr = FakeVR(mpc_end={vc.MPC_END_INITIATED: ("171", None), vc.MPC_END_EVENT: ("", None)})
    calls = _leave(monkeypatch, rr, vr, _hangup_params(), {"transfer_type": "agent"})
    assert calls["child"] == []


def test_primary_leave_promotion_full_chain(monkeypatch):
    rr = FakeRR(
        to_be_primary=_mapping(sip=CARA, agent_uuid="aom-cara"),
        qnr_cfg=json.dumps({"queue": "q-1", "sticky_routing_type": "Bind"}),
    )
    calls = _leave(
        monkeypatch,
        rr,
        FakeVR(),
        _hangup_params(),
        {"transfer_type": "warm_external", "transfer_mode": "warm"},
        recording=(False, True, None),
    )
    # child create: warm_external → external, in the transfer's mode
    payload = calls["child"][0]
    assert payload["transfer_type"] == "external" and payload["agent_sip_or_uuid"] == CARA
    assert payload["mode"] == "warm" and payload["channel"] == rc.CHANNEL_CALL
    # transfer_successful event (original transfer type), mapping promoted w/ previous primary
    promoted = json.loads(rr.calls["set_mapping"][0][1])
    assert promoted["PreviousPrimaryAgent"] == BEN and promoted["AgentSipEndpoint"] == CARA
    assert rr.calls["set_mapping"][0][2] == vc.EXPIRY_DURATION_IN_HOURS_CALL * 3600
    assert calls["sticky"] == [("aom-cara", "+14150000001")]
    assert calls["unhold"][0][3] == "77"  # the customer member unheld
    assert rr.calls["del_tip"] == ["c-1"]
    assert len(calls["monitor"]) == 1  # warm_external → drop supervisor
    assert calls["chunks"][0][1] == "AGENT1_LEAVING"  # warm mode → Agent1 leaving chunk


def test_primary_leave_agent_transfer_no_monitor(monkeypatch):
    rr = FakeRR(to_be_primary=_mapping(sip=CARA, agent_uuid="aom-cara"))
    calls = _leave(monkeypatch, rr, FakeVR(), _hangup_params(), {"transfer_type": "agent", "transfer_mode": "cold"})
    assert calls["monitor"] == []  # only external transfers drop the supervisor
    assert calls["chunks"][0][1] == "TRANSFER_SUCCESSFUL"


# ============================================================================ transfer chunks
def _names(monkeypatch, names):
    monkeypatch.setattr(
        ar, "get_agent_by_uuid", lambda u, a: ar.AgentDetails(u, "s", a, names[u]) if u in names else None
    )


def test_transfer_chunks_cold_agent_names(monkeypatch):
    chunks = []
    monkeypatch.setattr(vh, "_signal_chunk", lambda *a: chunks.append(a))
    _names(monkeypatch, {"aom-ben": "Ben", "aom-cara": "Cara"})
    vh.handle_primary_agent_leave_transfer_chunks(
        "c-1",
        "aom-ben",
        CP,
        {"transfer_type": "agent", "transfer_mode": "cold"},
        AgentAndContactMapping.parse_redis(_mapping(sip=CARA, agent_uuid="aom-cara")),
    )
    _c, desc, ident, itype, msg = chunks[0]
    assert desc == "TRANSFER_SUCCESSFUL" and itype == "agent" and ident == "aom-ben"
    assert msg == "Conversation transferred as agent transfer from Ben to Cara"


def test_transfer_chunks_team_and_external_messages(monkeypatch):
    chunks = []
    monkeypatch.setattr(vh, "_signal_chunk", lambda *a: chunks.append(a))
    _names(monkeypatch, {"aom-ben": "Ben", "aom-cara": "Cara"})
    vh.handle_primary_agent_leave_transfer_chunks(
        "c-1",
        "aom-ben",
        CP,
        {"transfer_type": "team", "transfer_mode": "cold"},
        AgentAndContactMapping.parse_redis(_mapping(sip=CARA, agent_uuid="aom-cara")),
    )
    assert chunks[0][4] == "Conversation transferred as team transfer from Ben to Cara"
    chunks.clear()
    # external: agent2 = contact_params.To; warm_external: agent2 = member address
    vh.handle_primary_agent_leave_transfer_chunks(
        "c-1",
        "aom-ben",
        CP,
        {"transfer_type": "external", "transfer_mode": "cold"},
        AgentAndContactMapping.parse_redis(_mapping(sip="+1999", agent_uuid="")),
    )
    assert chunks[0][4] == "Conversation transferred as external transfer from Ben to +18000000001"
    assert chunks[0][3] == "external"
    chunks.clear()
    vh.handle_primary_agent_leave_transfer_chunks(
        "c-1",
        "aom-ben",
        CP,
        {"transfer_type": "warm_external", "transfer_mode": "cold"},
        AgentAndContactMapping.parse_redis(_mapping(sip="+1999#22", agent_uuid="")),
    )
    assert chunks[0][4] == "Conversation transferred as external transfer from Ben to +1999#22"


def test_transfer_chunks_agent1_lookup_failure_writes_nothing(monkeypatch):
    chunks = []
    monkeypatch.setattr(vh, "_signal_chunk", lambda *a: chunks.append(a))
    _names(monkeypatch, {"aom-cara": "Cara"})  # agent1 (aom-ben) lookup fails
    vh.handle_primary_agent_leave_transfer_chunks(
        "c-1",
        "aom-ben",
        CP,
        {"transfer_type": "agent", "transfer_mode": "cold"},
        AgentAndContactMapping.parse_redis(_mapping(sip=CARA, agent_uuid="aom-cara")),
    )
    assert chunks == []


# ============================================================================ secondary leave
def _secondary(monkeypatch, rr, vr, params, transfer_info, *, participants=None, ext_err=None):
    calls = {"delete": [], "chunks": [], "event": [], "external": []}
    import hs.conversations.transfer as ct

    monkeypatch.setattr(ct, "remove_secondary", lambda **mapping: calls["delete"].append(mapping) or None)
    monkeypatch.setattr(vh, "_commit_session", lambda: None)
    monkeypatch.setattr(vh, "_signal_chunk", lambda *a: calls["chunks"].append(a))
    monkeypatch.setattr(vh, "handle_external_participant_leave", lambda *a, **k: calls["external"].append(a) or ext_err)
    monkeypatch.setattr(
        vh.voice_agent_leg,
        "_get_participants",
        lambda *a: (
            participants if participants is not None else [{"role": "customer", "exit_cause": "", "member_id": "7"}],
            None,
        ),
    )
    monkeypatch.setattr(
        vh.voice_agent_leg,
        "get_account_details_for_sip_endpoints",
        lambda sips: ([{"uuid": "aom-ben", "sip_endpoint": BEN, "name": "Ben", "email": ""}], None),
    )
    monkeypatch.setattr(vh, "RoutingRedis", lambda: rr)
    monkeypatch.setattr(vh, "VoiceRedis", lambda: vr)
    monkeypatch.setattr(vh, "_spawn", _inline)
    vh.handle_secondary_agent_leave("c-1", "qnr-1", params, CARA, transfer_info)
    return calls


def test_secondary_leave_add_participant_header_routes_external(monkeypatch):
    calls = _secondary(monkeypatch, FakeRR(), FakeVR(), _hangup_params(add_participant_header="true"), {})
    assert len(calls["external"]) == 1 and calls["delete"] == []


def test_secondary_leave_unanswered_non_warm_external_noops(monkeypatch):
    calls = _secondary(monkeypatch, FakeRR(), FakeVR(), _hangup_params(answer_time=""), {"transfer_type": "agent"})
    assert calls["delete"] == []


def test_secondary_leave_agent_happy(monkeypatch):
    rr, vr = FakeRR(), FakeVR()
    calls = _secondary(monkeypatch, rr, vr, _hangup_params(), {"transfer_type": "agent"})
    payload = calls["delete"][0]
    assert payload["agent_sip_or_uuid"] == BEN and payload["ignore_event"] is False
    assert "participant_identifier" not in payload
    # customer in MPC → AS reset + MTBPAK + TIP deletes
    assert vr.calls["as_reset"] == [("qnr-1", CARA, "")]
    assert rr.calls["del_tbp"] == ["qnr-1"] and rr.calls["del_tip"] == ["c-1"]
    assert calls["chunks"][0][1] == "AGENT2_DROPPED"
    assert calls["chunks"][0][4] == "Ben dropped from the conversation"


def test_secondary_leave_warm_external_identifier_and_digits(monkeypatch):
    rr, vr = FakeRR(), FakeVR()
    calls = _secondary(
        monkeypatch,
        rr,
        vr,
        _hangup_params(to="+1999", digits_sent="22", answer_time=""),
        {"transfer_type": "warm_external"},
        participants=[{"role": "customer", "exit_cause": "gone", "member_id": "7"}],
    )
    payload = calls["delete"][0]
    assert payload["participant_identifier"] == "+1999#22"
    assert payload["type"] == "external_transfer" and payload["ignore_event"] is True
    # customer NOT in MPC → no AS/MTBPAK/TIP cleanup
    assert vr.calls["as_reset"] == [] and rr.calls["del_tbp"] == []
    assert calls["chunks"][0][4] == "+1999 dropped from the conversation"
    assert calls["chunks"][0][3] == "warm_external"


# ============================================================================ external leave
def _external(monkeypatch, rr, vr, params, *, delete_err=None):
    calls = {"delete": [], "chunks": []}
    import hs.conversations.transfer as ct

    monkeypatch.setattr(ct, "remove_secondary", lambda **mapping: calls["delete"].append(mapping) or delete_err)
    monkeypatch.setattr(vh, "_commit_session", lambda: None)
    monkeypatch.setattr(vh, "_signal_chunk", lambda *a: calls["chunks"].append(a))
    monkeypatch.setattr(vh, "RoutingRedis", lambda: rr)
    monkeypatch.setattr(vh, "VoiceRedis", lambda: vr)
    err = vh.handle_external_participant_leave("c-1", "qnr-1", params)
    return err, calls


def test_external_leave_unanswered_api_hangup_noop(monkeypatch):
    for code in ("1000", "4000", "4020"):
        err, calls = _external(
            monkeypatch, FakeRR(), FakeVR(), _hangup_params(answer_time="", plivo_hangup_cause_code=code)
        )
        assert err is None and calls["delete"] == [] and calls["chunks"] == []


def test_external_leave_unanswered_other_writes_did_not_answer(monkeypatch):
    err, calls = _external(
        monkeypatch,
        FakeRR(),
        FakeVR(),
        _hangup_params(
            answer_time="", plivo_hangup_cause_code="3010", to="+1999", add_participant_alias_name="Bob%20Jr"
        ),
    )
    assert err is None and calls["delete"] == []
    _c, desc, ident, itype, msg = calls["chunks"][0]
    assert desc == "participant_did_not_answered" and itype == ""
    assert msg == "Bob Jr declined the call or failed to pick up" and ident == "+1999"


def test_external_leave_answered_removed_vs_left(monkeypatch):
    vr = FakeVR()
    err, calls = _external(
        monkeypatch,
        FakeRR(),
        vr,
        _hangup_params(to="+1999", plivo_hangup_source="API Request", add_participant_alias_name="Bob"),
    )
    assert err is None
    payload = calls["delete"][0]
    assert payload["secondary_agent_state"] == "participant_removed"
    assert payload["type"] == "add_participant" and payload["participant_alias"] == "Bob"
    assert vr.calls["remove_cap"] == [("c-1", "+1999")]
    vr2 = FakeVR()
    err, calls = _external(monkeypatch, FakeRR(), vr2, _hangup_params(to="+1999", plivo_hangup_source="Caller"))
    assert calls["delete"][0]["secondary_agent_state"] == "participant_left"


def test_external_leave_delete_error_returned(monkeypatch):
    err, _calls = _external(
        monkeypatch, FakeRR(), FakeVR(), _hangup_params(to="+1999"), delete_err="removing the secondary agent failed"
    )
    assert err == "removing the secondary agent failed"


# ============================================================================ dispatcher events
def _chunk_sinks(monkeypatch, *, error=None):
    """The two hangup chunk writers, recorded as (kind, chunk); `error` makes each answer a 4xx."""
    import hs.conversations.child_update as ccu
    import hs.conversations.management as cm
    import hs.routing.invitation as invitation
    from hs.infra.models import UseCaseResponse

    seen = []

    def sink(kind):
        def write(chunk):
            seen.append((kind, chunk))
            return UseCaseResponse(status_code=400, error=error) if error else UseCaseResponse(status_code=201)

        return write

    monkeypatch.setattr(invitation, "_ensure_request_context", lambda: None)
    monkeypatch.setattr(ccu, "add_chunk_to_non_qnr_child", sink("end"))
    monkeypatch.setattr(cm, "create_hangup_conversation_chunk", sink("exit"))
    return seen


def test_mpc_end_chunk_4xx_is_best_effort_after_terminal_fact(monkeypatch):
    legs = []
    monkeypatch.setattr(vh, "reconcile_terminal_call_leg", lambda c, p, is_mpc: legs.append((c, p, is_mpc)))
    monkeypatch.setattr(vh, "_commit_session", lambda: None)
    sinks = _chunk_sinks(monkeypatch, error="missing child")
    msg, err = vh.handle_mpc_end_event("c-1", {"mpc_end_time": "T1"})
    assert (msg, err) == ("successfully handled end mpc", None)
    assert legs == [("c-1", {"mpc_end_time": "T1"}, True)] and [kind for kind, _ in sinks] == ["end"]


def test_participant_exit_termination_source_chunk(monkeypatch):
    legs = []
    monkeypatch.setattr(vh, "reconcile_terminal_call_leg", lambda c, p, is_mpc: legs.append((c, is_mpc)))
    monkeypatch.setattr(vh, "_commit_session", lambda: None)
    sinks = _chunk_sinks(monkeypatch)
    msg, err = vh.handle_participant_exit_event(
        "c-1", _hangup_params(is_mpc_termination_source="true", participant_role="Customer")
    )
    assert (msg, err) == ("Successfully updated with hangup source", None)
    ((kind, chunk),) = sinks
    assert (kind, chunk.contact_uuid, chunk.type, chunk.description, chunk.channel) == (
        "exit",
        "c-1",
        "hangup",
        "Incoming_call_hangup",
        rc.CHANNEL_CALL,
    )
    assert [a.hangup_source for a in chunk.attributes] == ["Customer"] and legs == [("c-1", False)]


def test_participant_exit_chunk_error_400_and_no_source_skips(monkeypatch):
    monkeypatch.setattr(vh, "reconcile_terminal_call_leg", lambda *a, **k: None)
    monkeypatch.setattr(vh, "_commit_session", lambda: None)
    sinks = _chunk_sinks(monkeypatch, error="missing child")
    msg, err = vh.handle_participant_exit_event("c-1", _hangup_params(is_mpc_termination_source="true"))
    assert (msg, err) == ("Successfully updated with hangup source", None) and len(sinks) == 1
    msg, err = vh.handle_participant_exit_event("c-1", _hangup_params(is_mpc_termination_source="false"))
    assert (msg, err) == ("Successfully updated with hangup source", None) and len(sinks) == 1


def test_terminal_leg_retryable_failure_stops_non_durable_fanout(monkeypatch):
    monkeypatch.setattr(vh, "reconcile_terminal_call_leg", lambda *args, **kwargs: vh.CALL_LEG_PERSISTENCE_RETRYABLE)
    sinks = _chunk_sinks(monkeypatch)

    msg, err = vh.handle_mpc_end_event("c-1", {"mpc_end_time": "T1"})

    assert (msg, err) == ("", vh.CALL_LEG_PERSISTENCE_RETRYABLE)
    assert sinks == []


def _apc(monkeypatch, rr, vr, params, *, route=("", None), child=None):
    from hs.conversations.store import Conversation

    monkeypatch.setattr(Conversation, "get_primary_active_conversation", lambda number, aom: child)
    calls = {
        "route": [],
        "publish": [],
        "chunks": [],
        "event": [],
        "unanswered": [],
        "flow": [],
        "node": [],
        "aqe_del": [],
        "aqe_upd": [],
    }
    monkeypatch.setattr(
        vh.voice_callbacks, "route_and_add_to_mpc", lambda *a, **k: calls["route"].append((a, k)) or route
    )
    monkeypatch.setattr(vh.voice_callbacks, "_update_unanswered", lambda c, r, i: calls["unanswered"].append((c, r, i)))
    monkeypatch.setattr(vh.voice_callbacks, "send_to_queue_exit", lambda *a, **k: calls["flow"].append(a) or ("", None))
    monkeypatch.setattr(vh, "_signal_chunk", lambda *a: calls["chunks"].append(a))
    monkeypatch.setattr(vh, "get_conversation_number_from_contact_uuid", lambda c, r: ("5550001111", None))
    monkeypatch.setattr(
        vh.voice_agent_leg,
        "_get_participants",
        lambda *a: ([{"role": "customer", "member_id": "7", "exit_cause": ""}], None),
    )
    monkeypatch.setattr(vh.ar, "get_aom_row_for_sip", lambda s: ar.AomRowForSip(9, "aom-sup", "user-9"))
    monkeypatch.setattr(vh, "hold_or_unhold_call", lambda *a: calls.setdefault("hold", []).append(a) or None)
    monkeypatch.setattr(
        vh.aq,
        "get_current_active_queue_entry_by_contact_uuid",
        lambda db, c, run: type("E", (), {"status": "IN_REVERSE_ROUTING"})(),
    )
    monkeypatch.setattr(vh.aq, "update_queue_status", lambda db, c, q, s, **k: calls["aqe_upd"].append(s))
    monkeypatch.setattr(vh.aq, "delete_active_queue_entry", lambda db, c, q, s: calls["aqe_del"].append(s) or 41)
    monkeypatch.setattr(vh, "RoutingRedis", lambda: rr)
    monkeypatch.setattr(vh, "VoiceRedis", lambda: vr)
    monkeypatch.setattr(vh, "_spawn", _inline)
    msg, err = vh.handle_add_participant_action_completed("c-1", "qnr-1", params, db=object())
    return msg, err, calls


def test_apc_success_agent_resets_conference(monkeypatch):
    vr = FakeVR()
    msg, err, _calls = _apc(
        monkeypatch,
        FakeRR(transfer_info=None),
        vr,
        _hangup_params(add_participant_api_request_outcome="Successful", participant_role="Agent"),
    )
    assert (msg, err) == ("Handled AddParticipantByAPIActionCompleted", None)
    assert vr.calls["toggle"] == [("qnr-1", "false", 0)]


def test_apc_unsuccessful_new_flow_retries_queue(monkeypatch):
    vr = FakeVR()
    msg, err, calls = _apc(
        monkeypatch,
        FakeRR(transfer_info=None),
        vr,
        _hangup_params(add_participant_api_request_outcome="Unsuccessful", participant_role="Agent"),
    )
    assert (msg, err) == ("Handled AddParticipantByAPIActionCompleted", None)
    assert calls["aqe_upd"] == ["IN_TRANSIT"]
    assert len(calls["route"]) == 1


def test_apc_retry_failure_full_chain(monkeypatch):
    vr = FakeVR()
    msg, err, calls = _apc(
        monkeypatch,
        FakeRR(transfer_info={"transfer_type": "flow"}),
        vr,
        _hangup_params(add_participant_api_request_outcome="unsuccessful", participant_role="agent"),
        route=("assignment_failed", "known_error: retry limit reached"),
    )
    assert msg == "" and err == "known_error: retry limit reached"
    assert vr.calls["toggle"] == [("qnr-1", "false", 0)]
    assert calls["aqe_del"] == ["FAILED"]
    assert calls["unanswered"] == [("c-1", "assignment_failed", 41)]
    assert calls["flow"][0][2] == "assignment_failed"  # res rides flow_action


def test_apc_transient_route_failure_skips_terminal_cleanup(monkeypatch):
    vr = FakeVR()
    retryable = vh.retryable_voice_callback_error("provider unavailable")

    msg, err, calls = _apc(
        monkeypatch,
        FakeRR(transfer_info={"transfer_type": "flow"}),
        vr,
        _hangup_params(add_participant_api_request_outcome="unsuccessful", participant_role="agent"),
        route=("", retryable),
    )

    assert (msg, err) == ("", retryable)
    assert vr.calls["toggle"] == []
    assert calls["aqe_del"] == []
    assert calls["unanswered"] == []
    assert calls["flow"] == []


def test_apc_transfer_failed_fanout(monkeypatch, emitted):
    child = type(
        "Child", (), {"uuid": "child-1", "parent_conversation": type("P", (), {"organization": 1, "uuid": "p-1"})}
    )
    vr = FakeVR()
    rr = FakeRR(mapping=_mapping(), transfer_info={"transfer_type": "queue"})
    msg, err, calls = _apc(
        monkeypatch,
        rr,
        vr,
        _hangup_params(add_participant_api_request_outcome="Unsuccessful", participant_role="Agent"),
        child=child,
    )
    assert (msg, err) == ("Handled AddParticipantByAPIActionCompleted", None)
    assert vr.calls["del_mtck"] == ["qnr-1"]
    assert [(t, d["status"], d["transfer_id"]) for t, d, _ in emitted] == [("call.transferred", "failed", "child-1")]
    assert calls["chunks"][0][1] == "TRANSFER_FAILED"
    assert calls["chunks"][0][4] == "Transfer request was not accepted"
    assert calls.get("hold")  # queue transfer → unhold the customer


def test_apc_transfer_failed_agent_team_external_skip_unhold(monkeypatch):
    for ttype in ("agent", "team", "external", "warm_external"):
        vr = FakeVR()
        rr = FakeRR(mapping=_mapping(), transfer_info={"transfer_type": ttype})
        _msg, err, calls = _apc(
            monkeypatch,
            rr,
            vr,
            _hangup_params(add_participant_api_request_outcome="Unsuccessful", participant_role="Agent"),
        )
        assert err is None and "hold" not in calls


def test_apc_supervisor_unsuccessful_is_handled(monkeypatch):
    sup = {"supervisor_uuid": "sup-1", "conversation_number": "888"}
    vr = FakeVR(sup=sup)
    msg, err, _calls = _apc(
        monkeypatch,
        FakeRR(transfer_info=None),
        vr,
        _hangup_params(
            add_participant_api_request_outcome="Unsuccessful",
            participant_role="Supervisor",
            member_address="sip:sup@x",
        ),
    )
    assert (msg, err) == ("Handled AddParticipantByAPIActionCompleted", None)


# ============================================================================ voice wrap-up
class _WrapParams:
    contact_uuid = "c-1"
    parent_contact_uuid = "p-1"
    conversation_number = "5550001111"
    agent_uuid = "aom-ben"
    agent_sip_endpoint = BEN
    channel = "call"
    timeout_type = rc.TIMEOUT_TYPE_SESSION
    send_conversation_closed_event = True
    release_agent_capacity = True
    is_transfer_resolve = False


def test_voice_wrap_up_releases_the_agent(monkeypatch):
    calls = {"reverse": []}
    monkeypatch.setattr(
        vh,
        "handle_reverse_routing_and_agent_capacity",
        lambda sip, ch, conv, db=None, routing_redis=None: calls["reverse"].append((sip, ch, conv)) or ("ok", None),
    )
    assert vh.voice_handle_wrap_up(_WrapParams()) is None
    assert calls["reverse"] == [(BEN, "call", "5550001111")]


def test_voice_wrap_up_reverse_error_returned(monkeypatch):
    monkeypatch.setattr(
        vh, "handle_reverse_routing_and_agent_capacity", lambda *a, **k: ("", "known_error: no valid contact found")
    )
    p = _WrapParams()
    p.send_conversation_closed_event = False
    monkeypatch.setattr(vh, "_spawn", _inline)
    assert vh.voice_handle_wrap_up(p) == "known_error: no valid contact found"


def test_voice_resolve_releases_capacity_on_the_call_channel(monkeypatch):
    calls = {"reverse": []}
    monkeypatch.setattr(
        vh,
        "handle_reverse_routing_and_agent_capacity",
        lambda sip, ch, conv, db=None, routing_redis=None: calls["reverse"].append((sip, ch, conv)) or ("ok", None),
    )
    params = _WrapParams()
    params.channel = "chat"
    assert vh.voice_handle_resolve(params) is None
    assert calls["reverse"] == [(BEN, "call", "5550001111")]  # hard-coded Call channel


# ============================================================================ hangup sink
def test_create_hangup_conversation_chunk_defaults_and_404(monkeypatch):
    from hs.conversations import management as cm
    from hs.conversations.models import ConvChunkInsertByContactUUID
    from hs.conversations.store import Conversation

    monkeypatch.setattr(Conversation, "get_conversation_for_hangup_chunk", classmethod(lambda cls, c: None))
    chunk = ConvChunkInsertByContactUUID(
        contact_uuid="c-1",
        type="hangup",
        description="Incoming_call_hangup",
        channel="call",
        attributes=[{"hangup_source": "Customer"}],
    )
    resp = cm.create_hangup_conversation_chunk(chunk)
    assert resp.status_code == 404 and resp.error == "Conversation not found"

    class FakeParent:
        contact_identifier = "+14150000001"

    class FakeConv:
        id = 7
        direction_inbound = True
        parent_conversation = FakeParent()

    built = {}
    monkeypatch.setattr(Conversation, "get_conversation_for_hangup_chunk", classmethod(lambda cls, c: FakeConv()))
    monkeypatch.setattr(
        ConvChunkInsertByContactUUID,
        "build_db_model",
        lambda self, conversation: (
            built.update(from_user=self.from_user, to_user=self.to_user, conversation=conversation) or "DBCHUNK"
        ),
    )
    monkeypatch.setattr(cm, "create_chunk_with_related_data", lambda c: built.update(persisted=c))
    chunk2 = ConvChunkInsertByContactUUID(
        contact_uuid="c-1", type="hangup", description="Incoming_call_hangup", channel="call"
    )
    resp = cm.create_hangup_conversation_chunk(chunk2)
    assert resp.status_code == 201 and resp.msg == "Conversation chunk added successfully"
    # inbound → from = contact identifier, to = the bot agent uuid
    from hs.infra.constants import SYSTEM_USER_UUID

    assert built["from_user"] == "+14150000001" and built["to_user"] == SYSTEM_USER_UUID
    assert built["persisted"] == "DBCHUNK"


# ============================================================================ call-ended flow
def _flow_fixture(monkeypatch, parent_channel):
    import hs.conversations.child_update as ccu

    _pch = parent_channel

    class FakeAgent:
        uuid = "aom-ben"
        sip_endpoint = BEN

    class FakeParent:
        parent_channel = _pch
        contact_identifier = "+1415"
        parent_contact_uuid = "p"

    class FakeConv:
        id = 9
        contact_uuid = "call-child"
        parent = 5
        parent_conversation = FakeParent()
        primary_agent = FakeAgent()

    class FakeChild:
        parent = 5
        contact_uuid = "bot-child"
        parent_conversation = FakeParent()

    calls = {}
    monkeypatch.setattr(ccu.Conversation, "get_all_active_conversations_for_parent", classmethod(lambda cls, p: []))
    monkeypatch.setattr(
        ccu.Conversation,
        "get_child_by_conv_parent",
        classmethod(
            lambda cls, parent, channel, inbound_direction=True, filter_active=True, filter_qnr=False: (
                calls.setdefault("lookup", (parent, channel, inbound_direction, filter_active)) and FakeConv()
            )
        ),
    )
    monkeypatch.setattr(
        ccu,
        "_handle_capacity_clear_on_call_drop",
        lambda conv, aom_uuid, sip: calls.setdefault("clear", (aom_uuid, sip)),
    )
    monkeypatch.setattr(ccu, "_emit_call_ended", lambda conv: calls.setdefault("ended", conv.contact_uuid))
    return ccu, FakeChild(), calls


def test_call_ended_flow_call_parent_early_200(monkeypatch):
    from hs.conversations.models import NonQNRChunkInsert

    ccu, child, calls = _flow_fixture(monkeypatch, parent_channel="call")
    chunk = NonQNRChunkInsert(
        parent_contact_uuid="p",
        type="signal",
        description="Incoming_call_ended",
        channel="call",
        message_id="",
        is_inbound_chunk=False,
        create_chunk=True,
        is_main_recording=False,
    )
    early = ccu._call_ended_chunk_flow(chunk, child)
    assert early is not None
    assert early.status_code == 200 and early.data == "bot-child"
    assert early.msg == "Conversation status update to in_wrap_up"
    assert calls["clear"] == ("aom-ben", BEN) and calls["ended"] == "call-child"
    assert calls["lookup"] == (5, "call", True, True)  # INCOMING → inbound_direction=True


def test_call_ended_flow_without_agent_child_persists(monkeypatch):
    import hs.conversations.child_update as ccu
    from hs.conversations.models import NonQNRChunkInsert

    class FakeParent:
        parent_contact_uuid = "p"

    class FakeChild:
        parent = 5
        contact_uuid = "bot-child"
        parent_conversation = FakeParent()

    chunk = NonQNRChunkInsert(
        parent_contact_uuid="p",
        type="signal",
        description="Incoming_call_ended",
        channel="call",
        message_id="",
        is_inbound_chunk=False,
        create_chunk=True,
        is_main_recording=False,
    )
    monkeypatch.setattr(ccu.Conversation, "get_child_by_conv_parent", classmethod(lambda cls, *a, **k: None))
    monkeypatch.setattr(ccu.Conversation, "get_all_active_conversations_for_parent", classmethod(lambda cls, p: []))
    assert ccu._call_ended_chunk_flow(chunk, FakeChild()) is None


@pytest.mark.parametrize("error", [None, "missing child", RuntimeError("writer unavailable")])
def test_a_chunk_write_commits_on_success_and_rolls_back_on_failure(monkeypatch, error):
    import hs.conversations.management as cm
    import hs.infra.context as context_management

    state = {"rollback": 0, "commit": 0}
    sinks = _chunk_sinks(monkeypatch, error=error if isinstance(error, str) else None)
    if isinstance(error, Exception):

        def boom(_chunk):
            raise error

        monkeypatch.setattr(cm, "create_hangup_conversation_chunk", boom)
    monkeypatch.setattr(
        context_management, "set_db_session_rollback", lambda: state.__setitem__("rollback", state["rollback"] + 1)
    )
    monkeypatch.setattr(vh, "_commit_session", lambda: state.__setitem__("commit", state["commit"] + 1))
    monkeypatch.setattr(vh, "reconcile_terminal_call_leg", lambda *a, **k: None)

    msg, err = vh.handle_participant_exit_event("c-1", _hangup_params(is_mpc_termination_source="true"))

    assert (msg, err) == ("Successfully updated with hangup source", None)
    assert state == {"rollback": 0 if error is None else 1, "commit": 1}
    assert len(sinks) == (0 if isinstance(error, Exception) else 1)


@pytest.mark.parametrize(
    "auth_id,auth_token",
    [("", ""), ("MATESTAUTHID00000001", ""), ("", "token-only")],
    ids=["both", "no-token", "no-id"],
)
def test_end_mpc_missing_creds(auth_id, auth_token):
    assert vh.end_mpc("mpc-qnr", auth_id, auth_token) == "missing credentials"


def test_end_mpc_404_without_exact_terminal_proof_remains_error(monkeypatch):
    class _Client:
        def __init__(self, *_args):
            pass

        def mpc_stop(self, _mpc_id, *, quiet_not_found=False):
            assert quiet_not_found is False
            return "", 404, "not found"

    monkeypatch.setattr(vh.pv, "PlivoVoiceClient", _Client)
    monkeypatch.setattr(vh.pv, "make_mpc_id", lambda **_kw: "mpc-id")

    assert vh.end_mpc("mpc-qnr", "auth", "token") == "not found"


def test_end_mpc_404_is_success_only_with_exact_terminal_cache_proof(monkeypatch):
    class _Client:
        quiet_values = []

        def __init__(self, *_args):
            pass

        def mpc_stop(self, _mpc_id, *, quiet_not_found=False):
            self.quiet_values.append(quiet_not_found)
            return "", 404, "not found"

    class _TerminalProof:
        def __init__(self):
            self.lookups = []

        def get_mpc_callback_event(self, call_uuid, qnr_run_uuid, event_name):
            self.lookups.append((call_uuid, qnr_run_uuid, event_name))
            if (call_uuid, qnr_run_uuid) == ("call-1", "qnr-1"):
                return "terminal-ts", None
            return "", "event not found in cache"

        def set_mpc_callback_event(self, *_args):
            return None

    monkeypatch.setattr(vh.pv, "PlivoVoiceClient", _Client)
    monkeypatch.setattr(vh.pv, "make_mpc_id", lambda **_kw: "mpc-id")
    proof = _TerminalProof()
    monkeypatch.setattr(vh, "VoiceRedis", lambda: proof)

    assert vh.end_mpc("mpc-qnr-1", "auth", "token", "call-1", "qnr-1") is None
    assert vh.end_mpc("mpc-qnr-2", "auth", "token", "call-1", "qnr-2") == "not found"
    assert proof.lookups == [("call-1", "qnr-1", vc.MPC_END_EVENT), ("call-1", "qnr-2", vc.MPC_END_EVENT)]
    assert _Client.quiet_values == [True, False]


def test_end_mpc_reads_the_room_as_ending_before_plivo_is_asked_to_stop_it(monkeypatch):
    cache = Cache()
    cache._client = fakeredis.FakeStrictRedis(decode_responses=True)
    vr, ending = VoiceRedis(cache=cache), []

    class _Client:
        def __init__(self, *_args):
            pass

        def mpc_stop(self, _mpc_id, *, quiet_not_found=False):
            ending.append(vr.room_ended("call-1", "qnr-1"))
            return "", 204, None

    monkeypatch.setattr(vh.pv, "PlivoVoiceClient", _Client)
    monkeypatch.setattr(vh, "VoiceRedis", lambda: vr)
    assert vh.end_mpc("mpc_qnr-1", "auth", "token", "call-1", "qnr-1") is None
    assert ending == [True]


def test_active_child_lookup_rejects_an_empty_contact_as_a_failure(monkeypatch):
    import hs.routing.invitation as invitation

    monkeypatch.setattr(invitation, "_ensure_request_context", lambda: None)
    assert vh._active_child("") is None
