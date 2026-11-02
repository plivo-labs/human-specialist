"""Monitor/coach/barge + add-participant pins: the pre-req error taxonomy (the
event-NAME strings + the 'could get organization details' typo), the capacity fork
(capacity-full vs switch-call), SUPCM-before-CSUPM + the add-failure SUPCM delete, the
parallel-monitor switch chain, the actions dispatch (no-action → None), the mode update, the leave taxonomy + chunk-skip, the hangup same-call
gate, the supervisor-join race/match, the participant validate gates (CAP ==2, owner,
add-allowed, outbound params-seed), the merge state flips, and the switch two-pass."""

import pytest

import hs.voice.monitor as vmon
import hs.voice.participant as vpart
import tests.conftest  # noqa: F401
from hs.routing.invitation_models import AgentAndContactMapping
from hs.voice import constants as vc

META = {"api_id": "a", "auth_id": "AUTH1", "org_name": "plivo", "org_uuid": "o-1", "aom_uuid": "sup-1"}
DETAILS = {
    "conversation_number": "5550001111",
    "conv_channels_list": ["call"],
    "conv_channel_to_contact_uuid_map": {"call": "ct-1"},
    "conv_parent_contact_uuid": "par-1",
    "conv_participants_aom_uuids": ["aom-a"],
    "conv_current_agent_name": "agent two",
}


class FakeVR:
    def __init__(self, sup=None, call_mapping=None, count=0, leg="", marks=(), leg_err=None):
        self.sup, self.leg, self.marks, self.leg_err = sup, leg, set(marks), leg_err
        self.call_mapping = call_mapping
        self.count = count
        self.calls = {"supcm": [], "csupm": [], "del_supcm": []}

    def get_call_to_supervisor_mapping(self, ct):
        return self.sup, None if self.sup else "not found in cache"

    def get_supervisor_to_call_mapping(self, uuid_):
        return self.call_mapping, None if self.call_mapping else "not found in cache"

    def set_supervisor_to_call_mapping(self, uuid_, m):
        self.calls["supcm"].append((uuid_, m))
        return

    def set_call_to_supervisor_mapping(self, ct, m):
        self.calls["csupm"].append((ct, m))
        return

    def delete_supervisor_to_call_mapping(self, uuid_, ct):
        self.calls["del_supcm"].append((uuid_, ct))

    def get_current_participant_count(self, ct):
        return self.count

    def get_supervisor_leg(self, ct, sup_uuid):
        return self.leg, self.leg_err

    def set_mpc_callback_event(self, ct, qnr, event, ts=""):
        self.marks.add((ct, event))

    def get_mpc_callback_event(self, ct, qnr, event):
        return ("1", None) if (ct, event) in self.marks else ("", "event not found in cache")


class FakeRR:
    def __init__(self, mapping=None, cp=None):
        self.mapping = mapping
        self.cp = cp
        self.calls = {"cap": [], "dec": [], "swap": [], "del_lock": []}

    def get_contact_to_agent_mapping(self, ct):
        return self.mapping

    def get_contact_parameters(self, ct, qnr):
        return self.cp

    def increase_agent_capacity(self, auth, agent, channel, org_cap, conv):
        self.calls["cap"].append((agent, channel, org_cap, conv))
        return (0, None)

    def decrease_agents_capacity(self, auth, agent, channel, conv):
        self.calls["dec"].append((agent, conv))

    def update_agents_capacity(self, auth, agent, channel, old, new, cap):
        self.calls["swap"].append((agent, old, new, cap))
        return

    def delete_reverse_routing_lock(self, sip, conv, channel):
        self.calls["del_lock"].append((sip, conv, channel))


class Aom:
    uuid = "sup-1"
    sip_endpoint = "sip:sup@x"
    name = "agent two"


def _details_ok(monkeypatch, details=None):
    monkeypatch.setattr(
        vmon, "get_parent_conversation_detail", lambda meta, cn, sup, act: (dict(details or DETAILS), None)
    )


# ============================================================================ pre-req
def test_prereq_taxonomy(monkeypatch):
    monkeypatch.setattr(
        vmon, "get_parent_conversation_detail", lambda *a: (None, "failed to fetch parent conversation")
    )
    monkeypatch.setattr(vmon, "VoiceRedis", FakeVR)
    _c, _a, _o, err = vmon.pre_req_details_for_monitoring(META, "555", "", [], True)
    assert err == "CONVERSATION_CLOSED"  # the event-NAME error string
    _details_ok(monkeypatch, dict(DETAILS, conv_participants_aom_uuids=["sup-1"]))
    _c, _a, _o, err = vmon.pre_req_details_for_monitoring(META, "555", "", [], True)
    assert err == "SUPERVISOR_PRESENT"  # supervisor already a participant
    _details_ok(monkeypatch)
    _c, _a, _o, err = vmon.pre_req_details_for_monitoring(META, "555", "", ["aom-z"], True)
    assert err == "CONVERSATION_NOT_ACCESSIBLE"  # no accessible overlap
    monkeypatch.setattr(vmon, "VoiceRedis", lambda: FakeVR(sup={"supervisor_uuid": "other-sup"}))
    _c, _a, _o, err = vmon.pre_req_details_for_monitoring(META, "555", "", ["aom-a"], True)
    assert err == "SUPERVISOR_PRESENT"  # ANOTHER supervisor holds the CSUPM
    monkeypatch.setattr(vmon.ar, "get_agent_by_uuid", lambda u, _auth: Aom())
    monkeypatch.setattr(vmon.ar, "get_organisation_auth_token", lambda a: ("", "not found"))
    monkeypatch.setattr(vmon, "VoiceRedis", FakeVR)
    _c, _a, _o, err = vmon.pre_req_details_for_monitoring(META, "555", "", ["aom-a"], True)
    assert err == "could get organization details"  # the typo is part of the reply


def test_capacity_fork(monkeypatch):
    rr = FakeRR()
    rr.increase_agent_capacity = lambda *a: (-2, "error in increasing agent capacity")
    monkeypatch.setattr(vmon, "RoutingRedis", lambda: rr)
    monkeypatch.setattr(vmon, "_occupied_blocks", lambda meta, ch: True)
    err = vmon.handle_capacity_and_err_events(META, "call", Aom(), dict(DETAILS))
    assert err == "capacity full"
    monkeypatch.setattr(vmon, "_occupied_blocks", lambda meta, ch: False)
    err = vmon.handle_capacity_and_err_events(META, "call", Aom(), dict(DETAILS))
    assert err == "switch call monitoring"


def test_call_monitoring_writes_and_add_failure_deletes_supcm(monkeypatch):
    vr = FakeVR()
    monkeypatch.setattr(vmon, "VoiceRedis", lambda: vr)
    monkeypatch.setattr(vmon, "add_supervisor_to_mpc", lambda *a, **k: None)
    err = vmon.handle_call_monitoring(META, "ct-1", dict(DETAILS), Aom(), "tok")
    assert err is None
    assert vr.calls["supcm"][0][0] == "sup-1"
    assert vr.calls["supcm"][0][1]["supervisor_name"] == "Agent Two"  # Title case
    assert vr.calls["csupm"][0][0] == "ct-1"
    assert vr.calls["del_supcm"] == []
    monkeypatch.setattr(vmon, "add_supervisor_to_mpc", lambda *a, **k: "MPC is not active")
    err = vmon.handle_call_monitoring(META, "ct-1", dict(DETAILS), Aom(), "tok")
    assert err == "MPC is not active"
    assert vr.calls["del_supcm"] == [("sup-1", "ct-1")]  # ONLY the SUPCM half


# ============================================================================ actions
def test_actions_dispatch_and_noop(monkeypatch):
    calls = []
    monkeypatch.setattr(
        vmon, "handle_supervisor_mute_unmute", lambda meta, mute, cn: calls.append(("mute", mute)) or None
    )
    monkeypatch.setattr(vmon, "handle_notify_action", lambda meta, cn: calls.append(("notify",)) or None)
    assert vmon.monitor_conversation_actions(META, "555", {"mute": False}) is None
    assert calls == [("mute", False)]  # mute present (even false) WINS
    assert vmon.monitor_conversation_actions(META, "555", {"notify": True}) is None
    assert calls[-1] == ("notify",)
    # NOTHING recognized → None (the 200 'success' quirk)
    assert vmon.monitor_conversation_actions(META, "555", {"notify": None}) is None


@pytest.mark.parametrize(
    ("state", "body"), [("coach", {"mute": False, "coach_mode": True}), ("barge", {"mute": False, "coach_mode": False})]
)
def test_participant_state_sends_mute_and_coach_mode(monkeypatch, state, body):
    from hs.voice import agent_leg as voice_agent_leg

    monkeypatch.setattr(
        voice_agent_leg,
        "_get_participants",
        lambda n, a, t: ([{"member_address": "sip:sup@x", "member_id": "7", "exit_cause": ""}], None),
    )
    sent = []

    class FakePV:
        def __init__(self, a, t):
            pass

        def mpc_update_participant(self, mpc, member, body):
            sent.append((mpc, member, body))
            return "", 202, None

    monkeypatch.setattr(vmon.pv, "PlivoVoiceClient", FakePV)
    supervisor = {"supervisor_sip": "sip:sup@x", "parent_contact_uuid": "par-1", "conversation_number": "555"}
    mapping = AgentAndContactMapping(qnr_run_uuid="q", agent_uuid="aom-a")
    found = ("ct-1", mapping, supervisor, ("AUTH1", "tok"), dict(DETAILS), None)
    monkeypatch.setattr(vmon, "_conv_details_and_mappings", lambda meta, cn: found)
    monkeypatch.setattr(vmon, "_update_supervisor_state", lambda *a, **k: None)
    assert vmon.update_participant_call_state(META, "555", state, {"accessible_aom_uuids": ["aom-a"]}) is None
    assert sent == [("name_mpc_q", "7", body)]


def test_update_participant_call_state_gates(monkeypatch):
    assert vmon.update_participant_call_state(META, "555", "bogus", {}) == "invalid participant state update requested"
    monkeypatch.setattr(
        vmon, "_conv_details_and_mappings", lambda meta, cn: ("ct-1", None, None, None, dict(DETAILS), None)
    )
    err = vmon.update_participant_call_state(META, "555", "coach", {"accessible_aom_uuids": ["aom-z"]})
    assert err == "you do not have permission to access this conversation"


# ============================================================================ leave/hangup
def test_leave_taxonomy_and_chunk_skip(monkeypatch):
    monkeypatch.setattr(vmon.ar, "get_agent_by_uuid", lambda u, _auth: Aom())
    _details_ok(monkeypatch)
    assert (
        vmon.leave_monitored_conversation(META, "555", {"state": "in_wrap_up"}) == "wrap up states are not implemented"
    )
    assert vmon.leave_monitored_conversation(META, "555", {"state": "bogus"}) == "invalid state"
    import hs.conversations.transfer as cxfer

    monkeypatch.setattr(cxfer, "remove_secondary", lambda **m: None)
    chunks = []
    monkeypatch.setattr(vmon, "_spawn", lambda target, *a: chunks.append(a))
    assert vmon.leave_monitored_conversation(META, "555", {"state": "closed", "agent_state": "monitor"}) is None
    assert chunks[-1][3] is False  # create_chunk=False for monitor
    assert vmon.leave_monitored_conversation(META, "555", {"state": "closed", "agent_state": "coach"}) is None
    assert chunks[-1][3] is True
    # delete-secondary failure → the fixed string (nothing to remove is not one: missing_ok)
    removals = []
    monkeypatch.setattr(
        cxfer, "remove_secondary", lambda **m: removals.append(m) or "removing the secondary agent failed"
    )
    assert (
        vmon.leave_monitored_conversation(META, "555", {"state": "closed", "agent_state": "coach"})
        == "could not delete secondary agent mapping"
    )
    assert (removals[0]["active"], removals[0]["missing_ok"]) == (False, True)


def test_hangup_same_call_gate(monkeypatch):
    _details_ok(monkeypatch)
    vr = FakeVR(sup={"supervisor_uuid": "sup-1", "supervisor_sip": "sip:sup@x"}, call_mapping={"call_uuid": "OTHER"})
    monkeypatch.setattr(vmon, "VoiceRedis", lambda: vr)
    monkeypatch.setattr(vmon, "RoutingRedis", FakeRR)
    err = vmon.hangup_monitored_conversation(META, "555", "mpc_q")
    assert err == "Supervisor is not on this call"  # par-1 != OTHER
    vr2 = FakeVR(sup={"supervisor_uuid": "sup-1", "supervisor_sip": "sip:sup@x"}, call_mapping={"call_uuid": "par-1"})
    monkeypatch.setattr(vmon.ar, "get_organisation_auth_token", lambda a: ("tok", None))
    import hs.voice.transfer as ct

    hangs = []
    monkeypatch.setattr(ct, "hangup_call_for_mpc_member", lambda mpc, member, a, t: hangs.append((mpc, member)) or None)
    rr = FakeRR(mapping={"QnrRunUuid": "q"}, cp={"auth_id": "AUTH1", "auth_token": "tok"})
    monkeypatch.setattr(vmon, "VoiceRedis", lambda: vr2)
    monkeypatch.setattr(vmon, "RoutingRedis", lambda: rr)
    assert vmon.hangup_monitored_conversation(META, "555", "mpc_q") is None
    assert hangs == [("mpc_q", "sip:sup@x")]


class _Plivo:
    status, deleted = 204, []

    def __init__(self, *a):
        pass

    def call_delete(self, call_uuid, **kw):
        _Plivo.deleted.append(call_uuid)
        return None, _Plivo.status, None if _Plivo.status < 300 else f"non 2xx response ({_Plivo.status})"


@pytest.mark.parametrize(("status", "answer"), [(204, None), (404, None), (503, "non 2xx response (503)")])
def test_a_leave_ends_the_supervisors_own_leg(monkeypatch, status, answer):
    _details_ok(monkeypatch)
    sup = {"supervisor_uuid": "sup-1", "supervisor_sip": "sip:sup@x"}
    vr = FakeVR(sup=sup, call_mapping={"call_uuid": "par-1"}, leg="leg-7")
    monkeypatch.setattr(vmon, "VoiceRedis", lambda: vr)
    monkeypatch.setattr(vmon.ar, "get_organisation_auth_token", lambda a: ("tok", None))
    monkeypatch.setattr(vmon.pv, "PlivoVoiceClient", _Plivo)
    _Plivo.status, _Plivo.deleted = status, []
    assert vmon.hangup_monitored_conversation(META, "555", "mpc_q") == answer
    assert _Plivo.deleted == ["leg-7"]  # ringing or joined, by its own call uuid
    assert ("par-1", f"{vc.SUPERVISOR_LEAVE_REQUESTED}:leg-7") in vr.marks


def test_a_leave_that_cannot_read_the_supervisors_leg_fails(monkeypatch):
    import hs.voice.transfer as ct

    _details_ok(monkeypatch)
    sup = {"supervisor_uuid": "sup-1", "supervisor_sip": "sip:sup@x"}
    monkeypatch.setattr(
        vmon, "VoiceRedis", lambda: FakeVR(sup=sup, call_mapping={"call_uuid": "par-1"}, leg_err="down")
    )
    monkeypatch.setattr(vmon.ar, "get_organisation_auth_token", lambda a: ("tok", None))
    hangs = []
    monkeypatch.setattr(ct, "hangup_call_for_mpc_member", lambda *a: hangs.append(a))
    assert (vmon.hangup_monitored_conversation(META, "555", "mpc_q"), hangs) == ("down", [])


def test_a_leg_whose_leave_already_ran_does_not_join(monkeypatch):
    import hs.conversations.transfer as cxfer

    adds = []
    monkeypatch.setattr(cxfer, "add_secondary", lambda **m: adds.append(m) or None)
    sup = {"supervisor_sip": "sip:sup@x", "supervisor_uuid": "sup-1", "conversation_number": "555"}
    marks = [("ct-1", f"{vc.SUPERVISOR_LEAVE_REQUESTED}:leg-7")]
    monkeypatch.setattr(vmon, "VoiceRedis", lambda: FakeVR(sup=sup, marks=marks))
    monkeypatch.setattr(vmon, "RoutingRedis", lambda: FakeRR(cp={"auth_id": "AUTH1", "auth_token": "tok"}))
    monkeypatch.setattr(vmon.pv, "PlivoVoiceClient", _Plivo)
    _Plivo.status, _Plivo.deleted = 404, []
    assert vmon.handle_supervisor_join_event("ct-1", "qnr-1", "sip:sup@x", "mpc_q", "leg-7") is None
    assert (adds, _Plivo.deleted) == ([], ["leg-7"])
    # a rejoin is a new leg: the old leg's leave does not touch it, and its committed join is checked once more
    staged = []
    monkeypatch.setattr(vmon, "_spawn", lambda target, *a: staged.append((target.__name__, a)))
    assert vmon.handle_supervisor_join_event("ct-1", "qnr-1", "sip:sup@x", "mpc_q", "leg-8") is None
    assert adds[0]["agent_sip_or_uuid"] == "sip:sup@x"
    assert staged == [("_drop_if_left", ("ct-1", "leg-8", "555", "sip:sup@x", "sup-1"))]


LEFT = [("ct-1", f"{vc.SUPERVISOR_LEAVE_REQUESTED}:leg-7")]


def test_a_join_committed_after_its_leave_is_removed_unless_a_rejoin_followed(monkeypatch):
    removed = []
    monkeypatch.setattr(vmon.voice_hangup, "_delete_secondary", lambda payload, member: removed.append(payload))
    monkeypatch.setattr(vmon, "VoiceRedis", lambda: FakeVR(marks=LEFT, leg="leg-9"))
    vmon._drop_if_left("ct-1", "leg-7", "555", "sip:sup@x", "sup-1")  # a rejoin stored leg-9: the row is its
    assert removed == []
    monkeypatch.setattr(vmon, "VoiceRedis", lambda: FakeVR(marks=LEFT, leg="leg-7"))
    vmon._drop_if_left("ct-1", "leg-8", "555", "sip:sup@x", "sup-1")  # the leave was for another leg
    vmon._drop_if_left("ct-1", "leg-7", "555", "sip:sup@x", "sup-1")
    [off] = removed
    assert (off["conversation_number"], off["agent_sip_or_uuid"], off["missing_ok"]) == ("555", "sip:sup@x", True)


def test_a_left_supervisor_that_is_not_removed_is_counted(monkeypatch):
    import hs.conversations.transfer as cxfer

    counted = []
    monkeypatch.setattr(cxfer, "remove_secondary", lambda **m: None)
    monkeypatch.setattr(vmon.voice_hangup, "_commit_session", lambda: "commit failed")
    monkeypatch.setattr(vmon, "increase_counter", lambda name, labels: counted.append(labels["output_state"]))
    monkeypatch.setattr(vmon, "VoiceRedis", lambda: FakeVR(marks=LEFT, leg="leg-7"))
    vmon._drop_if_left("ct-1", "leg-7", "555", "sip:sup@x", "sup-1")
    monkeypatch.setattr(vmon, "VoiceRedis", lambda: FakeVR(marks=LEFT, leg_err="down"))
    vmon._drop_if_left("ct-1", "leg-7", "555", "sip:sup@x", "sup-1")
    assert counted == ["remove_left_supervisor"] * 2


# ============================================================================ supervisor join
def test_supervisor_join_race_and_match(monkeypatch):
    raced = []
    monkeypatch.setattr(vmon, "_spawn", lambda target, *a: raced.append(target.__name__))
    monkeypatch.setattr(
        vmon, "VoiceRedis", lambda: FakeVR(sup={"supervisor_sip": "sip:OTHER@x", "conversation_number": "555"})
    )
    err = vmon.handle_supervisor_join_event("ct-1", "qnr-1", "sip:sup@x", "mpc_q")
    assert err is None  # the race path answers None
    assert raced == ["_remove_racing_supervisor"]
    import hs.conversations.transfer as cxfer

    adds = []
    monkeypatch.setattr(cxfer, "add_secondary", lambda **m: adds.append(m) or None)
    vr2 = FakeVR(sup={"supervisor_sip": "sip:sup@x", "supervisor_uuid": "sup-1", "conversation_number": "555"})
    monkeypatch.setattr(vmon, "VoiceRedis", lambda: vr2)
    err = vmon.handle_supervisor_join_event("ct-1", "qnr-1", "sip:sup@x", "mpc_q")
    assert err is None
    assert adds[0]["secondary_agent_state"] == vc.SECONDARY_STATE_MONITOR
    assert adds[0]["type"] == vc.PARTICIPANT_TYPE_SUPERVISOR
    monkeypatch.setattr(cxfer, "add_secondary", lambda **m: "adding the secondary agent failed")
    err = vmon.handle_supervisor_join_event("ct-1", "qnr-1", "sip:sup@x", "mpc_q")
    assert err == "technical error: could not add supervisor to the conversation"


# ============================================================================ participant
def _owner_mapping(sip="sip:a@x", inbound=True):
    from hs.routing.invitation_models import AgentAndContactMapping

    return AgentAndContactMapping(
        AgentSipEndpoint=sip, QnrRunUuid="qnr-1", FlowRunUUID="flow-1", IsInbound=inbound
    ).to_json()


class Org:
    auth_id = "AUTH1"
    auth_token = "tok"


def test_validate_participant_gates(monkeypatch):
    import hs.conversations.transfer as cxfer

    monkeypatch.setattr(cxfer, "transfer_allowed", lambda c, ch, x, y: (True, True, {}))
    # CAP ==2 gate
    _m, _p, err = vpart.validate_add_participant_req_owner(
        {"contact_uuid": "ct", "agent_sip": "sip:a@x"}, True, routing_redis=FakeRR(), voice_redis=FakeVR(count=2)
    )
    assert err == "maximum secondary participants reached"
    # owner mismatch
    monkeypatch.setattr(vpart.ar, "get_org_details_for_agent_sip", lambda s: Org())
    rr = FakeRR(mapping=_owner_mapping("sip:other@x"))
    _m, _p, err = vpart.validate_add_participant_req_owner(
        {"contact_uuid": "ct", "agent_sip": "sip:a@x"}, False, routing_redis=rr, voice_redis=FakeVR()
    )
    assert err == "not the owner of the channel"
    # add-not-allowed
    monkeypatch.setattr(cxfer, "transfer_allowed", lambda c, ch, x, y: (True, False, {}))
    rr2 = FakeRR(mapping=_owner_mapping())
    _m, _p, err = vpart.validate_add_participant_req_owner(
        {"contact_uuid": "ct", "agent_sip": "sip:a@x"}, False, routing_redis=rr2, voice_redis=FakeVR()
    )
    assert err == "add participant is not allowed"
    monkeypatch.setattr(cxfer, "transfer_allowed", lambda c, ch, x, y: (True, True, {}))
    # inbound params-miss is FATAL
    rr3 = FakeRR(mapping=_owner_mapping(), cp=None)
    _m, _p, err = vpart.validate_add_participant_req_owner(
        {"contact_uuid": "ct", "agent_sip": "sip:a@x"}, False, routing_redis=rr3, voice_redis=FakeVR()
    )
    assert err == "not found in cache"


def test_validate_outbound_seeds_params(monkeypatch):
    import hs.conversations.transfer as cxfer

    monkeypatch.setattr(vpart.ar, "get_org_details_for_agent_sip", lambda s: Org())
    conv = {"from": "+1415", "to": "+1650", "parent_channel": "call", "conversation_number": "555"}
    monkeypatch.setattr(cxfer, "transfer_allowed", lambda c, ch, x, y: (True, True, conv))
    rr = FakeRR(mapping=_owner_mapping(inbound=False), cp=None)
    stored = []
    rr.store_contact_params = lambda ct, qnr, js: stored.append((ct, qnr, js))
    _mapping, params, err = vpart.validate_add_participant_req_owner(
        {"contact_uuid": "ct", "agent_sip": "sip:a@x"}, False, routing_redis=rr, voice_redis=FakeVR()
    )
    assert err is None
    assert stored and stored[0][1] == "flow-1"  # keyed by the FLOW run for outbound
    assert params["queue_and_route_run_uuid"] == "flow-1"
    assert params["conversation_number"] == "555"


def test_merge_flips_states(monkeypatch):
    import hs.conversations.transfer as cxfer
    from hs.routing.invitation_models import AgentAndContactMapping

    monkeypatch.setattr(
        vpart,
        "validate_add_participant_req_owner",
        lambda req, v, routing_redis=None, voice_redis=None: (
            AgentAndContactMapping(QnrRunUuid="qnr-1", IsInbound=True),
            {"auth_id": "A", "auth_token": "T", "conversation_number": "555"},
            None,
        ),
    )
    updates = []
    monkeypatch.setattr(cxfer, "update_secondary", lambda **m: updates.append(m) or None)
    monkeypatch.setattr(vpart, "hold_unhold_members", lambda *a, **k: None)
    err = vpart.merge_participant({"contact_uuid": "ct", "agent_sip": "s", "member": "+18005550000"})
    assert err is None
    assert updates[-1]["participant_identifier"] == "18005550000"  # '+' stripped
    assert updates[-1]["secondary_agent_state"] == "add_participant_merge_success"
    monkeypatch.setattr(vpart, "hold_unhold_members", lambda *a, **k: "hold failed")
    err = vpart.merge_participant({"contact_uuid": "ct", "agent_sip": "s", "member": "+18005550000"})
    assert err == "hold failed"  # the hold error returned…
    assert updates[-1]["secondary_agent_state"] == "add_participant_merge_failed"  # …but the secondary still flips


def test_switch_two_pass(monkeypatch):
    from hs.routing.invitation_models import AgentAndContactMapping

    monkeypatch.setattr(
        vpart,
        "_owner_params_for",
        lambda req, rr=None: (
            AgentAndContactMapping(QnrRunUuid="qnr-1", IsInbound=True),
            {"auth_id": "A", "auth_token": "T"},
            None,
        ),
    )
    passes = []
    monkeypatch.setattr(
        vpart, "hold_unhold_members", lambda mpc, a, t, ct, sip, hold, member="": passes.append((hold, member)) or None
    )
    err = vpart.switch_participant(
        {"contact_uuid": "ct", "agent_sip": "s", "switch_value": True, "member_call_uuid": "leg-9"}
    )
    assert err is None
    assert passes == [(True, ""), (False, "leg-9")]  # everyone→hold, the leg→unhold


def test_hold_unhold_members_skip_rules(monkeypatch):
    from hs.voice import agent_leg as voice_agent_leg

    monkeypatch.setattr(
        voice_agent_leg,
        "_get_participants",
        lambda n, a, t: (
            [
                {"member_address": "sip:agent@x", "member_id": "1", "exit_cause": "", "call_uuid": "l1"},
                {"member_address": "+1650", "member_id": "2", "exit_cause": "hangup", "call_uuid": "l2"},
                {"member_address": "+1650", "member_id": "3", "exit_cause": "", "call_uuid": "l3"},
            ],
            None,
        ),
    )
    held = []

    class FakePV:
        def __init__(self, a, t):
            pass

        def mpc_update_participant(self, mpc, member, body):
            held.append((member, body["hold"]))
            return "", 202, None

    monkeypatch.setattr(vpart.pv, "PlivoVoiceClient", FakePV)
    assert vpart.hold_unhold_members("mpc_q", "A", "T", "ct", "sip:agent@x", True) is None
    assert held == [("3", True)]  # primary + exited both skipped
    held.clear()
    assert vpart.hold_unhold_members("mpc_q", "A", "T", "ct", "sip:agent@x", False, member_call_uuid="l3") is None
    assert held == [("3", False)]  # narrowed to the one leg


def test_alias_header_trim(monkeypatch):
    from hs.voice import agent_leg as voice_agent_leg

    monkeypatch.setattr(voice_agent_leg, "encode_sip_header_value", lambda v: v)
    assert vpart._alias_header_value("Short Name") == "Short Name"
    out = vpart._alias_header_value("A" * 25)
    assert out == "A" * 20 + "..."  # trim + ellipsis BEFORE the escape
