"""MPC status callbacks + the queue-wait loop: the order-gate composite naming, the dispatcher's
per-event replies (dup 200 / unhandled 422 / empty-case 200-empty), MPCInitialisedCallback's
side-effect chain (call-leg seam, ACTIVECONFERENCE TTL, first prompt speak/play, RouteAndAddToMPC
failure → AQE delete + flow hand-back), the prompt rotation FSM, DialInput queue-exit, WaitTimeout,
the missed-event mimic, and the wait-time text machinery.
"""

import pytest

import hs.voice.callbacks as vcb
from hs.routing import constants as rc
from hs.voice import constants as vc


class FakeVoiceRedis:
    def __init__(self):
        self.dep_calls = []
        self.plain_calls = []
        self.dep_result = None
        self.plain_result = None
        self.events = {}
        self.toggles = []
        self.prompt_sets = []
        self.prompt_set_result = None
        self.prompt_params = ({}, None)
        self.conference_active = (True, None)
        self.mpc_call_uuids = []
        self.released = []

    def set_mpc_event_with_dependency(
        self, call_uuid, qnr_run_uuid, event_name, sequence_number="", dependent_event="", timestamp=""
    ):
        self.dep_calls.append((call_uuid, qnr_run_uuid, event_name, sequence_number, dependent_event))
        return self.dep_result

    def set_mpc_callback_event(self, call_uuid, qnr_run_uuid, event_name, timestamp=""):
        self.plain_calls.append((call_uuid, qnr_run_uuid, event_name))
        return self.plain_result

    def release_mpc_callback_event(self, call_uuid, qnr_run_uuid, event_name, timestamp):
        self.released.append((call_uuid, qnr_run_uuid, event_name, timestamp))
        return

    def get_mpc_callback_event(self, call_uuid, qnr_run_uuid, event_name):
        val = self.events.get(event_name, "")
        return (val, None) if val else ("", "event not found in cache")

    def toggle_active_conference(self, qnr_run_uuid, is_active, expiry_seconds):
        self.toggles.append((qnr_run_uuid, is_active, expiry_seconds))
        return

    def is_conference_active(self, qnr_run_uuid):
        return self.conference_active

    def get_prompt_parameters(self, call_uuid):
        return self.prompt_params

    def set_prompt_parameters(self, call_uuid, prompt_params):
        self.prompt_sets.append(prompt_params)
        return self.prompt_set_result

    def get_mpc_call_uuids(self, flow_run_uuid, qnr_run_uuid):
        return self.mpc_call_uuids


class FakeRoutingRedis:
    def __init__(self, cp=None, queue_config=None, tip=None):
        self._cp = cp
        self._qc = queue_config
        self._tip = tip
        self.qpwt = {}
        self.qpwt_sets = []
        self.decreases = []

    def get_contact_parameters(self, contact_uuid, qnr_run_uuid):
        return self._cp

    def get_queue_config(self, flow_run_uuid, run_uuid):
        if self._qc is None:
            return None, "queue config not found"
        return self._qc, None

    def get_transfer_in_progress_info(self, contact_uuid):
        return self._tip

    def get_queue_position_and_wait_time(self, node_uuid):
        return self.qpwt.get(node_uuid)  # the existing contract: dict | None

    def set_queue_position_and_wait_time(self, node_uuid, data, ttl_seconds):
        self.qpwt_sets.append((node_uuid, data, ttl_seconds))
        self.qpwt[node_uuid] = data
        return True

    def set_recompute_flag_for_queue_position_calculation(self, node_uuid, queue_uuid=""):
        if self.get_queue_position_and_wait_time(node_uuid) is None:
            return
        self.set_queue_position_and_wait_time(
            node_uuid,
            {
                "estimated_wait_time_minutes": 0,
                "estimated_wait_time_seconds": 0,
                "queue_position": 0,
                "recompute": True,
            },
            7200,
        )
        return

    def decrease_agents_capacity(self, auth_id, agent, channel, conversation_id):
        self.decreases.append((auth_id, agent, channel, conversation_id))


class FakeAfterCommitSession:
    def __init__(self):
        self.info = {}


def _qc(prompts=None, allowed=None, timeout=120):
    from hs.intake.models import QueueConfigModel

    return QueueConfigModel.model_validate(
        {
            "queue_call_timeout": timeout,
            "queue_call_allowed_inputs": allowed or [],
            "prompt": prompts
            if prompts is not None
            else [{"prompt_type": "speak", "text": "hello", "voice": "WOMAN", "language": "en-US"}],
        }
    )


def _prompt(**fields):
    return _qc(prompts=[fields]).prompt[0]


_CP = {
    "auth_id": "A",
    "auth_token": "T",
    "flow_run_uuid": "flow-1",
    "queue_and_route_run_uuid": "qnr-1",
    "queue_uuid": "q-1",
    "node_uuid": "n-1",
    "contact_uuid": "ct-1",
    "conversation_number": "9911",
    "from_number": "+15550001234",
    "flow_name": "f",
}


# ======================================================================================
# The order gate
# ======================================================================================
def test_build_event_details_participant_specific_composite():
    d = vcb.build_event_details("ParticipantJoin", "+1555", "3")
    assert d.event_name == "ParticipantJoin:+1555:3"
    assert d.dependent_event == "ParticipantExit:+1555"
    assert d.sequence_number == "3"
    plain = vcb.build_event_details("MPCInitialized", "+1555", "3")
    assert plain.event_name == "MPCInitialized" and plain.dependent_event == ""


def test_update_voice_event_gate_routes_and_sentinels(monkeypatch):
    rr = FakeVoiceRedis()
    monkeypatch.setattr(vcb, "VoiceRedis", lambda: rr)
    vcb.update_voice_event_time_in_cache("c1", "q1", vcb.EventDetails("MPCStart"))
    assert rr.plain_calls == [("c1", "q1", "MPCStart")]
    vcb.update_voice_event_time_in_cache(
        "c1", "q1", vcb.EventDetails("ParticipantJoin:+1:2", "ParticipantExit:+1", "2")
    )
    assert rr.dep_calls == [("c1", "q1", "ParticipantJoin:+1:2", "2", "ParticipantExit:+1")]
    rr.plain_result = "redis exploded"  # real gate failure propagates for retry
    assert vcb.update_voice_event_time_in_cache("c1", "q1", vcb.EventDetails("X")) == "redis exploded"
    rr.plain_result = vc.VALUE_EXISTS_ERROR  # the 2 sentinels propagate
    assert vcb.update_voice_event_time_in_cache("c1", "q1", vcb.EventDetails("X")) == "value already exists"


# ======================================================================================
# MPCInitialisedCallback
# ======================================================================================
@pytest.fixture()
def init_wiring(monkeypatch):
    state = {"legs": [], "played": [], "routed": [], "events": []}
    monkeypatch.setattr(vcb, "_spawn", lambda target, *a: target(*a))
    monkeypatch.setattr(
        vcb,
        "_play_prompt",
        lambda prompt, cp, m, call_uuid, db: (
            state["played"].append((prompt.prompt_type, prompt.text or prompt.url, m)) or ("ok", None)
        ),
    )
    return state


def test_initialised_callback_happy_path(init_wiring, monkeypatch):
    import hs.routing.get_agents as rga

    monkeypatch.setattr(rga, "get_agents", lambda *a, **k: (["sip:agent-1"], None))
    rr = FakeRoutingRedis(cp=_CP, queue_config=_qc())
    vr = FakeVoiceRedis()
    legs = []
    monkeypatch.setattr(vcb, "RoutingRedis", lambda: rr)
    monkeypatch.setattr(vcb, "VoiceRedis", lambda: vr)
    monkeypatch.setattr(vcb, "_create_call_leg_seam", lambda *a: legs.append(a))
    monkeypatch.setattr("hs.voice.agent_leg.add_agents_to_mpc", lambda *a: None)
    msg, err = vcb.handle_initialised_callback(
        "ct-1", "qnr-1", {"mpc_start_time": "2026-06-11 10:00:00", "member_id": "10"}, db=None
    )
    assert (msg, err) == ("Initialised MPC", None)
    # the call-leg seam got the parent leg (callUUID empty, leg=ct, start=MPCStartTime, is_mpc=True)
    assert legs == [("", "ct-1", "", "2026-06-11 10:00:00", "", "", True)]
    assert vr.toggles == [("qnr-1", "true", 120)]  # ACTIVECONFERENCE TTL=queue timeout
    assert init_wiring["played"] == [("speak", "hello", "10")]
    assert vr.prompt_sets == [
        {"prompt_number": 0, "prompt_play_complete": False, "prompt_loop_count": 0, "repeat_count": 0}
    ]


def test_the_initialised_callback_leaves_the_recording_stop_to_intake(init_wiring, monkeypatch):
    import hs.routing.get_agents as rga

    monkeypatch.setattr(rga, "get_agents", lambda *a, **k: (["sip:agent-1"], None))
    spawned = []
    monkeypatch.setattr(vcb, "_spawn", lambda target, *a: spawned.append(target.__name__))
    monkeypatch.setattr(vcb, "RoutingRedis", lambda _v=FakeRoutingRedis(cp=_CP, queue_config=_qc()): _v)
    monkeypatch.setattr(vcb, "VoiceRedis", lambda _v=FakeVoiceRedis(): _v)
    monkeypatch.setattr(vcb, "_create_call_leg_seam", lambda *a: None)
    monkeypatch.setattr("hs.voice.agent_leg.add_agents_to_mpc", lambda *a: None)
    vcb.handle_initialised_callback(
        "ct-1", "qnr-1", {"mpc_start_time": "2026-06-11 10:00:00", "member_id": "10"}, db=None
    )
    assert "_stop_call_recording" not in spawned


def test_initialised_callback_does_not_stamp_when_mpc_already_ended(init_wiring, monkeypatch):
    # an already-ended MPC returns before the stamp — no ownership for a dead call
    owner = []
    vr = FakeVoiceRedis()
    vr.events[vc.MPC_END_EVENT] = "111"
    monkeypatch.setattr(vcb, "RoutingRedis", lambda _v=FakeRoutingRedis(cp=_CP, queue_config=_qc()): _v)
    monkeypatch.setattr(vcb, "VoiceRedis", lambda: vr)
    monkeypatch.setattr(vcb, "_create_call_leg_seam", lambda *a: None)
    monkeypatch.setattr("hs.voice.agent_leg.add_agents_to_mpc", lambda *a: None)
    vcb.handle_initialised_callback("ct-1", "qnr-1", {"mpc_start_time": "t", "member_id": "10"}, db=None)
    assert owner == []


def test_initialised_callback_falls_back_to_creation_time(init_wiring, monkeypatch):
    import hs.routing.get_agents as rga

    monkeypatch.setattr(rga, "get_agents", lambda *a, **k: (["sip:agent-1"], None))
    rr = FakeRoutingRedis(cp=_CP, queue_config=_qc())
    legs = []
    monkeypatch.setattr(vcb, "RoutingRedis", lambda: rr)
    monkeypatch.setattr(vcb, "VoiceRedis", lambda _v=FakeVoiceRedis(): _v)
    monkeypatch.setattr(vcb, "_create_call_leg_seam", lambda *a: legs.append(a))
    monkeypatch.setattr("hs.voice.agent_leg.add_agents_to_mpc", lambda *a: None)
    vcb.handle_initialised_callback("ct-1", "qnr-1", {"mpc_creation_time": "CREATED", "member_id": "10"}, db=None)
    assert legs[0][3] == "CREATED"


def test_initialised_callback_mpc_already_ended(init_wiring, monkeypatch):
    vr = FakeVoiceRedis()
    vr.events[vc.MPC_END_EVENT] = "111"
    monkeypatch.setattr(vcb, "RoutingRedis", lambda _v=FakeRoutingRedis(cp=_CP, queue_config=_qc()): _v)
    monkeypatch.setattr(vcb, "VoiceRedis", lambda: vr)
    monkeypatch.setattr(vcb, "_create_call_leg_seam", lambda *a: None)
    monkeypatch.setattr("hs.voice.agent_leg.add_agents_to_mpc", lambda *a: None)
    msg, err = vcb.handle_initialised_callback("ct-1", "qnr-1", {"member_id": "10"}, db=None)
    assert (msg, err) == ("", "mpc is already ended")


def test_initialised_callback_invalid_prompt_runs_permanent_failure_handoff(init_wiring, monkeypatch):
    deleted, unanswered, transfers = [], [], []
    db = FakeAfterCommitSession()
    monkeypatch.setattr(
        vcb.aq,
        "delete_active_queue_entry",
        lambda _db, contact, run, status: deleted.append((contact, run, status)) or 23,
    )
    monkeypatch.setattr(
        vcb, "_update_unanswered", lambda contact, reason, queue_id: unanswered.append((contact, reason, queue_id))
    )
    monkeypatch.setattr(vcb, "send_to_queue_exit", lambda _c, _cp, action: transfers.append(action) or ("ok", None))

    monkeypatch.setattr(
        vcb,
        "RoutingRedis",
        lambda _v=FakeRoutingRedis(cp=_CP, queue_config=_qc(prompts=[{"prompt_type": "unsupported"}])): _v,
    )
    monkeypatch.setattr(vcb, "VoiceRedis", lambda _v=FakeVoiceRedis(): _v)
    monkeypatch.setattr(
        vcb, "_create_call_leg_seam", lambda *args: pytest.fail("invalid config must fail before creating the call leg")
    )
    result = vcb.handle_initialised_callback("ct-1", "qnr-1", {"member_id": "10"}, db=db)

    assert result == ("", "invalid prompt type")
    assert deleted == [("ct-1", "qnr-1", rc.CONTACT_STATE_FAILED)]
    assert unanswered == [("ct-1", rc.UNANSWERED_ASSIGNMENT_FAILED, 23)]
    assert transfers == [rc.QNR_STATE_ASSIGNMENT_FAILED]
    assert vcb.has_unsafe_callback_effect(db) is True
    vcb.drain_after_commit_effects(db)


def test_initialised_callback_empty_prompt_list_skips_prompt_and_routes(init_wiring, monkeypatch):
    """An empty prompt list skips the (absent) customer prompt and still routes the call."""
    import hs.routing.get_agents as rga

    monkeypatch.setattr(rga, "get_agents", lambda *a, **k: (["sip:agent-1"], None))
    rr = FakeRoutingRedis(cp=_CP, queue_config=_qc(prompts=[]))
    vr = FakeVoiceRedis()
    monkeypatch.setattr(vcb, "RoutingRedis", lambda: rr)
    monkeypatch.setattr(vcb, "VoiceRedis", lambda: vr)
    monkeypatch.setattr(vcb, "_create_call_leg_seam", lambda *a: None)
    monkeypatch.setattr("hs.voice.agent_leg.add_agents_to_mpc", lambda *a: None)
    msg, err = vcb.handle_initialised_callback("ct-1", "qnr-1", {"member_id": "10"}, db=None)
    assert (msg, err) == ("Initialised MPC", None)  # call routed — no IndexError / 500
    assert init_wiring["played"] == []  # no prompt played
    assert vr.prompt_sets == []  # no prompt params set
    assert vr.toggles == [("qnr-1", "true", 120)]  # proceeded past the conference toggle


def test_initialised_callback_prompt_cache_failure_still_routes(init_wiring, monkeypatch):
    """A failed best-effort prompt-params cache write (e.g. a transient Redis conn-reset) must NOT
    abort the chain, or the caller sits in the MPC for the full wait time with an idle agent online;
    the call still routes to an agent."""
    import hs.routing.get_agents as rga

    monkeypatch.setattr(rga, "get_agents", lambda *a, **k: (["sip:agent-1"], None))
    rr = FakeRoutingRedis(cp=_CP, queue_config=_qc())
    vr = FakeVoiceRedis()
    vr.prompt_set_result = "redis connection reset by peer"
    added = []
    monkeypatch.setattr(vcb, "RoutingRedis", lambda: rr)
    monkeypatch.setattr(vcb, "VoiceRedis", lambda: vr)
    monkeypatch.setattr(vcb, "_create_call_leg_seam", lambda *a: None)
    monkeypatch.setattr("hs.voice.agent_leg.add_agents_to_mpc", lambda *a: added.append(a) and None)
    msg, err = vcb.handle_initialised_callback("ct-1", "qnr-1", {"member_id": "10"}, db=None)
    assert (msg, err) == ("Initialised MPC", None)  # routed, not "could not set prompt params"
    assert len(added) == 1  # the agent WAS dialed
    assert init_wiring["played"] == [("speak", "hello", "10")]  # prompt still played first
    assert vr.prompt_sets == [
        {"prompt_number": 0, "prompt_play_complete": False, "prompt_loop_count": 0, "repeat_count": 0}
    ]  # write attempted


def test_initialised_callback_route_failure_runs_the_assignment_failed_chain(init_wiring, monkeypatch):
    """RouteAndAddToMPC err → AQE delete(FAILED) + unanswered(assignment failed) + the aleg
    hand-back with flow_action=<route result> + the assignment_failed CH event → ('', err)."""
    deleted, unanswered, transfers = [], [], []
    monkeypatch.setattr(vcb.aq, "delete_active_queue_entry", lambda db, c, q, s: deleted.append((c, q, s)) or 7)
    monkeypatch.setattr(vcb, "_update_unanswered", lambda c, reason, aqid: unanswered.append((c, reason, aqid)))
    monkeypatch.setattr(vcb, "send_to_queue_exit", lambda _c, _cp, action: transfers.append(action) or ("ok", None))
    import hs.routing.get_agents as rga

    monkeypatch.setattr(rga, "get_agents", lambda *a, **k: (["sip:agent-1"], None))
    rr = FakeRoutingRedis(cp=_CP, queue_config=_qc())
    monkeypatch.setattr(vcb, "RoutingRedis", lambda: rr)
    monkeypatch.setattr(vcb, "VoiceRedis", lambda _v=FakeVoiceRedis(): _v)
    monkeypatch.setattr(vcb, "_create_call_leg_seam", lambda *a: None)
    msg, err = vcb.handle_initialised_callback("ct-1", "qnr-1", {"member_id": "10"}, db=None)  # the default seam raises
    assert msg == "" and err is not None
    assert deleted == [("ct-1", "qnr-1", rc.CONTACT_STATE_FAILED)]
    assert unanswered == [("ct-1", rc.UNANSWERED_ASSIGNMENT_FAILED, 7)]
    assert transfers == [rc.QNR_STATE_ASSIGNMENT_FAILED]  # res rides as the flow_action


def test_initialised_callback_queue_persistence_failure_is_retryable_without_cleanup(init_wiring, monkeypatch):
    deleted, unanswered, transfers = [], [], []
    monkeypatch.setattr(vcb.aq, "delete_active_queue_entry", lambda *args, **kwargs: deleted.append(args))
    monkeypatch.setattr(vcb, "_update_unanswered", lambda *args, **kwargs: unanswered.append(args))
    monkeypatch.setattr(vcb, "send_to_queue_exit", lambda *args, **kwargs: transfers.append(args) or ("", None))
    monkeypatch.setattr(
        vcb,
        "preflight_route_and_add_to_mpc",
        lambda *args, **kwargs: (None, "retryable_queue_persistence: database unavailable"),
    )

    monkeypatch.setattr(vcb, "RoutingRedis", lambda _v=FakeRoutingRedis(cp=_CP, queue_config=_qc()): _v)
    monkeypatch.setattr(vcb, "VoiceRedis", lambda _v=FakeVoiceRedis(): _v)
    monkeypatch.setattr(vcb, "_create_call_leg_seam", lambda *args: None)
    msg, err = vcb.handle_initialised_callback("ct-1", "qnr-1", {"member_id": "10"}, db=None)

    assert msg == ""
    assert err == "retryable_queue_persistence: database unavailable"
    assert deleted == []
    assert unanswered == []
    assert transfers == []


@pytest.mark.parametrize(
    ("preflight_error", "expected_action"),
    [("no available agents found", ""), ("known_error: routing configuration invalid", rc.QNR_STATE_ASSIGNMENT_FAILED)],
)
def test_initialised_callback_permanent_preflight_failure_runs_cleanup_chain(
    init_wiring, monkeypatch, preflight_error, expected_action
):
    deleted, unanswered, transfers = [], [], []
    db = FakeAfterCommitSession()
    monkeypatch.setattr(
        vcb.aq,
        "delete_active_queue_entry",
        lambda _db, contact, run, status: deleted.append((contact, run, status)) or 17,
    )
    monkeypatch.setattr(
        vcb, "_update_unanswered", lambda contact, reason, queue_id: unanswered.append((contact, reason, queue_id))
    )
    monkeypatch.setattr(vcb, "send_to_queue_exit", lambda _c, _cp, action: transfers.append(action) or ("ok", None))
    monkeypatch.setattr(vcb, "preflight_route_and_add_to_mpc", lambda *args, **kwargs: (None, preflight_error))

    monkeypatch.setattr(vcb, "RoutingRedis", lambda _v=FakeRoutingRedis(cp=_CP, queue_config=_qc()): _v)
    monkeypatch.setattr(vcb, "VoiceRedis", lambda _v=FakeVoiceRedis(): _v)
    monkeypatch.setattr(vcb, "_create_call_leg_seam", lambda *args: None)
    result = vcb.handle_initialised_callback("ct-1", "qnr-1", {"member_id": "10"}, db=db)

    assert result == ("", preflight_error)
    assert deleted == [("ct-1", "qnr-1", rc.CONTACT_STATE_FAILED)]
    assert unanswered == [("ct-1", rc.UNANSWERED_ASSIGNMENT_FAILED, 17)]
    assert transfers == [expected_action]
    assert vcb.has_unsafe_callback_effect(db) is True
    # The conversation event cannot escape before the controller commits SQL.
    vcb.drain_after_commit_effects(db)


def test_initialised_callback_transient_route_failure_skips_terminal_cleanup(init_wiring, monkeypatch):
    deleted, unanswered, transfers = [], [], []
    monkeypatch.setattr(vcb.aq, "delete_active_queue_entry", lambda *args, **kwargs: deleted.append(args))
    monkeypatch.setattr(vcb, "_update_unanswered", lambda *args, **kwargs: unanswered.append(args))
    monkeypatch.setattr(vcb, "send_to_queue_exit", lambda *args, **kwargs: transfers.append(args) or ("", None))
    monkeypatch.setattr(
        vcb, "route_and_add_to_mpc", lambda *args, **kwargs: ("", "retryable voice callback: provider unavailable")
    )
    monkeypatch.setattr(
        vcb,
        "preflight_route_and_add_to_mpc",
        lambda *args, **kwargs: (
            {"terminal_noop": False, "contact_params": dict(_CP), "agents": ["sip:agent-1"]},
            None,
        ),
    )

    monkeypatch.setattr(vcb, "RoutingRedis", lambda _v=FakeRoutingRedis(cp=_CP, queue_config=_qc()): _v)
    monkeypatch.setattr(vcb, "VoiceRedis", lambda _v=FakeVoiceRedis(): _v)
    monkeypatch.setattr(vcb, "_create_call_leg_seam", lambda *args: None)
    result = vcb.handle_initialised_callback("ct-1", "qnr-1", {"member_id": "10"}, db=None)

    assert result == ("", "retryable voice callback: provider unavailable")
    assert deleted == []
    assert unanswered == []
    assert transfers == []


# ======================================================================================
# RouteAndAddToMPC
# ======================================================================================
@pytest.mark.parametrize(
    ("agents", "selection_error", "expected_error"),
    [
        (
            None,
            "error filtering agents by capacity",
            "retryable voice callback: agent selection dependency unavailable",
        ),
        (["sip:agent-1"], None, None),
    ],
)
def test_capacity_selection_is_replay_safe_before_provider_mutation(
    monkeypatch, agents, selection_error, expected_error
):
    import hs.routing.get_agents as rga

    monkeypatch.setattr(rga, "get_agents", lambda *args, **kwargs: (agents, selection_error))
    db = FakeAfterCommitSession()

    monkeypatch.setattr(vcb, "RoutingRedis", lambda _v=FakeRoutingRedis(cp=_CP): _v)
    preflight, error = vcb.preflight_route_and_add_to_mpc("ct-1", "qnr-1", False, "", "", "", db)

    assert error == expected_error
    if expected_error is None:
        assert preflight["agents"] == ["sip:agent-1"]
    else:
        assert preflight is None
    assert vcb.has_unsafe_callback_effect(db) is False


def test_retry_limit_routes_to_assignment_failed_without_error_log(monkeypatch):
    import hs.routing.get_agents as rga

    known_outcome = "known_error: retry limit reached"
    monkeypatch.setattr(rga, "get_agents", lambda *args, **kwargs: (None, known_outcome))
    infos = []
    errors = []
    monkeypatch.setattr(
        vcb.logger, "info", lambda *args, **kwargs: infos.append(args[0] if args else kwargs.get("msg", ""))
    )
    monkeypatch.setattr(
        vcb.logger, "error", lambda *args, **kwargs: errors.append(args[0] if args else kwargs.get("msg", ""))
    )

    monkeypatch.setattr(vcb, "RoutingRedis", lambda _v=FakeRoutingRedis(cp=_CP): _v)
    preflight, error = vcb.preflight_route_and_add_to_mpc("ct-1", "qnr-1", False, "", "", "", FakeAfterCommitSession())

    assert preflight is None and error == known_outcome
    assert any("routing retries exhausted" in msg for msg in infos)
    assert not any("Could not fetch agents" in msg for msg in errors)


def test_route_and_add_seam_failure_releases_capacity(monkeypatch):
    """add_agents_to_mpc failure (an injected failing impl with the raise contract) → per-agent
    capacity release + assignment_failed."""
    import hs.routing.get_agents as rga

    monkeypatch.setattr(rga, "get_agents", lambda *a, **k: (["sip:agent-1", "sip:agent-2"], None))
    rr = FakeRoutingRedis(cp=_CP)

    def boom(*a, **k):
        raise RuntimeError("adding agents failed")

    monkeypatch.setattr(vcb, "RoutingRedis", lambda: rr)
    monkeypatch.setattr("hs.voice.agent_leg.add_agents_to_mpc", boom)
    res, err = vcb.route_and_add_to_mpc("ct-1", "qnr-1", False, "", "", "", "", db=None)
    assert res == rc.QNR_STATE_ASSIGNMENT_FAILED and "adding agents failed" in err
    assert rr.decreases == [("A", "sip:agent-1", "call", "9911"), ("A", "sip:agent-2", "call", "9911")]


def test_route_and_add_retryable_provider_failure_preserves_reservations(monkeypatch):
    import hs.routing.get_agents as rga

    monkeypatch.setattr(rga, "get_agents", lambda *args, **kwargs: (["sip:agent-1"], None))
    rr = FakeRoutingRedis(cp=_CP)

    def unavailable(*args, **kwargs):
        raise RuntimeError("retryable voice callback: provider voice API unavailable")

    monkeypatch.setattr(vcb, "RoutingRedis", lambda: rr)
    monkeypatch.setattr("hs.voice.agent_leg.add_agents_to_mpc", unavailable)
    result = vcb.route_and_add_to_mpc("ct-1", "qnr-1", False, "", "", "", "", db=None)

    assert result == (rc.QNR_STATE_ASSIGNMENT_FAILED, "retryable voice callback: provider voice API unavailable")
    assert rr.decreases == []


def test_route_and_add_zero_agents_requeues_and_flags_recompute(monkeypatch):
    statuses = []
    monkeypatch.setattr(
        vcb.aq, "update_queue_status", lambda db, c, q, s, *, strict=False: statuses.append((c, s, strict))
    )
    import hs.routing.get_agents as rga

    monkeypatch.setattr(rga, "get_agents", lambda *a, **k: ([], None))
    rr = FakeRoutingRedis(cp=_CP)
    rr.qpwt["n-1"] = {"recompute": False}  # the QPWT entry exists → flag rewritten
    db = FakeAfterCommitSession()
    monkeypatch.setattr(vcb, "RoutingRedis", lambda: rr)
    res, err = vcb.route_and_add_to_mpc("ct-1", "qnr-1", False, "", "", "", "", db=db)
    assert (res, err) == ("", None)
    assert statuses == [("ct-1", rc.CONTACT_STATE_QUEUED, True)]
    # The DB row is only flushed here. External visibility waits for the
    # controller-owned commit and explicit drain.
    assert rr.qpwt_sets == []
    vcb.drain_after_commit_effects(db)
    assert rr.qpwt_sets[-1][1]["recompute"] is True
    # A second drain cannot replay already-executed effects.
    vcb.drain_after_commit_effects(db)
    assert len(rr.qpwt_sets) == 1


def test_route_and_add_zero_agents_requires_durable_queued_before_side_effects(monkeypatch):
    strict_calls = []
    monkeypatch.setattr(
        vcb.aq,
        "update_queue_status",
        lambda db, c, q, s, *, strict=False: (
            strict_calls.append((c, q, s, strict)) or vcb.aq.QUEUE_STATUS_PERSISTENCE_RETRYABLE
        ),
    )
    import hs.routing.get_agents as rga

    monkeypatch.setattr(rga, "get_agents", lambda *a, **k: ([], None))
    rr = FakeRoutingRedis(cp=_CP)
    rr.qpwt["n-1"] = {"recompute": False}
    db = FakeAfterCommitSession()

    monkeypatch.setattr(vcb, "RoutingRedis", lambda: rr)
    res, err = vcb.route_and_add_to_mpc("ct-1", "qnr-1", False, "", "", "", "", db=db)

    assert res == ""
    assert err.startswith("retryable_queue_persistence:")
    assert strict_calls == [("ct-1", "qnr-1", rc.CONTACT_STATE_QUEUED, True)]
    assert rr.qpwt_sets == []
    assert db.info == {}


def test_route_and_add_zero_agents_late_after_hangup_is_terminal_noop(monkeypatch):
    monkeypatch.setattr(
        vcb.aq, "update_queue_status", lambda db, c, q, s, *, strict=False: vcb.aq.QUEUE_STATUS_NO_LIVE_ROW
    )
    import hs.routing.get_agents as rga

    monkeypatch.setattr(rga, "get_agents", lambda *a, **k: ([], None))
    rr = FakeRoutingRedis(cp=_CP)
    rr.qpwt["n-1"] = {"recompute": False}
    db = FakeAfterCommitSession()

    monkeypatch.setattr(vcb, "RoutingRedis", lambda: rr)
    result = vcb.route_and_add_to_mpc("ct-1", "qnr-1", False, "", "", "", "", db=db)

    # A hangup already soft-deleted the row. The late callback is acknowledged
    # without a retry loop and cannot resurrect QUEUED externally.
    assert result == ("", None)
    assert rr.qpwt_sets == []
    assert db.info == {}
    vcb.drain_after_commit_effects(db)


def test_route_and_add_zero_agents_rollback_discards_staged_effect(monkeypatch):
    monkeypatch.setattr(vcb.aq, "update_queue_status", lambda db, c, q, s, *, strict=False: None)
    import hs.routing.get_agents as rga

    monkeypatch.setattr(rga, "get_agents", lambda *a, **k: ([], None))
    rr = FakeRoutingRedis(cp=_CP)
    rr.qpwt["n-1"] = {"recompute": False}
    db = FakeAfterCommitSession()

    monkeypatch.setattr(vcb, "RoutingRedis", lambda: rr)
    assert vcb.route_and_add_to_mpc("ct-1", "qnr-1", False, "", "", "", "", db=db) == ("", None)
    assert rr.qpwt_sets == []

    # This is the controller's rollback/commit-failure path.
    vcb.discard_after_commit_effects(db)
    vcb.drain_after_commit_effects(db)
    assert rr.qpwt_sets == []


def test_route_and_add_transfer_no_agents_is_an_error(monkeypatch):
    import hs.routing.get_agents as rga

    monkeypatch.setattr(rga, "get_agents", lambda *a, **k: ([], None))
    monkeypatch.setattr(vcb, "RoutingRedis", lambda _v=FakeRoutingRedis(cp=_CP): _v)
    res, err = vcb.route_and_add_to_mpc("ct-1", "qnr-1", True, "warm", "", "", "", db=None)
    assert (res, err) == ("", "no available agents found")


def test_route_and_add_member_bypasses_get_agents(monkeypatch):
    """member != '' → agents=[member], NO get_agents (the transfer/queue-next entry)."""
    captured = []
    monkeypatch.setattr(vcb, "RoutingRedis", lambda _v=FakeRoutingRedis(cp=_CP): _v)
    monkeypatch.setattr(
        "hs.voice.agent_leg.add_agents_to_mpc", lambda cp, agents, prim, it, tt, td: captured.append((agents, prim))
    )
    res, err = vcb.route_and_add_to_mpc("ct-1", "qnr-1", False, "", "", "sip:member@x", "sip:primary@x", db=None)
    assert (res, err) == ("", None)
    assert captured == [(["sip:member@x"], "sip:primary@x")]


# ======================================================================================
# Prompt rotation
# ======================================================================================
def _rotation(monkeypatch, prompts, params, member="10"):
    played = []
    monkeypatch.setattr(
        vcb,
        "_play_prompt",
        lambda prompt, cp, m, call_uuid, db: (
            played.append((prompt.prompt_type, prompt.text or prompt.url)) or ("ok", None)
        ),
    )
    vr = FakeVoiceRedis()
    vr.prompt_params = (params, None)
    rr = FakeRoutingRedis(cp=_CP, queue_config=_qc(prompts=prompts))
    monkeypatch.setattr(vcb, "RoutingRedis", lambda: rr)
    monkeypatch.setattr(vcb, "VoiceRedis", lambda: vr)
    msg, err = vcb.handle_prompt_stopped("ct-1", "qnr-1", {"member_id": member}, db=None)
    return msg, err, played, vr.prompt_sets


def test_prompt_rotation_advances_sequentially(monkeypatch):
    prompts = [
        {"prompt_type": "speak", "text": "one"},
        {"prompt_type": "speak", "text": "two"},
        {"prompt_type": "play", "url": "http://h/loop.mp3", "loop_with_others": True},
    ]
    _msg, err, played, sets = _rotation(
        monkeypatch,
        prompts,
        {"prompt_number": 0, "prompt_play_complete": False, "prompt_loop_count": 0, "repeat_count": 0},
    )
    assert err is None and played == [("speak", "two")]
    assert sets == [{"prompt_number": 1, "prompt_play_complete": False, "prompt_loop_count": 0, "repeat_count": 0}]


def test_prompt_rotation_last_sequential_hands_off_to_loop_prompts(monkeypatch):
    prompts = [
        {"prompt_type": "speak", "text": "one"},
        {"prompt_type": "play", "url": "http://h/loop.mp3", "loop_with_others": True},
    ]
    _msg, err, played, sets = _rotation(
        monkeypatch,
        prompts,
        {"prompt_number": 1, "prompt_play_complete": False, "prompt_loop_count": 0, "repeat_count": 0},
    )
    assert err is None and played == [("play", "http://h/loop.mp3")]
    assert sets == [{"prompt_number": 2, "prompt_play_complete": True, "prompt_loop_count": 0, "repeat_count": 0}]


def test_prompt_rotation_loop_wraps_to_first_loop_prompt(monkeypatch):
    prompts = [
        {"prompt_type": "play", "url": "http://h/a.mp3", "loop_with_others": True},
        {"prompt_type": "play", "url": "http://h/b.mp3", "loop_with_others": True},
    ]
    _msg, err, played, sets = _rotation(
        monkeypatch,
        prompts,
        {"prompt_number": 0, "prompt_play_complete": True, "prompt_loop_count": 1, "repeat_count": 0},
    )
    assert err is None and played == [("play", "http://h/a.mp3")]
    assert sets == [{"prompt_number": 0, "prompt_play_complete": True, "prompt_loop_count": 0, "repeat_count": 0}]


@pytest.mark.parametrize(
    ("loop", "repeat", "expected_url", "expected_state"),
    [
        (0, 0, "http://h/hold-a.mp3", (0, True, 0, 1)),  # HoldA repeats (its own loops=2)
        (0, 2, "http://h/hold-b.mp3", (0, True, 1, 0)),  # HoldA's repeats spent: advance
        (1, 0, "http://h/hold-a.mp3", (0, True, 0, 0)),  # HoldB never repeats (loops=0): wrap
    ],
)
def test_prompt_rotation_loop_repeats_use_the_loop_prompts_own_count(
    monkeypatch, loop, repeat, expected_url, expected_state
):
    prompts = [
        {"prompt_type": "speak", "text": "Welcome", "loops": 0},
        {"prompt_type": "play", "url": "http://h/hold-a.mp3", "loop_with_others": True, "loops": 2},
        {"prompt_type": "play", "url": "http://h/hold-b.mp3", "loop_with_others": True, "loops": 0},
    ]
    state = {"prompt_number": 0, "prompt_play_complete": True, "prompt_loop_count": loop, "repeat_count": repeat}
    _msg, err, played, sets = _rotation(monkeypatch, prompts, state)
    assert err is None and played == [("play", expected_url)]
    assert sets == [dict(zip(state, expected_state))]


def test_prompt_rotation_completed_no_loops_errors(monkeypatch):
    prompts = [{"prompt_type": "speak", "text": "only"}]
    _msg, err, played, _sets = _rotation(
        monkeypatch,
        prompts,
        {"prompt_number": 0, "prompt_play_complete": False, "prompt_loop_count": 0, "repeat_count": 0},
    )
    assert err == "completed playing all prompts, no loop prompts" and played == []


def test_prompt_rotation_member_all_is_ignored(monkeypatch):
    msg, err, played, _sets = _rotation(
        monkeypatch,
        [{"prompt_type": "speak", "text": "x"}],
        {"prompt_number": 0, "prompt_play_complete": False, "prompt_loop_count": 0, "repeat_count": 0},
        member="all",
    )
    assert (msg, err) == ("", None) and played == []


def test_prompt_rotation_redis_transport_error_is_retryable(monkeypatch):
    vr = FakeVoiceRedis()
    vr.conference_active = (False, "redis timeout")

    monkeypatch.setattr(vcb, "RoutingRedis", lambda _v=FakeRoutingRedis(cp=_CP): _v)
    monkeypatch.setattr(vcb, "VoiceRedis", lambda: vr)
    result = vcb.handle_prompt_stopped("ct-1", "qnr-1", {"member_id": "10"}, db=None)

    assert result == ("", "retryable voice callback: conference state cache unavailable")


@pytest.mark.parametrize(("status", "expected"), [(503, "non 2xx response (503)"), (404, "non 2xx response (404)")])
def test_prompt_provider_mutation_failure_is_never_callback_retried(monkeypatch, status, expected):
    class Client:
        def __init__(self, _auth_id, _auth_token):
            pass

        def mpc_start_play_audio(self, _mpc_id, _member_id, _body, quiet_not_found=False):
            return None, status, f"non 2xx response ({status})"

    monkeypatch.setattr(vcb.pv, "PlivoVoiceClient", Client)
    prompt = _prompt(prompt_type="play", url="https://example.test/prompt.mp3")

    assert vcb._play_prompt(prompt, _CP, "10", "", None) == ("", expected)


# ======================================================================================
# DialInput queue-exit
# ======================================================================================
def test_dial_input_allowed_digit_runs_the_exit_chain(monkeypatch):
    deleted, unanswered, transfers, hangups = [], [], [], []
    monkeypatch.setattr(vcb, "_spawn", lambda target, *a: target(*a))
    monkeypatch.setattr(vcb.aq, "delete_active_queue_entry", lambda db, c, q, s: deleted.append((c, s)) or 9)
    monkeypatch.setattr(vcb, "_update_unanswered", lambda c, r, aqid: unanswered.append((c, r, aqid)))
    monkeypatch.setattr(
        vcb,
        "send_to_queue_exit",
        lambda _c, _cp, result, digit="": transfers.append((result, digit)) or ("moved", None),
    )

    class FakeClient:
        def __init__(self, a, t):
            pass

        def call_delete(self, call_id):
            hangups.append(call_id)
            return None, 204, None

    monkeypatch.setattr(vcb.pv, "PlivoVoiceClient", FakeClient)
    vr = FakeVoiceRedis()
    vr.mpc_call_uuids = ["leg-1", "leg-2"]
    rr = FakeRoutingRedis(cp=_CP, queue_config=_qc(allowed=["1", "2"]))
    monkeypatch.setattr(vcb, "RoutingRedis", lambda: rr)
    monkeypatch.setattr(vcb, "VoiceRedis", lambda: vr)
    msg, err = vcb.handle_dial_input(
        "ct-1", "qnr-1", {"participant_role": "customer", "digit_input": "1", "member_id": "10"}, db=None
    )
    assert (msg, err) == ("moved", None)
    assert vr.toggles == [("qnr-1", "false", 0)]
    assert deleted == [("ct-1", rc.CONTACT_STATE_EXIT)]
    assert unanswered == [("ct-1", rc.UNANSWERED_QUEUE_EXIT, 9)]
    assert transfers == [("queue_exit", "1")]  # the exit digit rides with the queue_exit result
    assert hangups == ["leg-1", "leg-2"]


def test_dial_input_disallowed_digit_and_agent_role(monkeypatch):
    rr = FakeRoutingRedis(cp=_CP, queue_config=_qc(allowed=["1"]))
    monkeypatch.setattr(vcb, "RoutingRedis", lambda: rr)
    monkeypatch.setattr(vcb, "VoiceRedis", lambda _v=FakeVoiceRedis(): _v)
    msg, err = vcb.handle_dial_input("ct-1", "qnr-1", {"participant_role": "customer", "digit_input": "9"}, db=None)
    assert (msg, err) == ("Entered invalid digit", None)
    monkeypatch.setattr(vcb, "RoutingRedis", lambda: rr)
    monkeypatch.setattr(vcb, "VoiceRedis", lambda _v=FakeVoiceRedis(): _v)
    msg, err = vcb.handle_dial_input("ct-1", "qnr-1", {"participant_role": "agent", "digit_input": "1"}, db=None)
    assert (msg, err) == ("input for agent received", None)


def test_dial_input_inactive_conference_is_terminal_noop(monkeypatch):
    vr = FakeVoiceRedis()
    vr.conference_active = (False, None)
    monkeypatch.setattr(vcb, "RoutingRedis", lambda _v=FakeRoutingRedis(cp=_CP): _v)
    monkeypatch.setattr(vcb, "VoiceRedis", lambda: vr)
    msg, err = vcb.handle_dial_input("ct-1", "qnr-1", {"participant_role": "customer", "digit_input": "1"}, db=None)
    assert (msg, err) == ("input for agent received", None)


def test_dial_input_redis_failure_remains_retryable_error(monkeypatch):
    vr = FakeVoiceRedis()
    vr.conference_active = (False, "redis unavailable")
    monkeypatch.setattr(vcb, "RoutingRedis", lambda _v=FakeRoutingRedis(cp=_CP): _v)
    monkeypatch.setattr(vcb, "VoiceRedis", lambda: vr)
    msg, err = vcb.handle_dial_input("ct-1", "qnr-1", {"participant_role": "customer", "digit_input": "1"}, db=None)
    assert (msg, err) == ("", "retryable voice callback: conference state cache unavailable")


def test_dial_input_db_failure_is_retryable_before_conference_is_disabled(monkeypatch):
    monkeypatch.setattr(
        vcb.aq,
        "delete_active_queue_entry",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("writer unavailable")),
    )
    vr = FakeVoiceRedis()

    monkeypatch.setattr(vcb, "RoutingRedis", lambda _v=FakeRoutingRedis(cp=_CP, queue_config=_qc(allowed=["1"])): _v)
    monkeypatch.setattr(vcb, "VoiceRedis", lambda: vr)
    result = vcb.handle_dial_input("ct-1", "qnr-1", {"participant_role": "customer", "digit_input": "1"}, db=None)

    assert result == ("", "retryable voice callback: unable to update queue status")
    assert vr.toggles == []


# ======================================================================================
# WaitTimeout + MPCStart + missed event
# ======================================================================================
def test_wait_timeout_chain(monkeypatch):
    deleted, unanswered = [], []
    monkeypatch.setattr(vcb, "_spawn", lambda target, *a: target(*a))
    monkeypatch.setattr(vcb.aq, "delete_active_queue_entry", lambda db, c, q, s: deleted.append((c, s)) or 5)
    import hs.conversations.child_update as ccu
    import hs.routing.invitation as ri
    from hs.infra.models import UseCaseResponse

    monkeypatch.setattr(ri, "_ensure_request_context", lambda: None)
    monkeypatch.setattr(
        ccu, "update_child_conversation", lambda update: unanswered.append(update) or UseCaseResponse(status_code=200)
    )
    vr = FakeVoiceRedis()
    monkeypatch.setattr(vcb, "RoutingRedis", lambda _v=FakeRoutingRedis(cp=_CP): _v)
    monkeypatch.setattr(vcb, "VoiceRedis", lambda: vr)
    msg, err = vcb.handle_wait_timeout("ct-1", "qnr-1", db=None)
    assert (msg, err) == ("Successfully handled wait timeout event", None)
    assert vr.toggles == [("qnr-1", "false", 0)]
    assert deleted == [("ct-1", rc.CONTACT_STATE_TIMEDOUT)]
    # the unanswered reason lands on the child linked to the queue entry just closed
    [update] = unanswered
    assert (update.contact_uuid, update.reason_for_unanswered, update.active_queue_id) == (
        "ct-1",
        rc.UNANSWERED_QUEUE_TIMEOUT,
        5,
    )


def test_mpc_start_toggles_off_and_guards_mpc_end(monkeypatch):
    real_end_mpc_calls = vcb._end_mpc_calls_seam
    vr = FakeVoiceRedis()
    monkeypatch.setattr(vcb, "VoiceRedis", lambda: vr)
    monkeypatch.setattr(vcb, "_end_mpc_calls_seam", lambda c, q: None)
    msg, err = vcb.handle_mpc_start("ct-1", "qnr-1")
    assert (msg, err) == ("Successfully updated conversation", None)
    assert vr.toggles == [("qnr-1", "false", 0)]
    # MPCEnd present → end_mpc_calls fires; its failure (the seam raises) is LOGGED only
    vr2 = FakeVoiceRedis()
    vr2.events[vc.MPC_END_EVENT] = "111"
    ended = []
    monkeypatch.setattr(vcb, "VoiceRedis", lambda: vr2)
    monkeypatch.setattr(vcb, "_end_mpc_calls_seam", lambda c, q: ended.append((c, q)))
    msg, err = vcb.handle_mpc_start("ct-1", "qnr-1")
    assert (msg, err) == ("Successfully updated conversation", None)
    assert ended == [("ct-1", "qnr-1")]
    monkeypatch.setattr(vcb, "_end_mpc_calls_seam", real_end_mpc_calls)
    msg, err = vcb.handle_mpc_start("ct-1", "qnr-1")  # the real seam raises offline
    assert (msg, err) == ("Successfully updated conversation", None)  # the error is swallowed


def test_missed_event_ignores_non_initialized_and_mimics_initialized(monkeypatch):
    assert vcb.handle_missed_mpc_event({"eventName": "MPCEnd", "callUUID": "c", "qnrRunUUID": "q"}, db=None) is None
    # the mimic: customer fetched from the LIVE MPC, the MPCInitialized-dep-MPCEnd gate, then
    # handle_initialised_callback with the participant-shaped params
    monkeypatch.setattr(
        vcb,
        "_get_customer_details_from_mpc",
        lambda c, q: ({"role": "Customer", "member_address": "+1555", "member_id": 10, "mpc_uuid": "m-1"}, None),
    )
    captured = {}

    def fake_init(call_uuid, qnr_run_uuid, callback_params, db, **kw):
        captured.update(callback_params)
        return "Initialised MPC", None

    monkeypatch.setattr(vcb, "handle_initialised_callback", fake_init)
    vr = FakeVoiceRedis()
    monkeypatch.setattr(vcb, "RoutingRedis", lambda _v=FakeRoutingRedis(cp=_CP): _v)
    monkeypatch.setattr(vcb, "VoiceRedis", lambda: vr)
    err = vcb.handle_missed_mpc_event(
        {"eventName": "MPCInitialized", "callUUID": "ct-1", "qnrRunUUID": "qnr-1"}, db=None
    )
    assert err is None
    assert vr.dep_calls == [("ct-1", "qnr-1", "MPCInitialized", "", "MPCEnd")]
    assert captured == {"member_id": "10"}


def test_missed_event_already_processed_short_circuits(monkeypatch):
    monkeypatch.setattr(vcb, "_get_customer_details_from_mpc", lambda c, q: ({"role": "customer"}, None))
    vr = FakeVoiceRedis()
    vr.dep_result = vc.VALUE_EXISTS_ERROR
    monkeypatch.setattr(vcb, "RoutingRedis", lambda _v=FakeRoutingRedis(cp=_CP): _v)
    monkeypatch.setattr(vcb, "VoiceRedis", lambda: vr)
    err = vcb.handle_missed_mpc_event({"eventName": "MPCInitialized", "callUUID": "c", "qnrRunUUID": "q"}, db=None)
    assert err is None


def test_missed_event_handler_failure_releases_exact_claim(monkeypatch):
    monkeypatch.setattr(
        vcb,
        "_get_customer_details_from_mpc",
        lambda _c, _q: ({"role": "customer", "member_address": "+1555", "member_id": 10, "mpc_uuid": "mpc-1"}, None),
    )
    monkeypatch.setattr(vcb, "handle_initialised_callback", lambda *_args, **_kwargs: ("", "DB write failed"))
    vr = FakeVoiceRedis()

    monkeypatch.setattr(vcb, "RoutingRedis", lambda _v=FakeRoutingRedis(cp=_CP): _v)
    monkeypatch.setattr(vcb, "VoiceRedis", lambda: vr)
    err = vcb.handle_missed_mpc_event({"eventName": "MPCInitialized", "callUUID": "c", "qnrRunUUID": "q"}, db=None)

    assert err == "DB write failed"
    assert len(vr.released) == 1
    assert vr.released[0][:3] == ("c", "q", "MPCInitialized")
    assert vr.released[0][3]


def test_missed_event_provider_404_after_teardown_is_idempotent(monkeypatch):
    monkeypatch.setattr(vcb, "_get_customer_details_from_mpc", lambda _c, _q: (None, "mpc already absent"))
    vr = FakeVoiceRedis()

    monkeypatch.setattr(vcb, "RoutingRedis", lambda _v=FakeRoutingRedis(cp=_CP): _v)
    monkeypatch.setattr(vcb, "VoiceRedis", lambda: vr)
    err = vcb.handle_missed_mpc_event({"eventName": "MPCInitialized", "callUUID": "c", "qnrRunUUID": "q"}, db=None)

    assert err is None
    assert vr.dep_calls == []


def test_get_customer_details_requires_exact_terminal_proof_for_provider_404(monkeypatch):
    quiet_values = []

    class _GoneClient:
        def __init__(self, *_args):
            pass

        def mpc_list_participants(self, _mpc_id, *, quiet_not_found=False):
            quiet_values.append(quiet_not_found)
            return None, 404, "non 2xx response (404)"

    monkeypatch.setattr(vcb.pv, "PlivoVoiceClient", _GoneClient)

    vr = FakeVoiceRedis()
    monkeypatch.setattr(vcb, "RoutingRedis", lambda: FakeRoutingRedis(cp=_CP))
    monkeypatch.setattr(vcb, "VoiceRedis", lambda: vr)
    customer, error = vcb._get_customer_details_from_mpc("c", "q")

    assert customer is None
    assert error == "non 2xx response (404)"

    vr.events[vc.MPC_END_EVENT] = "terminal-time"
    customer, error = vcb._get_customer_details_from_mpc("c", "q")

    assert customer is None
    assert error == "mpc already absent"
    assert quiet_values == [False, True]


@pytest.mark.parametrize("status", [0, 503])
def test_get_customer_details_marks_replay_safe_provider_outage_retryable(monkeypatch, status):
    class _UnavailableClient:
        def __init__(self, *_args):
            pass

        def mpc_list_participants(self, *_args, **_kwargs):
            return None, status, f"non 2xx response ({status})"

    monkeypatch.setattr(vcb.pv, "PlivoVoiceClient", _UnavailableClient)

    monkeypatch.setattr(vcb, "RoutingRedis", lambda: FakeRoutingRedis(cp=_CP))
    monkeypatch.setattr(vcb, "VoiceRedis", FakeVoiceRedis)
    customer, error = vcb._get_customer_details_from_mpc("c", "q")

    assert customer is None
    assert error == ("retryable voice callback: provider voice API unavailable")


@pytest.mark.parametrize(
    ("operation", "expected_message"), [("audio", "MPC already ended"), ("text", "MPC already ended")]
)
def test_prompt_member_404_is_noop_only_with_exact_terminal_event(monkeypatch, operation, expected_message):
    calls = []

    class _GoneClient:
        def __init__(self, *_args):
            pass

        def mpc_start_play_audio(self, mpc_id, member_id, body, *, quiet_not_found=False):
            calls.append(("play", mpc_id, member_id, body, quiet_not_found))
            return None, 404, "non 2xx response (404)"

        def mpc_start_speak(self, mpc_id, member_id, body, *, quiet_not_found=False):
            calls.append(("speak", mpc_id, member_id, body, quiet_not_found))
            return None, 404, "non 2xx response (404)"

    monkeypatch.setattr(vcb.pv, "PlivoVoiceClient", _GoneClient)
    vr = FakeVoiceRedis()
    vr.events[vc.MPC_END_EVENT] = "terminal-time"

    monkeypatch.setattr(vcb, "VoiceRedis", lambda: vr)
    if operation == "audio":
        prompt = _prompt(prompt_type="play", url="https://audio.example/wait.mp3")
    else:
        prompt = _prompt(prompt_type="speak", prompt_speak_type="text", text="Please wait", voice="WOMAN")
    message, error = vcb._play_prompt(prompt, _CP, "10121", "call-1", None)

    assert (message, error) == (expected_message, None)
    assert len(calls) == 1
    assert calls[0][-1] is True


@pytest.mark.parametrize("operation", ["audio", "text"])
def test_prompt_member_404_remains_error_while_mpc_is_active(monkeypatch, operation):
    quiet_values = []

    class _MemberMissingClient:
        def __init__(self, *_args):
            pass

        def mpc_start_play_audio(self, *_args, **kwargs):
            quiet_values.append(kwargs.get("quiet_not_found", False))
            return None, 404, "non 2xx response (404)"

        def mpc_start_speak(self, *_args, **kwargs):
            quiet_values.append(kwargs.get("quiet_not_found", False))
            return None, 404, "non 2xx response (404)"

    monkeypatch.setattr(vcb.pv, "PlivoVoiceClient", _MemberMissingClient)
    vr = FakeVoiceRedis()

    monkeypatch.setattr(vcb, "VoiceRedis", lambda: vr)
    if operation == "audio":
        prompt = _prompt(prompt_type="play", url="https://audio.example/wait.mp3")
    else:
        prompt = _prompt(prompt_type="speak", prompt_speak_type="text", text="Please wait", voice="WOMAN")
    message, error = vcb._play_prompt(prompt, _CP, "bad-member", "call-1", None)

    assert message == ""
    assert error == "non 2xx response (404)"
    assert quiet_values == [False]


# ======================================================================================
# Wait-time text machinery
# ======================================================================================
def test_ssml_get_voice_and_content():
    v, t, err = vcb._ssml_get_voice_and_content('<Speak Voice="Polly.Amy">Hi  there</Speak>')
    assert (v, t, err) == ("Polly.Amy", "Hi  there", None)
    v, t, err = vcb._ssml_get_voice_and_content("plain text")
    assert v == "Polly.Joey" and t == "plain text" and err is not None
    v, t, _ = vcb._ssml_get_voice_and_content("<speak>line1\nline2\tx</speak>")
    assert t == "line1line2x"  # innerxml newline/tab strip


def _fill(monkeypatch, text, estimate=(0, 0, 1, None)):
    """check_and_update_play_text over a stubbed estimate; returns (text, strategies asked, cache writes)."""
    calls, rr = [], FakeRoutingRedis()
    monkeypatch.setattr(vcb, "RoutingRedis", lambda: rr)
    monkeypatch.setattr(
        vcb, "calculate_estimated_wait_time_and_queue_position", lambda db, q, s, qnr: calls.append(s) or estimate
    )
    return vcb.check_and_update_play_text(None, "qnr-1", "q-1", "n-1", text), calls, rr.qpwt_sets


def test_format_wait_time_branches(monkeypatch):
    w = vc.ESTIMATED_WAIT_TIME_VALUE
    assert _fill(monkeypatch, f"wait {w}", (2, 30, 0, None))[0] == "wait 2 minutes and 30 seconds"
    assert _fill(monkeypatch, f"wait {w}", (2, 0, 0, None))[0] == "wait 2 minutes"
    assert _fill(monkeypatch, f"wait {w}", (0, 45, 0, None))[0] == "wait 45 seconds"
    text = f"p {vc.QUEUE_POSITION_VALUE} {w}"
    assert _fill(monkeypatch, text, (1, 0, 3, None))[0] == "p 3 1 minutes"
    assert _fill(monkeypatch, text, (1, 0, 0, None))[0] == f"p {vc.QUEUE_POSITION_VALUE} 1 minutes"  # position 0: kept


def test_mean_mode_average_and_mode_fallback(monkeypatch):
    from datetime import datetime, timedelta

    t0 = datetime(2026, 1, 1)

    def estimate(strategy, *seconds):
        entries = [type("E", (), {"created_at": t0, "deleted_at": t0 + timedelta(seconds=x)})() for x in seconds]
        monkeypatch.setattr(vcb.aq, "get_last_n_active_queue_entries_for_queue_uuid", lambda db, q, qnr: (entries, 1))
        return vcb.calculate_estimated_wait_time_and_queue_position(None, "q-1", strategy, "qnr-1")

    # all unique → mode falls back to MAX; mean=(10+20+30)/3=20; (20+30)/2=25
    assert estimate("mean", 10, 20, 30) == (0, 25, 1, None)
    # a true mode: [10,10,40] → mean 20, mode 10 → 15
    assert estimate("mean", 10, 10, 40) == (0, 15, 1, None)
    assert estimate("min", 70, 90) == (1, 10, 1, None) and estimate("max", 70, 90) == (1, 30, 1, None)
    open_entry = type("E", (), {"created_at": t0, "deleted_at": None})()
    monkeypatch.setattr(vcb.aq, "get_last_n_active_queue_entries_for_queue_uuid", lambda db, q, qnr: ([open_entry], 1))
    assert vcb.calculate_estimated_wait_time_and_queue_position(None, "q", "x", "r")[3] is not None  # unsupported
    assert vcb.calculate_estimated_wait_time_and_queue_position(None, "q", "mean", "r")[3] is not None  # no durations


def test_dead_recompute_bug_always_recomputes(monkeypatch):
    """A fresh cached value with recompute=false is
    IGNORED — the Postgres recompute runs anyway and rewrites the cache."""
    text, calls, sets = _fill(monkeypatch, f"wait {vc.ESTIMATED_WAIT_TIME_VALUE}", (2, 30, 4, None))
    assert calls == ["mean"]
    assert text == "wait 2 minutes and 30 seconds"
    assert sets[-1][1] == {
        "estimated_wait_time_minutes": 2,
        "estimated_wait_time_seconds": 30,
        "queue_position": 4,
        "recompute": False,
    }


def test_wait_time_zero_forced_to_one_minute(monkeypatch):
    assert _fill(monkeypatch, f"wait {vc.ESTIMATED_WAIT_TIME_VALUE}")[0] == "wait 1 minutes"  # forced 1m0s


@pytest.mark.parametrize(
    "text, strategies",
    [
        ("no pattern here", []),
        ("", []),
        ("<speak>please hold</speak>", []),
        (f"x {vc.MIN_ESTIMATED_WAIT_TIME_VALUE}", ["min"]),
        (f"x {vc.MAX_ESTIMATED_WAIT_TIME_VALUE}", ["max"]),
        (f"x {vc.ESTIMATED_WAIT_TIME_VALUE}", ["mean"]),
        (f"you are number {vc.QUEUE_POSITION_VALUE}", ["mean"]),
    ],
)
def test_check_and_update_play_text_estimates_only_for_placeholders(monkeypatch, text, strategies):
    filled, calls, _sets = _fill(monkeypatch, text)
    assert calls == strategies
    if not strategies:
        assert filled == text


def test_send_to_queue_exit_moves_the_caller_to_the_continuation(monkeypatch):
    sent = {}

    class FakeClient:
        def __init__(self, a, t):
            sent["auth"] = (a, t)

        def call_update(self, call_uuid, body):
            sent["call_uuid"] = call_uuid
            sent["body"] = body
            return {"message": "call updated"}, 202, None

    monkeypatch.setattr(vcb.pv, "PlivoVoiceClient", FakeClient)
    monkeypatch.setattr(vcb, "queue_exit_url", lambda auth, call, run, **outcome: f"exit:{auth}:{call}:{run}:{outcome}")
    cp = {"auth_id": "A", "auth_token": "T", "flow_run_uuid": "flow-1", "queue_and_route_run_uuid": "qnr-1"}
    msg, err = vcb.send_to_queue_exit("ct-1", cp, "assignment_failed")
    assert (msg, err) == ("call updated", None)
    assert sent["body"] == {"legs": "aleg", "aleg_url": "exit:A:ct-1:qnr-1:{'result': 'assignment_failed'}"}
