"""Enqueue: the handle_incoming_contact decision tree + invite_agents_to_channel dispatch, with fakes
(no live stack).
"""

import json
from types import SimpleNamespace

import pytest

import hs.voice.intake as vi
from hs.intake import enqueue as ic
from hs.intake import flow
from hs.intake.models import IncomingContactParams
from hs.routing import constants as rc
from hs.routing import invitation as inv
from hs.setup.api import QueueIn
from hs.voice import constants as vc
from tests.unit._invite_seams import patch_invite_seams

_QUEUE = "00000000-0000-4000-a000-000000000004"
_START_MPC = vi.handle_incoming_call


# ======================================================================================
# Fakes
# ======================================================================================
class FakeRoutingRedis:
    def __init__(self):
        self.sets = {}  # key tuple → (json, ttl)
        self.hsets = {}  # (key, field) → json
        self.deleted = []
        self.tip = {}
        self.retry_val = 0

    def set_queue_and_route_config(self, flow, run, cfg_json, ttl):
        self.sets[("QARC", flow, run)] = (cfg_json, ttl)
        return True

    def get_contact_parameters(self, contact_uuid, run):
        return {"contact_uuid": contact_uuid}

    def set_queue_config(self, flow, run, cfg_json, ttl):
        self.sets[("QC", flow, run)] = (cfg_json, ttl)
        return True

    def store_contact_params(self, contact_uuid, qnr, params_json):
        self.hsets[(contact_uuid, qnr)] = params_json
        return 1

    def delete_contact_to_agent_mapping(self, contact_uuid):
        self.deleted.append(contact_uuid)
        return 1

    def set_transfer_in_progress(self, contact_uuid, info_json, ttl):
        self.tip[contact_uuid] = (info_json, ttl)
        return True

    def delete_transfer_in_progress(self, contact_uuid):
        self.tip.pop(contact_uuid, None)

    def get_retry_val(self, flow, run):
        return self.retry_val


class FakeSession:
    def __init__(self):
        self.committed = 0

    def commit(self):
        self.committed += 1

    def close(self):
        pass


def _params(**over):
    base = {
        "auth_id": "TESTORG0AUTH0ID000000",
        "contact_uuid": "c-123",
        "contact_channel": rc.CHANNEL_CALL,
        "from": "+15550001111",
        "flow_run_uuid": "pr-1",
        "queue_and_route_run_uuid": "qnr-1",
        "conversation_number": "9990001",
        "to": "to-app",
        "queue_config": {"queue_capacity": 10, "queue_priority": 0, "dequeue_order": "first-in-first-out"},
        "queue_and_route_config": {
            "queue": _QUEUE,
            "routing": rc.SIMULTANEOUS_ROUTING,
            "retry_limit": 3,
            "routing_teams": ["t-1"],
        },
    }
    base.update(over)
    return IncomingContactParams.model_validate(base)


@pytest.fixture()
def patched(monkeypatch):
    """Patch the data layer + get_agents so handle_incoming_contact runs fully offline. Returns a dict of
    knobs the tests tweak (capacity, agents, account-read returns, recorders)."""
    knobs = {
        "capacity": 0,
        "max": 10,
        "agents": ["sip:agent-1"],
        "agents_err": None,
        "auth_token": ("tok", False, None),
        "email_assoc": (None, None),
        "final_agents": ([], None),
        "created_id": 77,
        "create_err": None,
        "node_updates": [],
        "queue_status_updates": [],
        "deleted_aqe": [],
        "child_payloads": [],
        "child_err": None,
        "invite_calls": [],
        "events": [],
    }
    fake = FakeRoutingRedis()
    knobs["redis"] = fake
    monkeypatch.setattr(ic, "SessionLocal", lambda: FakeSession())
    monkeypatch.setattr(ic.aq, "get_queue_capacity", lambda db, q, ch: knobs["capacity"])

    def _create(db, entry, teams=None, skills_filter=None, agents=None):
        if knobs["create_err"]:
            raise RuntimeError(knobs["create_err"])
        knobs["created_entry"] = {
            "status": entry.status,
            "teams": teams,
            "agents": agents,
            "queue_channel": entry.queue_channel,
        }
        return knobs["created_id"]

    monkeypatch.setattr(ic.aq, "create_active_queue_entry", _create)
    monkeypatch.setattr(
        ic.ar, "get_agent_details_for_email_or_aom_id", lambda emails, uuids, auth: knobs["final_agents"]
    )
    monkeypatch.setattr(
        ic.ar, "get_organisation_auth_token_and_record_value", lambda auth, inbound=True: knobs["auth_token"]
    )

    # invitation layer: get_agents + the active_queue writes used by get_routed_agents / dispatch
    monkeypatch.setattr(inv.aq, "update_queue_status", lambda db, c, q, s: knobs["queue_status_updates"].append(s))
    monkeypatch.setattr(inv.aq, "delete_active_queue_entry", lambda db, c, q, s: knobs["deleted_aqe"].append(s))

    def _child(payload):
        knobs["child_payloads"].append(payload)
        return knobs["child_err"]

    knobs["create_child"] = _child

    def _invite_workflow(params, agents, is_transfer, db, rr):
        knobs["invite_calls"].append(list(agents))
        return

    import hs.voice.intake as voice_intake

    monkeypatch.setattr(voice_intake, "handle_incoming_call", lambda params: "<Response>x</Response>")
    monkeypatch.setattr(ic, "RoutingRedis", lambda: knobs["redis"])
    monkeypatch.setattr(ic, "create_qnr_child_conversation", lambda payload: knobs["create_child"](payload))
    monkeypatch.setattr(ic, "emit_by_number", lambda *a: knobs["events"].append(a))
    return knobs


def _run(params, knobs):
    return ic.handle_incoming_contact(params)


def test_simultaneous_zeroes_retry_limit_in_cached_config(patched):
    _run(_params(), patched)
    qarc_json, ttl = patched["redis"].sets[("QARC", "pr-1", "qnr-1")]
    assert json.loads(qarc_json)["retry_limit"] == 0  # Simultaneous → retry_limit=0
    assert ttl == rc.get_cache_duration_for_contact_channel_session_flow(rc.CHANNEL_CALL) * 3600


def test_an_enqueued_call_is_announced_as_queued_once_its_conversation_exists(patched):
    _run(_params(), patched)
    assert patched["events"] == [
        ("conversation.queued", "9990001", {"queue_id": _QUEUE, "from": "+15550001111", "to": "to-app"})
    ]
    patched["events"].clear()
    patched["child_err"] = "child conversation creation failed"
    _run(_params(), patched)
    assert patched["events"] == []


def test_child_create_failure_returns_enqueue_failed(patched):
    patched["child_err"] = "child conversation creation failed"
    _msg, state, _aqid, err = _run(_params(), patched)
    assert state == rc.QNR_STATE_ENQUEUE_FAILED
    assert err == "child conversation creation failed"


def test_a_refusal_after_the_queue_row_was_written_leaves_a_job_that_closes_it(patched, monkeypatch):
    from contextlib import nullcontext

    from hs import jobs

    scheduled = []
    monkeypatch.setattr(jobs, "schedule", lambda kind, payload, **kw: scheduled.append((kind, payload)))
    patched["child_err"] = "child conversation creation failed"
    assert (_run(_params(), patched)[1], patched["deleted_aqe"]) == (rc.QNR_STATE_ENQUEUE_FAILED, [])
    patched["child_err"], patched["create_err"] = None, "boom-insert"  # refused before any row: nothing to close
    assert _run(_params(), patched)[1] == rc.QNR_STATE_ENQUEUE_FAILED
    [(kind, payload)] = scheduled
    monkeypatch.setattr(ic, "SessionLocal", SimpleNamespace(begin=nullcontext))
    jobs._handlers[kind][0](payload)
    assert patched["deleted_aqe"] == ["FAILED"]


def test_child_payload_omits_contact_uuid_when_parent_equals_contact(patched):
    _run(_params(parent_contact_uuid="c-123"), patched)  # parent == contact → omit contact_uuid
    payload = patched["child_payloads"][0]
    assert "contact_uuid" not in payload
    assert payload["generation_type"] == "qnr_generated"
    assert payload["channel"] == rc.CHANNEL_CALL and payload["sender_id"] == "to-app"


def test_child_payload_includes_contact_uuid_and_transfer_gen_for_queue_transfer(patched):
    _run(_params(parent_contact_uuid="parent-9", is_queue_transfer=True), patched)
    payload = patched["child_payloads"][0]
    assert payload["contact_uuid"] == "c-123"  # parent != contact → include
    assert payload["generation_type"] == "transfer_qnr_generated"
    assert "c-123" in patched["redis"].tip  # set_transfer_in_progress (flow)


# ======================================================================================
# Dispatch (invite_agents_to_channel) — the per-channel state mapping
# ======================================================================================
def test_dispatch_unknown_channel_deletes_aqe_and_assignment_failed(patched):
    # an unknown channel falls through to the default branch
    p = _params(contact_channel="http")
    _msg, state, err = inv.invite_agents_to_channel(p, FakeSession(), patched["redis"])
    assert (state, err) == (rc.QNR_STATE_ASSIGNMENT_FAILED, "invalid Contact Channel")
    assert rc.CONTACT_STATE_FAILED in patched["deleted_aqe"]  # default branch delete_active_queue_entry


def test_dispatch_call_channel_returns_xml_msg(patched, monkeypatch):
    """The Call branch returns handle_incoming_call's XML as msg with state=""/err=None."""
    p = _params(contact_channel=rc.CHANNEL_CALL)
    patch_invite_seams(monkeypatch, handle_incoming_call=lambda params: "<Response>x</Response>")
    msg, state, err = inv.invite_agents_to_channel(p, FakeSession(), patched["redis"])
    assert (msg, state, err) == ("<Response>x</Response>", "", None)
    assert patched["deleted_aqe"] == []


def test_dispatch_call_channel_error_deletes_aqe_and_returns_nil_err(patched, monkeypatch):
    """handle_incoming_call error → NODE_UPDATED(enqueue_failed) +
    delete_active_queue_entry(Failed); the error is REASSIGNED to the delete's result → a clean delete yields
    ("", enqueue_failed, None) — the controller then answers 200 with msg="" (preserved bug)."""
    p = _params(contact_channel=rc.CHANNEL_CALL)

    def _boom(params):
        raise RuntimeError("dispatch failed")

    patch_invite_seams(monkeypatch, handle_incoming_call=_boom)
    msg, state, err = inv.invite_agents_to_channel(p, FakeSession(), patched["redis"])
    assert (msg, state, err) == ("", rc.QNR_STATE_ENQUEUE_FAILED, None)  # err is None: the bug
    assert patched["deleted_aqe"] == [rc.CONTACT_STATE_FAILED]


def test_dispatch_call_channel_error_with_delete_failure_propagates_err(patched, monkeypatch):
    p = _params(contact_channel=rc.CHANNEL_CALL)

    def _boom(params):
        raise RuntimeError("dispatch failed")

    patch_invite_seams(monkeypatch, handle_incoming_call=_boom)

    def _del_boom(db, c, q, s):
        raise RuntimeError("delete blew up")

    monkeypatch.setattr(inv.aq, "delete_active_queue_entry", _del_boom)
    msg, state, err = inv.invite_agents_to_channel(p, FakeSession(), patched["redis"])
    assert (msg, state) == ("", rc.QNR_STATE_ENQUEUE_FAILED)
    assert "delete blew up" in err


def test_incoming_contact_params_coerces_nulls_to_defaults():
    # null takes the field default; pydantic would reject null on non-Optional fields,
    # The _LenientModel null-coercion must drop nulls -> field defaults, recursively into nested models.
    p = IncomingContactParams.model_validate(
        {
            "contact_uuid": "C",
            "contact_channel": "call",
            "is_queue_transfer": None,
            "flow_name": None,
            "queue_and_route_config": {
                "routing": "Availability",
                "sticky_routing_options": None,
                "routing_teams": None,
            },
            "queue_config": {"queue_priority": 5, "prompt": None},
        }
    )
    assert p.flow_name == "" and p.is_queue_transfer is False  # top-level null -> default
    assert p.queue_and_route_config.sticky_routing_type == ""  # NESTED null -> default
    assert p.queue_and_route_config.routing_teams == []  # null list -> []
    assert p.queue_config.queue_priority == 5  # non-null untouched


def _fake_instance(cls):
    return type("CH", (), {"insert_conversation_event": lambda self, *a, **k: False})()


# ======================================================================================
# Helper-function forks (offline, no SessionLocal) — the error/edge paths the decision-tree
# tests don't reach. Reuses _params() / FakeRoutingRedis / FakeSession above.
# ======================================================================================
def test_is_valid_uuid_true_false_and_none():
    # drives the routing_agents email-vs-uuid split.
    assert ic.is_valid_uuid("550e8400-e29b-41d4-a716-446655440000") is True
    assert ic.is_valid_uuid("agent@example.com") is False
    assert ic.is_valid_uuid(None) is False


def test_check_queue_capacity_fetch_exception_returns_error(monkeypatch):
    # A capacity-read failure is a soft error, not a crash.
    monkeypatch.setattr(ic.aq, "get_queue_capacity", lambda db, q, ch: (_ for _ in ()).throw(RuntimeError("db down")))
    assert ic.check_queue_capacity(_params(), db=object()) == "unable to fetch queue capacity"


def test_check_queue_capacity_under_limit_is_ok(monkeypatch):
    monkeypatch.setattr(ic.aq, "get_queue_capacity", lambda db, q, ch: 3)  # 3 < max 10
    assert ic.check_queue_capacity(_params(), db=object()) is None


def test_set_queue_props_route_save_false_returns_error():
    # A False from the route-config write surfaces as an error.
    rr = FakeRoutingRedis()
    rr.set_queue_and_route_config = lambda *a: False
    assert ic.set_queue_props_in_cache(_params(), rr) == "Could not save queue and route"


def test_set_queue_props_queue_save_false_returns_error():
    # A False from the queue-config write surfaces as an error.
    rr = FakeRoutingRedis()
    rr.set_queue_config = lambda *a: False
    assert ic.set_queue_props_in_cache(_params(), rr) == "Could not save queue"


def test_set_queue_props_exception_returns_error():
    # Any cache exception maps to the generic save error.
    rr = FakeRoutingRedis()

    def _boom(*a):
        raise RuntimeError("redis down")

    rr.set_queue_and_route_config = _boom
    assert ic.set_queue_props_in_cache(_params(), rr) == "could not save queue properties"


def test_create_active_queue_entry_insert_exception_returns_error(monkeypatch):
    # A failed insert returns (0, error), not a raise.

    def _boom(*a, **k):
        raise RuntimeError("insert failed")

    monkeypatch.setattr(ic.aq, "create_active_queue_entry", _boom)
    aqid, err = ic.create_active_queue_entry(_params(), db=object())
    assert (aqid, err) == (0, "could not save queue entry")


def test_store_contact_params_cache_exception_returns_error():
    # A redis write failure is a soft error.
    rr = FakeRoutingRedis()

    def _boom(*a):
        raise RuntimeError("redis down")

    rr.store_contact_params = _boom
    assert ic.store_contact_params_cache(_params(), "tok", rr) == "unable to store contact params in cache"


# ---- handle_incoming_contact branches the decision-tree tests miss ----------------------
_UUID = "550e8400-e29b-41d4-a716-446655440000"


def test_routing_agents_split_into_emails_and_uuids(patched, monkeypatch):
    # routing_agents are partitioned into emails vs aom uuids
    # before resolution.
    seen = {}
    monkeypatch.setattr(
        ic.ar,
        "get_agent_details_for_email_or_aom_id",
        lambda emails, uuids, auth: seen.update(emails=emails, uuids=uuids) or (["sip:x"], None),
    )
    p = _params(
        queue_and_route_config={
            "queue": _QUEUE,
            "routing": "Availability",
            "routing_agents": [_UUID, "agent@example.com"],
        }
    )
    _msg, _state, _aqid, err = _run(p, patched)
    assert err is None
    assert seen["uuids"] == [_UUID] and seen["emails"] == ["agent@example.com"]


def test_routing_agents_resolution_error_enqueue_failed(patched, monkeypatch):
    # A resolution error aborts enqueue.
    monkeypatch.setattr(ic.ar, "get_agent_details_for_email_or_aom_id", lambda e, u, a: (None, "agent lookup failed"))
    p = _params(queue_and_route_config={"queue": _QUEUE, "routing": "Availability", "routing_agents": [_UUID]})
    _msg, state, _aqid, err = _run(p, patched)
    assert (state, err) == (rc.QNR_STATE_ENQUEUE_FAILED, "agent lookup failed")


def test_handle_set_queue_props_failure_returns_enqueue_failed(patched):
    patched["redis"].set_queue_and_route_config = lambda *a: False
    _msg, state, _aqid, err = _run(_params(), patched)
    assert (state, err) == (rc.QNR_STATE_ENQUEUE_FAILED, "Could not save queue and route")


def test_handle_create_entry_failure_returns_enqueue_failed(patched):
    patched["create_err"] = "boom-insert"
    _msg, state, aqid, err = _run(_params(), patched)
    assert (state, aqid, err) == (rc.QNR_STATE_ENQUEUE_FAILED, 0, "could not save queue entry")


def test_handle_auth_token_failure_returns_enqueue_failed(patched):
    patched["auth_token"] = ("", False, "org token lookup failed")
    _msg, state, _aqid, err = _run(_params(), patched)
    assert (state, err) == (rc.QNR_STATE_ENQUEUE_FAILED, "org token lookup failed")


def test_handle_store_params_failure_returns_enqueue_failed(patched):
    def _boom(*a):
        raise RuntimeError("redis down")

    patched["redis"].store_contact_params = _boom
    _msg, state, _aqid, err = _run(_params(), patched)
    assert (state, err) == (rc.QNR_STATE_ENQUEUE_FAILED, "unable to store contact params in cache")


def test_handle_delete_mapping_exception_returns_enqueue_failed(patched):
    # A mapping-delete failure aborts the enqueue.
    def _boom(c):
        raise RuntimeError("redis down")

    patched["redis"].delete_contact_to_agent_mapping = _boom
    _msg, state, _aqid, err = _run(_params(), patched)
    assert (state, err) == (rc.QNR_STATE_ENQUEUE_FAILED, "unable to delete contact to agent mapping")


def test_handle_transfer_in_progress_set_exception_is_swallowed(patched):
    # A set_transfer_in_progress failure is logged, not fatal.
    def _boom(*a):
        raise RuntimeError("redis down")

    patched["redis"].set_transfer_in_progress = _boom
    _msg, _state, _aqid, err = _run(_params(is_queue_transfer=True), patched)
    assert err is None  # flow continues despite the TIP write failing


def test_handle_delete_transfer_in_progress_exception_is_swallowed(patched):
    # A non-transfer enqueue clears any stale TIP; a failure
    # to clear is logged, not fatal.
    def _boom(c):
        raise RuntimeError("redis down")

    patched["redis"].delete_transfer_in_progress = _boom
    _msg, _state, _aqid, err = _run(_params(), patched)
    assert err is None


def test_one_enqueue_starts_the_room_once(patched, monkeypatch, caplog):
    """Every entry (answer, menu, enqueue URL, hand-off) reaches the room through flow.enqueue: one MPCInitialise
    stamp, one missed-event timer, and no refused second start."""
    stamps, timers = set(), []

    class Stamps:
        def set_mpc_callback_event(self, call_uuid, qnr, event, ts=""):
            fresh = (call_uuid, qnr, event) not in stamps
            stamps.add((call_uuid, qnr, event))
            return None if fresh else vc.VALUE_EXISTS_ERROR

    monkeypatch.setattr(vi, "handle_incoming_call", _START_MPC)
    monkeypatch.setattr(vi, "VoiceRedis", Stamps)
    monkeypatch.setattr(vi, "RoutingRedis", lambda: patched["redis"])
    monkeypatch.setattr(vi, "_spawn", lambda *a: None)
    monkeypatch.setattr(vi.jobs, "schedule", lambda kind, payload, **kw: timers.append(kind))
    monkeypatch.setattr(vi.ar, "get_org_for_auth_id", lambda auth: SimpleNamespace(org_uuid="T1"))
    monkeypatch.setattr(vi.ar, "get_organisation_auth_token_and_record_value", lambda auth, inbound: ("t", 0, None))
    monkeypatch.setattr(flow.setup, "get", lambda *a: QueueIn(name="Q").model_dump(mode="json"))
    monkeypatch.setattr(flow, "save", lambda *a, **kw: None)
    call = {"call_uuid": "c-1", "auth_id": "A1", "conversation_number": "9", "run_id": "r", "from": "+1", "to": "+2"}
    assert "<MultiPartyCall" in flow.enqueue(SimpleNamespace(org_id=1), call, _QUEUE, [])
    assert len(stamps) == 1 and timers == ["missed_mpc_event"] and "unable to set initiate mpc time" not in caplog.text
