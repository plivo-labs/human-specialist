"""get_contact reverse-router orchestration + QPWT recompute (offline).

The reverse router (hs/routing/reverse.get_contact) is pure orchestration over leaf reads, so it
is tested with the leaf account reads + active_queue pick monkeypatched and a fake RoutingRedis — pinning
the control flow (the retry:=3 loop, skip-self via IA, requeue-on-miss + recompute, the
Call/conference gate, and the hard-abort error propagation) without a DB. Plus the QPWT
get/set/recompute-blank-overwrite Redis behaviour against a FakeCache.
"""

import json

import tests.conftest  # noqa: F401 — eager data_adapter imports (AbstractConcreteBase registry)
from hs.routing import account_read as ar
from hs.routing import active_queue as aq
from hs.routing import constants as rc
from hs.routing import reverse as rev
from hs.routing.redis_store import QUEUE_POSITION_WAIT_TIME_EXPIRY_HOURS, RoutingRedis

AGENT = "sip:agent@example"


class FakeContact:
    def __init__(
        self, contact_uuid="c1", qnr_run_uuid="qnr1", flow_run_uuid="flow1", queue_channel=rc.CHANNEL_TASK, version=1
    ):
        self.contact_uuid = contact_uuid
        self.qnr_run_uuid = qnr_run_uuid
        self.flow_run_uuid = flow_run_uuid
        self.queue_channel = queue_channel
        self.version = version


class FakeRR:
    def __init__(self, invited=None, contact_params={}):  # noqa: B006 — read-only; None is a cache miss
        self._invited = list(invited or [])
        self._contact_params = contact_params
        self.recompute_calls = []

    def get_invited_agents(self, flow, run):
        return list(self._invited)

    def get_contact_parameters(self, contact_uuid, qnr_run_uuid):
        return self._contact_params

    def set_recompute_flag_for_queue_position_calculation(self, node_uuid, queue_uuid=""):
        self.recompute_calls.append((node_uuid, queue_uuid))


def _patch_account(monkeypatch, *, available=True, teams=(), aom="aom-uuid", skills=()):
    monkeypatch.setattr(ar, "check_agent_availability", lambda a: available)
    monkeypatch.setattr(ar, "get_agent_teams", lambda a, qp: None if teams is None else list(teams))
    monkeypatch.setattr(ar, "get_aom_for_sip_endpoint", lambda a: aom)
    monkeypatch.setattr(ar, "get_agent_skills", lambda a: None if skills is None else list(skills))


# =====================================================================================
# pre-loop gates
# =====================================================================================
def test_not_available(monkeypatch):
    _patch_account(monkeypatch, available=False)
    c, err = rev.get_contact(AGENT, [rc.CHANNEL_TASK], db=None, routing_redis=FakeRR())
    assert c is None and err == "agent is not available"


def test_teams_read_error_aborts(monkeypatch):
    _patch_account(monkeypatch, teams=None)  # get_agent_teams None ⇒ the error path
    c, err = rev.get_contact(AGENT, [rc.CHANNEL_TASK], db=None, routing_redis=FakeRR())
    assert c is None and err == "error fetching agent teams"


def test_aom_missing_aborts(monkeypatch):
    _patch_account(monkeypatch, teams=["T1"], aom=None)
    c, err = rev.get_contact(AGENT, [rc.CHANNEL_TASK], db=None, routing_redis=FakeRR())
    assert c is None and err == "no agent found for the SIP"


# =====================================================================================
# happy path + error propagation
# =====================================================================================
def test_happy_non_call_returns_contact(monkeypatch):
    _patch_account(monkeypatch, teams=["T1"], aom="aom1")
    contact = FakeContact(queue_channel=rc.CHANNEL_TASK)
    monkeypatch.setattr(aq, "get_best_contact", lambda *a, **k: (contact, None))
    monkeypatch.setattr(aq, "check_if_valid_contact", lambda db, e: True)
    c, err = rev.get_contact(AGENT, [rc.CHANNEL_TASK], db=object(), routing_redis=FakeRR(invited=[]))
    assert err is None and c is contact


def test_best_contact_error_aborts_no_retry(monkeypatch):
    _patch_account(monkeypatch, teams=["T1"], aom="aom1")
    calls = {"n": 0}

    def _gbc(*a, **k):
        calls["n"] += 1
        return None, "version conflict: record was updated by another process"

    monkeypatch.setattr(aq, "get_best_contact", _gbc)
    c, err = rev.get_contact(AGENT, [rc.CHANNEL_TASK], db=object(), routing_redis=FakeRR())
    assert c is None and err == "version conflict: record was updated by another process"
    assert calls["n"] == 1  # a get_best_contact error aborts immediately (no retry)


# =====================================================================================
# miss → requeue + recompute → retry exhaustion
# =====================================================================================
def test_self_invited_requeues_and_exhausts(monkeypatch):
    _patch_account(monkeypatch, teams=["T1"], aom="aom1")
    contact = FakeContact()
    monkeypatch.setattr(aq, "get_best_contact", lambda *a, **k: (contact, None))
    monkeypatch.setattr(aq, "check_if_valid_contact", lambda db, e: True)
    requeues = []
    monkeypatch.setattr(aq, "update_queue_status", lambda db, cu, qru, st: requeues.append((cu, qru, st)))
    rr = FakeRR(invited=[AGENT], contact_params={"node_uuid": "N1", "queue_uuid": "Q1"})
    c, err = rev.get_contact(AGENT, [rc.CHANNEL_TASK], db=object(), routing_redis=rr)
    assert c is None and err == "known_error: no valid contact found"
    assert len(requeues) == 3 and all(st == rc.CONTACT_STATE_QUEUED for _, _, st in requeues)
    assert rr.recompute_calls == [("N1", "Q1"), ("N1", "Q1"), ("N1", "Q1")]  # node_uuid from params


def test_invalid_contact_requeues_and_exhausts(monkeypatch):
    _patch_account(monkeypatch, teams=["T1"], aom="aom1")
    monkeypatch.setattr(aq, "get_best_contact", lambda *a, **k: (FakeContact(), None))
    monkeypatch.setattr(aq, "check_if_valid_contact", lambda db, e: False)
    requeues = []
    monkeypatch.setattr(aq, "update_queue_status", lambda db, cu, qru, st: requeues.append(st))
    rr = FakeRR(invited=[])
    c, err = rev.get_contact(AGENT, [rc.CHANNEL_TASK], db=object(), routing_redis=rr)
    assert c is None and err == "known_error: no valid contact found" and len(requeues) == 3


def test_recompute_uses_empty_when_no_params(monkeypatch):
    _patch_account(monkeypatch, teams=["T1"], aom="aom1")
    monkeypatch.setattr(aq, "get_best_contact", lambda *a, **k: (FakeContact(), None))
    monkeypatch.setattr(aq, "check_if_valid_contact", lambda db, e: False)
    monkeypatch.setattr(aq, "update_queue_status", lambda *a, **k: None)
    rr = FakeRR(invited=[], contact_params=None)  # get_contact_parameters miss → "" node/queue
    rev.get_contact(AGENT, [rc.CHANNEL_TASK], db=object(), routing_redis=rr)
    assert rr.recompute_calls == [("", ""), ("", ""), ("", "")]


# =====================================================================================
# Call-channel conference gate
# =====================================================================================
def test_contact_without_params_is_requeued_not_returned(monkeypatch):
    # A claimed entry whose CONTACT_PARAM is gone can never be offered: it goes back to QUEUED, not stranded
    # IN_REVERSE_ROUTING with the error.
    _patch_account(monkeypatch, teams=["T1"], aom="aom1")
    monkeypatch.setattr(aq, "get_best_contact", lambda *a, **k: (FakeContact(queue_channel=rc.CHANNEL_CALL), None))
    monkeypatch.setattr(aq, "check_if_valid_contact", lambda db, e: True)
    requeues = []
    monkeypatch.setattr(aq, "update_queue_status", lambda db, cu, qru, st: requeues.append(st))
    c, err = rev.get_contact(
        AGENT,
        [rc.CHANNEL_CALL],
        db=object(),
        routing_redis=FakeRR(contact_params=None),
        is_conference_active=lambda q: True,
    )
    assert c is None and err == "known_error: no valid contact found"
    assert requeues == [rc.CONTACT_STATE_QUEUED] * 3


def test_call_conference_active_returns(monkeypatch):
    _patch_account(monkeypatch, teams=["T1"], aom="aom1")
    contact = FakeContact(queue_channel=rc.CHANNEL_CALL)
    monkeypatch.setattr(aq, "get_best_contact", lambda *a, **k: (contact, None))
    monkeypatch.setattr(aq, "check_if_valid_contact", lambda db, e: True)
    c, err = rev.get_contact(
        AGENT, [rc.CHANNEL_CALL], db=object(), routing_redis=FakeRR(invited=[]), is_conference_active=lambda q: True
    )
    assert err is None and c is contact


def test_call_conference_inactive_requeues(monkeypatch):
    _patch_account(monkeypatch, teams=["T1"], aom="aom1")
    contact = FakeContact(queue_channel=rc.CHANNEL_CALL)
    monkeypatch.setattr(aq, "get_best_contact", lambda *a, **k: (contact, None))
    monkeypatch.setattr(aq, "check_if_valid_contact", lambda db, e: True)
    requeues = []
    monkeypatch.setattr(aq, "update_queue_status", lambda db, cu, qru, st: requeues.append(st))
    c, err = rev.get_contact(
        AGENT, [rc.CHANNEL_CALL], db=object(), routing_redis=FakeRR(invited=[]), is_conference_active=lambda q: False
    )
    assert c is None and err == "known_error: no valid contact found" and len(requeues) == 3


def test_call_default_conference_fetcher_reads_activeconference(monkeypatch):
    # The default is_conference_active is REAL — VoiceRedis.is_conference_active, whose read error
    # counts as inactive; an active conference returns the
    # Call contact through the DEFAULT fetcher (no injection).
    from hs.voice.redis_store import VoiceRedis

    _patch_account(monkeypatch, teams=["T1"], aom="aom1")
    contact = FakeContact(queue_channel=rc.CHANNEL_CALL)
    monkeypatch.setattr(aq, "get_best_contact", lambda *a, **k: (contact, None))
    monkeypatch.setattr(aq, "check_if_valid_contact", lambda db, e: True)
    seen = {}

    def fake_is_active(self, qnr):
        seen["qnr"] = qnr
        return True, None

    monkeypatch.setattr(VoiceRedis, "is_conference_active", fake_is_active)
    monkeypatch.setattr(VoiceRedis, "__init__", lambda self, cache=None: None)
    c, err = rev.get_contact(AGENT, [rc.CHANNEL_CALL], db=object(), routing_redis=FakeRR(invited=[]))
    assert err is None and c is contact and seen["qnr"] == contact.qnr_run_uuid


# =====================================================================================
# QPWT recompute blank-overwrite + compact JSON (FakeCache)
# =====================================================================================
class FakeCache:
    def __init__(self, strings=None):
        self.strings = dict(strings or {})

    def get(self, key):
        return self.strings.get(key)

    def set(self, key, value, expiry_in_seconds=None, nx=False):
        self.strings[key] = value
        return True


def test_qpwt_recompute_blank_overwrite_discards_cached():
    existing = json.dumps(
        {"estimated_wait_time_minutes": 5, "estimated_wait_time_seconds": 30, "queue_position": 4, "recompute": False}
    )
    cache = FakeCache(strings={"QPWT:node1": existing})
    RoutingRedis(cache=cache).set_recompute_flag_for_queue_position_calculation("node1", "q1")
    stored = json.loads(cache.strings["QPWT:node1"])
    assert stored == {
        "estimated_wait_time_minutes": 0,
        "estimated_wait_time_seconds": 0,
        "queue_position": 0,
        "recompute": True,
    }  # cached minutes/position discarded


def test_qpwt_recompute_noop_on_miss():
    cache = FakeCache()  # no existing QPWT value
    RoutingRedis(cache=cache).set_recompute_flag_for_queue_position_calculation("nodeX")
    assert "QPWT:nodeX" not in cache.strings  # the flag is stamped only over an existing value


def test_qpwt_set_compact_json_field_order():
    cache = FakeCache()
    data = {"estimated_wait_time_minutes": 1, "estimated_wait_time_seconds": 2, "queue_position": 3, "recompute": True}
    RoutingRedis(cache=cache).set_queue_position_and_wait_time("n", data, 3600)
    # compact (no spaces) + declaration field order
    assert cache.strings["QPWT:n"] == (
        '{"estimated_wait_time_minutes":1,"estimated_wait_time_seconds":2,"queue_position":3,"recompute":true}'
    )


def test_qpwt_expiry_constant():
    assert QUEUE_POSITION_WAIT_TIME_EXPIRY_HOURS == 25
