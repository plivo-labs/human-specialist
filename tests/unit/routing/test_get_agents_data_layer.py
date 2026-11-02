"""The get_agents data layer (offline-deterministic foundation coverage).

Pins the four QUEUEANDROUTECONFIG/CONTACTRETRY/IA/CONTACT_PARAM Redis reads + the qnr-config JSON
parser the get_agents assembly consumes. The key REMAPS (sticky_routing_options →
sticky_routing_type, finalRoutingAgents → final_routing_agents, expiry* → sticky_*) are the
landmine — a wrong key silently drops the strategy/sticky-mode and routes wrong, so the parser is
pinned field-by-field. No live stack (a fake Cache injected into RoutingRedis).
"""

import json

import pytest

import tests.conftest  # noqa: F401 — eager data_adapter imports (AbstractConcreteBase registry)
from hs.intake.models import QueueAndRouteConfigModel, SkillFilterModel
from hs.routing import constants as rc
from hs.routing.redis_store import RoutingRedis
from hs.routing.sticky import parse_queue_and_route_config


class FakeCache:
    """Minimal Cache stand-in: scripted string-GET store + hash store, matching the swallow-to-
    None/{} error contract of hs.infra.redis.Cache."""

    def __init__(self, strings=None, hashes=None):
        self.strings = dict(strings or {})  # key -> str
        self.hashes = dict(hashes or {})  # key -> {field: str}

    def get(self, key):
        return self.strings.get(key)

    def hget(self, key, field):
        return self.hashes.get(key, {}).get(field)

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))


def _rr(strings=None, hashes=None):
    return RoutingRedis(cache=FakeCache(strings, hashes))


# ---------------------------------------------------------------------------------------------
# Redis key builders
# ---------------------------------------------------------------------------------------------
def test_key_builders():
    assert rc.queue_and_route_config_key("flow1", "run1") == "QUEUEANDROUTECONFIG:flow1:run1"
    assert rc.contact_retry_key("flow1", "run1") == "CONTACTRETRY:flow1:run1"
    assert rc.invited_agents_key("flow1", "run1") == "IA:flow1:run1"
    assert rc.contact_params_key("contact1") == "CONTACT_PARAM:contact1"


# ---------------------------------------------------------------------------------------------
# get_retry_val — 0 on ANY error/miss/parse-fail
# ---------------------------------------------------------------------------------------------
def test_get_retry_val_present():
    rr = _rr(strings={"CONTACTRETRY:p:r": "3"})
    assert rr.get_retry_val("p", "r") == 3


def test_get_retry_val_missing_is_zero():
    assert _rr().get_retry_val("p", "r") == 0


def test_get_retry_val_garbage_is_zero():
    rr = _rr(strings={"CONTACTRETRY:p:r": "not-an-int"})
    assert rr.get_retry_val("p", "r") == 0


# ---------------------------------------------------------------------------------------------
# get_invited_agents — HGETALL field names (the invited sips); [] on miss/error
# ---------------------------------------------------------------------------------------------
def test_get_invited_agents_returns_field_names():
    rr = _rr(hashes={"IA:p:r": {"sip:a@x": "1", "sip:b@x": "2"}})
    assert set(rr.get_invited_agents("p", "r")) == {"sip:a@x", "sip:b@x"}


def test_get_invited_agents_empty():
    assert _rr().get_invited_agents("p", "r") == []


# ---------------------------------------------------------------------------------------------
# get_contact_parameters — HGET CONTACT_PARAM:<contact> field=qnr_run → dict; None on miss/parse-fail
# ---------------------------------------------------------------------------------------------
def test_get_contact_parameters_parses_json():
    params = {"contactUuid": "c1", "qnrRunUuid": "r1", "channel": "call"}
    rr = _rr(hashes={"CONTACT_PARAM:c1": {"r1": json.dumps(params)}})
    assert rr.get_contact_parameters("c1", "r1") == params


def test_get_contact_parameters_missing_is_none():
    assert _rr().get_contact_parameters("c1", "r1") is None


def test_get_contact_parameters_bad_json_is_none():
    rr = _rr(hashes={"CONTACT_PARAM:c1": {"r1": "{not json"}, "CONTACT_PARAM:c2": {"r1": "[1]"}})
    assert rr.get_contact_parameters("c1", "r1") is None
    assert rr.get_contact_parameters("c2", "r1") is None  # a record is an object


# ---------------------------------------------------------------------------------------------
# get_queue_and_route_config — raw passthrough (the parse is the usecase layer)
# ---------------------------------------------------------------------------------------------
def test_get_queue_and_route_config_raw_passthrough():
    rr = _rr(strings={"QUEUEANDROUTECONFIG:p:r": '{"queue":"q1"}'})
    assert rr.get_queue_and_route_config("p", "r") == '{"queue":"q1"}'
    assert _rr().get_queue_and_route_config("p", "r") is None


# ---------------------------------------------------------------------------------------------
# parse_queue_and_route_config — the key remap is the landmine
# ---------------------------------------------------------------------------------------------
def test_parse_full_config_maps_all_tags():
    raw = json.dumps(
        {
            "queue": "q-uuid",
            "routing_teams": ["t1", "t2"],
            "routing_skills_filter": [{"skill": "sk1", "operator": ">", "value": 3}],
            "routing": "Availability",
            "agent_limit": 5,
            "retry_limit": 2,
            "routing_agents": ["a1"],
            "finalRoutingAgents": ["fa1", "fa2"],
            "sticky_routing_options": "Loosely bind",
            "expiry_time_value": 7,
            "expiry_time_unit": "days",
            "expiry": False,
            "auto_accept_call": {"allow_auto_accept": True, "auto_accept_delay": 10},
        }
    )
    cfg, err = parse_queue_and_route_config(raw)
    assert err is None
    assert cfg.queue == "q-uuid"
    assert cfg.routing_teams == ["t1", "t2"]
    assert cfg.routing_skills_filter == [SkillFilterModel(skill="sk1", operator=">", value=3)]
    assert cfg.routing == "Availability"
    assert cfg.agent_limit == 5 and cfg.retry_limit == 2
    assert cfg.routing_agents == ["a1"]
    assert cfg.final_routing_agents == ["fa1", "fa2"]  # json tag finalRoutingAgents
    assert cfg.sticky_routing_type == "Loosely bind"  # json tag sticky_routing_options
    assert cfg.sticky_routing_expiry is False  # json tag expiry
    assert cfg.sticky_routing_expiry_unit == "days" and cfg.sticky_routing_expiry_time == 7
    assert cfg.allow_auto_accept is True and cfg.auto_accept_delay == 10


def test_parse_defaults_for_missing_fields():
    cfg, err = parse_queue_and_route_config('{"queue":"q1"}')
    assert err is None
    assert cfg.queue == "q1"
    assert cfg.routing == "" and cfg.routing_teams == [] and cfg.final_routing_agents == []
    assert cfg.routing_skills_filter == [] and cfg.agent_limit == 0 and cfg.retry_limit == 0
    assert cfg.sticky_routing_type == "" and cfg.allow_auto_accept is False


def test_parse_empty_or_none_is_not_found():
    # the TWO miss branches: missing key → the cache miss passes through;
    # empty-string VALUE → the not-found error
    cfg, err = parse_queue_and_route_config(None)
    assert cfg is None and err == "not found in cache"
    cfg, err = parse_queue_and_route_config("")
    assert cfg is None and err == "queue routing config not found"


def test_parse_bad_json_returns_error():
    cfg, err = parse_queue_and_route_config("{not json")
    assert cfg is None and err.startswith("queue routing config is not")


def test_parse_non_object_returns_error():
    cfg, err = parse_queue_and_route_config("[1,2,3]")
    assert cfg is None and err.startswith("queue routing config is not")


def test_parse_skills_with_no_operator():
    raw = json.dumps({"queue": "q", "routing_skills_filter": [{"skill": "sk1"}]})
    cfg, err = parse_queue_and_route_config(raw)
    assert err is None
    assert cfg.routing_skills_filter == [SkillFilterModel(skill="sk1", operator="", value=0)]


def test_parse_null_fields_take_their_defaults():
    raw = json.dumps({"queue": "q", "routing_teams": None, "agent_limit": None, "auto_accept_call": None})
    cfg, err = parse_queue_and_route_config(raw)
    assert err is None and cfg.routing_teams == [] and cfg.agent_limit == 0 and cfg.allow_auto_accept is False


def test_config_round_trips_through_the_cached_json():
    cfg = QueueAndRouteConfigModel(queue="q", sticky_routing_type=rc.STICKY_STRICTLY_BIND, final_routing_agents=["a"])
    parsed, err = parse_queue_and_route_config(cfg.to_compact_json())
    assert err is None and parsed == cfg
    assert json.loads(cfg.to_compact_json())["sticky_routing_options"] == rc.STICKY_STRICTLY_BIND


# ---------------------------------------------------------------------------------------------
# get_agent_sip_by_uuid — the no-row / NULL / EMPTY-STRING split . A NULL scan errors
# on no-row + NULL (→ caller 'agent not found'), but reads a non-NULL '' successfully (→ caller
# PROCEEDS); collapsing '' to None would wrongly abort, so the read returns '' for an empty-string row.
# ---------------------------------------------------------------------------------------------
class _FakeResult:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _FakeDB:
    def __init__(self, row):
        self._row = row

    def execute(self, *a, **k):
        return _FakeResult(self._row)

    def close(self):
        pass


@pytest.mark.parametrize(
    "row,expected",
    [
        (None, None),  # no row → None ('agent not found')
        ((None,), None),  # NULL sip → None ('agent not found')
        (("",), ""),  # present non-NULL empty-string → '' (caller PROCEEDS)
        (("sip:x@y",), "sip:x@y"),
    ],
)
def test_get_agent_sip_by_uuid_row_handling(monkeypatch, row, expected):
    from hs.routing import account_read as ar

    monkeypatch.setattr(ar, "SessionLocal", lambda: _FakeDB(row))
    assert ar.get_agent_sip_by_uuid("some-uuid", "MA_TEST") == expected
