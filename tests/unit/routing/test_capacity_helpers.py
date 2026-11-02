"""increase_agent_capacity: the settings-miss, redis-error and success forks."""

from types import SimpleNamespace

import hs.routing.capacity as rcap
import tests.conftest  # noqa: F401 — eager data_adapter imports (AbstractConcreteBase registry)


class FakeRR:
    def __init__(self, increase_ret=("", None)):
        self._increase_ret = increase_ret
        self.increased = []

    def increase_agent_capacity(self, auth, sip, channel, cap, conv):
        self.increased.append((auth, sip, channel, cap, conv))
        return self._increase_ret


def _patch_rr(monkeypatch, fake):
    monkeypatch.setattr(rcap, "RoutingRedis", lambda: fake)


def _org(auth_id="AUTH1"):
    return SimpleNamespace(auth_id=auth_id)


def test_increase_capacity_settings_missing_is_error(monkeypatch):
    monkeypatch.setattr(rcap.ar, "get_org_details_for_agent_sip", lambda s: _org())
    monkeypatch.setattr(rcap.RoutingSettings, "get_org_routing_settings", staticmethod(lambda auth: None))
    msg, err = rcap.increase_agent_capacity("sip:a", "chat", "CN1")
    assert (msg, err) == ("", "error while fetching routing settings")


def test_increase_capacity_redis_error_propagates(monkeypatch):
    monkeypatch.setattr(rcap.ar, "get_org_details_for_agent_sip", lambda s: _org())
    monkeypatch.setattr(
        rcap.RoutingSettings,
        "get_org_routing_settings",
        staticmethod(lambda auth: SimpleNamespace(capacity={"chat": 5})),
    )
    _patch_rr(monkeypatch, FakeRR(increase_ret=("", "redis down")))
    msg, err = rcap.increase_agent_capacity("sip:a", "chat", "CN1")
    assert (msg, err) == ("", "redis down")


def test_increase_capacity_success_passes_org_capacity(monkeypatch):
    monkeypatch.setattr(rcap.ar, "get_org_details_for_agent_sip", lambda s: _org())
    monkeypatch.setattr(
        rcap.RoutingSettings,
        "get_org_routing_settings",
        staticmethod(lambda auth: SimpleNamespace(capacity={"chat": 7})),
    )
    fake = FakeRR(increase_ret=("ok", None))
    _patch_rr(monkeypatch, fake)
    msg, err = rcap.increase_agent_capacity("sip:a", "chat", "CN1")
    assert (msg, err) == ("Successfully increased agent capacity", None)
    assert fake.increased[0] == ("AUTH1", "sip:a", "chat", 7, "CN1")  # org capacity threaded through
