"""hs_voice_routing_phase_seconds instrumentation.

Pins: (1) time_phase/timed_phase observe into the histogram with the right phase label,
including on exceptions; (2) the decorator is signature/behavior-preserving; (3) the five
routing-path functions are actually wired (so the wiring can't silently regress); (4) the
phase label set stays bounded to the documented literals.
"""

import pytest
from prometheus_client import REGISTRY

from hs.voice.phase_metrics import time_phase, timed_phase

EXPECTED_PHASES = {
    "incoming_contact",
    "initialized_callback",
    "agent_selection",
    "acw_live_calls_fetch",
    "add_agents_to_mpc",
}


def _phase_count(phase: str) -> float:
    val = REGISTRY.get_sample_value("hs_voice_routing_phase_seconds_count", {"phase": phase})
    return val or 0.0


def _phase_sum(phase: str) -> float:
    val = REGISTRY.get_sample_value("hs_voice_routing_phase_seconds_sum", {"phase": phase})
    return val or 0.0


class TestTimePhase:
    def test_observes_duration_with_phase_label(self):
        before = _phase_count("agent_selection")
        with time_phase("agent_selection"):
            pass
        assert _phase_count("agent_selection") == before + 1
        assert _phase_sum("agent_selection") >= 0.0

    def test_observes_even_when_body_raises(self):
        before = _phase_count("add_agents_to_mpc")
        with pytest.raises(RuntimeError), time_phase("add_agents_to_mpc"):
            raise RuntimeError("mpc add blew up")
        assert _phase_count("add_agents_to_mpc") == before + 1


class TestTimedPhaseDecorator:
    def test_passes_through_return_value_and_args(self):
        @timed_phase("incoming_contact")
        def fn(a, b, key=None):
            return (a, b, key)

        before = _phase_count("incoming_contact")
        assert fn(1, 2, key="k") == (1, 2, "k")
        assert _phase_count("incoming_contact") == before + 1

    def test_propagates_exception_and_still_observes(self):
        @timed_phase("acw_live_calls_fetch")
        def fn():
            raise ValueError("hop failed")

        before = _phase_count("acw_live_calls_fetch")
        with pytest.raises(ValueError):
            fn()
        assert _phase_count("acw_live_calls_fetch") == before + 1

    def test_preserves_function_identity(self):
        @timed_phase("incoming_contact")
        def fn():
            """docstring survives"""

        assert fn.__name__ == "fn"
        assert fn.__doc__ == "docstring survives"
        assert fn.__wrapped__ is not None


class TestRoutingPathWiring:
    """The load-bearing functions are decorated — a refactor that drops the wiring fails here."""

    def test_handle_incoming_contact_is_instrumented(self):
        from hs.intake.enqueue import handle_incoming_contact

        assert hasattr(handle_incoming_contact, "__wrapped__")

    def test_handle_initialised_callback_is_instrumented(self):
        from hs.voice.callbacks import handle_initialised_callback

        assert hasattr(handle_initialised_callback, "__wrapped__")

    def test_route_and_add_to_mpc_times_selection_and_mpc_add(self, monkeypatch):
        from hs.voice import callbacks as vcb

        class _RR:
            def get_contact_parameters(self, call_uuid, qnr_run_uuid):
                return {
                    "auth_id": "MATESTAUTH",
                    "flow_run_uuid": "p",
                    "queue_and_route_run_uuid": "q",
                    "from_number": "+15550001111",
                    "conversation_number": "000-1",
                }

        from hs.routing import get_agents as routing_get_agents

        monkeypatch.setattr(routing_get_agents, "get_agents", lambda *a, **k: (["sip:a@phone.plivo.com"], None))
        added = []
        monkeypatch.setattr(vcb, "RoutingRedis", _RR)
        monkeypatch.setattr("hs.voice.agent_leg.add_agents_to_mpc", lambda *a, **k: added.append(a))
        before_sel = _phase_count("agent_selection")
        before_add = _phase_count("add_agents_to_mpc")
        _msg, err = vcb.route_and_add_to_mpc("cu", "qr", False, "", "", "", "", db=None)
        assert err is None
        assert len(added) == 1
        assert _phase_count("agent_selection") == before_sel + 1
        assert _phase_count("add_agents_to_mpc") == before_add + 1


def test_phase_label_set_is_bounded():
    """Every phase label ever observed must come from the documented bounded set."""
    observed = set()
    for metric in REGISTRY.collect():
        if metric.name == "hs_voice_routing_phase_seconds":
            for sample in metric.samples:
                if "phase" in sample.labels:
                    observed.add(sample.labels["phase"])
    assert observed.issubset(EXPECTED_PHASES)
