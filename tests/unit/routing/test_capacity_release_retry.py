"""Capacity-release retry + alert.

On conversation close HS releases agent capacity ONLY inline/synchronously via
`clear_capacity` → `rr.decrease_agents_capacity`. A transient failure there would strand
capacity (agent stays "busy", dropping the available-agent count) with no retry.

The code wraps that inline release in `_release_capacity_with_retry`: bounded retries
with short backoff, and a loud structured WARNING (org/agent/conversation/channel ids) on
final failure so stranded capacity is alertable. These pin the three behaviours:
  - success first try  → no retry, no sleep, no warning (happy-path preserved),
  - fail-then-succeed  → retried, eventually None, no warning,
  - always-fails       → retried N times, WARNING logged with the ids.
"""

import logging

import pytest

import tests.conftest  # noqa: F401 — eager data_adapter imports
from hs.routing import capacity as rcap

_AUTH = "TESTORG0AUTH0ID000000"
_AGENT = "sip:agent@org"
_CHANNEL = "chat"
_CONV = "CN123"


class FakeRR:
    """Scripts a sequence of decrease_agents_capacity results (str = error, None = success).

    Each call pops the next scripted result; if a result is an Exception instance it is raised
    (to exercise the defensive try/except path). Records every call's args.
    """

    def __init__(self, results):
        self._results = list(results)
        self.calls = []

    def decrease_agents_capacity(self, auth_id, agent, channel, conversation_id):
        self.calls.append((auth_id, agent, channel, conversation_id))
        result = self._results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """Keep the unit test fast/deterministic — the backoff sleep is behavioural noise here."""
    monkeypatch.setattr(rcap.time, "sleep", lambda *_a, **_k: None)


def _warnings(caplog):
    return [
        r for r in caplog.records if r.levelno == logging.WARNING and "_release_capacity_with_retry" in r.getMessage()
    ]


# ----------------------------------------------------------------- success first try
def test_release_succeeds_first_try_no_retry_no_warning(caplog):
    rr = FakeRR([None])
    with caplog.at_level(logging.DEBUG, logger="human-specialist"):
        assert rcap._release_capacity_with_retry(rr, _AUTH, _AGENT, _CHANNEL, _CONV) is None
    # Exactly one release attempt, with the ids passed through verbatim.
    assert rr.calls == [(_AUTH, _AGENT, _CHANNEL, _CONV)]
    assert _warnings(caplog) == []


def test_release_succeeds_first_try_does_not_sleep(monkeypatch):
    slept = []
    monkeypatch.setattr(rcap.time, "sleep", lambda s: slept.append(s))
    rr = FakeRR([None])
    assert rcap._release_capacity_with_retry(rr, _AUTH, _AGENT, _CHANNEL, _CONV) is None
    assert slept == []  # happy path never backs off


# ----------------------------------------------------------------- transient → self-heals
def test_release_fails_then_succeeds_is_retried_no_warning(caplog):
    rr = FakeRR(["transient redis blip", None])
    with caplog.at_level(logging.DEBUG, logger="human-specialist"):
        assert rcap._release_capacity_with_retry(rr, _AUTH, _AGENT, _CHANNEL, _CONV) is None
    assert len(rr.calls) == 2  # retried once, then succeeded
    assert _warnings(caplog) == []  # self-healed → not alerted


def test_release_raises_then_succeeds_is_retried(caplog):
    rr = FakeRR([RuntimeError("NOSCRIPT / connection refused"), None])
    with caplog.at_level(logging.DEBUG, logger="human-specialist"):
        assert rcap._release_capacity_with_retry(rr, _AUTH, _AGENT, _CHANNEL, _CONV) is None
    assert len(rr.calls) == 2
    assert _warnings(caplog) == []


# ----------------------------------------------------------------- persistent → loud alert
def test_release_always_fails_retries_n_times_and_warns(caplog):
    rr = FakeRR(["boom"] * rcap._RELEASE_MAX_ATTEMPTS)
    with caplog.at_level(logging.DEBUG, logger="human-specialist"):
        err = rcap._release_capacity_with_retry(rr, _AUTH, _AGENT, _CHANNEL, _CONV)
    assert err == "boom"  # the last error is surfaced to the caller
    assert len(rr.calls) == rcap._RELEASE_MAX_ATTEMPTS  # exhausted all attempts
    warns = _warnings(caplog)
    assert len(warns) == 1
    msg = warns[0].getMessage()
    assert "STRANDED CAPACITY" in msg
    # the alert carries every id needed to reconcile by hand
    for ident in (_AUTH, _AGENT, _CHANNEL, _CONV):
        assert ident in msg


def test_release_always_raises_warns_with_exception_text(caplog):
    rr = FakeRR([RuntimeError("redis down")] * rcap._RELEASE_MAX_ATTEMPTS)
    with caplog.at_level(logging.DEBUG, logger="human-specialist"):
        err = rcap._release_capacity_with_retry(rr, _AUTH, _AGENT, _CHANNEL, _CONV)
    assert err == "redis down"
    assert len(rr.calls) == rcap._RELEASE_MAX_ATTEMPTS
    assert len(_warnings(caplog)) == 1
