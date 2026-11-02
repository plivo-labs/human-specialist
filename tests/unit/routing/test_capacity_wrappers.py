"""Capacity wrapper return-code interpretation tables (offline).

Each wrapper maps the SAME update_agent_capacity codes (0/-1/-2/-3/-4) to a DIFFERENT error/success
per call shape. A drift here leaks capacity or spuriously
fails routing. A fake cache feeds each code; no Redis needed.
"""

import pytest

from hs.routing.redis_store import RoutingRedis


class _FakeCache:
    """Returns a canned EVALSHA reply so the wrapper's code→error mapping is tested in isolation."""

    def __init__(self, code):
        self._code = code

    def eval_script(self, src, keys=None, args=None):
        return self._code


class _RaisingCache:
    """Raises on eval (Redis-down) so the wrapper's eval-error → error return is tested."""

    def eval_script(self, src, keys=None, args=None):
        raise RuntimeError("NOSCRIPT / connection refused")


def _rr(code):
    return RoutingRedis(cache=_FakeCache(code))


@pytest.mark.parametrize(
    "code,expect_err",
    [
        ("0", None),
        ("-1", None),
        ("-3", None),
        ("-4", None),
        ("-2", "error in increasing agent capacity"),  # ONLY -2 is an error on acquire
    ],
)
def test_increase_agent_capacity_only_minus2_is_error(code, expect_err):
    got_code, err = _rr(code).increase_agent_capacity("o", "a", "chat", 5, "c1")
    assert got_code == int(code)
    assert err == expect_err


def test_decrease_empty_conversation_errors_before_eval():
    # The conversation_id=='' guard fires BEFORE any eval (load-bearing); the exact error string.
    class _Boom:
        def eval_script(self, *a, **k):
            raise AssertionError("must not eval when conversation_id is empty")

    assert (
        RoutingRedis(cache=_Boom()).decrease_agents_capacity("o", "a", "chat", "")
        == "empty conversation id, failed to decrease capacity"
    )


@pytest.mark.parametrize(
    "code,expect",
    [
        ("-1", "failed to decrease capacity as contact not found"),
        ("-4", "key not found while decreasing capacity"),
        ("-3", None),
        ("-2", None),
        ("0", None),  # -3 is the expected release success; -2/0 also success
    ],
)
def test_decrease_agents_capacity_code_table(code, expect):
    assert _rr(code).decrease_agents_capacity("o", "a", "chat", "c1") == expect


@pytest.mark.parametrize(
    "code,expect",
    [
        ("-1", "failed to update capacity as old contact not found"),
        ("-2", "failed to update capacity as capacity already occupied"),
        ("-4", "failed to update capacity as key not present"),
        ("0", None),
        ("-3", None),  # move success
    ],
)
def test_update_agents_capacity_move_code_table(code, expect):
    assert _rr(code).update_agents_capacity("o", "a", "chat", "old", "new", 5) == expect


def test_eval_error_maps_to_an_error_return():
    # An EVALSHA exception (Redis down) → an error return: increase → (-2, err),
    # decrease/move → err string (NOT a raised exception, so the route maps to 400 not 500).
    r = RoutingRedis(cache=_RaisingCache())
    code, err = r.increase_agent_capacity("o", "a", "chat", 5, "c1")
    assert code == -2 and err and "NOSCRIPT" in err
    assert "NOSCRIPT" in r.decrease_agents_capacity("o", "a", "chat", "c1")
    assert "NOSCRIPT" in r.update_agents_capacity("o", "a", "chat", "old", "new", 5)
