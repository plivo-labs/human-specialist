"""The offline sweeper's staleness rule: a ping older than 40 s (or none) marks the agent offline."""

import time

import tests.conftest  # noqa: F401 — eager data_adapter imports (AbstractConcreteBase registry)
from hs.admin.heartbeat import _is_ping_stale
from hs.infra.constants import HEARTBEAT_TIME_TO_MARK_OFFLINE

AOM = "aom-1"


# --- the staleness rule (both sides, incl. the exact 40s boundary) -------------------------------


def test_missing_lst_is_stale():
    assert _is_ping_stale(None) is True  # key expired/missing ⇒ offline candidate


def test_non_numeric_lst_is_stale():
    assert _is_ping_stale("garbage") is True  # unparseable ⇒ treat as stale (fail-safe)


def test_lst_exactly_at_40s_boundary_is_stale():
    boundary = str(int(time.time()) - HEARTBEAT_TIME_TO_MARK_OFFLINE)
    assert _is_ping_stale(boundary) is True  # (now - last ping) >= 40s: the boundary itself is stale


def test_lst_just_before_40s_boundary_is_fresh():
    just_before = str(int(time.time()) - (HEARTBEAT_TIME_TO_MARK_OFFLINE - 1))
    assert _is_ping_stale(just_before) is False  # 39s elapsed ⇒ still fresh


def test_recent_lst_is_fresh():
    assert _is_ping_stale(str(int(time.time()))) is False
