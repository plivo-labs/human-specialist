"""The heartbeat's presence edges: the org_event_actions read and the status change they make.

Pins the edge-triggered dedup (a status change only on a real online<->offline transition), "no org config
row -> no change", the reset of the ping count on every offline edge, the retry contract on a failed status
write, the online-edge backgrounding and the read-only raw-SQL reads.
"""

import pytest

import hs.admin.heartbeat as hb
import hs.routing.agent_status_read as asr
import tests.conftest  # noqa: F401 — eager data_adapter imports (AbstractConcreteBase registry)
from hs.admin.heartbeat import HeartbeatReqMeta, presence_edge
from hs.infra.constants import (
    HEARTBEAT_EVENT_AGENT_LOGGED_IN,
    HEARTBEAT_EVENT_AGENT_LOGGED_OUT,
    HEARTBEAT_REASON_INTERNET_CONNECTED,
    HEARTBEAT_REASON_INTERNET_DISCONNECTED,
)

AOM = "aom-1"
ORG = "org-1"


# --- presence_edge: dedup + resolve + reset + notify --------------------------------------------


class _NotifyRecorder:
    def __init__(self):
        self.calls = []

    def __call__(self, agent_id, status_id, reason, reason_for_change=None):
        self.calls.append(
            {"aom_uuid": agent_id, "status_uuid": status_id, "reason": reason, "reason_for_change": reason_for_change}
        )
        return True


def _wire(monkeypatch, *, available, status_uuid="status-xyz"):
    """available: True/False/None (read error). status_uuid: resolved org-event status or None."""
    rec = _NotifyRecorder()
    resets = []
    monkeypatch.setattr("hs.routing.agent_status_read.is_agent_currently_available", lambda a: available)
    monkeypatch.setattr("hs.routing.agent_status_read.get_status_uuid_for_org_event", lambda org, event: status_uuid)
    monkeypatch.setattr("hs.admin.status.change_status", rec)
    monkeypatch.setattr(hb.HeartbeatPresenceCache, "reset_ping_count", classmethod(lambda cls, a: resets.append(a)))
    return rec, resets


def test_online_edge_notifies_when_currently_offline(monkeypatch):
    rec, resets = _wire(monkeypatch, available=False)
    presence_edge(HeartbeatReqMeta(aom_uuid=AOM, org_uuid=ORG, api_id="api-9"), online=True)
    assert len(rec.calls) == 1
    c = rec.calls[0]
    assert c["status_uuid"] == "status-xyz" and c["aom_uuid"] == AOM
    assert (c["reason"], c["reason_for_change"]) == ("heartbeat", HEARTBEAT_REASON_INTERNET_CONNECTED)
    assert resets == []  # online never resets the count


def test_online_edge_skips_when_already_online(monkeypatch):
    rec, _resets = _wire(monkeypatch, available=True)
    presence_edge(HeartbeatReqMeta(aom_uuid=AOM, org_uuid=ORG), online=True)
    assert rec.calls == []  # dedup: already online → no notify


def test_offline_edge_notifies_and_resets_when_currently_online(monkeypatch):
    rec, resets = _wire(monkeypatch, available=True)
    presence_edge(HeartbeatReqMeta(aom_uuid=AOM, org_uuid=ORG), online=False)
    assert len(rec.calls) == 1
    assert rec.calls[0]["reason_for_change"] == HEARTBEAT_REASON_INTERNET_DISCONNECTED
    assert resets == [AOM]  # confirmed offline → reset the ping count


def test_offline_reset_happens_before_notify(monkeypatch):
    # the ping count resets BEFORE the status change. Pin the order.
    seq = []
    monkeypatch.setattr("hs.routing.agent_status_read.is_agent_currently_available", lambda a: True)
    monkeypatch.setattr("hs.routing.agent_status_read.get_status_uuid_for_org_event", lambda org, event: "s")
    monkeypatch.setattr(hb.HeartbeatPresenceCache, "reset_ping_count", classmethod(lambda cls, a: seq.append("reset")))

    monkeypatch.setattr("hs.admin.status.change_status", lambda *a: seq.append("notify"))
    presence_edge(HeartbeatReqMeta(aom_uuid=AOM, org_uuid=ORG), online=False)
    assert seq == ["reset", "notify"]  # reset strictly BEFORE the notify


def test_offline_edge_skips_notify_but_still_resets_when_already_offline(monkeypatch):
    rec, resets = _wire(monkeypatch, available=False)
    assert presence_edge(HeartbeatReqMeta(aom_uuid=AOM, org_uuid=ORG), online=False) is True
    assert rec.calls == []  # dedup: already offline → no notify
    # but the counter STILL resets: the session is over, and skipping the reset here is what let a
    # stuck-unavailable agent (failed online notify) count 3,4,5… across logins, unroutable for the
    # counter's full TTL.
    assert resets == [AOM]


def test_no_notify_on_availability_read_error(monkeypatch):
    rec, resets = _wire(monkeypatch, available=None)  # DB read error
    presence_edge(HeartbeatReqMeta(aom_uuid=AOM, org_uuid=ORG), online=True)
    assert rec.calls == [] and resets == []  # a read error returns early


def test_no_notify_when_no_org_config(monkeypatch):
    rec, resets = _wire(monkeypatch, available=True, status_uuid=None)  # no org_event_actions row
    # a config gap is permanent, not transient → True so callers don't retry forever
    assert presence_edge(HeartbeatReqMeta(aom_uuid=AOM, org_uuid=ORG), online=False) is True
    assert rec.calls == []  # "no config row → no notify"
    assert resets == [AOM]  # session over → counter reset regardless


def test_offline_event_uses_logged_out(monkeypatch):
    # the event name maps online→AGENT_LOGGED_IN / offline→AGENT_LOGGED_OUT (resolved via org config).
    seen = []
    monkeypatch.setattr("hs.routing.agent_status_read.is_agent_currently_available", lambda a: True)
    monkeypatch.setattr(
        "hs.routing.agent_status_read.get_status_uuid_for_org_event", lambda org, event: seen.append(event) or "s"
    )
    monkeypatch.setattr("hs.admin.status.change_status", _NotifyRecorder())
    monkeypatch.setattr(hb.HeartbeatPresenceCache, "reset_ping_count", classmethod(lambda cls, a: None))
    presence_edge(HeartbeatReqMeta(aom_uuid=AOM, org_uuid=ORG), online=False)
    assert seen == [HEARTBEAT_EVENT_AGENT_LOGGED_OUT]


def test_a_failed_status_write_is_retried_and_a_missing_agent_is_not(monkeypatch):
    from sqlalchemy.exc import OperationalError

    _wire(monkeypatch, available=False)

    def change(error):
        def fail(*a):
            raise error

        monkeypatch.setattr("hs.admin.status.change_status", fail)

    change(OperationalError("UPDATE", {}, Exception("db down")))
    assert presence_edge(HeartbeatReqMeta(aom_uuid=AOM, org_uuid=ORG), online=True) is False
    change(LookupError("no agent"))
    assert presence_edge(HeartbeatReqMeta(aom_uuid=AOM, org_uuid=ORG), online=True) is True


class _LogRecorder:
    def __init__(self):
        self.warnings, self.errors = [], []

    def warning(self, *a, **kw):
        self.warnings.append(kw.get("msg") or (a[0] if a else ""))

    def error(self, *a, **kw):
        self.errors.append(kw.get("msg") or (a[0] if a else ""))

    def info(self, *a, **kw):
        pass


# --- the read-only raw-SQL reads ----------------------------------------------------------------


class _FakeResult:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _FakeSession:
    def __init__(self, row=None, raise_exc=False):
        self._row, self._raise, self.closed = row, raise_exc, False

    def execute(self, sql, params):
        if self._raise:
            raise RuntimeError("db down")
        return _FakeResult(self._row)

    def close(self):
        self.closed = True


def test_is_currently_available_reads_the_row(monkeypatch):
    fake = _FakeSession(row=(True,))
    monkeypatch.setattr(asr, "SessionLocal", lambda: fake)
    assert asr.is_agent_currently_available(AOM) is True
    assert fake.closed is True  # session always closed


def test_is_currently_available_no_row_is_false(monkeypatch):
    monkeypatch.setattr(asr, "SessionLocal", lambda: _FakeSession(row=None))
    assert asr.is_agent_currently_available(AOM) is False  # no row ≡ offline


def test_is_currently_available_db_error_is_none(monkeypatch):
    monkeypatch.setattr(asr, "SessionLocal", lambda: _FakeSession(raise_exc=True))
    assert asr.is_agent_currently_available(AOM) is None  # read error → caller must skip notify


def test_status_uuid_resolves_from_org_event(monkeypatch):
    monkeypatch.setattr(asr, "SessionLocal", lambda: _FakeSession(row=("status-from-db",)))
    assert asr.get_status_uuid_for_org_event(ORG, HEARTBEAT_EVENT_AGENT_LOGGED_IN) == "status-from-db"


def test_status_uuid_none_when_no_config_row(monkeypatch):
    monkeypatch.setattr(asr, "SessionLocal", lambda: _FakeSession(row=None))
    assert asr.get_status_uuid_for_org_event(ORG, HEARTBEAT_EVENT_AGENT_LOGGED_OUT) is None


def test_status_uuid_db_error_propagates(monkeypatch):
    fake = _FakeSession(raise_exc=True)
    monkeypatch.setattr(asr, "SessionLocal", lambda: fake)
    with pytest.raises(RuntimeError):
        asr.get_status_uuid_for_org_event(ORG, HEARTBEAT_EVENT_AGENT_LOGGED_IN)  # read error ≠ no config
    assert fake.closed is True  # session still closed on the error path


def test_status_uuid_none_when_org_missing(monkeypatch):
    # no org_uuid → no query at all (guard before touching the DB)
    monkeypatch.setattr(asr, "SessionLocal", lambda: pytest.fail("should not open a session"))
    assert asr.get_status_uuid_for_org_event(None, HEARTBEAT_EVENT_AGENT_LOGGED_IN) is None


# --- the online edge is backgrounded (not run inline on the ping path) --------------------------


class _BG:
    def __init__(self):
        self.tasks = []

    def add_task(self, fn, *args):
        self.tasks.append((fn, args))


def test_online_edge_is_scheduled_as_a_background_task(monkeypatch):
    # with a background_tasks, the online notify is deferred (not run inline → never blocks the ping).
    class _Cache:
        def incrby(self, k, c):
            return 2  # 2nd ping → online edge

        def expire(self, k, s):
            return True

        def set(self, k, v, expiry_in_seconds=None, nx=False):
            return True

        def get(self, k):
            return None

    from hs.infra.redis import Cache

    monkeypatch.setattr(Cache, "get_instance", staticmethod(lambda: _Cache()))
    monkeypatch.setattr(hb.HeartbeatPresenceCache, "mark_online", classmethod(lambda cls, *a: None))
    bg = _BG()
    hb.handle_agent_ping(HeartbeatReqMeta(aom_uuid=AOM, org_uuid=ORG), bg)
    assert len(bg.tasks) == 1
    fn, args = bg.tasks[0]
    # the deferred callable is the retry wrapper (rolls the counter back on a failed notify)
    assert fn is hb._online_edge_with_retry and args[0].aom_uuid == AOM
