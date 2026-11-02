import time

import hs.admin.heartbeat as hb


def test_sweep_flips_only_stale_agents(monkeypatch):
    now = int(time.time())
    online = ["stale-aom", "fresh-aom", "expired-aom"]
    ping_times = {
        "stale-aom": str(now - hb.non_env.HEARTBEAT_TIME_TO_MARK_OFFLINE - 5),
        "fresh-aom": str(now),
        "expired-aom": None,  # LST key already expired
    }
    flips = []
    unmarked = []
    monkeypatch.setattr(hb.HeartbeatPresenceCache, "get_online_agents", classmethod(lambda cls: list(online)))
    monkeypatch.setattr(hb.HeartbeatPresenceCache, "get_ping_time", classmethod(lambda cls, aom: ping_times.get(aom)))
    monkeypatch.setattr(
        hb.HeartbeatPresenceCache, "get_presence_context", classmethod(lambda cls, aom: ("org-uuid", "org-name"))
    )
    monkeypatch.setattr(hb.HeartbeatPresenceCache, "unmark_online", classmethod(lambda cls, aom: unmarked.append(aom)))
    monkeypatch.setattr(hb, "presence_edge", lambda req_meta, online: flips.append((req_meta.aom_uuid, online)) or True)

    flipped = hb.sweep_offline_agents()

    assert flipped == 2
    assert ("stale-aom", False) in flips
    assert ("expired-aom", False) in flips
    assert "fresh-aom" not in [a for a, _ in flips]
    assert set(unmarked) == {"stale-aom", "expired-aom"}


def test_sweep_passes_org_context(monkeypatch):
    monkeypatch.setattr(hb.HeartbeatPresenceCache, "get_online_agents", classmethod(lambda cls: ["a1"]))
    monkeypatch.setattr(hb.HeartbeatPresenceCache, "get_ping_time", classmethod(lambda cls, aom: None))
    monkeypatch.setattr(
        hb.HeartbeatPresenceCache, "get_presence_context", classmethod(lambda cls, aom: ("ORG", "NAME"))
    )
    monkeypatch.setattr(hb.HeartbeatPresenceCache, "unmark_online", classmethod(lambda cls, aom: None))
    captured = {}

    def _fake_edge(req_meta, online):
        captured["org_uuid"] = req_meta.org_uuid
        captured["org_name"] = req_meta.org_name
        captured["online"] = online

    monkeypatch.setattr(hb, "presence_edge", _fake_edge)

    hb.sweep_offline_agents()

    assert captured == {"org_uuid": "ORG", "org_name": "NAME", "online": False}


def test_sweep_skips_when_no_online_agents(monkeypatch):
    monkeypatch.setattr(hb.HeartbeatPresenceCache, "get_online_agents", classmethod(lambda cls: []))
    called = []
    monkeypatch.setattr(hb, "presence_edge", lambda req_meta, online: called.append(req_meta.aom_uuid))
    assert hb.sweep_offline_agents() == 0
    assert called == []


def test_sweep_with_debug_log_on_still_flips(monkeypatch):
    import hs.infra.constants

    monkeypatch.setattr(hs.infra.constants, "HEARTBEAT_DEBUG_LOG", True)
    now = int(time.time())
    online = ["stale-aom", "fresh-aom"]
    ping_times = {"stale-aom": str(now - hb.non_env.HEARTBEAT_TIME_TO_MARK_OFFLINE - 5), "fresh-aom": str(now)}
    flips = []
    monkeypatch.setattr(hb.HeartbeatPresenceCache, "get_online_agents", classmethod(lambda cls: list(online)))
    monkeypatch.setattr(hb.HeartbeatPresenceCache, "get_ping_time", classmethod(lambda cls, aom: ping_times.get(aom)))
    monkeypatch.setattr(
        hb.HeartbeatPresenceCache, "get_presence_context", classmethod(lambda cls, aom: ("org", "name"))
    )
    monkeypatch.setattr(hb.HeartbeatPresenceCache, "unmark_online", classmethod(lambda cls, aom: None))
    monkeypatch.setattr(hb, "presence_edge", lambda req_meta, online: flips.append((req_meta.aom_uuid, online)) or True)
    # debug snapshot path must run without error and not change which agents flip
    assert hb.sweep_offline_agents() == 1
    assert ("stale-aom", False) in flips
    assert "fresh-aom" not in [a for a, _ in flips]


def test_sweep_keeps_agent_online_marked_when_flip_fails(monkeypatch):
    # a flip whose status write fails must NOT be counted or unmarked: the agent stays in the online set so
    # the next tick retries, instead of being silently left routable with no desk attached.
    unmarked, attempts = [], []
    monkeypatch.setattr(hb.HeartbeatPresenceCache, "get_online_agents", classmethod(lambda cls: ["a1"]))
    monkeypatch.setattr(hb.HeartbeatPresenceCache, "get_ping_time", classmethod(lambda cls, aom: None))
    monkeypatch.setattr(
        hb.HeartbeatPresenceCache, "get_presence_context", classmethod(lambda cls, aom: ("org", "name"))
    )
    monkeypatch.setattr(hb.HeartbeatPresenceCache, "unmark_online", classmethod(lambda cls, aom: unmarked.append(aom)))
    monkeypatch.setattr(hb, "_renew_sweep_lock", lambda: None)
    monkeypatch.setattr(hb, "presence_edge", lambda req_meta, online: attempts.append(req_meta.aom_uuid) or False)
    assert hb.sweep_offline_agents() == 0
    assert attempts == ["a1"]  # the flip WAS attempted
    assert unmarked == []  # …but the agent stays tracked for the next tick


# ── sweeper lock keepalive (lock must not expire mid-sweep) ──────────────────────
# presence_edge -> notify_agent_status can block for multiple HTTP retries per stale
# agent; without renewal the interval-1s NX lock expires mid-pass and a second replica
# starts a CONCURRENT sweep (double-fired status notifications).
def _wire_sweep(monkeypatch, online, stale=True):
    import hs.admin.heartbeat as hb

    renewals = []

    class _FakeCache:
        def expire(self, key, ttl):
            renewals.append((key, ttl))
            return True

    monkeypatch.setattr(hb, "_renew_sweep_lock", hb._renew_sweep_lock)  # keep real helper
    import hs.infra.redis as dr

    monkeypatch.setattr(dr.Cache, "get_instance", classmethod(lambda cls: _FakeCache()))
    monkeypatch.setattr(hb.HeartbeatPresenceCache, "get_online_agents", classmethod(lambda cls: list(online)))
    monkeypatch.setattr(
        hb.HeartbeatPresenceCache, "get_ping_time", classmethod(lambda cls, aom: None if stale else str(2**62))
    )
    monkeypatch.setattr(
        hb.HeartbeatPresenceCache, "get_presence_context", classmethod(lambda cls, aom: ("org", "name"))
    )
    monkeypatch.setattr(hb.HeartbeatPresenceCache, "unmark_online", classmethod(lambda cls, aom: None))
    monkeypatch.setattr(hb, "presence_edge", lambda req_meta, online: True)
    return hb, renewals


def test_sweep_renews_lock_per_stale_agent(monkeypatch):
    hb, renewals = _wire_sweep(monkeypatch, online=["a1", "a2", "a3"], stale=True)
    assert hb.sweep_offline_agents() == 3
    from hs.infra.constants import HEARTBEAT_SWEEP_INTERVAL_SECONDS, HEARTBEAT_SWEEP_LOCK_KEY

    assert len(renewals) == 3  # one keepalive per slow notify chain
    assert renewals[0] == (HEARTBEAT_SWEEP_LOCK_KEY, max(1, HEARTBEAT_SWEEP_INTERVAL_SECONDS - 1))


def test_sweep_no_renewal_when_nothing_stale(monkeypatch):
    import time as _time

    hb, renewals = _wire_sweep(monkeypatch, online=["a1"], stale=False)
    monkeypatch.setattr(
        hb.HeartbeatPresenceCache, "get_ping_time", classmethod(lambda cls, aom: str(int(_time.time())))
    )
    assert hb.sweep_offline_agents() == 0
    assert renewals == []  # fast pass never touches the lock


def test_sweep_survives_renewal_failure(monkeypatch):
    import hs.admin.heartbeat as hb

    monkeypatch.setattr(hb, "_renew_sweep_lock", lambda: (_ for _ in ()).throw(RuntimeError("redis down")))
    monkeypatch.setattr(hb.HeartbeatPresenceCache, "get_online_agents", classmethod(lambda cls: ["a1"]))
    monkeypatch.setattr(hb.HeartbeatPresenceCache, "get_ping_time", classmethod(lambda cls, aom: None))
    monkeypatch.setattr(hb.HeartbeatPresenceCache, "get_presence_context", classmethod(lambda cls, aom: ("o", "n")))
    monkeypatch.setattr(hb.HeartbeatPresenceCache, "unmark_online", classmethod(lambda cls, aom: None))
    flips = []
    monkeypatch.setattr(hb, "presence_edge", lambda req_meta, online: flips.append(req_meta.aom_uuid))
    # helper raising is caught by the per-agent try/except: the sweep continues, no crash.
    assert hb.sweep_offline_agents() == 0
    assert flips == []
