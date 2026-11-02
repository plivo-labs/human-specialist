"""Reverse routing when an agent becomes available + the
heartbeat agent-online wiring.

Offline-deterministic (get_contact pick + capacity Lua + invite + account/settings reads all faked).
Pins the load-bearing quirks:
  * ADD-ONLY increase_agent_capacity (old="" / new=conversation_id), capacity bucket keyed by the FETCHED
    contact's CACHED channel (org_capacity[channel]); a missing channel → 0;
  * NO RRLOCK on this path (negative assert) — the defining difference from the wrap-up locked-swap path;
  * the ASYMMETRIC rollback — capacity-fail reverts the AQE ONLY (no decrease); invite-fail reverts the
    AQE AND decrease_agents_capacity;
  * get_contact error / (None, None) no-contact → no mutation; a CONTACT_PARAM miss aborts BEFORE capacity
    with NO AQE revert (the abort comes before the capacity increase);
  * HARD-CODED [email,task,callback] allowlist + abort on missing org / missing routing settings;
  * the heartbeat online edge does NOT self-trigger reverse routing unless its flag is ON (default
    OFF; no double-route).
"""

from types import SimpleNamespace

import tests.conftest  # noqa: F401 — eager data_adapter imports (AbstractConcreteBase registry)
from hs.routing import reverse as rrev

_PARAMS = {
    "conversation_number": "CN1",
    "flow_run_uuid": "P",
    "queue_and_route_run_uuid": "Q",
    "contact_uuid": "C1",
    "auth_id": "A",
    "from_number": "+1",
    "contact_channel": "task",
    "to": "",
}
_MISSING = object()  # distinguish "use default" from explicit None


class FakeRR:
    """Records the capacity + lock side-effects; serves the cached ContactParameters."""

    def __init__(self, params=_MISSING, inc_result=(1, None)):
        self._params = _PARAMS if params is _MISSING else params
        self.inc_result = inc_result
        self.increase_calls = []
        self.decrease_calls = []
        self.rrlock_calls = []

    def get_contact_parameters(self, contact_uuid, qnr_run_uuid):
        return dict(self._params) if self._params is not None else None

    def increase_agent_capacity(self, auth_id, agent, channel, org_capacity, conversation_id):
        self.increase_calls.append((auth_id, agent, channel, org_capacity, conversation_id))
        return self.inc_result

    def decrease_agents_capacity(self, auth_id, agent, channel, conversation_id):
        self.decrease_calls.append((auth_id, agent, channel, conversation_id))
        return

    def acquire_reverse_routing_lock(self, *a, **k):  # negative-assert: this path must NEVER call it
        self.rrlock_calls.append((a, k))
        return "0"


def _patch_pick(monkeypatch, contact=_MISSING, err=None):
    """Patch get_contact (the reverse pick) so the tests focus on the post-pick half."""
    if contact is _MISSING:
        contact = SimpleNamespace(contact_uuid="C1", qnr_run_uuid="Q")
    monkeypatch.setattr(rrev, "get_contact", lambda agent, channel, db, routing_redis=None: (contact, err))


def _patch_aq(monkeypatch):
    calls = []
    monkeypatch.setattr(rrev.aq, "update_queue_status", lambda db, c, q, s: calls.append((c, q, s)))
    return calls


def _patch_invite(monkeypatch, result=None):
    calls = []

    def fake(params, agents, customer_timeout, is_transfer, db, rr):
        calls.append((params.contact_uuid, list(agents), customer_timeout, is_transfer))
        return result

    return calls


_ORG = SimpleNamespace(auth_id="A")


# --- presence_edge → reverse-routing gate (default OFF) -----------------------------------------
def _wire_presence(monkeypatch, *, available=False, status_uuid="status-xyz"):
    """Drive presence_edge through to its status change (a real online transition), recording whether
    the reverse-route trigger fires."""
    import hs.admin.heartbeat as hb

    monkeypatch.setattr("hs.routing.agent_status_read.is_agent_currently_available", lambda a: available)
    monkeypatch.setattr("hs.routing.agent_status_read.get_status_uuid_for_org_event", lambda org, event: status_uuid)
    monkeypatch.setattr("hs.admin.status.change_status", lambda *a, **kw: True)
    monkeypatch.setattr(hb.HeartbeatPresenceCache, "reset_ping_count", classmethod(lambda cls, a: None))
    triggered = []
    return hb, triggered


def test_presence_edge_online_flag_off_does_not_self_trigger(monkeypatch):
    hb, triggered = _wire_presence(monkeypatch, available=False)
    hb.presence_edge(hb.HeartbeatReqMeta(aom_uuid="aom-1", org_uuid="org-1", api_id="api-1"), online=True)
    assert triggered == []  # default: the flag is off, so no trigger


def test_presence_edge_offline_never_triggers_even_with_flag_on(monkeypatch):
    hb, triggered = _wire_presence(monkeypatch, available=True)  # currently online → real offline edge
    hb.presence_edge(hb.HeartbeatReqMeta(aom_uuid="aom-1", org_uuid="org-1"), online=False)
    assert triggered == []  # only the ONLINE edge reverse-routes
