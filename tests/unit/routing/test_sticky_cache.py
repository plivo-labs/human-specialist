"""Sticky-routing cache (CASM/OCASM) layer + helpers.

Offline: format_phone_number (digits-only), the CASM/OCASM key builders, and the
create_sticky_routing_cache TTL (no-expiry→0 / days→×24h / else→hours). Live (gated): a
set→get→del round-trip on CASM + OCASM against real Redis (throwaway contact, cleaned up).
This is the data layer sticky routing builds on.
"""

import uuid

import pytest

from hs.infra.redis import Cache
from hs.routing import constants as rc
from hs.routing.redis_store import RoutingRedis
from tests._gate import gate


def test_sticky_keys_keep_only_digits():
    assert rc.sticky_agent_key("+1 (555) 000-1234", auth_id="MA1") == "OCASM:MA1:15550001234"
    assert rc.sticky_agent_key("sip:abc123@x", auth_id="MA1") == "OCASM:MA1:123"
    assert rc.sticky_agent_key("a@example.com", auth_id="MA1") == "OCASM:MA1:"  # no digits → '' (faithful quirk)
    assert rc.sticky_agent_key("", auth_id="MA1") == rc.sticky_agent_key(None, auth_id="MA1") == "OCASM:MA1:"
    with pytest.raises(ValueError):
        rc.sticky_agent_key("+1-555-0001")  # an outbound key is always one tenant's


def test_sticky_key_layouts():
    assert rc.sticky_agent_key("+1-555-0001", "q1") == "CASM:15550001:q1"
    assert rc.sticky_agent_key("+1-555-0001", auth_id="MA1") == "OCASM:MA1:15550001"
    assert rc.sticky_agent_key("+1-555-0001", "") == "CASM:15550001:"


def test_create_sticky_routing_cache_seconds():
    assert rc.create_sticky_routing_cache_seconds(True, "days", 5) == 0  # no_expiry → persistent
    assert rc.create_sticky_routing_cache_seconds(False, "days", 1) == 86400  # 1 day
    assert rc.create_sticky_routing_cache_seconds(False, "hours", 2) == 7200  # 2 hours
    assert rc.create_sticky_routing_cache_seconds(False, "minutes", 3) == 10800  # else → value hours


@pytest.fixture()
def redis_up():
    cache = Cache.get_instance()
    gate([] if cache.get_status() else ["redis"], what="platform Redis")
    return cache


def test_casm_ocasm_round_trip(redis_up):
    r = RoutingRedis()
    contact = "+1999" + uuid.uuid4().hex[:6].replace("a", "1").replace("b", "2").replace("c", "3").replace(
        "d", "4"
    ).replace("e", "5").replace("f", "6")  # digits-ish, unique
    queue = "q-" + uuid.uuid4().hex[:6]
    try:
        # CASM (inbound, queue-scoped)
        assert r.get_sticky_agent(contact, queue) is None
        r.set_sticky_agent(contact, "aom-inbound", 60, queue)
        assert r.get_sticky_agent(contact, queue) == "aom-inbound"
        r.del_sticky_agent(contact, queue)
        assert r.get_sticky_agent(contact, queue) is None
        # OCASM (outbound, no queue)
        assert r.get_sticky_agent(contact, auth_id="MA1") is None
        r.set_sticky_agent(contact, "aom-outbound", 60, auth_id="MA1")
        assert r.get_sticky_agent(contact, auth_id="MA1") == "aom-outbound"
        assert r.get_sticky_agent(contact, auth_id="MA2") is None  # another tenant's caller with the same number
        r.del_sticky_agent(contact, auth_id="MA1")
        assert r.get_sticky_agent(contact, auth_id="MA1") is None
    finally:
        redis_up.delete(rc.sticky_agent_key(contact, queue))
        redis_up.delete(rc.sticky_agent_key(contact, auth_id="MA1"))
