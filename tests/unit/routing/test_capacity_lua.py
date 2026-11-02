"""Lua capacity/lock seam round-trip vs live Redis (gated).

Proves the Lua scripts (hs/routing/redis_store) return the documented codes and
maintain the SET-based capacity model against a REAL Redis, under cluster hash-tag co-location.
Skips offline; HS_REQUIRE_STACK=1 makes a missing Redis a hard fail.

Isolation: a throwaway `auth_id`/contact namespace (the `{auth_id}.` hash-tag confines the keys);
every key touched is DELeted in teardown, so NO seeded/golden routing state is mutated.
"""

import uuid

import pytest

from hs.infra.redis import Cache
from hs.routing import constants as rc
from hs.routing.redis_store import RoutingRedis
from tests._gate import gate


def _conversation_capacity_key(auth_id, channel, contact_id):
    return f"{rc.conversation_capacity_base_key(auth_id, channel)}:{contact_id}"


@pytest.fixture()
def redis_up():
    cache = Cache.get_instance()
    gate([] if cache.get_status() else ["redis"], what="platform Redis")
    return cache


@pytest.fixture()
def ns(redis_up):
    """A throwaway auth_id + the keys we touch; cleaned up after."""
    auth_id = "test-capacity-" + uuid.uuid4().hex[:8]
    agent = "sip:s0test@plivo"
    channel = rc.CHANNEL_CALL
    contacts = [f"c-{uuid.uuid4().hex[:6]}" for _ in range(3)]
    cia_key = f"CIA:flow-{auth_id}:run1"
    keys = {"auth_id": auth_id, "agent": agent, "channel": channel, "contacts": contacts, "cia_key": cia_key}
    yield keys
    cache = Cache.get_instance()
    to_del = [
        rc.agent_capacity_key(auth_id, agent, channel),
        rc.org_channel_unavailability_key(auth_id, channel),
        cia_key,
        rc.contact_to_agent_mapping_key(contacts[0]),
        rc.reverse_routing_lock_key(agent, contacts[0], channel),
    ]
    for c in contacts:
        to_del.append(_conversation_capacity_key(auth_id, channel, c))
    for k in to_del:
        cache.delete(k)


def test_update_agent_capacity_set_semantics(ns):
    r = RoutingRedis()
    a, ch, auth = ns["agent"], ns["channel"], ns["auth_id"]
    c1, c2, c3 = ns["contacts"]
    cache = Cache.get_instance()
    cap_key = rc.agent_capacity_key(auth, a, ch)
    ocu_key = rc.org_channel_unavailability_key(auth, ch)

    # org_capacity=3. First acquire of a fresh key → '0', SCARD 1, agent in CCK, NOT in OCU.
    assert r.update_agent_capacity(auth, a, ch, 3, new_contact=c1) == "0"
    assert cache.scard(cap_key) == 1
    assert a in cache.smembers(_conversation_capacity_key(auth, ch, c1))
    assert a not in cache.smembers(ocu_key)

    # Idempotent re-acquire of the SAME conversation → '0' but SCARD stays 1 (SADD set semantics).
    assert r.update_agent_capacity(auth, a, ch, 3, new_contact=c1) == "0"
    assert cache.scard(cap_key) == 1

    # Acquire c2, c3 → fills to capacity; at SCARD==3 the agent lands in OCU.
    assert r.update_agent_capacity(auth, a, ch, 3, new_contact=c2) == "0"
    assert r.update_agent_capacity(auth, a, ch, 3, new_contact=c3) == "0"
    assert cache.scard(cap_key) == 3
    assert a in cache.smembers(ocu_key)

    # A provider/callback replay for an already-held conversation stays
    # idempotent even at full capacity, and repairs derived indexes that may
    # have been lost by an interrupted cleanup.
    c1_key = _conversation_capacity_key(auth, ch, c1)
    cache.srem(c1_key, a)
    cache.srem(ocu_key, a)
    assert r.update_agent_capacity(auth, a, ch, 3, new_contact=c1) == "0"
    assert cache.scard(cap_key) == 3
    assert a in cache.smembers(c1_key)
    assert a in cache.smembers(ocu_key)

    # Full → a NEW contact returns '-2' and does NOT grow the set.
    assert r.update_agent_capacity(auth, a, ch, 3, new_contact="c-overflow") == "-2"
    assert cache.scard(cap_key) == 3

    # Release c1 (old set, new empty) → '-3' (pure decrease); SCARD 2; agent leaves OCU + that CCK.
    assert r.update_agent_capacity(auth, a, ch, 3, old_contact=c1) == "-3"
    assert cache.scard(cap_key) == 2
    assert a not in cache.smembers(ocu_key)
    assert a not in cache.smembers(_conversation_capacity_key(auth, ch, c1))

    # Release a contact NOT in the set → '-1' (old_contact not found).
    assert r.update_agent_capacity(auth, a, ch, 3, old_contact="c-not-there") == "-1"


def test_update_agent_capacity_missing_key_paths(ns):
    r = RoutingRedis()
    a, ch, auth = ns["agent"], ns["channel"], ns["auth_id"]
    c1 = ns["contacts"][0]
    cache = Cache.get_instance()
    cap_key = rc.agent_capacity_key(auth, a, ch)
    # Key does not exist yet + old_contact present → '-4' (can't release from a missing key).
    assert cache.scard(cap_key) == 0
    assert r.update_agent_capacity(auth, a, ch, 3, old_contact=c1) == "-4"
    # org_capacity==1, fresh key, acquire → '0' AND immediately marked unavailable (else-branch).
    assert r.update_agent_capacity(auth, a, ch, 1, new_contact=c1) == "0"
    assert a in cache.smembers(rc.org_channel_unavailability_key(auth, ch))


def test_acquire_reverse_routing_lock_first_wins(ns):
    r = RoutingRedis()
    a, ch = ns["agent"], ns["channel"]
    c1 = ns["contacts"][0]
    assert r.acquire_reverse_routing_lock_bool(a, c1, ch) is True
    assert r.acquire_reverse_routing_lock_bool(a, c1, ch) is False  # held
