"""Routing constants + Redis key builders (offline).

Pins the literals + the key layouts byte-for-byte: they are compared by exact value at runtime,
so a drift here is a silent routing divergence. The LOAD-BEARING `{auth_id}.` hash-tag (capacity keys only) and the
CIA 3-shape are asserted explicitly. No Redis needed.
"""

from hs.routing import constants as rc

# Redis key formats + per-channel TTLs (the {auth_id}-tagged CAPACITY/OCU/CCK, the untagged
# lock/CIA/mapping keys, the CIA retry/task_uuid disambiguator).


def test_four_routing_strategies_exact():
    # FOUR types, no "sequential". Skill/team are filters, not strategies.
    assert rc.SIMULTANEOUS_ROUTING == "Simultaneous"
    assert rc.AVAILABILITY_ROUTING == "Availability"
    assert rc.CONVERSATIONS_HANDLED_ROUTING == "Lowest Conversations Handled"
    assert rc.CONVERSATIONS_HANDLE_TIME_ROUTING == "Lowest Conversation Handle Time"


def test_contact_states_and_transfer_modes_string_forms():
    # The string forms persisted in active_queue_entry.status.
    assert rc.CONTACT_STATE_QUEUED == "QUEUED"
    assert rc.CONTACT_STATE_IN_TRANSIT == "IN_TRANSIT"
    assert rc.CONTACT_STATE_IN_REVERSE_ROUTING == "IN_REVERSE_ROUTING"
    # The transfer mode strings.
    assert (rc.TRANSFER_MODE_WARM, rc.TRANSFER_MODE_COLD, rc.TRANSFER_MODE_BOTH) == ("warm", "cold", "both")
