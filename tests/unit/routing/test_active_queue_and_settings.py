"""ActiveQueueEntry dual-deletion-marker scope + OrgRoutingSettings shape (offline).

The routing read scope for active_queue_entry MUST filter `deleted_at IS NULL` as well as
`is_deleted`, or get_best_contact double-picks deleted contacts. No DB connection — the query is
compiled to SQL text and inspected.
"""

from hs.admin.account import OrgRoutingSettings
from hs.conversations.store import ActiveQueueEntry
from hs.infra.db import SessionLocal


def test_active_queue_entry_has_both_markers_and_routing_columns():
    cols = set(ActiveQueueEntry.__mapper__.columns.keys())
    assert ActiveQueueEntry.__tablename__ == "active_queue_entry"
    # both deletion markers exist (matches the live 17-col schema)
    assert "deleted_at" in cols and "is_deleted" in cols
    # the routing-load-bearing columns
    for c in (
        "queue_priority",
        "queue_uuid",
        "contact_uuid",
        "flow_run_uuid",
        "queue_type",
        "queue_channel",
        "contact_phone_or_email",
        "version",
        "qnr_run_uuid",
        "status",
        "auth_id",
    ):
        assert c in cols, c


def test_active_query_filters_both_deletion_markers():
    # the best-contact scope is the CONJUNCTION: explicit is_deleted=false + soft-delete deleted_at
    # IS NULL. Inspect the WHERE clause (the SELECT lists every column — not a filter).
    where = str(ActiveQueueEntry.active_query(SessionLocal()).statement.whereclause).lower()
    assert "deleted_at is null" in where
    # is_deleted=false must ALSO be a filter (both markers are written together; a deleted_at-only
    # scope would still pick a row a soft-delete only half-marked). `.is_(False)` → "is_deleted IS false".
    assert "is_deleted is false" in where


def test_org_routing_settings_tuple_shape():
    s = OrgRoutingSettings(
        capacity={"chat": 10},
        reservation_timeout={"chat": 420},
        simultaneous_agent_limit=10,
        auto_accept=False,
        outbound_sticky_routing={"enabled": True},
        wrap_up_configuration={"unit": "seconds", "wrap_up_time": 120},
        release_capacity_on_resolve=True,
    )
    assert s.capacity["chat"] == 10
    assert s.simultaneous_agent_limit == 10
    assert s.wrap_up_configuration["wrap_up_time"] == 120
    assert set(OrgRoutingSettings._fields) == {
        "capacity",
        "reservation_timeout",
        "simultaneous_agent_limit",
        "auto_accept",
        "outbound_sticky_routing",
        "wrap_up_configuration",
        "release_capacity_on_resolve",
    }
