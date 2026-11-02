"""What a new tenant starts with: the rows the call path reads before any admin call."""

import json

from sqlalchemy import column, func, insert, table

from hs.conversations.models import ConversationCustomStatusMappingStates

# name -> availability (routable)
STATUSES = {
    "Available": True,
    "Idle": False,
    "Busy": False,
    "After Contact Work": False,
    "Away": False,
    "Offline": False,
    "Break": False,
}
# event -> the status it moves the agent to; AGENT_MISSED_CALLS is RONA after `occurrences` missed offers.
EVENT_ACTIONS = {
    "AGENT_LOGGED_IN": ("Available", {}),
    "AGENT_LOGGED_OUT": ("Offline", {}),
    "AGENT_INACTIVITY_TIMEOUT": ("Offline", {}),
    "AGENT_MISSED_CALLS": ("Break", {"occurrences": 2}),
}
ROLES = ("admin", "supervisor", "agent")
# System dispositions (internal): name -> answered. The engine writes some by name; the rest are status-map targets.
SYSTEM_DISPOSITIONS = {
    "Auto-closed": True,
    "Transferred back to same agent": True,
    "Customer didn't respond": False,
    "Queue timeout": False,
    "Agent assignment failed": False,
    "Enqueue failed": False,
    "Dropped": False,
    "Queue exit": False,
    "Out of business hour": False,
    "Voicemail": False,
    "Forwarded": False,
}
STATUS_MAP_DEFAULTS = {
    "inbound": {
        "answered": "Auto-closed",
        "queue_timeout": "Queue timeout",
        "voicemail": "Voicemail",
        "abandoned": "Dropped",
        "after_hours": "Out of business hour",
        "rejected": "Dropped",
        "forwarded": "Forwarded",
        "enqueue_failed": "Enqueue failed",
        "assignment_failed": "Agent assignment failed",
        "queue_exit": "Queue exit",
    },
    "outbound": {"answered": "Auto-closed"}
    | dict.fromkeys(
        ("no_answer", "busy", "rejected", "cancelled", "failed_invalid", "failed_carrier"), "Customer didn't respond"
    ),
}
ROUTING_SETTINGS = {
    "capacity": {"call": 1},
    "reservation_timeout": {"call": 30},
    "auto_accept": False,
    "release_capacity_on_resolve": False,
    "simultaneous_agent_limit": 3,
    "outbound_sticky_routing": {"enabled": False},
    "wrap_up_configuration": {},
}


def _add(conn, name: str, rows: list[dict], key: str) -> dict:
    """Insert `rows` with the columns every adopted table carries; {row[key]: (id, uuid)}."""
    base = {"uuid": func.gen_random_uuid(), "created_at": func.now(), "updated_at": func.now(), "is_deleted": False}
    t = table(name, *map(column, ("id", *base, *rows[0])))
    values = [base | {k: json.dumps(v) if isinstance(v, dict) else v for k, v in row.items()} for row in rows]
    found = conn.execute(insert(t).values(values).returning(t.c[key], t.c.id, t.c.uuid))
    return {k: (row_id, str(uuid)) for k, row_id, uuid in found}


def seed(conn, org_id: int, plivo_app_id: str) -> None:
    rows = [{"organization": org_id, "status": name, "availability": a} for name, a in STATUSES.items()]
    statuses = _add(conn, "org_statuses", rows, "status")
    actions = [
        {"organization": org_id, "event": event, "agent_status_action": statuses[status][0], "event_metadata": meta}
        for event, (status, meta) in EVENT_ACTIONS.items()
    ]
    _add(conn, "org_event_actions", actions, "event")
    states = [
        {"organization": org_id, "status": s.value, "state": s.value} for s in ConversationCustomStatusMappingStates
    ]
    _add(conn, "conversation_custom_status_mapping", states, "state")
    _add(conn, "role", [{"organization": org_id, "name": role} for role in ROLES], "name")
    dispositions = _add(
        conn,
        "dispositions",
        [
            {"organization": org_id, "name": name, "sequence": n, "answered": answered, "internal": True}
            | {"state": not answered}
            for n, (name, answered) in enumerate(SYSTEM_DISPOSITIONS.items(), 1)
        ],
        "name",
    )
    status_map = {d: {r: dispositions[name][1] for r, name in m.items()} for d, m in STATUS_MAP_DEFAULTS.items()}
    _add(conn, "routing_settings", [{"organization": org_id} | ROUTING_SETTINGS], "organization")
    settings = {"organization": org_id, "plivo_app_id": plivo_app_id, "voice_settings": {"status_map": status_map}}
    _add(conn, "org_setting", [settings], "organization")
