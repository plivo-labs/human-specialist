"""The public route table: (method, path, auth). Every route hs.app serves must be listed here.

Auth codes: `op` operator key; `key:<scope>` tenant key with that scope; `agent` agent token (own
resources); `sup` supervisor token (accessible agents); `stream` stream token; `sig` signed URL;
`plivo` Plivo signature; `none` public. Path parameter names are free: `{id}` matches any `{...}`.
"""

import re

NONE = ("none",)
OP = ("op",)
ADMIN = ("key:admin",)
ADMIN_AGENT = ("key:admin", "agent")
ADMIN_SUP = ("key:admin", "sup")
CALLS_AGENT = ("key:calls", "agent")
CALLS_AGENT_SUP = ("key:calls", "agent", "sup")
RECORDINGS = ("key:recordings", "agent", "sup")
REPORTS_SUP = ("key:reports", "sup")
EVENTS = ("key:events",)
PLIVO = ("plivo",)

ROUTES = [
    ("GET", "/status", NONE),
    ("GET", "/openapi.json", NONE),
    # tenants and their keys
    ("POST", "/api/v1/tenants", OP),
    ("GET", "/api/v1/tenants", OP),
    ("GET", "/api/v1/tenants/{id}", OP),
    ("PATCH", "/api/v1/tenants/{id}", OP),
    ("DELETE", "/api/v1/tenants/{id}", OP),
    ("POST", "/api/v1/tenants/{id}/api-keys", OP),
    ("GET", "/api/v1/tenants/{id}/api-keys", OP),
    ("DELETE", "/api/v1/tenants/{id}/api-keys/{key_id}", OP),
    # agents
    ("GET", "/api/v1/agents", ADMIN_SUP),
    ("POST", "/api/v1/agents", ADMIN),
    ("GET", "/api/v1/agents/{id}", ADMIN),
    ("PATCH", "/api/v1/agents/{id}", ADMIN),
    ("DELETE", "/api/v1/agents/{id}", ADMIN),
    ("POST", "/api/v1/agents/{id}/sessions", ("key:sessions",)),
    ("DELETE", "/api/v1/agents/{id}/sessions", ("key:sessions",)),
    ("GET", "/api/v1/agents/{id}/status", ADMIN_SUP),
    ("PUT", "/api/v1/agents/{id}/status", ADMIN_SUP),
    ("POST", "/api/v1/agents/{id}/sip-credentials", ADMIN),
    # admin resources
    ("GET", "/api/v1/teams", ADMIN_AGENT),
    ("POST", "/api/v1/teams", ADMIN),
    ("GET", "/api/v1/teams/{id}", ADMIN_AGENT),
    ("PATCH", "/api/v1/teams/{id}", ADMIN),
    ("DELETE", "/api/v1/teams/{id}", ADMIN),
    ("GET", "/api/v1/skills", ADMIN_AGENT),
    ("POST", "/api/v1/skills", ADMIN),
    ("GET", "/api/v1/skills/{id}", ADMIN_AGENT),
    ("PATCH", "/api/v1/skills/{id}", ADMIN),
    ("DELETE", "/api/v1/skills/{id}", ADMIN),
    ("GET", "/api/v1/roles", ADMIN),
    ("GET", "/api/v1/agent-statuses", ADMIN_AGENT),
    ("POST", "/api/v1/agent-statuses", ADMIN),
    ("GET", "/api/v1/agent-statuses/{id}", ADMIN_AGENT),
    ("PATCH", "/api/v1/agent-statuses/{id}", ADMIN),
    ("DELETE", "/api/v1/agent-statuses/{id}", ADMIN),
    ("GET", "/api/v1/settings", ADMIN),
    ("PATCH", "/api/v1/settings", ADMIN),
    ("GET", "/api/v1/dispositions", ADMIN_AGENT),
    ("POST", "/api/v1/dispositions", ADMIN),
    ("GET", "/api/v1/dispositions/{id}", ADMIN_AGENT),
    ("PATCH", "/api/v1/dispositions/{id}", ADMIN),
    ("DELETE", "/api/v1/dispositions/{id}", ADMIN),
    ("POST", "/api/v1/dispositions/reorder", ADMIN),
    ("GET", "/api/v1/status-map", ADMIN),
    ("PUT", "/api/v1/status-map", ADMIN),
    # setup: queues, hours, menus, numbers
    ("GET", "/api/v1/queues", ADMIN),
    ("POST", "/api/v1/queues", ADMIN),
    ("GET", "/api/v1/queues/{id}", ADMIN),
    ("PATCH", "/api/v1/queues/{id}", ADMIN),
    ("DELETE", "/api/v1/queues/{id}", ADMIN),
    ("POST", "/api/v1/queues/{id}/enqueue", ("plivo", "key:calls")),
    ("GET", "/api/v1/queues/{id}/stats", REPORTS_SUP),
    ("GET", "/api/v1/business-hours", ADMIN),
    ("POST", "/api/v1/business-hours", ADMIN),
    ("GET", "/api/v1/business-hours/{id}", ADMIN),
    ("PATCH", "/api/v1/business-hours/{id}", ADMIN),
    ("DELETE", "/api/v1/business-hours/{id}", ADMIN),
    ("GET", "/api/v1/business-hours/{id}/status", ADMIN),
    ("GET", "/api/v1/menus", ADMIN),
    ("POST", "/api/v1/menus", ADMIN),
    ("GET", "/api/v1/menus/{id}", ADMIN),
    ("PATCH", "/api/v1/menus/{id}", ADMIN),
    ("DELETE", "/api/v1/menus/{id}", ADMIN),
    ("GET", "/api/v1/numbers", ADMIN),
    ("GET", "/api/v1/numbers/{number}", ADMIN),
    ("PUT", "/api/v1/numbers/{number}", ADMIN),
    ("DELETE", "/api/v1/numbers/{number}", ADMIN),
    # the agent's desk
    ("GET", "/api/v1/me", ("agent",)),
    ("PUT", "/api/v1/me/status", ("agent",)),
    ("POST", "/api/v1/me/heartbeat", ("agent",)),
    ("GET", "/api/v1/transfer-targets", ("agent",)),
    # conversations
    ("GET", "/api/v1/conversations", CALLS_AGENT_SUP),
    ("GET", "/api/v1/conversations/{id}", CALLS_AGENT_SUP),
    ("POST", "/api/v1/conversations/{id}/notes", CALLS_AGENT),
    ("POST", "/api/v1/conversations/{id}/wrap-up", CALLS_AGENT),
    ("POST", "/api/v1/conversations/{id}/wrap-up/extend", CALLS_AGENT),
    ("GET", "/api/v1/conversations/{id}/timeline", ("key:calls", "sup")),
    # calls
    ("POST", "/api/v1/calls", CALLS_AGENT),
    ("POST", "/api/v1/calls/{id}/hold", CALLS_AGENT),
    ("POST", "/api/v1/calls/{id}/recording", CALLS_AGENT),
    ("POST", "/api/v1/calls/{id}/transfers", CALLS_AGENT),
    ("POST", "/api/v1/calls/{id}/transfers/{tid}/complete", CALLS_AGENT),
    ("POST", "/api/v1/calls/{id}/transfers/{tid}/cancel", CALLS_AGENT),
    ("POST", "/api/v1/calls/{id}/participants", CALLS_AGENT),
    ("DELETE", "/api/v1/calls/{id}/participants/{participant_id}", CALLS_AGENT),
    ("POST", "/api/v1/calls/{id}/hangup", CALLS_AGENT),
    ("POST", "/api/v1/calls/{id}/monitor", ("sup", "key:calls")),
    ("PATCH", "/api/v1/calls/{id}/monitor", ("sup", "key:calls")),
    ("DELETE", "/api/v1/calls/{id}/monitor", ("sup", "key:calls")),
    # recordings
    ("GET", "/api/v1/recordings", RECORDINGS),
    ("GET", "/api/v1/recordings/{id}", RECORDINGS),
    ("DELETE", "/api/v1/recordings/{id}", ("key:recordings",)),
    ("GET", "/api/v1/recordings/{id}/audio", ("sig",)),
    ("GET", "/api/v1/recordings/{id}/transcript", RECORDINGS),
    # reports and the floor
    ("GET", "/api/v1/floor", REPORTS_SUP),
    ("GET", "/api/v1/reports/calls", REPORTS_SUP),
    ("GET", "/api/v1/reports/dispositions", REPORTS_SUP),
    ("GET", "/api/v1/reports/agent-activity", REPORTS_SUP),
    # events and webhooks
    ("GET", "/api/v1/webhooks", EVENTS),
    ("POST", "/api/v1/webhooks", EVENTS),
    ("GET", "/api/v1/webhooks/{id}", EVENTS),
    ("PATCH", "/api/v1/webhooks/{id}", EVENTS),
    ("DELETE", "/api/v1/webhooks/{id}", EVENTS),
    ("POST", "/api/v1/webhooks/{id}/rotate-secret", EVENTS),
    ("POST", "/api/v1/webhooks/{id}/test", EVENTS),
    ("GET", "/api/v1/events", EVENTS),
    ("GET", "/api/v1/events/stream", ("key:events", "agent", "stream")),
    # Plivo callbacks
    ("POST", "/plivo/{tenant_id}/answer", PLIVO),
    ("POST", "/plivo/{tenant_id}/hangup", PLIVO),
    ("POST", "/plivo/{tenant_id}/mpc-status", PLIVO),
    ("POST", "/plivo/{tenant_id}/call-status", PLIVO),
    ("POST", "/plivo/{tenant_id}/recording", PLIVO),
    ("POST", "/plivo/{tenant_id}/transcription", PLIVO),
    ("POST", "/plivo/{tenant_id}/actions/menu", PLIVO),
    ("POST", "/plivo/{tenant_id}/actions/voicemail", PLIVO),
    ("POST", "/plivo/{tenant_id}/actions/queue-exit", PLIVO),
    ("POST", "/plivo/{tenant_id}/actions/move", PLIVO),
]


def norm(path: str) -> str:
    return re.sub(r"\{[^}]*\}", "{}", path)


SPEC = {(method, norm(path)) for method, path, _ in ROUTES}


def import_app():
    """hs.app's FastAPI app, or None when it cannot be imported."""
    try:
        from hs.app import app
    except ModuleNotFoundError as e:
        if e.name in ("hs", "hs.app"):
            return None
        raise
    return app


def served(app) -> list[tuple[str, str, str]]:
    """(method, path, endpoint module) per route; HEAD rides along with GET."""
    rows = []
    for route in app.routes:
        module = getattr(getattr(route, "endpoint", None), "__module__", "") or ""
        for method in sorted(getattr(route, "methods", None) or {"*"}):
            if method != "HEAD":
                rows.append((method, route.path, module))
    return rows


def coverage(app) -> tuple[int, int]:
    """(spec routes the app serves, spec routes)."""
    built = {(method, norm(path)) for method, path, _ in served(app)} & SPEC
    return len(built), len(SPEC)
