"""The public route table: (method, path, auth). Every route hs.app serves must be listed here.

Auth codes: `op` operator key; `key:<scope>` tenant key with that scope; `agent` agent token (own
resources); `sup` supervisor token (accessible agents); `admin` admin token (the tenant's settings); `stream` stream
token; `sig` signed URL; `plivo` Plivo signature; `none` public. Path parameter names are free: `{id}` matches any
`{...}`.
"""

import re

NONE = ("none",)
OP = ("op",)
ADMIN = ("key:admin", "admin")
ADMIN_AGENT = ("key:admin", "agent")
ADMIN_SUP = ("key:admin", "sup")
CALLS_AGENT = ("key:calls", "agent")
CALLS_AGENT_SUP = ("key:calls", "agent", "sup")
RECORDINGS = ("key:recordings", "agent", "sup")
REPORTS_SUP = ("key:reports", "sup")
EVENTS = ("key:events",)
PLIVO = ("plivo",)

# One block per module of src/hs.

# app.py
APP = [("GET", "/status", NONE), ("GET", "/openapi.json", NONE)]

# access.py
ACCESS = [
    ("POST", "/api/v1/tenants/{id}/api-keys", OP),
    ("GET", "/api/v1/tenants/{id}/api-keys", OP),
    ("DELETE", "/api/v1/tenants/{id}/api-keys/{key_id}", OP),
]

# config/
CONFIG = [
    ("POST", "/api/v1/tenants", OP),
    ("GET", "/api/v1/tenants", OP),
    ("GET", "/api/v1/tenants/{id}", OP),
    ("PATCH", "/api/v1/tenants/{id}", OP),
    ("DELETE", "/api/v1/tenants/{id}", OP),
    ("GET", "/api/v1/transfer-destinations", ADMIN_AGENT),
    ("POST", "/api/v1/transfer-destinations", ADMIN),
    ("GET", "/api/v1/transfer-destinations/{id}", ADMIN_AGENT),
    ("PATCH", "/api/v1/transfer-destinations/{id}", ADMIN),
    ("DELETE", "/api/v1/transfer-destinations/{id}", ADMIN),
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
    ("POST", "/api/v1/agent-statuses/reorder", ADMIN),
    ("GET", "/api/v1/holiday-groups", ADMIN),
    ("POST", "/api/v1/holiday-groups", ADMIN),
    ("GET", "/api/v1/holiday-groups/{id}", ADMIN),
    ("PATCH", "/api/v1/holiday-groups/{id}", ADMIN),
    ("DELETE", "/api/v1/holiday-groups/{id}", ADMIN),
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
    ("GET", "/api/v1/queues", ADMIN),
    ("POST", "/api/v1/queues", ADMIN),
    ("GET", "/api/v1/queues/{id}", ADMIN),
    ("PATCH", "/api/v1/queues/{id}", ADMIN),
    ("DELETE", "/api/v1/queues/{id}", ADMIN),
    ("GET", "/api/v1/business-hours", ADMIN),
    ("POST", "/api/v1/business-hours", ADMIN),
    ("GET", "/api/v1/business-hours/{id}", ADMIN),
    ("PATCH", "/api/v1/business-hours/{id}", ADMIN),
    ("DELETE", "/api/v1/business-hours/{id}", ADMIN),
    ("GET", "/api/v1/business-hours/{id}/status", ADMIN),
    ("GET", "/api/v1/numbers", ADMIN),
    ("GET", "/api/v1/numbers/{number}", ADMIN),
    ("PUT", "/api/v1/numbers/{number}", ADMIN),
    ("DELETE", "/api/v1/numbers/{number}", ADMIN),
    ("GET", "/api/v1/menus", ADMIN),
    ("POST", "/api/v1/menus", ADMIN),
    ("GET", "/api/v1/menus/{id}", ADMIN),
    ("PATCH", "/api/v1/menus/{id}", ADMIN),
    ("DELETE", "/api/v1/menus/{id}", ADMIN),
]

# agents.py
AGENTS = [
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
    ("POST", "/api/v1/agents/{id}/password", ADMIN),
    ("POST", "/api/v1/sessions", NONE),
    ("POST", "/api/v1/me/sessions", ("agent",)),
    ("DELETE", "/api/v1/me/sessions", ("agent",)),
    ("PUT", "/api/v1/me/password", ("agent",)),
    ("GET", "/api/v1/me", ("agent",)),
    ("PUT", "/api/v1/me/status", ("agent",)),
    ("POST", "/api/v1/me/heartbeat", ("agent",)),
    ("POST", "/api/v1/me/idle", ("agent",)),
    ("GET", "/api/v1/transfer-targets", ("agent",)),
]

# conversations/
CONVERSATIONS = [
    ("GET", "/api/v1/conversations", CALLS_AGENT_SUP),
    ("GET", "/api/v1/conversations/counts", CALLS_AGENT_SUP),
    ("GET", "/api/v1/conversations/{id}", CALLS_AGENT_SUP),
    ("POST", "/api/v1/conversations/{id}/viewed", ("agent",)),
    ("PATCH", "/api/v1/conversations/{id}", CALLS_AGENT),
    ("POST", "/api/v1/conversations/{id}/notes", CALLS_AGENT),
    ("PATCH", "/api/v1/conversations/{id}/notes/{note_id}", CALLS_AGENT),
    ("DELETE", "/api/v1/conversations/{id}/notes/{note_id}", CALLS_AGENT),
    ("POST", "/api/v1/conversations/{id}/wrap-up", CALLS_AGENT),
    ("POST", "/api/v1/conversations/{id}/wrap-up/extend", CALLS_AGENT),
    ("POST", "/api/v1/conversations/{id}/csat", ("key:calls",)),
]

# routing.py
ROUTING = [("POST", "/api/v1/offers/{id}/accept", ("agent",)), ("POST", "/api/v1/offers/{id}/decline", ("agent",))]

# voice/flow.py
VOICE_FLOW = [
    ("POST", "/api/v1/queues/{id}/enqueue", ("plivo", "key:calls")),
    ("POST", "/plivo/{tenant_id}/answer", PLIVO),
    ("POST", "/plivo/{tenant_id}/actions/menu", PLIVO),
    ("POST", "/plivo/{tenant_id}/actions/voicemail", PLIVO),
    ("POST", "/plivo/{tenant_id}/actions/queue-exit", PLIVO),
    ("POST", "/plivo/{tenant_id}/actions/move", PLIVO),
    ("POST", "/plivo/{tenant_id}/hangup", PLIVO),
]

# voice/mpc.py
VOICE_MPC = [("POST", "/plivo/{tenant_id}/mpc-status", PLIVO), ("POST", "/plivo/{tenant_id}/call-status", PLIVO)]

# voice/calls.py
VOICE_CALLS = [
    ("POST", "/api/v1/calls/{id}/hold", CALLS_AGENT),
    ("POST", "/api/v1/calls/{id}/switch", CALLS_AGENT),
    ("POST", "/api/v1/calls/{id}/merge", CALLS_AGENT),
    ("POST", "/api/v1/calls/{id}/transfers", CALLS_AGENT),
    ("POST", "/api/v1/calls/{id}/transfers/{tid}/complete", CALLS_AGENT),
    ("POST", "/api/v1/calls/{id}/transfers/{tid}/cancel", CALLS_AGENT),
    ("POST", "/api/v1/calls/{id}/participants", CALLS_AGENT),
    ("DELETE", "/api/v1/calls/{id}/participants/{participant_id}", CALLS_AGENT),
    ("POST", "/api/v1/calls/{id}/hangup", CALLS_AGENT),
    ("POST", "/api/v1/calls/{id}/monitor", ("sup", "key:calls")),
    ("PATCH", "/api/v1/calls/{id}/monitor", ("sup", "key:calls")),
    ("DELETE", "/api/v1/calls/{id}/monitor", ("sup", "key:calls")),
    ("POST", "/api/v1/calls/{id}/monitor/mute", ("sup", "key:calls")),
    ("POST", "/api/v1/calls/{id}/monitor/reveal", ("sup", "key:calls")),
    ("POST", "/api/v1/calls/{id}/monitor/assistance", CALLS_AGENT),
    ("DELETE", "/api/v1/calls/{id}/monitor/assistance", ("sup", "key:calls")),
]

# voice/outbound.py
VOICE_OUTBOUND = [("POST", "/api/v1/calls", CALLS_AGENT)]

# voice/media.py
VOICE_MEDIA = [
    ("POST", "/api/v1/calls/{id}/recording", CALLS_AGENT),
    ("POST", "/api/v1/calls/{id}/feedback", CALLS_AGENT),
    ("GET", "/api/v1/recordings", RECORDINGS),
    ("GET", "/api/v1/recordings/{id}", RECORDINGS),
    ("DELETE", "/api/v1/recordings/{id}", ("key:recordings",)),
    ("GET", "/api/v1/recordings/{id}/audio", ("sig",)),
    ("GET", "/api/v1/recordings/{id}/transcript", RECORDINGS),
    ("POST", "/plivo/{tenant_id}/recording", PLIVO),
    ("POST", "/plivo/{tenant_id}/transcription", PLIVO),
]

# text/__init__.py
TEXT = [
    ("GET", "/api/v1/conversations/{id}/messages", CALLS_AGENT_SUP),
    ("POST", "/api/v1/conversations/{id}/messages", CALLS_AGENT),
    ("POST", "/api/v1/conversations", CALLS_AGENT),
    ("POST", "/api/v1/conversations/{id}/close", CALLS_AGENT),
    ("POST", "/api/v1/conversations/{id}/transfers", CALLS_AGENT),
    ("POST", "/api/v1/conversations/{id}/transfers/{tid}/complete", CALLS_AGENT),
    ("POST", "/api/v1/conversations/{id}/transfers/{tid}/cancel", CALLS_AGENT),
    ("POST", "/api/v1/conversations/{id}/participants", CALLS_AGENT),
    ("DELETE", "/api/v1/conversations/{id}/participants/{participant_id}", CALLS_AGENT),
    ("POST", "/api/v1/messages", ("key:messages",)),
    ("POST", "/api/v1/messages/{id}/status", ("key:messages",)),
]

# text/plivo.py
TEXT_PLIVO = [
    ("POST", "/plivo/{tenant_id}/message", PLIVO),
    ("POST", "/plivo/{tenant_id}/message-status/{id}", PLIVO),
    ("GET", "/api/v1/messages/{id}/media/{n}", ("sig",)),
]

# text/templates.py
TEXT_TEMPLATES = [("GET", "/api/v1/whatsapp/templates", CALLS_AGENT_SUP)]

# tasks.py
TASKS = [
    ("POST", "/api/v1/tasks", CALLS_AGENT),
    ("GET", "/api/v1/tasks", CALLS_AGENT_SUP),
    ("GET", "/api/v1/tasks/{id}", CALLS_AGENT_SUP),
    ("PATCH", "/api/v1/tasks/{id}", CALLS_AGENT),
]

# search.py
VIEWS = [
    ("GET", "/api/v1/views", ADMIN_AGENT),
    ("POST", "/api/v1/views", ADMIN_AGENT),
    ("PUT", "/api/v1/views/order", ("agent",)),
    ("GET", "/api/v1/views/{id}", ADMIN_AGENT),
    ("GET", "/api/v1/views/{id}/conversations", ADMIN_AGENT),
    ("PATCH", "/api/v1/views/{id}", ADMIN_AGENT),
    ("DELETE", "/api/v1/views/{id}", ADMIN_AGENT),
    ("PUT", "/api/v1/views/{id}/favorite", ("agent",)),
    ("DELETE", "/api/v1/views/{id}/favorite", ("agent",)),
]

SEARCH = [("POST", "/api/v1/conversations/search", CALLS_AGENT_SUP)]

# export.py
EXPORT = [
    ("POST", "/api/v1/conversations/export", CALLS_AGENT_SUP),
    ("POST", "/api/v1/conversations/export/content", CALLS_AGENT_SUP),
]

# reports.py
REPORTS = [
    ("GET", "/api/v1/queues/{id}/stats", REPORTS_SUP),
    ("GET", "/api/v1/floor", REPORTS_SUP),
    ("GET", "/api/v1/reports/agent-activity", REPORTS_SUP),
    ("GET", "/api/v1/reports/available-agents", REPORTS_SUP),
    ("GET", "/api/v1/reports/agent-status-log", REPORTS_SUP),
    ("GET", "/api/v1/reports/conversations", REPORTS_SUP),
    ("GET", "/api/v1/reports/conversations/rows", REPORTS_SUP),
]

# events/
EVENTS = [
    ("GET", "/api/v1/webhooks", EVENTS),
    ("POST", "/api/v1/webhooks", EVENTS),
    ("GET", "/api/v1/webhooks/{id}", EVENTS),
    ("PATCH", "/api/v1/webhooks/{id}", EVENTS),
    ("DELETE", "/api/v1/webhooks/{id}", EVENTS),
    ("POST", "/api/v1/webhooks/{id}/rotate-secret", EVENTS),
    ("POST", "/api/v1/webhooks/{id}/test", EVENTS),
    ("GET", "/api/v1/events", EVENTS),
    ("GET", "/api/v1/events/stream", ("key:events", "agent", "stream")),
    ("GET", "/api/v1/conversations/{id}/timeline", ("key:calls", "sup")),
]

ROUTES = (
    APP
    + ACCESS
    + CONFIG
    + AGENTS
    + CONVERSATIONS
    + ROUTING
    + VOICE_FLOW
    + VOICE_MPC
    + VOICE_CALLS
    + VOICE_OUTBOUND
    + VOICE_MEDIA
    + TEXT
    + TEXT_PLIVO
    + TEXT_TEMPLATES
    + TASKS
    + SEARCH
    + EXPORT
    + REPORTS
    + EVENTS
    + VIEWS
)


def norm(path: str) -> str:
    return re.sub(r"\{[^}]*\}", "{}", path)


SPEC = {(method, norm(path)) for method, path, _ in ROUTES}


def import_app():
    """hs.app's FastAPI app, imported late so tests/conftest.py sets the env hs reads first."""
    from hs.app import app

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
