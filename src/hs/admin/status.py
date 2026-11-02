"""The one writer of an agent's presence status. HS never sets a status for a call or a wrap-up; it changes only
by API, by the heartbeat (sign-in and Offline) and by RONA."""

from typing import Literal

from sqlalchemy import text

from hs.events import emit
from hs.infra.bg_spawn import spawn_detached
from hs.infra.db import SessionLocal
from hs.routing import constants as rc
from hs.routing import rona
from hs.voice.hangup import handle_reverse_routing_and_agent_capacity

Reason = Literal["manual", "admin", "heartbeat", "rona"]

_AGENT = text(
    "SELECT aom.id, aom.sip_endpoint, o.uuid::text AS tenant_id, cur.uuid::text AS from_id, cur.status AS from_name,"
    " new.id AS to_row, new.uuid::text AS to_id, new.status AS to_name, new.availability"
    " FROM account_org_mapping aom JOIN organization o ON o.id = aom.organization"
    " JOIN org_statuses new ON new.organization = aom.organization AND new.uuid = CAST(:status AS uuid)"
    "  AND new.is_deleted IS NOT TRUE"
    " LEFT JOIN org_statuses cur ON cur.id = aom.agent_status"
    " WHERE aom.uuid = CAST(:agent AS uuid) AND aom.is_deleted IS NOT TRUE FOR UPDATE OF aom"
)
_SET = text("UPDATE account_org_mapping SET agent_status = :to_row, updated_at = now() WHERE id = :aid")
# Availability and the handled-count strategies order by these. available_since also moves on every handled
# contact, so it also serves as the first-available time in the 12 h rule.
_ROUTING = text(
    "UPDATE aom_routing_params SET updated_at = now(),"
    " available_since = CASE WHEN :refresh AND (available_since IS NULL OR available_since < now() - interval"
    "  '12 hours') THEN now() ELSE available_since END,"
    " contacts_handled = CASE WHEN :sign_in THEN 0 ELSE contacts_handled END,"
    " contacts_handle_time = CASE WHEN :sign_in THEN 0 ELSE contacts_handle_time END"
    " WHERE aom = :aid AND is_deleted IS NOT TRUE"
)


def change_status(agent_id: str, status_id: str, reason: Reason, reason_for_change: str | None = None) -> bool:
    """Move the agent (account_org_mapping.uuid) to a status of its tenant (org_statuses.uuid) and announce it;
    False when it already had that status. A heartbeat sign-in to an available status also resets the handled
    counters, changed or not. An agent who becomes available is offered the next queued
    call it can take. LookupError when the agent, or that status in its tenant, does not exist."""
    with SessionLocal.begin() as s:
        row = s.execute(_AGENT, {"agent": agent_id, "status": status_id}).one_or_none()
        if row is None:
            raise LookupError(f"no agent {agent_id} with status {status_id} in its tenant")
        changed = row.from_id != row.to_id
        if changed:
            s.execute(_SET, {"to_row": row.to_row, "aid": row.id})
        refresh, sign_in = changed and row.availability, reason == "heartbeat" and row.availability
        s.execute(_ROUTING, {"aid": row.id, "refresh": refresh, "sign_in": sign_in})
        if changed:
            data = {
                "from_status": {"id": row.from_id, "name": row.from_name} if row.from_id else None,
                "to_status": {"id": row.to_id, "name": row.to_name},
                "reason": reason,
                "reason_for_change": reason_for_change,
            }
            emit("agent.status_changed", tenant_id=row.tenant_id, agent_id=agent_id, data=data, session=s)
    if refresh:
        rona.reset(row.tenant_id, agent_id)
    if refresh and row.sip_endpoint:
        # No old conversation: an empty id makes the capacity step a pure acquire of the queued call's slot.
        args = (row.sip_endpoint, rc.CHANNEL_CALL, "")
        spawn_detached(handle_reverse_routing_and_agent_capacity, *args, log_context="newly available agent")
    return changed
