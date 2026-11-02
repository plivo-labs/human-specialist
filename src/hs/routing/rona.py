"""Ring-no-answer: an agent who lets `strikes` offers in a row ring out moves to the tenant's RONA status."""

from sqlalchemy import text

from hs.events import emit
from hs.infra.db import core_engine
from hs.infra.redis import Cache

_AGENT = text(
    "SELECT o.uuid::text AS tenant_id, aom.uuid::text AS agent_id, st.uuid::text AS status_id,"
    " COALESCE(CAST(oea.event_metadata ->> 'occurrences' AS int), 2) AS strikes,"
    " (SELECT uuid::text FROM conversation_parent WHERE conversation_number = :number AND organization = o.id)"
    " AS conversation_id"
    " FROM account_org_mapping aom JOIN organization o ON o.id = aom.organization"
    " JOIN org_event_actions oea ON oea.organization = o.id AND oea.event = 'AGENT_MISSED_CALLS'"
    "  AND oea.is_deleted IS NOT TRUE"
    " JOIN org_statuses st ON st.id = oea.agent_status_action"
    " WHERE aom.sip_endpoint = :sip AND aom.is_deleted IS NOT TRUE"
)


def _key(tenant_id: str, agent_id: str) -> str:
    return f"RONA:{tenant_id}:{agent_id}"


def reset(tenant_id: str, agent_id: str) -> None:
    Cache.get_instance().client.delete(_key(tenant_id, agent_id))


def missed(agent_sip: str, offer_id: str, conversation_number: str, strike: bool = True) -> None:
    """An offer to this agent went unanswered. A ring-out (6010) counts a strike and, at the tenant's limit, moves
    the agent; any other miss (`strike` off) carries the count as it stands."""
    with core_engine.connect() as conn:
        row = conn.execute(_AGENT, {"sip": agent_sip, "number": conversation_number}).one_or_none()
    if row is None:  # an external number, not an agent
        return
    client, key = Cache.get_instance().client, _key(row.tenant_id, row.agent_id)
    count = client.incr(key) if strike else int(client.get(key) or 0)
    data = {"offer_id": offer_id, "strike": count}
    emit("offer.missed", tenant_id=row.tenant_id, data=data, conversation_id=row.conversation_id, agent_id=row.agent_id)
    if strike and count >= row.strikes:
        from hs.admin.status import change_status  # status imports the hangup handler, which calls here

        change_status(row.agent_id, row.status_id, "rona")
        reset(row.tenant_id, row.agent_id)
