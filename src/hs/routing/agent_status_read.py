from sqlalchemy import text

from hs.infra.db import SessionLocal
from hs.infra.log import logger

_CURRENT_AVAILABILITY_SQL = text(
    "SELECT og.availability FROM account_org_mapping aom "
    "INNER JOIN org_statuses og ON aom.agent_status = og.id WHERE aom.uuid = :aom_uuid"
)
_ORG_EVENT_STATUS_SQL = text(
    "SELECT agent_status.uuid FROM org_event_actions AS oea "
    "INNER JOIN org_statuses AS agent_status ON oea.agent_status_action = agent_status.id "
    "INNER JOIN organization ON oea.organization = organization.id "
    "WHERE organization.uuid = :org_uuid AND oea.event = :event LIMIT 1"
)


def _one(sql, **params):
    db = SessionLocal()
    try:
        return db.execute(sql, params).fetchone()
    finally:
        db.close()


def is_agent_currently_available(aom_uuid) -> bool | None:
    try:
        row = _one(_CURRENT_AVAILABILITY_SQL, aom_uuid=str(aom_uuid))
    except Exception as e:
        logger.error(f"heartbeat current-availability read error: {e}")
        return None
    return bool(row and row[0])


def get_status_uuid_for_org_event(org_uuid, event) -> str | None:
    if not org_uuid:
        return None
    row = _one(_ORG_EVENT_STATUS_SQL, org_uuid=str(org_uuid), event=event)
    return str(row[0]) if row else None
