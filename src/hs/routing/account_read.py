import copy
import functools
from collections import namedtuple

from sqlalchemy import String, bindparam, text
from sqlalchemy.dialects.postgresql import UUID

from hs.access import tenant_plivo
from hs.infra.db import SessionLocal
from hs.infra.log import logger
from hs.routing import constants as rc


def _reader(error: str, fallback=None):
    """A read on its own session. A failure is logged as `<error>: <exception>` and answered with `fallback` (or
    fallback(exception) when it is callable)."""

    def wrap(fn):
        @functools.wraps(fn)
        def read(*args, **kwargs):
            db = SessionLocal()
            try:
                return fn(db, *args, **kwargs)
            except Exception as e:
                logger.error(f"{error}: {e}")
                return fallback(e) if callable(fallback) else copy.copy(fallback)
            finally:
                db.close()

        return read

    return wrap


def _in(sql: str, name: str):
    return text(sql).bindparams(bindparam(name, expanding=True))


def _str(value) -> str:
    return "" if value is None else str(value)


def _name(first, last, name) -> str:
    return " ".join(part for part in (first, last) if part) or name or ""


AgentDetails = namedtuple("AgentDetails", "uuid sip_endpoint auth_id name", defaults=("", ""))


OrgDetails = namedtuple("OrgDetails", "id auth_id auth_token name")
OrgTransferSettings = namedtuple("OrgTransferSettings", "allow_acw_post_transfer allow_transfer_type default_transfer")
AgentSkill = namedtuple("AgentSkill", "skill_id level")
AomRowForSip = namedtuple("AomRowForSip", "aom_id uuid")
OrgForAuthId = namedtuple("OrgForAuthId", "id org_name auth_token auth_id org_uuid")
AomRowBySip = namedtuple("AomRowBySip", "uuid sip_endpoint name email")


# an agent's display name: its account's first and last name, else its name (see _name)
_NAME_SOURCES = "acc.first_name, acc.last_name, acc.name"
_AGENT_BY_UUID_SQL = text(
    f"SELECT aom.sip_endpoint, org.auth_id, {_NAME_SOURCES} FROM account_org_mapping aom "
    "INNER JOIN organization org ON org.id = aom.organization LEFT JOIN account acc ON acc.id = aom.account "
    "WHERE aom.uuid = :uuid AND org.auth_id = :auth_id"
)
_ORG_FOR_SIP_SQL = text(
    "SELECT org.id, org.auth_id, org.uuid, org.name, org.allow_acw_post_transfer, org.allow_transfer_type, "
    "org.default_transfer_type FROM account_org_mapping AS aom INNER JOIN organization AS org "
    "ON org.id = aom.organization WHERE aom.sip_endpoint = :sip"
)
_AOM_UUID_FOR_SIP_SQL = text("SELECT aom.uuid FROM account_org_mapping aom WHERE aom.sip_endpoint = :sip")
_ACCOUNT_BY_SIPS_SQL = _in(
    "SELECT aom.uuid, aom.sip_endpoint, acc.name, acc.email FROM account_org_mapping aom "
    "LEFT JOIN account acc ON acc.id = aom.account WHERE aom.sip_endpoint IN :sips",
    "sips",
)
_AGENT_TEAMS_BY_UUID_SQL = text(
    "SELECT t.uuid FROM account_org_mapping aom INNER JOIN aom_team at ON at.aom=aom.id "
    "INNER JOIN team t ON t.id=at.team WHERE aom.uuid = :agent"
)
_AGENT_TEAMS_BY_SIP_SQL = text(
    "SELECT t.uuid FROM account_org_mapping aom INNER JOIN aom_team at ON at.aom=aom.id "
    "INNER JOIN team t ON t.id=at.team WHERE aom.sip_endpoint = :agent"
)
# two static queries per recording direction: the column is never caller input
_TOKEN_INBOUND_REC_SQL = text("SELECT uuid, call_recording_inbound FROM organization WHERE auth_id = :auth_id")
_TOKEN_OUTBOUND_REC_SQL = text("SELECT uuid, call_recording_outbound FROM organization WHERE auth_id = :auth_id")
_INBOUND_REC_PAUSE_SQL = text(
    "SELECT call_recording_inbound, allow_recording_pause_play FROM organization WHERE auth_id = :auth_id"
)
_OUTBOUND_REC_PAUSE_SQL = text(
    "SELECT call_recording_outbound, allow_recording_pause_play FROM organization WHERE auth_id = :auth_id"
)


@_reader("check_agent_availability read error", False)
def check_agent_availability(db, agent_sip: str) -> bool:
    sql = (
        "SELECT EXISTS(SELECT 1 FROM account_org_mapping aom INNER JOIN org_statuses AS os ON os.id = aom.agent_status "
        "WHERE os.availability = true AND aom.sip_endpoint = :sip)"
    )
    row = db.execute(text(sql), {"sip": agent_sip}).fetchone()
    return bool(row and row[0])


@_reader("get_agent_with_status_info read error")
def get_agent_with_status_info(db, agent_uuid: str) -> tuple[str, str, bool] | None:
    sql = (
        "SELECT aom.sip_endpoint, aom.status, os.availability FROM account_org_mapping aom "
        "INNER JOIN org_statuses AS os ON os.id = aom.agent_status WHERE aom.uuid = :aom_uuid"
    )
    row = db.execute(text(sql), {"aom_uuid": str(agent_uuid)}).fetchone()
    return (row[0], row[1], bool(row[2])) if row else None


@_reader("get_org_details_for_agent_sip read error")
def get_org_details_for_agent_sip(db, agent_sip: str) -> OrgDetails | None:
    row = db.execute(_ORG_FOR_SIP_SQL, {"sip": agent_sip}).fetchone()
    return OrgDetails(row[0], row[1], tenant_plivo(str(row[2]))[1], row[3]) if row else None


@_reader("get_org_transfer_settings_for_agent_sip read error")
def get_org_transfer_settings_for_agent_sip(db, agent_sip: str) -> OrgTransferSettings | None:
    row = db.execute(_ORG_FOR_SIP_SQL, {"sip": agent_sip}).fetchone()
    return OrgTransferSettings(bool(row[4]), bool(row[5]), row[6] or "") if row else None


@_reader("get_team_name read error")
def get_team_name(db, team_uuid: str) -> str | None:
    row = db.execute(text("SELECT name FROM team WHERE uuid = :uuid"), {"uuid": str(team_uuid)}).fetchone()
    return (row[0] or "") if row else None


@_reader("get_organisation_auth_token read error", ("", "error fetching org auth token"))
def get_organisation_auth_token(db, auth_id: str) -> tuple[str, str | None]:
    row = db.execute(text("SELECT uuid FROM organization WHERE auth_id = :auth_id"), {"auth_id": auth_id}).fetchone()
    return (tenant_plivo(str(row[0]))[1], None) if row is not None else ("", rc.NOT_FOUND)


@_reader("get_agent_teams read error")
def get_agent_teams(db, agent: str, query_param: str = "uuid") -> list[str] | None:
    sql = _AGENT_TEAMS_BY_UUID_SQL if query_param == "uuid" else _AGENT_TEAMS_BY_SIP_SQL
    rows = db.execute(sql, {"agent": str(agent)}).fetchall()
    return [str(row[0]) for row in rows]


def get_account_uuid_for_sip_endpoint(agent_sip: str) -> str | None:
    db = SessionLocal()
    try:
        row = db.execute(_AOM_UUID_FOR_SIP_SQL, {"sip": agent_sip}).fetchone()
        return str(row[0]) if row else None
    except Exception as e:
        logger.error(f"get_account_uuid_for_sip_endpoint read error: {e}")
        raise
    finally:
        db.close()


@_reader("get_agent_sip_by_uuid read error")
def get_agent_sip_by_uuid(db, agent_uuid: str, auth_id: str) -> str | None:
    row = db.execute(_AGENT_BY_UUID_SQL, {"uuid": str(agent_uuid), "auth_id": auth_id}).fetchone()
    return str(row[0]) if row and row[0] is not None else None


@_reader("get_agent_by_uuid read error")
def get_agent_by_uuid(db, agent_uuid: str, auth_id: str) -> AgentDetails | None:
    row = db.execute(_AGENT_BY_UUID_SQL, {"uuid": str(agent_uuid), "auth_id": auth_id}).fetchone()
    if row is None:
        return None
    return AgentDetails(str(agent_uuid), row[0] or "", row[1] or "", _name(*row[2:]))


@_reader("get_agent_skills read error")
def get_agent_skills(db, agent_sip: str) -> list[AgentSkill] | None:
    sql = (
        "SELECT sk.uuid, ask.skill_level FROM account_org_mapping aom INNER JOIN aom_skills ask ON ask.aom=aom.id "
        "INNER JOIN skills sk ON sk.id=ask.skill WHERE aom.sip_endpoint = :agent"
    )
    rows = db.execute(text(sql), {"agent": agent_sip}).fetchall()
    return [AgentSkill(str(r[0]), int(r[1]) if r[1] is not None else None) for r in rows]


@_reader("get_aom_for_sip_endpoint read error")
def get_aom_for_sip_endpoint(db, agent_sip: str) -> str | None:
    row = db.execute(_AOM_UUID_FOR_SIP_SQL, {"sip": agent_sip}).fetchone()
    return str(row[0]) if row and row[0] is not None else None


_ROUTING_AGENTS_SQL = text(
    "SELECT aom.uuid FROM account_org_mapping aom INNER JOIN organization org ON org.id = aom.organization "
    "LEFT JOIN account acc ON acc.id = aom.account WHERE aom.status = 'active' AND (aom.uuid IN :uuids "
    "OR (org.auth_id = :auth_id AND acc.email IN :emails)) ORDER BY aom.id"
).bindparams(bindparam("uuids", expanding=True, type_=UUID()), bindparam("emails", expanding=True, type_=String()))


def get_agent_details_for_email_or_aom_id(
    routing_agents_email: list[str], routing_agents_uuid: list[str], auth_id: str
) -> tuple[list[str] | None, str | None]:
    """The active agents a queue routes to: listed by uuid, or by account email within the run's organization."""
    if not routing_agents_uuid and not routing_agents_email:
        return [], None
    return _routing_agents(list(routing_agents_uuid), list(routing_agents_email), auth_id)


@_reader("get_agent_details_for_email_or_aom_id error", (None, "error in getting agent details"))
def _routing_agents(db, uuids, emails, auth_id: str) -> tuple[list[str] | None, str | None]:
    params = {"uuids": uuids, "emails": emails, "auth_id": auth_id}
    return [str(r[0]) for r in db.execute(_ROUTING_AGENTS_SQL, params).fetchall()], None


@_reader("get_organisation_auth_token_and_record_value error", ("", False, "error fetching org record value"))
def get_organisation_auth_token_and_record_value(
    db, auth_id: str, call_inbound: bool = True
) -> tuple[str | None, bool, str | None]:
    sql = _TOKEN_INBOUND_REC_SQL if call_inbound else _TOKEN_OUTBOUND_REC_SQL
    row = db.execute(sql, {"auth_id": auth_id}).fetchone()
    return (
        (tenant_plivo(str(row[0]))[1], bool(row[1]), None) if row is not None else ("", False, "organization not found")
    )


@_reader("get_aom_row_for_sip error")
def get_aom_row_for_sip(db, agent_sip: str) -> AomRowForSip | None:
    sql = "SELECT aom.id, aom.uuid FROM account_org_mapping aom WHERE sip_endpoint = :sip"
    row = db.execute(text(sql), {"sip": agent_sip}).fetchone()
    return AomRowForSip(int(row[0]), str(row[1] or "")) if row is not None else None


@_reader("get_org_for_auth_id error")
def get_org_for_auth_id(db, auth_id: str) -> OrgForAuthId | None:
    sql = "SELECT id, name, uuid FROM organization WHERE auth_id = :auth_id AND is_deleted = false"
    row = db.execute(text(sql), {"auth_id": auth_id}).fetchone()
    if row is None:
        return None
    return OrgForAuthId(int(row[0]), str(row[1] or ""), tenant_plivo(str(row[2]))[1], auth_id, str(row[2]))


@_reader("get_team_names_for_sips_in_queue error")
def get_team_names_for_sips_in_queue(db, agents: list[str], qnr_run_uuid: str) -> list[str] | None:
    if not agents:
        return []
    sql = _in(
        "SELECT t.name, aom.sip_endpoint FROM account_org_mapping aom INNER JOIN aom_team at ON at.aom = aom.id "
        "INNER JOIN team t ON at.team = t.id INNER JOIN teams ts ON ts.team_id = CAST(t.uuid AS varchar) "
        "INNER JOIN active_queue_entry aqe ON aqe.id = ts.active_queue_entry_id "
        "WHERE aom.sip_endpoint IN :sips AND aqe.qnr_run_uuid = :qnr_run_uuid",
        "sips",
    )
    rows = db.execute(sql, {"sips": list(agents), "qnr_run_uuid": qnr_run_uuid}).fetchall()
    by_sip = {str(r[1]): str(r[0] or "") for r in rows}
    return [by_sip.get(agent, "") for agent in agents]


@_reader("get_organisation_record_value error", lambda e: (False, False, str(e)))
def get_organisation_record_value(db, auth_id: str, call_inbound: bool = True) -> tuple[bool, bool, str | None]:
    sql = _INBOUND_REC_PAUSE_SQL if call_inbound else _OUTBOUND_REC_PAUSE_SQL
    row = db.execute(sql, {"auth_id": auth_id}).fetchone()
    return (bool(row[0]), bool(row[1]), None) if row is not None else (False, False, rc.NOT_FOUND)


@_reader("get_account_rows_for_sip_endpoints error")
def get_account_rows_for_sip_endpoints(db, sips: list[str]) -> list[AomRowBySip] | None:
    if not sips:
        return []
    rows = db.execute(_ACCOUNT_BY_SIPS_SQL, {"sips": list(sips)}).fetchall()
    return [AomRowBySip(str(r[0]), str(r[1] or ""), r[2] or "", r[3] or "") for r in rows]


@_reader("get_allow_routing_in_acw_flag error", lambda e: (None, str(e)))
def get_allow_routing_in_acw_flag(db, auth_id: str) -> tuple[bool | None, str | None]:
    sql = (
        "SELECT rs.release_capacity_on_resolve FROM routing_settings AS rs "
        "INNER JOIN organization AS o ON o.id=rs.organization WHERE o.auth_id = :auth_id"
    )
    row = db.execute(text(sql), {"auth_id": auth_id}).fetchone()
    return (None, rc.NOT_FOUND) if row is None or row[0] is None else (bool(row[0]), None)


def contact_pair(cp) -> tuple[str, str]:
    """The tenant pair a contact record carries: it signs every Plivo call on that contact's call and MPC."""
    cp = cp or {}
    return cp.get("auth_id") or "", cp.get("auth_token") or ""
