from typing import NamedTuple

from sqlalchemy import bindparam, text

from hs.infra.db import SessionLocal
from hs.infra.log import logger
from hs.routing import constants as rc
from hs.routing.redis_store import RoutingRedis


class ActiveAgentsOnCallAndACW(NamedTuple):
    active_agents_in_call: tuple = ()
    agents_in_acw: tuple = ()
    allow_routing_in_acw: bool = False
    agents_with_registered_sip: tuple = ()


def allow_agent_for_routing(
    channel: str,
    is_capacity_occupied: bool,
    acw: ActiveAgentsOnCallAndACW,
    agent_sip: str,
    check_sip_registration: bool = False,
) -> bool:
    registered = not check_sip_registration or agent_sip in acw.agents_with_registered_sip
    if not is_capacity_occupied:
        return channel != rc.CHANNEL_CALL or registered
    # an agent at capacity is still offered a call unless on one, unregistered, or in wrap-up where that is off
    if channel != rc.CHANNEL_CALL or agent_sip in acw.active_agents_in_call or not registered:
        return False
    return agent_sip not in acw.agents_in_acw or acw.allow_routing_in_acw


def get_available_agents_by_capacity(
    agents,
    channel_type: str,
    capacity: int,
    agent_limit: int,
    auth_id: str,
    acw: ActiveAgentsOnCallAndACW = None,
    conversation_id: str = "",
    routing_redis=None,
) -> list[str]:
    acw, capacity, agent_list = acw or ActiveAgentsOnCallAndACW(), capacity or 1, []
    if agent_limit <= 0:
        return agent_list
    rr = routing_redis or RoutingRedis()
    for agent in agents:
        code = int(
            rr.update_agent_capacity(
                auth_id, agent, channel_type, capacity, old_contact="", new_contact=conversation_id
            )
        )
        # the acquire code is the only signal telling a routable agent from one dropped for a held slot (-2)
        logger.info(
            f"capacity acquire: code={code} agent={agent} channel={channel_type} capacity={capacity} "
            f"conv={conversation_id}"
        )
        if allow_agent_for_routing(channel_type, code == -2, acw, agent):
            agent_list.append(agent)
            if len(agent_list) == agent_limit:
                break
    return agent_list


# the aom_routing_params columns each routing type selects and orders by
_ROUTING_COLUMNS = {
    rc.AVAILABILITY_ROUTING: ("available_since",),
    rc.CONVERSATIONS_HANDLED_ROUTING: ("contacts_handled", "available_since"),
    rc.CONVERSATIONS_HANDLE_TIME_ROUTING: ("contacts_handle_time", "available_since"),
}
_SKILLS_FROM = "LEFT JOIN aom_skills AS ask ON ask.aom=aom.id LEFT JOIN skills AS sk ON sk.id=ask.skill"
_TEAM_FROM = (
    "FROM account_org_mapping aom INNER JOIN aom_team AS at ON at.aom=aom.id INNER JOIN team AS t ON t.id=at.team "
    f"{_SKILLS_FROM} INNER JOIN org_statuses AS os ON os.id=aom.agent_status"
)
_AGENT_FROM = f"FROM account_org_mapping aom {_SKILLS_FROM} INNER JOIN org_statuses AS os ON os.id=aom.agent_status"
_IN_TENANT = " AND aom.organization = (SELECT id FROM organization WHERE auth_id = :auth_id AND is_deleted IS NOT TRUE)"


def build_get_agents_query(agent_routing_list, routing_teams, skills_filter, skip_agents, routing_type, auth_id):
    """One statement: the tenant's team members (active, available, in the teams) UNION its listed agents
    (available), each narrowed by the skills filter and the skip list, ordered by the routing type's metric
    columns. Every caller-supplied value is a bind parameter; a skill operator outside the comparison set is refused."""
    columns = _ROUTING_COLUMNS.get(routing_type, ())
    select = "SELECT " + ",".join(("sip_endpoint", *columns))
    distinct = "(SELECT DISTINCT aom.sip_endpoint as sip_endpoint" + "".join(f",arp.{c} as {c}" for c in columns)
    arp = "" if routing_type == rc.SIMULTANEOUS_ROUTING else " INNER JOIN aom_routing_params arp ON arp.aom=aom.id"
    params = {"teams": list(routing_teams), "agents": list(agent_routing_list), "skip": list(skip_agents)}
    params["auth_id"] = auth_id
    skills = []
    for i, rs in enumerate(skills_filter):
        op = rs.operator.strip()
        # an operator of only spaces is not "no level filter": refuse it like any other unknown operator
        if op not in rc.SKILL_OPERATORS or (rs.operator and not op):
            raise ValueError(f"invalid skill operator {rs.operator!r}")
        params[f"skill_{i}"], params[f"level_{i}"] = rs.skill, rs.value
        level = f" AND ask.skill_level{op}:level_{i}" if op else ""
        skills.append(f"(sk.uuid = :skill_{i}{level})")
    tail = (
        (f" AND ({' OR '.join(skills)})" if skills else "")
        + (" AND aom.sip_endpoint NOT IN :skip" if skip_agents else "")
        + _IN_TENANT
    )
    query = ""
    if routing_teams:
        head, union_start = (select, "From (") if agent_routing_list else (select + " from", "")
        query = (
            f"{head} {union_start} {distinct} {_TEAM_FROM}{arp} WHERE aom.status='active' and aom.sip_endpoint is not "  # sql: constants only
            f"NULL and os.availability = true AND t.uuid IN :teams {tail})"
        )
    if agent_routing_list:
        lead, head = ("UNION ", "") if routing_teams else ("", select + " from")
        query = (
            f"{query} {lead} {head} {distinct} {_AGENT_FROM}{arp} WHERE aom.uuid in :agents and aom.sip_endpoint "  # sql: constants only
            f"is not NULL and os.availability = true{tail})" + (")" if routing_teams else "")
        )
    query += " AS subquery"
    if columns:
        query = f"{query} ORDER BY {', '.join(columns)}"  # sql: constants only
    return query, {k: v for k, v in params.items() if f":{k}" in query}


def get_agents_for_contact(
    agent_routing_list, routing_teams, skills_filter, skip_agents, routing_type, auth_id
) -> list[str]:
    if not agent_routing_list and not routing_teams:
        return []  # a queue that names nobody keeps its callers waiting
    db = SessionLocal()
    try:
        query, params = build_get_agents_query(
            agent_routing_list or [], routing_teams or [], skills_filter or [], skip_agents or [], routing_type, auth_id
        )
        lists = [bindparam(name, expanding=True) for name, value in params.items() if isinstance(value, list)]
        return [row[0] for row in db.execute(text(query).bindparams(*lists), params).fetchall()]
    except Exception as e:
        logger.error(f"get_agents_for_contact query error: {e}")
        raise
    finally:
        db.close()
