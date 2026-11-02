from hs.admin.account import RoutingSettings
from hs.infra.log import logger
from hs.routing import account_read as ar
from hs.routing import acw as routing_acw
from hs.routing import constants as rc
from hs.routing.redis_store import RoutingRedis
from hs.routing.selection import ActiveAgentsOnCallAndACW, get_agents_for_contact, get_available_agents_by_capacity
from hs.routing.sticky import STRICT_WAITS, get_sticky_routing_agents, parse_queue_and_route_config


def get_agents(
    contact_uuid: str,
    contact_channel: str,
    auth_id: str,
    conversation_id: str,
    flow_run_uuid: str = "",
    qnr_run_uuid: str = "",
    *,
    transfer_to: str | None = None,
    transfer_type: str = "",
    skip_agents=(),
    contact_identifier: str = "",
    contact_params: dict | None = None,
    routing_redis=None,
) -> tuple[list[str] | None, str | None]:
    """The agents to invite, per the run's queue-and-route config (sticky agent first) or the transfer_to agent/team."""
    rr = routing_redis or RoutingRedis()
    skip_agents, skills, agent_routing_list = list(skip_agents), (), []
    settings = RoutingSettings.get_org_routing_settings(auth_id)
    if settings is None:
        logger.error(f"get_agents: org routing settings not found auth_id={auth_id}")
        return None, "error fetching org routing settings"
    channel_capacity = settings.capacity.get(contact_channel, 0)
    if transfer_to is not None:
        current_retry = rr.get_retry_val(flow_run_uuid, qnr_run_uuid) if contact_params is not None else 0
        teams, routing_type, agent_limit = [transfer_to], rc.SIMULTANEOUS_ROUTING, settings.simultaneous_agent_limit
    else:
        current_retry = rr.get_retry_val(flow_run_uuid, qnr_run_uuid)
        cfg, err = parse_queue_and_route_config(rr.get_queue_and_route_config(flow_run_uuid, qnr_run_uuid))
        if err is not None:
            logger.error(f"get_agents: queue routing config error flow={flow_run_uuid}: {err}")
            return None, err
        if current_retry != 0 and cfg.retry_limit >= 0 and current_retry > cfg.retry_limit:
            logger.info(
                f"get_agents: contact retry limit reached flow={flow_run_uuid} retry={current_retry} "
                f"limit={cfg.retry_limit}"
            )
            return None, "known_error: retry limit reached"
        outbound_sticky = bool(settings.outbound_sticky_routing.get("enabled"))
        args = (contact_uuid, cfg, contact_identifier, current_retry, contact_channel, channel_capacity, auth_id)
        sticky_agents, sticky_err = get_sticky_routing_agents(*args, outbound_sticky, conversation_id, rr)
        if sticky_err is not None:
            logger.error(f"get_agents: sticky agents not found contact={contact_uuid}: {sticky_err}")
            # an ACW fetch fault is infrastructure, not a strict-sticky outcome: voice retries it
            if sticky_err == routing_acw.ACW_FETCH_FAILED:
                return None, sticky_err
            if cfg.sticky_routing_type == rc.STICKY_STRICTLY_BIND:
                return ([], None) if sticky_err in STRICT_WAITS else (None, "known_error: %s" % sticky_err)
        if sticky_agents:
            return sticky_agents, None
        if current_retry > 0:
            skip_agents = rr.get_invited_agents(flow_run_uuid, qnr_run_uuid)
        teams, routing_type = list(cfg.routing_teams), cfg.routing or rc.SIMULTANEOUS_ROUTING
        skills, agent_limit = cfg.routing_skills_filter, cfg.agent_limit
        agent_routing_list = list(cfg.final_routing_agents)

    if transfer_to is not None and transfer_type == rc.TRANSFER_TYPE_AGENT:
        sip = ar.get_agent_sip_by_uuid(transfer_to, auth_id)
        if sip is None:
            logger.error(f"get_agents: agent not found {transfer_to}")
            return None, "agent not found"
        agents = [sip] if sip not in skip_agents else []
    else:
        try:
            agents = get_agents_for_contact(agent_routing_list, teams, skills, skip_agents, routing_type, auth_id)
        except Exception as e:
            logger.error(f"get_agents: agent query failed contact={contact_uuid}: {e}")
            return None, "error fetching agents for contact"
    logger.info(f"get_agents: {len(agents)} agents returned from db contact={contact_uuid}")
    if not agents:
        return agents, None
    acw = ActiveAgentsOnCallAndACW()
    if contact_channel == rc.CHANNEL_CALL:
        try:
            acw = routing_acw.get_agents_on_active_call_and_in_acw(auth_id, agents)
        except Exception as e:
            logger.error(f"Error While Fetching active agents on call and acw contact={contact_uuid}: {e}")
            return None, routing_acw.ACW_FETCH_FAILED
    try:
        agents = get_available_agents_by_capacity(
            agents, contact_channel, channel_capacity, agent_limit, auth_id, acw, conversation_id, routing_redis=rr
        )
    except Exception as e:
        logger.error(f"get_agents: lua capacity error contact={contact_uuid}: {e}")
        return None, "error filtering agents by capacity"
    return agents, None
