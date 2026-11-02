import json

from hs.admin.account import RoutingSettings
from hs.infra.log import logger
from hs.intake.models import QueueAndRouteConfigModel
from hs.routing import account_read as ar
from hs.routing import acw as routing_acw
from hs.routing import constants as rc
from hs.routing.redis_store import RoutingRedis
from hs.routing.selection import ActiveAgentsOnCallAndACW, get_available_agents_by_capacity

# a strict-sticky call waits for its agent in these cases, and fails in any other
STRICT_WAITS = (
    "sticky routing: agent status is marked as unavailable",
    "sticky routing: cached agent capacity not available",
)


def parse_queue_and_route_config(raw: str | None) -> tuple[QueueAndRouteConfigModel | None, str | None]:
    if raw is None:
        return None, rc.CACHE_MISS
    if raw == "":
        return None, "queue routing config not found"
    try:
        d = json.loads(raw)
    except (ValueError, TypeError) as e:
        return None, "queue routing config is not valid JSON: %s" % e
    if not isinstance(d, dict):
        return None, "queue routing config is not an object"
    return QueueAndRouteConfigModel.model_validate(d), None


def is_agent_in_routing_assignment(qnr_config: QueueAndRouteConfigModel, agent: str) -> tuple[bool, str | None]:
    if agent in qnr_config.final_routing_agents:
        return True, None
    teams = ar.get_agent_teams(agent, "uuid")
    if teams is None:
        return False, "sticky routing : No teams found for agent"
    return any(t in qnr_config.routing_teams for t in teams), None


def get_sticky_routing_agents(
    contact_uuid: str,
    qnr_config: QueueAndRouteConfigModel,
    contact_identifier: str,
    retry_count: int,
    channel_type: str,
    capacity: int,
    auth_id: str,
    outbound_sticky_enabled: bool,
    conversation_id: str,
    rr: RoutingRedis,
) -> tuple[list[str] | None, str | None]:
    queue_uuid, sticky_type = qnr_config.queue, qnr_config.sticky_routing_type
    where = f"contact={contact_uuid} queue={queue_uuid}"
    if sticky_type == rc.STICKY_NO_STICKY_ROUTING or channel_type == rc.CHANNEL_EMAIL:
        logger.info(f"sticky routing: no sticky routing {where}")
        return None, None
    agent = rr.get_sticky_agent(contact_identifier, auth_id=auth_id) if outbound_sticky_enabled else ""
    queue = None if agent else queue_uuid  # the outbound mapping has no queue
    if outbound_sticky_enabled and not agent:
        logger.info(f"sticky routing: no outbound sticky info {where}")
    if not agent:
        agent = rr.get_sticky_agent(contact_identifier, queue_uuid, auth_id)
        if not agent:
            logger.info(f"sticky routing: no inbound sticky info {where}")
            return None, None

    def refuse(err_msg):
        logger.error(f"{err_msg} contact={contact_uuid} agent={agent}")
        return None, err_msg

    if retry_count > 0 and sticky_type == rc.STICKY_STRICTLY_BIND:
        return refuse("sticky routing: strictly bind, assignment failed as retry > 0")
    if retry_count > 0 and sticky_type == rc.STICKY_LOOSELY_BIND:
        logger.info(f"sticky routing: ignoring sticky as retry > 0 contact={contact_uuid}")
        return None, None
    info = ar.get_agent_with_status_info(agent)
    if info is None:
        return refuse("sticky routing: unable to get agent availability")
    agent_sip, agent_status, available = info
    if agent_status != "active":
        logger.info(f"sticky routing: agent not active contact={contact_uuid} agent={agent}")
        rr.del_sticky_agent(contact_identifier, queue, auth_id)
        return None, None
    if not available:
        return refuse(STRICT_WAITS[0])
    in_assignment, err = is_agent_in_routing_assignment(qnr_config, agent)
    if err is not None:
        logger.error(f"sticky routing: unable to get sticky agent teams contact={contact_uuid}: {err}")
        return None, err
    if not in_assignment:
        logger.error(f"sticky routing: agent/team not same as initial queue config contact={contact_uuid}")
        return None, None
    acw = ActiveAgentsOnCallAndACW()
    if channel_type == rc.CHANNEL_CALL:
        try:
            acw = routing_acw.get_agents_on_active_call_and_in_acw(auth_id, [agent_sip])
        except Exception as e:
            logger.error(
                f"sticky routing: Error While Fetching active agents on call and acw contact={contact_uuid}: {e}"
            )
            return None, routing_acw.ACW_FETCH_FAILED
    try:
        agents = get_available_agents_by_capacity(
            [agent_sip], channel_type, capacity, 1, auth_id, acw=acw, conversation_id=conversation_id, routing_redis=rr
        )
    except Exception as e:
        logger.error(f"sticky routing: error getting capacity for agent contact={contact_uuid}: {e}")
        return None, "sticky routing: error in getting capacity for agent"
    return (agents, None) if agents else refuse(STRICT_WAITS[1])


def set_outbound_sticky_mapping(agent_uuid: str, contact_identifier: str, auth_id: str) -> str | None:
    if (agent_uuid or "").startswith("sip:"):
        try:
            resolved = ar.get_account_uuid_for_sip_endpoint(agent_uuid)
        except Exception as e:
            logger.error(f"set_outbound_sticky_mapping: error fetching agent uuid from sip {agent_uuid}: {e}")
            return "error while fetching agent uuid from agent sip"
        if resolved is None:
            raise IndexError("agent sip matched no aom row")
        agent_uuid = resolved
    settings = RoutingSettings.get_org_routing_settings(auth_id)
    if settings is None:
        logger.error(f"set_outbound_sticky_mapping: error fetching routing settings auth_id={auth_id}")
        return "error while fetching routing settings"
    osr = settings.outbound_sticky_routing or {}
    if not osr.get("enabled"):
        return None
    expiry = osr.get("expiry") or {}
    ttl = rc.create_sticky_routing_cache_seconds(
        bool(expiry.get("no_expiry")),
        expiry.get("expiry_time_unit", "") or "",
        int(expiry.get("expiry_time_value") or 0),
    )
    if not RoutingRedis().set_sticky_agent(contact_identifier, agent_uuid, ttl, auth_id=auth_id):
        logger.error(f"set_outbound_sticky_mapping: cache set failure agent={agent_uuid}")
        return "error while setting outbound sticky routing in cache"
    return None


def set_inbound_sticky_mapping(
    agent_uuid: str, contact_identifier: str, qnr_config: QueueAndRouteConfigModel, auth_id: str
) -> str | None:
    rr, cfg = RoutingRedis(), qnr_config
    ttl = rc.create_sticky_routing_cache_seconds(
        cfg.sticky_routing_expiry, cfg.sticky_routing_expiry_unit, cfg.sticky_routing_expiry_time
    )
    if not rr.set_sticky_agent(contact_identifier, agent_uuid, ttl, cfg.queue, auth_id):
        logger.error("set_inbound_sticky_mapping: cache set failure")
        return "error in storing agent sticky routing in cache"
    if rr.del_sticky_agent(contact_identifier, auth_id=auth_id) is None:
        logger.error("set_inbound_sticky_mapping: outbound del failure")
        return "error in deleting outbound sticky routing agent in cache"
    return None
