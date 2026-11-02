from hs.infra.log import logger
from hs.routing import account_read as ar
from hs.routing import constants as rc
from hs.routing.selection import ActiveAgentsOnCallAndACW

ACW_FETCH_FAILED = "error while filtering agents in acw"


class AcwFetchError(Exception):
    pass


def _filter_agents_in_acw_inprocess(agents: list[str]) -> list[str]:
    try:
        from hs.conversations import management as cm
        from hs.routing.invitation import _ensure_request_context

        _ensure_request_context()
        resp = cm.filter_active_agents_in_acw(list(agents), rc.CHANNEL_CALL)
    except Exception as e:
        logger.error(f"Filtering agents in ACW failed: {e}")
        raise AcwFetchError(ACW_FETCH_FAILED) from e
    if resp.error:
        logger.error(f"Error while filtering agents in acw agents={agents}: {resp.error}")
        raise AcwFetchError(ACW_FETCH_FAILED)
    return list(resp.data or [])


def get_agents_on_active_call_and_in_acw(auth_id: str, agents: list[str]) -> ActiveAgentsOnCallAndACW:
    allow_routing_in_acw, flag_err = ar.get_allow_routing_in_acw_flag(auth_id)
    if flag_err is not None:
        logger.error(f"Error while fetching flag: <ALLOW_ROUTING_IN_ACW>: {flag_err}")
        raise AcwFetchError(flag_err)
    in_acw = _filter_agents_in_acw_inprocess(agents)
    # Every occupied call agent counts as on a call and SIP-registered.
    on_call = registered = agents
    logger.info(
        f"Fetched MetaData agents_in_active_call={list(on_call)} agents_in_acw={in_acw} "
        f"allow_routing_in_acw={allow_routing_in_acw} agents_with_registered_sip={list(registered)}"
    )
    return ActiveAgentsOnCallAndACW(tuple(on_call), tuple(in_acw), bool(allow_routing_in_acw), tuple(registered))
