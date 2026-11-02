import time

from hs.admin.account import RoutingSettings
from hs.infra.log import logger
from hs.routing import account_read as ar
from hs.routing.redis_store import CAPACITY_ALREADY_CLEAR_ERRS, RoutingRedis

# A transient failure of the inline release on conversation close would strand capacity (the agent stays busy)
# with no retry: bound a short retry and, on exhaustion, log a loud STRANDED warning so it is alertable.
_RELEASE_MAX_ATTEMPTS = 3
_RELEASE_BACKOFF_SECONDS = 0.05


def _release_capacity_with_retry(
    rr: RoutingRedis, auth_id: str, agent: str, channel: str, conversation_number: str
) -> str | None:
    """Release one capacity holder with a bounded retry; warn (no raise) on final failure."""
    where = f"org={auth_id} agent={agent} channel={channel} conv={conversation_number}"
    last_err: str | None = None
    for attempt in range(1, _RELEASE_MAX_ATTEMPTS + 1):
        try:
            err = rr.decrease_agents_capacity(auth_id, agent, channel, conversation_number)
        except Exception as e:  # an unexpected raise is a retryable failure
            err = str(e) or e.__class__.__name__
        if not err:
            return None
        if err in CAPACITY_ALREADY_CLEAR_ERRS:
            # nothing left to release is the desired end state of an idempotent re-run, not a strand
            logger.info(f"_release_capacity_with_retry: already clear {where} ({err})")
            return None
        last_err = err
        if attempt < _RELEASE_MAX_ATTEMPTS:
            logger.info(
                f"_release_capacity_with_retry: release failed, retrying "
                f"attempt={attempt}/{_RELEASE_MAX_ATTEMPTS} {where} err={err}"
            )
            time.sleep(_RELEASE_BACKOFF_SECONDS * attempt)
    logger.warning(
        f"_release_capacity_with_retry: STRANDED CAPACITY — release failed after {_RELEASE_MAX_ATTEMPTS} "
        f"attempts {where} err={last_err} (agent may remain busy; manual reconcile may be required)"
    )
    return last_err


def increase_agent_capacity(agent_sip_endpoint: str, channel: str, conversation_number: str) -> tuple[str, str | None]:
    org = ar.get_org_details_for_agent_sip(agent_sip_endpoint)
    if org is None:
        return "", "could not get organization details"
    routing = RoutingSettings.get_org_routing_settings(org.auth_id)
    if routing is None:
        return "", "error while fetching routing settings"
    capacity = routing.capacity.get(channel, 0)
    err = RoutingRedis().increase_agent_capacity(
        org.auth_id, agent_sip_endpoint, channel, capacity, conversation_number
    )[1]
    return ("", err) if err else ("Successfully increased agent capacity", None)


def release_reservation_best_effort(
    rr: RoutingRedis, agent_sip: str, channel: str, conversation_number: str
) -> str | None:
    """Last-resort direct release of one agent's capacity member when the reverse-routing vehicle could not run
    to completion (lock contention, mid-flight raise), so the reservation can't strand until the set-TTL
    backstop. Safe after a release that already happened: releasing an absent member is a no-op."""
    if not conversation_number:
        return "empty conversation id"
    org = ar.get_org_details_for_agent_sip(agent_sip)
    if org is None:
        logger.error(
            f"release_reservation_best_effort: org lookup failed sip={agent_sip} conv={conversation_number} "
            f"channel={channel} — reservation may strand"
        )
        return "could not get organization details"
    return _release_capacity_with_retry(rr, org.auth_id, agent_sip, channel, conversation_number)
