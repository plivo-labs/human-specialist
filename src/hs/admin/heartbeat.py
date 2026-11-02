import time

from sqlalchemy.exc import SQLAlchemyError

from hs.infra import constants as non_env
from hs.infra.log import logger
from hs.infra.redis import Cache, HeartbeatPresenceCache

MISSING_AGENT_ERROR = "no agent on the heartbeat"
CACHE_ERROR = "error updating agent heartbeat state"


class HeartbeatReqMeta:
    def __init__(self, aom_uuid=None, org_uuid=None, api_id=None, org_name=None):
        self.aom_uuid, self.org_uuid, self.api_id, self.org_name = aom_uuid, org_uuid, api_id, org_name


def _run(background_tasks, fn, *args) -> None:
    if background_tasks is not None:
        background_tasks.add_task(fn, *args)
    else:
        fn(*args)


def handle_agent_ping(req_meta: HeartbeatReqMeta, background_tasks=None) -> str | None:
    if not req_meta.aom_uuid:
        return MISSING_AGENT_ERROR
    count = HeartbeatPresenceCache.incr_ping_count(req_meta.aom_uuid)
    if count is None or not HeartbeatPresenceCache.set_ping_time(req_meta.aom_uuid):
        return CACHE_ERROR
    if count == non_env.HEARTBEAT_PING_COUNT_ONLINE:
        _run(background_tasks, _online_edge_with_retry, req_meta)
    # the in-process sweeper flips the agent offline when pings stop; LST:<aom> already carries the TTL
    HeartbeatPresenceCache.mark_online(req_meta.aom_uuid, req_meta.org_uuid, req_meta.org_name)
    return None


def _online_edge_with_retry(req_meta: HeartbeatReqMeta) -> None:
    # The online edge fires once per session (count == N), so a failed status change would leave the agent
    # unroutable while pinging happily: roll the counter back one so the NEXT ping is the edge again. A
    # rollback, not a reconcile, so an agent who later picks Away by hand is never forced back online.
    if not presence_edge(req_meta, online=True):
        HeartbeatPresenceCache.set_ping_count(req_meta.aom_uuid, non_env.HEARTBEAT_PING_COUNT_ONLINE - 1)


def _ping_age(last_ping_str: str | None, now: int) -> int | None:
    try:
        return now - int(last_ping_str) if last_ping_str else None
    except (TypeError, ValueError):
        return None


def _is_ping_stale(last_ping_str: str | None) -> bool:
    age = _ping_age(last_ping_str, int(time.time()))
    return age is None or age >= non_env.HEARTBEAT_TIME_TO_MARK_OFFLINE


def sweep_offline_agents() -> int:
    """Flip every tracked-online agent whose ping went stale offline (the in-process sweeper's tick); idempotent,
    since presence_edge no-ops for an agent already offline."""
    flipped, online_agents = 0, HeartbeatPresenceCache.get_online_agents()
    if non_env.HEARTBEAT_DEBUG_LOG:
        now = int(time.time())
        ages = [f"{a}={_ping_age(HeartbeatPresenceCache.get_ping_time(a), now)}" for a in online_agents]
        logger.info(f"heartbeat sweep tick: online={len(online_agents)} last_ping_age_s=[{', '.join(ages)}]")
    for aom_uuid in online_agents:
        try:
            if not _is_ping_stale(HeartbeatPresenceCache.get_ping_time(aom_uuid)):
                continue
            if non_env.HEARTBEAT_DEBUG_LOG:
                logger.info(f"heartbeat sweep: flipping aom={aom_uuid} offline (ping stale)")
            # A mass-disconnect sweep could outlive the lock and let the next tick's replica sweep
            # concurrently: extend the existing lock first.
            _renew_sweep_lock()
            org_uuid, org_name = HeartbeatPresenceCache.get_presence_context(aom_uuid)
            if not presence_edge(
                HeartbeatReqMeta(aom_uuid=aom_uuid, org_uuid=org_uuid, org_name=org_name), online=False
            ):
                continue  # still online: the next tick retries
            HeartbeatPresenceCache.unmark_online(aom_uuid)
            flipped += 1
        except Exception as e:
            logger.error(f"heartbeat sweep: failed to process aom={aom_uuid} error: {e}")
    return flipped


def _renew_sweep_lock() -> None:
    # best-effort keepalive: a renewal failure never breaks the sweep (the lock just expires as before)
    try:
        Cache.get_instance().expire(
            non_env.HEARTBEAT_SWEEP_LOCK_KEY, max(1, non_env.HEARTBEAT_SWEEP_INTERVAL_SECONDS - 1)
        )
    except Exception as e:
        logger.error(f"heartbeat sweep: lock renewal failed (non-fatal): {e}")


def presence_edge(req_meta: HeartbeatReqMeta, online: bool) -> bool:
    """True when the agent is (or already was) in the desired state, or there is permanently nothing to do;
    False on a transient failure the caller should retry (availability/org-config read error, status write)."""
    from hs.admin.status import change_status
    from hs.routing.agent_status_read import get_status_uuid_for_org_event, is_agent_currently_available

    event = non_env.HEARTBEAT_EVENT_AGENT_LOGGED_IN if online else non_env.HEARTBEAT_EVENT_AGENT_LOGGED_OUT
    reason = non_env.HEARTBEAT_REASON_INTERNET_CONNECTED if online else non_env.HEARTBEAT_REASON_INTERNET_DISCONNECTED
    current_available = is_agent_currently_available(req_meta.aom_uuid)
    if current_available is None:
        return False
    if not online:
        # Session over: ALWAYS reset the counter, even when the agent already shows unavailable, so the next
        # session's pings reach the count==N online edge again.
        HeartbeatPresenceCache.reset_ping_count(req_meta.aom_uuid)
    if bool(current_available) == online:
        return True
    try:
        status_uuid = get_status_uuid_for_org_event(req_meta.org_uuid, event)
    except Exception as e:
        logger.error(f"heartbeat org-event-config read error: {e}")
        return False
    if not status_uuid:
        logger.info(
            f"heartbeat: no org status config for event={event} org={req_meta.org_uuid} "
            f"aom={req_meta.aom_uuid} — skipping the status change"
        )
        return True  # an org-config gap, not a transient: don't retry forever
    try:
        change_status(str(req_meta.aom_uuid), status_uuid, "heartbeat", reason)
    except LookupError as e:
        logger.error(f"heartbeat: {e} — nothing to change")
    except SQLAlchemyError as e:
        logger.error(f"heartbeat status change failed aom={req_meta.aom_uuid}: {e}")
        return False
    return True
