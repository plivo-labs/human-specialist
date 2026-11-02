import json

from hs.infra.log import logger
from hs.infra.redis import Cache
from hs.routing import constants as rc

UPDATE_AGENT_CAPACITY = """
local agent_key, unavailable_key, base = KEYS[1], KEYS[2], KEYS[3]
local capacity, agent, old, new = tonumber(ARGV[1]), ARGV[2], ARGV[3], ARGV[4]
if redis.call('exists', agent_key) == 0 then
    if old ~= '' then return '-4' end
    redis.call('sadd', agent_key, new)
    redis.call('sadd', base .. ':' .. new, agent)
    if capacity == 1 then redis.call('sadd', unavailable_key, agent) end
    return '0'
end
if old ~= '' then
    if redis.call('sismember', agent_key, old) == 0 then return '-1' end
    if redis.call('srem', agent_key, old) == 1 then
        redis.call('srem', base .. ':' .. old, agent)
        redis.call('srem', unavailable_key, agent)
    end
end
if new == '' then return '-3' end
-- a same-conversation replay is idempotent even at capacity, and repairs the derived indexes
if redis.call('sismember', agent_key, new) == 0 then
    if tonumber(redis.call('scard', agent_key)) >= capacity then return '-2' end
    redis.call('sadd', agent_key, new)
end
redis.call('sadd', base .. ':' .. new, agent)
if tonumber(redis.call('scard', agent_key)) >= capacity then redis.call('sadd', unavailable_key, agent) end
return '0'
"""
QUEUE_POSITION_WAIT_TIME_EXPIRY_HOURS = 25
# update_agents_capacity's answer when the release cannot find the old contact (Lua -1)
CAPACITY_OLD_CONTACT_NOT_FOUND = "failed to update capacity as old contact not found"
# decrease_agents_capacity answers meaning nothing was left to release (Lua -4 key gone / -1 contact absent):
# the desired end state for a release that can legitimately run twice
CAPACITY_ALREADY_CLEAR_ERRS = (
    "key not found while decreasing capacity",
    "failed to decrease capacity as contact not found",
)
_NOT_IN_CACHE = "data not found in cache"


def _json_dict(val) -> dict | None:
    """val's JSON object; None when absent, undecodable or not an object."""
    try:
        data = json.loads(val or "null")
    except (ValueError, TypeError):
        return None
    return data if isinstance(data, dict) else None


def _int(val) -> int:
    try:
        return int(val) if val is not None else 0
    except (TypeError, ValueError):
        return 0


class RoutingRedis:
    def __init__(self, cache: Cache | None = None):
        self._cache = cache or Cache.get_instance()

    def _run(self, src: str, keys=None, args=None):
        return self._cache.eval_script(src, keys=keys, args=args)

    def _set(self, key: str, value: str, ttl_seconds) -> bool:
        return self._cache.set(key, value, expiry_in_seconds=ttl_seconds or None)

    def _get_dict(self, key: str) -> dict | None:
        return _json_dict(self._cache.get(key))

    def update_agent_capacity(
        self,
        auth_id: str,
        agent: str,
        channel: str,
        org_capacity: int,
        old_contact: str = "",
        new_contact: str = "",
        ttl_seconds: int | None = None,
    ) -> str:
        keys = [
            rc.agent_capacity_key(auth_id, agent, channel),
            rc.org_channel_unavailability_key(auth_id, channel),
            rc.conversation_capacity_base_key(auth_id, channel),
        ]
        code = self._run(UPDATE_AGENT_CAPACITY, keys=keys, args=[str(org_capacity), agent, old_contact, new_contact])
        # Refresh the capacity-set TTL after every successful mutation: acquire returns "0", a pure release
        # "-3". Best-effort: an expire failure must not change `code`.
        if str(code) == "0" or (str(code) == "-3" and old_contact):
            ttl = rc.agent_capacity_ttl_seconds_for_channel(channel) if ttl_seconds is None else ttl_seconds
            if ttl and ttl > 0:
                try:
                    self._cache.expire(keys[0], ttl)
                except Exception as e:
                    logger.warning(
                        f"update_agent_capacity: best-effort capacity TTL refresh failed agent={agent} "
                        f"channel={channel}: {e}"
                    )
        return code

    def _capacity_error(self, errors: dict, *args) -> str | None:
        try:
            return errors.get(int(self.update_agent_capacity(*args)))
        except Exception as e:
            return str(e)

    def increase_agent_capacity(self, auth_id: str, agent: str, channel: str, org_capacity: int, conversation_id: str):
        try:
            code = int(self.update_agent_capacity(auth_id, agent, channel, org_capacity, "", conversation_id))
        except Exception as e:
            return -2, str(e)
        return code, ("error in increasing agent capacity" if code == -2 else None)

    def decrease_agents_capacity(self, auth_id: str, agent: str, channel: str, conversation_id: str):
        if not conversation_id:
            return "empty conversation id, failed to decrease capacity"
        errors = {-1: "failed to decrease capacity as contact not found", -4: "key not found while decreasing capacity"}
        return self._capacity_error(errors, auth_id, agent, channel, 0, conversation_id, "")

    def update_agents_capacity(
        self,
        auth_id: str,
        agent: str,
        channel: str,
        old_conversation_id: str,
        new_conversation_id: str,
        org_capacity: int,
    ):
        errors = {
            -1: CAPACITY_OLD_CONTACT_NOT_FOUND,
            -2: "failed to update capacity as capacity already occupied",
            -4: "failed to update capacity as key not present",
        }
        args = (auth_id, agent, channel, org_capacity, old_conversation_id, new_conversation_id)
        return self._capacity_error(errors, *args)

    # queue_uuid None is the outbound mapping, kept per tenant (auth_id)
    def get_sticky_agent(self, contact_identifier: str, queue_uuid: str | None = None, auth_id: str = ""):
        return self._cache.get(rc.sticky_agent_key(contact_identifier, queue_uuid, auth_id))

    def set_sticky_agent(
        self, contact_identifier: str, agent_uuid: str, ttl_seconds: int, queue_uuid=None, auth_id: str = ""
    ) -> bool:
        return self._set(rc.sticky_agent_key(contact_identifier, queue_uuid, auth_id), agent_uuid, ttl_seconds)

    def del_sticky_agent(self, contact_identifier: str, queue_uuid: str | None = None, auth_id: str = ""):
        return self._cache.delete(rc.sticky_agent_key(contact_identifier, queue_uuid, auth_id))

    def get_queue_and_route_config(self, flow_run_uuid: str, run_uuid: str) -> str | None:
        return self._cache.get(rc.queue_and_route_config_key(flow_run_uuid, run_uuid))

    def get_retry_val(self, flow_run_uuid: str, run_uuid: str) -> int:
        return _int(self._cache.get(rc.contact_retry_key(flow_run_uuid, run_uuid)))

    def get_invited_agents(self, flow_run_uuid: str, run_uuid: str) -> list[str]:
        return list(self._cache.hgetall(rc.invited_agents_key(flow_run_uuid, run_uuid)).keys())

    def get_contact_to_agent_mapping(self, contact_uuid: str) -> dict | None:
        return self._get_dict(rc.contact_to_agent_mapping_key(contact_uuid))

    def get_contact_parameters(self, contact_uuid: str, qnr_run_uuid: str) -> dict | None:
        return _json_dict(self._cache.hget(rc.contact_params_key(contact_uuid), qnr_run_uuid))

    def set_queue_and_route_config(self, flow_run_uuid: str, run_uuid: str, config_json: str, ttl_seconds: int) -> bool:
        return self._set(rc.queue_and_route_config_key(flow_run_uuid, run_uuid), config_json, ttl_seconds)

    def set_queue_config(self, flow_run_uuid: str, run_uuid: str, config_json: str, ttl_seconds: int) -> bool:
        return self._set(rc.queue_config_key(flow_run_uuid, run_uuid), config_json, ttl_seconds)

    def get_queue_config(self, flow_run_uuid: str, run_uuid: str):
        from hs.intake.models import QueueConfigModel

        val = self._cache.get(rc.queue_config_key(flow_run_uuid, run_uuid))
        if not val:
            return None, "queue config not found"
        try:
            return QueueConfigModel.model_validate(json.loads(val)), None
        except (ValueError, TypeError) as e:
            return None, str(e)

    def store_contact_params(self, contact_uuid: str, qnr_run_uuid: str, params_json: str) -> int | None:
        return self._cache.hset(rc.contact_params_key(contact_uuid), qnr_run_uuid, params_json)

    def delete_contact_to_agent_mapping(self, contact_uuid: str) -> int:
        return self._cache.delete(rc.contact_to_agent_mapping_key(contact_uuid))

    def set_transfer_in_progress(self, contact_uuid: str, info_json: str, ttl_seconds: int, nx: bool = False) -> bool:
        return self._cache.set(rc.transfer_in_progress_key(contact_uuid), info_json, ttl_seconds or None, nx=nx)

    def delete_transfer_in_progress(self, contact_uuid: str) -> None:
        if not self._cache.delete(rc.transfer_in_progress_key(contact_uuid)):
            logger.info(f"delete_transfer_in_progress: key not found in DB contact_uuid={contact_uuid}")

    def get_transfer_in_progress_info(self, contact_uuid: str) -> dict | None:
        return self._get_dict(rc.transfer_in_progress_key(contact_uuid))

    def is_transfer_in_progress(self, contact_uuid: str) -> tuple[bool, str | None]:
        info = self.get_transfer_in_progress_info(contact_uuid) or {}
        return (True, None) if info.get("transfer_type") else (False, _NOT_IN_CACHE)

    def set_to_be_primary_agent(self, qnr_run_uuid: str, mapping_json: str, ttl_seconds: int, nx: bool = False) -> bool:
        return self._cache.set(rc.mpc_to_be_primary_agent_key(qnr_run_uuid), mapping_json, ttl_seconds, nx=nx)

    def get_to_be_primary_agent(self, qnr_run_uuid: str) -> dict | None:
        return self._get_dict(rc.mpc_to_be_primary_agent_key(qnr_run_uuid))

    def delete_to_be_primary_agent(self, qnr_run_uuid: str) -> None:
        self._cache.delete(rc.mpc_to_be_primary_agent_key(qnr_run_uuid))

    def get_queue_position_and_wait_time(self, node_uuid: str) -> dict | None:
        return self._get_dict(rc.queue_position_wait_time_key(node_uuid))

    def set_queue_position_and_wait_time(self, node_uuid: str, data: dict, ttl_seconds: int) -> bool:
        return self._set(
            rc.queue_position_wait_time_key(node_uuid), json.dumps(data, separators=(",", ":")), ttl_seconds
        )

    def set_recompute_flag_for_queue_position_calculation(self, node_uuid: str, queue_uuid: str = "") -> None:
        if self.get_queue_position_and_wait_time(node_uuid) is not None:
            blank = {"estimated_wait_time_minutes": 0, "estimated_wait_time_seconds": 0, "queue_position": 0}
            ttl = QUEUE_POSITION_WAIT_TIME_EXPIRY_HOURS * 3600
            self.set_queue_position_and_wait_time(node_uuid, {**blank, "recompute": True}, ttl)

    def set_retry_val(self, flow_run_uuid: str, run_uuid: str):
        return self._cache.incrby(rc.contact_retry_key(flow_run_uuid, run_uuid), 1)

    def set_invited_agents(self, flow_run_uuid: str, run_uuid: str, agents, ttl_seconds: int, attempt_count) -> None:
        if not agents:
            return
        key = rc.invited_agents_key(flow_run_uuid, run_uuid)
        for agent in agents:
            self._cache.hset(key, agent, str(int(attempt_count or 0)))
        if ttl_seconds:
            self._cache.expire(key, ttl_seconds)

    def set_offer(self, run_uuid: str, agents, offer_id: str, ttl_seconds: int) -> str | None:
        """Each agent's open offer of this run: the id its leg's join and hangup events carry. Every offer made
        is kept beside it, to check a leg's own id by; one HSET writes both, so they never disagree."""
        key = rc.offers_key(run_uuid)
        fields = {k: v for agent in agents for k, v in ((agent, offer_id), (f"{agent}:{offer_id}", "1"))}
        ok = self._cache.hset(key, mapping=fields) is not None and self._cache.expire(key, ttl_seconds)
        return None if ok else "error while storing the offer"

    def get_offer(self, run_uuid: str, agent: str) -> str:
        return self._cache.hget(rc.offers_key(run_uuid), agent) or ""

    def is_offer_of(self, run_uuid: str, agent: str, offer_id: str | None) -> bool:
        return bool(offer_id) and bool(self._cache.hget(rc.offers_key(run_uuid), f"{agent}:{offer_id}"))

    def set_contact_to_agent_mapping(self, contact_uuid: str, mapping_json: str, ttl_seconds: int):
        ok = self._set(rc.contact_to_agent_mapping_key(contact_uuid), mapping_json, ttl_seconds)
        return None if ok else "error while storing contact to agent mapping"

    def get_reservation_timeout(self, auth_id: str, channel: str):
        cached = self._cache.get(rc.reservation_timeout_key(auth_id))
        if cached:
            try:
                return int(json.loads(cached).get(channel, 0) or 0) or rc.RESERVATION_TIMEOUT_DEFAULT_SECONDS, None
            except (ValueError, TypeError):
                return 0, "error decoding reservation timeout"
        from hs.admin.account import RoutingSettings

        settings = RoutingSettings.get_org_routing_settings(auth_id)
        if settings is None:
            return 0, "error fetching routing settings"
        reservation = settings.reservation_timeout or {}
        timeout = int(reservation.get(channel, 0) or 0) or rc.RESERVATION_TIMEOUT_DEFAULT_SECONDS
        cached = json.dumps(reservation, separators=(",", ":"))
        self._cache.set(
            rc.reservation_timeout_key(auth_id), cached, expiry_in_seconds=rc.RESERVATION_TIMEOUT_CACHE_TTL_SECONDS
        )
        return timeout, None

    def acquire_reverse_routing_lock_bool(self, agent_sip, conversation_id, channel) -> bool:
        key = rc.reverse_routing_lock_key(agent_sip, conversation_id, channel)
        return self._cache.set_if_absent(key, "1", rc.REVERSE_ROUTING_LOCK_TTL_SECONDS)

    def delete_reverse_routing_lock(self, agent_sip, conversation_id, channel):
        return self._cache.delete(rc.reverse_routing_lock_key(agent_sip, conversation_id, channel))
