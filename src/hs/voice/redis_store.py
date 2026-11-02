import json
import time as _time

from hs.infra.log import logger
from hs.infra.redis import Cache
from hs.voice import constants as vc

_TTL = vc.EXPIRY_DURATION_IN_HOURS_CALL * 3600

# KEYS[1] event hash; ARGV: event, claim value, repeatable ('1' overwrites), ttl
SET_MPC_EVENT_WITH_TTL = """
local repeatable = ARGV[3] == '1'
if repeatable then redis.call('HSET', KEYS[1], ARGV[1], ARGV[2]) end
local fresh = repeatable or redis.call('HSETNX', KEYS[1], ARGV[1], ARGV[2]) == 1
redis.call('EXPIRE', KEYS[1], ARGV[4])
return fresh and 'success' or 'value already exists'
"""
# ARGV: event, its sequence, the later event that supersedes it, claim value, ttl. A later event
# field, or one of its "<later>:<seq>" family with a greater (string-compared) sequence, wins.
MPC_CUR_AND_LATER_EVENT_DEPENDENCY = """
local key, seq, later = KEYS[1], ARGV[2], ARGV[3]
local function done(result)
    redis.call('EXPIRE', key, ARGV[5])
    return result
end
if redis.call('HGET', key, later) then return done('later event found') end
if seq ~= '' and later ~= '' then
    local fields = redis.call('HGETALL', key)
    for i = 1, #fields, 2 do
        if string.sub(fields[i], 1, #later) == later then
            local last
            for token in string.gmatch(fields[i], '[^:]+') do last = token end
            if last > seq then return done('later event found') end
        end
    end
end
if redis.call('HSETNX', key, ARGV[1], ARGV[4]) == 0 then return done('value already exists') end
return done('success')
"""
RELEASE_MPC_EVENT_CLAIM = """
if redis.call('HGET', KEYS[1], ARGV[1]) == ARGV[2] then return redis.call('HDEL', KEYS[1], ARGV[1]) end
return 0
"""
# Deletes the mapping only while it still names ARGV[1]: '0' deleted, '-2' names another, '-1' absent.
DELETE_MAPPING_NAMING = """
local mapping = redis.call('GET', KEYS[1])
if not mapping then return '-1' end
if not string.find(mapping, ARGV[1], 1, true) then return '-2' end
redis.call('DEL', KEYS[1])
return '0'
"""


def mpc_call_events_key(call_uuid: str, qnr_run_uuid: str) -> str:
    if qnr_run_uuid:
        return f"{vc.MPC_VOICE_EVENT_KEY}:{call_uuid}:{qnr_run_uuid}"
    return f"{vc.MPC_VOICE_EVENT_KEY}:{call_uuid}"


def mpc_calls_key(flow_run_uuid: str, qnr_run_uuid: str) -> str:
    return f"MPCCALLSKEY:{flow_run_uuid}:{qnr_run_uuid}"


def mpc_transfer_calls_key(qnr_run_uuid: str) -> str:
    return f"MTCK:{qnr_run_uuid}"


def active_conference_key(qnr_run_uuid: str) -> str:
    return f"ACTIVECONFERENCE:{qnr_run_uuid}"


def prompt_param_key(call_uuid: str) -> str:
    return f"PROMPT:{call_uuid}"


def supervisor_to_call_key(supervisor_uuid: str) -> str:
    return f"SUPCM:{supervisor_uuid}"


def call_to_supervisor_key(call_uuid: str) -> str:
    return f"CSUPM:{call_uuid}"


def current_participant_key(contact_uuid: str) -> str:
    return f"CAP:{contact_uuid}"


def agent_switch_key(qnr_run_uuid: str, agent_sip: str) -> str:
    return f"AS:{qnr_run_uuid}:{agent_sip}"


class VoiceRedis:
    def __init__(self, cache: Cache | None = None):
        self._cache = cache or Cache.get_instance()

    def _eval(self, src: str, keys: list[str], args: list[str]):
        return self._cache.client.eval(src, len(keys), *keys, *args)

    def _try(self, label: str, op) -> str | None:
        try:
            op()
            return None
        except Exception as e:
            logger.error(f"{label}: {e}")
            return str(e)

    def _set_json(self, key: str, value: dict, failure: str) -> str | None:
        payload = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        return None if self._cache.set(key, payload, expiry_in_seconds=_TTL) else failure

    def _get_json(self, key: str, failure: str, missing: str | None = None) -> tuple[dict | None, str | None]:
        """(decoded value, None), or (None, missing or failure) when absent and (None, failure) when undecodable."""
        val = self._cache.get(key)
        if not val:
            return None, missing or failure
        try:
            return json.loads(val), None
        except (TypeError, ValueError) as e:
            return None, failure or str(e)

    def _add_members(self, key: str, members: list[str], expiry_seconds: int, label: str) -> str | None:
        def op():
            if members:
                self._cache.client.sadd(key, *members)
            self._cache.client.expire(key, expiry_seconds)

        return self._try(f"{label} error key={key}", op)

    def _delete_mapping(self, key: str, member: str, label: str) -> str | None:
        try:
            val = self._eval(DELETE_MAPPING_NAMING, [key], [member])
        except Exception as e:
            logger.error(f"{label} error: {e}")
            return str(e)
        return None if str(val) == "0" else "don't clear the cache"

    def set_mpc_callback_event(self, call_uuid: str, qnr_run_uuid: str, event: str, ts: str = "") -> str | None:
        key = mpc_call_events_key(call_uuid, qnr_run_uuid)
        repeatable = "1" if event in vc.REPEATABLE_EVENTS else "0"
        args = [event, ts or str(_time.time_ns()), repeatable, str(_TTL)]
        try:
            val = self._eval(SET_MPC_EVENT_WITH_TTL, [key], args)
        except Exception as e:
            logger.error(f"set_mpc_callback_event error key={key} event={event}: {e}")
            return str(e)
        if val == "success":
            return None
        return val if val == vc.VALUE_EXISTS_ERROR else "error while storing event in redis"

    def room_ended(self, call_uuid: str, qnr_run_uuid: str) -> bool:
        """HS asked the room to end, or Plivo reported its end. A Redis failure raises: it says neither."""
        key = mpc_call_events_key(call_uuid, qnr_run_uuid)
        return any(self._cache.client.hmget(key, [vc.MPC_END_INITIATED, vc.MPC_END_EVENT]))

    def get_mpc_callback_event(self, call_uuid: str, qnr_run_uuid: str, event_name: str) -> tuple[str, str | None]:
        key = mpc_call_events_key(call_uuid, qnr_run_uuid)
        try:
            val = self._cache.client.hget(key, event_name)
        except Exception as e:
            logger.error(f"get_mpc_callback_event error key={key}: {e}")
            return "", str(e)
        return (val, None) if val else ("", "event not found in cache")

    def release_mpc_callback_event(self, call_uuid: str, qnr_run_uuid: str, event: str, claim: str) -> str | None:
        """Compare-and-delete only this worker's claim: a repeatable event may hold a newer one."""
        key = mpc_call_events_key(call_uuid, qnr_run_uuid)
        try:
            released = self._eval(RELEASE_MPC_EVENT_CLAIM, [key], [event, claim])
        except Exception as e:
            logger.error(f"release_mpc_callback_event error key={key} event={event}: {e}")
            return str(e)
        return None if int(released or 0) == 1 else "event claim no longer owned"

    def set_mpc_event_with_dependency(
        self,
        call_uuid: str,
        qnr_run_uuid: str,
        event_name: str,
        sequence_number: str = "",
        dependent_event: str = "",
        timestamp: str = "",
    ) -> str | None:
        key = mpc_call_events_key(call_uuid, qnr_run_uuid)
        args = [event_name, sequence_number, dependent_event, timestamp or str(_time.time_ns()), str(_TTL)]
        try:
            val = self._eval(MPC_CUR_AND_LATER_EVENT_DEPENDENCY, [key], args)
        except Exception as e:
            logger.error(f"set_mpc_event_with_dependency error key={key}: {e}")
            return "error while storing event in redis"
        if val in (vc.LATER_EVENT_FOUND_ERROR, vc.VALUE_EXISTS_ERROR):
            return val
        return None if val else "error while storing event in redis"

    def set_mpc_call_uuids(self, flow_run_uuid: str, qnr_run_uuid: str, uuids: list[str], ttl: int) -> str | None:
        key = mpc_calls_key(flow_run_uuid, qnr_run_uuid)
        return self._add_members(key, uuids, ttl, "set_mpc_call_uuids")

    def get_mpc_call_uuids(self, flow_run_uuid: str, qnr_run_uuid: str) -> list[str]:
        return self._cache.smembers(mpc_calls_key(flow_run_uuid, qnr_run_uuid))

    def set_transfer_mpc_call_uuids(self, qnr_run_uuid: str, call_uuids: list[str], ttl: int) -> str | None:
        key = mpc_transfer_calls_key(qnr_run_uuid)
        return self._add_members(key, call_uuids, ttl, "set_transfer_mpc_call_uuids")

    def get_transfer_mpc_call_uuids(self, qnr_run_uuid: str) -> list[str]:
        return self._cache.smembers(mpc_transfer_calls_key(qnr_run_uuid))

    def delete_transfer_mpc_call_uuids(self, qnr_run_uuid: str) -> None:
        self._cache.delete(mpc_transfer_calls_key(qnr_run_uuid))

    def toggle_active_conference(self, qnr_run_uuid: str, is_active: str, expiry_seconds: int) -> str | None:
        ok = self._cache.set(active_conference_key(qnr_run_uuid), is_active, expiry_in_seconds=expiry_seconds + 1)
        return None if ok else "error while storing active conference to redis"

    def is_conference_active(self, qnr_run_uuid: str) -> tuple[bool, str | None]:
        """A missing / "false" flag is a terminal no-op (prompt and digit callbacks race MPC teardown);
        a Redis failure stays distinguishable, and retryable, for the caller."""
        val, err = self._cache.get_with_error(active_conference_key(qnr_run_uuid))
        return bool(err is None and val and val != "false"), err

    def get_prompt_parameters(self, call_uuid: str) -> tuple[dict | None, str | None]:
        return self._get_json(prompt_param_key(call_uuid), "", missing="prompt params not found")

    def set_prompt_parameters(self, call_uuid: str, prompt_params: dict) -> str | None:
        return self._set_json(prompt_param_key(call_uuid), prompt_params, "error while storing prompt details to redis")

    def set_supervisor_to_call_mapping(self, supervisor_uuid: str, call_mapping: dict) -> str | None:
        key = supervisor_to_call_key(supervisor_uuid)
        return self._set_json(key, call_mapping, "could not set supervisor to call mapping")

    def get_supervisor_to_call_mapping(self, supervisor_uuid: str) -> tuple[dict | None, str | None]:
        return self._get_json(supervisor_to_call_key(supervisor_uuid), "could not get supervisor to call mapping")

    def delete_supervisor_to_call_mapping(self, supervisor_uuid: str, call_uuid: str) -> str | None:
        key = supervisor_to_call_key(supervisor_uuid)
        return self._delete_mapping(key, call_uuid, "delete_supervisor_to_call_mapping")

    def set_call_to_supervisor_mapping(self, call_uuid: str, supervisor_mapping: dict) -> str | None:
        failure = "error while setting call to supervisor mapping from cache"
        return self._set_json(call_to_supervisor_key(call_uuid), supervisor_mapping, failure)

    def get_call_to_supervisor_mapping(self, call_uuid: str) -> tuple[dict | None, str | None]:
        return self._get_json(call_to_supervisor_key(call_uuid), "could not get call to supervisor mapping")

    def delete_call_to_supervisor_mapping(self, call_uuid: str, supervisor_uuid: str) -> str | None:
        key = call_to_supervisor_key(call_uuid)
        return self._delete_mapping(key, supervisor_uuid, "delete_call_to_supervisor_mapping")

    def set_supervisor_leg(self, call_uuid: str, supervisor_uuid: str, leg: str) -> str | None:
        ok = self._cache.set(f"{vc.SUPERVISOR_LEG}:{call_uuid}:{supervisor_uuid}", leg, expiry_in_seconds=_TTL)
        return None if ok else "error while storing the supervisor leg"

    def get_supervisor_leg(self, call_uuid: str, supervisor_uuid: str) -> tuple[str, str | None]:
        leg, err = self._cache.get_with_error(f"{vc.SUPERVISOR_LEG}:{call_uuid}:{supervisor_uuid}")
        return leg or "", err

    def set_current_participant(self, contact_uuid: str, participant: str) -> str | None:
        return self._add_members(current_participant_key(contact_uuid), [participant], _TTL, "set_current_participant")

    def remove_current_participant(self, contact_uuid: str, member: str) -> str | None:
        key = current_participant_key(contact_uuid)
        return self._try(f"remove_current_participant error key={key}", lambda: self._cache.client.srem(key, member))

    def get_current_participant_count(self, contact_uuid: str) -> int:
        try:
            return int(self._cache.client.scard(current_participant_key(contact_uuid)))
        except Exception as e:
            logger.error(f"get_current_participant_count error: {e}")
            return -1

    def set_agent_switch_role(self, qnr_run_uuid: str, agent_sip: str, value: str) -> str | None:
        ok = self._cache.set(agent_switch_key(qnr_run_uuid, agent_sip), value, expiry_in_seconds=_TTL)
        return None if ok else "error while storing agent switch role to redis"

    def get_agent_switch_role(self, qnr_run_uuid: str, agent_sip: str) -> tuple[str, str | None]:
        val = self._cache.get(agent_switch_key(qnr_run_uuid, agent_sip))
        return ("", "agent switch role not found") if val is None else (val, None)

    def stop_recording_for_call(self, call_uuid: str) -> str | None:
        key = f"ACR:{call_uuid}"
        return self._try(f"stop_recording_for_call error call_uuid={call_uuid}", lambda: self._cache.client.delete(key))
