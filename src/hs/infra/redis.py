import threading
import time

from hs.infra.log import logger
from hs.infra.settings import settings


def _err_str(e: Exception) -> str:
    """Return a non-empty error message for error-returning cache methods."""
    try:
        return str(e) or repr(e) or type(e).__name__ or "unknown error"
    except Exception:
        return "unknown error"


class Cache:
    _instance = None
    _lock = threading.Lock()

    def __init__(self):
        self._client = None
        self._scripts = {}

    @classmethod
    def get_instance(cls) -> "Cache":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    # Bound connection and read waits in both Redis modes; do not transport-retry non-idempotent Lua calls.
    # Idle connection health checks apply only where the client supports them.
    _SOCKET_CONNECT_TIMEOUT_SECONDS = 2
    _SOCKET_TIMEOUT_SECONDS = 5
    # Renewal uses one direct EVAL command (no EVALSHA/NOSCRIPT fallback), so
    # a reconnect plus the command read are bounded by these two socket limits.
    # retry_on_timeout remains disabled by the client default.
    RENEWAL_COMMAND_TIMEOUT_SECONDS = _SOCKET_CONNECT_TIMEOUT_SECONDS + _SOCKET_TIMEOUT_SECONDS
    _CONN_KWARGS = {
        "socket_connect_timeout": _SOCKET_CONNECT_TIMEOUT_SECONDS,
        "socket_timeout": _SOCKET_TIMEOUT_SECONDS,
        "health_check_interval": 30,
    }

    @property
    def client(self):
        if self._client is None:
            import redis as redis_lib

            self._client = redis_lib.Redis.from_url(settings.redis_url, decode_responses=True, **self._CONN_KWARGS)
        return self._client

    def __validate(self, key="default_valid") -> bool:
        return self.client and key

    def get_status(self) -> bool:
        try:
            return bool(self.client.ping())
        except Exception as e:
            logger.error(f"redis ping failed: {e}")
            return False

    def set(self, key: str, value: str, expiry_in_seconds=None, nx=False) -> bool:
        try:
            if not self.__validate(key=key):
                return False
            # Propagate the client result so NX semantics survive: redis-py returns None when
            # nx=True and the key already exists (set NOT performed). Discarding it and returning
            # True unconditionally defeated single-replica lock election (HBSWEEP:lock). For a
            # plain SET (nx=False) the client returns True on success, so this is unchanged.
            return bool(self.client.set(key, value, ex=expiry_in_seconds, nx=nx))
        except Exception as e:
            logger.error(f"error in redis set : {e}")
            return False

    _DELETE_IF_VALUE_MATCHES_LUA = (
        "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end"
    )

    def delete_if_value_matches_with_error(self, key: str, value: str):
        """delete_if_value_matches, but a Redis dependency failure is reported separately from
        a legitimate compare miss (claim re-owned or expired) — the single-flight release must
        retry outages and page on exhaustion, never mistake them for a miss: a swallowed False
        would strand the claim for its lease."""
        try:
            if not self.__validate(key=key):
                return False, "invalid redis key"
            return bool(self.eval_script(self._DELETE_IF_VALUE_MATCHES_LUA, keys=[key], args=[value])), None
        except Exception as e:
            err = _err_str(e)
            logger.error(f"error in redis delete_if_value_matches : {err}")
            return False, err

    def get(self, key: str) -> str:
        try:
            if not self.__validate(key=key):
                return None
            return self.client.get(key)
        except Exception as e:
            logger.error(f"error in redis get : {e}")
            return None

    def get_with_error(self, key: str):
        """Return a cache miss separately from a Redis dependency failure."""
        try:
            if not self.__validate(key=key):
                return None, "invalid redis key"
            return self.client.get(key), None
        except Exception as e:
            err = _err_str(e)
            logger.error(f"error in redis get : {err}")
            return None, err

    def persist(self, key: str) -> bool:
        """Ensure a present key has no TTL and verify the resulting state."""
        try:
            if not self.__validate(key=key):
                return False
            if self.client.persist(key):
                return True
            # Redis returns False when the key was already persistent as well
            # as when it is absent. TTL disambiguates those outcomes.
            return self.client.ttl(key) == -1
        except Exception as e:
            logger.error(f"error in redis persist : {e}")
            return False

    def set_if_absent(self, key: str, value: str, expiry_seconds: int) -> bool:
        """SET key value NX EX — returns True iff this caller claimed the key."""
        return self.set_if_absent_with_error(key, value, expiry_seconds)[0]

    def set_if_absent_with_error(self, key: str, value: str, expiry_seconds: int):
        """set_if_absent, but a Redis failure is reported separately from a
        lost claim — callers that must not misread an outage as a duplicate
        (the inbound dedupe) decide their own fallback."""
        try:
            if not self.__validate(key=key):
                return False, "invalid redis key"
            return bool(self.client.set(key, value, ex=expiry_seconds, nx=True)), None
        except Exception as e:
            err = _err_str(e)
            logger.error(f"error in redis set_if_absent : {err}")
            return False, err

    _CLAIM_PROCESSING_LUA = """
local current = redis.call('GET', KEYS[1])
if not current then
  redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
  return 1
end
if current == ARGV[3] or current == '1' then return 3 end
return 2
"""

    _CLAIM_JSON_IF_ABSENT_LUA = """
local current = redis.call('GET', KEYS[1])
if current then return {0, current} end
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
return {1, ARGV[1]}
"""

    _CLAIM_JSON_STATE_LEASE_SERVER_TIME_LUA = """
local raw = redis.call('GET', KEYS[1])
if not raw then return {0, '', ''} end
local decoded, current = pcall(cjson.decode, raw)
if not decoded or type(current) ~= 'table' then return {-1, raw, ''} end
if current['state'] ~= ARGV[1] or current['digest'] ~= ARGV[2] then
  return {3, raw, ''}
end
local now = redis.call('TIME')
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local marker = current[ARGV[3]]
if type(marker) == 'table' and tonumber(marker['expires_at_ms'] or 0) > now_ms then
  return {2, raw, ''}
end
current[ARGV[3]] = nil
local original = cjson.encode(current)
current[ARGV[3]] = {
  token = ARGV[4],
  expires_at_ms = now_ms + tonumber(ARGV[5]) * 1000
}
local leased = cjson.encode(current)
redis.call('SET', KEYS[1], leased, 'KEEPTTL')
return {1, leased, original}
"""

    _COMPARE_AND_SET_WITH_EXPIRY_LUA = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3])
return 1
"""

    _COMPARE_AND_EXPIRE_LUA = (
        "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('expire', KEYS[1], ARGV[2]) else return 0 end"
    )

    _COMPARE_AND_SET_KEEP_TTL_LUA = "if redis.call('get', KEYS[1]) == ARGV[1] then redis.call('set', KEYS[1], ARGV[2], 'KEEPTTL'); return 1 else return 0 end"

    _CLAIM_JSON_LEASE_SERVER_TIME_LUA = """
local raw = redis.call('GET', KEYS[1])
if not raw then return {0, '', ''} end
local decoded, current = pcall(cjson.decode, raw)
if not decoded or type(current) ~= 'table' then return {-1, raw, ''} end
local now = redis.call('TIME')
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local marker = current[ARGV[1]]
if type(marker) == 'table' and tonumber(marker['expires_at_ms'] or 0) > now_ms then
  return {2, raw, ''}
end
current[ARGV[1]] = nil
local original = cjson.encode(current)
current[ARGV[1]] = {
  token = ARGV[2],
  expires_at_ms = now_ms + tonumber(ARGV[3]) * 1000
}
local leased = cjson.encode(current)
redis.call('SET', KEYS[1], leased, 'KEEPTTL')
return {1, leased, original}
"""

    _RELEASE_IF_LIST_EMPTY_LUA = """
local cur = redis.call('GET', KEYS[1])
if cur ~= ARGV[1] then return 2 end
if redis.call('LLEN', KEYS[2]) > 0 then return 0 end
redis.call('DEL', KEYS[1])
return 1
"""

    _TOKEN_SWAP_LUA = """
local old = redis.call('GET', KEYS[1])
if not old then return nil end
if old == 'armed' or old == '1' or old == ARGV[1] then
  redis.call('SET', KEYS[1], ARGV[1], 'KEEPTTL')
end
return old
"""

    def lpop(self, key: str):
        """Pop the head of a list (None when empty)."""
        try:
            if not self.__validate(key=key):
                return None
            return self.client.lpop(key)
        except Exception as e:
            logger.error(f"error in redis lpop : {e}")
            return None

    # Move only an existing entry, atomically.
    _MOVE_TO_TAIL_LUA = (
        "local removed = redis.call('LREM', KEYS[1], 1, ARGV[1]) "
        "if removed > 0 then redis.call('RPUSH', KEYS[2], ARGV[1]) end "
        "return removed"
    )

    def lrem(self, key: str, count: int, value: str) -> int:
        try:
            if not self.__validate(key=key):
                return 0
            return int(self.client.lrem(key, count, value) or 0)
        except Exception as e:
            logger.error(f"error in redis lrem : {e}")
            return 0

    def mset(self, key_value_mapping: dict, expiry_in_seconds=None) -> bool:
        try:
            if not self.__validate():
                return False
            if not key_value_mapping:
                return True
            if not expiry_in_seconds:
                return self.client.mset(key_value_mapping)
            # SET with EX per key, so each value and its TTL land together.
            with self.client.pipeline() as pipe:
                for key, value in key_value_mapping.items():
                    pipe.set(key, value, ex=expiry_in_seconds)
                pipe.execute()
                return True
        except Exception as e:
            logger.error(f"error in redis mset : {e}")
            return False

    def delete(self, key: str) -> int:
        try:
            if not self.__validate(key=key):
                return None
            return self.client.delete(key)
        except Exception as e:
            logger.error(f"error in redis get : {e}")
            return None

    def rpush(self, key: str, values) -> int:
        try:
            if not self.__validate(key=key):
                return 0
            return self.client.rpush(key, values)
        except Exception as e:
            logger.error(f"error in redis rpush : {e}")
            return 0

    def lpush(self, key: str, values) -> int:
        try:
            if not self.__validate(key=key):
                return 0
            return self.client.lpush(key, values)
        except Exception as e:
            logger.error(f"error in redis lpush : {e}")
            return 0

    _UPSERT_JSON_LIST_ITEM_WITH_EXPIRY_LUA = """
local length = redis.call('LLEN', KEYS[1])
for index = 0, length - 1 do
    local raw = redis.call('LINDEX', KEYS[1], index)
    local decoded, item = pcall(cjson.decode, raw)
    if decoded and type(item) == 'table' and item[ARGV[1]] == ARGV[2] then
        redis.call('LSET', KEYS[1], index, ARGV[3])
        redis.call('EXPIRE', KEYS[1], ARGV[4])
        return 1
    end
end
redis.call('RPUSH', KEYS[1], ARGV[3])
redis.call('EXPIRE', KEYS[1], ARGV[4])
return 2
"""

    def lrange(self, key: str, start: int = 0, end: int = -1) -> list[str]:
        try:
            if not self.__validate(key=key):
                return []
            return self.client.lrange(key, start, end)
        except Exception as e:
            logger.error(f"error in redis lrange : {e}")
            return []

    # Head-guarded LTRIM: trims [start:] only while index 0 is still byte-identical to the
    # head the caller observed. If the list was cleared/expired and recreated in between,
    # its head is a different entry and the trim must not fire — a count-based trim on a
    # recreated list would delete entries the caller never observed.
    LTRIM_IF_HEAD_MATCHES_LUA = (
        "local head = redis.call('LINDEX', KEYS[1], 0) "
        "if head == ARGV[2] then redis.call('LTRIM', KEYS[1], tonumber(ARGV[1]), -1) return 1 end "
        "return 0"
    )

    def incrby(self, key: str, count: int) -> int:
        try:
            if not self.__validate(key=key):
                return None
            return self.client.incrby(key, count)
        except Exception as e:
            logger.error(f"error in redis incrby : {e}")
            return None

    def expire(self, key: str, expiry_in_seconds: int) -> bool:
        try:
            if not self.__validate(key=key):
                return False
            return bool(self.client.expire(key, expiry_in_seconds))
        except Exception as e:
            logger.error(f"error in redis expire : {e}")
            return False

    def ttl(self, key: str) -> int | None:
        """Return TTL in seconds; None indicates validation or client failure."""
        try:
            if not self.__validate(key=key):
                return None
            return self.client.ttl(key)
        except Exception as e:
            logger.error(f"error in redis ttl : {e}")
            return None

    def sadd(self, key: str, *members) -> int | None:
        try:
            if not self.__validate(key=key):
                return None
            return self.client.sadd(key, *members)
        except Exception as e:
            logger.error(f"error in redis sadd : {e}")
            return None

    def srem(self, key: str, *members) -> int | None:
        try:
            if not self.__validate(key=key):
                return None
            return self.client.srem(key, *members)
        except Exception as e:
            logger.error(f"error in redis srem : {e}")
            return None

    def scard(self, key: str) -> int:
        try:
            if not self.__validate(key=key):
                return 0
            return self.client.scard(key)
        except Exception as e:
            logger.error(f"error in redis scard : {e}")
            return 0

    def smembers(self, key: str) -> list[str]:
        try:
            if not self.__validate(key=key):
                return []
            return list(self.client.smembers(key))
        except Exception as e:
            logger.error(f"error in redis smembers : {e}")
            return []

    def sismember(self, key: str, member: str) -> bool:
        try:
            if not self.__validate(key=key):
                return False
            return bool(self.client.sismember(key, member))
        except Exception as e:
            logger.error(f"error in redis sismember : {e}")
            return False

    def hset(self, key: str, field: str | None = None, value: str | None = None, mapping=None) -> int | None:
        try:
            if not self.__validate(key=key):
                return None
            return self.client.hset(key, field, value, mapping=mapping)
        except Exception as e:
            logger.error(f"error in redis hset : {e}")
            return None

    def hget(self, key: str, field: str) -> str | None:
        try:
            if not self.__validate(key=key):
                return None
            return self.client.hget(key, field)
        except Exception as e:
            logger.error(f"error in redis hget : {e}")
            return None

    def hgetall(self, key: str) -> dict:
        try:
            if not self.__validate(key=key):
                return {}
            return self.client.hgetall(key) or {}
        except Exception as e:
            logger.error(f"error in redis hgetall : {e}")
            return {}

    def hdel(self, key: str, *fields) -> int | None:
        try:
            if not self.__validate(key=key):
                return None
            return self.client.hdel(key, *fields)
        except Exception as e:
            logger.error(f"error in redis hdel : {e}")
            return None

    def eval_script(self, src: str, keys=None, args=None):
        keys = keys or []
        args = args or []
        script = self._scripts.get(src)
        if script is None:
            script = self.client.register_script(src)
            self._scripts[src] = script
        return script(keys=keys, args=args, client=self.client)

    def close_redis(self):
        try:
            if self._client is not None:
                self._client.close()
        except Exception:
            pass
        finally:
            self._client = None


class HeartbeatPresenceCache:
    @staticmethod
    def _ping_time_key(aom_uuid: str) -> str:
        from hs.infra.constants import HEARTBEAT_PING_TIME_KEY

        return f"{HEARTBEAT_PING_TIME_KEY}:{aom_uuid}"

    @staticmethod
    def _ping_count_key(aom_uuid: str) -> str:
        from hs.infra.constants import HEARTBEAT_PING_COUNT_KEY

        return f"{HEARTBEAT_PING_COUNT_KEY}:{aom_uuid}"

    @classmethod
    def incr_ping_count(cls, aom_uuid: str) -> int | None:
        from hs.infra.constants import HEARTBEAT_PING_COUNT_TTL_SECONDS

        key = cls._ping_count_key(aom_uuid)
        count = Cache.get_instance().incrby(key, 1)
        if count is None:
            return None
        Cache.get_instance().expire(key, HEARTBEAT_PING_COUNT_TTL_SECONDS)
        return count

    @classmethod
    def reset_ping_count(cls, aom_uuid: str) -> bool:
        from hs.infra.constants import HEARTBEAT_PING_COUNT_TTL_SECONDS

        return Cache.get_instance().set(cls._ping_count_key(aom_uuid), "0", HEARTBEAT_PING_COUNT_TTL_SECONDS)

    @classmethod
    def set_ping_count(cls, aom_uuid: str, count: int) -> bool:
        from hs.infra.constants import HEARTBEAT_PING_COUNT_TTL_SECONDS

        return Cache.get_instance().set(cls._ping_count_key(aom_uuid), str(count), HEARTBEAT_PING_COUNT_TTL_SECONDS)

    @classmethod
    def set_ping_time(cls, aom_uuid: str) -> bool:
        from hs.infra.constants import HEARTBEAT_TIME_TO_MARK_OFFLINE

        now_unix = str(int(time.time()))
        return Cache.get_instance().set(cls._ping_time_key(aom_uuid), now_unix, HEARTBEAT_TIME_TO_MARK_OFFLINE)

    @classmethod
    def get_ping_time(cls, aom_uuid: str) -> str | None:
        return Cache.get_instance().get(cls._ping_time_key(aom_uuid))

    # --- self-contained offline sweep ---
    # set_ping_time already writes LST:<aom> with a TTL of HEARTBEAT_TIME_TO_MARK_OFFLINE, so it
    # disappears when an agent stops pinging. The online set + context hash below let an in-process
    # sweeper enumerate who is (was) online and flip the stale ones offline, with no external scheduler.
    @staticmethod
    def _online_set_key() -> str:
        from hs.infra.constants import HEARTBEAT_ONLINE_SET_KEY

        return HEARTBEAT_ONLINE_SET_KEY

    @staticmethod
    def _ctx_key() -> str:
        from hs.infra.constants import HEARTBEAT_CTX_KEY

        return HEARTBEAT_CTX_KEY

    @classmethod
    def mark_online(cls, aom_uuid: str, org_uuid: str | None = None, org_name: str | None = None) -> None:
        Cache.get_instance().sadd(cls._online_set_key(), aom_uuid)
        Cache.get_instance().hset(cls._ctx_key(), aom_uuid, f"{org_uuid or ''}|{org_name or ''}")

    @classmethod
    def unmark_online(cls, aom_uuid: str) -> None:
        Cache.get_instance().srem(cls._online_set_key(), aom_uuid)
        Cache.get_instance().hdel(cls._ctx_key(), aom_uuid)

    @classmethod
    def get_online_agents(cls) -> list[str]:
        return Cache.get_instance().smembers(cls._online_set_key()) or []

    @classmethod
    def get_presence_context(cls, aom_uuid: str):
        raw = Cache.get_instance().hget(cls._ctx_key(), aom_uuid)
        if not raw:
            return None, None
        org_uuid, _, org_name = raw.partition("|")
        return (org_uuid or None), (org_name or None)

    @staticmethod
    def _watch_marker_key(aom_uuid: str) -> str:
        from hs.infra.constants import HEARTBEAT_WATCH_MARKER_KEY

        return f"{HEARTBEAT_WATCH_MARKER_KEY}:{aom_uuid}"

    @classmethod
    def has_active_watcher(cls, aom_uuid: str) -> bool:
        return Cache.get_instance().get(cls._watch_marker_key(aom_uuid)) is not None

    @classmethod
    def mark_watcher_active(cls, aom_uuid: str) -> bool:
        from hs.infra.constants import HEARTBEAT_WATCH_MARKER_TTL

        return Cache.get_instance().set(cls._watch_marker_key(aom_uuid), "1", HEARTBEAT_WATCH_MARKER_TTL)
