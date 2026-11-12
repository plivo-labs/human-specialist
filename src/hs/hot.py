"""The hot-state store: every Redis key and command HS sends, as every timer lives in `jobs`.

A key is `hs:{<tenant id>}:...`, so a tenant's keys share one cluster slot, and time lives only in a key's TTL. A
tenant's `epoch` says its keys are whole: registering a tenant sets it, and once a Redis loss takes it the next sweep
rebuilds the tenant's keys from Postgres, then sets it again once the rebuild commits. Each set mints a new value, so a
write fenced on the epoch it read never lands on keys a rebuild made.

Liveness: a beat keeps the desk's `hb` for 40 s and counts the session's beats in `beats`, kept 2 h past the last.

Config: a read of a tenant's config documents, settings or Plivo account is kept 60 s in `cfg:<key>` as
`<generation>:<JSON>`, and counts only while `cfg` still holds that generation; a config or tenant write sets a new
one once it commits.

Webhooks: a delivery whose exchange failed keeps `wh:<webhook>` for 30 s, and its subscription's deliveries skip their
try while it lives.

Sign-in: `signin` counts the tries of an email in a tenant, or of a client, in a window from its first. The tag in
braces is `email:<SHA-256 of tenant id and email>` or `ip:<IPv4 address or IPv6 /64>`."""

import json
import secrets
from collections import defaultdict
from collections.abc import Callable, Iterable
from contextlib import suppress
from datetime import UTC, datetime
from functools import partial
from typing import NamedTuple

from redis import RedisError, ResponseError
from sqlalchemy import text

from hs.core import after_commit, redis, undoable

LIVE, KEPT, SIGN_IN = 40, 7200, 2
EPOCH, HB, BEATS = "hs:{%s}:epoch", "hs:{%s}:hb:%s", "hs:{%s}:beats:%s"
_BEAT = redis.register_script("""
redis.call('SET', KEYS[1], 1, 'EX', ARGV[1])
local n = redis.call('INCR', KEYS[2])
if n == tonumber(ARGV[3]) then redis.call('DECR', KEYS[2]) end
redis.call('EXPIRE', KEYS[2], ARGV[2])
return {n, redis.call('EXISTS', KEYS[3])}
""")
_REBUILD = redis.register_script("""
if redis.call('EXISTS', KEYS[1]) == 1 then return end
for i = 2, #KEYS, 2 do
  redis.call('SET', KEYS[i], 1, 'EX', ARGV[1], 'NX')
  if tonumber(redis.call('GET', KEYS[i + 1]) or 0) < tonumber(ARGV[3]) then
    redis.call('SET', KEYS[i + 1], ARGV[3], 'EX', ARGV[2])
  end
end
redis.call('SET', KEYS[1], ARGV[4])
""")


def rebuilt(tenant_id) -> None:
    redis.set(EPOCH % tenant_id, secrets.token_hex(8))


def lost(tenants: set) -> set:
    ordered = list(tenants)
    with redis.pipeline(transaction=False) as p:
        for t in ordered:
            p.exists(EPOCH % t)
        return {t for t, whole in zip(ordered, p.execute(), strict=True) if not whole}


def beat(tenant_id, agent_id) -> tuple[int, bool]:
    """The session's count with this beat, and whether the tenant's keys are whole, so the count can be trusted. The
    sign-in beat leaves the count short of itself until `recount` says the sign-in is done, so a process that dies
    before then leaves the next beat to sign in."""
    keys = [HB % (tenant_id, agent_id), BEATS % (tenant_id, agent_id), EPOCH % tenant_id]
    n, whole = _BEAT(keys=keys, args=[LIVE, KEPT, SIGN_IN])
    return n, bool(whole)


def recount(tenant_id, agent_id, beats: int = 0) -> None:
    """The session's next beat counts `beats + 1`; a count dropped meanwhile stays dropped."""
    if beats:
        redis.set(BEATS % (tenant_id, agent_id), beats, ex=KEPT, xx=True)
    else:
        redis.delete(BEATS % (tenant_id, agent_id))


def silent(tenant_id, agent_id) -> bool:
    with redis.pipeline() as p:
        whole, live = p.exists(EPOCH % tenant_id).exists(HB % (tenant_id, agent_id)).execute()
    return bool(whole and not live)


def sweep(
    db, desks: list[tuple], held: list = (), rebuild: set = frozenset(), tried: Iterable[list] = ()
) -> tuple[list[tuple], list[tuple]]:
    """The (tenant, agent, signed in) desks to sign out, which counted beats and then went 40 s without one, and
    the (tenant, agent) desks signed in on a tenant it rebuilt. A tenant of `rebuild` gets its `cap` and `full` keys
    rebuilt from `held`, rows as `audit` takes them, and its waiting conversations' `route` and `tried` keys from
    `tried`, pages of rows of tenant, id, channel, stint, the agents tried mapped to their offers, and the ms a call
    keeps them. Once `db` commits, one still without its epoch gives each agent signed in 40 s to beat and a count
    raised past sign-in, so none is silent, and sets its epoch again; a rebuild that fails before then is the next
    sweep's to run again."""
    tenants = list({t for t, _, _ in desks})
    with redis.pipeline(transaction=False) as p:
        for t in tenants:
            p.exists(EPOCH % t)
        for t, a, _ in desks:
            p.exists(HB % (t, a)).exists(BEATS % (t, a))
        found = p.execute()
    whole, keys = dict(zip(tenants, found, strict=False)), found[len(tenants) :]
    lost, rebuilt = {t: [EPOCH % t] for t in rebuild}, []
    for t, a, signed_in in desks:
        if signed_in and t in lost:
            lost[t] += [HB % (t, a), BEATS % (t, a)]
            rebuilt.append((str(t), str(a)))
    gone = {str(t) for t in lost}
    for page in tried if gone else ():
        with redis.pipeline(transaction=False) as p:
            for r in (r for r in page if r.tenant in gone):
                kept, agents = life(r.channel), [x for pair in r.tried.items() for x in pair]
                rkeys = [ROUTE % (r.tenant, r.id), TRIED % (r.tenant, r.id)]
                _RETRIED(keys=rkeys, args=[r.stint, kept, r.ms or kept * 1000, *agents], client=p)
            p.execute()
    stints, caps = _held(r for r in held if r.tenant in gone)
    with redis.pipeline(transaction=False) as p:
        for (t, ch, a), holders in stints.items():
            p.hset(CAP % (t, ch, a), mapping=holders)
            if len(set(holders.values())) >= caps[t, ch, a]:
                p.sadd(FULL % (t, ch), a)
        p.execute()
    if lost:
        epochs = redis.pipeline(transaction=False)
        for epoch_first in lost.values():
            _REBUILD(keys=epoch_first, args=[LIVE, KEPT, SIGN_IN, secrets.token_hex(8)], client=epochs)
        after_commit(db, epochs.execute)
    silent = [d for d, hb, beats in zip(desks, keys[::2], keys[1::2], strict=True) if whole[d[0]] and beats and not hb]
    return silent, rebuilt


# Capacity. `cap:<channel>:<agent>` maps each holder, the assignment id of an offer or stint holding one of the agent's
# slots, to its conversation; load counts distinct conversations. A give removes only its own holder. `full:<channel>`
# names agents at their cap: only `take` adds to it, so it can miss a full agent, which `take` refuses and adds back.
# `take` refuses while the epoch is missing, so no round runs on unrebuilt keys.
CAP, FULL = "hs:{%s}:cap:%s:%s", "hs:{%s}:full:%s"
_TAKE = redis.register_script("""
if redis.call('EXISTS', KEYS[1]) == 0 then return redis.error_reply('UNREBUILT tenant hot state awaits the sweep') end
local convs, n = {}, 0
for _, c in ipairs(redis.call('HVALS', KEYS[3])) do
  if not convs[c] then convs[c], n = true, n + 1 end
end
local took = convs[ARGV[2]] or ARGV[4] == '1' or n < tonumber(ARGV[3])
if took then
  if not convs[ARGV[2]] then n = n + 1 end
  redis.call('HSET', KEYS[3], ARGV[1], ARGV[2])
end
if n >= tonumber(ARGV[3]) then redis.call('SADD', KEYS[2], ARGV[5]) end
return took and 1 or 0
""")


SLOTS = text("SELECT pg_try_advisory_xact_lock_shared(5107, hashtext(:t))")
REBUILDING = text("SELECT pg_advisory_xact_lock(5107, hashtext(t)) FROM unnest(CAST(:ts AS text[])) t ORDER BY t")


def share_slots(db, tenant_id) -> None:
    """Hold the tenant's SLOTS shared to COMMIT, which the sweep holds exclusively while it rebuilds the tenant. A
    try never waits, so it closes no cycle with a row lock; it fails while the sweep holds or awaits the lock."""
    if not db.info["tx"].setdefault(("slots", tenant_id), db.execute(SLOTS, {"t": tenant_id}).scalar()):
        raise ResponseError("UNREBUILT tenant hot state is being rebuilt")


def take(db, conv, agent_id, holder, limit: int, force: bool = False, channel: str | None = None) -> bool:
    """Whether offer or stint `holder` now holds one of the agent's slots on `channel`, the conversation's by default:
    the one the conversation holds already, else a new one below `limit`, or past it when `force`d, as a supervisor or
    a participant joins whatever its load. A rollback gives it back."""
    t, ch, agent, h = str(conv.tenant_id), channel or conv.channel, str(agent_id), str(holder)
    share_slots(db, t)
    keys = [EPOCH % t, FULL % (t, ch), CAP % (t, ch, agent)]
    took = partial(_TAKE, keys=keys, args=[h, str(conv.id), limit, int(force), agent])
    return bool(undoable(db, took, partial(_give, t, ch, agent, h)))


def give(db, a) -> None:
    """Stint `a` gives its agent's slot back once the transaction commits: a slot given before then may go to
    another replica, and a rollback could not take it back."""
    after_commit(db, partial(_give, str(a.tenant_id), a.channel, str(a.agent_id), str(a.id)))


def _give(t, ch, agent, holder) -> None:
    with redis.pipeline() as p:
        p.hdel(CAP % (t, ch, agent), holder).srem(FULL % (t, ch), agent).execute()


def full(tenant_id, *channels) -> list[str]:
    """`<channel>:<agent>` for each agent `full` names on each of `channels`."""
    with redis.pipeline(transaction=False) as p:
        for ch in channels:
            p.smembers(FULL % (tenant_id, ch))
        return [f"{ch}:{a}" for ch, named in zip(channels, p.execute(), strict=True) for a in named]


def _held(held) -> tuple[dict, dict]:
    stints, caps = defaultdict(dict), {}
    for r in held:
        stints[r.tenant, r.channel, r.agent][r.holder] = r.conv
        caps[r.tenant, r.channel, r.agent] = r.cap
    return stints, caps


def audit(desks: list[tuple], held: list, channels: list[str]) -> tuple[list[tuple], list[tuple]]:
    """Each desk's `cap` holders on `channels` against `held`, rows of tenant, channel, agent, holder, conv and cap
    for the slots Postgres holds. Returns the (tenant, channel, agent, holder, conversation) slots they disagree on,
    to settle under the conversation's lock, and the (tenant, channel, agent) `full` names with fewer conversations
    than their cap, as after a cap was raised, for `settle` to take out of `full`."""
    stints, caps = _held(held)
    seats = [(str(t), ch, str(a)) for t, a, _ in desks for ch in channels]
    with redis.pipeline(transaction=False) as p:
        for t, ch, a in seats:
            p.hgetall(CAP % (t, ch, a)).sismember(FULL % (t, ch), a)
        found = p.execute()
    odd, room = [], []
    for seat, slots, named in zip(seats, found[::2], found[1::2], strict=True):
        mine = stints.get(seat, {})
        odd += [(*seat, *slot) for slot in slots.items() ^ mine.items()]
        if named and len(set(mine.values())) < caps.get(seat, 1):
            room.append(seat)
    return odd, room


def freed(odd: list[tuple], held: list, room: list[tuple]) -> set[tuple]:
    """The (tenant, agent) that `settle(odd, held, room)` gives room: a slot `held` lacks, or a seat out of `full`."""
    now = {(r.tenant, r.channel, r.agent, r.holder) for r in held}
    return {(t, a) for t, ch, a, h, _ in odd if (t, ch, a, h) not in now} | {(t, a) for t, _, a in room}


def settle(odd: list[tuple], held: list, room: list[tuple]) -> None:
    """Each (tenant, channel, agent) seat of `room` out of `full`, and each (tenant, channel, agent, holder,
    conversation) slot of `odd` as `held` says now: taken if held, past the cap if need be, else given back. The
    caller holds each conversation's share lock, or knows it never committed."""
    now = {(r.tenant, r.channel, r.agent, r.holder): r.cap for r in held}
    with redis.pipeline(transaction=False) as p:
        for t, ch, a in room:
            p.srem(FULL % (t, ch), a)
        for t, ch, a, h, c in odd:
            if (t, ch, a, h) in now:
                keys = [EPOCH % t, FULL % (t, ch), CAP % (t, ch, a)]
                _TAKE(keys=keys, args=[h, c, now[t, ch, a, h], 1, a], client=p)
            else:
                p.hdel(CAP % (t, ch, a), h).srem(FULL % (t, ch), a)
        p.execute()


# Offers. `offers:<conversation>` maps each offer id to its row as JSON, open while in state "offered" and a tombstone
# once ended, for the channel's life, which each new offer extends, so a leg that reaches a closed conversation late
# still finds its offer ended.
OFFERS, DESK = "hs:{%s}:offers:%s", "hs:{%s}:desk:%s"
OFFER_CONVERSATION = "hs:{%s}:offer_conversation:%s"
_CREATE = redis.register_script("""
local new = redis.call('HSETNX', KEYS[1], ARGV[1], ARGV[2]) == 1
if new and KEYS[2] then redis.call('HSET', KEYS[2], ARGV[1], ARGV[2]) end
for i = 1, math.min(#KEYS, 2) do
  if redis.call('EXPIRE', KEYS[i], ARGV[3], 'NX') == 0 then redis.call('EXPIRE', KEYS[i], ARGV[3], 'GT') end
end
local ttl = redis.call('PTTL', KEYS[1])
if new and KEYS[3] then redis.call('SET', KEYS[3], ARGV[4], 'PX', ttl) end
for i = 4, #KEYS do redis.call('PEXPIRE', KEYS[i], ttl, 'GT') end
""")
_CHANGE = redis.register_script("""
local row = redis.call('HGET', KEYS[1], ARGV[1])
if not row then return 0 end
row = cjson.decode(row)
if (ARGV[2] == 'call_uuid' and row.call_uuid ~= cjson.null) or (ARGV[2] ~= 'call_uuid' and row.state ~= 'offered') then
  return 0
end
row[ARGV[2]] = ARGV[3]
if ARGV[4] ~= '' then row.reason = ARGV[4] end
row = cjson.encode(row)
for _, k in ipairs(KEYS) do redis.call('HSET', k, ARGV[1], row) end
return 1
""")


def _keys(tenant_id, cid, agent_id) -> list[str]:
    return [OFFERS % (tenant_id, cid)] + ([DESK % (tenant_id, agent_id)] if agent_id else [])


def create_and_bind(tenant_id, cid, channel: str, creates: list[tuple], binds: list[tuple]) -> None:
    # One script call a trip: a pipeline of scripts costs a SCRIPT EXISTS trip first. Only a text or task offer is
    # answered by its id, so only it gets an `offer_conversation` lookup.
    new = [o for o, agent, _ in creates if agent] if channel != "call" else []
    found = [OFFER_CONVERSATION % (tenant_id, o) for o in redis.hkeys(OFFERS % (tenant_id, cid)) + new] if new else []
    for oid, agent, row in creates:
        lookups = [OFFER_CONVERSATION % (tenant_id, oid), *found] if found and agent else []
        _CREATE(keys=_keys(tenant_id, cid, agent) + lookups, args=[oid, row, life(channel), str(cid)])
    for oid, agent, call_uuid in binds:
        _CHANGE(keys=_keys(tenant_id, cid, agent), args=[oid, "call_uuid", call_uuid, ""])


def tombstone(tenant_id, cid, agent_id, oid, state: str, reason: str | None) -> None:
    _CHANGE(keys=_keys(tenant_id, cid, agent_id), args=[str(oid), "state", state, reason or ""])


def offers(tenant_id, cid) -> dict[str, dict]:
    return {o: json.loads(row) for o, row in redis.hgetall(OFFERS % (tenant_id, cid)).items()}


def states(offers: list[tuple]) -> list[str | None]:
    with redis.pipeline(transaction=False) as p:
        for t, c, o in offers:
            p.hget(OFFERS % (t, c), o)
        return [row and json.loads(row)["state"] for row in p.execute()]


ACCEPTED = text("SELECT id::text FROM assignment WHERE id = ANY(CAST(:ids AS uuid[])) AND started_at IS NOT NULL")


def standing(db, offers: list[tuple], *, blind: bool = False) -> list[str | None]:
    try:
        now = states(offers) if offers else []
    except RedisError:
        if not blind:
            raise
        now = [None] * len(offers)
    unsure = [o for (_, _, o), s in zip(offers, now, strict=True) if s not in ("offered", "active")]
    # Read committed: a statement after the caller's read sees an accept that committed since.
    took = set(db.execute(ACCEPTED, {"ids": unsure}).scalars()) if unsure else set()
    return ["active" if o in took else s for (_, _, o), s in zip(offers, now, strict=True)]


def desk(tenant_id, agent_id) -> dict[str, str]:
    return {o: json.loads(row)["conversation_id"] for o, row in redis.hgetall(DESK % (tenant_id, agent_id)).items()}


def located(tenant_id, ids: list[str]) -> dict[str, str]:
    found = redis.mget([OFFER_CONVERSATION % (tenant_id, o) for o in ids]) if ids else []
    return {o: c for o, c in zip(ids, found, strict=True) if c}


def fresh_rows(tenant_id, homes: dict[str, str]) -> dict[str, dict]:
    with redis.pipeline(transaction=False) as p:
        for o, c in homes.items():
            p.hget(OFFERS % (tenant_id, c), o)
        found = p.execute()
    return {o: json.loads(row) for o, row in zip(homes, found, strict=True) if row}


def drop_conversation(tenant_id, cid) -> None:
    with redis.pipeline(transaction=False) as p:
        for oid, row in offers(tenant_id, cid).items():
            if row["agent_id"]:
                p.hdel(DESK % (tenant_id, row["agent_id"]), oid)
        p.delete(ROUTE % (tenant_id, cid), TRIED % (tenant_id, cid), UNREAD % (tenant_id, cid)).execute()


# A round stores a conversation's `route` and `tried` keys after COMMIT, only while Redis holds the stint, round and
# epoch it read.
ROUTE, TRIED = "hs:{%s}:route:%s", "hs:{%s}:tried:%s"
CALL_TRIED, LIFE = 300_000, {"callback": 75 * 3600, "task": 360 * 3600}
_ROUNDS = redis.register_script("""
if redis.call('GET', KEYS[3]) ~= ARGV[1] then return 0 end
local q, r = unpack(redis.call('HMGET', KEYS[1], 'stint', 'round'))
if (q or '') ~= ARGV[2] or tonumber(r or 0) ~= tonumber(ARGV[3]) then return 0 end
redis.call('HSET', KEYS[1], 'stint', ARGV[4], 'round', ARGV[5])
redis.call('EXPIRE', KEYS[1], ARGV[6])
redis.call('DEL', KEYS[2])
if #ARGV > 7 then
  redis.call('HSET', KEYS[2], unpack(ARGV, 8))
  redis.call('PEXPIRE', KEYS[2], ARGV[7])
end
return 1
""")


class Seen(NamedTuple):
    stint: str
    round: int
    epoch: str


class Rounds(NamedTuple):
    round: int = 0
    tried: dict[str, str] = {}
    ttl: int | None = None  # ms `tried` is kept; None for the stint
    seen: Seen = Seen("", 0, "")


def stint(conv) -> str:
    return conv.routing.get("stint", "")


def life(channel: str) -> int:
    return LIFE.get(channel, 90000)


def rounds(conv) -> Rounds:
    route, tried = ROUTE % (conv.tenant_id, conv.id), TRIED % (conv.tenant_id, conv.id)
    with redis.pipeline() as p:
        p.hmget(route, "stint", "round").hgetall(tried).pttl(tried).get(EPOCH % conv.tenant_id)
        (q, r), t, ttl, epoch = p.execute()
    if epoch is None:
        raise ResponseError("UNREBUILT tenant hot state awaits the sweep")
    seen = Seen(q or "", int(r or 0), epoch)
    if q != stint(conv):
        return Rounds(seen=seen)
    return Rounds(int(r or 0), t, ttl if ttl > 0 else None, seen)


def keep_rounds(db, conv, new: Rounds) -> None:
    t, s, ttl = conv.tenant_id, new.seen, life(conv.channel)
    args = [s.epoch, s.stint, s.round, stint(conv), new.round, ttl]
    args += [new.ttl or ttl * 1000, *(x for pair in new.tried.items() for x in pair)]
    after_commit(db, partial(_ROUNDS, keys=[ROUTE % (t, conv.id), TRIED % (t, conv.id), EPOCH % t], args=args))


_RETRIED = redis.register_script("""
if redis.call('HGET', KEYS[1], 'stint') ~= ARGV[1] then
  redis.call('HSET', KEYS[1], 'stint', ARGV[1], 'round', 0)
  redis.call('DEL', KEYS[2])
end
redis.call('EXPIRE', KEYS[1], ARGV[2])
redis.call('HSET', KEYS[2], unpack(ARGV, 4))
redis.call('PEXPIRE', KEYS[2], ARGV[3])
""")


def tried_by(tenant_id, agent_id, cids: list) -> list:
    with redis.pipeline(transaction=False) as p:
        for c in cids:
            p.hexists(TRIED % (tenant_id, c), str(agent_id))
        return [c for c, tried in zip(cids, p.execute(), strict=True) if tried]


# `fence` is _SPEAK's ARGV: epoch, stint, claim read, new claim, prompt, event sequence, TTL, room.
_SPEAK = redis.register_script("""
if redis.call('GET', KEYS[2]) ~= ARGV[1] then return 0 end
local pq, n, claim, room = unpack(redis.call('HMGET', KEYS[1], 'pq', 'prompt', 'claim', 'room'))
if tonumber(room or -1) > tonumber(ARGV[8]) then return 0 end
if pq ~= ARGV[2] then n, claim = false, false end
if n == '-1' or (claim or '') ~= ARGV[3] then return 0 end
redis.call('HSET', KEYS[1], 'pq', ARGV[2], 'room', ARGV[8], 'claim', ARGV[4], 'prompt', ARGV[5], 'heard', ARGV[6])
redis.call('EXPIRE', KEYS[1], ARGV[7])
return 1
""")
_UNCLAIM = redis.register_script("""
if redis.call('HGET', KEYS[1], 'claim') == ARGV[2] then redis.call('HSET', KEYS[1], 'claim', ARGV[1]) end
""")
_HUSH = redis.register_script("""
if tonumber(redis.call('HGET', KEYS[1], 'room') or -1) > tonumber(ARGV[3]) then return end
redis.call('HSET', KEYS[1], 'pq', ARGV[1], 'room', ARGV[3], 'prompt', -1, 'claim', '')
redis.call('EXPIRE', KEYS[1], ARGV[2])
""")


class Prompted(NamedTuple):
    n: int
    heard: int
    claim: str
    epoch: str


def prompted(conv) -> Prompted:
    route = ROUTE % (conv.tenant_id, conv.id)
    with redis.pipeline() as p:
        (pq, n, heard, c), epoch = (
            p.hmget(route, "pq", "prompt", "heard", "claim").get(EPOCH % conv.tenant_id).execute()
        )
    if pq != stint(conv):
        n = heard = c = None
    return Prompted(int(n or 0), int(heard or 0), c or "", epoch or "")


def fence(conv, n: int, seq: int, was: Prompted) -> list:
    return [was.epoch, stint(conv), was.claim, secrets.token_hex(8), n, seq, life(conv.channel), conv.room]


def claim(tenant_id, cid, fence: list) -> bool:
    return bool(_SPEAK(keys=[ROUTE % (tenant_id, cid), EPOCH % tenant_id], args=fence))


def unclaim(tenant_id, cid, fence: list) -> None:
    _UNCLAIM(keys=[ROUTE % (tenant_id, cid)], args=fence[2:4])


def hush(db, conv) -> None:
    args = [stint(conv), life(conv.channel), conv.room]
    after_commit(db, partial(_HUSH, keys=[ROUTE % (conv.tenant_id, conv.id)], args=args))


STICKY, OSTICKY = "hs:{%s}:sticky:%s:%s", "hs:{%s}:osticky:%s"
# A value is `<order> <agent id>`, its order `<when its stint set it> <stint id>`. A write replaces only an older value,
# or one with no order, which only earlier builds wrote. A queue's write leaves the outbound key its order with no
# agent, kept as long as an outbound agent set then would be, so an older outbound write that lands later loses.
_STICK = redis.register_script("""
local function older(k)
  local v = redis.call('GET', k)
  return not v or not v:find(' ') or v:sub(1, #ARGV[1]) < ARGV[1]
end
local function put(k, v, at)
  if at == '' then redis.call('SET', k, v) else redis.call('SET', k, v, 'PXAT', at) end
end
if KEYS[2] and older(KEYS[2]) then put(KEYS[2], ARGV[1] .. ' ', ARGV[4]) end
if not older(KEYS[1]) then return 0 end
put(KEYS[1], ARGV[1] .. ' ' .. ARGV[2], ARGV[3])
return 1
""")
_UNSTICK = redis.register_script("if redis.call('GET', KEYS[1]) == ARGV[1] then redis.call('DEL', KEYS[1]) end")


def stick(tenant_id, customer, agent_id, at: datetime, sid, hours, queue_id=None, outbound=None, client=None) -> None:
    """`agent_id` is the customer's sticky agent for `queue_id`, or else its outbound one, set at `at` by stint `sid`,
    for `hours`. A queue's write given `outbound` hours ends an older outbound agent."""
    out = OSTICKY % (tenant_id, customer)
    keys = ([STICKY % (tenant_id, queue_id, customer)] + ([out] if outbound is not None else [])) if queue_id else [out]
    args = [f"{at.astimezone(UTC):%Y%m%d%H%M%S%f} {sid}", str(agent_id), _until(at, hours), _until(at, outbound)]
    _STICK(keys=keys, args=args, client=client)


def _until(at: datetime, hours: int | None) -> int | str:
    return int(at.timestamp() * 1000) + hours * 3_600_000 if hours else ""


def stuck(tenant_id, queue_id, customer, outbound: bool) -> tuple[str, str, str] | None:
    """The first of the customer's sticky keys that names an agent: the key, its value and the agent id."""
    keys = ([OSTICKY % (tenant_id, customer)] if outbound else []) + [STICKY % (tenant_id, queue_id, customer)]
    found = ((k, v, v.rpartition(" ")[2]) for k, v in zip(keys, redis.mget(keys), strict=True) if v)
    return next((f for f in found if f[2]), None)


def unstick(key: str, value: str) -> None:
    _UNSTICK(keys=[key], args=[value])


def fill(tenant_id, found: list[tuple], outbound: int | None) -> None:
    """Rebuilt keys. A row with no queue is an outbound agent, and one with a queue but no agent only ends an older
    outbound agent; both only while outbound sticky is on, for `outbound` hours."""
    with redis.pipeline(transaction=False) as p:
        for queue_id, customer, agent_id, at, sid, hours in found:
            if queue_id and agent_id:
                stick(tenant_id, customer, agent_id, at, sid, hours, queue_id, outbound, p)
            elif outbound is not None:
                stick(tenant_id, customer, agent_id or "", at, sid, outbound, client=p)
        p.execute()


GEN, CFG, FRESH = "hs:{%s}:cfg", "hs:{%s}:cfg:%s", 60


def cached(tenant_id, key: str, load: Callable[[], object]) -> str:
    """`load()`'s value as JSON, from the cache while its entry holds the tenant's generation. A read that finds
    nothing is not kept, so an id that names no tenant leaves no key. A lookup Redis fails reads Postgres; a fill it
    fails is dropped."""
    try:
        gen_before_load, entry = redis.mget(GEN % tenant_id, CFG % (tenant_id, key))
    except RedisError:
        return json.dumps(load())
    kept, _, value = (entry or "").partition(":")
    if entry and kept == (gen_before_load or ""):
        return value
    if (found := load()) is None:
        return "null"
    value = json.dumps(found)
    with suppress(RedisError):
        redis.set(CFG % (tenant_id, key), f"{gen_before_load or ''}:{value}", ex=FRESH)
    return value


def changed(tenant_id) -> None:
    redis.set(GEN % tenant_id, secrets.token_hex(8))


WH, BREAK_MS = "hs:{%s}:wh:%s", 30_000


def trip(tenant_id, webhook_id) -> None:
    with suppress(RedisError):
        redis.set(WH % (tenant_id, webhook_id), 1, px=BREAK_MS)


def tripped(tenant_id, webhook_id) -> bool:
    with suppress(RedisError):
        return bool(redis.exists(WH % (tenant_id, webhook_id)))
    return False


UNREAD = "hs:{%s}:unread:%s"
_UNSEEN = redis.register_script("if redis.call('EXISTS', KEYS[1]) == 1 then redis.call('INCR', KEYS[1]) end")


def unread(tenant_id, cid, read: bool) -> None:
    # Two replicas' after-commits may run a reset and an increment out of order, one off until the next agent read or
    # write. Upgrade: a sorted set of counted entries scored by written_at, reset by ZREMRANGEBYSCORE.
    if read:
        redis.set(UNREAD % (tenant_id, cid), 0)
    else:
        _UNSEEN(keys=[UNREAD % (tenant_id, cid)])


def unread_counts(tenant_id, cids: list[str]) -> list[int | None]:
    return [None if n is None else int(n) for n in redis.mget([UNREAD % (tenant_id, c) for c in cids])] if cids else []


SIGNIN = "hs:{%s}:signin"
# A try given back to a window that ended meanwhile leaves no count without a TTL.
_GIVE_BACK = redis.register_script("if redis.call('DECR', KEYS[1]) < 0 then redis.call('DEL', KEYS[1]) end")
# Atomic, so a count never lacks a TTL; one found without takes a new window.
_TRY = redis.register_script("""
local n = redis.call('INCR', KEYS[1])
if redis.call('TTL', KEYS[1]) < 0 then redis.call('EXPIRE', KEYS[1], ARGV[1]) end
return {n, redis.call('TTL', KEYS[1])}
""")


def sign_in_tried(limits: dict[str, int], window: int) -> int:
    """Count a try against each tag's limit, before its check, so parallel tries never pass the limit; the seconds
    until every tag over its limit takes tries again, else 0."""
    with redis.pipeline(transaction=False) as p:
        for tag in limits:
            _TRY(keys=[SIGNIN % tag], args=[window], client=p)
        got = p.execute()
    return max((max(ttl, 1) for (n, ttl), limit in zip(got, limits.values(), strict=True) if n > limit), default=0)


def signed_in(email: str, address: str) -> None:
    """A right password: the email's count goes, and the address gets its try back."""
    with redis.pipeline(transaction=False) as p:
        p.delete(SIGNIN % email)
        _GIVE_BACK(keys=[SIGNIN % address], client=p)
        p.execute()
