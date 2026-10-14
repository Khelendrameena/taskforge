"""Lua scripts executed atomically inside Redis.

Why Lua: several operations must be atomic to be correct, and Redis executes
a Lua script single-threaded, so the whole script is one indivisible step.
The alternative (WATCH/MULTI retries in Python) works but is slower and
harder to reason about under contention. Each script documents exactly what
race it eliminates.
"""

from __future__ import annotations

import redis

# Priority score math (also see client.enqueue):
#   score = enqueue_time_ms - priority * 10**13 + seq
# A priority-10 job always sorts before any priority-9 job no matter when it
# arrived, because 10**13 ms (~317 years) dwarfs any realistic clock delta.
# Within one priority level the score is the enqueue time -> FIFO, with an
# integer per-producer sequence tiebreak for same-millisecond enqueues
# (integer because doubles can't resolve fractions at this magnitude).
PRIORITY_SPAN = 10**13

ENQUEUE = """
-- Atomically enqueue, or return the existing job id when the idempotency
-- key was seen before. Race eliminated: two producers racing with the same
-- key cannot both create a job, because GET+SET happen in one script.
-- KEYS: queue, idem_key, job_key, delayed, metrics
-- ARGV: job_id, job_json, queue_score, idem_ttl_s, not_before_ms
local existing = nil
if KEYS[2] ~= "" then
  existing = redis.call("GET", KEYS[2])
end
if existing then
  redis.call("HINCRBY", KEYS[5], "idempotency_hits", 1)
  return {existing, 0}
end
redis.call("HSET", KEYS[3], "data", ARGV[2], "status", "queued")
if tonumber(ARGV[5]) > 0 then
  redis.call("ZADD", KEYS[4], ARGV[5], ARGV[1])
else
  redis.call("ZADD", KEYS[1], ARGV[3], ARGV[1])
end
if KEYS[2] ~= "" then
  redis.call("SET", KEYS[2], ARGV[1], "EX", ARGV[4])
end
redis.call("HINCRBY", KEYS[5], "enqueued", 1)
return {ARGV[1], 1}
"""

CLAIM = """
-- Atomically pop the highest-priority job and register a lease on it.
-- Race eliminated: ZPOPMIN + lease creation are one step, so two workers
-- can never be handed the same job.
-- The lease deadline honors the job's own lease_timeout_s (per-job
-- visibility timeout), falling back to the worker's default.
-- KEYS: queue, leases, metrics
-- ARGV: now_ms, worker_id, key_prefix, default_visibility_s
local popped = redis.call("ZPOPMIN", KEYS[1], 1)
if #popped == 0 then
  return nil
end
local job_id = popped[1]
local job_key = ARGV[3] .. "job:" .. job_id
local raw = redis.call("HGET", job_key, "data")
if not raw then
  return nil  -- job hash vanished (admin purge); drop the pop
end
local vt = tonumber(cjson.decode(raw)["lease_timeout_s"] or ARGV[4])
local deadline = tonumber(ARGV[1]) + vt * 1000
redis.call("HSET", ARGV[3] .. "lease:" .. job_id,
           "worker_id", ARGV[2], "deadline", deadline, "claimed_at", ARGV[1])
redis.call("ZADD", KEYS[2], deadline, job_id)
redis.call("HSET", job_key, "status", "leased")
redis.call("HINCRBY", KEYS[3], "claimed", 1)
return {job_id, raw}
"""

ACK = """
-- Complete a job. The lease ownership check is the critical safety: if our
-- lease expired (slow handler, worker pause) the sweeper already requeued
-- the job and someone else owns it -- acking would double-count/double-log.
-- Returning 0 tells the worker "your copy is stale, drop it".
-- KEYS: leases, metrics, latency
-- ARGV: key_prefix, job_id, worker_id, now_ms, result_json, latency_ms
local lease_key = ARGV[1] .. "lease:" .. ARGV[2]
local job_key = ARGV[1] .. "job:" .. ARGV[2]
if redis.call("HGET", lease_key, "worker_id") ~= ARGV[3] then
  return 0
end
redis.call("DEL", lease_key)
redis.call("ZREM", KEYS[1], ARGV[2])
redis.call("HSET", job_key, "status", "done",
           "result", ARGV[5], "completed_at", ARGV[4])
redis.call("HINCRBY", KEYS[2], "acked", 1)
redis.call("LPUSH", KEYS[3], ARGV[6])
redis.call("LTRIM", KEYS[3], 0, 19999)
return 1
"""

FAIL = """
-- Record a failed attempt. Either schedule a retry with backoff or, when
-- attempts are exhausted, park the job in the DLQ with full context.
-- KEYS: leases, delayed, dlq, metrics
-- ARGV: key_prefix, job_id, worker_id, now_ms, backoff_ms, last_error
local lease_key = ARGV[1] .. "lease:" .. ARGV[2]
local job_key = ARGV[1] .. "job:" .. ARGV[2]
if redis.call("HGET", lease_key, "worker_id") ~= ARGV[3] then
  return {-1}  -- lease lost; not ours to fail
end
local attempts = redis.call("HINCRBY", job_key, "attempts", 1)
local job = cjson.decode(redis.call("HGET", job_key, "data"))
local max_retries = tonumber(job["max_retries"] or 3)
redis.call("DEL", lease_key)
redis.call("ZREM", KEYS[1], ARGV[2])
if attempts > max_retries then
  job["attempts"] = attempts
  job["status"] = "dead"
  job["last_error"] = ARGV[6]
  job["dead_at"] = tonumber(ARGV[4])
  redis.call("RPUSH", KEYS[3], cjson.encode(job))
  redis.call("HSET", job_key, "status", "dead")
  redis.call("HINCRBY", KEYS[4], "dlq", 1)
  return {attempts, 1}
else
  redis.call("ZADD", KEYS[2], tonumber(ARGV[4]) + tonumber(ARGV[5]), ARGV[2])
  redis.call("HSET", job_key, "status", "retrying", "last_error", ARGV[6])
  redis.call("HINCRBY", KEYS[4], "retried", 1)
  return {attempts, 0}
end
"""

SWEEP = """
-- Requeue jobs whose leases expired (worker died / handler overran the
-- visibility timeout). Entirely atomic: no race with a concurrent ack,
-- because ack also runs atomically and the lease check is inside it.
-- KEYS: leases, queue, dlq, metrics
-- ARGV: key_prefix, now_ms
local now_ms = tonumber(ARGV[2])
local expired = redis.call("ZRANGEBYSCORE", KEYS[1], 0, now_ms)
local n = 0
for _, job_id in ipairs(expired) do
  local job_key = ARGV[1] .. "job:" .. job_id
  local raw = redis.call("HGET", job_key, "data")
  if raw then
    local job = cjson.decode(raw)
    local attempts = redis.call("HINCRBY", job_key, "attempts", 1)
    local max_retries = tonumber(job["max_retries"] or 3)
    redis.call("DEL", ARGV[1] .. "lease:" .. job_id)
    redis.call("ZREM", KEYS[1], job_id)
    if attempts > max_retries then
      job["attempts"] = attempts
      job["status"] = "dead"
      job["last_error"] = "lease expired: worker died or overran visibility timeout"
      job["dead_at"] = now_ms
      redis.call("RPUSH", KEYS[3], cjson.encode(job))
      redis.call("HSET", job_key, "status", "dead")
      redis.call("HINCRBY", KEYS[4], "dlq", 1)
    else
      local priority = tonumber(job["priority"] or 5)
      redis.call("ZADD", KEYS[2], now_ms - priority * 10000000000000, job_id)
      redis.call("HSET", job_key, "status", "queued")
      redis.call("HINCRBY", KEYS[4], "lease_expired", 1)
      redis.call("HINCRBY", KEYS[4], "retried", 1)
    end
    n = n + 1
  else
    redis.call("DEL", ARGV[1] .. "lease:" .. job_id)
    redis.call("ZREM", KEYS[1], job_id)
  end
end
return n
"""

PROMOTE = """
-- Move due delayed jobs (retry backoff elapsed / explicit delay passed)
-- back to the ready queue, preserving priority ordering.
-- KEYS: delayed, queue
-- ARGV: key_prefix, now_ms, limit
local due = redis.call("ZRANGEBYSCORE", KEYS[1], 0, ARGV[2], "LIMIT", 0, tonumber(ARGV[3]))
local n = 0
for _, job_id in ipairs(due) do
  local job_key = ARGV[1] .. "job:" .. job_id
  local raw = redis.call("HGET", job_key, "data")
  if raw then
    local priority = tonumber(cjson.decode(raw)["priority"] or 5)
    redis.call("ZADD", KEYS[2], tonumber(ARGV[2]) - priority * 10000000000000, job_id)
    redis.call("HSET", job_key, "status", "queued")
    n = n + 1
  end
  redis.call("ZREM", KEYS[1], job_id)
end
return n
"""

RATE_LIMIT = """
-- Token bucket, refilled lazily on each check. Atomic, so concurrent
-- workers cannot overspend the bucket.
-- KEYS: bucket
-- ARGV: capacity, refill_per_sec, now_ms, cost
local capacity = tonumber(ARGV[1])
local refill = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local cost = tonumber(ARGV[4] or 1)
local data = redis.call("HMGET", KEYS[1], "tokens", "updated")
local tokens = tonumber(data[1])
local updated = tonumber(data[2])
if not tokens then
  tokens = capacity
  updated = now
else
  tokens = math.min(capacity, tokens + (now - updated) / 1000 * refill)
end
if tokens >= cost then
  tokens = tokens - cost
  redis.call("HSET", KEYS[1], "tokens", tokens, "updated", now)
  redis.call("EXPIRE", KEYS[1], 3600)
  return 1
else
  redis.call("HSET", KEYS[1], "tokens", tokens, "updated", now)
  redis.call("EXPIRE", KEYS[1], 3600)
  return 0
end
"""

DEFER = """
-- Give up a lease and park the job in the delayed set (used when a tenant
-- is rate limited: the job is NOT lost, just postponed).
-- KEYS: leases, delayed
-- ARGV: key_prefix, job_id, worker_id, not_before_ms
local lease_key = ARGV[1] .. "lease:" .. ARGV[2]
if redis.call("HGET", lease_key, "worker_id") ~= ARGV[3] then
  return 0
end
redis.call("DEL", lease_key)
redis.call("ZREM", KEYS[1], ARGV[2])
redis.call("ZADD", KEYS[2], ARGV[4], ARGV[2])
redis.call("HSET", ARGV[1] .. "job:" .. ARGV[2], "status", "deferred")
return 1
"""

DEAD = """
-- Move a leased job straight to the DLQ without consuming retries.
-- Used for failures that will never succeed on retry (e.g. no handler
-- registered): retrying those is how you fill a DLQ with noise.
-- KEYS: leases, dlq, metrics
-- ARGV: key_prefix, job_id, worker_id, now_ms, error
local lease_key = ARGV[1] .. "lease:" .. ARGV[2]
local job_key = ARGV[1] .. "job:" .. ARGV[2]
if redis.call("HGET", lease_key, "worker_id") ~= ARGV[3] then
  return 0
end
local raw = redis.call("HGET", job_key, "data")
if raw then
  local job = cjson.decode(raw)
  job["attempts"] = tonumber(redis.call("HGET", job_key, "attempts") or 0)
  job["status"] = "dead"
  job["last_error"] = ARGV[5]
  job["dead_at"] = tonumber(ARGV[4])
  redis.call("RPUSH", KEYS[2], cjson.encode(job))
  redis.call("HSET", job_key, "status", "dead", "last_error", ARGV[5])
end
redis.call("DEL", lease_key)
redis.call("ZREM", KEYS[1], ARGV[2])
redis.call("HINCRBY", KEYS[3], "dlq", 1)
return 1
"""

RELEASE_ALL = """
-- Graceful shutdown: return every lease held by this worker to the ready
-- queue immediately instead of waiting for the visibility timeout.
-- KEYS: leases, queue
-- ARGV: key_prefix, worker_id, now_ms
local all = redis.call("ZRANGE", KEYS[1], 0, -1)
local n = 0
for _, job_id in ipairs(all) do
  if redis.call("HGET", ARGV[1] .. "lease:" .. job_id, "worker_id") == ARGV[2] then
    local raw = redis.call("HGET", ARGV[1] .. "job:" .. job_id, "data")
    if raw then
      local priority = tonumber(cjson.decode(raw)["priority"] or 5)
      redis.call("ZADD", KEYS[2], tonumber(ARGV[3]) - priority * 10000000000000, job_id)
      redis.call("HSET", ARGV[1] .. "job:" .. job_id, "status", "queued")
    end
    redis.call("DEL", ARGV[1] .. "lease:" .. job_id)
    redis.call("ZREM", KEYS[1], job_id)
    n = n + 1
  end
end
return n
"""

SCRIPTS = {
    "enqueue": ENQUEUE,
    "claim": CLAIM,
    "ack": ACK,
    "fail": FAIL,
    "sweep": SWEEP,
    "promote": PROMOTE,
    "rate_limit": RATE_LIMIT,
    "defer": DEFER,
    "dead": DEAD,
    "release_all": RELEASE_ALL,
}


def register(client: "redis.Redis") -> dict[str, "redis.client.Script"]:
    """Register all scripts on a redis-py client; returns name -> Script."""
    return {name: client.register_script(src) for name, src in SCRIPTS.items()}
