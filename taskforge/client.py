"""Producer SDK + admin API.

The producer side of TaskForge. Enqueueing is idempotent when you pass an
``idempotency_key``: if the broker already saw the key, you get the original
job id back and nothing is enqueued twice. That is what makes "the producer
timed out, so it retried" safe -- the canonical duplicate-charge scenario.
"""

from __future__ import annotations

import itertools
import json
import logging
import time
import uuid
from dataclasses import dataclass

import redis

from . import lua as lua_scripts
from .config import Config
from .keys import Keys

log = logging.getLogger("taskforge.client")


def _now_ms() -> int:
    return int(time.time() * 1000)


@dataclass
class EnqueueResult:
    job_id: str
    duplicate: bool  # True when the idempotency key was already seen


class TaskForge:
    """Thin, explicit client over Redis. No hidden threads, no magic."""

    def __init__(
        self,
        redis_url: str | None = None,
        namespace: str = "tf",
        config: Config | None = None,
    ):
        self.config = config or Config()
        self.r = redis.Redis.from_url(
            redis_url or self.config.redis_url,
            decode_responses=True,
            socket_timeout=10,
        )
        self.keys = Keys(namespace)
        self.prefix = namespace + ":"
        self._scripts = lua_scripts.register(self.r)
        # tenant -> (capacity, refill_per_second); empty = no limiting
        self._rate_limits: dict[str, tuple[float, float]] = {}
        # Per-producer sequence number: breaks score ties between jobs
        # enqueued in the same millisecond, so same-priority FIFO is exact
        # instead of falling back to lexicographic job-id order.
        self._seq = itertools.count(1)

    # -- producing ------------------------------------------------------
    def enqueue(
        self,
        fn: str,
        payload: dict | None = None,
        *,
        idempotency_key: str | None = None,
        priority: int = 5,
        tenant: str = "default",
        max_retries: int = 3,
        lease_timeout_s: float | None = None,
        delay_s: float = 0,
        idempotency_ttl_s: int | None = None,
    ) -> EnqueueResult:
        """Enqueue one job.

        priority: 1 (lowest) .. 10 (highest). Higher priority jobs are
            claimed first regardless of enqueue time; FIFO within a level.
        """
        if not 1 <= priority <= 10:
            raise ValueError("priority must be 1..10")
        job_id = uuid.uuid4().hex
        now = _now_ms()
        job = {
            "id": job_id,
            "fn": fn,
            "payload": payload or {},
            "tenant": tenant,
            "priority": priority,
            "max_retries": max_retries,
            "attempts": 0,
            "lease_timeout_s": lease_timeout_s or self.config.visibility_timeout_s,
            "idempotency_key": idempotency_key,
            "enqueued_at": now,
            "status": "queued",
        }
        idem_key = self.keys.idempotency(tenant, idempotency_key) if idempotency_key else ""
        # Fractional tiebreak keeps FIFO exact within a priority level even
        # when two enqueues land in the same millisecond.
        score = now - priority * lua_scripts.PRIORITY_SPAN + next(self._seq) / 1000
        not_before = now + int(delay_s * 1000) if delay_s > 0 else 0
        out = self._scripts["enqueue"](
            keys=[self.keys.queue, idem_key, self.keys.job(job_id),
                  self.keys.delayed, self.keys.metrics],
            args=[job_id, json.dumps(job), score,
                  idempotency_ttl_s or self.config.idempotency_ttl_s, not_before],
        )
        returned_id, is_new = out[0], bool(int(out[1]))
        if not is_new:
            log.info("duplicate suppressed", extra={"event": "duplicate_suppressed",
                     "job_id": returned_id, "tenant": tenant,
                     "idempotency_key": idempotency_key})
        else:
            log.info("enqueued", extra={"event": "enqueued", "job_id": returned_id,
                     "fn": fn, "tenant": tenant, "priority": priority})
        return EnqueueResult(job_id=returned_id, duplicate=not is_new)

    # -- inspection ------------------------------------------------------
    def get_job(self, job_id: str) -> dict | None:
        key = self.keys.job(job_id)
        data = self.r.hget(key, "data")
        if not data:
            return None
        job = json.loads(data)
        fields = self.r.hmget(key, ["attempts", "status", "result", "last_error"])
        job["attempts"] = int(fields[0] or 0)
        job["status"] = fields[1] or job.get("status")
        if fields[2]:
            job["result"] = json.loads(fields[2])
        if fields[3]:
            job["last_error"] = fields[3]
        return job

    def stats(self) -> dict:
        r, k = self.r, self.keys
        counters = r.hgetall(k.metrics)
        return {
            "queue_depth": r.zcard(k.queue),
            "delayed": r.zcard(k.delayed),
            "active_leases": r.zcard(k.leases),
            "dlq_depth": r.llen(k.dlq),
            "counters": {name: int(v) for name, v in counters.items()},
        }

    def latency_samples(self, limit: int = 20000) -> list[float]:
        return [float(x) for x in self.r.lrange(self.keys.latency, 0, limit - 1)]

    # -- dead-letter queue -----------------------------------------------
    def dlq_list(self, limit: int = 50) -> list[dict]:
        return [json.loads(x) for x in self.r.lrange(self.keys.dlq, 0, limit - 1)]

    def dlq_replay(self, job_id: str | None = None, replay_all: bool = False) -> int:
        """Return dead jobs to the ready queue with attempts reset.

        Returns the number of jobs replayed.
        """
        if not job_id and not replay_all:
            raise ValueError("pass job_id or replay_all=True")
        entries = self.r.lrange(self.keys.dlq, 0, -1)
        replayed = 0
        for raw in entries:
            job = json.loads(raw)
            if replay_all or job.get("id") == job_id:
                if self.r.lrem(self.keys.dlq, 1, raw):
                    job["attempts"] = 0
                    job["status"] = "queued"
                    job.pop("last_error", None)
                    job.pop("dead_at", None)
                    now = _now_ms()
                    job["enqueued_at"] = now
                    pipe = self.r.pipeline()
                    pipe.hset(self.keys.job(job["id"]), mapping={
                        "data": json.dumps(job), "status": "queued", "attempts": 0})
                    pipe.zadd(self.keys.queue,
                              {job["id"]: now - job.get("priority", 5) * lua_scripts.PRIORITY_SPAN})
                    pipe.hincrby(self.keys.metrics, "dlq_replayed", 1)
                    pipe.execute()
                    replayed += 1
                    log.info("dlq replayed", extra={"event": "dlq_replayed",
                             "job_id": job["id"], "fn": job.get("fn")})
        return replayed

    # -- rate limiting ----------------------------------------------------
    def set_rate_limit(self, tenant: str, capacity: float, refill_per_second: float) -> None:
        """Allow `capacity` burst, refilling at `refill_per_second` tokens/sec."""
        self._rate_limits[tenant] = (capacity, refill_per_second)

    def allow(self, tenant: str, cost: float = 1) -> bool:
        limit = self._rate_limits.get(tenant)
        if limit is None:
            return True
        capacity, refill = limit
        return bool(self._scripts["rate_limit"](
            keys=[self.keys.rate_limit(tenant)],
            args=[capacity, refill, _now_ms(), cost]))

    # -- worker internals (used by Worker; public for tests) ---------------
    def claim(self, worker_id: str, default_lease_timeout_s: float) -> tuple[str, dict] | None:
        out = self._scripts["claim"](
            keys=[self.keys.queue, self.keys.leases, self.keys.metrics],
            args=[_now_ms(), worker_id, self.prefix, default_lease_timeout_s],
        )
        if not out:
            return None
        return out[0], json.loads(out[1])

    def ack(self, job_id: str, worker_id: str, result: object = None) -> bool:
        job = self.get_job(job_id)
        enqueued_at = (job or {}).get("enqueued_at", _now_ms())
        latency_ms = _now_ms() - enqueued_at
        ok = bool(self._scripts["ack"](
            keys=[self.keys.leases, self.keys.metrics, self.keys.latency],
            args=[self.prefix, job_id, worker_id, _now_ms(),
                  json.dumps(result), latency_ms],
        ))
        if ok:
            log.info("acked", extra={"event": "acked", "job_id": job_id,
                     "latency_ms": latency_ms})
        else:
            log.warning("ack on lost lease", extra={"event": "ack_lease_lost",
                        "job_id": job_id})
        return ok

    def fail(self, job_id: str, worker_id: str, error: str, backoff_ms: int) -> tuple[int, bool]:
        """Returns (attempts, dead_lettered). attempts == -1 means lease lost."""
        out = self._scripts["fail"](
            keys=[self.keys.leases, self.keys.delayed, self.keys.dlq, self.keys.metrics],
            args=[self.prefix, job_id, worker_id, _now_ms(), backoff_ms, error[:500]],
        )
        attempts, dead = int(out[0]), bool(int(out[1]))
        if attempts == -1:
            log.warning("fail on lost lease", extra={"event": "fail_lease_lost",
                        "job_id": job_id})
        else:
            log.info("job failed", extra={"event": "job_failed" if not dead else "job_dead",
                     "job_id": job_id, "attempts": attempts, "error": error[:200],
                     "backoff_ms": backoff_ms})
        return attempts, dead

    def dead_letter(self, job_id: str, worker_id: str, error: str) -> bool:
        """Park a leased job in the DLQ immediately, skipping retries."""
        ok = bool(self._scripts["dead"](
            keys=[self.keys.leases, self.keys.dlq, self.keys.metrics],
            args=[self.prefix, job_id, worker_id, _now_ms(), error[:500]],
        ))
        if ok:
            log.info("dead-lettered directly", extra={"event": "job_dead",
                     "job_id": job_id, "error": error[:200]})
        return ok

    def sweep_expired_leases(self) -> int:
        n = int(self._scripts["sweep"](
            keys=[self.keys.leases, self.keys.queue, self.keys.dlq, self.keys.metrics],
            args=[self.prefix, _now_ms()]))
        if n:
            log.info("swept expired leases", extra={"event": "leases_swept", "count": n})
        return n

    def promote_due(self, limit: int = 500) -> int:
        return int(self._scripts["promote"](
            keys=[self.keys.delayed, self.keys.queue],
            args=[self.prefix, _now_ms(), limit]))

    def defer(self, job_id: str, worker_id: str, delay_ms: int) -> bool:
        return bool(self._scripts["defer"](
            keys=[self.keys.leases, self.keys.delayed],
            args=[self.prefix, job_id, worker_id, _now_ms() + delay_ms]))

    def release_all_leases(self, worker_id: str) -> int:
        return int(self._scripts["release_all"](
            keys=[self.keys.leases, self.keys.queue],
            args=[self.prefix, worker_id, _now_ms()]))

    def wait_for(self, job_id: str, timeout_s: float = 30) -> dict | None:
        """Block until a job reaches a terminal state; returns the job dict."""
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            job = self.get_job(job_id)
            if job and job.get("status") in ("done", "dead"):
                return job
            time.sleep(0.05)
        return self.get_job(job_id)

    def purge(self) -> int:
        """Delete every key in this namespace. Tests and local dev only."""
        n = 0
        for key in self.r.scan_iter(self.keys.scan_pattern(), count=500):
            self.r.delete(key)
            n += 1
        return n
