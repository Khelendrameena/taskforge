"""Redis key schema for TaskForge.

All keys live under a single namespace prefix (default ``tf``) so multiple
TaskForge deployments can share one Redis instance, and so tests can
``purge()`` everything with one SCAN.
"""

from __future__ import annotations


class Keys:
    def __init__(self, prefix: str = "tf"):
        self.p = prefix

    # -- queues -----------------------------------------------------------
    @property
    def queue(self) -> str:
        """Ready jobs, sorted set scored by priority-adjusted enqueue time."""
        return f"{self.p}:queue"

    @property
    def delayed(self) -> str:
        """Jobs waiting for retry backoff / explicit delay. Score = run-at ms."""
        return f"{self.p}:delayed"

    @property
    def dlq(self) -> str:
        """Dead-letter queue: list of JSON snapshots of poison jobs."""
        return f"{self.p}:dlq"

    # -- leases -----------------------------------------------------------
    @property
    def leases(self) -> str:
        """Active leases. Sorted set member=job_id, score=deadline (unix ms)."""
        return f"{self.p}:leases"

    def lease(self, job_id: str) -> str:
        return f"{self.p}:lease:{job_id}"

    # -- jobs / idempotency -----------------------------------------------
    def job(self, job_id: str) -> str:
        return f"{self.p}:job:{job_id}"

    def idempotency(self, tenant: str, key: str) -> str:
        return f"{self.p}:idem:{tenant}:{key}"

    # -- rate limiting / metrics ------------------------------------------
    def rate_limit(self, tenant: str) -> str:
        return f"{self.p}:rl:{tenant}"

    @property
    def metrics(self) -> str:
        return f"{self.p}:metrics"

    @property
    def latency(self) -> str:
        """Recent enqueue->ack latency samples (ms), newest first, capped."""
        return f"{self.p}:latency"

    def scan_pattern(self) -> str:
        return f"{self.p}:*"
