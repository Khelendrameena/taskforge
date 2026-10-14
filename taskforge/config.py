"""Configuration, sourced from environment variables with sane defaults."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _getenv(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass
class Config:
    redis_url: str = field(default_factory=lambda: _getenv("REDIS_URL", "redis://localhost:6379/0"))
    namespace: str = field(default_factory=lambda: _getenv("TASKFORGE_NAMESPACE", "tf"))

    # How long a worker owns a claimed job before the lease expires and the
    # job is requeued (the "visibility timeout" in SQS terms).
    visibility_timeout_s: float = field(
        default_factory=lambda: float(_getenv("TASKFORGE_VISIBILITY_TIMEOUT_S", "30"))
    )
    # How often a worker sweeps expired leases.
    sweep_interval_s: float = field(
        default_factory=lambda: float(_getenv("TASKFORGE_SWEEP_INTERVAL_S", "5"))
    )
    # Idle sleep when the queue is empty.
    poll_interval_s: float = field(
        default_factory=lambda: float(_getenv("TASKFORGE_POLL_INTERVAL_S", "0.05"))
    )
    worker_concurrency: int = field(
        default_factory=lambda: int(_getenv("TASKFORGE_WORKER_CONCURRENCY", "4"))
    )

    # Retry backoff: delay = min(cap, base * 2**attempt) + uniform(0, delay/2).
    retry_base_s: float = field(default_factory=lambda: float(_getenv("TASKFORGE_RETRY_BASE_S", "1.0")))
    retry_cap_s: float = field(default_factory=lambda: float(_getenv("TASKFORGE_RETRY_CAP_S", "60.0")))

    # Idempotency keys live this long so late producer retries are still
    # recognised as duplicates (even after the job finished).
    idempotency_ttl_s: int = field(
        default_factory=lambda: int(_getenv("TASKFORGE_IDEMPOTENCY_TTL_S", "86400"))
    )

    log_level: str = field(default_factory=lambda: _getenv("TASKFORGE_LOG_LEVEL", "INFO"))
