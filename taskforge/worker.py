"""Competing-consumer worker pool.

Each worker process runs one claim loop and a thread pool. Many worker
processes can run against the same Redis: the Lua ``claim`` script guarantees
a job is handed to exactly one of them (competing consumers). Lease expiry
plus the periodic sweep is what makes "worker dies mid-job" recoverable.

Shutdown is graceful: on SIGTERM/SIGINT the worker stops claiming, waits for
in-flight jobs to finish, and immediately releases any leases it still holds
so other workers pick them up without waiting for the visibility timeout.
"""

from __future__ import annotations

import logging
import random
import signal
import socket
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

from .client import TaskForge
from .config import Config
from .exceptions import HandlerNotFound
from .handlers import Registry

log = logging.getLogger("taskforge.worker")


class Worker:
    def __init__(
        self,
        client: TaskForge | None = None,
        handlers: Registry | None = None,
        config: Config | None = None,
        concurrency: int | None = None,
        worker_id: str | None = None,
    ):
        self.config = config or Config()
        self.client = client or TaskForge(config=self.config)
        self.handlers = handlers or Registry()
        self.concurrency = concurrency or self.config.worker_concurrency
        self.worker_id = worker_id or f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
        self._stop = threading.Event()
        self._pool = ThreadPoolExecutor(
            max_workers=self.concurrency, thread_name_prefix=f"tf-{self.worker_id[:8]}")
        self._last_sweep = 0.0
        self._last_promote = 0.0
        self._claimed = 0

    # -- lifecycle ------------------------------------------------------
    def run(self, install_signals: bool = True) -> None:
        """Block running the claim loop until shutdown is requested."""
        if install_signals and threading.current_thread() is threading.main_thread():
            for sig in (signal.SIGTERM, signal.SIGINT):
                signal.signal(sig, self._handle_signal)
        log.info("worker started", extra={"event": "worker_started",
                 "worker_id": self.worker_id, "concurrency": self.concurrency})
        try:
            while not self._stop.is_set():
                self._tick()
        finally:
            self._shutdown()
        log.info("worker stopped", extra={"event": "worker_stopped",
                 "worker_id": self.worker_id, "claimed": self._claimed})

    def request_stop(self) -> None:
        self._stop.set()

    def _handle_signal(self, signum, frame) -> None:  # noqa: ARG002
        log.info("shutdown requested", extra={"event": "shutdown_requested",
                 "worker_id": self.worker_id, "signal": signum})
        self.request_stop()

    def _shutdown(self) -> None:
        # Stop accepting new work, let in-flight jobs finish (bounded wait),
        # then hand any leases we still hold straight back to the queue.
        self._pool.shutdown(wait=True)
        released = self.client.release_all_leases(self.worker_id)
        if released:
            log.info("released leases on shutdown",
                     extra={"event": "leases_released", "count": released})

    # -- main loop --------------------------------------------------------
    def _tick(self) -> None:
        now = time.monotonic()
        # Promoting the delayed set costs a Lua round trip; under load _tick
        # runs thousands of times per second, so throttle it. 250ms
        # granularity is plenty for retry backoff and explicit delays.
        if now - self._last_promote >= 0.25:
            self._last_promote = now
            promoted = self.client.promote_due()
            if promoted:
                log.debug("promoted delayed jobs",
                          extra={"event": "delayed_promoted", "count": promoted})

        if now - self._last_sweep >= self.config.sweep_interval_s:
            self._last_sweep = now
            self.client.sweep_expired_leases()

        claimed = self.client.claim(self.worker_id, self.config.visibility_timeout_s)
        if claimed is None:
            time.sleep(self.config.poll_interval_s)
            return
        job_id, job = claimed
        self._claimed += 1

        # Per-tenant rate limiting: if the bucket is empty the job is NOT
        # executed and NOT dropped -- it is deferred 1s and re-queued by the
        # promote step. This keeps the limiter honest without busy-spinning.
        if not self.client.allow(job.get("tenant", "default")):
            self.client.defer(job_id, self.worker_id, delay_ms=1000)
            log.info("rate limited, deferred", extra={"event": "rate_limited",
                     "job_id": job_id, "tenant": job.get("tenant")})
            return

        self._pool.submit(self._execute, job_id, job)

    # -- execution ----------------------------------------------------------
    def _execute(self, job_id: str, job: dict) -> None:
        fn_name = job.get("fn", "")
        log.info("executing", extra={"event": "job_started", "job_id": job_id,
                 "fn": fn_name, "tenant": job.get("tenant"),
                 "attempt": int(job.get("attempts", 0)) + 1})
        try:
            handler = self.handlers.get(fn_name)
        except HandlerNotFound:
            # Unknown function: dead-letter immediately with no retries --
            # retrying a job nobody can ever execute is how you fill a DLQ
            # with noise.
            self.client.dead_letter(job_id, self.worker_id,
                                    f"HandlerNotFound: {fn_name!r}")
            log.error("no handler, dead-lettered", extra={"event": "job_dead",
                      "job_id": job_id, "fn": fn_name})
            return

        started = time.monotonic()
        try:
            result = handler(job.get("payload") or {})
        except Exception as exc:  # noqa: BLE001 -- the queue must survive any handler bug
            attempt = int(job.get("attempts", 0)) + 1
            backoff_ms = self._backoff_ms(attempt)
            attempts, dead = self.client.fail(
                job_id, self.worker_id, f"{type(exc).__name__}: {exc}", backoff_ms)
            if attempts == -1:
                # Our lease expired while the handler ran (slow handler or a
                # long GC pause). The sweeper requeued it; another worker owns
                # it now. Do nothing -- this is at-least-once working as designed.
                log.warning("lease lost during execution",
                            extra={"event": "lease_lost", "job_id": job_id})
            return

        elapsed_ms = (time.monotonic() - started) * 1000
        try:
            ok = self.client.ack(job_id, self.worker_id, result)
        except Exception:  # noqa: BLE001
            log.exception("ack failed", extra={"event": "ack_error", "job_id": job_id})
            return
        if not ok:
            # Lease expired while we ran (slow handler / long pause): the
            # sweeper requeued the job and someone else owns it. Our copy is
            # stale -- dropping it is correct under at-least-once semantics.
            log.warning("dropping stale copy after lost lease",
                        extra={"event": "stale_copy_dropped", "job_id": job_id})
            return
        log.info("done", extra={"event": "job_done", "job_id": job_id,
                 "fn": fn_name, "exec_ms": round(elapsed_ms, 2)})

    def _backoff_ms(self, attempt: int) -> int:
        """Exponential backoff with jitter.

        delay = min(cap, base * 2**attempt); actual = delay + U(0, delay/2).

        The exponential part spreads retries out as failures persist; the
        jitter ("equal-ish jitter") breaks the thundering-herd pattern where
        many failed jobs would otherwise retry at the exact same instant.
        """
        cfg = self.config
        delay = min(cfg.retry_cap_s, cfg.retry_base_s * (2**attempt))
        jitter = random.uniform(0, delay / 2)
        return int((delay + jitter) * 1000)
