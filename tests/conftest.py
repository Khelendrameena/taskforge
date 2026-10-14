"""Shared fixtures: real Redis, isolated namespace per test, worker runner."""

from __future__ import annotations

import os
import threading
import uuid

import pytest
import redis

from taskforge import Config, Registry, TaskForge, Worker


@pytest.fixture(scope="session")
def redis_url() -> str:
    url = os.environ.get("TASKFORGE_TEST_REDIS_URL", "redis://localhost:6379/0")
    redis.Redis.from_url(url, socket_timeout=5).ping()
    return url


@pytest.fixture()
def tf(redis_url: str) -> TaskForge:
    """Fresh client on a unique namespace; purged after the test."""
    client = TaskForge(redis_url, namespace=f"test-{uuid.uuid4().hex[:8]}")
    yield client
    client.purge()


@pytest.fixture()
def registry() -> Registry:
    return Registry()


@pytest.fixture()
def fast_config() -> Config:
    """Aggressive timings so failure-path tests run in seconds, not minutes."""
    return Config(
        visibility_timeout_s=30,
        sweep_interval_s=0.2,
        poll_interval_s=0.01,
        retry_base_s=0.05,
        retry_cap_s=0.2,
    )


class WorkerRunner:
    def __init__(self):
        self._workers: list[tuple[Worker, threading.Thread]] = []

    def start(self, client: TaskForge, registry: Registry,
              concurrency: int = 2, config: Config | None = None) -> Worker:
        worker = Worker(client=client, handlers=registry,
                        concurrency=concurrency, config=config or Config())
        t = threading.Thread(target=worker.run,
                             kwargs={"install_signals": False}, daemon=True)
        t.start()
        self._workers.append((worker, t))
        return worker

    def stop_all(self):
        for worker, t in self._workers:
            worker.request_stop()
        for _, t in self._workers:
            t.join(timeout=15)
        self._workers.clear()


@pytest.fixture()
def runner() -> WorkerRunner:
    r = WorkerRunner()
    yield r
    r.stop_all()


@pytest.fixture()
def gw(redis_url: str):
    """Raw Redis handle for test doubles (fake gateways, counters)."""
    return redis.Redis.from_url(redis_url, decode_responses=True)
