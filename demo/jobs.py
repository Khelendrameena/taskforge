"""Realistic demo handlers for TaskForge.

Run a worker with these via::

    python -m taskforge.cli worker --handlers-module demo.jobs --concurrency 4

The star of the show is ``charge_card``: it is an *idempotent* handler. The
fake payment gateway records charges keyed by the order id in Redis, so even
if the queue redelivers the job (at-least-once!), the card is charged once.
That pairing -- at-least-once delivery + idempotent handler = effective
exactly-once -- is the core lesson of this project.
"""

from __future__ import annotations

import os
import time

import redis

from taskforge import Registry

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
registry = Registry()
_gw = redis.Redis.from_url(REDIS_URL, decode_responses=True)


def _gateway() -> redis.Redis:
    # Re-resolve per call so demo scripts can point at a fresh DB.
    return redis.Redis.from_url(os.environ.get("REDIS_URL", REDIS_URL),
                                decode_responses=True)


@registry.register("charge_card")
def charge_card(payload: dict) -> dict:
    """Fake payment charge. Idempotent on order_id via the gateway ledger."""
    order_id = payload["order_id"]
    amount = payload["amount"]
    gw = _gateway()
    # SET NX = "create only if absent": the atomicity that makes the handler
    # itself idempotent, independent of the queue's delivery guarantees.
    created = gw.set(f"demo:ledger:{order_id}", amount, nx=True)
    if not created:
        return {"order_id": order_id, "status": "already_charged",
                "amount": float(gw.get(f"demo:ledger:{order_id}"))}
    time.sleep(0.05)  # pretend to talk to a payment provider
    gw.rpush("demo:charges", order_id)
    return {"order_id": order_id, "status": "charged", "amount": amount}


@registry.register("send_welcome_email")
def send_welcome_email(payload: dict) -> dict:
    time.sleep(0.05)
    _gateway().rpush("demo:emails", payload["email"])
    return {"email": payload["email"], "status": "sent"}


@registry.register("flaky_job")
def flaky_job(payload: dict) -> dict:
    """Fails the first `fail_times` attempts, then succeeds. Shows retries."""
    gw = _gateway()
    key = f"demo:flaky:{payload['name']}"
    attempts = gw.incr(key)
    if attempts <= payload.get("fail_times", 2):
        raise RuntimeError(f"flaky failure #{attempts} (simulated)")
    return {"name": payload["name"], "status": "ok", "attempts": attempts}


@registry.register("poison_job")
def poison_job(payload: dict) -> dict:
    """Always fails. Ends up in the DLQ."""
    raise RuntimeError(f"poison pill detonated for {payload.get('name')}")


@registry.register("slow_job")
def slow_job(payload: dict) -> dict:
    """Sleeps longer than the lease timeout: used by the crash demo."""
    time.sleep(payload.get("sleep_s", 5))
    _gateway().rpush("demo:slow_done", payload.get("name", "slow"))
    return {"status": "ok"}


@registry.register("benchmark_noop")
def benchmark_noop(payload: dict) -> dict:
    return {"ok": True}
