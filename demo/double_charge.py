"""The double-charge demo: one command, the highest-ROI interview story.

Scenario: a customer clicks "Pay" once. The producer sends the charge job to
the broker, but the *acknowledgement* times out, so the producer retries with
the SAME idempotency key. A naive queue would enqueue (and charge) twice.

Run:  python demo/double_charge.py
Needs: Redis on localhost:6379 (or $REDIS_URL), e.g. `docker compose up -d redis`

Expected output ends with:  charges recorded: 1  -> the card was charged once.
"""

from __future__ import annotations

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import redis

from demo.jobs import registry
from taskforge import Config, TaskForge, Worker
from taskforge.logging_config import setup_logging

setup_logging("WARNING")

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")


def main() -> int:
    tf = TaskForge(REDIS_URL, namespace="demo-charge")
    tf.purge()
    gw = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    gw.delete("demo:charges")
    for k in gw.scan_iter("demo:ledger:*"):
        gw.delete(k)

    print("1. Customer clicks Pay for order-123 (amount 499).")
    r1 = tf.enqueue("charge_card", {"order_id": "order-123", "amount": 499},
                    idempotency_key="order-123", priority=10, tenant="shop")
    print(f"   producer -> broker: job {r1.job_id[:8]}...  (duplicate={r1.duplicate})")

    print("2. Broker ack TIMES OUT on the way back. Producer retries, same key.")
    r2 = tf.enqueue("charge_card", {"order_id": "order-123", "amount": 499},
                    idempotency_key="order-123", priority=10, tenant="shop")
    print(f"   producer -> broker: job {r2.job_id[:8]}...  (duplicate={r2.duplicate})")
    assert r1.job_id == r2.job_id, "idempotency key must map to the same job"

    print("3. Worker picks it up and charges the card.")
    worker = Worker(client=tf, handlers=registry,
                    config=Config(visibility_timeout_s=10, sweep_interval_s=1),
                    concurrency=2)
    import threading
    t = threading.Thread(target=worker.run, kwargs={"install_signals": False}, daemon=True)
    t.start()
    job = tf.wait_for(r1.job_id, timeout_s=15)
    worker.request_stop()
    t.join(timeout=10)

    charges = gw.lrange("demo:charges", 0, -1)
    print(f"4. Result: job status={job['status']}, charges recorded: {len(charges)}")
    print(f"   idempotency_hits counter: {tf.stats()['counters'].get('idempotency_hits', 0)}")
    assert len(charges) == 1, f"DOUBLE CHARGE! charges={charges}"
    assert job["status"] == "done"
    print("\nOK: one click, one charge -- the duplicate submission was suppressed.")
    print("Interview line: 'at-least-once delivery + idempotent producer keys +")
    print("idempotent handler = effective exactly-once for money.'")
    return 0


if __name__ == "__main__":
    sys.exit(main())
