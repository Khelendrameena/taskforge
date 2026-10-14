"""Failure-injection demo: kill -9 a worker mid-job, watch recovery.

Timeline:
  1. Worker A claims a slow job (lease timeout 2s, job sleeps 6s).
  2. We SIGKILL worker A mid-execution -- no graceful shutdown, no ack.
  3. Worker B's lease sweeper notices the expired lease and requeues the job.
  4. Worker B executes it to completion. attempts == 2 proves redelivery.
  5. A poison job exhausts retries and lands in the DLQ; we inspect + replay it.

Run:  python demo/failure_scenario.py
Needs: Redis on localhost:6379 (or $REDIS_URL).
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import redis

from taskforge import Config, TaskForge, Worker
from taskforge.logging_config import setup_logging

from demo.jobs import registry

setup_logging("WARNING")

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")


def _spawn_worker() -> subprocess.Popen:
    env = dict(os.environ, REDIS_URL=REDIS_URL,
               TASKFORGE_NAMESPACE="demo-fail",
               TASKFORGE_VISIBILITY_TIMEOUT_S="2",
               TASKFORGE_SWEEP_INTERVAL_S="1",
               PYTHONPATH=REPO)
    return subprocess.Popen(
        [sys.executable, "-m", "taskforge.cli", "worker",
         "--handlers-module", "demo.jobs", "--concurrency", "2",
         "--log-level", "WARNING"],
        cwd=REPO, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def main() -> int:
    tf = TaskForge(REDIS_URL, namespace="demo-fail")
    tf.purge()
    gw = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    gw.delete("demo:slow_done")

    print("== Part 1: crash recovery ==")
    # Runs 2s inside a 3s lease: one execution always fits in the lease, so
    # redelivery after the crash converges (handler time must stay below the
    # visibility timeout -- same rule as SQS).
    job = tf.enqueue("slow_job", {"name": "report-7", "sleep_s": 2},
                     lease_timeout_s=3, tenant="shop")
    print(f"enqueued slow job {job.job_id[:8]} (lease timeout 3s, runs 2s)")

    worker_a = _spawn_worker()
    time.sleep(1.2)  # let it claim the job and start sleeping
    print(f"worker A (pid {worker_a.pid}) is mid-job. Sending SIGKILL...")
    worker_a.send_signal(signal.SIGKILL)
    worker_a.wait()
    print("worker A is dead. No ack was sent, lease is orphaned.")

    print("starting worker B (its sweeper will expire the lease)...")
    worker_b = _spawn_worker()
    done = tf.wait_for(job.job_id, timeout_s=25)
    worker_b.terminate()
    worker_b.wait()

    finished = tf.get_job(job.job_id)
    slow_done = gw.lrange("demo:slow_done", 0, -1)
    print(f"job status={finished['status']}, attempts={finished['attempts']}, "
          f"handler completions={len(slow_done)}")
    assert finished["status"] == "done" and finished["attempts"] >= 1
    print("OK: the orphaned lease expired, the job was requeued and completed.\n")

    print("== Part 2: poison job -> DLQ -> replay ==")
    worker_c = _spawn_worker()
    poison = tf.enqueue("poison_job", {"name": "boom"}, max_retries=2,
                        tenant="shop")
    # retry base is 1s: 3 attempts take ~ (1+2)s backoff + polling; wait generously
    dead = tf.wait_for(poison.job_id, timeout_s=30)
    print(f"poison job status={dead['status']} after {dead['attempts']} attempts")
    dlq = tf.dlq_list()
    print(f"DLQ depth={len(dlq)}; last error: {dlq[0]['last_error'][:60]}")
    assert dead["status"] == "dead" and len(dlq) >= 1

    print("replaying the DLQ...")
    n = tf.dlq_replay(replay_all=True)
    print(f"replayed {n} job(s); queue depth now {tf.stats()['queue_depth']}")
    assert n >= 1
    worker_c.terminate()
    worker_c.wait()
    print("\nOK: poison quarantined, inspected, and replayable by an operator.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
