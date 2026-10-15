"""Automated fault injection: SIGKILL a worker mid-job, prove recovery.

This is the test that mirrors the production incident story: a worker dies
without acking, the lease expires, another worker picks the job up.
"""

import os
import signal
import subprocess
import sys
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _spawn_worker(namespace: str) -> subprocess.Popen:
    # The subprocess must talk to the SAME redis the test used: honour the
    # test URL env var (the default pytest run uses DB 0).
    redis_url = os.environ.get("TASKFORGE_TEST_REDIS_URL", "redis://localhost:6379/0")
    env = dict(
        os.environ,
        REDIS_URL=redis_url,
        TASKFORGE_NAMESPACE=namespace,
        TASKFORGE_SWEEP_INTERVAL_S="0.3",
        TASKFORGE_POLL_INTERVAL_S="0.02",
        PYTHONPATH=REPO,
    )
    return subprocess.Popen(
        [sys.executable, "-m", "taskforge.cli", "worker",
         "--handlers-module", "demo.jobs", "--concurrency", "2",
         "--log-level", "ERROR"],
        cwd=REPO, env=env,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def test_worker_crash_mid_job_recovers(tf, gw):
    # Job runs 2s with a 3s lease: any single execution fits in the lease,
    # so redelivery after the crash converges instead of expiring forever.
    # (If handler time > visibility timeout, SQS-style queues redeliver
    # forever -- that is correct behavior, just not this test's scenario.)
    res = tf.enqueue("slow_job", {"name": "crash-me", "sleep_s": 2},
                     lease_timeout_s=3)
    ns = tf.keys.p  # namespace, so the subprocess joins the same queue

    killer = _spawn_worker(ns)
    time.sleep(1.2)  # worker claimed the job and is asleep inside the handler
    killer.send_signal(signal.SIGKILL)
    killer.wait(timeout=10)

    # A fresh worker's sweeper expires the orphaned lease and re-runs the job.
    rescuer = _spawn_worker(ns)
    try:
        done = tf.wait_for(res.job_id, timeout_s=25)
    finally:
        rescuer.terminate()
        rescuer.wait(timeout=10)

    assert done["status"] == "done"
    assert done["attempts"] >= 1, "job must have been redelivered after the crash"
    assert tf.stats()["counters"].get("lease_expired", 0) >= 1
