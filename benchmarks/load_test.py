"""Throughput + latency benchmark for TaskForge.

Spawns producer threads and in-process workers against a Redis instance,
then reports sustained throughput (jobs/sec, jobs/hour) and enqueue->ack
latency percentiles.

    python benchmarks/load_test.py --jobs 20000 --producers 4 --workers 2 --concurrency 8

Results are written as JSON to benchmarks/results/ and printed as a table.
Methodology notes are printed with the results -- quote them in the README,
don't cherry-pick the numbers.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import sys
import threading
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from taskforge import Config, Registry, TaskForge, Worker  # noqa: E402
from taskforge.logging_config import setup_logging  # noqa: E402
from taskforge.metrics import summarize  # noqa: E402

setup_logging("WARNING")


def _cpu_model() -> str:
    try:
        with open("/proc/cpuinfo") as f:
            for line in f:
                if line.startswith("model name"):
                    return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or "unknown"


def _redis_version(tf: TaskForge) -> str:
    return tf.r.info().get("redis_version", "unknown")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--jobs", type=int, default=20000)
    ap.add_argument("--producers", type=int, default=4)
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--concurrency", type=int, default=8)
    ap.add_argument("--redis-url", default=os.environ.get("REDIS_URL", "redis://localhost:6379/0"))
    ap.add_argument("--namespace", default="bench")
    ap.add_argument("--out-dir", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "results"))
    args = ap.parse_args()

    tf = TaskForge(args.redis_url, namespace=args.namespace)
    tf.purge()

    registry = Registry()

    @registry.register("benchmark_noop")
    def benchmark_noop(payload):
        return {"ok": True}

    config = Config(sweep_interval_s=5, poll_interval_s=0.01)
    workers = []
    for _ in range(args.workers):
        w = Worker(client=tf, handlers=registry, config=config,
                   concurrency=args.concurrency)
        t = threading.Thread(target=w.run, kwargs={"install_signals": False}, daemon=True)
        t.start()
        workers.append((w, t))

    per_producer = args.jobs // args.producers

    def produce(pid: int):
        for i in range(per_producer):
            tf.enqueue("benchmark_noop", {"p": pid, "i": i}, tenant=f"t{pid % 4}")

    threads = [threading.Thread(target=produce, args=(p,)) for p in range(args.producers)]
    t0 = time.monotonic()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    enqueue_done_s = time.monotonic() - t0

    # Wait for the drain.
    deadline = time.monotonic() + 300
    while time.monotonic() < deadline:
        if tf.stats()["counters"].get("acked", 0) >= args.jobs:
            break
        time.sleep(0.2)
    t1 = time.monotonic()
    for w, t in workers:
        w.request_stop()
    for _, t in workers:
        t.join(timeout=15)

    stats = tf.stats()
    acked = stats["counters"].get("acked", 0)
    elapsed = t1 - t0
    lat = summarize(tf.latency_samples())
    result = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "tool": "benchmarks/load_test.py (python producers + in-process workers)",
        "config": {"jobs": args.jobs, "producers": args.producers,
                   "workers": args.workers, "concurrency": args.concurrency,
                   "handler": "benchmark_noop (no-op)"},
        "machine": {"platform": platform.platform(), "cpu": _cpu_model(),
                    "cpus": os.cpu_count(), "redis_version": _redis_version(tf)},
        "acked": acked,
        "enqueue_phase_s": round(enqueue_done_s, 2),
        "total_elapsed_s": round(elapsed, 2),
        "jobs_per_sec": round(acked / elapsed, 1),
        "jobs_per_hour": int(acked / elapsed * 3600),
        "latency_ms": {k: round(v, 2) for k, v in lat.items()},
        "methodology": (
            "Producers and workers ran as threads in one Python process on the "
            "same host as Redis; producers enqueue via Lua, workers claim via "
            "Lua ZPOPMIN+lease and ack via Lua. Latency = enqueue->ack per job "
            "(includes queue wait). No-op handler isolates queue overhead. "
            "Same-host numbers are a ceiling; expect lower over a network."
        ),
    }

    os.makedirs(args.out_dir, exist_ok=True)
    path = os.path.join(args.out_dir,
                        f"load_test_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}.json")
    with open(path, "w") as f:
        json.dump(result, f, indent=2)

    print(f"\nTaskForge load test: {acked}/{args.jobs} acked in {elapsed:.1f}s")
    print(f"  throughput : {result['jobs_per_sec']:,.1f} jobs/s "
          f"({result['jobs_per_hour']:,} jobs/hour)")
    print(f"  latency    : p50={lat['p50']:.1f}ms p99={lat['p99']:.1f}ms "
          f"(n={lat['count']})")
    print(f"  machine    : {_cpu_model()} x{os.cpu_count()}, "
          f"redis {_redis_version(tf)}")
    print(f"  saved      : {path}")
    return 0 if acked >= args.jobs else 1


if __name__ == "__main__":
    sys.exit(main())
