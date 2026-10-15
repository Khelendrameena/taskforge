"""Tiny HTTP front-end for TaskForge, used by the k6 benchmark.

Stdlib only (http.server):

    POST /enqueue          accept a job into the queue (k6 load generation)
    GET  /stats             raw TaskForge stats (k6 harness)
    GET  /api/stats         enriched stats for the dashboard: depth per
                           priority, per-second rates, workers, latency
    GET  /dashboard         minimal live dashboard (dashboard.html)
    GET  /health            liveness probe

    python benchmarks/demo_api.py  # serves on :8080, namespace "k6bench"
"""

from __future__ import annotations

import json
import os
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from taskforge import TaskForge  # noqa: E402
from taskforge.logging_config import setup_logging  # noqa: E402
from taskforge.metrics import summarize  # noqa: E402

setup_logging("WARNING")
PORT = int(os.environ.get("DEMO_API_PORT", "8080"))
tf = TaskForge(os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
               namespace=os.environ.get("TASKFORGE_NAMESPACE", "k6bench"))

# Last sampled counters + timestamp, so the API can report honest per-second
# rates between polls (rates are computed server-side, never fabricated).
_prev_sample: dict | None = None


def _enriched_stats() -> dict:
    """Everything the dashboard needs in one JSON payload."""
    global _prev_sample
    now = time.monotonic()
    base = tf.stats()
    counters = base.pop("counters")

    rates: dict[str, float] = {}
    if _prev_sample is not None:
        dt = max(now - _prev_sample["ts"], 1e-3)
        for name in ("enqueued", "claimed", "acked", "retried", "dlq"):
            delta = counters.get(name, 0) - _prev_sample["counters"].get(name, 0)
            rates[f"{name}_per_s"] = round(max(delta, 0) / dt, 2)
    _prev_sample = {"ts": now, "counters": counters}

    latency = summarize(tf.latency_samples(1000))
    return {
        "ts": now,
        "namespace": tf.keys.p,
        "queue": {"total": base["queue_depth"],
                  "by_priority": tf.depth_by_priority()},
        "delayed": base["delayed"],
        "active_leases": base["active_leases"],
        "dlq_depth": base["dlq_depth"],
        "counters": counters,
        "rates": rates,
        "workers": tf.workers(),
        "latency_ms": {"p50": round(latency.get("p50", 0), 1),
                       "p99": round(latency.get("p99", 0), 1),
                       "count": latency.get("count", 0)},
    }


_DASHBOARD_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               "dashboard.html")


class Handler(BaseHTTPRequestHandler):
    server_version = "TaskForgeDemoAPI/0.1"

    def _json(self, code: int, obj: dict) -> None:
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _serve_dashboard(self) -> None:
        try:
            with open(_DASHBOARD_PATH, "rb") as f:
                body = f.read()
        except OSError:
            self._json(500, {"error": "dashboard.html not found"})
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802
        if self.path == "/health":
            self._json(200, {"ok": True})
        elif self.path == "/stats":
            self._json(200, tf.stats())
        elif self.path == "/api/stats":
            self._json(200, _enriched_stats())
        elif self.path in ("/dashboard", "/dashboard/"):
            self._serve_dashboard()
        else:
            self._json(404, {"error": "not found"})

    def do_POST(self):  # noqa: N802
        if self.path != "/enqueue":
            self._json(404, {"error": "not found"})
            return
        length = int(self.headers.get("Content-Length", 0))
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError:
            self._json(400, {"error": "invalid JSON"})
            return
        try:
            priority = int(body.get("priority", 5))
        except (TypeError, ValueError):
            self._json(400, {"error": "priority must be an integer 1..10"})
            return
        try:
            res = tf.enqueue(
                body.get("fn", "benchmark_noop"),
                body.get("payload", {}),
                idempotency_key=body.get("idempotency_key"),
                priority=priority,
                tenant=body.get("tenant", "k6"),
            )
        except ValueError as exc:
            self._json(400, {"error": str(exc)})
            return
        self._json(202, {"job_id": res.job_id, "duplicate": res.duplicate})

    def log_message(self, *args):  # keep k6 output clean
        pass


if __name__ == "__main__":
    print(f"demo API on :{PORT} (namespace k6bench)", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
