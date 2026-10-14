"""Tiny HTTP front-end for TaskForge, used by the k6 benchmark.

Stdlib only (http.server): POST /enqueue, GET /stats, GET /health.
This is a load-generation harness, not production API design.

    python benchmarks/demo_api.py  # serves on :8080, namespace "k6bench"
"""

from __future__ import annotations

import json
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from taskforge import TaskForge  # noqa: E402
from taskforge.logging_config import setup_logging  # noqa: E402

setup_logging("WARNING")
PORT = int(os.environ.get("DEMO_API_PORT", "8080"))
tf = TaskForge(os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
               namespace=os.environ.get("TASKFORGE_NAMESPACE", "k6bench"))


class Handler(BaseHTTPRequestHandler):
    server_version = "TaskForgeDemoAPI/0.1"

    def _json(self, code: int, obj: dict) -> None:
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802
        if self.path == "/health":
            self._json(200, {"ok": True})
        elif self.path == "/stats":
            self._json(200, tf.stats())
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
            res = tf.enqueue(
                body.get("fn", "benchmark_noop"),
                body.get("payload", {}),
                idempotency_key=body.get("idempotency_key"),
                priority=int(body.get("priority", 5)),
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
