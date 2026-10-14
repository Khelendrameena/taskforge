#!/bin/bash
# Run the TaskForge benchmarks. Requires Redis on localhost:6379 (or $REDIS_URL).
#   ./benchmarks/run.sh            # python load test (always available)
#   ./benchmarks/run.sh --k6       # k6 HTTP producer benchmark (needs k6)
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ "${1:-}" == "--k6" ]]; then
  command -v k6 >/dev/null || { echo "k6 not found (https://k6.io/docs/get-started/installation/)"; exit 1; }
  TASKFORGE_NAMESPACE=k6bench python benchmarks/demo_api.py &
  API_PID=$!
  trap 'kill $API_PID' EXIT
  sleep 1
  curl -sf localhost:8080/health >/dev/null
  k6 run benchmarks/k6_producer.js
else
  PYTHONPATH=. .venv/bin/python benchmarks/load_test.py "$@"
fi
