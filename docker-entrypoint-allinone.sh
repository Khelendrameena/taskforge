#!/bin/sh
# Starts ephemeral Redis, then the worker (background) and the demo API (foreground).
set -e

redis-server --daemonize yes --save '' --appendonly no --port 6379
sleep 1
redis-cli ping >/dev/null

# Hosts like Render inject $PORT; default to 8080 locally.
export DEMO_API_PORT="${PORT:-8080}"

python -m taskforge.cli worker --handlers-module demo.jobs --concurrency 2 &
exec python benchmarks/demo_api.py
