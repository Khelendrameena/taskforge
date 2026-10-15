"""TaskForge command line interface.

    taskforge enqueue --fn charge_card --payload '{"amount": 499}' --idempotency-key order-123
    taskforge worker --concurrency 8 --handlers-module demo.jobs
    taskforge stats
    taskforge inspect <job_id>
    taskforge dlq list
    taskforge dlq replay --all
    taskforge latency
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import sys

from .client import TaskForge
from .config import Config
from .handlers import Registry
from .logging_config import setup_logging
from .metrics import summarize
from .worker import Worker


def _load_registry(module_path: str | None) -> Registry:
    registry = Registry()
    if module_path:
        mod = importlib.import_module(module_path)
        found = getattr(mod, "registry", None)
        if isinstance(found, Registry):
            return found
        # Fall back: collect any Registry instance defined in the module.
        for value in vars(mod).values():
            if isinstance(value, Registry):
                return value
        raise SystemExit(f"{module_path} does not define a taskforge Registry")
    return registry


def _make_client(args) -> TaskForge:
    return TaskForge(redis_url=args.redis_url, namespace=args.namespace)


def cmd_enqueue(args) -> int:
    client = _make_client(args)
    try:
        payload = json.loads(args.payload) if args.payload else {}
    except json.JSONDecodeError as exc:
        raise SystemExit(f"invalid --payload JSON: {exc}")
    res = None
    try:
        res = client.enqueue(
            args.fn, payload,
            idempotency_key=args.idempotency_key,
            priority=args.priority,
            tenant=args.tenant,
            max_retries=args.max_retries,
            lease_timeout_s=args.lease_timeout_s,
            delay_s=args.delay_s,
        )
    except ValueError as exc:
        raise SystemExit(f"enqueue failed: {exc}")
    print(json.dumps({"job_id": res.job_id, "duplicate": res.duplicate}))
    return 0


def cmd_worker(args) -> int:
    setup_logging(args.log_level)
    config = Config()
    registry = _load_registry(args.handlers_module)
    client = TaskForge(redis_url=args.redis_url, namespace=args.namespace, config=config)
    for spec in args.rate_limit or []:
        # format: tenant:capacity:refill_per_sec  e.g.  acme:100:10
        try:
            tenant, capacity, refill = spec.split(":")
            client.set_rate_limit(tenant, float(capacity), float(refill))
        except ValueError:
            raise SystemExit(
                f"invalid --rate-limit {spec!r}: expected tenant:capacity:refill_per_sec")
    worker = Worker(client=client, handlers=registry, config=config,
                    concurrency=args.concurrency)
    worker.run()
    return 0


def cmd_stats(args) -> int:
    print(json.dumps(_make_client(args).stats(), indent=2))
    return 0


def cmd_inspect(args) -> int:
    job = _make_client(args).get_job(args.job_id)
    if not job:
        raise SystemExit(f"unknown job {args.job_id}")
    print(json.dumps(job, indent=2))
    return 0


def cmd_dlq_list(args) -> int:
    entries = _make_client(args).dlq_list(limit=args.limit)
    for e in entries:
        print(f"{e.get('id')}  fn={e.get('fn')} tenant={e.get('tenant')} "
              f"attempts={e.get('attempts')} error={str(e.get('last_error'))[:80]}")
    if not entries:
        print("(dlq empty)")
    return 0


def cmd_dlq_replay(args) -> int:
    n = _make_client(args).dlq_replay(job_id=args.job_id, replay_all=args.all)
    print(f"replayed {n} job(s)")
    return 0


def cmd_latency(args) -> int:
    client = _make_client(args)
    print(json.dumps(summarize(client.latency_samples()), indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="taskforge", description="TaskForge distributed task queue")
    p.add_argument("--redis-url", default=os.environ.get("REDIS_URL"),
                   help="default: $REDIS_URL or redis://localhost:6379/0")
    p.add_argument("--namespace", default=os.environ.get("TASKFORGE_NAMESPACE", "tf"))
    sub = p.add_subparsers(dest="cmd", required=True)

    e = sub.add_parser("enqueue", help="enqueue a job")
    e.add_argument("--fn", required=True)
    e.add_argument("--payload", default="{}")
    e.add_argument("--idempotency-key", default=None)
    e.add_argument("--priority", type=int, default=5)
    e.add_argument("--tenant", default="default")
    e.add_argument("--max-retries", type=int, default=3)
    e.add_argument("--lease-timeout-s", type=float, default=None)
    e.add_argument("--delay-s", type=float, default=0)
    e.set_defaults(func=cmd_enqueue)

    w = sub.add_parser("worker", help="run a worker")
    w.add_argument("--concurrency", type=int, default=None)
    w.add_argument("--handlers-module", default=None,
                   help="python module defining a taskforge Registry (e.g. demo.jobs)")
    w.add_argument("--rate-limit", action="append", default=[],
                   help="tenant:capacity:refill_per_sec (repeatable)")
    w.add_argument("--log-level", default="INFO")
    w.set_defaults(func=cmd_worker)

    s = sub.add_parser("stats", help="queue counters and depths")
    s.set_defaults(func=cmd_stats)

    i = sub.add_parser("inspect", help="show a job")
    i.add_argument("job_id")
    i.set_defaults(func=cmd_inspect)

    d = sub.add_parser("dlq", help="dead-letter queue")
    dsub = d.add_subparsers(dest="dlq_cmd", required=True)
    dl = dsub.add_parser("list", help="list dead jobs")
    dl.add_argument("--limit", type=int, default=50)
    dl.set_defaults(func=cmd_dlq_list)
    dr = dsub.add_parser("replay", help="requeue dead jobs")
    dr.add_argument("job_id", nargs="?")
    dr.add_argument("--all", action="store_true")
    dr.set_defaults(func=cmd_dlq_replay)

    l = sub.add_parser("latency", help="enqueue->ack latency summary")
    l.set_defaults(func=cmd_latency)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
