# Delivery semantics: why TaskForge chose at-least-once

## The three options

**At-most-once.** The broker hands the job to a worker and forgets it. If the
worker crashes mid-job, the job is lost silently. Cheap, fast, and wrong for
anything that matters (charges, emails, provisioning).

**At-least-once.** The broker keeps the job until a worker explicitly
acknowledges it. If the worker dies, the lease expires and the job is
redelivered -- possibly to a different worker. Nothing is ever lost, but a
job *can* execute twice: the worker may crash after performing the effect
(charging the card) but before the ack reaches the broker.

**Exactly-once.** The holy grail: each job's effect happens precisely one
time, even across crashes, retries, and network partitions. In a distributed
system this is impossible in the general case (it reduces to consensus on
whether the effect happened). Systems that claim it (Kafka transactions,
Flink checkpoints) actually implement *effectively-once*: at-least-once
delivery plus idempotent effects, scoped carefully.

## What TaskForge guarantees

TaskForge is **at-least-once**, by explicit design:

1. **Claim is atomic** (Lua `ZPOPMIN` + lease registration in one step), so a
   job is never handed to two workers *at claim time*.
2. **Leases expire.** Every claim carries a visibility timeout. The sweeper
   requeues jobs whose leases lapsed -- the crash-recovery path. This is what
   makes worker death survivable instead of silently losing jobs.
3. **Ack is ownership-checked.** If a lease expired while a slow worker was
   still running, the late ack is rejected (`ack_lease_lost`): the job was
   already requeued and may be owned by someone else. Acking it would corrupt
   accounting (and could double-apply non-idempotent side effects the new
   owner already performed).
4. **Producers are idempotent.** The same `idempotency_key` never creates two
   jobs, even if the producer retries after a timeout. So the *enqueue* side
   is duplicate-free; duplicates can only arise from *redelivery* after a
   worker failure.

## Getting effectively-once where it counts

At-least-once delivery + idempotent handlers = effectively-once effects.
TaskForge pushes idempotency to the edges, where it belongs:

- **Producer edge:** `enqueue(..., idempotency_key="order-123")` suppresses
  duplicate submissions (see `demo/double_charge.py`).
- **Handler edge:** the demo `charge_card` handler records charges with
  `SET NX` on the order id in the payment ledger. Even if the queue
  redelivers, the gateway charges once.

The queue cannot make your handler idempotent for you -- and any queue that
pretends otherwise is lying. What the queue *can* do is make redelivery
visible and bounded: `attempts` counters, dead-letter quarantine, and logs
that say exactly why a job ran twice.

## When at-most-once would be fine

Metrics ingestion, log shipping, cache warming: workloads where dropping the
occasional job is cheaper than the lease/sweep machinery. TaskForge doesn't
implement that mode -- if you need it, that's a one-line conversation, not a
mode flag: just don't retry.

## Interview-ready summary

> "TaskForge is at-least-once: atomic claim with a visibility-timeout lease,
> a sweeper that requeues orphaned jobs, and ownership-checked acks so a
> stale worker can't double-complete. Exactly-once is impossible in general,
> so for money-path jobs I pair it with idempotency on both edges --
> idempotency keys on the producer, SET NX in the handler -- which gives
> effectively-once where it matters."
