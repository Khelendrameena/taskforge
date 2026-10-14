"""Idempotent producers: retry-after-timeout never enqueues twice."""


def test_duplicate_key_returns_same_job(tf):
    r1 = tf.enqueue("charge", {"order": "1"}, idempotency_key="order-1")
    r2 = tf.enqueue("charge", {"order": "1"}, idempotency_key="order-1")

    assert not r1.duplicate
    assert r2.duplicate
    assert r1.job_id == r2.job_id
    assert tf.stats()["queue_depth"] == 1
    assert tf.stats()["counters"].get("idempotency_hits", 0) == 1


def test_different_keys_are_different_jobs(tf):
    r1 = tf.enqueue("charge", {"order": "1"}, idempotency_key="order-1")
    r2 = tf.enqueue("charge", {"order": "2"}, idempotency_key="order-2")
    assert r1.job_id != r2.job_id
    assert tf.stats()["queue_depth"] == 2


def test_idempotency_survives_completion(tf, registry, runner, fast_config):
    """A late producer retry AFTER the job finished still resolves to the
    original job instead of creating (and executing) a duplicate."""
    calls = []

    @registry.register("charge")
    def charge(payload):
        calls.append(payload["order"])
        return {"ok": True}

    r1 = tf.enqueue("charge", {"order": "9"}, idempotency_key="order-9")
    runner.start(tf, registry, config=fast_config)
    done = tf.wait_for(r1.job_id, timeout_s=10)
    assert done["status"] == "done"

    # Producer never got the first ack; retries minutes later.
    r2 = tf.enqueue("charge", {"order": "9"}, idempotency_key="order-9")
    assert r2.duplicate and r2.job_id == r1.job_id
    assert calls == ["9"], f"handler must run exactly once, ran: {calls}"


def test_keys_are_scoped_per_tenant(tf):
    r1 = tf.enqueue("j", {}, idempotency_key="k", tenant="a")
    r2 = tf.enqueue("j", {}, idempotency_key="k", tenant="b")
    assert r1.job_id != r2.job_id  # same key, different tenants
