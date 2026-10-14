"""Per-tenant token-bucket rate limiting with deferral (never drops)."""

import time


def test_token_bucket_allow_deny_refill(tf):
    tf.set_rate_limit("acme", capacity=2, refill_per_second=10)
    assert tf.allow("acme") is True
    assert tf.allow("acme") is True
    assert tf.allow("acme") is False  # bucket empty
    time.sleep(0.25)  # ~2.5 tokens refill
    assert tf.allow("acme") is True


def test_unlimited_tenant_always_allowed(tf):
    for _ in range(50):
        assert tf.allow("no-limit-tenant") is True


def test_rate_limited_job_is_deferred_not_dropped(tf, registry, runner, fast_config):
    """1 token per 10s: first job runs now, second waits in `delayed`."""
    done = []

    @registry.register("limited")
    def limited(payload):
        done.append(payload["n"])
        return {"ok": True}

    tf.set_rate_limit("acme", capacity=1, refill_per_second=0.1)
    r1 = tf.enqueue("limited", {"n": 1}, tenant="acme")
    r2 = tf.enqueue("limited", {"n": 2}, tenant="acme")
    runner.start(tf, registry, config=fast_config)

    assert tf.wait_for(r1.job_id, timeout_s=10)["status"] == "done"
    time.sleep(2.0)  # bucket still empty (needs 10s); job 2 must be deferred
    assert tf.get_job(r2.job_id)["status"] != "done"
    assert r2.job_id not in done
    # Nothing lost: it sits in the delayed set waiting for tokens.
    assert tf.stats()["delayed"] >= 1


def test_rate_limit_is_per_tenant(tf, registry, runner, fast_config):
    done = []

    @registry.register("limited")
    def limited(payload):
        done.append(payload["tenant"])
        return {"ok": True}

    tf.set_rate_limit("slow-tenant", capacity=1, refill_per_second=0.1)
    tf.enqueue("limited", {"tenant": "slow-tenant"}, tenant="slow-tenant")
    tf.enqueue("limited", {"tenant": "fast-tenant"}, tenant="fast-tenant")
    runner.start(tf, registry, config=fast_config)
    time.sleep(3)
    # fast-tenant unaffected by slow-tenant's empty bucket
    assert "fast-tenant" in done
