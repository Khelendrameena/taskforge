"""Retries with backoff, then quarantine in the DLQ, then operator replay."""


def test_retry_then_dlq(tf, registry, runner, fast_config):
    attempts = []

    @registry.register("boom")
    def boom(payload):
        attempts.append(1)
        raise RuntimeError("always fails")

    res = tf.enqueue("boom", {}, max_retries=2)
    runner.start(tf, registry, config=fast_config)

    dead = tf.wait_for(res.job_id, timeout_s=15)
    assert dead["status"] == "dead"
    assert dead["attempts"] == 3  # 1 initial + 2 retries
    assert "always fails" in dead["last_error"]

    dlq = tf.dlq_list()
    assert len(dlq) == 1
    assert dlq[0]["id"] == res.job_id
    assert dlq[0]["fn"] == "boom"
    assert tf.stats()["counters"].get("dlq", 0) == 1


def test_flaky_job_recovers(tf, registry, runner, fast_config):
    state = {"n": 0}

    @registry.register("flaky")
    def flaky(payload):
        state["n"] += 1
        if state["n"] < 3:
            raise RuntimeError("not yet")
        return {"ok": True}

    res = tf.enqueue("flaky", {}, max_retries=5)
    runner.start(tf, registry, config=fast_config)
    done = tf.wait_for(res.job_id, timeout_s=15)
    assert done["status"] == "done"
    assert done["result"] == {"ok": True}
    assert done["attempts"] == 2  # two failures, third attempt succeeded
    assert tf.stats()["counters"].get("retried", 0) == 2


def test_dlq_replay_resets_and_requeues(tf, registry, runner, fast_config):
    @registry.register("boom")
    def boom(payload):
        raise RuntimeError("still broken")

    res = tf.enqueue("boom", {}, max_retries=0)  # straight to DLQ
    runner.start(tf, registry, config=fast_config)
    dead = tf.wait_for(res.job_id, timeout_s=10)
    assert dead["status"] == "dead"
    assert tf.stats()["dlq_depth"] == 1

    # Stop the worker first: otherwise it would instantly re-claim the
    # replayed job and dead-letter it again, racing our assertions.
    runner.stop_all()
    n = tf.dlq_replay(job_id=res.job_id)
    assert n == 1
    assert tf.stats()["dlq_depth"] == 0
    replayed = tf.get_job(res.job_id)
    assert replayed["status"] == "queued"
    assert replayed["attempts"] == 0
    assert tf.stats()["queue_depth"] == 1


def test_unknown_handler_goes_straight_to_dlq(tf, registry, runner, fast_config):
    res = tf.enqueue("no_such_fn", {}, max_retries=5)
    runner.start(tf, registry, config=fast_config)
    dead = tf.wait_for(res.job_id, timeout_s=10)
    assert dead["status"] == "dead"
    assert "HandlerNotFound" in dead["last_error"]
