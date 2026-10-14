"""Priority ordering: higher priority claims first, FIFO within a level."""


def test_priority_ordering(tf):
    tf.enqueue("job", {"n": "low"}, priority=1)
    tf.enqueue("job", {"n": "high"}, priority=10)
    tf.enqueue("job", {"n": "mid"}, priority=5)
    tf.enqueue("job", {"n": "high2"}, priority=10)

    order = []
    for _ in range(4):
        claimed = tf.claim("w1", default_lease_timeout_s=30)
        assert claimed is not None
        _, job = claimed
        order.append(job["payload"]["n"])
        tf.ack(job["id"], "w1")

    assert order == ["high", "high2", "mid", "low"]


def test_same_millisecond_fifo_is_exact(tf):
    """Regression: 50 same-priority enqueues in a tight loop (many share one
    millisecond) must still come out in exact FIFO order, not lexicographic
    job-id order. This caught a float-precision bug in the score tiebreak."""
    n = 50
    ids = [tf.enqueue("job", {"i": i}, priority=5).job_id for i in range(n)]
    out = []
    for _ in range(n):
        claimed = tf.claim("w1", default_lease_timeout_s=30)
        assert claimed is not None
        _, job = claimed
        out.append(job["id"])
        tf.ack(job["id"], "w1")
    assert out == ids


def test_invalid_priority_rejected(tf):
    import pytest
    with pytest.raises(ValueError):
        tf.enqueue("job", {}, priority=11)
    with pytest.raises(ValueError):
        tf.enqueue("job", {}, priority=0)
