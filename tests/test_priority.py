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


def test_invalid_priority_rejected(tf):
    import pytest
    with pytest.raises(ValueError):
        tf.enqueue("job", {}, priority=11)
    with pytest.raises(ValueError):
        tf.enqueue("job", {}, priority=0)
