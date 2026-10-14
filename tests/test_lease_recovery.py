"""Lease expiry: orphaned jobs return to the queue via the sweeper."""

import time


def test_expired_lease_requeues_job(tf):
    res = tf.enqueue("job", {"x": 1}, lease_timeout_s=0.3)
    claimed = tf.claim("worker-a", default_lease_timeout_s=30)
    assert claimed is not None
    job_id, _ = claimed
    assert job_id == res.job_id
    # worker-a "dies": never acks, never releases.

    time.sleep(0.5)
    swept = tf.sweep_expired_leases()
    assert swept == 1

    # The job is claimable again, with attempts incremented.
    claimed2 = tf.claim("worker-b", default_lease_timeout_s=30)
    assert claimed2 is not None
    assert claimed2[0] == res.job_id
    assert tf.get_job(res.job_id)["attempts"] == 1
    tf.ack(res.job_id, "worker-b")
    assert tf.get_job(res.job_id)["status"] == "done"


def test_healthy_lease_is_not_swept(tf):
    res = tf.enqueue("job", {}, lease_timeout_s=30)
    tf.claim("worker-a", default_lease_timeout_s=30)
    assert tf.sweep_expired_leases() == 0
    assert tf.stats()["queue_depth"] == 0  # still leased, not requeued


def test_ack_after_lease_expiry_is_rejected(tf):
    """The ownership check: a slow worker's late ack must not complete a job
    that the sweeper already handed to someone else."""
    res = tf.enqueue("job", {}, lease_timeout_s=0.2)
    tf.claim("slow-worker", default_lease_timeout_s=30)
    time.sleep(0.35)
    tf.sweep_expired_leases()  # job requeued, lease gone

    # slow-worker finally finishes and tries to ack its stale copy
    assert tf.ack(res.job_id, "slow-worker") is False

    # the job is still pending for whoever claims it next
    claimed = tf.claim("worker-b", default_lease_timeout_s=30)
    assert claimed[0] == res.job_id
    assert tf.ack(res.job_id, "worker-b") is True
    assert tf.stats()["counters"].get("acked", 0) == 1
