"""Dashboard support: depth_by_priority, worker heartbeat listing."""

import time


def test_depth_by_priority(tf):
    tf.enqueue("fn", {}, priority=10)
    tf.enqueue("fn", {}, priority=10)
    tf.enqueue("fn", {}, priority=5)
    tf.enqueue("fn", {}, priority=1)
    depths = tf.depth_by_priority()
    assert depths[10] == 2
    assert depths[5] == 1
    assert depths[1] == 1
    assert sum(depths.values()) == 4 == tf.stats()["queue_depth"]


def test_worker_heartbeat_and_staleness(tf):
    tf.heartbeat("w-alive", claimed=3)
    workers = tf.workers()
    assert len(workers) == 1
    w = workers[0]
    assert w["worker_id"] == "w-alive" and w["claimed"] == 3
    assert w["alive"] is True

    # A worker that never heartbeats again goes stale, not missing.
    tf.r.hset(tf.keys.workers, "w-dead",
              '{"worker_id": "w-dead", "claimed": 0, "last_seen_ms": 1}')
    workers = {w["worker_id"]: w for w in tf.workers()}
    assert workers["w-dead"]["alive"] is False
    assert workers["w-dead"]["age_s"] > 15


def test_heartbeat_survives_purge_scope(tf):
    # heartbeat keys live under the namespace, so purge() clears them
    tf.heartbeat("w1")
    tf.purge()
    assert tf.workers() == []
