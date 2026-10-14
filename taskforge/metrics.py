"""Latency percentile helpers (benchmark + `taskforge latency`)."""

from __future__ import annotations


def percentiles(samples: list[float], ps: tuple[float, ...] = (50, 90, 99)) -> dict[str, float]:
    if not samples:
        return {f"p{int(p)}": 0.0 for p in ps}
    ordered = sorted(samples)
    out: dict[str, float] = {}
    for p in ps:
        # Nearest-rank method: the value at ceil(p/100 * n), 1-indexed.
        rank = max(1, -(-p * len(ordered) // 100))  # ceil without floats
        out[f"p{int(p)}"] = ordered[int(rank) - 1]
    return out


def summarize(samples: list[float]) -> dict:
    return {
        "count": len(samples),
        "min_ms": min(samples) if samples else 0.0,
        "mean_ms": sum(samples) / len(samples) if samples else 0.0,
        "max_ms": max(samples) if samples else 0.0,
        **percentiles(samples),
    }
