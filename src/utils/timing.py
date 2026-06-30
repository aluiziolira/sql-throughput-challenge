"""
Lightweight wall-clock timing helper for strategies' self-reported metrics.

Strategies report their own `duration_seconds` / `throughput_rows_per_sec`
(consumed directly by callers that use a strategy standalone, outside the
orchestrator's profiler) - this just centralizes that arithmetic.
"""

from __future__ import annotations

import time
from collections.abc import Callable


def run_timed(fn: Callable[[], int]) -> tuple[int, float, float]:
    """
    Run `fn`, timing it, and compute throughput from its returned row count.

    Returns
    -------
    tuple[int, float, float]
        (rows, duration_seconds, throughput_rows_per_sec)
    """
    start = time.perf_counter()
    rows = fn()
    duration_seconds = time.perf_counter() - start
    throughput = rows / duration_seconds if duration_seconds > 0 else 0.0
    return rows, duration_seconds, throughput


__all__ = ["run_timed"]
