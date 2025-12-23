"""
Profiling utilities for the SQL Throughput Challenge.

This module provides lightweight context managers and decorators to measure
wall-clock time, CPU usage, and memory usage. It is intentionally minimal and
can be extended alongside the orchestrator to record richer telemetry.

Usage examples (planned):
    from src.utils.profiler import profile_block

    with profile_block("naive-select") as stats:
        run_strategy()

    print(stats.duration_seconds, stats.peak_rss_bytes)
"""
from __future__ import annotations

import contextlib
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Generator, Optional

try:
    import psutil
except ImportError:  # pragma: no cover - optional until dependencies are installed
    psutil = None  # type: ignore[assignment]

try:
    import tracemalloc
except ImportError:  # pragma: no cover - Python without tracemalloc (unlikely)
    tracemalloc = None  # type: ignore[assignment]


@dataclass
class ProfileStats:
    """
    Container for profiling measurements.
    """

    label: str
    start_ts: float = field(default=0.0)
    end_ts: float = field(default=0.0)
    duration_seconds: float = field(default=0.0)
    peak_rss_bytes: Optional[int] = field(default=None)
    cpu_percent: Optional[float] = field(default=None)
    extra: dict[str, Any] = field(default_factory=dict)


@contextlib.contextmanager
def profile_block(label: str) -> Generator[ProfileStats, None, None]:
    """
    Context manager to profile a block of code.

    Measures:
    - Wall-clock duration (perf_counter)
    - Peak RSS (psutil, if available)
    - CPU percent (psutil, best-effort snapshot)

    Parameters
    ----------
    label : str
        Human-friendly label for the profiled block.
    """
    stats = ProfileStats(label=label)
    process = psutil.Process() if psutil else None

    # CPU percent needs a priming call; use interval=None for non-blocking snapshot.
    if process:
        process.cpu_percent(interval=None)

    # Memory snapshot before execution.
    if process:
        with process.oneshot():
            stats.peak_rss_bytes = process.memory_info().rss

    stats.start_ts = time.perf_counter()
    try:
        yield stats
    finally:
        stats.end_ts = time.perf_counter()
        stats.duration_seconds = stats.end_ts - stats.start_ts

        if process:
            # Capture CPU percent since last call.
            stats.cpu_percent = process.cpu_percent(interval=None)
            with process.oneshot():
                current_rss = process.memory_info().rss
                if stats.peak_rss_bytes is None:
                    stats.peak_rss_bytes = current_rss
                else:
                    stats.peak_rss_bytes = max(stats.peak_rss_bytes, current_rss)


def profile_function(label: Optional[str] = None) -> Callable[[Callable[..., Any]], Callable[..., ProfileStats]]:
    """
    Decorator to profile a function call and return ProfileStats.

    Example:
        @profile_function("naive-strategy")
        def run():
            ...

        stats = run()
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., ProfileStats]:
        def wrapper(*args: Any, **kwargs: Any) -> ProfileStats:
            tag = label or func.__name__
            with profile_block(tag) as stats:
                func(*args, **kwargs)
            return stats

        return wrapper

    return decorator


__all__ = ["ProfileStats", "profile_block", "profile_function"]
