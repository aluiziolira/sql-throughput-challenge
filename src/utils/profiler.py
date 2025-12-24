"""
Profiling utilities for the SQL Throughput Challenge.

This module provides context managers and decorators to measure:
- Wall-clock time (perf_counter)
- CPU usage (psutil)
- Memory usage (RSS via psutil + tracemalloc for Python allocations)
- Peak memory via background sampling thread

Usage examples:
    from src.utils.profiler import profile_block

    with profile_block("naive-select") as stats:
        run_strategy()

    print(stats.duration_seconds, stats.peak_rss_bytes, stats.peak_traced_bytes)
"""

from __future__ import annotations

import contextlib
import threading
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
    peak_traced_bytes: Optional[int] = field(default=None)
    cpu_percent: Optional[float] = field(default=None)
    extra: dict[str, Any] = field(default_factory=dict)


@contextlib.contextmanager
def profile_block(
    label: str, sample_interval_ms: int = 50, enable_tracemalloc: bool = True
) -> Generator[ProfileStats, None, None]:
    """
    Context manager to profile a block of code with enhanced memory tracking.

    Measures:
    - Wall-clock duration (perf_counter)
    - Peak RSS via background sampling thread (psutil)
    - Peak Python memory allocations (tracemalloc)
    - CPU percent (psutil, best-effort snapshot)

    Parameters
    ----------
    label : str
        Human-friendly label for the profiled block.
    sample_interval_ms : int
        Interval in milliseconds for RSS sampling. Lower = more accurate but higher overhead.
    enable_tracemalloc : bool
        Whether to enable tracemalloc for tracking Python-level allocations.

    Notes
    -----
    The background sampling thread ensures we capture true peak memory usage,
    not just start/end snapshots. This is crucial for bursty workloads.
    """
    stats = ProfileStats(label=label)
    process = psutil.Process() if psutil else None
    peak_rss = 0
    stop_sampling = threading.Event()

    def _sample_memory():
        """Background thread to sample RSS at regular intervals."""
        nonlocal peak_rss
        if not process:
            return
        while not stop_sampling.is_set():
            try:
                current_rss = process.memory_info().rss
                peak_rss = max(peak_rss, current_rss)
            except Exception:
                # Process may have terminated or other issues
                pass
            stop_sampling.wait(timeout=sample_interval_ms / 1000.0)

    # Start tracemalloc if requested
    tracemalloc_was_running = False
    if enable_tracemalloc and tracemalloc:
        tracemalloc_was_running = tracemalloc.is_tracing()
        if not tracemalloc_was_running:
            tracemalloc.start()

    # CPU percent needs a priming call
    if process:
        process.cpu_percent(interval=None)

    # Initialize peak_rss with current value
    if process:
        peak_rss = process.memory_info().rss

    # Start background sampling thread
    sampler = threading.Thread(target=_sample_memory, daemon=True)
    sampler.start()

    stats.start_ts = time.perf_counter()
    try:
        yield stats
    finally:
        stats.end_ts = time.perf_counter()
        stats.duration_seconds = stats.end_ts - stats.start_ts

        # Stop sampling thread
        stop_sampling.set()
        sampler.join(timeout=1.0)

        # Record peak RSS from sampling
        stats.peak_rss_bytes = peak_rss if peak_rss > 0 else None

        # Capture CPU percent
        if process:
            stats.cpu_percent = process.cpu_percent(interval=None)

        # Capture tracemalloc peak
        if enable_tracemalloc and tracemalloc and tracemalloc.is_tracing():
            _, peak_traced = tracemalloc.get_traced_memory()
            stats.peak_traced_bytes = peak_traced
            # Stop tracemalloc only if we started it
            if not tracemalloc_was_running:
                tracemalloc.stop()


def profile_function(
    label: Optional[str] = None,
    sample_interval_ms: int = 50,
    enable_tracemalloc: bool = True,
) -> Callable[[Callable[..., Any]], Callable[..., ProfileStats]]:
    """
    Decorator to profile a function call and return ProfileStats.

    Parameters
    ----------
    label : str, optional
        Custom label for the profiled function. Defaults to function name.
    sample_interval_ms : int
        Interval for memory sampling (passed to profile_block).
    enable_tracemalloc : bool
        Whether to enable tracemalloc (passed to profile_block).

    Example
    -------
        @profile_function("naive-strategy")
        def run():
            ...

        stats = run()
        print(f"Peak RSS: {stats.peak_rss_bytes} bytes")
        print(f"Peak traced: {stats.peak_traced_bytes} bytes")
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., ProfileStats]:
        def wrapper(*args: Any, **kwargs: Any) -> ProfileStats:
            tag = label or func.__name__
            with profile_block(
                tag, sample_interval_ms=sample_interval_ms, enable_tracemalloc=enable_tracemalloc
            ) as stats:
                func(*args, **kwargs)
            return stats

        return wrapper

    return decorator


__all__ = ["ProfileStats", "profile_block", "profile_function"]
