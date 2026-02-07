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
from collections.abc import Generator
from dataclasses import dataclass, field
from typing import Any

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
    peak_rss_bytes: int | None = field(default=None)
    peak_traced_bytes: int | None = field(default=None)
    cpu_percent: float | None = field(default=None)
    extra: dict[str, Any] = field(default_factory=dict)


def _get_cpu_times_total(proc: psutil.Process) -> float:
    """Get total CPU time (user + system) for a process."""
    try:
        times = proc.cpu_times()
        return times.user + times.system
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return 0.0


def _start_tracemalloc_if_needed(enable_tracemalloc: bool) -> bool:
    """Start tracemalloc if requested and not already running. Returns whether it was already running."""
    if not (enable_tracemalloc and tracemalloc):
        return False
    was_running = tracemalloc.is_tracing()
    if not was_running:
        tracemalloc.start()
    return was_running


def _initialize_peak_rss(process: psutil.Process | None) -> int:
    """Initialize peak RSS with current process memory value."""
    if process:
        return process.memory_info().rss
    return 0


def _capture_cpu_percent(
    process: psutil.Process,
    start_cpu_times: Any,
    total_child_cpu_time: float,
    duration_seconds: float,
) -> float:
    """Capture CPU percent using time-based calculation."""
    end_cpu_times = process.cpu_times()
    parent_cpu_time = float(end_cpu_times.user - start_cpu_times.user) + float(
        end_cpu_times.system - start_cpu_times.system
    )
    total_cpu_time = parent_cpu_time + total_child_cpu_time
    if duration_seconds > 0:
        return (total_cpu_time / duration_seconds) * 100
    return 0.0


def _capture_tracemalloc_peak(
    enable_tracemalloc: bool,
    tracemalloc_was_running: bool,
) -> int | None:
    """Capture tracemalloc peak memory usage."""
    if not (enable_tracemalloc and tracemalloc and tracemalloc.is_tracing()):
        return None
    _, peak_traced = tracemalloc.get_traced_memory()
    # Stop tracemalloc only if we started it
    if not tracemalloc_was_running:
        tracemalloc.stop()
    return peak_traced


@contextlib.contextmanager
def profile_block(
    label: str,
    sample_interval_ms: int = 20,
    enable_tracemalloc: bool = True,
    track_children: bool = False,
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
    track_children : bool
        Whether to include child processes (e.g., from multiprocessing.Pool) in
        memory and CPU metrics. When True, aggregates RSS and CPU across all
        descendant processes. Default False for backward compatibility.

    Notes
    -----
    The background sampling thread ensures we capture true peak memory usage,
    not just start/end snapshots. This is crucial for bursty workloads.
    When track_children=True, metrics include all spawned child processes,
    essential for accurate multiprocessing strategy profiling.
    """
    stats = ProfileStats(label=label)
    process = psutil.Process() if psutil else None
    peak_rss = 0
    total_child_cpu_time = 0.0  # Accumulated CPU time (user + system) for children
    seen_children: dict[int, float] = {}  # pid -> last recorded cpu_time
    stop_sampling = threading.Event()

    def _sample_resources() -> None:
        """Background thread to sample RSS and CPU time at regular intervals."""
        nonlocal peak_rss, total_child_cpu_time
        if not process:
            return
        while not stop_sampling.is_set():
            try:
                current_rss = process.memory_info().rss
                # Aggregate child process memory and CPU if tracking is enabled
                if track_children:
                    for child in process.children(recursive=True):
                        try:
                            current_rss += child.memory_info().rss
                            # Track cumulative CPU time for this child
                            child_cpu = _get_cpu_times_total(child)
                            pid = child.pid
                            if pid in seen_children:
                                # Accumulate delta since last sample
                                delta = child_cpu - seen_children[pid]
                                if delta > 0:
                                    total_child_cpu_time += delta
                            seen_children[pid] = child_cpu
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            # Child may have terminated between enumeration and query
                            pass
                peak_rss = max(peak_rss, current_rss)
            except Exception:
                # Process may have terminated or other issues
                pass
            stop_sampling.wait(timeout=sample_interval_ms / 1000.0)

    # Start tracemalloc if requested
    tracemalloc_was_running = _start_tracemalloc_if_needed(enable_tracemalloc)

    # Initialize peak_rss with current value
    peak_rss = _initialize_peak_rss(process)

    # Record start CPU times for time-based calculation
    start_cpu_times = process.cpu_times() if process else None

    # Start background sampling thread
    sampler = threading.Thread(target=_sample_resources, daemon=True)
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

        # Capture CPU percent using time-based calculation
        if process and start_cpu_times:
            stats.cpu_percent = _capture_cpu_percent(
                process, start_cpu_times, total_child_cpu_time, stats.duration_seconds
            )

        # Capture tracemalloc peak
        stats.peak_traced_bytes = _capture_tracemalloc_peak(
            enable_tracemalloc, tracemalloc_was_running
        )


__all__ = ["ProfileStats", "profile_block"]
