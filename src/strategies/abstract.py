"""
Abstract strategy interfaces and result contracts for the SQL Throughput Challenge.

Concrete strategies (e.g., naive, cursor pagination, async stream, multiprocessing)
should implement the BenchmarkStrategy Protocol and return a StrategyResult TypedDict to
standardize downstream orchestration and reporting.
"""

from __future__ import annotations

from typing import Any, Protocol, TypedDict, runtime_checkable


class StrategyResult(TypedDict, total=False):
    """
    Metrics contract returned by strategies.

    ## Responsibility Matrix

    ### Strategy MUST Provide
    - `rows: int` - Actual number of rows processed

    ### Strategy MAY Provide (Optional)
    - `notes: str` - Strategy-specific details (batch size, pool config, etc.)
    - `error: str` - Error message if execution failed
    - `extra: Dict[str, Any]` - Custom metrics for advanced use cases

    ### Orchestrator WILL Override
    The orchestrator profiler wraps each `strategy.execute()` call and
    provides/overrides these fields:
    - `duration_seconds: float` - Wall-clock time (includes overhead)
    - `throughput_rows_per_sec: float` - Calculated from rows/duration
    - `peak_rss_bytes: int` - Peak memory via background sampling
    - `cpu_percent: float` - CPU utilization via psutil

    ## Why This Design?

    Multiprocessing strategies cannot measure per-worker memory because
    workers run in separate OS processes. The orchestrator profiler
    measures the main process, which includes coordination overhead but
    not individual worker resources.

    See docs/architecture-metrics.md for full details.
    """

    rows: int
    duration_seconds: float
    throughput_rows_per_sec: float
    peak_rss_bytes: int | None
    cpu_percent: float | None
    error: str | None
    notes: str | None
    extra: dict[str, Any]


@runtime_checkable
class BenchmarkStrategy(Protocol):
    """
    Common interface all benchmark strategies must implement.

    Attributes
    ----------
    name : str
        A short machine-friendly identifier.
    description : str
        A human-friendly summary of the approach.

    Lifecycle
    ---------
    Strategies MAY expose a callable ``close()`` method for resource cleanup
    (for example connection pools). The orchestrator performs best-effort
    lifecycle hygiene by invoking ``close()`` when present after each run.
    Implementations should keep ``close()`` idempotent.
    """

    name: str
    description: str

    def execute(self, limit: int) -> StrategyResult:
        """
        Execute the strategy against the database and return metrics.

        Parameters
        ----------
        limit : int
            Maximum number of rows to process (cap for benchmarking).

        Returns
        -------
        StrategyResult
            Basic metrics including rows processed, duration, and throughput.
        """
        ...


__all__ = [
    "BenchmarkStrategy",
    "StrategyResult",
]
