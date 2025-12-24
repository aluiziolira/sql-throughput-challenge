"""
Abstract strategy interfaces and result contracts for the SQL Throughput Challenge.

Concrete strategies (e.g., naive, cursor pagination, async stream, multiprocessing)
should implement the BenchmarkStrategy ABC and return a StrategyResult TypedDict to
standardize downstream orchestration and reporting.
"""

from __future__ import annotations

import abc
from typing import Any, Dict, Optional, Protocol, TypedDict, runtime_checkable


class StrategyResult(TypedDict, total=False):
    """
    Minimal metrics contract returned by strategies.

    Fields are optional to keep implementations lightweight; orchestrator/reporters
    should tolerate missing values and enrich when possible.
    """

    rows: int
    duration_seconds: float
    throughput_rows_per_sec: float
    peak_rss_bytes: Optional[int]
    cpu_percent: Optional[float]
    error: Optional[str]
    notes: Optional[str]
    extra: Dict[str, Any]


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


class AbstractBenchmarkStrategy(abc.ABC):
    """
    Optional ABC helper for class-based implementations.

    Subclasses should set `name` and `description` and implement `execute`.
    """

    name: str
    description: str

    @abc.abstractmethod
    def execute(self, limit: int) -> StrategyResult:  # pragma: no cover - interface only
        """Run the strategy and return metrics."""
        raise NotImplementedError


__all__ = [
    "StrategyResult",
    "BenchmarkStrategy",
    "AbstractBenchmarkStrategy",
]
