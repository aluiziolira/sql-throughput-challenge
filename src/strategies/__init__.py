"""
Strategies package for the SQL Throughput Challenge.

This module re-exports the abstract interfaces and the concrete strategy classes
so downstream code can import from `src.strategies` directly.
"""

from src.strategies.abstract import (
    AbstractBenchmarkStrategy,
    BenchmarkStrategy,
    StrategyResult,
)
from src.strategies.async_stream import AsyncStreamStrategy
from src.strategies.cursor_pagination import CursorPaginationStrategy
from src.strategies.multiprocessing import MultiprocessingStrategy
from src.strategies.naive import NaiveStrategy
from src.strategies.pooled_sync import PooledSyncStrategy

__all__ = [
    # Abstracts
    "AbstractBenchmarkStrategy",
    "BenchmarkStrategy",
    "StrategyResult",
    # Concrete strategies
    "AsyncStreamStrategy",
    "CursorPaginationStrategy",
    "MultiprocessingStrategy",
    "NaiveStrategy",
    "PooledSyncStrategy",
]
