"""
SQL Throughput Challenge - Benchmarking suite for PostgreSQL read strategies.

This package provides a comprehensive benchmarking framework to compare different
Python strategies for reading large datasets from PostgreSQL, including:

- Naive single-query approaches
- Cursor-based pagination
- Connection pooling
- Multiprocessing parallelization
- Asynchronous streaming

The package is designed to demonstrate backend engineering best practices including
design patterns, resource management, structured logging, and comprehensive testing.
"""

from __future__ import annotations

__version__ = "0.1.0"
__author__ = "Your Name"
__license__ = "MIT"

# Public API exports
from src.config import Settings, get_settings
from src.orchestrator import available_strategies, run_strategies
from src.strategies.abstract import (
    AbstractBenchmarkStrategy,
    BenchmarkStrategy,
    StrategyResult,
)
from src.utils.logging import configure_logging, get_logger
from src.utils.profiler import ProfileStats, profile_block, profile_function

__all__ = [
    # Version info
    "__version__",
    "__author__",
    "__license__",
    # Configuration
    "Settings",
    "get_settings",
    # Orchestration
    "available_strategies",
    "run_strategies",
    # Strategy abstractions
    "BenchmarkStrategy",
    "AbstractBenchmarkStrategy",
    "StrategyResult",
    # Logging
    "configure_logging",
    "get_logger",
    # Profiling
    "ProfileStats",
    "profile_block",
    "profile_function",
]
