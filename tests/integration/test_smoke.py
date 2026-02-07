"""
Integration tests for SQL Throughput Challenge strategies.

These tests run against a real PostgreSQL instance and verify that:
1. Each strategy can execute without errors
2. Each strategy returns the expected row count
3. Basic performance metrics are captured

Run with: RUN_INTEGRATION_TESTS=1 pytest tests/integration/
"""

from __future__ import annotations

import os

import pytest

from src.orchestrator import RunConfig, available_strategies, run_strategies
from src.strategies.async_stream import AsyncStreamStrategy
from src.strategies.cursor_pagination import CursorPaginationStrategy
from src.strategies.multiprocessing import MultiprocessingStrategy
from src.strategies.naive import NaiveStrategy
from src.strategies.pooled_sync import PooledSyncStrategy

# Test configuration constants
DEFAULT_PROCESS_COUNT = 2
DEFAULT_CHUNK_SIZE = 25
DEFAULT_BATCH_SIZE = 10
DEFAULT_POOL_MIN = 1
DEFAULT_POOL_MAX = 5
DEFAULT_LIMIT = 25
DEFAULT_SLEEP_SECONDS = 0.05
DEFAULT_ROWS = 5
DEFAULT_TEST_BATCH_SIZE = 2
DEFAULT_SEED = 123

# Test expectation constants
EXPECTED_STRATEGIES_COUNT = 5
MULTI_STRATEGY_COUNT = 2
MULTI_RUN_COUNT = 3
MIN_THROUGHPUT_ROWS_PER_SEC = 100

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_INTEGRATION_TESTS", "0") != "1",
    reason="Integration tests require RUN_INTEGRATION_TESTS=1 and reachable Postgres",
)


class TestStrategyRegistry:
    """Test orchestrator strategy registry."""

    def test_available_strategies_includes_all_expected(self):
        """Verify all implemented strategies are registered."""
        strategies = available_strategies()
        expected = ["naive", "cursor_pagination", "pooled_sync", "multiprocessing", "async_stream"]
        for name in expected:
            assert name in strategies, f"Strategy '{name}' not found in registry"

    def test_available_strategies_returns_sorted_list(self):
        """Verify registry returns a sorted list."""
        strategies = available_strategies()
        assert strategies == sorted(strategies)


class TestNaiveStrategy:
    """Test the baseline naive strategy."""

    def test_naive_strategy_returns_correct_row_count(self, seeded_db_small: int, test_dsn: str):
        """Verify naive strategy fetches all rows."""
        strategy = NaiveStrategy(dsn_override=test_dsn)
        result = strategy.execute(limit=seeded_db_small)

        assert result["rows"] == seeded_db_small
        assert result["duration_seconds"] > 0
        assert result["throughput_rows_per_sec"] > 0

    def test_naive_strategy_respects_limit(self, seeded_db_small: int, test_dsn: str):
        """Verify naive strategy respects row limit."""
        limit = seeded_db_small // 2
        strategy = NaiveStrategy(dsn_override=test_dsn)
        result = strategy.execute(limit=limit)

        assert result["rows"] == limit

    def test_naive_strategy_handles_empty_table(self, clean_records_table, test_dsn: str):
        """Verify naive strategy handles empty table gracefully."""
        strategy = NaiveStrategy(dsn_override=test_dsn)
        result = strategy.execute(limit=100)

        assert result["rows"] == 0
        assert result.get("error") is None


class TestCursorPaginationStrategy:
    """Test server-side cursor pagination strategy."""

    def test_cursor_pagination_returns_correct_row_count(self, seeded_db_small: int, test_dsn: str):
        """Verify cursor pagination fetches all rows."""
        strategy = CursorPaginationStrategy(batch_size=DEFAULT_BATCH_SIZE, dsn_override=test_dsn)
        result = strategy.execute(limit=seeded_db_small)

        assert result["rows"] == seeded_db_small
        assert result["duration_seconds"] > 0

    def test_cursor_pagination_with_various_batch_sizes(self, seeded_db_small: int, test_dsn: str):
        """Verify cursor pagination works with different batch sizes."""
        batch_sizes = [1, 10, 50, 200]
        for batch_size in batch_sizes:
            strategy = CursorPaginationStrategy(batch_size=batch_size, dsn_override=test_dsn)
            result = strategy.execute(limit=seeded_db_small)
            assert result["rows"] == seeded_db_small, f"Failed with batch_size={batch_size}"

    def test_cursor_pagination_respects_limit(self, seeded_db_small: int, test_dsn: str):
        """Verify cursor pagination respects limit."""
        limit = DEFAULT_LIMIT
        strategy = CursorPaginationStrategy(batch_size=DEFAULT_BATCH_SIZE, dsn_override=test_dsn)
        result = strategy.execute(limit=limit)

        assert result["rows"] == limit


class TestPooledSyncStrategy:
    """Test connection pool-based sync strategy."""

    def test_pooled_sync_returns_correct_row_count(self, seeded_db_small: int, test_dsn: str):
        """Verify pooled sync strategy fetches all rows."""
        strategy = PooledSyncStrategy(
            batch_size=DEFAULT_BATCH_SIZE,
            pool_min_size=DEFAULT_POOL_MIN,
            pool_max_size=DEFAULT_POOL_MAX,
            dsn_override=test_dsn,
        )
        result = strategy.execute(limit=seeded_db_small)

        assert result["rows"] == seeded_db_small
        assert result["duration_seconds"] > 0

    def test_pooled_sync_with_various_pool_sizes(self, seeded_db_small: int, test_dsn: str):
        """Verify pooled sync works with different pool configurations."""
        pool_configs = [(1, 2), (2, 5), (1, 10)]
        for min_size, max_size in pool_configs:
            strategy = PooledSyncStrategy(
                batch_size=20,
                pool_min_size=min_size,
                pool_max_size=max_size,
                dsn_override=test_dsn,
            )
            result = strategy.execute(limit=seeded_db_small)
            assert result["rows"] == seeded_db_small, f"Failed with pool=({min_size},{max_size})"


class TestMultiprocessingStrategy:
    """Test multiprocessing-based strategy."""

    def test_multiprocessing_returns_correct_row_count(self, seeded_db_small: int, test_dsn: str):
        """Verify multiprocessing strategy fetches all rows."""
        strategy = MultiprocessingStrategy(
            processes=DEFAULT_PROCESS_COUNT,
            chunk_size=DEFAULT_CHUNK_SIZE,
            dsn_override=test_dsn,
        )
        result = strategy.execute(limit=seeded_db_small)

        assert result["rows"] == seeded_db_small
        # Duration is measured by orchestrator profiler, not by strategy directly

    def test_multiprocessing_with_various_process_counts(self, seeded_db_small: int, test_dsn: str):
        """Verify multiprocessing works with different process counts."""
        process_counts = [1, DEFAULT_PROCESS_COUNT, 4]
        for processes in process_counts:
            strategy = MultiprocessingStrategy(
                processes=processes, chunk_size=20, dsn_override=test_dsn
            )
            result = strategy.execute(limit=seeded_db_small)
            assert result["rows"] == seeded_db_small, f"Failed with processes={processes}"

    def test_multiprocessing_with_various_chunk_sizes(self, seeded_db_small: int, test_dsn: str):
        """Verify multiprocessing works with different chunk sizes."""
        chunk_sizes = [10, DEFAULT_CHUNK_SIZE, 50]
        for chunk_size in chunk_sizes:
            strategy = MultiprocessingStrategy(
                processes=DEFAULT_PROCESS_COUNT,
                chunk_size=chunk_size,
                dsn_override=test_dsn,
            )
            result = strategy.execute(limit=seeded_db_small)
            assert result["rows"] == seeded_db_small, f"Failed with chunk_size={chunk_size}"

    def test_multiprocessing_handles_gapped_ids(self, seeded_db_gapped: int, test_dsn: str):
        """Verify multiprocessing strategy respects limit with non-contiguous IDs."""
        limit = min(20, seeded_db_gapped)
        strategy = MultiprocessingStrategy(
            processes=DEFAULT_PROCESS_COUNT,
            chunk_size=7,
            dsn_override=test_dsn,
        )
        result = strategy.execute(limit=limit)

        assert result["rows"] == limit


class TestAsyncStreamStrategy:
    """Test async streaming strategy."""

    def test_async_stream_returns_correct_row_count(self, seeded_db_small: int, test_dsn: str):
        """Verify async stream strategy fetches all rows."""
        strategy = AsyncStreamStrategy(batch_size=DEFAULT_BATCH_SIZE, dsn_override=test_dsn)
        result = strategy.execute(limit=seeded_db_small)

        assert result["rows"] == seeded_db_small
        assert result["duration_seconds"] > 0

    def test_async_stream_with_various_batch_sizes(self, seeded_db_small: int, test_dsn: str):
        """Verify async stream works with different batch sizes."""
        batch_sizes = [5, 20, 50]
        for batch_size in batch_sizes:
            strategy = AsyncStreamStrategy(batch_size=batch_size, dsn_override=test_dsn)
            result = strategy.execute(limit=seeded_db_small)
            assert result["rows"] == seeded_db_small, f"Failed with batch_size={batch_size}"

    def test_async_stream_concurrent_handles_gapped_ids(self, seeded_db_gapped: int, test_dsn: str):
        """Verify async concurrent mode respects LIMIT with gapped IDs."""
        limit = min(20, seeded_db_gapped)
        strategy = AsyncStreamStrategy(
            batch_size=DEFAULT_BATCH_SIZE,
            concurrency=3,
            dsn_override=test_dsn,
        )
        result = strategy.execute(limit=limit)

        assert result["rows"] == limit


class TestOrchestratorIntegration:
    """Test the orchestrator with real database."""

    def test_run_single_strategy(self, seeded_db_small: int):
        """Verify orchestrator can run a single strategy."""
        os.environ["DB_HOST"] = "localhost"
        results = run_strategies(
            RunConfig(strategy_names=["naive"], limit=seeded_db_small, persist=False)
        )

        assert len(results) == 1
        assert results[0]["strategy"] == "naive"
        assert results[0]["rows"] == seeded_db_small

    def test_run_multiple_strategies(self, seeded_db_small: int):
        """Verify orchestrator can run multiple strategies."""
        os.environ["DB_HOST"] = "localhost"
        results = run_strategies(
            RunConfig(
                strategy_names=["naive", "cursor_pagination"],
                limit=seeded_db_small,
                persist=False,
            )
        )

        assert len(results) == MULTI_STRATEGY_COUNT
        strategy_names = [r["strategy"] for r in results]
        assert "naive" in strategy_names
        assert "cursor_pagination" in strategy_names

    def test_run_all_strategies(self, seeded_db_small: int):
        """Verify orchestrator can run all strategies."""
        os.environ["DB_HOST"] = "localhost"
        results = run_strategies(
            RunConfig(strategy_names=["all"], limit=seeded_db_small, persist=False)
        )

        assert len(results) >= EXPECTED_STRATEGIES_COUNT  # At least the 5 core strategies
        for result in results:
            assert result["rows"] == seeded_db_small
            assert result["duration_seconds"] > 0

    def test_orchestrator_with_warmup(self, seeded_db_small: int):
        """Verify orchestrator warmup functionality."""
        os.environ["DB_HOST"] = "localhost"
        results = run_strategies(
            RunConfig(
                strategy_names=["naive"],
                limit=seeded_db_small,
                persist=False,
                warmup=True,
            )
        )

        assert len(results) == 1
        assert results[0]["rows"] == seeded_db_small

    def test_orchestrator_with_multiple_runs(self, seeded_db_small: int):
        """Verify orchestrator can aggregate multiple runs."""
        os.environ["DB_HOST"] = "localhost"
        results = run_strategies(
            RunConfig(
                strategy_names=["naive"],
                limit=seeded_db_small,
                persist=False,
                runs=MULTI_RUN_COUNT,
            )
        )

        assert len(results) == 1
        result = results[0]
        assert result["runs"] == MULTI_RUN_COUNT
        assert "individual_runs" in result
        assert len(result["individual_runs"]) == MULTI_RUN_COUNT
        assert "duration_seconds" in result
        assert "median" in result["duration_seconds"]
        assert "mean" in result["duration_seconds"]
        assert "stddev" in result["duration_seconds"]


class TestPerformanceComparison:
    """Compare relative performance of strategies."""

    @pytest.mark.slow
    def test_medium_dataset_performance(self, seeded_db_medium: int):
        """
        Run all strategies on a medium dataset and verify they complete.

        This test is marked as slow and can be skipped with: pytest -m "not slow"
        """
        os.environ["DB_HOST"] = "localhost"
        results = run_strategies(
            RunConfig(strategy_names=["all"], limit=seeded_db_medium, persist=False)
        )

        assert len(results) >= EXPECTED_STRATEGIES_COUNT
        for result in results:
            assert result["rows"] == seeded_db_medium
            assert result["duration_seconds"] > 0
            # Verify reasonable throughput (at least 100 rows/sec on medium dataset)
            assert result["throughput_rows_per_sec"] > MIN_THROUGHPUT_ROWS_PER_SEC

    def test_strategies_produce_consistent_results(self, seeded_db_small: int):
        """Verify all strategies return the same row count."""
        os.environ["DB_HOST"] = "localhost"
        results = run_strategies(
            RunConfig(strategy_names=["all"], limit=seeded_db_small, persist=False)
        )

        row_counts = {r["strategy"]: r["rows"] for r in results}
        unique_counts = set(row_counts.values())

        # All strategies should return the same count
        assert len(unique_counts) == 1
        assert unique_counts.pop() == seeded_db_small
