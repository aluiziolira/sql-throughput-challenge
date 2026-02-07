"""
Naive (baseline) strategy: fetch-all, single-threaded, synchronous.

Intended as the simplest possible baseline to compare against more sophisticated
approaches (cursor pagination, pooling, async streaming, multiprocessing).
"""

from __future__ import annotations

import time

import psycopg

from src.config import get_settings
from src.infrastructure.db_factory import apply_statement_timeout, get_sync_connection
from src.strategies.abstract import BenchmarkStrategy, StrategyResult


class NaiveStrategy(BenchmarkStrategy):
    """
    Fetch all rows in a single query using a plain psycopg connection.

    WARNING: This will load the entire result set into memory. For large datasets
    (e.g., 1M rows), this is expected to be slower and memory-heavy compared to
    streaming/paginated strategies. Keep as a baseline only.
    """

    name: str = "naive"
    description: str = "Single SELECT * with fetchall (sync, no pagination/pooling)."

    def __init__(self, dsn_override: str | None = None) -> None:
        self._dsn_override = dsn_override

    def execute(self, limit: int) -> StrategyResult:
        """
        Run the naive fetch-all query and return basic metrics.
        """
        sql = "SELECT * FROM public.records ORDER BY id LIMIT %s;"
        rows_fetched: int = 0
        timeout_ms = get_settings().db_statement_timeout_ms

        start_time = time.perf_counter()
        # Use connection factory; allow optional DSN override for testing.
        if self._dsn_override:
            conn = psycopg.connect(self._dsn_override)
        else:
            conn = get_sync_connection()

        try:
            with conn.cursor() as cur:
                apply_statement_timeout(cur, timeout_ms)
                cur.execute(sql, (limit,))
                results = cur.fetchall()
                rows_fetched = len(results)
        finally:
            conn.close()
        duration_seconds = time.perf_counter() - start_time
        throughput_rows_per_sec = rows_fetched / duration_seconds if duration_seconds > 0 else 0.0

        return StrategyResult(
            rows=rows_fetched,
            duration_seconds=duration_seconds,
            throughput_rows_per_sec=throughput_rows_per_sec,
            peak_rss_bytes=None,  # Not measured in this baseline
            notes="Naive fetchall baseline; no pagination or pooling.",
        )


__all__ = ["NaiveStrategy"]
