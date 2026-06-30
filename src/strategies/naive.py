"""
Naive (baseline) strategy: fetch-all, single-threaded, synchronous.

Intended as the simplest possible baseline to compare against more sophisticated
approaches (cursor pagination, pooling, async streaming, multiprocessing).
"""

from __future__ import annotations

from src.config import get_settings
from src.infrastructure.db_factory import (
    SELECT_RECORDS_SQL,
    apply_statement_timeout,
    resolve_sync_connection,
)
from src.strategies.abstract import BenchmarkStrategy, StrategyResult
from src.utils.timing import run_timed


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

    def _fetch_all(self, limit: int) -> int:
        timeout_ms = get_settings().db_statement_timeout_ms
        with resolve_sync_connection(self._dsn_override) as conn, conn.cursor() as cur:
            apply_statement_timeout(cur, timeout_ms)
            cur.execute(SELECT_RECORDS_SQL, (limit,))
            return len(cur.fetchall())

    def execute(self, limit: int) -> StrategyResult:
        """
        Run the naive fetch-all query and return basic metrics.
        """
        rows, duration_seconds, throughput_rows_per_sec = run_timed(lambda: self._fetch_all(limit))

        return StrategyResult(
            rows=rows,
            duration_seconds=duration_seconds,
            throughput_rows_per_sec=throughput_rows_per_sec,
            peak_rss_bytes=None,  # Not measured in this baseline
            notes="Naive fetchall baseline; no pagination or pooling.",
        )


__all__ = ["NaiveStrategy"]
