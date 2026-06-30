"""
Cursor pagination strategy for the SQL Throughput Challenge.

Implements server-side cursor streaming with configurable `fetchmany` batching
to keep client memory usage stable while measuring sync read throughput.
"""

from __future__ import annotations

from src.config import get_settings
from src.infrastructure.db_factory import (
    SELECT_RECORDS_SQL,
    apply_statement_timeout,
    fetch_in_batches,
    resolve_sync_connection,
)
from src.strategies.abstract import BenchmarkStrategy, StrategyResult
from src.utils.timing import run_timed


class CursorPaginationStrategy(BenchmarkStrategy):
    """
    Cursor-based pagination using psycopg server-side cursor semantics.
    """

    name: str = "cursor_pagination"
    description: str = "Server-side cursor with fetchmany batching (sync)."

    def __init__(self, batch_size: int | None = None, dsn_override: str | None = None) -> None:
        settings = get_settings()
        self.batch_size = batch_size or settings.benchmark_batch_size
        self._dsn_override = dsn_override

    def _fetch_all(self, limit: int) -> int:
        timeout_ms = get_settings().db_statement_timeout_ms
        rows_fetched = 0
        with resolve_sync_connection(self._dsn_override) as conn:
            # Use name to trigger server-side cursor
            with conn.cursor(name="cursor_pagination") as cur:
                apply_statement_timeout(cur, timeout_ms)
                cur.execute(SELECT_RECORDS_SQL, (limit,))
                for batch in fetch_in_batches(cur, self.batch_size):
                    rows_fetched += len(batch)
        return rows_fetched

    def execute(self, limit: int) -> StrategyResult:
        """
        Execute batched reads with a server-side cursor.
        """
        rows, duration_seconds, throughput_rows_per_sec = run_timed(lambda: self._fetch_all(limit))

        return StrategyResult(
            rows=rows,
            duration_seconds=duration_seconds,
            throughput_rows_per_sec=throughput_rows_per_sec,
            peak_rss_bytes=None,  # Not measured yet
            notes=f"Cursor pagination batch_size={self.batch_size}.",
        )


__all__ = ["CursorPaginationStrategy"]
