"""
Cursor pagination strategy for the SQL Throughput Challenge.

Implements server-side cursor streaming with configurable `fetchmany` batching
to keep client memory usage stable while measuring sync read throughput.
"""

from __future__ import annotations

import time

from src.config import get_settings
from src.infrastructure.db_factory import (
    SELECT_RECORDS_SQL,
    apply_statement_timeout,
    fetch_in_batches,
    resolve_sync_connection,
)
from src.strategies.abstract import BenchmarkStrategy, StrategyResult


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

    def execute(self, limit: int) -> StrategyResult:
        """
        Execute batched reads with a server-side cursor.
        """
        notes = f"Cursor pagination batch_size={self.batch_size}."
        timeout_ms = get_settings().db_statement_timeout_ms
        rows_fetched = 0
        start = time.perf_counter()
        try:
            with resolve_sync_connection(self._dsn_override) as conn:
                apply_statement_timeout(conn, timeout_ms)
                # Use name to trigger server-side cursor
                with conn.cursor(name="cursor_pagination") as cur:
                    cur.execute(SELECT_RECORDS_SQL, (limit,))
                    for batch in fetch_in_batches(cur, self.batch_size):
                        rows_fetched += len(batch)
        except Exception as exc:
            if rows_fetched == 0:
                raise
            duration_seconds = time.perf_counter() - start
            throughput = rows_fetched / duration_seconds if duration_seconds > 0 else 0.0
            return StrategyResult(
                rows=rows_fetched,
                duration_seconds=duration_seconds,
                throughput_rows_per_sec=throughput,
                peak_rss_bytes=None,
                notes=notes,
                error=f"{type(exc).__name__}: {exc}",
                extra={"error_count": 1},
            )

        duration_seconds = time.perf_counter() - start
        throughput = rows_fetched / duration_seconds if duration_seconds > 0 else 0.0
        return StrategyResult(
            rows=rows_fetched,
            duration_seconds=duration_seconds,
            throughput_rows_per_sec=throughput,
            peak_rss_bytes=None,
            notes=notes,
        )


__all__ = ["CursorPaginationStrategy"]
