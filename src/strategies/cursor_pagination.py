"""
Cursor pagination strategy (stub) for the SQL Throughput Challenge.

Intent:
- Demonstrate batched retrieval without loading the entire result set.
- Use a server-side cursor or LIMIT/OFFSET with configurable batch size.
- Keep this minimal until the orchestrator wires metrics collection.
"""

from __future__ import annotations

import time
from typing import Iterator, Optional

import psycopg

from src.config import get_settings
from src.infrastructure.db_factory import get_sync_connection
from src.strategies.abstract import BenchmarkStrategy, StrategyResult


def _batched_fetch(cursor: psycopg.Cursor, batch_size: int) -> Iterator[list]:
    """
    Yield batches from a cursor using fetchmany.
    """
    while True:
        batch = cursor.fetchmany(batch_size)
        if not batch:
            break
        yield batch


class CursorPaginationStrategy(BenchmarkStrategy):
    """
    Cursor-based pagination using psycopg server-side cursor semantics.
    """

    name: str = "cursor_pagination"
    description: str = "Server-side cursor with fetchmany batching (sync)."

    def __init__(self, batch_size: int | None = None, dsn_override: Optional[str] = None) -> None:
        settings = get_settings()
        self.batch_size = batch_size or settings.benchmark_batch_size
        self._dsn_override = dsn_override

    def execute(self, limit: int) -> StrategyResult:
        """
        Execute batched reads with a server-side cursor.
        """
        sql = "SELECT * FROM public.records ORDER BY id LIMIT %s;"
        start = time.perf_counter()
        rows_fetched = 0

        if self._dsn_override:
            conn = psycopg.connect(self._dsn_override)
        else:
            conn = get_sync_connection()

        try:
            # Use name to trigger server-side cursor
            with conn.cursor(name="cursor_pagination") as cur:
                cur.execute(sql, (limit,))
                for batch in _batched_fetch(cur, self.batch_size):
                    rows_fetched += len(batch)
        finally:
            conn.close()

        duration = time.perf_counter() - start
        throughput = rows_fetched / duration if duration > 0 else 0.0

        return StrategyResult(
            rows=rows_fetched,
            duration_seconds=duration,
            throughput_rows_per_sec=throughput,
            peak_rss_bytes=None,  # Not measured yet
            notes=f"Cursor pagination batch_size={self.batch_size}.",
        )


__all__ = ["CursorPaginationStrategy"]
