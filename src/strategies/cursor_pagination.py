"""
Cursor pagination strategy for the SQL Throughput Challenge.

Implements server-side cursor streaming with configurable `fetchmany` batching
to keep client memory usage stable while measuring sync read throughput.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from typing import Any

import psycopg

from src.config import get_settings
from src.infrastructure.db_factory import apply_statement_timeout, get_sync_connection
from src.strategies.abstract import BenchmarkStrategy, StrategyResult


def _batched_fetch(cursor: psycopg.Cursor, batch_size: int) -> Iterator[list[tuple[Any, ...]]]:
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

    def __init__(self, batch_size: int | None = None, dsn_override: str | None = None) -> None:
        settings = get_settings()
        self.batch_size = batch_size or settings.benchmark_batch_size
        self._dsn_override = dsn_override

    def execute(self, limit: int) -> StrategyResult:
        """
        Execute batched reads with a server-side cursor.
        """
        sql = "SELECT * FROM public.records ORDER BY id LIMIT %s;"
        rows_fetched = 0
        timeout_ms = get_settings().db_statement_timeout_ms

        start_time = time.perf_counter()
        if self._dsn_override:
            conn = psycopg.connect(self._dsn_override)
        else:
            conn = get_sync_connection()

        try:
            # Use name to trigger server-side cursor
            with conn.cursor(name="cursor_pagination") as cur:
                apply_statement_timeout(cur, timeout_ms)
                cur.execute(sql, (limit,))
                for batch in _batched_fetch(cur, self.batch_size):
                    rows_fetched += len(batch)
        finally:
            conn.close()
        duration_seconds = time.perf_counter() - start_time
        throughput_rows_per_sec = rows_fetched / duration_seconds if duration_seconds > 0 else 0.0

        return StrategyResult(
            rows=rows_fetched,
            duration_seconds=duration_seconds,
            throughput_rows_per_sec=throughput_rows_per_sec,
            peak_rss_bytes=None,  # Not measured yet
            notes=f"Cursor pagination batch_size={self.batch_size}.",
        )


__all__ = ["CursorPaginationStrategy"]
