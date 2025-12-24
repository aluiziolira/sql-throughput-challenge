"""
Async streaming strategy stub for the SQL Throughput Challenge.

Intent:
- Demonstrate non-blocking, backpressure-friendly streaming of rows from Postgres.
- Use asyncpg with cursor-based iteration to avoid loading the full result set.
- Keep metrics collection minimal; orchestration wiring will be added later.
"""

from __future__ import annotations

import asyncio
import time
from typing import Optional

import asyncpg

from src.config import get_settings
from src.strategies.abstract import BenchmarkStrategy, StrategyResult


class AsyncStreamStrategy(BenchmarkStrategy):
    """
    Stream rows asynchronously using asyncpg cursors.

    Note: This strategy intentionally uses asyncpg directly (rather than psycopg async)
    because asyncpg provides superior performance for streaming workloads due to its
    native async implementation and binary protocol support. This is a deliberate
    architectural choice for the async benchmarking scenario.

    This stub focuses on structure and a basic count of streamed rows. Memory/CPU
    measurements and richer orchestration hooks will be added later.
    """

    name: str = "async_stream"
    description: str = "asyncpg cursor-based streaming with fetch batch size."

    def __init__(self, batch_size: int | None = None, dsn_override: Optional[str] = None) -> None:
        settings = get_settings()
        self.batch_size = batch_size or settings.benchmark_batch_size
        self._dsn_override = dsn_override

    async def _stream(self, limit: int) -> int:
        """
        Stream rows in batches and return the count.
        """
        settings = get_settings()
        dsn = (
            self._dsn_override
            or f"postgresql://{settings.db_user}:{settings.db_password}"
            f"@{settings.db_host}:{settings.db_port}/{settings.db_name}"
        )

        rows_read = 0
        conn = await asyncpg.connect(dsn)
        try:
            async with conn.transaction():
                cursor = await conn.cursor(
                    "SELECT * FROM public.records ORDER BY id LIMIT $1", limit
                )
                while True:
                    batch = await cursor.fetch(self.batch_size)
                    if not batch:
                        break
                    rows_read += len(batch)
        finally:
            await conn.close()

        return rows_read

    def execute(self, limit: int) -> StrategyResult:
        """
        Execute the async streaming strategy and return basic metrics.
        """
        start = time.perf_counter()
        rows_read = asyncio.run(self._stream(limit))
        duration = time.perf_counter() - start
        throughput = rows_read / duration if duration > 0 else 0.0

        return StrategyResult(
            rows=rows_read,
            duration_seconds=duration,
            throughput_rows_per_sec=throughput,
            peak_rss_bytes=None,  # Not measured yet
            notes=f"asyncpg streaming with batch_size={self.batch_size}",
        )


__all__ = ["AsyncStreamStrategy"]
