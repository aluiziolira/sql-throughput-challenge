"""
Async streaming strategy for the SQL Throughput Challenge.

Intent:
- Demonstrate non-blocking, backpressure-friendly streaming of rows from Postgres.
- Use asyncpg with cursor-based iteration to avoid loading the full result set.
- Provide event-loop safety for both sync and async contexts.
"""

from __future__ import annotations

import asyncio
import time

import asyncpg

from src.config import get_settings
from src.infrastructure.db_factory import async_apply_statement_timeout, build_dsn
from src.strategies.abstract import BenchmarkStrategy, StrategyResult

_CONCURRENT_ID_WINDOW_THRESHOLD = 50_000
_CONCURRENT_ID_WINDOW_SIZE = 20_000


def _split_limit_ranges(limit: int, concurrency: int) -> list[tuple[int, int]]:
    """Split a limit into ranges for concurrent processing using 0-based indices.

    Returns list of (start_idx, end_idx) tuples where:
    - start_idx is inclusive (0-based)
    - end_idx is exclusive
    - The slice ids[start_idx:end_idx] gives the correct chunk
    """
    if limit <= 0:
        return []

    effective_concurrency = max(1, min(concurrency, limit))
    base_size, remainder = divmod(limit, effective_concurrency)
    ranges: list[tuple[int, int]] = []
    start_idx = 0

    for index in range(effective_concurrency):
        chunk_size = base_size + (1 if index < remainder else 0)
        end_idx = start_idx + chunk_size
        ranges.append((start_idx, end_idx))
        start_idx = end_idx

    return ranges


class AsyncStreamStrategy(BenchmarkStrategy):
    """
    Stream rows asynchronously using asyncpg cursors with event-loop safety.

    Note: This strategy intentionally uses asyncpg directly (rather than psycopg async)
    because asyncpg provides superior performance for streaming workloads due to its
    native async implementation and binary protocol support. This is a deliberate
    architectural choice for the async benchmarking scenario.

    Event-Loop Safety:
    - execute(): Safe to call from synchronous code only
    - execute_async(): Safe to call from async contexts (FastAPI, aiohttp, etc.)
    """

    name: str = "async_stream"
    description: str = "asyncpg cursor-based streaming with event-loop safety."

    def __init__(
        self,
        batch_size: int | None = None,
        concurrency: int = 1,
        dsn_override: str | None = None,
    ) -> None:
        settings = get_settings()
        self.batch_size = batch_size or settings.benchmark_batch_size
        self.concurrency = concurrency
        self._dsn_override = dsn_override

    async def _stream_concurrent(self, limit: int) -> int:
        """
        Stream with multiple concurrent cursors.

        Splits work into N ranges and fetches concurrently using connection pool.
        """
        if limit <= 0:
            return 0

        settings = get_settings()
        dsn = self._dsn_override or build_dsn()

        effective_concurrency = max(1, min(self.concurrency, limit))
        if effective_concurrency <= 1:
            return await self._stream(limit)

        # Create async pool for concurrent connections
        pool = await asyncpg.create_pool(
            dsn,
            min_size=1,
            max_size=effective_concurrency * 2,
        )

        try:

            async def fetch_range(selected_ids: list[int], start_idx: int, end_idx: int) -> int:
                chunk_ids = selected_ids[start_idx:end_idx]
                if not chunk_ids:
                    return 0
                async with pool.acquire() as conn:
                    async with conn.transaction():
                        await async_apply_statement_timeout(conn, settings.db_statement_timeout_ms)
                        cursor = await conn.cursor(
                            "SELECT * FROM public.records WHERE id = ANY($1) ORDER BY id",
                            chunk_ids,
                        )
                        count = 0
                        while True:
                            batch = await cursor.fetch(self.batch_size)
                            if not batch:
                                break
                            count += len(batch)
                        return count

            async def process_selected_ids(ids: list[int]) -> int:
                if not ids:
                    return 0
                ranges = _split_limit_ranges(len(ids), effective_concurrency)
                tasks = [fetch_range(ids, start_idx, end_idx) for start_idx, end_idx in ranges]
                counts = await asyncio.gather(*tasks)
                return sum(counts)

            # Preserve existing behavior for small/medium limits.
            if limit <= _CONCURRENT_ID_WINDOW_THRESHOLD:
                async with pool.acquire() as conn:
                    async with conn.transaction():
                        await async_apply_statement_timeout(conn, settings.db_statement_timeout_ms)
                        rows = await conn.fetch(
                            "SELECT id FROM public.records ORDER BY id LIMIT $1", limit
                        )
                ids = [row["id"] for row in rows]
                return await process_selected_ids(ids)

            # Guardrail path for large limits: bound in-memory IDs via windowed selection.
            rows_read = 0
            remaining = limit
            last_selected_id: int | None = None

            while remaining > 0:
                window_limit = min(_CONCURRENT_ID_WINDOW_SIZE, remaining)
                async with pool.acquire() as conn:
                    async with conn.transaction():
                        await async_apply_statement_timeout(conn, settings.db_statement_timeout_ms)
                        rows = await conn.fetch(
                            (
                                "SELECT id FROM public.records "
                                "WHERE ($1::bigint IS NULL OR id > $1) "
                                "ORDER BY id LIMIT $2"
                            ),
                            last_selected_id,
                            window_limit,
                        )

                window_ids = [row["id"] for row in rows]
                if not window_ids:
                    break

                rows_read += await process_selected_ids(window_ids)
                remaining -= len(window_ids)
                last_selected_id = window_ids[-1]

            return rows_read

        finally:
            await pool.close()

    async def _stream(self, limit: int) -> int:
        """
        Stream rows in batches and return the count.
        """
        settings = get_settings()
        dsn = self._dsn_override or build_dsn()

        rows_read = 0
        conn = None
        try:
            conn = await asyncpg.connect(dsn)
            async with conn.transaction():
                await async_apply_statement_timeout(conn, settings.db_statement_timeout_ms)
                cursor = await conn.cursor(
                    "SELECT * FROM public.records ORDER BY id LIMIT $1", limit
                )
                while True:
                    batch = await cursor.fetch(self.batch_size)
                    if not batch:
                        break
                    rows_read += len(batch)
        finally:
            if conn:
                await conn.close()

        return rows_read

    def execute(self, limit: int) -> StrategyResult:
        """
        Execute async streaming with automatic loop detection.

        Raises RuntimeError if called from within async context.
        Use execute_async() instead from async code.
        """
        start_time = time.perf_counter()
        try:
            # Check if event loop is already running
            asyncio.get_running_loop()
            # We're inside an async context - cannot use asyncio.run()
            raise RuntimeError(
                "AsyncStreamStrategy.execute() called from async context. "
                "Use execute_async() instead or call from synchronous code."
            )
        except RuntimeError as e:
            if "no running event loop" in str(e).lower():
                # No loop running - safe to create one
                if self.concurrency > 1:
                    rows_read = asyncio.run(self._stream_concurrent(limit))
                else:
                    rows_read = asyncio.run(self._stream(limit))
            else:
                # Re-raise if it's our error message
                raise
        duration_seconds = time.perf_counter() - start_time
        throughput_rows_per_sec = rows_read / duration_seconds if duration_seconds > 0 else 0.0

        return StrategyResult(
            rows=rows_read,
            duration_seconds=duration_seconds,
            throughput_rows_per_sec=throughput_rows_per_sec,
            peak_rss_bytes=None,
            notes=f"asyncpg batch_size={self.batch_size} concurrency={self.concurrency}",
        )

    async def execute_async(self, limit: int) -> StrategyResult:
        """
        Async-native execution for use within async contexts.

        Use this when calling from async frameworks (FastAPI, aiohttp).
        """
        start_time = time.perf_counter()
        if self.concurrency > 1:
            rows_read = await self._stream_concurrent(limit)
        else:
            rows_read = await self._stream(limit)
        duration_seconds = time.perf_counter() - start_time
        throughput_rows_per_sec = rows_read / duration_seconds if duration_seconds > 0 else 0.0

        return StrategyResult(
            rows=rows_read,
            duration_seconds=duration_seconds,
            throughput_rows_per_sec=throughput_rows_per_sec,
            peak_rss_bytes=None,
            notes=f"asyncpg batch_size={self.batch_size} concurrency={self.concurrency}",
        )


__all__ = ["AsyncStreamStrategy"]
