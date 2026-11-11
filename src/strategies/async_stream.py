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
from dataclasses import dataclass, field

import asyncpg

from src.config import get_settings
from src.infrastructure.db_factory import async_apply_statement_timeout, build_dsn
from src.strategies.abstract import BenchmarkStrategy, StrategyResult

_CONCURRENT_ID_WINDOW_THRESHOLD = 50_000
_CONCURRENT_ID_WINDOW_SIZE = 20_000


@dataclass
class _StreamOutcome:
    """Rows read plus worker/batch errors for partial-run accounting."""

    rows: int = 0
    errors: list[str] = field(default_factory=list)


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


def _merge_task_outcomes(results: list[object]) -> _StreamOutcome:
    """
    Merge gather outcomes without double-counting.

    Successful ints add to rows; exceptions become errors and contribute 0 rows.
    """
    outcome = _StreamOutcome()
    for result in results:
        if isinstance(result, BaseException):
            outcome.errors.append(f"{result.__class__.__name__}: {result}")
            continue
        if isinstance(result, _StreamOutcome):
            outcome.rows += result.rows
            outcome.errors.extend(result.errors)
            continue
        if isinstance(result, int):
            outcome.rows += result
            continue
        outcome.errors.append(f"Unexpected worker result type: {type(result)!r}")
    return outcome


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

    async def _fetch_id_chunk(
        self,
        pool: asyncpg.Pool,
        selected_ids: list[int],
        start_idx: int,
        end_idx: int,
        timeout_ms: int,
    ) -> int:
        chunk_ids = selected_ids[start_idx:end_idx]
        if not chunk_ids:
            return 0
        async with pool.acquire() as conn:
            async with conn.transaction():
                await async_apply_statement_timeout(conn, timeout_ms)
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

    async def _process_selected_ids(
        self,
        pool: asyncpg.Pool,
        ids: list[int],
        effective_concurrency: int,
        timeout_ms: int,
    ) -> _StreamOutcome:
        if not ids:
            return _StreamOutcome()
        ranges = _split_limit_ranges(len(ids), effective_concurrency)
        tasks = [
            self._fetch_id_chunk(pool, ids, start_idx, end_idx, timeout_ms)
            for start_idx, end_idx in ranges
        ]
        # return_exceptions keeps successful chunk counts when a peer fails.
        raw_results: list[object] = list(await asyncio.gather(*tasks, return_exceptions=True))
        return _merge_task_outcomes(raw_results)

    async def _select_ids_full(self, pool: asyncpg.Pool, limit: int, timeout_ms: int) -> list[int]:
        async with pool.acquire() as conn:
            async with conn.transaction():
                await async_apply_statement_timeout(conn, timeout_ms)
                rows = await conn.fetch("SELECT id FROM public.records ORDER BY id LIMIT $1", limit)
        return [row["id"] for row in rows]

    async def _select_ids_window(
        self,
        pool: asyncpg.Pool,
        last_selected_id: int | None,
        window_limit: int,
        timeout_ms: int,
    ) -> list[int]:
        async with pool.acquire() as conn:
            async with conn.transaction():
                await async_apply_statement_timeout(conn, timeout_ms)
                rows = await conn.fetch(
                    (
                        "SELECT id FROM public.records "
                        "WHERE ($1::bigint IS NULL OR id > $1) "
                        "ORDER BY id LIMIT $2"
                    ),
                    last_selected_id,
                    window_limit,
                )
        return [row["id"] for row in rows]

    async def _stream_windowed(
        self,
        pool: asyncpg.Pool,
        limit: int,
        effective_concurrency: int,
        timeout_ms: int,
    ) -> _StreamOutcome:
        outcome = _StreamOutcome()
        remaining = limit
        last_selected_id: int | None = None

        while remaining > 0:
            window_limit = min(_CONCURRENT_ID_WINDOW_SIZE, remaining)
            window_ids = await self._select_ids_window(
                pool, last_selected_id, window_limit, timeout_ms
            )
            if not window_ids:
                break

            window_outcome = await self._process_selected_ids(
                pool, window_ids, effective_concurrency, timeout_ms
            )
            outcome.rows += window_outcome.rows
            outcome.errors.extend(window_outcome.errors)
            remaining -= len(window_ids)
            last_selected_id = window_ids[-1]

        return outcome

    async def _stream_concurrent(self, limit: int) -> _StreamOutcome:
        """
        Stream with multiple concurrent cursors.

        Splits work into N ranges and fetches concurrently using connection pool.
        Worker failures are collected so partial row progress is preserved.
        """
        if limit <= 0:
            return _StreamOutcome()

        settings = get_settings()
        dsn = self._dsn_override or build_dsn()
        timeout_ms = settings.db_statement_timeout_ms

        effective_concurrency = max(1, min(self.concurrency, limit))
        if effective_concurrency <= 1:
            return await self._stream(limit)

        pool = await asyncpg.create_pool(
            dsn,
            min_size=1,
            max_size=effective_concurrency * 2,
        )

        try:
            if limit <= _CONCURRENT_ID_WINDOW_THRESHOLD:
                ids = await self._select_ids_full(pool, limit, timeout_ms)
                return await self._process_selected_ids(
                    pool, ids, effective_concurrency, timeout_ms
                )
            return await self._stream_windowed(pool, limit, effective_concurrency, timeout_ms)
        finally:
            await pool.close()

    async def _stream(self, limit: int) -> _StreamOutcome:
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
            if conn is not None:
                await conn.close()

        return _StreamOutcome(rows=rows_read)

    def execute(self, limit: int) -> StrategyResult:
        """
        Execute async streaming with automatic loop detection.

        Raises RuntimeError if called from within async context.
        Use execute_async() instead from async code.
        """
        try:
            # Check if event loop is already running
            asyncio.get_running_loop()
        except RuntimeError as e:
            if "no running event loop" not in str(e).lower():
                raise
            # No loop running - safe to create one and delegate to the async-native path.
            return asyncio.run(self.execute_async(limit))

        # We're inside an async context - cannot use asyncio.run()
        raise RuntimeError(
            "AsyncStreamStrategy.execute() called from async context. "
            "Use execute_async() instead or call from synchronous code."
        )

    async def execute_async(self, limit: int) -> StrategyResult:
        """
        Async-native execution for use within async contexts.

        Use this when calling from async frameworks (FastAPI, aiohttp).
        """
        start_time = time.perf_counter()
        if self.concurrency > 1:
            outcome = await self._stream_concurrent(limit)
        else:
            outcome = await self._stream(limit)
        duration_seconds = time.perf_counter() - start_time
        throughput_rows_per_sec = outcome.rows / duration_seconds if duration_seconds > 0 else 0.0

        error_text = "; ".join(outcome.errors[:3]) if outcome.errors else None
        notes = f"asyncpg batch_size={self.batch_size} concurrency={self.concurrency}"
        if outcome.errors:
            notes += f", errors={len(outcome.errors)}"

        return StrategyResult(
            rows=outcome.rows,
            duration_seconds=duration_seconds,
            throughput_rows_per_sec=throughput_rows_per_sec,
            peak_rss_bytes=None,
            notes=notes,
            error=error_text,
            extra={"error_count": len(outcome.errors)},
        )


__all__ = ["AsyncStreamStrategy"]
