"""
Multiprocessing strategy for the SQL Throughput Challenge.

Intent:
- Demonstrate parallel processing of data chunks fetched from Postgres.
- Split work into ID chunks and fan out to worker processes.
- Includes error capture, timeout protection, and Windows compatibility.
"""

from __future__ import annotations

import multiprocessing as mp
import time
from collections.abc import Sequence
from dataclasses import dataclass
from functools import partial

import psycopg

from src.config import get_settings
from src.infrastructure.db_factory import build_dsn, fetch_in_batches
from src.strategies.abstract import BenchmarkStrategy, StrategyResult

_WORKER_POOL_TIMEOUT_SECONDS = 600


@dataclass(frozen=True)
class WorkItem:
    ids: list[int]


@dataclass(frozen=True)
class _PoolRunStats:
    rows_read: int
    ids_count: int
    errors: tuple[str, ...]
    completed_chunks: int
    total_chunks: int
    timed_out: bool


def _fetch_ids(dsn: str, timeout_ms: int, work: WorkItem) -> tuple[int, str | None]:
    """
    Worker function: fetch rows for an explicit list of IDs.

    Returns (row_count, error_message).
    Uses streaming with server-side cursor to reduce memory.
    """
    try:
        if not work.ids:
            return (0, None)

        sql = "SELECT * FROM public.records WHERE id = ANY(%s) ORDER BY id;"
        with psycopg.connect(dsn) as conn:
            with conn.cursor(name=f"mp_worker_{work.ids[0]}") as cur:
                if timeout_ms > 0:
                    cur.execute("SET LOCAL statement_timeout = %s;", (timeout_ms,))
                cur.execute(sql, (work.ids,))

                # Stream instead of fetchall to reduce per-worker memory
                row_count = sum(len(batch) for batch in fetch_in_batches(cur, 10_000))

                return (row_count, None)
    except Exception as exc:
        if work.ids:
            return (
                0,
                f"Worker ids[{len(work.ids)}] {work.ids[0]}..{work.ids[-1]}: {exc!s}",
            )
        return (0, f"Worker ids[0]: {exc!s}")


def _accumulate_worker_outcome(
    rows_read: int,
    errors: list[str],
    count: int,
    error: str | None,
) -> int:
    """
    Fold one worker outcome into totals without double-counting.

    Failed workers contribute to ``errors`` only; successful workers add ``count``.
    """
    if error:
        errors.append(error)
        return rows_read
    return rows_read + count


class MultiprocessingStrategy(BenchmarkStrategy):
    """
    Fan-out reads across multiple processes by ID chunks.

    Includes error capture and timeout protection for robustness.
    """

    name: str = "multiprocessing"
    description: str = "ProcessPool with ID chunking, error handling, and streaming."

    def __init__(
        self,
        processes: int | None = None,
        chunk_size: int = 50_000,
        dsn_override: str | None = None,
        worker_timeout_seconds: int = _WORKER_POOL_TIMEOUT_SECONDS,
    ) -> None:
        self.processes = processes or max(mp.cpu_count() - 1, 1)
        self.chunk_size = chunk_size
        self._dsn_override = dsn_override
        # Explicit timeout keeps cancelled runs bounded; 0 disables the deadline.
        self.worker_timeout_seconds = worker_timeout_seconds

    def _make_work_items(self, ids: Sequence[int]) -> list[WorkItem]:
        work: list[WorkItem] = []
        for start in range(0, len(ids), self.chunk_size):
            chunk_ids = list(ids[start : start + self.chunk_size])
            if chunk_ids:
                work.append(WorkItem(ids=chunk_ids))
        return work

    def _build_result(self, stats: _PoolRunStats) -> StrategyResult:
        notes = (
            f"ProcessPool size={self.processes}, chunk_size={self.chunk_size}, "
            f"ids={stats.ids_count}, "
            f"chunks_completed={stats.completed_chunks}/{stats.total_chunks}"
        )
        if stats.errors:
            notes += f", errors={len(stats.errors)}"
        if stats.timed_out:
            notes += ", timed_out=true"

        timeout_message = (
            f"Workers timed out after {self.worker_timeout_seconds} seconds "
            f"({stats.completed_chunks}/{stats.total_chunks} chunks completed)"
        )
        error_text = "; ".join(stats.errors[:3]) if stats.errors else None
        if stats.timed_out and error_text is None:
            error_text = timeout_message
        elif stats.timed_out:
            error_text = f"{error_text}; {timeout_message}"

        return StrategyResult(
            rows=stats.rows_read,
            peak_rss_bytes=None,  # Cannot measure worker memory from main process
            notes=notes,
            error=error_text,
            extra={
                "error_count": len(stats.errors) + (1 if stats.timed_out else 0),
                "chunks_completed": stats.completed_chunks,
                "chunks_total": stats.total_chunks,
                "timed_out": stats.timed_out,
            },
        )

    def execute(self, limit: int) -> StrategyResult:
        """
        Execute multiprocessing with timeout and error capture.

        Note: Duration is measured by orchestrator profiler to include
        process spawn overhead, ID selection, and result aggregation.
        """
        dsn = self._dsn_override or build_dsn()

        timeout_ms = get_settings().db_statement_timeout_ms
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                if timeout_ms > 0:
                    cur.execute("SET LOCAL statement_timeout = %s;", (timeout_ms,))
                cur.execute("SELECT id FROM public.records ORDER BY id LIMIT %s;", (limit,))
                ids = [row[0] for row in cur.fetchall()]

        if not ids:
            return StrategyResult(
                rows=0,
                peak_rss_bytes=None,
                notes=f"ProcessPool size={self.processes}, chunk_size={self.chunk_size}, ids=0",
                error=None,
                extra={
                    "error_count": 0,
                    "chunks_completed": 0,
                    "chunks_total": 0,
                    "timed_out": False,
                },
            )

        work_items: Sequence[WorkItem] = self._make_work_items(ids)

        rows_read = 0
        errors: list[str] = []
        completed_chunks = 0
        timed_out = False
        worker = partial(_fetch_ids, dsn, timeout_ms)

        ctx = mp.get_context("spawn")
        with ctx.Pool(processes=self.processes) as pool:
            # imap_unordered yields completed chunks so a timeout can still report
            # partial rows/failures instead of discarding finished work.
            iterator = pool.imap_unordered(worker, work_items)
            deadline = (
                time.monotonic() + self.worker_timeout_seconds
                if self.worker_timeout_seconds > 0
                else None
            )
            try:
                for _ in range(len(work_items)):
                    if deadline is None:
                        count, error = iterator.next()
                    else:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            raise mp.TimeoutError()
                        count, error = iterator.next(timeout=remaining)
                    completed_chunks += 1
                    rows_read = _accumulate_worker_outcome(rows_read, errors, count, error)
            except mp.TimeoutError:
                # Mark timeout only; _build_result records a single timeout failure.
                timed_out = True
                pool.terminate()

        return self._build_result(
            _PoolRunStats(
                rows_read=rows_read,
                ids_count=len(ids),
                errors=tuple(errors),
                completed_chunks=completed_chunks,
                total_chunks=len(work_items),
                timed_out=timed_out,
            )
        )


__all__ = ["MultiprocessingStrategy"]
