"""
Multiprocessing strategy for the SQL Throughput Challenge.

Intent:
- Demonstrate parallel processing of data chunks fetched from Postgres.
- Split work into ID chunks and fan out to worker processes.
- Includes error capture, timeout protection, and Windows compatibility.
"""

from __future__ import annotations

import multiprocessing as mp
from collections.abc import Sequence
from dataclasses import dataclass
from functools import partial

import psycopg

from src.config import get_settings
from src.infrastructure.db_factory import build_dsn
from src.strategies.abstract import BenchmarkStrategy, StrategyResult


@dataclass(frozen=True)
class WorkItem:
    ids: list[int]


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
                row_count = 0
                while True:
                    batch = cur.fetchmany(10_000)
                    if not batch:
                        break
                    row_count += len(batch)

                return (row_count, None)
    except Exception as exc:
        if work.ids:
            return (
                0,
                f"Worker ids[{len(work.ids)}] {work.ids[0]}..{work.ids[-1]}: {exc!s}",
            )
        return (0, f"Worker ids[0]: {exc!s}")


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
    ) -> None:
        self.processes = processes or max(mp.cpu_count() - 1, 1)
        self.chunk_size = chunk_size
        self._dsn_override = dsn_override

    def _make_work_items(self, ids: Sequence[int]) -> list[WorkItem]:
        work: list[WorkItem] = []
        for start in range(0, len(ids), self.chunk_size):
            chunk_ids = list(ids[start : start + self.chunk_size])
            if chunk_ids:
                work.append(WorkItem(ids=chunk_ids))
        return work

    def execute(self, limit: int) -> StrategyResult:
        """
        Execute multiprocessing with timeout and error capture.

        Note: Duration is measured by orchestrator profiler to include
        process spawn overhead, ID selection, and result aggregation.
        """
        if self._dsn_override:
            dsn = self._dsn_override
        else:
            dsn = build_dsn()

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
            )

        work_items: Sequence[WorkItem] = self._make_work_items(ids)

        rows_read = 0
        errors: list[str] = []
        worker = partial(_fetch_ids, dsn, timeout_ms)

        ctx = mp.get_context("spawn")
        with ctx.Pool(processes=self.processes) as pool:
            try:
                # Add timeout to prevent hanging (10 minutes)
                results = pool.map_async(worker, work_items).get(timeout=600)

                for count, error in results:
                    if error:
                        errors.append(error)
                    else:
                        rows_read += count

            except mp.TimeoutError:
                pool.terminate()
                pool.join()
                errors.append("Workers timed out after 10 minutes")

        notes = f"ProcessPool size={self.processes}, chunk_size={self.chunk_size}, ids={len(ids)}"
        if errors:
            notes += f", errors={len(errors)}"

        return StrategyResult(
            rows=rows_read,
            peak_rss_bytes=None,  # Cannot measure worker memory from main process
            notes=notes,
            error="; ".join(errors[:3]) if errors else None,
        )


__all__ = ["MultiprocessingStrategy"]
