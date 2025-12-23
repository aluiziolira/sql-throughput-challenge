"""
Multiprocessing strategy stub for the SQL Throughput Challenge.

Intent:
- Demonstrate parallel processing of data chunks fetched from Postgres.
- This stub focuses on structure and type safety; wiring to orchestrator/metrics
  will come later.
- The strategy should split work into ranges/batches and fan out to worker
  processes that execute database reads independently.
"""
from __future__ import annotations

import multiprocessing as mp
import time
from dataclasses import dataclass
from functools import partial
from typing import List, Optional, Sequence

import psycopg

from src.config import get_settings
from src.strategies.abstract import BenchmarkStrategy, StrategyResult


@dataclass(frozen=True)
class WorkItem:
    start_id: int
    end_id: int


def _fetch_range(dsn: str, work: WorkItem) -> int:
    """
    Worker function: fetch rows within an ID range and return count.

    NOTE: Using a simple range-based partitioning; in production you might prefer
    OFFSET/LIMIT or keyset pagination depending on distribution and indexes.
    """
    sql = "SELECT id FROM public.records WHERE id BETWEEN %s AND %s;"
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (work.start_id, work.end_id))
            rows = cur.fetchall()
            return len(rows)


class MultiprocessingStrategy(BenchmarkStrategy):
    """
    Fan-out reads across multiple processes by ID ranges.

    This stub does not yet collect RSS/CPU metrics or handle orchestration of
    progress; it simply returns counts and timing.
    """

    name: str = "multiprocessing"
    description: str = "ProcessPool with range partitioning over primary key."

    def __init__(
        self,
        processes: Optional[int] = None,
        chunk_size: int = 50_000,
        dsn_override: Optional[str] = None,
    ) -> None:
        self.processes = processes or max(mp.cpu_count() - 1, 1)
        self.chunk_size = chunk_size
        self._dsn_override = dsn_override

    def _make_work_items(self, total_rows: int) -> List[WorkItem]:
        work: List[WorkItem] = []
        start = 1
        while start <= total_rows:
            end = min(start + self.chunk_size - 1, total_rows)
            work.append(WorkItem(start_id=start, end_id=end))
            start = end + 1
        return work

    def execute(self, limit: int) -> StrategyResult:
        """
        Execute the multiprocessing fan-out and return basic metrics.
        """
        # Build DSN once; workers get a string to connect independently.
        if self._dsn_override:
            dsn = self._dsn_override
        else:
            settings = get_settings()
            dsn = (
                f"postgresql://{settings.db_user}:{settings.db_password}"
                f"@{settings.db_host}:{settings.db_port}/{settings.db_name}"
            )

        work_items: Sequence[WorkItem] = self._make_work_items(limit)
        start_ts = time.perf_counter()

        rows_read = 0
        worker = partial(_fetch_range, dsn)
        with mp.Pool(processes=self.processes) as pool:
            for count in pool.imap_unordered(worker, work_items):
                rows_read += count

        duration = time.perf_counter() - start_ts
        throughput = rows_read / duration if duration > 0 else 0.0

        return StrategyResult(
            rows=rows_read,
            duration_seconds=duration,
            throughput_rows_per_sec=throughput,
            peak_rss_bytes=None,  # Not measured yet
            notes=f"ProcessPool size={self.processes}, chunk_size={self.chunk_size}",
        )


__all__ = ["MultiprocessingStrategy"]
