from __future__ import annotations

import time
from typing import Optional

import psycopg
from psycopg_pool import ConnectionPool

from src.config import get_settings
from src.infrastructure.db_factory import get_sync_pool
from src.strategies.abstract import BenchmarkStrategy, StrategyResult


class PooledSyncStrategy(BenchmarkStrategy):
    """
    Batched fetch using a psycopg ConnectionPool and fetchmany batching.

    Designed to compare the impact of pooling vs. single-connection approaches.
    """

    name: str = "pooled_sync"
    description: str = "Sync psycopg pool + fetchmany batching."

    def __init__(
        self,
        batch_size: Optional[int] = None,
        pool_min_size: int = 1,
        pool_max_size: int = 10,
        dsn_override: Optional[str] = None,
    ) -> None:
        settings = get_settings()
        self.batch_size = batch_size or settings.benchmark_batch_size
        self.pool_min_size = pool_min_size
        self.pool_max_size = pool_max_size
        self._dsn_override = dsn_override
        self._pool: ConnectionPool | None = None

    def _pool(self) -> ConnectionPool:
        if self._pool is not None:
            return self._pool
        if self._dsn_override:
            self._pool = ConnectionPool(
                conninfo=self._dsn_override,
                min_size=self.pool_min_size,
                max_size=self.pool_max_size,
            )
        else:
            self._pool = get_sync_pool(
                min_size=self.pool_min_size, max_size=self.pool_max_size
            )
        return self._pool

    def execute(self, limit: int) -> StrategyResult:
        sql = "SELECT * FROM public.records ORDER BY id LIMIT %s;"
        rows_fetched = 0
        start = time.perf_counter()

        pool = self._pool()
        with pool.connection() as conn:
            # Explicitly use named cursor to avoid client-side caching of full result.
            with conn.cursor(name="pooled_sync_cursor") as cur:
                cur.execute(sql, (limit,))
                while True:
                    batch = cur.fetchmany(self.batch_size)
                    if not batch:
                        break
                    rows_fetched += len(batch)

        duration = time.perf_counter() - start
        throughput = rows_fetched / duration if duration > 0 else 0.0

        return StrategyResult(
            rows=rows_fetched,
            duration_seconds=duration,
            throughput_rows_per_sec=throughput,
            peak_rss_bytes=None,
            notes=(
                f"pooled_sync batch_size={self.batch_size} "
                f"pool=({self.pool_min_size},{self.pool_max_size})"
            ),
        )


__all__ = ["PooledSyncStrategy"]
