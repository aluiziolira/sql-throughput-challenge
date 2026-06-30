from __future__ import annotations

from psycopg_pool import ConnectionPool

from src.config import get_settings
from src.infrastructure.db_factory import (
    SELECT_RECORDS_SQL,
    apply_statement_timeout,
    build_dsn,
    fetch_in_batches,
)
from src.strategies.abstract import BenchmarkStrategy, StrategyResult
from src.utils.timing import run_timed


class PooledSyncStrategy(BenchmarkStrategy):
    """
    Batched fetch using a psycopg ConnectionPool and fetchmany batching.

    Designed to compare the impact of pooling vs. single-connection approaches.
    """

    name: str = "pooled_sync"
    description: str = "Sync psycopg pool + fetchmany batching."

    def __init__(
        self,
        batch_size: int | None = None,
        pool_min_size: int = 1,
        pool_max_size: int = 10,
        dsn_override: str | None = None,
    ) -> None:
        settings = get_settings()
        self.batch_size = batch_size or settings.benchmark_batch_size
        self.pool_min_size = pool_min_size
        self.pool_max_size = pool_max_size
        self._dsn_override = dsn_override
        self._pool_instance: ConnectionPool | None = None

    def _get_pool(self) -> ConnectionPool:
        if self._pool_instance is not None:
            return self._pool_instance

        conninfo = self._dsn_override or build_dsn()
        self._pool_instance = ConnectionPool(
            conninfo=conninfo,
            min_size=self.pool_min_size,
            max_size=self.pool_max_size,
            open=True,
        )
        return self._pool_instance

    def close(self) -> None:
        """
        Close the connection pool if it exists.

        This method is idempotent and is invoked by the orchestrator as an
        optional lifecycle cleanup hook after each run.
        """
        if self._pool_instance is not None:
            self._pool_instance.close()
            self._pool_instance = None

    def _fetch_all(self, limit: int) -> int:
        timeout_ms = get_settings().db_statement_timeout_ms
        rows_fetched = 0
        pool = self._get_pool()
        with pool.connection() as conn:
            # Explicitly use named cursor to avoid client-side caching of full result.
            with conn.cursor(name="pooled_sync_cursor") as cur:
                apply_statement_timeout(cur, timeout_ms)
                cur.execute(SELECT_RECORDS_SQL, (limit,))
                for batch in fetch_in_batches(cur, self.batch_size):
                    rows_fetched += len(batch)
        return rows_fetched

    def execute(self, limit: int) -> StrategyResult:
        rows, duration_seconds, throughput_rows_per_sec = run_timed(lambda: self._fetch_all(limit))

        return StrategyResult(
            rows=rows,
            duration_seconds=duration_seconds,
            throughput_rows_per_sec=throughput_rows_per_sec,
            peak_rss_bytes=None,
            notes=(
                f"pooled_sync batch_size={self.batch_size} "
                f"pool=({self.pool_min_size},{self.pool_max_size})"
            ),
        )


__all__ = ["PooledSyncStrategy"]
