from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import Any, ClassVar, cast

import pytest

from src import orchestrator
from src.orchestrator import RunConfig, run_strategies
from src.strategies.pooled_sync import PooledSyncStrategy

DEFAULT_LIMIT = 7
WARMUP_RUN_COUNT = 1
MEASUREMENT_RUN_COUNT = 3
FAILING_MEASUREMENT_RUN_COUNT = 2
POOLED_MEASUREMENT_RUN_COUNT = 2


class _LifecycleProbeStrategy:
    name = "lifecycle_probe"
    description = "test strategy with explicit close lifecycle"

    def __init__(self) -> None:
        self.resource_open = True
        self.close_calls = 0

    def execute(self, limit: int) -> dict[str, Any]:
        return {
            "rows": limit,
            "duration_seconds": 0.001,
            "throughput_rows_per_sec": float(limit),
        }

    def close(self) -> None:
        self.close_calls += 1
        self.resource_open = False


class _FailingLifecycleProbeStrategy(_LifecycleProbeStrategy):
    name = "failing_lifecycle_probe"

    def execute(self, limit: int) -> dict[str, Any]:
        del limit
        raise RuntimeError("intentional failure")


class _FakePool:
    def __init__(self) -> None:
        self.close_calls = 0

    def close(self) -> None:
        self.close_calls += 1


class _FakePoolConnectionContext(AbstractContextManager[object]):
    def __init__(self, pool: _FakeConnectionPool) -> None:
        self._pool = pool

    def __enter__(self) -> object:
        if self._pool.closed:
            raise RuntimeError("pool is already closed")
        return object()

    def __exit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb


class _FakeConnectionPool:
    instances: ClassVar[list[_FakeConnectionPool]] = []

    def __init__(
        self,
        conninfo: str,
        min_size: int,
        max_size: int,
        open: bool,
    ) -> None:
        self.conninfo = conninfo
        self.min_size = min_size
        self.max_size = max_size
        self.open = open
        self.closed = False
        _FakeConnectionPool.instances.append(self)

    def connection(self) -> _FakePoolConnectionContext:
        return _FakePoolConnectionContext(self)

    def close(self) -> None:
        self.closed = True


class _PooledSyncProbeStrategy(PooledSyncStrategy):
    name = "pooled_sync"

    def __init__(self) -> None:
        super().__init__(
            batch_size=1,
            pool_min_size=1,
            pool_max_size=1,
            dsn_override="postgresql://test",
        )
        self.fake_pool = _FakePool()
        self._pool_instance = cast(Any, self.fake_pool)

    def execute(self, limit: int) -> dict[str, Any]:
        return {
            "rows": limit,
            "duration_seconds": 0.001,
            "throughput_rows_per_sec": float(limit),
        }


class _PooledSyncLifecycleStrategy(PooledSyncStrategy):
    name = "pooled_sync"

    def execute(self, limit: int) -> dict[str, Any]:
        with self._get_pool().connection():
            pass
        return {
            "rows": limit,
            "duration_seconds": 0.001,
            "throughput_rows_per_sec": float(limit),
        }


def test_run_strategies_closes_instances_for_warmup_and_repeated_runs(
    monkeypatch,
) -> None:
    created: list[_LifecycleProbeStrategy] = []

    def make_strategy() -> _LifecycleProbeStrategy:
        strategy = _LifecycleProbeStrategy()
        created.append(strategy)
        return strategy

    def fake_factories(
        concurrency: int | None = None,
    ) -> dict[str, Callable[[], _LifecycleProbeStrategy]]:
        del concurrency
        return {_LifecycleProbeStrategy.name: make_strategy}

    monkeypatch.setattr(orchestrator, "_strategy_factories", fake_factories)

    run_strategies(
        RunConfig(
            strategy_names=[_LifecycleProbeStrategy.name],
            limit=DEFAULT_LIMIT,
            persist=False,
            warmup=True,
            runs=MEASUREMENT_RUN_COUNT,
        )
    )

    assert len(created) == WARMUP_RUN_COUNT + MEASUREMENT_RUN_COUNT
    assert all(strategy.close_calls == 1 for strategy in created)
    assert all(strategy.resource_open is False for strategy in created)


def test_run_strategies_closes_instances_even_when_execute_fails(monkeypatch) -> None:
    created: list[_FailingLifecycleProbeStrategy] = []

    def make_strategy() -> _FailingLifecycleProbeStrategy:
        strategy = _FailingLifecycleProbeStrategy()
        created.append(strategy)
        return strategy

    def fake_factories(
        concurrency: int | None = None,
    ) -> dict[str, Callable[[], _FailingLifecycleProbeStrategy]]:
        del concurrency
        return {_FailingLifecycleProbeStrategy.name: make_strategy}

    monkeypatch.setattr(orchestrator, "_strategy_factories", fake_factories)

    results = run_strategies(
        RunConfig(
            strategy_names=[_FailingLifecycleProbeStrategy.name],
            limit=DEFAULT_LIMIT,
            persist=False,
            runs=FAILING_MEASUREMENT_RUN_COUNT,
        )
    )

    assert len(results) == 1
    assert len(created) == FAILING_MEASUREMENT_RUN_COUNT
    assert all(strategy.close_calls == 1 for strategy in created)
    assert all(strategy.resource_open is False for strategy in created)
    # runs > 1 returns aggregated payload with per-run details
    individual_runs = results[0]["individual_runs"]
    assert len(individual_runs) == FAILING_MEASUREMENT_RUN_COUNT
    for run in individual_runs:
        assert run["error"] == "intentional failure"
        assert run["rows"] == 0
        assert run["notes"] == "Execution failed in tolerant mode; run continued."
        assert run["extra"]["failed"] is True
        assert run["extra"]["error_type"] == "RuntimeError"
        assert run["extra"]["failure_policy"] == "tolerant"


def test_run_strategies_strict_failure_policy_fails_fast(monkeypatch) -> None:
    created: list[_FailingLifecycleProbeStrategy] = []

    def make_strategy() -> _FailingLifecycleProbeStrategy:
        strategy = _FailingLifecycleProbeStrategy()
        created.append(strategy)
        return strategy

    def fake_factories(
        concurrency: int | None = None,
    ) -> dict[str, Callable[[], _FailingLifecycleProbeStrategy]]:
        del concurrency
        return {_FailingLifecycleProbeStrategy.name: make_strategy}

    monkeypatch.setattr(orchestrator, "_strategy_factories", fake_factories)

    with pytest.raises(RuntimeError, match="intentional failure"):
        run_strategies(
            RunConfig(
                strategy_names=[_FailingLifecycleProbeStrategy.name],
                limit=DEFAULT_LIMIT,
                persist=False,
                runs=FAILING_MEASUREMENT_RUN_COUNT,
                failure_policy="strict",
            )
        )

    assert len(created) == 1
    assert created[0].close_calls == 1
    assert created[0].resource_open is False


def test_run_strategies_uses_pooled_sync_close_path(monkeypatch) -> None:
    created: list[_PooledSyncProbeStrategy] = []

    def make_strategy() -> _PooledSyncProbeStrategy:
        strategy = _PooledSyncProbeStrategy()
        created.append(strategy)
        return strategy

    def fake_factories(
        concurrency: int | None = None,
    ) -> dict[str, Callable[[], _PooledSyncProbeStrategy]]:
        del concurrency
        return {"pooled_sync": make_strategy}

    monkeypatch.setattr(orchestrator, "_strategy_factories", fake_factories)

    run_strategies(
        RunConfig(
            strategy_names=["pooled_sync"],
            limit=DEFAULT_LIMIT,
            persist=False,
            runs=POOLED_MEASUREMENT_RUN_COUNT,
        )
    )

    assert len(created) == POOLED_MEASUREMENT_RUN_COUNT
    for strategy in created:
        assert strategy.fake_pool.close_calls == 1
        assert strategy._pool_instance is None


def test_run_strategies_pooled_sync_does_not_reuse_closed_pool_between_runs(
    monkeypatch,
) -> None:
    _FakeConnectionPool.instances.clear()
    created: list[_PooledSyncLifecycleStrategy] = []

    def make_strategy() -> _PooledSyncLifecycleStrategy:
        strategy = _PooledSyncLifecycleStrategy(batch_size=1, pool_min_size=1, pool_max_size=2)
        created.append(strategy)
        return strategy

    def fake_factories(
        concurrency: int | None = None,
    ) -> dict[str, Callable[[], _PooledSyncLifecycleStrategy]]:
        del concurrency
        return {"pooled_sync": make_strategy}

    monkeypatch.setattr(orchestrator, "_strategy_factories", fake_factories)
    monkeypatch.setattr("src.strategies.pooled_sync.ConnectionPool", _FakeConnectionPool)
    monkeypatch.setattr("src.strategies.pooled_sync.build_dsn", lambda: "postgresql://test")

    results = run_strategies(
        RunConfig(
            strategy_names=["pooled_sync"],
            limit=DEFAULT_LIMIT,
            persist=False,
            runs=POOLED_MEASUREMENT_RUN_COUNT,
        )
    )

    assert len(results) == 1
    assert len(results[0]["individual_runs"]) == POOLED_MEASUREMENT_RUN_COUNT
    assert all(run["rows"] == DEFAULT_LIMIT for run in results[0]["individual_runs"])
    assert len(_FakeConnectionPool.instances) == POOLED_MEASUREMENT_RUN_COUNT
    assert _FakeConnectionPool.instances[0] is not _FakeConnectionPool.instances[1]
    assert all(pool.closed for pool in _FakeConnectionPool.instances)
    assert all(strategy._pool_instance is None for strategy in created)
