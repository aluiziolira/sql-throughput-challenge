from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from src import orchestrator
from src.orchestrator import BenchmarkInterrupted, RunConfig, run_strategies

BENCHMARK_LIMIT = 50
PLANNED_RUNS_COUNT = 3
COMPLETED_RUNS_COUNT = 1
FIRST_RUN_NUM = 1
SECOND_RUN_NUM = 2
CLOSE_CALLS_EXPECTED = 2
WARMUP_PLANNED_RUNS = 2
WARMUP_COMPLETED_RUNS = 1
WARMUP_ERROR_COUNT = 1


class _InterruptOnSecondRunStrategy:
    name = "interrupt_strategy"
    description = "raises KeyboardInterrupt on 2nd execute call"

    def __init__(self, state: dict[str, int]) -> None:
        self._state = state
        self.close_calls = 0

    def execute(self, limit: int) -> dict[str, Any]:
        self._state["call_count"] += 1
        if self._state["call_count"] == SECOND_RUN_NUM:
            raise KeyboardInterrupt()
        return {
            "rows": limit,
            "duration_seconds": 0.05,
            "throughput_rows_per_sec": float(limit) / 0.05,
        }

    def close(self) -> None:
        self.close_calls += 1


def test_benchmark_interrupted_persists_and_reports_interrupted_run(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    created: list[_InterruptOnSecondRunStrategy] = []
    state = {"call_count": 0}

    def make_strategy() -> _InterruptOnSecondRunStrategy:
        strat = _InterruptOnSecondRunStrategy(state)
        created.append(strat)
        return strat

    def fake_factories(
        concurrency: int | None = None,
    ) -> dict[str, Callable[[], _InterruptOnSecondRunStrategy]]:
        del concurrency
        return {_InterruptOnSecondRunStrategy.name: make_strategy}

    monkeypatch.setattr(orchestrator, "_strategy_factories", fake_factories)

    config = RunConfig(
        strategy_names=[_InterruptOnSecondRunStrategy.name],
        limit=BENCHMARK_LIMIT,
        results_dir=tmp_path,
        persist=True,
        runs=PLANNED_RUNS_COUNT,
    )

    with pytest.raises(BenchmarkInterrupted) as exc_info:
        run_strategies(config)

    exc = exc_info.value
    # Assert payload summary
    summary = exc.payload["summary"]
    assert summary["interrupted"] is True
    assert summary["planned_runs"] == PLANNED_RUNS_COUNT
    assert summary["completed_runs"] == COMPLETED_RUNS_COUNT
    assert summary["has_failures"] is True

    # Check results structure
    assert len(exc.results) == 1
    agg = exc.results[0]
    individual_runs = agg["individual_runs"]
    assert len(individual_runs) == SECOND_RUN_NUM

    run1 = individual_runs[0]
    assert run1["status"] == "ok"
    assert run1["run"] == FIRST_RUN_NUM

    interrupted_run = individual_runs[1]
    assert interrupted_run["status"] == "failed"
    assert interrupted_run["run"] == SECOND_RUN_NUM
    assert interrupted_run["rows"] == 0
    assert interrupted_run["error"] == "Interrupted by user (KeyboardInterrupt)"
    assert interrupted_run["extra"]["interrupted"] is True
    assert interrupted_run["extra"]["failed"] is True
    assert interrupted_run["extra"]["error_count"] == 1

    # Verify close() was called on both created strategy instances
    assert len(created) == SECOND_RUN_NUM
    total_close_calls = sum(s.close_calls for s in created)
    assert total_close_calls == CLOSE_CALLS_EXPECTED

    # Verify persisted latest.json exists and contains correct content
    latest_file = tmp_path / "latest.json"
    assert latest_file.exists()
    persisted_data = json.loads(latest_file.read_text(encoding="utf-8"))
    assert persisted_data["summary"]["interrupted"] is True
    assert persisted_data["summary"]["planned_runs"] == PLANNED_RUNS_COUNT
    assert persisted_data["summary"]["completed_runs"] == COMPLETED_RUNS_COUNT


class _OkStrategy:
    name = "ok_strategy"
    description = "returns rows without errors"

    def execute(self, limit: int) -> dict[str, Any]:
        return {
            "rows": limit,
            "duration_seconds": 0.05,
            "throughput_rows_per_sec": float(limit) / 0.05,
        }

    def close(self) -> None:
        pass


def test_warmup_interrupt_persists_prior_results(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    warmup_calls = {"count": 0}

    def fake_warmup(name: str, limit: int, concurrency: int | None) -> None:
        del name, limit, concurrency
        warmup_calls["count"] += 1
        if warmup_calls["count"] == SECOND_RUN_NUM:
            raise KeyboardInterrupt()

    def fake_factories(
        concurrency: int | None = None,
    ) -> dict[str, Callable[[], _OkStrategy]]:
        del concurrency
        return {"first": _OkStrategy, "second": _OkStrategy}

    monkeypatch.setattr(orchestrator, "_strategy_factories", fake_factories)
    monkeypatch.setattr(orchestrator, "_run_warmup", fake_warmup)

    config = RunConfig(
        strategy_names=["first", "second"],
        limit=BENCHMARK_LIMIT,
        results_dir=tmp_path,
        persist=True,
        warmup=True,
        runs=1,
    )

    with pytest.raises(BenchmarkInterrupted) as exc_info:
        run_strategies(config)

    summary = exc_info.value.payload["summary"]
    assert summary["interrupted"] is True
    assert summary["planned_runs"] == WARMUP_PLANNED_RUNS
    assert summary["completed_runs"] == WARMUP_COMPLETED_RUNS
    assert summary["has_failures"] is True
    assert summary["error_count"] == WARMUP_ERROR_COUNT
    assert len(exc_info.value.results) == WARMUP_COMPLETED_RUNS
    assert exc_info.value.results[0]["strategy"] == "first"
    assert exc_info.value.results[0]["status"] == "ok"
    assert (tmp_path / "latest.json").exists()
