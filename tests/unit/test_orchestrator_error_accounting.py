from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from src import orchestrator
from src.orchestrator import (
    RunConfig,
    annotate_result_status,
    classify_run_status,
    results_exit_code,
    run_strategies,
    summarize_results,
)

DEFAULT_LIMIT = 10
RETURNED_ERROR_COUNT = 2
PARTIAL_ROWS = 4
AGG_ERROR_COUNT = 2
AGG_MEDIAN_THROUGHPUT = 70.0
AGG_MEDIAN_ROWS = 90
SUMMARY_ERROR_COUNT = 3


class _ReturnedErrorStrategy:
    name = "returned_error"
    description = "returns worker errors without raising"

    def __init__(self, *, rows: int, error: str, error_count: int) -> None:
        self._rows = rows
        self._error = error
        self._error_count = error_count
        self.close_calls = 0

    def execute(self, limit: int) -> dict[str, Any]:
        del limit
        return {
            "rows": self._rows,
            "duration_seconds": 0.01,
            "throughput_rows_per_sec": float(self._rows),
            "error": self._error,
            "extra": {"error_count": self._error_count},
        }

    def close(self) -> None:
        self.close_calls += 1


class _OkStrategy:
    name = "ok_strategy"
    description = "always succeeds"

    def __init__(self) -> None:
        self.close_calls = 0

    def execute(self, limit: int) -> dict[str, Any]:
        return {
            "rows": limit,
            "duration_seconds": 0.01,
            "throughput_rows_per_sec": float(limit),
        }

    def close(self) -> None:
        self.close_calls += 1


def test_classify_run_status_for_ok_partial_and_failed() -> None:
    assert classify_run_status({"rows": 5}) == "ok"
    assert classify_run_status({"rows": 5, "error": "chunk failed"}) == "partial"
    assert classify_run_status({"rows": 0, "error": "all failed"}) == "failed"
    assert classify_run_status({"rows": 0, "error": "   "}) == "ok"


def test_annotate_result_status_surfaces_returned_errors() -> None:
    annotated = annotate_result_status(
        {
            "rows": 3,
            "error": "worker boom",
            "extra": {"error_count": 2},
        }
    )
    assert annotated["status"] == "partial"
    assert annotated["extra"]["failed"] is True
    assert annotated["extra"]["error_count"] == RETURNED_ERROR_COUNT


def test_run_strategies_counts_returned_worker_errors_in_summary_and_exit_code(
    monkeypatch,
) -> None:
    created: list[_ReturnedErrorStrategy] = []

    def make_strategy() -> _ReturnedErrorStrategy:
        strategy = _ReturnedErrorStrategy(
            rows=0, error="worker failed", error_count=RETURNED_ERROR_COUNT
        )
        created.append(strategy)
        return strategy

    def fake_factories(
        concurrency: int | None = None,
    ) -> dict[str, Callable[[], _ReturnedErrorStrategy]]:
        del concurrency
        return {_ReturnedErrorStrategy.name: make_strategy}

    monkeypatch.setattr(orchestrator, "_strategy_factories", fake_factories)

    results = run_strategies(
        RunConfig(
            strategy_names=[_ReturnedErrorStrategy.name],
            limit=DEFAULT_LIMIT,
            persist=False,
            runs=1,
        )
    )

    assert len(results) == 1
    result = results[0]
    assert result["status"] == "failed"
    assert result["error"] == "worker failed"
    assert result["extra"]["failed"] is True
    assert result["extra"]["error_count"] == RETURNED_ERROR_COUNT
    assert results_exit_code(results) == 1
    assert created[0].close_calls == 1


def test_run_strategies_reports_partial_without_looking_like_clean_success(
    monkeypatch,
) -> None:
    def make_strategy() -> _ReturnedErrorStrategy:
        return _ReturnedErrorStrategy(
            rows=PARTIAL_ROWS, error="1 of 2 chunks failed", error_count=1
        )

    def fake_factories(
        concurrency: int | None = None,
    ) -> dict[str, Callable[[], _ReturnedErrorStrategy]]:
        del concurrency
        return {_ReturnedErrorStrategy.name: make_strategy}

    monkeypatch.setattr(orchestrator, "_strategy_factories", fake_factories)

    results = run_strategies(
        RunConfig(
            strategy_names=[_ReturnedErrorStrategy.name],
            limit=DEFAULT_LIMIT,
            persist=False,
            runs=1,
        )
    )

    assert results[0]["status"] == "partial"
    assert results[0]["rows"] == PARTIAL_ROWS
    assert results[0]["extra"]["error_count"] == 1
    assert results_exit_code(results) == 1


def test_aggregate_runs_marks_partial_and_excludes_failed_zeros_from_throughput() -> None:
    aggregated = orchestrator._aggregate_runs(
        [
            {
                "rows": 100,
                "duration_seconds": 1.0,
                "throughput_rows_per_sec": 100.0,
                "status": "ok",
            },
            {
                "rows": 0,
                "duration_seconds": 0.5,
                "throughput_rows_per_sec": 0.0,
                "error": "failed run",
                "extra": {"error_count": 1},
            },
            {
                "rows": 80,
                "duration_seconds": 2.0,
                "throughput_rows_per_sec": 40.0,
                "error": "partial run",
                "extra": {"error_count": 1},
            },
        ]
    )

    assert aggregated["status"] == "partial"
    assert aggregated["extra"]["error_count"] == AGG_ERROR_COUNT
    assert aggregated["extra"]["status_counts"]["ok"] == 1
    assert aggregated["extra"]["status_counts"]["failed"] == 1
    assert aggregated["extra"]["status_counts"]["partial"] == 1
    # Failed zero-throughput run excluded from median when measured runs exist.
    assert aggregated["throughput_rows_per_sec"]["median"] == AGG_MEDIAN_THROUGHPUT
    assert aggregated["rows"] == AGG_MEDIAN_ROWS


def test_summarize_results_and_exit_code() -> None:
    results = [
        annotate_result_status({"rows": 1}),
        annotate_result_status(
            {"rows": 2, "error": "x", "extra": {"error_count": SUMMARY_ERROR_COUNT}}
        ),
    ]
    summary = summarize_results(results)
    assert summary["status_counts"] == {"ok": 1, "partial": 1, "failed": 0}
    assert summary["error_count"] == SUMMARY_ERROR_COUNT
    assert summary["has_failures"] is True
    assert results_exit_code(results) == 1


def test_strict_policy_raises_on_returned_errors(monkeypatch) -> None:
    def make_strategy() -> _ReturnedErrorStrategy:
        return _ReturnedErrorStrategy(rows=0, error="returned failure", error_count=1)

    def fake_factories(
        concurrency: int | None = None,
    ) -> dict[str, Callable[[], _ReturnedErrorStrategy]]:
        del concurrency
        return {_ReturnedErrorStrategy.name: make_strategy}

    monkeypatch.setattr(orchestrator, "_strategy_factories", fake_factories)

    with pytest.raises(RuntimeError, match="returned failure"):
        run_strategies(
            RunConfig(
                strategy_names=[_ReturnedErrorStrategy.name],
                limit=DEFAULT_LIMIT,
                persist=False,
                runs=1,
                failure_policy="strict",
            )
        )


def test_mixed_strategies_summary_does_not_claim_clean_success(monkeypatch) -> None:
    def fake_factories(
        concurrency: int | None = None,
    ) -> dict[str, Callable[[], Any]]:
        del concurrency
        return {
            _OkStrategy.name: _OkStrategy,
            _ReturnedErrorStrategy.name: lambda: _ReturnedErrorStrategy(
                rows=0, error="boom", error_count=1
            ),
        }

    monkeypatch.setattr(orchestrator, "_strategy_factories", fake_factories)

    results = run_strategies(
        RunConfig(
            strategy_names=[_OkStrategy.name, _ReturnedErrorStrategy.name],
            limit=DEFAULT_LIMIT,
            persist=False,
            runs=1,
        )
    )

    assert results[0]["status"] == "ok"
    assert results[1]["status"] == "failed"
    assert results_exit_code(results) == 1
