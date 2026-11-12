from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

import pytest

from src import orchestrator
from src.orchestrator import _run_warmup

WARMUP_LIMIT = 10
WARMUP_ROWS = 0
WARMUP_ERRORS = 2


class _WarmupReturnedErrorStrategy:
    name = "warmup_error_strat"
    description = "returns error during execute"

    def __init__(self, *, status_error: str, error_count: int, rows: int = 0) -> None:
        self._error = status_error
        self._error_count = error_count
        self._rows = rows
        self.close_calls = 0

    def execute(self, limit: int) -> dict[str, Any]:
        del limit
        return {
            "rows": self._rows,
            "error": self._error,
            "extra": {"error_count": self._error_count},
        }

    def close(self) -> None:
        self.close_calls += 1


def test_run_warmup_logs_warning_on_returned_error(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    strat = _WarmupReturnedErrorStrategy(
        status_error="warmup failed",
        error_count=WARMUP_ERRORS,
        rows=WARMUP_ROWS,
    )

    def fake_factories(
        concurrency: int | None = None,
    ) -> dict[str, Callable[[], _WarmupReturnedErrorStrategy]]:
        del concurrency
        return {_WarmupReturnedErrorStrategy.name: lambda: strat}

    monkeypatch.setattr(orchestrator, "_strategy_factories", fake_factories)

    with caplog.at_level(logging.WARNING):
        _run_warmup(_WarmupReturnedErrorStrategy.name, WARMUP_LIMIT, None)

    # Check that warning was logged
    warning_records = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warning_records) == 1
    record = warning_records[0]
    assert "failed" in record.message
    assert "warmup failed" in record.message
    assert "errors=2" in record.message
    # Check extra attributes
    assert getattr(record, "status", None) == "failed"
    assert getattr(record, "error", None) == "warmup failed"
    assert getattr(record, "error_count", None) == WARMUP_ERRORS
    # Close called
    assert strat.close_calls == 1
