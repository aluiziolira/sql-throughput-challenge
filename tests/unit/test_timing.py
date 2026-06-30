from __future__ import annotations

import pytest

from src.utils import timing

ROWS_RETURNED = 100
ELAPSED_SECONDS = 2.0


def test_run_timed_returns_rows_and_computes_throughput(monkeypatch) -> None:
    ticks = iter([0.0, ELAPSED_SECONDS])
    monkeypatch.setattr(timing.time, "perf_counter", lambda: next(ticks))

    rows, duration_seconds, throughput = timing.run_timed(lambda: ROWS_RETURNED)

    assert rows == ROWS_RETURNED
    assert duration_seconds == ELAPSED_SECONDS
    assert throughput == ROWS_RETURNED / ELAPSED_SECONDS


def test_run_timed_zero_duration_yields_zero_throughput(monkeypatch) -> None:
    monkeypatch.setattr(timing.time, "perf_counter", lambda: 5.0)

    rows, duration_seconds, throughput = timing.run_timed(lambda: ROWS_RETURNED)

    assert rows == ROWS_RETURNED
    assert duration_seconds == 0.0
    assert throughput == 0.0


def test_run_timed_propagates_fn_exceptions() -> None:
    def boom() -> int:
        raise ValueError("execution failed")

    with pytest.raises(ValueError, match="execution failed"):
        timing.run_timed(boom)
