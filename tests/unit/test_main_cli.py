from __future__ import annotations

from typing import Any

import pytest
from typer.testing import CliRunner

from src.main import app
from src.orchestrator import BenchmarkInterrupted

EXIT_CODE_OK = 0
EXIT_CODE_ERROR = 1
EXIT_CODE_INTERRUPTED = 130
ROWS_COUNT = 100


def test_main_cli_run_success(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = CliRunner()
    ok_result: dict[str, Any] = {
        "strategy": "naive",
        "rows": ROWS_COUNT,
        "status": "ok",
        "duration_seconds": 0.1,
        "throughput_rows_per_sec": 1000.0,
        "extra": {"error_count": 0},
    }
    monkeypatch.setattr("src.main.run_strategies", lambda config: [ok_result])
    monkeypatch.setattr("src.main.print_results", lambda results: None)
    monkeypatch.setattr("src.main.configure_logging", lambda level=None: None)

    result = runner.invoke(app, ["run", "--strategy", "naive"])
    assert result.exit_code == EXIT_CODE_OK


def test_main_cli_run_partial_exit_code_1(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = CliRunner()
    partial_result: dict[str, Any] = {
        "strategy": "naive",
        "rows": ROWS_COUNT,
        "status": "partial",
        "duration_seconds": 0.1,
        "throughput_rows_per_sec": 1000.0,
        "error": "partial failure",
        "extra": {"failed": True, "error_count": 1},
    }
    monkeypatch.setattr("src.main.run_strategies", lambda config: [partial_result])
    monkeypatch.setattr("src.main.print_results", lambda results: None)
    monkeypatch.setattr("src.main.configure_logging", lambda level=None: None)

    result = runner.invoke(app, ["run", "--strategy", "naive"])
    assert result.exit_code == EXIT_CODE_ERROR


def test_main_cli_run_interrupted_exit_code_130(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = CliRunner()
    interrupted_result: dict[str, Any] = {
        "strategy": "naive",
        "rows": 0,
        "status": "failed",
        "duration_seconds": 0.1,
        "throughput_rows_per_sec": 0.0,
        "error": "Interrupted by user (KeyboardInterrupt)",
        "extra": {"interrupted": True, "failed": True, "error_count": 1},
    }
    payload: dict[str, Any] = {"summary": {"interrupted": True}}

    def raise_interrupted(config: Any) -> list[dict[str, Any]]:
        del config
        raise BenchmarkInterrupted([interrupted_result], payload)

    monkeypatch.setattr("src.main.run_strategies", raise_interrupted)
    monkeypatch.setattr("src.main.print_results", lambda results: None)
    monkeypatch.setattr("src.main.configure_logging", lambda level=None: None)

    result = runner.invoke(app, ["run", "--strategy", "naive"])
    assert result.exit_code == EXIT_CODE_INTERRUPTED
    assert "Partial artifact written to results/latest.json" in result.output
