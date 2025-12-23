"""
Orchestrator for running benchmark strategies, profiling execution, and persisting results.

Usage (example from CLI):
    from src.orchestrator import run_strategies

    results = run_strategies(strategy_names=["naive", "cursor_pagination"], limit=100_000)
    print(results)

Outputs are saved to `results/` by default:
- `results/latest.json` (last run)
- `results/run-<timestamp>.json` (timestamped archive)
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from src.config import get_settings
from src.strategies.abstract import BenchmarkStrategy, StrategyResult
from src.strategies.async_stream import AsyncStreamStrategy
from src.strategies.cursor_pagination import CursorPaginationStrategy
from src.strategies.multiprocessing import MultiprocessingStrategy
from src.strategies.naive import NaiveStrategy
from src.strategies.pooled_sync import PooledSyncStrategy
from src.utils.logging import get_logger
from src.utils.profiler import ProfileStats, profile_block

log = get_logger(__name__)


def _strategy_factories() -> Dict[str, callable]:
    """
    Registry of available strategies mapped to factory callables.

    Add new strategies here (e.g., pooled sync) as they are implemented.
    """
    return {
        "naive": lambda: NaiveStrategy(),
        "cursor_pagination": lambda: CursorPaginationStrategy(),
        "async_stream": lambda: AsyncStreamStrategy(),
        "multiprocessing": lambda: MultiprocessingStrategy(),
        "pooled_sync": lambda: PooledSyncStrategy(),
    }


def available_strategies() -> List[str]:
    """List available strategy names."""
    return sorted(_strategy_factories().keys())


def _resolve_strategy(name: str) -> BenchmarkStrategy:
    factories = _strategy_factories()
    if name not in factories:
        raise ValueError(f"Unknown strategy '{name}'. Available: {', '.join(factories)}")
    return factories[name]()


def _persist_results(payload: dict, results_dir: Path) -> None:
    results_dir.mkdir(parents=True, exist_ok=True)
    latest_path = results_dir / "latest.json"
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    archive_path = results_dir / f"run-{timestamp}.json"

    with latest_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
    with archive_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)

    log.info("Results persisted", extra={"latest": str(latest_path), "archive": str(archive_path)})


def _profiled_execute(strategy: BenchmarkStrategy, limit: int) -> dict:
    with profile_block(strategy.name) as stats:
        try:
            result = strategy.execute(limit)
        except Exception as exc:  # noqa: BLE001 - intentional broad catch to record failures
            log.exception("Strategy failed", extra={"strategy": strategy.name})
            result = StrategyResult(error=str(exc), rows=0, duration_seconds=0.0)

    return _merge_result(result, stats)


def _merge_result(result: StrategyResult, stats: ProfileStats) -> dict:
    merged = dict(result)
    merged.setdefault("rows", 0)
    merged.setdefault("duration_seconds", stats.duration_seconds)
    merged.setdefault(
        "throughput_rows_per_sec",
        (merged["rows"] / merged["duration_seconds"]) if merged["duration_seconds"] else 0.0,
    )
    merged.setdefault("peak_rss_bytes", stats.peak_rss_bytes)
    merged.setdefault("cpu_percent", stats.cpu_percent)
    merged["profile"] = {
        "label": stats.label,
        "start_ts": stats.start_ts,
        "end_ts": stats.end_ts,
        "duration_seconds": stats.duration_seconds,
        "peak_rss_bytes": stats.peak_rss_bytes,
        "cpu_percent": stats.cpu_percent,
    }
    return merged


def run_strategies(
    strategy_names: Optional[Iterable[str]] = None,
    limit: Optional[int] = None,
    results_dir: Path | str = "results",
    persist: bool = True,
) -> List[dict]:
    """
    Run one or more strategies and optionally persist the aggregated results.

    Parameters
    ----------
    strategy_names : iterable[str] | None
        Strategy names to execute. If None or ["all"], executes all available.
    limit : int | None
        Row cap for each strategy. Defaults to settings.benchmark_rows.
    results_dir : Path | str
        Directory to store JSON artifacts.
    persist : bool
        Whether to write results to disk.

    Returns
    -------
    List[dict]
        List of per-strategy result dictionaries including profiler stats.
    """
    settings = get_settings()
    effective_limit = limit or settings.benchmark_rows

    names = list(strategy_names) if strategy_names is not None else ["all"]
    if len(names) == 1 and names[0] == "all":
        names = available_strategies()

    results: List[dict] = []
    for name in names:
        log.info("Running strategy", extra={"strategy": name, "limit": effective_limit})
        strategy = _resolve_strategy(name)
        result = _profiled_execute(strategy, effective_limit)
        result["strategy"] = name
        result["limit"] = effective_limit
        results.append(result)
        log.info(
            "Completed strategy",
            extra={
                "strategy": name,
                "rows": result.get("rows"),
                "duration": result.get("duration_seconds"),
                "throughput_rps": result.get("throughput_rows_per_sec"),
            },
        )

    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "limit": effective_limit,
        "strategies": names,
        "results": results,
    }

    if persist:
        _persist_results(payload, Path(results_dir))

    return results


__all__ = [
    "available_strategies",
    "run_strategies",
]
