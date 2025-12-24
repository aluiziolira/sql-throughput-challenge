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
from typing import Callable, Dict, Iterable, List, Optional

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


def _round_float(value: float, decimals: int = 2) -> float:
    """Round a float to specified decimal places for human-readable output."""
    return round(value, decimals)


def _round_stats(stats: dict, decimals: int = 2) -> dict:
    """Round all float values in a stats dictionary."""
    return {k: _round_float(v, decimals) if isinstance(v, float) else v for k, v in stats.items()}


def _aggregate_runs(run_results: List[dict]) -> dict:
    """
    Aggregate multiple runs into statistical summary.

    Returns median, mean, and stddev for key metrics.
    All float values are rounded to 2 decimal places for readability.
    """
    import statistics

    durations = [r["duration_seconds"] for r in run_results]
    throughputs = [r["throughput_rows_per_sec"] for r in run_results]

    # Aggregate CPU and memory metrics
    cpu_percents = [r.get("cpu_percent", 0.0) for r in run_results if r.get("cpu_percent")]
    peak_rss_values = [r.get("peak_rss_bytes", 0) for r in run_results if r.get("peak_rss_bytes")]

    aggregated = {
        "duration_seconds": _round_stats(
            {
                "median": statistics.median(durations),
                "mean": statistics.mean(durations),
                "stddev": statistics.stdev(durations) if len(durations) > 1 else 0.0,
                "min": min(durations),
                "max": max(durations),
            }
        ),
        "throughput_rows_per_sec": _round_stats(
            {
                "median": statistics.median(throughputs),
                "mean": statistics.mean(throughputs),
                "stddev": statistics.stdev(throughputs) if len(throughputs) > 1 else 0.0,
                "min": min(throughputs),
                "max": max(throughputs),
            }
        ),
        "rows": run_results[0]["rows"],  # Should be same across all runs
    }

    # Add CPU statistics if available
    if cpu_percents:
        aggregated["cpu_percent"] = _round_stats(
            {
                "median": statistics.median(cpu_percents),
                "mean": statistics.mean(cpu_percents),
                "stddev": statistics.stdev(cpu_percents) if len(cpu_percents) > 1 else 0.0,
                "min": min(cpu_percents),
                "max": max(cpu_percents),
            },
            decimals=1,
        )

    # Add memory statistics if available (integers, no rounding needed)
    if peak_rss_values:
        aggregated["peak_rss_bytes"] = {
            "median": int(statistics.median(peak_rss_values)),
            "mean": int(statistics.mean(peak_rss_values)),
            "stddev": int(statistics.stdev(peak_rss_values)) if len(peak_rss_values) > 1 else 0,
            "min": min(peak_rss_values),
            "max": max(peak_rss_values),
        }

    return aggregated


def _strategy_factories() -> Dict[str, Callable[[], BenchmarkStrategy]]:
    """Registry of available strategies."""
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
    log.info(f"[STRATEGY START] {strategy.name}", extra={"strategy": strategy.name})
    with profile_block(strategy.name) as stats:
        try:
            result = strategy.execute(limit)
            log.info(
                f"[STRATEGY SUCCESS] {strategy.name}",
                extra={"strategy": strategy.name, "rows": result["rows"]},
            )
        except Exception as exc:  # noqa: BLE001 - intentional broad catch to record failures
            log.exception(f"[STRATEGY FAILED] {strategy.name}", extra={"strategy": strategy.name})
            result = StrategyResult(error=str(exc), rows=0, duration_seconds=0.0)

    return _merge_result(result, stats)


def _merge_result(result: StrategyResult, stats: ProfileStats) -> dict:
    """Merge strategy result with profiler stats, rounding floats for readability."""
    merged = dict(result)
    merged.setdefault("rows", 0)
    merged.setdefault("duration_seconds", _round_float(stats.duration_seconds))
    merged.setdefault(
        "throughput_rows_per_sec",
        _round_float(merged["rows"] / merged["duration_seconds"])
        if merged["duration_seconds"]
        else 0.0,
    )
    # Round existing float values from strategy
    if "duration_seconds" in merged:
        merged["duration_seconds"] = _round_float(merged["duration_seconds"])
    if "throughput_rows_per_sec" in merged:
        merged["throughput_rows_per_sec"] = _round_float(merged["throughput_rows_per_sec"])
    # Override None values from strategies with profiler stats
    if merged.get("peak_rss_bytes") is None:
        merged["peak_rss_bytes"] = stats.peak_rss_bytes
    if merged.get("cpu_percent") is None:
        merged["cpu_percent"] = _round_float(stats.cpu_percent, 1) if stats.cpu_percent else None
    elif merged.get("cpu_percent") is not None:
        merged["cpu_percent"] = _round_float(merged["cpu_percent"], 1)
    merged["profile"] = {
        "label": stats.label,
        "start_ts": _round_float(stats.start_ts, 3),
        "end_ts": _round_float(stats.end_ts, 3),
        "duration_seconds": _round_float(stats.duration_seconds),
        "peak_rss_bytes": stats.peak_rss_bytes,
        "cpu_percent": _round_float(stats.cpu_percent, 1) if stats.cpu_percent else None,
    }
    return merged


def run_strategies(
    strategy_names: Optional[Iterable[str]] = None,
    limit: Optional[int] = None,
    results_dir: Path | str = "results",
    persist: bool = True,
    warmup: bool = False,
    runs: int = 1,
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
    warmup : bool
        Whether to run each strategy once before measurement to warm caches.
    runs : int
        Number of measurement runs per strategy (for statistical aggregation).

    Returns
    -------
    List[dict]
        List of per-strategy result dictionaries including profiler stats.
        If runs > 1, includes aggregated statistics (median, mean, stddev).
    """
    settings = get_settings()
    effective_limit = limit or settings.benchmark_rows

    names = list(strategy_names) if strategy_names is not None else ["all"]
    if len(names) == 1 and names[0] == "all":
        names = available_strategies()

    # Calculate total number of runs across all strategies
    total_global_runs = len(names) * runs
    current_run = 0

    results: List[dict] = []
    for name in names:
        log.info(f"{'=' * 60}")
        log.info(f"[STRATEGY] {name.upper()}", extra={"strategy": name})
        log.info(f"{'=' * 60}")

        # Warmup run to prime caches
        if warmup:
            log.info(f"[WARMUP] Starting warmup run for {name}", extra={"strategy": name})
            strategy = _resolve_strategy(name)
            try:
                strategy.execute(effective_limit)
                log.info(f"[WARMUP] Completed warmup for {name}", extra={"strategy": name})
            except Exception as e:
                log.warning(
                    f"[WARMUP] Failed for {name}", extra={"strategy": name, "error": str(e)}
                )

        # Measurement runs
        run_results: List[dict] = []
        for run_num in range(1, runs + 1):
            current_run += 1
            log.info(
                f"[RUN {current_run}/{total_global_runs}] Starting measurement for {name}",
                extra={
                    "strategy": name,
                    "run": run_num,
                    "total_runs": runs,
                    "limit": effective_limit,
                    "global_run": current_run,
                    "total_global_runs": total_global_runs,
                },
            )
            strategy = _resolve_strategy(name)
            result = _profiled_execute(strategy, effective_limit)
            result["strategy"] = name
            result["limit"] = effective_limit
            result["run"] = run_num
            run_results.append(result)
            log.info(
                f"[RUN {current_run}/{total_global_runs}] Completed {name}",
                extra={
                    "strategy": name,
                    "run": run_num,
                    "rows": result.get("rows"),
                    "duration": result.get("duration_seconds"),
                    "throughput_rps": result.get("throughput_rows_per_sec"),
                    "global_run": current_run,
                    "total_global_runs": total_global_runs,
                },
            )

        # Aggregate if multiple runs
        if runs > 1:
            aggregated = _aggregate_runs(run_results)
            aggregated["strategy"] = name
            aggregated["limit"] = effective_limit
            aggregated["runs"] = runs
            aggregated["individual_runs"] = run_results
            results.append(aggregated)
            log.info(
                f"[AGGREGATION] Results for {name}",
                extra={
                    "strategy": name,
                    "runs": runs,
                    "median_duration": aggregated["duration_seconds"]["median"],
                    "median_throughput_rps": aggregated["throughput_rows_per_sec"]["median"],
                    "stddev_duration": aggregated["duration_seconds"]["stddev"],
                },
            )
        else:
            results.extend(run_results)

        log.info(f"[STRATEGY COMPLETE] {name.upper()}", extra={"strategy": name})

    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "limit": effective_limit,
        "strategies": names,
        "results": results,
    }

    if persist:
        _persist_results(payload, Path(results_dir))

    log.info(
        f"[ORCHESTRATOR COMPLETE] All {len(names)} strategy/strategies executed successfully",
        extra={"strategies": names, "total_strategies": len(names)},
    )

    return results


__all__ = [
    "available_strategies",
    "run_strategies",
]
