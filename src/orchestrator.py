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
import statistics
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, cast

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

FailurePolicy = Literal["tolerant", "strict"]


@dataclass
class RunConfig:
    """Configuration for running benchmark strategies."""

    strategy_names: Iterable[str] | None = None
    limit: int | None = None
    results_dir: Path | str = "results"
    persist: bool = True
    warmup: bool = False
    runs: int = 1
    concurrency: int | None = None
    failure_policy: FailurePolicy = "tolerant"


def _round_float(value: float, decimals: int = 2) -> float:
    """Round a float to specified decimal places for human-readable output."""
    return round(value, decimals)


def _round_stats(stats: dict[str, Any], decimals: int = 2) -> dict[str, Any]:
    """Round all float values in a stats dictionary."""
    return {k: _round_float(v, decimals) if isinstance(v, float) else v for k, v in stats.items()}


def _aggregate_runs(run_results: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Aggregate multiple runs into statistical summary.

    Returns median, mean, and stddev for key metrics.
    All float values are rounded to 2 decimal places for readability.
    """

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


def _strategy_factories(
    concurrency: int | None = None,
) -> dict[str, Callable[[], BenchmarkStrategy]]:
    """Registry of available strategies with optional concurrency override."""
    settings = get_settings()
    effective_concurrency = concurrency or settings.benchmark_concurrency

    return {
        "naive": lambda: NaiveStrategy(),
        "cursor_pagination": lambda: CursorPaginationStrategy(),
        "async_stream": lambda: AsyncStreamStrategy(concurrency=effective_concurrency),
        "multiprocessing": lambda: MultiprocessingStrategy(processes=effective_concurrency),
        "pooled_sync": lambda: PooledSyncStrategy(
            pool_min_size=max(1, effective_concurrency // 2),
            pool_max_size=effective_concurrency,
        ),
    }


def available_strategies() -> list[str]:
    """List available strategy names."""
    return sorted(_strategy_factories().keys())


def _resolve_strategy(name: str, concurrency: int | None = None) -> BenchmarkStrategy:
    factories = _strategy_factories(concurrency=concurrency)
    if name not in factories:
        raise ValueError(f"Unknown strategy '{name}'. Available: {', '.join(factories)}")
    return factories[name]()


def _persist_results(payload: dict[str, Any], results_dir: Path) -> None:
    results_dir.mkdir(parents=True, exist_ok=True)
    latest_path = results_dir / "latest.json"
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    archive_path = results_dir / f"run-{timestamp}.json"

    with latest_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
    with archive_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)

    log.info("Results persisted", extra={"latest": str(latest_path), "archive": str(archive_path)})


# Strategies that spawn child processes and need aggregate resource tracking
_MULTIPROCESS_STRATEGIES = frozenset({"multiprocessing"})


def _cleanup_strategy(strategy: BenchmarkStrategy) -> None:
    """Best-effort strategy cleanup hook for optional strategy-owned resources."""
    close_method = getattr(strategy, "close", None)
    if not callable(close_method):
        return

    try:
        close_method()
    except Exception:
        log.warning(
            "[STRATEGY CLEANUP FAILED] close() raised",
            extra={"strategy": strategy.name},
            exc_info=True,
        )


def _profiled_execute(
    strategy: BenchmarkStrategy,
    limit: int,
    failure_policy: FailurePolicy = "tolerant",
) -> dict[str, Any]:
    log.info("[STRATEGY START] %s", strategy.name, extra={"strategy": strategy.name})
    # Enable child process tracking for multiprocessing strategies
    track_children = strategy.name in _MULTIPROCESS_STRATEGIES
    with profile_block(strategy.name, track_children=track_children) as stats:
        try:
            result = strategy.execute(limit)
            log.info(
                "[STRATEGY SUCCESS] %s",
                strategy.name,
                extra={"strategy": strategy.name, "rows": result.get("rows")},
            )
        except Exception as exc:
            log.exception("[STRATEGY FAILED] %s", strategy.name, extra={"strategy": strategy.name})
            if failure_policy == "strict":
                raise
            failure_result: StrategyResult = {
                "error": str(exc),
                "rows": 0,
                "duration_seconds": 0.0,
                "notes": "Execution failed in tolerant mode; run continued.",
                "extra": {
                    "failed": True,
                    "error_type": exc.__class__.__name__,
                    "failure_policy": failure_policy,
                },
            }
            result = failure_result

    return _merge_result(result, stats)


def _merge_result(result: StrategyResult, stats: ProfileStats) -> dict[str, Any]:
    """Merge strategy result with profiler stats, rounding floats for readability."""
    merged = dict(result)
    merged.setdefault("rows", 0)
    duration_seconds = _round_float(stats.duration_seconds)
    merged["duration_seconds"] = duration_seconds
    merged["throughput_rows_per_sec"] = (
        _round_float(cast(int, merged["rows"]) / duration_seconds) if duration_seconds else 0.0
    )
    merged["peak_rss_bytes"] = stats.peak_rss_bytes
    merged["cpu_percent"] = _round_float(stats.cpu_percent, 1) if stats.cpu_percent else None
    merged["profile"] = {
        "label": stats.label,
        "start_ts": _round_float(stats.start_ts, 3),
        "end_ts": _round_float(stats.end_ts, 3),
        "duration_seconds": _round_float(stats.duration_seconds),
        "peak_rss_bytes": stats.peak_rss_bytes,
        "cpu_percent": _round_float(stats.cpu_percent, 1) if stats.cpu_percent else None,
    }
    return merged


def _resolve_strategy_names(strategy_names: Iterable[str] | None) -> list[str]:
    names = list(strategy_names) if strategy_names is not None else ["all"]
    if len(names) == 1 and names[0] == "all":
        return available_strategies()
    return names


def _log_strategy_header(name: str) -> None:
    log.info("=" * 60)
    log.info("[STRATEGY] %s", name.upper(), extra={"strategy": name})
    log.info("=" * 60)


def _run_warmup(name: str, limit: int, concurrency: int | None) -> None:
    log.info("[WARMUP] Starting warmup run for %s", name, extra={"strategy": name})
    strategy = _resolve_strategy(name, concurrency=concurrency)
    try:
        strategy.execute(limit)
        log.info("[WARMUP] Completed warmup for %s", name, extra={"strategy": name})
    except Exception as exc:
        log.warning(
            "[WARMUP] Failed for %s",
            name,
            extra={"strategy": name, "error": str(exc)},
        )
    finally:
        _cleanup_strategy(strategy)


def _run_measurement_runs(
    name: str,
    config: RunConfig,
    effective_limit: int,
    current_run: int,
    total_global_runs: int,
) -> tuple[list[dict[str, Any]], int]:
    run_results: list[dict[str, Any]] = []

    for run_num in range(1, config.runs + 1):
        current_run += 1
        log.info(
            "[RUN %s/%s] Starting measurement for %s",
            current_run,
            total_global_runs,
            name,
            extra={
                "strategy": name,
                "run": run_num,
                "total_runs": config.runs,
                "limit": effective_limit,
                "global_run": current_run,
                "total_global_runs": total_global_runs,
            },
        )

        strategy = _resolve_strategy(name, concurrency=config.concurrency)
        try:
            result = _profiled_execute(
                strategy,
                effective_limit,
                failure_policy=config.failure_policy,
            )
        finally:
            _cleanup_strategy(strategy)

        result["strategy"] = name
        result["limit"] = effective_limit
        result["run"] = run_num
        run_results.append(result)
        log.info(
            "[RUN %s/%s] Completed %s",
            current_run,
            total_global_runs,
            name,
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

    return run_results, current_run


def _combine_strategy_results(
    name: str,
    effective_limit: int,
    runs: int,
    run_results: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if runs == 1:
        return run_results

    aggregated = _aggregate_runs(run_results)
    aggregated["strategy"] = name
    aggregated["limit"] = effective_limit
    aggregated["runs"] = runs
    aggregated["individual_runs"] = run_results
    log.info(
        "[AGGREGATION] Results for %s",
        name,
        extra={
            "strategy": name,
            "runs": runs,
            "median_duration": aggregated["duration_seconds"]["median"],
            "median_throughput_rps": aggregated["throughput_rows_per_sec"]["median"],
            "stddev_duration": aggregated["duration_seconds"]["stddev"],
        },
    )
    return [aggregated]


def _build_payload(
    effective_limit: int,
    names: list[str],
    results: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "timestamp": datetime.now(UTC).isoformat(),
        "limit": effective_limit,
        "strategies": names,
        "results": results,
    }


def run_strategies(config: RunConfig) -> list[dict[str, Any]]:
    """
    Run one or more strategies and optionally persist the aggregated results.

    Parameters
    ----------
    config : RunConfig
        Configuration object containing strategy names, limit, results directory,
        persist flag, warmup flag, number of runs, and concurrency.

    Returns
    -------
    List[dict]
        List of per-strategy result dictionaries including profiler stats.
        If runs > 1, includes aggregated statistics (median, mean, stddev).
    """
    settings = get_settings()
    effective_limit = config.limit or settings.benchmark_rows

    names = _resolve_strategy_names(config.strategy_names)

    # Calculate total number of runs across all strategies
    total_global_runs = len(names) * config.runs
    current_run = 0

    results: list[dict[str, Any]] = []
    for name in names:
        _log_strategy_header(name)

        # Warmup run to prime caches
        if config.warmup:
            _run_warmup(name, effective_limit, config.concurrency)

        run_results, current_run = _run_measurement_runs(
            name,
            config,
            effective_limit,
            current_run,
            total_global_runs,
        )
        results.extend(_combine_strategy_results(name, effective_limit, config.runs, run_results))

        log.info("[STRATEGY COMPLETE] %s", name.upper(), extra={"strategy": name})

    payload = _build_payload(effective_limit, names, results)

    if config.persist:
        _persist_results(payload, Path(config.results_dir))

    log.info(
        "[ORCHESTRATOR COMPLETE] All %s strategy/strategies executed successfully",
        len(names),
        extra={"strategies": names, "total_strategies": len(names)},
    )

    return results


__all__ = [
    "RunConfig",
    "available_strategies",
    "run_strategies",
]
