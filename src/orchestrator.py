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
import time
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
RunStatus = Literal["ok", "partial", "failed"]


class BenchmarkInterrupted(KeyboardInterrupt):
    """Raised when a benchmark run is interrupted by user."""

    def __init__(self, results: list[dict[str, Any]], payload: dict[str, Any]) -> None:
        super().__init__("Benchmark run interrupted by user")
        self.results = results
        self.payload = payload


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


def _result_error_text(result: dict[str, Any]) -> str | None:
    """Return a non-empty error string from a result, if present."""
    error = result.get("error")
    if error is None:
        return None
    text = str(error).strip()
    return text or None


def _error_count(result: dict[str, Any]) -> int:
    """Count distinct failures recorded on a result."""
    extra = result.get("extra")
    if isinstance(extra, dict):
        raw = extra.get("error_count")
        if isinstance(raw, int) and raw >= 0:
            return raw
    return 1 if _result_error_text(result) else 0


def classify_run_status(result: dict[str, Any]) -> RunStatus:
    """
    Classify a single run from rows + returned/raised error state.

    - ok: no error
    - partial: error present with some rows read
    - failed: error present with zero rows read
    """
    has_error = _result_error_text(result) is not None or _error_count(result) > 0
    if not has_error:
        return "ok"
    rows = int(result.get("rows") or 0)
    if rows > 0:
        return "partial"
    return "failed"


def annotate_result_status(result: dict[str, Any]) -> dict[str, Any]:
    """Attach status + failure metadata for raised or returned worker errors."""
    annotated = dict(result)
    status = classify_run_status(annotated)
    annotated["status"] = status

    extra = dict(annotated.get("extra") or {})
    error_count = _error_count(annotated)
    if status == "ok":
        extra.pop("failed", None)
        extra.setdefault("error_count", 0)
    else:
        error_count = max(1, error_count)
        extra["failed"] = True
        extra["error_count"] = error_count
        if "failure_policy" not in extra:
            extra["failure_policy"] = "tolerant"
        if _result_error_text(annotated) is None:
            annotated["error"] = f"{error_count} worker error(s) reported without message"
    annotated["extra"] = extra
    return annotated


def summarize_results(
    results: list[dict[str, Any]],
    *,
    interrupted: bool = False,
    planned_runs: int | None = None,
    completed_runs: int | None = None,
) -> dict[str, Any]:
    """Build a top-level summary of ok/partial/failed runs and error counts."""
    status_counts = {"ok": 0, "partial": 0, "failed": 0}
    error_count = 0
    for result in results:
        status = cast(RunStatus, result.get("status") or classify_run_status(result))
        status_counts[status] = status_counts.get(status, 0) + 1
        count = _error_count(result)
        if status != "ok":
            count = max(1, count)
        error_count += count
    has_failures = status_counts["failed"] > 0 or status_counts["partial"] > 0 or interrupted
    if interrupted:
        # An interrupt with no in-flight measurement still counts as a failure.
        error_count = max(1, error_count)
    summary: dict[str, Any] = {
        "status_counts": status_counts,
        "error_count": error_count,
        "has_failures": has_failures,
        "interrupted": interrupted,
    }
    if planned_runs is not None:
        summary["planned_runs"] = planned_runs
    if completed_runs is not None:
        summary["completed_runs"] = completed_runs
    return summary


def results_exit_code(results: list[dict[str, Any]]) -> int:
    """Non-zero when any run is partial or failed."""
    summary = summarize_results(results)
    return 1 if summary["has_failures"] else 0


def _aggregate_runs(run_results: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Aggregate multiple runs into statistical summary.

    Returns median, mean, and stddev for key metrics.
    All float values are rounded to 2 decimal places for readability.
    Headline stats are computed from 'ok' runs if any exist; else from
    'partial' runs; else from all runs ('failed').
    Status/error counts always include every run.
    """
    annotated_runs = [annotate_result_status(dict(r)) for r in run_results]
    ok_runs = [r for r in annotated_runs if r.get("status") == "ok"]
    partial_runs = [r for r in annotated_runs if r.get("status") == "partial"]

    if ok_runs:
        measured_runs = ok_runs
        measured_status: RunStatus = "ok"
    elif partial_runs:
        measured_runs = partial_runs
        measured_status = "partial"
    else:
        measured_runs = annotated_runs
        measured_status = "failed"

    durations = [r["duration_seconds"] for r in measured_runs]
    throughputs = [r["throughput_rows_per_sec"] for r in measured_runs]
    row_counts = [int(r.get("rows") or 0) for r in measured_runs]

    # Aggregate CPU and memory metrics
    cpu_percents = [r.get("cpu_percent", 0.0) for r in measured_runs if r.get("cpu_percent")]
    peak_rss_values = [r.get("peak_rss_bytes", 0) for r in measured_runs if r.get("peak_rss_bytes")]

    status_counts = {"ok": 0, "partial": 0, "failed": 0}
    total_errors = 0
    for run in annotated_runs:
        status = cast(RunStatus, run.get("status") or "ok")
        status_counts[status] = status_counts.get(status, 0) + 1
        count = _error_count(run)
        if status != "ok":
            count = max(1, count)
        total_errors += count

    if status_counts["failed"] and not status_counts["ok"] and not status_counts["partial"]:
        aggregate_status: RunStatus = "failed"
    elif status_counts["failed"] or status_counts["partial"]:
        aggregate_status = "partial"
    else:
        aggregate_status = "ok"

    aggregated: dict[str, Any] = {
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
        # Median avoids privileging a single run when partial failures diverge.
        "rows": int(statistics.median(row_counts)) if row_counts else 0,
        "status": aggregate_status,
        "extra": {
            "failed": aggregate_status != "ok",
            "error_count": total_errors,
            "status_counts": status_counts,
            "measured_status": measured_status,
            "measured_runs": len(measured_runs),
        },
    }
    if total_errors:
        error_messages = [
            _result_error_text(run) for run in annotated_runs if _result_error_text(run)
        ]
        aggregated["error"] = "; ".join(cast(list[str], error_messages)[:3])

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
        "naive": NaiveStrategy,
        "cursor_pagination": CursorPaginationStrategy,
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
        result: dict[str, Any]
        try:
            raw_result = strategy.execute(limit)
            annotated = annotate_result_status(dict(raw_result))
            if annotated["status"] != "ok" and failure_policy == "strict":
                raise RuntimeError(
                    _result_error_text(annotated) or f"{strategy.name} reported failure"
                )
            if annotated["status"] == "ok":
                log.info(
                    "[STRATEGY SUCCESS] %s",
                    strategy.name,
                    extra={"strategy": strategy.name, "rows": annotated.get("rows")},
                )
            else:
                log.error(
                    "[STRATEGY %s] %s",
                    annotated["status"].upper(),
                    strategy.name,
                    extra={
                        "strategy": strategy.name,
                        "rows": annotated.get("rows"),
                        "error": annotated.get("error"),
                        "error_count": _error_count(annotated),
                        "status": annotated["status"],
                    },
                )
            result = annotated
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
                    "error_count": 1,
                },
            }
            result = annotate_result_status(dict(failure_result))

    return _merge_result(result, stats)


def _merge_result(result: dict[str, Any], stats: ProfileStats | Any) -> dict[str, Any]:
    """Merge strategy result with profiler stats, rounding floats for readability."""
    merged = dict(result)
    merged.setdefault("rows", 0)
    raw_duration = float(getattr(stats, "duration_seconds", 0.0))
    duration_seconds = _round_float(raw_duration)
    merged["duration_seconds"] = duration_seconds
    merged["throughput_rows_per_sec"] = (
        _round_float(cast(int, merged["rows"]) / raw_duration) if raw_duration else 0.0
    )
    peak_rss = getattr(stats, "peak_rss_bytes", None)
    cpu_percent = getattr(stats, "cpu_percent", None)
    label = getattr(stats, "label", "")
    start_ts = getattr(stats, "start_ts", 0.0)
    end_ts = getattr(stats, "end_ts", 0.0)

    merged["peak_rss_bytes"] = peak_rss
    merged["cpu_percent"] = (
        _round_float(cpu_percent, 1) if isinstance(cpu_percent, (int, float)) else None
    )
    merged["profile"] = {
        "label": label,
        "start_ts": _round_float(start_ts, 3) if isinstance(start_ts, (int, float)) else 0.0,
        "end_ts": _round_float(end_ts, 3) if isinstance(end_ts, (int, float)) else 0.0,
        "duration_seconds": duration_seconds,
        "peak_rss_bytes": peak_rss,
        "cpu_percent": (
            _round_float(cpu_percent, 1) if isinstance(cpu_percent, (int, float)) else None
        ),
    }
    return annotate_result_status(merged)


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
        raw_result = strategy.execute(limit)
        annotated = annotate_result_status(dict(raw_result))
        if annotated["status"] != "ok":
            log.warning(
                "[WARMUP] %s reported %s: %s (errors=%s)",
                name,
                annotated["status"],
                annotated.get("error"),
                _error_count(annotated),
                extra={
                    "strategy": name,
                    "status": annotated["status"],
                    "error": annotated.get("error"),
                    "error_count": _error_count(annotated),
                },
            )
        else:
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
) -> tuple[list[dict[str, Any]], int, int, bool]:
    run_results: list[dict[str, Any]] = []
    completed_runs = 0
    interrupted = False

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
        run_start = time.perf_counter()
        try:
            result = _profiled_execute(
                strategy,
                effective_limit,
                failure_policy=config.failure_policy,
            )
        except KeyboardInterrupt:
            elapsed = time.perf_counter() - run_start
            interrupted_result: dict[str, Any] = {
                "strategy": name,
                "limit": effective_limit,
                "run": run_num,
                "rows": 0,
                "error": "Interrupted by user (KeyboardInterrupt)",
                "duration_seconds": _round_float(elapsed),
                "throughput_rows_per_sec": 0.0,
                "extra": {
                    "interrupted": True,
                    "error_count": 1,
                    "failed": True,
                },
            }
            annotated = annotate_result_status(interrupted_result)
            run_results.append(annotated)
            interrupted = True
            break
        finally:
            _cleanup_strategy(strategy)

        result["strategy"] = name
        result["limit"] = effective_limit
        result["run"] = run_num
        run_results.append(result)
        completed_runs += 1
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
                "status": result.get("status"),
                "error_count": _error_count(result),
                "global_run": current_run,
                "total_global_runs": total_global_runs,
            },
        )

    return run_results, current_run, completed_runs, interrupted


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
            "status": aggregated.get("status"),
            "error_count": _error_count(aggregated),
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
    summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    effective_summary = summary if summary is not None else summarize_results(results)
    return {
        "timestamp": datetime.now(UTC).isoformat(),
        "limit": effective_limit,
        "strategies": names,
        "summary": effective_summary,
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
    total_completed_runs = 0
    interrupted = False

    results: list[dict[str, Any]] = []
    for name in names:
        _log_strategy_header(name)

        # Warmup run to prime caches. An interrupt here must not discard
        # results already collected for earlier strategies.
        if config.warmup:
            try:
                _run_warmup(name, effective_limit, config.concurrency)
            except KeyboardInterrupt:
                interrupted = True
                break

        run_results, current_run, strat_completed, strat_interrupted = _run_measurement_runs(
            name,
            config,
            effective_limit,
            current_run,
            total_global_runs,
        )
        total_completed_runs += strat_completed
        results.extend(_combine_strategy_results(name, effective_limit, config.runs, run_results))

        if strat_interrupted:
            interrupted = True
            break

        log.info("[STRATEGY COMPLETE] %s", name.upper(), extra={"strategy": name})

    summary = summarize_results(
        results,
        interrupted=interrupted,
        planned_runs=total_global_runs,
        completed_runs=total_completed_runs,
    )
    payload = _build_payload(effective_limit, names, results, summary=summary)

    if config.persist:
        _persist_results(payload, Path(config.results_dir))

    summary = payload["summary"]
    log.info(
        "[ORCHESTRATOR COMPLETE] strategies=%s ok=%s partial=%s failed=%s errors=%s",
        len(names),
        summary["status_counts"]["ok"],
        summary["status_counts"]["partial"],
        summary["status_counts"]["failed"],
        summary["error_count"],
        extra={
            "strategies": names,
            "total_strategies": len(names),
            "summary": summary,
        },
    )

    if interrupted:
        raise BenchmarkInterrupted(results, payload)

    return results


__all__ = [
    "BenchmarkInterrupted",
    "RunConfig",
    "annotate_result_status",
    "available_strategies",
    "classify_run_status",
    "results_exit_code",
    "run_strategies",
    "summarize_results",
]
