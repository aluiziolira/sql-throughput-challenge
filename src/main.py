from __future__ import annotations

import multiprocessing
import sys

import typer

from src.config import get_settings
from src.orchestrator import RunConfig, available_strategies, run_strategies
from src.reporter import print_results
from src.utils.logging import configure_logging

app = typer.Typer(help="SQL Throughput Challenge CLI.")


@app.command()
def info() -> None:
    """
    Show effective configuration values.
    """
    settings = get_settings()
    typer.echo(
        f"DB={settings.db_user}@{settings.db_host}:{settings.db_port}/{settings.db_name} | "
        f"rows={settings.benchmark_rows} batch={settings.benchmark_batch_size} "
        f"concurrency={settings.benchmark_concurrency}"
    )


@app.command()
def run(
    strategy: str = typer.Option(
        "all",
        "--strategy",
        "--strategies",
        "-s",
        help="Strategy to run (e.g., naive, cursor_pagination, pooled_sync, multiprocessing, async_stream, all).",
    ),
    rows: int | None = typer.Option(
        None,
        "--rows",
        "-r",
        help="Override number of rows to process (default from settings).",
    ),
    concurrency: int | None = typer.Option(
        None,
        "--concurrency",
        "-c",
        help="Concurrency level (processes for multiprocessing, cursors for async). Overrides BENCHMARK_CONCURRENCY.",
    ),
    warmup: bool = typer.Option(
        False,
        "--warmup",
        help="Run each strategy once before measurement to warm caches.",
    ),
    runs: int = typer.Option(
        1,
        "--runs",
        help="Number of measurement runs per strategy for statistical aggregation.",
    ),
) -> None:
    """
    Run one or all strategies via orchestrator and persist results.
    """
    settings = get_settings()
    configure_logging(level=settings.log_level)
    total_rows = rows or settings.benchmark_rows

    if strategy == "list":
        typer.echo("Available strategies: " + ", ".join(available_strategies()))
        return

    strategy_names = ["all"] if strategy == "all" else [strategy]

    # Display effective concurrency
    effective_concurrency = concurrency or settings.benchmark_concurrency
    typer.echo(
        f"Running strategy='{strategy}' for rows={total_rows} "
        f"(batch={settings.benchmark_batch_size}, concurrency={effective_concurrency}, "
        f"warmup={warmup}, runs={runs})."
    )

    results = run_strategies(
        RunConfig(
            strategy_names=strategy_names,
            limit=total_rows,
            persist=True,
            warmup=warmup,
            runs=runs,
            concurrency=concurrency,
        )
    )
    print_results(results)


def main() -> None:
    """CLI entry point with multiprocessing support."""
    try:
        app()
    except KeyboardInterrupt:
        typer.echo("Cancelled by user.", err=True)
        sys.exit(130)


if __name__ == "__main__":
    # Required for multiprocessing on Windows (spawn start method)
    # Without this guard, child processes will re-execute main()
    multiprocessing.freeze_support()
    main()
