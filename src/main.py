from __future__ import annotations

import json
import sys
from typing import Optional

import typer

from src.config import get_settings
from src.orchestrator import available_strategies, run_strategies
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
    rows: Optional[int] = typer.Option(
        None,
        "--rows",
        "-r",
        help="Override number of rows to process (default from settings).",
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
    typer.echo(
        f"Running strategy='{strategy}' for rows={total_rows} "
        f"(batch={settings.benchmark_batch_size}, concurrency={settings.benchmark_concurrency})."
    )
    results = run_strategies(strategy_names=strategy_names, limit=total_rows, persist=True)
    typer.echo(json.dumps(results, indent=2))


def main() -> None:
    try:
        app()
    except KeyboardInterrupt:
        typer.echo("Cancelled by user.", err=True)
        sys.exit(130)


if __name__ == "__main__":
    main()
