"""
Data generation and loading script for the SQL Throughput Challenge.

Implements deterministic pseudo-random row generation, CSV emission, and Postgres
COPY loading for maximum throughput.
"""

from __future__ import annotations

import csv
import json
import random
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

import psycopg
import typer

from src.infrastructure.db_factory import build_dsn

app = typer.Typer(help="Generate synthetic data and load into Postgres (CSV + COPY).")


def _build_dsn(dsn_override: str | None) -> str:
    if dsn_override:
        return dsn_override
    return build_dsn()


def _generate_rows_csv(csv_path: Path, rows: int, batch_size: int, seed: int) -> None:
    rng = random.Random(seed)
    categories = ["alpha", "beta", "gamma", "delta"]
    now = datetime.now(UTC).isoformat()

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "created_at",
                "updated_at",
                "category",
                "payload",
                "amount",
                "is_active",
                "source",
            ]
        )

        buffer: list[list[str]] = []
        for i in range(rows):
            category = rng.choice(categories)
            amount = round(rng.uniform(1, 10_000), 2)
            is_active = rng.choice([True, False])
            payload = {
                "user_id": rng.randint(1, 1_000_000),
                "action": rng.choice(["view", "click", "purchase", "impression"]),
                "meta": {"session": rng.randint(1, 1_000_000)},
            }
            buffer.append(
                [
                    now,
                    now,
                    category,
                    json.dumps(payload),
                    f"{amount:.2f}",
                    "t" if is_active else "f",
                    "generator",
                ]
            )
            if len(buffer) >= batch_size:
                writer.writerows(buffer)
                buffer.clear()
        if buffer:
            writer.writerows(buffer)


def _copy_into_db(dsn: str, csv_path: Path) -> int:
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            with cur.copy(
                """
                COPY public.records (created_at, updated_at, category, payload, amount, is_active, source)
                FROM STDIN WITH (FORMAT csv, HEADER TRUE)
                """
            ) as copy:
                with csv_path.open("r", encoding="utf-8") as f:
                    for line in f:
                        copy.write(line)
            conn.commit()
    return 0


@app.command()
def main(
    rows: int = typer.Option(
        100_000,
        "--rows",
        "-r",
        help="Number of rows to generate.",
    ),
    batch_size: int = typer.Option(
        10_000,
        "--batch-size",
        "-b",
        help="Batch size for CSV buffering during generation.",
    ),
    seed: int = typer.Option(
        42,
        "--seed",
        help="Deterministic RNG seed.",
    ),
    output: Path | None = typer.Option(
        None,
        "--output",
        "-o",
        help="Optional CSV output path (if omitted, a temp file will be used).",
    ),
    dsn: str | None = typer.Option(
        None,
        "--dsn",
        help="Optional DSN override for Postgres.",
    ),
    no_load: bool = typer.Option(
        False,
        "--no-load",
        help="Only generate CSV; skip loading into Postgres.",
    ),
) -> None:
    """
    Generate synthetic data and optionally load it into Postgres using COPY.
    """
    start = time.perf_counter()
    if output:
        csv_path = output
        csv_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        tmpdir = Path(tempfile.mkdtemp(prefix="throughput_csv_"))
        csv_path = tmpdir / "records.csv"

    typer.echo(f"Generating {rows:,} rows -> {csv_path} (batch={batch_size}, seed={seed})")
    _generate_rows_csv(csv_path, rows=rows, batch_size=batch_size, seed=seed)
    gen_duration = time.perf_counter() - start
    typer.echo(
        f"CSV generation completed in {gen_duration:.2f}s ({rows / gen_duration:,.0f} rows/s)"
    )

    if no_load:
        typer.echo("Skipping load (no-load flag set).")
        return

    load_start = time.perf_counter()
    conn_dsn = _build_dsn(dsn)
    typer.echo("Loading CSV into Postgres via COPY...")
    _copy_into_db(conn_dsn, csv_path)
    load_duration = time.perf_counter() - load_start

    total_duration = time.perf_counter() - start
    typer.echo(
        f"Load completed in {load_duration:.2f}s. Total time {total_duration:.2f}s "
        f"({rows / total_duration:,.0f} rows/s overall)."
    )


if __name__ == "__main__":
    try:
        app()
    except KeyboardInterrupt:
        typer.echo("Cancelled by user.", err=True)
        sys.exit(130)
