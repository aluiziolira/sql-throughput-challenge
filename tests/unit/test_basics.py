import csv
import json
from pathlib import Path
from time import sleep

import pytest

from src import config
from src.orchestrator import available_strategies
from src.utils import profiler
from scripts import generate_data


def test_get_settings_defaults():
    settings = config.get_settings()
    assert settings.db_host == "localhost"
    assert settings.db_port == 5432
    assert settings.db_user == "postgres"
    assert settings.db_name == "throughput_challenge"
    assert settings.benchmark_rows > 0
    assert settings.benchmark_batch_size > 0
    assert settings.benchmark_concurrency > 0


def test_profile_block_measures_time():
    with profiler.profile_block("sleep") as stats:
        sleep(0.05)
    assert stats.duration_seconds >= 0.05
    # cpu_percent may be None if psutil missing; only assert type when present
    if stats.cpu_percent is not None:
        assert isinstance(stats.cpu_percent, float)


def test_available_strategies_contains_known_entries():
    names = available_strategies()
    assert "naive" in names
    assert isinstance(names, list)
    assert len(names) >= 1


def test_generate_data_writes_csv(tmp_path: Path):
    csv_path = tmp_path / "records.csv"
    # Generate a tiny dataset without loading into DB
    generate_data._generate_rows_csv(csv_path, rows=5, batch_size=2, seed=123)
    assert csv_path.exists()
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    # header + 5 rows = 6 lines
    assert len(rows) == 6
    header = rows[0]
    assert header == [
        "created_at",
        "updated_at",
        "category",
        "payload",
        "amount",
        "is_active",
        "source",
    ]
    # Ensure payload is valid JSON for first data row
    first_payload = rows[1][3]
    json.loads(first_payload)
