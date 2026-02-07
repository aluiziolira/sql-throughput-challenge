import csv
import json
from pathlib import Path
from time import sleep

from scripts import generate_data
from src import config
from src.orchestrator import available_strategies
from src.utils import profiler

# Test configuration constants
DEFAULT_SLEEP_SECONDS = 0.05
DEFAULT_ROWS = 5
DEFAULT_BATCH_SIZE = 2
DEFAULT_SEED = 123
DEFAULT_POSTGRES_PORT = 5432


def test_get_settings_defaults():
    settings = config.get_settings()
    assert settings.db_host == "localhost"
    assert settings.db_port == DEFAULT_POSTGRES_PORT
    assert settings.db_user == "postgres"
    assert settings.db_name == "throughput_challenge"
    assert settings.benchmark_rows > 0
    assert settings.benchmark_batch_size > 0
    assert settings.benchmark_concurrency > 0


def test_profile_block_measures_time():
    with profiler.profile_block("sleep") as stats:
        sleep(DEFAULT_SLEEP_SECONDS)
    assert stats.duration_seconds >= DEFAULT_SLEEP_SECONDS
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
    generate_data._generate_rows_csv(
        csv_path, rows=DEFAULT_ROWS, batch_size=DEFAULT_BATCH_SIZE, seed=DEFAULT_SEED
    )
    assert csv_path.exists()
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    # header + 5 rows = 6 lines
    assert len(rows) == DEFAULT_ROWS + 1
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
