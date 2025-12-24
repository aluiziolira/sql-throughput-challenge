"""
Pytest configuration for the SQL Throughput Challenge.

Provides fixtures for:
- Database connection management
- Test data seeding
- Settings override for integration tests
"""

from __future__ import annotations

import os
import subprocess
import tempfile
from pathlib import Path
from typing import Generator

import psycopg
import pytest

from src.config import Settings, get_settings


@pytest.fixture(scope="session")
def test_settings() -> Settings:
    """
    Settings fixture with test-specific overrides.

    Can be overridden via environment variables in CI or local testing.
    """
    return Settings(
        db_host=os.getenv("DB_HOST", "localhost"),
        db_port=int(os.getenv("DB_PORT", "5432")),
        db_user=os.getenv("DB_USER", "postgres"),
        db_password=os.getenv("DB_PASSWORD", "postgres"),
        db_name=os.getenv("DB_NAME", "throughput_challenge"),
        log_level="DEBUG",
    )


@pytest.fixture(scope="session")
def test_dsn(test_settings: Settings) -> str:
    """
    Database connection string for tests.
    """
    return (
        f"postgresql://{test_settings.db_user}:{test_settings.db_password}"
        f"@{test_settings.db_host}:{test_settings.db_port}/{test_settings.db_name}"
    )


@pytest.fixture(scope="session")
def db_connection_available(test_dsn: str) -> bool:
    """
    Check if database is reachable.

    Used to conditionally skip integration tests when DB is not available.
    """
    try:
        with psycopg.connect(test_dsn, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                cur.fetchone()
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def db_connection(
    test_dsn: str, db_connection_available: bool
) -> Generator[psycopg.Connection, None, None]:
    """
    Provide a session-scoped database connection for integration tests.

    Skips tests if database is not available.
    """
    if not db_connection_available:
        pytest.skip("Database not available for integration tests")

    conn = psycopg.connect(test_dsn)
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture(scope="session")
def db_schema_initialized(db_connection: psycopg.Connection) -> bool:
    """
    Ensure database schema is initialized.

    Checks if the records table exists and creates it if necessary.
    """
    with db_connection.cursor() as cur:
        # Check if table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = 'records'
            );
        """)
        exists = cur.fetchone()[0]

        if not exists:
            # Read and execute init.sql
            init_sql_path = Path(__file__).parent.parent / "db" / "init.sql"
            if init_sql_path.exists():
                with open(init_sql_path, "r") as f:
                    cur.execute(f.read())
                db_connection.commit()

    return True


@pytest.fixture(scope="function")
def clean_records_table(db_connection: psycopg.Connection, db_schema_initialized: bool):
    """
    Clean the records table before each test function.

    This ensures test isolation by starting with an empty table.
    """
    with db_connection.cursor() as cur:
        cur.execute("TRUNCATE TABLE public.records RESTART IDENTITY CASCADE;")
    db_connection.commit()
    yield
    # Cleanup after test
    with db_connection.cursor() as cur:
        cur.execute("TRUNCATE TABLE public.records RESTART IDENTITY CASCADE;")
    db_connection.commit()


@pytest.fixture(scope="function")
def seeded_db_small(
    db_connection: psycopg.Connection,
    clean_records_table,
    test_dsn: str,
) -> int:
    """
    Seed a small dataset (100 rows) for quick integration tests.

    Returns the number of rows seeded.
    """
    rows_to_seed = 100

    # Use generate_data script to create CSV
    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = Path(tmpdir) / "test_data.csv"

        # Call generate_data module programmatically
        from scripts.generate_data import _copy_into_db, _generate_rows_csv

        _generate_rows_csv(csv_path, rows=rows_to_seed, batch_size=50, seed=42)
        _copy_into_db(test_dsn, csv_path)

    # Verify count
    with db_connection.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM public.records;")
        count = cur.fetchone()[0]

    return count


@pytest.fixture(scope="function")
def seeded_db_medium(
    db_connection: psycopg.Connection,
    clean_records_table,
    test_dsn: str,
) -> int:
    """
    Seed a medium dataset (10,000 rows) for performance testing.

    Returns the number of rows seeded.
    """
    rows_to_seed = 10_000

    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = Path(tmpdir) / "test_data.csv"

        from scripts.generate_data import _copy_into_db, _generate_rows_csv

        _generate_rows_csv(csv_path, rows=rows_to_seed, batch_size=2000, seed=42)
        _copy_into_db(test_dsn, csv_path)

    with db_connection.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM public.records;")
        count = cur.fetchone()[0]

    return count


@pytest.fixture(scope="session")
def settings_stub():
    """
    Legacy stub for backward compatibility.

    Use test_settings fixture instead.
    """
    return {
        "db_host": "localhost",
        "db_port": 5432,
        "db_user": "postgres",
        "db_password": "postgres",
        "db_name": "throughput_challenge",
    }
