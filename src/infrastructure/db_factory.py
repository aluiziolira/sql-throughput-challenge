"""
Database connection factory utilities for the SQL Throughput Challenge.

Provides centralized connection utilities with retry logic for transient
connection failures using tenacity, plus shared SQL/cursor primitives used
by the sync read strategies (naive, cursor_pagination, pooled_sync,
multiprocessing) to avoid re-implementing the same fetch mechanics in
each one.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import psycopg
from psycopg import Connection
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.config import get_settings

# Shared by strategies that read the full row set up to a LIMIT.
SELECT_RECORDS_SQL = "SELECT * FROM public.records ORDER BY id LIMIT %s;"


def build_dsn() -> str:
    """Compose a DSN string from settings."""
    settings = get_settings()
    return (
        f"postgresql://{settings.db_user}:{settings.db_password}"
        f"@{settings.db_host}:{settings.db_port}/{settings.db_name}"
    )


def apply_statement_timeout(cursor: psycopg.Cursor, timeout_ms: int) -> None:
    """Apply transaction-local statement timeout for sync psycopg usage."""
    if timeout_ms > 0:
        cursor.execute("SET LOCAL statement_timeout = %s", (timeout_ms,))


async def async_apply_statement_timeout(conn: Any, timeout_ms: int) -> None:
    """Apply transaction-local statement timeout for asyncpg usage."""
    if timeout_ms > 0:
        await conn.execute("SET LOCAL statement_timeout = $1", timeout_ms)


# Tenacity provides production resilience for transient connection failures.
# The retry decorators ensure that temporary network issues or database
# unavailability during startup don't cause immediate failures.
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type((psycopg.OperationalError, psycopg.InterfaceError)),
    reraise=True,
)
def get_sync_connection() -> Connection:
    """
    Acquire a dedicated synchronous connection with automatic retry.

    Retries up to 3 times with exponential backoff for transient connection errors.

    Returns
    -------
    Connection
        A new psycopg connection instance.

    Raises
    ------
    psycopg.OperationalError
        If connection fails after all retry attempts.
    """
    return psycopg.connect(build_dsn())


def resolve_sync_connection(dsn_override: str | None) -> Connection:
    """
    Acquire a sync connection, honoring a strategy's `dsn_override`.

    With no override, goes through `get_sync_connection()` (retrying,
    settings-derived DSN). An override (used by tests to point at a known
    DSN) connects directly without retry, since tests want immediate
    failure rather than a delayed retry loop.
    """
    if dsn_override:
        return psycopg.connect(dsn_override)
    return get_sync_connection()


def fetch_in_batches(cursor: psycopg.Cursor, batch_size: int) -> Iterator[list[tuple[Any, ...]]]:
    """Yield successive `fetchmany(batch_size)` batches from an open cursor until exhausted."""
    while True:
        batch = cursor.fetchmany(batch_size)
        if not batch:
            break
        yield batch


__all__ = [
    "SELECT_RECORDS_SQL",
    "apply_statement_timeout",
    "async_apply_statement_timeout",
    "build_dsn",
    "fetch_in_batches",
    "get_sync_connection",
    "resolve_sync_connection",
]
