"""
Database connection factory utilities for the SQL Throughput Challenge.

Provides centralized connection utilities with retry logic for transient
connection failures using tenacity.
"""

from __future__ import annotations

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


# Convenience functions for backward compatibility and simple use cases


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


__all__ = [
    "apply_statement_timeout",
    "async_apply_statement_timeout",
    "build_dsn",
    "get_sync_connection",
]
