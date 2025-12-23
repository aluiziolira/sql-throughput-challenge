"""
Database connection factory utilities for the SQL Throughput Challenge.

This stub centralizes creation of sync and async PostgreSQL connections/pools.
It will be expanded as strategies are implemented. The goal is to keep the
orchestrator and strategies decoupled from connection details and pooling
configuration.
"""
from __future__ import annotations

from typing import Optional

import psycopg
from psycopg import AsyncConnection, Connection
from psycopg_pool import AsyncConnectionPool, ConnectionPool

from src.config import get_settings

# Pool handles (lazy-initialized)
_sync_pool: Optional[ConnectionPool] = None
_async_pool: Optional[AsyncConnectionPool] = None


def _dsn() -> str:
    """Compose a DSN string from settings."""
    settings = get_settings()
    return (
        f"postgresql://{settings.db_user}:{settings.db_password}"
        f"@{settings.db_host}:{settings.db_port}/{settings.db_name}"
    )


def get_sync_connection() -> Connection:
    """
    Acquire a dedicated synchronous connection (autocommit off by default).

    Use this for simple, one-off operations. Prefer the pool for repeated use.
    """
    return psycopg.connect(_dsn())


def get_sync_pool(min_size: int = 1, max_size: int = 10) -> ConnectionPool:
    """
    Get or create a synchronous connection pool.

    Parameters
    ----------
    min_size : int
        Minimum number of idle connections to keep.
    max_size : int
        Maximum total connections in the pool.
    """
    global _sync_pool
    if _sync_pool is None:
        _sync_pool = ConnectionPool(conninfo=_dsn(), min_size=min_size, max_size=max_size)
    return _sync_pool


async def get_async_connection() -> AsyncConnection:
    """
    Acquire an asynchronous connection (autocommit off by default).

    Use this for one-off async operations. Prefer the async pool for repeated use.
    """
    return await AsyncConnection.connect(_dsn())


def get_async_pool(min_size: int = 1, max_size: int = 10) -> AsyncConnectionPool:
    """
    Get or create an asynchronous connection pool.

    Parameters
    ----------
    min_size : int
        Minimum number of idle connections to keep.
    max_size : int
        Maximum total connections in the pool.
    """
    global _async_pool
    if _async_pool is None:
        _async_pool = AsyncConnectionPool(conninfo=_dsn(), min_size=min_size, max_size=max_size)
    return _async_pool


__all__ = [
    "get_sync_connection",
    "get_sync_pool",
    "get_async_connection",
    "get_async_pool",
]
