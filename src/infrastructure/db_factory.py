"""
Database connection factory utilities for the SQL Throughput Challenge.

Provides centralized management of sync and async PostgreSQL connections/pools
with proper lifecycle management. The PoolManager singleton ensures resources
are properly cleaned up on application exit.

Includes retry logic for transient connection failures using tenacity.
"""

from __future__ import annotations

import atexit
import threading
from contextlib import contextmanager
from typing import Generator, Optional

import psycopg
from psycopg import AsyncConnection, Connection
from psycopg_pool import AsyncConnectionPool, ConnectionPool
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.config import get_settings


class PoolManager:
    """
    Thread-safe singleton for managing database connection pools.

    Handles lifecycle management with automatic cleanup via atexit hook.
    """

    _instance: Optional["PoolManager"] = None
    _lock = threading.Lock()

    def __new__(cls) -> "PoolManager":
        """Create or return the singleton instance."""
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._sync_pool: Optional[ConnectionPool] = None
                cls._instance._async_pool: Optional[AsyncConnectionPool] = None
                cls._instance._initialized = False
                # Register cleanup on exit
                atexit.register(cls._instance.close_all)
            return cls._instance

    def _ensure_initialized(self) -> None:
        """Ensure the manager is initialized (idempotent)."""
        if not self._initialized:
            self._initialized = True

    def get_sync_pool(self, min_size: int = 1, max_size: int = 10) -> ConnectionPool:
        """
        Get or create the synchronous connection pool.

        Parameters
        ----------
        min_size : int
            Minimum number of idle connections to keep.
        max_size : int
            Maximum total connections in the pool.

        Returns
        -------
        ConnectionPool
            The managed sync pool instance.
        """
        with self._lock:
            if self._sync_pool is None:
                settings = get_settings()
                dsn = (
                    f"postgresql://{settings.db_user}:{settings.db_password}"
                    f"@{settings.db_host}:{settings.db_port}/{settings.db_name}"
                )
                self._sync_pool = ConnectionPool(conninfo=dsn, min_size=min_size, max_size=max_size)
            return self._sync_pool

    def get_async_pool(self, min_size: int = 1, max_size: int = 10) -> AsyncConnectionPool:
        """
        Get or create the asynchronous connection pool.

        Parameters
        ----------
        min_size : int
            Minimum number of idle connections to keep.
        max_size : int
            Maximum total connections in the pool.

        Returns
        -------
        AsyncConnectionPool
            The managed async pool instance.
        """
        with self._lock:
            if self._async_pool is None:
                settings = get_settings()
                dsn = (
                    f"postgresql://{settings.db_user}:{settings.db_password}"
                    f"@{settings.db_host}:{settings.db_port}/{settings.db_name}"
                )
                self._async_pool = AsyncConnectionPool(
                    conninfo=dsn, min_size=min_size, max_size=max_size
                )
            return self._async_pool

    @contextmanager
    def sync_connection(self) -> Generator[Connection, None, None]:
        """
        Context manager for obtaining a sync connection from the pool.

        Example
        -------
            manager = PoolManager()
            with manager.sync_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
        """
        pool = self.get_sync_pool()
        with pool.connection() as conn:
            yield conn

    def close_all(self) -> None:
        """
        Close all managed pools and release resources.

        This is called automatically on exit via atexit hook.
        """
        with self._lock:
            if self._sync_pool is not None:
                try:
                    self._sync_pool.close()
                except Exception:
                    pass  # Best-effort cleanup
                finally:
                    self._sync_pool = None

            if self._async_pool is not None:
                try:
                    self._async_pool.close()
                except Exception:
                    pass  # Best-effort cleanup
                finally:
                    self._async_pool = None

    def __del__(self) -> None:
        """Ensure cleanup on garbage collection."""
        self.close_all()


def _dsn() -> str:
    """Compose a DSN string from settings."""
    settings = get_settings()
    return (
        f"postgresql://{settings.db_user}:{settings.db_password}"
        f"@{settings.db_host}:{settings.db_port}/{settings.db_name}"
    )


# Convenience functions for backward compatibility and simple use cases


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
    Use this for simple, one-off operations. Prefer the pool for repeated use.

    Returns
    -------
    Connection
        A new psycopg connection instance.

    Raises
    ------
    psycopg.OperationalError
        If connection fails after all retry attempts.
    """
    return psycopg.connect(_dsn())


def get_sync_pool(min_size: int = 1, max_size: int = 10) -> ConnectionPool:
    """
    Get or create a synchronous connection pool via PoolManager.

    Parameters
    ----------
    min_size : int
        Minimum number of idle connections to keep.
    max_size : int
        Maximum total connections in the pool.

    Returns
    -------
    ConnectionPool
        The managed sync pool instance.
    """
    manager = PoolManager()
    return manager.get_sync_pool(min_size=min_size, max_size=max_size)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type((OSError, ConnectionError)),
    reraise=True,
)
async def get_async_connection() -> AsyncConnection:
    """
    Acquire an asynchronous connection with automatic retry.

    Retries up to 3 times with exponential backoff for transient connection errors.
    Use this for one-off async operations. Prefer the async pool for repeated use.

    Returns
    -------
    AsyncConnection
        A new asyncpg-compatible connection instance.

    Raises
    ------
    ConnectionError
        If connection fails after all retry attempts.
    """
    return await AsyncConnection.connect(_dsn())


def get_async_pool(min_size: int = 1, max_size: int = 10) -> AsyncConnectionPool:
    """
    Get or create an asynchronous connection pool via PoolManager.

    Parameters
    ----------
    min_size : int
        Minimum number of idle connections to keep.
    max_size : int
        Maximum total connections in the pool.

    Returns
    -------
    AsyncConnectionPool
        The managed async pool instance.
    """
    manager = PoolManager()
    return manager.get_async_pool(min_size=min_size, max_size=max_size)


__all__ = [
    "PoolManager",
    "get_sync_connection",
    "get_sync_pool",
    "get_async_connection",
    "get_async_pool",
]
