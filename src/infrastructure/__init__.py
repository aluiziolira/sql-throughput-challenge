"""
Infrastructure package for the SQL Throughput Challenge.

Centralizes database connectivity concerns (sync/async factories, pooling).
Keep this layer focused on I/O and resource management, decoupled from
strategy/orchestrator logic.
"""

from src.infrastructure.db_factory import (
    get_async_connection,
    get_async_pool,
    get_sync_connection,
    get_sync_pool,
)

__all__ = [
    "get_async_connection",
    "get_async_pool",
    "get_sync_connection",
    "get_sync_pool",
]
