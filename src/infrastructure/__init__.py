"""
Infrastructure package for the SQL Throughput Challenge.

Centralizes database connectivity concerns (sync factories).
Keep this layer focused on I/O and resource management, decoupled from
strategy/orchestrator logic.
"""

from src.infrastructure.db_factory import get_sync_connection

__all__ = [
    "get_sync_connection",
]
