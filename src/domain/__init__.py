"""
Domain package for the SQL Throughput Challenge.

Exports the core domain models used across strategies and orchestrator logic.
Keep this package focused on data definitions and validation concerns.
"""

from src.domain.models import Record

__all__ = [
    "Record",
]
