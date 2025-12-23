"""
Domain models for the SQL Throughput Challenge.

Defines a simple record schema aligned with `db/init.sql`. This model can be
used for validation, serialization, and type hints across strategies and the
orchestrator.
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Dict

from pydantic import BaseModel, Field


class Record(BaseModel):
    """
    Representation of a single row in the `records` table.
    """

    id: int = Field(..., description="Primary key (BIGSERIAL).")
    created_at: datetime = Field(..., description="Row creation timestamp.")
    updated_at: datetime = Field(..., description="Row update timestamp.")
    category: str = Field(..., description="Categorical label for the record.")
    payload: Dict[str, Any] = Field(..., description="Arbitrary JSON payload.")
    amount: Decimal = Field(..., description="Numeric amount.")
    is_active: bool = Field(True, description="Whether the record is active.")
    source: str = Field("generator", description="Origin of the record data.")

    model_config = {
        "frozen": True,
        "populate_by_name": True,
        "arbitrary_types_allowed": False,
    }


__all__ = ["Record"]
