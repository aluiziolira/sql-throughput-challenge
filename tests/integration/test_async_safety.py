"""Test async strategy event-loop safety."""

import asyncio

import pytest

from src.strategies.async_stream import AsyncStreamStrategy


@pytest.mark.asyncio
async def test_async_stream_from_async_context(test_dsn: str, seeded_db_small: int):
    """Verify async strategy works within async context."""
    strategy = AsyncStreamStrategy(batch_size=10, dsn_override=test_dsn)
    result = await strategy.execute_async(limit=seeded_db_small)

    assert result["rows"] == seeded_db_small
    assert result["duration_seconds"] > 0


def test_async_stream_from_sync_context(test_dsn: str, seeded_db_small: int):
    """Verify async strategy works from synchronous code."""
    strategy = AsyncStreamStrategy(batch_size=10, dsn_override=test_dsn)
    result = strategy.execute(limit=seeded_db_small)

    assert result["rows"] == seeded_db_small


def test_async_stream_execute_raises_in_async_context(test_dsn: str):
    """Verify execute() raises clear error when called from async context."""

    async def try_sync_execute():
        strategy = AsyncStreamStrategy(batch_size=10, dsn_override=test_dsn)
        # This should raise RuntimeError
        strategy.execute(limit=100)

    with pytest.raises(RuntimeError, match="async context"):
        asyncio.run(try_sync_execute())
