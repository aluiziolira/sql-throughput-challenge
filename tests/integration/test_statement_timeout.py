from __future__ import annotations

import os

import pytest

from src.config import get_settings
from src.strategies.async_stream import AsyncStreamStrategy
from src.strategies.cursor_pagination import CursorPaginationStrategy
from src.strategies.pooled_sync import PooledSyncStrategy

TIMEOUT_MS = 60_000
DEFAULT_BATCH_SIZE = 10
DEFAULT_POOL_MIN = 1
DEFAULT_POOL_MAX = 5

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_INTEGRATION_TESTS", "0") != "1",
    reason="Integration tests require RUN_INTEGRATION_TESTS=1 and reachable Postgres",
)


def test_cursor_pagination_with_statement_timeout(
    test_dsn: str, seeded_db_small: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify cursor_pagination runs successfully with statement timeout enabled."""
    monkeypatch.setenv("DB_STATEMENT_TIMEOUT_MS", str(TIMEOUT_MS))
    get_settings.cache_clear()
    try:
        strategy = CursorPaginationStrategy(batch_size=DEFAULT_BATCH_SIZE, dsn_override=test_dsn)
        result = strategy.execute(limit=seeded_db_small)
        assert result["rows"] == seeded_db_small
        assert result.get("error") is None
    finally:
        get_settings.cache_clear()


def test_pooled_sync_with_statement_timeout(
    test_dsn: str, seeded_db_small: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify pooled_sync runs successfully with statement timeout enabled."""
    monkeypatch.setenv("DB_STATEMENT_TIMEOUT_MS", str(TIMEOUT_MS))
    get_settings.cache_clear()
    strategy = PooledSyncStrategy(
        batch_size=DEFAULT_BATCH_SIZE,
        pool_min_size=DEFAULT_POOL_MIN,
        pool_max_size=DEFAULT_POOL_MAX,
        dsn_override=test_dsn,
    )
    try:
        result = strategy.execute(limit=seeded_db_small)
        assert result["rows"] == seeded_db_small
        assert result.get("error") is None
    finally:
        strategy.close()
        get_settings.cache_clear()


@pytest.mark.asyncio
async def test_async_stream_with_statement_timeout(
    test_dsn: str, seeded_db_small: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify async_stream runs successfully with statement timeout enabled."""
    monkeypatch.setenv("DB_STATEMENT_TIMEOUT_MS", str(TIMEOUT_MS))
    get_settings.cache_clear()
    try:
        strategy = AsyncStreamStrategy(batch_size=DEFAULT_BATCH_SIZE, dsn_override=test_dsn)
        result = await strategy.execute_async(limit=seeded_db_small)
        assert result["rows"] == seeded_db_small
        assert result.get("error") is None
    finally:
        get_settings.cache_clear()
