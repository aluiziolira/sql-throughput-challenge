from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from types import SimpleNamespace
from typing import Any

import pytest

from src.strategies import async_stream as async_stream_module
from src.strategies.async_stream import AsyncStreamStrategy, _merge_task_outcomes

SUCCESS_ROWS = 3
FAILED_ROWS = 2
MERGED_ROWS = SUCCESS_ROWS + FAILED_ROWS
PARTIAL_ROWS = 2
TOTAL_LIMIT = 4
FAILING_CHUNK_START_ID = 3
CONCURRENCY = 2
BATCH_SIZE = 2


class _AsyncNoopContext(AbstractAsyncContextManager[None]):
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        del exc_type, exc, tb
        return False


class _FakeRecordCursor:
    def __init__(self, ids: list[int], *, fail: bool) -> None:
        self._remaining = len(ids)
        self._fail = fail

    async def fetch(self, batch_size: int) -> list[dict[str, int]]:
        if self._fail:
            raise RuntimeError("cursor fetch failed")
        if self._remaining <= 0:
            return []
        batch_len = min(batch_size, self._remaining)
        self._remaining -= batch_len
        return [{"id": index} for index in range(batch_len)]


class _FakeConnection:
    def __init__(self, selected_ids: list[int]) -> None:
        self._selected_ids = selected_ids

    def transaction(self) -> _AsyncNoopContext:
        return _AsyncNoopContext()

    async def execute(self, sql: str, *params: Any) -> str:
        del sql, params
        return "OK"

    async def fetch(self, sql: str, *params: Any) -> list[dict[str, int]]:
        del sql
        (limit,) = params
        return [{"id": value} for value in self._selected_ids[: int(limit)]]

    async def cursor(self, sql: str, chunk_ids: list[int]) -> _FakeRecordCursor:
        del sql
        fail = chunk_ids[0] == FAILING_CHUNK_START_ID
        return _FakeRecordCursor(chunk_ids, fail=fail)

    async def close(self) -> None:
        return None


class _AcquireContext(AbstractAsyncContextManager[_FakeConnection]):
    def __init__(self, conn: _FakeConnection) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakeConnection:
        return self._conn

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        del exc_type, exc, tb
        return False


class _FakePool:
    def __init__(self, conn: _FakeConnection) -> None:
        self.conn = conn
        self.closed = False

    def acquire(self) -> _AcquireContext:
        return _AcquireContext(self.conn)

    async def close(self) -> None:
        self.closed = True


def test_merge_task_outcomes_does_not_double_count() -> None:
    outcome = _merge_task_outcomes([SUCCESS_ROWS, RuntimeError("boom"), FAILED_ROWS])
    assert outcome.rows == MERGED_ROWS
    assert len(outcome.errors) == 1
    assert "boom" in outcome.errors[0]


@pytest.mark.asyncio
async def test_stream_concurrent_preserves_partial_rows_and_closes_pool_on_worker_error(
    monkeypatch,
) -> None:
    selected_ids = list(range(1, TOTAL_LIMIT + 1))
    conn = _FakeConnection(selected_ids)
    fake_pool = _FakePool(conn)

    async def fake_create_pool(*args: Any, **kwargs: Any) -> _FakePool:
        del args, kwargs
        return fake_pool

    monkeypatch.setattr(async_stream_module.asyncpg, "create_pool", fake_create_pool)
    monkeypatch.setattr(
        async_stream_module,
        "get_settings",
        lambda: SimpleNamespace(
            benchmark_batch_size=BATCH_SIZE,
            db_statement_timeout_ms=0,
            db_user="user",
            db_password="pass",
            db_host="localhost",
            db_port=5432,
            db_name="db",
        ),
    )

    strategy = AsyncStreamStrategy(
        batch_size=BATCH_SIZE,
        concurrency=CONCURRENCY,
        dsn_override="postgresql://test",
    )
    result = await strategy.execute_async(limit=TOTAL_LIMIT)

    assert result["rows"] == PARTIAL_ROWS
    assert result["error"] is not None
    assert "cursor fetch failed" in result["error"]
    assert result["extra"]["error_count"] == 1
    assert fake_pool.closed is True


@pytest.mark.asyncio
async def test_stream_closes_connection_when_fetch_raises(monkeypatch) -> None:
    class _BoomConnection:
        def __init__(self) -> None:
            self.closed = False

        def transaction(self) -> _AsyncNoopContext:
            return _AsyncNoopContext()

        async def execute(self, sql: str, *params: Any) -> str:
            del sql, params
            return "OK"

        async def cursor(self, sql: str, *params: Any) -> Any:
            del sql, params
            raise RuntimeError("connect-time cursor failure")

        async def close(self) -> None:
            self.closed = True

    boom_conn = _BoomConnection()

    async def fake_connect(dsn: str) -> _BoomConnection:
        del dsn
        return boom_conn

    monkeypatch.setattr(async_stream_module.asyncpg, "connect", fake_connect)
    monkeypatch.setattr(
        async_stream_module,
        "get_settings",
        lambda: SimpleNamespace(
            benchmark_batch_size=BATCH_SIZE,
            db_statement_timeout_ms=0,
        ),
    )

    strategy = AsyncStreamStrategy(
        batch_size=BATCH_SIZE,
        concurrency=1,
        dsn_override="postgresql://test",
    )
    with pytest.raises(RuntimeError, match="connect-time cursor failure"):
        await strategy._stream(limit=3)

    assert boom_conn.closed is True
