from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from types import SimpleNamespace
from typing import Any

import pytest

from src.strategies import async_stream as async_stream_module
from src.strategies.async_stream import AsyncStreamStrategy

SMALL_LIMIT = 9
LARGE_LIMIT = 8


class _AsyncNoopContext(AbstractAsyncContextManager[None]):
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        del exc_type, exc, tb
        return False


class _FakeRecordCursor:
    def __init__(self, ids: list[int]) -> None:
        self._remaining = len(ids)

    async def fetch(self, batch_size: int) -> list[dict[str, int]]:
        if self._remaining <= 0:
            return []
        batch_len = min(batch_size, self._remaining)
        self._remaining -= batch_len
        return [{"id": index} for index in range(batch_len)]


class _FakeConnection:
    def __init__(self, selected_ids: list[int]) -> None:
        self._selected_ids = selected_ids
        self.fetch_calls: list[tuple[Any, ...]] = []
        self.cursor_chunk_sizes: list[int] = []

    def transaction(self) -> _AsyncNoopContext:
        return _AsyncNoopContext()

    async def execute(self, sql: str, *params: Any) -> str:
        del sql, params
        return "OK"

    async def fetch(self, sql: str, *params: Any) -> list[dict[str, int]]:
        if "WHERE ($1::bigint IS NULL OR id > $1)" in sql:
            last_selected_id, window_limit = params
            self.fetch_calls.append(("window", last_selected_id, window_limit))
            start_idx = 0
            if last_selected_id is not None:
                start_idx = self._selected_ids.index(last_selected_id) + 1
            window_ids = self._selected_ids[start_idx : start_idx + int(window_limit)]
            return [{"id": value} for value in window_ids]

        (limit,) = params
        self.fetch_calls.append(("full", limit))
        return [{"id": value} for value in self._selected_ids[: int(limit)]]

    async def cursor(self, sql: str, chunk_ids: list[int]) -> _FakeRecordCursor:
        del sql
        self.cursor_chunk_sizes.append(len(chunk_ids))
        return _FakeRecordCursor(chunk_ids)


class _AcquireContext(AbstractAsyncContextManager[_FakeConnection]):
    def __init__(self, conn: _FakeConnection) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakeConnection:
        return self._conn

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        del exc_type, exc, tb
        return False


class _FakePool:
    def __init__(self, selected_ids: list[int]) -> None:
        self.conn = _FakeConnection(selected_ids)
        self.closed = False

    def acquire(self) -> _AcquireContext:
        return _AcquireContext(self.conn)

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_stream_concurrent_preserves_full_id_prefetch_for_small_limits(
    monkeypatch,
) -> None:
    selected_ids = list(range(1, SMALL_LIMIT + 1))
    fake_pool = _FakePool(selected_ids)

    async def fake_create_pool(*args: Any, **kwargs: Any) -> _FakePool:
        del args, kwargs
        return fake_pool

    monkeypatch.setattr(async_stream_module.asyncpg, "create_pool", fake_create_pool)
    monkeypatch.setattr(
        async_stream_module,
        "get_settings",
        lambda: SimpleNamespace(
            benchmark_batch_size=4,
            db_statement_timeout_ms=0,
            db_user="user",
            db_password="pass",
            db_host="localhost",
            db_port=5432,
            db_name="db",
        ),
    )

    strategy = AsyncStreamStrategy(batch_size=3, concurrency=3, dsn_override="postgresql://test")
    rows_read = await strategy._stream_concurrent(limit=SMALL_LIMIT)

    assert rows_read == SMALL_LIMIT
    assert fake_pool.closed is True
    assert fake_pool.conn.fetch_calls == [("full", SMALL_LIMIT)]


@pytest.mark.asyncio
async def test_stream_concurrent_uses_bounded_id_windows_for_large_limits(
    monkeypatch,
) -> None:
    selected_ids = list(range(1, LARGE_LIMIT + 1))
    fake_pool = _FakePool(selected_ids)

    async def fake_create_pool(*args: Any, **kwargs: Any) -> _FakePool:
        del args, kwargs
        return fake_pool

    monkeypatch.setattr(async_stream_module.asyncpg, "create_pool", fake_create_pool)
    monkeypatch.setattr(async_stream_module, "_CONCURRENT_ID_WINDOW_THRESHOLD", 5)
    monkeypatch.setattr(async_stream_module, "_CONCURRENT_ID_WINDOW_SIZE", 3)
    monkeypatch.setattr(
        async_stream_module,
        "get_settings",
        lambda: SimpleNamespace(
            benchmark_batch_size=4,
            db_statement_timeout_ms=0,
            db_user="user",
            db_password="pass",
            db_host="localhost",
            db_port=5432,
            db_name="db",
        ),
    )

    strategy = AsyncStreamStrategy(batch_size=2, concurrency=3, dsn_override="postgresql://test")
    rows_read = await strategy._stream_concurrent(limit=LARGE_LIMIT)

    assert rows_read == LARGE_LIMIT
    assert fake_pool.closed is True
    assert fake_pool.conn.fetch_calls == [
        ("window", None, 3),
        ("window", 3, 3),
        ("window", 6, 2),
    ]
