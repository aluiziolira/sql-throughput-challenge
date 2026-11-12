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
STREAM_LIMIT = 3
STREAM_FAIL_LIMIT = 5
FIRST_BATCH_ROWS = 2
WINDOWED_TOTAL_LIMIT = 6
WINDOW_1_ROWS = 3
WINDOW_THRESHOLD = 5
WINDOW_SIZE = 3
WINDOW_FAIL_AFTER_ID = 3
EXPECTED_ERRORS_COUNT = 1
ZERO_ROWS = 0


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
async def test_stream_closes_connection_when_cursor_raises(monkeypatch) -> None:
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
    outcome = await strategy._stream(limit=STREAM_LIMIT)

    assert outcome.rows == ZERO_ROWS
    assert len(outcome.errors) == EXPECTED_ERRORS_COUNT
    assert "connect-time cursor failure" in outcome.errors[0]
    assert boom_conn.closed is True


@pytest.mark.asyncio
async def test_stream_closes_connection_and_reports_partial_when_fetch_raises(
    monkeypatch,
) -> None:
    class _FakeSingleCursor:
        def __init__(self, first_batch: list[dict[str, int]]) -> None:
            self._first_batch = first_batch
            self._calls = 0

        async def fetch(self, batch_size: int) -> list[dict[str, int]]:
            del batch_size
            self._calls += 1
            if self._calls == 1:
                return self._first_batch
            raise RuntimeError("fetch failed mid-stream")

    class _FakeStreamConnection:
        def __init__(self, first_batch: list[dict[str, int]]) -> None:
            self._first_batch = first_batch
            self.closed = False

        def transaction(self) -> _AsyncNoopContext:
            return _AsyncNoopContext()

        async def execute(self, sql: str, *params: Any) -> str:
            del sql, params
            return "OK"

        async def cursor(self, sql: str, *params: Any) -> _FakeSingleCursor:
            del sql, params
            return _FakeSingleCursor(self._first_batch)

        async def close(self) -> None:
            self.closed = True

    first_batch = [{"id": idx} for idx in range(FIRST_BATCH_ROWS)]
    stream_conn = _FakeStreamConnection(first_batch)

    async def fake_connect(dsn: str) -> _FakeStreamConnection:
        del dsn
        return stream_conn

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
    outcome = await strategy._stream(limit=STREAM_FAIL_LIMIT)

    assert outcome.rows == FIRST_BATCH_ROWS
    assert len(outcome.errors) == EXPECTED_ERRORS_COUNT
    assert "fetch failed mid-stream" in outcome.errors[0]
    assert stream_conn.closed is True


@pytest.mark.asyncio
async def test_stream_windowed_stops_and_preserves_partial_rows_when_window_id_select_raises(
    monkeypatch,
) -> None:
    class _FakeWindowFailConnection:
        def __init__(self, selected_ids: list[int], fail_after_id: int) -> None:
            self._selected_ids = selected_ids
            self._fail_after_id = fail_after_id

        def transaction(self) -> _AsyncNoopContext:
            return _AsyncNoopContext()

        async def execute(self, sql: str, *params: Any) -> str:
            del sql, params
            return "OK"

        async def fetch(self, sql: str, *params: Any) -> list[dict[str, int]]:
            if "WHERE ($1::bigint IS NULL OR id > $1)" in sql:
                last_selected_id, window_limit = params
                if last_selected_id == self._fail_after_id:
                    raise RuntimeError("window 2 id select failed")
                start_idx = 0
                if last_selected_id is not None:
                    start_idx = self._selected_ids.index(last_selected_id) + 1
                window_ids = self._selected_ids[start_idx : start_idx + int(window_limit)]
                return [{"id": value} for value in window_ids]
            (limit,) = params
            return [{"id": value} for value in self._selected_ids[: int(limit)]]

        async def cursor(self, sql: str, chunk_ids: list[int]) -> _FakeRecordCursor:
            del sql
            return _FakeRecordCursor(chunk_ids, fail=False)

        async def close(self) -> None:
            return None

    selected_ids = list(range(1, WINDOWED_TOTAL_LIMIT + 1))
    conn = _FakeWindowFailConnection(selected_ids, fail_after_id=WINDOW_FAIL_AFTER_ID)
    fake_pool = _FakePool(conn)  # type: ignore[arg-type]

    async def fake_create_pool(*args: Any, **kwargs: Any) -> _FakePool:
        del args, kwargs
        return fake_pool

    monkeypatch.setattr(async_stream_module.asyncpg, "create_pool", fake_create_pool)
    monkeypatch.setattr(async_stream_module, "_CONCURRENT_ID_WINDOW_THRESHOLD", WINDOW_THRESHOLD)
    monkeypatch.setattr(async_stream_module, "_CONCURRENT_ID_WINDOW_SIZE", WINDOW_SIZE)
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
    result = await strategy.execute_async(limit=WINDOWED_TOTAL_LIMIT)

    assert result["rows"] == WINDOW_1_ROWS
    assert result["extra"]["error_count"] == EXPECTED_ERRORS_COUNT
    assert result["error"] is not None
    assert "window 2 id select failed" in result["error"]
    assert f"id window after id={WINDOW_FAIL_AFTER_ID}" in result["error"]
    assert fake_pool.closed is True
