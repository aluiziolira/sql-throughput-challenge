from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from src.strategies import cursor_pagination as cursor_mod
from src.strategies import pooled_sync as pool_mod
from src.strategies.cursor_pagination import CursorPaginationStrategy
from src.strategies.pooled_sync import PooledSyncStrategy

BATCH_SIZE = 2
TEST_LIMIT = 10
PARTIAL_ROWS = 2
EXPECTED_ERROR_COUNT = 1
ZERO_TIMEOUT = 0
FAIL_IMMEDIATELY = 0
FAIL_AFTER_FIRST_BATCH = 2


class _FakeBatchCursor:
    def __init__(self, batches: list[list[tuple[Any, ...]]], fail_at_batch: int = -1) -> None:
        self._batches = list(batches)
        self._fail_at_batch = fail_at_batch
        self._call_count = 0

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> None:
        del sql, params
        if self._fail_at_batch == FAIL_IMMEDIATELY:
            raise RuntimeError("immediate query failure")

    def fetchmany(self, batch_size: int) -> list[tuple[Any, ...]]:
        del batch_size
        self._call_count += 1
        if self._call_count == self._fail_at_batch:
            raise RuntimeError("batch fetch failure")
        if not self._batches:
            return []
        return self._batches.pop(0)

    def __enter__(self) -> _FakeBatchCursor:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        del exc_type, exc, tb
        return False


class _FakeSyncConnection:
    def __init__(self, cursor: _FakeBatchCursor) -> None:
        self._cursor = cursor
        self.closed = False

    def cursor(self, *args: Any, **kwargs: Any) -> _FakeBatchCursor:
        del args, kwargs
        return self._cursor

    def execute(self, sql: str, params: Any = None) -> None:
        del sql, params

    def __enter__(self) -> _FakeSyncConnection:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        del exc_type, exc, tb
        self.closed = True
        return False


class _FakePoolContext:
    def __init__(self, conn: _FakeSyncConnection) -> None:
        self._conn = conn

    def __enter__(self) -> _FakeSyncConnection:
        return self._conn

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        del exc_type, exc, tb
        return False


class _FakeConnectionPool:
    def __init__(self, conn: _FakeSyncConnection) -> None:
        self._conn = conn
        self.closed = False

    def connection(self) -> _FakePoolContext:
        return _FakePoolContext(self._conn)

    def close(self) -> None:
        self.closed = True


def test_cursor_pagination_failure_after_one_batch_returns_partial_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cur = _FakeBatchCursor([[(1,), (2,)], [(3,), (4,)]], fail_at_batch=FAIL_AFTER_FIRST_BATCH)
    conn = _FakeSyncConnection(cur)

    monkeypatch.setattr(cursor_mod, "resolve_sync_connection", lambda dsn: conn)
    monkeypatch.setattr(
        cursor_mod,
        "get_settings",
        lambda: SimpleNamespace(
            benchmark_batch_size=BATCH_SIZE,
            db_statement_timeout_ms=ZERO_TIMEOUT,
        ),
    )

    strategy = CursorPaginationStrategy(batch_size=BATCH_SIZE, dsn_override="postgresql://test")
    result = strategy.execute(limit=TEST_LIMIT)

    assert result["rows"] == PARTIAL_ROWS
    assert result["duration_seconds"] >= 0
    assert result["throughput_rows_per_sec"] >= 0
    assert result["error"] == "RuntimeError: batch fetch failure"
    assert result["extra"]["error_count"] == EXPECTED_ERROR_COUNT
    assert conn.closed is True


def test_cursor_pagination_failure_before_any_row_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cur = _FakeBatchCursor([], fail_at_batch=FAIL_IMMEDIATELY)
    conn = _FakeSyncConnection(cur)

    monkeypatch.setattr(cursor_mod, "resolve_sync_connection", lambda dsn: conn)
    monkeypatch.setattr(
        cursor_mod,
        "get_settings",
        lambda: SimpleNamespace(
            benchmark_batch_size=BATCH_SIZE,
            db_statement_timeout_ms=ZERO_TIMEOUT,
        ),
    )

    strategy = CursorPaginationStrategy(batch_size=BATCH_SIZE, dsn_override="postgresql://test")
    with pytest.raises(RuntimeError, match="immediate query failure"):
        strategy.execute(limit=TEST_LIMIT)

    assert conn.closed is True


def test_pooled_sync_failure_after_one_batch_returns_partial_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cur = _FakeBatchCursor([[(1,), (2,)], [(3,), (4,)]], fail_at_batch=FAIL_AFTER_FIRST_BATCH)
    conn = _FakeSyncConnection(cur)
    pool = _FakeConnectionPool(conn)

    monkeypatch.setattr(
        pool_mod,
        "get_settings",
        lambda: SimpleNamespace(
            benchmark_batch_size=BATCH_SIZE,
            db_statement_timeout_ms=ZERO_TIMEOUT,
        ),
    )

    strategy = PooledSyncStrategy(batch_size=BATCH_SIZE, dsn_override="postgresql://test")
    monkeypatch.setattr(strategy, "_get_pool", lambda: pool)

    result = strategy.execute(limit=TEST_LIMIT)

    assert result["rows"] == PARTIAL_ROWS
    assert result["duration_seconds"] >= 0
    assert result["throughput_rows_per_sec"] >= 0
    assert result["error"] == "RuntimeError: batch fetch failure"
    assert result["extra"]["error_count"] == EXPECTED_ERROR_COUNT


def test_pooled_sync_failure_before_any_row_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cur = _FakeBatchCursor([], fail_at_batch=FAIL_IMMEDIATELY)
    conn = _FakeSyncConnection(cur)
    pool = _FakeConnectionPool(conn)

    monkeypatch.setattr(
        pool_mod,
        "get_settings",
        lambda: SimpleNamespace(
            benchmark_batch_size=BATCH_SIZE,
            db_statement_timeout_ms=ZERO_TIMEOUT,
        ),
    )

    strategy = PooledSyncStrategy(batch_size=BATCH_SIZE, dsn_override="postgresql://test")
    monkeypatch.setattr(strategy, "_get_pool", lambda: pool)

    with pytest.raises(RuntimeError, match="immediate query failure"):
        strategy.execute(limit=TEST_LIMIT)
