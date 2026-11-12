from __future__ import annotations

from typing import Any

from src.infrastructure import db_factory

OVERRIDE_DSN = "postgresql://override"
DEFAULT_DSN = "postgresql://default"


class _FakeConnection:
    """Stand-in for psycopg.Connection that records how it was constructed."""

    def __init__(self, source: str) -> None:
        self.source = source


def test_resolve_sync_connection_uses_override_without_retry(monkeypatch) -> None:
    connect_calls: list[str] = []
    get_sync_calls: list[None] = []

    def fake_connect(dsn: str) -> _FakeConnection:
        connect_calls.append(dsn)
        return _FakeConnection(dsn)

    def fake_get_sync_connection() -> _FakeConnection:
        get_sync_calls.append(None)
        return _FakeConnection(DEFAULT_DSN)

    monkeypatch.setattr(db_factory.psycopg, "connect", fake_connect)
    monkeypatch.setattr(db_factory, "get_sync_connection", fake_get_sync_connection)

    conn = db_factory.resolve_sync_connection(OVERRIDE_DSN)

    assert isinstance(conn, _FakeConnection)
    assert conn.source == OVERRIDE_DSN
    assert connect_calls == [OVERRIDE_DSN]
    assert get_sync_calls == []


def test_resolve_sync_connection_falls_back_to_retrying_default(monkeypatch) -> None:
    connect_calls: list[str] = []
    get_sync_calls: list[None] = []

    def fake_connect(dsn: str) -> _FakeConnection:
        connect_calls.append(dsn)
        return _FakeConnection(dsn)

    def fake_get_sync_connection() -> _FakeConnection:
        get_sync_calls.append(None)
        return _FakeConnection(DEFAULT_DSN)

    monkeypatch.setattr(db_factory.psycopg, "connect", fake_connect)
    monkeypatch.setattr(db_factory, "get_sync_connection", fake_get_sync_connection)

    conn = db_factory.resolve_sync_connection(None)

    assert isinstance(conn, _FakeConnection)
    assert conn.source == DEFAULT_DSN
    assert connect_calls == []
    assert get_sync_calls == [None]


class _FakeCursor:
    def __init__(self, batches: list[list[tuple[Any, ...]]]) -> None:
        self._batches = list(batches)

    def fetchmany(self, batch_size: int) -> list[tuple[Any, ...]]:
        del batch_size
        if not self._batches:
            return []
        return self._batches.pop(0)


def test_fetch_in_batches_yields_until_empty() -> None:
    cursor = _FakeCursor([[(1,), (2,)], [(3,)], []])

    batches = list(db_factory.fetch_in_batches(cursor, batch_size=2))

    assert batches == [[(1,), (2,)], [(3,)]]


def test_fetch_in_batches_handles_no_rows() -> None:
    cursor = _FakeCursor([[]])

    batches = list(db_factory.fetch_in_batches(cursor, batch_size=10))

    assert batches == []


TIMEOUT_ZERO = 0
TIMEOUT_POSITIVE = 1500
TIMEOUT_POSITIVE_STR = "1500"
EXPECTED_SQL_PSYCOPG = "SELECT set_config('statement_timeout', %s, true)"
EXPECTED_SQL_ASYNCPG = "SELECT set_config('statement_timeout', $1, true)"


class _FakeTimeoutConnection:
    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, Any]] = []

    def execute(self, sql: str, params: Any = None) -> Any:
        self.execute_calls.append((sql, params))
        return None


class _FakeAsyncTimeoutConnection:
    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, Any]] = []

    async def execute(self, sql: str, *args: Any) -> str:
        self.execute_calls.append((sql, args))
        return "OK"


def test_apply_statement_timeout_noop_when_zero() -> None:
    conn = _FakeTimeoutConnection()
    db_factory.apply_statement_timeout(conn, TIMEOUT_ZERO)  # type: ignore[arg-type]
    assert conn.execute_calls == []


def test_apply_statement_timeout_positive_issues_set_config() -> None:
    conn = _FakeTimeoutConnection()
    db_factory.apply_statement_timeout(conn, TIMEOUT_POSITIVE)  # type: ignore[arg-type]
    assert conn.execute_calls == [(EXPECTED_SQL_PSYCOPG, (TIMEOUT_POSITIVE_STR,))]


async def test_async_apply_statement_timeout_noop_when_zero() -> None:
    conn = _FakeAsyncTimeoutConnection()
    await db_factory.async_apply_statement_timeout(conn, TIMEOUT_ZERO)
    assert conn.execute_calls == []


async def test_async_apply_statement_timeout_positive_issues_set_config() -> None:
    conn = _FakeAsyncTimeoutConnection()
    await db_factory.async_apply_statement_timeout(conn, TIMEOUT_POSITIVE)
    assert conn.execute_calls == [(EXPECTED_SQL_ASYNCPG, (TIMEOUT_POSITIVE_STR,))]
