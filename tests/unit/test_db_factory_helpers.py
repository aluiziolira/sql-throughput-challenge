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
