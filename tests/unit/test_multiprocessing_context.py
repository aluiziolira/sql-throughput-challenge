from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from src.strategies.multiprocessing import MultiprocessingStrategy

EXPECTED_ROWS = 4
EXPECTED_RUN_CONTEXTS = 2
EXPECTED_PROCESSES = 2


class _FakeCursor:
    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> None:
        del sql, params

    def fetchall(self) -> list[tuple[int]]:
        return [(1,), (2,), (3,), (4,)]

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        del exc_type, exc, tb
        return False


class _FakeConnection:
    def cursor(self) -> _FakeCursor:
        return _FakeCursor()

    def __enter__(self) -> _FakeConnection:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        del exc_type, exc, tb
        return False


class _FakeAsyncResult:
    def __init__(self, results: list[tuple[int, str | None]]) -> None:
        self._results = results

    def get(self, timeout: int) -> list[tuple[int, str | None]]:
        del timeout
        return self._results


class _FakePool:
    def __init__(self) -> None:
        self.map_async_calls = 0

    def map_async(self, worker, work_items):
        del worker
        self.map_async_calls += 1
        results = [(len(work.ids), None) for work in work_items]
        return _FakeAsyncResult(results)

    def terminate(self) -> None:
        return None

    def join(self) -> None:
        return None

    def __enter__(self) -> _FakePool:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        del exc_type, exc, tb
        return False


class _FakeContext:
    def __init__(self) -> None:
        self.pool_processes: list[int | None] = []

    def Pool(self, processes: int | None = None) -> _FakePool:  # noqa: N802
        self.pool_processes.append(processes)
        return _FakePool()


def test_multiprocessing_init_does_not_set_global_start_method(monkeypatch) -> None:
    def fail_if_called(method: str, force: bool = False) -> None:
        del method, force
        raise AssertionError("set_start_method must not be called in strategy init")

    monkeypatch.setattr("src.strategies.multiprocessing.mp.set_start_method", fail_if_called)

    MultiprocessingStrategy(
        processes=EXPECTED_PROCESSES,
        chunk_size=EXPECTED_PROCESSES,
        dsn_override="postgresql://test",
    )


def test_multiprocessing_execute_uses_local_spawn_context_and_supports_repeated_runs(
    monkeypatch,
) -> None:
    contexts: list[_FakeContext] = []

    def fake_get_context(method: str) -> _FakeContext:
        assert method == "spawn"
        context = _FakeContext()
        contexts.append(context)
        return context

    monkeypatch.setattr("src.strategies.multiprocessing.mp.get_context", fake_get_context)
    monkeypatch.setattr(
        "src.strategies.multiprocessing.psycopg.connect",
        lambda dsn: _FakeConnection(),
    )
    monkeypatch.setattr(
        "src.strategies.multiprocessing.get_settings",
        lambda: SimpleNamespace(db_statement_timeout_ms=0),
    )

    strategy = MultiprocessingStrategy(
        processes=EXPECTED_PROCESSES,
        chunk_size=EXPECTED_PROCESSES,
        dsn_override="postgresql://test",
    )

    first = strategy.execute(limit=EXPECTED_ROWS)
    second = strategy.execute(limit=EXPECTED_ROWS)

    assert first["rows"] == EXPECTED_ROWS
    assert second["rows"] == EXPECTED_ROWS
    assert first["error"] is None
    assert second["error"] is None

    assert len(contexts) == EXPECTED_RUN_CONTEXTS
    assert contexts[0].pool_processes == [EXPECTED_PROCESSES]
    assert contexts[1].pool_processes == [EXPECTED_PROCESSES]
