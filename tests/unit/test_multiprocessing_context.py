from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from src.strategies.multiprocessing import MultiprocessingStrategy

EXPECTED_ROWS = 4
EXPECTED_RUN_CONTEXTS = 2
EXPECTED_PROCESSES = 2
PARTIAL_SUCCESS_ROWS = 2
EXPECTED_ERROR_CHUNKS = 2
EXPECTED_TIMEOUT_CHUNKS_COMPLETED = 1
TIMEOUT_TOTAL_ROWS = 6
TIMEOUT_TOTAL_CHUNKS = 3
TIMEOUT_COMPLETED_CHUNKS = 1
TIMEOUT_INCOMPLETE_CHUNKS = 2
TIMEOUT_FAILED_CHUNKS = 0
TIMEOUT_ERROR_COUNT = 2
CHUNK_SIZE_TWO = 2
WORKER_TIMEOUT_ONE_SEC = 1
SINGLE_TERMINATE_CALL = 1
DEADLINE_INITIAL_TICK = 100.0
DEADLINE_EXPIRED_TICK = 102.0


class _FakeCursor:
    def __init__(self, ids: list[int] | None = None) -> None:
        self._ids = ids if ids is not None else [1, 2, 3, 4]

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> None:
        del sql
        if params and len(params) == 1 and isinstance(params[0], int):
            self._ids = list(range(1, params[0] + 1))

    def fetchall(self) -> list[tuple[int]]:
        return [(i,) for i in self._ids]

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        del exc_type, exc, tb
        return False


class _FakeConnection:
    def execute(self, sql: str, params: Any = None) -> None:
        del sql, params

    def cursor(self, *args: Any, **kwargs: Any) -> _FakeCursor:
        del args, kwargs
        return _FakeCursor()

    def __enter__(self) -> _FakeConnection:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        del exc_type, exc, tb
        return False


class _FakeIMapIterator:
    def __init__(self, results: list[tuple[int, str | None]]) -> None:
        self._results = list(results)
        self._index = 0

    def next(self, timeout: float | None = None) -> tuple[int, str | None]:
        del timeout
        if self._index >= len(self._results):
            raise StopIteration
        value = self._results[self._index]
        self._index += 1
        return value


class _FakePool:
    def __init__(self, results: list[tuple[int, str | None]] | None = None) -> None:
        self.imap_unordered_calls = 0
        self.terminate_calls = 0
        self._results = results

    def imap_unordered(self, worker, work_items):
        del worker
        self.imap_unordered_calls += 1
        if self._results is not None:
            return _FakeIMapIterator(self._results)
        results = [(len(work.ids), None) for work in work_items]
        return _FakeIMapIterator(results)

    def terminate(self) -> None:
        self.terminate_calls += 1

    def join(self) -> None:
        return None

    def __enter__(self) -> _FakePool:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        del exc_type, exc, tb
        return False


class _FakeContext:
    def __init__(self, pool: _FakePool | None = None) -> None:
        self.pool_processes: list[int | None] = []
        self._pool = pool

    def Pool(self, processes: int | None = None) -> _FakePool:  # noqa: N802
        self.pool_processes.append(processes)
        if self._pool is not None:
            return self._pool
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
    assert first["extra"]["error_count"] == 0
    assert second["extra"]["error_count"] == 0

    assert len(contexts) == EXPECTED_RUN_CONTEXTS
    assert contexts[0].pool_processes == [EXPECTED_PROCESSES]
    assert contexts[1].pool_processes == [EXPECTED_PROCESSES]


def test_multiprocessing_returned_worker_errors_are_counted_without_double_counting_rows(
    monkeypatch,
) -> None:
    pool = _FakePool(
        results=[
            (2, None),
            (0, "Worker ids[2] 3..4: boom"),
        ]
    )
    context = _FakeContext(pool=pool)

    monkeypatch.setattr(
        "src.strategies.multiprocessing.mp.get_context",
        lambda method: context,
    )
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
    result = strategy.execute(limit=EXPECTED_ROWS)

    assert result["rows"] == PARTIAL_SUCCESS_ROWS
    assert result["error"] is not None
    assert "boom" in result["error"]
    assert result["extra"]["error_count"] == 1
    assert result["extra"]["chunks_completed"] == EXPECTED_ERROR_CHUNKS
    assert result["extra"]["timed_out"] is False
    assert pool.imap_unordered_calls == 1


def test_multiprocessing_timeout_reports_partial_progress(monkeypatch) -> None:
    class _TimeoutIterator:
        def __init__(self) -> None:
            self._calls = 0

        def next(self, timeout: float | None = None) -> tuple[int, str | None]:
            del timeout
            self._calls += 1
            if self._calls == 1:
                return (PARTIAL_SUCCESS_ROWS, None)
            raise TimeoutError()

    class _TimeoutPool(_FakePool):
        def imap_unordered(self, worker: Any, work_items: Any) -> _TimeoutIterator:
            del worker, work_items
            self.imap_unordered_calls += 1
            return _TimeoutIterator()

    pool = _TimeoutPool()
    context = _FakeContext(pool=pool)

    monkeypatch.setattr(
        "src.strategies.multiprocessing.mp.get_context",
        lambda method: context,
    )
    monkeypatch.setattr(
        "src.strategies.multiprocessing.mp.TimeoutError",
        TimeoutError,
    )
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
        chunk_size=CHUNK_SIZE_TWO,
        dsn_override="postgresql://test",
        worker_timeout_seconds=WORKER_TIMEOUT_ONE_SEC,
    )
    result = strategy.execute(limit=TIMEOUT_TOTAL_ROWS)

    assert result["rows"] == PARTIAL_SUCCESS_ROWS
    assert result["error"] is not None
    assert "timed out" in result["error"]
    assert result["extra"]["timed_out"] is True
    assert result["extra"]["chunks_completed"] == TIMEOUT_COMPLETED_CHUNKS
    assert result["extra"]["chunks_total"] == TIMEOUT_TOTAL_CHUNKS
    assert result["extra"]["chunks_failed"] == TIMEOUT_FAILED_CHUNKS
    assert result["extra"]["chunks_incomplete"] == TIMEOUT_INCOMPLETE_CHUNKS
    assert result["extra"]["error_count"] == TIMEOUT_ERROR_COUNT
    assert pool.terminate_calls == SINGLE_TERMINATE_CALL


def test_multiprocessing_deadline_expired_before_next_raises_timeout_and_terminates(
    monkeypatch,
) -> None:
    pool = _FakePool()
    context = _FakeContext(pool=pool)

    # First call for deadline setup: 100.0. Second call in loop: 102.0 (remaining <= 0).
    monotonic_ticks = iter([DEADLINE_INITIAL_TICK, DEADLINE_EXPIRED_TICK])

    monkeypatch.setattr(
        "src.strategies.multiprocessing.mp.get_context",
        lambda method: context,
    )
    monkeypatch.setattr(
        "src.strategies.multiprocessing.time.monotonic",
        lambda: next(monotonic_ticks),
    )
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
        worker_timeout_seconds=WORKER_TIMEOUT_ONE_SEC,
    )
    result = strategy.execute(limit=EXPECTED_ROWS)

    assert result["extra"]["timed_out"] is True
    assert pool.terminate_calls == SINGLE_TERMINATE_CALL
    assert "timed out" in (result["error"] or "")
