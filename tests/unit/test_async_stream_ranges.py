from src.strategies.async_stream import _split_limit_ranges


def _flatten_ranges(ranges: list[tuple[int, int]]) -> list[int]:
    return [value for start, end in ranges for value in range(start, end)]


def test_split_limit_ranges_handles_limit_less_than_concurrency():
    ranges = _split_limit_ranges(limit=3, concurrency=5)
    assert ranges == [(0, 1), (1, 2), (2, 3)]


def test_split_limit_ranges_distributes_remainder():
    ranges = _split_limit_ranges(limit=10, concurrency=3)
    assert ranges == [(0, 4), (4, 7), (7, 10)]


def test_split_limit_ranges_covers_exact_limit():
    limit = 12
    ranges = _split_limit_ranges(limit=limit, concurrency=4)
    assert _flatten_ranges(ranges) == list(range(0, limit))
