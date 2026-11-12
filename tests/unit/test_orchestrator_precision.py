from __future__ import annotations

from types import SimpleNamespace

from src.orchestrator import _merge_result

ROWS_COUNT = 10_000
RAW_DURATION = 0.2649
ROUNDED_DURATION = 0.26
EXPECTED_THROUGHPUT = 37750.09
QUANTIZED_THROUGHPUT = 38461.54


def test_merge_result_computes_throughput_from_unrounded_duration() -> None:
    stats = SimpleNamespace(
        duration_seconds=RAW_DURATION,
        peak_rss_bytes=None,
        cpu_percent=None,
        label="test_precision",
        start_ts=0.0,
        end_ts=RAW_DURATION,
    )
    result = {"rows": ROWS_COUNT}
    merged = _merge_result(result, stats)

    # duration_seconds is rounded to 2 decimals
    assert merged["duration_seconds"] == ROUNDED_DURATION
    # throughput is calculated with unrounded duration, avoiding quantizing to 38461.54
    assert merged["throughput_rows_per_sec"] == EXPECTED_THROUGHPUT
    assert merged["throughput_rows_per_sec"] != QUANTIZED_THROUGHPUT
