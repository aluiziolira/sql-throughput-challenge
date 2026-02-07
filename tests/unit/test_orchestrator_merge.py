from src.orchestrator import _merge_result
from src.utils.profiler import ProfileStats

# Expected values after orchestrator profiler overrides strategy timing
EXPECTED_DURATION = 2.0  # Profiler duration (not strategy's 1.0)
EXPECTED_THROUGHPUT = 50.0  # Recalculated: 100 rows / 2.0 seconds
EXPECTED_PEAK_RSS = 123  # Profiler memory measurement
EXPECTED_CPU = 12.3  # Profiler CPU usage (rounded to 1 decimal)


def test_merge_result_overrides_strategy_timing():
    result = {
        "rows": 100,
        "duration_seconds": 1.0,
        "throughput_rows_per_sec": 100.0,
        "peak_rss_bytes": 999,
        "cpu_percent": 99.9,
    }
    stats = ProfileStats(
        label="test",
        start_ts=1.0,
        end_ts=3.0,
        duration_seconds=2.0,
        peak_rss_bytes=123,
        cpu_percent=12.34,
    )

    merged = _merge_result(result, stats)

    assert merged["duration_seconds"] == EXPECTED_DURATION
    assert merged["throughput_rows_per_sec"] == EXPECTED_THROUGHPUT
    assert merged["peak_rss_bytes"] == EXPECTED_PEAK_RSS
    assert merged["cpu_percent"] == EXPECTED_CPU
