from __future__ import annotations

import json
import logging

from src.utils.logging import _json_formatter

EXPECTED_ROWS = 10
EXPECTED_BATCH_SIZE = 1000


def test_json_formatter_promotes_standard_extra_fields() -> None:
    record = logging.LogRecord(
        name="test.logger",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="hello",
        args=(),
        exc_info=None,
    )
    record.rows = EXPECTED_ROWS
    record.strategy = "naive"

    payload = json.loads(_json_formatter(record))

    assert payload["level"] == "INFO"
    assert payload["logger"] == "test.logger"
    assert payload["message"] == "hello"
    assert payload["rows"] == EXPECTED_ROWS
    assert payload["strategy"] == "naive"


def test_json_formatter_supports_legacy_nested_extra_field() -> None:
    record = logging.LogRecord(
        name="test.logger",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="hello",
        args=(),
        exc_info=None,
    )
    record.extra = {"batch_size": EXPECTED_BATCH_SIZE}

    payload = json.loads(_json_formatter(record))

    assert payload["batch_size"] == EXPECTED_BATCH_SIZE
