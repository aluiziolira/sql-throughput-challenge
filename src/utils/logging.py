"""
Structured logging utilities for the SQL Throughput Challenge.

Centralizes logging configuration so CLI, orchestrator, and strategies share
the same handlers/formatters. It favors standard library logging with a
human-readable formatter by default and an optional JSON formatter for
structured logs (useful for pipelines/CI).

Usage:
    from src.utils.logging import configure_logging, get_logger

    configure_logging(level="INFO", json_logs=False)
    log = get_logger(__name__)
    log.info("message", extra={"rows": 1000, "strategy": "naive"})
"""

from __future__ import annotations

import json
import logging
import logging.config
from typing import Any

_STANDARD_LOG_RECORD_FIELDS = frozenset(logging.makeLogRecord({}).__dict__.keys())


def _record_extra_fields(record: logging.LogRecord) -> dict[str, Any]:
    """Extract non-standard LogRecord fields injected via logging `extra=`."""
    extra_fields: dict[str, Any] = {}

    for key, value in record.__dict__.items():
        if key in _STANDARD_LOG_RECORD_FIELDS or key in {"message", "asctime", "extra"}:
            continue
        extra_fields[key] = value

    # Backward-compatible support for callers that set `extra={"extra": {...}}`.
    legacy_extra = getattr(record, "extra", None)
    if isinstance(legacy_extra, dict):
        extra_fields.update(legacy_extra)

    return extra_fields


def _json_formatter(record: logging.LogRecord) -> str:
    """
    Render a log record as a JSON string.

    Output always includes `level`, `logger`, and `message`.
    Any non-standard fields added through `logging`'s `extra=` argument are
    promoted to top-level JSON keys.
    """
    payload: dict[str, Any] = {
        "level": record.levelname,
        "logger": record.name,
        "message": record.getMessage(),
    }
    if record.exc_info:
        payload["exc_info"] = logging.Formatter().formatException(record.exc_info)
    if record.stack_info:
        payload["stack_info"] = record.stack_info
    payload.update(_record_extra_fields(record))
    return json.dumps(payload)


class JsonFormatter(logging.Formatter):
    """JSON formatter for structured logs."""

    def format(self, record: logging.LogRecord) -> str:
        return _json_formatter(record)


def configure_logging(
    level: str = "INFO",
    json_logs: bool = False,
    force: bool = True,
) -> None:
    """
    Configure root logging.

    Parameters
    ----------
    level : str
        Logging level name (e.g., "DEBUG", "INFO", "WARNING").
    json_logs : bool
        Whether to emit logs as JSON. If False, uses a concise human formatter.
    force : bool
        Whether to override existing logging configuration (recommended in CLI apps).
    """
    formatter_name = "json" if json_logs else "console"

    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "console": {
                    "format": "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
                    "datefmt": "%Y-%m-%d %H:%M:%S",
                },
                "json": {
                    "()": JsonFormatter,
                },
            },
            "handlers": {
                "default": {
                    "class": "logging.StreamHandler",
                    "formatter": formatter_name,
                    "level": level,
                }
            },
            "root": {
                "handlers": ["default"],
                "level": level,
            },
        }
    )


def get_logger(name: str | None = None) -> logging.Logger:
    """
    Get a logger with the given name. If name is None, returns the root logger.
    """
    return logging.getLogger(name)


__all__ = ["JsonFormatter", "configure_logging", "get_logger"]
