"""
Structured logging utilities for the SQL Throughput Challenge.

This stub centralizes logging configuration to keep the CLI, orchestrator,
and strategies consistent. It favors standard library logging with a
human-readable formatter by default and an optional JSON formatter for
structured logs (useful for pipelines/CI).

Usage:
    from src.utils.logging import configure_logging, get_logger

    configure_logging(level="INFO", json=False)
    log = get_logger(__name__)
    log.info("message", extra={"rows": 1000})
"""

from __future__ import annotations

import json
import logging
import logging.config
from typing import Any, Dict, Optional


def _json_formatter(record: logging.LogRecord) -> str:
    """Render a log record as JSON string."""
    payload: Dict[str, Any] = {
        "level": record.levelname,
        "logger": record.name,
        "message": record.getMessage(),
    }
    if record.exc_info:
        payload["exc_info"] = logging.Formatter().formatException(record.exc_info)
    if record.stack_info:
        payload["stack_info"] = record.stack_info
    if hasattr(record, "extra") and isinstance(record.extra, dict):
        payload.update(record.extra)
    return json.dumps(payload)


class JsonFormatter(logging.Formatter):
    """Minimal JSON formatter for structured logs."""

    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
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


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Get a logger with the given name. If name is None, returns the root logger.
    """
    return logging.getLogger(name)


__all__ = ["configure_logging", "get_logger", "JsonFormatter"]
