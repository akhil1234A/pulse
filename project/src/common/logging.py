"""Structured JSON logging with correlation ID support using contextvars."""

import logging
import sys
from contextvars import ContextVar
from typing import Any, Dict, Optional

try:
    from pythonjsonlogger import json as jsonlogger
except ImportError:
    from pythonjsonlogger import jsonlogger

_CORRELATION_ID_VAR: ContextVar[Optional[str]] = ContextVar("correlation_id", default=None)


def set_correlation_id(correlation_id: str) -> None:
    """Sets the correlation ID for the current async context."""
    _CORRELATION_ID_VAR.set(correlation_id)


def get_correlation_id() -> Optional[str]:
    """Retrieves the current correlation ID."""
    return _CORRELATION_ID_VAR.get()


class PulseStreamJsonFormatter(jsonlogger.JsonFormatter):
    """Custom JSON formatter injecting timestamp, correlation_id, and service metadata."""

    def add_fields(
        self,
        log_record: Dict[str, Any],
        record: logging.LogRecord,
        message_dict: Dict[str, Any],
    ) -> None:
        super().add_fields(log_record, record, message_dict)
        log_record["timestamp"] = self.formatTime(record, self.datefmt)
        log_record["level"] = record.levelname
        log_record["logger"] = record.name

        correlation_id = get_correlation_id()
        if correlation_id:
            log_record["correlation_id"] = correlation_id


def setup_logging(log_level: str = "INFO") -> None:
    """Configures structured JSON logging on the root logger."""
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level.upper())

    # Remove existing handlers to avoid duplicates
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)

    handler = logging.StreamHandler(sys.stdout)
    formatter = PulseStreamJsonFormatter("%(timestamp)s %(level)s %(logger)s %(message)s")
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)


def get_logger(name: str) -> logging.Logger:
    """Convenience getter for namespaced loggers."""
    return logging.getLogger(name)
