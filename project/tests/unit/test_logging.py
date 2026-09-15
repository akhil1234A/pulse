"""Unit tests for structured logging and correlation ID support."""

import json
import logging
import pytest
from src.common.logging import (
    PulseStreamJsonFormatter,
    get_correlation_id,
    get_logger,
    set_correlation_id,
    setup_logging,
)


def test_correlation_id_context():
    set_correlation_id(None)
    assert get_correlation_id() is None
    set_correlation_id("test-corr-123")
    assert get_correlation_id() == "test-corr-123"
    set_correlation_id(None)


def test_json_formatter_with_correlation_id():
    set_correlation_id("corr-xyz-987")
    formatter = PulseStreamJsonFormatter("%(timestamp)s %(level)s %(logger)s %(message)s")

    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="test.py",
        lineno=10,
        msg="Processing batch completed",
        args=(),
        exc_info=None,
    )

    formatted_str = formatter.format(record)
    parsed = json.loads(formatted_str)

    assert parsed["level"] == "INFO"
    assert parsed["logger"] == "test_logger"
    assert parsed["message"] == "Processing batch completed"
    assert parsed["correlation_id"] == "corr-xyz-987"
    assert "timestamp" in parsed


def test_setup_logging_and_get_logger():
    setup_logging(log_level="DEBUG")
    logger = get_logger("my_service")
    assert logger.name == "my_service"
