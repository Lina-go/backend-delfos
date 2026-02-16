"""Structured JSON logging for production."""

import contextvars
import json
import logging
import sys
import traceback
from datetime import UTC, datetime
from typing import Any


class JSONFormatter(logging.Formatter):
    """
    Formats log records as JSON for structured logging.

    Output format:
    {
        "timestamp": "2025-01-20T14:30:00.123456Z",
        "level": "INFO",
        "logger": "src.services.sql.executor",
        "message": "SQL executed successfully",
        "context": {...},  # optional extra data
        "error": {...}     # optional error info
    }
    """

    def format(self, record: logging.LogRecord) -> str:
        log_data: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Add extra context if provided via extra={}
        if hasattr(record, "context") and record.context:
            log_data["context"] = record.context

        # Add request_id if available (for tracing)
        if hasattr(record, "request_id") and record.request_id:
            log_data["request_id"] = record.request_id

        # Add error info if this is an exception
        if record.exc_info:
            log_data["error"] = {
                "type": record.exc_info[0].__name__ if record.exc_info[0] else "Unknown",
                "message": str(record.exc_info[1]) if record.exc_info[1] else "",
                "traceback": "".join(traceback.format_exception(*record.exc_info)),
            }

        # Add source location for debugging
        if record.levelno >= logging.WARNING:
            log_data["source"] = {
                "file": record.pathname,
                "line": record.lineno,
                "function": record.funcName,
            }

        return json.dumps(log_data, default=str, ensure_ascii=False)


class ConsoleFormatter(logging.Formatter):
    """Human-readable console formatter for development."""

    COLORS = {
        "DEBUG": "\033[36m",  # Cyan
        "INFO": "\033[32m",  # Green
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",  # Red
        "CRITICAL": "\033[35m",  # Magenta
    }
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        color = self.COLORS.get(record.levelname, "")
        timestamp = datetime.now().strftime("%H:%M:%S")

        # Shorten logger name for readability
        logger_name = record.name
        if logger_name.startswith("src."):
            logger_name = logger_name[4:]
        if len(logger_name) > 30:
            logger_name = "..." + logger_name[-27:]

        base = f"{color}{timestamp} [{record.levelname:^7}]{self.RESET} {logger_name}: {record.getMessage()}"

        if record.exc_info:
            base += "\n" + "".join(traceback.format_exception(*record.exc_info))

        return base


def setup_logging(
    level: str = "INFO",
    json_output: bool = True,
    silence_noisy_loggers: bool = True,
) -> None:
    """
    Configure application logging.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        json_output: If True, output JSON format. If False, human-readable console format.
        silence_noisy_loggers: If True, reduce verbosity of third-party loggers.
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))

    # Remove existing handlers
    root_logger.handlers.clear()

    # Create handler with appropriate formatter
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(getattr(logging, level.upper()))

    if json_output:
        handler.setFormatter(JSONFormatter())
    else:
        handler.setFormatter(ConsoleFormatter())

    root_logger.addHandler(handler)

    # Silence noisy third-party loggers
    if silence_noisy_loggers:
        noisy_loggers = [
            "uvicorn",
            "uvicorn.access",
            "uvicorn.error",
            "httpx",
            "httpcore",
            "azure",
            "azure.core",
            "azure.identity",
            "urllib3",
            "asyncio",
        ]
        for logger_name in noisy_loggers:
            logging.getLogger(logger_name).setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Get a logger with the given name."""
    return logging.getLogger(name)


_log_context: contextvars.ContextVar[dict[str, Any]] = contextvars.ContextVar(
    "log_context",
)


class LogContext:
    """Context manager for adding context to log messages.

    Thread-safe and async-safe using contextvars.

    Usage:
        with LogContext(request_id="abc123", user_id="user1"):
            logger.info("Processing request")
    """

    def __init__(self, **kwargs: Any):
        self.new_context = kwargs
        self._token: contextvars.Token[dict[str, Any]] | None = None

    def __enter__(self) -> "LogContext":
        current = _log_context.get({}).copy()
        current.update(self.new_context)
        self._token = _log_context.set(current)
        return self

    def __exit__(self, *args: Any) -> None:
        if self._token is not None:
            _log_context.reset(self._token)

    @classmethod
    def get_context(cls) -> dict[str, Any]:
        return _log_context.get({}).copy()
