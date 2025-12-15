"""Structured logging."""

import json
import logging
from datetime import datetime
from typing import Any


class StructuredLogger:
    """Structured logger for pipeline events."""

    def __init__(self, name: str = __name__):
        """Initialize structured logger."""
        self.logger = logging.getLogger(name)

    def log_step(
        self,
        step: str,
        state: dict[str, Any],
        duration_ms: float | None = None,
    ) -> None:
        """Log a pipeline step."""
        log_data: dict[str, Any] = {
            "step": step,
            "timestamp": datetime.utcnow().isoformat(),
            "state": state,
        }
        if duration_ms:
            log_data["duration_ms"] = duration_ms

        self.logger.info(json.dumps(log_data))

    def log_error(
        self,
        step: str,
        error: Exception,
        context: dict[str, Any] | None = None,
    ) -> None:
        """Log an error with context."""
        log_data: dict[str, Any] = {
            "step": step,
            "error": str(error),
            "error_type": type(error).__name__,
            "timestamp": datetime.utcnow().isoformat(),
        }
        if context:
            log_data["context"] = context

        self.logger.error(json.dumps(log_data), exc_info=True)
