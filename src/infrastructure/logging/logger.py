"""Structured logging."""

import logging
import json
from typing import Dict, Any, Optional
from datetime import datetime


class StructuredLogger:
    """Structured logger for pipeline events."""

    def __init__(self, name: str = __name__):
        """Initialize structured logger."""
        self.logger = logging.getLogger(name)

    def log_step(
        self,
        step: str,
        state: Dict[str, Any],
        duration_ms: Optional[float] = None,
    ):
        """Log a pipeline step."""
        log_data = {
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
        context: Optional[Dict[str, Any]] = None,
    ):
        """Log an error with context."""
        log_data = {
            "step": step,
            "error": str(error),
            "error_type": type(error).__name__,
            "timestamp": datetime.utcnow().isoformat(),
        }
        if context:
            log_data["context"] = context

        self.logger.error(json.dumps(log_data), exc_info=True)

