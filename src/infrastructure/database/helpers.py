"""Shared database operation helpers."""

import logging
from typing import Any

from fastapi import HTTPException

logger = logging.getLogger(__name__)


def check_db_result(result: dict[str, Any], operation: str) -> None:
    """Raise HTTPException if a database operation failed."""
    if not result.get("success") or result.get("error"):
        logger.error("Failed to %s: %s", operation, result.get("error"))
        raise HTTPException(status_code=500, detail=f"Failed to {operation}")


def audit_log(operation: str, resource: str, resource_id: str) -> None:
    """Log CRUD operations for audit trail."""
    logger.info("AUDIT %s %s id=%s", operation, resource, resource_id)
