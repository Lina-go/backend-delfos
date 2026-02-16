"""Cache management endpoints."""

import logging
from typing import Any

from fastapi import APIRouter

from src.infrastructure.cache.semantic_cache import SemanticCache

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/stats")
async def get_cache_stats() -> dict[str, Any]:
    """Return cache hit/miss statistics."""
    return SemanticCache.get_stats()


@router.delete("/")
async def clear_cache() -> dict[str, str]:
    """Invalidate all cached SQL generation results."""
    logger.warning("Cache cleared via API request — affects all users")
    SemanticCache.clear()
    return {"message": "Cache cleared successfully", "status": "success"}
