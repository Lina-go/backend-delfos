"""Cache management endpoints."""

import logging
from typing import Any

from fastapi import APIRouter, Depends

from src.config.settings import Settings, get_settings
from src.infrastructure.cache.semantic_cache import SemanticCache

logger = logging.getLogger(__name__)

router = APIRouter()


def _get_semantic_cache_v2(settings: Settings) -> Any:
    """Import and return the singleton SemanticCacheV2 from the agent module."""
    from src.services.chat_v2.agent import _get_semantic_cache
    return _get_semantic_cache(settings)


@router.get("/stats")
async def get_cache_stats(
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    """Return cache hit/miss statistics for both caches."""
    stats: dict[str, Any] = {"legacy": SemanticCache.get_stats()}
    sc = _get_semantic_cache_v2(settings)
    if sc is not None:
        stats["semantic_v2"] = sc.get_stats()
    return stats


@router.delete("/")
async def clear_cache(
    settings: Settings = Depends(get_settings),
) -> dict[str, str]:
    """Invalidate all cached results (legacy + semantic v2)."""
    logger.warning("Cache cleared via API request — affects all users")
    SemanticCache.clear()
    sc = _get_semantic_cache_v2(settings)
    if sc is not None:
        sc.clear()
    return {"message": "Cache cleared successfully", "status": "success"}
