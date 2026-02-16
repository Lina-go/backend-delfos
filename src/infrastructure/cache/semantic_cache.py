"""Semantic cache for NL -> SQL/Results."""

import logging
from typing import Any

from src.infrastructure.cache.bounded_cache import BoundedCache

logger = logging.getLogger(__name__)

_instance = BoundedCache[Any](max_size=200, ttl_seconds=1800)


class SemanticCache:
    """Semantic cache for query results backed by BoundedCache."""

    @classmethod
    def get(cls, key: str) -> Any | None:
        return _instance.get(key)

    @classmethod
    def set(cls, key: str, value: Any) -> None:
        _instance.set(key, value)

    @classmethod
    def delete(cls, key: str) -> bool:
        return _instance.delete(key)

    @classmethod
    def clear(cls) -> None:
        _instance.clear()

    @classmethod
    def get_stats(cls) -> dict[str, Any]:
        return _instance.get_stats()
