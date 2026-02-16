"""Schema caching service."""

import logging
from typing import Any

from src.infrastructure.cache.bounded_cache import BoundedCache

logger = logging.getLogger(__name__)

_instance = BoundedCache[Any](max_size=500, ttl_seconds=3600)


class SchemaCache:
    """Caches schema information backed by BoundedCache."""

    @classmethod
    def get(cls, key: str) -> Any | None:
        return _instance.get(key)

    @classmethod
    def set(cls, key: str, value: Any) -> None:
        _instance.set(key, value)

    @classmethod
    def clear(cls) -> None:
        _instance.clear()
