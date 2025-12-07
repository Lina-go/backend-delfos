"""Schema caching service."""

import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class SchemaCache:
    """Caches schema information to reduce MCP calls."""

    _cache: Dict[str, Any] = {}

    @classmethod
    def get(cls, key: str) -> Optional[Any]:
        """Get cached schema data.
        
        Args:
            key: Cache key (typically "schema_{table_name}")
            
        Returns:
            Cached schema data or None if not found
        """
        return cls._cache.get(key)

    @classmethod
    def set(cls, key: str, value: Any) -> None:
        """Set cached schema data.
        
        Args:
            key: Cache key
            value: Schema data to cache
        """
        cls._cache[key] = value

    @classmethod
    def clear(cls) -> None:
        """Clear all cached schema data."""
        cls._cache.clear()

