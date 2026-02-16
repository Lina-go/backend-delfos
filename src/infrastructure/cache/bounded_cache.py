"""Generic bounded cache with TTL eviction."""

import threading
import time
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class BoundedCache(Generic[T]):
    """Thread-safe cache with max size and TTL."""

    def __init__(self, max_size: int = 200, ttl_seconds: int = 3600):
        self._max_size = max_size
        self._ttl = ttl_seconds
        self._cache: dict[str, tuple[T, float]] = {}
        self._lock = threading.Lock()
        self._hits = 0
        self._misses = 0

    def get(self, key: str) -> T | None:
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                self._misses += 1
                return None
            value, timestamp = entry
            if time.time() - timestamp > self._ttl:
                del self._cache[key]
                self._misses += 1
                return None
            self._hits += 1
            return value

    def set(self, key: str, value: T) -> None:
        with self._lock:
            if len(self._cache) >= self._max_size and key not in self._cache:
                oldest_key = min(self._cache, key=lambda k: self._cache[k][1])
                del self._cache[oldest_key]
            self._cache[key] = (value, time.time())

    def delete(self, key: str) -> bool:
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()
            self._hits = 0
            self._misses = 0

    def get_stats(self) -> dict[str, Any]:
        with self._lock:
            total = self._hits + self._misses
            return {
                "size": len(self._cache),
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": round(self._hits / total * 100, 2) if total > 0 else 0.0,
                "max_size": self._max_size,
                "ttl_seconds": self._ttl,
            }
