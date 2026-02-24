"""Semantic cache for ChatV2 — embed questions, find similar cached results.

Uses Azure OpenAI embeddings + cosine similarity over an in-memory BoundedCache.
Pattern based on: github.com/Shailender-Youtube/prompt-and-semantic-caching-azure
"""

import logging
from typing import Any

import numpy as np
from openai import AzureOpenAI

from src.infrastructure.cache.bounded_cache import BoundedCache

logger = logging.getLogger(__name__)


class SemanticCacheV2:
    """In-memory semantic cache using Azure OpenAI embeddings + cosine similarity."""

    def __init__(
        self,
        endpoint: str,
        api_key: str,
        deployment: str = "text-embedding-3-small",
        threshold: float = 0.82,
        max_size: int = 200,
        ttl_seconds: int = 1800,
    ):
        self._client = AzureOpenAI(
            azure_endpoint=endpoint,
            api_key=api_key,
            api_version="2024-06-01",
        )
        self._deployment = deployment
        self._threshold = threshold
        self._cache: BoundedCache[dict[str, Any]] = BoundedCache(
            max_size=max_size, ttl_seconds=ttl_seconds,
        )
        self._keys: list[str] = []

    def embed(self, text: str) -> list[float]:
        """Convert text to embedding vector via Azure OpenAI."""
        resp = self._client.embeddings.create(model=self._deployment, input=text)
        return resp.data[0].embedding

    @staticmethod
    def cosine_similarity(a: list[float], b: list[float]) -> float:
        va, vb = np.array(a), np.array(b)
        return float(np.dot(va, vb) / (np.linalg.norm(va) * np.linalg.norm(vb)))

    def search(self, query_embedding: list[float]) -> tuple[dict[str, Any] | None, float]:
        """Search cache for semantically similar question. Returns (result, score)."""
        best_result, best_score, best_key = None, 0.0, ""
        for key in list(self._keys):
            entry = self._cache.get(key)
            if entry is None:
                self._keys.remove(key)
                continue
            score = self.cosine_similarity(query_embedding, entry["embedding"])
            if score > best_score:
                best_score = score
                best_result = entry["result"]
                best_key = key
        if best_score >= self._threshold:
            logger.info("[SEMANTIC CACHE] HIT (score=%.3f, key=%s)", best_score, best_key[:40])
            return best_result, best_score
        if self._keys:
            logger.info(
                "[SEMANTIC CACHE] MISS (best_score=%.3f < threshold=%.2f, best_key=%s)",
                best_score, self._threshold, best_key[:40],
            )
        return None, best_score

    def store(
        self, key: str, question: str, result: dict[str, Any], embedding: list[float],
    ) -> None:
        """Store a result with its embedding."""
        self._cache.set(key, {
            "question": question,
            "embedding": embedding,
            "result": result,
        })
        if key not in self._keys:
            self._keys.append(key)
        logger.info("[SEMANTIC CACHE] Stored: %s (keys=%d)", key[:30], len(self._keys))

    def clear(self) -> None:
        """Flush all cached entries."""
        self._cache.clear()
        self._keys.clear()
        logger.info("[SEMANTIC CACHE] Cleared")

    def get_stats(self) -> dict[str, Any]:
        stats = self._cache.get_stats()
        stats["threshold"] = self._threshold
        return stats
