"""Semantic cache using Azure OpenAI embeddings and cosine similarity."""

import logging
import re
import unicodedata
from typing import Any

import numpy as np
from openai import AzureOpenAI

from src.config.database.concepts import CONCEPT_TO_TABLES
from src.infrastructure.cache.bounded_cache import BoundedCache

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Concept fingerprinting — derived automatically from CONCEPT_TO_TABLES
# ---------------------------------------------------------------------------

_CONCEPT_KEYS: list[str] = sorted(CONCEPT_TO_TABLES.keys(), key=len, reverse=True)
_SQL_TABLE_RE = re.compile(r"gold\.\w+", re.IGNORECASE)


def _strip_accents(text: str) -> str:
    """Remove diacritical marks (accents) from text."""
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _extract_concepts(text: str) -> frozenset[str]:
    """Extract all matching CONCEPT_TO_TABLES keys from text (accent-insensitive)."""
    normalized = _strip_accents(text.lower())
    return frozenset(c for c in _CONCEPT_KEYS if c in normalized)


def _extract_sql_tables(sql: str) -> frozenset[str]:
    """Extract table names (gold.xxx) from a SQL query string."""
    return frozenset(m.lower() for m in _SQL_TABLE_RE.findall(sql))


# Pre-compute specialization pairs once at import time.
# A pair (generic, specific) means "specific" is a word-superset of "generic"
# and both map to at least one shared table.
def _build_specialization_pairs() -> frozenset[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    for i, a in enumerate(_CONCEPT_KEYS):
        words_a = frozenset(a.split())
        tables_a = frozenset(CONCEPT_TO_TABLES[a])
        for b in _CONCEPT_KEYS[i + 1 :]:
            words_b = frozenset(b.split())
            tables_b = frozenset(CONCEPT_TO_TABLES[b])
            if not tables_a & tables_b:
                continue
            if words_a < words_b:
                pairs.add((a, b))
            elif words_b < words_a:
                pairs.add((b, a))
    return frozenset(pairs)


_SPECIALIZATION_PAIRS: frozenset[tuple[str, str]] = _build_specialization_pairs()


def _concepts_compatible(
    query_concepts: frozenset[str], cached_concepts: frozenset[str],
) -> bool:
    """Return False if a specialization is present in one set but not the other."""
    for _generic, specific in _SPECIALIZATION_PAIRS:
        if (specific in query_concepts) != (specific in cached_concepts):
            return False
    return True


def _tables_for_concepts(concepts: frozenset[str]) -> frozenset[str]:
    """Resolve the set of tables that a concept fingerprint references."""
    tables: set[str] = set()
    for concept in concepts:
        tables.update(CONCEPT_TO_TABLES.get(concept, []))
    return frozenset(tables)


class SemanticCacheV2:
    """In-memory semantic cache with configurable similarity threshold."""

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
        """Convert text to an embedding vector (sync)."""
        resp = self._client.embeddings.create(model=self._deployment, input=text)
        return resp.data[0].embedding

    async def embed_async(self, text: str) -> list[float]:
        """Convert text to an embedding vector without blocking the event loop."""
        import asyncio
        return await asyncio.to_thread(self.embed, text)

    @staticmethod
    def cosine_similarity(a: list[float], b: list[float]) -> float:
        va, vb = np.array(a), np.array(b)
        return float(np.dot(va, vb) / (np.linalg.norm(va) * np.linalg.norm(vb)))

    def search(
        self, query_embedding: list[float], query_text: str = "",
    ) -> tuple[dict[str, Any] | None, float]:
        """Find the best matching cached result by cosine similarity."""
        best_result, best_score, best_key = None, 0.0, ""
        best_concepts: frozenset[str] = frozenset()
        best_sql_tables: frozenset[str] = frozenset()
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
                best_concepts = entry.get("concepts", frozenset())
                best_sql_tables = entry.get("sql_tables", frozenset())
        if best_score >= self._threshold:
            # Extract query concepts once — reused by both layers
            query_concepts = _extract_concepts(query_text) if query_text else frozenset()

            # Layer 1: concept guard — block if specializations differ
            if query_concepts and best_concepts:
                if not _concepts_compatible(query_concepts, best_concepts):
                    logger.info(
                        "[SEMANTIC CACHE] MISS (concept mismatch, score=%.3f, "
                        "query=%s, cached=%s)",
                        best_score, query_text[:60], best_key[:60],
                    )
                    return None, best_score

            # Layer 2: table guard — block if SQL tables don't overlap
            if query_concepts and best_sql_tables:
                query_tables = _tables_for_concepts(query_concepts)
                if query_tables and not (query_tables & best_sql_tables):
                    logger.info(
                        "[SEMANTIC CACHE] MISS (table mismatch, score=%.3f, "
                        "query_tables=%s, cached_tables=%s)",
                        best_score, query_tables, best_sql_tables,
                    )
                    return None, best_score

            logger.info("[SEMANTIC CACHE] HIT (score=%.3f, key=%s)", best_score, best_key[:40])
            return best_result, best_score
        if self._keys:
            logger.info(
                "[SEMANTIC CACHE] MISS (best_score=%.3f < threshold=%.2f, best_key=%s)",
                best_score, self._threshold, best_key[:40],
            )
        return None, best_score

    def store(
        self,
        key: str,
        question: str,
        result: dict[str, Any],
        embedding: list[float],
        sql_tables: frozenset[str] | None = None,
    ) -> None:
        """Store a result with its embedding in the cache."""
        self._cache.set(key, {
            "question": question,
            "embedding": embedding,
            "result": result,
            "concepts": _extract_concepts(question),
            "sql_tables": sql_tables or frozenset(),
        })
        if key not in self._keys:
            self._keys.append(key)
        logger.info("[SEMANTIC CACHE] Stored: %s (keys=%d)", key[:30], len(self._keys))

    def clear(self) -> None:
        """Flush all cached entries and keys."""
        self._cache.clear()
        self._keys.clear()
        logger.info("[SEMANTIC CACHE] Cleared")

    def get_stats(self) -> dict[str, Any]:
        stats = self._cache.get_stats()
        stats["threshold"] = self._threshold
        return stats
