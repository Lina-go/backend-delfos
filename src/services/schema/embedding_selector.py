"""Embedding-based table selector using OpenAI embeddings."""

import logging
from typing import Any

import numpy as np

from src.config.database.concepts import CONCEPT_TO_TABLES
from src.config.database.schemas import DATABASE_TABLES
from src.config.settings import Settings

logger = logging.getLogger(__name__)


def _cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    """Compute cosine similarity between two vectors."""
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return float(np.dot(a, b) / (norm_a * norm_b))


def _build_table_text(table_name: str) -> str:
    """Build a rich text representation of a table for embedding."""
    info = DATABASE_TABLES.get(table_name)
    if not info:
        return table_name

    parts = [
        f"Table: {table_name}",
        f"Description: {info.table_description}",
        f"Columns: {', '.join(col.column_name for col in info.table_columns)}",
    ]

    concepts = [
        concept
        for concept, tables in CONCEPT_TO_TABLES.items()
        if table_name in tables
    ]
    if concepts:
        parts.append(f"Concepts: {', '.join(concepts)}")

    return "\n".join(parts)


class EmbeddingTableSelector:
    """Selects tables using embedding similarity.

    Pre-computes embeddings for all tables at first use.
    At query time, embeds the user question and returns tables
    ranked by cosine similarity above a threshold.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: Any = None
        self._table_embeddings: dict[str, np.ndarray] = {}
        self._initialized = False

    async def _ensure_initialized(self) -> bool:
        """Lazily initialize OpenAI client and compute table embeddings.

        Returns True if initialization succeeded, False otherwise.
        """
        if self._initialized:
            return bool(self._table_embeddings)

        self._initialized = True

        if not self._settings.openai_api_key:
            logger.info("EmbeddingTableSelector: No OpenAI API key, disabled")
            return False

        try:
            from openai import AsyncOpenAI

            self._client = AsyncOpenAI(api_key=self._settings.openai_api_key)

            table_texts = {
                name: _build_table_text(name) for name in DATABASE_TABLES
            }

            texts = list(table_texts.values())
            table_names = list(table_texts.keys())

            response = await self._client.embeddings.create(
                model=self._settings.embedding_model,
                input=texts,
            )

            for i, embedding_data in enumerate(response.data):
                self._table_embeddings[table_names[i]] = np.array(
                    embedding_data.embedding, dtype=np.float32
                )

            logger.info(
                "EmbeddingTableSelector initialized: %d table embeddings computed",
                len(self._table_embeddings),
            )
            return True

        except Exception:
            logger.warning("EmbeddingTableSelector initialization failed", exc_info=True)
            self._table_embeddings = {}
            return False

    async def select_tables(self, message: str) -> list[tuple[str, float]]:
        """Select tables by embedding similarity.

        Args:
            message: User's natural language question

        Returns:
            List of (table_name, similarity_score) tuples,
            sorted by score descending, filtered by threshold.
        """
        if not await self._ensure_initialized():
            return []

        try:
            response = await self._client.embeddings.create(
                model=self._settings.embedding_model,
                input=[message],
            )
            query_embedding = np.array(
                response.data[0].embedding, dtype=np.float32
            )

            scores: list[tuple[str, float]] = []
            for table_name, table_embedding in self._table_embeddings.items():
                sim = _cosine_similarity(query_embedding, table_embedding)
                if sim >= self._settings.embedding_similarity_threshold:
                    scores.append((table_name, sim))

            scores.sort(key=lambda x: x[1], reverse=True)
            return scores[:self._settings.embedding_top_k]

        except Exception:
            logger.warning("Embedding query failed", exc_info=True)
            return []
