"""Conversation context management."""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ConversationContext:
    """Stores context from the last query for follow-ups and viz requests."""

    last_query: str | None = None
    last_sql: str | None = None
    last_results: list[dict[str, Any]] | None = None
    last_response: dict[str, Any] | None = None
    last_chart_type: str | None = None


class ConversationStore:
    """In-memory store for conversation contexts by user_id."""

    _contexts: dict[str, ConversationContext] = {}

    @classmethod
    def get(cls, user_id: str) -> ConversationContext:
        """Get or create context for user."""
        if user_id not in cls._contexts:
            cls._contexts[user_id] = ConversationContext()
        return cls._contexts[user_id]

    @classmethod
    def has_data(cls, user_id: str) -> bool:
        """Check if user has previous query results."""
        ctx = cls._contexts.get(user_id)
        return ctx is not None and ctx.last_results is not None

    @classmethod
    def update(
        cls,
        user_id: str,
        query: str,
        sql: str | None,
        results: list[dict[str, Any]] | None,
        response: dict[str, Any],
        chart_type: str | None = None,
    ) -> None:
        """Update context after a successful data query."""
        ctx = cls.get(user_id)
        ctx.last_query = query
        ctx.last_sql = sql
        ctx.last_results = results
        ctx.last_response = response
        ctx.last_chart_type = chart_type

    @classmethod
    def clear(cls, user_id: str) -> None:
        """Clear context for user."""
        if user_id in cls._contexts:
            del cls._contexts[user_id]