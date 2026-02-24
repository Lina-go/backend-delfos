"""Schema context provider for the Chat V2 agent."""

import json
from collections.abc import MutableSequence
from typing import Any

from agent_framework import ChatMessage, Context, ContextProvider


class SchemaContextProvider(ContextProvider):
    """Injects available table schemas as dynamic instructions before each model call."""

    def __init__(self, schema_context: dict[str, Any] | None = None) -> None:
        self._schema_context = schema_context or {}

    async def invoking(
        self,
        messages: ChatMessage | MutableSequence[ChatMessage],
        **kwargs: Any,
    ) -> Context:
        """Inject schema context before each model call."""
        if not self._schema_context:
            return Context(instructions="")
        context_str = json.dumps(
            self._schema_context, ensure_ascii=False, default=str
        )
        return Context(
            instructions=f"## Esquema de tablas disponibles\n{context_str}"
        )

    def update_context(self, schema_context: dict[str, Any]) -> None:
        """Replace the schema context for subsequent calls."""
        self._schema_context = schema_context
