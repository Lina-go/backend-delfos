"""Report context provider for the advisor agent."""

import json
from collections.abc import MutableSequence
from typing import Any

from agent_framework import ChatMessage, Context, ContextProvider


class ReportContextProvider(ContextProvider):
    """Injects report context as dynamic instructions before each model call."""

    def __init__(self, report_context: dict[str, Any]) -> None:
        self._report_context = report_context

    async def invoking(
        self,
        messages: ChatMessage | MutableSequence[ChatMessage],
        **kwargs: Any,
    ) -> Context:
        """Inject report context before each model call."""
        context_str = json.dumps(
            self._report_context, ensure_ascii=False, default=str
        )
        return Context(
            instructions=f"## Contexto del informe actual\n{context_str}"
        )

    def update_context(self, report_context: dict[str, Any]) -> None:
        """Replace the report context for subsequent calls."""
        self._report_context = report_context
