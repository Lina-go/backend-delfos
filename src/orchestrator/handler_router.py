"""Route non-data queries to specialised handlers."""

import logging
from typing import Any

from src.config.constants import QueryType
from src.orchestrator.context import ConversationContext
from src.orchestrator.handlers import (
    ClarificationHandler,
    FollowUpHandler,
    GeneralHandler,
    GreetingHandler,
    VizRequestHandler,
)
from src.orchestrator.state import PipelineState

logger = logging.getLogger(__name__)


class HandlerRouter:
    """Dispatch a triaged query to the appropriate lightweight handler.

    Returns a response dict for non-data query types (greeting, follow_up,
    viz_request, general, out_of_scope) and ``None`` when the query is a
    ``data_question`` that should proceed through the full pipeline.
    """

    def __init__(
        self,
        greeting: GreetingHandler,
        follow_up: FollowUpHandler,
        viz_request: VizRequestHandler,
        general: GeneralHandler,
        clarification: ClarificationHandler,
    ) -> None:
        self._greeting = greeting
        self._follow_up = follow_up
        self._viz_request = viz_request
        self._general = general
        self._clarification = clarification

    async def route(
        self,
        state: PipelineState,
        message: str,
        user_id: str,
        context: ConversationContext,
    ) -> dict[str, Any] | None:
        """Return a response dict or *None* for data questions."""
        qt = state.query_type

        if qt == QueryType.GREETING:
            return self._greeting.handle(message)

        if qt == QueryType.FOLLOW_UP:
            return await self._follow_up.handle(message, context)

        if qt == QueryType.VIZ_REQUEST:
            return await self._viz_request.handle(message, user_id, context)

        if qt in (QueryType.GENERAL, QueryType.OUT_OF_SCOPE):
            return await self._general.handle(message)

        if qt == QueryType.NEEDS_CLARIFICATION:
            conversation_history = context.get_history_summary()
            return await self._clarification.handle(message, conversation_history)

        if qt == QueryType.DATA_QUESTION:
            return None  # proceed with full pipeline

        # Unknown query type — fall back to general handler
        logger.warning("Unknown query_type '%s', falling back to general handler", qt)
        return await self._general.handle(message)
