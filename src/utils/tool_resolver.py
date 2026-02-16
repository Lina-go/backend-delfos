"""Shared utility for resolving agent tools."""

import logging
from typing import Any

logger = logging.getLogger(__name__)


def resolve_agent_tools(
    db_tools: Any | None = None,
    *,
    context: str = "",
) -> Any | None:
    """Resolve which tools to pass to an agent.

    Args:
        db_tools: DelfosTools instance (direct DB access).
        context: Label for log messages (e.g. "triage", "sql_generation").

    Returns:
        Tools list/object to pass to the agent, or None.
    """
    if db_tools is not None:
        logger.info("Using direct DB tools%s", f" for {context}" if context else "")
        return db_tools.get_exploration_tools()
    return None
