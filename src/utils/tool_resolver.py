"""Agent tool resolution utility."""

import logging
from typing import Any

logger = logging.getLogger(__name__)


def resolve_agent_tools(
    db_tools: Any | None = None,
    *,
    context: str = "",
) -> Any | None:
    """Return exploration tools from db_tools, or None."""
    if db_tools is not None:
        logger.info("Using direct DB tools%s", f" for {context}" if context else "")
        return db_tools.get_exploration_tools()
    return None
