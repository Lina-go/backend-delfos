"""Pattern hooks registry -- extensible dispatch by sub_type.

Each sub_type can register optional hook functions that customize
pipeline behavior at specific divergence points. Unregistered sub_types
fall through to default behavior (= comparacion).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from src.orchestrator.state import PipelineState
    from src.services.viz.models import VizColumnMapping

logger = logging.getLogger(__name__)


@dataclass
class PatternHooks:
    """Optional hook functions for a specific sub_type."""

    enrich_sql_prompt: Callable[[str, Any], str] | None = None
    """(base_prompt, state) -> enriched prompt string."""

    post_process: Callable[[list[dict[str, Any]], Any], None] | None = None
    """(sql_results, state) -> None. Mutate state directly (e.g. state.stats_summary)."""

    build_data_points: Callable[[list[dict[str, Any]], Any], list[dict[str, Any]]] | None = None
    """(sql_results, mapping) -> formatted data_points list."""

    get_chart_type: Callable[[str], str | None] | None = None
    """(sub_type) -> ChartType value string or None."""


_REGISTRY: dict[str, PatternHooks] = {}


def register(sub_type: str, hooks: PatternHooks) -> None:
    """Register hooks for a sub_type."""
    _REGISTRY[sub_type] = hooks
    logger.info("Registered pattern hooks for sub_type=%s", sub_type)


def get_hooks(sub_type: str | None) -> PatternHooks:
    """Get hooks for a sub_type. Returns empty hooks if not registered."""
    if sub_type and sub_type in _REGISTRY:
        return _REGISTRY[sub_type]
    return PatternHooks()
