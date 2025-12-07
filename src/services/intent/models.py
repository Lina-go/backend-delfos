"""Intent service models."""

from dataclasses import dataclass
from typing import Optional
from src.config.constants import Intent, PatternType


@dataclass
class IntentResult:
    """Result from intent classification."""

    intent: Intent
    pattern_type: PatternType
    reasoning: Optional[str] = None

