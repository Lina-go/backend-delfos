"""Intent service models."""

from pydantic import BaseModel
from typing import Optional
from src.config.constants import Intent, PatternType


class IntentResult(BaseModel):
    """Result from intent classification."""

    intent: Intent
    pattern_type: PatternType
    reasoning: Optional[str] = None

