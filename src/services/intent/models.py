"""Intent service models."""

from pydantic import BaseModel
from typing import Optional
from src.config.constants import Intent, PatternType


class IntentResult(BaseModel):
    """Result from intent classification."""

    user_question: str
    intent: Intent
    tipo_patron: str  # Pattern type as single letter (A-N)
    arquetipo: str  # Analytical archetype
    razon: Optional[str] = None  # Reasoning in Spanish

