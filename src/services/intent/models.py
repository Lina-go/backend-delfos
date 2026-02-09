"""Intent service models."""

from pydantic import BaseModel

from src.config.constants import Intent


class IntentResult(BaseModel):
    """Result from intent classification."""

    user_question: str
    intent: Intent
    tipo_patron: str  # Pattern type as single letter (A-N)
    arquetipo: str  # Analytical archetype
    razon: str | None = None  # Reasoning in Spanish
    titulo_grafica: str | None = None # Title for the graph if applicable
