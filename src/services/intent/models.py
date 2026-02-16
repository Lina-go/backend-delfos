"""Intent service models."""

from pydantic import BaseModel

from src.config.constants import Intent


class IntentResult(BaseModel):
    """Result from intent classification."""

    user_question: str
    intent: Intent
    tipo_patron: str  # Pattern type (Comparación, Relación, Proyección, Simulación)
    arquetipo: str  # Analytical archetype (A-K)
    razon: str | None = None  # Reasoning in Spanish
    titulo_grafica: str | None = None  # Title for the graph if applicable
    is_tasa: bool = False  # Whether the question involves interest rates
    temporality: str = "estatico"  # "estatico" or "temporal"
    subject_cardinality: int = 1  # Number of subjects in the question
