"""Triage service models."""

from dataclasses import dataclass
from typing import Optional


@dataclass
class TriageResult:
    """Result from triage classification."""

    query_type: str  # data_question, general, out_of_scope
    confidence: float
    reasoning: Optional[str] = None

