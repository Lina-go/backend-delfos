"""Triage service models."""

from dataclasses import dataclass


@dataclass
class TriageResult:
    """Result from triage classification."""

    query_type: str  # data_question, general, out_of_scope
    confidence: float
    reasoning: str | None = None
