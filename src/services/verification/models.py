"""Verification service models."""

from dataclasses import dataclass, field


@dataclass
class VerificationResult:
    """Result from result verification."""

    passed: bool
    issues: list[str] = field(default_factory=list)
    confidence: float = 1.0
