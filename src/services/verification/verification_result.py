"""Verification result model."""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class VerificationResult:
    """Result from verification step with detailed feedback."""

    passed: bool
    issues: list[str] = field(default_factory=list)
    suggestion: str | None = None
    insight: str | None = None
    summary: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for logging."""
        return {
            "passed": self.passed,
            "issues": self.issues,
            "suggestion": self.suggestion,
            "insight": self.insight,
            "summary": self.summary,
        }
