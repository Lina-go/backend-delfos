"""Verification result model."""

from dataclasses import dataclass, field


@dataclass
class VerificationResult:
    """Result from verification step with detailed feedback."""
    
    passed: bool
    issues: list[str] = field(default_factory=list)
    suggestion: str | None = None
    insight: str | None = None
    summary: str | None = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary for logging."""
        return {
            "passed": self.passed,
            "issues": self.issues,
            "suggestion": self.suggestion,
            "insight": self.insight,
            "summary": self.summary,
        }