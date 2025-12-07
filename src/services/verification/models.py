"""Verification service models."""

from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional


@dataclass
class VerificationResult:
    """Result from result verification."""

    passed: bool
    issues: List[str] = field(default_factory=list)
    confidence: float = 1.0

