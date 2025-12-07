"""Verification service models."""

from dataclasses import dataclass
from typing import List, Dict, Any, Optional


@dataclass
class VerificationResult:
    """Result from result verification."""

    passed: bool
    issues: List[str] = None
    confidence: float = 1.0

