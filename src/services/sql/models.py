"""SQL service models."""

from dataclasses import dataclass
from pydantic import BaseModel
from typing import List, Dict, Any, Optional


class SQLResult(BaseModel):
    """Result from SQL generation (before execution)."""

    pregunta_original: str
    sql: str
    tablas: List[str]
    resumen: str


@dataclass
class ValidationResult:
    """Result from SQL validation."""

    is_valid: bool
    errors: List[str]
    warnings: List[str] = None

