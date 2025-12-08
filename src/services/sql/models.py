"""SQL service models."""

from dataclasses import dataclass, field
from pydantic import BaseModel
from typing import List, Dict, Any, Optional


class SQLResult(BaseModel):
    """Result from SQL generation (before execution)."""

    pregunta_original: str
    sql: str
    tablas: List[str]
    resumen: str
    error: Optional[str] = None


class SQLExecutionResult(BaseModel):
    """Result from SQL execution with formatted results."""

    resultados: List[Dict[str, Any]]
    total_filas: int
    resumen: str
    insights: Optional[str] = None


@dataclass
class ValidationResult:
    """Result from SQL validation."""

    is_valid: bool
    errors: List[str]
    warnings: List[str] = field(default_factory=list)

