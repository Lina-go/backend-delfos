"""SQL service models."""

from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel


class SQLResult(BaseModel):
    """Result from SQL generation (before execution)."""

    pregunta_original: str
    sql: str
    tablas: list[str]
    resumen: str
    error: str | None = None


class SQLExecutionResult(BaseModel):
    """Result from SQL execution with formatted results."""

    resultados: list[dict[str, Any]]
    total_filas: int
    resumen: str
    insights: str | None = None


@dataclass
class ValidationResult:
    """Result from SQL validation."""

    is_valid: bool
    errors: list[str]
    warnings: list[str] = field(default_factory=list)
