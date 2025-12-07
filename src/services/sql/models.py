"""SQL service models."""

from dataclasses import dataclass
from typing import List, Dict, Any, Optional


@dataclass
class SQLResult:
    """Result from SQL execution."""

    pregunta_original: str
    sql: str
    tablas: List[str]
    resultados: List[Dict[str, Any]]
    total_filas: int
    resumen: str


@dataclass
class ValidationResult:
    """Result from SQL validation."""

    is_valid: bool
    errors: List[str]
    warnings: List[str] = None

