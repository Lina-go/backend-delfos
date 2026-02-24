"""Comparacion temporal pattern hooks -- enforce 12-month default.

Registers hooks for temporal sub_types under comparacion:
tendencia_simple, tendencia_comparada, evolucion_composicion, evolucion_concentracion.
"""

from __future__ import annotations

import logging
from typing import Any

from src.patterns.registry import PatternHooks, register

logger = logging.getLogger(__name__)

_SQL_ENRICHMENT = """
## MANDATORY: Default Time Range for Temporal Queries
If the user does NOT explicitly specify a time period (e.g., 'últimos 3 meses', 'en 2024',
'desde enero', 'último año', 'entre X y Y'), you MUST limit the results to the LAST 12 MONTHS.

Add this WHERE clause to EVERY CTE and subquery that accesses the fact table:
```sql
WHERE (year * 100 + month) >= (
    SELECT MAX(year * 100 + month) - 100 FROM <FACT_TABLE>
)
```
Replace `<FACT_TABLE>` with the actual fact table used (e.g., gold.distribucion_cartera, gold.tasas_interes_captacion).

CRITICAL: This filter MUST be applied in ALL CTEs, not just the final SELECT.
If you have multiple CTEs, each one that queries the fact table must include this filter.

Example with CTEs:
```sql
WITH cte1 AS (
    SELECT year, month, SUM(...) AS total
    FROM gold.distribucion_cartera
    WHERE (year * 100 + month) >= (SELECT MAX(year * 100 + month) - 100 FROM gold.distribucion_cartera)
    GROUP BY year, month
),
cte2 AS (
    SELECT year, month, SUM(...) AS subtotal
    FROM gold.distribucion_cartera
    WHERE NOMBRE_ENTIDAD = '...'
      AND (year * 100 + month) >= (SELECT MAX(year * 100 + month) - 100 FROM gold.distribucion_cartera)
    GROUP BY year, month
)
SELECT ...
```

Do NOT return the entire history — only the last 12 months unless the user explicitly asks for more.
"""


def enrich_sql_prompt(base_prompt: str, _state: Any) -> str:
    """Append temporal default time range instructions to the base prompt."""
    return base_prompt + _SQL_ENRICHMENT


_hooks = PatternHooks(
    enrich_sql_prompt=enrich_sql_prompt,
)

# Register for all temporal comparacion sub-types
register("tendencia_simple", _hooks)
register("tendencia_comparada", _hooks)
register("evolucion_composicion", _hooks)
register("evolucion_concentracion", _hooks)
