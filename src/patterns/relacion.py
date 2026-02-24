"""Relacion pattern hooks -- scatter plot with correlation analysis.

Registers hooks for sub_types: relacion, covariacion.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from src.config.constants import ChartType
from src.patterns.registry import PatternHooks, register
from src.services.analysis.correlation import compute_relationship_stats

if TYPE_CHECKING:
    from src.orchestrator.state import PipelineState
    from src.services.viz.models import VizColumnMapping

logger = logging.getLogger(__name__)

_SQL_ENRICHMENT = """
## IMPORTANT: Relacion Pattern — Two-Metric Scatter Analysis
This question compares TWO different numeric metrics for the SAME set of subjects.
The SQL must return data structured for scatter plot analysis.

### Required output columns (exact aliases):
- `label`        — entity/subject name (e.g., NOMBRE_ENTIDAD or bank name). ALWAYS include.
- `x_value`      — first metric (numeric, e.g., tasa de captación). ALWAYS include.
- `y_value`      — second metric (numeric, e.g., volumen captado). ALWAYS include.
- `year`, `month` — include ONLY when the question is temporal (see rules below).
- `color_group`   — categorical grouping (e.g., TIPO_ENTIDAD). Include ONLY when explicitly requested (see rules below).

### JOIN rules override
For relacion queries, JOINs between fact tables ARE allowed when the two metrics
come from different fact tables. Use `ID_ENTIDAD`, `year`, and `month` as the
JOIN key between fact tables.

Snapshot example (no time columns in output):
```sql
SELECT
    b.NOMBRE_ENTIDAD AS label,
    AVG(t1.metric_a) AS x_value,
    AVG(t2.metric_b) AS y_value
FROM gold.fact_table_1 t1
JOIN gold.fact_table_2 t2
    ON t1.ID_ENTIDAD = t2.ID_ENTIDAD
    AND t1.year      = t2.year
    AND t1.month     = t2.month
JOIN gold.banco b ON t1.ID_ENTIDAD = b.ID_ENTIDAD
WHERE (t1.year * 100 + t1.month) = (
    SELECT MAX(year * 100 + month) FROM gold.fact_table_1
)
GROUP BY b.NOMBRE_ENTIDAD
```

Temporal example (with time columns in output):
```sql
SELECT
    b.NOMBRE_ENTIDAD AS label,
    AVG(t1.metric_a) AS x_value,
    AVG(t2.metric_b) AS y_value,
    t1.year,
    t1.month
FROM gold.fact_table_1 t1
JOIN gold.fact_table_2 t2
    ON t1.ID_ENTIDAD = t2.ID_ENTIDAD
    AND t1.year      = t2.year
    AND t1.month     = t2.month
JOIN gold.banco b ON t1.ID_ENTIDAD = b.ID_ENTIDAD
WHERE (t1.year * 100 + t1.month) >= (
    SELECT MAX(year * 100 + month) - 100 FROM gold.fact_table_1
)
GROUP BY b.NOMBRE_ENTIDAD, t1.year, t1.month
```

If both metrics come from the SAME fact table, no fact-to-fact JOIN is needed.

### WHEN TO INCLUDE time columns (year, month)
DEFAULT: Do NOT include time columns. Use the LATEST available period only:
```sql
WHERE (year * 100 + month) = (SELECT MAX(year * 100 + month) FROM <PRIMARY_FACT_TABLE>)
```
This gives a clean snapshot: each point = 1 entity.

Include year and month in SELECT and GROUP BY ONLY when:
- The user explicitly asks about evolution, trend, or change over time
  ("a lo largo del tiempo", "evolución", "en los últimos N meses", "histórico")
- The user mentions a specific entity AND a time range
  ("tasa vs saldo de Bancolombia en los últimos 12 meses")
When temporal, apply the LAST 12 MONTHS default if no explicit time range is given.

Replace `<PRIMARY_FACT_TABLE>` with the actual fact table used for x_value.

### WHEN TO INCLUDE color_group
DEFAULT: Do NOT include color_group.

Include color_group ONLY when the user explicitly asks for a categorical breakdown:
- YES: "relación entre tasa y saldo por tipo de crédito" → include TIPO_DE_CR_DITO AS color_group
- YES: "separado por tipo de entidad" → include TIPO_ENTIDAD AS color_group
- NO: "¿hay relación entre tasa y saldo?" → omit color_group

### CRITICAL: Grouping rule for scatter
The GROUP BY ALWAYS includes the unit of observation:
- Snapshot → NOMBRE_ENTIDAD (each point = 1 entity)
- Temporal → year, month (each point = 1 month)

When the user says "por [dimension]" (por tipo de crédito, por tipo de entidad, etc.):
- ADD [dimension] to GROUP BY **in addition to** the unit of observation
- Include [dimension] AS color_group in the SELECT
- `label` is ALWAYS NOMBRE_ENTIDAD — NEVER replace it with the color dimension
- Each entity may appear N times (1 per color_group value)

NEVER GROUP BY only the color dimension. A scatter with <10 points has no analytical value.

### GROUP BY summary
| Variant                   | GROUP BY columns                         |
|---------------------------|------------------------------------------|
| Snapshot, no color        | NOMBRE_ENTIDAD                           |
| Snapshot, with color      | NOMBRE_ENTIDAD, color_dimension          |
| Temporal, no color        | NOMBRE_ENTIDAD, year, month              |
| Temporal, with color      | NOMBRE_ENTIDAD, year, month, color_dim   |

### Numeric types and NULL handling
For SUM of monetary columns (MONTOS_DESEMBOLSADOS, SALDO_*, etc.), use CAST(... AS FLOAT)
instead of CAST(... AS BIGINT) to avoid arithmetic overflow.
Use ISNULL or COALESCE to avoid NULL values in x_value and y_value.
NULLs from bad JOINs indicate incompatible granularity — restructure the query.
"""


def enrich_sql_prompt(base_prompt: str, _state: Any) -> str:
    """Append relacion-specific SQL instructions to the base prompt."""
    return base_prompt + _SQL_ENRICHMENT


def post_process(sql_results: list[dict[str, Any]], state: PipelineState) -> None:
    """Compute correlation statistics after SQL execution."""
    if not sql_results:
        return

    data_points = [
        {"x_value": row.get("x_value"), "y_value": row.get("y_value"), "label": row.get("label", "")}
        for row in sql_results
        if row.get("x_value") is not None and row.get("y_value") is not None
    ]

    if not data_points:
        logger.warning("No valid data points for correlation analysis")
        return

    stats = compute_relationship_stats(data_points)
    state.stats_summary = stats
    logger.info(
        "Correlation stats: r=%s, n=%s, strength=%s",
        stats.get("r"),
        stats.get("n"),
        stats.get("strength"),
    )


def build_scatter_points(
    sql_results: list[dict[str, Any]], mapping: VizColumnMapping
) -> list[dict[str, Any]]:
    """Build data_points formatted for scatter chart."""
    points: list[dict[str, Any]] = []
    for row in sql_results:
        x_raw = row.get(mapping.x_column)
        y_raw = row.get(mapping.y_column)
        if x_raw is None or y_raw is None:
            continue

        try:
            x_val = float(x_raw)
            y_val = float(y_raw)
        except (ValueError, TypeError):
            continue

        label = str(row.get("label", "")) if "label" in row else ""

        # Default: single group (no color_group). Use metric_name or "Datos".
        # Ignore series_column="label" — label is for hover only, not color grouping.
        series_col = mapping.series_column if mapping.series_column != "label" else None
        series = mapping.metric_name or "Datos"
        if series_col and series_col in row:
            series = str(row[series_col])

        category = series
        if mapping.category_column and mapping.category_column in row:
            category = str(row[mapping.category_column])

        point: dict[str, Any] = {
            "x_value": x_val,
            "y_value": y_val,
            "series": series,
            "category": category,
        }
        if label:
            point["label"] = label
        points.append(point)

    return points


def get_chart_type(_sub_type: str) -> str | None:
    """Relacion always renders as scatter."""
    return ChartType.SCATTER


_hooks = PatternHooks(
    enrich_sql_prompt=enrich_sql_prompt,
    post_process=post_process,
    build_data_points=build_scatter_points,
    get_chart_type=get_chart_type,
)

register("relacion", _hooks)
register("covariacion", _hooks)
