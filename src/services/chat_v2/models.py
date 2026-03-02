"""Chat V2 models."""

from __future__ import annotations

from pydantic import BaseModel, Field, field_validator


class IndicatorSpec(BaseModel):
    """Spec for a KPI indicator — LLM decides label/calc/unit, Python computes."""

    label: str = Field(description="Indicator label in Spanish")
    calc: str = Field(
        description="Arithmetic operation: period_delta, pct_change, "
        "prev_delta, momentum, "
        "max_change, rank_change, share_of_growth, growth_vs_market",
    )
    unit: str = Field(
        description="Unit: pp (participation), bps (rates), % (balance growth), abs (amounts)",
    )
    series: str | None = Field(
        default=None,
        description="Specific series name, or null for the main/only series",
    )


class UnifiedClassification(BaseModel):
    """Combined intent classification and visualization column mapping."""

    # --- Intent classification fields ---
    sub_type: str = Field(
        description="Sub-type classification: valor_puntual, tendencia_simple, "
        "tendencia_comparada, evolucion_composicion, evolucion_concentracion, "
        "covariacion, comparacion_directa, ranking, concentracion, "
        "composicion_simple, composicion_comparada, relacion"
    )
    titulo_grafica: str | None = Field(
        default=None,
        description="Short chart title in Spanish (max 10 words). null for valor_puntual.",
    )
    is_tasa: bool = Field(
        default=False,
        description="True if the question is about interest rates.",
    )

    # --- Viz column mapping fields (all nullable for valor_puntual) ---
    x_column: str | None = Field(default=None, description="SQL column name for the X axis")
    y_column: str | None = Field(default=None, description="SQL column name for the Y axis (numeric)")
    month_column: str | None = Field(
        default=None,
        description="If year and month are separate columns, the month column name. "
        "x_column will contain the year column.",
    )
    series_column: str | None = Field(
        default=None, description="SQL column for series/categories, if applicable"
    )
    category_column: str | None = Field(
        default=None, description="SQL column for additional categories, if applicable"
    )
    x_format: str | None = Field(
        default=None,
        description="'YYYY-MM' if x is temporal (year+month), null if text",
    )
    metric_name: str | None = Field(default=None, description="Metric name in Spanish")
    x_axis_name: str | None = Field(default=None, description="X axis label in Spanish")
    y_axis_name: str | None = Field(default=None, description="Y axis label in Spanish")
    series_name: str | None = Field(default=None, description="Series label in Spanish")
    category_name: str | None = Field(default=None, description="Category label in Spanish")

    # --- Indicator selection (LLM decides what to show, Python computes) ---
    indicators: list[IndicatorSpec] = Field(
        default_factory=list,
        description="KPI indicators for the chart (as many as relevant). Empty for valor_puntual, scatter, stacked_bar.",
    )

    @field_validator("sub_type", mode="before")
    @classmethod
    def normalize_sub_type(cls, v: str) -> str:
        """Ensure sub_type is lowercase and stripped."""
        return v.lower().strip() if isinstance(v, str) else v
