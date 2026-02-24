"""Chat V2 models — unified classification + viz mapping result."""

from pydantic import BaseModel, Field, field_validator


class UnifiedClassification(BaseModel):
    """Combined intent classification + visualization column mapping.

    Returned by a single LLM call that does both tasks at once,
    eliminating one API round-trip compared to separate calls.
    """

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

    @field_validator("sub_type", mode="before")
    @classmethod
    def normalize_sub_type(cls, v: str) -> str:
        """Ensure sub_type is lowercase and stripped."""
        return v.lower().strip() if isinstance(v, str) else v
