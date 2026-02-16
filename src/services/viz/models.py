"""Visualization service models."""

from pydantic import BaseModel, Field


class VizColumnMapping(BaseModel):
    """Lightweight LLM response: column mappings + labels only (no data_points)."""

    x_column: str = Field(description="Nombre exacto de la columna SQL para el eje X")
    y_column: str = Field(description="Nombre exacto de la columna SQL para el eje Y (valores numéricos)")
    month_column: str | None = Field(
        default=None,
        description="Si year y month son columnas separadas, indicar aquí la columna del mes. "
        "x_column contendrá la columna del año.",
    )
    series_column: str | None = Field(
        default=None, description="Columna SQL para series/categorías, si aplica"
    )
    category_column: str | None = Field(
        default=None, description="Columna SQL para categorías adicionales, si aplica"
    )
    x_format: str | None = Field(
        default=None,
        description="Formato para x_value: 'YYYY-MM' si x es temporal (fecha codificada o year+month separados), null si es texto",
    )
    metric_name: str = Field(description="Nombre de la métrica en español")
    x_axis_name: str = Field(description="Etiqueta del eje X en español")
    y_axis_name: str = Field(description="Etiqueta del eje Y en español")
    series_name: str | None = Field(default=None, description="Nombre de la serie en español")
    category_name: str | None = Field(default=None, description="Nombre de la categoría en español")
