"""Pipeline state model."""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class PipelineState:
    """State object passed through the pipeline."""

    # Input
    user_message: str
    user_id: str

    # Step 1: Triage
    query_type: str | None = None  # data_question | general | out_of_scope

    # Step 2: Intent
    intent: str | None = None  # nivel_puntual | requiere_viz
    pattern_type: str | None = None  # comparacion | relacion | proyeccion | simulacion
    arquetipo: str | None = None
    titulo_grafica: str | None = None
    is_tasa: bool = False
    temporality: str | None = None  # "estatico" or "temporal"
    subject_cardinality: int = 1  # Number of subjects in the question

    # Step 3: Schema
    selected_tables: list[str] = field(default_factory=list)
    schema_context: dict[str, Any] | None = None

    # Step 4-5: SQL
    sql_query: str | None = None
    sql_tables: list[str] = field(default_factory=list)
    sql_results: list[Any] | None = None
    total_filas: int = 0
    sql_resumen: str | None = None
    sql_insights: str | None = None

    # Step 6: Verification (with detailed feedback for retry)
    verification_passed: bool = False
    verification_issues: list[str] = field(default_factory=list)
    verification_suggestion: str | None = None
    verification_insight: str | None = None

    # Step 7: Visualization
    viz_required: bool = False
    tipo_grafico: str | None = None
    powerbi_url: str | None = None
    data_points: list[dict[str, Any]] | None = None
    metric_name: str | None = None
    run_id: str | None = None
    x_axis_name: str | None = None
    y_axis_name: str | None = None
    series_name: str | None = None
    category_name: str | None = None

    # Step 8: Final response
    final_response: dict[str, Any] | None = None

    @property
    def resolved_tables(self) -> list[str]:
        """Tables to associate with this query.

        Prefers the schema-selected tables; falls back to the tables
        reported by the SQL generator when schema context is absent.
        """
        schema_tables = self.schema_context.get("tables", []) if self.schema_context else []
        return schema_tables or self.sql_tables

    def reset_sql_state(self) -> None:
        """Reset SQL-related state for retry attempts."""
        self.sql_query = None
        self.sql_tables = []
        self.sql_results = None
        self.total_filas = 0
        self.sql_resumen = None
        self.sql_insights = None
        self.verification_passed = False
        self.verification_issues = []
        self.verification_suggestion = None
        self.verification_insight = None
