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

    # Step 3: Schema
    selected_tables: list[str] = field(default_factory=list)
    schema_context: dict[str, Any] | None = None

    # Step 4-5: SQL
    sql_query: str | None = None
    sql_results: list[Any] | None = None  # Can be List[str] from MCP or List[Dict] after parsing
    total_filas: int = 0
    sql_resumen: str | None = None
    sql_insights: str | None = None

    # Step 6: Verification
    verification_passed: bool = False

    # Step 7: Visualization
    viz_required: bool = False
    tipo_grafico: str | None = None
    powerbi_url: str | None = None
    image_url: str | None = None
    html_url: str | None = None
    png_url: str | None = None
    run_id: str | None = None

    # Step 8: Final response
    final_response: dict[str, Any] | None = None
