"""Pipeline state model."""

from dataclasses import dataclass, field
from typing import Optional, Any, Dict, List


@dataclass
class PipelineState:
    """State object passed through the pipeline."""

    # Input
    user_message: str
    user_id: str

    # Step 1: Triage
    query_type: Optional[str] = None  # data_question | general | out_of_scope

    # Step 2: Intent
    intent: Optional[str] = None  # nivel_puntual | requiere_viz
    pattern_type: Optional[str] = None # comparacion | relacion | proyeccion | simulacion
    arquetipo: Optional[str] = None

    # Step 3: Schema
    selected_tables: List[str] = field(default_factory=list)
    schema_context: Optional[Dict[str, Any]] = None

    # Step 4-5: SQL
    sql_query: Optional[str] = None
    sql_results: Optional[List[Any]] = None  # Can be List[str] from MCP or List[Dict] after parsing
    total_filas: int = 0
    sql_resumen: Optional[str] = None
    sql_insights: Optional[str] = None

    # Step 6: Verification
    verification_passed: bool = False

    # Step 7: Visualization
    viz_required: bool = False
    tipo_grafico: Optional[str] = None
    powerbi_url: Optional[str] = None
    image_url: Optional[str] = None
    html_url: Optional[str] = None
    png_url: Optional[str] = None
    run_id: Optional[str] = None

    # Step 8: Final response
    final_response: Optional[Dict[str, Any]] = None

