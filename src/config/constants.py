"""
Constants, enums, and static values.

Naming convention:
  Domain terms use Spanish (resultados, resumen, arquetipo, informes).
  Technical terms use English (cache, pipeline, verify).
"""

from enum import Enum


class QueryType(str, Enum):
    """Query classification types."""

    DATA_QUESTION = "data_question"  # Proceed to the rest of pipeline
    GENERAL = "general"  # Reject the question: Not about data
    OUT_OF_SCOPE = "out_of_scope"  # Reject the question: Not in the DB
    GREETING = "greeting"  # Say/ Respond greeting
    FOLLOW_UP = "follow_up"  # Handle follow-up question
    VIZ_REQUEST = "viz_request"  # Handle visualization request
    NEEDS_CLARIFICATION = "needs_clarification"  # Ask user to clarify ambiguous query


class Intent(str, Enum):
    """Intent classification for data questions."""

    NIVEL_PUNTUAL = "nivel_puntual"
    REQUIERE_VIZ = "requiere_visualizacion"


class PatternType(str, Enum):
    """Pattern type classification for data questions."""

    COMPARACION = "comparacion"
    RELACION = "relacion"
    PROYECCION = "proyeccion"
    SIMULACION = "simulacion"


class Archetype(str, Enum):
    """Archetype classification for data questions (A-K)."""

    ARCHETYPE_A = "A"
    ARCHETYPE_B = "B"
    ARCHETYPE_C = "C"
    ARCHETYPE_D = "D"
    ARCHETYPE_E = "E"
    ARCHETYPE_F = "F"
    ARCHETYPE_G = "G"
    ARCHETYPE_H = "H"
    ARCHETYPE_I = "I"
    ARCHETYPE_J = "J"
    ARCHETYPE_K = "K"


class ChartType(str, Enum):
    """Chart types for visualization."""

    PIE = "pie"
    BAR = "bar"
    LINE = "line"
    STACKED_BAR = "stackedbar"
    SCATTER = "scatter"


class ColumnType(str, Enum):
    """Database column types."""

    INTEGER = "integer"
    FLOAT = "float"
    STRING = "string"
    DATE = "date"
    DATETIME = "datetime"
    BOOLEAN = "boolean"


class PipelineStep(str, Enum):
    """Pipeline execution steps."""

    TRIAGE = "triage"
    INTENT = "intent"
    SCHEMA = "schema"
    SQL_GENERATION = "sql_generation"
    SQL_VALIDATION = "sql_validation"
    SQL_EXECUTION = "sql_execution"
    VERIFICATION = "verification"
    VIZ = "viz"
    FORMAT = "format"
    RESPONSE = "response"


class PipelineStepDescription(str, Enum):
    """Pipeline execution step descriptions."""

    TRIAGE = "Triage the user's question to determine the intent and type of patron"
    INTENT = "Determine the intent of the user's question"
    SCHEMA = "Generate the schema to answer the user's question"
    SQL_GENERATION = "Generate the SQL query to answer the user's question"
    SQL_VALIDATION = "Validate the SQL query to answer the user's question"
    SQL_EXECUTION = "Execute the SQL query to answer the user's question"
    VERIFICATION = "Verify the SQL query to answer the user's question"
    VIZ = "Generate the visualization to answer the user's question"
    FORMAT = "Format the response to answer the user's question"


class PipelineStatus(str, Enum):
    """Pipeline execution status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    REJECTED = "rejected"


class TableName:
    """Database table names used in SQL queries."""

    PROJECTS = "dbo.Projects"
    PROJECT_ITEMS = "dbo.ProjectItems"
    GRAPHS = "dbo.Graphs"


def log_pipeline_step(step: PipelineStep) -> None:
    """Log a pipeline step with its description."""
    import logging

    logging.getLogger("src.orchestrator").info(
        "%s: %s", step.value, PipelineStepDescription[step.name].value
    )


# Pagination defaults
DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 200
