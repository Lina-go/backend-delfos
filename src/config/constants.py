"""
Constants, enums, and static values.
"""

from enum import Enum


class QueryType(str, Enum):
    """Query classification types."""

    DATA_QUESTION = "data_question" # Proceed to the rest of pipeline
    GENERAL = "general" # Reject the question: Not about data
    OUT_OF_SCOPE = "out_of_scope" # Reject the question: Not in the DB


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
    """Archetype classification for data questions."""

    PATTERN_A = "A"
    PATTERN_B = "B"
    PATTERN_C = "C"
    PATTERN_D = "D"
    PATTERN_E = "E"
    PATTERN_F = "F"
    PATTERN_G = "G"
    PATTERN_H = "H"
    PATTERN_I = "I"
    PATTERN_J = "J"
    PATTERN_K = "K"
    PATTERN_L = "L"
    PATTERN_M = "M"
    PATTERN_N = "N"


class ChartType(str, Enum):
    """Chart types for visualization."""

    PIE = "pie"
    BAR = "bar"
    LINE = "line"
    STACKED_BAR = "stackedbar"


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
    GRAPH = "graph"
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
    GRAPH = "Generate the graph to answer the user's question"
    FORMAT = "Format the response to answer the user's question"

class PipelineStatus(str, Enum):
    """Pipeline execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    REJECTED = "rejected"
