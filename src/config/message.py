"""
User-facing messages for the NL2SQL pipeline.
"""
from src.config.constants import QueryType

# =============================================================================
# Triage Rejection Messages
# =============================================================================

REJECTION_MESSAGES: dict[QueryType, str] = {
    QueryType.GENERAL: (
        "Solo puedo responder preguntas sobre los datos de FinancialDB. "
        "¿Tienes alguna pregunta sobre cuentas, clientes, préstamos o transacciones?"
    ),
    QueryType.OUT_OF_SCOPE: (
        "No tengo acceso a esa información. "
        "Puedo ayudarte con datos de cuentas, clientes, préstamos, "
        "transacciones, sucursales y empleados de FinancialDB."
    ),
}

# =============================================================================
# Error Messages
# =============================================================================
ERROR_MESSAGES: dict[str, str] = {
    "sql_generation_failed": "No pude generar una consulta SQL válida. Por favor, reformula tu pregunta.",
    "sql_validation_failed": "La consulta generada no pasó las validaciones de seguridad.",
    "sql_execution_failed": "Error al ejecutar la consulta. Por favor, intenta de nuevo.",
    "empty_results": "La consulta no retornó resultados.",
    "timeout": "La consulta tardó demasiado. Por favor, intenta con una pregunta más específica.",
    "viz_generation_failed": "No pude generar la visualización. Los datos están disponibles sin gráfico.",
    "graph_generation_failed": "No pude generar el gráfico. El enlace de Power BI está disponible.",
    "unknown_error": "Ocurrió un error inesperado. Por favor, intenta de nuevo.",
}

# =============================================================================
# Success Messages
# =============================================================================
SUCCESS_MESSAGES: dict[str, str] = {
    "sql_generation_success": "Consulta SQL generada correctamente.",
    "sql_validation_success": "Consulta SQL validada correctamente.",
    "sql_execution_success": "Consulta SQL ejecutada correctamente.",
    "viz_generation_success": "Visualización generada correctamente.",
    "graph_generation_success": "Gráfico generado correctamente.",
}

# =============================================================================
# Helper Functions
# =============================================================================

def get_rejection_message(query_type: QueryType) -> str:
    """Get rejection message for a query type."""
    return REJECTION_MESSAGES.get(query_type, REJECTION_MESSAGES[QueryType.OUT_OF_SCOPE])


def get_error_message(error_key: str) -> str:
    """Get error message by key."""
    return ERROR_MESSAGES.get(error_key, ERROR_MESSAGES["unknown_error"])


def format_success_message(message_key: str, **kwargs) -> str:
    """Format a success message with provided values."""
    template = SUCCESS_MESSAGES.get(message_key, "")
    return template.format(**kwargs) if template else ""
    