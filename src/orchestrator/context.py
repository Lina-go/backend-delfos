"""Conversation context management for Delfos NL2SQL Pipeline."""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ConversationContext:
    """Stores context from the last query for follow-ups and viz requests."""

    # Core fields
    last_query: str | None = None
    last_sql: str | None = None
    last_results: list[dict[str, Any]] | None = None
    last_response: dict[str, Any] | None = None
    last_chart_type: str | None = None
    last_run_id: str | None = None
    last_data_points: list[dict[str, Any]] | None = None

    # Schema context for intelligent follow-ups
    last_tables: list[str] = field(default_factory=list)
    last_schema_context: dict[str, Any] | None = None
    last_columns: list[str] = field(default_factory=list)

    def get_summary(self) -> str:
        """
        Generate a generic summary of the context for Triage.

        This summary helps the Triage LLM understand what data is available
        so it can decide if a question is a follow-up or requires new data.

        Returns:
            A formatted string summarizing available data, or empty string if no data.
        """
        if not self.last_results or len(self.last_results) == 0:
            return ""

        # Extract unique values per column (limit to avoid huge summaries)
        MAX_VALUES_PER_COLUMN = 8
        MAX_COLUMNS_TO_SHOW = 10

        column_values: dict[str, set[str]] = {}

        for row in self.last_results:
            for col_name, value in row.items():
                if col_name not in column_values:
                    column_values[col_name] = set()

                if value is not None and len(column_values[col_name]) < MAX_VALUES_PER_COLUMN:
                    # Convert to string, truncate if too long
                    str_value = str(value)
                    if len(str_value) > 50:
                        str_value = str_value[:47] + "..."
                    column_values[col_name].add(str_value)

        # Build summary
        summary_parts = [
            f'Pregunta anterior: "{self.last_query}"',
            f"Filas de datos: {len(self.last_results)}",
            f"Columnas: {', '.join(self.last_columns[:MAX_COLUMNS_TO_SHOW])}",
            "",
            "Valores disponibles por columna:",
        ]

        for col, values in list(column_values.items())[:MAX_COLUMNS_TO_SHOW]:
            values_list = sorted(values, key=lambda x: (not x.replace('.', '').replace('-', '').isdigit(), x))
            values_preview = ", ".join(values_list[:5])
            if len(values) > 5:
                values_preview += f" ... (+{len(values) - 5} mas)"
            summary_parts.append(f"  - {col}: [{values_preview}]")

        return "\n".join(summary_parts)


class ConversationStore:
    """In-memory store for conversation contexts by user_id."""

    _contexts: dict[str, ConversationContext] = {}

    @classmethod
    def get(cls, user_id: str) -> ConversationContext:
        """Get or create context for user."""
        if user_id not in cls._contexts:
            cls._contexts[user_id] = ConversationContext()
        return cls._contexts[user_id]

    @classmethod
    def has_data(cls, user_id: str) -> bool:
        """Check if user has previous query results."""
        ctx = cls._contexts.get(user_id)
        return ctx is not None and ctx.last_results is not None and len(ctx.last_results) > 0

    @classmethod
    def update(
        cls,
        user_id: str,
        query: str,
        sql: str | None,
        results: list[dict[str, Any]] | None,
        response: dict[str, Any],
        chart_type: str | None = None,
        run_id: str | None = None,
        data_points: list[dict[str, Any]] | None = None,
        tables: list[str] | None = None,
        schema_context: dict[str, Any] | None = None,
    ) -> None:
        """Update context after a successful data query."""
        ctx = cls.get(user_id)
        ctx.last_query = query
        ctx.last_sql = sql
        ctx.last_results = results
        ctx.last_response = response
        ctx.last_chart_type = chart_type
        ctx.last_run_id = run_id
        ctx.last_data_points = data_points
        ctx.last_tables = tables or []
        ctx.last_schema_context = schema_context

        # Extract column names from results
        if results and len(results) > 0:
            ctx.last_columns = list(results[0].keys())
        else:
            ctx.last_columns = []

    @classmethod
    def clear(cls, user_id: str) -> None:
        """Clear context for user."""
        if user_id in cls._contexts:
            del cls._contexts[user_id]
