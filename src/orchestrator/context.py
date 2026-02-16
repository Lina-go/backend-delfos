"""Conversation context management for Delfos NL2SQL Pipeline."""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any


@dataclass
class MessageTurn:
    """A single turn in the conversation history."""

    role: str  # "user" | "assistant"
    content: str
    query_type: str | None = None
    timestamp: str = ""
    had_viz: bool = False
    tables_used: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.timestamp:
            self.timestamp = datetime.now(UTC).isoformat()


@dataclass
class ConversationContext:
    """Stores context from the last query for follow-ups and viz requests."""

    # Core fields
    last_query: str | None = None
    last_sql: str | None = None
    last_results: list[dict[str, Any]] | None = None
    last_response: dict[str, Any] | None = None
    last_chart_type: str | None = None
    last_title: str | None = None
    last_run_id: str | None = None
    last_data_points: list[dict[str, Any]] | None = None

    # Schema context for intelligent follow-ups
    last_tables: list[str] = field(default_factory=list)
    last_schema_context: dict[str, Any] | None = None
    last_columns: list[str] = field(default_factory=list)
    last_temporality: str | None = None  # "estatico" | "temporal"

    # Conversation history (sliding window)
    message_history: list[MessageTurn] = field(default_factory=list)

    def get_history_summary(self, max_turns: int = 10) -> str:
        """Generate a formatted summary of recent conversation history for LLM prompts.

        Returns:
            Formatted conversation history string, or empty string if no history.
        """
        if not self.message_history:
            return ""

        recent = self.message_history[-(max_turns * 2) :]
        lines = ["## Historial de Conversacion Reciente"]

        for turn in recent:
            role_label = "Usuario" if turn.role == "user" else "Asistente"
            content = turn.content
            if len(content) > 300:
                content = content[:297] + "..."

            line = f"- **{role_label}**: {content}"
            if turn.query_type:
                line += f" [{turn.query_type}]"
            lines.append(line)

        return "\n".join(lines)

    def get_summary(self) -> str:
        """
        Generate a generic summary of the context for Triage.

        This summary helps the Triage LLM understand what data is available
        so it can decide if a question is a follow-up or requires new data.

        Returns:
            A formatted string summarizing available data, or empty string if no data.
        """
        if not self.last_results:
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
            values_list = sorted(
                values, key=lambda x: (not x.replace(".", "").replace("-", "").isdigit(), x)
            )
            values_preview = ", ".join(values_list[:5])
            if len(values) > 5:
                values_preview += f" ... (+{len(values) - 5} mas)"
            summary_parts.append(f"  - {col}: [{values_preview}]")

        return "\n".join(summary_parts)


class ConversationStore:
    """In-memory store for conversation contexts by user_id."""

    _MAX_CONTEXT_ROWS = 100

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
        return ctx is not None and bool(ctx.last_results)

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
        title: str | None = None,
        temporality: str | None = None,
    ) -> None:
        """Update context after a successful data query."""
        ctx = cls.get(user_id)
        ctx.last_query = query
        ctx.last_sql = sql
        ctx.last_results = results[:cls._MAX_CONTEXT_ROWS] if results else results
        ctx.last_response = response
        ctx.last_chart_type = chart_type
        ctx.last_title = title
        ctx.last_run_id = run_id
        ctx.last_data_points = data_points
        ctx.last_tables = tables or []
        ctx.last_schema_context = schema_context
        ctx.last_temporality = temporality

        # Extraer nombres de columnas de los resultados
        ctx.last_columns = list(results[0].keys()) if results else []

    @classmethod
    def add_turn(
        cls,
        user_id: str,
        role: str,
        content: str,
        query_type: str | None = None,
        had_viz: bool = False,
        tables_used: list[str] | None = None,
        max_history_turns: int = 10,
    ) -> None:
        """Add a conversation turn to the user's message history.

        Maintains a sliding window of max_history_turns * 2 messages.
        """
        ctx = cls.get(user_id)
        turn = MessageTurn(
            role=role,
            content=content,
            query_type=query_type,
            had_viz=had_viz,
            tables_used=tables_used or [],
        )
        ctx.message_history.append(turn)

        max_messages = max_history_turns * 2
        if len(ctx.message_history) > max_messages:
            ctx.message_history = ctx.message_history[-max_messages:]

    @classmethod
    def clear(cls, user_id: str) -> None:
        """Clear context for user."""
        if user_id in cls._contexts:
            del cls._contexts[user_id]
