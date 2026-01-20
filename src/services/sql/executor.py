"""SQL executor service."""

import datetime
import logging
import re
from decimal import Decimal
from typing import Any

from src.config.settings import Settings
from src.infrastructure.mcp.client import MCPClient
from src.utils.retry import run_with_retry

logger = logging.getLogger(__name__)

# Restricted eval context - only allows specific safe types
# No builtins prevents access to dangerous functions
_SAFE_EVAL_CONTEXT: dict[str, Any] = {
    "__builtins__": {},
    "Decimal": Decimal,
    "datetime": datetime,
    "None": None,
    "True": True,
    "False": False,
}

# Pattern to validate input contains only expected characters
# Allows: tuples, strings, numbers, Decimal(), datetime.X(), None, True, False
_SAFE_INPUT_PATTERN = re.compile(
    r"^[\(\),\s\w\.'\":\-\+]+$"
)


class SQLExecutionResult:
    """Standardized SQL execution result."""

    def __init__(
        self,
        resultados: list[dict[str, Any]],
        total_filas: int,
        resumen: str,
        insights: str | None = None,
    ):
        self.resultados = resultados
        self.total_filas = total_filas
        self.resumen = resumen
        self.insights = insights

    def to_dict(self) -> dict[str, Any]:
        return {
            "resultados": self.resultados,
            "total_filas": self.total_filas,
            "resumen": self.resumen,
            "insights": self.insights,
        }

    @classmethod
    def success(cls, resultados: list[dict[str, Any]], total_filas: int) -> "SQLExecutionResult":
        return cls(
            resultados=resultados,
            total_filas=total_filas,
            resumen=f"Consulta ejecutada exitosamente. Se devolvieron {total_filas} filas.",
        )

    @classmethod
    def error(cls, message: str) -> "SQLExecutionResult":
        return cls(
            resultados=[],
            total_filas=0,
            resumen=message,
        )


class RowParser:
    """Parses raw SQL result strings into structured data."""

    @staticmethod
    def parse(raw_results: str) -> list[tuple[Any, ...]]:
        """
        Parse raw SQL result rows into Python tuples.

        Supports common SQL types: Decimal, datetime.date, datetime.datetime,
        datetime.time, and basic Python literals.
        """
        if not raw_results:
            return []

        rows: list[tuple[Any, ...]] = []
        for line in raw_results.splitlines():
            line = line.strip()
            if not line or line.lower() == "no results found.":
                continue

            parsed = RowParser._parse_line(line)
            if isinstance(parsed, tuple):
                rows.append(parsed)
            else:
                rows.append((parsed,))

        return rows

    @staticmethod
    def _parse_line(line: str) -> Any:
        """
        Parse a single line, returning normalized values.

        Uses restricted eval with:
        1. Input validation - only allows safe characters
        2. No builtins - prevents dangerous function calls
        3. Limited namespace - only Decimal, datetime, None, True, False
        """
        # Validate input contains only expected characters
        if not _SAFE_INPUT_PATTERN.match(line):
            logger.warning(f"Row contains unexpected characters, skipping eval: '{line[:100]}...'")
            return line

        try:
            value = eval(line, _SAFE_EVAL_CONTEXT)  # noqa: S307
            return RowParser._normalize(value)
        except Exception as e:
            logger.warning(f"Failed to parse row '{line[:100]}...': {e}")
            return line

    @staticmethod
    def _normalize(value: Any) -> Any:
        """Convert complex types to JSON-serializable forms."""
        if isinstance(value, tuple):
            return tuple(RowParser._normalize_value(v) for v in value)
        return RowParser._normalize_value(value)

    @staticmethod
    def _normalize_value(val: Any) -> Any:
        """Convert a single value to JSON-serializable form."""
        if val is None:
            return None
        if isinstance(val, Decimal):
            return float(val)
        if isinstance(val, datetime.datetime):
            return val.isoformat()
        if isinstance(val, datetime.date):
            return val.isoformat()
        if isinstance(val, datetime.time):
            return val.isoformat()
        if isinstance(val, bytes):
            return val.decode("utf-8", errors="replace")
        return val


class ColumnExtractor:
    """Extracts column names from SQL SELECT statements."""

    @staticmethod
    def extract(sql: str) -> list[str]:
        """Extract column names/aliases from SQL SELECT clause."""
        select_clause = ColumnExtractor._find_select_clause(sql)
        if not select_clause:
            return []

        if select_clause.lower().startswith("distinct"):
            select_clause = select_clause[8:].strip()

        parts = ColumnExtractor._split_columns(select_clause)
        return [name for part in parts if (name := ColumnExtractor._get_column_name(part))]

    @staticmethod
    def _find_select_clause(sql: str) -> str:
        """Find the SELECT ... FROM clause, handling nested subqueries."""
        text = sql.strip()
        if not text:
            return ""

        lower_text = text.lower()
        depth = 0
        in_single = False
        in_double = False
        last_select_idx = None
        last_from_idx = None

        for i, ch in enumerate(lower_text):
            if ch == "'" and not in_double:
                in_single = not in_single
            elif ch == '"' and not in_single:
                in_double = not in_double
            elif not in_single and not in_double:
                if ch == "(":
                    depth += 1
                elif ch == ")":
                    depth = max(depth - 1, 0)
                elif depth == 0:
                    if lower_text.startswith("select", i):
                        last_select_idx = i
                        last_from_idx = None
                    elif lower_text.startswith("from", i) and last_select_idx is not None:
                        last_from_idx = i

        if last_select_idx is None or last_from_idx is None:
            return ""

        return text[last_select_idx + len("select") : last_from_idx].strip()

    @staticmethod
    def _split_columns(select_clause: str) -> list[str]:
        """Split SELECT clause into individual column expressions."""
        parts: list[str] = []
        current: list[str] = []
        depth = 0
        in_single = False
        in_double = False

        for ch in select_clause:
            if ch == "'" and not in_double:
                in_single = not in_single
            elif ch == '"' and not in_single:
                in_double = not in_double
            elif not in_single and not in_double:
                if ch == "(":
                    depth += 1
                elif ch == ")":
                    depth = max(depth - 1, 0)
                elif ch == "," and depth == 0:
                    if part := "".join(current).strip():
                        parts.append(part)
                    current = []
                    continue
            current.append(ch)

        if trailing := "".join(current).strip():
            parts.append(trailing)

        return parts

    @staticmethod
    def _get_column_name(expression: str) -> str:
        """Extract column name or alias from expression."""
        exp = " ".join(expression.strip().split())
        lower_exp = exp.lower()

        # Check for AS alias
        if " as " in lower_exp:
            alias = exp[lower_exp.rfind(" as ") + 4 :].strip()
            return ColumnExtractor._strip_wrappers(alias)

        # Check for implicit alias (last token)
        tokens = exp.split()
        if len(tokens) > 1:
            return ColumnExtractor._strip_wrappers(tokens[-1])

        # Check for table.column format
        if "." in exp:
            return ColumnExtractor._strip_wrappers(exp.split(".")[-1])

        return ColumnExtractor._strip_wrappers(exp)

    @staticmethod
    def _strip_wrappers(value: str) -> str:
        """Remove SQL identifier wrappers like [], '', ""."""
        value = value.strip()
        if len(value) >= 2:
            if (value[0], value[-1]) in [("[", "]"), ('"', '"'), ("'", "'")]:
                return value[1:-1]
        return value


class ResultFormatter:
    """Formats parsed rows into dictionaries with column names."""

    @staticmethod
    def format(rows: list[tuple[Any, ...]], columns: list[str]) -> list[dict[str, Any]]:
        """Convert row tuples to list of dictionaries."""
        if not rows:
            return []

        # Generate column names if not provided
        if not columns or columns == ["*"]:
            columns = [f"col{i + 1}" for i in range(len(rows[0]))]

        return [ResultFormatter._row_to_dict(row, columns) for row in rows]

    @staticmethod
    def _row_to_dict(row: tuple[Any, ...], columns: list[str]) -> dict[str, Any]:
        """Convert a single row tuple to dictionary."""
        return {col: (row[idx] if idx < len(row) else None) for idx, col in enumerate(columns)}


class SQLExecutor:
    """Executes SQL queries via MCP and formats results deterministically."""

    def __init__(self, settings: Settings):
        self.settings = settings

    async def close(self) -> None:
        """Close resources (no-op, connections managed via context manager)."""
        pass

    async def execute(self, sql: str, mcp: Any | None = None) -> dict[str, Any]:
        """Execute SQL query and return formatted results."""
        try:
            execution_result = await self._execute_with_retry(sql, mcp)

            if error := execution_result.get("error"):
                logger.error(f"SQL execution error: {error}")
                return SQLExecutionResult.error(f"Error executing SQL: {error}").to_dict()

            raw_results = execution_result.get("raw", "")
            row_count = execution_result.get("row_count", 0)

            logger.info(f"SQL executed successfully: {row_count} rows returned")
            logger.debug("SQL raw results (first 2000 chars): %s", raw_results[:2000])

            # Parse and format results
            columns = ColumnExtractor.extract(sql)
            rows = RowParser.parse(raw_results)
            resultados = ResultFormatter.format(rows, columns)

            return SQLExecutionResult.success(resultados, row_count).to_dict()

        except Exception as e:
            logger.error(f"SQL execution error: {e}", exc_info=True)
            return SQLExecutionResult.error(f"Error executing SQL: {e}").to_dict()

    async def _execute_with_retry(self, sql: str, mcp: Any | None) -> dict[str, Any]:
        """Execute SQL with retry logic for transient failures."""

        async def _execute_once() -> dict[str, Any]:
            if mcp is None:
                async with MCPClient(self.settings) as mcp_client:
                    result = await mcp_client.execute_sql(sql)
            else:
                mcp_client = MCPClient.from_connection(mcp)
                result = await mcp_client.execute_sql(sql)

            error_msg = str(result.get("error") or "").lower()
            if "login timeout" in error_msg or "timeout expired" in error_msg:
                raise TimeoutError(f"SQL Connection Timeout: {result['error']}")

            return result

        logger.info("Executing SQL query via MCP")
        return await run_with_retry(
            _execute_once,
            max_retries=3,
            initial_delay=2.0,
            backoff_factor=2.0,
        )
