"""SQL executor service."""

from __future__ import annotations

import ast
import datetime
import logging
import re
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from src.config.settings import Settings

if TYPE_CHECKING:
    from src.infrastructure.database import DelfosTools

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
# Also allows comparison operators < > = that may appear in string data
_SAFE_INPUT_PATTERN = re.compile(r"^[\(\),\s\w\.'\":\-\+<>=]+$")


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
            logger.warning("Row contains unexpected characters, skipping eval: '%s...'", line[:100])
            return line

        # Prefer ast.literal_eval for basic Python literals (safe, no code execution)
        try:
            return RowParser._normalize(ast.literal_eval(line))
        except (ValueError, SyntaxError):
            pass

        # Fallback for Decimal/datetime types that ast.literal_eval cannot handle.
        # Input is already validated by _SAFE_INPUT_PATTERN regex above.
        try:
            value = eval(line, _SAFE_EVAL_CONTEXT)  # noqa: S307 - restricted context, validated input
            return RowParser._normalize(value)
        except Exception as e:
            logger.warning("Failed to parse row '%s...': %s", line[:100], e)
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

        if last_select_idx is None:
            return ""

        if last_from_idx is not None:
            return text[last_select_idx + len("select") : last_from_idx].strip()

        # SELECT without FROM (e.g., scalar subqueries from CTEs).
        # Find next depth-0 terminator: ORDER BY, UNION, HAVING, or end of string.
        end_idx = len(text)
        depth2 = 0
        in_s = False
        in_d = False
        scan_start = last_select_idx + len("select")
        for j in range(scan_start, len(lower_text)):
            ch2 = lower_text[j]
            if ch2 == "'" and not in_d:
                in_s = not in_s
            elif ch2 == '"' and not in_s:
                in_d = not in_d
            elif not in_s and not in_d:
                if ch2 == "(":
                    depth2 += 1
                elif ch2 == ")":
                    depth2 = max(depth2 - 1, 0)
                elif depth2 == 0:
                    for kw in ("order by", "union", "having"):
                        if lower_text.startswith(kw, j):
                            end_idx = j
                            break
                    if end_idx != len(text):
                        break

        return text[scan_start:end_idx].strip()

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
        if len(value) >= 2 and (value[0], value[-1]) in [("[", "]"), ('"', '"'), ("'", "'")]:
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
    """Executes SQL queries and formats results deterministically."""

    def __init__(self, settings: Settings):
        self.settings = settings

    async def execute(
        self,
        sql: str,
        db_tools: DelfosTools | None = None,
    ) -> dict[str, Any]:
        """Execute SQL query and return formatted results.

        Args:
            sql: SQL query to execute
            db_tools: DelfosTools instance for direct DB access
        """
        try:
            if db_tools is None:
                return SQLExecutionResult.error("No database tools available").to_dict()

            execution_result = db_tools.execute_sql(sql)

            if error := execution_result.get("error"):
                logger.error("SQL execution error: %s", error)
                return SQLExecutionResult.error(f"Error executing SQL: {error}").to_dict()

            raw_results = execution_result.get("raw", "")
            row_count = execution_result.get("row_count", 0)

            logger.info("SQL executed successfully: %s rows returned", row_count)
            logger.debug("SQL raw results (first 2000 chars): %s", raw_results[:2000])

            # Parse and format results
            columns = ColumnExtractor.extract(sql)
            rows = RowParser.parse(raw_results)
            resultados = ResultFormatter.format(rows, columns)

            return SQLExecutionResult.success(resultados, row_count).to_dict()

        except Exception as e:
            logger.error("SQL execution error: %s", e, exc_info=True)
            return SQLExecutionResult.error(f"Error executing SQL: {e}").to_dict()
