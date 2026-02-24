"""Delfos Database Tools - Direct database access for Agent Framework.

Supports dual connections:
- Warehouse (WH) for agent READ operations (schema exploration, SQL execution)
- SQL Database (DB) for app WRITE operations (insert_agent_output, etc.)
"""

import logging
import queue
import re
import threading
import uuid
from collections.abc import Generator
from contextlib import contextmanager, suppress
from datetime import datetime
from typing import Annotated, Any

import pyodbc
from pydantic import Field

from src.infrastructure.database.connection import FabricConnectionFactory, adapt_sql_for_wh

logger = logging.getLogger(__name__)


class _ConnectionPool:
    """Small thread-safe connection pool for pyodbc (not thread-safe per conn)."""

    def __init__(self, factory: FabricConnectionFactory, label: str, max_size: int = 3):
        self._factory = factory
        self._label = label
        self._max_size = max_size
        self._pool: queue.Queue[pyodbc.Connection] = queue.Queue(maxsize=max_size)
        self._created = 0
        self._lock = threading.Lock()
        # Pre-warm one connection to avoid cold-start latency (~8s token + ODBC)
        try:
            conn = self._factory.create_connection()
            self._pool.put(conn)
            self._created = 1
            logger.info("%s pool: pre-warmed 1 connection", self._label)
        except Exception as e:
            logger.warning("%s pool: warmup failed (non-fatal): %s", self._label, e)

    def acquire(self) -> pyodbc.Connection:
        """Get a connection from the pool or create a new one."""
        try:
            return self._pool.get_nowait()
        except queue.Empty:
            pass

        with self._lock:
            if self._created < self._max_size:
                self._created += 1
                try:
                    conn = self._factory.create_connection()
                    logger.info("%s pool: created connection %s/%s", self._label, self._created, self._max_size)
                    return conn
                except Exception:
                    self._created -= 1
                    raise

        logger.debug("%s pool: at capacity, waiting for available connection", self._label)
        return self._pool.get(timeout=30)

    def release(self, conn: pyodbc.Connection) -> None:
        """Return a healthy connection to the pool."""
        try:
            self._pool.put_nowait(conn)
        except queue.Full:
            with suppress(Exception):
                conn.close()
            with self._lock:
                self._created -= 1

    def discard(self, conn: pyodbc.Connection) -> None:
        """Discard a broken connection (will be replaced on next acquire)."""
        with suppress(Exception):
            conn.close()
        with self._lock:
            self._created -= 1
        logger.info("%s pool: discarded broken connection (%s/%s remaining)", self._label, self._created, self._max_size)

    def close_all(self) -> None:
        """Close all connections in the pool."""
        closed = 0
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                with suppress(Exception):
                    conn.close()
                closed += 1
            except queue.Empty:
                break
        with self._lock:
            self._created = 0
        if closed:
            logger.info("Closed %s %s pool connections", closed, self._label)


class DelfosTools:
    """Database tools for Delfos SQL database interactions.

    Uses two separate Fabric connection pools:
    - WH (Warehouse) for all READ operations (agent tools, SQL execution)
    - DB (SQL Database) for all WRITE operations (insert_agent_output_batch)

    Connection pools allow concurrent tool calls to run in parallel.
    """

    VISUAL_PAGE_MAP = {
        "linea": "Line",
        "barras": "Bar",
        "barras_agrupadas": "StackedBar",
        "pie": "PieChart",
        "scatter": "Scatter",
    }

    @staticmethod
    def _validate_identifier(name: str) -> str:
        """Validate a SQL identifier to prevent injection."""
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
            raise ValueError(f"Invalid SQL identifier: {name}")
        return name

    def __init__(
        self,
        wh_factory: FabricConnectionFactory,
        db_factory: FabricConnectionFactory,
        wh_schema: str = "gold",
        db_schema: str = "dbo",
        workspace_id: str | None = None,
        report_id: str | None = None,
    ):
        """Initialize with dual Fabric connection pools.

        Args:
            wh_factory: Factory for Warehouse connections (reads).
            db_factory: Factory for SQL Database connections (writes).
            wh_schema: Schema for WH tables (default: gold).
            db_schema: Schema for DB tables (default: dbo).
            workspace_id: Power BI workspace ID.
            report_id: Power BI report ID.
        """
        self._wh_schema = wh_schema
        self._db_schema = db_schema
        self._workspace_id = workspace_id
        self._report_id = report_id
        self._wh_pool = _ConnectionPool(wh_factory, label="WH", max_size=5)
        self._db_pool = _ConnectionPool(db_factory, label="DB", max_size=2)

    @contextmanager
    def _get_connection(self, pool: "_ConnectionPool") -> Generator[pyodbc.Connection, None, None]:
        """Acquire a connection from the given pool, auto-release on exit."""
        conn = pool.acquire()
        try:
            yield conn
        except pyodbc.Error:
            pool.discard(conn)
            raise
        except Exception:
            pool.release(conn)
            raise
        else:
            pool.release(conn)

    def _get_wh_connection(self) -> Generator[pyodbc.Connection, None, None]:
        """Acquire a WH connection from the pool."""
        return self._get_connection(self._wh_pool)

    def _get_db_connection(self) -> Generator[pyodbc.Connection, None, None]:
        """Acquire a DB connection from the pool."""
        return self._get_connection(self._db_pool)

    def close(self) -> None:
        """Close all pooled connections (call on shutdown)."""
        self._wh_pool.close_all()
        self._db_pool.close_all()

    def _adapt_sql_for_wh(self, sql: str) -> str:
        """Replace dbo schema references with WH schema for read queries."""
        return adapt_sql_for_wh(sql, target_schema=self._wh_schema)

    # =========================================================================
    # READ TOOLS — use WH connection
    # =========================================================================

    def execute_sql_query(
        self, query: Annotated[str, Field(description="The SQL query to execute.")]
    ) -> str:
        """Execute a SQL query against the Delfos Warehouse and return the results.

        Args:
            query (str): The SQL query to execute.
        Returns:
            str: The results of the SQL query.
        """
        adapted_query = self._adapt_sql_for_wh(query)
        with self._get_wh_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(adapted_query)
            results = cursor.fetchall()
            cursor.close()

        result_str = "\n".join([str(row) for row in results])
        return result_str if result_str else "No results found."

    def get_table_schema(
        self,
        table_name: Annotated[
            str, Field(description="The name of the table to get the schema for.")
        ],
    ) -> str:
        """Retrieve the schema of a specified table in the Delfos Warehouse.

        Args:
            table_name (str): The name of the table to get the schema for.
        Returns:
            str: The schema of the specified table.
        """
        with self._get_wh_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?",
                (table_name,),
            )
            columns = cursor.fetchall()
            cursor.close()

        if not columns:
            return f"No schema found for table '{table_name}'."

        schema_str = "\n".join([f"{col.COLUMN_NAME}: {col.DATA_TYPE}" for col in columns])
        return schema_str

    def list_tables(self) -> str:
        """List all tables in the Delfos Warehouse.

        Returns:
            str: A list of all table names in the database.
        """
        with self._get_wh_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
            )
            tables = cursor.fetchall()
            cursor.close()

        table_list = [table.TABLE_NAME for table in tables]
        return "\n".join(table_list) if table_list else "No tables found in the database."

    def get_database_info(self) -> str:
        """Retrieve general information about the Delfos Warehouse.

        Returns:
            str: General information about the database.
        """
        with self._get_wh_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DB_NAME() AS DatabaseName")
            info = cursor.fetchone()

            cursor.execute(
                "SELECT COUNT(*) AS TableCount FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_TYPE = 'BASE TABLE'"
            )
            table_count = cursor.fetchone()
            cursor.close()

        return (
            f"Database Name: {info.DatabaseName}\n"
            f"Total Tables: {table_count.TableCount}"
        )

    def get_table_row_count(
        self,
        table_name: Annotated[str, Field(description="The name of the table to count rows for.")],
    ) -> str:
        """Get the number of rows in a specified table.

        Args:
            table_name (str): The name of the table to count rows for.
        Returns:
            str: The number of rows in the specified table.
        """
        with self._get_wh_connection() as conn:
            cursor = conn.cursor()
            table = self._validate_identifier(table_name)
            cursor.execute(
                f"SELECT COUNT(*) AS TotalRows FROM [{self._wh_schema}].[{table}]"
            )
            row_count = cursor.fetchone()
            cursor.close()

        return f"Table '{table_name}' has {row_count.TotalRows} rows."

    def get_primary_keys(
        self,
        table_name: Annotated[
            str, Field(description="The name of the table to get primary keys for.")
        ],
    ) -> str:
        """Retrieve the primary keys of a specified table.

        Args:
            table_name (str): The name of the table to get primary keys for.
        Returns:
            str: The primary keys of the specified table.
        """
        with self._get_wh_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT kcu.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                    AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
                WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                    AND tc.TABLE_NAME = ?
                ORDER BY kcu.ORDINAL_POSITION
                """,
                (table_name,),
            )
            keys = cursor.fetchall()
            cursor.close()

        if not keys:
            return f"No primary keys found for table '{table_name}'."

        key_list = [key.COLUMN_NAME for key in keys]
        return ", ".join(key_list)

    def get_distinct_values(
        self,
        table_name: Annotated[str, Field(description="The name of the table.")],
        column_name: Annotated[str, Field(description="The name of the column.")],
    ) -> str:
        """Retrieve distinct values from a specified column in a table.

        Args:
            table_name (str): The name of the table.
            column_name (str): The name of the column.
        Returns:
            str: Distinct values from the specified column.
        """
        with self._get_wh_connection() as conn:
            cursor = conn.cursor()
            table = self._validate_identifier(table_name)
            column = self._validate_identifier(column_name)
            cursor.execute(
                f"SELECT DISTINCT [{column}] FROM [{self._wh_schema}].[{table}]"
            )
            values = cursor.fetchall()
            cursor.close()

        if not values:
            return f"No distinct values found in column '{column_name}' of table '{table_name}'."

        value_list = [str(value[0]) for value in values]
        return "\n".join(value_list)

    def get_table_relationships(self) -> str:
        """Retrieve foreign key relationships between tables.

        Returns:
            str: List of foreign key relationships in the database.
        """
        with self._get_wh_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT
                    rc.CONSTRAINT_NAME AS ForeignKey,
                    kcu1.TABLE_NAME AS ParentTable,
                    kcu1.COLUMN_NAME AS ParentColumn,
                    kcu2.TABLE_NAME AS ReferencedTable,
                    kcu2.COLUMN_NAME AS ReferencedColumn
                FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu1
                    ON rc.CONSTRAINT_NAME = kcu1.CONSTRAINT_NAME
                    AND rc.CONSTRAINT_SCHEMA = kcu1.CONSTRAINT_SCHEMA
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu2
                    ON rc.UNIQUE_CONSTRAINT_NAME = kcu2.CONSTRAINT_NAME
                    AND rc.UNIQUE_CONSTRAINT_SCHEMA = kcu2.CONSTRAINT_SCHEMA
                    AND kcu1.ORDINAL_POSITION = kcu2.ORDINAL_POSITION
            """)
            relationships = cursor.fetchall()
            cursor.close()

        if not relationships:
            return "No foreign key relationships found in the database."

        rel_list = [
            f"Foreign Key: {rel.ForeignKey}, {rel.ParentTable}({rel.ParentColumn}) -> {rel.ReferencedTable}({rel.ReferencedColumn})"
            for rel in relationships
        ]
        return "\n".join(rel_list)

    # =========================================================================
    # WRITE TOOLS — use DB connection
    # =========================================================================

    def insert_agent_output_batch(
        self,
        user_id: str,
        question: str,
        results: list[dict[str, Any]],
        metric_name: str,
        visual_hint: str,
    ) -> str:
        """Insert multiple rows of query results into agent_output table.

        Args:
            user_id: User identifier/email.
            question: The natural language question asked by the user.
            results: List of result rows, each with keys:
                x_value (str), y_value (float), series (str, optional), category (str, optional).
            metric_name: Name of the metric being measured.
            visual_hint: Visualization type ('pie', 'bar', 'line', 'table').

        Returns:
            The run_id generated for this batch.
        """
        run_id = str(uuid.uuid4())
        created_at = datetime.now()

        query = f"""
            INSERT INTO [{self._db_schema}].[agent_output]
                ([run_id], [user_id], [question], [x_value], [y_value],
                 [series], [category], [metric_name], [visual_hint], [created_at])
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        with self._get_db_connection() as conn:
            cursor = conn.cursor()

            params_list = [
                (
                    run_id,
                    user_id,
                    question,
                    row.get("x_value"),
                    row.get("y_value"),
                    row.get("series") or row.get("category"),
                    row.get("category"),
                    metric_name,
                    visual_hint,
                    created_at,
                )
                for row in results
            ]
            cursor.fast_executemany = True
            cursor.executemany(query, params_list)
            rows_inserted = len(params_list)

            conn.commit()
            cursor.close()

        logger.info("Inserted %s rows with run_id: %s", rows_inserted, run_id)
        return run_id

    def generate_powerbi_url(self, run_id: str, visual_hint: str) -> str:
        """Generate a Power BI report URL filtered by run_id and visual type.

        Args:
            run_id: The run identifier to filter the report.
            visual_hint: The type of visualization.
        Returns:
            The generated Power BI report URL, or empty string if not configured.
        """
        if not self._workspace_id or not self._report_id:
            logger.warning("Power BI workspace_id or report_id not configured")
            return ""

        page_name = self.VISUAL_PAGE_MAP.get(visual_hint, "ReportSectionBarras")

        url = (
            f"https://app.powerbi.com/groups/{self._workspace_id}/reports/{self._report_id}"
            f"?pageName={page_name}"
            f"&filter=agent_output/run_id%20eq%20'{run_id}'"
        )

        return url

    # =========================================================================
    # AGENT TOOL LISTS
    # =========================================================================

    def get_exploration_tools(self) -> list[Any]:
        """Get exploration tools for SQL generation agents."""
        return [
            self.list_tables,
            self.get_table_schema,
            self.get_table_relationships,
            self.get_distinct_values,
            self.get_primary_keys,
        ]

    def get_all_tools(self) -> list[Any]:
        """Get all available agent tools."""
        return [
            self.list_tables,
            self.get_table_schema,
            self.get_table_relationships,
            self.get_distinct_values,
            self.get_primary_keys,
            self.get_database_info,
            self.get_table_row_count,
            self.execute_sql_query,
        ]

    # =========================================================================
    # SERVICE HELPERS (not agent tools)
    # =========================================================================

    def execute_sql(self, sql: str) -> dict[str, Any]:
        """Execute SQL and return a structured result dict.

        Args:
            sql (str): The SQL query to execute.

        Returns:
            dict: Result with keys: data, row_count, raw, error.
        """
        try:
            adapted_sql = self._adapt_sql_for_wh(sql)
            with self._get_wh_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(adapted_sql)
                results = cursor.fetchall()
                columns = [col[0] for col in cursor.description] if cursor.description else []
                cursor.close()

            if not results:
                return {"data": [], "row_count": 0, "raw": "No results found.", "error": None}

            rows = [dict(zip(columns, row)) for row in results]
            raw_str = "\n".join(str(tuple(row.values())) for row in rows)
            return {"data": rows, "row_count": len(rows), "raw": raw_str, "error": None}

        except pyodbc.Error as e:
            logger.error("SQL execution error: %s", e)
            return {"data": [], "row_count": 0, "raw": "", "error": str(e)}

    def get_schema(self, table_name: str) -> dict[str, Any]:
        """Get table schema as a structured result dict.

        Args:
            table_name (str): The name of the table.

        Returns:
            dict: Result with keys: name, columns (list of {name, type}).
        """
        try:
            with self._get_wh_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
                    "WHERE TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
                    (table_name,),
                )
                columns = cursor.fetchall()
                cursor.close()

            if not columns:
                return {"name": table_name, "columns": []}

            return {
                "name": table_name,
                "columns": [{"name": col.COLUMN_NAME, "type": col.DATA_TYPE} for col in columns],
            }

        except pyodbc.Error as e:
            logger.error("Schema retrieval error: %s", e)
            return {"name": table_name, "columns": [], "error": str(e)}
