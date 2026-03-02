"""Direct database tools for the Agent Framework."""

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
from src.utils.retry import is_transient_pyodbc_error

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

    @staticmethod
    def _is_alive(conn: pyodbc.Connection) -> bool:
        """Quick liveness check — return False if the connection is stale."""
        try:
            conn.execute("SELECT 1")
            return True
        except Exception:
            return False

    def acquire(self) -> pyodbc.Connection:
        """Get a connection from the pool or create a new one."""
        try:
            conn = self._pool.get_nowait()
            if self._is_alive(conn):
                return conn
            # Stale connection — discard and fall through to create new
            self.discard(conn)
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

    def ping_idle_connections(self) -> tuple[int, int]:
        """Ping idle connections; discard stale ones and refill if empty.

        Returns ``(pinged, replaced)`` counts.
        """
        pinged = 0
        replaced = 0
        survivors: list[pyodbc.Connection] = []

        while True:
            try:
                conn = self._pool.get_nowait()
            except queue.Empty:
                break
            pinged += 1
            if self._is_alive(conn):
                survivors.append(conn)
            else:
                self.discard(conn)
                replaced += 1

        for conn in survivors:
            try:
                self._pool.put_nowait(conn)
            except queue.Full:
                with suppress(Exception):
                    conn.close()
                with self._lock:
                    self._created -= 1

        # Refill at least 1 connection if pool is now empty
        with self._lock:
            need_refill = self._pool.empty() and self._created == 0
            if need_refill:
                self._created += 1
        if need_refill:
            try:
                new_conn = self._factory.create_connection()
                self._pool.put_nowait(new_conn)
                logger.info("%s pool: keep-alive created replacement connection", self._label)
            except Exception as exc:
                with self._lock:
                    self._created -= 1
                logger.warning("%s pool: keep-alive replacement failed: %s", self._label, exc)

        return pinged, replaced

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
    """Database tools using dual Fabric pools (WH for reads, DB for writes)."""

    VISUAL_PAGE_MAP = {
        "linea": "Line",
        "barras": "Bar",
        "barras_agrupadas": "StackedBar",
        "pie": "PieChart",
        "scatter": "Scatter",
    }

    @staticmethod
    def _validate_identifier(name: str) -> str:
        """Validate a SQL identifier against injection."""
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
        self._wh_schema = wh_schema
        self._db_schema = db_schema
        self._workspace_id = workspace_id
        self._report_id = report_id
        self._wh_pool = _ConnectionPool(wh_factory, label="WH", max_size=5)
        self._db_pool = _ConnectionPool(db_factory, label="DB", max_size=2)

    @contextmanager
    def _get_connection(self, pool: "_ConnectionPool") -> Generator[pyodbc.Connection, None, None]:
        """Context manager: acquire from pool, auto-release or discard on error."""
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
        """Acquire a Warehouse connection."""
        return self._get_connection(self._wh_pool)

    def _get_db_connection(self) -> Generator[pyodbc.Connection, None, None]:
        """Acquire a Database connection."""
        return self._get_connection(self._db_pool)

    def close(self) -> None:
        """Close all pooled connections."""
        self._wh_pool.close_all()
        self._db_pool.close_all()

    def ping_idle_connections(self) -> tuple[int, int]:
        """Ping idle connections in both pools; return ``(pinged, replaced)``."""
        wh_pinged, wh_replaced = self._wh_pool.ping_idle_connections()
        db_pinged, db_replaced = self._db_pool.ping_idle_connections()
        return wh_pinged + db_pinged, wh_replaced + db_replaced

    def _adapt_sql_for_wh(self, sql: str) -> str:
        """Replace dbo schema references with the WH schema."""
        return adapt_sql_for_wh(sql, target_schema=self._wh_schema)

    # =========================================================================
    # READ TOOLS — use WH connection
    # =========================================================================

    def execute_sql_query(
        self, query: Annotated[str, Field(description="The SQL query to execute.")]
    ) -> str:
        """Execute a SQL query against the Warehouse and return results."""
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
        """Retrieve column names and types for a table."""
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
        """List all base tables in the Warehouse."""
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
        """Return database name and table count."""
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
        """Return the row count for a table."""
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
        """Return comma-separated primary key columns for a table."""
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
        """Return distinct values from a column."""
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
        """Return foreign key relationships between tables."""
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

    _INSERT_MAX_RETRIES = 2

    def insert_agent_output_batch(
        self,
        user_id: str,
        question: str,
        results: list[dict[str, Any]],
        metric_name: str,
        visual_hint: str,
    ) -> str:
        """Batch-insert query results into agent_output and return the generated run_id."""
        run_id = str(uuid.uuid4())
        created_at = datetime.now()

        query = f"""
            INSERT INTO [{self._db_schema}].[agent_output]
                ([run_id], [user_id], [question], [x_value], [y_value],
                 [series], [category], [metric_name], [visual_hint], [created_at])
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

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

        last_error: Exception | None = None
        for attempt in range(1, self._INSERT_MAX_RETRIES + 1):
            try:
                with self._get_db_connection() as conn:
                    cursor = conn.cursor()
                    cursor.fast_executemany = True
                    cursor.executemany(query, params_list)
                    conn.commit()
                    cursor.close()

                logger.info("Inserted %s rows with run_id: %s", len(params_list), run_id)
                return run_id
            except pyodbc.Error as e:
                last_error = e
                if attempt < self._INSERT_MAX_RETRIES and is_transient_pyodbc_error(e):
                    logger.warning(
                        "Insert transient error (attempt %s/%s), retrying: %s",
                        attempt, self._INSERT_MAX_RETRIES, e,
                    )
                    continue
                raise

        raise last_error  # type: ignore[misc]

    def generate_powerbi_url(self, run_id: str, visual_hint: str) -> str:
        """Generate a Power BI report URL filtered by run_id."""
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
        """Return the schema-exploration tool list."""
        return [
            self.list_tables,
            self.get_table_schema,
            self.get_table_relationships,
            self.get_distinct_values,
            self.get_primary_keys,
        ]

    def get_all_tools(self) -> list[Any]:
        """Return all available agent tools."""
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

    _SQL_MAX_RETRIES = 2

    def execute_sql(self, sql: str) -> dict[str, Any]:
        """Execute SQL with one transient-error retry, returning a structured result dict."""
        adapted_sql = self._adapt_sql_for_wh(sql)
        last_error: Exception | None = None

        for attempt in range(1, self._SQL_MAX_RETRIES + 1):
            try:
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
                last_error = e
                if attempt < self._SQL_MAX_RETRIES and is_transient_pyodbc_error(e):
                    logger.warning(
                        "SQL transient error (attempt %s/%s), retrying: %s",
                        attempt, self._SQL_MAX_RETRIES, e,
                    )
                    continue
                logger.error("SQL execution error: %s", e)
                return {"data": [], "row_count": 0, "raw": "", "error": str(e)}

        logger.error("SQL execution failed after %s attempts: %s", self._SQL_MAX_RETRIES, last_error)
        return {"data": [], "row_count": 0, "raw": "", "error": str(last_error)}

    def get_schema(self, table_name: str) -> dict[str, Any]:
        """Return table schema as ``{name, columns: [{name, type}]}``."""
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
