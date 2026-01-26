"""
Delfos Database Tools - Direct database access for Agent Framework.

This module provides the same functionality as delfos_mcp.py but as a class
for use with Microsoft Agent Framework function tools.

Location: src/infrastructure/database/tools.py
"""

import uuid
import logging
from datetime import datetime
from typing import Annotated, Any
from contextlib import contextmanager

import pyodbc
from pydantic import Field

logger = logging.getLogger(__name__)


class DelfosTools:
    """
    Database tools for Delfos SQL database interactions.

    This class provides the same functionality as the FastMCP server tools
    but can be used directly with Microsoft Agent Framework.

    Usage:
        tools = DelfosTools(connection_string="...", workspace_id="...", report_id="...")
        agent = client.create_agent(
            instructions="...",
            tools=tools.get_exploration_tools()
        )
    """

    VISUAL_PAGE_MAP = {
        "linea": "Line",
        "barras": "Bar",
        "barras_agrupadas": "StackedBar",
        "pie": "PieChart",
    }

    def __init__(
        self,
        connection_string: str,
        workspace_id: str | None = None,
        report_id: str | None = None,
    ):
        """
        Initialize DelfosTools with database and Power BI configuration.

        Args:
            connection_string: Azure SQL Database connection string.
            workspace_id: Power BI workspace ID.
            report_id: Power BI report ID.
        """
        self._connection_string = connection_string
        self._workspace_id = workspace_id
        self._report_id = report_id
        pyodbc.pooling = True

        if not self._connection_string:
            raise ValueError("connection_string is required")

    @contextmanager
    def _get_connection(self):
        """Establish and return a connection to the Azure SQL Database."""
        conn = pyodbc.connect(self._connection_string)
        try:
            yield conn
        finally:
            conn.close()

    def execute_sql_query(
        self,
        query: Annotated[str, Field(description="The SQL query to execute.")]
    ) -> str:
        """
        Execute a SQL query against the Delfos database and return the results.

        Args:
            query (str): The SQL query to execute.
        Returns:
            str: The results of the SQL query.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()

        result_str = "\n".join([str(row) for row in results])
        return result_str if result_str else "No results found."

    def get_table_schema(
        self,
        table_name: Annotated[str, Field(description="The name of the table to get the schema for.")]
    ) -> str:
        """
        Retrieve the schema of a specified table in the Delfos database.

        Args:
            table_name (str): The name of the table to get the schema for.
        Returns:
            str: The schema of the specified table.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
                f"WHERE TABLE_NAME = '{table_name}'"
            )
            columns = cursor.fetchall()
            cursor.close()

        if not columns:
            return f"No schema found for table '{table_name}'."

        schema_str = "\n".join([f"{col.COLUMN_NAME}: {col.DATA_TYPE}" for col in columns])
        return schema_str

    def list_tables(self) -> str:
        """
        List all tables in the Delfos database.

        Returns:
            str: A list of all table names in the database.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_TYPE = 'BASE TABLE'"
            )
            tables = cursor.fetchall()
            cursor.close()

        table_list = [table.TABLE_NAME for table in tables]
        return "\n".join(table_list) if table_list else "No tables found in the database."

    def get_database_info(self) -> str:
        """
        Retrieve general information about the Delfos database.

        Returns:
            str: General information about the database.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT DB_NAME() AS DatabaseName, "
                "SERVERPROPERTY('ProductVersion') AS Version"
            )
            info = cursor.fetchone()
            cursor.close()

        return f"Database Name: {info.DatabaseName}\nVersion: {info.Version}"

    def get_table_row_count(
        self,
        table_name: Annotated[str, Field(description="The name of the table to count rows for.")]
    ) -> str:
        """
        Get the number of rows in a specified table.

        Args:
            table_name (str): The name of the table to count rows for.
        Returns:
            str: The number of rows in the specified table.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) AS TotalRows FROM [dbo].[{table_name}]")
            row_count = cursor.fetchone()
            cursor.close()

        return f"Table '{table_name}' has {row_count.TotalRows} rows."

    def get_primary_keys(
        self,
        table_name: Annotated[str, Field(description="The name of the table to get primary keys for.")]
    ) -> str:
        """
        Retrieve the primary keys of a specified table.

        Args:
            table_name (str): The name of the table to get primary keys for.
        Returns:
            str: The primary keys of the specified table.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1
                AND TABLE_NAME = '{table_name}'
            """)
            keys = cursor.fetchall()
            cursor.close()

        if not keys:
            return f"No primary keys found for table '{table_name}'."

        key_list = [key.COLUMN_NAME for key in keys]
        return ", ".join(key_list)

    def get_distinct_values(
        self,
        table_name: Annotated[str, Field(description="The name of the table.")],
        column_name: Annotated[str, Field(description="The name of the column.")]
    ) -> str:
        """
        Retrieve distinct values from a specified column in a table.

        Args:
            table_name (str): The name of the table.
            column_name (str): The name of the column.
        Returns:
            str: Distinct values from the specified column.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT DISTINCT [{column_name}] FROM [dbo].[{table_name}]")
            values = cursor.fetchall()
            cursor.close()

        if not values:
            return f"No distinct values found in column '{column_name}' of table '{table_name}'."

        value_list = [str(value[0]) for value in values]
        return "\n".join(value_list)

    def get_table_relationships(self) -> str:
        """
        Retrieve foreign key relationships between tables.

        Returns:
            str: List of foreign key relationships in the database.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT
                    fk.name AS ForeignKey,
                    tp.name AS ParentTable,
                    cp.name AS ParentColumn,
                    tr.name AS ReferencedTable,
                    cr.name AS ReferencedColumn
                FROM
                    sys.foreign_keys AS fk
                INNER JOIN
                    sys.foreign_key_columns AS fkc ON fk.object_id = fkc.constraint_object_id
                INNER JOIN
                    sys.tables AS tp ON fkc.parent_object_id = tp.object_id
                INNER JOIN
                    sys.columns AS cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
                INNER JOIN
                    sys.tables AS tr ON fkc.referenced_object_id = tr.object_id
                INNER JOIN
                    sys.columns AS cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
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

    def insert_agent_output_batch(
        self,
        user_id: str,
        question: str,
        results: list[dict],
        metric_name: str,
        visual_hint: str,
    ) -> str:
        """
        Insert multiple rows of query results into agent_output table.

        Args:
            user_id (str): User identifier/email.
            question (str): The natural language question asked by the user.
            results (List[Dict]): List of result rows, each with keys:
                - x_value (str): X-axis value
                - y_value (float): Y-axis numeric value
                - series (str, optional): Series name for grouping (if not provided, category will be used)
                - category (str, optional): Category name
            metric_name (str): Name of the metric being measured.
            visual_hint (str): Visualization type ('pie', 'bar', 'line', 'table').

        Returns:
            str: The run_id generated for this batch.

        Example:
            results = [
                {"x_value": "United States", "y_value": 123456.78, "category": "United States"},
                {"x_value": "Canada", "y_value": 45678.90, "category": "Canada"}
            ]
        """
        run_id = str(uuid.uuid4())
        created_at = datetime.now()

        query = """
            INSERT INTO [dbo].[agent_output]
                ([run_id], [user_id], [question], [x_value], [y_value],
                 [series], [category], [metric_name], [visual_hint], [created_at])
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        with self._get_connection() as conn:
            cursor = conn.cursor()
            rows_inserted = 0

            for row in results:
                series_value = row.get("series") or row.get("category")
                cursor.execute(query, (
                    run_id,
                    user_id,
                    question,
                    row.get("x_value"),
                    row.get("y_value"),
                    series_value,
                    row.get("category"),
                    metric_name,
                    visual_hint,
                    created_at
                ))
                rows_inserted += 1

            conn.commit()
            cursor.close()

        logger.info(f"Inserted {rows_inserted} rows with run_id: {run_id}")
        return run_id

    def generate_powerbi_url(self, run_id: str, visual_hint: str) -> str:
        """
        Generate a Power BI report URL filtered by run_id and visual type.

        Args:
            run_id (str): The run identifier to filter the report.
            visual_hint (str): The type of visualization ('linea', 'barras', 'barras_agrupadas', 'pie').
        Returns:
            str: The generated Power BI report URL.
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

    def get_exploration_tools(self) -> list:
        """
        Get exploration tools for SQL generation agents.

        Returns the same tools that were filtered with:
            exploration_tools = [
                "list_tables",
                "get_table_schema",
                "get_table_relationships",
                "get_distinct_values",
                "get_primary_keys",
            ]

        Returns:
            list: List of bound methods for agent tools parameter.
        """
        return [
            self.list_tables,
            self.get_table_schema,
            self.get_table_relationships,
            self.get_distinct_values,
            self.get_primary_keys,
        ]

    def get_all_tools(self) -> list:
        """
        Get all available agent tools.

        Returns:
            list: List of all bound methods for agent tools parameter.
        """
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

    def execute_sql(self, sql: str) -> dict[str, Any]:
        """
        Execute SQL and return structured result for SQLExecutor service.

        This method is not an agent tool. It returns a dict matching
        the MCPClient.execute_sql() format for service compatibility.

        Args:
            sql (str): The SQL query to execute.

        Returns:
            dict: Result with keys: data, row_count, raw, error
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql)
                results = cursor.fetchall()
                cursor.close()

            if not results:
                return {"data": [], "row_count": 0, "raw": "No results found.", "error": None}

            rows = [str(row) for row in results]
            return {"data": rows, "row_count": len(rows), "raw": "\n".join(rows), "error": None}

        except pyodbc.Error as e:
            logger.error(f"SQL execution error: {e}")
            return {"data": [], "row_count": 0, "raw": "", "error": str(e)}

    def get_schema(self, table_name: str) -> dict[str, Any]:
        """
        Get table schema as structured result for SchemaService.

        This method is not an agent tool. It returns a dict matching
        the MCPClient.get_table_schema() format for service compatibility.

        Args:
            table_name (str): The name of the table.

        Returns:
            dict: Result with keys: name, columns (list of {name, type})
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
                    f"WHERE TABLE_NAME = '{table_name}' ORDER BY ORDINAL_POSITION"
                )
                columns = cursor.fetchall()
                cursor.close()

            if not columns:
                return {"name": table_name, "columns": []}

            return {
                "name": table_name,
                "columns": [{"name": col.COLUMN_NAME, "type": col.DATA_TYPE} for col in columns]
            }

        except pyodbc.Error as e:
            logger.error(f"Schema retrieval error: {e}")
            return {"name": table_name, "columns": [], "error": str(e)}
