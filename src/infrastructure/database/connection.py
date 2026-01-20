"""Database connection utilities with connection pooling."""

import asyncio
import logging
import threading
from contextlib import contextmanager
from queue import Empty, Queue
from typing import Any, Generator, cast

import pyodbc

from src.config.settings import Settings
from src.utils.retry import is_pyodbc_timeout_error, run_with_retry

logger = logging.getLogger(__name__)


class ConnectionPool:
    """
    Thread-safe connection pool for pyodbc connections.

    Features:
    - Configurable pool size (min/max connections)
    - Connection health checks before reuse
    - Automatic connection recycling
    - Thread-safe operation
    """

    _instance: "ConnectionPool | None" = None
    _lock = threading.Lock()

    def __init__(
        self,
        connection_string: str,
        min_size: int = 2,
        max_size: int = 10,
        connection_timeout: int = 30,
    ):
        self._connection_string = connection_string
        self._min_size = min_size
        self._max_size = max_size
        self._connection_timeout = connection_timeout

        self._pool: Queue[pyodbc.Connection] = Queue(maxsize=max_size)
        self._size = 0
        self._size_lock = threading.Lock()
        self._closed = False

        # Pre-create minimum connections
        self._initialize_pool()

    def _initialize_pool(self) -> None:
        """Create initial connections up to min_size."""
        for _ in range(self._min_size):
            try:
                conn = self._create_connection()
                self._pool.put(conn)
            except Exception as e:
                logger.warning(f"Failed to pre-create connection: {e}")

    def _create_connection(self) -> pyodbc.Connection:
        """Create a new database connection."""
        with self._size_lock:
            if self._size >= self._max_size:
                raise RuntimeError(f"Connection pool exhausted (max={self._max_size})")
            self._size += 1

        try:
            conn = pyodbc.connect(
                self._connection_string,
                timeout=self._connection_timeout,
            )
            logger.debug(f"Created new connection (pool size: {self._size})")
            return conn
        except Exception:
            with self._size_lock:
                self._size -= 1
            raise

    def _is_connection_healthy(self, conn: pyodbc.Connection) -> bool:
        """Check if connection is still usable."""
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return True
        except Exception:
            return False

    def get_connection(self, timeout: float = 5.0) -> pyodbc.Connection:
        """
        Get a connection from the pool.

        Args:
            timeout: Max seconds to wait for a connection

        Returns:
            A database connection

        Raises:
            RuntimeError: If pool is closed or no connection available
        """
        if self._closed:
            raise RuntimeError("Connection pool is closed")

        # Try to get from pool first
        try:
            conn = self._pool.get(timeout=timeout)
            if self._is_connection_healthy(conn):
                return conn
            # Connection is stale, close it and create new one
            self._close_connection(conn)
        except Empty:
            pass

        # Create new connection if pool is empty and we have capacity
        return self._create_connection()

    def return_connection(self, conn: pyodbc.Connection) -> None:
        """Return a connection to the pool."""
        if self._closed:
            self._close_connection(conn)
            return

        try:
            # Reset connection state
            conn.rollback()
            self._pool.put_nowait(conn)
        except Exception:
            self._close_connection(conn)

    def _close_connection(self, conn: pyodbc.Connection) -> None:
        """Close a connection and update pool size."""
        try:
            conn.close()
        except Exception:
            pass
        with self._size_lock:
            self._size = max(0, self._size - 1)

    def close_all(self) -> None:
        """Close all connections in the pool."""
        self._closed = True
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                self._close_connection(conn)
            except Empty:
                break
        logger.info("Connection pool closed")

    @contextmanager
    def connection(self) -> Generator[pyodbc.Connection, None, None]:
        """Context manager for getting and returning connections."""
        conn = self.get_connection()
        try:
            yield conn
        finally:
            self.return_connection(conn)

    @classmethod
    def get_pool(cls, settings: Settings) -> "ConnectionPool":
        """Get or create the singleton connection pool."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    if not settings.database_connection_string:
                        raise ValueError("database_connection_string is not configured")
                    cls._instance = ConnectionPool(
                        connection_string=settings.database_connection_string,
                        min_size=2,
                        max_size=10,
                    )
        return cls._instance

    @classmethod
    def close_pool(cls) -> None:
        """Close the singleton pool if it exists."""
        with cls._lock:
            if cls._instance is not None:
                cls._instance.close_all()
                cls._instance = None


async def execute_query(
    settings: Settings, sql: str, params: tuple[Any, ...] | None = None
) -> list[dict[str, Any]]:
    """
    Execute a SELECT query and return results as a list of dictionaries.

    Args:
        settings: Application settings containing database_connection_string
        sql: SQL query string (use ? placeholders for parameters)
        params: Optional tuple of parameters for parameterized queries

    Returns:
        List of dictionaries, where each dictionary represents a row

    Raises:
        Exception: If database connection or query execution fails
    """
    pool = ConnectionPool.get_pool(settings)

    def _execute() -> list[dict[str, Any]]:
        """Execute query synchronously using connection pool."""
        with pool.connection() as conn:
            cursor = conn.cursor()
            try:
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)

                columns = [column[0] for column in cursor.description]
                rows = cursor.fetchall()

                return [
                    {col_name: row[i] for i, col_name in enumerate(columns)}
                    for row in rows
                ]
            finally:
                cursor.close()

    async def _execute_with_retry() -> list[dict[str, Any]]:
        return await asyncio.to_thread(_execute)

    return cast(
        list[dict[str, Any]],
        await run_with_retry(
            _execute_with_retry,
            max_retries=5,
            initial_delay=2.0,
            backoff_factor=1.5,
            retry_on_rate_limit=True,
        ),
    )


async def execute_insert(
    settings: Settings, sql: str, params: tuple[Any, ...] | None = None
) -> dict[str, Any]:
    """
    Execute an INSERT, UPDATE, or DELETE query.

    Args:
        settings: Application settings containing database_connection_string
        sql: SQL query string (use ? placeholders for parameters)
        params: Optional tuple of parameters for parameterized queries

    Returns:
        Dictionary with success status and affected row count
    """
    pool = ConnectionPool.get_pool(settings)

    def _execute() -> dict[str, Any]:
        """Execute insert/update/delete synchronously using connection pool."""
        with pool.connection() as conn:
            cursor = conn.cursor()
            try:
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)

                rows_affected = cursor.rowcount
                conn.commit()

                return {
                    "success": True,
                    "rows_affected": rows_affected,
                    "error": None,
                }
            except Exception as e:
                logger.error(f"Database insert/update error: {e}")
                conn.rollback()
                if is_pyodbc_timeout_error(e):
                    raise
                return {
                    "success": False,
                    "rows_affected": 0,
                    "error": str(e),
                }
            finally:
                cursor.close()

    async def _execute_with_retry() -> dict[str, Any]:
        return await asyncio.to_thread(_execute)

    return cast(
        dict[str, Any],
        await run_with_retry(
            _execute_with_retry,
            max_retries=5,
            initial_delay=2.0,
            backoff_factor=1.5,
            retry_on_rate_limit=True,
        ),
    )
