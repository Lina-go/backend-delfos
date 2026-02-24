"""Database connection utilities with connection pooling for Microsoft Fabric."""

import asyncio
import logging
import re
import struct
import threading
import time
from collections.abc import Generator
from contextlib import contextmanager, suppress
from queue import Empty, Queue
from typing import Any, cast

import pyodbc
from azure.identity import ClientSecretCredential, DefaultAzureCredential

from src.config.settings import Settings
from src.utils.retry import is_transient_pyodbc_error, run_with_retry

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared sync credential singleton — reused by both DB and WH pools so the
# token is fetched only once.
# ---------------------------------------------------------------------------
_shared_sync_credential: DefaultAzureCredential | ClientSecretCredential | None = None
_credential_lock = threading.Lock()


def get_shared_sync_credential(settings: Settings) -> DefaultAzureCredential | ClientSecretCredential:
    """Return a shared sync credential, creating it on first call."""
    global _shared_sync_credential
    if _shared_sync_credential is not None:
        return _shared_sync_credential
    with _credential_lock:
        if _shared_sync_credential is not None:
            return _shared_sync_credential
        if settings.use_service_principal:
            logger.info("DB credential: Service Principal (ClientSecretCredential)")
            _shared_sync_credential = ClientSecretCredential(
                tenant_id=settings.azure_tenant_id,
                client_id=settings.azure_client_id,
                client_secret=settings.azure_client_secret,
            )
        else:
            logger.info("DB credential: DefaultAzureCredential (Managed Identity)")
            _shared_sync_credential = DefaultAzureCredential()
        return _shared_sync_credential


def close_shared_sync_credential() -> None:
    """Close and discard the shared sync credential."""
    global _shared_sync_credential
    with _credential_lock:
        if _shared_sync_credential is not None:
            with suppress(Exception):
                _shared_sync_credential.close()
            _shared_sync_credential = None


class FabricConnectionFactory:
    """
    Factory for Microsoft Fabric connections via Managed Identity.

    Handles token acquisition and automatic refresh (tokens expire ~1 hour).
    Thread-safe.
    """

    TOKEN_SCOPE = "https://database.windows.net/.default"
    TOKEN_REFRESH_MARGIN = 300  # refresh 5 minutes before expiry

    def __init__(
        self,
        server: str,
        database: str,
        connection_timeout: int = 30,
        credential: DefaultAzureCredential | None = None,
    ):
        self._server = server
        self._database = database
        self._timeout = connection_timeout
        self._credential = credential or DefaultAzureCredential()
        self._token: str | None = None
        self._token_expiry: float = 0
        self._token_lock = threading.Lock()

    def _get_token_struct(self) -> bytes:
        """Get a valid token struct, refreshing if needed."""
        with self._token_lock:
            now = time.time()
            if self._token is None or now >= (self._token_expiry - self.TOKEN_REFRESH_MARGIN):
                access_token = self._credential.get_token(self.TOKEN_SCOPE)
                self._token = access_token.token
                self._token_expiry = access_token.expires_on
                logger.debug("Fabric token refreshed for %s, expires at %s", self._database, self._token_expiry)

            token_bytes = self._token.encode("UTF-16-LE")
            return struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)

    def create_connection(self) -> pyodbc.Connection:
        """Create a new Fabric connection with current token."""
        logger.info("Creating Fabric connection to %s/%s", self._server, self._database)
        token_struct = self._get_token_struct()
        conn_str = (
            "Driver={ODBC Driver 18 for SQL Server};"
            f"Server={self._server};"
            f"Database={self._database};"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
        )
        conn = pyodbc.connect(conn_str, timeout=self._timeout, attrs_before={1256: token_struct})
        logger.debug("Connected to %s/%s", self._server, self._database)
        return conn


class ConnectionPool:
    """
    Thread-safe connection pool for pyodbc connections.

    Supports dual pools: one for Warehouse (reads) and one for DB (writes).

    Features:
    - Configurable pool size (min/max connections)
    - Connection health checks before reuse
    - Automatic connection recycling
    - Thread-safe operation
    """

    _wh_instance: "ConnectionPool | None" = None
    _db_instance: "ConnectionPool | None" = None
    _lock = threading.Lock()

    def __init__(
        self,
        factory: FabricConnectionFactory,
        min_size: int = 2,
        max_size: int = 10,
    ):
        self._factory = factory
        self._min_size = min_size
        self._max_size = max_size

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
                logger.warning("Failed to pre-create connection: %s", e)

    def _create_connection(self) -> pyodbc.Connection:
        """Create a new database connection via factory."""
        with self._size_lock:
            if self._size >= self._max_size:
                raise RuntimeError(f"Connection pool exhausted (max={self._max_size})")
            self._size += 1

        try:
            conn = self._factory.create_connection()
            logger.debug("Created new connection (pool size: %s)", self._size)
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
        except pyodbc.Error:
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
        except (pyodbc.Error, Exception):
            self._close_connection(conn)

    def _close_connection(self, conn: pyodbc.Connection) -> None:
        """Close a connection and update pool size."""
        with suppress(Exception):
            conn.close()
        with self._size_lock:
            self._size = max(0, self._size - 1)

    @property
    def stats(self) -> dict[str, int]:
        """Return pool statistics."""
        available = self._pool.qsize()
        return {
            "total_connections": self._size,
            "available": available,
            "in_use": self._size - available,
            "max_size": self._max_size,
        }

    def close_all(self) -> None:
        """Close all connections in the pool."""
        self._closed = True
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                self._close_connection(conn)
            except Empty:
                break
        logger.debug("Connection pool closed")

    @contextmanager
    def connection(self) -> Generator[pyodbc.Connection, None, None]:
        """Context manager for getting and returning connections."""
        conn = self.get_connection()
        try:
            yield conn
        finally:
            self.return_connection(conn)

    @classmethod
    def get_db_pool(cls, settings: Settings) -> "ConnectionPool":
        """Get or create the DB (writes) connection pool."""
        if cls._db_instance is None:
            with cls._lock:
                if cls._db_instance is None:
                    if not settings.db_server or not settings.db_database:
                        raise ValueError("db_server and db_database are required")
                    credential = get_shared_sync_credential(settings)
                    factory = FabricConnectionFactory(
                        settings.db_server, settings.db_database, credential=credential,
                    )
                    cls._db_instance = ConnectionPool(factory, min_size=1, max_size=10)
        return cls._db_instance

    @classmethod
    def get_wh_pool(cls, settings: Settings) -> "ConnectionPool":
        """Get or create the WH (reads) connection pool."""
        if cls._wh_instance is None:
            with cls._lock:
                if cls._wh_instance is None:
                    if not settings.wh_server or not settings.wh_database:
                        raise ValueError("wh_server and wh_database are required")
                    credential = get_shared_sync_credential(settings)
                    factory = FabricConnectionFactory(
                        settings.wh_server, settings.wh_database, credential=credential,
                    )
                    cls._wh_instance = ConnectionPool(factory, min_size=1, max_size=10)
        return cls._wh_instance

    @classmethod
    def close_all_pools(cls) -> None:
        """Close all singleton pools."""
        with cls._lock:
            if cls._db_instance is not None:
                cls._db_instance.close_all()
                cls._db_instance = None
            if cls._wh_instance is not None:
                cls._wh_instance.close_all()
                cls._wh_instance = None


async def execute_query(
    settings: Settings, sql: str, params: tuple[Any, ...] | None = None
) -> list[dict[str, Any]]:
    """
    Execute a SELECT query and return results as a list of dictionaries.

    Uses the DB pool (SuperDB) for application CRUD operations.

    Args:
        settings: Application settings
        sql: SQL query string (use ? placeholders for parameters)
        params: Optional tuple of parameters for parameterized queries

    Returns:
        List of dictionaries, where each dictionary represents a row
    """
    pool = ConnectionPool.get_db_pool(settings)

    def _execute() -> list[dict[str, Any]]:
        """Execute query synchronously using connection pool."""
        with pool.connection() as conn:
            cursor = conn.cursor()
            try:
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)

                if cursor.description is None:
                    return []
                columns = [column[0] for column in cursor.description]
                rows = cursor.fetchall()

                return [{col_name: row[i] for i, col_name in enumerate(columns)} for row in rows]
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


def adapt_sql_for_wh(sql: str, target_schema: str = "gold") -> str:
    """Replace dbo schema references with the warehouse schema.

    Handles both bracketed ([dbo].) and unbracketed (dbo.) variants,
    using case-insensitive matching.
    """
    sql = re.sub(r"\[dbo\]\.", f"[{target_schema}].", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bdbo\.", f"[{target_schema}].", sql, flags=re.IGNORECASE)
    return sql


async def execute_wh_query(
    settings: Settings, sql: str, params: tuple[Any, ...] | None = None
) -> list[dict[str, Any]]:
    """Execute a SELECT query against the Warehouse (gold schema).

    Same interface as ``execute_query`` but uses the WH connection pool.
    Schema references are automatically adapted (dbo -> wh_schema).
    """
    sql = adapt_sql_for_wh(sql, target_schema=settings.wh_schema)
    pool = ConnectionPool.get_wh_pool(settings)

    def _execute() -> list[dict[str, Any]]:
        with pool.connection() as conn:
            cursor = conn.cursor()
            try:
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)
                if cursor.description is None:
                    return []
                columns = [column[0] for column in cursor.description]
                rows = cursor.fetchall()
                return [{col_name: row[i] for i, col_name in enumerate(columns)} for row in rows]
            finally:
                cursor.close()

    async def _execute_with_retry() -> list[dict[str, Any]]:
        return await asyncio.to_thread(_execute)

    return cast(
        list[dict[str, Any]],
        await run_with_retry(
            _execute_with_retry,
            max_retries=3,
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

    Uses the DB pool (SuperDB) for application CRUD operations.

    Args:
        settings: Application settings
        sql: SQL query string (use ? placeholders for parameters)
        params: Optional tuple of parameters for parameterized queries

    Returns:
        Dictionary with success status and affected row count
    """
    pool = ConnectionPool.get_db_pool(settings)

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
                logger.error("Database insert/update error: %s", e)
                conn.rollback()
                if is_transient_pyodbc_error(e):
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
