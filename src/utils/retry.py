"""
Retry utilities for handling rate limits and transient errors.
"""

import asyncio
import logging
import re
from collections.abc import Callable
from typing import Any

import pyodbc

logger = logging.getLogger(__name__)


# SQLSTATE codes that represent transient errors worth retrying.
# All other pyodbc errors (syntax, missing table, permission, etc.) are permanent.
_TRANSIENT_SQLSTATES: frozenset[str] = frozenset({
    "HYT00",  # Timeout expired
    "HYT01",  # Connection timeout expired
    "08S01",  # Communication link failure
    "08001",  # Unable to connect to data source
    "08007",  # Connection failure during transaction
    "40001",  # Deadlock victim
})


def is_transient_pyodbc_error(exception: Exception) -> bool:
    """Check if a pyodbc.Error is transient and worth retrying.

    Extracts the SQLSTATE from exception.args[0] and checks it against
    the known set of transient error codes. Returns False for permanent
    errors like 42S02 (table not found), 42000 (syntax error), etc.
    """
    if not isinstance(exception, pyodbc.Error):
        return False
    if exception.args and isinstance(exception.args[0], str):
        sqlstate = exception.args[0].strip()
        if sqlstate in _TRANSIENT_SQLSTATES:
            return True
    # Fallback: check string representation for known transient patterns
    error_str = str(exception)
    return any(code in error_str for code in _TRANSIENT_SQLSTATES)


async def run_with_retry(
    func: Callable[[], Any],
    max_retries: int = 3,
    initial_delay: float = 5.0,
    backoff_factor: float = 2.0,
    retry_on_rate_limit: bool = True,
) -> Any:
    """
    Execute an async function with retry logic for rate limit and transient errors.

    Args:
        func: Async function to execute (no parameters)
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay in seconds before first retry
        backoff_factor: Multiplier for delay between retries
        retry_on_rate_limit: Whether to retry on rate limit errors

    Returns:
        Result from the function

    Raises:
        Exception: If max retries exceeded or non-retryable error occurs
    """
    last_exception = None

    for attempt in range(max_retries):
        try:
            return await func()
        except Exception as e:
            last_exception = e

            is_transient_db = is_transient_pyodbc_error(e)

            error_str = str(e).lower()
            is_rate_limit = "rate limit" in error_str or "rate_limit" in error_str
            is_connection_error = (
                isinstance(e, (TimeoutError, asyncio.TimeoutError))
                or is_transient_db
                or "login timeout" in error_str
                or "connection timeout" in error_str
                or "timeout expired" in error_str
                or "communication link failure" in error_str
            )

            should_retry = (is_rate_limit or is_connection_error) and retry_on_rate_limit

            if should_retry and attempt < max_retries - 1:
                wait_time_match = re.search(r"(\d+)\s{0,10}seconds?", str(e), re.IGNORECASE)
                if wait_time_match:
                    wait_time = float(wait_time_match.group(1))
                else:
                    wait_time = initial_delay * (backoff_factor**attempt)

                error_type = "transient DB" if is_transient_db else "connection/timeout"
                logger.warning(
                    "Transient %s error detected (%s). Attempt %s/%s. Waiting %.1f seconds before retry...",
                    error_type,
                    e,
                    attempt + 1,
                    max_retries,
                    wait_time,
                )
                await asyncio.sleep(wait_time)
                continue

            # If not retryable or max retries reached, raise the exception
            raise

    # If we exhausted retries, raise the last exception
    if last_exception:
        raise last_exception
    raise Exception("Max retries exceeded")
