"""
Retry utilities for handling rate limits and transient errors.
"""
 
import asyncio
import logging
import re
from collections.abc import Callable
from typing import Any, TypeVar
import pyodbc
logger = logging.getLogger(__name__)
 
T = TypeVar("T")
 
 
def is_pyodbc_timeout_error(exception: Exception) -> bool:
    """
    Check if an exception is a pyodbc timeout error (HYT00).
 
    Args:
        exception: The exception to check
 
    Returns:
        True if the exception is a pyodbc HYT00 timeout error
    """
    try:
        if isinstance(exception, pyodbc.Error):
            error_str = str(exception)
            # Check for HYT00 error code (timeout expired)
            if "HYT00" in error_str or "HYT01" in error_str:
                return True
            # Also check error args which may contain the SQL state
            if (
                hasattr(exception, "args")
                and len(exception.args) > 0
                and isinstance(exception.args[0], str)
                and ("HYT00" in exception.args[0] or "HYT01" in exception.args[0])
            ):
                return True
    except ImportError:
        # pyodbc not available, can't be a pyodbc error
        pass
    return False
 
 
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
            error_str = str(e).lower()
 
            # Check if it's a rate limit error
            is_rate_limit = (
                "rate limit" in error_str
                or "rate_limit" in error_str
                or "rate limit is exceeded" in error_str
            )
 
            # Check for connection or timeout errors
            is_connection_error = (
                "login timeout" in error_str
                or "connection timeout" in error_str
                or "timeout expired" in error_str
                or "communication link failure" in error_str
                or is_pyodbc_timeout_error(e)
            )
 
            should_retry = (is_rate_limit or is_connection_error) and retry_on_rate_limit
 
            if should_retry and attempt < max_retries - 1:
                # Try to extract wait time from error message
                wait_time_match = re.search(r"(\d+)\s*seconds?", str(e), re.IGNORECASE)
                if wait_time_match:
                    wait_time = float(wait_time_match.group(1))
                else:
                    # Use exponential backoff
                    wait_time = initial_delay * (backoff_factor**attempt)
 
                error_type = "HYT00 timeout" if is_pyodbc_timeout_error(e) else "connection/timeout"
                logger.warning(
                    f"Transient {error_type} error detected ({str(e)}). Attempt {attempt + 1}/{max_retries}. "
                    f"Waiting {wait_time:.1f} seconds before retry..."
                )
                await asyncio.sleep(wait_time)
                continue
 
            # If not retryable or max retries reached, raise the exception
            raise
 
    # If we exhausted retries, raise the last exception
    if last_exception:
        raise last_exception
    raise Exception("Max retries exceeded")
 
 