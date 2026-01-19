"""
SQL validation rules and security checks for Azure SQL Database.
"""

import re

# =============================================================================
# Blocked Keywords (Security - DDL/DML Operations)
# =============================================================================

BLOCKED_KEYWORDS: frozenset[str] = frozenset(
    {
        # DDL
        "CREATE",
        "ALTER",
        "DROP",
        "TRUNCATE",
        "RENAME",
        # DML (write) - INSERT is allowed per prompt, only UPDATE/DELETE blocked
        "UPDATE",
        "DELETE",
        "MERGE",
        "UPSERT",
        # DCL
        "GRANT",
        "REVOKE",
        "DENY",
        # Transaction
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT",
        # Administrative
        "EXEC",
        "EXECUTE",
        "BACKUP",
        "RESTORE",
        "SHUTDOWN",
        "KILL",
        "RECONFIGURE",
        # Injection patterns
        "xp_cmdshell",
        "sp_executesql",
        "OPENROWSET",
        "OPENDATASOURCE",
        "OPENQUERY",
    }
)

BLOCKED_PATTERNS: frozenset[str] = frozenset(
    {
        "--",
        "/*",
        "*/",
        ";--",
        "xp_",
        "sp_",
    }
)

BLOCKED_SCHEMAS: frozenset[str] = frozenset(
    {
        "sys",
        "information_schema",
        "master",
        "tempdb",
        "msdb",
    }
)

ALLOWED_STATEMENT_PREFIXES: frozenset[str] = frozenset(
    {
        "SELECT",
        "WITH",
        "INSERT",
    }
)

ALLOWED_STATEMENT_NAMES = ", ".join(sorted(ALLOWED_STATEMENT_PREFIXES))  # For error messages

REQUIRED_TABLE_PREFIX: str = "dbo."

# =============================================================================
# Security Validation (Primary - Always Run)
# =============================================================================


def is_sql_safe(sql: str) -> tuple[bool, str | None]:
    """
    Security validation for SQL query.

    This is the PRIMARY validation that MUST pass before execution.

    Returns:
        Tuple of (is_safe, error_message)
    """
    if not sql or not sql.strip():
        return False, "SQL query is empty"

    sql_upper = sql.upper().strip()

    # 1. Check blocked patterns (exact match)
    for pattern in BLOCKED_PATTERNS:
        if pattern in sql:
            return False, f"Blocked pattern: {pattern}"

    # 2. Check blocked keywords (word boundary)
    for keyword in BLOCKED_KEYWORDS:
        regex = rf"\b{re.escape(keyword)}\b"
        if re.search(regex, sql_upper):
            return False, f"Blocked keyword: {keyword}"

    # 3. Check system schemas
    sql_lower = sql.lower()
    for schema in BLOCKED_SCHEMAS:
        regex = rf"\b{schema}\.\w+"
        if re.search(regex, sql_lower):
            return False, f"System schema not allowed: {schema}"

    # 4. Check starts with allowed statement
    if not any(sql_upper.startswith(prefix) for prefix in ALLOWED_STATEMENT_PREFIXES):
        return False, f"Query must start with one of: {ALLOWED_STATEMENT_NAMES}"

    """
    # 5. Check dbo. prefix (if FROM exists)
    if "FROM" in sql_upper and REQUIRED_TABLE_PREFIX.upper() not in sql_upper:
        return False, f"Tables must use '{REQUIRED_TABLE_PREFIX}' prefix"
    """
    return True, None


# =============================================================================
# Schema Validation (Optional - For Pre-flight Checks)
# =============================================================================


def validate_table_references(sql: str, valid_tables: set[str]) -> list[str]:
    """
    Validate table references against a set of valid tables.

    Note: This is OPTIONAL. The database will also validate this.
    Use for early feedback before hitting the DB.

    Args:
        sql: SQL query string
        valid_tables: Set of valid table names (e.g., {"dbo.People", "dbo.Accounts"})

    Returns:
        List of error messages (empty if valid)
    """
    errors = []
    table_names = extract_table_names(sql)

    for table_name in table_names:
        # Normalize to match format in valid_tables
        normalized = table_name.lower()
        if not any(normalized == t.lower() for t in valid_tables):
            errors.append(f"Unknown table: {table_name}")

    return errors


def extract_table_names(sql: str) -> list[str]:
    """Extract table names from SQL query."""
    table_names = []

    patterns = [
        r"\bFROM\s+\[?(\w+\.\w+)\]?",
        r"\bJOIN\s+\[?(\w+\.\w+)\]?",
    ]

    for pattern in patterns:
        matches = re.finditer(pattern, sql, re.IGNORECASE)
        for match in matches:
            table = match.group(1).lower()
            if table not in table_names:
                table_names.append(table)

    return table_names


# =============================================================================
# Main Validation Function
# =============================================================================


def validate_sql_query(sql: str, valid_tables: set[str] | None = None) -> tuple[bool, list[str]]:
    """
    Validate SQL query for security and optionally schema.

    Args:
        sql: SQL query string
        valid_tables: Optional set of valid table names for schema validation

    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []

    # 1. Security validation (REQUIRED)
    is_safe, error = is_sql_safe(sql)
    if not is_safe:
        return False, [error] if error else ["Security validation failed"]

    # 2. Schema validation (OPTIONAL)
    if valid_tables:
        table_errors = validate_table_references(sql, valid_tables)
        errors.extend(table_errors)

    return len(errors) == 0, errors
