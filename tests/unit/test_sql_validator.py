"""Tests for SQL validation service."""

from src.services.sql.validation import SQLValidationService


def test_sql_validator_safe_query():
    """Test SQL validation service with safe query."""
    validator = SQLValidationService()
    result = validator.validate("SELECT * FROM dbo.Distribucion_Cartera")
    assert result["is_valid"]


def test_sql_validator_dangerous_query():
    """Test SQL validation service with dangerous query."""
    validator = SQLValidationService()
    result = validator.validate("DROP TABLE dbo.Customers")
    assert not result["is_valid"]
    assert len(result["errors"]) > 0


def test_sql_validator_no_select():
    """Test SQL validation service with query without SELECT."""
    validator = SQLValidationService()
    result = validator.validate("UPDATE dbo.Distribucion_Cartera SET name='test'")
    assert not result["is_valid"]
    assert "Blocked keyword" in str(result["errors"])
