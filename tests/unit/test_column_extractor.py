"""Tests for ColumnExtractor — extracts column names/aliases from SQL SELECT."""

import pytest

from src.services.sql.executor import ColumnExtractor


class TestExtract:
    """ColumnExtractor.extract() tests."""

    def test_simple_select(self):
        sql = "SELECT name, age FROM users"
        assert ColumnExtractor.extract(sql) == ["name", "age"]

    def test_aliases(self):
        sql = "SELECT u.name AS label, AVG(u.rate) AS x_value FROM gold.users u"
        assert ColumnExtractor.extract(sql) == ["label", "x_value"]

    def test_with_cte_and_from(self):
        sql = (
            "WITH cte AS (SELECT id FROM t) "
            "SELECT c.id AS entity_id, c.name AS entity_name FROM cte c"
        )
        assert ColumnExtractor.extract(sql) == ["entity_id", "entity_name"]

    def test_select_without_from_scalar_subqueries(self):
        """SELECT with scalar subqueries and no outer FROM — the key regression."""
        sql = """
WITH saldo_inicial AS (SELECT SUM(x) AS saldo FROM t),
saldo_final AS (SELECT SUM(x) AS saldo FROM t)
SELECT
    'Grupo Aval' AS entidad,
    (SELECT year FROM periodo_inicial) AS year_inicial,
    (SELECT month FROM periodo_inicial) AS month_inicial,
    (SELECT saldo FROM saldo_inicial) AS saldo_inicial,
    (SELECT saldo FROM saldo_final) AS saldo_final,
    (SELECT saldo FROM saldo_final) - (SELECT saldo FROM saldo_inicial) AS crecimiento_absoluto,
    ROUND(((SELECT saldo FROM saldo_final) - (SELECT saldo FROM saldo_inicial)) * 100.0 / (SELECT saldo FROM saldo_inicial), 2) AS crecimiento_porcentual
"""
        cols = ColumnExtractor.extract(sql)
        assert cols == [
            "entidad",
            "year_inicial",
            "month_inicial",
            "saldo_inicial",
            "saldo_final",
            "crecimiento_absoluto",
            "crecimiento_porcentual",
        ]

    def test_select_without_from_order_by(self):
        """SELECT without FROM followed by ORDER BY."""
        sql = """
WITH cte AS (SELECT 1 AS x FROM t)
SELECT (SELECT x FROM cte) AS val1, 42 AS val2
ORDER BY val1
"""
        assert ColumnExtractor.extract(sql) == ["val1", "val2"]

    def test_distinct(self):
        sql = "SELECT DISTINCT name, country FROM gold.banco"
        assert ColumnExtractor.extract(sql) == ["name", "country"]

    def test_table_dot_column(self):
        sql = "SELECT gold.banco.NOMBRE_ENTIDAD FROM gold.banco"
        assert ColumnExtractor.extract(sql) == ["NOMBRE_ENTIDAD"]

    def test_empty_sql(self):
        assert ColumnExtractor.extract("") == []

    def test_no_select(self):
        assert ColumnExtractor.extract("INSERT INTO t VALUES (1)") == []
