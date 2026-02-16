"""Tests for CodeFormatter (code-based response formatter)."""

import pytest

from src.orchestrator.state import PipelineState
from src.services.formatting.code_formatter import CodeFormatter


def _make_state(**overrides) -> PipelineState:
    """Create a PipelineState with sensible defaults; override any field."""
    defaults = dict(
        user_message="total ventas 2024",
        user_id="test_user",
        pattern_type="COMPARACION",
        arquetipo="A",
        sql_query="SELECT 1",
        sql_results=[{"col": "val"}],
        sql_insights="Insight text",
        titulo_grafica="Ventas por mes",
        viz_required=True,
        tipo_grafico="bar",
        powerbi_url="https://app.powerbi.com/view?r=abc",
        data_points=[{"x_value": "2024", "y_value": 100, "category": "ventas"}],
        metric_name="total_ventas",
    )
    defaults.update(overrides)
    return PipelineState(**defaults)


# ==========================================
#  Happy path
# ==========================================


def test_format_happy_path():
    state = _make_state()
    result = CodeFormatter.format(state)

    assert result["patron"] == "COMPARACION"
    assert result["datos"] == [{"col": "val"}]
    assert result["arquetipo"] == "A"
    assert result["visualizacion"] == "YES"
    assert result["tipo_grafica"] == "bar"
    assert result["titulo_grafica"] == "Ventas por mes"
    assert result["data_points"] == [{"x_value": "2024", "y_value": 100, "category": "ventas"}]
    assert result["metric_name"] == "total_ventas"
    assert result["link_power_bi"] == "https://app.powerbi.com/view?r=abc"
    assert result["insight"] == "Insight text"
    assert result["sql_query"] == "SELECT 1"
    assert result["error"] == ""


# ==========================================
#  Visualización logic  (viz_required AND powerbi_url)
# ==========================================


def test_visualizacion_no_when_viz_required_false():
    state = _make_state(viz_required=False, powerbi_url="https://powerbi")
    assert CodeFormatter.format(state)["visualizacion"] == "NO"


def test_visualizacion_no_when_powerbi_url_none():
    state = _make_state(viz_required=True, powerbi_url=None)
    assert CodeFormatter.format(state)["visualizacion"] == "NO"


def test_visualizacion_no_when_both_missing():
    state = _make_state(viz_required=False, powerbi_url=None)
    assert CodeFormatter.format(state)["visualizacion"] == "NO"


# ==========================================
#  Fallback defaults (None / empty fields)
# ==========================================


def test_pattern_type_defaults_to_general():
    state = _make_state(pattern_type=None)
    assert CodeFormatter.format(state)["patron"] == "general"


def test_arquetipo_defaults_to_a():
    state = _make_state(arquetipo=None)
    assert CodeFormatter.format(state)["arquetipo"] == "A"


def test_sql_results_defaults_to_empty_list():
    state = _make_state(sql_results=None)
    assert CodeFormatter.format(state)["datos"] == []


def test_insight_falls_back_to_resumen():
    state = _make_state(sql_insights=None, sql_resumen="Resumen text")
    assert CodeFormatter.format(state)["insight"] == "Resumen text"


def test_insight_falls_back_to_default_message():
    state = _make_state(sql_insights=None, sql_resumen=None)
    assert CodeFormatter.format(state)["insight"] == "No insight available"


# ==========================================
#  Minimal state (only required fields)
# ==========================================


def test_format_minimal_state():
    state = PipelineState(user_message="hola", user_id="u1")
    result = CodeFormatter.format(state)

    assert result["patron"] == "general"
    assert result["datos"] == []
    assert result["arquetipo"] == "A"
    assert result["visualizacion"] == "NO"
    assert result["tipo_grafica"] is None
    assert result["data_points"] is None
    assert result["metric_name"] is None
    assert result["link_power_bi"] is None
    assert result["insight"] == "No insight available"
    assert result["sql_query"] is None
    assert result["error"] == ""
