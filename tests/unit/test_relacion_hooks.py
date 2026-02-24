"""Tests for relacion pattern hook functions."""

from unittest.mock import patch

from src.config.constants import ChartType
from src.orchestrator.state import PipelineState
from src.patterns.relacion import (
    build_scatter_points,
    enrich_sql_prompt,
    get_chart_type,
    post_process,
)
from src.services.viz.models import VizColumnMapping


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_state(**overrides) -> PipelineState:
    defaults = dict(user_message="test", user_id="u1")
    defaults.update(overrides)
    return PipelineState(**defaults)


def _make_mapping(**overrides) -> VizColumnMapping:
    defaults = dict(
        x_column="x_value",
        y_column="y_value",
        metric_name="Test",
        x_axis_name="X",
        y_axis_name="Y",
    )
    defaults.update(overrides)
    return VizColumnMapping(**defaults)


# ---------------------------------------------------------------------------
# enrich_sql_prompt
# ---------------------------------------------------------------------------


class TestEnrichSqlPrompt:
    def test_appends_enrichment(self):
        base = "Base SQL prompt."
        result = enrich_sql_prompt(base, _make_state())
        assert result.startswith("Base SQL prompt.")
        assert len(result) > len(base)

    def test_contains_required_columns(self):
        result = enrich_sql_prompt("", _make_state())
        assert "x_value" in result
        assert "y_value" in result
        assert "label" in result

    def test_contains_time_rules(self):
        result = enrich_sql_prompt("", _make_state())
        assert "LATEST available period" in result
        assert "WHEN TO INCLUDE time columns" in result

    def test_contains_join_override(self):
        result = enrich_sql_prompt("", _make_state())
        assert "JOINs between fact tables ARE allowed" in result


# ---------------------------------------------------------------------------
# post_process
# ---------------------------------------------------------------------------


class TestPostProcess:
    def test_empty_results_returns_none(self):
        state = _make_state()
        assert post_process([], state) is None
        assert state.stats_summary is None

    def test_all_null_values_returns_none(self):
        state = _make_state()
        rows = [{"x_value": None, "y_value": None, "label": "a"}]
        assert post_process(rows, state) is None

    @patch("src.patterns.relacion.compute_relationship_stats")
    def test_valid_data_returns_stats(self, mock_stats):
        mock_stats.return_value = {"r": 0.9, "n": 10}
        state = _make_state()
        rows = [
            {"x_value": 1.0, "y_value": 2.0, "label": "a"},
            {"x_value": 3.0, "y_value": 4.0, "label": "b"},
            {"x_value": 5.0, "y_value": 6.0, "label": "c"},
        ]
        post_process(rows, state)
        assert state.stats_summary == {"r": 0.9, "n": 10}
        mock_stats.assert_called_once()

    @patch("src.patterns.relacion.compute_relationship_stats")
    def test_filters_null_rows(self, mock_stats):
        mock_stats.return_value = {"r": 0.5, "n": 2}
        state = _make_state()
        rows = [
            {"x_value": 1.0, "y_value": 2.0, "label": "a"},
            {"x_value": None, "y_value": 3.0, "label": "b"},
            {"x_value": 4.0, "y_value": None, "label": "c"},
            {"x_value": 5.0, "y_value": 6.0, "label": "d"},
        ]
        post_process(rows, state)
        call_args = mock_stats.call_args[0][0]
        assert len(call_args) == 2  # only 2 valid rows


# ---------------------------------------------------------------------------
# build_scatter_points
# ---------------------------------------------------------------------------


class TestBuildScatterPoints:
    def test_basic_formatting(self):
        rows = [
            {"x_value": 1.5, "y_value": 2.5, "label": "BankA"},
            {"x_value": 3.0, "y_value": 4.0, "label": "BankB"},
        ]
        mapping = _make_mapping()
        points = build_scatter_points(rows, mapping)
        assert len(points) == 2
        assert points[0]["x_value"] == 1.5
        assert points[0]["y_value"] == 2.5
        assert points[0]["series"] == "Test"  # metric_name default, not label
        assert points[0]["label"] == "BankA"

    def test_skips_null_values(self):
        rows = [
            {"x_value": 1.0, "y_value": None, "label": "a"},
            {"x_value": None, "y_value": 2.0, "label": "b"},
            {"x_value": 3.0, "y_value": 4.0, "label": "c"},
        ]
        points = build_scatter_points(rows, _make_mapping())
        assert len(points) == 1
        assert points[0]["label"] == "c"

    def test_skips_non_numeric(self):
        rows = [
            {"x_value": "not_a_number", "y_value": 2.0, "label": "a"},
            {"x_value": 1.0, "y_value": "text", "label": "b"},
            {"x_value": 3.0, "y_value": 4.0, "label": "c"},
        ]
        points = build_scatter_points(rows, _make_mapping())
        assert len(points) == 1

    def test_series_from_mapping(self):
        rows = [{"x_value": 1, "y_value": 2, "label": "Bank", "color_group": "Tipo1"}]
        mapping = _make_mapping(series_column="color_group")
        points = build_scatter_points(rows, mapping)
        assert points[0]["series"] == "Tipo1"

    def test_category_from_mapping(self):
        rows = [{"x_value": 1, "y_value": 2, "label": "Bank", "tipo": "Grande"}]
        mapping = _make_mapping(category_column="tipo")
        points = build_scatter_points(rows, mapping)
        assert points[0]["category"] == "Grande"

    def test_empty_results(self):
        assert build_scatter_points([], _make_mapping()) == []

    def test_string_numeric_coercion(self):
        """Values like '1.5' (string) should be coerced to float."""
        rows = [{"x_value": "1.5", "y_value": "2.5", "label": "a"}]
        points = build_scatter_points(rows, _make_mapping())
        assert len(points) == 1
        assert points[0]["x_value"] == 1.5


# ---------------------------------------------------------------------------
# get_chart_type
# ---------------------------------------------------------------------------


class TestGetChartType:
    def test_returns_scatter(self):
        assert get_chart_type("relacion") == ChartType.SCATTER

    def test_returns_scatter_for_any_input(self):
        assert get_chart_type("anything") == ChartType.SCATTER
