"""Tests for src.services.chat_v2.indicators."""

import pytest

from src.services.chat_v2.indicators import (
    _fmt_co,
    _format_value,
    _color,
    _ordinal_es,
    compute_series_stats,
    compute_full_series_stats,
    resolve_indicators,
)
from src.services.chat_v2.models import IndicatorSpec


# -- Helpers ------------------------------------------------------------------


def _line_points(values: list[float], start_month: int = 1, series: str = "Grupo Aval") -> list[dict]:
    return [
        {
            "x_value": f"2025-{start_month + i:02d}",
            "y_value": v,
            "series": series,
            "category": series,
        }
        for i, v in enumerate(values)
    ]


def _multi_series() -> list[dict]:
    pts = []
    for m, bb, bo in [(1, 12.34, 6.81), (2, 12.46, 6.91), (3, 12.29, 6.85)]:
        pts.append({"x_value": f"2025-{m:02d}", "y_value": bb, "series": "Banco Bogota", "category": "Banco Bogota"})
        pts.append({"x_value": f"2025-{m:02d}", "y_value": bo, "series": "Banco de Occidente", "category": "Banco de Occidente"})
    return pts


def _spec(label: str, calc: str, unit: str, series: str | None = None) -> IndicatorSpec:
    return IndicatorSpec(label=label, calc=calc, unit=unit, series=series)


# -- Colombian formatting -----------------------------------------------------


class TestFmtColombian:
    def test_basic(self):
        assert _fmt_co(1234.56) == "1.234,56"

    def test_zero(self):
        assert _fmt_co(0.0) == "0,00"

    def test_negative(self):
        assert _fmt_co(-0.28) == "-0,28"

    def test_large(self):
        assert _fmt_co(86186000.0, 0) == "86.186.000"

    def test_small_decimals(self):
        assert _fmt_co(0.0041, 4) == "0,0041"


class TestFormatValue:
    def test_pp_negative(self):
        assert _format_value(-0.28, "pp") == "-0,28 pp"

    def test_pp_positive(self):
        assert _format_value(0.01, "pp") == "+0,01 pp"

    def test_bps(self):
        result = _format_value(-0.89, "bps")
        assert "bps" in result

    def test_pct(self):
        assert _format_value(3.12, "%") == "+3,12 %"

    def test_abs(self):
        assert _format_value(24.49, "abs") == "24,49"


class TestColor:
    def test_positive(self):
        assert _color(0.5) == "positive"

    def test_negative(self):
        assert _color(-0.28) == "negative"

    def test_zero(self):
        assert _color(0.0) == "neutral"

    def test_near_zero(self):
        assert _color(1e-12) == "neutral"


# -- compute_series_stats ------------------------------------------------------


class TestComputeSeriesStats:
    def test_single_series(self):
        pts = _line_points([24.77, 24.49, 24.80, 24.48])
        stats = compute_series_stats(pts)
        assert "Grupo Aval" in stats
        s = stats["Grupo Aval"]
        assert s["first_y"] == 24.77
        assert s["last_y"] == 24.48
        assert s["prev_y"] == 24.80
        assert s["count"] == 4
        assert s["first_x"] == "2025-01"
        assert s["last_x"] == "2025-04"

    def test_multi_series(self):
        pts = _multi_series()
        stats = compute_series_stats(pts)
        assert len(stats) == 2
        assert "Banco Bogota" in stats
        assert "Banco de Occidente" in stats
        assert stats["Banco Bogota"]["first_y"] == 12.34
        assert stats["Banco Bogota"]["last_y"] == 12.29

    def test_single_point_excluded(self):
        pts = _line_points([24.0])
        stats = compute_series_stats(pts)
        assert stats == {}

    def test_empty_input(self):
        assert compute_series_stats([]) == {}

    def test_sorts_by_x_value(self):
        # Points out of order
        pts = [
            {"x_value": "2025-03", "y_value": 30.0, "series": "A", "category": "A"},
            {"x_value": "2025-01", "y_value": 10.0, "series": "A", "category": "A"},
            {"x_value": "2025-02", "y_value": 20.0, "series": "A", "category": "A"},
        ]
        stats = compute_series_stats(pts)
        assert stats["A"]["first_y"] == 10.0
        assert stats["A"]["last_y"] == 30.0
        assert stats["A"]["prev_y"] == 20.0


# -- resolve_indicators --------------------------------------------------------


class TestResolveIndicators:
    def test_period_delta_pp(self):
        pts = _line_points([24.77, 24.49])
        stats = compute_series_stats(pts)
        specs = [_spec("Cambio en el periodo", "period_delta", "pp")]
        result = resolve_indicators(stats, specs)
        assert len(result) == 1
        ind = result[0]
        assert ind["value"] == pytest.approx(24.49 - 24.77, abs=0.001)
        assert "pp" in ind["formatted"]
        assert ind["color"] == "negative"
        assert "->" in ind["detail"]

    def test_pct_change(self):
        pts = _line_points([86186.0, 88798.0])
        stats = compute_series_stats(pts)
        specs = [_spec("Crecimiento", "pct_change", "%")]
        result = resolve_indicators(stats, specs)
        ind = result[0]
        expected = ((88798.0 / 86186.0) - 1) * 100
        assert ind["value"] == pytest.approx(expected, abs=0.01)
        assert ind["unit"] == "%"
        assert ind["color"] == "positive"

    def test_prev_delta_pp(self):
        pts = _line_points([24.77, 24.48, 24.49])
        stats = compute_series_stats(pts)
        specs = [_spec("vs. mes anterior", "prev_delta", "pp")]
        result = resolve_indicators(stats, specs)
        ind = result[0]
        assert ind["value"] == pytest.approx(24.49 - 24.48, abs=0.001)
        assert ind["color"] == "positive"

    def test_empty_specs(self):
        pts = _line_points([24.77, 24.49])
        stats = compute_series_stats(pts)
        assert resolve_indicators(stats, []) == []

    def test_empty_stats(self):
        specs = [_spec("Test", "period_delta", "pp")]
        assert resolve_indicators({}, specs) == []

    def test_unknown_calc_ignored(self):
        pts = _line_points([24.77, 24.49])
        stats = compute_series_stats(pts)
        specs = [_spec("Unknown", "nonexistent_calc", "pp")]
        result = resolve_indicators(stats, specs)
        assert result == []

    def test_multiple_indicators(self):
        pts = _line_points([24.77, 24.48, 24.49])
        stats = compute_series_stats(pts)
        specs = [
            _spec("Cambio en el periodo", "period_delta", "pp"),
            _spec("vs. mes anterior", "prev_delta", "pp"),
        ]
        result = resolve_indicators(stats, specs)
        assert len(result) == 2
        assert result[0]["label"] == "Cambio en el periodo"
        assert result[1]["label"] == "vs. mes anterior"

    def test_multi_series_specific(self):
        pts = _multi_series()
        stats = compute_series_stats(pts)
        specs = [_spec("Cambio Bogota", "period_delta", "pp", series="Banco Bogota")]
        result = resolve_indicators(stats, specs)
        assert len(result) == 1
        assert result[0]["value"] == pytest.approx(12.29 - 12.34, abs=0.01)
        assert result[0]["series"] == "Banco Bogota"

    def test_multi_series_null_uses_first(self):
        pts = _multi_series()
        stats = compute_series_stats(pts)
        specs = [_spec("Cambio general", "period_delta", "pp")]
        result = resolve_indicators(stats, specs)
        assert len(result) == 1
        # Should use first series in the dict

    def test_zero_first_value_pct_change(self):
        pts = _line_points([0.0, 100.0])
        stats = compute_series_stats(pts)
        specs = [_spec("Crecimiento", "pct_change", "%")]
        result = resolve_indicators(stats, specs)
        assert result[0]["value"] == 0.0  # Safe division

    def test_period_delta_pct_mode(self):
        """period_delta with unit '%' computes pct growth, not absolute diff."""
        pts = _line_points([100.0, 150.0])
        stats = compute_series_stats(pts)
        specs = [_spec("Crecimiento", "period_delta", "%")]
        result = resolve_indicators(stats, specs)
        assert result[0]["value"] == pytest.approx(50.0, abs=0.01)

    def test_bps_format(self):
        pts = _line_points([8.95, 8.06])
        stats = compute_series_stats(pts)
        specs = [_spec("Cambio tasa", "period_delta", "bps")]
        result = resolve_indicators(stats, specs)
        ind = result[0]
        assert ind["unit"] == "bps"
        assert "bps" in ind["formatted"]


# -- compute_full_series_stats ------------------------------------------------


class TestComputeFullSeriesStats:
    def test_includes_all_y(self):
        pts = _line_points([10.0, 20.0, 30.0, 25.0])
        stats = compute_full_series_stats(pts)
        s = stats["Grupo Aval"]
        assert s["all_y"] == [10.0, 20.0, 30.0, 25.0]
        assert s["all_x"] == ["2025-01", "2025-02", "2025-03", "2025-04"]

    def test_std_dev_computed(self):
        pts = _line_points([10.0, 20.0, 30.0, 25.0])
        stats = compute_full_series_stats(pts)
        s = stats["Grupo Aval"]
        assert s["std_dev"] > 0
        assert s["mean_y"] == pytest.approx(21.25, abs=0.01)

    def test_backward_compatible(self):
        pts = _line_points([24.77, 24.49, 24.80, 24.48])
        stats = compute_full_series_stats(pts)
        s = stats["Grupo Aval"]
        assert s["first_y"] == 24.77
        assert s["last_y"] == 24.48
        assert s["prev_y"] == 24.80
        assert s["count"] == 4

    def test_single_point_excluded(self):
        pts = _line_points([24.0])
        assert compute_full_series_stats(pts) == {}

    def test_two_points(self):
        pts = _line_points([10.0, 20.0])
        stats = compute_full_series_stats(pts)
        s = stats["Grupo Aval"]
        assert s["std_dev"] > 0
        assert len(s["all_y"]) == 2


# -- momentum ------------------------------------------------------------------


class TestMomentum:
    def test_accelerating(self):
        # First half [10, 12]: slope=2. Second half [12, 20]: slope=8. Change=+6
        pts = _line_points([10.0, 12.0, 12.0, 20.0])
        stats = compute_full_series_stats(pts)
        specs = [_spec("Pendiente", "momentum", "pp")]
        result = resolve_indicators(stats, specs)
        assert len(result) == 1
        assert result[0]["value"] > 0
        assert result[0]["color"] == "positive"
        assert "Aceleracion" in result[0]["detail"]

    def test_decelerating(self):
        # First half [10, 20]: slope=10. Second half [20, 22]: slope=2. Change=-8
        pts = _line_points([10.0, 20.0, 20.0, 22.0])
        stats = compute_full_series_stats(pts)
        specs = [_spec("Pendiente", "momentum", "pp")]
        result = resolve_indicators(stats, specs)
        assert result[0]["value"] < 0
        assert result[0]["color"] == "negative"

    def test_requires_4_points(self):
        pts = _line_points([10.0, 20.0, 30.0])
        stats = compute_full_series_stats(pts)
        specs = [_spec("Pendiente", "momentum", "pp")]
        result = resolve_indicators(stats, specs)
        assert result == []

    def test_pct_unit_uses_percentage(self):
        # [100, 200, 200, 400]
        # mid=2. First half: (200/100 - 1)*100 / 1 = 100% per period
        # Second half: (400/200 - 1)*100 / 1 = 100% per period
        # slope_change = 0 (constant growth rate)
        pts = _line_points([100.0, 200.0, 200.0, 400.0])
        stats = compute_full_series_stats(pts)
        specs = [_spec("Aceleracion", "momentum", "%")]
        result = resolve_indicators(stats, specs)
        assert result[0]["value"] == pytest.approx(0.0, abs=0.01)

    def test_pct_unit_accelerating(self):
        # [100, 101, 101, 200]
        # mid=2. First half: (101/100 - 1)*100 / 1 = 1% per period
        # Second half: (200/101 - 1)*100 / 1 = ~98% per period
        # slope_change = ~97 (accelerating)
        pts = _line_points([100.0, 101.0, 101.0, 200.0])
        stats = compute_full_series_stats(pts)
        specs = [_spec("Aceleracion", "momentum", "%")]
        result = resolve_indicators(stats, specs)
        assert result[0]["value"] > 90  # big acceleration
        assert result[0]["color"] == "positive"


# -- Helpers for multi-series cross-series tests --------------------------------


def _three_series_data() -> list[dict]:
    """3 series, 4 months each. SerieA grows most, SerieB shrinks."""
    pts = []
    for m, a, b, c in [
        (1, 10.0, 20.0, 15.0),
        (2, 12.0, 19.0, 16.0),
        (3, 15.0, 18.0, 14.0),
        (4, 20.0, 17.0, 15.5),
    ]:
        pts.append({"x_value": f"2025-{m:02d}", "y_value": a, "series": "SerieA", "category": "SerieA"})
        pts.append({"x_value": f"2025-{m:02d}", "y_value": b, "series": "SerieB", "category": "SerieB"})
        pts.append({"x_value": f"2025-{m:02d}", "y_value": c, "series": "SerieC", "category": "SerieC"})
    return pts


# -- max_change ----------------------------------------------------------------


class TestMaxChange:
    def test_finds_largest(self):
        stats = compute_full_series_stats(_three_series_data())
        specs = [_spec("Mayor cambio", "max_change", "pp")]
        result = resolve_indicators(stats, specs)
        assert len(result) == 1
        # SerieA: 20-10=+10, SerieB: 17-20=-3, SerieC: 15.5-15=+0.5
        assert result[0]["series"] == "SerieA"
        assert result[0]["value"] == pytest.approx(10.0, abs=0.01)
        assert "mayor aumento" in result[0]["detail"]

    def test_negative_change(self):
        # SerieA flat, SerieB drops a lot
        pts = []
        for m, a, b in [(1, 10.0, 30.0), (2, 10.0, 10.0)]:
            pts.append({"x_value": f"2025-{m:02d}", "y_value": a, "series": "A", "category": "A"})
            pts.append({"x_value": f"2025-{m:02d}", "y_value": b, "series": "B", "category": "B"})
        stats = compute_full_series_stats(pts)
        specs = [_spec("Mayor cambio", "max_change", "pp")]
        result = resolve_indicators(stats, specs)
        assert result[0]["series"] == "B"
        assert result[0]["value"] < 0
        assert "mayor caida" in result[0]["detail"]

    def test_single_series_returns_empty(self):
        pts = _line_points([10.0, 20.0])
        stats = compute_full_series_stats(pts)
        specs = [_spec("Mayor cambio", "max_change", "pp")]
        result = resolve_indicators(stats, specs)
        assert result == []


# -- rank_change ---------------------------------------------------------------


class TestRankChange:
    def test_detects_movement(self):
        # SerieA: 5->25 (starts 3rd, ends 1st), SerieB: 20->8 (starts 1st, ends 3rd)
        pts = []
        for m, a, b, c in [(1, 5.0, 20.0, 15.0), (2, 25.0, 8.0, 12.0)]:
            pts.append({"x_value": f"2025-{m:02d}", "y_value": a, "series": "A", "category": "A"})
            pts.append({"x_value": f"2025-{m:02d}", "y_value": b, "series": "B", "category": "B"})
            pts.append({"x_value": f"2025-{m:02d}", "y_value": c, "series": "C", "category": "C"})
        stats = compute_full_series_stats(pts)
        specs = [_spec("Cambio ranking", "rank_change", "pp")]
        result = resolve_indicators(stats, specs)
        assert len(result) == 1
        # Either A (moved up 2) or B (moved down 2) — both have abs(2)
        assert abs(result[0]["value"]) == 2
        assert "->" in result[0]["detail"]

    def test_no_movement_returns_empty(self):
        # Same ranking start and end
        pts = []
        for m, a, b in [(1, 20.0, 10.0), (2, 21.0, 11.0)]:
            pts.append({"x_value": f"2025-{m:02d}", "y_value": a, "series": "A", "category": "A"})
            pts.append({"x_value": f"2025-{m:02d}", "y_value": b, "series": "B", "category": "B"})
        stats = compute_full_series_stats(pts)
        specs = [_spec("Cambio ranking", "rank_change", "pp")]
        result = resolve_indicators(stats, specs)
        assert result == []

    def test_ordinal_format(self):
        assert _ordinal_es(1) == "1ro"
        assert _ordinal_es(2) == "2do"
        assert _ordinal_es(3) == "3ro"
        assert _ordinal_es(4) == "4to"
        assert _ordinal_es(7) == "7to"


# -- share_of_growth -----------------------------------------------------------


class TestShareOfGrowth:
    def test_finds_largest_contributor(self):
        # SerieA: 10->20 (delta=+10), SerieB: 20->17 (delta=-3), SerieC: 15->15.5 (delta=+0.5)
        # Total delta = 10 - 3 + 0.5 = 7.5
        # SoG_A = 10/7.5 * 100 = 133.33%
        stats = compute_full_series_stats(_three_series_data())
        specs = [_spec("Share of Growth", "share_of_growth", "%")]
        result = resolve_indicators(stats, specs)
        assert len(result) == 1
        assert result[0]["series"] == "SerieA"
        assert result[0]["value"] > 100  # captured more than total (others declined)
        assert "capturo" in result[0]["detail"]

    def test_zero_total_growth_returns_empty(self):
        # Two series that cancel out: A: 10->20 (+10), B: 20->10 (-10)
        pts = []
        for m, a, b in [(1, 10.0, 20.0), (2, 20.0, 10.0)]:
            pts.append({"x_value": f"2025-{m:02d}", "y_value": a, "series": "A", "category": "A"})
            pts.append({"x_value": f"2025-{m:02d}", "y_value": b, "series": "B", "category": "B"})
        stats = compute_full_series_stats(pts)
        specs = [_spec("SoG", "share_of_growth", "%")]
        result = resolve_indicators(stats, specs)
        assert result == []  # total delta = 0

    def test_single_series_returns_empty(self):
        pts = _line_points([10.0, 20.0])
        stats = compute_full_series_stats(pts)
        specs = [_spec("SoG", "share_of_growth", "%")]
        result = resolve_indicators(stats, specs)
        assert result == []  # needs 2+ series

    def test_all_declining(self):
        # A: 20->15 (-5), B: 30->25 (-5). Total=-10. SoG_A=50%, SoG_B=50%
        pts = []
        for m, a, b in [(1, 20.0, 30.0), (2, 15.0, 25.0)]:
            pts.append({"x_value": f"2025-{m:02d}", "y_value": a, "series": "A", "category": "A"})
            pts.append({"x_value": f"2025-{m:02d}", "y_value": b, "series": "B", "category": "B"})
        stats = compute_full_series_stats(pts)
        specs = [_spec("SoG", "share_of_growth", "%")]
        result = resolve_indicators(stats, specs)
        assert len(result) == 1
        assert result[0]["value"] == pytest.approx(50.0, abs=0.01)


# -- growth_vs_market ----------------------------------------------------------


class TestGrowthVsMarket:
    def test_finds_outperformer(self):
        # SerieA: 10->20 (+100%), SerieB: 20->17 (-15%), SerieC: 15->15.5 (+3.33%)
        # Market: 45->52.5. Market growth = (52.5/45-1)*100 = 16.67%
        # Spread_A = 100 - 16.67 = +83.33
        stats = compute_full_series_stats(_three_series_data())
        specs = [_spec("vs Mercado", "growth_vs_market", "pp")]
        result = resolve_indicators(stats, specs)
        assert len(result) == 1
        assert result[0]["series"] == "SerieA"
        assert result[0]["value"] > 80
        assert "vs mercado" in result[0]["detail"]
        assert result[0]["unit"] == "pp"

    def test_equal_growth_zero_spread(self):
        # Both grow 10%: A: 100->110, B: 200->220. Market: 300->330 (10%)
        pts = []
        for m, a, b in [(1, 100.0, 200.0), (2, 110.0, 220.0)]:
            pts.append({"x_value": f"2025-{m:02d}", "y_value": a, "series": "A", "category": "A"})
            pts.append({"x_value": f"2025-{m:02d}", "y_value": b, "series": "B", "category": "B"})
        stats = compute_full_series_stats(pts)
        specs = [_spec("vs Mercado", "growth_vs_market", "pp")]
        result = resolve_indicators(stats, specs)
        assert len(result) == 1
        assert abs(result[0]["value"]) < 0.01  # both at market rate

    def test_zero_first_value_skipped(self):
        # A starts at 0 (can't compute growth), B is fine
        pts = []
        for m, a, b in [(1, 0.0, 20.0), (2, 10.0, 30.0)]:
            pts.append({"x_value": f"2025-{m:02d}", "y_value": a, "series": "A", "category": "A"})
            pts.append({"x_value": f"2025-{m:02d}", "y_value": b, "series": "B", "category": "B"})
        stats = compute_full_series_stats(pts)
        specs = [_spec("vs Mercado", "growth_vs_market", "pp")]
        result = resolve_indicators(stats, specs)
        assert len(result) == 1
        assert result[0]["series"] == "B"  # A skipped due to first_y=0

    def test_single_series_returns_empty(self):
        pts = _line_points([10.0, 20.0])
        stats = compute_full_series_stats(pts)
        specs = [_spec("vs Mercado", "growth_vs_market", "pp")]
        result = resolve_indicators(stats, specs)
        assert result == []  # needs 2+ series
