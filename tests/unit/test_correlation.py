"""Tests for compute_relationship_stats (correlation analysis)."""

import pytest

from src.services.analysis.correlation import compute_relationship_stats


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _points(xs: list[float], ys: list[float]) -> list[dict]:
    """Build data_points list from x and y value lists."""
    return [{"x_value": x, "y_value": y, "label": f"p{i}"} for i, (x, y) in enumerate(zip(xs, ys))]


# ---------------------------------------------------------------------------
# Edge cases: insufficient or degenerate data
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_fewer_than_3_returns_warning(self):
        result = compute_relationship_stats(_points([1, 2], [3, 4]))
        assert "warning" in result
        assert result["n"] == 2

    def test_exactly_3_returns_stats_with_small_n_warning(self):
        result = compute_relationship_stats(_points([1, 2, 3], [2, 4, 6]))
        assert "r" in result
        assert result["n"] == 3
        assert "warning" in result

    def test_zero_variance_x_returns_warning(self):
        result = compute_relationship_stats(_points([5, 5, 5, 5], [1, 2, 3, 4]))
        assert "warning" in result
        assert "varianza cero" in result["warning"]

    def test_zero_variance_y_returns_warning(self):
        result = compute_relationship_stats(_points([1, 2, 3, 4], [7, 7, 7, 7]))
        assert "warning" in result
        assert "varianza cero" in result["warning"]


# ---------------------------------------------------------------------------
# Perfect and near-perfect correlations
# ---------------------------------------------------------------------------


class TestCorrelationValues:
    def test_perfect_positive(self):
        result = compute_relationship_stats(_points([1, 2, 3, 4, 5], [10, 20, 30, 40, 50]))
        assert result["r"] == pytest.approx(1.0, abs=1e-3)
        assert result["r2"] == pytest.approx(1.0, abs=1e-3)
        assert result["direction"] == "positiva"
        assert result["strength"] == "muy fuerte"

    def test_perfect_negative(self):
        result = compute_relationship_stats(_points([1, 2, 3, 4, 5], [50, 40, 30, 20, 10]))
        assert result["r"] == pytest.approx(-1.0, abs=1e-3)
        assert result["direction"] == "negativa"
        assert result["strength"] == "muy fuerte"

    def test_no_correlation(self):
        # Alternating pattern that should have near-zero correlation
        xs = list(range(1, 101))
        ys = [(1 if i % 2 == 0 else -1) * (i % 7) for i in xs]
        result = compute_relationship_stats(_points(xs, ys))
        assert abs(result["r"]) < 0.3
        assert result["strength"] == "debil"


# ---------------------------------------------------------------------------
# Strength classification boundaries
# ---------------------------------------------------------------------------


class TestStrengthClassification:
    @pytest.mark.parametrize(
        "r_target, expected_strength",
        [
            (0.95, "muy fuerte"),
            (0.80, "fuerte"),
            (0.55, "moderada"),
            (0.20, "debil"),
        ],
    )
    def test_strength_boundary(self, r_target: float, expected_strength: str):
        """Use a crafted dataset that produces approximately the target r."""
        import numpy as np

        rng = np.random.RandomState(42)
        n = 200
        x = rng.randn(n)
        noise = rng.randn(n)
        # y = r*x + sqrt(1-r^2)*noise gives r ≈ r_target
        y = r_target * x + (1 - r_target**2) ** 0.5 * noise
        points = [{"x_value": float(xi), "y_value": float(yi)} for xi, yi in zip(x, y)]
        result = compute_relationship_stats(points)
        assert result["strength"] == expected_strength


# ---------------------------------------------------------------------------
# Linear regression and outliers
# ---------------------------------------------------------------------------


class TestRegressionAndOutliers:
    def test_slope_and_intercept(self):
        # y = 2x + 10
        result = compute_relationship_stats(_points([0, 1, 2, 3, 4], [10, 12, 14, 16, 18]))
        assert result["slope"] == pytest.approx(2.0, abs=1e-3)
        assert result["intercept"] == pytest.approx(10.0, abs=1e-3)

    def test_outlier_detected(self):
        # Linear trend y ≈ x, but one extreme outlier
        xs = list(range(50))
        ys = list(range(50))
        ys[25] = 1000  # massive outlier
        result = compute_relationship_stats(_points(xs, ys))
        assert len(result["outliers"]) >= 1
        outlier_labels = [o["label"] for o in result["outliers"]]
        assert "p25" in outlier_labels

    def test_no_outliers_in_clean_data(self):
        # Perfect linear: no outliers possible
        result = compute_relationship_stats(_points([1, 2, 3, 4, 5], [2, 4, 6, 8, 10]))
        assert result["outliers"] == []


# ---------------------------------------------------------------------------
# Warning for small samples
# ---------------------------------------------------------------------------


class TestSmallSampleWarning:
    def test_n_below_30_has_warning(self):
        result = compute_relationship_stats(
            _points(list(range(20)), [x * 2 for x in range(20)])
        )
        assert "warning" in result
        assert "observaciones" in result["warning"]

    def test_n_at_30_has_no_extra_warning(self):
        result = compute_relationship_stats(
            _points(list(range(30)), [x * 2 for x in range(30)])
        )
        assert "warning" not in result

    def test_large_dataset_stable(self):
        import numpy as np

        rng = np.random.RandomState(0)
        n = 200
        x = rng.randn(n)
        y = 0.8 * x + 0.2 * rng.randn(n)
        points = [{"x_value": float(xi), "y_value": float(yi)} for xi, yi in zip(x, y)]
        result = compute_relationship_stats(points)
        assert result["n"] == n
        assert 0.5 < result["r"] < 1.0
        assert "warning" not in result
