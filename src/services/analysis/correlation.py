"""Correlation and relationship statistics for scatter analysis."""

from __future__ import annotations

import logging
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)


def _classify_strength(r: float) -> str:
    """Classify correlation strength from a Pearson r value."""
    abs_r = abs(r)
    if abs_r > 0.9:
        return "muy fuerte"
    if abs_r > 0.7:
        return "fuerte"
    if abs_r > 0.4:
        return "moderada"
    return "debil"


def _classify_direction(r: float) -> str:
    """Classify correlation direction from a Pearson r value."""
    if r > 0:
        return "positiva"
    if r < 0:
        return "negativa"
    return "nula"


def compute_relationship_stats(data_points: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute correlation, trend line, and outlier statistics.

    Args:
        data_points: List of dicts with 'x_value' and 'y_value' keys (numeric).

    Returns:
        Dict with r, r2, direction, strength, n, slope, intercept, outliers.
        On error (e.g., insufficient data), returns dict with 'warning' key.
    """
    try:
        x = np.array([float(p["x_value"]) for p in data_points], dtype=float)
        y = np.array([float(p["y_value"]) for p in data_points], dtype=float)
    except (ValueError, TypeError) as e:
        return {
            "warning": f"Valores no numericos en los datos: {e}",
            "n": len(data_points),
        }
    n = len(x)

    if n < 3:
        return {
            "warning": f"Solo {n} observaciones. Se necesitan al menos 3 para analisis de correlacion.",
            "n": n,
        }

    r = float(np.corrcoef(x, y)[0, 1])
    if np.isnan(r):
        return {
            "warning": "No se pudo calcular correlacion (varianza cero en una o ambas variables).",
            "n": n,
        }

    r2 = r ** 2

    try:
        slope, intercept = np.polyfit(x, y, 1)
    except np.linalg.LinAlgError:
        return {
            "r": round(r, 4),
            "r2": round(r2, 4),
            "n": n,
            "direction": _classify_direction(r),
            "strength": _classify_strength(r),
            "warning": "No se pudo ajustar regresion lineal (valores constantes).",
        }

    y_pred = slope * x + intercept
    residuals = y - y_pred
    std_res = float(np.std(residuals))

    outliers = []
    if std_res > 1e-10:
        for i, p in enumerate(data_points):
            dev = float(residuals[i]) / std_res
            if abs(dev) > 2:
                outliers.append({
                    "label": p.get("label", f"punto_{i}"),
                    "x": float(x[i]),
                    "y": float(y[i]),
                    "deviation": round(dev, 2),
                })

    # Trend line endpoints for frontend rendering
    x_min, x_max = float(np.min(x)), float(np.max(x))
    trend_line = {
        "x_start": round(x_min, 4),
        "y_start": round(float(slope * x_min + intercept), 4),
        "x_end": round(x_max, 4),
        "y_end": round(float(slope * x_max + intercept), 4),
    }

    direction = _classify_direction(r)
    strength = _classify_strength(r)

    stats: dict[str, Any] = {
        "r": round(r, 4),
        "r2": round(r2, 4),
        "direction": direction,
        "strength": strength,
        "n": n,
        "slope": round(float(slope), 6),
        "intercept": round(float(intercept), 4),
        "trend_line": trend_line,
        "outliers": outliers,
        "interpretation": (
            f"R² = {round(r2, 4)}: el {round(r2 * 100, 1)}% de la variabilidad en Y "
            f"se explica por X. Correlacion {direction} {strength} (r = {round(r, 4)})."
        ),
    }

    if n < 30:
        stats["warning"] = (
            f"Solo {n} observaciones. Para mayor confiabilidad estadistica, "
            "considere analizar un periodo mas amplio."
        )

    return stats
