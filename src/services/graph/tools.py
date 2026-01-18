"""Plotly chart builders that return styled HTML/PNG bytes."""

import logging
from typing import Any
from collections import defaultdict

import plotly.graph_objects as go

from src.config.settings import get_settings

logger = logging.getLogger(__name__)

ChartArtifacts = dict[str, Any]
config_params = {
    "displayModeBar": True,
    "modeBarButtonsToRemove": [],
    "staticPlot": False,
}


def _apply_styling(fig: go.Figure, title: str) -> None:
    """Apply shared styling similar to plotly_blob_demo."""
    fig.update_layout(
        # title=f"<b>{title}</b>",
        # title_font={"color": "black", "size": 14, "family": "Inter, Arial, sans-serif"},
        template="plotly_white",
        hovermode="x unified",
        font={"family": "Inter, Arial, sans-serif", "size": 12},
        margin={"l": 60, "r": 30, "t": 70, "b": 50},
    )
    fig.update_xaxes(
        showgrid=True, gridcolor="rgba(0,0,0,0.05)", showspikes=True, spikemode="across"
    )
    fig.update_yaxes(showgrid=True, gridcolor="rgba(0,0,0,0.05)", zeroline=False)


def _render_outputs(fig: go.Figure) -> tuple[bytes, bytes]:
    """Render figure to HTML and PNG bytes.

    This function is intentionally defensive around PNG generation so that
    failures in the underlying browser / Kaleido stack do not break the
    overall graph generation pipeline.
    """
    html_bytes = fig.to_html(
        full_html=True,
        include_plotlyjs="cdn",
        config=config_params,
    ).encode("utf-8")

    png_bytes: bytes
    try:
        png_bytes = fig.to_image(format="png", width=1100, height=650, scale=2)
    except Exception as exc:  # noqa: BLE001
        # If PNG generation fails (e.g. Kaleido / browser issues), keep HTML output
        # and log a warning so the pipeline can continue gracefully.
        logger.warning("PNG generation failed: %s", exc)
        png_bytes = b""

    return html_bytes, png_bytes


def generate_pie_chart(
    data_points: list[dict[str, Any]],
    title: str,
    colors: list[str] | None = None,
) -> ChartArtifacts:
    """Generate a styled pie chart and return HTML/PNG bytes."""
    if colors is None:
        colors = get_settings().chart_color_palette

    labels = [d.get("x_value", "") for d in data_points]
    values = [d.get("y_value", 0) for d in data_points]

    fig = go.Figure(
        data=[go.Pie(labels=labels, values=values, marker_colors=colors[: len(labels)])]
    )
    _apply_styling(fig, title)
    html_bytes, png_bytes = _render_outputs(fig)
    return {"fig": fig, "html_bytes": html_bytes, "png_bytes": png_bytes}


def generate_bar_chart(
    data_points: list[dict[str, Any]],
    title: str,
    colors: list[str] | None = None,
) -> ChartArtifacts:
    """Generate a styled bar chart and return HTML/PNG bytes."""
    if colors is None:
        colors = get_settings().chart_color_palette

    x_values = [d.get("x_value", "") for d in data_points]
    y_values = [d.get("y_value", 0) for d in data_points]

    fig = go.Figure(data=[go.Bar(x=x_values, y=y_values, marker_color=colors[0])])
    _apply_styling(fig, title)
    html_bytes, png_bytes = _render_outputs(fig)
    return {"fig": fig, "html_bytes": html_bytes, "png_bytes": png_bytes}


def generate_line_chart(
    data_points: list[dict[str, Any]],
    title: str,
    colors: list[str] | None = None,
) -> ChartArtifacts:
    """Generate a styled line chart and return HTML/PNG bytes."""
    if colors is None:
        colors = get_settings().chart_color_palette
 
    series_data: dict[str, dict[str, list]] = defaultdict(lambda: {"x": [], "y": []})
    for d in data_points:
        category = d.get("category", "default")
        series_data[category]["x"].append(d.get("x_value", ""))
        series_data[category]["y"].append(d.get("y_value", 0))
 
    fig = go.Figure()
    # Crear una línea por cada category
    for idx, (category, values) in enumerate(series_data.items()):
        color = colors[idx % len(colors)]
        fig.add_trace(
            go.Scatter(
                x=values["x"],
                y=values["y"],
                mode="lines+markers",
                name=category,
                line_color=color,
            )
        )
    _apply_styling(fig, title)
    html_bytes, png_bytes = _render_outputs(fig)
    return {"fig": fig, "html_bytes": html_bytes, "png_bytes": png_bytes}


def generate_stacked_bar_chart(
    data_points: list[dict[str, Any]],
    title: str,
    colors: list[str] | None = None,
) -> ChartArtifacts:
    """Generate a styled stacked bar chart and return HTML/PNG bytes."""
    if colors is None:
        colors = get_settings().chart_color_palette

    categories: dict[str, list[dict[str, Any]]] = {}
    for d in data_points:
        cat = d.get("category", "default")
        categories.setdefault(cat, []).append(d)

    fig = go.Figure()
    for idx, (cat, points) in enumerate(categories.items()):
        x_values = [p.get("x_value", "") for p in points]
        y_values = [p.get("y_value", 0) for p in points]
        fig.add_trace(
            go.Bar(
                name=cat,
                x=x_values,
                y=y_values,
                marker_color=colors[idx % len(colors)],
            )
        )

    fig.update_layout(barmode="stack")
    _apply_styling(fig, title)
    html_bytes, png_bytes = _render_outputs(fig)
    return {"fig": fig, "html_bytes": html_bytes, "png_bytes": png_bytes}
