"""Plotly chart builders that return styled HTML/PNG bytes."""

from typing import Any, Dict, List, Optional, Tuple

import plotly.graph_objects as go

from src.config.settings import get_settings


ChartArtifacts = Dict[str, Any]


def _apply_styling(fig: go.Figure, title: str) -> None:
    """Apply shared styling similar to plotly_blob_demo."""
    fig.update_layout(
        title=title,
        template="plotly_white",
        hovermode="x unified",
        font={"family": "Inter, Arial, sans-serif", "size": 14},
        margin={"l": 60, "r": 30, "t": 70, "b": 50},
    )
    fig.update_xaxes(showgrid=True, gridcolor="rgba(0,0,0,0.05)", showspikes=True, spikemode="across")
    fig.update_yaxes(showgrid=True, gridcolor="rgba(0,0,0,0.05)", zeroline=False)


def _render_outputs(fig: go.Figure) -> Tuple[bytes, bytes]:
    """Render figure to HTML and PNG bytes."""
    html_bytes = fig.to_html(full_html=True, include_plotlyjs="cdn").encode("utf-8")
    png_bytes = fig.to_image(format="png", width=1100, height=650, scale=2)
    return html_bytes, png_bytes


def generate_pie_chart(
    data_points: List[Dict[str, Any]],
    title: str,
    colors: Optional[List[str]] = None,
) -> ChartArtifacts:
    """Generate a styled pie chart and return HTML/PNG bytes."""
    if colors is None:
        colors = get_settings().chart_color_palette

    labels = [d.get("x_value", "") for d in data_points]
    values = [d.get("y_value", 0) for d in data_points]

    fig = go.Figure(data=[go.Pie(labels=labels, values=values, marker_colors=colors[: len(labels)])])
    _apply_styling(fig, title)
    html_bytes, png_bytes = _render_outputs(fig)
    return {"fig": fig, "html_bytes": html_bytes, "png_bytes": png_bytes}


def generate_bar_chart(
    data_points: List[Dict[str, Any]],
    title: str,
    colors: Optional[List[str]] = None,
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
    data_points: List[Dict[str, Any]],
    title: str,
    colors: Optional[List[str]] = None,
) -> ChartArtifacts:
    """Generate a styled line chart and return HTML/PNG bytes."""
    if colors is None:
        colors = get_settings().chart_color_palette

    x_values = [d.get("x_value", "") for d in data_points]
    y_values = [d.get("y_value", 0) for d in data_points]

    fig = go.Figure(data=[go.Scatter(x=x_values, y=y_values, mode="lines+markers", line_color=colors[0])])
    _apply_styling(fig, title)
    html_bytes, png_bytes = _render_outputs(fig)
    return {"fig": fig, "html_bytes": html_bytes, "png_bytes": png_bytes}


def generate_stacked_bar_chart(
    data_points: List[Dict[str, Any]],
    title: str,
    colors: Optional[List[str]] = None,
) -> ChartArtifacts:
    """Generate a styled stacked bar chart and return HTML/PNG bytes."""
    if colors is None:
        colors = get_settings().chart_color_palette

    categories: Dict[str, List[Dict[str, Any]]] = {}
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

